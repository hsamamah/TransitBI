"""
GTFS-RT Parse + Load Glue Job (Glue 4.0 / Spark)
=================================================
Replaces the old Python Shell job.

What this job does (in order):
  1. List all .pb files for yesterday from seattle-transit-raw
  2. Parse TripUpdates and VehiclePositions from each .pb file
  3. Deduplicate (keep latest per key)
  4. Write parsed CSVs to seattle-transit-staging with date-partitioned paths
  5. COPY both CSVs into stg.rt_stop_time_updates and stg.rt_vehicle_positions
     via the Redshift Data API (WorkgroupName — requires Glue 4.0)

Path convention (staging bucket):
  s3://seattle-transit-staging/gtfs-rt/{agency}/{YYYY}/{MM}/{DD}/trip_updates.csv
  s3://seattle-transit-staging/gtfs-rt/{agency}/{YYYY}/{MM}/{DD}/vehicle_positions.csv

Redshift:
  Workgroup : team
  Database  : dev
  IAM Role  : arn:aws:iam::805699509606:role/RedshiftS3CopyRole

Schedule: Daily at 07:00 AM PST via EventBridge (processes previous day)

Note on the 90-day .pb retention:
  Handled by an S3 Lifecycle policy on seattle-transit-raw — not in this job.
  See lifecycle_policy.json delivered alongside this script.
"""

import sys
import os
import csv
import io
import time
import boto3
import pytz
from datetime import datetime, timezone, timedelta
from google.transit import gtfs_realtime_pb2

from awsglue.utils import getResolvedOptions

# ============================================================
# 1. Configuration
# ============================================================
RAW_BUCKET      = 'seattle-transit-raw'
STAGING_BUCKET  = 'seattle-transit-staging'
REGION          = 'us-west-2'
WORKGROUP       = 'team'
DATABASE        = 'dev'
IAM_ROLE        = 'arn:aws:iam::805699509606:role/RedshiftS3CopyRole'
LOCAL_TZ        = pytz.timezone('America/Los_Angeles')

# Poll timeout for Redshift Data API (seconds)
REDSHIFT_POLL_TIMEOUT = 300
REDSHIFT_POLL_INTERVAL = 5

AGENCIES = {
    'king-county-metro': {'agency_id': '1',  'display': 'King County Metro'},
    'sound-transit':     {'agency_id': '40', 'display': 'Sound Transit'},
}

SCHEDULE_REL_MAP = {0: 'SCHEDULED', 1: 'ADDED', 2: 'UNSCHEDULED', 3: 'CANCELED'}
VP_STATUS_MAP    = {0: 'INCOMING_AT', 1: 'STOPPED_AT', 2: 'IN_TRANSIT_TO'}

s3       = boto3.client('s3',              region_name=REGION)
rs_data  = boto3.client('redshift-data',   region_name=REGION)

# ============================================================
# 2. Date Resolution
# ============================================================
def resolve_target_date():
    """
    Use --target_date job parameter if provided (YYYY-MM-DD).
    Falls back to yesterday in local time for scheduled daily runs.
    """
    try:
        args = getResolvedOptions(sys.argv, ['target_date'])
        return args['target_date']
    except Exception:
        return (datetime.now(LOCAL_TZ) - timedelta(days=1)).strftime('%Y-%m-%d')

# ============================================================
# 3. S3 Helpers
# ============================================================
def list_pb_files(agency, feed_type, date_str):
    """List all .pb files for a given agency/feed/date with pagination."""
    prefix = f"gtfs-rt/{agency}/{feed_type}/{date_str.replace('-', '/')}/"
    paginator = s3.get_paginator('list_objects_v2')
    files = []
    for page in paginator.paginate(Bucket=RAW_BUCKET, Prefix=prefix):
        for obj in page.get('Contents', []):
            if obj['Key'].endswith('.pb'):
                files.append(obj['Key'])
    return sorted(files)


def read_pb_from_s3(s3_key):
    """Download and parse a single .pb file. Returns FeedMessage or None."""
    try:
        resp = s3.get_object(Bucket=RAW_BUCKET, Key=s3_key)
        raw  = resp['Body'].read()
        if not raw or len(raw) < 100:
            return None
        feed = gtfs_realtime_pb2.FeedMessage()
        feed.ParseFromString(raw)
        return feed if len(feed.entity) > 0 else None
    except Exception as e:
        print(f"WARN: Parse failed for {s3_key}: {e}")
        return None


def write_csv_to_staging(records, fields, agency, date_str, file_name):
    """
    Write records as CSV to the staging bucket.
    Path: s3://seattle-transit-staging/gtfs-rt/{agency}/{YYYY}/{MM}/{DD}/{file_name}
    """
    if not records:
        print(f"  SKIP: No records to write for {agency}/{file_name}")
        return None

    year, month, day = date_str.split('-')
    s3_key = f"gtfs-rt/{agency}/{year}/{month}/{day}/{file_name}"

    output = io.StringIO()
    writer = csv.DictWriter(output, fieldnames=fields, extrasaction='ignore')
    writer.writeheader()
    writer.writerows(records)

    s3.put_object(
        Bucket=STAGING_BUCKET,
        Key=s3_key,
        Body=output.getvalue().encode('utf-8'),
        ContentType='text/csv',
    )
    print(f"  WROTE: s3://{STAGING_BUCKET}/{s3_key} ({len(records)} records)")
    return f"s3://{STAGING_BUCKET}/{s3_key}"

# ============================================================
# 4. Parsing
# ============================================================
def extract_trip_updates(feed, agency_key):
    records = []
    feed_ts     = feed.header.timestamp
    feed_dt_utc = datetime.fromtimestamp(feed_ts, tz=timezone.utc)
    service_date = feed_dt_utc.astimezone(LOCAL_TZ).strftime('%Y-%m-%d')

    for entity in feed.entity:
        if not entity.HasField('trip_update'):
            continue
        tu = entity.trip_update

        for stu in tu.stop_time_update:
            # direction_id is a scalar int — HasField() would raise ValueError
            dir_id = tu.trip.direction_id
            direction = str(dir_id) if dir_id in (0, 1) else ''

            record = {
                'agency_key':            agency_key,
                'trip_id':               tu.trip.trip_id,
                'route_id':              tu.trip.route_id or '',
                'direction_id':          direction,
                'schedule_relationship': SCHEDULE_REL_MAP.get(
                                             tu.trip.schedule_relationship, 'UNKNOWN'),
                'service_date':          service_date,
                'stop_id':               stu.stop_id,
                'stop_sequence':         stu.stop_sequence,
                'arrival_delay':         stu.arrival.delay
                                             if stu.HasField('arrival') else '',
                'arrival_time_utc':      stu.arrival.time
                                             if stu.HasField('arrival') and stu.arrival.time > 0
                                             else '',
                'arrival_time_local':    '',
                'departure_delay':       stu.departure.delay
                                             if stu.HasField('departure') else '',
                'departure_time_utc':    stu.departure.time
                                             if stu.HasField('departure') and stu.departure.time > 0
                                             else '',
                'departure_time_local':  '',
                'feed_timestamp_utc':    feed_dt_utc.strftime('%Y-%m-%d %H:%M:%S'),
                'arrival_source':        'GTFS_RT_REPORTED'
                                             if stu.HasField('arrival')
                                             else 'FALLBACK_SCHEDULED',
            }

            if record['arrival_time_utc']:
                dt = datetime.fromtimestamp(record['arrival_time_utc'], tz=timezone.utc)
                record['arrival_time_local'] = dt.astimezone(LOCAL_TZ).strftime(
                    '%Y-%m-%d %H:%M:%S')

            if record['departure_time_utc']:
                dt = datetime.fromtimestamp(record['departure_time_utc'], tz=timezone.utc)
                record['departure_time_local'] = dt.astimezone(LOCAL_TZ).strftime(
                    '%Y-%m-%d %H:%M:%S')

            records.append(record)

    return records


def extract_vehicle_positions(feed, agency_key):
    records = []
    feed_ts     = feed.header.timestamp
    feed_dt_utc = datetime.fromtimestamp(feed_ts, tz=timezone.utc)

    for entity in feed.entity:
        if not entity.HasField('vehicle'):
            continue
        v = entity.vehicle

        ts_utc_str = ''
        ts_local_str = ''
        if v.timestamp > 0:
            dt_utc = datetime.fromtimestamp(v.timestamp, tz=timezone.utc)
            ts_utc_str   = dt_utc.strftime('%Y-%m-%d %H:%M:%S')
            ts_local_str = dt_utc.astimezone(LOCAL_TZ).strftime('%Y-%m-%d %H:%M:%S')

        record = {
            'agency_key':            agency_key,
            'trip_id':               v.trip.trip_id   if v.HasField('trip')    else '',
            'route_id':              v.trip.route_id  if v.HasField('trip')    else '',
            'vehicle_id':            v.vehicle.id     if v.HasField('vehicle') else '',
            'latitude':              v.position.latitude,
            'longitude':             v.position.longitude,
            'current_stop_sequence': v.current_stop_sequence
                                         if v.current_stop_sequence else '',
            'current_status':        VP_STATUS_MAP.get(v.current_status, 'UNKNOWN'),
            'timestamp_utc':         ts_utc_str,
            'timestamp_local':       ts_local_str,
            'feed_timestamp_utc':    feed_dt_utc.strftime('%Y-%m-%d %H:%M:%S'),
        }
        records.append(record)

    return records

# ============================================================
# 5. Deduplication — keep LATEST record per key
# ============================================================
def deduplicate_latest(records, key_fields, sort_field):
    """Keep the record with the highest sort_field value per composite key."""
    unique = {}
    for r in records:
        key = tuple(r.get(f, '') for f in key_fields)
        if key not in unique or str(r.get(sort_field, '')) > str(unique[key].get(sort_field, '')):
            unique[key] = r
    return list(unique.values())

# ============================================================
# 6. Redshift Data API — execute + poll
# ============================================================
def run_redshift_sql(sql, description=''):
    """
    Submit SQL to Redshift Serverless via Data API and poll until complete.
    Raises RuntimeError on failure or timeout.
    """
    print(f"  REDSHIFT: {description or sql[:80]}")
    resp = rs_data.execute_statement(
        WorkgroupName=WORKGROUP,
        Database=DATABASE,
        Sql=sql,
        WithEvent=True,
    )
    query_id = resp['Id']

    elapsed = 0
    while elapsed < REDSHIFT_POLL_TIMEOUT:
        time.sleep(REDSHIFT_POLL_INTERVAL)
        elapsed += REDSHIFT_POLL_INTERVAL
        status_resp = rs_data.describe_statement(Id=query_id)
        status = status_resp['Status']

        if status == 'FINISHED':
            print(f"  OK: {description} completed in {elapsed}s")
            return
        elif status in ('FAILED', 'ABORTED'):
            err = status_resp.get('Error', 'No error detail')
            raise RuntimeError(f"Redshift SQL failed [{description}]: {err}")
        # PICKED / STARTED / SUBMITTED — keep waiting

    raise RuntimeError(
        f"Redshift SQL timed out after {REDSHIFT_POLL_TIMEOUT}s [{description}]")


def copy_to_redshift(s3_uri, table, date_str, truncate_first=False):
    """
    COPY a single CSV from S3 into a Redshift staging table.
    Optionally truncates the table partition for the date first to allow reruns.
    """
    if truncate_first:
        if 'vehicle_positions' in table:
            run_redshift_sql(
                f"DELETE FROM {table} WHERE DATE(timestamp_local) = '{date_str}';",
                description=f"Delete {date_str} from {table}",
            )
        else:
            run_redshift_sql(
                f"DELETE FROM {table} WHERE service_date = '{date_str}';",
                description=f"Delete {date_str} from {table}",
            )

    copy_sql = f"""
        COPY {table}
        FROM '{s3_uri}'
        IAM_ROLE '{IAM_ROLE}'
        CSV
        IGNOREHEADER 1
        TIMEFORMAT 'auto'
        BLANKSASNULL
        EMPTYASNULL
        REGION '{REGION}';
    """
    run_redshift_sql(copy_sql, description=f"COPY {s3_uri} → {table}")

# ============================================================
# 7. Field Definitions (must match stg table column order)
# ============================================================
TU_FIELDS = [
    'agency_key', 'trip_id', 'route_id', 'direction_id', 'schedule_relationship',
    'service_date', 'stop_id', 'stop_sequence', 'arrival_delay', 'arrival_time_utc',
    'arrival_time_local', 'departure_delay', 'departure_time_utc', 'departure_time_local',
    'feed_timestamp_utc', 'arrival_source',
]

VP_FIELDS = [
    'agency_key', 'trip_id', 'route_id', 'vehicle_id', 'latitude', 'longitude',
    'current_stop_sequence', 'current_status', 'timestamp_utc', 'timestamp_local',
    'feed_timestamp_utc',
]

# ============================================================
# 8. Main
# ============================================================
def main():
    target_date = resolve_target_date()
    print(f"{'='*60}")
    print(f"GTFS-RT PARSE + LOAD  |  date={target_date}")
    print(f"{'='*60}")

    all_tu_s3_uris = []
    all_vp_s3_uris = []

    for agency_key, config in AGENCIES.items():
        print(f"\n--- Agency: {config['display']} ---")

        # ── Trip Updates ──────────────────────────────────────────
        tu_files = list_pb_files(agency_key, 'trip-updates', target_date)
        print(f"  Found {len(tu_files)} trip-update .pb files")

        tu_records = []
        for f in tu_files:
            feed = read_pb_from_s3(f)
            if feed:
                tu_records.extend(extract_trip_updates(feed, agency_key))

        tu_records = deduplicate_latest(
            tu_records,
            key_fields=['trip_id', 'stop_id', 'stop_sequence', 'service_date'],
            sort_field='feed_timestamp_utc',
        )
        print(f"  Trip updates after dedup: {len(tu_records)}")

        uri = write_csv_to_staging(
            tu_records, TU_FIELDS, agency_key, target_date, 'trip_updates.csv')
        if uri:
            all_tu_s3_uris.append(uri)

        # ── Vehicle Positions ─────────────────────────────────────
        vp_files = list_pb_files(agency_key, 'vehicle-positions', target_date)
        print(f"  Found {len(vp_files)} vehicle-position .pb files")

        vp_records = []
        for f in vp_files:
            feed = read_pb_from_s3(f)
            if feed:
                vp_records.extend(extract_vehicle_positions(feed, agency_key))

        vp_records = deduplicate_latest(
            vp_records,
            key_fields=['vehicle_id', 'timestamp_utc'],
            sort_field='feed_timestamp_utc',
        )
        print(f"  Vehicle positions after dedup: {len(vp_records)}")

        uri = write_csv_to_staging(
            vp_records, VP_FIELDS, agency_key, target_date, 'vehicle_positions.csv')
        if uri:
            all_vp_s3_uris.append(uri)

    # ── COPY to Redshift ──────────────────────────────────────────
    print(f"\n--- Redshift COPY ---")

    for uri in all_tu_s3_uris:
        copy_to_redshift(
            uri,
            table='stg.rt_stop_time_updates',
            date_str=target_date,
            truncate_first=True,   # safe reruns
        )

    for uri in all_vp_s3_uris:
        copy_to_redshift(
            uri,
            table='stg.rt_vehicle_positions',
            date_str=target_date,
            truncate_first=True,
        )

    # ── Populate FactTrip ActualStartTime / ActualEndTime ─────────
    print(f"\n--- FactTrip ActualStartTime/EndTime Update ---")
    facttrip_sql = f"""
        UPDATE dw.FactTrip ft
        SET
            actualstarttime = src.first_ping,
            actualendtime   = src.last_ping,
            isestimated     = 0
        FROM (
            SELECT
                vp.trip_id,
                vp.agency_key,
                CAST('{target_date}' AS DATE)   AS service_date,
                MIN(vp.timestamp_local)          AS first_ping,
                MAX(vp.timestamp_local)          AS last_ping
            FROM stg.rt_vehicle_positions vp
            WHERE vp.timestamp_local IS NOT NULL
              AND vp.trip_id <> ''
              AND DATE(vp.timestamp_local) = CAST('{target_date}' AS DATE)
            GROUP BY vp.trip_id, vp.agency_key
        ) src
        JOIN dw.DimTrip dt
            ON dt.tripid = src.trip_id
        WHERE ft.tripkey  = dt.tripkey
          AND ft.datekey  = CAST(REPLACE('{target_date}', '-', '') AS INT)
          AND ft.tripstatus = 'OPERATED';
    """
    run_redshift_sql(facttrip_sql, description=f"Update FactTrip ActualStartTime for {target_date}")

    print(f"\n{'='*60}")
    print(f"JOB COMPLETE  |  date={target_date}")
    print(f"  Trip update CSVs  : {len(all_tu_s3_uris)}")
    print(f"  Vehicle pos CSVs  : {len(all_vp_s3_uris)}")
    print(f"{'='*60}")


if __name__ == '__main__':
    main()