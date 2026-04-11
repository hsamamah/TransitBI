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
  IAM Role  : injected via --iam_role job parameter (RedshiftS3CopyRole)

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
import threading
import boto3
import pytz
from concurrent.futures import ThreadPoolExecutor, as_completed
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
LOCAL_TZ        = pytz.timezone('America/Los_Angeles')

# IAM role ARN for Redshift COPY — injected as Glue job parameter --iam_role
try:
    _base_args = getResolvedOptions(sys.argv, ['iam_role'])
    IAM_ROLE = _base_args['iam_role']
except Exception:
    raise RuntimeError(
        "Missing required Glue job parameter --iam_role. "
        "Set it in DefaultArguments (deploy_glue.sh sets this automatically)."
    )

# Poll timeout for Redshift Data API (seconds)
REDSHIFT_POLL_TIMEOUT = 300
REDSHIFT_POLL_INTERVAL = 5

AGENCIES = {
    'king-county-metro': {'agency_id': '1',  'display': 'King County Metro'},
    'sound-transit':     {'agency_id': '40', 'display': 'Sound Transit'},
}

SCHEDULE_REL_MAP = {0: 'SCHEDULED', 1: 'ADDED', 2: 'UNSCHEDULED', 3: 'CANCELED'}
VP_STATUS_MAP    = {0: 'INCOMING_AT', 1: 'STOPPED_AT', 2: 'IN_TRANSIT_TO'}

# Module-level client for single-threaded use (Redshift, S3 listings, CSV writes)
s3       = boto3.client('s3',              region_name=REGION)
rs_data  = boto3.client('redshift-data',   region_name=REGION)

# Per-thread S3 clients for parallel reads — boto3 clients are not thread-safe
_thread_local = threading.local()

def _get_thread_s3():
    if not hasattr(_thread_local, 's3'):
        _thread_local.s3 = boto3.client('s3', region_name=REGION)
    return _thread_local.s3

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
        resp = _get_thread_s3().get_object(Bucket=RAW_BUCKET, Key=s3_key)
        raw  = resp['Body'].read()
        if not raw or len(raw) < 100:
            return None
        feed = gtfs_realtime_pb2.FeedMessage()
        feed.ParseFromString(raw)
        return feed if len(feed.entity) > 0 else None
    except Exception as e:
        print(f"WARN: Parse failed for {s3_key}: {e}")
        return None


S3_READ_WORKERS = 16

def read_pb_files_parallel(s3_keys):
    """
    Download and parse .pb files in parallel.
    Yields FeedMessage objects as they complete (generator) so callers can
    process each feed immediately rather than accumulating all in memory.
    """
    with ThreadPoolExecutor(max_workers=S3_READ_WORKERS) as pool:
        futures = {pool.submit(read_pb_from_s3, key): key for key in s3_keys}
        for future in as_completed(futures):
            feed = future.result()
            if feed is not None:
                yield feed


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

    buf = io.BytesIO()
    wrapper = io.TextIOWrapper(buf, encoding='utf-8', newline='')
    writer = csv.DictWriter(wrapper, fieldnames=fields, extrasaction='ignore')
    writer.writeheader()
    writer.writerows(records)
    wrapper.flush()

    s3.put_object(
        Bucket=STAGING_BUCKET,
        Key=s3_key,
        Body=buf.getvalue(),
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
        tu   = entity.trip_update
        trip = tu.trip  # cache — accessed many times per stop_time_update

        # Per-trip fields: constant across all stop_time_updates for this entity
        dir_id            = trip.direction_id
        direction         = str(dir_id) if dir_id in (0, 1) else ''
        trip_id           = trip.trip_id
        route_id          = trip.route_id or ''
        schedule_rel      = SCHEDULE_REL_MAP.get(trip.schedule_relationship, 'UNKNOWN')
        feed_ts_str       = feed_dt_utc.strftime('%Y-%m-%d %H:%M:%S')

        for stu in tu.stop_time_update:
            has_arrival   = stu.HasField('arrival')
            has_departure = stu.HasField('departure')

            arr_time_utc  = stu.arrival.time   if has_arrival   and stu.arrival.time > 0   else ''
            dep_time_utc  = stu.departure.time if has_departure and stu.departure.time > 0 else ''

            arr_time_local = ''
            if arr_time_utc:
                arr_time_local = datetime.fromtimestamp(
                    arr_time_utc, tz=timezone.utc).astimezone(LOCAL_TZ).strftime('%Y-%m-%d %H:%M:%S')

            dep_time_local = ''
            if dep_time_utc:
                dep_time_local = datetime.fromtimestamp(
                    dep_time_utc, tz=timezone.utc).astimezone(LOCAL_TZ).strftime('%Y-%m-%d %H:%M:%S')

            records.append({
                'agency_key':            agency_key,
                'trip_id':               trip_id,
                'route_id':              route_id,
                'direction_id':          direction,
                'schedule_relationship': schedule_rel,
                'service_date':          service_date,
                'stop_id':               stu.stop_id,
                'stop_sequence':         stu.stop_sequence,
                'arrival_delay':         stu.arrival.delay   if has_arrival   else '',
                'arrival_time_utc':      arr_time_utc,
                'arrival_time_local':    arr_time_local,
                'departure_delay':       stu.departure.delay if has_departure else '',
                'departure_time_utc':    dep_time_utc,
                'departure_time_local':  dep_time_local,
                'feed_timestamp_utc':    feed_ts_str,
                'arrival_source':        'GTFS_RT_REPORTED' if has_arrival else 'FALLBACK_SCHEDULED',
                '_feed_ts':              feed_ts,
            })

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

        has_trip    = v.HasField('trip')
        has_vehicle = v.HasField('vehicle')
        record = {
            'agency_key':            agency_key,
            'trip_id':               v.trip.trip_id   if has_trip    else '',
            'route_id':              v.trip.route_id  if has_trip    else '',
            'vehicle_id':            v.vehicle.id     if has_vehicle else '',
            'latitude':              v.position.latitude,
            'longitude':             v.position.longitude,
            'current_stop_sequence': v.current_stop_sequence
                                         if v.current_stop_sequence else '',
            'current_status':        VP_STATUS_MAP.get(v.current_status, 'UNKNOWN'),
            'timestamp_utc':         ts_utc_str,
            'timestamp_local':       ts_local_str,
            'feed_timestamp_utc':    feed_dt_utc.strftime('%Y-%m-%d %H:%M:%S'),
        }
        record['_feed_ts'] = feed_ts
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
        if key not in unique or r.get(sort_field, '') > unique[key].get(sort_field, ''):
            unique[key] = r
    return list(unique.values())

# ============================================================
# 6. Redshift Data API — execute + poll
# ============================================================
def submit_redshift_sql(sql, description=''):
    """
    Submit SQL to Redshift Serverless via Data API without blocking.
    Returns (query_id, description).
    """
    print(f"  REDSHIFT SUBMIT: {description or sql[:80]}")
    resp = rs_data.execute_statement(
        WorkgroupName=WORKGROUP,
        Database=DATABASE,
        Sql=sql,
        WithEvent=True,
    )
    return resp['Id'], description


def wait_for_statements(id_description_pairs):
    """
    Poll a list of (query_id, description) pairs concurrently until all finish.
    Raises RuntimeError on any failure, abort, or timeout.
    """
    pending = dict(id_description_pairs)  # query_id -> description
    elapsed = 0
    while pending and elapsed < REDSHIFT_POLL_TIMEOUT:
        time.sleep(REDSHIFT_POLL_INTERVAL)
        elapsed += REDSHIFT_POLL_INTERVAL
        for query_id in list(pending):
            status_resp = rs_data.describe_statement(Id=query_id)
            status = status_resp['Status']
            description = pending[query_id]
            if status == 'FINISHED':
                print(f"  OK: {description} completed in {elapsed}s")
                del pending[query_id]
            elif status in ('FAILED', 'ABORTED'):
                err = status_resp.get('Error', 'No error detail')
                raise RuntimeError(f"Redshift SQL failed [{description}]: {err}")
            # PICKED / STARTED / SUBMITTED — keep waiting
    if pending:
        raise RuntimeError(
            f"Redshift SQL timed out after {REDSHIFT_POLL_TIMEOUT}s: "
            + ', '.join(pending.values()))


def run_redshift_sql(sql, description=''):
    """Submit a single SQL statement and block until complete."""
    qid, desc = submit_redshift_sql(sql, description)
    wait_for_statements([(qid, desc)])


def copy_to_redshift_parallel(copy_specs):
    """
    Execute DELETE + COPY for multiple (s3_uri, table, date_str) specs in parallel.

    Phase 1: submit all DELETE statements concurrently, wait for all to finish.
    Phase 2: submit all COPY  statements concurrently, wait for all to finish.

    copy_specs: list of dicts with keys: s3_uri, table, date_str, truncate_first
    """
    # Phase 1 — parallel DELETEs
    delete_pairs = []
    for spec in copy_specs:
        if not spec.get('truncate_first'):
            continue
        table    = spec['table']
        date_str = spec['date_str']
        if 'vehicle_positions' in table:
            sql = f"DELETE FROM {table} WHERE DATE(timestamp_local) = '{date_str}';"
        else:
            sql = f"DELETE FROM {table} WHERE service_date = '{date_str}';"
        delete_pairs.append(submit_redshift_sql(sql, description=f"Delete {date_str} from {table}"))

    if delete_pairs:
        wait_for_statements(delete_pairs)

    # Phase 2 — parallel COPYs
    copy_pairs = []
    for spec in copy_specs:
        s3_uri = spec['s3_uri']
        table  = spec['table']
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
        copy_pairs.append(submit_redshift_sql(copy_sql, description=f"COPY {s3_uri} → {table}"))

    if copy_pairs:
        wait_for_statements(copy_pairs)

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

        # Incremental dedup: maintain a dict keyed by (trip_id, stop_id, stop_sequence,
        # service_date) and keep only the record with the latest feed_timestamp_utc.
        # This avoids accumulating all records in memory before reducing them.
        tu_unique = {}
        for feed in read_pb_files_parallel(tu_files):
            for record in extract_trip_updates(feed, agency_key):
                key = (record['trip_id'], record['stop_id'],
                       record['stop_sequence'], record['service_date'])
                existing = tu_unique.get(key)
                if existing is None or record['_feed_ts'] > existing['_feed_ts']:
                    tu_unique[key] = record
        tu_records = list(tu_unique.values())
        print(f"  Trip updates after dedup: {len(tu_records)}")

        uri = write_csv_to_staging(
            tu_records, TU_FIELDS, agency_key, target_date, 'trip_updates.csv')
        if uri:
            all_tu_s3_uris.append(uri)

        # ── Vehicle Positions ─────────────────────────────────────
        vp_files = list_pb_files(agency_key, 'vehicle-positions', target_date)
        print(f"  Found {len(vp_files)} vehicle-position .pb files")

        vp_unique = {}
        for feed in read_pb_files_parallel(vp_files):
            for record in extract_vehicle_positions(feed, agency_key):
                key = (record['vehicle_id'], record['timestamp_utc'])
                existing = vp_unique.get(key)
                if existing is None or record['_feed_ts'] > existing['_feed_ts']:
                    vp_unique[key] = record
        vp_records = list(vp_unique.values())
        print(f"  Vehicle positions after dedup: {len(vp_records)}")

        uri = write_csv_to_staging(
            vp_records, VP_FIELDS, agency_key, target_date, 'vehicle_positions.csv')
        if uri:
            all_vp_s3_uris.append(uri)

    # ── COPY to Redshift (parallel DELETE then parallel COPY) ─────
    print(f"\n--- Redshift COPY ---")

    copy_specs = [
        {'s3_uri': uri, 'table': 'stg.rt_stop_time_updates',
         'date_str': target_date, 'truncate_first': True}
        for uri in all_tu_s3_uris
    ] + [
        {'s3_uri': uri, 'table': 'stg.rt_vehicle_positions',
         'date_str': target_date, 'truncate_first': True}
        for uri in all_vp_s3_uris
    ]
    copy_to_redshift_parallel(copy_specs)

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