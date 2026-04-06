"""
FactServiceDay Load Job  v2
============================
Derives one row per agency per service day from FactTrip.
Must run AFTER facttrip-skeleton-and-merge-load is complete for the
same date range.

Grain: one row per agencykey per datekey (not per mode — FactServiceDay
has no mode column; mode-level analysis is done via BI views on DimAgency).

What this job does:
  1. Reads job parameters from DynamoDB (set by transit-pipeline-inspector)
  2. For each date, DELETE existing rows then INSERT fresh aggregation:
       - Trip counts by status (OPERATED, MISSED, CANCELLED, ADDED)
       - MissedTripRate = MISSED / (total - CANCELLED) — NTD convention
       - ReportedVRM / VRH totals and EstimatedVRM / VRH split
       - PeakVehicleCount via VOMS time-slice algorithm
       - IsSpecialEventDay, FeedGapFlag, DataQualityAlertFlag, IsOfficial
  3. DELETE + INSERT is always run for idempotency (no MERGE in Redshift)

VOMS time-slice algorithm:
  Non-equi join FactTrip × DimTime — a trip is active at minute M when:
    minutes_from_midnight(ActualStartTime) <= DimTime.TimeKey <= minutes_from_midnight(ActualEndTime)
  EXTRACT(hour)*60 + EXTRACT(minute) used on both sides to convert
  TIMESTAMP to minute-of-day integer matching DimTime.TimeKey (0-1439).
  This avoids the HH24:MI string comparison bug which fails for overnight
  trips where endtime < starttime when compared as strings.
  Only OPERATED, non-special-event trips with non-NULL start/end counted.

Changelog v2:
  - Fixed VOMS non-equi join — replaced TO_CHAR/CAST(TIME) with
    EXTRACT(hour)*60+EXTRACT(minute) to correctly handle overnight trips
  - Fixed docstring — grain is per agency per day, not per mode
  - Fixed _build_date_list type hint — proper date import
  - Fixed exception chaining in _read_ddb_params — raise ... from e
  - Removed unused force parameter from process_date signature
    (DELETE always runs — force has no distinct effect)
  - Added gap_dates support — reads from DynamoDB gap_dates list
    rather than always expanding start→end range

DynamoDB parameter key:
  PK: {workflow_run_id}#{job_name}
  SK: "config"
  job_name: factserviceday-load

Supports:
  --WORKFLOW_RUN_ID   passed by Glue workflow (reads DynamoDB params)
  --start_date        YYYY-MM-DD override (skips DynamoDB lookup)
  --end_date          YYYY-MM-DD override (skips DynamoDB lookup)

Author: Transit DW Team — P4
"""

import sys
import time
import json
import boto3
from datetime import datetime, timedelta, date
import pytz
from awsglue.utils import getResolvedOptions

# ============================================================
# Configuration
# ============================================================
REGION    = 'us-west-2'
WORKGROUP = 'team'
DATABASE  = 'dev'
LOCAL_TZ  = pytz.timezone('America/Los_Angeles')

DYNAMODB_TABLE         = 'seattle-transit-pipeline'
JOB_NAME               = 'factserviceday-load'
REDSHIFT_POLL_TIMEOUT  = 900
REDSHIFT_POLL_INTERVAL = 5

# DataQualityAlertFlag: flag day if >15% of operated trips used fallback VRM
DQ_ESTIMATED_RATE_THRESHOLD = 0.15

# FeedGapFlag: flag day if >20% of operated trips have no RT ping coverage
FEED_GAP_THRESHOLD = 0.20

rs_data   = boto3.client('redshift-data', region_name=REGION)
dynamodb  = boto3.resource('dynamodb',    region_name=REGION)
ddb_table = dynamodb.Table(DYNAMODB_TABLE)


# ============================================================
# Argument Resolution
# ============================================================
def resolve_args() -> dict:
    """
    Reads job parameters in priority order:
      1. --start_date + --end_date  (direct override)
      2. --WORKFLOW_RUN_ID          (reads DynamoDB)
      3. Default: yesterday

    Returns dict: dates, skip, source.
    Force is not returned — DELETE always runs regardless.
    """
    args_available = set()
    for arg in ['start_date', 'end_date', 'WORKFLOW_RUN_ID']:
        try:
            getResolvedOptions(sys.argv, [arg])
            args_available.add(arg)
        except Exception:
            pass

    # Priority 1: direct override
    if 'start_date' in args_available and 'end_date' in args_available:
        resolved = getResolvedOptions(sys.argv, ['start_date', 'end_date'])
        start    = datetime.strptime(resolved['start_date'], '%Y-%m-%d').date()
        end      = datetime.strptime(resolved['end_date'],   '%Y-%m-%d').date()
        return {
            'dates' : _build_date_list(start, end),
            'skip'  : False,
            'source': 'direct_params',
        }

    # Priority 2: DynamoDB
    if 'WORKFLOW_RUN_ID' in args_available:
        workflow_run_id = getResolvedOptions(
            sys.argv, ['WORKFLOW_RUN_ID'])['WORKFLOW_RUN_ID']
        params = _read_ddb_params(workflow_run_id)

        if params.get('skip') == 'true':
            print(f"  DynamoDB skip=true for {JOB_NAME} — nothing to do.")
            return {'dates': [], 'skip': True, 'source': 'dynamodb'}

        # Use gap_dates list when available — avoids processing clean dates
        # between non-contiguous gaps
        gap_dates_raw = params.get('gap_dates', '[]')
        gap_dates     = json.loads(gap_dates_raw) if gap_dates_raw else []

        if gap_dates:
            dates = sorted(gap_dates)
        else:
            start = datetime.strptime(params['start_date'], '%Y-%m-%d').date()
            end   = datetime.strptime(params['end_date'],   '%Y-%m-%d').date()
            dates = _build_date_list(start, end)

        return {'dates': dates, 'skip': False, 'source': 'dynamodb'}

    # Priority 3: default yesterday
    yesterday = (datetime.now(LOCAL_TZ) - timedelta(days=1)).date()
    return {
        'dates' : [yesterday.strftime('%Y-%m-%d')],
        'skip'  : False,
        'source': 'default_yesterday',
    }


def _read_ddb_params(workflow_run_id: str) -> dict:
    """Read job config from DynamoDB. Raises with chained exception on failure."""
    try:
        resp = ddb_table.get_item(Key={
            'PK': f"{workflow_run_id}#{JOB_NAME}",
            'SK': 'config'
        })
        item = resp.get('Item')
        if not item:
            raise RuntimeError(
                f"No DynamoDB config found for {workflow_run_id}#{JOB_NAME}"
            )
        return item
    except Exception as e:
        raise RuntimeError(
            f"DynamoDB read failed for {JOB_NAME}: {e}"
        ) from e                                  # chain for full traceback


def _build_date_list(start: date, end: date) -> list:
    """Build list of YYYY-MM-DD strings from start to end inclusive."""
    dates   = []
    current = start
    while current <= end:
        dates.append(current.strftime('%Y-%m-%d'))
        current += timedelta(days=1)
    return dates


# ============================================================
# Redshift Data API
# ============================================================
def run_sql(sql: str, description: str = '', timeout: int = None,
            fetch_results: bool = False):
    """Execute SQL and poll until complete. Returns (status_resp, query_id)."""
    timeout = timeout or REDSHIFT_POLL_TIMEOUT
    label   = description or sql[:80].strip()
    print(f"\n  → {label}")

    resp     = rs_data.execute_statement(
        WorkgroupName=WORKGROUP,
        Database=DATABASE,
        Sql=sql,
        WithEvent=True,
    )
    query_id = resp['Id']
    elapsed  = 0

    while elapsed < timeout:
        time.sleep(REDSHIFT_POLL_INTERVAL)
        elapsed += REDSHIFT_POLL_INTERVAL
        status_resp = rs_data.describe_statement(Id=query_id)
        status      = status_resp['Status']

        if status == 'FINISHED':
            rows    = status_resp.get('ResultRows',  0)
            updated = status_resp.get('UpdatedRows', 0)
            print(f"     ✓ {label} — ResultRows={rows} UpdatedRows={updated} ({elapsed}s)")
            if fetch_results and rows and rows > 0:
                _print_result_rows(query_id)
            return status_resp, query_id

        elif status in ('FAILED', 'ABORTED'):
            err = status_resp.get('Error', 'No error detail')
            raise RuntimeError(f"SQL FAILED [{label}]: {err}")

    raise RuntimeError(f"SQL TIMED OUT after {timeout}s [{label}]")


def _print_result_rows(query_id: str):
    """Fetch and print result rows. Uses next() to handle zero values correctly."""
    try:
        result = rs_data.get_statement_result(Id=query_id)
        cols   = [c['label'] for c in result.get('ColumnMetadata', [])]
        if cols:
            print(f"     {'  |  '.join(cols)}")
            print(f"     {'  |  '.join(['---'] * len(cols))}")
        for row in result.get('Records', []):
            values = [
                next((str(v) for k, v in col.items() if v is not None), 'NULL')
                for col in row
            ]
            print(f"     {'  |  '.join(values)}")
    except Exception as e:
        print(f"     (Could not fetch result rows: {e})")


# ============================================================
# FactServiceDay Load SQL
# ============================================================
def delete_sql(target_date: str) -> str:
    """
    Delete existing FactServiceDay rows for target_date.
    Always runs before INSERT — guarantees idempotency on re-runs.
    """
    return f"""
    DELETE FROM dw.FactServiceDay
    WHERE datekey = CAST(REPLACE('{target_date}', '-', '') AS INTEGER);
    """


def load_sql(target_date: str) -> str:
    """
    Aggregates FactTrip into FactServiceDay for target_date.

    Grain: one row per agencykey per datekey.

    Trip counts:
      ScheduledTrips  = all rows (OPERATED + MISSED + CANCELLED + ADDED)
      OperatedTrips   = OPERATED + ADDED
      MissedTrips     = MISSED only
      CancelledTrips  = CANCELLED only
      MissedTripRate  = MISSED / (total - CANCELLED)  — NTD convention

    VRM/VRH:
      ReportedVRM/VRH  = SUM for OPERATED+ADDED rows
      EstimatedVRM/VRH = portion of above where IsEstimated=TRUE

    VOMS (PeakVehicleCount):
      Non-equi join FactTrip × DimTime.
      A trip is active at minute M (DimTime.TimeKey = M) when:
        EXTRACT(hour)*60 + EXTRACT(minute) of ActualStartTime <= M
        AND
        EXTRACT(hour)*60 + EXTRACT(minute) of ActualEndTime   >= M
      Uses integer minute-of-day (0-1439) matching DimTime.TimeKey
      directly — avoids HH24:MI string comparison which breaks for
      overnight trips where endtime string < starttime string.
      COUNT active trips per minute, MAX over day = PeakVehicleCount.
      Only OPERATED, IsSpecialEvent=FALSE trips with non-NULL times counted.

    FeedGapFlag:
      TRUE if >20% of operated trips have actualpingcount IS NULL

    DataQualityAlertFlag:
      TRUE if >15% of operated trips used estimated (fallback) VRM

    IsOfficial:
      TRUE when IsSpecialEventDay=FALSE, OperatedTrips>0,
      and DataQualityAlertFlag=FALSE
    """
    return f"""
    INSERT INTO dw.FactServiceDay (
        agencykey,
        datekey,
        scheduledtrips,
        operatedtrips,
        missedtrips,
        cancelledtrips,
        missedtriprate,
        unmatchedtripcount,
        reportedvrm,
        reportedvrh,
        estimatedvrm,
        estimatedvrh,
        peakvehiclecount,
        peaktimekey,
        isspecialeventday,
        feedgapflag,
        dataqualityalertflag,
        isofficial
    )
    WITH

    -- ── Base trip aggregation per agency per day ─────────────
    trip_agg AS (
        SELECT
            ft.agencykey,
            ft.datekey,
            COUNT(*)                                                AS total_trips,
            SUM(CASE WHEN ft.tripstatus IN ('OPERATED','ADDED')
                     THEN 1 ELSE 0 END)                            AS operated_trips,
            SUM(CASE WHEN ft.tripstatus = 'MISSED'
                     THEN 1 ELSE 0 END)                            AS missed_trips,
            SUM(CASE WHEN ft.tripstatus = 'CANCELLED'
                     THEN 1 ELSE 0 END)                            AS cancelled_trips,
            SUM(CASE WHEN ft.tripkey = 0
                     THEN 1 ELSE 0 END)                            AS unmatched_trips,
            -- MissedTripRate: MISSED / (total - CANCELLED)
            -- CANCELLED excluded from denominator per NTD S-10 convention
            CASE
                WHEN COUNT(*) - SUM(CASE WHEN ft.tripstatus = 'CANCELLED'
                                         THEN 1 ELSE 0 END) > 0
                THEN CAST(SUM(CASE WHEN ft.tripstatus = 'MISSED'
                                   THEN 1 ELSE 0 END) AS NUMERIC) /
                     NULLIF(
                         COUNT(*) - SUM(CASE WHEN ft.tripstatus = 'CANCELLED'
                                             THEN 1 ELSE 0 END),
                     0)
                ELSE NULL
            END                                                    AS missed_trip_rate,
            SUM(CASE WHEN ft.tripstatus IN ('OPERATED','ADDED')
                     THEN ft.reportedvrm ELSE 0 END)               AS reported_vrm,
            SUM(CASE WHEN ft.tripstatus IN ('OPERATED','ADDED')
                     THEN ft.reportedvrh ELSE 0 END)               AS reported_vrh,
            SUM(CASE WHEN ft.isestimated = TRUE
                      AND ft.tripstatus IN ('OPERATED','ADDED')
                     THEN ft.reportedvrm ELSE 0 END)               AS estimated_vrm,
            SUM(CASE WHEN ft.isestimated = TRUE
                      AND ft.tripstatus IN ('OPERATED','ADDED')
                     THEN ft.reportedvrh ELSE 0 END)               AS estimated_vrh,
            -- FeedGapFlag: >20% of operated trips have no RT ping data
            CASE
                WHEN SUM(CASE WHEN ft.tripstatus IN ('OPERATED','ADDED')
                              THEN 1 ELSE 0 END) > 0
                 AND CAST(SUM(CASE WHEN ft.tripstatus IN ('OPERATED','ADDED')
                                    AND ft.actualpingcount IS NULL
                               THEN 1 ELSE 0 END) AS NUMERIC) /
                     NULLIF(SUM(CASE WHEN ft.tripstatus IN ('OPERATED','ADDED')
                                     THEN 1 ELSE 0 END), 0)
                     > {FEED_GAP_THRESHOLD}
                THEN TRUE ELSE FALSE
            END                                                    AS feed_gap_flag,
            -- DataQualityAlertFlag: >15% of operated trips used fallback VRM
            CASE
                WHEN SUM(CASE WHEN ft.tripstatus IN ('OPERATED','ADDED')
                              THEN 1 ELSE 0 END) > 0
                 AND CAST(SUM(CASE WHEN ft.isestimated = TRUE
                                    AND ft.tripstatus IN ('OPERATED','ADDED')
                               THEN 1 ELSE 0 END) AS NUMERIC) /
                     NULLIF(SUM(CASE WHEN ft.tripstatus IN ('OPERATED','ADDED')
                                     THEN 1 ELSE 0 END), 0)
                     > {DQ_ESTIMATED_RATE_THRESHOLD}
                THEN TRUE ELSE FALSE
            END                                                    AS dq_alert_flag,
            CASE WHEN SUM(CASE WHEN ft.isspecialevent = TRUE THEN 1 ELSE 0 END) > 0
                 THEN TRUE ELSE FALSE END                              AS is_special_event_day
        FROM dw.FactTrip ft
        WHERE ft.datekey = CAST(REPLACE('{target_date}', '-', '') AS INTEGER)
        GROUP BY ft.agencykey, ft.datekey
    ),

    -- ── VOMS: active trips per minute ────────────────────────
    -- Non-equi join FactTrip × DimTime.
    -- DimTime.TimeKey = minutes from midnight (0-1439).
    -- Trip start/end converted to minutes from midnight via EXTRACT
    -- so the join uses integer comparison on both sides.
    -- This correctly handles overnight trips:
    --   a trip 23:50→00:10 has start_min=1430, end_min=10
    --   it is active for minutes 1430-1439 (handled by start_min <= timekey)
    --   and minutes 0-10 (handled by end_min >= timekey)
    --   Two separate ranges joined with OR.
    -- Only OPERATED, non-special-event trips with non-NULL start/end counted.
    voms_per_minute AS (
        SELECT
            ft.agencykey,
            ft.datekey,
            dt.timekey,
            COUNT(*)                                               AS active_trips
        FROM dw.FactTrip ft
        CROSS JOIN dw.DimTime dt
        WHERE ft.datekey        = CAST(REPLACE('{target_date}', '-', '') AS INTEGER)
          AND ft.tripstatus     = 'OPERATED'
          AND ft.isspecialevent = FALSE
          AND ft.actualstarttime IS NOT NULL
          AND ft.actualendtime   IS NOT NULL
          AND (
              -- Normal trip: start <= minute <= end (same-day trips)
              (
                  (EXTRACT(hour FROM ft.actualstarttime) * 60 +
                   EXTRACT(minute FROM ft.actualstarttime)) <= dt.timekey
                  AND
                  (EXTRACT(hour FROM ft.actualendtime) * 60 +
                   EXTRACT(minute FROM ft.actualendtime)) >= dt.timekey
                  AND ft.actualstarttime <= ft.actualendtime
              )
              OR
              -- Overnight trip: start > end in minute-of-day terms
              -- Active from start_min to 1439 OR from 0 to end_min
              (
                  ft.actualstarttime > ft.actualendtime
                  AND (
                      (EXTRACT(hour FROM ft.actualstarttime) * 60 +
                       EXTRACT(minute FROM ft.actualstarttime)) <= dt.timekey
                      OR
                      (EXTRACT(hour FROM ft.actualendtime) * 60 +
                       EXTRACT(minute FROM ft.actualendtime)) >= dt.timekey
                  )
              )
          )
        GROUP BY ft.agencykey, ft.datekey, dt.timekey
    ),

    -- ── Peak vehicle count per agency ────────────────────────
    -- MAX(active_trips) over all minutes = PeakVehicleCount (VOMS).
    -- PeakTimeKey = the timekey of the minute with most active trips.
    -- On ties, MAX(timekey) picks the latest peak minute.
    voms_peak AS (
        SELECT
            agencykey,
            datekey,
            MAX(active_trips)                                      AS peak_vehicle_count,
            MAX(CASE
                WHEN active_trips = (
                    SELECT MAX(v2.active_trips)
                    FROM voms_per_minute v2
                    WHERE v2.agencykey = voms_per_minute.agencykey
                      AND v2.datekey   = voms_per_minute.datekey
                ) THEN timekey ELSE NULL END)                      AS peak_timekey
        FROM voms_per_minute
        GROUP BY agencykey, datekey
    )

    -- ── Final INSERT SELECT ───────────────────────────────────
    SELECT
        ta.agencykey,
        ta.datekey,
        ta.total_trips                                             AS scheduledtrips,
        ta.operated_trips                                          AS operatedtrips,
        ta.missed_trips                                            AS missedtrips,
        ta.cancelled_trips                                         AS cancelledtrips,
        ta.missed_trip_rate                                        AS missedtriprate,
        ta.unmatched_trips                                         AS unmatchedtripcount,
        ta.reported_vrm                                            AS reportedvrm,
        ta.reported_vrh                                            AS reportedvrh,
        ta.estimated_vrm                                           AS estimatedvrm,
        ta.estimated_vrh                                           AS estimatedvrh,
        COALESCE(vp.peak_vehicle_count, 0)                         AS peakvehiclecount,
        COALESCE(vp.peak_timekey, 0)                               AS peaktimekey,
        ta.is_special_event_day                                    AS isspecialeventday,
        ta.feed_gap_flag                                           AS feedgapflag,
        ta.dq_alert_flag                                           AS dataqualityalertflag,
        CASE
            WHEN ta.is_special_event_day = FALSE
             AND ta.operated_trips       > 0
             AND ta.dq_alert_flag        = FALSE
            THEN TRUE
            ELSE FALSE
        END                                                        AS isofficial
    FROM trip_agg ta
    LEFT JOIN voms_peak vp
        ON  vp.agencykey = ta.agencykey
        AND vp.datekey   = ta.datekey;
    """


def validation_sql(target_date: str) -> str:
    return f"""
    SELECT
        agencykey,
        scheduledtrips,
        operatedtrips,
        missedtrips,
        cancelledtrips,
        ROUND(missedtriprate::NUMERIC,  3)    AS missedtriprate,
        ROUND(reportedvrm::NUMERIC,     2)    AS reportedvrm,
        ROUND(reportedvrh::NUMERIC,     2)    AS reportedvrh,
        peakvehiclecount,
        isspecialeventday,
        feedgapflag,
        dataqualityalertflag,
        isofficial
    FROM dw.FactServiceDay
    WHERE datekey = CAST(REPLACE('{target_date}', '-', '') AS INTEGER)
    ORDER BY agencykey;
    """


# ============================================================
# Per-date processing
# ============================================================
def process_date(target_date: str, timeout: int):
    """
    DELETE + INSERT for a single date.
    Force flag removed — DELETE always runs, making force redundant.
    """
    print(f"\n{'='*60}")
    print(f"  DATE: {target_date}")
    print(f"{'='*60}")

    run_sql(
        delete_sql(target_date),
        description=f"[{target_date}] Delete existing FactServiceDay rows",
        timeout=timeout
    )
    run_sql(
        load_sql(target_date),
        description=f"[{target_date}] Load FactServiceDay from FactTrip",
        timeout=timeout
    )
    run_sql(
        validation_sql(target_date),
        description=f"[{target_date}] Validation summary",
        timeout=timeout,
        fetch_results=True
    )


# ============================================================
# Main
# ============================================================
def main():
    config  = resolve_args()
    dates   = config['dates']
    skip    = config['skip']
    source  = config['source']
    timeout = REDSHIFT_POLL_TIMEOUT

    print(f"\n{'='*60}")
    print(f"FACTSERVICEDAY LOAD JOB  v2")
    print(f"  source  : {source}")
    print(f"  dates   : {dates[0] if dates else 'none'} → "
          f"{dates[-1] if dates else 'none'}  ({len(dates)} day(s))")
    print(f"  skip    : {skip}")
    print(f"{'='*60}")

    if skip or not dates:
        print("\n  Nothing to do — exiting cleanly.")
        return

    results = {'success': [], 'failed': []}

    for target_date in dates:
        try:
            process_date(target_date, timeout)
            results['success'].append(target_date)
        except Exception as e:
            print(f"\n  ✗ FAILED [{target_date}]: {e}")
            results['failed'].append((target_date, str(e)))
            continue

    print(f"\n{'='*60}")
    print(f"JOB COMPLETE")
    print(f"  Succeeded : {len(results['success'])} date(s) — {results['success']}")
    print(f"  Failed    : {len(results['failed'])} date(s)")
    if results['failed']:
        print(f"\n  Failed dates:")
        for d, err in results['failed']:
            print(f"    {d} — {err}")
        raise RuntimeError(
            f"Job completed with {len(results['failed'])} failed date(s). "
            "See logs above for details."
        )
    print(f"{'='*60}\n")


if __name__ == '__main__':
    main()
