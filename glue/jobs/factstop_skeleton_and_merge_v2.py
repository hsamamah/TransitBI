"""
FactStop Skeleton Load + RT Merge Job  v2
==========================================
Phase 1 — Skeleton Insert
  Explodes GTFS static schedule into one FactStop row per stop visit
  per trip per service date. All actual columns are NULL on insert.
  Fully idempotent — skips rows that already exist.

Phase 2 — RT Merge Update
  Joins stg.rt_stop_time_updates against the skeleton rows and updates
  ActualArrival, ArrivalDevSeconds, OTP flags, and IsOfficial.
  Only targets FALLBACK_SCHEDULED rows where RT data exists.

Supports:
  --target_date  YYYY-MM-DD           single date (default: yesterday)
  --start_date   YYYY-MM-DD           backfill range start
  --end_date     YYYY-MM-DD           backfill range end
  --phase        skeleton|merge|both  which phases to run (default: both)
  --force        re-process GTFS_RT_REPORTED rows in merge phase
  --timeout      seconds              Redshift poll timeout (default: 900)

Run order dependency:
  DimRoute, DimStop, DimTrip, DimService, DimDirection, DimShape,
  DimDate, DimTime, DimAgency, DimFeedVersion must all be populated
  before running Phase 1.
  stg.rt_stop_time_updates must be populated before Phase 2.

Changelog v2:
  - Fixed stg.calendar date column comparisons — cast VARCHAR to DATE
  - Fixed stg.calendar_dates date column cast
  - Fixed stop_sequence cast with numeric guard
  - Fixed NOT EXISTS idempotency — excludes zero-key rows from insert
  - Fixed missing table aliases on calendar CTE to avoid column ambiguity
  - Added pre-flight dimension check before skeleton insert
  - Added get_statement_result to print validation query output
  - Raised default REDSHIFT_POLL_TIMEOUT to 900s
  - Documented OTP DATEDIFF triple-computation as intentional
  - Removed unused date import
  - Simplified dow_col lookup to a list

Author: Transit DW Team — P1 / P3
"""

import sys
import time
import boto3
from datetime import datetime, timedelta
import pytz
from awsglue.utils import getResolvedOptions

# ============================================================
# Configuration
# ============================================================
REGION    = 'us-west-2'
WORKGROUP = 'team'
DATABASE  = 'dev'
LOCAL_TZ  = pytz.timezone('America/Los_Angeles')

REDSHIFT_POLL_TIMEOUT  = 900   # seconds — raised for large skeleton inserts
REDSHIFT_POLL_INTERVAL = 5

# OTP thresholds (seconds)
OTP_EARLY_THRESHOLD = -60    # more than 1 min early
OTP_LATE_THRESHOLD  = 300    # more than 5 min late

# Minimum dimension rows required before skeleton insert proceeds
PREFLIGHT_MIN_DIMTRIP_ROWS = 1

# Day-of-week column names in stg.calendar indexed by Python weekday() 0=Monday
DOW_COLUMNS = [
    'monday', 'tuesday', 'wednesday', 'thursday',
    'friday', 'saturday', 'sunday'
]

rs_data = boto3.client('redshift-data', region_name=REGION)

# ============================================================
# Argument Resolution
# ============================================================
def resolve_args():
    """
    Returns a dict with:
      dates   : list of YYYY-MM-DD strings to process
      phase   : 'skeleton' | 'merge' | 'both'
      force   : bool — re-process already-updated RT rows
      timeout : int  — Redshift poll timeout in seconds
    """
    args_available = set()
    for arg in ['start_date', 'end_date', 'target_date', 'phase', 'force', 'timeout']:
        try:
            getResolvedOptions(sys.argv, [arg])
            args_available.add(arg)
        except Exception:
            pass

    # Phase
    phase = 'both'
    if 'phase' in args_available:
        phase = getResolvedOptions(sys.argv, ['phase'])['phase'].lower()
        if phase not in ('skeleton', 'merge', 'both'):
            raise ValueError(f"--phase must be skeleton | merge | both, got: {phase}")

    # Force flag
    force = False
    if 'force' in args_available:
        force_val = getResolvedOptions(sys.argv, ['force'])['force'].lower()
        force = force_val in ('true', '1', 'yes')

    # Timeout
    timeout = REDSHIFT_POLL_TIMEOUT
    if 'timeout' in args_available:
        timeout = int(getResolvedOptions(sys.argv, ['timeout'])['timeout'])

    # Date range
    yesterday = (datetime.now(LOCAL_TZ) - timedelta(days=1)).date()

    if 'start_date' in args_available and 'end_date' in args_available:
        resolved = getResolvedOptions(sys.argv, ['start_date', 'end_date'])
        start = datetime.strptime(resolved['start_date'], '%Y-%m-%d').date()
        end   = datetime.strptime(resolved['end_date'],   '%Y-%m-%d').date()
        if start > end:
            raise ValueError(f"start_date {start} is after end_date {end}")
    elif 'target_date' in args_available:
        target = getResolvedOptions(sys.argv, ['target_date'])['target_date']
        start = end = datetime.strptime(target, '%Y-%m-%d').date()
    else:
        start = end = yesterday

    dates = []
    current = start
    while current <= end:
        dates.append(current.strftime('%Y-%m-%d'))
        current += timedelta(days=1)

    return {'dates': dates, 'phase': phase, 'force': force, 'timeout': timeout}


# ============================================================
# Redshift Data API
# ============================================================
def run_sql(sql, description='', timeout=None, fetch_results=False):
    """
    Execute SQL via Redshift Data API and poll until complete.
    Returns the describe_statement response.
    If fetch_results=True, also fetches and prints result rows.
    """
    timeout = timeout or REDSHIFT_POLL_TIMEOUT
    label   = description or sql[:80].strip()
    print(f"\n  → {label}")

    resp = rs_data.execute_statement(
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
        status = status_resp['Status']

        if status == 'FINISHED':
            rows    = status_resp.get('ResultRows', 0)
            updated = status_resp.get('UpdatedRows', 0)
            print(f"     ✓ {label} — ResultRows={rows} UpdatedRows={updated} ({elapsed}s)")

            if fetch_results and rows and rows > 0:
                _print_result_rows(query_id)

            return status_resp

        elif status in ('FAILED', 'ABORTED'):
            err = status_resp.get('Error', 'No error detail')
            raise RuntimeError(f"SQL FAILED [{label}]: {err}")

    raise RuntimeError(f"SQL TIMED OUT after {timeout}s [{label}]")


def _print_result_rows(query_id: str):
    """Fetch and print result rows from a completed SELECT statement."""
    try:
        result = rs_data.get_statement_result(Id=query_id)
        # Print column headers
        cols = [c['label'] for c in result.get('ColumnMetadata', [])]
        if cols:
            print(f"     {'  |  '.join(cols)}")
            print(f"     {'  |  '.join(['---'] * len(cols))}")
        # Print rows
        for row in result.get('Records', []):
            values = []
            for col in row:
                # Each col is a dict with one key indicating type
                val = (
                    col.get('stringValue') or
                    str(col.get('longValue', '')) or
                    str(col.get('doubleValue', '')) or
                    col.get('booleanValue', '') or
                    'NULL'
                )
                values.append(str(val))
            print(f"     {'  |  '.join(values)}")
    except Exception as e:
        print(f"     (Could not fetch result rows: {e})")


# ============================================================
# Pre-flight Check
# ============================================================
def preflight_check():
    """
    Verify DimTrip is populated before attempting skeleton insert.
    Fails fast with a clear error rather than silently inserting
    zero-key rows for every stop visit.
    """
    print("\n  → Pre-flight: checking DimTrip is populated")
    resp = run_sql(
        f"""
        SELECT COUNT(*) AS dimtrip_rows
        FROM dw.DimTrip
        WHERE tripkey > 0;
        """,
        description="Pre-flight DimTrip row count",
        fetch_results=True
    )

    # Fetch the count value
    try:
        result  = rs_data.get_statement_result(Id=resp['Id'] if 'Id' in resp else None)
        count   = int(result['Records'][0][0].get('longValue', 0))
    except Exception:
        # If we can't read the count, fail safe
        raise RuntimeError(
            "Pre-flight check failed — could not read DimTrip row count. "
            "Ensure DimTrip is populated before running skeleton insert."
        )

    if count < PREFLIGHT_MIN_DIMTRIP_ROWS:
        raise RuntimeError(
            f"Pre-flight FAILED — DimTrip has {count} rows (minimum: "
            f"{PREFLIGHT_MIN_DIMTRIP_ROWS}). "
            f"Populate dimension tables before running skeleton insert."
        )

    print(f"     ✓ Pre-flight passed — DimTrip has {count:,} rows")


# ============================================================
# Phase 1 — Skeleton Insert SQL
# ============================================================
def skeleton_insert_sql(target_date: str) -> str:
    """
    Inserts one FactStop row per stop visit per trip that runs on target_date.

    Service date resolution:
      1. stg.calendar — service_ids where target_date falls within
         [start_date, end_date] AND the day-of-week column = '1'.
         All date comparisons cast VARCHAR → DATE explicitly.
      2. UNION calendar_dates exception_type='1' (service added)
      3. EXCEPT calendar_dates exception_type='2' (service cancelled)

    Time conversion:
      arrival_time / departure_time strings like '07:30:00' or '25:15:00'
      are converted to seconds from midnight as integers.
      Overnight trips (>= 24:00:00) produce seconds > 86400 — intentional
      per schema design, never clamped.

    TimeKey:
      Derived as (hours * 60 + minutes) MOD 1440 to map overnight
      times back into the 0–1439 DimTime range.

    stop_sequence:
      Cast guarded — rows with non-numeric stop_sequence are excluded
      rather than failing the entire insert.

    Surrogate keys:
      All resolved via LEFT JOIN with COALESCE(..., 0) fallback to zero key.

    Idempotency:
      WHERE NOT EXISTS prevents duplicate inserts on re-runs.
      Zero-key rows (tripkey=0 OR stopkey=0) are excluded from insert
      to prevent phantom matches on the NOT EXISTS check.
    """
    dow_col = DOW_COLUMNS[datetime.strptime(target_date, '%Y-%m-%d').weekday()]

    return f"""
    INSERT INTO dw.FactStop (
        tripkey,
        routekey,
        stopkey,
        datekey,
        timekey,
        agencykey,
        versionkey,
        scheduledarrivalseconds,
        scheduleddepartureseconds,
        stopsequence,
        actualarrival,
        actualdeparture,
        arrivaldevseconds,
        arrivalsource,
        interpolationconfidence,
        isontime,
        islate,
        isearly,
        isoriginstop,
        isterminalstop,
        ismissed,
        isbunching,
        isestimated,
        isofficial
    )
    WITH

    -- ── Step 1: Resolve active service_ids for target_date ──────────
    -- All date comparisons explicitly cast VARCHAR → DATE to avoid
    -- lexicographic string comparison on the staging columns.
    active_services AS (
        -- Regular schedule
        SELECT c.service_id
        FROM stg.calendar c
        WHERE c.{dow_col} = '1'
          AND CAST(c.start_date AS DATE) <= CAST('{target_date}' AS DATE)
          AND CAST(c.end_date   AS DATE) >= CAST('{target_date}' AS DATE)

        UNION

        -- Exception: service added on this specific date
        SELECT cd.service_id
        FROM stg.calendar_dates cd
        WHERE CAST(cd.date AS DATE) = CAST('{target_date}' AS DATE)
          AND cd.exception_type = '1'

        EXCEPT

        -- Exception: service cancelled on this specific date
        -- Cancellation overrides regular schedule even if calendar marks day active
        SELECT cd.service_id
        FROM stg.calendar_dates cd
        WHERE CAST(cd.date AS DATE) = CAST('{target_date}' AS DATE)
          AND cd.exception_type = '2'
    ),

    -- ── Step 2: All trips running on target_date ────────────────────
    active_trips AS (
        SELECT
            t.trip_id,
            t.route_id,
            t.service_id,
            t.direction_id,
            t.shape_id
        FROM stg.trips t
        INNER JOIN active_services s ON s.service_id = t.service_id
    ),

    -- ── Step 3: Stop visits for active trips ────────────────────────
    -- stop_sequence cast is guarded — non-numeric values are excluded
    -- rather than failing the entire insert.
    -- arrival_time / departure_time converted to seconds from midnight.
    -- Overnight times like '25:15:00' correctly produce seconds > 86400.
    stop_visits AS (
        SELECT
            at.trip_id,
            at.route_id,
            at.service_id,
            at.direction_id,
            at.shape_id,
            st.stop_id,
            CAST(st.stop_sequence AS INTEGER)                          AS stop_sequence,
            -- Seconds from midnight — handles overnight trips correctly
            (
                CAST(SPLIT_PART(st.arrival_time,   ':', 1) AS INTEGER) * 3600 +
                CAST(SPLIT_PART(st.arrival_time,   ':', 2) AS INTEGER) * 60   +
                CAST(SPLIT_PART(st.arrival_time,   ':', 3) AS INTEGER)
            )                                                          AS arr_secs,
            (
                CAST(SPLIT_PART(st.departure_time, ':', 1) AS INTEGER) * 3600 +
                CAST(SPLIT_PART(st.departure_time, ':', 2) AS INTEGER) * 60   +
                CAST(SPLIT_PART(st.departure_time, ':', 3) AS INTEGER)
            )                                                          AS dep_secs,
            -- TimeKey: map arrival into DimTime 0-1439 range via MOD 1440
            (
                (
                    CAST(SPLIT_PART(st.arrival_time, ':', 1) AS INTEGER) * 60 +
                    CAST(SPLIT_PART(st.arrival_time, ':', 2) AS INTEGER)
                ) % 1440
            )                                                          AS time_key_raw
        FROM stg.stop_times st
        INNER JOIN active_trips at ON at.trip_id = st.trip_id
        WHERE st.arrival_time   IS NOT NULL
          AND st.departure_time IS NOT NULL
          AND st.arrival_time   <> ''
          AND st.departure_time <> ''
          -- Guard: exclude rows where stop_sequence is not a plain integer
          AND st.stop_sequence ~ '^[0-9]+$'
    ),

    -- ── Step 4: Flag origin and terminal stops per trip ─────────────
    stop_flags AS (
        SELECT
            sv.*,
            CASE WHEN sv.stop_sequence = MIN(sv.stop_sequence)
                          OVER (PARTITION BY sv.trip_id)
                 THEN TRUE ELSE FALSE END AS is_origin,
            CASE WHEN sv.stop_sequence = MAX(sv.stop_sequence)
                          OVER (PARTITION BY sv.trip_id)
                 THEN TRUE ELSE FALSE END AS is_terminal
        FROM stop_visits sv
    ),

    -- ── Step 5: Resolve surrogate keys ──────────────────────────────
    -- All joins are LEFT JOIN with COALESCE(..., 0) fallback.
    -- Zero-key rows are excluded from the final INSERT (Step 6)
    -- to prevent phantom NOT EXISTS matches on future re-runs.
    resolved AS (
        SELECT
            COALESCE(dt.tripkey,  0)                                   AS tripkey,
            COALESCE(dr.routekey, 0)                                   AS routekey,
            COALESCE(ds.stopkey,  0)                                   AS stopkey,
            CAST(REPLACE('{target_date}', '-', '') AS INTEGER)         AS datekey,
            COALESCE(dtime.timekey, 0)                                 AS timekey,
            COALESCE(da.agencykey, 0)                                  AS agencykey,
            COALESCE(dfv.versionkey, 0)                                AS versionkey,
            sf.arr_secs                                                AS scheduledarrivalseconds,
            sf.dep_secs                                                AS scheduleddepartureseconds,
            sf.stop_sequence                                           AS stopsequence,
            sf.is_origin                                               AS isoriginstop,
            sf.is_terminal                                             AS isterminalstop
        FROM stop_flags sf

        -- DimTrip — natural key: tripid
        LEFT JOIN dw.DimTrip dt
            ON dt.tripid = sf.trip_id

        -- DimRoute — natural key: routeid
        LEFT JOIN dw.DimRoute dr
            ON dr.routeid = sf.route_id

        -- DimStop — natural key: stopid
        LEFT JOIN dw.DimStop ds
            ON ds.stopid = sf.stop_id

        -- DimAgency — via DimRoute.agencykey (already resolved above)
        LEFT JOIN dw.DimAgency da
            ON da.agencykey = dr.agencykey

        -- DimTime — match on computed minute index 0-1439
        LEFT JOIN dw.DimTime dtime
            ON dtime.timekey = sf.time_key_raw

        -- DimFeedVersion — current active feed only
        LEFT JOIN (
            SELECT versionkey
            FROM dw.DimFeedVersion
            WHERE iscurrent = TRUE
            ORDER BY ingestedat DESC
            LIMIT 1
        ) dfv ON TRUE
    )

    -- ── Step 6: Insert skeleton rows ────────────────────────────────
    -- Excludes zero-key rows: unresolved trips/stops belong in a
    -- data quality log, not in the fact table.
    -- NOT EXISTS check uses the natural composite key to prevent
    -- duplicates on re-runs.
    SELECT
        r.tripkey,
        r.routekey,
        r.stopkey,
        r.datekey,
        r.timekey,
        r.agencykey,
        r.versionkey,
        r.scheduledarrivalseconds,
        r.scheduleddepartureseconds,
        r.stopsequence,
        NULL::TIMESTAMP      AS actualarrival,
        NULL::TIMESTAMP      AS actualdeparture,
        NULL::INTEGER        AS arrivaldevseconds,
        'FALLBACK_SCHEDULED' AS arrivalsource,
        NULL::NUMERIC        AS interpolationconfidence,
        NULL::BOOLEAN        AS isontime,
        NULL::BOOLEAN        AS islate,
        NULL::BOOLEAN        AS isearly,
        r.isoriginstop,
        r.isterminalstop,
        FALSE                AS ismissed,
        NULL::BOOLEAN        AS isbunching,
        TRUE                 AS isestimated,
        FALSE                AS isofficial
    FROM resolved r
    WHERE r.tripkey > 0
      AND r.stopkey > 0
      AND NOT EXISTS (
          SELECT 1
          FROM dw.FactStop fs
          WHERE fs.tripkey      = r.tripkey
            AND fs.stopkey      = r.stopkey
            AND fs.stopsequence = r.stopsequence
            AND fs.datekey      = r.datekey
      );
    """


# ============================================================
# Phase 2 — RT Merge Update SQL
# ============================================================
def rt_merge_update_sql(target_date: str, force: bool = False) -> str:
    """
    Updates FactStop rows with actual arrival times and OTP flags
    from stg.rt_stop_time_updates.

    force=False : only updates FALLBACK_SCHEDULED rows
    force=True  : re-evaluates all rows including GTFS_RT_REPORTED

    NOTE: The DATEDIFF expression for arrivaldevseconds is intentionally
    repeated three times (isontime, isearly, islate) because Redshift
    UPDATE SET does not allow referencing a column being set in the same
    statement. Do not attempt to refactor these into a single expression
    referencing arrivaldevseconds — it will produce a column reference error.
    """
    source_filter = "" if force else "AND fs.arrivalsource = 'FALLBACK_SCHEDULED'"

    return f"""
    UPDATE dw.FactStop fs
    SET
        actualarrival     = CAST(stu.arrival_time_local   AS TIMESTAMP),
        actualdeparture   = CAST(stu.departure_time_local AS TIMESTAMP),

        -- Deviation: positive = late, negative = early (seconds)
        arrivaldevseconds = DATEDIFF(
                                second,
                                DATEADD(
                                    second,
                                    fs.scheduledarrivalseconds,
                                    CAST('{target_date}' AS DATE)
                                ),
                                CAST(stu.arrival_time_local AS TIMESTAMP)
                            ),

        arrivalsource     = 'GTFS_RT_REPORTED',

        -- NOTE: DATEDIFF is repeated below because Redshift UPDATE SET
        -- cannot reference arrivaldevseconds (being set above) in the
        -- same statement. This is intentional — do not refactor.
        isontime          = CASE
                                WHEN DATEDIFF(
                                         second,
                                         DATEADD(second, fs.scheduledarrivalseconds,
                                                 CAST('{target_date}' AS DATE)),
                                         CAST(stu.arrival_time_local AS TIMESTAMP)
                                     ) BETWEEN {OTP_EARLY_THRESHOLD} AND {OTP_LATE_THRESHOLD}
                                THEN TRUE ELSE FALSE
                            END,
        isearly           = CASE
                                WHEN DATEDIFF(
                                         second,
                                         DATEADD(second, fs.scheduledarrivalseconds,
                                                 CAST('{target_date}' AS DATE)),
                                         CAST(stu.arrival_time_local AS TIMESTAMP)
                                     ) < {OTP_EARLY_THRESHOLD}
                                THEN TRUE ELSE FALSE
                            END,
        islate            = CASE
                                WHEN DATEDIFF(
                                         second,
                                         DATEADD(second, fs.scheduledarrivalseconds,
                                                 CAST('{target_date}' AS DATE)),
                                         CAST(stu.arrival_time_local AS TIMESTAMP)
                                     ) > {OTP_LATE_THRESHOLD}
                                THEN TRUE ELSE FALSE
                            END,

        isestimated       = FALSE

    FROM stg.rt_stop_time_updates stu
    JOIN dw.DimTrip dt
        ON dt.tripid = stu.trip_id
    JOIN dw.DimStop ds
        ON ds.stopid = stu.stop_id
    WHERE fs.tripkey      = dt.tripkey
      AND fs.stopkey      = ds.stopkey
      AND fs.stopsequence = CAST(stu.stop_sequence AS INTEGER)
      AND fs.datekey      = CAST(REPLACE('{target_date}', '-', '') AS INTEGER)
      AND stu.arrival_time_local IS NOT NULL
      AND stu.service_date = '{target_date}'
      {source_filter};
    """


def isofficial_update_sql(target_date: str) -> str:
    """
    Recomputes IsOfficial for ALL rows on target_date — both
    GTFS_RT_REPORTED and FALLBACK_SCHEDULED — to ensure consistent
    state regardless of insert default values.

    IsOfficial = TRUE when all four conditions are met:
      - IsOnTime IS NOT NULL  (RT data was applied, OTP was evaluated)
      - IsMissed = FALSE      (stop was served)
      - IsOriginStop = FALSE  (no valid deviation at origin)
      - IsTerminalStop = FALSE
    """
    return f"""
    UPDATE dw.FactStop
    SET isofficial = CASE
        WHEN isontime       IS NOT NULL
         AND ismissed       = FALSE
         AND isoriginstop   = FALSE
         AND isterminalstop = FALSE
        THEN TRUE
        ELSE FALSE
    END
    WHERE datekey = CAST(REPLACE('{target_date}', '-', '') AS INTEGER);
    """


def validation_sql(target_date: str) -> str:
    return f"""
    SELECT
        arrivalsource,
        isofficial,
        isontime,
        COUNT(*) AS row_count
    FROM dw.FactStop
    WHERE datekey = CAST(REPLACE('{target_date}', '-', '') AS INTEGER)
    GROUP BY arrivalsource, isofficial, isontime
    ORDER BY arrivalsource, isofficial, isontime;
    """


# ============================================================
# Per-date processing
# ============================================================
def process_date(target_date: str, phase: str, force: bool, timeout: int):
    print(f"\n{'='*60}")
    print(f"  DATE: {target_date}  |  phase={phase}  |  force={force}")
    print(f"{'='*60}")

    if phase in ('skeleton', 'both'):
        run_sql(
            skeleton_insert_sql(target_date),
            description=f"[{target_date}] Phase 1 — Skeleton insert",
            timeout=timeout
        )

    if phase in ('merge', 'both'):
        run_sql(
            rt_merge_update_sql(target_date, force=force),
            description=f"[{target_date}] Phase 2 — RT merge update",
            timeout=timeout
        )
        run_sql(
            isofficial_update_sql(target_date),
            description=f"[{target_date}] Phase 2 — IsOfficial recompute",
            timeout=timeout
        )

    # Validation — fetch and print result rows
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
    phase   = config['phase']
    force   = config['force']
    timeout = config['timeout']

    print(f"\n{'='*60}")
    print(f"FACTSTOP SKELETON + MERGE JOB  v2")
    print(f"  dates   : {dates[0]} → {dates[-1]}  ({len(dates)} day(s))")
    print(f"  phase   : {phase}")
    print(f"  force   : {force}")
    print(f"  timeout : {timeout}s per SQL statement")
    print(f"{'='*60}")

    # Pre-flight check before any skeleton inserts
    if phase in ('skeleton', 'both'):
        preflight_check()

    results = {'success': [], 'failed': []}

    for target_date in dates:
        try:
            process_date(target_date, phase, force, timeout)
            results['success'].append(target_date)
        except Exception as e:
            print(f"\n  ✗ FAILED [{target_date}]: {e}")
            results['failed'].append((target_date, str(e)))
            # Continue to next date — don't abort entire backfill on one failure
            continue

    # ── Final summary ─────────────────────────────────────────
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
            f"See logs above for details."
        )
    print(f"{'='*60}\n")


if __name__ == '__main__':
    main()
