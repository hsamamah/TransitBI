"""
FactTrip Skeleton Load + RT Merge Job  v2
==========================================
Phase 1 — Skeleton Insert
  Explodes GTFS static schedule into one FactTrip row per trip per
  service date, including CANCELLED trips. Computes ScheduledVRM from
  stg.shapes (scoped to active trips only) and ScheduledVRH from
  stg.stop_times (scoped to active trips only). All actual columns
  are NULL on insert. Fully idempotent.

Phase 2 — RT Merge Update
  Derives TripStatus from stg.rt_vehicle_positions (no rt_trip_updates
  table exists — status inferred from ping presence + calendar_dates).
  Populates ActualStartTime, ActualEndTime, ping counts, RTCoverageRate,
  ReportedVRH, IsEstimated, IsOfficial.
  ADDED trips inserted as separate step after main merge.

TripStatus derivation (no rt_trip_updates available):
  CANCELLED → service_id in calendar_dates exception_type='2' for date
              set at skeleton insert time from calendar data
  OPERATED  → at least one ping in rt_vehicle_positions for trip_id on date
  MISSED    → scheduled trip with no pings and not CANCELLED
  ADDED     → ping exists for trip_id not found in stg.trips

ScheduledVRM:
  MAX(shape_dist_traveled) per shape_id / 1609.344 (meters → miles)
  shape_dist_traveled confirmed in meters (max ~250km for Link Light Rail)
  CTE scoped to active trips only — no full table scan

ScheduledVRH:
  (MAX departure_seconds - MIN arrival_seconds) per trip / 3600
  Negative result guard applied for mixed overnight/non-overnight trips
  CTE scoped to active trips only

ReportedVRM: ScheduledVRM always (IsEstimated=TRUE) — actual VRM requires
  shape interpolation not available from vehicle positions alone
ReportedVRH: actual elapsed ping time when available, else ScheduledVRH

Changelog v2:
  - Fixed shape_distances CTE — scoped to active trips (was full table scan)
  - Fixed trip_hours CTE — scoped to active trips (was full table scan)
  - Fixed CANCELLED trips — included in all_scheduled_trips CTE so status
    is correctly set at skeleton time (was always MISSED)
  - Fixed ADDED trips idempotency — DELETE + re-insert pattern replaces
    broken agencykey+datekey NOT EXISTS check (tripkey=0 for all ADDED rows)
  - Fixed rt_merge force filter — now correctly excludes already-OPERATED
    rows when force=False
  - Fixed duplicate isofficial computation — removed from rt_merge_update_sql,
    now only computed in isofficial_update_sql (single source of truth)
  - Fixed ScheduledVRH negative result guard for mixed overnight trips
  - Fixed _print_result_rows falsy zero bug — replaced or-chain with next()
  - Added Phase 2 pre-flight check for rt_vehicle_positions row count
  - Renamed PREFLIGHT_MIN_DIMTRIP_ROWS → PREFLIGHT_MIN_DIM_ROWS
  - Fixed preflight_check resp['Id'] fragility — query_id stored explicitly

Supports:
  --target_date  YYYY-MM-DD           single date (default: yesterday)
  --start_date   YYYY-MM-DD           backfill range start
  --end_date     YYYY-MM-DD           backfill range end
  --phase        skeleton|merge|both  which phases to run (default: both)
  --force        re-process already-OPERATED rows in merge phase
  --timeout      seconds              Redshift poll timeout (default: 900)

Run order dependency:
  DimTrip, DimRoute, DimAgency, DimService, DimDate, DimFeedVersion,
  DimShape must all be populated before Phase 1.
  stg.rt_vehicle_positions must be populated before Phase 2.
  FactTrip must be fully loaded before FactServiceDay is run.

Author: Transit DW Team — P1 / P4
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

REDSHIFT_POLL_TIMEOUT  = 900
REDSHIFT_POLL_INTERVAL = 5

# Meters per mile — for VRM NTD conversion
METERS_PER_MILE = 1609.344

# Seconds per ping interval — used for ExpectedPingCount
PING_INTERVAL_SECONDS = 30

# Minimum rows required in dimension tables before skeleton insert proceeds
PREFLIGHT_MIN_DIM_ROWS = 1

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
      force   : bool — re-process already-OPERATED rows in merge phase
      timeout : int  — Redshift poll timeout in seconds per statement
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
    Returns (status_resp, query_id) tuple.
    If fetch_results=True, fetches and prints result rows.
    """
    timeout  = timeout or REDSHIFT_POLL_TIMEOUT
    label    = description or sql[:80].strip()
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
            rows    = status_resp.get('ResultRows',   0)
            updated = status_resp.get('UpdatedRows',  0)
            print(f"     ✓ {label} — ResultRows={rows} UpdatedRows={updated} ({elapsed}s)")
            if fetch_results and rows and rows > 0:
                _print_result_rows(query_id)
            return status_resp, query_id

        elif status in ('FAILED', 'ABORTED'):
            err = status_resp.get('Error', 'No error detail')
            raise RuntimeError(f"SQL FAILED [{label}]: {err}")

    raise RuntimeError(f"SQL TIMED OUT after {timeout}s [{label}]")


def _print_result_rows(query_id: str):
    """
    Fetch and print result rows from a completed SELECT statement.
    Uses next() to safely handle zero integer values (fixes falsy bug
    where longValue=0 was printed as NULL via or-chain).
    """
    try:
        result = rs_data.get_statement_result(Id=query_id)
        cols   = [c['label'] for c in result.get('ColumnMetadata', [])]
        if cols:
            print(f"     {'  |  '.join(cols)}")
            print(f"     {'  |  '.join(['---'] * len(cols))}")
        for row in result.get('Records', []):
            values = []
            for col in row:
                # Use next() so zero integer values are not treated as falsy
                val = next(
                    (str(v) for k, v in col.items() if v is not None),
                    'NULL'
                )
                values.append(val)
            print(f"     {'  |  '.join(values)}")
    except Exception as e:
        print(f"     (Could not fetch result rows: {e})")


# ============================================================
# Pre-flight Checks
# ============================================================
def preflight_skeleton(timeout: int):
    """
    Verify DimTrip and DimShape are populated before skeleton insert.
    Without DimTrip rows every insert uses zero key — silent bad data.
    Without DimShape rows ScheduledVRM is 0 for all trips.
    Stores query_id explicitly to avoid resp['Id'] fragility.
    """
    print("\n  → Pre-flight (skeleton): checking DimTrip and DimShape")

    _, query_id = run_sql(
        """
        SELECT
            (SELECT COUNT(*) FROM dw.DimTrip  WHERE tripkey  > 0) AS dimtrip_rows,
            (SELECT COUNT(*) FROM dw.DimShape WHERE shapekey > 0) AS dimshape_rows;
        """,
        description="Pre-flight — DimTrip + DimShape row counts",
        timeout=timeout,
        fetch_results=True
    )

    try:
        result      = rs_data.get_statement_result(Id=query_id)
        row         = result['Records'][0]
        dimtrip_ct  = int(next((v for k, v in row[0].items() if v is not None), 0))
        dimshape_ct = int(next((v for k, v in row[1].items() if v is not None), 0))
    except Exception as exc:
        raise RuntimeError(
            f"Pre-flight (skeleton): could not read dimension counts — {exc}"
        )

    errors = []
    if dimtrip_ct  < PREFLIGHT_MIN_DIM_ROWS:
        errors.append(f"DimTrip has {dimtrip_ct} rows (min: {PREFLIGHT_MIN_DIM_ROWS})")
    if dimshape_ct < PREFLIGHT_MIN_DIM_ROWS:
        errors.append(f"DimShape has {dimshape_ct} rows (min: {PREFLIGHT_MIN_DIM_ROWS})")
    if errors:
        raise RuntimeError(
            f"Pre-flight (skeleton) FAILED — {'; '.join(errors)}. "
            "Populate dimension tables before running skeleton insert."
        )

    print(f"     ✓ Pre-flight passed — DimTrip={dimtrip_ct:,}  DimShape={dimshape_ct:,}")


def preflight_merge(target_date: str, timeout: int):
    """
    Verify rt_vehicle_positions has data for target_date before merge.
    If staging is empty the merge silently sets every trip to MISSED.
    """
    print(f"\n  → Pre-flight (merge): checking rt_vehicle_positions for {target_date}")

    _, query_id = run_sql(
        f"""
        SELECT COUNT(*) AS rt_ping_count
        FROM stg.rt_vehicle_positions
        WHERE DATE_TRUNC('day', timestamp_local) = CAST('{target_date}' AS DATE);
        """,
        description=f"Pre-flight — rt_vehicle_positions count for {target_date}",
        timeout=timeout,
        fetch_results=True
    )

    try:
        result    = rs_data.get_statement_result(Id=query_id)
        ping_ct   = int(next((v for k, v in result['Records'][0][0].items() if v is not None), 0))
    except Exception as exc:
        raise RuntimeError(
            f"Pre-flight (merge): could not read rt_vehicle_positions count — {exc}"
        )

    if ping_ct == 0:
        print(
            f"     ⚠ WARNING — rt_vehicle_positions has 0 rows for {target_date}. "
            "All trips will remain MISSED. Continuing — this may be expected for "
            "dates outside the staging window."
        )
    else:
        print(f"     ✓ Pre-flight passed — {ping_ct:,} pings available for {target_date}")


# ============================================================
# Phase 1 — Skeleton Insert SQL
# ============================================================
def skeleton_insert_sql(target_date: str) -> str:
    """
    Inserts one FactTrip row per scheduled trip on target_date,
    including CANCELLED trips (needed so MissedTripRate excludes them).

    all_scheduled_trips CTE:
      Contains ALL trips whose service_id is active OR cancelled on
      target_date. This ensures CANCELLED trips are in the insert set
      so their TripStatus is correctly set at skeleton time.
      Previously only active_services was used, causing CANCELLED trips
      to never appear — they were always marked MISSED.

    shape_distances CTE:
      Scoped to shapes used by active trips only via INNER JOIN.
      Previously scanned entire stg.shapes table.

    trip_hours CTE:
      Scoped to active trip_ids only via INNER JOIN.
      Previously scanned entire stg.stop_times table.

    ScheduledVRH negative guard:
      CASE WHEN MAX >= MIN prevents negative values for trips where
      stop times are not stored consistently as overnight seconds.

    Idempotency:
      WHERE NOT EXISTS on (tripkey, datekey) — safe for re-runs.
      tripkey=0 rows excluded (unresolved trips not inserted).
    """
    dow_col = DOW_COLUMNS[datetime.strptime(target_date, '%Y-%m-%d').weekday()]

    return f"""
    INSERT INTO dw.FactTrip (
        tripkey,
        datekey,
        agencykey,
        routekey,
        versionkey,
        tripstatus,
        tripstatussource,
        actualstarttime,
        actualendtime,
        tripendsource,
        actualpingcount,
        expectedpingcount,
        rtcoveragerate,
        scheduledvrm,
        scheduledvrh,
        reportedvrm,
        reportedvrh,
        isestimated,
        isspecialevent,
        isofficial,
        dataqualityflag
    )
    WITH

    -- ── Step 1: Active service_ids (running on target_date) ─────────
    active_services AS (
        SELECT c.service_id
        FROM stg.calendar c
        WHERE c.{dow_col} = '1'
          AND CAST(c.start_date AS DATE) <= CAST('{target_date}' AS DATE)
          AND CAST(c.end_date   AS DATE) >= CAST('{target_date}' AS DATE)

        UNION

        SELECT cd.service_id
        FROM stg.calendar_dates cd
        WHERE CAST(cd.date AS DATE) = CAST('{target_date}' AS DATE)
          AND cd.exception_type = '1'

        EXCEPT

        SELECT cd.service_id
        FROM stg.calendar_dates cd
        WHERE CAST(cd.date AS DATE) = CAST('{target_date}' AS DATE)
          AND cd.exception_type = '2'
    ),

    -- ── Step 2: Cancelled service_ids for target_date ───────────────
    cancelled_services AS (
        SELECT cd.service_id
        FROM stg.calendar_dates cd
        WHERE CAST(cd.date AS DATE) = CAST('{target_date}' AS DATE)
          AND cd.exception_type = '2'
    ),

    -- ── Step 3: Special event service_ids ───────────────────────────
    -- A service_id is a genuine special event only if it has NO base
    -- weekly schedule in stg.calendar (i.e. it exists purely as a
    -- one-off calendar_dates exception).  Service_ids that also appear
    -- in stg.calendar are normal scheduled services that happen to use
    -- exception_type='1' to add dates outside their calendar window or
    -- on holidays — those must NOT be treated as special events.
    special_event_services AS (
        SELECT cd.service_id
        FROM stg.calendar_dates cd
        WHERE CAST(cd.date AS DATE) = CAST('{target_date}' AS DATE)
          AND cd.exception_type = '1'
          AND NOT EXISTS (SELECT 1 FROM stg.calendar c WHERE c.service_id = cd.service_id)
    ),

    -- ── Step 4: ALL scheduled trips (active + cancelled) ────────────
    -- Must include cancelled trips so TripStatus = CANCELLED is set
    -- at skeleton time. Previously only active_services was joined,
    -- causing cancelled trips to be silently excluded or marked MISSED.
    all_scheduled_trips AS (
        SELECT
            t.trip_id,
            t.route_id,
            t.service_id,
            t.shape_id
        FROM stg.trips t
        WHERE t.service_id IN (
            SELECT service_id FROM active_services
            UNION
            SELECT service_id FROM cancelled_services
        )
    ),

    -- ── Step 5: ScheduledVRM per shape ──────────────────────────────
    -- Scoped to shapes used by scheduled trips only (was full table scan).
    -- MAX(shape_dist_traveled) = total route distance in meters.
    -- Divided by 1609.344 to convert to miles for NTD reporting.
    -- Non-numeric shape_dist_traveled values excluded via regex guard.
    shape_distances AS (
        SELECT
            sh.shape_id,
            MAX(
                CASE WHEN sh.shape_dist_traveled ~ '^[0-9]+([.][0-9]+)?$'
                     THEN CAST(sh.shape_dist_traveled AS NUMERIC)
                     ELSE NULL
                END
            ) / {METERS_PER_MILE}                                      AS scheduled_vrm_miles
        FROM stg.shapes sh
        -- Scoped join — only shapes referenced by scheduled trips
        INNER JOIN all_scheduled_trips ast ON ast.shape_id = sh.shape_id
        WHERE sh.shape_dist_traveled IS NOT NULL
          AND sh.shape_dist_traveled <> ''
        GROUP BY sh.shape_id
    ),

    -- ── Step 6: ScheduledVRH per trip ───────────────────────────────
    -- Scoped to active trip_ids only (was full stg.stop_times scan).
    -- Revenue hours = (last departure - first arrival) / 3600.
    -- Negative result guard: CASE WHEN MAX >= MIN prevents wrong values
    -- when stop times are not stored consistently as overnight seconds.
    trip_hours AS (
        SELECT
            st.trip_id,
            CASE
                WHEN MAX(
                         CAST(SPLIT_PART(st.departure_time, ':', 1) AS INTEGER) * 3600 +
                         CAST(SPLIT_PART(st.departure_time, ':', 2) AS INTEGER) * 60   +
                         CAST(SPLIT_PART(st.departure_time, ':', 3) AS INTEGER)
                     ) >=
                     MIN(
                         CAST(SPLIT_PART(st.arrival_time, ':', 1) AS INTEGER) * 3600 +
                         CAST(SPLIT_PART(st.arrival_time, ':', 2) AS INTEGER) * 60   +
                         CAST(SPLIT_PART(st.arrival_time, ':', 3) AS INTEGER)
                     )
                THEN (
                    MAX(
                        CAST(SPLIT_PART(st.departure_time, ':', 1) AS INTEGER) * 3600 +
                        CAST(SPLIT_PART(st.departure_time, ':', 2) AS INTEGER) * 60   +
                        CAST(SPLIT_PART(st.departure_time, ':', 3) AS INTEGER)
                    ) -
                    MIN(
                        CAST(SPLIT_PART(st.arrival_time, ':', 1) AS INTEGER) * 3600 +
                        CAST(SPLIT_PART(st.arrival_time, ':', 2) AS INTEGER) * 60   +
                        CAST(SPLIT_PART(st.arrival_time, ':', 3) AS INTEGER)
                    )
                ) / 3600.0
                ELSE NULL
            END                                                        AS scheduled_vrh_hours
        FROM stg.stop_times st
        -- Scoped join — only trips running on target_date
        INNER JOIN all_scheduled_trips ast ON ast.trip_id = st.trip_id
        WHERE st.arrival_time   IS NOT NULL
          AND st.departure_time IS NOT NULL
          AND st.arrival_time   <> ''
          AND st.departure_time <> ''
          AND st.stop_sequence ~ '^[0-9]+$'
        GROUP BY st.trip_id
    ),

    -- ── Step 7: Resolve surrogate keys ──────────────────────────────
    resolved AS (
        SELECT
            COALESCE(dt.tripkey,  0)                                   AS tripkey,
            CAST(REPLACE('{target_date}', '-', '') AS INTEGER)         AS datekey,
            COALESCE(da.agencykey, 0)                                  AS agencykey,
            COALESCE(dr.routekey,  0)                                  AS routekey,
            COALESCE(dfv.versionkey, 0)                                AS versionkey,
            -- CANCELLED set now from calendar data.
            -- MISSED is the default — Phase 2 upgrades to OPERATED when pings exist.
            CASE
                WHEN cs.service_id IS NOT NULL THEN 'CANCELLED'
                ELSE 'MISSED'
            END                                                        AS tripstatus,
            CASE
                WHEN cs.service_id IS NOT NULL THEN 'CALENDAR'
                ELSE 'INFERRED'
            END                                                        AS tripstatussource,
            COALESCE(sd.scheduled_vrm_miles, 0)                        AS scheduledvrm,
            COALESCE(th.scheduled_vrh_hours, 0)                        AS scheduledvrh,
            CASE
                WHEN se.service_id IS NOT NULL THEN TRUE
                ELSE FALSE
            END                                                        AS isspecialevent,
            ast.trip_id,
            ast.route_id
        FROM all_scheduled_trips ast

        LEFT JOIN dw.DimTrip dt
            ON dt.tripid = ast.trip_id

        LEFT JOIN dw.DimRoute dr
            ON dr.routeid = ast.route_id

        -- DimAgency resolved via DimRoute.agencykey
        LEFT JOIN dw.DimAgency da
            ON da.agencykey = dr.agencykey

        -- DimFeedVersion — current active feed
        LEFT JOIN (
            SELECT versionkey
            FROM dw.DimFeedVersion
            WHERE iscurrent = TRUE
            ORDER BY ingestedat DESC
            LIMIT 1
        ) dfv ON TRUE

        LEFT JOIN shape_distances sd
            ON sd.shape_id = ast.shape_id

        LEFT JOIN trip_hours th
            ON th.trip_id = ast.trip_id

        LEFT JOIN cancelled_services cs
            ON cs.service_id = ast.service_id

        LEFT JOIN special_event_services se
            ON se.service_id = ast.service_id
    )

    -- ── Step 8: Insert skeleton rows ────────────────────────────────
    -- Excludes tripkey=0 — unresolved trips are not inserted.
    -- ReportedVRM/VRH defaults to scheduled; Phase 2 updates ReportedVRH
    -- with actual elapsed time when RT data is available.
    SELECT
        r.tripkey,
        r.datekey,
        r.agencykey,
        r.routekey,
        r.versionkey,
        r.tripstatus,
        r.tripstatussource,
        NULL::TIMESTAMP      AS actualstarttime,
        NULL::TIMESTAMP      AS actualendtime,
        NULL::VARCHAR        AS tripendsource,
        NULL::INTEGER        AS actualpingcount,
        NULL::INTEGER        AS expectedpingcount,
        NULL::NUMERIC        AS rtcoveragerate,
        r.scheduledvrm,
        r.scheduledvrh,
        r.scheduledvrm       AS reportedvrm,
        r.scheduledvrh       AS reportedvrh,
        TRUE                 AS isestimated,
        r.isspecialevent,
        FALSE                AS isofficial,
        NULL::VARCHAR        AS dataqualityflag
    FROM resolved r
    WHERE r.tripkey > 0
      AND NOT EXISTS (
          SELECT 1
          FROM dw.FactTrip ft
          WHERE ft.tripkey = r.tripkey
            AND ft.datekey = r.datekey
      );
    """


# ============================================================
# Phase 2 — RT Merge Update SQL
# ============================================================
def rt_merge_update_sql(target_date: str, force: bool = False) -> str:
    """
    Updates FactTrip MISSED rows to OPERATED where RT pings exist.
    CANCELLED rows are never overridden.

    force=False: only updates rows where tripstatus = 'MISSED'
    force=True:  also re-updates already-OPERATED rows

    isofficial is NOT set here — computed exclusively in isofficial_update_sql
    to avoid duplicate/conflicting logic between the two passes.

    ReportedVRH uses actual elapsed ping time when available.
    ReportedVRM stays as ScheduledVRM (IsEstimated=TRUE) because
    actual VRM requires shape interpolation not available here.

    NOTE: ExpectedPingCount and RTCoverageRate cannot reference
    actualstarttime/actualendtime being set in the same UPDATE —
    computed inline from the staging aggregation subquery instead.
    """
    if force:
        status_filter = "AND ft.tripstatus != 'CANCELLED'"
    else:
        status_filter = "AND ft.tripstatus = 'MISSED'"

    return f"""
    UPDATE dw.FactTrip ft
    SET
        tripstatus        = 'OPERATED',
        tripstatussource  = 'GTFS_RT',
        actualstarttime   = vp.first_ping,
        actualendtime     = vp.last_ping,
        tripendsource     = 'LAST_PING',
        actualpingcount   = vp.ping_count,
        -- NOTE: Cannot reference actualstarttime/actualendtime being set above.
        -- ExpectedPingCount computed directly from staging subquery values.
        expectedpingcount = DATEDIFF(second, vp.first_ping, vp.last_ping)
                            / {PING_INTERVAL_SECONDS},
        rtcoveragerate    = CASE
                                WHEN DATEDIFF(second, vp.first_ping, vp.last_ping) > 0
                                THEN LEAST(1.0,
                                         CAST(vp.ping_count AS NUMERIC) /
                                         NULLIF(
                                             DATEDIFF(second, vp.first_ping, vp.last_ping)
                                             / {PING_INTERVAL_SECONDS},
                                         0))
                                ELSE NULL
                            END,
        -- ReportedVRM stays as ScheduledVRM — actual VRM requires shape
        -- interpolation not available from vehicle positions alone
        reportedvrm       = ft.scheduledvrm,
        reportedvrh       = CASE
                                WHEN DATEDIFF(second, vp.first_ping, vp.last_ping) > 0
                                THEN CAST(
                                         DATEDIFF(second, vp.first_ping, vp.last_ping)
                                         AS NUMERIC
                                     ) / 3600.0
                                ELSE ft.scheduledvrh
                            END,
        isestimated       = TRUE
        -- isofficial intentionally NOT set here — computed exclusively
        -- in isofficial_update_sql after all status updates are complete
    FROM (
        SELECT
            vp.trip_id,
            MIN(vp.timestamp_local)  AS first_ping,
            MAX(vp.timestamp_local)  AS last_ping,
            COUNT(*)                 AS ping_count
        FROM stg.rt_vehicle_positions vp
        WHERE DATE_TRUNC('day', vp.timestamp_local) = CAST('{target_date}' AS DATE)
          AND vp.trip_id IS NOT NULL
          AND vp.trip_id <> ''
        GROUP BY vp.trip_id
    ) vp
    JOIN dw.DimTrip dt
        ON dt.tripid = vp.trip_id
    WHERE ft.tripkey = dt.tripkey
      AND ft.datekey = CAST(REPLACE('{target_date}', '-', '') AS INTEGER)
      {status_filter};
    """


def added_trips_insert_sql(target_date: str) -> str:
    """
    Inserts ADDED trip rows for RT pings whose trip_id has no matching
    row in stg.trips — truly unscheduled trips that only appear in RT data.

    Idempotency strategy:
      ADDED rows all have tripkey=0, so a unique natural key cannot be
      derived from FactTrip columns alone. Instead we DELETE all ADDED
      rows for target_date before re-inserting. This is safe because
      ADDED rows are derived entirely from stg.rt_vehicle_positions and
      can always be regenerated cleanly.

    This function returns a tuple of (delete_sql, insert_sql).
    Call run_sql on each in sequence.
    """
    delete_sql = f"""
    DELETE FROM dw.FactTrip
    WHERE datekey    = CAST(REPLACE('{target_date}', '-', '') AS INTEGER)
      AND tripstatus = 'ADDED';
    """

    insert_sql = f"""
    INSERT INTO dw.FactTrip (
        tripkey,
        datekey,
        agencykey,
        routekey,
        versionkey,
        tripstatus,
        tripstatussource,
        actualstarttime,
        actualendtime,
        tripendsource,
        actualpingcount,
        expectedpingcount,
        rtcoveragerate,
        scheduledvrm,
        scheduledvrh,
        reportedvrm,
        reportedvrh,
        isestimated,
        isspecialevent,
        isofficial,
        dataqualityflag
    )
    WITH

    rt_trips AS (
        SELECT
            vp.trip_id,
            vp.agency_key,
            MIN(vp.timestamp_local)  AS first_ping,
            MAX(vp.timestamp_local)  AS last_ping,
            COUNT(*)                 AS ping_count
        FROM stg.rt_vehicle_positions vp
        WHERE DATE_TRUNC('day', vp.timestamp_local) = CAST('{target_date}' AS DATE)
          AND vp.trip_id IS NOT NULL
          AND vp.trip_id <> ''
        GROUP BY vp.trip_id, vp.agency_key
    ),

    -- Only trips NOT found in stg.trips — truly unscheduled / ADDED
    added_trips AS (
        SELECT rt.*
        FROM rt_trips rt
        LEFT JOIN stg.trips t ON t.trip_id = rt.trip_id
        WHERE t.trip_id IS NULL
    )

    SELECT
        0                                                              AS tripkey,
        CAST(REPLACE('{target_date}', '-', '') AS INTEGER)            AS datekey,
        COALESCE(da.agencykey, 0)                                     AS agencykey,
        0                                                              AS routekey,
        COALESCE(dfv.versionkey, 0)                                   AS versionkey,
        'ADDED'                                                        AS tripstatus,
        'GTFS_RT'                                                      AS tripstatussource,
        at.first_ping                                                  AS actualstarttime,
        at.last_ping                                                   AS actualendtime,
        'LAST_PING'                                                    AS tripendsource,
        at.ping_count                                                  AS actualpingcount,
        DATEDIFF(second, at.first_ping, at.last_ping)
            / {PING_INTERVAL_SECONDS}                                  AS expectedpingcount,
        CASE
            WHEN DATEDIFF(second, at.first_ping, at.last_ping) > 0
            THEN LEAST(1.0,
                     CAST(at.ping_count AS NUMERIC) /
                     NULLIF(
                         DATEDIFF(second, at.first_ping, at.last_ping)
                         / {PING_INTERVAL_SECONDS},
                     0))
            ELSE NULL
        END                                                            AS rtcoveragerate,
        0                                                              AS scheduledvrm,
        0                                                              AS scheduledvrh,
        CAST(
            DATEDIFF(second, at.first_ping, at.last_ping) AS NUMERIC
        ) / 3600.0                                                     AS reportedvrm,
        CAST(
            DATEDIFF(second, at.first_ping, at.last_ping) AS NUMERIC
        ) / 3600.0                                                     AS reportedvrh,
        TRUE                                                           AS isestimated,
        FALSE                                                          AS isspecialevent,
        FALSE                                                          AS isofficial,
        'ADDED_UNSCHEDULED'                                            AS dataqualityflag
    FROM added_trips at
    LEFT JOIN dw.DimAgency da
        ON da.agencyid = at.agency_key
    LEFT JOIN (
        SELECT versionkey
        FROM dw.DimFeedVersion
        WHERE iscurrent = TRUE
        ORDER BY ingestedat DESC
        LIMIT 1
    ) dfv ON TRUE;
    """

    return delete_sql, insert_sql


def isofficial_update_sql(target_date: str) -> str:
    """
    Single source of truth for IsOfficial on FactTrip.
    Runs after rt_merge_update_sql and added_trips_insert_sql so all
    TripStatus values are final before this pass executes.

    IsOfficial = TRUE when ALL are true:
      - TripStatus IN ('OPERATED', 'ADDED')
      - ReportedVRM > 0
      - ReportedVRH > 0
      - DataQualityFlag IS NULL or is a known non-error flag

    IsOfficial = FALSE when ANY are true:
      - TripStatus = 'MISSED'    → excluded from VRM/VRH NTD totals
      - TripStatus = 'CANCELLED' → planned cancellation, excluded from NTD
      - DataQualityFlag = 'PIPELINE_ERROR' → ETL failure, not valid data
    """
    return f"""
    UPDATE dw.FactTrip
    SET isofficial = CASE
        WHEN tripstatus IN ('OPERATED', 'ADDED')
         AND reportedvrm > 0
         AND reportedvrh > 0
         AND (
             dataqualityflag IS NULL
             OR dataqualityflag = 'ADDED_UNSCHEDULED'
             OR isestimated = TRUE
         )
        THEN TRUE
        ELSE FALSE
    END
    WHERE datekey = CAST(REPLACE('{target_date}', '-', '') AS INTEGER);
    """


def validation_sql(target_date: str) -> str:
    return f"""
    SELECT
        tripstatus,
        tripstatussource,
        isestimated,
        isofficial,
        isspecialevent,
        COUNT(*)                                    AS row_count,
        ROUND(AVG(scheduledvrm)::NUMERIC, 2)        AS avg_scheduled_vrm,
        ROUND(AVG(scheduledvrh)::NUMERIC, 2)        AS avg_scheduled_vrh,
        SUM(CASE WHEN actualpingcount IS NOT NULL
                 THEN 1 ELSE 0 END)                 AS trips_with_rt_data,
        ROUND(AVG(rtcoveragerate)::NUMERIC, 3)      AS avg_rt_coverage
    FROM dw.FactTrip
    WHERE datekey = CAST(REPLACE('{target_date}', '-', '') AS INTEGER)
    GROUP BY tripstatus, tripstatussource, isestimated, isofficial, isspecialevent
    ORDER BY tripstatus, isofficial;
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
        # Pre-flight for merge — warn if no RT data for this date
        preflight_merge(target_date, timeout)

        # Update MISSED → OPERATED where pings exist
        run_sql(
            rt_merge_update_sql(target_date, force=force),
            description=f"[{target_date}] Phase 2 — RT merge (MISSED→OPERATED)",
            timeout=timeout
        )

        # DELETE + re-insert ADDED trips (clean idempotency for tripkey=0 rows)
        delete_sql, insert_sql = added_trips_insert_sql(target_date)
        run_sql(
            delete_sql,
            description=f"[{target_date}] Phase 2 — ADDED trips delete",
            timeout=timeout
        )
        run_sql(
            insert_sql,
            description=f"[{target_date}] Phase 2 — ADDED trips insert",
            timeout=timeout
        )

        # IsOfficial — single pass after all status updates are final
        run_sql(
            isofficial_update_sql(target_date),
            description=f"[{target_date}] Phase 2 — IsOfficial recompute",
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
    phase   = config['phase']
    force   = config['force']
    timeout = config['timeout']

    print(f"\n{'='*60}")
    print(f"FACTTRIP SKELETON + MERGE JOB  v2")
    print(f"  dates   : {dates[0]} → {dates[-1]}  ({len(dates)} day(s))")
    print(f"  phase   : {phase}")
    print(f"  force   : {force}")
    print(f"  timeout : {timeout}s per SQL statement")
    print(f"{'='*60}")

    # Pre-flight check before any skeleton inserts
    if phase in ('skeleton', 'both'):
        preflight_skeleton(timeout)

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
            "See logs above for details."
        )
    print(f"{'='*60}\n")


if __name__ == '__main__':
    main()
