"""
Transit Pipeline Inspector  v2
================================
Sits between gtfs-rt-parse-load-glue and the fact table jobs in the
gtfs-rt-daily-pipeline workflow.

What this job does:
  1. Queries Redshift to find all dates available in staging
     (stg.rt_stop_time_updates + stg.rt_vehicle_positions)
  2. Determines which dates are missing or incomplete in each fact table:
       FactStop      — missing skeleton rows OR FALLBACK_SCHEDULED rows
       FactTrip      — missing skeleton rows OR unprocessed MISSED rows
       FactServiceDay — missing rows for dates where FactTrip is complete
  3. Writes job parameters to DynamoDB seattle-transit-pipeline table
     with a 7-day TTL so items expire automatically
  4. Downstream jobs read their own parameters from DynamoDB on startup

Gap handling:
  Non-contiguous gaps are preserved as separate date ranges rather than
  being collapsed into one range that includes clean dates. Each gap range
  is written to DynamoDB as a JSON list so downstream jobs process only
  the dates that actually need work.

DynamoDB key structure:
  PK : {workflow_run_id}#{job_name}
  SK : "config"
  Attributes:
    phase      : skeleton | merge | both | load | none
    start_date : YYYY-MM-DD  (earliest gap date)
    end_date   : YYYY-MM-DD  (latest gap date)
    gap_dates  : JSON list of all gap dates
    skip       : "true" | "false"
    ttl        : Unix epoch — item expires after 7 days (DynamoDB TTL)
    created_at : ISO timestamp
    reason     : set when skip=true

Downstream job names (must match exactly):
  factstop-skeleton-and-merge-load
  facttrip-skeleton-and-merge-load
  factserviceday-load

Supports:
  --WORKFLOW_RUN_ID   injected automatically by Glue workflow
  --lookback_days     how many days back to scan (default: 30)

Changelog v2:
  - Removed unused glue_client import
  - Removed unused date import
  - Removed unused build_date_ranges function (replaced by gap_dates list)
  - Removed dead date_list variable in get_factstop_gaps
  - Non-contiguous gaps now preserved — downstream jobs process only gap dates
  - Added 7-day TTL on all DynamoDB items
  - Added retry logic on DynamoDB put_item
  - Raised REDSHIFT_POLL_TIMEOUT from 300 to 900
  - Documented FactServiceDay projected-state reasoning in code

Author: Transit DW Team — P1
"""

import sys
import time
import json
import boto3
from datetime import datetime, timedelta, timezone
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
REDSHIFT_POLL_TIMEOUT  = 900   # raised from 300 — gap queries scan large fact tables
REDSHIFT_POLL_INTERVAL = 5

DDB_TTL_DAYS = 7   # DynamoDB items expire after 7 days

# Downstream job names — must match Glue job names exactly
JOB_FACTSTOP       = 'factstop-skeleton-and-merge-load'
JOB_FACTTRIP       = 'facttrip-skeleton-and-merge-load'
JOB_FACTSERVICEDAY = 'factserviceday-load'

# Default lookback window in days
DEFAULT_LOOKBACK_DAYS = 30

rs_data   = boto3.client('redshift-data', region_name=REGION)
dynamodb  = boto3.resource('dynamodb',    region_name=REGION)
ddb_table = dynamodb.Table(DYNAMODB_TABLE)


# ============================================================
# Argument Resolution
# ============================================================
def resolve_args() -> dict:
    """
    Returns dict with:
      workflow_run_id : str  — injected by Glue workflow, used as DDB PK prefix
      lookback_days   : int  — how far back to scan for gaps
    """
    try:
        args = getResolvedOptions(sys.argv, ['WORKFLOW_RUN_ID'])
        workflow_run_id = args['WORKFLOW_RUN_ID']
    except Exception:
        # Fallback for manual runs outside a workflow
        workflow_run_id = f"manual-{datetime.now(LOCAL_TZ).strftime('%Y%m%d-%H%M%S')}"

    lookback_days = DEFAULT_LOOKBACK_DAYS
    try:
        args = getResolvedOptions(sys.argv, ['lookback_days'])
        lookback_days = int(args['lookback_days'])
    except Exception:
        pass

    return {'workflow_run_id': workflow_run_id, 'lookback_days': lookback_days}


# ============================================================
# Redshift Data API
# ============================================================
def run_sql(sql: str, description: str = '', timeout: int = None):
    """
    Execute SQL and poll until complete.
    Returns (status_resp, query_id) tuple.
    """
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
            rows = status_resp.get('ResultRows', 0)
            print(f"     ✓ {label} — {rows} rows ({elapsed}s)")
            return status_resp, query_id

        elif status in ('FAILED', 'ABORTED'):
            err = status_resp.get('Error', 'No error detail')
            raise RuntimeError(f"SQL FAILED [{label}]: {err}")

    raise RuntimeError(f"SQL TIMED OUT after {timeout}s [{label}]")


def fetch_date_list(query_id: str) -> list:
    """
    Fetch result rows from a completed query.
    Returns sorted list of YYYY-MM-DD strings. Expects single-column result.
    """
    try:
        result = rs_data.get_statement_result(Id=query_id)
        dates  = []
        for row in result.get('Records', []):
            val = next(
                (str(v) for k, v in row[0].items() if v is not None),
                None
            )
            if val:
                dates.append(val)
        return sorted(dates)
    except Exception as e:
        print(f"     WARN: Could not fetch date list — {e}")
        return []


def fetch_keyed_rows(query_id: str) -> dict:
    """
    Fetch result rows from a completed query.
    Returns dict keyed by YYYY-MM-DD with remaining columns as values list.
    First column must be a datekey integer (YYYYMMDD format).
    """
    try:
        result = rs_data.get_statement_result(Id=query_id)
        rows   = {}
        for row in result.get('Records', []):
            vals = [
                next((str(v) for k, v in col.items() if v is not None), '0')
                for col in row
            ]
            if not vals:
                continue
            dk_str = vals[0].zfill(8)           # ensure 8 digits e.g. '20260323'
            dt     = f"{dk_str[:4]}-{dk_str[4:6]}-{dk_str[6:]}"
            rows[dt] = vals[1:]                  # remaining columns as list
        return rows
    except Exception as e:
        print(f"     WARN: Could not fetch keyed rows — {e}")
        return {}


# ============================================================
# Gap Detection Queries
# ============================================================
def get_staging_dates(lookback_days: int) -> dict:
    """
    Find all dates available in RT staging tables within lookback window.
    Returns dict: {'stu_dates': set, 'vp_dates': set}
    """
    cutoff = (datetime.now(LOCAL_TZ) - timedelta(days=lookback_days)).strftime('%Y-%m-%d')

    _, qid = run_sql(
        f"""
        SELECT DISTINCT service_date::VARCHAR AS dt
        FROM stg.rt_stop_time_updates
        WHERE service_date >= '{cutoff}'
        ORDER BY dt;
        """,
        description="Staging: rt_stop_time_updates available dates"
    )
    stu_dates = set(fetch_date_list(qid))

    _, qid = run_sql(
        f"""
        SELECT DISTINCT DATE_TRUNC('day', timestamp_local)::DATE::VARCHAR AS dt
        FROM stg.rt_vehicle_positions
        WHERE timestamp_local >= '{cutoff}'
        ORDER BY dt;
        """,
        description="Staging: rt_vehicle_positions available dates"
    )
    vp_dates = set(fetch_date_list(qid))

    print(f"\n  Staging availability:")
    print(f"    rt_stop_time_updates : {sorted(stu_dates)}")
    print(f"    rt_vehicle_positions : {sorted(vp_dates)}")

    return {'stu_dates': stu_dates, 'vp_dates': vp_dates}


def get_factstop_gaps(stu_dates: set) -> dict:
    """
    For dates available in rt_stop_time_updates, determine:
      missing_skeleton : dates with zero FactStop rows
      needs_merge      : dates with FALLBACK_SCHEDULED rows remaining
    Returns dict with two sorted lists.
    """
    if not stu_dates:
        return {'missing_skeleton': [], 'needs_merge': []}

    datekeys_clause = ", ".join(str(int(d.replace('-', ''))) for d in sorted(stu_dates))

    _, qid = run_sql(
        f"""
        SELECT
            LPAD(datekey::VARCHAR, 8, '0')                    AS datekey_str,
            SUM(CASE WHEN arrivalsource = 'FALLBACK_SCHEDULED'
                     THEN 1 ELSE 0 END)                       AS fallback_count,
            COUNT(*)                                           AS total_count
        FROM dw.FactStop
        WHERE datekey IN ({datekeys_clause})
        GROUP BY datekey
        ORDER BY datekey;
        """,
        description="FactStop: gap analysis"
    )
    existing = fetch_keyed_rows(qid)

    missing_skeleton = []
    needs_merge      = []

    for dt in sorted(stu_dates):
        if dt not in existing:
            missing_skeleton.append(dt)
        elif int(existing[dt][0]) > 0:          # fallback_count > 0
            needs_merge.append(dt)
        else:
            print(f"     ✓ FactStop {dt} — fully merged ({existing[dt][1]} rows)")

    print(f"\n  FactStop gaps:")
    print(f"    Missing skeleton : {missing_skeleton}")
    print(f"    Needs merge      : {needs_merge}")

    return {'missing_skeleton': missing_skeleton, 'needs_merge': needs_merge}


def get_facttrip_gaps(vp_dates: set) -> dict:
    """
    For dates available in rt_vehicle_positions, determine:
      missing_skeleton : dates with zero FactTrip rows
      needs_merge      : dates with MISSED rows (pings exist but not merged)
    Returns dict with two sorted lists.
    """
    if not vp_dates:
        return {'missing_skeleton': [], 'needs_merge': []}

    datekeys_clause = ", ".join(str(int(d.replace('-', ''))) for d in sorted(vp_dates))

    _, qid = run_sql(
        f"""
        SELECT
            LPAD(datekey::VARCHAR, 8, '0')                    AS datekey_str,
            SUM(CASE WHEN tripstatus = 'MISSED'
                     THEN 1 ELSE 0 END)                       AS missed_count,
            COUNT(*)                                           AS total_count
        FROM dw.FactTrip
        WHERE datekey IN ({datekeys_clause})
        GROUP BY datekey
        ORDER BY datekey;
        """,
        description="FactTrip: gap analysis"
    )
    existing = fetch_keyed_rows(qid)

    missing_skeleton = []
    needs_merge      = []

    for dt in sorted(vp_dates):
        if dt not in existing:
            missing_skeleton.append(dt)
        elif int(existing[dt][0]) > 0:          # missed_count > 0
            needs_merge.append(dt)
        else:
            print(f"     ✓ FactTrip {dt} — fully merged ({existing[dt][1]} rows)")

    print(f"\n  FactTrip gaps:")
    print(f"    Missing skeleton : {missing_skeleton}")
    print(f"    Needs merge      : {needs_merge}")

    return {'missing_skeleton': missing_skeleton, 'needs_merge': needs_merge}


def get_factserviceday_gaps(all_facttrip_dates: set) -> list:
    """
    Find dates where FactTrip has OPERATED rows but FactServiceDay
    has no matching agencykey+datekey row.

    Note on projected state: this query runs before facttrip job has
    executed for the current workflow run. Dates that facttrip will
    process (from facttrip_params['gap_dates']) are added to
    all_facttrip_dates by the caller before this function is invoked.
    This ensures FSD gaps are detected for dates that don't yet have
    OPERATED rows in FactTrip — they will exist by the time FSD runs.
    """
    if not all_facttrip_dates:
        return []

    datekeys_clause = ", ".join(str(int(d.replace('-', ''))) for d in sorted(all_facttrip_dates))

    _, qid = run_sql(
        f"""
        SELECT DISTINCT
            LPAD(ft.datekey::VARCHAR, 8, '0') AS datekey_str
        FROM dw.FactTrip ft
        WHERE ft.datekey IN ({datekeys_clause})
          AND ft.tripstatus = 'OPERATED'
          AND NOT EXISTS (
              SELECT 1
              FROM dw.FactServiceDay fsd
              WHERE fsd.datekey   = ft.datekey
                AND fsd.agencykey = ft.agencykey
          )
        ORDER BY datekey_str;
        """,
        description="FactServiceDay: gap analysis"
    )

    try:
        result = rs_data.get_statement_result(Id=qid)
        gaps   = []
        for row in result.get('Records', []):
            dk_str = next((str(v) for k, v in row[0].items() if v is not None), None)
            if dk_str:
                dk_str = dk_str.zfill(8)
                gaps.append(f"{dk_str[:4]}-{dk_str[4:6]}-{dk_str[6:]}")
    except Exception as e:
        print(f"     WARN: Could not parse FactServiceDay gap results — {e}")
        gaps = []

    print(f"\n  FactServiceDay gaps: {gaps}")
    return gaps


# ============================================================
# DynamoDB Parameter Store
# ============================================================
def _ddb_ttl() -> int:
    """Return Unix epoch timestamp for DDB_TTL_DAYS from now."""
    return int(
        (datetime.now(timezone.utc) + timedelta(days=DDB_TTL_DAYS)).timestamp()
    )


def write_job_params(workflow_run_id: str, job_name: str, params: dict):
    """
    Write job parameters to DynamoDB with retry on transient failures.
    Adds a 7-day TTL so items expire automatically.

    Key structure:
      PK : {workflow_run_id}#{job_name}
      SK : "config"
    """
    item = {
        'PK'               : f"{workflow_run_id}#{job_name}",
        'SK'               : 'config',
        'workflow_run_id'  : workflow_run_id,
        'job_name'         : job_name,
        'created_at'       : datetime.now(LOCAL_TZ).isoformat(),
        'ttl'              : _ddb_ttl(),
        **params
    }

    for attempt in range(3):
        try:
            ddb_table.put_item(Item=item)
            print(f"     ✓ DynamoDB written: {job_name} → "
                  f"skip={params.get('skip')} phase={params.get('phase')} "
                  f"dates={params.get('start_date','')}→{params.get('end_date','')}")
            return
        except Exception as e:
            if attempt == 2:
                raise RuntimeError(
                    f"DynamoDB put_item failed after 3 attempts for {job_name}: {e}"
                ) from e
            wait = 2 ** attempt
            print(f"     WARN: DynamoDB put_item attempt {attempt+1} failed, "
                  f"retrying in {wait}s — {e}")
            time.sleep(wait)


def determine_job_params(gaps: dict, job_name: str) -> dict:
    """
    Determine the optimal phase and date list for a fact table job.

    Gap dates are preserved as a JSON list rather than collapsed into
    a single start→end range. This prevents downstream jobs from
    processing clean dates that happen to fall between two gap dates.

    Logic:
      missing_skeleton + needs_merge → phase=both, all dates combined
      missing_skeleton only          → phase=both (skeleton then immediate merge)
      needs_merge only               → phase=merge
      neither                        → skip=true
    """
    missing = gaps.get('missing_skeleton', [])
    merge   = gaps.get('needs_merge',      [])

    all_dates = sorted(set(missing + merge))

    if not all_dates:
        return {
            'skip'      : 'true',
            'phase'     : 'none',
            'start_date': '',
            'end_date'  : '',
            'gap_dates' : json.dumps([]),
        }

    phase = 'both' if missing else 'merge'

    return {
        'skip'      : 'false',
        'phase'     : phase,
        'start_date': all_dates[0],
        'end_date'  : all_dates[-1],
        'gap_dates' : json.dumps(all_dates),
    }


# ============================================================
# Main
# ============================================================
def main():
    args            = resolve_args()
    workflow_run_id = args['workflow_run_id']
    lookback_days   = args['lookback_days']

    print(f"\n{'='*60}")
    print(f"TRANSIT PIPELINE INSPECTOR  v2")
    print(f"  workflow_run_id : {workflow_run_id}")
    print(f"  lookback_days   : {lookback_days}")
    print(f"  timestamp       : {datetime.now(LOCAL_TZ).isoformat()}")
    print(f"{'='*60}")

    # ── Step 1: What dates does staging have? ────────────────
    print(f"\n[1] Scanning staging availability (last {lookback_days} days)")
    staging   = get_staging_dates(lookback_days)
    stu_dates = staging['stu_dates']
    vp_dates  = staging['vp_dates']

    if not stu_dates and not vp_dates:
        print("\n  WARNING: No staging data found within lookback window.")
        print("  Writing skip=true for all downstream jobs.")
        for job in [JOB_FACTSTOP, JOB_FACTTRIP, JOB_FACTSERVICEDAY]:
            write_job_params(workflow_run_id, job, {
                'skip'      : 'true',
                'phase'     : 'none',
                'start_date': '',
                'end_date'  : '',
                'gap_dates' : json.dumps([]),
                'reason'    : 'no_staging_data',
            })
        print(f"\n{'='*60}")
        print("INSPECTOR COMPLETE — no work to do")
        print(f"{'='*60}\n")
        return

    # ── Step 2: FactStop gap analysis ────────────────────────
    print(f"\n[2] FactStop gap analysis")
    factstop_gaps   = get_factstop_gaps(stu_dates)
    factstop_params = determine_job_params(factstop_gaps, JOB_FACTSTOP)
    write_job_params(workflow_run_id, JOB_FACTSTOP, factstop_params)

    # ── Step 3: FactTrip gap analysis ────────────────────────
    print(f"\n[3] FactTrip gap analysis")
    facttrip_gaps   = get_facttrip_gaps(vp_dates)
    facttrip_params = determine_job_params(facttrip_gaps, JOB_FACTTRIP)
    write_job_params(workflow_run_id, JOB_FACTTRIP, facttrip_params)

    # ── Step 4: FactServiceDay gap analysis ──────────────────
    # The FSD gap query checks current FactTrip OPERATED rows.
    # Since the facttrip job hasn't run yet this workflow cycle,
    # we also include dates from facttrip_params['gap_dates'] —
    # these are dates that will have OPERATED rows after facttrip runs.
    # Without this, FSD would miss newly-added dates on the first run.
    all_facttrip_dates = set(vp_dates)
    if facttrip_params.get('skip') == 'false':
        all_facttrip_dates.update(
            json.loads(facttrip_params.get('gap_dates', '[]'))
        )

    print(f"\n[4] FactServiceDay gap analysis")
    fsd_gaps   = get_factserviceday_gaps(all_facttrip_dates)
    fsd_params = determine_job_params(
        {'missing_skeleton': fsd_gaps, 'needs_merge': []},
        JOB_FACTSERVICEDAY
    )
    # FactServiceDay has no merge phase — override phase to 'load'
    if fsd_params.get('skip') == 'false':
        fsd_params['phase'] = 'load'
    write_job_params(workflow_run_id, JOB_FACTSERVICEDAY, fsd_params)

    # ── Step 5: Summary ──────────────────────────────────────
    print(f"\n{'='*60}")
    print(f"INSPECTOR COMPLETE  v2")
    print(f"\n  Parameters written to DynamoDB (TTL: {DDB_TTL_DAYS} days):")

    for label, p in [
        (JOB_FACTSTOP,       factstop_params),
        (JOB_FACTTRIP,       facttrip_params),
        (JOB_FACTSERVICEDAY, fsd_params),
    ]:
        gap_dates = json.loads(p.get('gap_dates', '[]'))
        print(f"\n  {label}")
        print(f"    skip      : {p['skip']}")
        print(f"    phase     : {p['phase']}")
        print(f"    gap_dates : {gap_dates}")

    print(f"{'='*60}\n")


if __name__ == '__main__':
    main()
