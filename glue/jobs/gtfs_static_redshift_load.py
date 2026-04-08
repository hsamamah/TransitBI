import sys
import boto3
import logging
import time
from datetime import datetime
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.utils import getResolvedOptions, GlueArgumentError

# ── Optional --target_date parameter ─────────────────────────────────────────
# When provided (YYYY-MM-DD), load from that specific staged S3 prefix instead
# of asserting today's date. Used for backloads and historical reloads.
# When absent, the scheduled behaviour is preserved: load from the latest
# staged prefix and fail loudly if it does not match today's UTC date.
try:
    _date_args = getResolvedOptions(sys.argv, ['target_date'])
    TARGET_DATE = _date_args['target_date']   # e.g. '2026-03-15'
except GlueArgumentError:
    TARGET_DATE = None

# Setup
sc = SparkContext()
glueContext = GlueContext(sc)
logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)

s3 = boto3.client('s3', region_name='us-west-2')
redshift_data = boto3.client('redshift-data', region_name='us-west-2')

# ── Config ────────────────────────────────────────────────────────────────────
STAGING_BUCKET = 'seattle-transit-staging'
WORKGROUP_NAME = 'team'
REDSHIFT_DB    = 'dev'

# IAM role ARN for Redshift COPY — injected as Glue job parameter --iam_role
try:
    _args = getResolvedOptions(sys.argv, ['iam_role'])
    IAM_ROLE = _args['iam_role']
except Exception:
    raise RuntimeError(
        "Missing required Glue job parameter --iam_role. "
        "Set it in DefaultArguments (deploy_glue.sh sets this automatically)."
    )

# Tables to load — values are the stg DDL columns we actually want to accept.
# Any column in the file but NOT in this set is silently ignored via the
# dynamic column list we build from the file header.
# If a file column IS in this set, it gets loaded; extras are dropped.
TABLES = [
    'agency',
    'routes',
    'stops',
    'trips',
    'stop_times',
    'calendar',
    'calendar_dates',
    'shapes',
    'transfers',
]

# ── Per-table: columns that exist in the stg DDL ─────────────────────────────
# Only columns present in BOTH the file header AND this set will be loaded.
# This is the source of truth for what stg actually has — update if DDL changes.
STG_COLUMNS = {
    'agency': {
        'agency_id', 'agency_name', 'agency_url', 'agency_timezone',
        'agency_lang', 'agency_phone', 'agency_fare_url', 'agency_email'
    },
    'routes': {
        'agency_id', 'route_id', 'route_short_name', 'route_long_name',
        'route_type', 'route_desc', 'route_url', 'route_color',
        'route_text_color', 'network_id', 'route_sort_order'
    },
    'stops': {
        'stop_id', 'stop_name', 'stop_lat', 'stop_lon', 'stop_code',
        'stop_desc', 'zone_id', 'stop_url', 'location_type', 'parent_station',
        'wheelchair_boarding', 'stop_timezone', 'platform_code', 'tts_stop_name'
    },
    'trips': {
        'route_id', 'trip_id', 'service_id', 'trip_short_name',
        'trip_headsign', 'direction_id', 'block_id', 'shape_id',
        'wheelchair_accessible', 'drt_advance_book_min', 'bikes_allowed',
        'fare_id', 'peak_offpeak', 'boarding_type'
    },
    'stop_times': {
        'trip_id', 'stop_id', 'arrival_time', 'departure_time',
        'timepoint', 'stop_sequence', 'stop_headsign', 'pickup_type',
        'drop_off_type', 'shape_dist_traveled', 'departure_buffer'
    },
    'calendar': {
        'service_id', 'monday', 'tuesday', 'wednesday', 'thursday',
        'friday', 'saturday', 'sunday', 'start_date', 'end_date'
    },
    'calendar_dates': {
        'service_id', 'date', 'exception_type'
    },
    'shapes': {
        'shape_id', 'shape_pt_sequence', 'shape_pt_lat', 'shape_pt_lon',
        'shape_dist_traveled'
    },
    'transfers': {
        'from_stop_id', 'from_route_id', 'to_stop_id', 'to_route_id',
        'transfer_type', 'min_transfer_time'
    },
}

# ── Helpers ───────────────────────────────────────────────────────────────────
def get_staging_prefix():
    """
    Return the S3 prefix to load from under gtfs-static/combined/.

    Backload mode (TARGET_DATE is set):
      Construct the exact prefix for the requested date and verify it exists
      in S3.  Does NOT check whether the date is today — that is intentional.

    Scheduled mode (TARGET_DATE is None):
      Find the most recent staged prefix and verify it matches today's UTC
      date.  Fails loudly if it does not, to prevent silently loading stale
      data during the normal nightly run.
    """
    paginator = s3.get_paginator('list_objects_v2')

    if TARGET_DATE is not None:
        # ── Backload path ─────────────────────────────────────────────────────
        date_path = TARGET_DATE.replace('-', '/')          # '2026-03-15' → '2026/03/15'
        prefix    = f'gtfs-static/combined/{date_path}/'
        result    = s3.list_objects_v2(
            Bucket=STAGING_BUCKET, Prefix=prefix, MaxKeys=1
        )
        if not result.get('Contents'):
            raise Exception(
                f'No staged files found for target date {TARGET_DATE} '
                f'at s3://{STAGING_BUCKET}/{prefix}'
            )
        logger.info(f'Backload mode: loading from {prefix}')
        return prefix

    # ── Scheduled path ────────────────────────────────────────────────────────
    # We always know what today's prefix should be — just verify it exists.
    # The previous 3-level paginated enumeration was O(N API calls) for N date
    # folders, then immediately compared to today_prefix anyway.
    today_prefix = (
        'gtfs-static/combined/'
        + datetime.utcnow().strftime('%Y/%m/%d') + '/'
    )
    result = s3.list_objects_v2(
        Bucket=STAGING_BUCKET, Prefix=today_prefix, MaxKeys=1
    )
    if not result.get('Contents'):
        raise Exception(
            f"Today's staged prefix not found: s3://{STAGING_BUCKET}/{today_prefix}. "
            f"Ingestion may have failed or used a fallback. "
            f"Aborting load to avoid loading stale data silently."
        )

    logger.info(f'Staging prefix verified for today: {today_prefix}')
    return today_prefix


def get_file_columns(s3_key):
    """
    Read the first line of a GTFS txt file from S3 and return the
    column names as a list, in file order, stripped of whitespace and BOM.
    """
    response = s3.get_object(Bucket=STAGING_BUCKET, Key=s3_key, Range='bytes=0-4095')
    raw = response['Body'].read().decode('utf-8-sig')  # utf-8-sig strips BOM if present
    first_line = raw.splitlines()[0]
    columns = [c.strip().lower() for c in first_line.split(',')]
    return columns


def build_column_list(table, file_columns):
    """
    Return an ordered list of columns to pass to the COPY statement.
    - Preserves file order (Redshift COPY maps positionally).
    - Only includes columns that exist in the stg DDL for this table.
    - Columns in the file but not in the DDL are replaced with a blank
      placeholder so Redshift skips them without erroring.

    We achieve the skip by using the COPY column list feature:
    columns listed = loaded; unlisted = must not appear, so we can't simply
    omit them. Instead we use the 'COPY ... (col_a, col_b, ...)' syntax
    which tells Redshift the ORDER of file columns — columns in the file
    that have no DDL match are represented as empty string sentinels using
    a Redshift dummy column trick... EXCEPT Redshift COPY does not support
    skipping mid-file columns natively.

    Real solution: we load ALL file columns positionally by adding any
    missing columns to the stg DDL as VARCHAR dummy cols, OR we use a
    manifest/JSONPaths approach.  The simplest production-safe approach
    here is to add the extra file columns to each stg table as nullable
    VARCHAR columns so the COPY never fails on unknown columns.

    This function therefore returns:
      - matched_columns : file-order list of columns present in stg DDL
      - extra_columns   : file columns NOT in stg DDL (caller can warn/add)
    """
    stg = STG_COLUMNS[table]
    matched = []
    extra = []
    for col in file_columns:
        if col in stg:
            matched.append(col)
        else:
            extra.append(col)
    return matched, extra


def run_sql(sql, description):
    """Execute SQL via Redshift Data API and wait for completion."""
    logger.info(f'Running: {description}')
    response = redshift_data.execute_statement(
        WorkgroupName=WORKGROUP_NAME,
        Database=REDSHIFT_DB,
        Sql=sql
    )
    statement_id = response['Id']

    while True:
        status_response = redshift_data.describe_statement(Id=statement_id)
        status = status_response['Status']
        if status == 'FINISHED':
            logger.info(f'Completed: {description}')
            return
        elif status in ('FAILED', 'ABORTED'):
            error = status_response.get('Error', 'Unknown error')
            raise Exception(f'SQL failed [{description}]: {error}')
        else:
            time.sleep(2)


def ensure_extra_columns(table, extra_columns):
    """
    ALTER TABLE to add any file columns that don't yet exist in stg.
    This makes the schema self-healing — new GTFS extension columns
    are added automatically on first encounter.
    Each new column is added as VARCHAR(256) NULL.
    """
    for col in extra_columns:
        # Use IF NOT EXISTS equivalent: catch the error silently via a
        # separate existence check against information_schema.
        check_sql = f"""
            SELECT COUNT(*) FROM information_schema.columns
            WHERE table_schema = 'stg'
              AND table_name   = '{table}'
              AND column_name  = '{col}';
        """
        # We can't easily read results back via run_sql (fire-and-forget),
        # so we attempt the ALTER and tolerate the "column already exists" error.
        alter_sql = f"ALTER TABLE stg.{table} ADD COLUMN {col} VARCHAR(512);"
        try:
            run_sql(alter_sql, f'ADD COLUMN stg.{table}.{col}')
            logger.info(f'Added new column stg.{table}.{col}')
        except Exception as e:
            if 'already exists' in str(e).lower():
                logger.info(f'Column stg.{table}.{col} already exists — skipping')
            else:
                raise


def load_table(table, s3_prefix):
    s3_key = f'{s3_prefix}{table}.txt'

    # ── 1. Read header from file ──────────────────────────────────────────────
    logger.info(f'Reading header for {table}.txt')
    file_columns = get_file_columns(s3_key)
    logger.info(f'  File columns ({len(file_columns)}): {file_columns}')

    # ── 2. Reconcile against stg DDL ─────────────────────────────────────────
    matched_columns, extra_columns = build_column_list(table, file_columns)
    logger.info(f'  Matched columns ({len(matched_columns)}): {matched_columns}')

    if extra_columns:
        logger.warning(f'  Extra columns not in stg DDL ({len(extra_columns)}): {extra_columns}')
        # Auto-add extra columns to stg so COPY does not fail
        ensure_extra_columns(table, extra_columns)
        # After adding, include them in the load — full file order
        load_columns = file_columns   # all columns, in file order
    else:
        load_columns = matched_columns

    # ── 3. Truncate ───────────────────────────────────────────────────────────
    run_sql(f'TRUNCATE TABLE stg.{table};', f'TRUNCATE stg.{table}')

    # ── 4. COPY using file-order column list ──────────────────────────────────
    col_list = ', '.join(load_columns)
    copy_sql = (
        f"COPY stg.{table} ({col_list}) "
        f"FROM 's3://{STAGING_BUCKET}/{s3_key}' "
        f"IAM_ROLE '{IAM_ROLE}' "
        f"CSV IGNOREHEADER 1 EMPTYASNULL BLANKSASNULL"
    )
    run_sql(copy_sql, f'COPY stg.{table}')

    # ── 5. Row count ──────────────────────────────────────────────────────────
    run_sql(f'SELECT COUNT(*) FROM stg.{table};', f'COUNT stg.{table}')


# ── Main ──────────────────────────────────────────────────────────────────────
logger.info('=== gtfs-static-redshift-load started ===')
logger.info(f'TARGET_DATE: {TARGET_DATE or "None (scheduled mode — expect today)"}')

try:
    latest_prefix = get_staging_prefix()
    logger.info(f'Loading from: s3://{STAGING_BUCKET}/{latest_prefix}')

    for table in TABLES:
        logger.info(f'--- Processing stg.{table} ---')
        load_table(table, latest_prefix)

    logger.info('=== gtfs-static-redshift-load completed successfully ===')

except Exception as e:
    logger.error(f'Job failed: {e}')
    raise