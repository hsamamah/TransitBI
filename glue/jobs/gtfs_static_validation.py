import boto3
import json
import logging
from datetime import datetime
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from pyspark.sql import functions as F

# Setup
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)
dynamodb = boto3.resource('dynamodb', region_name='us-west-2')
table = dynamodb.Table('seattle-transit-pipeline')
s3 = boto3.client('s3', region_name='us-west-2')

# Config
STAGING_BUCKET = 'seattle-transit-staging'
TODAY = datetime.now().strftime('%Y/%m/%d')
DATABASE = 'seattle_transit_staging'
REPORT_KEY = f'gtfs-static/combined/{TODAY}/validation_report.json'

# ── Required columns per GTFS file ───────────────────────────────────────────
REQUIRED_COLUMNS = {
    'gtfs_agency_txt':          ['agency_id', 'agency_name', 'agency_url', 'agency_timezone'],
    'gtfs_routes_txt':          ['route_id', 'agency_id', 'route_short_name', 'route_type'],
    'gtfs_stops_txt':           ['stop_id', 'stop_name', 'stop_lat', 'stop_lon'],
    'gtfs_trips_txt':           ['route_id', 'service_id', 'trip_id'],
    'gtfs_stop_times_txt':      ['trip_id', 'arrival_time', 'departure_time', 'stop_id', 'stop_sequence'],
    'gtfs_calendar_txt':        ['service_id', 'monday', 'tuesday', 'wednesday', 'thursday', 'friday', 'saturday', 'sunday', 'start_date', 'end_date'],
    'gtfs_calendar_dates_txt':  ['service_id', 'date', 'exception_type'],
    'gtfs_shapes_txt':          ['shape_id', 'shape_pt_lat', 'shape_pt_lon', 'shape_pt_sequence'],
    'gtfs_transfers_txt':       ['from_stop_id', 'to_stop_id', 'transfer_type'],
    'gtfs_fare_attributes_txt': ['fare_id', 'price', 'currency_type', 'payment_method', 'transfers'],
    'gtfs_fare_rules_txt':      ['fare_id'],
    'gtfs_feed_info_txt':       ['feed_publisher_name', 'feed_publisher_url', 'feed_lang'],
}

# ── Key fields that must not be NULL ─────────────────────────────────────────
NULL_CHECK_COLUMNS = {
    'gtfs_agency_txt':        ['agency_id', 'agency_name'],
    'gtfs_routes_txt':        ['route_id', 'agency_id', 'route_type'],
    'gtfs_stops_txt':         ['stop_id', 'stop_lat', 'stop_lon'],
    'gtfs_trips_txt':         ['trip_id', 'route_id', 'service_id'],
    'gtfs_stop_times_txt':    ['trip_id', 'stop_id', 'stop_sequence', 'departure_time'],
    'gtfs_calendar_txt':      ['service_id'],
    'gtfs_calendar_dates_txt':['service_id', 'date', 'exception_type'],
    'gtfs_shapes_txt':        ['shape_id', 'shape_pt_lat', 'shape_pt_lon'],
}

# ── Duplicate check primary keys ─────────────────────────────────────────────
DUPLICATE_CHECK_COLUMNS = {
    'gtfs_trips_txt':      ['trip_id'],
    'gtfs_stops_txt':      ['stop_id'],
    'gtfs_routes_txt':     ['route_id'],
    'gtfs_agency_txt':     ['agency_id'],
    'gtfs_shapes_txt':     ['shape_id', 'shape_pt_sequence'],
    'gtfs_stop_times_txt': ['trip_id', 'stop_sequence'],
}

def record_validation_results(report: dict):
    """
    Write validation results to DynamoDB.
    PK: VALIDATION#<date>
    SK: <table_name>
    One record per table validated.
    """
    from datetime import datetime
    today_str = datetime.now().strftime('%Y-%m-%d')
    
    # Write one record per table
    for result in report['results']:
        table_name = result['table']
        overall_passed = result['overall_passed']
        checks = result['checks']
        
        table.put_item(Item={
            'PK': f'VALIDATION#{today_str}',
            'SK': table_name,
            'entity_type': 'VALIDATION',
            'run_date': today_str,
            'table_name': table_name,
            'overall_passed': overall_passed,
            'row_count': checks.get('row_counts', {}).get('row_count', 0),
            'null_check_passed': checks.get('null_checks', {}).get('passed', True),
            'duplicate_check_passed': checks.get('duplicate_checks', {}).get('passed', True),
            'required_columns_passed': checks.get('required_columns', {}).get('passed', True),
            'missing_columns': checks.get('required_columns', {}).get('missing', [])
        })
    
    # Write one summary record
    summary = report['summary']
    table.put_item(Item={
        'PK': f'VALIDATION#{today_str}',
        'SK': 'SUMMARY',
        'entity_type': 'VALIDATION_SUMMARY',
        'run_date': today_str,
        'total_tables': summary['total_tables'],
        'passed': summary['passed'],
        'failed': summary['failed'],
        'failed_tables': summary['failed_tables'],
        'overall_passed': summary['overall_passed']
    })
    
    logger.info(f"DynamoDB: validation results recorded for {today_str}")

def is_new_feed_version() -> bool:
    """Check if today's feed version record indicates a new version."""
    version_key = f'gtfs-static/combined/{TODAY}/puget-sound-consolidated_feed_version.json'
    try:
        response = s3.get_object(Bucket=STAGING_BUCKET, Key=version_key)
        record = json.loads(response['Body'].read())
        is_new = record.get('is_new_version', False)
        logger.info(f"Feed version check — is_new_version: {is_new}")
        return is_new
    except Exception as e:
        logger.warning(f"Could not read feed version record: {e} — running validation anyway")
        return True

def load_table(table_name: str):
    """Load a table from Glue Data Catalog into a Spark DataFrame."""
    try:
        df = glueContext.create_dynamic_frame.from_catalog(
            database=DATABASE,
            table_name=table_name
        ).toDF()
        return df
    except Exception as e:
        logger.warning(f"Could not load {table_name}: {e}")
        return None

def check_row_counts(table_name: str, df) -> dict:
    """Count rows and flag if empty."""
    count = df.count()
    return {
        'row_count': count,
        'is_empty': count == 0,
        'passed': count > 0
    }

def check_required_columns(table_name: str, df) -> dict:
    """Check all required columns exist in the table."""
    if table_name not in REQUIRED_COLUMNS:
        return {'passed': True, 'note': 'No required columns defined'}
    
    required = REQUIRED_COLUMNS[table_name]
    actual = [c.lower() for c in df.columns]
    missing = [c for c in required if c.lower() not in actual]
    
    return {
        'required': required,
        'missing': missing,
        'passed': len(missing) == 0
    }

def check_nulls(table_name: str, df) -> dict:
    """Check for NULLs in key fields."""
    if table_name not in NULL_CHECK_COLUMNS:
        return {'passed': True, 'note': 'No null checks defined'}
    
    results = {}
    all_passed = True
    actual_cols = [c.lower() for c in df.columns]
    
    for col in NULL_CHECK_COLUMNS[table_name]:
        if col.lower() not in actual_cols:
            results[col] = {'null_count': 'column missing', 'passed': False}
            all_passed = False
            continue
        null_count = df.filter(F.col(col).isNull()).count()
        passed = null_count == 0
        results[col] = {
            'null_count': null_count,
            'passed': passed
        }
        if not passed:
            all_passed = False
    
    return {
        'columns': results,
        'passed': all_passed
    }

def check_departure_time_format(df) -> dict:
    """
    Validate stop_times departure_time format.
    GTFS times can exceed 24:00:00 for overnight trips.
    Valid format: H:MM:SS or HH:MM:SS where H can be > 23.
    """
    actual_cols = [c.lower() for c in df.columns]
    if 'departure_time' not in actual_cols:
        return {'passed': False, 'note': 'departure_time column missing'}

    total = df.count()
    
    # Valid GTFS time pattern — allows hours > 23 for overnight trips
    time_pattern = r'^\d{1,2}:\d{2}:\d{2}$'
    invalid_df = df.filter(
        F.col('departure_time').isNotNull() &
        ~F.col('departure_time').rlike(time_pattern)
    )
    invalid_count = invalid_df.count()

    # Also check for times stored as seconds from midnight (should be strings)
    sample_invalid = []
    if invalid_count > 0:
        sample_invalid = [
            row['departure_time'] 
            for row in invalid_df.select('departure_time').limit(5).collect()
        ]

    return {
        'total_rows': total,
        'invalid_format_count': invalid_count,
        'sample_invalid': sample_invalid,
        'passed': invalid_count == 0
    }

def check_duplicates(table_name: str, df) -> dict:
    """Flag duplicate primary keys."""
    if table_name not in DUPLICATE_CHECK_COLUMNS:
        return {'passed': True, 'note': 'No duplicate checks defined'}
    
    key_cols = DUPLICATE_CHECK_COLUMNS[table_name]
    actual_cols = [c.lower() for c in df.columns]
    missing_cols = [c for c in key_cols if c not in actual_cols]
    
    if missing_cols:
        return {
            'passed': False,
            'note': f'Key columns missing: {missing_cols}'
        }
    
    total = df.count()
    distinct = df.select(key_cols).distinct().count()
    duplicate_count = total - distinct
    
    return {
        'total_rows': total,
        'distinct_keys': distinct,
        'duplicate_count': duplicate_count,
        'passed': duplicate_count == 0
    }

def validate_table(table_name: str) -> dict:
    """Run all validation checks on a single table."""
    logger.info(f"Validating {table_name}...")
    result = {'table': table_name, 'checks': {}}

    df = load_table(table_name)
    if df is None:
        result['checks']['load'] = {'passed': False, 'note': 'Table could not be loaded'}
        result['overall_passed'] = False
        return result

    # Row counts
    result['checks']['row_counts'] = check_row_counts(table_name, df)

    # Required columns
    result['checks']['required_columns'] = check_required_columns(table_name, df)

    # NULL checks
    result['checks']['null_checks'] = check_nulls(table_name, df)

    # Departure time format — only for stop_times
    if table_name == 'gtfs_stop_times_txt':
        result['checks']['departure_time_format'] = check_departure_time_format(df)

    # Duplicate checks
    result['checks']['duplicate_checks'] = check_duplicates(table_name, df)

    # Overall pass/fail
    result['overall_passed'] = all(
        check.get('passed', True) 
        for check in result['checks'].values()
    )

    status = '✓ PASSED' if result['overall_passed'] else '✗ FAILED'
    logger.info(f"{table_name}: {status}")
    return result

def write_report(report: dict):
    """Write validation report to staging bucket."""
    s3.put_object(
        Bucket=STAGING_BUCKET,
        Key=REPORT_KEY,
        Body=json.dumps(report, indent=2).encode('utf-8')
    )
    logger.info(f"Validation report written to s3://{STAGING_BUCKET}/{REPORT_KEY}")

# ── Main ──────────────────────────────────────────────────────────────────────
logger.info("GTFS Static Validation Job started")
logger.info(f"Database: {DATABASE}")
logger.info(f"Date: {TODAY}")

# Skip validation if feed hasn't changed
if not is_new_feed_version():
    logger.info("Feed unchanged since last run — skipping validation")
    
    # Write a skipped record to DynamoDB so pipeline history is complete
    today_str = datetime.now().strftime('%Y-%m-%d')
    table.put_item(Item={
        'PK': f'VALIDATION#{today_str}',
        'SK': 'SUMMARY',
        'entity_type': 'VALIDATION_SUMMARY',
        'run_date': today_str,
        'total_tables': 0,
        'passed': 0,
        'failed': 0,
        'failed_tables': [],
        'overall_passed': True,
        'skipped': True,
        'skip_reason': 'Feed unchanged since last run'
    })
    logger.info("DynamoDB: skipped validation record written")
    logger.info("=== GTFS Static Validation Job completed (skipped) ===")
else:
    
    tables_to_validate = list(REQUIRED_COLUMNS.keys())
    
    logger.info("New feed version detected — proceeding with validation")
    
    tables_to_validate = list(REQUIRED_COLUMNS.keys())
    report = {
        'run_date': TODAY,
        'database': DATABASE,
        'tables_validated': len(tables_to_validate),
        'results': [],
        'summary': {}
    }
    
    for table_name in tables_to_validate:
        result = validate_table(table_name)
        report['results'].append(result)
    
    # Summary
    passed = [r for r in report['results'] if r['overall_passed']]
    failed = [r for r in report['results'] if not r['overall_passed']]
    
    report['summary'] = {
        'total_tables': len(tables_to_validate),
        'passed': len(passed),
        'failed': len(failed),
        'failed_tables': [r['table'] for r in failed],
        'overall_passed': len(failed) == 0
    }
    
    logger.info("=== Validation Summary ===")
    logger.info(f"Passed: {len(passed)}/{len(tables_to_validate)}")
    if failed:
        logger.warning(f"Failed tables: {[r['table'] for r in failed]}")
    
    record_validation_results(report)
    write_report(report)
    
    # Fail the job if any critical tables failed
    critical_tables = ['gtfs_trips_txt', 'gtfs_stops_txt', 'gtfs_routes_txt', 'gtfs_stop_times_txt']
    critical_failures = [r['table'] for r in failed if r['table'] in critical_tables]
    if critical_failures:
        raise Exception(f"Critical table validation failed: {critical_failures}")

logger.info("=== GTFS Static Validation Job completed ===")