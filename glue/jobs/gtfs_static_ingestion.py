import boto3
import hashlib
import zipfile
import io
import json
import logging
import time
from datetime import datetime, timedelta
from boto3.dynamodb.conditions import Key

# Setup
logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)

dynamodb = boto3.resource('dynamodb', region_name='us-west-2')
table = dynamodb.Table('seattle-transit-pipeline')
s3 = boto3.client('s3', region_name='us-west-2')
redshift_data = boto3.client('redshift-data', region_name='us-west-2')

# Config
RAW_BUCKET = 'seattle-transit-raw'
STAGING_BUCKET = 'seattle-transit-staging'
TODAY = datetime.now().strftime('%Y/%m/%d')

FEED_KEY = 'puget-sound-consolidated'
FEED_URL = 'https://gtfs.sound.obaweb.org/prod/gtfs_puget_sound_consolidated.zip'
AGENCY_ID = 'PSC'

REDSHIFT_WORKGROUP = 'team'
REDSHIFT_DATABASE  = 'dev'


# ── Redshift helpers ──────────────────────────────────────────────────────────

def _redshift_execute(sql: str) -> str:
    response = redshift_data.execute_statement(
        WorkgroupName=REDSHIFT_WORKGROUP,
        Database=REDSHIFT_DATABASE,
        Sql=sql,
        WithEvent=True,
    )
    return response['Id']


def _redshift_poll(sid: str, timeout: int = 300) -> dict:
    elapsed = 0
    while elapsed < timeout:
        r = redshift_data.describe_statement(Id=sid)
        status = r['Status']
        logger.info(f"Redshift [{sid[:8]}] {status} ({elapsed}s)")
        if status == 'FINISHED':
            return r
        if status in ('FAILED', 'ABORTED'):
            raise RuntimeError(f"Redshift query {status}: {r.get('Error', 'no detail')}")
        time.sleep(5)
        elapsed += 5
    raise RuntimeError(f"Redshift query timed out after {timeout}s — id={sid}")


def _run_query(sql: str) -> None:
    sid = _redshift_execute(sql)
    _redshift_poll(sid)


def _run_select(sql: str) -> list:
    sid = _redshift_execute(sql)
    r = _redshift_poll(sid)

    if r.get('ResultRows', 0) == 0:
        return []

    result = redshift_data.get_statement_result(Id=sid)
    columns = [col['name'] for col in result['ColumnMetadata']]
    rows = []
    for record in result['Records']:
        row = {col: next(iter(field.values()), None)
               for col, field in zip(columns, record)}
        rows.append(row)
    return rows


# ── DimFeedVersion ────────────────────────────────────────────────────────────

def parse_feed_info(zip_bytes: bytes) -> dict:
    try:
        with zipfile.ZipFile(io.BytesIO(zip_bytes)) as zf:
            if 'feed_info.txt' not in zf.namelist():
                logger.info("feed_info.txt not present — skipping")
                return {}
            content = zf.read('feed_info.txt').decode('utf-8-sig')
            lines = content.strip().splitlines()
            if len(lines) < 2:
                return {}
            headers = [h.strip() for h in lines[0].split(',')]
            values  = [v.strip() for v in lines[1].split(',')]
            return dict(zip(headers, values))
    except Exception as e:
        logger.warning(f"Could not parse feed_info.txt: {e}")
        return {}


def populate_dim_feed_version(new_hash: str, staged_files: list, feed_info: dict) -> int:
    """
    1. Query dw.dimfeedversion for this hash.
    2. If found — return existing VersionKey, no write.
    3. If not found — insert new row, return new VersionKey.
    """
    logger.info(f"DimFeedVersion: checking hash {new_hash[:12]}...")

    rows = _run_select(f"SELECT versionkey FROM dw.dimfeedversion WHERE feedhash = '{new_hash}' LIMIT 1")

    if rows:
        existing_key = rows[0]['versionkey']
        logger.info(f"DimFeedVersion: hash exists — VersionKey={existing_key}")
        return existing_key

    logger.info("DimFeedVersion: new hash — inserting row")

    ingested_at = datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S')

    def _s(v):
        return 'NULL' if v is None else "'" + str(v).replace("'", "''") + "'"

    def _d(v):
        return f"'{v}'" if v else 'NULL'

    # Flip all existing rows to IsCurrent = FALSE and insert new row atomically.
    # Both statements are sent in a single execute_statement call so Redshift
    # treats them as one transaction — a crash between them cannot leave all
    # rows with IsCurrent = FALSE and no current row.
    _run_query(f"""
        BEGIN;

        UPDATE dw.dimfeedversion
        SET    iscurrent = FALSE
        WHERE  iscurrent = TRUE;

        INSERT INTO dw.dimfeedversion (
            feedhash, sourceurl, ingestedat,
            feedstartdate, feedenddate, feedpublishername,
            feedversion, filecount, iscurrent, isactive, notes
        ) VALUES (
            '{new_hash}', {_s(FEED_URL)}, '{ingested_at}',
            {_d(feed_info.get('feed_start_date'))}, {_d(feed_info.get('feed_end_date'))},
            {_s(feed_info.get('feed_publisher_name'))}, {_s(feed_info.get('feed_version'))},
            {len(staged_files)}, TRUE, TRUE, 'Inserted by gtfs-static-ingestion job'
        );

        COMMIT;
    """)

    # Fetch the VersionKey IDENTITY assigned
    rows = _run_select(f"SELECT versionkey FROM dw.dimfeedversion WHERE feedhash = '{new_hash}' ORDER BY versionkey DESC LIMIT 1")
    version_key = rows[0]['versionkey'] if rows else None
    logger.info(f"DimFeedVersion: inserted — VersionKey={version_key}")
    return version_key


# ── Original helpers (unchanged) ──────────────────────────────────────────────

def record_feed_version(new_hash: str, is_new_version: bool,
                         staged_files: list, date_path: str):
    today_str = datetime.now().strftime('%Y-%m-%d')

    if is_new_version:
        table.put_item(Item={
            'PK': f'FEED#{new_hash}',
            'SK': today_str,
            'entity_type': 'FEED_VERSION',
            'feed_key': FEED_KEY,
            'agency_id': AGENCY_ID,
            'feed_hash': new_hash,
            'first_seen_date': today_str,
            'last_seen_date': today_str,
            'staged_path': f'gtfs-static/combined/{date_path}/',
            'file_count': len(staged_files),
            'staged_files': staged_files,
            'is_active': True
        })
        logger.info(f"DynamoDB: new feed version recorded — hash {new_hash[:12]}...")
    else:
        response = table.query(KeyConditionExpression=Key('PK').eq(f'FEED#{new_hash}'))
        if response['Items']:
            existing = response['Items'][0]
            table.update_item(
                Key={'PK': f'FEED#{new_hash}', 'SK': existing['SK']},
                UpdateExpression='SET last_seen_date = :date',
                ExpressionAttributeValues={':date': today_str}
            )
            logger.info(f"DynamoDB: feed version last_seen updated — hash {new_hash[:12]}...")
        else:
            table.put_item(Item={
                'PK': f'FEED#{new_hash}',
                'SK': today_str,
                'entity_type': 'FEED_VERSION',
                'feed_key': FEED_KEY,
                'agency_id': AGENCY_ID,
                'feed_hash': new_hash,
                'first_seen_date': today_str,
                'last_seen_date': today_str,
                'staged_path': f'gtfs-static/combined/{date_path}/',
                'file_count': len(staged_files),
                'staged_files': staged_files,
                'is_active': True,
                'note': 'Created on first DynamoDB run — feed was already known'
            })
            logger.info(f"DynamoDB: first-time feed version record created — hash {new_hash[:12]}...")


def record_pipeline_run(status: str, is_new_version: bool,
                         staged_files: list, version_key: int = None,
                         error: str = None):
    now = datetime.now()
    today_str = now.strftime('%Y-%m-%d')
    timestamp = now.strftime('%H:%M:%S')

    item = {
        'PK': f'PIPELINE_RUN#{today_str}',
        'SK': timestamp,
        'entity_type': 'PIPELINE_RUN',
        'job_name': 'gtfs-static-ingestion',
        'status': status,
        'is_new_version': is_new_version,
        'file_count': len(staged_files) if staged_files else 0,
        'run_date': today_str,
        'run_timestamp': timestamp
    }
    if version_key is not None:
        item['redshift_version_key'] = version_key
    if error:
        item['error'] = error

    table.put_item(Item=item)
    logger.info(f"DynamoDB: pipeline run recorded — status {status}")


def record_fallback_event(fallback_date: str, reason: str):
    now = datetime.now()
    today_str = now.strftime('%Y-%m-%d')
    timestamp = now.strftime('%H:%M:%S')

    table.put_item(Item={
        'PK': f'FALLBACK#{today_str}',
        'SK': timestamp,
        'entity_type': 'FALLBACK',
        'fallback_date': fallback_date,
        'reason': reason,
        'run_date': today_str,
        'run_timestamp': timestamp
    })
    logger.info(f"DynamoDB: fallback event recorded — using data from {fallback_date}")


def compute_sha256(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def get_previous_hash() -> str:
    hash_key = f'gtfs-static/combined/{FEED_KEY}_last_hash.json'
    try:
        response = s3.get_object(Bucket=RAW_BUCKET, Key=hash_key)
        data = json.loads(response['Body'].read())
        return data.get('hash', '')
    except Exception as e:
        logger.info(f"No previous hash found — first run: {e}")
        return ''


def save_current_hash(new_hash: str, loaded_date: str):
    hash_key = f'gtfs-static/combined/{FEED_KEY}_last_hash.json'
    payload = json.dumps({'hash': new_hash, 'loaded_date': loaded_date})
    s3.put_object(Bucket=RAW_BUCKET, Key=hash_key, Body=payload.encode('utf-8'))
    logger.info(f"Hash saved: {new_hash[:12]}...")


def download_gtfs_zip(url: str) -> bytes:
    import urllib.request
    logger.info(f"Downloading GTFS feed from {url}")
    with urllib.request.urlopen(url, timeout=60) as response:
        return response.read()


def save_raw_zip(zip_bytes: bytes, date_path: str, new_hash: str, is_new_version: bool):
    if not is_new_version:
        logger.info("Feed unchanged — skipping raw ZIP save")
        return
    raw_key = (
        f'gtfs-static/combined/unique-feeds/'
        f'{new_hash[:16]}_{date_path.replace("/", "-")}_{FEED_KEY}_gtfs.zip'
    )
    s3.put_object(Bucket=RAW_BUCKET, Key=raw_key, Body=zip_bytes)
    logger.info(f"New unique feed saved to s3://{RAW_BUCKET}/{raw_key}")


def stage_gtfs_files(zip_bytes: bytes, date_path: str):
    staged_files = []
    with zipfile.ZipFile(io.BytesIO(zip_bytes)) as zf:
        for gtfs_file in zf.namelist():
            if not gtfs_file.endswith('.txt'):
                logger.info(f"Skipping non-txt file: {gtfs_file}")
                continue
            content = zf.read(gtfs_file)
            staging_key = f'gtfs-static/combined/{date_path}/{gtfs_file}'
            s3.put_object(Bucket=STAGING_BUCKET, Key=staging_key, Body=content)
            staged_files.append(gtfs_file)
            logger.info(f"Staged {gtfs_file} ({len(content):,} bytes)")
    return staged_files


def write_feed_version_record(new_hash: str, is_new_version: bool,
                               date_path: str, staged_files: list,
                               version_key: int = None):
    record = {
        'feed_key': FEED_KEY,
        'agency_id': AGENCY_ID,
        'feed_hash': new_hash,
        'load_date': datetime.now().strftime('%Y-%m-%d'),
        'is_new_version': is_new_version,
        'staged_files': staged_files,
        'file_count': len(staged_files),
        'redshift_version_key': version_key
    }
    version_key_path = f'gtfs-static/combined/{date_path}/{FEED_KEY}_feed_version.json'
    s3.put_object(Bucket=STAGING_BUCKET, Key=version_key_path,
                  Body=json.dumps(record).encode('utf-8'))
    logger.info(
        f"Feed version record written — "
        f"new_version={is_new_version}, hash={new_hash[:12]}..., "
        f"redshift_version_key={version_key}"
    )


def get_fallback_staged_files() -> tuple:
    yesterday = (datetime.now() - timedelta(days=1)).strftime('%Y/%m/%d')
    yesterday_str = (datetime.now() - timedelta(days=1)).strftime('%Y-%m-%d')

    try:
        version_key = f'gtfs-static/combined/{yesterday}/puget-sound-consolidated_feed_version.json'
        response = s3.get_object(Bucket=STAGING_BUCKET, Key=version_key)
        yesterday_record = json.loads(response['Body'].read())
        yesterday_hash = yesterday_record.get('feed_hash', '')

        today_record = {
            'feed_key': FEED_KEY,
            'agency_id': AGENCY_ID,
            'feed_hash': yesterday_hash,
            'load_date': datetime.now().strftime('%Y-%m-%d'),
            'is_new_version': False,
            'staged_files': yesterday_record.get('staged_files', []),
            'file_count': yesterday_record.get('file_count', 0),
            'redshift_version_key': yesterday_record.get('redshift_version_key'),
            'fallback': True,
            'fallback_date': yesterday,
            'note': "Download failed — using yesterday's staged files"
        }

        version_key_today = f'gtfs-static/combined/{TODAY}/puget-sound-consolidated_feed_version.json'
        s3.put_object(Bucket=STAGING_BUCKET, Key=version_key_today,
                      Body=json.dumps(today_record).encode('utf-8'))

        logger.warning(f"Fallback: pointing today to yesterday's staged files at {yesterday}")
        return True, yesterday_str

    except Exception as e:
        logger.error(f"Fallback failed: {e}")
        return False, ''


# ── Main ──────────────────────────────────────────────────────────────────────
logger.info("GTFS Static Ingestion Job started")
logger.info(f"Feed URL: {FEED_URL}")
logger.info(f"Date path: {TODAY}")

staged_files   = []
is_new_version = False
version_key    = None

try:
    # 1. Download and hash
    zip_bytes = download_gtfs_zip(FEED_URL)
    logger.info(f"Downloaded {len(zip_bytes):,} bytes")

    new_hash      = compute_sha256(zip_bytes)
    previous_hash = get_previous_hash()
    is_new_version = new_hash != previous_hash

    if is_new_version:
        logger.info(f"Feed has changed — new version detected (hash: {new_hash[:12]}...)")
    else:
        logger.info(f"Feed unchanged since last run (hash: {new_hash[:12]}...)")

    # 2. Save and stage
    save_raw_zip(zip_bytes, TODAY, new_hash, is_new_version)
    staged_files = stage_gtfs_files(zip_bytes, TODAY)
    logger.info(f"Staged {len(staged_files)} files: {staged_files}")

    # 3. Parse feed_info.txt
    feed_info = parse_feed_info(zip_bytes)

    # 4. Populate DimFeedVersion
    version_key = populate_dim_feed_version(
        new_hash     = new_hash,
        staged_files = staged_files,
        feed_info    = feed_info,
    )

    # 5. Write staging manifest with version_key for downstream jobs
    write_feed_version_record(
        new_hash       = new_hash,
        is_new_version = is_new_version,
        date_path      = TODAY,
        staged_files   = staged_files,
        version_key    = version_key,
    )

    # 6. DynamoDB audit trail
    record_feed_version(
        new_hash       = new_hash,
        is_new_version = is_new_version,
        staged_files   = staged_files,
        date_path      = TODAY,
    )

    save_current_hash(new_hash, TODAY)

    record_pipeline_run(
        status         = 'SUCCEEDED',
        is_new_version = is_new_version,
        staged_files   = staged_files,
        version_key    = version_key,
    )

    logger.info("=== GTFS Static Ingestion Job completed successfully ===")
    logger.info(f"    VersionKey for downstream jobs: {version_key}")

except Exception as e:
    logger.error(f"Job failed: {e}")

    record_pipeline_run(
        status         = 'FAILED',
        is_new_version = is_new_version,
        staged_files   = staged_files,
        version_key    = version_key,
        error          = str(e),
    )

    logger.warning("Attempting fallback to yesterday's staged files...")
    fallback_success, fallback_date = get_fallback_staged_files()

    if fallback_success:
        record_fallback_event(fallback_date=fallback_date, reason=str(e))
        logger.warning("Fallback succeeded — downstream jobs will use yesterday's data")
    else:
        logger.error("Fallback failed — no data available for today")

    # Always re-raise so the Glue workflow sees FAILED and does not
    # silently proceed with stale data as if today's ingestion succeeded.
    raise