"""
GTFS-RT Lambda Polling Function — King County Metro + Sound Transit
===================================================================
Triggered by EventBridge every 1 minute.
Each invocation fetches twice with a 30-second sleep between fetches,
giving effective 30-second polling within Lambda's 1-minute minimum
EventBridge schedule.

Fixes applied vs. prior version:
  - HTTPS URL (was HTTP)
  - Double-fetch with time.sleep(30) for 30-second effective polling
  - Minimum byte validation (>100 bytes) catches empty-header responses
  - API key read from environment variable OBA_API_KEY
"""

import json
import os
import time
import boto3
from datetime import datetime, timezone
from urllib.request import urlopen, Request
from urllib.error import URLError, HTTPError

# ============================================================
# Configuration — set as Lambda Environment Variables
# ============================================================
S3_BUCKET = os.environ.get('S3_BUCKET', 'seattle-transit-raw')
API_KEY   = os.environ.get('OBA_API_KEY', '')          # never use TEST in prod
REGION    = os.environ.get('AWS_REGION', 'us-west-2')

if not API_KEY:
    raise RuntimeError("OBA_API_KEY environment variable is not set")

BASE_URL = "https://api.pugetsound.onebusaway.org/api/gtfs_realtime"   # HTTPS

FEEDS = {
    "king-county-metro": {
        "trip-updates":      f"{BASE_URL}/trip-updates-for-agency/1.pb?key={API_KEY}",
        "vehicle-positions": f"{BASE_URL}/vehicle-positions-for-agency/1.pb?key={API_KEY}",
    },
    "sound-transit": {
        "trip-updates":      f"{BASE_URL}/trip-updates-for-agency/40.pb?key={API_KEY}",
        "vehicle-positions": f"{BASE_URL}/vehicle-positions-for-agency/40.pb?key={API_KEY}",
    },
}

MIN_VALID_BYTES = 100   # a protobuf with only a header is ~20 bytes; real feeds are >> 100

s3 = boto3.client('s3', region_name=REGION)


# ============================================================
# Helpers
# ============================================================
def fetch_feed(url):
    """Download one GTFS-RT feed. Returns bytes or None."""
    try:
        req = Request(url)
        with urlopen(req, timeout=15) as resp:
            data = resp.read()
            return data if len(data) >= MIN_VALID_BYTES else None
    except (URLError, HTTPError) as e:
        print(f"WARN fetch failed: {url[:60]}... — {e}")
        return None


def save_to_s3(raw_bytes, agency, feed_type, now_utc):
    """Save raw .pb to s3://seattle-transit-raw/gtfs-rt/{agency}/{feed_type}/{YYYY/MM/DD/HHmmss}.pb"""
    key = (
        f"gtfs-rt/{agency}/{feed_type}/"
        f"{now_utc.strftime('%Y/%m/%d/%H%M%S')}.pb"
    )
    s3.put_object(
        Bucket=S3_BUCKET,
        Key=key,
        Body=raw_bytes,
        ContentType='application/x-protobuf',
    )
    return key


def fetch_all(now_utc):
    """Fetch all 4 feeds once and save to S3. Returns (ok, fail) counts."""
    ok = fail = 0
    feed_results = []

    for agency, feeds in FEEDS.items():
        for feed_type, url in feeds.items():
            raw = fetch_feed(url)

            if raw is None:
                fail += 1
                feed_results.append({
                    'agency': agency, 'feed': feed_type, 'status': 'FAILED'
                })
                print(f"FAIL {agency}/{feed_type}")
                continue

            s3_key = save_to_s3(raw, agency, feed_type, now_utc)
            ok += 1
            feed_results.append({
                'agency': agency, 'feed': feed_type,
                'status': 'OK', 'bytes': len(raw), 's3_key': s3_key,
            })
            print(f"OK   {agency}/{feed_type}: {len(raw):,}B → {s3_key}")

    return ok, fail, feed_results


# ============================================================
# Lambda Handler
# ============================================================
def lambda_handler(event, context):
    """
    Entry point.
    Fetches all feeds twice — once immediately, once after 30 seconds —
    to achieve effective 30-second polling within EventBridge's 1-minute
    minimum schedule.

    Lambda timeout must be set to >= 65 seconds in the function config.
    """
    results = {'fetches': []}
    total_ok = total_fail = 0

    # ── First fetch ──────────────────────────────────────────────
    now_utc = datetime.now(timezone.utc)
    print(f"--- Fetch 1 of 2  {now_utc.strftime('%H:%M:%S')} UTC ---")
    ok, fail, feed_results = fetch_all(now_utc)
    total_ok   += ok
    total_fail += fail
    results['fetches'].append({'fetch': 1, 'timestamp': now_utc.isoformat(), 'feeds': feed_results})

    # ── Wait 30 seconds ──────────────────────────────────────────
    time.sleep(30)

    # ── Second fetch ─────────────────────────────────────────────
    now_utc = datetime.now(timezone.utc)
    print(f"--- Fetch 2 of 2  {now_utc.strftime('%H:%M:%S')} UTC ---")
    ok, fail, feed_results = fetch_all(now_utc)
    total_ok   += ok
    total_fail += fail
    results['fetches'].append({'fetch': 2, 'timestamp': now_utc.isoformat(), 'feeds': feed_results})

    results['summary'] = {'total_ok': total_ok, 'total_fail': total_fail}
    print(f"Done: {total_ok} ok, {total_fail} fail")

    return {
        'statusCode': 200,
        'body': json.dumps(results),
    }