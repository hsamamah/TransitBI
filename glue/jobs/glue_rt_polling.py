"""
GTFS-RT Polling Job — AWS Glue Python Shell Version

Adapted from local rt_polling.py for AWS Glue deployment.
Self-contained: all config is inline (Glue cannot import from your local config/).

Behavior:
  - Runs for RUN_DURATION_MINUTES (default 5 min)
  - Polls every 30 seconds
  - Saves raw .pb files directly to S3
  - Logs to CloudWatch (Glue handles this automatically)
  - Exits cleanly when duration is reached

Schedule: Pair with a Glue Trigger (every 5 min) for continuous coverage.

Dependencies (set in Glue Job --additional-python-modules):
  gtfs-realtime-bindings, requests, pytz
"""

import sys
import time
import logging
from datetime import datetime, timezone

import requests
import boto3
from google.transit import gtfs_realtime_pb2

# ============================================================
# CONFIGURATION (inline — no external imports in Glue)
# ============================================================

AWS_REGION = "us-west-2"
S3_BUCKET_RAW = "seattle-transit-raw"

OBA_API_KEY = "TEST"
_BASE = "http://api.pugetsound.onebusaway.org/api/gtfs_realtime"

AGENCY_FEEDS = {
    "king-county-metro": {
        "agency_id": "1",
        "display_name": "King County Metro",
        "feeds": {
            "trip-updates":      f"{_BASE}/trip-updates-for-agency/1.pb?key={OBA_API_KEY}",
            "vehicle-positions": f"{_BASE}/vehicle-positions-for-agency/1.pb?key={OBA_API_KEY}",
        },
    },
    "sound-transit": {
        "agency_id": "40",
        "display_name": "Sound Transit",
        "feeds": {
            "trip-updates":      f"{_BASE}/trip-updates-for-agency/40.pb?key={OBA_API_KEY}",
            "vehicle-positions": f"{_BASE}/vehicle-positions-for-agency/40.pb?key={OBA_API_KEY}",
        },
    },
}

POLL_INTERVAL_SECONDS = 30
FETCH_TIMEOUT_SECONDS = 15
RUN_DURATION_MINUTES = 5  # Each Glue invocation runs for 5 min then exits

# ============================================================
# Logging (Glue sends stdout/stderr to CloudWatch automatically)
# ============================================================
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)],
)
log = logging.getLogger("glue_rt_polling")

# ============================================================
# AWS Client
# ============================================================
s3_client = boto3.client("s3", region_name=AWS_REGION)


# ============================================================
# Core Functions
# ============================================================

def fetch_feed(url):
    """Download one GTFS-RT feed. Returns bytes or None."""
    try:
        r = requests.get(url, timeout=FETCH_TIMEOUT_SECONDS)
        r.raise_for_status()
        return r.content
    except requests.exceptions.Timeout:
        log.warning(f"Timeout: {url}")
    except requests.exceptions.ConnectionError:
        log.warning(f"Connection failed: {url}")
    except requests.exceptions.HTTPError as e:
        log.warning(f"HTTP {e.response.status_code}: {url}")
    except Exception as e:
        log.error(f"Unexpected: {url} — {e}")
    return None


def validate_feed(raw):
    """Quick sanity check: parseable and non-empty. Returns (ok, entity_count)."""
    try:
        feed = gtfs_realtime_pb2.FeedMessage()
        feed.ParseFromString(raw)
        n = len(feed.entity)
        if n == 0:
            log.warning("Empty feed (0 entities)")
            return (False, 0)
        age = int(datetime.now(timezone.utc).timestamp()) - feed.header.timestamp
        if age > 300:
            log.warning(f"Stale feed: {age}s old")
        return (True, n)
    except Exception as e:
        log.error(f"Parse failed: {e}")
        return (False, 0)


def save_s3(raw, agency_key, feed_type, now_utc):
    """Upload raw .pb to S3 bucket: seattle-transit-raw."""
    key = (
        f"gtfs-rt/{agency_key}/{feed_type}/"
        f"{now_utc.strftime('%Y/%m/%d/%H%M%S')}.pb"
    )
    s3_client.put_object(
        Bucket=S3_BUCKET_RAW,
        Key=key,
        Body=raw,
        ContentType="application/x-protobuf",
    )
    return f"s3://{S3_BUCKET_RAW}/{key}"


def run_one_cycle(cycle_num):
    """Fetch → validate → save to S3 for each agency × feed type."""
    now_utc = datetime.now(timezone.utc)
    ok = 0
    fail = 0
    entities = 0

    for agency_key, config in AGENCY_FEEDS.items():
        display = config["display_name"]

        for feed_type, url in config["feeds"].items():
            raw = fetch_feed(url)
            if raw is None:
                fail += 1
                continue

            valid, n = validate_feed(raw)
            if not valid:
                fail += 1
                continue

            dest = save_s3(raw, agency_key, feed_type, now_utc)
            entities += n
            ok += 1
            log.info(
                f"OK  {display}/{feed_type}: "
                f"{n} entities, {len(raw):,}B -> {dest}"
            )

    return {"ok": ok, "fail": fail, "entities": entities}


# ============================================================
# Main — runs for RUN_DURATION_MINUTES then exits
# ============================================================

def main():
    agency_names = ", ".join(c["display_name"] for c in AGENCY_FEEDS.values())
    total_feeds = sum(len(c["feeds"]) for c in AGENCY_FEEDS.values())

    log.info("=" * 50)
    log.info("GTFS-RT Glue Polling Job — Starting")
    log.info(f"  Agencies:  {agency_names}")
    log.info(f"  Feeds:     {total_feeds} total")
    log.info(f"  Interval:  {POLL_INTERVAL_SECONDS}s")
    log.info(f"  Duration:  {RUN_DURATION_MINUTES} min")
    log.info(f"  S3 Bucket: {S3_BUCKET_RAW}")
    log.info("=" * 50)

    start_time = time.time()
    end_time = start_time + (RUN_DURATION_MINUTES * 60)
    cycle = 0
    total_ok = 0
    total_fail = 0

    while time.time() < end_time:
        cycle += 1
        t0 = time.time()
        stats = run_one_cycle(cycle)
        total_ok += stats["ok"]
        total_fail += stats["fail"]
        elapsed = time.time() - t0

        log.info(
            f"Cycle #{cycle}: {stats['ok']} ok, {stats['fail']} fail, "
            f"{stats['entities']} entities, {elapsed:.1f}s"
        )

        # Sleep remaining interval, but check if we should exit
        sleep = max(0, POLL_INTERVAL_SECONDS - elapsed)
        if time.time() + sleep > end_time:
            break
        if sleep > 0:
            time.sleep(sleep)

    log.info("")
    log.info("=" * 50)
    log.info("GTFS-RT Glue Polling Job — Finished")
    log.info(f"  Cycles:    {cycle}")
    log.info(f"  Succeeded: {total_ok}")
    log.info(f"  Failed:    {total_fail}")
    log.info(f"  Runtime:   {(time.time() - start_time) / 60:.1f} min")
    log.info("=" * 50)


if __name__ == "__main__":
    main()
