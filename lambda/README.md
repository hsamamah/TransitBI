# lambda/
AWS Lambda functions for the Seattle Transit DW — real-time feed polling, pipeline notifications, and failure alerting.

---

## Functions

| Function | Trigger | Purpose |
|---|---|---|
| `gtfs-rt-polling` | EventBridge — every 1 minute | Fetches GTFS-RT protobuf feeds from OBA API and saves to S3 |
| `gtfs-pipeline-notification` | Manual / EventBridge | Sends daily pipeline summary email via SNS |
| `failure-notifier` | EventBridge — Glue job/workflow FAILED | Enriches Glue failure events and sends formatted alert to SNS |

---

## gtfs-rt-polling

**Trigger:** `gtfs-rt-polling-schedule` EventBridge rule, `rate(1 minute)`

Fetches all 4 GTFS-RT feeds (2 agencies × 2 feed types) on every invocation and writes raw `.pb` protobufs to S3. Each invocation is a single fetch cycle — no internal polling loop.

**Feeds collected:**

| Agency | Feed Types |
|---|---|
| King County Metro (agency_id=1) | `trip-updates`, `vehicle-positions` |
| Sound Transit (agency_id=40) | `trip-updates`, `vehicle-positions` |

**S3 output path:** `s3://seattle-transit-raw/gtfs-rt/{agency}/{feed-type}/{YYYY/MM/DD/HHmmss}.pb`

**Environment variables:**
- `S3_BUCKET` — target bucket (default: `seattle-transit-raw`)
- `OBA_API_KEY` — OneBusAway API key (injected at deploy time, never hardcoded)

**Runtime:** Python 3.14, 128 MB, 30s timeout

> **Note:** `glue/jobs/glue_rt_polling.py` is an older Glue-based implementation of the same polling logic — it is **not in production** and has no active trigger. The Lambda is the sole active poller. Both write to the same S3 key format so they must not run simultaneously.

---

## gtfs-pipeline-notification

**Trigger:** `gtfs-static-pipeline-complete` EventBridge rule — fires when the `gtfs-static-pipeline` Glue workflow reaches `SUCCEEDED` state

Reads pipeline run metadata from DynamoDB (`seattle-transit-pipeline` table) and sends a formatted daily summary to SNS, including:
- Pipeline run status and timestamp
- Feed version (new vs. unchanged, SHA-256 hash)
- GTFS validation results (tables passed/failed)
- Fallback events (if stale data was used)

**Environment variables:**
- `SNS_TOPIC_ARN` — ARN of `transit-daily-digest` SNS topic

**Runtime:** Python 3.12, 128 MB, 30s timeout

---

## failure-notifier

**Triggers:**
- `transit-glue-job-failure` — any Glue job enters `FAILED` state
- `transit-glue-workflow-failure` — any Glue workflow enters `FAILED` or `STOPPED` state

Receives EventBridge events, calls Glue API to fetch the error message and run metadata, then publishes a formatted alert to the `transit-failure-alerts` SNS topic. Includes a direct CloudWatch Logs link for the failed run.

CloudWatch Alarms for Lambda function errors also route directly to the same SNS topic (bypassing this function).

**Environment variables:**
- `SNS_TOPIC_ARN` — ARN of `transit-failure-alerts` SNS topic
- `REGION` — AWS region (default: `us-west-2`)

**Runtime:** Python 3.12, 128 MB, 30s timeout

---

## Deploy

All three Lambda functions are deployed by `deploy/deploy_notifications.sh`. Each function has a `config.json` with runtime settings.

```bash
bash deploy/deploy_notifications.sh        # deploy all three functions + EventBridge rules + alarms
bash deploy/deploy_notifications.sh --dry-run
```

Deploy order within the script:
- **Step 3** — `transit-failure-notifier` (code + config + IAM role)
- **Step 3b** — `gtfs-rt-polling` (code + config)
- **Step 3c** — `gtfs-pipeline-notification` (code + config)

---

## Related

- [`eventbridge/README.md`](../eventbridge/README.md) — EventBridge rules that trigger these functions
- [`deploy/deploy_notifications.sh`](../deploy/deploy_notifications.sh) — deploy script for failure-notifier
- [`glue/README.md`](../glue/README.md) — Glue jobs monitored by failure-notifier
