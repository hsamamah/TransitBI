# eventbridge/
EventBridge rule definitions for the Seattle Transit DW — stored as JSON for source control and redeployment.

---

## Rules

| File | Rule Name | State | Trigger |
|---|---|---|---|
| `gtfs-rt-polling-schedule.json` | `gtfs-rt-polling-schedule` | ENABLED | `rate(1 minute)` → `gtfs-rt-polling` Lambda |
| `glue-job-failure-rule.json` | `transit-glue-job-failure` | ENABLED | Glue Job State Change `FAILED`, `TIMEOUT`, `ERROR` → `transit-failure-notifier` Lambda |
| `glue-workflow-failure-rule.json` | `transit-glue-workflow-failure` | ENABLED | Glue Workflow Run Status `FAILED`, `STOPPED`, `TIMEOUT`, `ERROR` → `transit-failure-notifier` Lambda |

---

## Rule Details

### `gtfs-rt-polling-schedule`
Fires every minute, targeting the `gtfs-rt-polling` Lambda. Each Lambda invocation fetches all 4 GTFS-RT feeds (King County Metro + Sound Transit, trip-updates + vehicle-positions) and writes raw `.pb` files to `s3://seattle-transit-raw/`.

### `transit-glue-job-failure`
Matches any Glue job entering `FAILED`, `TIMEOUT`, or `ERROR` state across the account. Routes to `transit-failure-notifier` Lambda, which enriches the event with the error message and publishes a formatted alert to the `transit-failure-alerts` SNS topic.

### `transit-glue-workflow-failure`
Matches any Glue workflow run entering `FAILED`, `STOPPED`, `TIMEOUT`, or `ERROR` state. Same target and alert path as above.

---

## Deploy

These rules are deployed by `deploy/deploy_notifications.sh`:

```bash
bash deploy/deploy_notifications.sh        # create/update all rules + Lambda + SNS
bash deploy/deploy_notifications.sh --dry-run
```

The JSON files here are the source of truth — if a rule drifts in the AWS console, re-running the deploy script will reconcile it.

---

## Related

- [`lambda/README.md`](../lambda/README.md) — Lambda functions targeted by these rules
- [`deploy/deploy_notifications.sh`](../deploy/deploy_notifications.sh) — deploy script
