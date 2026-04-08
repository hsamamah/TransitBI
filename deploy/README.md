# deploy/
Idempotent deploy scripts for all Seattle Transit DW infrastructure — run in any order, safe to re-run repeatedly.

---

## Quick Start

```bash
# Full deploy (IAM → Redshift → Glue → QuickSight → Notifications)
bash deploy/deploy_all.sh

# Dry-run (print what would change, touch nothing)
bash deploy/deploy_all.sh --dry-run

# Glue only (skip IAM/Redshift/QuickSight — fastest for code changes)
bash deploy/deploy_all.sh --glue-only
```

---

## Scripts

| Script | What it deploys | Key flags |
|---|---|---|
| `deploy_all.sh` | Master script — runs all others in dependency order | `--dry-run`, `--glue-only` |
| `deploy_iam.sh` | IAM roles, managed policies, groups, user memberships | `--dry-run` |
| `deploy_redshift.sh` | DDL schemas/tables + views via Redshift Data API | `--dry-run`, `--views-only` |
| `deploy_glue.sh` | Glue jobs, workflows, triggers, Glue crawler; uploads scripts to S3 | `--dry-run`, `--upload-only` |
| `deploy_quicksight.sh` | QuickSight data sources, datasets, analyses, folder membership | `--dry-run`, `--refresh-only` |
| `deploy_notifications.sh` | SNS topic, failure-notifier Lambda, EventBridge rules, CW alarms | `--dry-run` |

### Dependency order

```
deploy_iam.sh           ← IAM roles referenced by all other resources
  └─► deploy_redshift.sh    ← tables/views referenced by Glue + QuickSight
        └─► deploy_glue.sh          ← jobs + workflows
              └─► deploy_quicksight.sh    ← datasets + analyses
                    └─► deploy_notifications.sh  ← Lambda + EventBridge rules
```

### Idempotency guarantees

- **IAM**: roles/policies use `create-if-not-exists` + `put` (always overwrites policy body); user group memberships are skipped if already present
- **Redshift DDL**: `CREATE TABLE IF NOT EXISTS` — never drops data; `CREATE OR REPLACE VIEW` — always latest
- **Glue**: get → update-if-exists | create-if-not — safe to run on live workflows mid-day
- **QuickSight**: upsert pattern for data sources and datasets; analyses use delete-and-recreate if sheet definition is empty (Definition API limitation)
- **Notifications**: SNS topic creation is idempotent; Lambda uses `update-function-code` if function exists

---

## Configuration

All scripts source `config.env` at startup. This file contains all non-secret configuration:

```bash
source deploy/config.env
```

| Variable | Example | Purpose |
|---|---|---|
| `REGION` | `us-west-2` | AWS region for all resources |
| `ACCOUNT` | `805699509606` | AWS account ID |
| `S3_RAW` | `seattle-transit-raw` | Raw GTFS-RT protobuf storage |
| `S3_STAGING` | `seattle-transit-staging` | Parsed CSVs + Glue scripts |
| `S3_PROCESSED` | `seattle-transit-processed` | Final processed outputs |
| `SCRIPTS` | `s3://.../glue-scripts/v2` | Glue script S3 prefix |
| `GLUE_ROLE` | `arn:aws:iam::…:role/TransitGlueRole` | IAM role for all Glue jobs |
| `REDSHIFT_COPY_ROLE` | `arn:aws:iam::…:role/RedshiftS3CopyRole` | IAM role for Redshift COPY |
| `RS_WORKGROUP` | `team` | Redshift Serverless workgroup |
| `RS_DATABASE` | `dev` | Redshift database name |
| `FAILURE_SNS_ARN` | `arn:aws:sns:…:transit-failure-alerts` | SNS topic for Glue/Lambda failure alerts |
| `DIGEST_SNS_ARN` | `arn:aws:sns:…:transit-daily-digest` | SNS topic for daily pipeline digest |
| `FAILURE_NOTIFIER_LAMBDA` | `transit-failure-notifier` | Lambda function name (used by IAM + notifications) |
| `QS_VPC` | `vpc-0a11677a4786d95db` | VPC ID for QuickSight → Redshift connection |
| `QS_SECURITY_GROUP` | `sg-0f60cd6f1db33cff9` | Security group for Redshift workgroup + QS VPC connection |
| `QS_SUBNETS` | `subnet-… subnet-…` | Space-separated subnet IDs (all AZs) — workgroup + VPC connection |
| `QS_VPC_CONNECTION_NAME` | `seattle-transit-vpc` | Stable name for QuickSight VPC connection (looked up at deploy time) |
| `QS_SERVICE_ROLE_ARN` | `arn:aws:iam::…:role/aws-quicksight-service-role-v0` | QuickSight service role (created when QS first enabled) |

**Account-specific vars:** `QS_VPC`, `QS_SECURITY_GROUP`, `QS_SUBNETS` are tied to the VPC in this AWS account. Update all three when deploying to a new account — lookup commands are in the comments in `config.env`.

**Secrets** (API keys, passwords) are never in `config.env` — they are loaded from `.env` (git-ignored) at deploy time. Copy `.env.example` to `.env` and fill in `DIGEST_EMAIL` and `OBA_API_KEY`.

---

## Uploading a single Glue script without a full deploy

```bash
source deploy/config.env
aws s3 cp glue/jobs/<script>.py "${SCRIPTS}/<script>.py"
```

The `--upload-only` flag on `deploy_glue.sh` does the same for all scripts at once.

---

## Related

- [`glue/README.md`](../glue/README.md) — Glue job descriptions and pipeline structure
- [`quicksight/README.md`](../quicksight/README.md) — QuickSight asset inventory and deploy workflow
- [`iam/README.md`](../iam/README.md) — IAM role and policy definitions
- [`redshift/`](../redshift/) — DDL and view SQL files applied by `deploy_redshift.sh`
- [`lambda/`](../lambda/) — Lambda function source deployed by `deploy_notifications.sh`
