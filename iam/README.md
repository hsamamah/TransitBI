# IAM — Seattle Transit DW

All IAM resources for the project. Managed by `deploy/deploy_iam.sh`.

---

## Directory Structure

```
iam/
├── roles/      # Service roles (assumed by Glue, Redshift)
├── groups/     # IAM groups + member lists
├── users/      # Per-user policy summary (no credentials)
└── policies/   # Customer-managed policy documents
```

---

## Roles

### TransitGlueRole
Assumed by all Glue ETL jobs.

**Trust:** `glue.amazonaws.com`

**Attached policies:**
- `AWSGlueServiceRole` (AWS managed)
- `AmazonS3FullAccess` (AWS managed)
- `CloudWatchFullAccess` / `CloudWatchFullAccessV2` (AWS managed)
- `IAMFullAccess` (AWS managed)
- `RedshiftDataAPIPolicy` (customer managed — see `policies/`)

**Inline policies:**
- `TransitDynamoDBAccess` → `roles/TransitGlueRole_policy_TransitDynamoDBAccess.json`
- `TransitPipelineDynamoDBAccess` → `roles/TransitGlueRole_policy_TransitPipelineDynamoDBAccess.json`

### RedshiftS3CopyRole
Assumed by Redshift Serverless for COPY from S3.

**Trust:** `redshift.amazonaws.com`

---

## Groups

### TransitDWTeam
Project team members: `lingli_yang`, `minglei_ma`, `poojith`

Grants: Redshift full access + Query Editor v2, Glue console, S3 transit buckets, DynamoDB read, CloudWatch read, Redshift Data API.

See `groups/TransitDWTeam.json` for full policy ARN list.

---

## Users

| User | Groups | Extra Policies | Role |
|------|--------|----------------|------|
| `hani-admin` | — | `AdministratorAccess` | Project owner |
| `lingli_yang` | TransitDWTeam | RedshiftS3Copy, Lambda, EventBridge, QuickSight | Team member |
| `minglei_ma` | TransitDWTeam | RedshiftS3Copy, Lambda, EventBridge, QuickSight | Team member |
| `poojith` | TransitDWTeam | RedshiftS3Copy, Lambda, EventBridge, QuickSight | Team member |

All 3 team members also have inline policy `QuickSightCreateAdmin`.

---

## Customer-Managed Policies

| Policy | Used by | Purpose |
|--------|---------|---------|
| `RedshiftDataAPIPolicy` | TransitGlueRole + TransitDWTeam | Redshift Data API, Glue job read, DynamoDB pipeline table |
| `RedshiftDevPolicy` | TransitDWTeam | Query Editor v2, S3 transit buckets, Redshift Serverless |
| `RedshiftS3Copy` | Team members (user-level) | S3 read/write transit buckets, Glue catalog read |

---

## What deploy_iam.sh Does NOT Manage

- User creation, login profiles, MFA, access keys — done manually in console
- QuickSight user registration — done through QuickSight console
