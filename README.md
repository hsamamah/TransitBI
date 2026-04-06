# Seattle Transit Data Warehouse

Production-grade transit data warehouse for the Puget Sound region.
Ingests GTFS static and real-time feeds from Sound Transit and King County Metro
into an AWS-hosted dimensional model, ending in QuickSight BI dashboards.

**Course:** DAMG 7370 — Designing Data Architecture for Business Intelligence

---

## Repository Structure

```
seattle-transit-dw/
├── glue/
│   ├── jobs/               # All Glue ETL scripts
│   └── lib/                # Shared Python libraries (pipeline_param_reader_v2)
├── lambda/
│   ├── gtfs-rt-polling/    # RT feed poller (EventBridge → S3)
│   └── gtfs-pipeline-notification/ # SNS/email alerting
├── redshift/
│   ├── ddl/                # CREATE TABLE statements (stg, dim, fact)
│   └── views/              # BI layer SQL views (10 views)
├── quicksight/
│   ├── data-sources/       # Redshift data source config
│   └── datasets/           # 6 SPICE dataset definitions (one per BI view)
├── iam/
│   ├── groups/             # TransitDWTeam group definition
│   ├── policies/           # 3 custom managed policies
│   ├── roles/              # TransitGlueRole, RedshiftS3CopyRole trust + inline policies
│   └── users/              # Per-user policy snapshots (hani-admin, team members)
├── eventbridge/            # EventBridge rule definitions
├── .env.example            # Secret variable template (copy to .env)
└── deploy/
    ├── config.env          # Centralized project config (no secrets)
    ├── deploy_all.sh       # Master deploy — runs all scripts in order
    ├── deploy_iam.sh       # IAM roles, managed policies, group, user memberships
    ├── deploy_redshift.sh  # Redshift DDL + views
    ├── deploy_glue.sh      # Glue jobs + workflows + triggers
    └── deploy_quicksight.sh # QuickSight data source, datasets, shared folder, SPICE refresh
```

---

## Prerequisites

- AWS CLI configured with sufficient permissions (`AdministratorAccess` or equivalent)
- Default region set to `us-west-2`
- Python 3 available (used inline in deploy scripts)

```bash
aws configure set region us-west-2
aws sts get-caller-identity   # verify credentials
```

---

## Deploy

### Full deploy (first time or full refresh)
```bash
bash deploy/deploy_all.sh
```

### Dry run (see what would change)
```bash
bash deploy/deploy_all.sh --dry-run
```

### Glue only (jobs + triggers, skip IAM + Redshift)
```bash
bash deploy/deploy_all.sh --glue-only
```

### Individual components
```bash
bash deploy/deploy_iam.sh
bash deploy/deploy_redshift.sh
bash deploy/deploy_glue.sh
bash deploy/deploy_quicksight.sh
bash deploy/deploy_glue.sh --upload-only        # upload scripts to S3 only
bash deploy/deploy_redshift.sh --views-only     # redeploy views only
bash deploy/deploy_quicksight.sh --refresh-only # trigger SPICE refresh only
```

---

## Known Issues / Manual Steps Required

### ⚠ Redshift DDL: permission denied for schema `dw` / `stg`

`deploy_redshift.sh` executes SQL via the Redshift Data API using the calling IAM identity (`IAM:hani-admin`). This IAM-mapped database user lacks `CREATE` privilege on the `dw` and `stg` schemas, so Steps 1–3 (schemas, staging tables, dimension tables) and Step 5 (views) warn and skip rather than fail hard.

**Impact:** On a fresh environment these objects will not be created. On re-deploy they already exist, so the warnings are harmless.

**Root cause:** Redshift Serverless Data API does not support `--db-user` to run as the superuser (`admin`) directly. The fix requires storing the Redshift admin password in Secrets Manager and switching the deploy script to use `--secret-arn`.

**Current state:** `GRANT CREATE ON DATABASE dev`, `GRANT CREATE ON SCHEMA dw/stg`, and `GRANT USAGE ON SCHEMA dw/stg` have all been applied. Active blocker: existing views in `dw` were originally created by `admin`, so `IAM:hani-admin` cannot replace them (`CREATE OR REPLACE VIEW` requires ownership).

**Resolution options (pick one):**

1. **One-time console fix** — In the Redshift Query Editor, connect as `admin` and run:
   ```sql
   ALTER TABLE dw.vw_otp_by_route_month          OWNER TO "IAM:hani-admin";
   ALTER TABLE dw.vw_dailyvrm                    OWNER TO "IAM:hani-admin";
   ALTER TABLE dw.vw_dailyvrh                    OWNER TO "IAM:hani-admin";
   ALTER TABLE dw.v_missed_trip_rate_by_route    OWNER TO "IAM:hani-admin";
   ALTER TABLE dw.v_routes_consistently_late     OWNER TO "IAM:hani-admin";
   ALTER TABLE dw.v_voms                         OWNER TO "IAM:hani-admin";
   ALTER TABLE dw.vw_data_quality_daily          OWNER TO "IAM:hani-admin";
   ALTER TABLE dw.vw_dataqualityalert            OWNER TO "IAM:hani-admin";
   ALTER TABLE dw.vw_missedtriptrend             OWNER TO "IAM:hani-admin";
   ALTER TABLE dw.vw_monthlyntdsummary           OWNER TO "IAM:hani-admin";
   ```
   This transfers ownership of all views so `IAM:hani-admin` can replace them via `CREATE OR REPLACE VIEW`. Note: `ALTER USER ... CREATEUSER` does not work for IAM-federated users (disabled passwords).

   After this, `deploy_redshift.sh --views-only` will apply all view definitions cleanly.

2. **Secrets Manager fix** — Store the Redshift admin password in Secrets Manager, add the secret ARN to `deploy/config.env` as `RS_ADMIN_SECRET_ARN`, and update `deploy_redshift.sh` to pass `--secret-arn "${RS_ADMIN_SECRET_ARN}"` instead of relying on IAM identity. **Admin password is currently unknown** — retrieve it from whoever set up the Redshift namespace, or reset it via:
   ```bash
   aws redshift-serverless update-namespace \
     --namespace-name transit \
     --admin-user-password <new-password> \
     --region us-west-2
   ```

---

## Pipeline Architecture

```
EventBridge (cron)
      │
      ├── 07:00 PST → gtfs-static-pipeline
      │     gtfs-static-ingestion
      │     → gtfs-static-crawler
      │     → gtfs-static-validation
      │     → gtfs-static-redshift-load
      │
      └── 08:00 PST → gtfs-rt-daily-pipeline
            gtfs-rt-parse-load-glue
            → transit-pipeline-inspector     (gap detection + DynamoDB params)
            → factstop-skeleton-and-merge-load
            → facttrip-skeleton-and-merge-load
            → factserviceday-load
```

---

## AWS Resources

| Service | Resource | Purpose |
|---------|----------|---------|
| Glue | 8 jobs, 2 workflows | ETL pipeline |
| Redshift Serverless | workgroup: `team` / namespace: `transit` / db: `dev` | Data warehouse |
| S3 | seattle-transit-raw/staging/processed | Data lake |
| DynamoDB | seattle-transit-pipeline | Pipeline parameter store |
| EventBridge | gtfs-rt-polling-schedule | Polling trigger |
| QuickSight | 1 data source, 6 SPICE datasets, 1 shared folder, 1 dashboard | BI dashboards |
| IAM | 2 roles, 3 managed policies, 1 group (TransitDWTeam), 4 users | Access control |

---

## QuickSight

**Shared folder:** "Seattle Transit DW" — all 6 SPICE datasets and the Redshift data source are members.

**Team access:** `lingli_yang`, `minglei_ma`, and `poojith` are registered as QuickSight users and have full contributor/admin permissions on the shared folder.

**Managed by `deploy_quicksight.sh`:**
1. Redshift data source (`seattle-transit-dw` via VPC connection)
2. 6 SPICE datasets (one per BI view in `dw` schema)
3. SPICE refresh (triggered in parallel for all 6 datasets)
4. Shared folder membership + team permissions

**Not managed by script (console only):** Analyses and dashboards — QuickSight analysis definitions are not CLI-portable.

---

## IAM

**Roles** (used by AWS services):
- `TransitGlueRole` — assumed by Glue; S3 read/write on all 3 buckets + DynamoDB pipeline table
- `RedshiftS3CopyRole` — assumed by Redshift; S3 read on staging bucket

**Group:** `TransitDWTeam` — `lingli_yang`, `minglei_ma`, `poojith` are members.

**Custom managed policies:**
- `RedshiftDataAPIPolicy` — Redshift Data API access
- `RedshiftDevPolicy` — Redshift developer permissions
- `RedshiftS3Copy` — Redshift → S3 copy permissions

**User creation is manual** — `deploy_iam.sh` manages group memberships and policies only. Console-side user creation, login profiles, MFA, and access keys are out of scope.

---

## Configuration

All environment-specific values live in `deploy/config.env`.
No secrets — AWS credentials come from CLI profile or IAM role at runtime.

### Secrets setup

Secrets are stored in `.env` (git-ignored). Before deploying Lambda functions:

```bash
cp .env.example .env
# edit .env and fill in real values
```

| Variable | Used by | Where to get it |
|----------|---------|-----------------|
| `OBA_API_KEY` | `gtfs-rt-polling` Lambda | OneBusAway API key from transitapp.onebusaway.org |

To change script version (e.g. after breaking changes):
1. Update `SCRIPT_VERSION` in `config.env`
2. Run `bash deploy/deploy_glue.sh`

---

## Adding a New Glue Job

1. Add script to `glue/jobs/`
2. Add `upsert_glue_job` call to `deploy/deploy_glue.sh`
3. Add trigger call if it belongs in a workflow
4. Run `bash deploy/deploy_glue.sh --dry-run` to verify
5. Run `bash deploy/deploy_glue.sh` to apply

---

## Schema

Dimensional model: `dw` schema in Redshift database `dev`

**Fact tables:** FactStop · FactTrip · FactServiceDay

**Dimensions:** DimRoute · DimStop · DimTrip · DimService · DimDate · DimTime ·
DimAgency · DimFeedVersion · DimDirection · DimShape · DimCalendarException

**BI views (SPICE-backed):** vw_otp_by_route_month · vw_dailyvrm · vw_dailyvrh ·
v_missed_trip_rate_by_route · v_routes_consistently_late · v_voms

**Additional views:** vw_data_quality_daily · vw_dataqualityalert · vw_missedtriptrend · vw_monthlyntdsummary
