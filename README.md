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
│   └── lib/                # Shared Python libraries
├── redshift/
│   ├── ddl/                # CREATE TABLE statements
│   └── views/              # BI layer SQL views
├── iam/                    # IAM role trust + policy JSON
├── eventbridge/            # EventBridge rule definitions
└── deploy/
    ├── config.env          # Centralized project config (no secrets)
    ├── deploy_all.sh       # Master deploy — runs all scripts in order
    ├── deploy_iam.sh       # IAM roles + policies
    ├── deploy_redshift.sh  # Redshift DDL + views
    └── deploy_glue.sh      # Glue jobs + workflows + triggers
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
bash deploy/deploy_glue.sh --upload-only   # upload scripts to S3 only
bash deploy/deploy_redshift.sh --views-only # redeploy views only
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
| Redshift Serverless | workgroup: team / namespace: transit | Data warehouse |
| S3 | seattle-transit-raw/staging/processed | Data lake |
| DynamoDB | seattle-transit-pipeline | Pipeline parameter store |
| EventBridge | gtfs-rt-polling-schedule | Polling trigger |
| QuickSight | 6 datasets | BI dashboards |

---

## Configuration

All environment-specific values live in `deploy/config.env`.
No secrets — AWS credentials come from CLI profile or IAM role at runtime.

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

**BI views:** vw_otp_by_route_month · vw_dailyvrm · vw_dailyvrh ·
v_missed_trip_rate_by_route · v_routes_consistently_late · v_voms
