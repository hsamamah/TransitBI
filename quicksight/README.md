# quicksight/
Source-controlled QuickSight assets for the Seattle Transit BI project — data source, datasets, analyses, and dashboards, managed via `deploy/deploy_quicksight.sh`.

---

## How It Works

Assets are kept in three states:

1. **AWS** — live resources in QuickSight (us-west-2, account `805699509606`)
2. **JSON files here** — exported snapshots committed to git; the deploy script uses these to create or update AWS resources
3. **Placeholder files** (`*.placeholder.json`) — analyses with unresolved duplicates in AWS; these are skipped during deploy until resolved

The deploy script (`deploy/deploy_quicksight.sh`) is the single entry point for both deploying to AWS and exporting from it.

---

## Directory Structure

```
quicksight/
├── data-sources/
│   └── seattle-transit-dw.json          ← Redshift Serverless via VPC connection
├── datasets/
│   ├── v_missed_trip_rate_by_route.json ← missed trip rate per route (SPICE)
│   ├── v_routes_consistently_late.json  ← routes consistently late (SPICE)
│   ├── v_voms.json                      ← vehicles operated in max service (SPICE)
│   ├── vw_dailyvrh.json                 ← daily vehicle revenue hours (SPICE)
│   ├── vw_dailyvrm.json                 ← daily vehicle revenue miles (SPICE)
│   └── vw_otp_by_route_month.json       ← on-time performance by route/month (SPICE)
├── analyses/
│   ├── 397953c7-…json                   ← vw_dailyvrh analysis
│   ├── 42ebbed4-…json                   ← daily_otp_summary analysis
│   ├── 6d767618-…json                   ← facttrip analysis
│   ├── a1b2c3d4-voms-…json              ← VOMS — Vehicles Operated in Maximum Service
│   └── *.placeholder.json               ← duplicates pending resolution (see below)
├── dashboards/
│   └── 18f88e3f-…json                   ← Seattle Transit RT Pipeline — Coverage Dashboard
└── create_voms_analysis.py              ← programmatic VOMS analysis builder
```

---

## Deploy

**Deploy steps (in order):**

| Step | Action |
|------|--------|
| 0 | Look up VPC connection by name (`QS_VPC_CONNECTION_NAME`); create if absent and wait until `AVAILABLE` |
| 1 | Upsert Redshift data source |
| 2 | Upsert all 6 SPICE datasets |
| 3 | Trigger SPICE refresh for all 6 datasets |
| 4 | Create shared folder + add all assets as members + grant team permissions |
| 5 | Deploy analyses from `quicksight/analyses/*.json` |
| 6 | Deploy dashboards from `quicksight/dashboards/*.json` |

```bash
# Full deploy — upserts data source, all 6 datasets, analyses, dashboards; triggers SPICE refresh
bash deploy/deploy_quicksight.sh

# Dry run — print actions without executing
bash deploy/deploy_quicksight.sh --dry-run

# SPICE refresh only
bash deploy/deploy_quicksight.sh --refresh-only

# Export current AWS state → quicksight/ JSON files
bash deploy/deploy_quicksight.sh --export
```

**After exporting**, commit the updated JSON files:

```bash
bash deploy/deploy_quicksight.sh --export
git add quicksight/
git commit -m "chore(quicksight): export updated analyses/dashboards"
```

---

## Analyses

| File | Name | Status |
|---|---|---|
| `a1b2c3d4-voms-…json` | VOMS — Vehicles Operated in Maximum Service | ✓ deployed |
| `397953c7-…json` | vw_dailyvrh analysis | ✓ deployed |
| `42ebbed4-…json` | daily_otp_summary | ✓ deployed |
| `6d767618-…json` | facttrip analysis | ✓ deployed |
| `otp_by_route.placeholder.json` | OTP by Route | ⚠ duplicate — resolve first |
| `v_missed_trip_rate_by_route.placeholder.json` | v_missed_trip_rate_by_route analysis | ⚠ duplicate — resolve first |
| `v_voms.placeholder.json` | v_voms analysis | ⚠ duplicate — resolve first |
| `vw_dailyvrm.placeholder.json` | vw_dailyvrm analysis | ⚠ duplicate — resolve first |
| `vw_otp_by_route_month.placeholder.json` | vw_otp_by_route_month analysis | ⚠ duplicate — resolve first |

### Resolving placeholders

1. Open the QuickSight console, delete the stale duplicate analysis
2. Re-export: `bash deploy/deploy_quicksight.sh --export`
3. The placeholder file is replaced by a real `<analysis-id>.json`
4. Delete the placeholder and commit

---

## VOMS Analysis — Programmatic Build

`create_voms_analysis.py` builds the VOMS analysis directly via the QuickSight API (boto3) and adds it to the shared folder. Use this when you need to recreate the analysis definition from scratch rather than restoring from a JSON export.

```bash
python3 quicksight/create_voms_analysis.py
```

**Visuals on the VOMS Overview sheet:**

| # | Type | Title |
|---|---|---|
| 1 | KPI | Monthly VOMS |
| 2 | KPI | NTD Period VOMS |
| 3 | Line chart | Monthly VOMS by Agency |
| 4 | Horizontal bar | VOMS by Mode |
| 5 | Vertical bar | VOMS by Peak Period |
| 6 | Table | VOMS Detail — Agency / Mode / Month |

Dataset: `v_voms` (`f866df43-50a4-492d-92f1-14901dee795d`)

---

## Data Source

**`seattle-transit-dw`** — Redshift Serverless (`team.805699509606.us-west-2.redshift-serverless.amazonaws.com:5439/dev`) connected via VPC. All six datasets point to views in the `dw` schema of this data source.

---

## Shared Folder

All deployed analyses and dashboards are placed in the **Seattle Transit DW** shared folder (`240636fa-ade1-4f5a-9929-67acda51d579`). The deploy script grants team access at the folder level.
