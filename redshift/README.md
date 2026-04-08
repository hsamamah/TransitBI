# redshift/
Redshift Serverless DDL and view definitions for the Seattle Transit data warehouse.

---

## Structure

```
redshift/
├── ddl/
│   ├── stg_tables.sql       ← staging schema: raw GTFS static + RT data
│   ├── dim_tables.sql       ← dimension tables (DimDate, DimAgency, DimStop, …)
│   ├── dw_tables.sql        ← fact tables (FactTrip, FactStop, FactServiceDay)
│   ├── fact_tables.sql      ← additional fact table DDL
│   ├── grant_deploy_user.sql ← GRANT statements for the deploy IAM user
└── views/
    ├── v_voms.sql                  ← Peak vehicle count by agency/month/NTD period
    ├── v_missed_trip_rate_by_route.sql
    ├── v_routes_consistently_late.sql
    ├── vw_dailyvrh.sql             ← Daily VRH (reported + estimated) by agency
    ├── vw_dailyvrm.sql             ← Daily VRM by agency
    ├── vw_missedtriptrend.sql      ← Missed trip trend over time
    ├── vw_monthlyntdsummary.sql    ← Monthly NTD summary
    ├── vw_otp_by_route_month.sql   ← On-time performance by route/month
    ├── vw_dataqualityalert.sql     ← Data quality alerts
    └── vw_data_quality_daily.sql   ← Daily data quality metrics
```

---

## Schema Overview

### `stg` — Staging

Holds raw imported data loaded by Glue jobs. Tables are truncated and reloaded per run.

| Table | Loaded by | Content |
|---|---|---|
| `stg.agency`, `stg.routes`, `stg.stops`, `stg.trips`, `stg.stop_times`, `stg.calendar`, `stg.calendar_dates`, `stg.shapes`, `stg.transfers` | `gtfs-static-redshift-load` | GTFS static schedule CSVs |
| `stg.rt_stop_time_updates` | `gtfs-rt-parse-load-glue` | Parsed trip update protobufs |
| `stg.rt_vehicle_positions` | `gtfs-rt-parse-load-glue` | Parsed vehicle position protobufs |

### `dw` — Data Warehouse

Star schema with dimension and fact tables. Never truncated — fact jobs use DELETE + INSERT per date for idempotency.

**Dimensions:** `DimAgency`, `DimDate`, `DimTime`, `DimStop`, `DimRoute`, `DimTrip`, `DimFeedVersion`, `DimCalendarException`

**Facts:**

| Table | Grain | Loaded by |
|---|---|---|
| `FactTrip` | 1 row per trip per service date | `facttrip-skeleton-and-merge-load` |
| `FactStop` | 1 row per stop visit per trip per service date | `factstop-skeleton-and-merge-load` |
| `FactServiceDay` | 1 row per agency per service date | `factserviceday-load` |

---

## Views

All views live in the `dw` schema and are the primary query surface for QuickSight and ad-hoc analysis.

| View | Purpose |
|---|---|
| `v_voms` | Peak vehicle count (VOMS) by agency, month, and NTD period — filters special event days |
| `v_missed_trip_rate_by_route` | Missed trip rate aggregated by route |
| `v_routes_consistently_late` | Routes with persistent late arrivals |
| `vw_dailyvrh` | Daily vehicle revenue hours (reported + estimated) |
| `vw_dailyvrm` | Daily vehicle revenue miles |
| `vw_missedtriptrend` | Missed trip count trend over time |
| `vw_monthlyntdsummary` | Monthly NTD reporting summary |
| `vw_otp_by_route_month` | On-time performance by route and month |
| `vw_dataqualityalert` | Rows flagged with data quality issues |
| `vw_data_quality_daily` | Daily data quality metrics (official vs. unmatched trips) |

---

## Deploy

DDL and views are applied via `deploy/deploy_redshift.sh` using the Redshift Data API (no JDBC required):

```bash
bash deploy/deploy_redshift.sh              # apply all DDL + views
bash deploy/deploy_redshift.sh --views-only # redeploy views only (fast)
bash deploy/deploy_redshift.sh --dry-run    # print SQL without executing
```

**Idempotency:**
- DDL uses `CREATE TABLE IF NOT EXISTS` — never drops or truncates existing data
- Views use `CREATE OR REPLACE VIEW` — always applies the latest definition

**Connection:** Redshift Serverless workgroup `team`, database `dev`, via Redshift Data API (polls until complete).

---

## Related

- [`glue/README.md`](../glue/README.md) — Glue jobs that load data into `stg` and `dw`
- [`quicksight/README.md`](../quicksight/README.md) — QuickSight datasets built on these views
- [`deploy/deploy_redshift.sh`](../deploy/deploy_redshift.sh) — deploy script
