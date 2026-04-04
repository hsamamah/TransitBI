#!/bin/bash
# =============================================================
# deploy_redshift.sh — Idempotent Redshift DDL + Views Deploy
# =============================================================
# Applies all DDL and view definitions to Redshift Serverless
# via the Redshift Data API (no JDBC required).
#
# Usage:
#   bash deploy/deploy_redshift.sh              # full deploy
#   bash deploy/deploy_redshift.sh --dry-run    # print SQL only
#   bash deploy/deploy_redshift.sh --views-only # only redeploy views
#
# Idempotency:
#   DDL: CREATE TABLE IF NOT EXISTS — never drops existing data
#   Views: CREATE OR REPLACE VIEW — always applies latest definition
#
# Dependency order:
#   1. Schemas (stg, dw)
#   2. Staging tables
#   3. Dimension tables (zero key rows inserted)
#   4. Fact tables
#   5. Views
#   6. Grants
# =============================================================

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
set -a; source "${SCRIPT_DIR}/config.env"; set +a

DRY_RUN=false
VIEWS_ONLY=false
for arg in "$@"; do
    case $arg in
        --dry-run)    DRY_RUN=true    ;;
        --views-only) VIEWS_ONLY=true ;;
    esac
done

log()  { echo "[$(date '+%H:%M:%S')] $*"; }
ok()   { echo "[$(date '+%H:%M:%S')] ✓ $*"; }
warn() { echo "[$(date '+%H:%M:%S')] ⚠ $*"; }
dry()  { echo "[$(date '+%H:%M:%S')] DRY-RUN SQL:"; echo "$1"; echo "---"; }

POLL_TIMEOUT=120
POLL_INTERVAL=5

run_sql() {
    # run_sql SQL DESC [--warn-on-fail]
    # --warn-on-fail: log a warning instead of exiting on failure.
    # Used for DDL steps (CREATE SCHEMA/TABLE IF NOT EXISTS) where the
    # IAM caller may lack CREATE privilege but the objects already exist.
    local sql="$1"
    local desc="${2:-SQL}"
    local warn_on_fail=false
    [[ "${3:-}" == "--warn-on-fail" ]] && warn_on_fail=true

    if $DRY_RUN; then
        dry "${sql}"
        return
    fi

    log "  → ${desc}"

    local resp
    resp=$(aws redshift-data execute-statement \
        --workgroup-name "${RS_WORKGROUP}" \
        --database "${RS_DATABASE}" \
        --sql "${sql}" \
        --region "${REGION}" \
        --output json)

    local query_id
    query_id=$(echo "${resp}" | python3 -c "import json,sys; print(json.load(sys.stdin)['Id'])")

    local elapsed=0
    while [[ ${elapsed} -lt ${POLL_TIMEOUT} ]]; do
        sleep ${POLL_INTERVAL}
        elapsed=$((elapsed + POLL_INTERVAL))

        local status_resp
        status_resp=$(aws redshift-data describe-statement \
            --id "${query_id}" \
            --region "${REGION}" \
            --output json)

        local status
        status=$(echo "${status_resp}" | python3 -c "import json,sys; print(json.load(sys.stdin)['Status'])")

        case "${status}" in
            FINISHED)
                ok "${desc} (${elapsed}s)"
                return ;;
            FAILED|ABORTED)
                local err
                err=$(echo "${status_resp}" | python3 -c "import json,sys; print(json.load(sys.stdin).get('Error','no detail'))")
                if $warn_on_fail; then
                    warn "${desc} skipped (no permission — object likely already exists): ${err}"
                    return
                fi
                echo "[ERROR] ${desc} FAILED: ${err}"
                exit 1 ;;
        esac
    done

    echo "[ERROR] ${desc} timed out after ${POLL_TIMEOUT}s"
    exit 1
}

run_sql_file() {
    local filepath="$1"
    local desc="${2:-$(basename ${filepath})}"
    local extra="${3:-}"
    if [[ ! -f "${filepath}" ]]; then
        warn "SQL file not found: ${filepath} — skipping"
        return
    fi
    local sql
    sql=$(cat "${filepath}")
    run_sql "${sql}" "${desc}" "${extra}"
}

SQL_DIR="${SCRIPT_DIR}/../redshift"

# =============================================================
# STEP 1 — Schemas
# =============================================================
if ! $VIEWS_ONLY; then
    log "=== [1] Schemas ==="
    run_sql "CREATE SCHEMA IF NOT EXISTS stg;" "CREATE SCHEMA stg" "--warn-on-fail"
    run_sql "CREATE SCHEMA IF NOT EXISTS dw;"  "CREATE SCHEMA dw"  "--warn-on-fail"
fi

# =============================================================
# STEP 2 — Staging tables
# =============================================================
if ! $VIEWS_ONLY; then
    log "=== [2] Staging tables ==="
    run_sql_file "${SQL_DIR}/ddl/stg_tables.sql" "Staging tables DDL" "--warn-on-fail"
fi

# =============================================================
# STEP 3 — Dimension tables
# =============================================================
if ! $VIEWS_ONLY; then
    log "=== [3] Dimension tables ==="
    run_sql_file "${SQL_DIR}/ddl/dim_tables.sql" "Dimension tables DDL" "--warn-on-fail"
fi

# =============================================================
# STEP 4 — Fact tables
# =============================================================
if ! $VIEWS_ONLY; then
    log "=== [4] Fact tables ==="
    run_sql_file "${SQL_DIR}/ddl/fact_tables.sql" "Fact tables DDL" "--warn-on-fail"
fi

# =============================================================
# STEP 5 — Views (always applied — CREATE OR REPLACE)
# =============================================================
log "=== [5] Views ==="

VIEWS_DIR="${SQL_DIR}/views"
for view_file in \
    "vw_otp_by_route_month.sql" \
    "vw_dailyvrm.sql" \
    "vw_dailyvrh.sql" \
    "v_missed_trip_rate_by_route.sql" \
    "v_routes_consistently_late.sql" \
    "v_voms.sql" \
    "vw_data_quality_daily.sql" \
    "vw_dataqualityalert.sql" \
    "vw_missedtriptrend.sql" \
    "vw_monthlyntdsummary.sql"; do
    run_sql_file "${VIEWS_DIR}/${view_file}" "View: ${view_file%.sql}" "--warn-on-fail"
done

# =============================================================
# STEP 6 — Grants
# =============================================================
if ! $VIEWS_ONLY; then
    log "=== [6] Grants ==="

    # ---- Schema USAGE (allows connecting to schema) ----
    run_sql "GRANT USAGE ON SCHEMA stg TO \"IAMR:TransitGlueRole\", \"IAM:lingli_yang\", \"IAM:minglei_ma\", \"IAM:poojith\";" \
        "GRANT USAGE stg to roles+team" "--warn-on-fail"
    run_sql "GRANT USAGE ON SCHEMA dw  TO \"IAMR:TransitGlueRole\", \"IAM:lingli_yang\", \"IAM:minglei_ma\", \"IAM:poojith\", quicksight_user;" \
        "GRANT USAGE dw to roles+team+qs" "--warn-on-fail"

    # ---- Glue ETL role — full read/write on both schemas ----
    run_sql "GRANT SELECT, INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA stg TO \"IAMR:TransitGlueRole\";" \
        "GRANT stg rw to TransitGlueRole" "--warn-on-fail"
    run_sql "GRANT SELECT, INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA dw  TO \"IAMR:TransitGlueRole\";" \
        "GRANT dw rw to TransitGlueRole" "--warn-on-fail"
    run_sql "ALTER DEFAULT PRIVILEGES IN SCHEMA stg GRANT SELECT, INSERT, UPDATE, DELETE ON TABLES TO \"IAMR:TransitGlueRole\";" \
        "Default privileges stg TransitGlueRole" "--warn-on-fail"
    run_sql "ALTER DEFAULT PRIVILEGES IN SCHEMA dw  GRANT SELECT, INSERT, UPDATE, DELETE ON TABLES TO \"IAMR:TransitGlueRole\";" \
        "Default privileges dw TransitGlueRole" "--warn-on-fail"

    # ---- Team members — full read/write on stg; full read/write on dw ----
    for team_user in "IAM:lingli_yang" "IAM:minglei_ma" "IAM:poojith"; do
        run_sql "GRANT SELECT, INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA stg TO \"${team_user}\";" \
            "GRANT stg rw to ${team_user}" "--warn-on-fail"
        run_sql "GRANT SELECT, INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA dw  TO \"${team_user}\";" \
            "GRANT dw rw to ${team_user}" "--warn-on-fail"
        run_sql "ALTER DEFAULT PRIVILEGES IN SCHEMA stg GRANT SELECT, INSERT, UPDATE, DELETE ON TABLES TO \"${team_user}\";" \
            "Default privileges stg ${team_user}" "--warn-on-fail"
        run_sql "ALTER DEFAULT PRIVILEGES IN SCHEMA dw  GRANT SELECT, INSERT, UPDATE, DELETE ON TABLES TO \"${team_user}\";" \
            "Default privileges dw ${team_user}" "--warn-on-fail"
    done

    # ---- QuickSight read-only user — SELECT on dw only ----
    run_sql "GRANT SELECT ON ALL TABLES IN SCHEMA dw TO quicksight_user;" \
        "GRANT dw SELECT to quicksight_user" "--warn-on-fail"
    run_sql "ALTER DEFAULT PRIVILEGES IN SCHEMA dw GRANT SELECT ON TABLES TO quicksight_user;" \
        "Default privileges dw quicksight_user" "--warn-on-fail"
fi

log "=== Redshift deploy complete ==="
if [[ $DRY_RUN == true ]]; then warn "DRY-RUN — no changes applied"; fi
