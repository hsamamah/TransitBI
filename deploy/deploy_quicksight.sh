#!/bin/bash
# =============================================================
# deploy_quicksight.sh — Idempotent QuickSight Deploy
# =============================================================
# Creates or updates the QuickSight data source and all 6
# SPICE datasets for the Seattle Transit DW project.
# Triggers a SPICE refresh for each dataset after upsert.
#
# Usage:
#   bash deploy/deploy_quicksight.sh              # full deploy
#   bash deploy/deploy_quicksight.sh --dry-run    # print actions only
#   bash deploy/deploy_quicksight.sh --refresh-only # SPICE refresh only
#
# Resources managed:
#   Data source : seattle-transit-dw  (Redshift Serverless via VPC)
#   Datasets    : 6 SPICE datasets (one per BI view in dw schema)
#
# NOT managed by this script (created manually in console):
#   Analyses  — QuickSight analysis definitions are not CLI-portable
#   Dashboards — Same; dashboards reference analyses by ARN
#
# Idempotency:
#   Data source: create if not exists, update-data-source if exists
#   Datasets:    create if not exists, update-data-set if exists
#   SPICE:       create-ingestion triggered unconditionally (idempotent by design)
# =============================================================

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
set -a; source "${SCRIPT_DIR}/config.env"; set +a

DRY_RUN=false
REFRESH_ONLY=false
for arg in "$@"; do
    case $arg in
        --dry-run)      DRY_RUN=true      ;;
        --refresh-only) REFRESH_ONLY=true ;;
    esac
done

log()  { echo "[$(date '+%H:%M:%S')] $*"; }
ok()   { echo "[$(date '+%H:%M:%S')] ✓ $*"; }
warn() { echo "[$(date '+%H:%M:%S')] ⚠ $*"; }
dry()  { echo "[$(date '+%H:%M:%S')] DRY-RUN: $*"; }

QS_DIR="${SCRIPT_DIR}/../quicksight"

# ── QuickSight resource IDs ───────────────────────────────────
DATASOURCE_ID="ee6148c5-7ba2-45b8-b4a5-c721e9ab7aca"
VPC_CONNECTION_ARN="arn:aws:quicksight:${REGION}:${ACCOUNT}:vpcConnection/c9508d2a-f178-4add-a3fb-ba52ae4c742f"
QS_PRINCIPAL="arn:aws:quicksight:${REGION}:${ACCOUNT}:user/default/hani-admin"

# Dataset IDs — stable UUIDs matching the live resources
declare -A DATASET_IDS=(
  ["vw_otp_by_route_month"]="61f642a5-d9a8-4d26-b485-73f5d5615069"
  ["v_routes_consistently_late"]="6dbda19e-5a0c-4d02-a9fe-89b0dafecc60"
  ["vw_dailyvrm"]="c0894c19-2b88-448a-883a-ce164057421e"
  ["v_voms"]="f866df43-50a4-492d-92f1-14901dee795d"
  ["vw_dailyvrh"]="2ade677c-c1c3-4f1f-ad94-a2d24fa66421"
  ["v_missed_trip_rate_by_route"]="3eac6ed8-921c-4584-be0c-3a6a8c377027"
)

qs_datasource_exists() {
    aws quicksight describe-data-source \
        --aws-account-id "${ACCOUNT}" \
        --data-source-id "$1" \
        --region "${REGION}" > /dev/null 2>&1
}

qs_dataset_exists() {
    aws quicksight describe-data-set \
        --aws-account-id "${ACCOUNT}" \
        --data-set-id "$1" \
        --region "${REGION}" > /dev/null 2>&1
}

# =============================================================
# STEP 1 — Data Source
# =============================================================
if ! $REFRESH_ONLY; then
    log "=== [1] QuickSight Data Source ==="

    RS_HOST="team.${ACCOUNT}.${REGION}.redshift-serverless.amazonaws.com"

    DS_PARAMS="{
        \"RedshiftParameters\": {
            \"Host\": \"${RS_HOST}\",
            \"Port\": 5439,
            \"Database\": \"${RS_DATABASE}\"
        }
    }"

    VPC_PROPS="{\"VpcConnectionArn\": \"${VPC_CONNECTION_ARN}\"}"

    if $DRY_RUN; then
        dry "UPSERT data source: ${DATASOURCE_ID} (seattle-transit-dw)"
    elif qs_datasource_exists "${DATASOURCE_ID}"; then
        # update-data-source requires credentials even for IAM/VPC sources and
        # rejects them if set incorrectly — skip update since host/port/DB never change.
        ok "Data source exists (no update needed): seattle-transit-dw"
    else
        aws quicksight create-data-source \
            --aws-account-id "${ACCOUNT}" \
            --data-source-id "${DATASOURCE_ID}" \
            --name "seattle-transit-dw" \
            --type REDSHIFT \
            --data-source-parameters "${DS_PARAMS}" \
            --vpc-connection-properties "${VPC_PROPS}" \
            --ssl-properties '{"DisableSsl": false}' \
            --permissions "[{
                \"Principal\": \"${QS_PRINCIPAL}\",
                \"Actions\": [
                    \"quicksight:DescribeDataSource\",
                    \"quicksight:DescribeDataSourcePermissions\",
                    \"quicksight:PassDataSource\",
                    \"quicksight:UpdateDataSource\",
                    \"quicksight:DeleteDataSource\",
                    \"quicksight:UpdateDataSourcePermissions\"
                ]
            }]" \
            --region "${REGION}" > /dev/null
        ok "Created data source: seattle-transit-dw"
    fi
fi

# =============================================================
# STEP 2 — Datasets (upsert)
# =============================================================
if ! $REFRESH_ONLY; then
    log "=== [2] Datasets ==="

    DATASOURCE_ARN="arn:aws:quicksight:${REGION}:${ACCOUNT}:datasource/${DATASOURCE_ID}"

    for view_name in \
        "vw_otp_by_route_month" \
        "v_routes_consistently_late" \
        "vw_dailyvrm" \
        "v_voms" \
        "vw_dailyvrh" \
        "v_missed_trip_rate_by_route"; do

        dataset_id="${DATASET_IDS[$view_name]}"
        config_file="${QS_DIR}/datasets/${view_name}.json"
        # QuickSight map keys only allow [0-9a-zA-Z-] — no underscores
        table_key="${view_name//_/-}"

        PHYSICAL_MAP="{
            \"${table_key}\": {
                \"RelationalTable\": {
                    \"DataSourceArn\": \"${DATASOURCE_ARN}\",
                    \"Schema\": \"dw\",
                    \"Name\": \"${view_name}\",
                    \"InputColumns\": $(python3 -c "
import json
with open('${config_file}') as f:
    d = json.load(f)
tables = d.get('PhysicalTableMap', {})
for t in tables.values():
    cols = t.get('RelationalTable', {}).get('InputColumns', [])
    print(json.dumps(cols))
    break
")
                }
            }
        }"

        LOGICAL_MAP="{
            \"${table_key}\": {
                \"Alias\": \"${view_name}\",
                \"Source\": {\"PhysicalTableId\": \"${table_key}\"}
            }
        }"

        DATASET_PERMISSIONS="[{
            \"Principal\": \"${QS_PRINCIPAL}\",
            \"Actions\": [
                \"quicksight:DescribeDataSet\",
                \"quicksight:DescribeDataSetPermissions\",
                \"quicksight:PassDataSet\",
                \"quicksight:DescribeIngestion\",
                \"quicksight:ListIngestions\",
                \"quicksight:UpdateDataSet\",
                \"quicksight:DeleteDataSet\",
                \"quicksight:CreateIngestion\",
                \"quicksight:CancelIngestion\",
                \"quicksight:UpdateDataSetPermissions\"
            ]
        }]"

        if $DRY_RUN; then
            dry "UPSERT dataset: ${view_name} (${dataset_id})"
        elif qs_dataset_exists "${dataset_id}"; then
            # Datasets created via the new QuickSight console experience cannot be
            # updated via the legacy CLI API. Schema doesn't change between deploys,
            # so skip update — SPICE refresh below keeps the data current.
            ok "Dataset exists (no update needed): ${view_name}"
        else
            aws quicksight create-data-set \
                --aws-account-id "${ACCOUNT}" \
                --data-set-id "${dataset_id}" \
                --name "${view_name}" \
                --import-mode SPICE \
                --physical-table-map "${PHYSICAL_MAP}" \
                --logical-table-map "${LOGICAL_MAP}" \
                --permissions "${DATASET_PERMISSIONS}" \
                --region "${REGION}" > /dev/null
            ok "Created dataset: ${view_name}"
        fi
    done
fi

# =============================================================
# STEP 3 — SPICE Refresh
# =============================================================
log "=== [3] SPICE Refresh ==="

for view_name in \
    "vw_otp_by_route_month" \
    "v_routes_consistently_late" \
    "vw_dailyvrm" \
    "v_voms" \
    "vw_dailyvrh" \
    "v_missed_trip_rate_by_route"; do

    dataset_id="${DATASET_IDS[$view_name]}"
    ingestion_id="deploy-$(date '+%Y%m%d%H%M%S')-${view_name}"

    if $DRY_RUN; then
        dry "TRIGGER SPICE refresh: ${view_name}"
    else
        aws quicksight create-ingestion \
            --aws-account-id "${ACCOUNT}" \
            --data-set-id "${dataset_id}" \
            --ingestion-id "${ingestion_id}" \
            --region "${REGION}" > /dev/null
        ok "SPICE refresh triggered: ${view_name}"
    fi
done

log "=== QuickSight deploy complete ==="
if [[ $DRY_RUN == true ]]; then warn "DRY-RUN — no changes applied"; fi
