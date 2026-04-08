#!/bin/bash
# =============================================================
# deploy_quicksight.sh — Idempotent QuickSight Deploy
# =============================================================
# Creates or updates the QuickSight data source and all 6
# SPICE datasets for the Seattle Transit DW project.
# Triggers a SPICE refresh for each dataset after upsert.
# Associates all assets with the shared folder and grants
# team members full folder access.
#
# Usage:
#   bash deploy/deploy_quicksight.sh              # full deploy
#   bash deploy/deploy_quicksight.sh --dry-run    # print actions only
#   bash deploy/deploy_quicksight.sh --refresh-only # SPICE refresh only
#   bash deploy/deploy_quicksight.sh --export     # export analyses + dashboard → quicksight/
#
# Resources managed:
#   Data source : seattle-transit-dw  (Redshift Serverless via VPC)
#   Datasets    : 6 SPICE datasets (one per BI view in dw schema)
#   Folder      : Seattle Transit DW (shared folder, assets + team access)
#   Analyses    : exported to quicksight/analyses/<id>.json; deployed on full run
#   Dashboards  : exported to quicksight/dashboards/<id>.json; deployed on full run
#
# Analyses / dashboards with duplicate names in AWS are skipped during deploy
# and represented as *.placeholder.json files — resolve duplicates in the console
# first, then re-export with --export.
#
# Idempotency:
#   Data source:      create if not exists, update-data-source if exists
#   Datasets:         create if not exists, update-data-set if exists
#   SPICE:            create-ingestion triggered unconditionally (idempotent by design)
#   Folder members:   create-folder-membership, suppress AlreadyExists errors
#   Folder permissions: update-folder-permissions with grant-permissions (idempotent)
# =============================================================

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
set -a; source "${SCRIPT_DIR}/config.env"; set +a

DRY_RUN=false
REFRESH_ONLY=false
EXPORT_ONLY=false
for arg in "$@"; do
    case $arg in
        --dry-run)      DRY_RUN=true      ;;
        --refresh-only) REFRESH_ONLY=true ;;
        --export)       EXPORT_ONLY=true  ;;
    esac
done

log()  { echo "[$(date '+%H:%M:%S')] $*"; }
ok()   { echo "[$(date '+%H:%M:%S')] ✓ $*"; }
warn() { echo "[$(date '+%H:%M:%S')] ⚠ $*"; }
dry()  { echo "[$(date '+%H:%M:%S')] DRY-RUN: $*"; }

QS_DIR="${SCRIPT_DIR}/../quicksight"

# --export runs only Step 5 (export) and exits immediately
if $EXPORT_ONLY; then
    log "=== [5] Export: analyses + dashboard → quicksight/ ==="

    ANALYSES_DIR="${QS_DIR}/analyses"
    DASHBOARDS_DIR="${QS_DIR}/dashboards"
    mkdir -p "${ANALYSES_DIR}" "${DASHBOARDS_DIR}"

    mapfile -t all_analyses < <(
        aws quicksight list-analyses \
            --aws-account-id "${ACCOUNT}" \
            --region "${REGION}" \
            --query 'AnalysisSummaryList[*].[AnalysisId,Name]' \
            --output text
    )

    declare -A name_count=()
    for row in "${all_analyses[@]}"; do
        name=$(echo "$row" | cut -f2-)
        name_count["$name"]=$(( ${name_count["$name"]:-0} + 1 ))
    done

    exported=0; skipped=0
    for row in "${all_analyses[@]}"; do
        id=$(echo "$row" | awk '{print $1}')
        name=$(echo "$row" | cut -f2-)
        if [[ ${name_count["$name"]} -gt 1 ]]; then
            warn "Skipping duplicate analysis: '${name}' (${id}) — resolve in console first"
            skipped=$(( skipped + 1 ))
            continue
        fi
        aws quicksight describe-analysis-definition \
            --aws-account-id "${ACCOUNT}" \
            --analysis-id "${id}" \
            --region "${REGION}" \
            --query '{Name:Name,AnalysisId:AnalysisId,Definition:Definition,ThemeArn:ThemeArn}' \
            > "${ANALYSES_DIR}/${id}.json"
        ok "Exported analysis: ${name} → quicksight/analyses/${id}.json"
        exported=$(( exported + 1 ))
    done

    mapfile -t all_dashboards < <(
        aws quicksight list-dashboards \
            --aws-account-id "${ACCOUNT}" \
            --region "${REGION}" \
            --query 'DashboardSummaryList[*].[DashboardId,Name]' \
            --output text
    )
    for row in "${all_dashboards[@]}"; do
        db_id=$(echo "$row" | awk '{print $1}')
        db_name=$(echo "$row" | cut -f2-)
        aws quicksight describe-dashboard-definition \
            --aws-account-id "${ACCOUNT}" \
            --dashboard-id "${db_id}" \
            --region "${REGION}" \
            --query '{Name:Name,DashboardId:DashboardId,Definition:Definition}' \
            > "${DASHBOARDS_DIR}/${db_id}.json"
        ok "Exported dashboard: ${db_name} → quicksight/dashboards/${db_id}.json"
    done

    log "Export complete — ${exported} analyses exported, ${skipped} duplicates skipped"
    exit 0
fi

# ── QuickSight resource IDs ───────────────────────────────────
DATASOURCE_ID="ee6148c5-7ba2-45b8-b4a5-c721e9ab7aca"
VPC_CONNECTION_ARN="arn:aws:quicksight:${REGION}:${ACCOUNT}:vpcConnection/c9508d2a-f178-4add-a3fb-ba52ae4c742f"
QS_PRINCIPAL="arn:aws:quicksight:${REGION}:${ACCOUNT}:user/default/${ACCOUNT}"
FOLDER_ID="240636fa-ade1-4f5a-9929-67acda51d579"

# Canonical dataset order — drives all three loops (Steps 2, 3, 4)
DATASET_ORDER=(
    "vw_otp_by_route_month"
    "v_routes_consistently_late"
    "vw_dailyvrm"
    "v_voms"
    "vw_dailyvrh"
    "v_missed_trip_rate_by_route"
)

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

    for view_name in "${DATASET_ORDER[@]}"; do

        dataset_id="${DATASET_IDS[$view_name]}"

        if $DRY_RUN; then
            dry "UPSERT dataset: ${view_name} (${dataset_id})"
        elif qs_dataset_exists "${dataset_id}"; then
            # Datasets created via the new QuickSight console experience cannot be
            # updated via the legacy CLI API. Schema doesn't change between deploys,
            # so skip update — SPICE refresh below keeps the data current.
            ok "Dataset exists (no update needed): ${view_name}"
        else
            # Build table maps only when actually creating (avoids 6 python3 subprocesses on re-deploy)
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
# STEP 3 — SPICE Refresh (parallel)
# =============================================================
log "=== [3] SPICE Refresh ==="

DEPLOY_TS="$(date '+%Y%m%d%H%M%S')"
pids=()
for view_name in "${DATASET_ORDER[@]}"; do

    dataset_id="${DATASET_IDS[$view_name]}"
    ingestion_id="deploy-${DEPLOY_TS}-$$-${view_name}"

    if $DRY_RUN; then
        dry "TRIGGER SPICE refresh: ${view_name}"
    else
        ( if aws quicksight create-ingestion \
                  --aws-account-id "${ACCOUNT}" \
                  --data-set-id "${dataset_id}" \
                  --ingestion-id "${ingestion_id}" \
                  --region "${REGION}" > /dev/null 2>&1; then
              ok "SPICE refresh triggered: ${view_name}"
          else
              warn "SPICE refresh skipped (already queued): ${view_name}"
          fi ) &
        pids+=($!)
    fi
done
for pid in "${pids[@]+"${pids[@]}"}"; do wait "$pid"; done

# =============================================================
# STEP 4 — Shared Folder: asset membership + team permissions
# =============================================================
if ! $REFRESH_ONLY; then
    log "=== [4] Shared Folder ==="

    # Full contributor/admin permission set on the folder (single line — embedded in JSON arg)
    FOLDER_ACTIONS='["quicksight:CreateFolder","quicksight:DescribeFolder","quicksight:UpdateFolder","quicksight:DeleteFolder","quicksight:CreateFolderMembership","quicksight:DeleteFolderMembership","quicksight:DescribeFolderPermissions","quicksight:UpdateFolderPermissions"]'

    # 4a — Add each dataset as a folder member (parallel)
    pids=()
    for view_name in "${DATASET_ORDER[@]}"; do

        dataset_id="${DATASET_IDS[$view_name]}"

        if $DRY_RUN; then
            dry "ADD dataset to folder: ${view_name}"
        else
            ( if aws quicksight create-folder-membership \
                      --aws-account-id "${ACCOUNT}" \
                      --folder-id "${FOLDER_ID}" \
                      --member-id "${dataset_id}" \
                      --member-type DATASET \
                      --region "${REGION}" > /dev/null 2>&1; then
                  ok "Folder member (dataset): ${view_name}"
              else
                  warn "Folder membership skipped (already exists or error): ${view_name}"
              fi ) &
            pids+=($!)
        fi
    done
    for pid in "${pids[@]+"${pids[@]}"}"; do wait "$pid"; done

    # 4b — Add data source as a folder member
    if $DRY_RUN; then
        dry "ADD data source to folder: seattle-transit-dw"
    elif aws quicksight create-folder-membership \
            --aws-account-id "${ACCOUNT}" \
            --folder-id "${FOLDER_ID}" \
            --member-id "${DATASOURCE_ID}" \
            --member-type DATASOURCE \
            --region "${REGION}" > /dev/null 2>&1; then
        ok "Folder member (data source): seattle-transit-dw"
    else
        warn "Folder membership skipped (already exists or error): seattle-transit-dw"
    fi

    # 4c — Grant full folder permissions to each team member (parallel)
    pids=()
    for team_user in lingli_yang minglei_ma poojith; do
        qs_user_arn="arn:aws:quicksight:${REGION}:${ACCOUNT}:user/default/${team_user}"

        if $DRY_RUN; then
            dry "GRANT folder permissions to: ${team_user}"
        else
            ( if aws quicksight update-folder-permissions \
                      --aws-account-id "${ACCOUNT}" \
                      --folder-id "${FOLDER_ID}" \
                      --grant-permissions "[{\"Principal\": \"${qs_user_arn}\", \"Actions\": ${FOLDER_ACTIONS}}]" \
                      --region "${REGION}" > /dev/null 2>&1; then
                  ok "Folder permissions granted: ${team_user}"
              else
                  warn "Folder permissions skipped (already set or error): ${team_user}"
              fi ) &
            pids+=($!)
        fi
    done
    for pid in "${pids[@]+"${pids[@]}"}"; do wait "$pid"; done
fi

# =============================================================
# STEP 5 — Deploy: analyses + dashboard from quicksight/
# =============================================================
# Creates or updates analyses and dashboards from the JSON files
# committed in quicksight/analyses/ and quicksight/dashboards/.
# Placeholder files (*.placeholder.json) are skipped.
# =============================================================
if ! $REFRESH_ONLY; then
    log "=== [5] Deploy analyses ==="

    ANALYSES_DIR="${QS_DIR}/analyses"

    for def_file in "${ANALYSES_DIR}"/*.json; do
        # Skip placeholder files
        [[ "${def_file}" == *.placeholder.json ]] && continue
        [[ ! -f "${def_file}" ]] && continue

        analysis_id=$(python3 -c "import json,sys; d=json.load(open('${def_file}')); print(d['AnalysisId'])")
        analysis_name=$(python3 -c "import json,sys; d=json.load(open('${def_file}')); print(d['Name'])")
        definition=$(python3 -c "import json,sys; d=json.load(open('${def_file}')); print(json.dumps(d['Definition']))")
        theme_arn=$(python3 -c "import json,sys; d=json.load(open('${def_file}')); print(d.get('ThemeArn') or '')")

        if $DRY_RUN; then
            dry "UPSERT analysis: ${analysis_name} (${analysis_id})"
            dry "ADD analysis to shared folder: ${analysis_name}"
            continue
        fi

        # Check if analysis exists
        if aws quicksight describe-analysis \
                --aws-account-id "${ACCOUNT}" \
                --analysis-id "${analysis_id}" \
                --region "${REGION}" > /dev/null 2>&1; then

            update_args=(
                --aws-account-id "${ACCOUNT}"
                --analysis-id "${analysis_id}"
                --name "${analysis_name}"
                --definition "${definition}"
                --region "${REGION}"
            )
            [[ -n "${theme_arn}" ]] && update_args+=(--theme-arn "${theme_arn}")
            if aws quicksight update-analysis "${update_args[@]}" > /dev/null 2>&1; then
                ok "Updated analysis: ${analysis_name}"
            else
                warn "Update skipped (stale dataset reference?): ${analysis_name} — re-export after fixing in console"
            fi
        else
            create_args=(
                --aws-account-id "${ACCOUNT}"
                --analysis-id "${analysis_id}"
                --name "${analysis_name}"
                --definition "${definition}"
                --permissions "[{\"Principal\": \"${QS_PRINCIPAL}\", \"Actions\": [\"quicksight:RestoreAnalysis\",\"quicksight:UpdateAnalysisPermissions\",\"quicksight:DeleteAnalysis\",\"quicksight:DescribeAnalysisPermissions\",\"quicksight:QueryAnalysis\",\"quicksight:DescribeAnalysis\",\"quicksight:UpdateAnalysis\"]}]"
                --region "${REGION}"
            )
            [[ -n "${theme_arn}" ]] && create_args+=(--theme-arn "${theme_arn}")
            if aws quicksight create-analysis "${create_args[@]}" > /dev/null 2>&1; then
                ok "Created analysis: ${analysis_name}"
            else
                warn "Create failed (stale dataset reference?): ${analysis_name} — re-export after fixing in console"
            fi
        fi

        # Add to shared folder (idempotent — suppress AlreadyExists)
        if aws quicksight create-folder-membership \
                --aws-account-id "${ACCOUNT}" \
                --folder-id "${FOLDER_ID}" \
                --member-id "${analysis_id}" \
                --member-type ANALYSIS \
                --region "${REGION}" > /dev/null 2>&1; then
            ok "Folder member (analysis): ${analysis_name}"
        else
            warn "Folder membership skipped (already exists): ${analysis_name}"
        fi
    done

    # 5b — Ensure every exported analysis JSON is in the shared folder.
    # The deploy loop above only adds analyses it actively deploys.
    # Analyses that existed in AWS before being exported (and therefore
    # not created/updated by this run) would otherwise be missed.
    log "=== [5b] Folder membership sweep — all exported analyses ==="
    for def_file in "${ANALYSES_DIR}"/*.json; do
        [[ "${def_file}" == *.placeholder.json ]] && continue
        [[ ! -f "${def_file}" ]] && continue
        if $DRY_RUN; then
            analysis_name=$(python3 -c "import json,sys; d=json.load(open('${def_file}')); print(d['Name'])")
            dry "ENSURE folder member (analysis): ${analysis_name}"
            continue
        fi
        analysis_id=$(python3 -c "import json,sys; d=json.load(open('${def_file}')); print(d['AnalysisId'])")
        analysis_name=$(python3 -c "import json,sys; d=json.load(open('${def_file}')); print(d['Name'])")
        if aws quicksight create-folder-membership \
                --aws-account-id "${ACCOUNT}" \
                --folder-id "${FOLDER_ID}" \
                --member-id "${analysis_id}" \
                --member-type ANALYSIS \
                --region "${REGION}" > /dev/null 2>&1; then
            ok "Folder member (analysis): ${analysis_name}"
        else
            ok "Folder member (analysis): ${analysis_name} — already in folder"
        fi
    done

    log "=== [6] Deploy dashboards ==="

    DASHBOARDS_DIR="${QS_DIR}/dashboards"

    for def_file in "${DASHBOARDS_DIR}"/*.json; do
        [[ ! -f "${def_file}" ]] && continue

        dashboard_id=$(python3 -c "import json,sys; d=json.load(open('${def_file}')); print(d['DashboardId'])")
        dashboard_name=$(python3 -c "import json,sys; d=json.load(open('${def_file}')); print(d['Name'])")
        definition=$(python3 -c "import json,sys; d=json.load(open('${def_file}')); print(json.dumps(d['Definition']))")

        if $DRY_RUN; then
            dry "UPSERT dashboard: ${dashboard_name} (${dashboard_id})"
            dry "ADD dashboard to shared folder: ${dashboard_name}"
            continue
        fi

        if aws quicksight describe-dashboard \
                --aws-account-id "${ACCOUNT}" \
                --dashboard-id "${dashboard_id}" \
                --region "${REGION}" > /dev/null 2>&1; then

            aws quicksight update-dashboard \
                --aws-account-id "${ACCOUNT}" \
                --dashboard-id "${dashboard_id}" \
                --name "${dashboard_name}" \
                --definition "${definition}" \
                --region "${REGION}" > /dev/null
            ok "Updated dashboard: ${dashboard_name}"
        else
            aws quicksight create-dashboard \
                --aws-account-id "${ACCOUNT}" \
                --dashboard-id "${dashboard_id}" \
                --name "${dashboard_name}" \
                --definition "${definition}" \
                --permissions "[{\"Principal\": \"${QS_PRINCIPAL}\", \"Actions\": [\"quicksight:DescribeDashboard\",\"quicksight:ListDashboardVersions\",\"quicksight:UpdateDashboardPermissions\",\"quicksight:QueryDashboard\",\"quicksight:UpdateDashboard\",\"quicksight:DeleteDashboard\",\"quicksight:UpdateDashboardPublishedVersion\",\"quicksight:DescribeDashboardPermissions\"]}]" \
                --region "${REGION}" > /dev/null
            ok "Created dashboard: ${dashboard_name}"
        fi

        # Add to shared folder (idempotent — suppress AlreadyExists)
        if aws quicksight create-folder-membership \
                --aws-account-id "${ACCOUNT}" \
                --folder-id "${FOLDER_ID}" \
                --member-id "${dashboard_id}" \
                --member-type DASHBOARD \
                --region "${REGION}" > /dev/null 2>&1; then
            ok "Folder member (dashboard): ${dashboard_name}"
        else
            warn "Folder membership skipped (already exists): ${dashboard_name}"
        fi
    done
fi

log "=== QuickSight deploy complete ==="
if [[ $DRY_RUN == true ]]; then warn "DRY-RUN — no changes applied"; fi
