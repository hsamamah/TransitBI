#!/bin/bash
# =============================================================
# deploy_glue.sh — Idempotent Glue Deploy
# =============================================================
# Deploys all Glue jobs, workflows, and triggers for the
# Seattle Transit DW project.
#
# Usage:
#   bash deploy/deploy_glue.sh              # full deploy
#   bash deploy/deploy_glue.sh --dry-run    # print changes only
#   bash deploy/deploy_glue.sh --upload-only # upload scripts then exit
#
# Idempotency:
#   Every resource uses get → update-if-exists | create-if-not pattern.
#   Safe to run repeatedly — will not duplicate or break existing state.
#
# Dependency order:
#   1. Upload scripts to S3
#   2. Create/update Glue jobs (shared lib first)
#   3. Create/update workflows
#   4. Create/update triggers (in chain order)
#
# Fix applied:
#   Adds missing gtfs-rt-factstop-trigger that was absent from the
#   workflow graph, causing inspector → factstop disconnection.
#   Removes duplicate jobs: gtfs-static-ingestion-v2 (old) and
#   gtfs-static-redshift-load-v2 (old) which ran alongside their
#   canonical versions.
# =============================================================

set -euo pipefail

# ── Load config ───────────────────────────────────────────────
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
set -a; source "${SCRIPT_DIR}/config.env"; set +a

# ── Flags ─────────────────────────────────────────────────────
DRY_RUN=false
UPLOAD_ONLY=false
for arg in "$@"; do
    case $arg in
        --dry-run)    DRY_RUN=true    ;;
        --upload-only) UPLOAD_ONLY=true ;;
    esac
done

# ── Helpers ───────────────────────────────────────────────────
log()  { echo "[$(date '+%H:%M:%S')] $*"; }
ok()   { echo "[$(date '+%H:%M:%S')] ✓ $*"; }
warn() { echo "[$(date '+%H:%M:%S')] ⚠ $*"; }
dry()  { echo "[$(date '+%H:%M:%S')] DRY-RUN: $*"; }

run() {
    # run CMD [description]
    # In dry-run mode prints the command instead of executing it
    local desc="${2:-}"
    if $DRY_RUN; then
        dry "${desc:-$1}"
    else
        bash -c "$1"
    fi
}

tag_string() {
    # Convert "Key=Val Key2=Val2" to AWS CLI --tags format
    local result=""
    for pair in $TAGS; do
        key="${pair%%=*}"
        val="${pair#*=}"
        result+="Key=${key},Value=${val} "
    done
    echo "${result% }"
}

glue_job_exists() {
    aws glue get-job --job-name "$1" --region "${REGION}" \
        > /dev/null 2>&1
}

glue_trigger_exists() {
    aws glue get-trigger --name "$1" --region "${REGION}" \
        > /dev/null 2>&1
}

glue_trigger_state() {
    aws glue get-trigger --name "$1" --region "${REGION}" \
        | python3 -c "import json,sys; print(json.load(sys.stdin)['Trigger']['State'])" \
        2>/dev/null || echo "NOT_FOUND"
}

activate_trigger() {
    local name="$1"
    if $DRY_RUN; then
        dry "ACTIVATE trigger: ${name}"
        return
    fi
    local state
    state=$(glue_trigger_state "${name}")
    if [[ "${state}" == "CREATED" || "${state}" == "DEACTIVATED" ]]; then
        aws glue start-trigger --name "${name}" --region "${REGION}" > /dev/null
        ok "Activated trigger: ${name} (was ${state})"
    elif [[ "${state}" == "ACTIVATED" ]]; then
        ok "Trigger already active: ${name}"
    else
        warn "Trigger ${name} in unexpected state: ${state}"
    fi
}

glue_workflow_exists() {
    aws glue get-workflow --name "$1" --region "${REGION}" \
        > /dev/null 2>&1
}

# ── Create or update a Glue job ───────────────────────────────
upsert_glue_job() {
    local name="$1"
    local script="$2"
    local workers="${3:-2}"
    local timeout="${4:-60}"
    local extra_args="${5:-}"

    local default_args="{\"--enable-glue-datacatalog\": \"true\""
    if [[ -n "${extra_args}" ]]; then
        default_args+=", ${extra_args}"
    fi
    default_args+="}"

    local job_json="{
        \"Role\": \"${GLUE_ROLE}\",
        \"GlueVersion\": \"${GLUE_VERSION}\",
        \"WorkerType\": \"${WORKER_TYPE}\",
        \"NumberOfWorkers\": ${workers},
        \"Timeout\": ${timeout},
        \"MaxRetries\": 0,
        \"Command\": {
            \"Name\": \"glueetl\",
            \"ScriptLocation\": \"${script}\",
            \"PythonVersion\": \"3\"
        },
        \"DefaultArguments\": ${default_args}
    }"

    if glue_job_exists "${name}"; then
        run "aws glue update-job \
                --job-name '${name}' \
                --job-update '${job_json}' \
                --region '${REGION}' > /dev/null" \
            "UPDATE job: ${name}"
        ok "Updated job: ${name}"
    else
        run "aws glue create-job \
                --name '${name}' \
                --role '${GLUE_ROLE}' \
                --glue-version '${GLUE_VERSION}' \
                --worker-type '${WORKER_TYPE}' \
                --number-of-workers ${workers} \
                --timeout ${timeout} \
                --max-retries 0 \
                --command '{
                    \"Name\": \"glueetl\",
                    \"ScriptLocation\": \"${script}\",
                    \"PythonVersion\": \"3\"
                }' \
                --default-arguments '${default_args}' \
                --region '${REGION}' > /dev/null" \
            "CREATE job: ${name}"
        ok "Created job: ${name}"
    fi
}

# ── Create or replace a trigger ───────────────────────────────
# Glue does not support updating trigger predicates in-place.
# We always delete + recreate to ensure the definition is current.
upsert_trigger() {
    local name="$1"
    local type="$2"
    local workflow="$3"
    local predicate="$4"   # JSON string or "SCHEDULED"
    local actions="$5"     # JSON array string
    local schedule="${6:-}" # only for SCHEDULED type

    if glue_trigger_exists "${name}"; then
        run "aws glue delete-trigger \
                --name '${name}' \
                --region '${REGION}' > /dev/null" \
            "DELETE trigger (recreating): ${name}"
    fi

    local create_cmd="aws glue create-trigger \
        --name '${name}' \
        --workflow-name '${workflow}' \
        --type '${type}' \
        --actions '${actions}' \
        --region '${REGION}' > /dev/null"

    if [[ "${type}" == "SCHEDULED" ]]; then
        create_cmd="aws glue create-trigger \
            --name '${name}' \
            --workflow-name '${workflow}' \
            --type '${type}' \
            --schedule '${schedule}' \
            --actions '${actions}' \
            --start-on-creation \
            --region '${REGION}' > /dev/null"
    else
        create_cmd="aws glue create-trigger \
            --name '${name}' \
            --workflow-name '${workflow}' \
            --type '${type}' \
            --predicate '${predicate}' \
            --actions '${actions}' \
            --region '${REGION}' > /dev/null"
    fi

    run "${create_cmd}" "CREATE trigger: ${name}"
    ok "Upserted trigger: ${name}"
}

# ── Create workflow if not exists ─────────────────────────────
upsert_workflow() {
    local name="$1"
    local desc="$2"
    if ! glue_workflow_exists "${name}"; then
        run "aws glue create-workflow \
                --name '${name}' \
                --description '${desc}' \
                --region '${REGION}' > /dev/null" \
            "CREATE workflow: ${name}"
        ok "Created workflow: ${name}"
    else
        ok "Workflow exists (no update needed): ${name}"
    fi
}

# =============================================================
# STEP 1 — Upload scripts to S3
# =============================================================
log "=== [1] Uploading scripts to S3 (${SCRIPTS}) ==="

JOBS_DIR="${SCRIPT_DIR}/../glue/jobs"
LIB_DIR="${SCRIPT_DIR}/../glue/lib"

upload_script() {
    local src="$1"
    local dest="$2"
    run "aws s3 cp '${src}' '${dest}' --region '${REGION}'" \
        "UPLOAD: $(basename ${src}) → ${dest}"
}

# Shared library — upload first
upload_script "${LIB_DIR}/pipeline_param_reader_v2.py" \
    "${SCRIPTS}/pipeline_param_reader_v2.py"

# Glue job scripts
upload_script "${JOBS_DIR}/gtfs_static_ingestion.py"         "${SCRIPTS}/gtfs_static_ingestion.py"
upload_script "${JOBS_DIR}/gtfs_static_validation.py"        "${SCRIPTS}/gtfs_static_validation.py"
upload_script "${JOBS_DIR}/gtfs_static_redshift_load.py"     "${SCRIPTS}/gtfs_static_redshift_load.py"
upload_script "${JOBS_DIR}/gtfs-rt-parse-load-glue.py"        "${SCRIPTS}/gtfs-rt-parse-load-glue.py"
upload_script "${JOBS_DIR}/transit_pipeline_inspector_v2.py" "${SCRIPTS}/transit_pipeline_inspector_v2.py"
upload_script "${JOBS_DIR}/factstop_skeleton_and_merge_v2.py" "${SCRIPTS}/factstop_skeleton_and_merge_v2.py"
upload_script "${JOBS_DIR}/facttrip_skeleton_and_merge_v2.py" "${SCRIPTS}/facttrip_skeleton_and_merge_v2.py"
upload_script "${JOBS_DIR}/factserviceday_load_v2.py"        "${SCRIPTS}/factserviceday_load_v2.py"

ok "All scripts uploaded"

[[ "${UPLOAD_ONLY}" == true ]] && { log "Upload-only mode — exiting."; exit 0; }

# =============================================================
# STEP 2 — Glue Jobs
# =============================================================
log "=== [2] Upserting Glue jobs ==="

PARAM_READER="\"--extra-py-files\": \"${SCRIPTS}/pipeline_param_reader_v2.py\""

# gtfs-static-pipeline jobs
upsert_glue_job \
    "gtfs-static-ingestion" \
    "${SCRIPTS}/gtfs_static_ingestion.py" \
    2 60

upsert_glue_job \
    "gtfs-static-validation" \
    "${SCRIPTS}/gtfs_static_validation.py" \
    2 60

upsert_glue_job \
    "gtfs-static-redshift-load" \
    "${SCRIPTS}/gtfs_static_redshift_load.py" \
    2 60 \
    "\"--iam_role\": \"${REDSHIFT_COPY_ROLE}\""

# gtfs-rt-daily-pipeline jobs
upsert_glue_job \
    "gtfs-rt-parse-load-glue" \
    "${SCRIPTS}/gtfs-rt-parse-load-glue.py" \
    10 60 \
    "\"--iam_role\": \"${REDSHIFT_COPY_ROLE}\""

upsert_glue_job \
    "transit-pipeline-inspector" \
    "${SCRIPTS}/transit_pipeline_inspector_v2.py" \
    2 30 \
    "\"--lookback_days\": \"30\""

upsert_glue_job \
    "factstop-skeleton-and-merge-load" \
    "${SCRIPTS}/factstop_skeleton_and_merge_v2.py" \
    2 60 \
    "${PARAM_READER}, \"--phase\": \"both\", \"--force\": \"false\""

upsert_glue_job \
    "facttrip-skeleton-and-merge-load" \
    "${SCRIPTS}/facttrip_skeleton_and_merge_v2.py" \
    2 60 \
    "${PARAM_READER}, \"--phase\": \"both\", \"--force\": \"false\""

upsert_glue_job \
    "factserviceday-load" \
    "${SCRIPTS}/factserviceday_load_v2.py" \
    2 60 \
    "${PARAM_READER}"

ok "All jobs upserted"

# =============================================================
# STEP 3 — Remove deprecated duplicate jobs
# =============================================================
log "=== [3] Removing deprecated duplicate jobs ==="

for deprecated_job in "gtfs-static-ingestion-v2" "gtfs-static-redshift-load-v2"; do
    if glue_job_exists "${deprecated_job}"; then
        run "aws glue delete-job \
                --job-name '${deprecated_job}' \
                --region '${REGION}' > /dev/null" \
            "DELETE deprecated job: ${deprecated_job}"
        ok "Deleted deprecated job: ${deprecated_job}"
    else
        ok "Deprecated job already gone: ${deprecated_job}"
    fi
done

# =============================================================
# STEP 4 — Workflows
# =============================================================
log "=== [4] Upserting workflows ==="

upsert_workflow \
    "${WORKFLOW_STATIC}" \
    "Daily GTFS static ingestion, validation, and Redshift load"

upsert_workflow \
    "${WORKFLOW_RT}" \
    "Daily GTFS-RT parse, load, and fact table pipeline"

# =============================================================
# STEP 5 — Triggers: gtfs-static-pipeline
# =============================================================
log "=== [5] Upserting gtfs-static-pipeline triggers ==="

# Schedule trigger — fires static ingestion daily at 07:00 PST
upsert_trigger \
    "gtfs-static-daily-start" \
    "SCHEDULED" \
    "${WORKFLOW_STATIC}" \
    "" \
    "[{\"JobName\": \"gtfs-static-ingestion\"}]" \
    "${SCHEDULE_STATIC}"

# After ingestion → start crawler
upsert_trigger \
    "start-crawler" \
    "CONDITIONAL" \
    "${WORKFLOW_STATIC}" \
    "{\"Logical\": \"ANY\", \"Conditions\": [{
        \"LogicalOperator\": \"EQUALS\",
        \"JobName\": \"gtfs-static-ingestion\",
        \"State\": \"SUCCEEDED\"
    }]}" \
    "[{\"CrawlerName\": \"gtfs-static-crawler\"}]"

# After crawler → validation
upsert_trigger \
    "start-validation" \
    "CONDITIONAL" \
    "${WORKFLOW_STATIC}" \
    "{\"Logical\": \"ANY\", \"Conditions\": [{
        \"LogicalOperator\": \"EQUALS\",
        \"CrawlerName\": \"gtfs-static-crawler\",
        \"CrawlState\": \"SUCCEEDED\"
    }]}" \
    "[{\"JobName\": \"gtfs-static-validation\"}]"

# After validation → redshift load
upsert_trigger \
    "start-redshift-load-copy" \
    "CONDITIONAL" \
    "${WORKFLOW_STATIC}" \
    "{\"Logical\": \"ANY\", \"Conditions\": [{
        \"LogicalOperator\": \"EQUALS\",
        \"JobName\": \"gtfs-static-validation\",
        \"State\": \"SUCCEEDED\"
    }]}" \
    "[{\"JobName\": \"gtfs-static-redshift-load\"}]"

# =============================================================
# STEP 6 — Triggers: gtfs-rt-daily-pipeline
# =============================================================
log "=== [6] Upserting gtfs-rt-daily-pipeline triggers ==="

# Schedule trigger — fires RT parse daily at 08:00 PST
upsert_trigger \
    "gtfs-rt-daily-start" \
    "SCHEDULED" \
    "${WORKFLOW_RT}" \
    "" \
    "[{\"JobName\": \"gtfs-rt-parse-load-glue\"}]" \
    "${SCHEDULE_RT}"

# After parse → inspector
upsert_trigger \
    "gtfs-rt-inspector-trigger" \
    "CONDITIONAL" \
    "${WORKFLOW_RT}" \
    "{\"Logical\": \"ANY\", \"Conditions\": [{
        \"LogicalOperator\": \"EQUALS\",
        \"JobName\": \"gtfs-rt-parse-load-glue\",
        \"State\": \"SUCCEEDED\"
    }]}" \
    "[{\"JobName\": \"transit-pipeline-inspector\"}]"

# After inspector → factstop  ← THIS WAS THE MISSING TRIGGER
upsert_trigger \
    "gtfs-rt-factstop-trigger" \
    "CONDITIONAL" \
    "${WORKFLOW_RT}" \
    "{\"Logical\": \"ANY\", \"Conditions\": [{
        \"LogicalOperator\": \"EQUALS\",
        \"JobName\": \"transit-pipeline-inspector\",
        \"State\": \"SUCCEEDED\"
    }]}" \
    "[{\"JobName\": \"factstop-skeleton-and-merge-load\"}]"

# After factstop → facttrip
upsert_trigger \
    "gtfs-rt-facttrip-trigger" \
    "CONDITIONAL" \
    "${WORKFLOW_RT}" \
    "{\"Logical\": \"ANY\", \"Conditions\": [{
        \"LogicalOperator\": \"EQUALS\",
        \"JobName\": \"factstop-skeleton-and-merge-load\",
        \"State\": \"SUCCEEDED\"
    }]}" \
    "[{\"JobName\": \"facttrip-skeleton-and-merge-load\"}]"

# After facttrip → factserviceday
upsert_trigger \
    "gtfs-rt-factserviceday-trigger" \
    "CONDITIONAL" \
    "${WORKFLOW_RT}" \
    "{\"Logical\": \"ANY\", \"Conditions\": [{
        \"LogicalOperator\": \"EQUALS\",
        \"JobName\": \"facttrip-skeleton-and-merge-load\",
        \"State\": \"SUCCEEDED\"
    }]}" \
    "[{\"JobName\": \"factserviceday-load\"}]"

# =============================================================
# STEP 7 — Activate all triggers
# =============================================================
log "=== [7] Activating triggers ==="

# Conditional triggers must be explicitly activated after create.
# SCHEDULED triggers use --start-on-creation so they self-activate,
# but we call activate_trigger on all to ensure consistent state.
for trigger_name in \
    "gtfs-static-daily-start" \
    "start-crawler" \
    "start-validation" \
    "start-redshift-load-copy" \
    "gtfs-rt-daily-start" \
    "gtfs-rt-inspector-trigger" \
    "gtfs-rt-factstop-trigger" \
    "gtfs-rt-facttrip-trigger" \
    "gtfs-rt-factserviceday-trigger"; do
    activate_trigger "${trigger_name}"
done

ok "All triggers activated"

# =============================================================
# STEP 8 — Verify final state
# =============================================================
log "=== [8] Verifying final state ==="

if ! $DRY_RUN; then
    log "Glue jobs:"
    aws glue get-jobs --region "${REGION}" \
        | python3 -c "
import json, sys
jobs = json.load(sys.stdin)['Jobs']
for j in sorted(jobs, key=lambda x: x['Name']):
    ver = j.get('GlueVersion','?')
    print(f'  {ver:5}  {j[\"Name\"]}')
"

    log "Workflows:"
    aws glue list-workflows --region "${REGION}" \
        | python3 -c "
import json, sys
for w in json.load(sys.stdin)['Workflows']:
    print(f'  {w}')
"

    log "RT pipeline trigger chain:"
    aws glue get-triggers --region "${REGION}" \
        | python3 -c "
import json, sys
triggers = json.load(sys.stdin)['Triggers']
rt = [t for t in triggers if 'gtfs-rt' in t['Name'] or 'factstop' in t['Name']
      or 'facttrip' in t['Name'] or 'factservice' in t['Name']
      or 'inspector' in t['Name']]
for t in sorted(rt, key=lambda x: x['Name']):
    src  = [c.get('JobName','schedule') for c in t.get('Predicate',{}).get('Conditions',[])]
    acts = [a.get('JobName','?') for a in t.get('Actions',[])]
    print(f'  {t[\"Name\"]:45} {src} → {acts}')
"
fi

log "=== Deploy complete ==="
[[ $DRY_RUN == true ]] && warn "DRY-RUN mode — no changes were made"
