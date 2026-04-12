#!/bin/bash
# =============================================================
# backload.sh — Manual pipeline backload for a date range
# =============================================================
# Assumes raw data is already in S3 (within the 90-day .pb retention
# window for RT, or staged files present for static dims).
#
# What it runs (in order):
#   For each date in the range:
#     1. gtfs-rt-parse-load-glue  (--target_date)  → stg.rt_* tables
#   Then, for the full date range in one job each:
#     2. factstop-skeleton-and-merge-load  (--start_date / --end_date)
#     3. facttrip-skeleton-and-merge-load  (--start_date / --end_date)
#   Then, after fact jobs complete:
#     4. factserviceday-load               (--start_date / --end_date)
#
# Optionally:
#     5. gtfs-static-redshift-load  (--target_date)  if --static-date given
#
# The transit-pipeline-inspector is intentionally SKIPPED — date params
# are passed directly to the fact jobs, bypassing DynamoDB entirely.
#
# Usage:
#   bash scripts/backload.sh --start_date 2026-03-01 --end_date 2026-03-07
#   bash scripts/backload.sh --start_date 2026-03-01 --end_date 2026-03-07 --static-date 2026-03-01
#   bash scripts/backload.sh --start_date 2026-03-01 --end_date 2026-03-07 --rt-only
#   bash scripts/backload.sh --start_date 2026-03-01 --end_date 2026-03-07 --dry-run
#
# Flags:
#   --start_date DATE    Start of the date range (inclusive, YYYY-MM-DD) [required]
#   --end_date   DATE    End of the date range (inclusive, YYYY-MM-DD)   [required]
#   --static-date DATE   Also reload static dims from this staged date
#   --rt-only            Skip static dims even if --static-date is given
#   --force-skeleton     DELETE existing FactTrip rows before skeleton insert
#                        (use after a logic fix to replace already-inserted rows)
#   --dry-run            Print Glue job submissions without running them
# =============================================================

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
set -a; source "${SCRIPT_DIR}/../deploy/config.env"; set +a

# ── Argument parsing ──────────────────────────────────────────
START_DATE=""
END_DATE=""
STATIC_DATE=""
RT_ONLY=false
FORCE_SKELETON=false
DRY_RUN=false

while [[ $# -gt 0 ]]; do
    case "$1" in
        --start_date)      START_DATE="$2";   shift 2 ;;
        --end_date)        END_DATE="$2";     shift 2 ;;
        --static-date)     STATIC_DATE="$2";  shift 2 ;;
        --rt-only)         RT_ONLY=true;      shift   ;;
        --force-skeleton)  FORCE_SKELETON=true; shift  ;;
        --dry-run)         DRY_RUN=true;      shift   ;;
        *) echo "Unknown argument: $1"; exit 1 ;;
    esac
done

if [[ -z "${START_DATE}" || -z "${END_DATE}" ]]; then
    echo "Usage: bash scripts/backload.sh --start_date YYYY-MM-DD --end_date YYYY-MM-DD [--static-date YYYY-MM-DD] [--rt-only] [--dry-run]"
    exit 1
fi

# ── Helpers ───────────────────────────────────────────────────
log()  { echo ""; echo "  ── $*"; }
info() { echo "    $*"; }

date_range() {
    # Emit each YYYY-MM-DD between start and end (inclusive)
    local d="${START_DATE}"
    while [[ "${d}" < "${END_DATE}" || "${d}" == "${END_DATE}" ]]; do
        echo "${d}"
        d=$(date -d "${d} +1 day" +%Y-%m-%d)
    done
}

# Submit a Glue job run and return the job run ID.
# Usage: submit_job TIMEOUT_MINUTES JOB_NAME KEY=VALUE [KEY=VALUE ...]
# TIMEOUT_MINUTES overrides the job definition timeout for this run only.
submit_job() {
    local timeout_min="$1"; shift
    local job_name="$1";    shift
    local args_json=""
    for kv in "$@"; do
        local key="${kv%%=*}"
        local val="${kv#*=}"
        args_json+="\"--${key}\": \"${val}\","
    done
    args_json="{${args_json%,}}"

    if $DRY_RUN; then
        echo "  [dry-run] start-job-run: ${job_name}  timeout=${timeout_min}m  args=${args_json}"
        echo "dry-run-id"
        return
    fi

    local attempt run_id
    for attempt in 1 2 3; do
        run_id=$(aws glue start-job-run \
            --job-name "${job_name}" \
            --arguments "${args_json}" \
            --timeout "${timeout_min}" \
            --region "${REGION}" \
            --output text \
            --query 'JobRunId' 2>&1) && echo "${run_id}" && return
        echo "    WARN: start-job-run attempt ${attempt}/3 failed — ${run_id}"
        sleep $((attempt * 15))
    done
    echo "  ERROR: failed to submit ${job_name} after 3 attempts" >&2
    exit 1
}

# Wait for a list of "JOB_NAME:RUN_ID" pairs to all reach a terminal state.
# Exits with code 1 if any run FAILED.
wait_for_jobs() {
    local -a pairs=("$@")
    local failed=false
    # Track which run IDs are still in-flight; remove on terminal state
    declare -A pending=()
    for pair in "${pairs[@]}"; do
        local run_id="${pair#*:}"
        [[ "${run_id}" != "dry-run-id" ]] && pending["${pair}"]=1
    done

    while [[ ${#pending[@]} -gt 0 ]]; do
        sleep 15
        for pair in "${!pending[@]}"; do
            local job_name="${pair%%:*}"
            local run_id="${pair#*:}"

            local state
            state=$(aws glue get-job-run \
                --job-name "${job_name}" \
                --run-id "${run_id}" \
                --region "${REGION}" \
                --output text \
                --query 'JobRun.JobRunState')

            case "${state}" in
                SUCCEEDED)
                    info "${job_name} [${run_id}] — SUCCEEDED"
                    unset "pending[${pair}]" ;;
                FAILED|ERROR|TIMEOUT|STOPPED)
                    info "FAILED: ${job_name} [${run_id}] — ${state}"
                    unset "pending[${pair}]"
                    failed=true ;;
            esac
            # RUNNING/STARTING/STOPPING/WAITING: leave in pending, keep waiting
        done
    done

    if $failed; then echo ""; echo "  ERROR: One or more Glue jobs failed — aborting backload."; exit 1; fi
}

# ── Cost estimate ─────────────────────────────────────────────
count_dates() {
    date_range | wc -l | tr -d ' '
}

estimate_cost() {
    local n_days="$1"
    # Approximate: $0.44/DPU-hr
    # parse-load: 10 workers × 8 min/day  = $0.59/day
    # factstop:    2 workers × 5 min       = $0.07/day
    # facttrip:    2 workers × 5 min       = $0.07/day
    # factserviceday: 2 workers × 3 min    = $0.04/day
    # total ≈ $0.77/day
    local total
    total=$(python3 -c "print(f'{${n_days} * 0.77:.2f}')")
    echo "~\$${total}"
}

# ── Main ──────────────────────────────────────────────────────
N_DAYS=$(count_dates)
COST=$(estimate_cost "${N_DAYS}")

echo ""
echo "========================================================"
echo "  TransitBI Backload"
echo "  Range     : ${START_DATE} → ${END_DATE}  (${N_DAYS} days)"
[[ -n "${STATIC_DATE}" ]] && ! $RT_ONLY && \
    echo "  Static    : ${STATIC_DATE}"
echo "  Est. cost : ${COST}"
echo "  Force skel: ${FORCE_SKELETON}"
echo "  Dry run   : ${DRY_RUN}"
echo "========================================================"


# ── STEP 1: RT parse + load (one job per date) ────────────────
log "Step 1: RT parse → stg.rt_* tables (${N_DAYS} runs)"
echo "    Running sequentially — each day's staging must complete"
echo "    before fact jobs start."
echo ""

declare -a parse_pairs=()
for dt in $(date_range); do
    info "Submitting gtfs-rt-parse-load-glue for ${dt}..."
    run_id=$(submit_job 30 "gtfs-rt-parse-load-glue" "target_date=${dt}")
    info "  Run ID: ${run_id}"
    parse_pairs+=("gtfs-rt-parse-load-glue:${run_id}")

    # Wait for each date before moving to the next — stg tables are
    # truncated+reloaded per job run, so running in parallel would race.
    info "  Waiting for completion..."
    wait_for_jobs "gtfs-rt-parse-load-glue:${run_id}"
    info "  Done: ${dt}"
    # Brief pause — Glue's concurrency slot isn't released instantly
    # after SUCCEEDED; without this the next StartJobRun races.
    sleep 10
done


# ── STEP 2: Fact jobs (parallel, full range at once) ──────────
log "Step 2: Fact jobs (--start_date ${START_DATE} --end_date ${END_DATE})"

declare -a fact_pairs=()

info "Submitting factstop-skeleton-and-merge-load..."
run_id=$(submit_job 60 "factstop-skeleton-and-merge-load" \
    "start_date=${START_DATE}" "end_date=${END_DATE}" \
    "phase=both" "force=false")
info "  Run ID: ${run_id}"
fact_pairs+=("factstop-skeleton-and-merge-load:${run_id}")

info "Submitting facttrip-skeleton-and-merge-load..."
run_id=$(submit_job 60 "facttrip-skeleton-and-merge-load" \
    "start_date=${START_DATE}" "end_date=${END_DATE}" \
    "phase=both" "force=false" "force_skeleton=${FORCE_SKELETON}")
info "  Run ID: ${run_id}"
fact_pairs+=("facttrip-skeleton-and-merge-load:${run_id}")

info "Waiting for FactStop and FactTrip..."
wait_for_jobs "${fact_pairs[@]}"
info "Both completed."


# ── STEP 3: FactServiceDay (after facts) ─────────────────────
log "Step 3: factserviceday-load"

info "Submitting factserviceday-load..."
run_id=$(submit_job 30 "factserviceday-load" \
    "start_date=${START_DATE}" "end_date=${END_DATE}")
info "  Run ID: ${run_id}"
wait_for_jobs "factserviceday-load:${run_id}"
info "Completed."


# ── STEP 4: Static dims (optional) ───────────────────────────
if [[ -n "${STATIC_DATE}" ]] && ! $RT_ONLY; then
    log "Step 4: Static dims reload for ${STATIC_DATE}"
    info "Submitting gtfs-static-redshift-load with --target_date ${STATIC_DATE}..."
    run_id=$(submit_job 60 "gtfs-static-redshift-load" "target_date=${STATIC_DATE}")
    info "  Run ID: ${run_id}"
    wait_for_jobs "gtfs-static-redshift-load:${run_id}"
    info "Completed."
else
    log "Step 4: Static dims — skipped (pass --static-date YYYY-MM-DD to include)"
fi


# ── Done ──────────────────────────────────────────────────────
echo ""
echo "========================================================"
echo "  Backload complete"
echo "  Range  : ${START_DATE} → ${END_DATE}  (${N_DAYS} days)"
echo "  Cost   : ${COST} (estimate)"
[[ $DRY_RUN == true ]] && echo "  DRY-RUN — no Glue jobs were submitted"
echo "========================================================"
