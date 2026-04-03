#!/bin/bash
# =============================================================
# deploy_all.sh — Master Deploy Script
# =============================================================
# Runs all deploy scripts in correct dependency order.
#
# Usage:
#   bash deploy/deploy_all.sh              # full deploy
#   bash deploy/deploy_all.sh --dry-run    # print changes only
#   bash deploy/deploy_all.sh --glue-only  # skip IAM + Redshift
#
# Dependency order:
#   1. IAM roles + policies  (other resources reference these)
#   2. Redshift DDL + views  (fact jobs write to these tables)
#   3. Glue jobs + triggers  (reads IAM + Redshift)
# =============================================================

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
set -a; source "${SCRIPT_DIR}/config.env"; set +a

DRY_RUN=false
GLUE_ONLY=false
ARGS=()

for arg in "$@"; do
    case $arg in
        --dry-run)   DRY_RUN=true;  ARGS+=("--dry-run")  ;;
        --glue-only) GLUE_ONLY=true ;;
    esac
done

log() { echo ""; echo "========================================"; echo "  $*"; echo "========================================"; }

START=$(date +%s)

if ! $GLUE_ONLY; then
    log "STEP 1 — IAM"
    bash "${SCRIPT_DIR}/deploy_iam.sh" "${ARGS[@]+"${ARGS[@]}"}"

    log "STEP 2 — Redshift"
    bash "${SCRIPT_DIR}/deploy_redshift.sh" "${ARGS[@]+"${ARGS[@]}"}"
fi

log "STEP 3 — Glue"
bash "${SCRIPT_DIR}/deploy_glue.sh" "${ARGS[@]+"${ARGS[@]}"}"

END=$(date +%s)
ELAPSED=$((END - START))

echo ""
echo "========================================"
echo "  Deploy complete in ${ELAPSED}s"
[[ $DRY_RUN == true ]] && echo "  DRY-RUN — no changes were applied"
echo "========================================"
