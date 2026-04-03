#!/bin/bash
# =============================================================
# deploy_iam.sh — Idempotent IAM Roles + Policies Deploy
# =============================================================
# Creates or updates all IAM roles and policies for the
# Seattle Transit DW project.
#
# Usage:
#   bash deploy/deploy_iam.sh           # full deploy
#   bash deploy/deploy_iam.sh --dry-run # print changes only
#
# Idempotency:
#   Roles:    create-role skipped if exists, trust policy updated
#   Policies: put-role-policy always overwrites (upsert behavior)
#   Tags:     tag-role always applies latest tags
# =============================================================

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
set -a; source "${SCRIPT_DIR}/config.env"; set +a

DRY_RUN=false
for arg in "$@"; do
    [[ "$arg" == "--dry-run" ]] && DRY_RUN=true
done

log()  { echo "[$(date '+%H:%M:%S')] $*"; }
ok()   { echo "[$(date '+%H:%M:%S')] ✓ $*"; }
warn() { echo "[$(date '+%H:%M:%S')] ⚠ $*"; }

run() {
    if $DRY_RUN; then
        echo "[$(date '+%H:%M:%S')] DRY-RUN: ${2:-$1}"
    else
        eval "$1"
    fi
}

role_exists() {
    aws iam get-role --role-name "$1" --region "${REGION}" > /dev/null 2>&1
}

IAM_DIR="${SCRIPT_DIR}/../iam"

# =============================================================
# TransitGlueRole
# =============================================================
log "=== TransitGlueRole ==="

GLUE_TRUST='{
    "Version": "2012-10-17",
    "Statement": [{
        "Effect": "Allow",
        "Principal": {"Service": "glue.amazonaws.com"},
        "Action": "sts:AssumeRole"
    }]
}'

if role_exists "TransitGlueRole"; then
    run "aws iam update-assume-role-policy \
            --role-name TransitGlueRole \
            --policy-document '${GLUE_TRUST}' \
            --region '${REGION}' > /dev/null" \
        "UPDATE trust policy: TransitGlueRole"
    ok "Trust policy updated: TransitGlueRole"
else
    run "aws iam create-role \
            --role-name TransitGlueRole \
            --assume-role-policy-document '${GLUE_TRUST}' \
            --region '${REGION}' > /dev/null" \
        "CREATE role: TransitGlueRole"
    ok "Created role: TransitGlueRole"
fi

# Attach AWS managed Glue policy
run "aws iam attach-role-policy \
        --role-name TransitGlueRole \
        --policy-arn arn:aws:iam::aws:policy/service-role/AWSGlueServiceRole \
        --region '${REGION}' 2>/dev/null || true" \
    "ATTACH AWSGlueServiceRole to TransitGlueRole"

# Inline policy — S3 access
run "aws iam put-role-policy \
        --role-name TransitGlueRole \
        --policy-name TransitS3Access \
        --policy-document '{
            \"Version\": \"2012-10-17\",
            \"Statement\": [
                {
                    \"Effect\": \"Allow\",
                    \"Action\": [\"s3:GetObject\",\"s3:PutObject\",\"s3:DeleteObject\",\"s3:ListBucket\"],
                    \"Resource\": [
                        \"arn:aws:s3:::${S3_RAW}\",
                        \"arn:aws:s3:::${S3_RAW}/*\",
                        \"arn:aws:s3:::${S3_STAGING}\",
                        \"arn:aws:s3:::${S3_STAGING}/*\",
                        \"arn:aws:s3:::${S3_PROCESSED}\",
                        \"arn:aws:s3:::${S3_PROCESSED}/*\"
                    ]
                }
            ]
        }' \
        --region '${REGION}' > /dev/null" \
    "UPSERT inline policy: TransitS3Access"
ok "Upserted: TransitS3Access"

# Inline policy — Redshift Data API
run "aws iam put-role-policy \
        --role-name TransitGlueRole \
        --policy-name TransitRedshiftDataAPI \
        --policy-document '{
            \"Version\": \"2012-10-17\",
            \"Statement\": [{
                \"Effect\": \"Allow\",
                \"Action\": [
                    \"redshift-data:ExecuteStatement\",
                    \"redshift-data:DescribeStatement\",
                    \"redshift-data:GetStatementResult\",
                    \"redshift-data:ListStatements\",
                    \"redshift-serverless:GetCredentials\"
                ],
                \"Resource\": \"*\"
            }]
        }' \
        --region '${REGION}' > /dev/null" \
    "UPSERT inline policy: TransitRedshiftDataAPI"
ok "Upserted: TransitRedshiftDataAPI"

# Inline policy — DynamoDB pipeline table
run "aws iam put-role-policy \
        --role-name TransitGlueRole \
        --policy-name TransitPipelineDynamoDBAccess \
        --policy-document '{
            \"Version\": \"2012-10-17\",
            \"Statement\": [{
                \"Effect\": \"Allow\",
                \"Action\": [
                    \"dynamodb:GetItem\",
                    \"dynamodb:PutItem\",
                    \"dynamodb:UpdateItem\"
                ],
                \"Resource\": \"arn:aws:dynamodb:${REGION}:${ACCOUNT}:table/${DYNAMODB_PIPELINE_TABLE}\"
            }]
        }' \
        --region '${REGION}' > /dev/null" \
    "UPSERT inline policy: TransitPipelineDynamoDBAccess"
ok "Upserted: TransitPipelineDynamoDBAccess"

# Tags
run "aws iam tag-role \
        --role-name TransitGlueRole \
        --tags Key=Project,Value=seattle-transit-dw Key=ManagedBy,Value=deploy-script \
        --region '${REGION}' > /dev/null" \
    "TAG: TransitGlueRole"
ok "Tagged: TransitGlueRole"

# =============================================================
# RedshiftS3CopyRole
# =============================================================
log "=== RedshiftS3CopyRole ==="

REDSHIFT_TRUST='{
    "Version": "2012-10-17",
    "Statement": [{
        "Effect": "Allow",
        "Principal": {"Service": "redshift.amazonaws.com"},
        "Action": "sts:AssumeRole"
    }]
}'

if role_exists "RedshiftS3CopyRole"; then
    run "aws iam update-assume-role-policy \
            --role-name RedshiftS3CopyRole \
            --policy-document '${REDSHIFT_TRUST}' \
            --region '${REGION}' > /dev/null" \
        "UPDATE trust policy: RedshiftS3CopyRole"
    ok "Trust policy updated: RedshiftS3CopyRole"
else
    run "aws iam create-role \
            --role-name RedshiftS3CopyRole \
            --assume-role-policy-document '${REDSHIFT_TRUST}' \
            --region '${REGION}' > /dev/null" \
        "CREATE role: RedshiftS3CopyRole"
    ok "Created role: RedshiftS3CopyRole"
fi

run "aws iam put-role-policy \
        --role-name RedshiftS3CopyRole \
        --policy-name RedshiftS3ReadAccess \
        --policy-document '{
            \"Version\": \"2012-10-17\",
            \"Statement\": [{
                \"Effect\": \"Allow\",
                \"Action\": [\"s3:GetObject\",\"s3:ListBucket\"],
                \"Resource\": [
                    \"arn:aws:s3:::${S3_STAGING}\",
                    \"arn:aws:s3:::${S3_STAGING}/*\"
                ]
            }]
        }' \
        --region '${REGION}' > /dev/null" \
    "UPSERT inline policy: RedshiftS3ReadAccess"
ok "Upserted: RedshiftS3ReadAccess"

run "aws iam tag-role \
        --role-name RedshiftS3CopyRole \
        --tags Key=Project,Value=seattle-transit-dw Key=ManagedBy,Value=deploy-script \
        --region '${REGION}' > /dev/null" \
    "TAG: RedshiftS3CopyRole"
ok "Tagged: RedshiftS3CopyRole"

log "=== IAM deploy complete ==="
[[ $DRY_RUN == true ]] && warn "DRY-RUN — no changes applied"
