#!/bin/bash
# =============================================================
# deploy_iam.sh — Idempotent IAM Roles + Policies Deploy
# =============================================================
# Creates or updates all IAM roles, managed policies, groups,
# and user memberships for the Seattle Transit DW project.
#
# Usage:
#   bash deploy/deploy_iam.sh           # full deploy
#   bash deploy/deploy_iam.sh --dry-run # print changes only
#
# Idempotency:
#   Roles:          create-role skipped if exists, trust policy updated
#   Role policies:  put-role-policy always overwrites (upsert behavior)
#   Managed policy: create if not exists; create-policy-version if exists
#   Group:          create if not exists; attach policies idempotently
#   Memberships:    add-user-to-group skipped if already member
#   User policies:  attach/put idempotent (AWS ignores double-attach)
#   Tags:           tag-role always applies latest tags
#
# NOT managed here (manual console only):
#   User creation, login profiles, MFA, access keys
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
        bash -c "$1"
    fi
}

role_exists() {
    aws iam get-role --role-name "$1" > /dev/null 2>&1
}

group_exists() {
    aws iam get-group --group-name "$1" > /dev/null 2>&1
}

policy_exists() {
    aws iam get-policy --policy-arn "$1" > /dev/null 2>&1
}

user_in_group() {
    aws iam get-group --group-name "$2" \
        | python3 -c "import json,sys; users=[u['UserName'] for u in json.load(sys.stdin)['Users']]; exit(0 if '$1' in users else 1)" \
        2>/dev/null
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

# =============================================================
# Custom Managed Policies
# =============================================================
log "=== Custom Managed Policies ==="

upsert_managed_policy() {
    local name="$1"
    local arn="arn:aws:iam::${ACCOUNT}:policy/${name}"
    local doc
    doc=$(cat "${IAM_DIR}/policies/${name}.json")

    if $DRY_RUN; then
        dry "UPSERT managed policy: ${name}"
        return
    fi

    if policy_exists "${arn}"; then
        # Create a new version and set it as default (max 5 versions; prune oldest if needed)
        local versions
        versions=$(aws iam list-policy-versions --policy-arn "${arn}" \
            --query 'Versions[?!IsDefaultVersion].VersionId' --output text)
        for v in $versions; do
            aws iam delete-policy-version --policy-arn "${arn}" --version-id "${v}" > /dev/null 2>&1 || true
        done
        aws iam create-policy-version \
            --policy-arn "${arn}" \
            --policy-document "${doc}" \
            --set-as-default > /dev/null
        ok "Updated managed policy: ${name}"
    else
        aws iam create-policy \
            --policy-name "${name}" \
            --policy-document "${doc}" \
            --tags Key=Project,Value=seattle-transit-dw Key=ManagedBy,Value=deploy-script \
            > /dev/null
        ok "Created managed policy: ${name}"
    fi
}

upsert_managed_policy "RedshiftDataAPIPolicy"
upsert_managed_policy "RedshiftDevPolicy"
upsert_managed_policy "RedshiftS3Copy"

# =============================================================
# TransitDWTeam Group
# =============================================================
log "=== TransitDWTeam Group ==="

if $DRY_RUN; then
    dry "UPSERT group: TransitDWTeam"
else
    if ! group_exists "TransitDWTeam"; then
        aws iam create-group --group-name "TransitDWTeam" > /dev/null
        ok "Created group: TransitDWTeam"
    else
        ok "Group exists: TransitDWTeam"
    fi
fi

# Attach group policies
for policy_arn in \
    "arn:aws:iam::${ACCOUNT}:policy/RedshiftDataAPIPolicy" \
    "arn:aws:iam::${ACCOUNT}:policy/RedshiftDevPolicy" \
    "arn:aws:iam::aws:policy/AmazonRedshiftFullAccess" \
    "arn:aws:iam::aws:policy/CloudWatchReadOnlyAccess" \
    "arn:aws:iam::aws:policy/service-role/AWSGlueServiceRole" \
    "arn:aws:iam::aws:policy/AWSGlueConsoleFullAccess" \
    "arn:aws:iam::aws:policy/AmazonDynamoDBReadOnlyAccess" \
    "arn:aws:iam::aws:policy/AmazonS3ReadOnlyAccess" \
    "arn:aws:iam::aws:policy/AmazonS3FullAccess" \
    "arn:aws:iam::aws:policy/AmazonRedshiftQueryEditorV2FullAccess" \
    "arn:aws:iam::aws:policy/service-role/AwsGlueSessionUserRestrictedNotebookServiceRole"; do
    policy_name="${policy_arn##*/}"
    run "aws iam attach-group-policy \
            --group-name TransitDWTeam \
            --policy-arn '${policy_arn}' \
            2>/dev/null || true" \
        "ATTACH policy to TransitDWTeam: ${policy_name}"
done
ok "Group policies attached: TransitDWTeam"

# =============================================================
# Team Member Memberships + User-Level Policies
# =============================================================
log "=== Team Member Setup ==="

QUICKSIGHT_POLICY='{
    "Version": "2012-10-17",
    "Statement": [{
        "Effect": "Allow",
        "Action": [
            "quicksight:CreateAdmin",
            "quicksight:CreateUser",
            "quicksight:RegisterUser",
            "quicksight:DescribeUser"
        ],
        "Resource": "*"
    }]
}'

for team_user in lingli_yang minglei_ma poojith; do
    log "  → ${team_user}"

    # Group membership
    if $DRY_RUN; then
        dry "ADD ${team_user} to TransitDWTeam"
    else
        if ! user_in_group "${team_user}" "TransitDWTeam"; then
            aws iam add-user-to-group \
                --user-name "${team_user}" \
                --group-name "TransitDWTeam" > /dev/null
            ok "Added ${team_user} to TransitDWTeam"
        else
            ok "${team_user} already in TransitDWTeam"
        fi
    fi

    # User-level attached policies
    for policy_arn in \
        "arn:aws:iam::${ACCOUNT}:policy/RedshiftS3Copy" \
        "arn:aws:iam::aws:policy/IAMUserChangePassword" \
        "arn:aws:iam::aws:policy/AmazonRedshiftQueryEditorV2ReadWriteSharing" \
        "arn:aws:iam::aws:policy/AmazonEventBridgeFullAccess" \
        "arn:aws:iam::aws:policy/AWSLambda_FullAccess"; do
        policy_name="${policy_arn##*/}"
        run "aws iam attach-user-policy \
                --user-name '${team_user}' \
                --policy-arn '${policy_arn}' \
                2>/dev/null || true" \
            "ATTACH ${policy_name} to ${team_user}"
    done

    # QuickSight inline policy
    run "aws iam put-user-policy \
            --user-name '${team_user}' \
            --policy-name QuickSightCreateAdmin \
            --policy-document '${QUICKSIGHT_POLICY}' > /dev/null" \
        "UPSERT inline QuickSightCreateAdmin for ${team_user}"

    ok "Policies set: ${team_user}"
done

# =============================================================
# hani-admin — ensure AdministratorAccess
# =============================================================
log "=== hani-admin ==="
run "aws iam attach-user-policy \
        --user-name hani-admin \
        --policy-arn arn:aws:iam::aws:policy/AdministratorAccess \
        2>/dev/null || true" \
    "ATTACH AdministratorAccess to hani-admin"
ok "AdministratorAccess confirmed: hani-admin"

log "=== IAM deploy complete ==="
[[ $DRY_RUN == true ]] && warn "DRY-RUN — no changes applied"
