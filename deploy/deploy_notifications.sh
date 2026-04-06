#!/bin/bash
# =============================================================
# deploy_notifications.sh — Failure Alert Infrastructure
# =============================================================
# Sets up:
#   1. SNS topic  : transit-failure-alerts
#   2. IAM role   : transit-failure-notifier-role (for the Lambda)
#   3. Lambda     : transit-failure-notifier
#   4. EventBridge: glue-job-failure rule    → Lambda
#   5. EventBridge: glue-workflow-failure rule → Lambda
#   6. CW Alarms  : one per Lambda function  → SNS topic
#
# Usage:
#   bash deploy/deploy_notifications.sh [--dry-run]
# =============================================================

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
set -a; source "${SCRIPT_DIR}/config.env"; set +a

DRY_RUN=false
for arg in "$@"; do
    [[ $arg == --dry-run ]] && DRY_RUN=true
done

run() {
    if $DRY_RUN; then
        echo "  [dry-run] $*"
    else
        "$@"
    fi
}

log() { echo ""; echo "  ── $*"; }

LAMBDA_NAME="transit-failure-notifier"
LAMBDA_ZIP="/tmp/${LAMBDA_NAME}.zip"
LAMBDA_SRC="${SCRIPT_DIR}/../lambda/failure-notifier/lambda_function.py"
LAMBDA_ROLE_NAME="transit-failure-notifier-role"
LAMBDA_ROLE_ARN="arn:aws:iam::${ACCOUNT}:role/${LAMBDA_ROLE_NAME}"
FAILURE_SNS_ARN="arn:aws:sns:${REGION}:${ACCOUNT}:transit-failure-alerts"

# Lambda functions to monitor for errors (CloudWatch Alarms)
MONITORED_LAMBDAS=("gtfs-rt-polling" "gtfs-pipeline-notification" "transit-failure-notifier")

echo ""
echo "========================================================"
echo "  deploy_notifications.sh"
echo "  DRY_RUN=${DRY_RUN}"
echo "========================================================"


# ── Step 1: SNS topic ─────────────────────────────────────
log "Step 1: SNS topic — transit-failure-alerts"

EXISTING_TOPIC=$(aws sns list-topics --region "${REGION}" \
    --query "Topics[?TopicArn=='${FAILURE_SNS_ARN}'].TopicArn" \
    --output text 2>/dev/null || true)

if [[ -z "${EXISTING_TOPIC}" ]]; then
    echo "    Creating SNS topic..."
    run aws sns create-topic \
        --name "transit-failure-alerts" \
        --region "${REGION}" \
        --tags "Key=Project,Value=seattle-transit-dw" "Key=ManagedBy,Value=deploy-script" \
        --output text --query 'TopicArn'
else
    echo "    SNS topic already exists — skipping"
fi

echo "    ARN: ${FAILURE_SNS_ARN}"
echo ""
echo "    NOTE: Subscribe your email to this topic manually if not already done:"
echo "      aws sns subscribe --topic-arn ${FAILURE_SNS_ARN} \\"
echo "        --protocol email --notification-endpoint YOUR_EMAIL \\"
echo "        --region ${REGION}"


# ── Step 2: IAM role for the Lambda ──────────────────────
log "Step 2: IAM role — ${LAMBDA_ROLE_NAME}"

TRUST_POLICY='{
  "Version": "2012-10-17",
  "Statement": [{
    "Effect": "Allow",
    "Principal": {"Service": "lambda.amazonaws.com"},
    "Action": "sts:AssumeRole"
  }]
}'

ROLE_EXISTS=$(aws iam get-role --role-name "${LAMBDA_ROLE_NAME}" \
    --query 'Role.RoleName' --output text 2>/dev/null || true)

if [[ -z "${ROLE_EXISTS}" ]]; then
    echo "    Creating IAM role..."
    run aws iam create-role \
        --role-name "${LAMBDA_ROLE_NAME}" \
        --assume-role-policy-document "${TRUST_POLICY}" \
        --output text --query 'Role.Arn'
else
    echo "    IAM role already exists — skipping create"
fi

# Inline policy: SNS publish + Glue get_job_run + basic Lambda execution
INLINE_POLICY=$(cat <<EOF
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Sid": "SNSPublish",
      "Effect": "Allow",
      "Action": "sns:Publish",
      "Resource": "${FAILURE_SNS_ARN}"
    },
    {
      "Sid": "GlueGetJobRun",
      "Effect": "Allow",
      "Action": "glue:GetJobRun",
      "Resource": "*"
    },
    {
      "Sid": "CloudWatchLogs",
      "Effect": "Allow",
      "Action": [
        "logs:CreateLogGroup",
        "logs:CreateLogStream",
        "logs:PutLogEvents"
      ],
      "Resource": "arn:aws:logs:${REGION}:${ACCOUNT}:*"
    }
  ]
}
EOF
)

run aws iam put-role-policy \
    --role-name "${LAMBDA_ROLE_NAME}" \
    --policy-name "${LAMBDA_ROLE_NAME}-policy" \
    --policy-document "${INLINE_POLICY}"
echo "    Inline policy applied"


# ── Step 3: Lambda function ───────────────────────────────
log "Step 3: Lambda — ${LAMBDA_NAME}"

echo "    Zipping ${LAMBDA_SRC}..."
run zip -j "${LAMBDA_ZIP}" "${LAMBDA_SRC}"

LAMBDA_EXISTS=$(aws lambda get-function --function-name "${LAMBDA_NAME}" \
    --region "${REGION}" --query 'Configuration.FunctionName' \
    --output text 2>/dev/null || true)

if [[ -z "${LAMBDA_EXISTS}" ]]; then
    echo "    Creating Lambda function (waiting 10s for IAM role propagation)..."
    $DRY_RUN || sleep 10
    run aws lambda create-function \
        --function-name "${LAMBDA_NAME}" \
        --runtime "python3.12" \
        --handler "lambda_function.lambda_handler" \
        --role "${LAMBDA_ROLE_ARN}" \
        --zip-file "fileb://${LAMBDA_ZIP}" \
        --timeout 30 \
        --memory-size 128 \
        --environment "Variables={SNS_TOPIC_ARN=${FAILURE_SNS_ARN},REGION=${REGION}}" \
        --description "Receives Glue failure events from EventBridge and publishes alerts to SNS" \
        --region "${REGION}" \
        --output text --query 'FunctionArn'
else
    echo "    Updating Lambda code..."
    run aws lambda update-function-code \
        --function-name "${LAMBDA_NAME}" \
        --zip-file "fileb://${LAMBDA_ZIP}" \
        --region "${REGION}" \
        --output text --query 'FunctionArn'

    $DRY_RUN || aws lambda wait function-updated \
        --function-name "${LAMBDA_NAME}" \
        --region "${REGION}"

    echo "    Updating Lambda config..."
    run aws lambda update-function-configuration \
        --function-name "${LAMBDA_NAME}" \
        --environment "Variables={SNS_TOPIC_ARN=${FAILURE_SNS_ARN},REGION=${REGION}}" \
        --region "${REGION}" \
        --output text --query 'FunctionArn'
fi

LAMBDA_ARN="arn:aws:lambda:${REGION}:${ACCOUNT}:function:${LAMBDA_NAME}"


# ── Step 4: EventBridge rules → Lambda ───────────────────
log "Step 4: EventBridge rules"

_upsert_eb_rule() {
    local rule_name="$1"
    local description="$2"
    local event_pattern="$3"

    echo "    Rule: ${rule_name}"

    run aws events put-rule \
        --name "${rule_name}" \
        --description "${description}" \
        --event-pattern "${event_pattern}" \
        --state ENABLED \
        --region "${REGION}" \
        --output text --query 'RuleArn'

    run aws events put-targets \
        --rule "${rule_name}" \
        --targets "Id=failure-notifier-lambda,Arn=${LAMBDA_ARN}" \
        --region "${REGION}" \
        --output text

    # Allow EventBridge to invoke the Lambda
    STATEMENT_ID="eb-${rule_name}-invoke"
    $DRY_RUN || aws lambda remove-permission \
        --function-name "${LAMBDA_NAME}" \
        --statement-id "${STATEMENT_ID}" \
        --region "${REGION}" 2>/dev/null || true
    run aws lambda add-permission \
        --function-name "${LAMBDA_NAME}" \
        --statement-id "${STATEMENT_ID}" \
        --action "lambda:InvokeFunction" \
        --principal "events.amazonaws.com" \
        --source-arn "arn:aws:events:${REGION}:${ACCOUNT}:rule/${rule_name}" \
        --region "${REGION}" \
        --output text --query 'Statement'
}

_upsert_eb_rule \
    "transit-glue-job-failure" \
    "Triggers ${LAMBDA_NAME} when any Glue job enters FAILED state" \
    '{"source":["aws.glue"],"detail-type":["Glue Job State Change"],"detail":{"state":["FAILED"]}}'

_upsert_eb_rule \
    "transit-glue-workflow-failure" \
    "Triggers ${LAMBDA_NAME} when a Glue workflow run enters FAILED or STOPPED state" \
    '{"source":["aws.glue"],"detail-type":["Glue Workflow Run Status"],"detail":{"state":["FAILED","STOPPED"]}}'


# ── Step 5: CloudWatch Alarms for Lambda errors ───────────
log "Step 5: CloudWatch Alarms for Lambda errors → SNS"

for fn_name in "${MONITORED_LAMBDAS[@]}"; do
    alarm_name="transit-lambda-errors-${fn_name}"
    echo "    Alarm: ${alarm_name}"

    run aws cloudwatch put-metric-alarm \
        --alarm-name "${alarm_name}" \
        --alarm-description "Fires when Lambda ${fn_name} has 1+ errors in a 5-minute window" \
        --namespace "AWS/Lambda" \
        --metric-name "Errors" \
        --dimensions "Name=FunctionName,Value=${fn_name}" \
        --statistic Sum \
        --period 300 \
        --evaluation-periods 1 \
        --threshold 1 \
        --comparison-operator GreaterThanOrEqualToThreshold \
        --treat-missing-data notBreaching \
        --alarm-actions "${FAILURE_SNS_ARN}" \
        --region "${REGION}"
done


# ── Summary ───────────────────────────────────────────────
echo ""
echo "========================================================"
echo "  deploy_notifications.sh complete"
echo ""
echo "  Resources:"
echo "    SNS topic  : ${FAILURE_SNS_ARN}"
echo "    Lambda     : ${LAMBDA_NAME}"
echo "    EB rules   : transit-glue-job-failure"
echo "                 transit-glue-workflow-failure"
for fn_name in "${MONITORED_LAMBDAS[@]}"; do
    echo "    CW Alarm   : transit-lambda-errors-${fn_name}"
done
[[ $DRY_RUN == true ]] && echo ""  && echo "  DRY-RUN — no changes were applied"
echo "========================================================"
