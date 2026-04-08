#!/bin/bash
# =============================================================
# deploy_notifications.sh — Notification Infrastructure
# =============================================================
# Sets up:
#   1. SNS topics : transit-failure-alerts + transit-daily-digest
#   2. IAM role   : transit-failure-notifier-role
#   3. Lambdas    : transit-failure-notifier
#                   gtfs-rt-polling
#                   gtfs-pipeline-notification
#   4. EventBridge: glue-job-failure          → failure-notifier
#                   glue-workflow-failure      → failure-notifier
#                   gtfs-rt-polling-schedule   → gtfs-rt-polling (rate 1 min)
#                   gtfs-static-pipeline-complete → gtfs-pipeline-notification
#   5. CW Alarms  : Lambda errors (all 3 functions)
#                   GTFS-RT feed fetch failures + complete outage
#                   Pipeline notification invocation check (24h)
#                   SNS delivery failures (both topics)
#
# Usage:
#   bash deploy/deploy_notifications.sh [--dry-run]
#
# Prerequisites:
#   .env must exist with DIGEST_EMAIL and OBA_API_KEY set
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

LAMBDA_NAME="${FAILURE_NOTIFIER_LAMBDA}"
LAMBDA_ZIP="/tmp/${LAMBDA_NAME}.zip"
LAMBDA_SRC="${SCRIPT_DIR}/../lambda/failure-notifier/lambda_function.py"
LAMBDA_ROLE_NAME="transit-failure-notifier-role"
LAMBDA_ROLE_ARN="arn:aws:iam::${ACCOUNT}:role/${LAMBDA_ROLE_NAME}"
FAILURE_SNS_ARN="arn:aws:sns:${REGION}:${ACCOUNT}:transit-failure-alerts"

RT_POLLING_ROLE_ARN="arn:aws:iam::${ACCOUNT}:role/gtfs-rt-polling-role"
PIPELINE_NOTIF_ROLE_ARN="arn:aws:iam::${ACCOUNT}:role/gtfs-pipeline-notification-role"

# Lambda functions to monitor for errors (CloudWatch Alarms)
MONITORED_LAMBDAS=("gtfs-rt-polling" "gtfs-pipeline-notification" "transit-failure-notifier")

echo ""
echo "========================================================"
echo "  deploy_notifications.sh"
echo "  DRY_RUN=${DRY_RUN}"
echo "========================================================"


# ── Step 1: SNS topic ─────────────────────────────────────
log "Step 1: SNS topics"

# Load DIGEST_EMAIL from .env if present
DOTENV="${SCRIPT_DIR}/../.env"
if [[ -f "${DOTENV}" ]]; then
    set -a; source "${DOTENV}"; set +a
fi

# 1a — Failure alerts topic
EXISTING_TOPIC=$(aws sns list-topics --region "${REGION}" \
    --query "Topics[?TopicArn=='${FAILURE_SNS_ARN}'].TopicArn" \
    --output text 2>/dev/null || true)

if [[ -z "${EXISTING_TOPIC}" ]]; then
    echo "    Creating SNS topic: ${FAILURE_SNS_TOPIC}"
    run aws sns create-topic \
        --name "${FAILURE_SNS_TOPIC}" \
        --region "${REGION}" \
        --tags "Key=Project,Value=seattle-transit-dw" "Key=ManagedBy,Value=deploy-script" \
        --output text --query 'TopicArn'
else
    echo "    SNS topic already exists: ${FAILURE_SNS_TOPIC}"
fi

echo "    ARN: ${FAILURE_SNS_ARN}"
echo ""
echo "    NOTE: Subscribe your email to this topic manually if not already done:"
echo "      aws sns subscribe --topic-arn ${FAILURE_SNS_ARN} \\"
echo "        --protocol email --notification-endpoint YOUR_EMAIL \\"
echo "        --region ${REGION}"

# 1b — Daily digest topic
EXISTING_DIGEST=$(aws sns list-topics --region "${REGION}" \
    --query "Topics[?TopicArn=='${DIGEST_SNS_ARN}'].TopicArn" \
    --output text 2>/dev/null || true)

if [[ -z "${EXISTING_DIGEST}" ]]; then
    echo "    Creating SNS topic: ${DIGEST_SNS_TOPIC}"
    run aws sns create-topic \
        --name "${DIGEST_SNS_TOPIC}" \
        --region "${REGION}" \
        --tags "Key=Project,Value=seattle-transit-dw" "Key=ManagedBy,Value=deploy-script" \
        --output text --query 'TopicArn'
else
    echo "    SNS topic already exists: ${DIGEST_SNS_TOPIC}"
fi

echo "    ARN: ${DIGEST_SNS_ARN}"

# Subscribe DIGEST_EMAIL if provided via .env
if [[ -n "${DIGEST_EMAIL:-}" ]]; then
    EXISTING_SUB=$(aws sns list-subscriptions-by-topic \
        --topic-arn "${DIGEST_SNS_ARN}" --region "${REGION}" \
        --query "Subscriptions[?Endpoint=='${DIGEST_EMAIL}'].SubscriptionArn" \
        --output text 2>/dev/null || true)
    if [[ -z "${EXISTING_SUB}" ]]; then
        echo "    Subscribing ${DIGEST_EMAIL} to ${DIGEST_SNS_TOPIC}..."
        run aws sns subscribe \
            --topic-arn "${DIGEST_SNS_ARN}" \
            --protocol email \
            --notification-endpoint "${DIGEST_EMAIL}" \
            --region "${REGION}"
        echo "    ⚠ Check your inbox to confirm the subscription"
    else
        echo "    ${DIGEST_EMAIL} already subscribed"
    fi
else
    echo "    NOTE: Set DIGEST_EMAIL in .env to auto-subscribe your email"
fi


# ── Step 2: IAM role — managed by deploy_iam.sh ──────────
# transit-failure-notifier-role is created/updated in deploy_iam.sh
# (Step: transit-failure-notifier-role). deploy_all.sh runs IAM
# first, so the role is guaranteed to exist by the time this step runs.
log "Step 2: IAM role — managed by deploy_iam.sh (skipping)"
echo "    Role: ${LAMBDA_ROLE_NAME} — created/updated in deploy_iam.sh"


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


# ── Step 3b: gtfs-rt-polling Lambda ──────────────────────
log "Step 3b: Lambda — gtfs-rt-polling"

RT_SRC="${SCRIPT_DIR}/../lambda/gtfs-rt-polling/lambda_function.py"
RT_ZIP="/tmp/gtfs-rt-polling.zip"

echo "    Zipping ${RT_SRC}..."
run zip -j "${RT_ZIP}" "${RT_SRC}"

RT_EXISTS=$(aws lambda get-function --function-name "gtfs-rt-polling" \
    --region "${REGION}" --query 'Configuration.FunctionName' \
    --output text 2>/dev/null || true)

if [[ -z "${RT_EXISTS}" ]]; then
    echo "    Creating Lambda: gtfs-rt-polling (waiting 10s for IAM role propagation)..."
    $DRY_RUN || sleep 10
    run aws lambda create-function \
        --function-name "gtfs-rt-polling" \
        --runtime "python3.12" \
        --handler "lambda_function.lambda_handler" \
        --role "${RT_POLLING_ROLE_ARN}" \
        --zip-file "fileb://${RT_ZIP}" \
        --timeout 30 \
        --memory-size 128 \
        --environment "Variables={S3_BUCKET=${S3_RAW},OBA_API_KEY=${OBA_API_KEY:-REPLACE_ME}}" \
        --description "Fetches GTFS-RT feeds every 1 min and saves .pb files to S3" \
        --region "${REGION}" \
        --output text --query 'FunctionArn'
else
    echo "    Updating code: gtfs-rt-polling"
    run aws lambda update-function-code \
        --function-name "gtfs-rt-polling" \
        --zip-file "fileb://${RT_ZIP}" \
        --region "${REGION}" \
        --output text --query 'FunctionArn'

    $DRY_RUN || aws lambda wait function-updated \
        --function-name "gtfs-rt-polling" --region "${REGION}"

    run aws lambda update-function-configuration \
        --function-name "gtfs-rt-polling" \
        --environment "Variables={S3_BUCKET=${S3_RAW},OBA_API_KEY=${OBA_API_KEY:-REPLACE_ME}}" \
        --region "${REGION}" \
        --output text --query 'FunctionArn'
fi


# ── Step 3c: gtfs-pipeline-notification Lambda ────────────
log "Step 3c: Lambda — gtfs-pipeline-notification"

PN_SRC="${SCRIPT_DIR}/../lambda/gtfs-pipeline-notification/lambda_function.py"
PN_ZIP="/tmp/gtfs-pipeline-notification.zip"

echo "    Zipping ${PN_SRC}..."
run zip -j "${PN_ZIP}" "${PN_SRC}"

PN_EXISTS=$(aws lambda get-function --function-name "gtfs-pipeline-notification" \
    --region "${REGION}" --query 'Configuration.FunctionName' \
    --output text 2>/dev/null || true)

if [[ -z "${PN_EXISTS}" ]]; then
    echo "    Creating Lambda: gtfs-pipeline-notification (waiting 10s for IAM role propagation)..."
    $DRY_RUN || sleep 10
    run aws lambda create-function \
        --function-name "gtfs-pipeline-notification" \
        --runtime "python3.12" \
        --handler "lambda_function.lambda_handler" \
        --role "${PIPELINE_NOTIF_ROLE_ARN}" \
        --zip-file "fileb://${PN_ZIP}" \
        --timeout 30 \
        --memory-size 128 \
        --environment "Variables={SNS_TOPIC_ARN=${DIGEST_SNS_ARN}}" \
        --description "Sends daily GTFS static pipeline summary to SNS digest topic" \
        --region "${REGION}" \
        --output text --query 'FunctionArn'
else
    echo "    Updating code: gtfs-pipeline-notification"
    run aws lambda update-function-code \
        --function-name "gtfs-pipeline-notification" \
        --zip-file "fileb://${PN_ZIP}" \
        --region "${REGION}" \
        --output text --query 'FunctionArn'

    $DRY_RUN || aws lambda wait function-updated \
        --function-name "gtfs-pipeline-notification" --region "${REGION}"

    run aws lambda update-function-configuration \
        --function-name "gtfs-pipeline-notification" \
        --environment "Variables={SNS_TOPIC_ARN=${DIGEST_SNS_ARN}}" \
        --region "${REGION}" \
        --output text --query 'FunctionArn'
fi


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
    "Triggers ${LAMBDA_NAME} when any Glue job enters FAILED, TIMEOUT, or ERROR state" \
    '{"source":["aws.glue"],"detail-type":["Glue Job State Change"],"detail":{"state":["FAILED","TIMEOUT","ERROR"]}}'

_upsert_eb_rule \
    "transit-glue-workflow-failure" \
    "Triggers ${LAMBDA_NAME} when a Glue workflow run enters FAILED, STOPPED, TIMEOUT, or ERROR state" \
    '{"source":["aws.glue"],"detail-type":["Glue Workflow Run Status"],"detail":{"state":["FAILED","STOPPED","TIMEOUT","ERROR"]}}'

# 4b — gtfs-rt-polling-schedule (rate 1 min → gtfs-rt-polling Lambda)
RT_POLLING_ARN="arn:aws:lambda:${REGION}:${ACCOUNT}:function:gtfs-rt-polling"
echo "    Rule: gtfs-rt-polling-schedule"
run aws events put-rule \
    --name "gtfs-rt-polling-schedule" \
    --description "Triggers gtfs-rt-polling Lambda every minute to poll GTFS-RT feeds" \
    --schedule-expression "rate(1 minute)" \
    --state ENABLED \
    --region "${REGION}" \
    --output text --query 'RuleArn'

run aws events put-targets \
    --rule "gtfs-rt-polling-schedule" \
    --targets "Id=gtfs-rt-polling-lambda,Arn=${RT_POLLING_ARN}" \
    --region "${REGION}" \
    --output text

$DRY_RUN || aws lambda remove-permission \
    --function-name "gtfs-rt-polling" \
    --statement-id "eb-gtfs-rt-polling-schedule-invoke" \
    --region "${REGION}" 2>/dev/null || true
run aws lambda add-permission \
    --function-name "gtfs-rt-polling" \
    --statement-id "eb-gtfs-rt-polling-schedule-invoke" \
    --action "lambda:InvokeFunction" \
    --principal "events.amazonaws.com" \
    --source-arn "arn:aws:events:${REGION}:${ACCOUNT}:rule/gtfs-rt-polling-schedule" \
    --region "${REGION}" \
    --output text --query 'Statement'

# 4c — gtfs-static-pipeline-complete → gtfs-pipeline-notification Lambda
PN_ARN="arn:aws:lambda:${REGION}:${ACCOUNT}:function:gtfs-pipeline-notification"
echo "    Rule: gtfs-static-pipeline-complete"
run aws events put-rule \
    --name "gtfs-static-pipeline-complete" \
    --description "Triggers gtfs-pipeline-notification Lambda when gtfs-static-pipeline workflow completes" \
    --event-pattern '{"source":["aws.glue"],"detail-type":["Glue Workflow Run Status"],"detail":{"workflowName":["gtfs-static-pipeline"],"state":["COMPLETED"]}}' \
    --state ENABLED \
    --region "${REGION}" \
    --output text --query 'RuleArn'

run aws events put-targets \
    --rule "gtfs-static-pipeline-complete" \
    --targets "Id=gtfs-pipeline-notification-lambda,Arn=${PN_ARN}" \
    --region "${REGION}" \
    --output text

$DRY_RUN || aws lambda remove-permission \
    --function-name "gtfs-pipeline-notification" \
    --statement-id "eb-gtfs-static-pipeline-complete-invoke" \
    --region "${REGION}" 2>/dev/null || true
run aws lambda add-permission \
    --function-name "gtfs-pipeline-notification" \
    --statement-id "eb-gtfs-static-pipeline-complete-invoke" \
    --action "lambda:InvokeFunction" \
    --principal "events.amazonaws.com" \
    --source-arn "arn:aws:events:${REGION}:${ACCOUNT}:rule/gtfs-static-pipeline-complete" \
    --region "${REGION}" \
    --output text --query 'Statement'


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


# ── Step 5b: GTFS-RT feed health alarms ──────────────────
log "Step 5b: CloudWatch alarms — GTFS-RT feed health"

echo "    Alarm: gtfs-rt-feed-fetch-failures"
run aws cloudwatch put-metric-alarm \
    --alarm-name "gtfs-rt-feed-fetch-failures" \
    --alarm-description "One or more GTFS-RT feeds failed to fetch — OBA API may be down or returning empty responses" \
    --namespace "TransitDW/GtfsRt" \
    --metric-name "FeedFetchFailure" \
    --statistic Sum \
    --period 300 \
    --evaluation-periods 1 \
    --threshold 1 \
    --comparison-operator GreaterThanOrEqualToThreshold \
    --treat-missing-data notBreaching \
    --alarm-actions "${FAILURE_SNS_ARN}" \
    --ok-actions "${FAILURE_SNS_ARN}" \
    --region "${REGION}"

echo "    Alarm: gtfs-rt-feed-complete-outage"
run aws cloudwatch put-metric-alarm \
    --alarm-name "gtfs-rt-feed-complete-outage" \
    --alarm-description "No GTFS-RT feeds successfully fetched for 25+ minutes — complete polling outage" \
    --namespace "TransitDW/GtfsRt" \
    --metric-name "FeedFetchSuccess" \
    --statistic Sum \
    --period 300 \
    --evaluation-periods 5 \
    --threshold 1 \
    --comparison-operator LessThanThreshold \
    --treat-missing-data breaching \
    --alarm-actions "${FAILURE_SNS_ARN}" \
    --ok-actions "${FAILURE_SNS_ARN}" \
    --region "${REGION}"


# ── Step 5c: Pipeline notification invocation alarm ───────
log "Step 5c: CloudWatch alarm — pipeline notification invocation"

echo "    Alarm: gtfs-pipeline-notification-not-invoked"
run aws cloudwatch put-metric-alarm \
    --alarm-name "gtfs-pipeline-notification-not-invoked" \
    --alarm-description "gtfs-pipeline-notification Lambda did not run in the past 24h — static pipeline may not have completed or trigger is broken" \
    --namespace "AWS/Lambda" \
    --metric-name "Invocations" \
    --dimensions "Name=FunctionName,Value=gtfs-pipeline-notification" \
    --statistic Sum \
    --period 86400 \
    --evaluation-periods 1 \
    --threshold 1 \
    --comparison-operator LessThanThreshold \
    --treat-missing-data breaching \
    --alarm-actions "${FAILURE_SNS_ARN}" \
    --region "${REGION}"

echo "    Alarm: gtfs-static-pipeline-lambda-errors"
run aws cloudwatch put-metric-alarm \
    --alarm-name "gtfs-static-pipeline-lambda-errors" \
    --alarm-description "gtfs-pipeline-notification Lambda is erroring — daily pipeline summary may not be sending" \
    --namespace "AWS/Lambda" \
    --metric-name "Errors" \
    --dimensions "Name=FunctionName,Value=gtfs-pipeline-notification" \
    --statistic Sum \
    --period 300 \
    --evaluation-periods 1 \
    --threshold 1 \
    --comparison-operator GreaterThanOrEqualToThreshold \
    --treat-missing-data notBreaching \
    --alarm-actions "${FAILURE_SNS_ARN}" \
    --ok-actions "${FAILURE_SNS_ARN}" \
    --region "${REGION}"


# ── Step 5d: SNS delivery failure alarms ──────────────────
log "Step 5d: CloudWatch alarms — SNS delivery failures"

for sns_topic_name in "transit-failure-alerts" "transit-daily-digest"; do
    echo "    Alarm: sns-delivery-failures-${sns_topic_name}"
    run aws cloudwatch put-metric-alarm \
        --alarm-name "sns-delivery-failures-${sns_topic_name}" \
        --alarm-description "SNS topic ${sns_topic_name} has delivery failures — subscribers may not be receiving notifications" \
        --namespace "AWS/SNS" \
        --metric-name "NumberOfNotificationsFailed" \
        --dimensions "Name=TopicName,Value=${sns_topic_name}" \
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
echo "  SNS topics :"
echo "    ${FAILURE_SNS_ARN}"
echo "    ${DIGEST_SNS_ARN}"
echo "  Lambdas    :"
echo "    transit-failure-notifier"
echo "    gtfs-rt-polling"
echo "    gtfs-pipeline-notification"
echo "  EB rules   :"
echo "    transit-glue-job-failure"
echo "    transit-glue-workflow-failure"
echo "    gtfs-rt-polling-schedule"
echo "    gtfs-static-pipeline-complete"
echo "  CW Alarms  :"
for fn_name in "${MONITORED_LAMBDAS[@]}"; do
    echo "    transit-lambda-errors-${fn_name}"
done
echo "    gtfs-rt-feed-fetch-failures"
echo "    gtfs-rt-feed-complete-outage"
echo "    gtfs-pipeline-notification-not-invoked"
echo "    gtfs-static-pipeline-lambda-errors"
echo "    sns-delivery-failures-transit-failure-alerts"
echo "    sns-delivery-failures-transit-daily-digest"
[[ $DRY_RUN == true ]] && echo ""  && echo "  DRY-RUN — no changes were applied"
echo "========================================================"
