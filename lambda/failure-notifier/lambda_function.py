"""
Transit Failure Notifier
========================
Receives EventBridge events for Glue job failures and Glue workflow failures,
enriches them via the Glue API, and publishes a formatted alert to SNS.

Triggered by:
  - EventBridge: Glue Job State Change (state=FAILED)
  - EventBridge: Glue Workflow Run Status (state=FAILED or STOPPED)

CloudWatch Alarms for Lambda function errors route directly to the same
SNS topic without passing through this Lambda.

Environment variables:
  SNS_TOPIC_ARN  — ARN of the transit-failure-alerts SNS topic
  REGION         — AWS region (default: us-west-2)
"""

import boto3
import json
import os
from datetime import datetime, timezone

REGION        = os.environ.get('REGION', 'us-west-2')
SNS_TOPIC_ARN = os.environ['SNS_TOPIC_ARN']

sns  = boto3.client('sns',  region_name=REGION)
glue = boto3.client('glue', region_name=REGION)

CW_LOGS_BASE = (
    'https://{region}.console.aws.amazon.com/cloudwatch/home'
    '?region={region}#logsV2:log-groups/log-group/'
    '%2Faws-glue%2Fjobs%2Ferror/log-events/{run_id}'
)


def _fmt_time(ts) -> str:
    """Return a readable UTC timestamp string from a datetime or ISO string."""
    if ts is None:
        return 'unknown'
    if isinstance(ts, datetime):
        return ts.strftime('%Y-%m-%d %H:%M:%S UTC')
    return str(ts)


def _get_job_run_error(job_name: str, run_id: str) -> str:
    """Fetch the error message for a specific Glue job run."""
    try:
        resp = glue.get_job_run(JobName=job_name, RunId=run_id)
        return resp.get('JobRun', {}).get('ErrorMessage', 'No error message available')
    except Exception as e:
        return f'(Could not fetch error details: {e})'


def handle_job_failure(detail: dict) -> tuple[str, str]:
    """Build subject + message for a Glue Job State Change (FAILED) event."""
    job_name   = detail.get('jobName', 'unknown')
    run_id     = detail.get('jobRunId', '')
    state      = detail.get('state', 'FAILED')
    started    = _fmt_time(detail.get('startedOn'))
    completed  = _fmt_time(detail.get('completedOn'))

    error_msg  = _get_job_run_error(job_name, run_id)
    cw_url     = CW_LOGS_BASE.format(region=REGION, run_id=run_id)

    subject = f'[FAILED] Glue Job: {job_name}'

    message = f"""
╔══════════════════════════════════════════════╗
   Seattle Transit DW — Glue Job Failure Alert
╚══════════════════════════════════════════════╝

Job:       {job_name}
State:     {state}
Run ID:    {run_id}
Started:   {started}
Failed:    {completed}

Error:
  {error_msg}

CloudWatch Logs:
  {cw_url}

──────────────────────────────────────────────
Check the logs above for the full stack trace.
──────────────────────────────────────────────
""".strip()

    return subject, message


def handle_workflow_failure(detail: dict) -> tuple[str, str]:
    """Build subject + message for a Glue Workflow Run Status (FAILED/STOPPED) event."""
    workflow   = detail.get('workflowName', 'unknown')
    run_id     = detail.get('runId', '')
    state      = detail.get('state', 'FAILED')
    started    = _fmt_time(detail.get('startedOn'))
    completed  = _fmt_time(detail.get('completedOn'))

    subject = f'[{state}] Glue Workflow: {workflow}'

    message = f"""
╔══════════════════════════════════════════════╗
   Seattle Transit DW — Glue Workflow Alert
╚══════════════════════════════════════════════╝

Workflow:  {workflow}
State:     {state}
Run ID:    {run_id}
Started:   {started}
Ended:     {completed}

One or more jobs in this workflow failed or the run was stopped.
Check individual job alerts above for error details, or review
the workflow run in the Glue console:
  https://{REGION}.console.aws.amazon.com/glue/home?region={REGION}#/etl/workflows/runs/{workflow}

──────────────────────────────────────────────
""".strip()

    return subject, message


def lambda_handler(event, context):
    print(json.dumps(event))

    detail_type = event.get('detail-type', '')
    detail      = event.get('detail', {})

    if detail_type == 'Glue Job State Change':
        subject, message = handle_job_failure(detail)
    elif detail_type == 'Glue Workflow Run Status':
        subject, message = handle_workflow_failure(detail)
    else:
        print(f'Unrecognised detail-type: {detail_type!r} — skipping')
        return {'statusCode': 200, 'body': 'skipped'}

    sns.publish(TopicArn=SNS_TOPIC_ARN, Subject=subject, Message=message)
    print(f'Alert sent: {subject}')

    return {'statusCode': 200, 'body': json.dumps({'subject': subject})}
