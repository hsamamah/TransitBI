import boto3
import json
import os
from datetime import datetime
from boto3.dynamodb.conditions import Key

sns = boto3.client('sns', region_name='us-west-2')
dynamodb = boto3.resource('dynamodb', region_name='us-west-2')
table = dynamodb.Table('seattle-transit-pipeline')

SNS_TOPIC_ARN = os.environ['SNS_TOPIC_ARN']

def get_pipeline_run(today_str: str) -> dict:
    """Get latest pipeline run record from DynamoDB."""
    try:
        response = table.query(
            KeyConditionExpression=Key('PK').eq(f'PIPELINE_RUN#{today_str}')
        )
        runs = response.get('Items', [])
        if runs:
            # Get the latest run by timestamp
            latest = sorted(runs, key=lambda x: x['SK'])[-1]
            return {
                'status': latest.get('status', 'UNKNOWN'),
                'is_new_version': latest.get('is_new_version', False),
                'file_count': latest.get('file_count', 0),
                'timestamp': latest.get('SK', 'UNKNOWN'),
                'error': latest.get('error', None)
            }
    except Exception as e:
        print(f"Could not read pipeline run: {e}")
    return {}

def get_validation_summary(today_str: str) -> dict:
    """Get validation summary record from DynamoDB."""
    try:
        response = table.get_item(
            Key={
                'PK': f'VALIDATION#{today_str}',
                'SK': 'SUMMARY'
            }
        )
        item = response.get('Item', {})
        if item:
            return {
                'overall_passed': item.get('overall_passed', False),
                'passed': item.get('passed', 0),
                'total': item.get('total_tables', 0),
                'failed_tables': item.get('failed_tables', [])
            }
    except Exception as e:
        print(f"Could not read validation summary: {e}")
    return {}

def get_fallback_events(today_str: str) -> list:
    """Get any fallback events from DynamoDB."""
    try:
        response = table.query(
            KeyConditionExpression=Key('PK').eq(f'FALLBACK#{today_str}')
        )
        return response.get('Items', [])
    except Exception as e:
        print(f"Could not read fallback events: {e}")
    return []

def get_feed_version(today_str: str) -> dict:
    """Get feed version status from DynamoDB."""
    try:
        response = table.query(
            KeyConditionExpression=Key('PK').eq(f'FEED#{today_str}')
        )
        items = response.get('Items', [])
        if items:
            item = items[0]
            return {
                'hash': item.get('feed_hash', '')[:12],
                'is_new_version': item.get('is_active', False),
                'staged_path': item.get('staged_path', ''),
                'file_count': item.get('file_count', 0)
            }
    except Exception as e:
        print(f"Could not read feed version: {e}")
    return {}

def build_message(today_str: str, pipeline_run: dict, 
                   validation: dict, fallbacks: list, 
                   feed_version: dict) -> str:
    """Build the notification email message."""
    
    # Pipeline run section
    run_status = pipeline_run.get('status', 'UNKNOWN')
    run_emoji = '✅' if run_status == 'SUCCEEDED' else '❌'
    run_time = pipeline_run.get('timestamp', 'UNKNOWN')
    run_error = pipeline_run.get('error', None)

    # Feed version section
    is_new = pipeline_run.get('is_new_version', False)
    version_emoji = '🆕' if is_new else '✅'
    version_status = 'New version detected' if is_new else 'Feed unchanged since last run'
    feed_hash = feed_version.get('hash', 'UNKNOWN')
    file_count = pipeline_run.get('file_count', 0)

    # Validation section
    val_passed = validation.get('overall_passed', False)
    val_emoji = '✅' if val_passed else '⚠️'
    tables_passed = validation.get('passed', 0)
    tables_total = validation.get('total', 0)
    failed_tables = validation.get('failed_tables', [])

    # Fallback section
    fallback_emoji = '⚠️' if fallbacks else '✅'
    fallback_status = 'No fallback needed' if not fallbacks else f'FALLBACK USED — data from {fallbacks[0].get("fallback_date", "UNKNOWN")}'
    fallback_reason = fallbacks[0].get('reason', '') if fallbacks else ''

    message = f"""
╔══════════════════════════════════════════════╗
   Seattle Transit GTFS Pipeline — Daily Report
   Date: {today_str}
╚══════════════════════════════════════════════╝

{run_emoji} PIPELINE RUN
   Status:    {run_status}
   Time:      {run_time} UTC
   {"Error:     " + run_error if run_error else ""}

{version_emoji} FEED VERSION
   Status:    {version_status}
   Hash:      {feed_hash}...
   Files:     {file_count} files staged

{val_emoji} VALIDATION
   Status:    {"PASSED" if val_passed else "ISSUES FOUND"}
   Tables:    {tables_passed}/{tables_total} passed
   {"Failed:    " + str(failed_tables) if failed_tables else "Failed:    None"}

{fallback_emoji} FALLBACK
   Status:    {fallback_status}
   {"Reason:    " + fallback_reason if fallback_reason else ""}

──────────────────────────────────────────────
Fresh data is available in S3 staging:
s3://seattle-transit-staging/gtfs-static/combined/{today_str.replace("-", "/")}/

✅ P2 can begin dimension table loading.
──────────────────────────────────────────────
    """
    return message

def lambda_handler(event, context):
    today_str = datetime.now().strftime('%Y-%m-%d')
    print(f"Sending pipeline notification for {today_str}")

    # Gather data from DynamoDB
    pipeline_run = get_pipeline_run(today_str)
    validation = get_validation_summary(today_str)
    fallbacks = get_fallback_events(today_str)
    feed_version = get_feed_version(today_str)

    # Build message
    message = build_message(
        today_str=today_str,
        pipeline_run=pipeline_run,
        validation=validation,
        fallbacks=fallbacks,
        feed_version=feed_version
    )

    # Determine subject line
    run_status = pipeline_run.get('status', 'UNKNOWN')
    val_passed = validation.get('overall_passed', False)
    
    if run_status == 'SUCCEEDED' and val_passed:
        subject = f'✅ Seattle Transit GTFS Pipeline — {today_str} — All Good'
    elif run_status == 'SUCCEEDED' and not val_passed:
        subject = f'⚠️ Seattle Transit GTFS Pipeline — {today_str} — Validation Issues'
    else:
        subject = f'❌ Seattle Transit GTFS Pipeline — {today_str} — Pipeline Failed'

    # Send notification
    sns.publish(
        TopicArn=SNS_TOPIC_ARN,
        Subject=subject,
        Message=message
    )

    print(f"Notification sent: {subject}")
    return {
        'statusCode': 200,
        'body': json.dumps({'message': 'Notification sent', 'subject': subject})
    }