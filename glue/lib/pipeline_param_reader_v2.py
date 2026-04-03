"""
Pipeline Parameter Reader  v2
==============================
Shared utility for FactStop, FactTrip, and FactServiceDay jobs to read
their parameters from DynamoDB as set by transit-pipeline-inspector.

Usage in each fact job:

    from pipeline_param_reader import read_pipeline_params
    config = read_pipeline_params(JOB_NAME)
    dates  = config['dates']
    phase  = config['phase']
    force  = config['force']
    skip   = config['skip']

Returns dict keys:
  dates  : list of YYYY-MM-DD strings to process
           When source=dynamodb, contains only the gap dates written
           by the inspector — not every date in start→end range.
  phase  : 'skeleton' | 'merge' | 'both' | 'load' | 'none'
  force  : bool
  skip   : bool
  source : 'direct_params' | 'dynamodb' | 'default_yesterday'

Priority order:
  1. --start_date + --end_date  (direct override, bypasses DynamoDB)
  2. --WORKFLOW_RUN_ID           (reads DynamoDB config from inspector)
  3. Default: yesterday

Glue deployment:
  Upload this file to S3 and add to each fact job definition:
    --extra-py-files s3://seattle-transit-staging/glue-scripts/pipeline_param_reader.py

Changelog v2:
  - Fixed exception chaining — raise ... from e on DynamoDB failures
  - Fixed _build_date_list type hint — uses proper date import
  - When source=dynamodb, dates list is populated from gap_dates JSON
    (actual gap dates) rather than expanding the full start→end range

Author: Transit DW Team — P1
"""

import sys
import json
import boto3
from datetime import datetime, timedelta, date
import pytz
from awsglue.utils import getResolvedOptions

REGION         = 'us-west-2'
DYNAMODB_TABLE = 'seattle-transit-pipeline'
LOCAL_TZ       = pytz.timezone('America/Los_Angeles')

dynamodb  = boto3.resource('dynamodb', region_name=REGION)
ddb_table = dynamodb.Table(DYNAMODB_TABLE)


def read_pipeline_params(job_name: str) -> dict:
    """
    Resolve job parameters in priority order.
    See module docstring for full details.
    """
    args_available = set()
    for arg in ['start_date', 'end_date', 'phase', 'force', 'WORKFLOW_RUN_ID']:
        try:
            getResolvedOptions(sys.argv, [arg])
            args_available.add(arg)
        except Exception:
            pass

    force = False
    if 'force' in args_available:
        force_val = getResolvedOptions(sys.argv, ['force'])['force'].lower()
        force = force_val in ('true', '1', 'yes')

    # Priority 1: direct date + phase override — bypass DynamoDB entirely
    if 'start_date' in args_available and 'end_date' in args_available:
        resolved = getResolvedOptions(sys.argv, ['start_date', 'end_date'])
        start    = datetime.strptime(resolved['start_date'], '%Y-%m-%d').date()
        end      = datetime.strptime(resolved['end_date'],   '%Y-%m-%d').date()
        phase    = 'both'
        if 'phase' in args_available:
            phase = getResolvedOptions(sys.argv, ['phase'])['phase'].lower()
        return {
            'dates'  : _build_date_list(start, end),
            'phase'  : phase,
            'force'  : force,
            'skip'   : False,
            'source' : 'direct_params',
        }

    # Priority 2: DynamoDB via workflow_run_id
    if 'WORKFLOW_RUN_ID' in args_available:
        workflow_run_id = getResolvedOptions(
            sys.argv, ['WORKFLOW_RUN_ID'])['WORKFLOW_RUN_ID']

        try:
            resp = ddb_table.get_item(Key={
                'pipeline_job_key': f"{workflow_run_id}#{job_name}",
                'param_key':        'config'
            })
            item = resp.get('Item')
            if not item:
                raise RuntimeError(
                    f"No DynamoDB config found for {workflow_run_id}#{job_name}"
                )
        except Exception as e:
            raise RuntimeError(
                f"DynamoDB read failed for {job_name}: {e}"
            ) from e                              # v2: chain exception for full traceback

        if item.get('skip') == 'true':
            print(f"  DynamoDB skip=true for {job_name} — nothing to do.")
            return {
                'dates'  : [],
                'phase'  : 'none',
                'force'  : force,
                'skip'   : True,
                'source' : 'dynamodb',
            }

        # Use gap_dates list from inspector (actual gap dates only)
        # rather than expanding the full start→end range which may
        # include clean dates between two non-contiguous gaps.
        gap_dates_raw = item.get('gap_dates', '[]')
        gap_dates     = json.loads(gap_dates_raw) if gap_dates_raw else []

        if gap_dates:
            dates = sorted(gap_dates)
        else:
            # Fallback: expand start→end if gap_dates missing (older inspector runs)
            start = datetime.strptime(item['start_date'], '%Y-%m-%d').date()
            end   = datetime.strptime(item['end_date'],   '%Y-%m-%d').date()
            dates = _build_date_list(start, end)

        return {
            'dates'  : dates,
            'phase'  : item.get('phase', 'both'),
            'force'  : force,
            'skip'   : False,
            'source' : 'dynamodb',
        }

    # Priority 3: default — process yesterday
    yesterday = (datetime.now(LOCAL_TZ) - timedelta(days=1)).date()
    return {
        'dates'  : [yesterday.strftime('%Y-%m-%d')],
        'phase'  : 'both',
        'force'  : force,
        'skip'   : False,
        'source' : 'default_yesterday',
    }


def _build_date_list(start: date, end: date) -> list:
    """Build a list of YYYY-MM-DD strings from start to end inclusive."""
    dates   = []
    current = start
    while current <= end:
        dates.append(current.strftime('%Y-%m-%d'))
        current += timedelta(days=1)
    return dates
