# IAM Roles

## TransitGlueRole
Used by all Glue jobs.

**Attached policies:**
- `AWSGlueServiceRole` (AWS managed)
- `AmazonS3FullAccess` (AWS managed)
- `CloudWatchFullAccess` / `CloudWatchFullAccessV2` (AWS managed)
- `IAMFullAccess` (AWS managed)
- `RedshiftDataAPIPolicy` (customer managed, ARN: arn:aws:iam::805699509606:policy/RedshiftDataAPIPolicy)

**Inline policies:**
- `TransitDynamoDBAccess` → see `TransitGlueRole_policy_TransitDynamoDBAccess.json`
- `TransitPipelineDynamoDBAccess` → see `TransitGlueRole_policy_TransitPipelineDynamoDBAccess.json`

## RedshiftS3CopyRole
Used by Redshift Serverless for COPY from S3.

Trust relationship: `redshift.amazonaws.com`
