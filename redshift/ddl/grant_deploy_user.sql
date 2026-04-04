-- One-time grants required for deploy_redshift.sh to run as IAM:hani-admin
-- Run this as the 'admin' superuser in Redshift Query Editor v2.
-- Only needs to be run once per environment setup.

-- Allow IAM:hani-admin to create schemas and use the database
GRANT CREATE ON DATABASE dev TO "IAM:hani-admin";

-- Allow IAM:hani-admin full access to existing schemas
GRANT ALL ON SCHEMA stg TO "IAM:hani-admin";
GRANT ALL ON SCHEMA dw  TO "IAM:hani-admin";

-- Allow IAM:hani-admin to manage objects in these schemas
ALTER DEFAULT PRIVILEGES IN SCHEMA stg GRANT ALL ON TABLES TO "IAM:hani-admin";
ALTER DEFAULT PRIVILEGES IN SCHEMA dw  GRANT ALL ON TABLES TO "IAM:hani-admin";

-- Allow IAM:hani-admin full access to all existing tables
GRANT ALL ON ALL TABLES IN SCHEMA stg TO "IAM:hani-admin";
GRANT ALL ON ALL TABLES IN SCHEMA dw  TO "IAM:hani-admin";

-- Allow IAM:hani-admin to grant privileges to other users (needed for GRANT steps in deploy)
-- This requires superuser; if not possible, the GRANT steps will warn-and-skip (harmless —
-- the grants listed below are already applied in the live environment).

-- ============================================================
-- Live environment grants (already applied by RootIdentity)
-- These are documented here for reference and re-apply if the
-- environment is rebuilt from scratch by a superuser.
-- ============================================================

-- Schema USAGE
GRANT USAGE ON SCHEMA stg TO "IAMR:TransitGlueRole", "IAM:lingli_yang", "IAM:minglei_ma", "IAM:poojith";
GRANT USAGE ON SCHEMA dw  TO "IAMR:TransitGlueRole", "IAM:lingli_yang", "IAM:minglei_ma", "IAM:poojith", quicksight_user;

-- TransitGlueRole — full read/write (needed by all Glue ETL jobs)
GRANT SELECT, INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA stg TO "IAMR:TransitGlueRole";
GRANT SELECT, INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA dw  TO "IAMR:TransitGlueRole";
ALTER DEFAULT PRIVILEGES IN SCHEMA stg GRANT SELECT, INSERT, UPDATE, DELETE ON TABLES TO "IAMR:TransitGlueRole";
ALTER DEFAULT PRIVILEGES IN SCHEMA dw  GRANT SELECT, INSERT, UPDATE, DELETE ON TABLES TO "IAMR:TransitGlueRole";

-- Team members — full read/write on stg and dw
GRANT SELECT, INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA stg TO "IAM:lingli_yang", "IAM:minglei_ma", "IAM:poojith";
GRANT SELECT, INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA dw  TO "IAM:lingli_yang", "IAM:minglei_ma", "IAM:poojith";

-- QuickSight — read-only on dw
GRANT SELECT ON ALL TABLES IN SCHEMA dw TO quicksight_user;
ALTER DEFAULT PRIVILEGES IN SCHEMA dw GRANT SELECT ON TABLES TO quicksight_user;
