-- One-time grants required for deploy_redshift.sh to run as IAM:hani-admin
-- Run this as the 'admin' superuser in Redshift Query Editor v2.
-- Only needs to be run once per environment setup.

-- Allow IAM:hani-admin to create schemas and use the database
GRANT CREATE ON DATABASE dev TO "IAM:hani-admin";

-- Allow IAM:hani-admin full access to existing schemas
GRANT ALL ON SCHEMA stg TO "IAM:hani-admin";
GRANT ALL ON SCHEMA dw  TO "IAM:hani-admin";

-- Allow IAM:hani-admin to create objects in these schemas
ALTER DEFAULT PRIVILEGES IN SCHEMA stg GRANT ALL ON TABLES TO "IAM:hani-admin";
ALTER DEFAULT PRIVILEGES IN SCHEMA dw  GRANT ALL ON TABLES TO "IAM:hani-admin";

-- Allow IAM:hani-admin to create views
GRANT ALL ON ALL TABLES IN SCHEMA stg TO "IAM:hani-admin";
GRANT ALL ON ALL TABLES IN SCHEMA dw  TO "IAM:hani-admin";
