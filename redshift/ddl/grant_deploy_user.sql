-- =============================================================
-- grant_deploy_user.sql
-- =============================================================
-- Grants all permissions needed by IAM:hani-admin to:
--   • Run deploy_redshift.sh  (DDL, views)
--   • Run diagnostic queries  (SELECT on stg.*, dw.*)
--   • Replace views           (ownership transfer)
--   • Run Glue ETL jobs       (TransitGlueRole)
--   • Support team members    (lingli_yang, minglei_ma, poojith)
--   • Support QuickSight      (quicksight_user read-only on dw)
--
-- Run this as the 'admin' superuser in Redshift Query Editor v2.
-- Safe to re-run — all statements are idempotent.
-- Re-run any time a new table or view is added to stg or dw.
-- =============================================================


-- ── 1. Database-level ─────────────────────────────────────────
-- Allow hani-admin to create new schemas if needed
GRANT CREATE ON DATABASE dev TO "IAM:hani-admin";


-- ── 2. Schema-level ──────────────────────────────────────────
-- USAGE  = can reference objects in the schema
-- CREATE = can CREATE TABLE / CREATE VIEW in the schema
GRANT USAGE, CREATE ON SCHEMA stg TO "IAM:hani-admin";
GRANT USAGE, CREATE ON SCHEMA dw  TO "IAM:hani-admin";


-- ── 3. Existing tables and views ─────────────────────────────
-- Covers every table/view that exists right now.
-- Re-run this block after adding new tables.
GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA stg TO "IAM:hani-admin";
GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA dw  TO "IAM:hani-admin";


-- ── 4. Future tables and views ───────────────────────────────
-- Ensures any table/view created by admin in the future is
-- automatically accessible to hani-admin without re-running grants.
ALTER DEFAULT PRIVILEGES FOR USER admin IN SCHEMA stg
    GRANT ALL PRIVILEGES ON TABLES TO "IAM:hani-admin";
ALTER DEFAULT PRIVILEGES FOR USER admin IN SCHEMA dw
    GRANT ALL PRIVILEGES ON TABLES TO "IAM:hani-admin";


-- ── 5. View ownership transfer ───────────────────────────────
-- Required so hani-admin can run CREATE OR REPLACE VIEW.
-- Redshift requires the issuing user to OWN the view to replace it.
-- These views were originally created by 'admin'; transfer ownership
-- to hani-admin so deploy_redshift.sh can update them without a
-- superuser session.
ALTER TABLE dw.vw_otp_by_route_month        OWNER TO "IAM:hani-admin";
ALTER TABLE dw.vw_dailyvrm                  OWNER TO "IAM:hani-admin";
ALTER TABLE dw.vw_dailyvrh                  OWNER TO "IAM:hani-admin";
ALTER TABLE dw.v_missed_trip_rate_by_route  OWNER TO "IAM:hani-admin";
ALTER TABLE dw.v_routes_consistently_late   OWNER TO "IAM:hani-admin";
ALTER TABLE dw.v_voms                       OWNER TO "IAM:hani-admin";
ALTER TABLE dw.vw_data_quality_daily        OWNER TO "IAM:hani-admin";
ALTER TABLE dw.vw_dataqualityalert          OWNER TO "IAM:hani-admin";
ALTER TABLE dw.vw_missedtriptrend           OWNER TO "IAM:hani-admin";
ALTER TABLE dw.vw_monthlyntdsummary         OWNER TO "IAM:hani-admin";


-- =============================================================
-- Other principals (re-apply when new tables are added)
-- =============================================================

-- ── TransitGlueRole — full read/write (all Glue ETL jobs) ────
GRANT USAGE ON SCHEMA stg TO "IAMR:TransitGlueRole";
GRANT USAGE ON SCHEMA dw  TO "IAMR:TransitGlueRole";
GRANT SELECT, INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA stg TO "IAMR:TransitGlueRole";
GRANT SELECT, INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA dw  TO "IAMR:TransitGlueRole";
ALTER DEFAULT PRIVILEGES FOR USER admin IN SCHEMA stg
    GRANT SELECT, INSERT, UPDATE, DELETE ON TABLES TO "IAMR:TransitGlueRole";
ALTER DEFAULT PRIVILEGES FOR USER admin IN SCHEMA dw
    GRANT SELECT, INSERT, UPDATE, DELETE ON TABLES TO "IAMR:TransitGlueRole";

-- ── Team members — full read/write on stg and dw ─────────────
GRANT USAGE ON SCHEMA stg TO "IAM:lingli_yang", "IAM:minglei_ma", "IAM:poojith";
GRANT USAGE ON SCHEMA dw  TO "IAM:lingli_yang", "IAM:minglei_ma", "IAM:poojith";
GRANT SELECT, INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA stg TO "IAM:lingli_yang", "IAM:minglei_ma", "IAM:poojith";
GRANT SELECT, INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA dw  TO "IAM:lingli_yang", "IAM:minglei_ma", "IAM:poojith";
ALTER DEFAULT PRIVILEGES FOR USER admin IN SCHEMA stg
    GRANT SELECT, INSERT, UPDATE, DELETE ON TABLES TO "IAM:lingli_yang", "IAM:minglei_ma", "IAM:poojith";
ALTER DEFAULT PRIVILEGES FOR USER admin IN SCHEMA dw
    GRANT SELECT, INSERT, UPDATE, DELETE ON TABLES TO "IAM:lingli_yang", "IAM:minglei_ma", "IAM:poojith";

-- ── QuickSight — read-only on dw ─────────────────────────────
GRANT USAGE ON SCHEMA dw TO quicksight_user;
GRANT SELECT ON ALL TABLES IN SCHEMA dw TO quicksight_user;
ALTER DEFAULT PRIVILEGES FOR USER admin IN SCHEMA dw
    GRANT SELECT ON TABLES TO quicksight_user;
