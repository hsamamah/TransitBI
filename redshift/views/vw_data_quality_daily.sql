-- View: dw.vw_data_quality_daily
-- Extracted from Redshift Serverless workgroup: team, database: dev
-- Extracted: 2026-04-03

CREATE OR REPLACE VIEW dw.vw_data_quality_daily AS
SELECT dd.fulldate, count(*) AS total, sum(CASE WHEN (fs.isofficial = true) THEN 1 ELSE 0 END) AS official, sum(CASE WHEN (fs.tripkey = 0) THEN 1 ELSE 0 END) AS unmatched_trip FROM (dw.factstop fs JOIN dw.dimdate dd ON ((dd.datekey = fs.datekey))) GROUP BY dd.fulldate;;
