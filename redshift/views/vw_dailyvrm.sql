-- View: dw.vw_dailyvrm
-- Extracted from Redshift Serverless workgroup: team, database: dev
-- Extracted: 2026-04-03

CREATE OR REPLACE VIEW dw.vw_dailyvrm AS
SELECT fsd.agencykey, a.agencyname, a."mode", dd.fulldate, dd.calendaryear, dd.calendarmonth, dd.federalfiscalmonth, fsd.reportedvrm, fsd.estimatedvrm, (COALESCE(fsd.reportedvrm, ((0)::numeric)::numeric(18,0)) + COALESCE(fsd.estimatedvrm, ((0)::numeric)::numeric(18,0))) AS total_vrm_including_estimated, fsd.isofficial FROM ((dw.factserviceday fsd JOIN dw.dimagency a ON ((fsd.agencykey = a.agencykey))) JOIN dw.dimdate dd ON ((fsd.datekey = dd.datekey))) WHERE (fsd.isspecialeventday = false);;
