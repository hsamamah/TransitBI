-- View: dw.vw_dailyvrh
-- Extracted from Redshift Serverless workgroup: team, database: dev
-- Extracted: 2026-04-03

CREATE OR REPLACE VIEW dw.vw_dailyvrh AS
SELECT fsd.agencykey, a.agencyname, a."mode", dd.fulldate, dd.calendaryear, dd.calendarmonth, dd.federalfiscalmonth, fsd.reportedvrh, fsd.estimatedvrh, (COALESCE(fsd.reportedvrh, ((0)::numeric)::numeric(18,0)) + COALESCE(fsd.estimatedvrh, ((0)::numeric)::numeric(18,0))) AS total_vrh_including_estimated, fsd.isofficial FROM ((dw.factserviceday fsd JOIN dw.dimagency a ON ((fsd.agencykey = a.agencykey))) JOIN dw.dimdate dd ON ((fsd.datekey = dd.datekey))) WHERE (fsd.isspecialeventday = false);;
