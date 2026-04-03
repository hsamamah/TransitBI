-- View: dw.vw_dataqualityalert
-- Extracted from Redshift Serverless workgroup: team, database: dev
-- Extracted: 2026-04-03

CREATE OR REPLACE VIEW dw.vw_dataqualityalert AS
SELECT dd.fulldate, a.agencyname, a."mode", fsd.scheduledtrips, fsd.operatedtrips, fsd.dataqualityalertflag, fsd.feedgapflag, fsd.isofficial FROM ((dw.factserviceday fsd JOIN dw.dimagency a ON ((fsd.agencykey = a.agencykey))) JOIN dw.dimdate dd ON ((fsd.datekey = dd.datekey))) WHERE ((fsd.dataqualityalertflag = true) OR (fsd.feedgapflag = true));;
