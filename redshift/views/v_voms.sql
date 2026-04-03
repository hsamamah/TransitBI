-- View: dw.v_voms
-- Extracted from Redshift Serverless workgroup: team, database: dev
-- Extracted: 2026-04-03

CREATE OR REPLACE VIEW dw.v_voms AS
SELECT fsd.datekey, dd.fulldate, dd.calendarmonth, dd.calendaryear, dd.federalfiscalmonth, fsd.agencykey, da.agencyname, da."mode", fsd.peakvehiclecount, fsd.peaktimekey, dt.timevalue AS peak_time, dt.periodofday AS peak_period, "max"(fsd.peakvehiclecount) OVER(  PARTITION BY fsd.agencykey, da."mode", dd.calendaryear, dd.calendarmonth ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS monthly_voms, "max"(fsd.peakvehiclecount) OVER(  PARTITION BY fsd.agencykey, da."mode", dd.calendaryear, dd.federalfiscalmonth ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS ntd_period_voms FROM (((dw.factserviceday fsd JOIN dw.dimdate dd ON ((fsd.datekey = dd.datekey))) JOIN dw.dimagency da ON ((fsd.agencykey = da.agencykey))) LEFT JOIN dw.dimtime dt ON ((fsd.peaktimekey = dt.timekey))) WHERE ((fsd.isspecialeventday = false) AND (fsd.operatedtrips > 0));;
