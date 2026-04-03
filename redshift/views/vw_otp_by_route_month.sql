-- View: dw.vw_otp_by_route_month
-- Extracted from Redshift Serverless workgroup: team, database: dev
-- Extracted: 2026-04-03

CREATE OR REPLACE VIEW dw.vw_otp_by_route_month AS
SELECT fs.agencykey, a.agencyname, a."mode", fs.routekey, dr.routeid, dr.routeshortname, dr.routelongname, dd.calendaryear, dd.calendarmonth, count(*) AS total_obs, sum(CASE WHEN (fs.isontime = true) THEN 1 ELSE 0 END) AS ontime, sum(CASE WHEN (fs.islate = true) THEN 1 ELSE 0 END) AS late, sum(CASE WHEN (fs.isearly = true) THEN 1 ELSE 0 END) AS early, round(((100.0 * ((sum(CASE WHEN (fs.isontime = true) THEN 1 ELSE 0 END))::numeric)::numeric(18,0)) / ((CASE WHEN (count(*) = 0) THEN NULL::bigint ELSE count(*) END)::numeric)::numeric(18,0)), 1) AS otp_pct FROM (((dw.factstop fs JOIN dw.dimroute dr ON ((dr.routekey = fs.routekey))) JOIN dw.dimagency a ON ((a.agencykey = fs.agencykey))) JOIN dw.dimdate dd ON ((dd.datekey = fs.datekey))) WHERE (fs.isofficial = true) GROUP BY fs.agencykey, a.agencyname, a."mode", fs.routekey, dr.routeid, dr.routeshortname, dr.routelongname, dd.calendaryear, dd.calendarmonth;;
