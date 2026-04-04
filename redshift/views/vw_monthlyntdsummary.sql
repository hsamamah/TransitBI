-- View: dw.vw_monthlyntdsummary
-- Extracted from Redshift Serverless workgroup: team, database: dev
-- Extracted: 2026-04-03

CREATE OR REPLACE VIEW dw.vw_monthlyntdsummary AS
SELECT
    a.agencyname,
    a."mode",
    dd.calendaryear,
    dd.federalfiscalmonth,
    sum(fsd.reportedvrm)      AS total_vrm,
    sum(fsd.reportedvrh)      AS total_vrh,
    sum(fsd.scheduledtrips)   AS total_scheduled,
    sum(fsd.operatedtrips)    AS total_operated,
    sum(fsd.missedtrips)      AS total_missed
FROM dw.factserviceday fsd
JOIN dw.dimagency  a  ON fsd.agencykey = a.agencykey
JOIN dw.dimdate    dd ON fsd.datekey   = dd.datekey
WHERE fsd.isofficial = true
GROUP BY a.agencyname, a."mode", dd.calendaryear, dd.federalfiscalmonth;
