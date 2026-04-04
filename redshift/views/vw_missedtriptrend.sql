-- View: dw.vw_missedtriptrend
-- Extracted from Redshift Serverless workgroup: team, database: dev
-- Extracted: 2026-04-03

CREATE OR REPLACE VIEW dw.vw_missedtriptrend AS
SELECT
    dd.fulldate,
    a.agencyname,
    a."mode",
    fsd.scheduledtrips,
    fsd.missedtrips,
    fsd.missedtriprate
FROM dw.factserviceday fsd
JOIN dw.dimagency  a  ON fsd.agencykey = a.agencykey
JOIN dw.dimdate    dd ON fsd.datekey   = dd.datekey;
