-- View: dw.v_daily_otp_summary
-- Daily on-time performance summary, one row per date.
-- Filters to dates with actual pipeline data (fulldate <= CURRENT_DATE)
-- to prevent future skeleton FactStop rows from showing as 0% OTP.

CREATE OR REPLACE VIEW dw.v_daily_otp_summary AS
SELECT
    d.fulldate,
    d.dayofweek,
    d.isweekend,
    COUNT(*)                                                                    AS total_obs,
    COUNT(DISTINCT f.tripkey)                                                   AS unique_trips,
    COUNT(DISTINCT f.routekey)                                                  AS unique_routes,
    ROUND(100.0 * SUM(CASE WHEN f.isofficial THEN 1 ELSE 0 END)
          / NULLIF(COUNT(*), 0), 1)                                             AS otp_pct
FROM dw.factstop f
JOIN dw.dimdate d ON f.datekey = d.datekey
WHERE d.fulldate >= '2026-03-23'
  AND d.fulldate <= CURRENT_DATE
GROUP BY d.fulldate, d.dayofweek, d.isweekend;
