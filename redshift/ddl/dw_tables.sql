-- DDL for schema: dw
-- Extracted from Redshift Serverless workgroup: team, database: dev
-- Extracted: 2026-04-03

CREATE SCHEMA IF NOT EXISTS dw;

CREATE TABLE IF NOT EXISTS dw.dimagency (
    agencykey INTEGER,
    agencyid VARCHAR(100),
    agencyname VARCHAR(200),
    mode VARCHAR(50),
    agencytimezone VARCHAR(100),
    gtfsrtfeedurl VARCHAR(500)
);

CREATE TABLE IF NOT EXISTS dw.dimcalendarexception (
    calendarexceptionkey INTEGER,
    serviceid VARCHAR(100),
    exceptiondate DATE,
    exceptiontype INTEGER,
    versionkey INTEGER
);

CREATE TABLE IF NOT EXISTS dw.dimdate (
    datekey INTEGER,
    fulldate DATE,
    dayofweek VARCHAR(20),
    isweekend BOOLEAN,
    isholiday BOOLEAN,
    federalfiscalmonth INTEGER,
    calendarmonth INTEGER,
    calendaryear INTEGER
);

CREATE TABLE IF NOT EXISTS dw.dimdirection (
    directionkey INTEGER,
    directionid INTEGER,
    directionlabel VARCHAR(50),
    routekey INTEGER
);

CREATE TABLE IF NOT EXISTS dw.dimfeedversion (
    versionkey INTEGER,
    feedhash CHAR(64),
    sourceurl VARCHAR(512),
    ingestedat TIMESTAMP,
    feedstartdate DATE,
    feedenddate DATE,
    feedpublishername VARCHAR(255),
    feedversion VARCHAR(100),
    filecount SMALLINT,
    iscurrent BOOLEAN,
    isactive BOOLEAN,
    notes VARCHAR(500)
);

CREATE TABLE IF NOT EXISTS dw.dimheadwayschedule (
    headwaykey INTEGER,
    routekey INTEGER,
    timewindowstart INTEGER,
    timewindowend INTEGER,
    scheduledheadwayseconds INTEGER,
    headwaysource VARCHAR(50)
);

CREATE TABLE IF NOT EXISTS dw.dimroute (
    routekey INTEGER,
    agencykey INTEGER,
    routeid VARCHAR(100),
    routeshortname VARCHAR(200),
    routelongname VARCHAR(500),
    routetype INTEGER,
    versionkey INTEGER
);

CREATE TABLE IF NOT EXISTS dw.dimservice (
    servicekey INTEGER,
    serviceid VARCHAR(100),
    isspecialevent BOOLEAN,
    versionkey INTEGER
);

CREATE TABLE IF NOT EXISTS dw.dimshape (
    shapekey INTEGER,
    shapeid VARCHAR(100),
    totaldistancemeters NUMERIC(10,2),
    versionkey INTEGER
);

CREATE TABLE IF NOT EXISTS dw.dimstop (
    stopkey INTEGER,
    stopid VARCHAR(100),
    stopname VARCHAR(200),
    stoplat NUMERIC(10,6),
    stoplon NUMERIC(10,6),
    locationtype INTEGER,
    locationtypedesc VARCHAR(50),
    parentstation VARCHAR(100),
    versionkey INTEGER
);

CREATE TABLE IF NOT EXISTS dw.dimstoptransfer (
    stoptransferkey INTEGER,
    fromstopkey INTEGER,
    tostopkey INTEGER,
    transfertype INTEGER,
    mintransfertimeseconds INTEGER,
    versionkey INTEGER
);

CREATE TABLE IF NOT EXISTS dw.dimtime (
    timekey INTEGER,
    timevalue TIMESTAMP,
    hour INTEGER,
    minute INTEGER,
    periodofday VARCHAR(20)
);

CREATE TABLE IF NOT EXISTS dw.dimtrip (
    tripkey INTEGER,
    routekey INTEGER,
    servicekey INTEGER,
    directionkey INTEGER,
    shapekey INTEGER,
    tripid VARCHAR(100),
    tripheadsign VARCHAR(200),
    versionkey INTEGER
);

CREATE TABLE IF NOT EXISTS dw.factserviceday (
    agencykey INTEGER,
    datekey INTEGER,
    scheduledtrips INTEGER,
    operatedtrips INTEGER,
    missedtrips INTEGER,
    cancelledtrips INTEGER,
    missedtriprate NUMERIC(5,4),
    unmatchedtripcount INTEGER,
    reportedvrm NUMERIC(14,4),
    reportedvrh NUMERIC(12,4),
    estimatedvrm NUMERIC(14,4),
    estimatedvrh NUMERIC(12,4),
    peakvehiclecount INTEGER,
    peaktimekey INTEGER,
    isspecialeventday BOOLEAN,
    feedgapflag BOOLEAN,
    dataqualityalertflag BOOLEAN,
    isofficial BOOLEAN
);

CREATE TABLE IF NOT EXISTS dw.factstop (
    stopperformancekey BIGINT,
    tripkey INTEGER,
    routekey INTEGER,
    stopkey INTEGER,
    datekey INTEGER,
    timekey INTEGER,
    agencykey INTEGER,
    versionkey INTEGER,
    scheduledarrivalseconds INTEGER,
    scheduleddepartureseconds INTEGER,
    stopsequence INTEGER,
    actualarrival TIMESTAMP,
    actualdeparture TIMESTAMP,
    arrivaldevseconds INTEGER,
    arrivalsource VARCHAR(30),
    interpolationconfidence NUMERIC(3,2),
    isontime BOOLEAN,
    islate BOOLEAN,
    isearly BOOLEAN,
    isoriginstop BOOLEAN,
    isterminalstop BOOLEAN,
    ismissed BOOLEAN,
    isbunching BOOLEAN,
    isestimated BOOLEAN,
    isofficial BOOLEAN
);

CREATE TABLE IF NOT EXISTS dw.facttrip (
    tripkey INTEGER,
    datekey INTEGER,
    agencykey INTEGER,
    routekey INTEGER,
    versionkey INTEGER,
    tripstatus VARCHAR(20),
    tripstatussource VARCHAR(20),
    actualstarttime TIMESTAMP,
    actualendtime TIMESTAMP,
    tripendsource VARCHAR(20),
    actualpingcount INTEGER,
    expectedpingcount INTEGER,
    rtcoveragerate NUMERIC(5,4),
    scheduledvrm NUMERIC(12,4),
    scheduledvrh NUMERIC(10,4),
    reportedvrm NUMERIC(12,4),
    reportedvrh NUMERIC(10,4),
    isestimated BOOLEAN,
    isspecialevent BOOLEAN,
    isofficial BOOLEAN,
    dataqualityflag VARCHAR(30)
);

CREATE TABLE IF NOT EXISTS dw.v_missed_trip_rate_by_route (
    agencykey INTEGER,
    agencyname VARCHAR(200),
    mode VARCHAR(50),
    routekey INTEGER,
    routeid VARCHAR(100),
    routeshortname VARCHAR(200),
    routelongname VARCHAR(500),
    fulldate DATE,
    calendarmonth INTEGER,
    calendaryear INTEGER,
    total_scheduled_trips BIGINT,
    operated_trips BIGINT,
    missed_trips BIGINT,
    cancelled_trips BIGINT,
    missed_trip_rate_pct NUMERIC(25,2)
);

CREATE TABLE IF NOT EXISTS dw.v_routes_consistently_late (
    agencykey INTEGER,
    agencyname VARCHAR(200),
    mode VARCHAR(50),
    routekey INTEGER,
    routeid VARCHAR(100),
    routeshortname VARCHAR(200),
    routelongname VARCHAR(500),
    days_evaluated BIGINT,
    days_below_70pct BIGINT,
    avg_otp_pct NUMERIC(38,1),
    worst_day_otp_pct NUMERIC(24,1),
    best_day_otp_pct NUMERIC(24,1)
);

CREATE TABLE IF NOT EXISTS dw.v_voms (
    datekey INTEGER,
    fulldate DATE,
    calendarmonth INTEGER,
    calendaryear INTEGER,
    federalfiscalmonth INTEGER,
    agencykey INTEGER,
    agencyname VARCHAR(200),
    mode VARCHAR(50),
    peakvehiclecount INTEGER,
    peaktimekey INTEGER,
    peak_time TIMESTAMP,
    peak_period VARCHAR(20),
    monthly_voms INTEGER,
    ntd_period_voms INTEGER
);

CREATE TABLE IF NOT EXISTS dw.vw_dailyvrh (
    agencykey INTEGER,
    agencyname VARCHAR(200),
    mode VARCHAR(50),
    fulldate DATE,
    calendaryear INTEGER,
    calendarmonth INTEGER,
    federalfiscalmonth INTEGER,
    reportedvrh NUMERIC(12,4),
    estimatedvrh NUMERIC(12,4),
    total_vrh_including_estimated NUMERIC(13,4),
    isofficial BOOLEAN
);

CREATE TABLE IF NOT EXISTS dw.vw_dailyvrm (
    agencykey INTEGER,
    agencyname VARCHAR(200),
    mode VARCHAR(50),
    fulldate DATE,
    calendaryear INTEGER,
    calendarmonth INTEGER,
    federalfiscalmonth INTEGER,
    reportedvrm NUMERIC(14,4),
    estimatedvrm NUMERIC(14,4),
    total_vrm_including_estimated NUMERIC(15,4),
    isofficial BOOLEAN
);

CREATE TABLE IF NOT EXISTS dw.vw_data_quality_daily (
    fulldate DATE,
    total BIGINT,
    official BIGINT,
    unmatched_trip BIGINT
);

CREATE TABLE IF NOT EXISTS dw.vw_dataqualityalert (
    fulldate DATE,
    agencyname VARCHAR(200),
    mode VARCHAR(50),
    scheduledtrips INTEGER,
    operatedtrips INTEGER,
    dataqualityalertflag BOOLEAN,
    feedgapflag BOOLEAN,
    isofficial BOOLEAN
);

CREATE TABLE IF NOT EXISTS dw.vw_missedtriptrend (
    fulldate DATE,
    agencyname VARCHAR(200),
    mode VARCHAR(50),
    scheduledtrips INTEGER,
    missedtrips INTEGER,
    missedtriprate NUMERIC(5,4)
);

CREATE TABLE IF NOT EXISTS dw.vw_monthlyntdsummary (
    agencyname VARCHAR(200),
    mode VARCHAR(50),
    calendaryear INTEGER,
    federalfiscalmonth INTEGER,
    total_vrm NUMERIC(38,4),
    total_vrh NUMERIC(38,4),
    total_scheduled BIGINT,
    total_operated BIGINT,
    total_missed BIGINT
);

CREATE TABLE IF NOT EXISTS dw.vw_otp_by_route_month (
    agencykey INTEGER,
    agencyname VARCHAR(200),
    mode VARCHAR(50),
    routekey INTEGER,
    routeid VARCHAR(100),
    routeshortname VARCHAR(200),
    routelongname VARCHAR(500),
    calendaryear INTEGER,
    calendarmonth INTEGER,
    total_obs BIGINT,
    ontime BIGINT,
    late BIGINT,
    early BIGINT,
    otp_pct NUMERIC(24,1)
);

