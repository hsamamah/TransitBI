-- Fact table DDL for schema: dw
-- Extracted from Redshift Serverless workgroup: team, database: dev
-- Extracted: 2026-04-03

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
