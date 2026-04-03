-- Dimension table DDL for schema: dw
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
