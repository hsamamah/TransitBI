-- DDL for schema: stg
-- Extracted from Redshift Serverless workgroup: team, database: dev
-- Extracted: 2026-04-03

CREATE SCHEMA IF NOT EXISTS stg;

CREATE TABLE IF NOT EXISTS stg.agency (
    agency_id VARCHAR(100),
    agency_name VARCHAR(200),
    agency_url VARCHAR(500),
    agency_timezone VARCHAR(100),
    agency_lang VARCHAR(20),
    agency_phone VARCHAR(100),
    agency_fare_url VARCHAR(500),
    agency_email VARCHAR(200)
);

CREATE TABLE IF NOT EXISTS stg.calendar (
    service_id VARCHAR(100),
    monday VARCHAR(5),
    tuesday VARCHAR(5),
    wednesday VARCHAR(5),
    thursday VARCHAR(5),
    friday VARCHAR(5),
    saturday VARCHAR(5),
    sunday VARCHAR(5),
    start_date VARCHAR(10),
    end_date VARCHAR(10)
);

CREATE TABLE IF NOT EXISTS stg.calendar_dates (
    service_id VARCHAR(100),
    date VARCHAR(10),
    exception_type VARCHAR(5)
);

CREATE TABLE IF NOT EXISTS stg.routes (
    agency_id VARCHAR(100),
    route_id VARCHAR(100),
    route_short_name VARCHAR(200),
    route_long_name VARCHAR(500),
    route_type VARCHAR(10),
    route_desc VARCHAR(1000),
    route_url VARCHAR(500),
    route_color VARCHAR(20),
    route_text_color VARCHAR(20),
    network_id VARCHAR(100),
    route_sort_order VARCHAR(20)
);

CREATE TABLE IF NOT EXISTS stg.rt_stop_time_updates (
    agency_key VARCHAR(30),
    trip_id VARCHAR(200),
    route_id VARCHAR(100),
    direction_id INTEGER,
    schedule_relationship VARCHAR(20),
    service_date DATE,
    stop_id VARCHAR(100),
    stop_sequence INTEGER,
    arrival_delay INTEGER,
    arrival_time_utc BIGINT,
    arrival_time_local TIMESTAMP,
    departure_delay INTEGER,
    departure_time_utc BIGINT,
    departure_time_local TIMESTAMP,
    feed_timestamp_utc TIMESTAMP,
    arrival_source VARCHAR(30)
);

CREATE TABLE IF NOT EXISTS stg.rt_vehicle_positions (
    agency_key VARCHAR(50),
    trip_id VARCHAR(255),
    route_id VARCHAR(100),
    vehicle_id VARCHAR(100),
    latitude DOUBLE PRECISION,
    longitude DOUBLE PRECISION,
    current_stop_sequence INTEGER,
    current_status VARCHAR(50),
    timestamp_utc TIMESTAMP,
    timestamp_local TIMESTAMP,
    feed_timestamp_utc TIMESTAMP
);

CREATE TABLE IF NOT EXISTS stg.shapes (
    shape_id VARCHAR(100),
    shape_pt_lat VARCHAR(30),
    shape_pt_lon VARCHAR(30),
    shape_pt_sequence VARCHAR(20),
    shape_dist_traveled VARCHAR(20)
);

CREATE TABLE IF NOT EXISTS stg.stop_times (
    trip_id VARCHAR(100),
    stop_id VARCHAR(100),
    arrival_time VARCHAR(15),
    departure_time VARCHAR(15),
    timepoint VARCHAR(10),
    stop_sequence VARCHAR(10),
    stop_headsign VARCHAR(200),
    pickup_type VARCHAR(10),
    drop_off_type VARCHAR(10),
    shape_dist_traveled VARCHAR(20),
    departure_buffer VARCHAR(20)
);

CREATE TABLE IF NOT EXISTS stg.stops (
    stop_id VARCHAR(100),
    stop_name VARCHAR(200),
    stop_lat VARCHAR(30),
    stop_lon VARCHAR(30),
    stop_code VARCHAR(100),
    stop_desc VARCHAR(500),
    zone_id VARCHAR(100),
    stop_url VARCHAR(500),
    location_type VARCHAR(10),
    parent_station VARCHAR(100),
    wheelchair_boarding VARCHAR(10),
    stop_timezone VARCHAR(100),
    platform_code VARCHAR(50),
    tts_stop_name VARCHAR(200)
);

CREATE TABLE IF NOT EXISTS stg.transfers (
    from_stop_id VARCHAR(64),
    from_route_id VARCHAR(64),
    to_stop_id VARCHAR(64),
    to_route_id VARCHAR(64),
    transfer_type VARCHAR(8),
    min_transfer_time VARCHAR(16)
);

CREATE TABLE IF NOT EXISTS stg.trips (
    route_id VARCHAR(100),
    trip_id VARCHAR(100),
    service_id VARCHAR(100),
    trip_short_name VARCHAR(100),
    trip_headsign VARCHAR(200),
    direction_id VARCHAR(10),
    block_id VARCHAR(100),
    shape_id VARCHAR(100),
    wheelchair_accessible VARCHAR(10),
    drt_advance_book_min VARCHAR(20),
    bikes_allowed VARCHAR(10),
    fare_id VARCHAR(100),
    peak_offpeak VARCHAR(20),
    boarding_type VARCHAR(20)
);

