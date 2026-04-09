CREATE TABLE IF NOT EXISTS dwh.dim_date (
    date_key SERIAL PRIMARY KEY,
    full_date DATE UNIQUE NOT NULL,
    year INT NOT NULL,
    month INT NOT NULL,
    day INT NOT NULL,
    weekday_name TEXT NOT NULL,
    is_weekend BOOLEAN NOT NULL
);

CREATE TABLE IF NOT EXISTS dwh.dim_time (
    time_key SERIAL PRIMARY KEY,
    hour INT UNIQUE NOT NULL
);

CREATE TABLE IF NOT EXISTS dwh.dim_start_station (
    start_station_key SERIAL PRIMARY KEY,
    start_station_id TEXT UNIQUE NOT NULL,
    start_station_name TEXT NOT NULL,
    start_lat DOUBLE PRECISION,
    start_lng DOUBLE PRECISION
);

CREATE TABLE IF NOT EXISTS dwh.dim_bike_type (
    bike_type_key SERIAL PRIMARY KEY,
    rideable_type TEXT UNIQUE NOT NULL
);

CREATE TABLE IF NOT EXISTS dwh.dim_member_type (
    member_type_key SERIAL PRIMARY KEY,
    member_casual TEXT UNIQUE NOT NULL
);

CREATE TABLE IF NOT EXISTS dwh.fact_trip (
    ride_id TEXT PRIMARY KEY,
    date_key INT NOT NULL REFERENCES dwh.dim_date(date_key),
    time_key INT NOT NULL REFERENCES dwh.dim_time(time_key),
    start_station_key INT NOT NULL REFERENCES dwh.dim_start_station(start_station_key),
    bike_type_key INT REFERENCES dwh.dim_bike_type(bike_type_key),
    member_type_key INT REFERENCES dwh.dim_member_type(member_type_key),
    trip_count INT NOT NULL DEFAULT 1
);