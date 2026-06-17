CREATE EXTENSION IF NOT EXISTS "uuid-ossp";

CREATE USER airflow WITH PASSWORD 'airflow';
CREATE DATABASE airflow OWNER airflow;
GRANT ALL PRIVILEGES ON DATABASE airflow TO airflow;

CREATE TABLE IF NOT EXISTS raw_flights (
    id             SERIAL PRIMARY KEY,
    icao24         VARCHAR(10),
    callsign       VARCHAR(20),
    origin_country VARCHAR(50),
    longitude      FLOAT,
    latitude       FLOAT,
    altitude       FLOAT,
    on_ground      BOOLEAN,
    velocity       FLOAT,
    ingested_at    TIMESTAMP DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS raw_flight_status (
    id                  SERIAL PRIMARY KEY,
    flight_number       VARCHAR(20),
    airline             VARCHAR(100),
    departure_airport   VARCHAR(10),
    arrival_airport     VARCHAR(10),
    scheduled_departure TIMESTAMP,
    actual_departure    TIMESTAMP,
    scheduled_arrival   TIMESTAMP,
    actual_arrival      TIMESTAMP,
    status              VARCHAR(50),
    delay_minutes       INT,
    ingested_at         TIMESTAMP DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS streaming_flights (
    id             SERIAL PRIMARY KEY,
    icao24         VARCHAR(10),
    callsign       VARCHAR(20),
    origin_country VARCHAR(50),
    longitude      FLOAT,
    latitude       FLOAT,
    altitude       FLOAT,
    on_ground      BOOLEAN,
    velocity       FLOAT,
    received_at    TIMESTAMP DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS analytics_zone_congestion (
    id           SERIAL PRIMARY KEY,
    zone_name    VARCHAR(50),
    flight_count INT,
    avg_altitude FLOAT,
    computed_at  TIMESTAMP DEFAULT NOW()
);

CREATE OR REPLACE VIEW v_traffic_by_country AS
SELECT origin_country, COUNT(*) AS total_flights, ROUND(AVG(velocity)::numeric, 2) AS avg_velocity, ROUND(AVG(altitude)::numeric, 2) AS avg_altitude, SUM(CASE WHEN on_ground THEN 1 ELSE 0 END) AS on_ground_count, MAX(ingested_at) AS last_seen FROM raw_flights WHERE origin_country IS NOT NULL GROUP BY origin_country ORDER BY total_flights DESC;

CREATE OR REPLACE VIEW v_traffic_per_hour AS
SELECT DATE_TRUNC('hour', ingested_at) AS hour, COUNT(*) AS flight_count, ROUND(AVG(altitude)::numeric, 2) AS avg_altitude, ROUND(AVG(velocity)::numeric, 2) AS avg_velocity FROM raw_flights GROUP BY DATE_TRUNC('hour', ingested_at) ORDER BY hour DESC;

CREATE OR REPLACE VIEW v_ground_vs_air AS
SELECT CASE WHEN on_ground THEN 'Au sol' ELSE 'En vol' END AS status, COUNT(*) AS count, ROUND(AVG(velocity)::numeric, 2) AS avg_velocity FROM raw_flights GROUP BY on_ground;

CREATE OR REPLACE VIEW v_delays_by_airport AS
SELECT departure_airport, COUNT(*) AS total_flights, ROUND(AVG(delay_minutes)::numeric, 1) AS avg_delay, SUM(CASE WHEN delay_minutes > 15 THEN 1 ELSE 0 END) AS delayed_flights, SUM(CASE WHEN status = 'cancelled' THEN 1 ELSE 0 END) AS cancelled_flights FROM raw_flight_status WHERE departure_airport IS NOT NULL GROUP BY departure_airport ORDER BY avg_delay DESC;

CREATE OR REPLACE VIEW v_delays_by_airline AS
SELECT airline, COUNT(*) AS total_flights, ROUND(AVG(delay_minutes)::numeric, 1) AS avg_delay, SUM(CASE WHEN delay_minutes > 15 THEN 1 ELSE 0 END) AS delayed_flights FROM raw_flight_status WHERE airline IS NOT NULL GROUP BY airline ORDER BY avg_delay DESC;