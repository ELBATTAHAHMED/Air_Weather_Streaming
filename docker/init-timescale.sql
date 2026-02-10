-- Initialize TimescaleDB schema for real-time analytics metrics.
-- This script is executed automatically by Docker entrypoint on first startup.

CREATE EXTENSION IF NOT EXISTS timescaledb;

CREATE TABLE IF NOT EXISTS air_quality_metrics (
    window_start TIMESTAMPTZ NOT NULL,
    window_end TIMESTAMPTZ NOT NULL,
    city TEXT NOT NULL,
    parameter TEXT NOT NULL,
    rolling_avg DOUBLE PRECISION NOT NULL,
    status TEXT NOT NULL,
    window_duration TEXT NOT NULL,
    ingested_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    CONSTRAINT chk_air_quality_parameter
        CHECK (parameter IN ('pm25', 'pm10', 'co', 'no2', 'o3')),
    CONSTRAINT chk_air_quality_status
        CHECK (status IN ('OK', 'WARNING', 'DANGER', 'UNKNOWN')),
    CONSTRAINT chk_air_quality_window_duration
        CHECK (window_duration IN ('5 minutes', '10 minutes'))
);

SELECT create_hypertable(
    'air_quality_metrics',
    'window_start',
    if_not_exists => TRUE,
    migrate_data => TRUE
);

CREATE INDEX IF NOT EXISTS idx_air_quality_metrics_window_start
    ON air_quality_metrics (window_start DESC);

CREATE INDEX IF NOT EXISTS idx_air_quality_metrics_city_parameter_window_time
    ON air_quality_metrics (city, parameter, window_duration, window_start DESC);

CREATE INDEX IF NOT EXISTS idx_air_quality_metrics_status_time
    ON air_quality_metrics (status, window_start DESC);

CREATE UNIQUE INDEX IF NOT EXISTS uq_air_quality_metrics_natural_key
    ON air_quality_metrics (window_start, window_end, city, parameter, window_duration);

CREATE TABLE IF NOT EXISTS weather_metrics (
    window_start TIMESTAMPTZ NOT NULL,
    window_end TIMESTAMPTZ NOT NULL,
    city TEXT NOT NULL,
    avg_temperature DOUBLE PRECISION NOT NULL,
    avg_humidity DOUBLE PRECISION NOT NULL,
    avg_wind_speed DOUBLE PRECISION NOT NULL,
    window_duration TEXT NOT NULL,
    ingested_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    CONSTRAINT chk_weather_humidity_range
        CHECK (avg_humidity >= 0 AND avg_humidity <= 100),
    CONSTRAINT chk_weather_window_duration
        CHECK (window_duration IN ('5 minutes', '10 minutes'))
);

SELECT create_hypertable(
    'weather_metrics',
    'window_start',
    if_not_exists => TRUE,
    migrate_data => TRUE
);

CREATE INDEX IF NOT EXISTS idx_weather_metrics_window_start
    ON weather_metrics (window_start DESC);

CREATE INDEX IF NOT EXISTS idx_weather_metrics_city_window_time
    ON weather_metrics (city, window_duration, window_start DESC);

CREATE UNIQUE INDEX IF NOT EXISTS uq_weather_metrics_natural_key
    ON weather_metrics (window_start, window_end, city, window_duration);

ALTER TABLE air_quality_metrics SET (
    timescaledb.compress,
    timescaledb.compress_segmentby = 'city,parameter,window_duration',
    timescaledb.compress_orderby = 'window_start DESC'
);

ALTER TABLE weather_metrics SET (
    timescaledb.compress,
    timescaledb.compress_segmentby = 'city,window_duration',
    timescaledb.compress_orderby = 'window_start DESC'
);

SELECT add_compression_policy('air_quality_metrics', INTERVAL '7 days', if_not_exists => TRUE);
SELECT add_retention_policy('air_quality_metrics', INTERVAL '90 days', if_not_exists => TRUE);

SELECT add_compression_policy('weather_metrics', INTERVAL '7 days', if_not_exists => TRUE);
SELECT add_retention_policy('weather_metrics', INTERVAL '90 days', if_not_exists => TRUE);
