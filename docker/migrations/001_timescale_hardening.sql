-- Apply manually when the Timescale volume already exists.
-- Example:
-- docker exec -i timescaledb psql -U air_quality -d air_quality < docker/migrations/001_timescale_hardening.sql

-- 1) Remove historical duplicates before adding unique indexes.
WITH ranked_air_quality AS (
    SELECT
        ctid,
        ROW_NUMBER() OVER (
            PARTITION BY window_start, window_end, city, parameter, window_duration
            ORDER BY ingested_at DESC, ctid DESC
        ) AS rn
    FROM air_quality_metrics
)
DELETE FROM air_quality_metrics AS t
USING ranked_air_quality AS r
WHERE t.ctid = r.ctid
  AND r.rn > 1;

WITH ranked_weather AS (
    SELECT
        ctid,
        ROW_NUMBER() OVER (
            PARTITION BY window_start, window_end, city, window_duration
            ORDER BY ingested_at DESC, ctid DESC
        ) AS rn
    FROM weather_metrics
)
DELETE FROM weather_metrics AS t
USING ranked_weather AS r
WHERE t.ctid = r.ctid
  AND r.rn > 1;

CREATE UNIQUE INDEX IF NOT EXISTS uq_air_quality_metrics_natural_key
    ON air_quality_metrics (window_start, window_end, city, parameter, window_duration);

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
