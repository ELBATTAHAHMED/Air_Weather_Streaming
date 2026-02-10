-- City reference table for Grafana geomap panels.
-- Idempotent: safe to run multiple times.

CREATE TABLE IF NOT EXISTS city_dim (
    city TEXT PRIMARY KEY,
    latitude DOUBLE PRECISION NOT NULL,
    longitude DOUBLE PRECISION NOT NULL
);

INSERT INTO city_dim (city, latitude, longitude)
VALUES
    ('Paris', 48.8566, 2.3522),
    ('London', 51.5074, -0.1278),
    ('Berlin', 52.5200, 13.4050)
ON CONFLICT (city)
DO UPDATE SET
    latitude = EXCLUDED.latitude,
    longitude = EXCLUDED.longitude;

CREATE INDEX IF NOT EXISTS idx_city_dim_city
    ON city_dim (city);
