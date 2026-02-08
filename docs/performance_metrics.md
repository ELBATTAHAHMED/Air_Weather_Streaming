# Performance Metrics Playbook

Use this file to collect reproducible performance evidence for the final report.

## 1) Timescale Throughput (rows/min)

Run in TimescaleDB (`psql`):

```sql
-- Air quality throughput per minute
SELECT
  date_trunc('minute', ingested_at) AS minute_bucket,
  COUNT(*) AS rows_per_minute
FROM air_quality_metrics
WHERE ingested_at >= NOW() - INTERVAL '2 hours'
GROUP BY 1
ORDER BY 1;

-- Weather throughput per minute
SELECT
  date_trunc('minute', ingested_at) AS minute_bucket,
  COUNT(*) AS rows_per_minute
FROM weather_metrics
WHERE ingested_at >= NOW() - INTERVAL '2 hours'
GROUP BY 1
ORDER BY 1;
```

Command:

```powershell
docker exec -it timescaledb psql -U air_quality -d air_quality
```

## 2) Timescale Latency (p50 / p95)

Definition used here: `ingested_at - window_end` in seconds.

```sql
-- Air quality latency percentiles
SELECT
  percentile_cont(0.50) WITHIN GROUP (
    ORDER BY EXTRACT(EPOCH FROM (ingested_at - window_end))
  ) AS latency_p50_sec,
  percentile_cont(0.95) WITHIN GROUP (
    ORDER BY EXTRACT(EPOCH FROM (ingested_at - window_end))
  ) AS latency_p95_sec
FROM air_quality_metrics
WHERE ingested_at >= NOW() - INTERVAL '2 hours';

-- Weather latency percentiles
SELECT
  percentile_cont(0.50) WITHIN GROUP (
    ORDER BY EXTRACT(EPOCH FROM (ingested_at - window_end))
  ) AS latency_p50_sec,
  percentile_cont(0.95) WITHIN GROUP (
    ORDER BY EXTRACT(EPOCH FROM (ingested_at - window_end))
  ) AS latency_p95_sec
FROM weather_metrics
WHERE ingested_at >= NOW() - INTERVAL '2 hours';
```

## 3) Kafka Consumer Lag Snapshot

```powershell
docker exec kafka kafka-consumer-groups --bootstrap-server localhost:9092 --all-groups --describe
```

Capture `GROUP`, `TOPIC`, `CURRENT-OFFSET`, `LOG-END-OFFSET`, and `LAG`.

## 4) Optional Stability Snapshot

```powershell
docker exec -u airflow airflow-webserver airflow dags list-runs -d air_quality_weather_pipeline
docker exec -u airflow airflow-webserver airflow tasks states-for-dag-run air_quality_weather_pipeline <run_id>
```

Use this to correlate orchestration stability with throughput/latency measurements.
