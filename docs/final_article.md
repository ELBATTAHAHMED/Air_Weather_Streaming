# Observatoire Climat et Qualite de l'Air (Temps Reel)

## 1) State of the Art

Real-time environmental analytics platforms are commonly built around event-driven ingestion, stream processing, and multi-layer storage to separate raw capture from decision-ready data.

Two dominant patterns are relevant for this project:

1. Lambda-style separation between real-time and historical workloads.
2. Lakehouse layering (Bronze/Silver/Gold) to keep lineage and data quality explicit while enabling BI consumption.

For operational dashboards, low-latency time-series stores (for example TimescaleDB) are well-suited to near-real-time indicators. For historical analytics, SQL federation engines (for example Trino) over lakehouse tables reduce coupling between storage and BI tools.

## 2) Architecture and Design Choices

This project keeps the existing stack and responsibilities:

- Kafka for ingestion topics (`air_quality_raw`, `weather_raw`).
- Spark Structured Streaming for parsing, validation, deduplication, watermarking, and KPI computation.
- HDFS + Hive Metastore for persistent lakehouse tables.
- Trino for SQL access to historical Gold data.
- TimescaleDB for real-time serving to Grafana.
- Airflow for pipeline orchestration.
- Grafana for operational monitoring and Superset for historical decision BI.

Rationale:

- Event-time processing handles out-of-order data.
- Bronze/Silver/Gold + rejects improves observability and reproducibility.
- TimescaleDB supports low-latency queries for rolling indicators and alerts.
- Trino decouples BI from physical storage paths and formats.

## 3) Real-time Processing Logic

### 3.1 Ingestion and late events

- Producers accept delayed observations up to 6 hours.
- Spark watermark is aligned to 6 hours (`WATERMARK_DELAY`, default `6 hours`).
- Deduplication relies on `message_id` when available and deterministic fallback keys otherwise.

### 3.2 Quality and rejects

- Corrupt payloads and missing mandatory fields are routed to rejects.
- Out-of-range values are routed to rejects with `reject_reason = 'invalid_range'`.

### 3.3 Indicators

- Air quality: rolling averages (5 min, 10 min), threshold status (`OK`, `WARNING`, `DANGER`), and peak detection.
- Weather: rolling averages (temperature, humidity, wind) on 5 min and 10 min windows.

### 3.4 Serving

- Streaming KPIs are written to TimescaleDB through `foreachBatch` upsert logic.
- Lakehouse Parquet outputs are registered in Hive and synchronized in Trino by Airflow tasks.

## 4) Experimental Results

Measurement commands and SQL are listed in `docs/performance_metrics.md`.

### 4.1 Throughput (rows/min)

| Dataset | Avg rows/min | Peak rows/min | Window |
|---|---:|---:|---|
| air_quality_metrics | TODO | TODO | last 2h |
| weather_metrics | TODO | TODO | last 2h |

### 4.2 Latency (seconds)

Definition: `ingested_at - window_end`.

| Dataset | p50 latency (s) | p95 latency (s) | Window |
|---|---:|---:|---|
| air_quality_metrics | TODO | TODO | last 2h |
| weather_metrics | TODO | TODO | last 2h |

### 4.3 Kafka lag snapshot

Record at least one stable snapshot from:

`docker exec kafka kafka-consumer-groups --bootstrap-server localhost:9092 --all-groups --describe`

| Consumer group | Topic | Lag observed | Timestamp |
|---|---|---:|---|
| TODO | TODO | TODO | TODO |

## 5) Discussion and Limitations

### 5.1 Watermark trade-offs

A 6-hour watermark reduces late-event drops and aligns with producer policy, but increases state retention and can delay finality for some aggregations.

### 5.2 Timescale sink implementation

The upsert path currently uses `collect()` in `foreachBatch`, which can become a bottleneck or memory risk with larger micro-batches. A partitioned server-side strategy (for example batched JDBC writes without full collection) would improve scalability.

### 5.3 Parquet replay and duplication risk

Append writes to lakehouse layers may still duplicate rows during replay/recovery scenarios if replay boundaries overlap. Timescale unique constraints mitigate this in serving tables, but lakehouse dedup/merge strategy should be strengthened for strict exactly-once historical semantics.

### 5.4 Operational constraints

- Superset dashboard content must be exported/imported to guarantee full reproducibility across clean environments.
- End-to-end health depends on coordinated availability of Kafka, HDFS/Hive, Trino, and TimescaleDB.

## 6) Conclusion

The platform delivers a complete real-time analytics pipeline combining ingestion, stream processing, lakehouse persistence, time-series serving, orchestration, and dual BI/monitoring interfaces. Remaining optimization priorities focus on high-volume sink scalability and stronger replay-safe historical deduplication.
