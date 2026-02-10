# Air_Weather_Streaming

**Observatoire temps reel de la qualite de l'air et de la meteo**

This repository provides an end-to-end streaming data platform for weather and air-quality monitoring, from ingestion to decision dashboards.

## 1) What the platform does

- Ingests live weather and air-quality data from external APIs.
- Streams and transforms data with Spark Structured Streaming.
- Writes metrics to TimescaleDB for operational dashboards.
- Builds a Lakehouse layout on HDFS (Bronze/Silver/Gold/Rejects).
- Exposes SQL analytics via Hive Metastore + Trino.
- Serves decision dashboards in Grafana and Superset.
- Supports orchestration with Airflow.

## 2) End-to-end architecture

1. Producers publish to Kafka topics (`weather_raw`, `air_quality_raw`).
2. Spark jobs consume Kafka and compute cleaned + aggregated datasets.
3. Spark writes to:
   - TimescaleDB (`weather_metrics`, `air_quality_metrics`)
   - HDFS Lakehouse (`/lakehouse/bronze|silver|gold|rejects`)
4. Hive Metastore registers external tables in database `lakehouse`.
5. Trino queries Hive tables for BI/analytics.
6. Grafana reads TimescaleDB for real-time ops dashboards.
7. Superset reads Trino for decision-level BI dashboards.
8. Airflow can orchestrate producers + stream lifecycle.

## 3) Main service endpoints

- Grafana: `http://localhost:3000`
- NameNode UI: `http://localhost:9870`
- YARN ResourceManager UI: `http://localhost:8088`
- YARN HistoryServer UI: `http://localhost:19888/applicationhistory`
- Trino: `http://localhost:8090`
- Superset: `http://localhost:8082`

## 4) Prerequisites

- Windows + PowerShell (recommended command shell).
- Docker Desktop with Compose.
- Python 3 for local producer/validation scripts.

Create local env file:

```powershell
Copy-Item .env.example .env
```

Then set required API keys in `.env`.

Always run commands from project root:

```powershell
Set-Location C:\Users\LENOVO\Desktop\project-air-quality
```

Optional but recommended (install Python dependencies locally):

```powershell
py -m pip install -r .\requirements.txt
```

## 5) Core startup (manual mode)

Start base infrastructure:

```powershell
docker compose -f .\docker\docker-compose.yml up -d
```

Run producers (one terminal each):

```powershell
py ingestion\weather_openmeteo.py
py ingestion\weather_openweather.py
py ingestion\air_quality_openaq.py
```

Run streaming jobs (one terminal each):

```powershell
.\run_weather.ps1 -CheckpointMode stable
.\run_air_quality.ps1 -CheckpointMode stable
```

Register external lakehouse tables:

```powershell
spark-submit scripts\create_external_tables.py
```

Smoke validation:

```powershell
py scripts\validate_pipeline_smoke.py
```

## 6) Airflow orchestration mode (recommended)

Use this mode instead of manual producer/stream execution.

First-time setup (or after image updates):

```powershell
docker compose -f .\docker\docker-compose.yml up -d
Get-Content .\docker\migrations\001_timescale_hardening.sql | docker exec -i timescaledb psql -U air_quality -d air_quality
docker compose -f .\orchestration\airflow\docker-compose.yml up -d --build --force-recreate
docker exec -u airflow airflow-webserver airflow dags unpause air_quality_weather_pipeline
```

Daily run:

```powershell
docker exec -u airflow airflow-webserver airflow dags trigger air_quality_weather_pipeline
docker exec -u airflow airflow-webserver airflow dags list-runs -d air_quality_weather_pipeline
py .\scripts\validate_pipeline_smoke.py
```

Cleanup run (stop managed long-running processes):

```powershell
docker exec -u airflow airflow-webserver bash -lc "airflow dags trigger air_quality_weather_pipeline --conf '{\"auto_cleanup\": true}'"
```

Watch runtime logs:

```powershell
Get-Content .\orchestration\airflow\runtime\logs\spark_air_quality_stream.log -Tail 100 -Wait
Get-Content .\orchestration\airflow\runtime\logs\spark_weather_stream.log -Tail 100 -Wait
```

## 7) Hadoop + YARN + Lakehouse operations

Start stack:

```powershell
docker compose -f .\docker\docker-compose.yml -f .\docker\docker-compose.hadoop.yml up -d
```

Quick checks:

```powershell
docker exec resourcemanager yarn node -list
docker exec namenode hdfs dfsadmin -report
docker exec namenode hdfs dfs -ls /lakehouse
```

Run SparkPi on YARN (sanity test):

```powershell
docker exec airflow-webserver bash -lc "mkdir -p /tmp/hadoop-conf"
docker exec airflow-webserver bash -lc "printf '%s\n' '<configuration>' '  <property><name>fs.defaultFS</name><value>hdfs://namenode:8020</value></property>' '</configuration>' > /tmp/hadoop-conf/core-site.xml"
docker exec airflow-webserver bash -lc "printf '%s\n' '<configuration>' '  <property><name>yarn.resourcemanager.hostname</name><value>resourcemanager</value></property>' '  <property><name>yarn.resourcemanager.address</name><value>resourcemanager:8032</value></property>' '</configuration>' > /tmp/hadoop-conf/yarn-site.xml"
docker exec airflow-webserver bash -lc "HADOOP_CONF_DIR=/tmp/hadoop-conf YARN_CONF_DIR=/tmp/hadoop-conf spark-submit --master yarn --deploy-mode client --conf spark.hadoop.fs.defaultFS=hdfs://namenode:8020 --conf spark.yarn.resourcemanager.address=resourcemanager:8032 --class org.apache.spark.examples.SparkPi /opt/spark/examples/jars/spark-examples_2.12-3.5.8.jar 10"
docker exec resourcemanager yarn application -list -appStates FINISHED
```

Re-register external tables if needed:

```powershell
docker exec airflow-webserver bash -lc "cd /opt/project && spark-submit scripts/create_external_tables.py"
```

## 8) Trino analytics layer

Start with Trino:

```powershell
docker compose -f .\docker\docker-compose.yml -f .\docker\docker-compose.hadoop.yml -f .\docker\docker-compose.trino.yml up -d
```

Health and metadata checks:

```powershell
curl.exe -s http://localhost:8090/v1/info
docker exec trino trino --server http://localhost:8080 --execute "SHOW CATALOGS"
docker exec trino trino --server http://localhost:8080 --execute "SHOW TABLES FROM hive.lakehouse"
```

Sample analytics queries:

```powershell
docker exec trino trino --server http://localhost:8080 --execute "SELECT COUNT(*) AS weather_gold_rows FROM hive.lakehouse.gold_weather_metrics"
docker exec trino trino --server http://localhost:8080 --execute "SELECT COUNT(*) AS air_quality_gold_rows FROM hive.lakehouse.gold_air_quality_metrics"
```

## 9) Superset BI layer

Start Superset:

```powershell
docker compose -f .\docker\docker-compose.superset.yml up -d superset-db
docker compose -f .\docker\docker-compose.superset.yml run --rm superset-init
docker compose -f .\docker\docker-compose.superset.yml up -d superset
```

Login:

- URL: `http://localhost:8082`
- User: `admin`
- Password: `admin123`

Add Trino connection in Superset with SQLAlchemy URI:

```text
trino://trino@trino:8080/hive/lakehouse
```

Export dashboard bundle:

```powershell
powershell -ExecutionPolicy Bypass -File .\superset\exports\export_bundle.ps1
```

## 10) Grafana weather dashboard notes

If TimescaleDB volume already existed before `docker/init-city-dim.sql` changes, run once:

```powershell
docker exec -i timescaledb psql -U air_quality -d air_quality < .\docker\init-city-dim.sql
```

Reload Grafana provisioning:

```powershell
docker compose -f .\docker\docker-compose.yml up -d --force-recreate --no-deps grafana
```

Check dashboard API:

```powershell
curl -u admin:admin123 http://localhost:3000/api/dashboards/uid/weather-realtime-modern
```

## 11) Data model and storage

Lakehouse HDFS root:

- `hdfs://namenode:8020/lakehouse`

Layer layout:

- `/lakehouse/bronze/air_quality` (partition: `ingest_date`)
- `/lakehouse/bronze/weather` (partition: `ingest_date`)
- `/lakehouse/silver/air_quality` (partition: `event_date`, `city`)
- `/lakehouse/silver/weather` (partition: `event_date`, `city`)
- `/lakehouse/gold/air_quality_aggregates` (partition: `window_date`)
- `/lakehouse/gold/air_quality_aggregates/peaks` (append parquet)
- `/lakehouse/gold/weather_aggregates` (partition: `window_date`)
- `/lakehouse/rejects/air_quality` (partition: `reject_date`, `reject_reason`)
- `/lakehouse/rejects/weather` (partition: `reject_date`, `reject_reason`)

## 12) Fast troubleshooting

- Metastore healthy but tables missing: rerun `spark-submit scripts\create_external_tables.py`.
- Trino not healthy: `docker logs trino --tail 200`.
- YARN app visibility issues: `docker exec resourcemanager yarn application -list -appStates ALL`.
- Pipeline idle in Airflow: trigger `air_quality_weather_pipeline` again.
- No air-quality peaks yet: `gold_air_quality_peaks` can stay empty if thresholds are not crossed.

## 13) Optional stop commands

```powershell
docker compose -f .\orchestration\airflow\docker-compose.yml down
docker compose -f .\docker\docker-compose.yml -f .\docker\docker-compose.hadoop.yml down
```

Note: stopping containers keeps Docker volumes (persistent data remains available).

## 14) Quick command map

To avoid duplicated instructions, use these sections as the source of truth:

- Airflow orchestration run flow: section **6**.
- Manual run flow (without Airflow): section **5**.
- Hadoop + YARN + Lakehouse operations: section **7**.
- Trino analytics layer: section **8**.
- Superset BI layer: section **9**.
- Stop/cleanup commands: section **13**.
