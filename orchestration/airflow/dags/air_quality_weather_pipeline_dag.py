from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.utils.trigger_rule import TriggerRule


PROJECT_ROOT = "/opt/project"
RUNTIME_DIR = f"{PROJECT_ROOT}/orchestration/airflow/runtime"
RUNTIME_LOG_DIR = f"{RUNTIME_DIR}/logs"
RUNTIME_PID_DIR = f"{RUNTIME_DIR}/pids"

MANAGED_PROCESSES = [
    "air_quality_openaq",
    "weather_openmeteo",
    "weather_openweather",
    "spark_air_quality_stream",
    "spark_weather_stream",
]


def start_bg(task_name: str, command: str) -> str:
    return f"""
set -euo pipefail
mkdir -p {RUNTIME_LOG_DIR} {RUNTIME_PID_DIR}
PID_FILE={RUNTIME_PID_DIR}/{task_name}.pid
LOG_FILE={RUNTIME_LOG_DIR}/{task_name}.log
if [ -f "$PID_FILE" ]; then
  CURRENT_PID="$(cat "$PID_FILE")"
  if kill -0 "$CURRENT_PID" 2>/dev/null; then
    echo "{task_name} already running with pid=$CURRENT_PID"
    echo "runtime_log=$LOG_FILE"
    echo "airflow_log={{{{ ti.log_url }}}}"
    exit 0
  fi
  echo "{task_name} pid file is stale; removing it"
  rm -f "$PID_FILE"
fi
nohup {command} > "$LOG_FILE" 2>&1 &
NEW_PID=$!
echo "$NEW_PID" > "$PID_FILE"
sleep 3
if ! kill -0 "$NEW_PID" 2>/dev/null; then
  echo "{task_name} failed to start"
  tail -n 120 "$LOG_FILE" || true
  exit 1
fi
echo "{task_name} started with pid=$NEW_PID"
echo "runtime_log=$LOG_FILE"
echo "airflow_log={{{{ ti.log_url }}}}"
"""


def check_bg(task_name: str) -> str:
    return f"""
set -euo pipefail
PID_FILE={RUNTIME_PID_DIR}/{task_name}.pid
LOG_FILE={RUNTIME_LOG_DIR}/{task_name}.log
if [ ! -f "$PID_FILE" ]; then
  echo "{task_name} health check failed: missing pid file"
  exit 1
fi
PID="$(cat "$PID_FILE")"
if ! kill -0 "$PID" 2>/dev/null; then
  echo "{task_name} health check failed: process pid=$PID is not running"
  tail -n 120 "$LOG_FILE" || true
  exit 1
fi
echo "{task_name} healthy with pid=$PID"
tail -n 60 "$LOG_FILE" || true
echo "runtime_log=$LOG_FILE"
echo "airflow_log={{{{ ti.log_url }}}}"
"""


def cleanup_stale_pid_files() -> str:
    return f"""
set -euo pipefail
mkdir -p {RUNTIME_LOG_DIR} {RUNTIME_PID_DIR}
for pid_file in {RUNTIME_PID_DIR}/*.pid; do
  [ -e "$pid_file" ] || continue
  PID="$(cat "$pid_file")"
  if ! kill -0 "$PID" 2>/dev/null; then
    echo "Removing stale pid file: $pid_file"
    rm -f "$pid_file"
  fi
done
"""


def wait_for_kafka() -> str:
    return """
set -euo pipefail
for attempt in $(seq 1 30); do
  if docker exec kafka kafka-topics --bootstrap-server localhost:9092 --list >/dev/null 2>&1; then
    echo "kafka_ready"
    exit 0
  fi
  echo "Waiting for Kafka... attempt=$attempt"
  sleep 2
done
echo "Kafka is not reachable"
exit 1
"""


def wait_for_timescaledb() -> str:
    return """
set -euo pipefail
DB_USER="${TIMESCALE_USER:-air_quality}"
DB_NAME="${TIMESCALE_DB:-air_quality}"
for attempt in $(seq 1 30); do
  if docker exec timescaledb pg_isready -U "$DB_USER" -d "$DB_NAME" >/dev/null 2>&1; then
    echo "timescaledb_ready"
    exit 0
  fi
  echo "Waiting for TimescaleDB... attempt=$attempt"
  sleep 2
done
echo "TimescaleDB is not reachable"
exit 1
"""


def check_topics_exist() -> str:
    return """
set -euo pipefail
for topic in air_quality_raw weather_raw; do
  docker exec kafka kafka-topics --bootstrap-server localhost:9092 --describe --topic "$topic" >/dev/null
done
echo "topics_healthcheck_passed"
"""


def stop_all_processes() -> str:
    managed_processes = " ".join(MANAGED_PROCESSES)
    return f"""
set -euo pipefail
AUTO_CLEANUP="{{{{ dag_run.conf.get('auto_cleanup', False) }}}}"
AUTO_CLEANUP="$(echo "$AUTO_CLEANUP" | tr '[:upper:]' '[:lower:]')"
if [ "$AUTO_CLEANUP" != "true" ]; then
  echo "auto_cleanup disabled; keeping managed processes running"
  exit 0
fi
for task_name in {managed_processes}; do
  PID_FILE={RUNTIME_PID_DIR}/$task_name.pid
  if [ ! -f "$PID_FILE" ]; then
    echo "$task_name not running (no pid file)"
    continue
  fi

  PID="$(cat "$PID_FILE")"
  if kill -0 "$PID" 2>/dev/null; then
    echo "Stopping $task_name pid=$PID"
    kill "$PID" || true
    for _ in $(seq 1 20); do
      if ! kill -0 "$PID" 2>/dev/null; then
        break
      fi
      sleep 1
    done
    if kill -0 "$PID" 2>/dev/null; then
      echo "Force-killing $task_name pid=$PID"
      kill -9 "$PID" || true
    fi
  fi

  rm -f "$PID_FILE"
done
echo "Managed process cleanup complete"
"""


def register_hive_external_tables() -> str:
    return f"""
set -euo pipefail
export HDFS_DEFAULT_FS="hdfs://namenode:8020"
export HIVE_METASTORE_URI="thrift://hive-metastore:9083"
export LAKEHOUSE_ROOT="hdfs://namenode:8020/lakehouse"
cd {PROJECT_ROOT}
spark-submit scripts/create_external_tables.py
echo "hive_external_tables_registered"
"""


def sync_trino_partition_metadata() -> str:
    return """
set -euo pipefail
for attempt in $(seq 1 30); do
  if docker exec trino trino --server http://localhost:8080 --execute "SELECT 1" >/dev/null 2>&1; then
    break
  fi
  if [ "$attempt" -eq 30 ]; then
    echo "Trino is not reachable"
    exit 1
  fi
  echo "Waiting for Trino... attempt=$attempt"
  sleep 2
done

tables=(
  bronze_air_quality
  bronze_weather
  silver_air_quality
  silver_weather
  gold_air_quality_metrics
  gold_weather_metrics
  rejects_air_quality
  rejects_weather
)

for table_name in "${tables[@]}"; do
  docker exec trino trino --server http://localhost:8080 --execute "CALL hive.system.sync_partition_metadata('lakehouse', '${table_name}', 'ADD')"
done

echo "trino_partition_sync_complete"
"""


with DAG(
    dag_id="air_quality_weather_pipeline",
    start_date=datetime(2026, 1, 1),
    schedule_interval=None,
    catchup=False,
    max_active_runs=1,
    dagrun_timeout=timedelta(hours=4),
    default_args={"retries": 2, "retry_delay": timedelta(seconds=30)},
    tags=["air-quality", "weather", "kafka", "spark"],
) as dag:
    cleanup_stale_pids = BashOperator(
        task_id="cleanup_stale_pids",
        bash_command=cleanup_stale_pid_files(),
        retries=0,
        execution_timeout=timedelta(minutes=2),
    )

    wait_kafka = BashOperator(
        task_id="wait_for_kafka",
        bash_command=wait_for_kafka(),
        retries=4,
        retry_delay=timedelta(seconds=20),
        execution_timeout=timedelta(minutes=5),
    )

    wait_db = BashOperator(
        task_id="wait_for_timescaledb",
        bash_command=wait_for_timescaledb(),
        retries=4,
        retry_delay=timedelta(seconds=20),
        execution_timeout=timedelta(minutes=5),
    )

    create_kafka_topics = BashOperator(
        task_id="create_kafka_topics",
        bash_command=(
            f"sed -i 's/\\r$//' {PROJECT_ROOT}/kafka/create_topics.sh || true && "
            f"chmod +x {PROJECT_ROOT}/kafka/create_topics.sh || true && "
            f"bash {PROJECT_ROOT}/kafka/create_topics.sh && "
            "echo topics_created"
        ),
        execution_timeout=timedelta(minutes=3),
    )

    healthcheck_kafka_topics = BashOperator(
        task_id="healthcheck_kafka_topics",
        bash_command=check_topics_exist(),
        retries=2,
        retry_delay=timedelta(seconds=15),
        execution_timeout=timedelta(minutes=2),
    )

    start_air_quality_producer = BashOperator(
        task_id="start_air_quality_producer",
        bash_command=start_bg(
            "air_quality_openaq",
            f"python {PROJECT_ROOT}/ingestion/air_quality_openaq.py",
        ),
        execution_timeout=timedelta(minutes=3),
    )

    start_weather_openmeteo_producer = BashOperator(
        task_id="start_weather_openmeteo_producer",
        bash_command=start_bg(
            "weather_openmeteo",
            f"python {PROJECT_ROOT}/ingestion/weather_openmeteo.py",
        ),
        execution_timeout=timedelta(minutes=3),
    )

    start_weather_openweather_producer = BashOperator(
        task_id="start_weather_openweather_producer",
        bash_command=start_bg(
            "weather_openweather",
            f"python {PROJECT_ROOT}/ingestion/weather_openweather.py",
        ),
        execution_timeout=timedelta(minutes=3),
    )

    healthcheck_air_quality_producer = BashOperator(
        task_id="healthcheck_air_quality_producer",
        bash_command=check_bg("air_quality_openaq"),
        retries=3,
        retry_delay=timedelta(seconds=20),
        execution_timeout=timedelta(minutes=3),
    )

    healthcheck_weather_openmeteo_producer = BashOperator(
        task_id="healthcheck_weather_openmeteo_producer",
        bash_command=check_bg("weather_openmeteo"),
        retries=3,
        retry_delay=timedelta(seconds=20),
        execution_timeout=timedelta(minutes=3),
    )

    healthcheck_weather_openweather_producer = BashOperator(
        task_id="healthcheck_weather_openweather_producer",
        bash_command=check_bg("weather_openweather"),
        retries=3,
        retry_delay=timedelta(seconds=20),
        execution_timeout=timedelta(minutes=3),
    )

    start_air_quality_stream = BashOperator(
        task_id="start_air_quality_stream",
        bash_command=start_bg(
            "spark_air_quality_stream",
            (
                f"pwsh -File {PROJECT_ROOT}/run_air_quality.ps1 "
                "-TimescaleHost timescaledb "
                "-TimescalePort 5432 "
                "-TimescaleDb air_quality "
                "-TimescaleUser air_quality "
                "-TimescalePassword air_quality "
                "-CheckpointMode stable"
            ),
        ),
        execution_timeout=timedelta(minutes=4),
    )

    start_weather_stream = BashOperator(
        task_id="start_weather_stream",
        bash_command=start_bg(
            "spark_weather_stream",
            (
                f"pwsh -File {PROJECT_ROOT}/run_weather.ps1 "
                "-TimescaleHost timescaledb "
                "-TimescalePort 5432 "
                "-TimescaleDb air_quality "
                "-TimescaleUser air_quality "
                "-TimescalePassword air_quality "
                "-CheckpointMode stable"
            ),
        ),
        execution_timeout=timedelta(minutes=4),
    )

    healthcheck_air_quality_stream = BashOperator(
        task_id="healthcheck_air_quality_stream",
        bash_command=check_bg("spark_air_quality_stream"),
        retries=3,
        retry_delay=timedelta(seconds=20),
        execution_timeout=timedelta(minutes=3),
    )

    healthcheck_weather_stream = BashOperator(
        task_id="healthcheck_weather_stream",
        bash_command=check_bg("spark_weather_stream"),
        retries=3,
        retry_delay=timedelta(seconds=20),
        execution_timeout=timedelta(minutes=3),
    )

    register_external_tables = BashOperator(
        task_id="register_hive_external_tables",
        bash_command=register_hive_external_tables(),
        retries=2,
        retry_delay=timedelta(seconds=30),
        execution_timeout=timedelta(minutes=10),
    )

    sync_trino_partitions = BashOperator(
        task_id="sync_trino_partitions",
        bash_command=sync_trino_partition_metadata(),
        retries=2,
        retry_delay=timedelta(seconds=30),
        execution_timeout=timedelta(minutes=10),
    )

    stop_managed_processes = BashOperator(
        task_id="stop_managed_processes",
        bash_command=stop_all_processes(),
        trigger_rule=TriggerRule.ALL_DONE,
        retries=0,
        execution_timeout=timedelta(minutes=5),
    )

    cleanup_stale_pids >> [wait_kafka, wait_db]

    wait_kafka >> create_kafka_topics >> healthcheck_kafka_topics

    healthcheck_kafka_topics >> [
        start_air_quality_producer,
        start_weather_openmeteo_producer,
        start_weather_openweather_producer,
    ]

    start_air_quality_producer >> healthcheck_air_quality_producer >> start_air_quality_stream
    start_weather_openmeteo_producer >> healthcheck_weather_openmeteo_producer
    start_weather_openweather_producer >> healthcheck_weather_openweather_producer
    [healthcheck_weather_openmeteo_producer, healthcheck_weather_openweather_producer] >> start_weather_stream

    wait_db >> [start_air_quality_stream, start_weather_stream]
    start_air_quality_stream >> healthcheck_air_quality_stream
    start_weather_stream >> healthcheck_weather_stream
    [healthcheck_air_quality_stream, healthcheck_weather_stream] >> register_external_tables
    register_external_tables >> sync_trino_partitions

    [
        healthcheck_air_quality_producer,
        healthcheck_weather_openmeteo_producer,
        healthcheck_weather_openweather_producer,
        sync_trino_partitions,
        healthcheck_kafka_topics,
    ] >> stop_managed_processes
