import os
from pathlib import Path

from pyspark.sql import SparkSession


DATABASE_NAME = "lakehouse"

DEFAULT_AIR_QUALITY_PEAKS_COLUMNS = [
    ("window_start", "TIMESTAMP"),
    ("window_end", "TIMESTAMP"),
    ("city", "STRING"),
    ("parameter", "STRING"),
    ("rolling_avg", "DOUBLE"),
    ("status", "STRING"),
    ("window_duration", "STRING"),
    ("prev_rolling_avg", "DOUBLE"),
    ("delta", "DOUBLE"),
    ("is_peak", "BOOLEAN"),
]


def is_uri_path(path_value: str) -> bool:
    return "://" in path_value


def normalize_storage_root(root_path: str) -> str:
    if is_uri_path(root_path):
        return root_path.rstrip("/")
    return os.path.abspath(root_path)


def join_storage_path(root_path: str, *parts: str) -> str:
    if is_uri_path(root_path):
        suffix = "/".join(part.strip("/\\") for part in parts if part)
        return f"{root_path.rstrip('/')}/{suffix}"

    return os.path.abspath(os.path.join(root_path, *parts))


def to_table_location_uri(path_str: str) -> str:
    if is_uri_path(path_str):
        return path_str
    return Path(path_str).resolve().as_uri()


def sql_safe(value: str) -> str:
    return value.replace("'", "''")


def hadoop_fs_and_path(spark: SparkSession, path_str: str):
    fs_path = path_str if is_uri_path(path_str) else Path(path_str).resolve().as_uri()
    j_path = spark._jvm.org.apache.hadoop.fs.Path(fs_path)
    fs = j_path.getFileSystem(spark._jsc.hadoopConfiguration())
    return fs, j_path


def ensure_storage_path_exists(spark: SparkSession, path_str: str) -> None:
    fs, j_path = hadoop_fs_and_path(spark, path_str)
    fs.mkdirs(j_path)


def has_parquet_files(spark: SparkSession, path_str: str) -> bool:
    fs, j_path = hadoop_fs_and_path(spark, path_str)
    if not fs.exists(j_path):
        return False

    if fs.isFile(j_path):
        return j_path.getName().endswith(".parquet")

    file_iterator = fs.listFiles(j_path, True)
    while file_iterator.hasNext():
        if file_iterator.next().getPath().getName().endswith(".parquet"):
            return True

    return False


def spark_type_to_sql(data_type) -> str:
    type_name = str(data_type.simpleString()).upper()
    mapped = {
        "LONG": "BIGINT",
        "INTEGER": "INT",
        "SHORT": "SMALLINT",
        "BYTE": "TINYINT",
        "FLOAT": "FLOAT",
        "DOUBLE": "DOUBLE",
        "BOOLEAN": "BOOLEAN",
        "STRING": "STRING",
        "TIMESTAMP": "TIMESTAMP",
        "DATE": "DATE",
    }.get(type_name)
    return mapped if mapped is not None else type_name


def infer_air_quality_peaks_columns(spark: SparkSession, peaks_path: str) -> list[tuple[str, str]]:
    if not has_parquet_files(spark, peaks_path):
        return DEFAULT_AIR_QUALITY_PEAKS_COLUMNS

    schema = spark.read.parquet(to_table_location_uri(peaks_path)).schema
    return [(field.name, spark_type_to_sql(field.dataType)) for field in schema.fields]


def build_create_table_sql(
    table_name: str,
    columns: list[tuple[str, str]],
    location_uri: str,
    partitions: list[str] | None = None,
) -> str:
    columns_sql = ",\n    ".join(f"{name} {dtype}" for name, dtype in columns)
    partition_sql = ""
    if partitions:
        partition_sql = f"\nPARTITIONED BY ({', '.join(partitions)})"

    return (
        f"CREATE TABLE IF NOT EXISTS {DATABASE_NAME}.{table_name} (\n"
        f"    {columns_sql}\n"
        f")\n"
        f"USING PARQUET"
        f"{partition_sql}\n"
        f"LOCATION '{sql_safe(location_uri)}'"
    )


def build_spark_session() -> SparkSession:
    hdfs_default_fs = os.environ.get("HDFS_DEFAULT_FS", "hdfs://namenode:8020")
    hive_metastore_uri = os.environ.get("HIVE_METASTORE_URI", "thrift://hive-metastore:9083")
    warehouse_dir = os.environ.get("SPARK_SQL_WAREHOUSE_DIR", "hdfs://namenode:8020/warehouse")

    spark = (
        SparkSession.builder.appName("CreateLakehouseExternalTables")
        .config("spark.hadoop.fs.defaultFS", hdfs_default_fs)
        .config("spark.sql.warehouse.dir", warehouse_dir)
        .config("spark.hadoop.hive.metastore.uris", hive_metastore_uri)
        .config("hive.metastore.uris", hive_metastore_uri)
        .config("spark.sql.catalogImplementation", "hive")
        .enableHiveSupport()
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("ERROR")
    return spark


def main() -> None:
    default_local_root = os.path.abspath("./lakehouse")
    lakehouse_root = os.environ.get("LAKEHOUSE_ROOT", "hdfs://namenode:8020/lakehouse").strip()
    if not lakehouse_root:
        lakehouse_root = default_local_root
    lakehouse_root = normalize_storage_root(lakehouse_root)

    spark = build_spark_session()

    paths = {
        "bronze_air_quality": join_storage_path(lakehouse_root, "bronze", "air_quality"),
        "bronze_weather": join_storage_path(lakehouse_root, "bronze", "weather"),
        "silver_air_quality": join_storage_path(lakehouse_root, "silver", "air_quality"),
        "silver_weather": join_storage_path(lakehouse_root, "silver", "weather"),
        "gold_air_quality_metrics": join_storage_path(
            lakehouse_root, "gold", "air_quality_aggregates"
        ),
        "gold_air_quality_peaks": join_storage_path(
            lakehouse_root, "gold", "air_quality_aggregates", "peaks"
        ),
        "gold_weather_metrics": join_storage_path(lakehouse_root, "gold", "weather_aggregates"),
        "rejects_air_quality": join_storage_path(lakehouse_root, "rejects", "air_quality"),
        "rejects_weather": join_storage_path(lakehouse_root, "rejects", "weather"),
    }

    for path in paths.values():
        ensure_storage_path_exists(spark, path)

    peaks_columns = infer_air_quality_peaks_columns(spark, paths["gold_air_quality_peaks"])

    tables = [
        {
            "name": "bronze_air_quality",
            "location": paths["bronze_air_quality"],
            "columns": [
                ("raw_value", "STRING"),
                ("kafka_timestamp", "TIMESTAMP"),
                ("topic", "STRING"),
                ("partition", "INT"),
                ("offset", "BIGINT"),
                ("ingest_date", "DATE"),
            ],
            "partitions": ["ingest_date"],
        },
        {
            "name": "bronze_weather",
            "location": paths["bronze_weather"],
            "columns": [
                ("raw_value", "STRING"),
                ("kafka_timestamp", "TIMESTAMP"),
                ("topic", "STRING"),
                ("partition", "INT"),
                ("offset", "BIGINT"),
                ("ingest_date", "DATE"),
            ],
            "partitions": ["ingest_date"],
        },
        {
            "name": "silver_air_quality",
            "location": paths["silver_air_quality"],
            "columns": [
                ("source", "STRING"),
                ("schema_version", "STRING"),
                ("pipeline", "STRING"),
                ("pipeline_version", "STRING"),
                ("country", "STRING"),
                ("city", "STRING"),
                ("station_city", "STRING"),
                ("target_city", "STRING"),
                ("parameter", "STRING"),
                ("value", "DOUBLE"),
                ("unit", "STRING"),
                ("latitude", "DOUBLE"),
                ("longitude", "DOUBLE"),
                ("event_time", "TIMESTAMP"),
                ("ingestion_time", "STRING"),
                ("location_id", "STRING"),
                ("sensor_id", "STRING"),
                ("provider", "STRING"),
                ("parameter_id", "BIGINT"),
                ("message_id", "STRING"),
                ("event_date", "DATE"),
            ],
            "partitions": ["event_date", "city"],
        },
        {
            "name": "silver_weather",
            "location": paths["silver_weather"],
            "columns": [
                ("source", "STRING"),
                ("schema_version", "STRING"),
                ("pipeline", "STRING"),
                ("pipeline_version", "STRING"),
                ("country", "STRING"),
                ("city", "STRING"),
                ("latitude", "DOUBLE"),
                ("longitude", "DOUBLE"),
                ("temperature", "DOUBLE"),
                ("temperature_unit", "STRING"),
                ("wind_speed", "DOUBLE"),
                ("wind_unit", "STRING"),
                ("humidity", "INT"),
                ("humidity_unit", "STRING"),
                ("event_time", "TIMESTAMP"),
                ("ingestion_time", "STRING"),
                ("message_id", "STRING"),
                ("temperature_c", "DOUBLE"),
                ("wind_speed_ms", "DOUBLE"),
                ("humidity_pct", "DOUBLE"),
                ("event_date", "DATE"),
            ],
            "partitions": ["event_date", "city"],
        },
        {
            "name": "gold_air_quality_metrics",
            "location": paths["gold_air_quality_metrics"],
            "columns": [
                ("window_start", "TIMESTAMP"),
                ("window_end", "TIMESTAMP"),
                ("city", "STRING"),
                ("parameter", "STRING"),
                ("rolling_avg", "DOUBLE"),
                ("status", "STRING"),
                ("window_duration", "STRING"),
                ("window_date", "DATE"),
            ],
            "partitions": ["window_date"],
        },
        {
            "name": "gold_air_quality_peaks",
            "location": paths["gold_air_quality_peaks"],
            "columns": peaks_columns,
            "partitions": None,
        },
        {
            "name": "gold_weather_metrics",
            "location": paths["gold_weather_metrics"],
            "columns": [
                ("window_start", "TIMESTAMP"),
                ("window_end", "TIMESTAMP"),
                ("city", "STRING"),
                ("avg_temperature", "DOUBLE"),
                ("avg_humidity", "DOUBLE"),
                ("avg_wind_speed", "DOUBLE"),
                ("window_duration", "STRING"),
                ("window_date", "DATE"),
            ],
            "partitions": ["window_date"],
        },
        {
            "name": "rejects_air_quality",
            "location": paths["rejects_air_quality"],
            "columns": [
                ("raw_value", "STRING"),
                ("corrupt_record", "STRING"),
                ("event_time_raw", "STRING"),
                ("value_raw", "DOUBLE"),
                ("city_raw", "STRING"),
                ("reject_reason", "STRING"),
                ("rejected_at", "TIMESTAMP"),
                ("reject_date", "DATE"),
            ],
            "partitions": ["reject_date", "reject_reason"],
        },
        {
            "name": "rejects_weather",
            "location": paths["rejects_weather"],
            "columns": [
                ("raw_value", "STRING"),
                ("corrupt_record", "STRING"),
                ("event_time_raw", "STRING"),
                ("city_raw", "STRING"),
                ("reject_reason", "STRING"),
                ("rejected_at", "TIMESTAMP"),
                ("reject_date", "DATE"),
            ],
            "partitions": ["reject_date", "reject_reason"],
        },
    ]

    print(f"LAKEHOUSE_ROOT={lakehouse_root}")
    if has_parquet_files(spark, paths["gold_air_quality_peaks"]):
        print("gold_air_quality_peaks schema source: inferred from parquet sample")
    else:
        print("gold_air_quality_peaks schema source: fallback to known streaming output schema")

    spark.sql(f"CREATE DATABASE IF NOT EXISTS {DATABASE_NAME}")

    for table in tables:
        location_uri = to_table_location_uri(table["location"])
        create_sql = build_create_table_sql(
            table_name=table["name"],
            columns=table["columns"],
            location_uri=location_uri,
            partitions=table["partitions"],
        )
        print(f"Creating table {DATABASE_NAME}.{table['name']} -> {location_uri}")
        spark.sql(create_sql)

    print("\nRegistered tables in database lakehouse:")
    spark.sql(f"SHOW TABLES IN {DATABASE_NAME}").select("tableName").orderBy("tableName").show(
        truncate=False
    )

    print("\nSmoke tests (SELECT COUNT(*)):")
    for table in tables:
        full_name = f"{DATABASE_NAME}.{table['name']}"
        count_value = spark.sql(f"SELECT COUNT(*) AS cnt FROM {full_name}").collect()[0]["cnt"]
        print(f"- {full_name}: {count_value}")

    spark.stop()


if __name__ == "__main__":
    main()
