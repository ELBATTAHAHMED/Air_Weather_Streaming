import os
import sys

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    avg,
    col,
    coalesce,
    concat_ws,
    current_timestamp,
    from_json,
    length,
    lit,
    lower,
    to_date,
    to_timestamp,
    trim,
    when,
    window,
)
from pyspark.sql.types import DoubleType, IntegerType, StringType, StructField, StructType

from timescale_writer import foreach_batch_weather, test_timescaledb_connection


def require_hadoop_native_windows() -> None:
    if os.name != "nt":
        return

    hadoop_home = os.environ.get("HADOOP_HOME")
    if not hadoop_home:
        sys.stderr.write(
            "HADOOP_HOME is not set. On Windows, Spark needs Hadoop native "
            "libraries (hadoop.dll) to run streaming checkpoints. Set HADOOP_HOME "
            "to your Hadoop folder (e.g. C:\\hadoop) and ensure bin contains "
            "winutils.exe and hadoop.dll.\n"
        )
        sys.exit(1)

    hadoop_dll = os.path.join(hadoop_home, "bin", "hadoop.dll")
    if not os.path.isfile(hadoop_dll):
        sys.stderr.write(
            "Missing Hadoop native library: "
            f"{hadoop_dll}. Place the matching hadoop.dll next to winutils.exe "
            "and ensure HADOOP_HOME is set.\n"
        )
        sys.exit(1)


def is_uri_path(path_value: str) -> bool:
    return "://" in path_value


def resolve_storage_path(path_value: str) -> str:
    return path_value if is_uri_path(path_value) else os.path.abspath(path_value)


def join_storage_path(root_path: str, *parts: str) -> str:
    if is_uri_path(root_path):
        suffix = "/".join(part.strip("/\\") for part in parts if part)
        return f"{root_path.rstrip('/')}/{suffix}"

    return os.path.abspath(os.path.join(root_path, *parts))


def build_spark_session(app_name: str) -> SparkSession:
    hdfs_default_fs = os.environ.get("HDFS_DEFAULT_FS", "hdfs://namenode:8020")
    hive_metastore_uri = os.environ.get("HIVE_METASTORE_URI", "thrift://hive-metastore:9083")
    warehouse_dir = os.environ.get("SPARK_SQL_WAREHOUSE_DIR", "hdfs://namenode:8020/warehouse")

    spark = (
        SparkSession.builder.appName(app_name)
        .config("spark.ui.showConsoleProgress", "false")
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


def parse_event_time(event_time_col):
    return coalesce(
        to_timestamp(event_time_col, "yyyy-MM-dd'T'HH:mm:ss.SSSXXX"),
        to_timestamp(event_time_col, "yyyy-MM-dd'T'HH:mm:ss.SSSX"),
        to_timestamp(event_time_col, "yyyy-MM-dd'T'HH:mm:ssXXX"),
        to_timestamp(event_time_col, "yyyy-MM-dd'T'HH:mm:ssX"),
        to_timestamp(event_time_col),
    )


def build_weather_indicators(df, window_duration: str):
    return (
        df.groupBy(window(col("event_time"), window_duration), "city")
        .agg(
            avg("temperature_c").alias("avg_temperature"),
            avg("humidity_pct").alias("avg_humidity"),
            avg("wind_speed_ms").alias("avg_wind_speed"),
        )
        .select(
            col("window.start").alias("window_start"),
            col("window.end").alias("window_end"),
            col("city"),
            col("avg_temperature"),
            col("avg_humidity"),
            col("avg_wind_speed"),
            lit(window_duration).alias("window_duration"),
        )
    )


def main() -> None:
    require_hadoop_native_windows()

    kafka_bootstrap = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
    starting_offsets = os.environ.get("STARTING_OFFSETS", "latest")
    watermark_delay = os.environ.get("WATERMARK_DELAY", "6 hours")
    trigger_interval = os.environ.get("TRIGGER_INTERVAL", "5 seconds")
    max_offsets_per_trigger = os.environ.get("MAX_OFFSETS_PER_TRIGGER", "").strip()
    project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
    default_lakehouse_root = os.path.abspath(os.path.join(project_root, "lakehouse"))
    lakehouse_root = os.environ.get("LAKEHOUSE_ROOT", "hdfs://namenode:8020/lakehouse").strip()
    if not lakehouse_root:
        lakehouse_root = default_lakehouse_root
    lakehouse_root = resolve_storage_path(lakehouse_root)

    default_checkpoint = os.path.abspath(
        os.path.join(os.path.dirname(__file__), "checkpoints", "weather_indicators")
    )
    checkpoint_dir = resolve_storage_path(os.environ.get("WEATHER_CHECKPOINT_DIR", default_checkpoint))

    default_timescale_checkpoint = os.path.abspath(
        os.path.join(os.path.dirname(__file__), "checkpoints", "weather_timescale")
    )
    timescale_checkpoint_dir = resolve_storage_path(
        os.environ.get("WEATHER_TIMESCALE_CHECKPOINT_DIR", default_timescale_checkpoint)
    )

    default_bronze_checkpoint = os.path.abspath(
        os.path.join(os.path.dirname(__file__), "checkpoints", "weather_bronze")
    )
    bronze_checkpoint_dir = resolve_storage_path(
        os.environ.get("WEATHER_BRONZE_CHECKPOINT_DIR", default_bronze_checkpoint)
    )

    default_bronze_path = join_storage_path(lakehouse_root, "bronze", "weather")
    bronze_path = resolve_storage_path(os.environ.get("WEATHER_BRONZE_PATH", default_bronze_path))

    default_silver_path = join_storage_path(lakehouse_root, "silver", "weather")
    silver_path = resolve_storage_path(os.environ.get("WEATHER_SILVER_PATH", default_silver_path))

    default_gold_path = join_storage_path(lakehouse_root, "gold", "weather_aggregates")
    gold_path = resolve_storage_path(os.environ.get("WEATHER_GOLD_PATH", default_gold_path))

    default_rejects_path = join_storage_path(lakehouse_root, "rejects", "weather")
    rejects_path = resolve_storage_path(os.environ.get("WEATHER_REJECTS_PATH", default_rejects_path))

    default_silver_checkpoint = os.path.abspath(
        os.path.join(os.path.dirname(__file__), "checkpoints", "weather_silver")
    )
    silver_checkpoint_dir = resolve_storage_path(
        os.environ.get("WEATHER_SILVER_CHECKPOINT_DIR", default_silver_checkpoint)
    )

    default_gold_checkpoint = os.path.abspath(
        os.path.join(os.path.dirname(__file__), "checkpoints", "weather_gold")
    )
    gold_checkpoint_dir = resolve_storage_path(
        os.environ.get("WEATHER_GOLD_CHECKPOINT_DIR", default_gold_checkpoint)
    )

    default_rejects_checkpoint = os.path.abspath(
        os.path.join(os.path.dirname(__file__), "checkpoints", "weather_rejects")
    )
    rejects_checkpoint_dir = resolve_storage_path(
        os.environ.get("WEATHER_REJECTS_CHECKPOINT_DIR", default_rejects_checkpoint)
    )

    # Schema aligned with schemas/weather.json (plus corrupt-record capture).
    weather_schema = StructType(
        [
            StructField("source", StringType(), True),
            StructField("schema_version", StringType(), True),
            StructField("pipeline", StringType(), True),
            StructField("pipeline_version", StringType(), True),
            StructField("country", StringType(), True),
            StructField("city", StringType(), True),
            StructField("latitude", DoubleType(), True),
            StructField("longitude", DoubleType(), True),
            StructField("temperature", DoubleType(), True),
            StructField("temperature_unit", StringType(), True),
            StructField("wind_speed", DoubleType(), True),
            StructField("wind_unit", StringType(), True),
            StructField("humidity", IntegerType(), True),
            StructField("humidity_unit", StringType(), True),
            StructField("event_time", StringType(), True),
            StructField("ingestion_time", StringType(), True),
            StructField("message_id", StringType(), True),
            StructField("_corrupt_record", StringType(), True),
        ]
    )

    spark = build_spark_session("KafkaWeatherStreaming")
    test_timescaledb_connection(spark)

    raw_reader = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", kafka_bootstrap)
        .option("subscribe", "weather_raw")
        .option("startingOffsets", starting_offsets)
        .option("failOnDataLoss", "false")
    )
    if max_offsets_per_trigger:
        raw_reader = raw_reader.option("maxOffsetsPerTrigger", max_offsets_per_trigger)
    df_raw = raw_reader.load()

    df_bronze = (
        df_raw.selectExpr(
            "CAST(value AS STRING) AS raw_value",
            "timestamp AS kafka_timestamp",
            "topic",
            "partition",
            "offset",
        )
        .withColumn("ingest_date", to_date(col("kafka_timestamp")))
    )

    bronze_query = (
        df_bronze.writeStream.outputMode("append")
        .format("parquet")
        .option("path", bronze_path)
        .option("checkpointLocation", bronze_checkpoint_dir)
        .partitionBy("ingest_date")
        .trigger(processingTime=trigger_interval)
        .start()
    )

    json_options = {
        "mode": "PERMISSIVE",
        "columnNameOfCorruptRecord": "_corrupt_record",
    }

    df_parsed = df_raw.select(col("value").cast("string").alias("raw_value")).withColumn(
        "data",
        from_json(col("raw_value"), weather_schema, json_options),
    )

    df_expanded = (
        df_parsed.select(col("raw_value"), col("data.*"))
        .withColumn("corrupt_record", col("_corrupt_record"))
        .drop("_corrupt_record")
        .withColumnRenamed("event_time", "event_time_raw")
        .withColumn("event_time", parse_event_time(col("event_time_raw")))
    )

    # Normalize units for consistent cleaning and aggregation.
    temp_unit = lower(trim(col("temperature_unit")))
    wind_unit = lower(trim(col("wind_unit")))
    humidity_unit = lower(trim(col("humidity_unit")))

    df_normalized = (
        df_expanded.withColumn(
            "temperature_c",
            when(temp_unit.isin("f", "°f", "fahrenheit"), (col("temperature") - 32) * 5 / 9)
            .when(temp_unit.isin("c", "°c", "celsius") | temp_unit.isNull(), col("temperature"))
            .otherwise(col("temperature")),
        )
        .withColumn(
            "wind_speed_ms",
            when(wind_unit.isin("km/h", "kmh", "kph"), col("wind_speed") / 3.6)
            .when(wind_unit.isin("m/s", "ms") | wind_unit.isNull(), col("wind_speed"))
            .otherwise(col("wind_speed")),
        )
        .withColumn(
            "humidity_pct",
            when(
                humidity_unit.isin("%", "percent", "pct") | humidity_unit.isNull(),
                col("humidity").cast("double"),
            ).otherwise(col("humidity").cast("double")),
        )
    )

    valid_ranges = (
        col("temperature_c").between(-50, 60)
        & col("humidity_pct").between(0, 100)
        & (col("wind_speed_ms") >= 0)
    )

    df_rejects = (
        df_normalized.withColumn(
            "reject_reason",
            when(col("corrupt_record").isNotNull(), lit("corrupt_json"))
            .when(col("event_time").isNull(), lit("missing_event_time"))
            .when(col("city").isNull(), lit("missing_city"))
            .when(~valid_ranges, lit("invalid_range")),
        )
        .filter(col("reject_reason").isNotNull())
        .select(
            col("raw_value"),
            col("corrupt_record"),
            col("event_time_raw"),
            col("city").alias("city_raw"),
            col("reject_reason"),
            current_timestamp().alias("rejected_at"),
        )
        .withColumn("reject_date", to_date(col("rejected_at")))
    )

    # 1) Remove unusable records required for downstream validation.
    df_valid = (
        df_normalized.filter(col("corrupt_record").isNull())
        .filter(col("event_time").isNotNull())
        .filter(col("city").isNotNull())
    )

    rejects_query = (
        df_rejects.writeStream.outputMode("append")
        .format("parquet")
        .option("path", rejects_path)
        .option("checkpointLocation", rejects_checkpoint_dir)
        .partitionBy("reject_date", "reject_reason")
        .trigger(processingTime=trigger_interval)
        .start()
    )

    df_valid_for_processing = df_valid.drop("raw_value", "event_time_raw", "corrupt_record")

    # 2) Apply watermark once, then deduplicate using message_id when available.
    dedup_key = when(
        col("message_id").isNotNull() & (length(col("message_id")) > 0),
        col("message_id"),
    ).otherwise(
        concat_ws(
            "||",
            col("city"),
            col("event_time").cast("string"),
            col("source"),
            coalesce(col("temperature_c").cast("string"), lit("")),
            coalesce(col("humidity_pct").cast("string"), lit("")),
            coalesce(col("wind_speed_ms").cast("string"), lit("")),
        )
    )

    df_deduped = (
        df_valid_for_processing.withWatermark("event_time", watermark_delay)
        .withColumn("dedup_key", dedup_key)
        .dropDuplicates(["dedup_key"])
        .drop("dedup_key")
    )

    # 3) Validate obvious ranges (physical plausibility checks).
    df_cleaned = df_deduped.filter(valid_ranges)

    df_silver = df_cleaned.withColumn("event_date", to_date(col("event_time")))

    silver_query = (
        df_silver.writeStream.outputMode("append")
        .format("parquet")
        .option("path", silver_path)
        .option("checkpointLocation", silver_checkpoint_dir)
        .partitionBy("event_date", "city")
        .trigger(processingTime=trigger_interval)
        .start()
    )

    avg_5m = build_weather_indicators(df_cleaned, "5 minutes")
    avg_10m = build_weather_indicators(df_cleaned, "10 minutes")
    df_indicators = avg_5m.unionByName(avg_10m)

    output_cols = [
        "window_start",
        "window_end",
        "city",
        "avg_temperature",
        "avg_humidity",
        "avg_wind_speed",
        "window_duration",
    ]

    df_gold = df_indicators.select(*output_cols).withColumn("window_date", to_date(col("window_start")))

    gold_query = (
        df_gold.writeStream.outputMode("append")
        .format("parquet")
        .option("path", gold_path)
        .option("checkpointLocation", gold_checkpoint_dir)
        .partitionBy("window_date")
        .trigger(processingTime=trigger_interval)
        .start()
    )

    query = (
        df_indicators.select(*output_cols)
        .writeStream.outputMode("update")
        .format("console")
        .option("truncate", "false")
        .option("checkpointLocation", checkpoint_dir)
        .trigger(processingTime=trigger_interval)
        .start()
    )

    timescale_query = (
        df_indicators.select(*output_cols)
        .writeStream.outputMode("update")
        .foreachBatch(foreach_batch_weather)
        .option("checkpointLocation", timescale_checkpoint_dir)
        .trigger(processingTime=trigger_interval)
        .start()
    )

    try:
        spark.streams.awaitAnyTermination()
    finally:
        for active_query in spark.streams.active:
            active_query.stop()


if __name__ == "__main__":
    main()
