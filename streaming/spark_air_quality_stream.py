import os
import sys
from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    avg,
    col,
    coalesce,
    concat_ws,
    current_timestamp,
    from_json,
    lag,
    length,
    lit,
    row_number,
    to_date,
    to_timestamp,
    when,
    window,
)
from pyspark.sql.types import (
    DoubleType,
    LongType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)
from pyspark.sql.window import Window

from timescale_writer import foreach_batch_air_quality, test_timescaledb_connection


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


def build_rolling_average(df, window_duration: str):
    return (
        df.groupBy(window(col("event_time"), window_duration), "city", "parameter")
        .agg(avg("value").alias("rolling_avg"))
        .select(
            col("window.start").alias("window_start"),
            col("window.end").alias("window_end"),
            col("city"),
            col("parameter"),
            col("rolling_avg"),
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
        os.path.join(os.path.dirname(__file__), "checkpoints", "air_quality_stream")
    )
    checkpoint_dir = (
        os.environ.get("AIR_QUALITY_CHECKPOINT_DIR")
        or os.environ.get("CHECKPOINT_DIR")
        or default_checkpoint
    )
    checkpoint_dir = resolve_storage_path(checkpoint_dir)

    default_timescale_checkpoint = os.path.abspath(
        os.path.join(os.path.dirname(__file__), "checkpoints", "air_quality_timescale")
    )
    timescale_checkpoint_dir = resolve_storage_path(
        os.environ.get("AIR_QUALITY_TIMESCALE_CHECKPOINT_DIR", default_timescale_checkpoint)
    )

    default_bronze_checkpoint = os.path.abspath(
        os.path.join(os.path.dirname(__file__), "checkpoints", "air_quality_bronze")
    )
    bronze_checkpoint_dir = resolve_storage_path(
        os.environ.get("AIR_QUALITY_BRONZE_CHECKPOINT_DIR", default_bronze_checkpoint)
    )

    default_peaks_checkpoint = os.path.abspath(
        os.path.join(os.path.dirname(__file__), "checkpoints", "air_quality_peaks")
    )
    peaks_checkpoint_dir = (
        os.environ.get("AIR_QUALITY_PEAKS_CHECKPOINT_DIR")
        or os.environ.get("PEAKS_CHECKPOINT_DIR")
        or default_peaks_checkpoint
    )
    peaks_checkpoint_dir = resolve_storage_path(peaks_checkpoint_dir)

    default_state_path = os.path.abspath(
        os.path.join(os.path.dirname(__file__), "checkpoints", "air_quality_peaks_state_store")
    )
    state_path = os.environ.get("PEAKS_STATE_PATH", default_state_path)
    if is_uri_path(state_path):
        raise ValueError("PEAKS_STATE_PATH must be a local filesystem path")
    state_path = os.path.abspath(state_path)
    state_path_uri = Path(state_path).resolve().as_uri()

    default_peaks_path = join_storage_path(lakehouse_root, "gold", "air_quality_aggregates", "peaks")
    peaks_path = resolve_storage_path(os.environ.get("PEAKS_OUTPUT_PATH", default_peaks_path))

    default_bronze_path = join_storage_path(lakehouse_root, "bronze", "air_quality")
    bronze_path = resolve_storage_path(os.environ.get("AIR_QUALITY_BRONZE_PATH", default_bronze_path))

    default_silver_path = join_storage_path(lakehouse_root, "silver", "air_quality")
    silver_path = resolve_storage_path(os.environ.get("AIR_QUALITY_SILVER_PATH", default_silver_path))

    default_gold_path = join_storage_path(lakehouse_root, "gold", "air_quality_aggregates")
    gold_path = resolve_storage_path(os.environ.get("AIR_QUALITY_GOLD_PATH", default_gold_path))

    default_rejects_path = join_storage_path(lakehouse_root, "rejects", "air_quality")
    rejects_path = resolve_storage_path(os.environ.get("AIR_QUALITY_REJECTS_PATH", default_rejects_path))

    default_silver_checkpoint = os.path.abspath(
        os.path.join(os.path.dirname(__file__), "checkpoints", "air_quality_silver")
    )
    silver_checkpoint_dir = resolve_storage_path(
        os.environ.get("AIR_QUALITY_SILVER_CHECKPOINT_DIR", default_silver_checkpoint)
    )

    default_gold_checkpoint = os.path.abspath(
        os.path.join(os.path.dirname(__file__), "checkpoints", "air_quality_gold")
    )
    gold_checkpoint_dir = resolve_storage_path(
        os.environ.get("AIR_QUALITY_GOLD_CHECKPOINT_DIR", default_gold_checkpoint)
    )

    default_rejects_checkpoint = os.path.abspath(
        os.path.join(os.path.dirname(__file__), "checkpoints", "air_quality_rejects")
    )
    rejects_checkpoint_dir = resolve_storage_path(
        os.environ.get("AIR_QUALITY_REJECTS_CHECKPOINT_DIR", default_rejects_checkpoint)
    )

    peaks_debug = os.environ.get("PEAKS_DEBUG", "").strip().lower() in (
        "1",
        "true",
        "yes",
        "y",
        "on",
    )
    enable_debug_stream = os.environ.get("ENABLE_DEBUG_STREAM", "").strip().lower() in (
        "1",
        "true",
        "yes",
        "y",
        "on",
    )

    # Schema aligned with schemas/air_quality.json (plus corrupt-record capture).
    air_quality_schema = StructType(
        [
            StructField("source", StringType(), True),
            StructField("schema_version", StringType(), True),
            StructField("pipeline", StringType(), True),
            StructField("pipeline_version", StringType(), True),
            StructField("country", StringType(), True),
            StructField("city", StringType(), True),
            StructField("station_city", StringType(), True),
            StructField("target_city", StringType(), True),
            StructField("parameter", StringType(), True),
            StructField("value", DoubleType(), True),
            StructField("unit", StringType(), True),
            StructField("latitude", DoubleType(), True),
            StructField("longitude", DoubleType(), True),
            StructField("event_time", StringType(), True),
            StructField("ingestion_time", StringType(), True),
            StructField("location_id", StringType(), True),
            StructField("sensor_id", StringType(), True),
            StructField("provider", StringType(), True),
            StructField("parameter_id", LongType(), True),
            StructField("message_id", StringType(), True),
            StructField("_corrupt_record", StringType(), True),
        ]
    )

    spark = build_spark_session("KafkaAirQualityStreaming")
    test_timescaledb_connection(spark)

    raw_reader = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", kafka_bootstrap)
        .option("subscribe", "air_quality_raw")
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
        from_json(col("raw_value"), air_quality_schema, json_options),
    )

    df_expanded = (
        df_parsed.select(col("raw_value"), col("data.*"))
        .withColumn("corrupt_record", col("_corrupt_record"))
        .drop("_corrupt_record")
        .withColumnRenamed("event_time", "event_time_raw")
        .withColumn("event_time", parse_event_time(col("event_time_raw")))
    )

    # Keep only the key pollutants for downstream analytics.
    pollutant_filter = col("parameter").isin("pm25", "pm10", "co", "no2", "o3")

    # Pollutant-specific quality ranges.
    # Assumptions: PM values in ug/m3; CO in ppm; NO2/O3 in ug/m3.
    valid_ranges = (
        ((col("parameter") == "pm25") & col("value").between(0, 1000))
        | ((col("parameter") == "pm10") & col("value").between(0, 1000))
        | ((col("parameter") == "co") & col("value").between(0, 50))
        | ((col("parameter") == "no2") & col("value").between(0, 1000))
        | ((col("parameter") == "o3") & col("value").between(0, 1000))
    )

    df_rejects = (
        df_expanded.withColumn(
            "reject_reason",
            when(col("corrupt_record").isNotNull(), lit("corrupt_json"))
            .when(col("event_time").isNull(), lit("missing_event_time"))
            .when(col("value").isNull(), lit("missing_value"))
            .when(col("city").isNull(), lit("missing_city"))
            .when(pollutant_filter & (~valid_ranges), lit("invalid_range")),
        )
        .filter(col("reject_reason").isNotNull())
        .select(
            col("raw_value"),
            col("corrupt_record"),
            col("event_time_raw"),
            col("value").cast("double").alias("value_raw"),
            col("city").alias("city_raw"),
            col("reject_reason"),
            current_timestamp().alias("rejected_at"),
        )
        .withColumn("reject_date", to_date(col("rejected_at")))
    )

    # 1) Remove unusable records required for downstream validation.
    df_valid = (
        df_expanded.filter(col("corrupt_record").isNull())
        .filter(col("event_time").isNotNull())
        .filter(col("value").isNotNull())
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

    # Silver uses a stateless stream branch to improve checkpoint stability on local FS.
    df_silver = (
        df_valid_for_processing.filter(pollutant_filter)
        .filter(valid_ranges)
        .withColumn("event_date", to_date(col("event_time")))
    )

    # Silver layer: cleaned raw events (append-only) for historical analysis.
    silver_query = (
        df_silver.writeStream.outputMode("append")
        .format("parquet")
        .option("path", silver_path)
        .option("checkpointLocation", silver_checkpoint_dir)
        .partitionBy("event_date", "city")
        .trigger(processingTime=trigger_interval)
        .start()
    )

    # Analytics branch keeps event-time deduplication before windowed aggregates.
    dedup_key = when(
        col("message_id").isNotNull() & (length(col("message_id")) > 0),
        col("message_id"),
    ).otherwise(
        concat_ws(
            "||",
            col("city"),
            col("parameter"),
            col("event_time").cast("string"),
        )
    )

    df_deduped = (
        df_valid_for_processing.withWatermark("event_time", watermark_delay)
        .withColumn("dedup_key", dedup_key)
        .dropDuplicates(["dedup_key"])
        .drop("dedup_key")
    )
    df_cleaned = df_deduped.filter(pollutant_filter).filter(valid_ranges)

    # Rolling averages (event-time windows) for 5 and 10 minutes.
    avg_5m = build_rolling_average(df_cleaned, "5 minutes")
    avg_10m = build_rolling_average(df_cleaned, "10 minutes")
    df_rolling = avg_5m.unionByName(avg_10m)

    # Classify rolling averages with pollutant thresholds (units per assumptions above).
    # Thresholds: PM2.5 25/50, PM10 50/100, CO 2/9, NO2 100/200, O3 120/180.
    df_with_status = df_rolling.withColumn(
        "status",
        when((col("parameter") == "pm25") & (col("rolling_avg") < 25), "OK")
        .when(
            (col("parameter") == "pm25") & (col("rolling_avg").between(25, 50)),
            "WARNING",
        )
        .when((col("parameter") == "pm25") & (col("rolling_avg") > 50), "DANGER")
        .when((col("parameter") == "pm10") & (col("rolling_avg") < 50), "OK")
        .when(
            (col("parameter") == "pm10") & (col("rolling_avg").between(50, 100)),
            "WARNING",
        )
        .when((col("parameter") == "pm10") & (col("rolling_avg") > 100), "DANGER")
        .when((col("parameter") == "co") & (col("rolling_avg") < 2), "OK")
        .when((col("parameter") == "co") & (col("rolling_avg").between(2, 9)), "WARNING")
        .when((col("parameter") == "co") & (col("rolling_avg") > 9), "DANGER")
        .when((col("parameter") == "no2") & (col("rolling_avg") < 100), "OK")
        .when(
            (col("parameter") == "no2") & (col("rolling_avg").between(100, 200)),
            "WARNING",
        )
        .when((col("parameter") == "no2") & (col("rolling_avg") > 200), "DANGER")
        .when((col("parameter") == "o3") & (col("rolling_avg") < 120), "OK")
        .when(
            (col("parameter") == "o3") & (col("rolling_avg").between(120, 180)),
            "WARNING",
        )
        .when((col("parameter") == "o3") & (col("rolling_avg") > 180), "DANGER")
        .otherwise("UNKNOWN"),
    )

    # STEP 7: Peak detection via foreachBatch (keeps main stream in update mode).
    state_schema = StructType(
        [
            StructField("city", StringType(), True),
            StructField("parameter", StringType(), True),
            StructField("window_duration", StringType(), True),
            StructField("prev_rolling_avg", DoubleType(), True),
            StructField("prev_window_start", TimestampType(), True),
        ]
    )

    def process_peaks(batch_df, batch_id):
        if batch_df.rdd.isEmpty():
            return

        spark_session = batch_df.sparkSession

        current = batch_df.select(
            "window_start",
            "window_end",
            "city",
            "parameter",
            "rolling_avg",
            "status",
            "window_duration",
        )

        if os.path.isdir(state_path) and os.listdir(state_path):
            prev_state = spark_session.read.parquet(state_path_uri).select(
                "city",
                "parameter",
                "window_duration",
                "prev_rolling_avg",
                "prev_window_start",
            )
        else:
            prev_state = spark_session.createDataFrame([], schema=state_schema)

        prev_state_for_join = (
            prev_state.withColumnRenamed("prev_rolling_avg", "state_prev_rolling_avg")
            .withColumnRenamed("prev_window_start", "state_prev_window_start")
        )

        order_window = Window.partitionBy("city", "parameter", "window_duration").orderBy(
            col("window_start")
        )
        desc_window = Window.partitionBy("city", "parameter", "window_duration").orderBy(
            col("window_start").desc()
        )

        current_with_lag = current.withColumn("prev_in_batch", lag("rolling_avg").over(order_window))

        current_with_prev = (
            current_with_lag.join(
                prev_state_for_join,
                on=["city", "parameter", "window_duration"],
                how="left",
            )
            .withColumn(
                "prev_rolling_avg",
                coalesce(col("prev_in_batch"), col("state_prev_rolling_avg")),
            )
            .drop("prev_in_batch", "state_prev_rolling_avg", "state_prev_window_start")
        )

        # Peak delta thresholds (assumptions: CO ppm; NO2/O3 ug/m3).
        peaks = (
            current_with_prev.withColumn("delta", col("rolling_avg") - col("prev_rolling_avg"))
            .withColumn(
                "is_peak",
                when((col("parameter") == "pm25") & (col("delta") >= 15), True)
                .when((col("parameter") == "pm10") & (col("delta") >= 30), True)
                .when((col("parameter") == "co") & (col("delta") >= 1), True)
                .when((col("parameter") == "no2") & (col("delta") >= 50), True)
                .when((col("parameter") == "o3") & (col("delta") >= 60), True)
                .otherwise(False),
            )
        )

        peaks_only = peaks.filter(col("is_peak") == True)

        if not peaks_only.rdd.isEmpty():
            peaks_only.write.mode("append").parquet(peaks_path)

        if peaks_debug:
            peaks_only.select(
                "window_start",
                "window_end",
                "city",
                "parameter",
                "rolling_avg",
                "status",
                "delta",
                "is_peak",
                "window_duration",
            ).show(truncate=False)

        latest_in_batch = (
            current.select(
                "city",
                "parameter",
                "window_duration",
                "rolling_avg",
                "window_start",
            )
            .withColumn("row_num", row_number().over(desc_window))
            .filter(col("row_num") == 1)
            .drop("row_num")
            .withColumnRenamed("rolling_avg", "prev_rolling_avg")
            .withColumnRenamed("window_start", "prev_window_start")
        )

        merged_state = prev_state.unionByName(latest_in_batch)

        latest_state = (
            merged_state.withColumn(
                "row_num",
                row_number().over(
                    Window.partitionBy("city", "parameter", "window_duration").orderBy(
                        col("prev_window_start").desc()
                    )
                ),
            )
            .filter(col("row_num") == 1)
            .drop("row_num")
        )

        # Materialize before overwrite to avoid self-read/write race on the same path.
        state_to_write = latest_state.cache()
        state_to_write.count()
        state_to_write.write.mode("overwrite").parquet(state_path_uri)
        state_to_write.unpersist()

    output_cols = [
        "window_start",
        "window_end",
        "city",
        "parameter",
        "rolling_avg",
        "status",
        "window_duration",
    ]

    def process_air_quality_batch(batch_df, batch_id):
        output_batch = batch_df.select(*output_cols).cache()
        try:
            if output_batch.rdd.isEmpty():
                return

            process_peaks(output_batch, batch_id)

            gold_batch = output_batch.dropDuplicates(
                ["window_start", "window_end", "city", "parameter", "window_duration"]
            ).withColumn("window_date", to_date(col("window_start")))
            gold_batch.write.mode("append").partitionBy("window_date").parquet(gold_path)

            foreach_batch_air_quality(output_batch, batch_id)

            if enable_debug_stream:
                output_batch.orderBy(col("window_start").desc()).show(20, truncate=False)
        finally:
            output_batch.unpersist()

    console_query = (
        df_with_status.select(*output_cols)
        .writeStream.outputMode("update")
        .format("console")
        .option("truncate", "false")
        .option("checkpointLocation", checkpoint_dir)
        .trigger(processingTime=trigger_interval)
        .start()
    )

    query = (
        df_with_status.select(*output_cols)
        .writeStream.outputMode("update")
        .foreachBatch(process_air_quality_batch)
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
