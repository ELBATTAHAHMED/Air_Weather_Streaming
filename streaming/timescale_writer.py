import logging
import os
import re
import time
from typing import Any, Dict, Optional
from urllib.parse import parse_qsl, quote, unquote, urlencode, urlparse

import psycopg2
from psycopg2.extras import execute_values
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import col


LOGGER = logging.getLogger(__name__)
_CONNECTION_DEBUG_LOGGED = False


def _is_truthy(value: str) -> bool:
    return str(value).strip().lower() in {"1", "true", "yes", "y", "on"}


def _get_int_env(var_name: str, default_value: int) -> int:
    raw_value = os.environ.get(var_name, str(default_value)).strip()
    try:
        return int(raw_value)
    except ValueError:
        LOGGER.warning(
            "Invalid integer for %s='%s'. Falling back to %s.",
            var_name,
            raw_value,
            default_value,
        )
        return default_value


def _fail_fast() -> bool:
    return _is_truthy(os.environ.get("TIMESCALE_FAIL_FAST", "true"))


def _use_upsert() -> bool:
    return _is_truthy(os.environ.get("TIMESCALE_USE_UPSERT", "true"))


def _validate_sql_identifier(identifier: str) -> str:
    cleaned = str(identifier or "").strip()
    if not cleaned:
        raise ValueError("Timescale table identifier is empty.")

    for part in cleaned.split("."):
        if not re.match(r"^[A-Za-z_][A-Za-z0-9_]*$", part):
            raise ValueError(f"Invalid SQL identifier part: '{part}'")

    return cleaned


def _normalize_env_value(raw_value: Optional[str]) -> Optional[str]:
    if raw_value is None:
        return None

    cleaned = str(raw_value).strip()
    if len(cleaned) >= 2 and cleaned[0] == cleaned[-1] and cleaned[0] in {'"', "'"}:
        cleaned = cleaned[1:-1].strip()

    return cleaned


def _get_str_env(var_name: str, default_value: str) -> str:
    env_value = _normalize_env_value(os.environ.get(var_name))
    if not env_value:
        return default_value
    return env_value


def _get_optional_env(var_name: str) -> Optional[str]:
    env_value = _normalize_env_value(os.environ.get(var_name))
    if not env_value:
        return None
    return env_value


def _parse_jdbc_url(jdbc_url: str) -> Optional[Dict[str, Any]]:
    raw_url = _normalize_env_value(jdbc_url)
    if not raw_url:
        return None

    prefix = "jdbc:postgresql://"
    if not raw_url.lower().startswith(prefix):
        LOGGER.warning(
            "Ignoring TIMESCALE_JDBC_URL: only 'jdbc:postgresql://' URLs are supported."
        )
        return None

    parsed = urlparse(raw_url[len("jdbc:") :])
    if not parsed.hostname:
        LOGGER.warning("Ignoring TIMESCALE_JDBC_URL: unable to parse host from JDBC URL.")
        return None

    try:
        parsed_port = str(parsed.port) if parsed.port else None
    except ValueError:
        LOGGER.warning("Ignoring TIMESCALE_JDBC_URL: invalid port value in JDBC URL.")
        return None

    query_params: Dict[str, str] = {}
    query_user: Optional[str] = None
    query_password: Optional[str] = None
    for key, value in parse_qsl(parsed.query, keep_blank_values=True):
        lowered_key = key.lower()
        if lowered_key == "user":
            query_user = value
            continue
        if lowered_key == "password":
            query_password = value
            continue
        query_params[key] = value

    return {
        "host": parsed.hostname,
        "port": parsed_port,
        "database": unquote(parsed.path.lstrip("/")) if parsed.path else None,
        "user": unquote(parsed.username) if parsed.username else query_user,
        "password": unquote(parsed.password) if parsed.password else query_password,
        "query_params": query_params,
    }


def _build_jdbc_url(host: str, port: str, database: str, query_params: Dict[str, str]) -> str:
    encoded_db = quote(database, safe="")
    host_for_url = host
    if ":" in host and not host.startswith("["):
        host_for_url = f"[{host}]"

    query_string = urlencode(query_params)
    jdbc_url = f"jdbc:postgresql://{host_for_url}:{port}/{encoded_db}"
    if query_string:
        jdbc_url = f"{jdbc_url}?{query_string}"

    return jdbc_url


def _get_connection_settings() -> Dict[str, str]:
    host = _get_str_env("TIMESCALE_HOST", "localhost")
    port = _get_str_env("TIMESCALE_PORT", "5432")
    database = _get_str_env("TIMESCALE_DB", "air_quality")

    env_user = _get_optional_env("TIMESCALE_USER")
    env_password = _get_optional_env("TIMESCALE_PASSWORD")
    user = env_user or "air_quality"
    password = env_password or "air_quality"

    query_params: Dict[str, str] = {
        "sslmode": _get_str_env("TIMESCALE_SSLMODE", "disable"),
        "connectTimeout": str(_get_int_env("TIMESCALE_CONNECT_TIMEOUT", 10)),
        "socketTimeout": str(_get_int_env("TIMESCALE_SOCKET_TIMEOUT", 30)),
        "reWriteBatchedInserts": "true",
    }

    jdbc_url_override = _get_optional_env("TIMESCALE_JDBC_URL")
    if jdbc_url_override:
        parsed_override = _parse_jdbc_url(jdbc_url_override)
        if parsed_override:
            host = parsed_override.get("host") or host
            port = parsed_override.get("port") or port
            database = parsed_override.get("database") or database
            query_params.update(parsed_override.get("query_params", {}))

            # Keep env vars as source of truth when they are explicitly provided.
            if env_user is None and parsed_override.get("user"):
                user = str(parsed_override["user"])
            if env_password is None and parsed_override.get("password"):
                password = str(parsed_override["password"])

    return {
        "host": host,
        "port": port,
        "database": database,
        "user": user,
        "password": password,
        "url": _build_jdbc_url(host, port, database, query_params),
        "jdbc_url_override": "set" if jdbc_url_override else "unset",
    }


def _log_connection_settings(connection_settings: Dict[str, str], context: str) -> None:
    LOGGER.warning(
        "TimescaleDB connection (%s): host=%s port=%s db=%s user=%s password_set=%s jdbc_url_override=%s",
        context,
        connection_settings["host"],
        connection_settings["port"],
        connection_settings["database"],
        connection_settings["user"],
        bool(connection_settings.get("password")),
        connection_settings["jdbc_url_override"],
    )


def _get_connection_options(table_name: str) -> tuple[Dict[str, str], Dict[str, str]]:
    connection_settings = _get_connection_settings()
    options = {
        "url": connection_settings["url"],
        "dbtable": table_name,
        "user": connection_settings["user"],
        "password": connection_settings["password"],
        "driver": _get_str_env("TIMESCALE_JDBC_DRIVER", "org.postgresql.Driver"),
        "batchsize": str(_get_int_env("TIMESCALE_JDBC_BATCHSIZE", 1000)),
        "isolationLevel": _get_str_env("TIMESCALE_ISOLATION_LEVEL", "READ_COMMITTED"),
        "stringtype": _get_str_env("TIMESCALE_STRINGTYPE", "unspecified"),
    }
    return options, connection_settings


def test_timescaledb_connection(spark: SparkSession) -> bool:
    connection_settings = _get_connection_settings()
    _log_connection_settings(connection_settings, context="startup-test")

    test_options = {
        "url": connection_settings["url"],
        "dbtable": "(SELECT 1) AS timescaledb_connection_test",
        "user": connection_settings["user"],
        "password": connection_settings["password"],
        "driver": _get_str_env("TIMESCALE_JDBC_DRIVER", "org.postgresql.Driver"),
    }

    try:
        spark.read.format("jdbc").options(**test_options).load().collect()
        LOGGER.info("TimescaleDB startup connection test succeeded.")
        return True
    except Exception as exc:
        LOGGER.exception("TimescaleDB startup connection test failed: %s", exc)
        if _fail_fast():
            raise
        return False


def _with_target_partitions(df: DataFrame) -> DataFrame:
    target_partitions = max(1, _get_int_env("TIMESCALE_WRITE_PARTITIONS", 1))
    current_partitions = df.rdd.getNumPartitions()

    if current_partitions == target_partitions:
        return df
    if current_partitions > target_partitions:
        return df.coalesce(target_partitions)
    return df.repartition(target_partitions)


def write_to_timescaledb(df: DataFrame, table_name: str) -> None:
    """Write a micro-batch DataFrame to a TimescaleDB table over JDBC."""
    global _CONNECTION_DEBUG_LOGGED

    if df is None or df.rdd.isEmpty():
        LOGGER.debug("No rows to write for table '%s'.", table_name)
        return

    options, connection_settings = _get_connection_options(table_name)
    if not _CONNECTION_DEBUG_LOGGED:
        _log_connection_settings(connection_settings, context="writer")
        _CONNECTION_DEBUG_LOGGED = True

    mode = os.environ.get("TIMESCALE_WRITE_MODE", "append")
    max_retries = max(0, _get_int_env("TIMESCALE_MAX_RETRIES", 3))
    retry_delay_seconds = max(1, _get_int_env("TIMESCALE_RETRY_DELAY_SECONDS", 5))

    for attempt in range(max_retries + 1):
        try:
            # Keep partition count under control to avoid opening too many JDBC connections.
            write_df = _with_target_partitions(df)

            (
                write_df.write.format("jdbc")
                .mode(mode)
                .options(**options)
                .save()
            )

            LOGGER.info("Wrote micro-batch to TimescaleDB table '%s'.", table_name)
            return
        except Exception as exc:
            if attempt < max_retries:
                LOGGER.warning(
                    "Write to '%s' failed (attempt %s/%s): %s. Retrying in %ss.",
                    table_name,
                    attempt + 1,
                    max_retries + 1,
                    exc,
                    retry_delay_seconds,
                )
                time.sleep(retry_delay_seconds)
                continue

            LOGGER.exception("Failed writing micro-batch to '%s' after retries: %s", table_name, exc)
            if _fail_fast():
                raise
            return


def upsert_to_timescaledb(
    df: DataFrame,
    table_name: str,
    columns: list[str],
    conflict_columns: list[str],
) -> None:
    """Upsert a micro-batch DataFrame into TimescaleDB with ON CONFLICT."""
    global _CONNECTION_DEBUG_LOGGED

    if df is None or df.rdd.isEmpty():
        LOGGER.debug("No rows to upsert for table '%s'.", table_name)
        return

    safe_table_name = _validate_sql_identifier(table_name)
    safe_columns = [_validate_sql_identifier(column_name) for column_name in columns]
    safe_conflict_columns = [
        _validate_sql_identifier(column_name) for column_name in conflict_columns
    ]

    prepared_df = df.select(*[col(column_name) for column_name in safe_columns]).dropDuplicates(
        safe_conflict_columns
    )
    rows = [tuple(row[column_name] for column_name in safe_columns) for row in prepared_df.collect()]
    if not rows:
        LOGGER.debug("No rows remain after deduplication for table '%s'.", table_name)
        return

    connection_settings = _get_connection_settings()
    if not _CONNECTION_DEBUG_LOGGED:
        _log_connection_settings(connection_settings, context="upsert-writer")
        _CONNECTION_DEBUG_LOGGED = True

    update_columns = [column_name for column_name in safe_columns if column_name not in safe_conflict_columns]
    quoted_columns = ", ".join([f'"{column_name}"' for column_name in safe_columns])
    quoted_conflict_columns = ", ".join(
        [f'"{column_name}"' for column_name in safe_conflict_columns]
    )

    set_clauses = [
        f'"{column_name}" = EXCLUDED."{column_name}"' for column_name in update_columns
    ]
    set_clauses.append("ingested_at = NOW()")
    set_clause_sql = ", ".join(set_clauses)

    upsert_sql = (
        f"INSERT INTO {safe_table_name} ({quoted_columns}) VALUES %s "
        f"ON CONFLICT ({quoted_conflict_columns}) DO UPDATE SET {set_clause_sql}"
    )

    max_retries = max(0, _get_int_env("TIMESCALE_MAX_RETRIES", 3))
    retry_delay_seconds = max(1, _get_int_env("TIMESCALE_RETRY_DELAY_SECONDS", 5))
    page_size = max(1, _get_int_env("TIMESCALE_UPSERT_PAGE_SIZE", 500))

    for attempt in range(max_retries + 1):
        connection = None
        try:
            connection = psycopg2.connect(
                host=connection_settings["host"],
                port=connection_settings["port"],
                dbname=connection_settings["database"],
                user=connection_settings["user"],
                password=connection_settings["password"],
                connect_timeout=_get_int_env("TIMESCALE_CONNECT_TIMEOUT", 10),
                sslmode=_get_str_env("TIMESCALE_SSLMODE", "disable"),
            )
            with connection:
                with connection.cursor() as cursor:
                    execute_values(cursor, upsert_sql, rows, page_size=page_size)

            LOGGER.info("Upserted micro-batch into TimescaleDB table '%s'.", table_name)
            return
        except Exception as exc:
            error_text = str(exc).lower()
            if "no unique or exclusion constraint matching the on conflict" in error_text:
                LOGGER.error(
                    "Upsert requires natural-key unique indexes. "
                    "Apply docker/migrations/001_timescale_hardening.sql before running streams."
                )
                if _fail_fast():
                    raise
                return

            if attempt < max_retries:
                LOGGER.warning(
                    "Upsert to '%s' failed (attempt %s/%s): %s. Retrying in %ss.",
                    table_name,
                    attempt + 1,
                    max_retries + 1,
                    exc,
                    retry_delay_seconds,
                )
                time.sleep(retry_delay_seconds)
                continue

            LOGGER.exception("Failed upserting micro-batch to '%s' after retries: %s", table_name, exc)
            if _fail_fast():
                raise
            return
        finally:
            if connection is not None:
                connection.close()


def foreach_batch_air_quality(batch_df: DataFrame, batch_id: int) -> None:
    """foreachBatch callback for air quality streaming aggregates."""
    table_name = os.environ.get("TIMESCALE_AIR_QUALITY_TABLE", "air_quality_metrics")

    try:
        prepared_df = (
            batch_df.select(
                col("window_start").cast("timestamp").alias("window_start"),
                col("window_end").cast("timestamp").alias("window_end"),
                col("city").cast("string").alias("city"),
                col("parameter").cast("string").alias("parameter"),
                col("rolling_avg").cast("double").alias("rolling_avg"),
                col("status").cast("string").alias("status"),
                col("window_duration").cast("string").alias("window_duration"),
            )
            .filter(col("window_start").isNotNull())
            .filter(col("window_end").isNotNull())
            .filter(col("city").isNotNull())
            .filter(col("parameter").isNotNull())
            .filter(col("rolling_avg").isNotNull())
            .filter(col("status").isNotNull())
            .filter(col("window_duration").isNotNull())
            .dropDuplicates(["window_start", "window_end", "city", "parameter", "window_duration"])
        )

        if _use_upsert():
            upsert_to_timescaledb(
                prepared_df,
                table_name,
                columns=[
                    "window_start",
                    "window_end",
                    "city",
                    "parameter",
                    "rolling_avg",
                    "status",
                    "window_duration",
                ],
                conflict_columns=[
                    "window_start",
                    "window_end",
                    "city",
                    "parameter",
                    "window_duration",
                ],
            )
        else:
            write_to_timescaledb(prepared_df, table_name)
    except Exception as exc:
        LOGGER.exception(
            "Air quality batch %s failed before write: %s",
            batch_id,
            exc,
        )
        if _fail_fast():
            raise


def foreach_batch_weather(batch_df: DataFrame, batch_id: int) -> None:
    """foreachBatch callback for weather streaming aggregates."""
    table_name = os.environ.get("TIMESCALE_WEATHER_TABLE", "weather_metrics")

    try:
        prepared_df = (
            batch_df.select(
                col("window_start").cast("timestamp").alias("window_start"),
                col("window_end").cast("timestamp").alias("window_end"),
                col("city").cast("string").alias("city"),
                col("avg_temperature").cast("double").alias("avg_temperature"),
                col("avg_humidity").cast("double").alias("avg_humidity"),
                col("avg_wind_speed").cast("double").alias("avg_wind_speed"),
                col("window_duration").cast("string").alias("window_duration"),
            )
            .filter(col("window_start").isNotNull())
            .filter(col("window_end").isNotNull())
            .filter(col("city").isNotNull())
            .filter(col("avg_temperature").isNotNull())
            .filter(col("avg_humidity").isNotNull())
            .filter(col("avg_wind_speed").isNotNull())
            .filter(col("window_duration").isNotNull())
            .dropDuplicates(["window_start", "window_end", "city", "window_duration"])
        )

        if _use_upsert():
            upsert_to_timescaledb(
                prepared_df,
                table_name,
                columns=[
                    "window_start",
                    "window_end",
                    "city",
                    "avg_temperature",
                    "avg_humidity",
                    "avg_wind_speed",
                    "window_duration",
                ],
                conflict_columns=["window_start", "window_end", "city", "window_duration"],
            )
        else:
            write_to_timescaledb(prepared_df, table_name)
    except Exception as exc:
        LOGGER.exception(
            "Weather batch %s failed before write: %s",
            batch_id,
            exc,
        )
        if _fail_fast():
            raise
