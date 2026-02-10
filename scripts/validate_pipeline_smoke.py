from __future__ import annotations

import os
import sys
from pathlib import Path

import psycopg2


def fail(message: str) -> None:
    print(f"[FAIL] {message}")
    raise SystemExit(1)


def fetch_one_int(cursor, sql: str) -> int:
    cursor.execute(sql)
    row = cursor.fetchone()
    if not row or row[0] is None:
        return 0
    return int(row[0])


def ensure_parquet_data(root: Path) -> None:
    required_with_data = [
        "bronze/air_quality",
        "bronze/weather",
        "silver/air_quality",
        "silver/weather",
        "gold/air_quality_aggregates",
        "gold/weather_aggregates",
    ]

    optional_dirs = [
        "rejects/air_quality",
        "rejects/weather",
    ]

    for relative in required_with_data:
        target = root / relative
        if not target.exists():
            fail(f"Missing lakehouse directory: {target}")

        if not any(target.rglob("*.parquet")):
            fail(f"No parquet files found in: {target}")

        print(f"[OK] parquet data present: {target}")

    for relative in optional_dirs:
        target = root / relative
        if not target.exists():
            fail(f"Missing lakehouse directory: {target}")

        if any(target.rglob("*.parquet")):
            print(f"[OK] optional rejects data present: {target}")
        else:
            print(f"[OK] optional rejects path exists (no rows yet): {target}")


def ensure_timescale_data() -> None:
    conn = psycopg2.connect(
        host=os.getenv("TIMESCALE_HOST", "localhost"),
        port=os.getenv("TIMESCALE_PORT", "5432"),
        dbname=os.getenv("TIMESCALE_DB", "air_quality"),
        user=os.getenv("TIMESCALE_USER", "air_quality"),
        password=os.getenv("TIMESCALE_PASSWORD", "air_quality"),
        connect_timeout=10,
    )

    try:
        with conn, conn.cursor() as cursor:
            air_quality_count = fetch_one_int(cursor, "SELECT COUNT(*) FROM air_quality_metrics")
            if air_quality_count <= 0:
                fail("air_quality_metrics is empty")
            print(f"[OK] air_quality_metrics rows={air_quality_count}")

            weather_count = fetch_one_int(cursor, "SELECT COUNT(*) FROM weather_metrics")
            if weather_count <= 0:
                fail("weather_metrics is empty")
            print(f"[OK] weather_metrics rows={weather_count}")

            air_quality_duplicates = fetch_one_int(
                cursor,
                """
                SELECT COUNT(*)
                FROM (
                    SELECT window_start, window_end, city, parameter, window_duration, COUNT(*) AS c
                    FROM air_quality_metrics
                    GROUP BY 1, 2, 3, 4, 5
                    HAVING COUNT(*) > 1
                ) duplicates
                """,
            )
            if air_quality_duplicates > 0:
                fail(f"air_quality_metrics has duplicate natural keys: {air_quality_duplicates}")
            print("[OK] air_quality_metrics natural keys are unique")

            weather_duplicates = fetch_one_int(
                cursor,
                """
                SELECT COUNT(*)
                FROM (
                    SELECT window_start, window_end, city, window_duration, COUNT(*) AS c
                    FROM weather_metrics
                    GROUP BY 1, 2, 3, 4
                    HAVING COUNT(*) > 1
                ) duplicates
                """,
            )
            if weather_duplicates > 0:
                fail(f"weather_metrics has duplicate natural keys: {weather_duplicates}")
            print("[OK] weather_metrics natural keys are unique")
    finally:
        conn.close()


def main() -> None:
    lakehouse_root = Path(os.getenv("LAKEHOUSE_ROOT", "lakehouse")).resolve()
    if not lakehouse_root.exists():
        fail(f"Lakehouse root does not exist: {lakehouse_root}")

    ensure_parquet_data(lakehouse_root)
    ensure_timescale_data()
    print("[OK] pipeline smoke validation passed")


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        sys.exit(130)
