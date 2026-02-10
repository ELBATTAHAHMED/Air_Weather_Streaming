import json
import os
import time
from datetime import datetime, timedelta, timezone
import hashlib
import random

import requests
from kafka import KafkaProducer


BASE_URL = "https://api.open-meteo.com/v1/forecast"

REALTIME_WINDOW_MINUTES = 45
LATE_EVENT_WINDOW_HOURS = 6
ALLOW_LATE_EVENTS = True

SCHEMA_VERSION = "1.0"
PIPELINE_NAME = "weather_ingestion"
PIPELINE_VERSION = datetime.now(timezone.utc).strftime("%Y-%m-%d")

DEDUP_TTL_HOURS = 24
DEDUP_MAX_SIZE = 5000

RATE_LIMIT_SLEEP_SECONDS = 10

TEMPERATURE_UNIT = "C"
WIND_SPEED_UNIT = "m/s"
HUMIDITY_UNIT = "%"

CITIES = [
    {"country": "DE", "city": "Berlin", "latitude": 52.52, "longitude": 13.405},
    {"country": "GB", "city": "London", "latitude": 51.5074, "longitude": -0.1278},
    {"country": "FR", "city": "Paris", "latitude": 48.8566, "longitude": 2.3522},
]


def request_json_retry(
    url: str,
    params: dict,
    retry_counter: dict | None = None,
    max_attempts: int = 5,
    base_delay: float = 1.0,
    max_delay: float = 30.0,
) -> tuple[dict | None, int | None]:
    attempt = 0
    while attempt < max_attempts:
        try:
            response = requests.get(url, params=params, timeout=30)
        except requests.RequestException as exc:
            if attempt + 1 >= max_attempts:
                print(f"Error: request failed after retries: {exc}")
                return None, None
            if retry_counter is not None:
                retry_counter["count"] += 1
            delay = min(max_delay, base_delay * (2 ** attempt)) + random.uniform(0, 0.5)
            time.sleep(delay)
            attempt += 1
            continue

        status = response.status_code
        if status in (429, 500, 502, 503, 504):
            if attempt + 1 >= max_attempts:
                print(f"Error: HTTP {status} after retries")
                return None, status
            if retry_counter is not None:
                retry_counter["count"] += 1
            if status == 429:
                retry_after = response.headers.get("Retry-After")
                try:
                    delay = float(retry_after) if retry_after else RATE_LIMIT_SLEEP_SECONDS
                except ValueError:
                    delay = RATE_LIMIT_SLEEP_SECONDS
                delay = min(max_delay, delay) + random.uniform(0, 0.5)
                print(f"Error: HTTP 429 (rate limited), retrying in {delay:.1f}s")
            else:
                delay = base_delay * (2 ** attempt)
                delay = min(max_delay, delay) + random.uniform(0, 0.5)
                print(f"Error: HTTP {status}, retrying in {delay:.1f}s")
            time.sleep(delay)
            attempt += 1
            continue

        if not response.ok:
            print(f"Error: HTTP {status}")
            return None, status

        return response.json(), status

    return None, None


def parse_iso_utc(timestamp: str | None) -> datetime | None:
    if not timestamp or not isinstance(timestamp, str):
        return None
    normalized = timestamp.replace("Z", "+00:00")
    try:
        parsed = datetime.fromisoformat(normalized)
    except ValueError:
        return None
    if parsed.tzinfo is None:
        return parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def convert_temperature(value: float | None, unit: str | None) -> float | None:
    if value is None:
        return None
    if unit in ("C", "°C", "celsius") or unit is None:
        return float(value)
    if unit in ("F", "°F", "fahrenheit"):
        return (float(value) - 32) * 5 / 9
    return float(value)


def convert_wind_speed(value: float | None, unit: str | None) -> float | None:
    if value is None:
        return None
    if unit in ("m/s", "ms") or unit is None:
        return float(value)
    if unit in ("km/h", "kmh"):
        return float(value) / 3.6
    if unit in ("mph",):
        return float(value) * 0.44704
    if unit in ("kn", "kt", "knot", "knots"):
        return float(value) * 0.514444
    return float(value)


def is_recent(event_time: str | None, max_age_minutes: int) -> bool:
    parsed = parse_iso_utc(event_time)
    if parsed is None:
        return False
    now = datetime.now(timezone.utc)
    return now - parsed <= timedelta(minutes=max_age_minutes)


def prune_dedup_cache(cache: dict, now: datetime) -> None:
    cutoff = now - timedelta(hours=DEDUP_TTL_HOURS)
    expired = [key for key, entry in cache.items() if entry.get("updated_at") and entry["updated_at"] < cutoff]
    for key in expired:
        cache.pop(key, None)
    if len(cache) <= DEDUP_MAX_SIZE:
        return
    overflow = len(cache) - DEDUP_MAX_SIZE
    oldest = sorted(cache.items(), key=lambda item: item[1].get("updated_at", now))[:overflow]
    for key, _ in oldest:
        cache.pop(key, None)


def build_weather_message(
    data: dict,
    country: str | None,
    city: str,
    latitude: float,
    longitude: float,
) -> dict:
    current = data.get("current") or {}
    units = data.get("current_units") or {}
    temperature = current.get("temperature_2m")
    wind_speed = current.get("wind_speed_10m")
    humidity = current.get("relative_humidity_2m")
    event_time_raw = current.get("time")
    event_dt = parse_iso_utc(event_time_raw)
    event_time = event_dt.isoformat() if event_dt else None
    temperature_unit_raw = units.get("temperature_2m")
    wind_unit_raw = units.get("wind_speed_10m")
    humidity_unit_raw = units.get("relative_humidity_2m")
    temperature = convert_temperature(temperature, temperature_unit_raw)
    wind_speed = convert_wind_speed(wind_speed, wind_unit_raw)
    humidity_unit = HUMIDITY_UNIT if humidity_unit_raw else HUMIDITY_UNIT
    message_id_input = "|".join(
        [
            "open-meteo",
            city,
            str(event_time or ""),
            str(temperature if temperature is not None else ""),
            str(wind_speed if wind_speed is not None else ""),
            str(humidity if humidity is not None else ""),
            TEMPERATURE_UNIT,
            WIND_SPEED_UNIT,
            humidity_unit,
        ]
    )
    message_id = hashlib.sha1(message_id_input.encode("utf-8")).hexdigest()
    return {
        "source": "open-meteo",
        "schema_version": SCHEMA_VERSION,
        "pipeline": PIPELINE_NAME,
        "pipeline_version": PIPELINE_VERSION,
        "country": country,
        "city": city,
        "latitude": float(latitude),
        "longitude": float(longitude),
        "temperature": float(temperature) if temperature is not None else None,
        "temperature_unit": TEMPERATURE_UNIT,
        "wind_speed": float(wind_speed) if wind_speed is not None else None,
        "wind_unit": WIND_SPEED_UNIT,
        "humidity": int(humidity) if humidity is not None else None,
        "humidity_unit": humidity_unit,
        "event_time": event_time,
        "ingestion_time": datetime.now(timezone.utc).isoformat(),
        "message_id": message_id,
    }


def send_kafka(producer: KafkaProducer, topic: str, message: dict, counters: dict) -> None:
    try:
        key_str = "|".join([
            message.get("city") or "NA",
            message.get("source") or "unknown",
        ])
        future = producer.send(topic, key=key_str, value=message)
        future.add_errback(lambda exc: counters.__setitem__("send_errors", counters["send_errors"] + 1))
    except Exception as exc:
        counters["send_errors"] += 1
        print(f"[Kafka][ERROR] {exc}")


def main() -> None:
    bootstrap_servers = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
    kafka_servers = [server.strip() for server in bootstrap_servers.split(",") if server.strip()]

    producer = KafkaProducer(
        bootstrap_servers=kafka_servers,
        key_serializer=lambda value: (value or "").encode("utf-8"),
        value_serializer=lambda value: json.dumps(value).encode("utf-8"),
        linger_ms=100,
        batch_size=65536,
        acks="all",
    )
    dedup_cache: dict[tuple[str, str], dict[str, object]] = {}
    try:
        while True:
            for target in CITIES:
                now = datetime.now(timezone.utc)
                prune_dedup_cache(dedup_cache, now)
                counters = {
                    "produced": 0,
                    "skipped_dup": 0,
                    "skipped_old": 0,
                    "missing_time": 0,
                    "send_errors": 0,
                }
                retry_counter = {"count": 0}

                params = {
                    "latitude": target["latitude"],
                    "longitude": target["longitude"],
                    "current": "temperature_2m,wind_speed_10m,relative_humidity_2m",
                    "timezone": "UTC",
                    "temperature_unit": "celsius",
                    "windspeed_unit": "ms",
                }
                data, _ = request_json_retry(BASE_URL, params, retry_counter=retry_counter)
                if data is None:
                    print(
                        f"[Open-Meteo][{target['city']}] "
                        f"produced={counters['produced']} "
                        f"skipped_dup={counters['skipped_dup']} "
                        f"skipped_old={counters['skipped_old']} "
                        f"missing_time={counters['missing_time']} "
                        f"http_retries={retry_counter['count']} "
                        f"send_errors={counters['send_errors']}"
                    )
                    time.sleep(30)
                    continue

                message = build_weather_message(
                    data,
                    target.get("country"),
                    target["city"],
                    target["latitude"],
                    target["longitude"],
                )

                event_time = message.get("event_time")
                if event_time is None:
                    counters["missing_time"] += 1
                else:
                    if ALLOW_LATE_EVENTS:
                        max_age_minutes = LATE_EVENT_WINDOW_HOURS * 60
                    else:
                        max_age_minutes = REALTIME_WINDOW_MINUTES
                    if not is_recent(event_time, max_age_minutes=max_age_minutes):
                        counters["skipped_old"] += 1
                    else:
                        dedup_key = (message.get("source") or "unknown", message.get("city") or "NA")
                        dedup_entry = dedup_cache.get(dedup_key)
                        if dedup_entry:
                            if dedup_entry.get("message_id") == message.get("message_id"):
                                counters["skipped_dup"] += 1
                            elif (
                                dedup_entry.get("event_time") == message.get("event_time")
                                and dedup_entry.get("temperature") == message.get("temperature")
                                and dedup_entry.get("wind_speed") == message.get("wind_speed")
                                and dedup_entry.get("humidity") == message.get("humidity")
                            ):
                                counters["skipped_dup"] += 1
                            else:
                                send_kafka(producer, "weather_raw", message, counters)
                                counters["produced"] += 1
                                dedup_cache[dedup_key] = {
                                    "message_id": message.get("message_id"),
                                    "event_time": message.get("event_time"),
                                    "temperature": message.get("temperature"),
                                    "wind_speed": message.get("wind_speed"),
                                    "humidity": message.get("humidity"),
                                    "updated_at": now,
                                }
                        else:
                            send_kafka(producer, "weather_raw", message, counters)
                            counters["produced"] += 1
                            dedup_cache[dedup_key] = {
                                "message_id": message.get("message_id"),
                                "event_time": message.get("event_time"),
                                "temperature": message.get("temperature"),
                                "wind_speed": message.get("wind_speed"),
                                "humidity": message.get("humidity"),
                                "updated_at": now,
                            }

                producer.flush()
                print(
                    f"[Open-Meteo][{target['city']}] "
                    f"produced={counters['produced']} "
                    f"skipped_dup={counters['skipped_dup']} "
                    f"skipped_old={counters['skipped_old']} "
                    f"missing_time={counters['missing_time']} "
                    f"http_retries={retry_counter['count']} "
                    f"send_errors={counters['send_errors']}"
                )

            time.sleep(30)
    except KeyboardInterrupt:
        pass
    finally:
        producer.flush()
        producer.close()


if __name__ == "__main__":
    main()
