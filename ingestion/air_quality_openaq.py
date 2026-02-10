import json
import os
import time
from datetime import datetime, timedelta, timezone
import hashlib
import random
import re
import unicodedata

import requests
from kafka import KafkaProducer


"""
OpenAQ ingestion notes:
- Data availability is heterogeneous across cities.
- Sensors may publish every 30-90 minutes.
- The pipeline enforces a real-time window while tolerating late events.
- Absence of data reflects real-world conditions, not system failure.
"""

BASE_URL = "https://api.openaq.org/v3"

REALTIME_WINDOW_MINUTES = 45
LATE_EVENT_WINDOW_HOURS = 6
ALLOW_LATE_EVENTS = True

SCHEMA_VERSION = "1.0"
PIPELINE_NAME = "openaq_ingestion"
PIPELINE_VERSION = datetime.now(timezone.utc).strftime("%Y-%m-%d")

DEDUP_TTL_HOURS = 24
DEDUP_MAX_SIZE = 5000

MAX_LOCATION_PAGES = 3
RATE_LIMIT_SLEEP_SECONDS = 10
CITY_QUERY_DELAY_SECONDS = 2

DEBUG_OPENAQ = os.getenv("OPENAQ_DEBUG") == "1"

TARGET_CITIES = [
    {"country": "DE", "city": "Berlin"},
    {"country": "GB", "city": "London"},
    {"country": "FR", "city": "Paris"},
]


def request_json(url: str, params: dict, api_key: str) -> dict | None:
    data, _ = request_json_retry(url, params, api_key)
    return data


def request_json_with_status(url: str, params: dict, api_key: str) -> tuple[dict | None, int | None]:
    return request_json_retry(url, params, api_key)


def request_json_retry(
    url: str,
    params: dict,
    api_key: str,
    retry_counter: dict | None = None,
    max_attempts: int = 5,
    base_delay: float = 1.0,
    max_delay: float = 30.0,
) -> tuple[dict | None, int | None]:
    headers = {"X-API-Key": api_key}
    attempt = 0
    while attempt < max_attempts:
        try:
            response = requests.get(url, params=params, headers=headers, timeout=30)
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


def normalize_text(text: str) -> str:
    normalized = unicodedata.normalize("NFKD", text)
    normalized = "".join(char for char in normalized if not unicodedata.combining(char))
    cleaned = re.sub(r"[^A-Za-z0-9]+", " ", normalized).strip().lower()
    return " ".join(cleaned.split())


def matches_city(location: dict, city: str) -> bool:
    candidate = location.get("locality") or location.get("city") or location.get("name")
    if not candidate or not isinstance(candidate, str):
        return False
    aliases = {
        "Paris": ["paris", "paris city", "arrondissement"],
    }.get(city, [city])
    candidate_norm = normalize_text(candidate)
    candidate_compact = candidate_norm.replace(" ", "")
    candidate_tokens = set(candidate_norm.split())
    if city == "Paris" and "paris" not in candidate_tokens:
        return False
    for alias in aliases:
        alias_norm = normalize_text(alias)
        if not alias_norm:
            continue
        alias_compact = alias_norm.replace(" ", "")
        if city == "Paris" or len(alias_compact) <= 5:
            if " " in alias_norm:
                if alias_norm in candidate_norm:
                    return True
            elif alias_norm in candidate_tokens:
                return True
            continue
        if " " in alias_norm:
            if alias_norm in candidate_norm or alias_compact in candidate_compact:
                return True
            continue
        if alias_norm in candidate_norm:
            if re.search(rf"\b{re.escape(alias_norm)}\b", candidate_norm):
                return True
    return False


def _city_match_self_test() -> None:
    cases = [
        ("Paris 2e Arrondissement", "Paris", True),
        ("Parish", "Paris", False),
        ("Londonderry", "London", False),
    ]
    for candidate, city, expected in cases:
        result = matches_city({"locality": candidate}, city)
        print(f"[CityMatch] candidate='{candidate}' city='{city}' match={result} expected={expected}")


def select_locations(locations: list[dict], city: str) -> list[dict]:
    matches = []
    for location in locations:
        if not matches_city(location, city):
            continue
        sensors = location.get("sensors") or []
        if not sensors:
            continue
        matches.append(location)
    return matches


def extract_timestamp(measurement: dict) -> str | None:
    datetime_info = measurement.get("datetime")
    if isinstance(datetime_info, dict):
        return datetime_info.get("utc") or datetime_info.get("local")
    if isinstance(datetime_info, str):
        return datetime_info
    datetime_last = measurement.get("datetimeLast")
    if isinstance(datetime_last, dict) and datetime_last.get("utc"):
        return datetime_last.get("utc")
    period = measurement.get("period")
    if isinstance(period, dict):
        datetime_to = period.get("datetimeTo")
        datetime_from = period.get("datetimeFrom")
        if isinstance(datetime_to, dict) and datetime_to.get("utc"):
            return datetime_to.get("utc")
        if isinstance(datetime_from, dict) and datetime_from.get("utc"):
            return datetime_from.get("utc")
    date_info = measurement.get("date")
    if isinstance(date_info, dict):
        return date_info.get("utc") or date_info.get("local")
    return measurement.get("date_utc") or measurement.get("lastUpdated") or date_info


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


def is_recent(event_time: str | None, max_age_minutes: int = REALTIME_WINDOW_MINUTES) -> bool:
    parsed = parse_iso_utc(event_time)
    if parsed is None:
        return False
    now = datetime.now(timezone.utc)
    age = now - parsed
    return age <= timedelta(minutes=max_age_minutes)


def select_best_location(locations: list[dict], city: str) -> list[dict]:
    candidates = []
    for location in select_locations(locations, city):
        datetime_last = location.get("datetimeLast")
        datetime_last = datetime_last if isinstance(datetime_last, dict) else {}
        timestamp = datetime_last.get("utc")
        parsed = parse_iso_utc(timestamp) if timestamp else None
        candidates.append((parsed, location))
    candidates.sort(key=lambda item: item[0] or datetime.min.replace(tzinfo=timezone.utc), reverse=True)
    return [location for _, location in candidates]


def fetch_location_latest(
    api_key: str,
    location_id: int | str,
    retry_counter: dict | None = None,
) -> tuple[dict | None, int | None]:
    latest_url = f"{BASE_URL}/locations/{location_id}/latest"
    return request_json_retry(latest_url, {}, api_key, retry_counter=retry_counter)


def fetch_city_latest(
    api_key: str,
    country_code: str,
    city: str,
    retry_counter: dict | None = None,
) -> dict | None:
    locations_url = f"{BASE_URL}/locations"

    for page in range(1, MAX_LOCATION_PAGES + 1):
        locations_params = {"iso": country_code, "limit": 200, "page": page}
        locations_data, _ = request_json_retry(
            locations_url,
            locations_params,
            api_key,
            retry_counter=retry_counter,
        )
        if locations_data is None:
            return None

        results = locations_data.get("results", [])
        if not results:
            break

        # OpenAQ data availability is heterogeneous; ingest only stations with live measurements to guarantee real-time semantics.
        for location in select_best_location(results, city):
            location_id = location.get("id")
            if not isinstance(location_id, (int, str)):
                continue
            latest_data, status = fetch_location_latest(
                api_key,
                location_id,
                retry_counter=retry_counter,
            )
            if latest_data is None:
                if status in (404, 422):
                    continue
                return None
            latest_results = latest_data.get("results")
            if not isinstance(latest_results, list):
                continue
            if latest_results:
                station_city = location.get("locality") or location.get("name")
                datetime_last = location.get("datetimeLast")
                datetime_last = datetime_last if isinstance(datetime_last, dict) else {}
                if DEBUG_OPENAQ:
                    print(
                        f"[OpenAQ][{city}] selected_location id={location_id} "
                        f"station='{station_city}' datetimeLast={datetime_last.get('utc')}"
                    )
                return {
                    "location": location,
                    "latest": latest_results,
                    "location_id": location_id,
                }

    return {"location": None, "latest": []}


def parameter_from_location(
    location: dict,
    sensors_id: int | None,
) -> tuple[str | None, str | None, int | None]:
    if sensors_id is None:
        return None, None, None
    sensors = location.get("sensors") or []
    for sensor in sensors:
        if sensor.get("id") != sensors_id:
            continue
        parameter = sensor.get("parameter")
        if isinstance(parameter, dict):
            return (
                parameter.get("name"),
                parameter.get("units"),
                parameter.get("id"),
            )
    return None, None, None


def prune_dedup_cache(cache: dict, now: datetime) -> None:
    # Cross-cycle dedup cache avoids re-sending unchanged latest values.
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


def build_air_quality_message(
    latest_item: dict,
    location: dict,
    country_code: str | None,
    location_id: int | str | None,
    target_city: str | None,
) -> dict:
    station_city = location.get("locality") or location.get("name")
    city = target_city or station_city
    parameter_info = latest_item.get("parameter")
    if isinstance(parameter_info, dict):
        parameter = parameter_info.get("name") or parameter_info.get("code")
        unit = parameter_info.get("units") or parameter_info.get("unit")
        parameter_id = parameter_info.get("id")
    else:
        parameter = latest_item.get("parameter")
        unit = latest_item.get("unit") or latest_item.get("units")
        parameter_id = latest_item.get("parameter_id")

    sensor_id = (
        latest_item.get("sensorsId")
        or latest_item.get("sensorId")
        or latest_item.get("sensor_id")
    )
    sensor_info = latest_item.get("sensor")
    if sensor_id is None and isinstance(sensor_info, dict):
        sensor_id = sensor_info.get("id") or sensor_info.get("sensorId")

    if parameter is None or unit is None or parameter_id is None:
        loc_param, loc_unit, loc_param_id = parameter_from_location(location, sensor_id)
        parameter = parameter or loc_param
        unit = unit or loc_unit
        parameter_id = parameter_id if parameter_id is not None else loc_param_id

    value = latest_item.get("value")
    try:
        value = float(value) if value is not None else None
    except (TypeError, ValueError):
        value = None

    location_coords = location.get("coordinates")
    location_coords = location_coords if isinstance(location_coords, dict) else {}
    coordinates = latest_item.get("coordinates") or location_coords
    coordinates = coordinates if isinstance(coordinates, dict) else {}
    latitude = coordinates.get("latitude")
    longitude = coordinates.get("longitude")
    try:
        latitude = float(latitude) if latitude is not None else None
    except (TypeError, ValueError):
        latitude = None
    try:
        longitude = float(longitude) if longitude is not None else None
    except (TypeError, ValueError):
        longitude = None

    # event_time = observation time provided by OpenAQ
    # ingestion_time = Kafka ingestion time (UTC)
    provider = location.get("provider") or location.get("owner") or location.get("sourceName")
    if isinstance(provider, dict):
        provider = provider.get("name")
    event_time = extract_timestamp(latest_item)
    message_id_input = "|".join(
        [
            str(location_id or ""),
            str(parameter or ""),
            str(event_time or ""),
            str(value if value is not None else ""),
            str(unit or ""),
        ]
    )
    message_id = hashlib.sha1(message_id_input.encode("utf-8")).hexdigest()

    return {
        "source": "openaq",
        "schema_version": SCHEMA_VERSION,
        "pipeline": PIPELINE_NAME,
        "pipeline_version": PIPELINE_VERSION,
        "country": country_code,
        "city": city,
        "station_city": station_city,
        "target_city": target_city,
        "parameter": parameter,
        "value": value,
        "unit": unit,
        "latitude": latitude,
        "longitude": longitude,
        "event_time": event_time,
        "ingestion_time": datetime.now(timezone.utc).isoformat(),
        "location_id": location_id,
        "sensor_id": sensor_id,
        "provider": provider,
        "parameter_id": parameter_id,
        "message_id": message_id,
    }


def send_kafka(producer: KafkaProducer, topic: str, message: dict, counters: dict) -> None:
    try:
        key_str = "|".join(
            [
                message.get("country") or "NA",
                message.get("city") or "NA",
                message.get("parameter") or "unknown",
            ]
        )
        future = producer.send(topic, key=key_str, value=message)
        future.add_errback(lambda exc: counters.__setitem__("send_errors", counters["send_errors"] + 1))
    except Exception as exc:
        counters["send_errors"] += 1
        print(f"[Kafka][ERROR] {exc}")


def main() -> None:
    api_key = os.getenv("OPENAQ_API_KEY")
    if not api_key:
        print("Error: OPENAQ_API_KEY environment variable is missing.")
        return

    bootstrap_servers = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
    kafka_servers = [server.strip() for server in bootstrap_servers.split(",") if server.strip()]

    print(
        "[OpenAQ] Starting real-time ingestion "
        f"(cities={', '.join(c['city'] for c in TARGET_CITIES)}, "
        f"window={REALTIME_WINDOW_MINUTES}min)"
    )

    producer = KafkaProducer(
        bootstrap_servers=kafka_servers,
        key_serializer=lambda value: (value or "").encode("utf-8"),
        value_serializer=lambda value: json.dumps(value).encode("utf-8"),
        # Batch settings improve throughput without sacrificing delivery guarantees.
        linger_ms=100,
        batch_size=65536,
        acks="all",
    )
    location_cache: dict[tuple[str, str], dict[str, object]] = {}
    dedup_cache: dict[tuple[str, str, str], dict[str, object]] = {}
    try:
        while True:
            for target in TARGET_CITIES:
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

                cache_key = (target["country"], target["city"])
                cached = location_cache.get(cache_key)
                result = None
                cache_invalidated = False

                if cached:
                    cached_location_id = cached.get("location_id")
                    cached_location = cached.get("location")
                    if isinstance(cached_location_id, (int, str)) and isinstance(cached_location, dict):
                        latest_data, status = fetch_location_latest(
                            api_key,
                            cached_location_id,
                            retry_counter=retry_counter,
                        )
                        if latest_data and isinstance(latest_data.get("results"), list):
                            latest_results = latest_data.get("results")
                            result = {
                                "location": cached_location,
                                "latest": latest_results,
                                "location_id": cached_location_id,
                            }
                        elif status in (404, 422):
                            location_cache.pop(cache_key, None)
                            cache_invalidated = True
                        elif latest_data is not None and not isinstance(latest_data.get("results"), list):
                            location_cache.pop(cache_key, None)
                            cache_invalidated = True
                        elif latest_data is None:
                            result = {
                                "location": cached_location,
                                "latest": [],
                                "location_id": cached_location_id,
                            }

                if result is None and (cached is None or cache_invalidated):
                    result = fetch_city_latest(
                        api_key,
                        target["country"],
                        target["city"],
                        retry_counter=retry_counter,
                    )

                location = result.get("location") if result else None
                latest_items = result.get("latest", []) if result else []

                if location and latest_items:
                    location_id = result.get("location_id")
                    if isinstance(location_id, (int, str)):
                        location_cache[cache_key] = {"location_id": location_id, "location": location}

                    seen = set()
                    for latest_item in latest_items:
                        event_time = extract_timestamp(latest_item)
                        if event_time is None:
                            counters["missing_time"] += 1
                            continue
                        if not ALLOW_LATE_EVENTS:
                            if not is_recent(event_time, max_age_minutes=REALTIME_WINDOW_MINUTES):
                                counters["skipped_old"] += 1
                                continue
                        else:
                            max_age_minutes = LATE_EVENT_WINDOW_HOURS * 60
                            if not is_recent(event_time, max_age_minutes=max_age_minutes):
                                counters["skipped_old"] += 1
                                continue
                        message = build_air_quality_message(
                            latest_item,
                            location,
                            target["country"],
                            location_id,
                            target["city"],
                        )
                        message_id = message.get("message_id")
                        if message_id in seen:
                            counters["skipped_dup"] += 1
                            continue
                        dedup_key = (
                            message.get("country") or "NA",
                            message.get("city") or "NA",
                            message.get("parameter") or "unknown",
                        )
                        dedup_entry = dedup_cache.get(dedup_key)
                        if dedup_entry:
                            if dedup_entry.get("message_id") == message_id:
                                counters["skipped_dup"] += 1
                                continue
                            if (
                                dedup_entry.get("event_time") == message.get("event_time")
                                and dedup_entry.get("value") == message.get("value")
                            ):
                                counters["skipped_dup"] += 1
                                continue
                        seen.add(message_id)
                        dedup_cache[dedup_key] = {
                            "message_id": message_id,
                            "event_time": message.get("event_time"),
                            "value": message.get("value"),
                            "updated_at": now,
                        }
                        send_kafka(producer, "air_quality_raw", message, counters)
                        counters["produced"] += 1

                    producer.flush()

                print(
                    f"[OpenAQ][{target['city']}] "
                    f"produced={counters['produced']} "
                    f"skipped_dup={counters['skipped_dup']} "
                    f"skipped_old={counters['skipped_old']} "
                    f"missing_time={counters['missing_time']} "
                    f"http_retries={retry_counter['count']} "
                    f"send_errors={counters['send_errors']}"
                )

                time.sleep(CITY_QUERY_DELAY_SECONDS)

            time.sleep(60)
    except KeyboardInterrupt:
        pass
    finally:
        producer.flush()
        producer.close()


if __name__ == "__main__":
    if os.getenv("CITY_MATCH_SELF_TEST") == "1":
        _city_match_self_test()
    main()
