#!/usr/bin/env bash
set -euo pipefail

docker exec kafka kafka-topics --bootstrap-server localhost:9092 --create --if-not-exists --topic air_quality_raw --partitions 1 --replication-factor 1
docker exec kafka kafka-topics --bootstrap-server localhost:9092 --create --if-not-exists --topic weather_raw --partitions 1 --replication-factor 1

docker exec kafka kafka-topics --bootstrap-server localhost:9092 --list
