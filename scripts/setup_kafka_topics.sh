#!/bin/bash
# setup_kafka_topics.sh
# Creates Kafka topics needed for DES project

set -e

echo "Waiting for Kafka broker to start..."
sleep 10

echo "Creating Kafka topics..."

# docker exec -i des_kafka kafka-topics \
#   --create --topic SPORTS-ENTITY-TOPIC \
#   --bootstrap-server des_kafka:9092 \
#   --partitions 3 --replication-factor 1 || echo "SPORTS-ENTITY-TOPIC already exists"

docker exec -i des_kafka kafka-topics \
  --create --topic ENRICHED-ENTITY-TOPIC \
  --bootstrap-server des_kafka:9092 \
  --partitions 3 --replication-factor 1 || echo "ENRICHED-ENTITY-TOPIC already exists"

docker exec -i des_kafka kafka-topics \
  --create --topic ENGLISH-ENTITY-TOPIC \
  --bootstrap-server des_kafka:9092 \
  --partitions 3 --replication-factor 1 || echo "ENGLISH-ENTITY-TOPIC already exists"

echo "Listing topics..."
docker exec -i des_kafka kafka-topics --list --bootstrap-server des_kafka:9092

echo "Kafka topics setup complete."
