#!/bin/bash
# purge_kafka_topics.sh
# Deletes and recreates Kafka topics for the DES project

set -e

echo "Purging Kafka topics..."
echo "Waiting for Kafka broker to start..."
sleep 5

TOPICS=("RAW-ENTITY-TOPIC" "ENRICHED-ENTITY-TOPIC" "ENGLISH-ENTITY-TOPIC")

for topic in "${TOPICS[@]}"; do
  echo "Deleting topic: $topic"
  docker exec -i des_kafka kafka-topics.sh \
    --delete --topic "$topic" \
    --bootstrap-server des_kafka:9092 || echo "Failed to delete $topic (might not exist)"
done

echo "Recreating topics..."
for topic in "${TOPICS[@]}"; do
  echo "Creating topic: $topic"
  docker exec -i des_kafka kafka-topics.sh \
    --create --topic "$topic" \
    --bootstrap-server des_kafka:9092 \
    --partitions 3 --replication-factor 1 || echo "$topic already exists"
done

echo "Listing all topics..."
docker exec -i des_kafka kafka-topics.sh --list --bootstrap-server des_kafka:9092

echo "Purge and recreate complete."
