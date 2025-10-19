#!/bin/bash
# init_des.sh
# Bootstraps the full DES environment: Docker + Kafka topic setup

set -e

echo "=============================="
echo " Starting DES Environment..."
echo "=============================="

# Step 1: Bring up all containers
docker-compose -f ./docker/docker-compose.yaml up -d

echo ""
echo "Waiting for services to initialize..."
sleep 15

# Step 2: Verify containers
echo ""
echo "Active containers:"
docker ps --filter "name=des_" --format "table {{.Names}}\t{{.Status}}\t{{.Ports}}"

# Step 3: Wait for Kafka to be ready
echo ""
echo "Checking Kafka readiness..."
MAX_RETRIES=20
for i in $(seq 1 $MAX_RETRIES); do
  if docker exec des_kafka kafka-topics --bootstrap-server localhost:9092 --list >/dev/null 2>&1; then
    echo "Kafka is ready!"
    break
  else
    echo "Kafka not ready yet... retry $i/$MAX_RETRIES"
    sleep 10
  fi
done

if [ $i -eq $MAX_RETRIES ]; then
  echo "Kafka did not become ready in time. Bringing down containers and exiting."
  docker-compose -f ./docker/docker-compose.yaml down
  exit 1
fi

# Step 4: Setup topics (now under scripts/)
echo ""
echo "Initializing Kafka topics..."
bash ./scripts/setup_kafka_topics.sh

# Step 5: Final check
echo ""
echo "Environment setup complete."
echo "Kafka UI available at: http://localhost:8085"
echo "Neo4j Browser available at: http://localhost:7476"
echo "Spark Master UI available at: http://localhost:8080"
