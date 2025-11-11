#!/bin/bash

# Usage: ./submit_wet_to_kafka.sh <start_index> <num_files>
if [ $# -lt 2 ]; then
  echo "Usage: $0 <start_index> <num_files>"
  exit 1
fi

START_INDEX="$1"
NUM_FILES="$2"

echo "============================================================"
echo " Submitting Spark job: WET → Kafka"
echo "------------------------------------------------------------"
echo " Start index       : $START_INDEX"
echo " Num files         : $NUM_FILES"
echo " Target container  : des_spark-master"
echo " Timestamp         : $(date '+%Y-%m-%d %H:%M:%S')"
echo "============================================================"

# Make sure temporary Ivy cache exists
TMP_IVY_CACHE="/tmp/.ivy2"
mkdir -p "$TMP_IVY_CACHE"

# Set memory and cores
EXECUTOR_MEMORY="2G"
DRIVER_MEMORY="2G"
EXECUTOR_CORES="2"
MEMORY_OVERHEAD="512M"

# Run the Spark job inside the Docker container
docker exec -it des_spark-master /opt/spark/bin/spark-submit \
  --master spark://spark-master:7077 \
  --deploy-mode client \
  --name "wet_to_kafka_${START_INDEX}_${NUM_FILES}" \
  --conf spark.jars.ivy="$TMP_IVY_CACHE" \
  --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0 \
  --executor-memory $EXECUTOR_MEMORY \
  --driver-memory $DRIVER_MEMORY \
  --executor-cores $EXECUTOR_CORES \
  --conf spark.executor.memoryOverhead=$MEMORY_OVERHEAD \
  /opt/spark-apps/wet_to_kafka.py "$START_INDEX" "$NUM_FILES"

echo "============================================================"
echo " Job submitted!"
echo "============================================================"
