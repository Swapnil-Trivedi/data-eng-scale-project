#!/bin/bash

# Usage: ./submit_wet_to_kafka.sh <start_index> <num_files>
if [ $# -lt 2 ]; then
  echo "Usage: $0 <start_index> <num_files>"
  exit 1
fi

START_INDEX="$1"
NUM_FILES="$2"

echo "============================================================"
echo " Submitting Spark job in Single-Node Local Mode"
echo "------------------------------------------------------------"
echo " Start index : $START_INDEX"
echo " Num files   : $NUM_FILES"
echo " Container   : des_spark-master"
echo " Timestamp   : $(date '+%Y-%m-%d %H:%M:%S')"
echo "============================================================"

# -----------------------------
# Ensure Python and Prometheus client are installed in the container
# -----------------------------
# docker exec -u root des_spark-master bash -c "
#   apt-get update && \
#   apt-get install -y python3 python3-pip && \
#   pip3 install --no-cache-dir prometheus_client
# "

# -----------------------------
# Run Spark job in local mode
# -----------------------------
docker exec -it des_spark-master /opt/spark/bin/spark-submit \
  --master local[*] \
  --deploy-mode client \
  --name "wet_to_kafka_${START_INDEX}_${NUM_FILES}" \
  --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0 \
  --driver-memory 2G \
  --executor-memory 2G \
  --executor-cores 2 \
  --conf spark.executor.memoryOverhead=512M \
  /opt/spark-apps/wet_to_kafka.py "$START_INDEX" "$NUM_FILES"

echo "============================================================"
echo " Spark job finished."
echo "============================================================"