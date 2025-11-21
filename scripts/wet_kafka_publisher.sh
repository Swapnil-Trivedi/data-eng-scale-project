#!/bin/bash

# Usage: ./submit_wet_to_kafka.sh <start_index> <num_files>
if [ $# -lt 2 ]; then
  echo "Usage: $0 <start_index> <num_files>"
  exit 1
fi

START_INDEX="$1"
NUM_FILES="$2"
SCRIPT_PATH="../docker/kafka/wet_to_kafka.py"   # assumes script lives next to this launcher

echo "============================================================"
echo " Running Local WET → Kafka Ingest Job"
echo "------------------------------------------------------------"
echo " Start index : $START_INDEX"
echo " Num files   : $NUM_FILES"
echo " Script      : $SCRIPT_PATH"
echo " Working Dir : $(pwd)"
echo " Timestamp   : $(date '+%Y-%m-%d %H:%M:%S')"
echo "============================================================"

# ------------------------------------------------------------
# Check python exists
# ------------------------------------------------------------
if ! command -v python3 &> /dev/null; then
  echo "ERROR: python3 not found in PATH"
  exit 2
fi

# ------------------------------------------------------------
# Dependencies check (optional)
# ------------------------------------------------------------
missing_deps=()
python3 - <<EOF
import pkgutil
missing = []
for m in ["prometheus_client", "confluent_kafka"]:
    if pkgutil.find_loader(m) is None:
        missing.append(m)
if missing:
    print(" ".join(missing))
EOF

if [ $? -ne 0 ]; then
  echo "WARNING: Some dependencies appear missing."
  echo "Install with:"
  echo "  pip3 install prometheus_client confluent-kafka"
fi

# ------------------------------------------------------------
# Run the ingestion script
# ------------------------------------------------------------
python3 "$SCRIPT_PATH" "$START_INDEX" "$NUM_FILES"
EXIT_CODE=$?

echo "============================================================"
if [ $EXIT_CODE -eq 0 ]; then
  echo " Python Kafka ingest job finished successfully."
else
  echo " Python Kafka ingest job FAILED with exit code: $EXIT_CODE"
fi
echo "============================================================"

exit $EXIT_CODE
