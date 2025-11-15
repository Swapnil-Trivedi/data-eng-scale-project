from pyspark.sql import SparkSession
from pyspark.sql.functions import col, udf, explode
from pyspark.sql.types import StringType, ArrayType
import sys
import os
import time
import re
from prometheus_client import Counter, Gauge, start_http_server


# ============================================================
# Prometheus Metrics
# ============================================================
files_read_counter = Counter("files_read_total", "Total number of files read")
filtered_records_counter = Counter("filtered_records_total", "Total number of filtered English records")
kafka_records_counter = Counter("kafka_records_total", "Total number of records written to Kafka")
throughput_gauge = Gauge("throughput_records_per_second", "Records per second processed")

# IMPORTANT: Bind to 0.0.0.0 so Prometheus can see it
start_http_server(8000, addr="0.0.0.0")


# ============================================================
# Runtime arguments
# ============================================================
start_index = int(sys.argv[1]) if len(sys.argv) > 1 else 0
num_files = int(sys.argv[2]) if len(sys.argv) > 2 else 1

base_dir = "/opt/spark-data/data/289-index-data-extracted"
all_files = sorted([f for f in os.listdir(base_dir) if f.endswith(".warc.wet")])
selected_files = all_files[start_index:start_index + num_files]

if not selected_files:
    raise ValueError(f"No files found for range {start_index}:{start_index + num_files}")


# ============================================================
# Spark Setup (Local Mode)
# ============================================================
spark = (
    SparkSession.builder
        .appName("WET English Filter to Kafka (Local Mode)")
        .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0")
        .getOrCreate()
)

sc = spark.sparkContext
sc.setLogLevel("WARN")


# ============================================================
# Helpers
# ============================================================
def parse_wet_records(record: str):
    """
    Parses a WET file into structured (url, lang, content) tuples.
    """
    records = record.split("WARC/1.0")
    parsed = []

    for rec in records:
        rec = rec.strip()
        if not rec:
            continue

        uri_match = re.search(r"WARC-Target-URI:\s*(\S+)", rec)
        lang_match = re.search(r"WARC-Identified-Content-Language:\s*([\w,-]+)", rec)
        content_split = re.split(r"\r?\n\r?\n", rec, maxsplit=1)

        if not uri_match or not lang_match or len(content_split) < 2:
            continue

        url = uri_match.group(1).strip()
        lang = lang_match.group(1).strip().lower()
        content = content_split[1].strip()

        if content:
            parsed.append((url, lang, content))

    return parsed


def is_english(lang: str) -> bool:
    langs = re.split(r"[,\s]+", lang.lower())
    return any(l in ("en", "eng", "english") for l in langs)


def normalize_text(text: str) -> str:
    text = text.lower()
    text = re.sub(r"[^a-z0-9\s]", " ", text)
    text = re.sub(r"\s+", " ", text)
    return text.strip()


normalize_udf = udf(normalize_text, StringType())


MAX_CHUNK_SIZE = 512 * 1024  # 512 KB


def chunk_text(text: str):
    b = text.encode("utf-8")
    return [b[i:i + MAX_CHUNK_SIZE].decode("utf-8", "ignore") for i in range(0, len(b), MAX_CHUNK_SIZE)]


chunk_udf = udf(chunk_text, ArrayType(StringType()))


# ============================================================
# Kafka config
# ============================================================
KAFKA_SERVERS = "des_kafka:9092"
TOPIC = "ENGLISH-ENTITY-TOPIC"


# ============================================================
# Process selected files
# ============================================================
start_time = time.time()

for filename in selected_files:
    file_path = os.path.join(base_dir, filename)
    print(f"Processing file: {file_path}")

    files_read_counter.inc()

    rdd = sc.wholeTextFiles(file_path).flatMap(lambda x: parse_wet_records(x[1]))

    # Filter for English + normalized content
    rdd_filtered = (
        rdd.filter(lambda x: is_english(x[1]))
            .map(lambda x: (x[0], normalize_text(x[2])))
            .filter(lambda x: x[1] != "")
    )

    filtered_count = rdd_filtered.count()
    filtered_records_counter.inc(filtered_count)

    if filtered_count == 0:
        print(f"No English content found in {filename}, skipping.")
        continue

    df = rdd_filtered.toDF(["key", "value"])

    df_chunks = df.withColumn("value", chunk_udf(col("value")))
    df_final = df_chunks.select(col("key"), explode(col("value")).alias("value"))

    # Reduce pressure on local Spark
    df_final = df_final.repartition(4, "key")

    print(f"Writing {filtered_count} records to Kafka...")
    kafka_records_counter.inc(filtered_count)

    df_final.write \
        .format("kafka") \
        .option("kafka.bootstrap.servers", KAFKA_SERVERS) \
        .option("topic", TOPIC) \
        .save()

end_time = time.time()


# ============================================================
# Final Metrics
# ============================================================
total_time = end_time - start_time
total_records = kafka_records_counter._value.get()

throughput = total_records / total_time if total_time > 0 else 0.0
throughput_gauge.set(throughput)

print("============================================================")
print("Job Completed")
print("------------------------------------------------------------")
print(f"Total files read:              {files_read_counter._value.get()}")
print(f"Total English records:         {filtered_records_counter._value.get()}")
print(f"Total Kafka records inserted:  {total_records}")
print(f"Throughput (records/sec):      {throughput:.2f}")
print("============================================================")

# ============================================================
# Wait for user to quit (keeps Spark UI alive)
# ============================================================
print("Spark UI will stay alive for monitoring...")
try:
    time.sleep(3600)
except KeyboardInterrupt:
    print("Exiting...")
spark.stop()