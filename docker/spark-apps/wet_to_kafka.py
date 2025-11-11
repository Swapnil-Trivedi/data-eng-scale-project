from pyspark.sql import SparkSession
from pyspark.sql.functions import col, udf, explode
from pyspark.sql.types import StringType, ArrayType
import sys
import re
import os

# --------------------------
# Runtime arguments
# --------------------------
start_index = int(sys.argv[1]) if len(sys.argv) > 1 else 0
num_files = int(sys.argv[2]) if len(sys.argv) > 2 else 1

base_dir = "/opt/spark-data/data/289-index-data-extracted"
all_files = sorted([f for f in os.listdir(base_dir) if f.endswith(".warc.wet")])
selected_files = all_files[start_index:start_index + num_files]
if not selected_files:
    raise ValueError(f"No files found for range {start_index}:{start_index + num_files}")

# --------------------------
# Spark setup
# --------------------------
spark = (
    SparkSession.builder
    .appName("WET English Filter to Kafka (Chunked, Per-File)")
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0")
    .getOrCreate()
)
sc = spark.sparkContext
sc.setLogLevel("WARN")

# --------------------------
# Parser for WET records
# --------------------------
def parse_wet_records(record):
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

# --------------------------
# Language check for English
# --------------------------
def is_english(lang_str):
    langs = re.split(r"[,\s]+", lang_str.lower())
    return any(l in ["en", "eng", "english"] for l in langs)

# --------------------------
# Normalize text
# --------------------------
def normalize_text(text):
    text = text.lower()
    text = re.sub(r"[^a-z0-9\s]", " ", text)
    text = re.sub(r"\s+", " ", text)
    return text.strip()

normalize_udf = udf(normalize_text, StringType())

# --------------------------
# Chunk large messages
# --------------------------
MAX_CHUNK_SIZE = 512 * 1024  # 512 KB
def chunk_text(text):
    b = text.encode("utf-8")
    return [b[i:i+MAX_CHUNK_SIZE].decode("utf-8", errors="ignore") for i in range(0, len(b), MAX_CHUNK_SIZE)]

chunk_udf = udf(chunk_text, ArrayType(StringType()))

# --------------------------
# Kafka config
# --------------------------
KAFKA_SERVERS = "des_kafka:9092"
TOPIC = "ENGLISH-ENTITY-TOPIC"

# --------------------------
# Process files one by one
# --------------------------
for f in selected_files:
    file_path = os.path.join(base_dir, f)
    print(f"Reading file: {file_path}")

    rdd = sc.wholeTextFiles(file_path).flatMap(lambda x: parse_wet_records(x[1]))
    rdd_filtered = rdd.filter(lambda x: is_english(x[1])) \
                       .map(lambda x: (x[0], normalize_text(x[2]))) \
                       .filter(lambda x: x[1] != "")

    if rdd_filtered.isEmpty():
        print(f"No English content found in {f}, skipping.")
        continue

    df = rdd_filtered.toDF(["key", "value"])
    df_chunks = df.withColumn("value", chunk_udf(col("value")))
    df_final = df_chunks.select(col("key"), explode(col("value")).alias("value"))

    print(f"Writing {df_final.count()} records to Kafka for file: {f}")
    df_final.write \
        .format("kafka") \
        .option("kafka.bootstrap.servers", KAFKA_SERVERS) \
        .option("topic", TOPIC) \
        .save()

spark.stop()
