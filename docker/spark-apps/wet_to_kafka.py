from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col
from pyspark.sql.types import StringType
import sys
import re
import os

# ------------------------------------------------------------
# Runtime args (for file limits)
# ------------------------------------------------------------
# Usage example:
# spark-submit wet_to_kafka.py 0 10  --> process files 00000-00009
# spark-submit wet_to_kafka.py 5 1   --> process only file 00005
start_index = int(sys.argv[1]) if len(sys.argv) > 1 else 0
num_files = int(sys.argv[2]) if len(sys.argv) > 2 else 1

base_dir = "/opt/spark-data/data/289-index-data-extracted"

# Get all WET filenames in the directory and sort them
all_files = sorted([
    f for f in os.listdir(base_dir) if f.endswith(".warc.wet")
])

# Pick the subset you want to process
selected_files = all_files[start_index : start_index + num_files]

if not selected_files:
    raise ValueError(f"No files found for range {start_index}:{start_index + num_files}")

# Build the full input paths for Spark
input_paths = [os.path.join(base_dir, f) for f in selected_files]

print(f"Processing {len(input_paths)} WET file(s):")
for f in input_paths:
    print(" -", f)

# ------------------------------------------------------------
# Spark setup
# ------------------------------------------------------------
spark = (
    SparkSession.builder
    .appName("WET English Filter to Kafka (Chunked)")
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0")
    .getOrCreate()
)

sc = spark.sparkContext
sc.setLogLevel("WARN")

# ------------------------------------------------------------
# Parser for WET records
# ------------------------------------------------------------
def parse_wet_records(record):
    records = record.split("WARC/1.0")
    parsed = []
    for rec in records:
        rec = rec.strip()
        if not rec:
            continue
        uri_match = re.search(r"WARC-Target-URI:\s*(\S+)", rec)
        lang_match = re.search(r"WARC-Identified-Content-Language:\s*([a-zA-Z-]+)", rec)
        content_split = re.split(r"\r?\n\r?\n", rec, maxsplit=1)
        if not uri_match or not lang_match or len(content_split) < 2:
            continue
        url = uri_match.group(1).strip()
        lang = lang_match.group(1).strip().lower()
        content = content_split[1].strip()
        if content:  # Skip empty content
            parsed.append((url, lang, content))
    return parsed

# ------------------------------------------------------------
# Read and parse selected WET files
# ------------------------------------------------------------
raw_rdd = sc.wholeTextFiles(",".join(input_paths)).flatMap(lambda x: parse_wet_records(x[1]))
df = raw_rdd.toDF(["url", "lang", "content"])

print(f"Total parsed records: {df.count()}")

# ------------------------------------------------------------
# Filter English content (including en-US, en-GB, etc.)
# ------------------------------------------------------------
df_en = df.filter(col("lang").startswith("en"))

# ------------------------------------------------------------
# Normalize text
# ------------------------------------------------------------
def normalize_text(text):
    text = text.lower()
    text = re.sub(r"[^a-z0-9\s]", " ", text)
    text = re.sub(r"\s+", " ", text)
    return text.strip()

normalize_udf = udf(normalize_text, StringType())
df_norm = df_en.withColumn("normalized", normalize_udf(col("content")))

# Skip records where normalization results in empty content
df_norm = df_norm.filter(col("normalized") != "")

# ------------------------------------------------------------
# Send to Kafka (for demo you can disable write)
# ------------------------------------------------------------
df_kafka = df_norm.select(
    col("url").alias("key"),
    col("normalized").alias("value")
)

# # preview instead of sending everything
# df_kafka.show(5, truncate=False)

# Uncomment to enable write to Kafka
# """
df_kafka.write.format("kafka") \
    .option("kafka.bootstrap.servers", "des_kafka:9092") \
    .option("topic", "ENGLISH-ENTITY-TOPIC") \
    .save()


spark.stop()
