#!/usr/bin/env python3
import os
import re
import sys
import time
import threading
import logging
from logging.handlers import RotatingFileHandler
from confluent_kafka import Producer
from prometheus_client import Counter, Gauge, Histogram, start_http_server

# ============================================================
# Prometheus Metrics
# ============================================================
files_read_counter = Counter("files_read_total", "Total number of files read")
filtered_records_counter = Counter("filtered_records_total", "Total number of filtered English-normalized chunks")
kafka_records_counter = Counter("kafka_records_total", "Total number of Kafka messages sent")
kafka_send_errors = Counter("kafka_send_errors_total", "Kafka send errors")
kafka_send_latency = Histogram(
    "kafka_send_latency_seconds",
    "Kafka produce latency in seconds",
    buckets=[0.001, 0.01, 0.05, 0.1, 0.5, 1, 5, 10]
)
kafka_batch_size = Histogram(
    "kafka_batch_size_bytes",
    "Kafka message size in bytes",
    buckets=[128, 512, 1024, 4096, 16384, 65536, 262144, 524288, 1048576]
)
per_file_chunks_total = Counter("per_file_chunks_total", "Chunks produced per file", ["file"])
threads_active = Gauge("threads_active", "Number of active ingestion threads")
throughput_gauge = Gauge("throughput_records_per_second", "Records per second processed")

# Start Prometheus metrics server
start_http_server(8000, addr="0.0.0.0")

# ============================================================
# Config
# ============================================================
BASE_DIR = "../data/289-index-data-extracted"
KAFKA_BOOTSTRAP = "localhost:29092"
TOPIC = "ENGLISH-ENTITY-TOPIC"
MAX_CHUNK_SIZE = 512 * 1024  # 512 KB

# Logging for keys
LOG_DIR = "./ingest_logs"
os.makedirs(LOG_DIR, exist_ok=True)
key_logger = logging.getLogger("key_logger")
key_logger.setLevel(logging.INFO)
handler = RotatingFileHandler(os.path.join(LOG_DIR, "keys.log"),
                              maxBytes=5*1024*1024,
                              backupCount=5)
formatter = logging.Formatter('%(asctime)s | %(message)s')
handler.setFormatter(formatter)
key_logger.addHandler(handler)

# ============================================================
# Helpers
# ============================================================
def is_english(lang: str) -> bool:
    langs = re.split(r"[,\s]+", lang.lower())
    return any(l in ("en", "eng", "english") for l in langs)

def normalize(text: str) -> str:
    text = re.sub(r"[^a-z0-9\s]", " ", text.lower())
    return re.sub(r"\s+", " ", text).strip()

def parse_wet(content: str):
    for rec in content.split("WARC/1.0"):
        rec = rec.strip()
        if not rec:
            continue
        uri = re.search(r"WARC-Target-URI:\s*(\S+)", rec)
        lang = re.search(r"WARC-Identified-Content-Language:\s*([\w,-]+)", rec)
        body = re.split(r"\r?\n\r?\n", rec, maxsplit=1)
        if not uri or not lang or len(body) < 2:
            continue
        yield uri.group(1), lang.group(1), body[1].strip()

def chunk_text(text: str):
    b = text.encode("utf-8")
    for i in range(0, len(b), MAX_CHUNK_SIZE):
        yield b[i:i + MAX_CHUNK_SIZE].decode("utf-8", "ignore")

# ============================================================
# Kafka Producer (shared)
# ============================================================
producer = Producer({
    "bootstrap.servers": KAFKA_BOOTSTRAP,
    "linger.ms": 10,
    "batch.num.messages": 10000,
    "compression.type": "lz4",
})

def produce_with_metrics(key, value, file_name):
    size = len(value.encode("utf-8"))

    @kafka_send_latency.time()
    def send():
        try:
            producer.produce(TOPIC, key=key, value=value)
            kafka_records_counter.inc()
            kafka_batch_size.observe(size)
            per_file_chunks_total.labels(file=file_name).inc()
        except Exception as e:
            kafka_send_errors.inc()
            print(f"[Kafka ERROR] {key} -> {e}")

    send()

# ============================================================
# Worker Thread
# ============================================================
def process_file(path: str):
    fname = os.path.basename(path)
    print(f"[Thread] Starting: {fname}")
    threads_active.inc()

    try:
        with open(path, "r", encoding="utf-8", errors="ignore") as f:
            content = f.read()
    except Exception as e:
        print(f"[Thread] ERROR reading file {fname}: {e}")
        threads_active.dec()
        return

    count_chunks = 0
    for url, lang, body in parse_wet(content):
        if not is_english(lang):
            continue
        text = normalize(body)
        if not text:
            continue
        for chunk in chunk_text(text):
            key_logger.info(url)        # log key
            produce_with_metrics(url, chunk, fname)
            count_chunks += 1

    producer.flush()
    filtered_records_counter.inc(count_chunks)
    threads_active.dec()
    print(f"[Thread] Finished {fname}, sent {count_chunks} chunks to Kafka")

# ============================================================
# Main
# ============================================================
def main(start_index: int, num_files: int):
    all_files = sorted([f for f in os.listdir(BASE_DIR) if f.endswith(".warc.wet")])
    selected = all_files[start_index:start_index + num_files]
    if not selected:
        print(f"No files found in range {start_index}:{start_index + num_files}")
        return

    files_read_counter.inc(len(selected))
    print("============================================================")
    print(f"Processing {len(selected)} files (thread per file):")
    for f in selected:
        print(f" - {f}")
    print("============================================================")

    t0 = time.time()
    threads = []
    for filename in selected:
        path = os.path.join(BASE_DIR, filename)
        t = threading.Thread(target=process_file, args=(path,))
        t.start()
        threads.append(t)

    for t in threads:
        t.join()

    elapsed = time.time() - t0
    rate = kafka_records_counter._value.get() / elapsed
    throughput_gauge.set(rate)

    print("============================================================")
    print("Ingest Complete")
    print(f" Files processed       : {len(selected)}")
    print(f" Kafka messages sent   : {kafka_records_counter._value.get()}")
    print(f" Throughput            : {rate:.2f} msg/sec")
    print("============================================================")

# ============================================================
# CLI
# ============================================================
if __name__ == "__main__":
    if len(sys.argv) < 3:
        print("Usage: python3 kafka_wet_ingest.py <start_index> <num_files>")
        sys.exit(1)
    main(int(sys.argv[1]), int(sys.argv[2]))
