#!/usr/bin/env python3
import os
import re
import sys
import time
import threading
from queue import Queue
from confluent_kafka import Producer
from prometheus_client import Counter, Gauge, start_http_server

# ============================================================
# Prometheus Metrics
# ============================================================
files_read_counter = Counter("files_read_total", "Total number of files read")
filtered_records_counter = Counter("filtered_records_total", "Total number of filtered English-normalized chunks")
kafka_records_counter = Counter("kafka_records_total", "Total number of Kafka messages sent")
throughput_gauge = Gauge("throughput_records_per_second", "Records per second processed")

start_http_server(8000, addr="0.0.0.0")

# ============================================================
# Config
# ============================================================
BASE_DIR = "../data/289-index-data-extracted"
KAFKA_BOOTSTRAP = "localhost:29092"   # for host access
TOPIC = "ENGLISH-ENTITY-TOPIC"
MAX_CHUNK_SIZE = 512 * 1024  # 512 KB

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
# Kafka Producer (shared among threads)
# ============================================================
producer = Producer({
    "bootstrap.servers": KAFKA_BOOTSTRAP,
    "linger.ms": 10,
    "batch.num.messages": 10000,
    "compression.type": "lz4",
})

# ============================================================
# Worker Thread Function
# ============================================================
def process_file(path: str):
    fname = os.path.basename(path)
    print(f"[Thread] Starting: {fname}")

    try:
        with open(path, "r", encoding="utf-8", errors="ignore") as f:
            content = f.read()
    except Exception as e:
        print(f"[Thread] ERROR reading file {fname}: {e}")
        return

    count_chunks = 0

    for url, lang, body in parse_wet(content):
        if not is_english(lang):
            continue

        text = normalize(body)
        if not text:
            continue

        for chunk in chunk_text(text):
            producer.produce(TOPIC, key=url, value=chunk)
            kafka_records_counter.inc()
            count_chunks += 1

    producer.flush()  # flush after file

    filtered_records_counter.inc(count_chunks)
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
# CLI Entry Point
# ============================================================
if __name__ == "__main__":
    if len(sys.argv) < 3:
        print("Usage: python3 kafka_wet_ingest.py <start_index> <num_files>")
        sys.exit(1)

    main(int(sys.argv[1]), int(sys.argv[2]))