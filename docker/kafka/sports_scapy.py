#!/usr/bin/env python3
import json
import time
import threading
from queue import Queue, Empty
from confluent_kafka import Consumer, Producer, KafkaException
from prometheus_client import Counter, Gauge, start_http_server
import spacy

# ============================================================
# Prometheus Metrics
# ============================================================
messages_consumed_counter = Counter("messages_consumed_total", "Total messages consumed")
sports_messages_counter = Counter("sports_messages_total", "Total sports messages produced")
throughput_gauge = Gauge("sports_throughput_msgs_per_sec", "Sports messages processed per second")

start_http_server(8001, addr="0.0.0.0")  # separate port for consumer metrics

# ============================================================
# Kafka Config
# ============================================================
CONSUMER_CONF = {
    'bootstrap.servers': 'localhost:29092',
    'group.id': 'sports-consumer-group',
    'auto.offset.reset': 'earliest'
}

PRODUCER_CONF = {
    'bootstrap.servers': 'localhost:29092',
    'linger.ms': 10,
    'batch.num.messages': 1000,
    'compression.type': 'lz4'
}

INPUT_TOPIC = "ENGLISH-ENTITY-TOPIC"
OUTPUT_TOPIC = "SPORTS-ENTITY-TOPIC"

# ============================================================
# Sports classification
# ============================================================
sports_keywords = set([
    "basketball", "football", "soccer", "baseball", "tennis",
    "hockey", "rugby", "cricket", "athletics", "boxing",
    "swimming", "golf", "mma", "esports"
])

nlp = spacy.load("en_core_web_sm")

def classify_sports(text):
    """Return True if text is sports-related."""
    text_lower = text.lower()
    if any(keyword in text_lower for keyword in sports_keywords):
        return True

    # Optional NER fallback (slightly slower)
    doc = nlp(text)
    sports_entities = ["team", "player", "match", "tournament", "league", "competition"]
    for ent in doc.ents:
        if ent.label_ in ["ORG", "PERSON", "EVENT"]:
            if any(se in ent.text.lower() for se in sports_entities):
                return True
    return False

# ============================================================
# Kafka producer worker
# ============================================================
message_queue = Queue(maxsize=5000)
stop_event = threading.Event()
producer = Producer(PRODUCER_CONF)

def producer_worker():
    while not stop_event.is_set() or not message_queue.empty():
        try:
            key, value = message_queue.get(timeout=1)
        except Empty:
            continue
        msg = json.dumps({
            "url": key,
            "text_value": value,
            "timestamp": time.time()
        })
        producer.produce(OUTPUT_TOPIC, key=key, value=msg)
        sports_messages_counter.inc()
        print(f"[Producer] Sent sports message for URL: {key}")
    producer.flush()

# ============================================================
# Consumer loop
# ============================================================
def consumer_loop():
    consumer = Consumer(CONSUMER_CONF)
    consumer.subscribe([INPUT_TOPIC])
    try:
        while True:
            msg = consumer.poll(1.0)
            if msg is None:
                continue
            if msg.error():
                raise KafkaException(msg.error())

            key = msg.key().decode("utf-8")
            value = msg.value().decode("utf-8")

            messages_consumed_counter.inc()
            print(f"[Consumer] Consumed message: {key}")

            if classify_sports(value):
                message_queue.put((key, value))
            else:
                print(f"[Consumer] Skipped non-sports content: {key}")

    finally:
        consumer.close()
        stop_event.set()

# ============================================================
# Main
# ============================================================
def main():
    t_producer = threading.Thread(target=producer_worker, daemon=True)
    t_producer.start()

    start_time = time.time()
    try:
        consumer_loop()
    except KeyboardInterrupt:
        print("Shutting down consumer...")
    finally:
        stop_event.set()
        t_producer.join()
        elapsed = time.time() - start_time
        throughput = sports_messages_counter._value.get() / elapsed
        throughput_gauge.set(throughput)
        print(f"Total sports messages sent: {sports_messages_counter._value.get()}")
        print(f"Throughput: {throughput:.2f} msgs/sec")

if __name__ == "__main__":
    main()
