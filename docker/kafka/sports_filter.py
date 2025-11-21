from confluent_kafka import Consumer, KafkaException, Producer
import json
from datetime import datetime
from transformers import pipeline

# Load zero-shot model
classifier = pipeline("zero-shot-classification", model="facebook/bart-large-mnli")

def is_sports(text):
    candidate_labels = ["sports", "non-sports"]
    result = classifier(text, candidate_labels)
    return result['labels'][0] == "sports" and result['scores'][0] > 0.8

# Kafka configs
consumer_conf = {
    'bootstrap.servers': 'localhost:29092',
    'group.id': 'sports-consumer-group',
    'auto.offset.reset': 'earliest'
}
producer_conf = {
    'bootstrap.servers': 'localhost:29092'
}

consumer = Consumer(consumer_conf)
producer = Producer(producer_conf)
consumer.subscribe(['ENGLISH-ENTITY-TOPIC'])

def produce_to_sports_topic(url, text_value):
    timestamp = datetime.utcnow().isoformat() + "Z"
    message = {'url': url, 'text_value': text_value, 'timestamp': timestamp}
    producer.produce('SPORTS-ENTITY-TOPIC', key=url, value=json.dumps(message))
    producer.flush()
    print(f"[SPORTS] Produced: {url}")

try:
    while True:
        msg = consumer.poll(1.0)
        if msg is None:
            continue
        if msg.error():
            raise KafkaException(msg.error())

        key = msg.key().decode('utf-8')
        value = msg.value().decode('utf-8')

        if is_sports(value):
            produce_to_sports_topic(key, value)
        else:
            print(f"[SKIP] Not sports: {key}")

finally:
    consumer.close()
