import json
from confluent_kafka import Consumer, KafkaException, Producer
from transformers import pipeline, AutoModelForSequenceClassification, AutoTokenizer
import torch
from datetime import datetime

# Check if CUDA is available and set the device accordingly
device = torch.device("cuda" if torch.cuda.is_available() else "cpu")
print(f"Using device: {device}")

# Initialize Zero-shot classification model (for sports classification)
zero_shot_model_name = "facebook/bart-large-mnli"
zero_shot_tokenizer = AutoTokenizer.from_pretrained(zero_shot_model_name)
zero_shot_model = AutoModelForSequenceClassification.from_pretrained(zero_shot_model_name)
zero_shot_classifier = pipeline("zero-shot-classification", model=zero_shot_model, tokenizer=zero_shot_tokenizer, device=0 if torch.cuda.is_available() else -1)

# Kafka Consumer and Producer Configuration
consumer_conf = {
    'bootstrap.servers': 'localhost:29092',  # Use the external port of Kafka
    'group.id': 'sports-consumer-group',
    'auto.offset.reset': 'earliest'
}
consumer = Consumer(consumer_conf)

producer_conf = {
    'bootstrap.servers': 'localhost:29092'  # Use the external port of Kafka
}
producer = Producer(producer_conf)

consumer.subscribe(['ENGLISH-ENTITY-TOPIC'])

# Categories for classification
categories = ["sports", "technology", "politics", "health", "science", "entertainment", "business"]

def classify_as_sports(text):
    """Classify whether the text is sports-related using zero-shot classification."""
    result = zero_shot_classifier(text, candidate_labels=categories)
    if result['labels'][0] == 'sports' and result['scores'][0] > 0.6:  # Confidence threshold
        return True
    return False

def produce_to_sports_topic(url, text_value):
    """Produce the sports-related content to the SPORTS-ENTITY-TOPIC."""
    timestamp = datetime.utcnow().isoformat() + "Z"
    message = {
        'url': url,
        'text_value': text_value,  # Full original text content
        'timestamp': timestamp
    }

    producer.produce('SPORTS-ENTITY-TOPIC', value=json.dumps(message))
    producer.flush()  # Ensure the message is sent


# Main loop: consume messages
try:
    while True:
        msg = consumer.poll(1.0)

        if msg is None:  # No message available within the timeout
            continue
        if msg.error():  # Handle Kafka errors
            raise KafkaException(msg.error())

        key = msg.key().decode('utf-8')  # URL (key)
        value = msg.value().decode('utf-8')  # Normalized text content (value)

        print(f"Consumed message from {key}")

        # First classify as sports-related content
        if classify_as_sports(value):
            produce_to_sports_topic(key, value)  # Produce to new topic
            print(f"Produced sports-related content for URL: {key}")

finally:
    consumer.close()
