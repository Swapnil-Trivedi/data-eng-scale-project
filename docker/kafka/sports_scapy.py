import spacy
from confluent_kafka import Consumer, KafkaException, Producer
import json
from datetime import datetime

# Load the pre-trained spaCy model
nlp = spacy.load("en_core_web_sm")

# List of sports-related keywords
sports_keywords = ["basketball", "football", "soccer", "baseball", "tennis", "hockey", "rugby", "cricket", "athletics", "boxing", "swimming", "golf", "mma", "esports"]

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

# Function to check if the text is related to sports using keyword matching and NLP entity extraction
def classify_as_sports(text):
    """Check if the text contains sports-related keywords or named entities."""
    
    # First, check for sports-related keywords in the text
    for keyword in sports_keywords:
        if keyword.lower() in text.lower():
            return True
    
    # If no keyword is found, use spaCy's NER (Named Entity Recognition) to detect sports-related entities
    doc = nlp(text)
    
    # Look for sports-related entities like teams, players, events
    for ent in doc.ents:
        if ent.label_ in ["ORG", "PERSON", "EVENT"]:
            # Filter for likely sports-related entities based on common sports entities
            sports_related_entities = ["team", "player", "match", "tournament", "league", "competition"]
            if any(entity in ent.text.lower() for entity in sports_related_entities):
                return True
    
    # If no match is found, return False
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

    print(f"Produced sports-related content to SPORTS-ENTITY-TOPIC.")

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

        # Classify the content as sports-related
        if classify_as_sports(value):
            produce_to_sports_topic(key, value)  # Produce to new topic
        else:
            print("Content is not related to sports. Skipping...")

finally:
    consumer.close()
