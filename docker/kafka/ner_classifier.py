#!/usr/bin/env python3
import json
from kafka import KafkaConsumer, KafkaProducer
import spacy

# -----------------------------
# Load spaCy model
# -----------------------------
print("Loading spaCy model...")
nlp = spacy.load("en_core_web_trf")  # medium model is faster & lighter than TRF
print("spaCy model loaded!")

# -----------------------------
# Kafka configuration
# -----------------------------
KAFKA_BOOTSTRAP = "localhost:29092"
TOPIC_INPUT = "ENGLISH-ENTITY-TOPIC"
TOPIC_OUTPUT = "ENRICHED-ENTITY-TOPIC"

consumer = KafkaConsumer(
    TOPIC_INPUT,
    bootstrap_servers=[KAFKA_BOOTSTRAP],
    key_deserializer=lambda x: x.decode("utf-8"),
    value_deserializer=lambda x: x.decode("utf-8"),
    auto_offset_reset="earliest",
    enable_auto_commit=True
)

producer = KafkaProducer(
    bootstrap_servers=[KAFKA_BOOTSTRAP],
    value_serializer=lambda v: json.dumps(v).encode("utf-8")
)

# -----------------------------
# Entity extraction (PERSON + GPE only)
# -----------------------------
def extract_entities(doc):
    filtered = []
    for ent in doc.ents:
        if ent.label_ in ("PERSON", "GPE"):
            filtered.append({
                "text": ent.text,
                "label": ent.label_
            })
    return filtered

# -----------------------------
# Simple SVO relation extraction
# -----------------------------
def extract_relations(doc):
    relations = []
    for token in doc:
        if token.dep_ == "ROOT" and token.pos_ == "VERB":
            subjects = [w.text for w in token.lefts if w.dep_ in ("nsubj", "nsubjpass")]
            objects = [w.text for w in token.rights if w.dep_ in ("dobj", "attr", "pobj")]
            if subjects and objects:
                relations.append({
                    "subject": subjects[0],
                    "predicate": token.lemma_,
                    "object": objects[0]
                })
    return relations

# -----------------------------
# Summarization placeholder
# -----------------------------
def summarize_text(text):
    if len(text) < 200:
        return text
    return text[:300] + "..."  # basic truncation

# -----------------------------
# Main loop: Kafka → spaCy → Kafka
# -----------------------------
print("NER Processor running... Waiting for messages.")

for msg in consumer:
    url = msg.key
    content = msg.value

    doc = nlp(content)
    entities = extract_entities(doc)
    relations = extract_relations(doc)
    summary = summarize_text(content)

    package = {
        "url": url,
        "summary": summary,
        "entities": entities,
        "relations": relations
    }

    producer.send(TOPIC_OUTPUT, package)
    producer.flush()

    print(f"Processed URL: {url}, Entities: {len(entities)}, Relations: {len(relations)}")
