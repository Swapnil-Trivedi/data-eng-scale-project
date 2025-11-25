import json
from kafka import KafkaConsumer, KafkaProducer
import spacy

# -----------------------------
# Load spaCy Transformer model
# -----------------------------
# Make sure you installed torch w/ CUDA + the TRF model
# pip install torch torchvision torchaudio --index-url https://download.pytorch.org/whl/cu118
# pip install spacy
# python -m spacy download en_core_web_trf

print("Loading spaCy TRF model...")
#nlp = spacy.load("en_core_web_trf")
nlp = spacy.load("en_core_web_md")
print("spaCy model loaded!")


# -----------------------------
# Kafka config (based on your compose file)
# Use localhost:29092 to connect from host to dockerized Kafka
# -----------------------------

KAFKA_BOOTSTRAP = "localhost:29092"    # matches PLAINTEXT_HOST listener

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
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)


# -------------------------------------------------
# Entity Extraction
# -------------------------------------------------

def extract_entities(doc):
    entities = []
    for ent in doc.ents:
        entities.append({
            "text": ent.text,
            "label": ent.label_
        })
    return entities


# -------------------------------------------------
# Relation Extraction (simple SVO based)
# -------------------------------------------------

def extract_relations(doc):
    relations = []
    for token in doc:
        # Identify the main verb/root
        if token.dep_ == "ROOT" and token.pos_ == "VERB":
            subjects = [w.text for w in token.lefts if w.dep_ in ("nsubj", "nsubjpass")]
            objects = [w.text for w in token.rights if w.dep_ in ("dobj", "attr", "pobj")]

            if subjects and objects:
                relations.append({
                    "subject": subjects[0],
                    "predicate": token.lemma_,   # lemmatized verb
                    "object": objects[0]
                })
    return relations


# -------------------------------------------------
# Summarization Placeholder
# (replace with GPT-4, Llama 3, or any LLM)
# -------------------------------------------------

def summarize_text(text):
    # Replace with actual summarizer later
    if len(text) < 200:
        return text  # Short text == no need to summarize

    return text[:300] + "..."   # placeholder


# -------------------------------------------------
# Main loop: Kafka → spaCy → Kafka
# -------------------------------------------------

print("NER Processor running... Waiting for messages.")

for msg in consumer:
    url = msg.key
    content = msg.value

    print(f"\nReceived message from URL: {url}")

    # Run spaCy NER/RE
    doc = nlp(content)

    entities = extract_entities(doc)
    relations = extract_relations(doc)
    summary = summarize_text(content)

    ner_package = {
        "url": url,
        "summary": summary,
        "entities": entities,
        "relations": relations
    }

    # Publish to Topic B
    producer.send(TOPIC_OUTPUT, ner_package)
    producer.flush()

    print(f"Sent extracted package for URL: {url}")
    print(f"Entities: {len(entities)}  Relations: {len(relations)}")
