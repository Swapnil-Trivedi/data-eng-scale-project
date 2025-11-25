#!/usr/bin/env python3
import json
import time
from confluent_kafka import Consumer
from neo4j import GraphDatabase

# Kafka configuration
KAFKA_BOOTSTRAP = "localhost:29092"
KAFKA_TOPIC = "ENRICHED-ENTITY-TOPIC"
KAFKA_GROUP = "neo4j_upsert_group"
BATCH_SIZE = 50
BATCH_TIMEOUT = 2.0

# Neo4j configuration
NEO_URI = "bolt://localhost:7687"
NEO_USER = None
NEO_PASSWORD = None

# Initialize Neo4j driver
driver = GraphDatabase.driver(NEO_URI, auth=None)

# Kafka consumer config
consumer_conf = {
    "bootstrap.servers": KAFKA_BOOTSTRAP,
    "group.id": KAFKA_GROUP,
    "auto.offset.reset": "earliest"  # read from oldest messages
}

consumer = Consumer(consumer_conf)
consumer.subscribe([KAFKA_TOPIC])

def upsert_entities(tx, records):
    """
    Upsert URL and Entity nodes with relationships
    Only ORG, PERSON, LOCATION entities are considered
    """
    for rec in records:
        url = rec.get("url")
        entities = rec.get("entities", [])

        # Upsert URL node
        tx.run("MERGE (u:URL {url: $url})", url=url)

        for ent in entities:
            ent_type = ent.get("label")
            if ent_type not in ("ORG", "PERSON", "LOCATION"):
                continue

            # Upsert Entity node
            tx.run(
                "MERGE (e:Entity {name: $name, type: $type})",
                name=ent["text"],
                type=ent_type
            )

            # Create relationship: URL -> Entity
            tx.run(
                "MATCH (u:URL {url: $url}), (e:Entity {name: $name}) "
                "MERGE (u)-[:MENTIONS]->(e)",
                url=url,
                name=ent["text"]
            )

batch = []
last_flush = time.time()

print("Neo4j consumer started. Listening to Kafka...")

try:
    while True:
        msg = consumer.poll(timeout=1.0)
        now = time.time()

        if msg is None:
            if batch and now - last_flush > BATCH_TIMEOUT:
                with driver.session() as session:
                    session.execute_write(upsert_entities, batch)
                batch.clear()
                last_flush = now
            continue

        if msg.error():
            print(f"[Kafka ERROR] {msg.error()}")
            continue

        try:
            record = json.loads(msg.value().decode("utf-8"))
            batch.append(record)
        except Exception as e:
            print(f"[Consumer ERROR] Failed to decode JSON: {e}")
            continue

        if len(batch) >= BATCH_SIZE:
            try:
                with driver.session() as session:
                    session.execute_write(upsert_entities, batch)
            except Exception as e:
                print(f"[Neo4j ERROR] {e}")
            batch.clear()
            last_flush = now

except KeyboardInterrupt:
    print("Shutting down consumer...")

finally:
    if batch:
        try:
            with driver.session() as session:
                session.execute_write(upsert_entities, batch)
        except Exception as e:
            print(f"[Neo4j ERROR] Final upsert failed: {e}")
    consumer.close()
    driver.close()
    print("Consumer stopped.")
