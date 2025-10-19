# DES Project - Scalable NLP Pipeline with Kafka, Spark, and Neo4j

This project implements a scalable NLP data processing pipeline using Kafka for message streaming, Spark for distributed processing, and Neo4j for knowledge graph construction.  
All components run locally using Docker containers.

---
## Table of Contents
## Components

| Service | Description | Port |
|----------|--------------|------|
| **Zookeeper** | Coordinates Kafka brokers | 2181 |
| **Kafka** | Message broker for raw and enriched NLP entities | 9092 |
| **Kafka UI** | Web UI to inspect topics and messages | 8085 |
| **Spark Master** | Spark master node for distributed processing | 8080 |
| **Spark Worker** | Spark worker node | 8081 |
| **Neo4j** | Graph database for entity and relationship storage | 7474 (HTTP), 7687 (Bolt) |

---

## Setup Instructions

### 1. Prerequisites
Ensure the following are installed:
- Docker (v20+)
- Docker Compose (v2+)
- Bash shell (Linux/macOS or WSL for Windows)

### 2. Clone the repository
```bash
git clone https://github.com/Swapnil-Trivedi/data-eng-scale-project.git
cd des-project

### Make script executable
```bash
chmod +x init_des.sh setup_kafka_topics.sh purge_kafka_topics.sh
```

### Start full environment
```bash
./init_des.sh
```
This script will:
- Start all Docker containers
- Set up Kafka topics for raw and enriched NLP entities
- Display logs for all services
- Open Kafka UI in your default web browser

### 3. Stopping the environment
To stop all services, run:
```bash
docker-compose down
```

### 4. Purging Kafka Topics
To delete existing Kafka topics, run:
```bash
./purge_kafka_topics.sh
```
This will remove the raw and enriched NLP entity topics from Kafka.

---## Accessing Services
| Service | URL | Default Credentials |
|----------|-----|---------------------|
| **Kafka UI** | `http://localhost:8085` | N/A |
| **Spark Master UI** | `http://localhost:8080` | N/A |
| **Neo4j Browser** | `http://localhost:7474` | Username: neo4j Password: testpassword |
---



