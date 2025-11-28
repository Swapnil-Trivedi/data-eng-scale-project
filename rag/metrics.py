from prometheus_client import Counter, Histogram, start_http_server
import time
import threading

# Metrics definitions
RETRIEVAL_LATENCY = Histogram('rag_retrieval_latency_seconds', 'Time spent retrieving context from Neo4j')
LLM_LATENCY = Histogram('rag_llm_latency_seconds', 'Time spent generating LLM response')
QUERIES_TOTAL = Counter('rag_queries_total', 'Total number of RAG queries served')

# Start Prometheus metrics server in background

def start_metrics_server(port=8001):
    def run():
        start_http_server(port)
    thread = threading.Thread(target=run, daemon=True)
    thread.start()

# Usage:
# start_metrics_server(8001)
# Use @RETRIEVAL_LATENCY.time() and @LLM_LATENCY.time() as decorators
