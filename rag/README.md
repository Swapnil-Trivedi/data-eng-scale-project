# RAG Module Documentation

This folder contains the Retrieval-Augmented Generation (RAG) pipeline components for integrating a knowledge graph (Neo4j), a local LLM (Llama via llama-cpp-python), and metrics for monitoring.

## Modules

### 1. `graph_retriever.py`
- **Purpose:** Connects to Neo4j and retrieves relevant nodes based on a user query.
- **Key Class:** `GraphRetriever`
  - `retrieve_context(query: str, top_k: int = 5) -> List[Dict]`: Runs a Cypher query to find nodes whose `name` or `description` matches the query, returning up to `top_k` results.
  - `close()`: Closes the Neo4j connection.

### 2. `llm_local.py`
- **Purpose:** Runs a local Llama model using `llama-cpp-python` for text generation.
- **Key Class:** `LocalLLMWrapper`
  - `generate(user_query: str, context: List[dict], max_tokens=256) -> str`: Formats the context and user query into a prompt, sends it to the Llama model, and returns the generated answer.
  - Requires a GGUF model file (downloaded separately).

### 3. `llm_wrapper.py`
- **Purpose:** (Optional) Wrapper for OpenAI LLMs. Not used in the local-only pipeline.
- **Key Class:** `LLMWrapper`
  - `generate(user_query: str, context: List[dict]) -> str`: Formats context and query, sends to OpenAI API, returns answer.

### 4. `metrics.py`
- **Purpose:** Exposes Prometheus metrics for RAG operations.
- **Key Functions/Variables:**
  - `start_metrics_server(port=8001)`: Starts a metrics HTTP server.
  - `RETRIEVAL_LATENCY`, `LLM_LATENCY`, `QUERIES_TOTAL`: Prometheus metrics for monitoring.

### 5. `__init__.py`
- **Purpose:** Marks the folder as a Python package.

---

# Streamlit App Documentation

## File: `streamlit_app.py`

- **Purpose:** Provides a web UI for querying the knowledge graph, running RAG, and viewing metrics.
- **Main Features:**
  - Sidebar for configuring Neo4j connection and Llama model path.
  - Input box for user queries.
  - Displays retrieved graph context and LLM-generated answer.
  - Shows Prometheus metrics endpoint.

## Flow
1. User enters a query.
2. App retrieves relevant nodes from Neo4j using `GraphRetriever`.
3. App sends context and query to local Llama model via `LocalLLMWrapper`.
4. Displays context and answer in the UI.
5. Metrics are tracked and exposed for monitoring.

---

## Example Usage
```python
retriever = GraphRetriever()
context = retriever.retrieve_context("Kohli")
retriever.close()

llm = LocalLLMWrapper(model_path="./llama-2-7b-chat.Q4_K_M.gguf")
answer = llm.generate("Who is Kohli?", context)
```

---

## Requirements
- Python 3.8–3.11 recommended
- Install dependencies:
  ```bash
  pip install -r rag/requirements.txt
  ```
- Download a GGUF model for Llama and set its path in the UI or as `LLAMA_MODEL_PATH`.

---

For further details, see code comments in each module.
