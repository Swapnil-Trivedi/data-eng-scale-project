import streamlit as st
from rag.graph_retriever import GraphRetriever
from rag.llm_local import LocalLLMWrapper
from rag.metrics import start_metrics_server, RETRIEVAL_LATENCY, LLM_LATENCY, QUERIES_TOTAL

import os

# Start Prometheus metrics server
start_metrics_server(port=8001)

st.set_page_config(page_title="Graph RAG Demo", layout="wide")
st.title("Graph RAG + Local Llama LLM Demo")

# Sidebar for settings
with st.sidebar:
    st.header("Settings")
    neo4j_uri = st.text_input("Neo4j URI", value=os.getenv("NEO4J_URI", "bolt://localhost:7688"))
    neo4j_user = st.text_input("Neo4j User", value=os.getenv("NEO4J_USER", "neo4j"))
    neo4j_password = st.text_input("Neo4j Password", value=os.getenv("NEO4J_PASSWORD", "testpassword"), type="password")
    model_path = st.text_input("Llama GGUF Model Path", value=os.getenv("LLAMA_MODEL_PATH", "./llama-2-7b-chat.Q4_K_M.gguf"))
    top_k = st.slider("Top-K Graph Nodes", 1, 20, 5)

# Main app
user_query = st.text_input("Enter your question:")

if st.button("Run RAG") and user_query:
    QUERIES_TOTAL.inc()
    with RETRIEVAL_LATENCY.time():
        retriever = GraphRetriever(uri=neo4j_uri, user=neo4j_user, password=neo4j_password)
        context = retriever.retrieve_context(user_query, top_k=top_k)
        retriever.close()
    st.subheader("Graph Context")
    st.json([dict(node) for node in context])
    with LLM_LATENCY.time():
        llm = LocalLLMWrapper(model_path=model_path)
        answer = llm.generate(user_query, context)
    st.subheader("LLM Answer")
    st.write(answer)

st.markdown("---")
st.header("RAG Metrics")
st.write("Prometheus metrics available at :8001/metrics")
