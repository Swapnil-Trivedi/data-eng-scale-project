from neo4j import GraphDatabase
from typing import List, Dict
import os

class GraphRetriever:
    def __init__(self, uri=None, user=None, password=None):
        self.uri = uri or os.getenv("NEO4J_URI", "bolt://localhost:7688")
        self.user = user or os.getenv("NEO4J_USER", "neo4j")
        self.password = password or os.getenv("NEO4J_PASSWORD", "testpassword")
        self.driver = GraphDatabase.driver(self.uri, auth=(self.user, self.password))

    def close(self):
        self.driver.close()

    def retrieve_context(self, query: str, top_k: int = 5) -> List[Dict]:
        # Example Cypher query: match nodes/relationships relevant to the query
        cypher = """
        MATCH (n)
        WHERE n.name CONTAINS $query OR n.description CONTAINS $query
        RETURN n LIMIT $top_k
        """
        with self.driver.session() as session:
            result = session.run(cypher, {"query": query, "top_k": top_k})
            return [record["n"] for record in result]

# Example usage:
# retriever = GraphRetriever()
# context = retriever.retrieve_context("kohli")
# retriever.close()

