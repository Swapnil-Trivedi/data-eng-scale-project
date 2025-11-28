import streamlit as st
try:
    from pyvis.network import Network
    import networkx as nx
except ImportError:
    Network = None
    nx = None

def show_graph(context):
    if Network is None or nx is None:
        st.warning("pyvis/networkx not installed. Graph visualization unavailable.")
        return
    G = nx.Graph()
    for node in context:
        G.add_node(str(node.id), label=str(node.get('name', node.id)))
    net = Network(notebook=False)
    net.from_nx(G)
    net.show("graph.html")
    with open("graph.html", "r") as f:
        html = f.read()
    st.components.v1.html(html, height=500, scrolling=True)
