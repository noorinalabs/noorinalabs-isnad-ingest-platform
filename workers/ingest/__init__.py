"""ingest-worker.

Consumes ``pipeline.norm.done`` and writes into Neo4j (ports
``src/graph/load_nodes.py`` and ``src/graph/load_edges.py``). Terminal
stage — does not publish to a follow-on topic.
"""
