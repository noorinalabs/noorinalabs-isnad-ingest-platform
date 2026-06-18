"""Integration test package.

Shared constants for the testcontainers-based integration suite.
"""

# Single source of truth for the testcontainers Neo4j base image tag.
# Org-standardized on neo4j:5-community (noorinalabs-main#642) to share the image
# cache and avoid redundant CI pulls. This is the upstream community base image,
# NOT the app's custom isnad-graph-neo4j:5-community (which bundles APOC/plugins).
NEO4J_TEST_IMAGE = "neo4j:5-community"
