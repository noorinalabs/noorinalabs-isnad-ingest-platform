"""HTTP API surface for the ingest platform.

Currently exposes the admin-only pipeline-reset endpoints (issue #70) that
wrap the reset CLI surface (issue #9) so the isnad-graph admin reset UI can
drive pipeline resets over HTTP. The app factory lives in :mod:`src.api.app`.
"""
