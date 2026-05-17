"""Per-label property allow-lists for ingest MERGE.

Closes Farhan's Phase-4 safety flag (#192): the old ingest implementation
used ``SET n += row.props`` which lets an attacker-controlled property
key (``:label``, ``id``, a future scholar-curated field not present in
this batch) overwrite or subvert the node. This module enumerates the
exact properties each label may receive; anything outside the allow-list
is dropped before the Cypher statement is built.

Source of truth
---------------
The property names mirror the Pydantic models in
``noorinalabs-isnad-graph/src/models/*``. This file is hand-authored
canonical (developers grep against it), but mirror integrity is
enforced by a CI gate in the isnad-graph repo:

  noorinalabs-isnad-graph/.github/workflows/schema-drift.yml
  noorinalabs-isnad-graph/scripts/emit_ingest_schema.py

The gate runs on every isnad-graph PR. It fetches this file via
sparse-checkout, derives the model-canonical allow-list from
``model_fields``, and fails if either direction drifts (a model field
ingest doesn't accept, OR a phantom ingest field not on the model).
Rationalized asymmetry is declared in
``noorinalabs-isnad-graph/scripts/{ingest,model}-extras.yaml`` with
per-field tracking issue. See #24 for the design.

When a model grows a new field, the isnad-graph PR will fail CI until
EITHER (a) a sibling PR to this file lands adding the field to the
appropriate allow-list, OR (b) the model field is excused via
``model-extras.yaml`` with a category + tracking issue. The two PRs do
not need to merge atomically — the workflow resolves to the matching
wave-branch with main fallback so PRs in the same wave window converge.

Fields intentionally omitted
----------------------------
``id`` is matched in the MERGE clause and never re-SET. Phase-4-only
fields that are exclusively populated by post-ingest enrichment (e.g.
narrator centrality / pagerank) are excluded so an unenriched batch
doesn't wipe them out.

Contract: ingest batches cannot clear fields
--------------------------------------------
The allow-listed properties are written with
``n.<f> = coalesce(row.props.<f>, n.<f>)`` in ``_build_node_cypher`` /
``_build_edge_cypher``. The coalesce is asymmetric on the explicit-null
branch:

- field omitted from ``row.props``  → preserve existing value
- field present with explicit null  → preserve existing value (silent no-op)
- field present with a new value    → overwrite

A normalize-stage batch therefore cannot null out a stale ``grade`` or
remove a ``sect_affiliation`` that a later source disavowed — emitting
``{"grade": null}`` is a no-op against the live node. This is the
intended Phase-4 default (scholar curation must survive ingest churn);
field-clearing, if needed, will arrive on a separate corrections path
rather than through this allow-list. See #23 and the #192 design
thread.
"""

from __future__ import annotations

__all__ = [
    "ALLOWED_EDGE_LABELS",
    "EDGE_PROPERTY_MAP",
    "NODE_PROPERTY_MAP",
]

# Node properties that ingest will MERGE. Keyed by Neo4j label; values
# are the exact property names that appear on the Pydantic model in
# noorinalabs-isnad-graph. Anything outside this allow-list is silently
# dropped (see ``_build_node_cypher``).
NODE_PROPERTY_MAP: dict[str, list[str]] = {
    "Hadith": [
        "matn_ar",
        "matn_en",
        "isnad_raw_ar",
        "isnad_raw_en",
        "grade",
        "grade_composite",
        "source_corpus",
        "sect",
        "collection_name",
        "book_number",
        "chapter_number",
        "hadith_number",
        "chapter_name_ar",
        "chapter_name_en",
    ],
    "Narrator": [
        "name_ar",
        "name_en",
        "name_ar_normalized",
        "kunya",
        "nisba",
        "laqab",
        "birth_year_ah",
        "death_year_ah",
        "birth_location_id",
        "death_location_id",
        "generation",
        "gender",
        "sect_affiliation",
        "tabaqat_class",
        "trustworthiness_consensus",
        "transmission_method",
    ],
    "Collection": [
        "name_ar",
        "name_en",
        "compiler_name",
        "compiler_id",
        "compilation_year_ah",
        "sect",
        "canonical_rank",
        "total_hadiths",
        "book_count",
        "source_corpus",
    ],
    "Chain": [
        "hadith_id",
        "chain_index",
        "full_chain_text_ar",
        "full_chain_text_en",
        "chain_length",
        "is_complete",
        "is_elevated",
        "classification",
        "narrator_ids",
    ],
    "Grading": [
        "hadith_id",
        "scholar_name",
        "grade",
        "methodology_school",
        "era",
    ],
    "HistoricalEvent": [
        "name_ar",
        "name_en",
        "year_start_ah",
        "year_end_ah",
        "year_start_ce",
        "year_end_ce",
        "event_type",
        "caliphate",
        "region",
        "description",
        "source_url",
    ],
    "Location": [
        "name_ar",
        "name_en",
        "region",
        "lat",
        "lon",
        "political_entity_period",
    ],
}


# Edge relationship types the ingest stage may MERGE. Kept explicit to
# prevent Cypher-label injection from a manifest-listed edges.parquet
# whose rows carry an unexpected label.
ALLOWED_EDGE_LABELS: frozenset[str] = frozenset(
    {
        "TRANSMITTED_TO",
        "NARRATED",
        "APPEARS_IN",
        "STUDIED_UNDER",
        "PARALLEL_OF",
        "GRADED_BY",
        "ACTIVE_DURING",
        "BASED_IN",
    }
)


# Per-relationship property allow-lists, mirroring
# noorinalabs-isnad-graph/src/models/edges.py. Same rationale as the
# node map: per-field SET rather than ``SET r += row.props``.
EDGE_PROPERTY_MAP: dict[str, list[str]] = {
    "TRANSMITTED_TO": [
        "hadith_id",
        "chain_id",
        "position_in_chain",
        "transmission_method",
    ],
    "NARRATED": [],
    "APPEARS_IN": [
        "book_number",
        "chapter_number",
        "hadith_number",
        "hadith_number_in_book",
    ],
    "STUDIED_UNDER": [
        "period_ah",
        "location_id",
        "source",
    ],
    "PARALLEL_OF": [
        "similarity_score",
        "variant_type",
        "cross_sect",
    ],
    "GRADED_BY": [],
    "ACTIVE_DURING": [
        "role",
        "affiliation",
    ],
    "BASED_IN": [
        "period_ah",
        "role",
    ],
}
