"""Tests for the vendored ``src.parse.identity`` contract.

This module is a faithful mirror of the canonical contract in
``noorinalabs-data-acquisition`` (``src/parse/identity.py``). These tests pin
the behaviour the ingest pipeline relies on — chiefly that ``hadith_node_id`` is
the ONE prefixing rule both the streaming/normalize worker and the in-repo batch
loaders route through, so a hadith yields exactly one graph node id regardless of
path (#72 / main#139 double-prefix hazard).

The fixtures deliberately use realistic corpus-prefixed ids
(``sunnah:bukhari:1``) rather than toy ``h-1`` ids: a bare ``h-1`` has no corpus
segment and would mask the exact double-prefix bug this contract exists to close.
"""

from __future__ import annotations

import pytest

from src.parse.identity import (
    bare_source_id,
    hadith_node_id,
    is_double_prefixed,
    validate_source_id,
)


class TestHadithNodeId:
    def test_prefixes_bare_source_id(self) -> None:
        assert hadith_node_id("sunnah:bukhari:1") == "hdt:sunnah:bukhari:1"

    def test_idempotent_on_hdt_prefix(self) -> None:
        # Already-prefixed input must not gain a second ``hdt:``.
        assert hadith_node_id("hdt:sunnah:bukhari:1") == "hdt:sunnah:bukhari:1"

    def test_collapses_doubled_corpus(self) -> None:
        # The headline #72 / main#139 case: a doubled leading corpus collapses
        # so the streaming and batch paths converge on one id. The old ad-hoc
        # ``f"hdt:{sid}"`` guard would have produced ``hdt:sunnah:sunnah:...``.
        assert hadith_node_id("sunnah:sunnah:bukhari:1") == "hdt:sunnah:bukhari:1"

    def test_collapses_doubled_corpus_even_with_hdt_prefix(self) -> None:
        assert hadith_node_id("hdt:sunnah:sunnah:bukhari:1") == "hdt:sunnah:bukhari:1"

    def test_collapses_triple_corpus(self) -> None:
        assert hadith_node_id("sunnah:sunnah:sunnah:bukhari:1") == "hdt:sunnah:bukhari:1"

    def test_streaming_and_batch_converge_on_doubled_input(self) -> None:
        # Both ingest paths now call this one helper, so a doubled staging id
        # produces a single shared node id rather than two divergent nodes.
        doubled = "sunnah:sunnah:bukhari:1"
        clean = "sunnah:bukhari:1"
        assert hadith_node_id(doubled) == hadith_node_id(clean)

    def test_does_not_touch_repeated_non_leading_segment(self) -> None:
        # A repeated slug that isn't a doubled *leading corpus* is legitimate
        # and must be preserved (only the leading corpus collapses).
        assert hadith_node_id("sunnah:bukhari:bukhari:1") == "hdt:sunnah:bukhari:bukhari:1"

    def test_does_not_collapse_non_corpus_leading_repeat(self) -> None:
        # ``foo`` is not a known corpus, so a doubled ``foo`` is left alone.
        assert hadith_node_id("foo:foo:bar") == "hdt:foo:foo:bar"


class TestBareSourceId:
    def test_strips_hdt_and_collapses(self) -> None:
        assert bare_source_id("hdt:sunnah:sunnah:bukhari:1") == "sunnah:bukhari:1"


class TestIsDoublePrefixed:
    def test_detects_doubled_corpus(self) -> None:
        assert is_double_prefixed("hdt:sunnah:sunnah:bukhari:1") is True

    def test_clean_id_not_flagged(self) -> None:
        assert is_double_prefixed("hdt:sunnah:bukhari:1") is False


class TestValidateSourceId:
    def test_clean_source_id_valid(self) -> None:
        assert validate_source_id("sunnah:bukhari:1") == []

    def test_doubled_corpus_reported(self) -> None:
        problems = validate_source_id("sunnah:sunnah:bukhari:1")
        assert any("doubled leading corpus" in p for p in problems)

    @pytest.mark.parametrize("bad", ["", "bukhari", "sunnah::1"])
    def test_grammar_violations_reported(self, bad: str) -> None:
        assert validate_source_id(bad) != []
