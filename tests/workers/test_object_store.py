"""ObjectStore wrapper tests against the in-memory fake S3 client."""

from __future__ import annotations

import io
import tempfile
import tracemalloc

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from tests.workers.conftest import FakeS3Client
from workers.lib.object_store import SPOOL_MAX_SIZE, ObjectStore


def test_put_then_get_returns_seekable_bounded_reader() -> None:
    fake = FakeS3Client()
    store = ObjectStore(bucket="b", client=fake)

    store.put_object("raw/x.parquet", b"hello")
    body = store.get_object("raw/x.parquet")

    # get_object returns a seekable file-like (SpooledTemporaryFile) —
    # NOT raw bytes, NOT a non-seekable StreamingBody. The seekability
    # is mandatory because Parquet's footer-at-end format requires
    # random access (PR #46 reviewer convergence).
    assert not isinstance(body, (bytes, bytearray))
    assert hasattr(body, "read")
    assert body.seekable()
    assert body.read() == b"hello"
    # Cursor is at end after read; rewind works.
    body.seek(0)
    assert body.read() == b"hello"
    body.close()


def test_bucket_is_passed_through() -> None:
    fake = FakeS3Client()
    store = ObjectStore(bucket="my-bucket", client=fake)
    store.put_object("k", b"v")
    assert ("my-bucket", "k") in fake.objects


def test_get_object_can_be_used_as_context_manager() -> None:
    """The recommended caller pattern — `with store.get_object(k) as f:` —
    must close the spool deterministically without leaking the on-disk
    tempfile (if it spilled)."""
    fake = FakeS3Client()
    store = ObjectStore(bucket="b", client=fake)
    store.put_object("k", b"the body")

    with store.get_object("k") as body:
        assert body.read() == b"the body"
        assert not body.closed
    assert body.closed


def test_streaming_read_in_chunks_yields_full_content() -> None:
    """A streamed read in fixed-size chunks must reassemble the full object."""
    fake = FakeS3Client()
    store = ObjectStore(bucket="b", client=fake)
    payload = b"the quick brown fox jumps over the lazy dog" * 100
    store.put_object("k", payload)

    with store.get_object("k") as body:
        chunks: list[bytes] = []
        while True:
            chunk = body.read(64)
            if not chunk:
                break
            chunks.append(chunk)
    assert b"".join(chunks) == payload


def test_copy_object_is_server_side() -> None:
    """copy_object must produce the destination without round-tripping bytes."""
    fake = FakeS3Client()
    store = ObjectStore(bucket="b", client=fake)
    store.put_object("src/k", b"payload")

    fake.put_calls.clear()  # baseline AFTER the seed put
    store.copy_object("src/k", "dst/k")

    assert ("b", "src/k", "dst/k") in fake.copy_calls
    with store.get_object("dst/k") as body:
        assert body.read() == b"payload"
    # No put_object should have been triggered by copy_object — that's the
    # whole point: dedup/enrich pass-through must not rehydrate the payload
    # through worker memory.
    assert fake.put_calls == []


def test_rename_object_atomic_via_copy_plus_delete() -> None:
    fake = FakeS3Client()
    store = ObjectStore(bucket="b", client=fake)
    store.put_object("src/k", b"payload")

    store.rename_object("src/k", "dst/k")

    assert ("b", "src/k", "dst/k") in fake.copy_calls
    with store.get_object("dst/k") as body:
        assert body.read() == b"payload"
    # Source key must be gone.
    assert ("b", "src/k") not in fake.objects


def test_small_body_stays_in_memory_below_spool_threshold() -> None:
    """Under the spool threshold, the body lives entirely in memory —
    the SpooledTemporaryFile's `._rolled` flag stays False, so no
    on-disk tempfile is created. Verifies the 8 MiB threshold isn't
    silently bypassed by some pre-allocation path.
    """
    fake = FakeS3Client()
    store = ObjectStore(bucket="b", client=fake)
    payload = b"x" * (SPOOL_MAX_SIZE // 2)
    store.put_object("k", payload)

    with store.get_object("k") as body:
        assert isinstance(body, tempfile.SpooledTemporaryFile)
        # SpooledTemporaryFile exposes a `_rolled` flag — False means
        # still in-memory, True means it spilled to disk.
        assert body._rolled is False
        assert body.read() == payload


def test_large_body_spills_to_disk_above_spool_threshold() -> None:
    """Above the spool threshold, the body must spill to a real
    on-disk tempfile so worker memory stays bounded. Without this,
    the spool degrades to a regular BytesIO and we re-introduce the
    OOM the PR is trying to prevent.
    """
    fake = FakeS3Client()
    store = ObjectStore(bucket="b", client=fake)
    payload = b"x" * (SPOOL_MAX_SIZE + 4096)  # just over threshold
    store.put_object("k", payload)

    with store.get_object("k") as body:
        assert isinstance(body, tempfile.SpooledTemporaryFile)
        assert body._rolled is True
        assert body.read() == payload


def _build_multimb_parquet(target_bytes: int) -> bytes:
    """Build a Parquet payload at least ``target_bytes`` long on disk.

    Parquet compresses repetitive strings aggressively (dictionary
    encoding kicks in even with ``compression="none"``), so the rows
    are filled with random hex content that defeats dictionary
    encoding. Disabled dictionary + ``compression="none"`` yields an
    on-disk size within a factor of 1.1x the raw row bytes, keeping
    the memory-ceiling assertion meaningful.
    """
    import secrets

    schema = pa.schema([pa.field("text", pa.string(), nullable=False)])
    row_bytes = 256
    n_rows = max(1, target_bytes // row_bytes)
    rows = [secrets.token_hex(row_bytes // 2) for _ in range(n_rows)]
    table = pa.table({"text": rows}, schema=schema)
    buf = io.BytesIO()
    pq.write_table(table, buf, compression="none", use_dictionary=False)
    return buf.getvalue()


@pytest.mark.serial
def test_get_object_bounded_memory_for_multimb_parquet() -> None:
    """Memory-ceiling assertion for the spool path (issue #12 acceptance #3).

    Seeds a >=10 MiB synthetic Parquet (well above ``SPOOL_MAX_SIZE``
    so the spool MUST spill) and reads it via ``get_object`` +
    ``pq.read_table(stream)``. Asserts BOTH:

    1. Python-tracked peak allocations (``tracemalloc.get_traced_memory``)
       stay under 1.5x the payload size.
    2. Arrow's C++ memory pool delta
       (``pa.default_memory_pool().bytes_allocated()``) stays under
       1.5x the payload size.

    Why two pools matter (PR #46 review, Sayed)
    --------------------------------------------
    ``tracemalloc`` only sees Python-level allocations. Arrow does most
    of its real work in a C++ arena (``arrow::MemoryPool``) that the
    Python tracer cannot observe. A future regression that swaps
    buffering paths could double Arrow's arena invisibly to a
    tracemalloc-only assertion. Pair the assertions to close that gap.

    Threshold rationale (1.5x ceiling, both pools)
    ----------------------------------------------
    Under the pre-#12 implementation: ``body.read()`` bound a bytes
    payload (1x in Python) + ``pq.read_table(io.BytesIO(payload))``
    arena (~1x in Arrow). The streaming-spool path drops the explicit
    Python ``bytes`` binding — Arrow reads the file-like into its own
    arena, no Python-side full-buffer copy. The peak should sit just
    above 1x in EACH pool independently, well under 2x. 1.5x clears
    both with margin while strictly blocking any regression that
    re-introduces a separate full-size buffer in either pool.

    ``tracemalloc`` is per-snapshot and per-process; ``ru_maxrss`` is
    monotonic high-water and gets charged by unrelated tests in the
    same pytest session, so we avoid it.
    """
    object_size_bytes = 10 * 1024 * 1024  # ~10 MiB — comfortably above SPOOL_MAX_SIZE
    threshold_multiplier = 1.5

    fake = FakeS3Client()
    store = ObjectStore(bucket="b", client=fake)

    payload = _build_multimb_parquet(object_size_bytes)
    store.put_object("raw/big.parquet", payload)
    actual_payload_size = len(payload)
    # Self-validate the fixture: the synthetic Parquet must be large
    # enough to meet the multi-MB requirement (Tomás's review note).
    assert actual_payload_size > 8 * 1024 * 1024, (
        f"synthetic Parquet only produced {actual_payload_size:,} bytes — "
        "below the 8 MiB acceptance floor; bump `object_size_bytes` or the "
        "row_bytes/n_rows factors in _build_multimb_parquet"
    )
    del payload

    arrow_pool = pa.default_memory_pool()
    arrow_baseline = arrow_pool.bytes_allocated()

    tracemalloc.start()
    try:
        tracemalloc.reset_peak()
        with store.get_object("raw/big.parquet") as stream:
            table = pq.read_table(stream)
        # Sanity: assert the read decoded something — without this, a
        # short-circuit in pyarrow could yield an empty-table false-pass.
        assert table.num_rows > 0
        assert table.column_names == ["text"]
        _current, py_peak = tracemalloc.get_traced_memory()
    finally:
        tracemalloc.stop()

    # Arrow's C++ pool reports steady-state allocation; the table is
    # still live, so `bytes_allocated()` reflects the working-set
    # delta the read actually paid for.
    arrow_delta = arrow_pool.bytes_allocated() - arrow_baseline

    ceiling_bytes = actual_payload_size * threshold_multiplier
    assert py_peak < ceiling_bytes, (
        f"python tracemalloc peak {py_peak:,} bytes exceeded ceiling "
        f"{ceiling_bytes:,.0f} ({threshold_multiplier}x of {actual_payload_size:,} payload) — "
        "regression to buffered-bytes behaviour in Python pool suspected"
    )
    assert arrow_delta < ceiling_bytes, (
        f"arrow C++ pool delta {arrow_delta:,} bytes exceeded ceiling "
        f"{ceiling_bytes:,.0f} ({threshold_multiplier}x of {actual_payload_size:,} payload) — "
        "silent Arrow arena regression suspected (not visible to tracemalloc)"
    )


@pytest.mark.serial
def test_spool_path_does_not_double_arrow_arena_vs_direct_bytesio() -> None:
    """Sanity: the spool-backed read shouldn't double Arrow's C++ arena
    against the simplest possible baseline (``pq.read_table`` on a
    direct ``io.BytesIO``). Both should land in roughly the same Arrow
    pool delta — if the spool's path materially worsens Arrow's
    allocation pattern, this catches it.

    Tolerance is 5% (Tomás's review note: 1% was potentially flaky on
    noisy CI; 5% keeps the ratio meaningful while resisting
    pool-fragmentation jitter).
    """
    object_size_bytes = 4 * 1024 * 1024  # 4 MiB — large enough to be measurable
    fake = FakeS3Client()
    store = ObjectStore(bucket="b", client=fake)

    payload = _build_multimb_parquet(object_size_bytes)
    store.put_object("raw/big.parquet", payload)
    arrow_pool = pa.default_memory_pool()

    # --- direct-bytesio baseline ---
    baseline_before = arrow_pool.bytes_allocated()
    bytesio_table = pq.read_table(io.BytesIO(payload))
    assert bytesio_table.num_rows > 0
    bytesio_delta = arrow_pool.bytes_allocated() - baseline_before
    del bytesio_table, payload

    # --- spool path: post-fixup ---
    baseline_spool = arrow_pool.bytes_allocated()
    with store.get_object("raw/big.parquet") as stream:
        spool_table = pq.read_table(stream)
    assert spool_table.num_rows > 0
    spool_delta = arrow_pool.bytes_allocated() - baseline_spool

    tolerance_ratio = 1.05
    assert spool_delta <= bytesio_delta * tolerance_ratio, (
        f"spool path Arrow C++ delta {spool_delta:,} > BytesIO baseline "
        f"{bytesio_delta:,} by more than {tolerance_ratio:.2f}x — "
        "spool wrapper is materially regressing Arrow's allocation pattern"
    )
