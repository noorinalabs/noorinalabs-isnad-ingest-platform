"""S3-compatible object store client (B2 in prod, MinIO in local dev).

Thin wrapper over ``boto3`` that hides endpoint/credential plumbing and
exposes the primitives each worker needs: ``get_object`` (read a batch
from the input stage), ``put_object`` (write results for the next
stage), ``copy_object`` (same-bucket pass-through without rehydrating
bytes through the worker process), and ``rename_object`` (atomic
.part → final). Paths follow the layout documented in issue #105:

::

    raw/{source}/{YYYY-MM-DD}/{filename}
    dedup/{source}/{batch-id}/{filename}
    enriched/{source}/{batch-id}/{filename}
    normalized/{batch-id}/{filename}
    staged/{batch-id}/{filename}

Bounded-memory reads (#12, PR #46 fixup)
----------------------------------------
``get_object`` spools the response body into a
``tempfile.SpooledTemporaryFile`` and returns the seekable file-like.
The spool keeps the body in-memory while it's under ``SPOOL_MAX_SIZE``
and transparently spills to a real on-disk tempfile above that
threshold — so worker memory is bounded regardless of batch size while
Parquet's footer-at-end format still gets the random access it needs.

Why a spool and not a raw ``StreamingBody``: real botocore
``StreamingBody`` inherits ``io.IOBase`` defaults (``seekable() ->
False``, ``seek()`` raises ``UnsupportedOperation``). pyarrow's
``read_table`` / ``ParquetFile`` always seek to the Parquet footer
before reading row groups, so a raw non-seekable body hard-fails on
the first batch. The PR #46 first cut returned the body directly and
the test fake hid the failure by exposing seek; the spool fix is the
prod-faithful path.

Callers MUST treat the returned object as a context manager
(``with store.get_object(key) as f: ...``) so the spool's disk file
(if it spilled) is properly closed.

In tests, the :class:`ObjectStore` can be constructed with a fake
``client`` to stub out network I/O — see ``tests/workers/conftest.py``.
"""

from __future__ import annotations

import shutil
import tempfile
from typing import IO, Any

__all__ = ["ObjectStore", "SPOOL_MAX_SIZE"]

# Max in-memory bytes before SpooledTemporaryFile spills to disk. Set so
# the realistic small-Parquet path (sub-batch manifests, side-tables) stays
# fully in-memory while the multi-GB hadith-batch Parquets always spill —
# the pipeline's pointer-message contract permits batches well beyond this,
# and disk-backed reads are still streaming-fast on the SSD-class storage
# the worker containers run on.
SPOOL_MAX_SIZE = 8 * 1024 * 1024  # 8 MiB

# Chunk size for the spool-fill copy. 64 KiB matches the page-cache friendly
# I/O size and is the default ``shutil.copyfileobj`` would use minus a
# factor — explicit so the budget is reviewable.
_SPOOL_CHUNK_SIZE = 64 * 1024


class ObjectStore:
    """S3-compatible object store wrapper."""

    def __init__(
        self, bucket: str, client: Any | None = None, endpoint_url: str | None = None
    ) -> None:
        self.bucket = bucket
        self.endpoint_url = endpoint_url
        self._client = client

    @property
    def client(self) -> Any:
        """Lazily construct a boto3 S3 client if one wasn't injected."""
        if self._client is None:
            import boto3  # type: ignore[import-untyped]

            kwargs: dict[str, Any] = {}
            if self.endpoint_url:
                kwargs["endpoint_url"] = self.endpoint_url
            self._client = boto3.client("s3", **kwargs)
        return self._client

    def get_object(self, key: str) -> IO[bytes]:
        """Fetch a bounded-memory seekable reader for ``key``.

        Returns a ``tempfile.SpooledTemporaryFile`` pre-filled from the
        S3/B2 body and rewound to offset 0. The spool keeps the body in
        memory while it stays under :data:`SPOOL_MAX_SIZE` (8 MiB) and
        spills to disk above that — so worker memory is bounded
        regardless of batch size while pyarrow's seek-to-footer still
        works.

        Callers SHOULD use this as a context manager so the on-disk
        spool (if it spilled) is properly closed and unlinked::

            with store.get_object(key) as stream:
                table = pq.read_table(stream)
        """
        response = self.client.get_object(Bucket=self.bucket, Key=key)
        body = response["Body"]
        spool = tempfile.SpooledTemporaryFile(max_size=SPOOL_MAX_SIZE)
        try:
            shutil.copyfileobj(body, spool, length=_SPOOL_CHUNK_SIZE)
            spool.seek(0)
        except BaseException:
            spool.close()
            raise
        finally:
            close = getattr(body, "close", None)
            if callable(close):
                close()
        return spool

    def put_object(
        self, key: str, data: bytes, content_type: str = "application/octet-stream"
    ) -> None:
        """Upload ``data`` to ``key`` in the configured bucket."""
        self.client.put_object(Bucket=self.bucket, Key=key, Body=data, ContentType=content_type)

    def copy_object(self, src_key: str, dst_key: str) -> None:
        """Server-side copy ``src_key`` to ``dst_key`` within the bucket.

        Used by dedup/enrich pass-through (the input Parquet is re-emitted
        unchanged under the next stage's prefix so downstream stages have
        a payload to consume). Server-side ``CopyObject`` avoids
        round-tripping the bytes through the worker — important now that
        ``get_object`` is a stream and we don't have a buffered copy in
        memory to ``put_object`` back. ``CopyObject`` is atomic from the
        reader's perspective.
        """
        self.client.copy_object(
            Bucket=self.bucket,
            Key=dst_key,
            CopySource={"Bucket": self.bucket, "Key": src_key},
        )

    def rename_object(self, src_key: str, dst_key: str) -> None:
        """Atomically rename ``src_key`` to ``dst_key`` within the bucket.

        Implemented as server-side copy + delete: ``CopyObject`` is atomic
        from the reader's perspective (the destination either exists fully
        or not at all), and the subsequent delete reclaims the source key.
        Used by normalize (#192 D-ii) to stage per-label Parquets as
        ``.part`` files and promote them once the write succeeds.
        """
        self.copy_object(src_key, dst_key)
        self.client.delete_object(Bucket=self.bucket, Key=src_key)
