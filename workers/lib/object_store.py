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

Streaming reads (#12)
---------------------
``get_object`` returns the underlying response body — botocore's
``StreamingBody`` in prod, a ``.read()``-compatible fake in tests — so
consumers feed it directly to ``pyarrow.parquet.read_table`` /
``ParquetFile`` instead of buffering the entire Parquet (multi-GB in
realistic pipeline batches) into worker memory. Callers that genuinely
need the bytes (e.g. ``json.loads`` over a small manifest) call
``.read()`` on the returned stream explicitly.

In tests, the :class:`ObjectStore` can be constructed with a fake
``client`` to stub out network I/O — see ``tests/workers/conftest.py``.
"""

from __future__ import annotations

from typing import Any

__all__ = ["ObjectStore"]


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

    def get_object(self, key: str) -> Any:
        """Fetch a streaming reader for ``key`` from the configured bucket.

        Returns the response body directly — botocore ``StreamingBody`` in
        production. The returned object exposes ``.read([amt])`` and is
        accepted by ``pyarrow.parquet.read_table`` / ``ParquetFile`` as a
        file-like input, so Parquet consumers iterate row groups without
        materializing the full object in worker memory. Callers that need
        the bytes (small JSON manifests, JSON-encoded props blobs) call
        ``.read()`` on the returned stream once and operate on the bytes.
        """
        response = self.client.get_object(Bucket=self.bucket, Key=key)
        return response["Body"]

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
