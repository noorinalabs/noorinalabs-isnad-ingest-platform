"""S3-compatible object store client (B2 in prod, MinIO in local dev).

Thin wrapper over ``boto3`` that hides endpoint/credential plumbing and
exposes two primitives each worker needs: ``get_object`` (read a batch
from the input stage) and ``put_object`` (write results for the next
stage). Paths follow the layout documented in issue #105:

::

    raw/{source}/{YYYY-MM-DD}/{filename}
    dedup/{source}/{batch-id}/{filename}
    enriched/{source}/{batch-id}/{filename}
    normalized/{batch-id}/{filename}
    staged/{batch-id}/{filename}

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

    def get_object(self, key: str) -> bytes:
        """Fetch object bytes at ``key`` from the configured bucket."""
        response = self.client.get_object(Bucket=self.bucket, Key=key)
        body = response["Body"]
        # TODO: stream large Parquet files instead of buffering entirely in memory.
        return body.read() if hasattr(body, "read") else bytes(body)

    def put_object(
        self, key: str, data: bytes, content_type: str = "application/octet-stream"
    ) -> None:
        """Upload ``data`` to ``key`` in the configured bucket."""
        self.client.put_object(Bucket=self.bucket, Key=key, Body=data, ContentType=content_type)

    def rename_object(self, src_key: str, dst_key: str) -> None:
        """Atomically rename ``src_key`` to ``dst_key`` within the bucket.

        Implemented as server-side copy + delete: ``CopyObject`` is atomic
        from the reader's perspective (the destination either exists fully
        or not at all), and the subsequent delete reclaims the source key.
        Used by normalize (#192 D-ii) to stage per-label Parquets as
        ``.part`` files and promote them once the write succeeds.
        """
        self.client.copy_object(
            Bucket=self.bucket,
            Key=dst_key,
            CopySource={"Bucket": self.bucket, "Key": src_key},
        )
        self.client.delete_object(Bucket=self.bucket, Key=src_key)
