"""Immutable local and S3-compatible object publication."""

from __future__ import annotations

import base64
import hashlib
import io
import json

import pytest
from botocore.exceptions import ClientError
from src.object_store import (
    ImmutableObjectConflict,
    LocalObjectStore,
    ObjectIntegrityError,
    S3ObjectStore,
    TransientObjectStoreError,
    content_addressed_key,
)


def test_local_store_is_immutable_idempotent_and_hash_verified(tmp_path) -> None:
    store = LocalObjectStore(tmp_path)
    payload = b"governed analytics"
    key = content_addressed_key(payload)

    first = store.put_immutable(key, payload, content_type="text/plain")
    second = store.put_immutable(key, payload, content_type="text/plain")

    assert first == second
    assert first.sha256 == hashlib.sha256(payload).hexdigest()
    assert store.read_verified(key, first.sha256) == payload
    with pytest.raises(ImmutableObjectConflict):
        store.put_immutable(key, b"different", content_type="text/plain")

    (tmp_path / key).write_bytes(b"tampered")
    with pytest.raises(ObjectIntegrityError):
        store.read_verified(key, first.sha256)


def test_local_pointer_promotion_is_atomic_and_leaves_no_temporary_files(tmp_path) -> None:
    store = LocalObjectStore(tmp_path)

    store.promote_pointer("pointers/latest.json", {"run_id": "run-1"})
    store.promote_pointer("pointers/latest.json", {"run_id": "run-2"})

    assert json.loads((tmp_path / "pointers/latest.json").read_text()) == {"run_id": "run-2"}
    assert not list(tmp_path.rglob("*.tmp"))


@pytest.mark.parametrize(
    "key",
    ["../secret", "/absolute", "objects/../../secret", "", "objects\\value", "objects//value"],
)
def test_local_store_rejects_unsafe_keys(tmp_path, key: str) -> None:
    store = LocalObjectStore(tmp_path)

    with pytest.raises(ValueError):
        store.put_immutable(key, b"x", content_type="application/octet-stream")


class _FakeS3Client:
    def __init__(self) -> None:
        self.calls: list[dict] = []
        self.objects: dict[tuple[str, str], bytes] = {}

    def put_object(self, **kwargs):
        self.calls.append(kwargs)
        identity = (kwargs["Bucket"], kwargs["Key"])
        if kwargs.get("IfNoneMatch") == "*" and identity in self.objects:
            raise AssertionError("test should not overwrite immutable S3 objects")
        self.objects[identity] = kwargs["Body"]
        return {"ETag": '"abc"', "VersionId": "v1"}

    def get_object(self, **kwargs):
        return {"Body": io.BytesIO(self.objects[(kwargs["Bucket"], kwargs["Key"])])}


def test_s3_store_requires_bucket() -> None:
    with pytest.raises(ValueError, match="bucket must not be empty"):
        S3ObjectStore(
            bucket=" ",
            prefix="runs",
            client=_FakeS3Client(),
            server_side_encryption="AES256",
        )


def test_s3_store_sends_checksum_metadata_encryption_and_conditional_write() -> None:
    client = _FakeS3Client()
    store = S3ObjectStore(
        bucket="analytics-prod",
        prefix="inventory/runs",
        client=client,
        server_side_encryption="AES256",
    )
    payload = b"parquet-bytes"
    key = content_addressed_key(payload)

    descriptor = store.put_immutable(key, payload, content_type="application/vnd.apache.parquet")
    request = client.calls[0]

    assert request["Bucket"] == "analytics-prod"
    assert request["Key"] == f"inventory/runs/{key}"
    assert request["IfNoneMatch"] == "*"
    assert request["ServerSideEncryption"] == "AES256"
    assert request["Metadata"]["sha256"] == descriptor.sha256
    assert request["ChecksumSHA256"] == base64.b64encode(hashlib.sha256(payload).digest()).decode(
        "ascii"
    )
    assert store.read_verified(key, descriptor.sha256) == payload

    client.objects[("analytics-prod", f"inventory/runs/{key}")] = b"tampered"
    with pytest.raises(ObjectIntegrityError, match="Object hash mismatch"):
        store.read_verified(key, descriptor.sha256)

    store.promote_pointer("pointers/latest.json", {"run_id": "run-1"})
    assert "IfNoneMatch" not in client.calls[-1]


def test_s3_store_classifies_service_throttling_as_transient() -> None:
    class ThrottledClient(_FakeS3Client):
        def put_object(self, **kwargs):
            raise ClientError(
                {"Error": {"Code": "SlowDown", "Message": "retry later"}},
                "PutObject",
            )

    store = S3ObjectStore(
        bucket="analytics-prod",
        prefix="inventory/runs",
        client=ThrottledClient(),
        server_side_encryption="AES256",
    )

    with pytest.raises(TransientObjectStoreError, match="SlowDown"):
        store.put_immutable("objects/value", b"value", content_type="text/plain")


def test_s3_immutable_precondition_is_idempotent_or_conflicting() -> None:
    class ExistingClient(_FakeS3Client):
        def put_object(self, **kwargs):
            if kwargs.get("IfNoneMatch") == "*":
                raise ClientError(
                    {"Error": {"Code": "PreconditionFailed", "Message": "exists"}},
                    "PutObject",
                )
            return super().put_object(**kwargs)

    client = ExistingClient()
    payload = b"existing"
    key = content_addressed_key(payload)
    client.objects[("analytics-prod", f"runs/{key}")] = payload
    store = S3ObjectStore(
        bucket="analytics-prod",
        prefix="runs",
        client=client,
        server_side_encryption="AES256",
    )

    descriptor = store.put_immutable(key, payload, content_type="text/plain")
    assert descriptor.sha256 == hashlib.sha256(payload).hexdigest()

    client.objects[("analytics-prod", f"runs/{key}")] = b"different"
    with pytest.raises(ImmutableObjectConflict, match="already differs"):
        store.put_immutable(key, payload, content_type="text/plain")
