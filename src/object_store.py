"""Immutable content-addressed publication for local and S3 object stores."""

from __future__ import annotations

import base64
import hashlib
import json
import os
import tempfile
from dataclasses import dataclass
from pathlib import Path, PurePosixPath
from typing import Any, Protocol

from botocore.exceptions import (
    ClientError,
    ConnectionClosedError,
    EndpointConnectionError,
    ReadTimeoutError,
)


class ImmutableObjectConflict(RuntimeError):
    """Raised when an immutable key already contains different bytes."""


class ObjectIntegrityError(RuntimeError):
    """Raised when retrieved bytes do not match the governed SHA-256."""


class TransientObjectStoreError(RuntimeError):
    """Raised for retryable object-store transport or service failures."""


_TRANSIENT_CLIENT_CODES = {
    "500",
    "503",
    "InternalError",
    "RequestTimeout",
    "RequestTimeoutException",
    "ServiceUnavailable",
    "SlowDown",
    "Throttling",
    "ThrottlingException",
}
_TRANSIENT_TRANSPORT_ERRORS = (
    ConnectionClosedError,
    EndpointConnectionError,
    ReadTimeoutError,
)


def _raise_if_transient(exc: ClientError) -> None:
    code = str(exc.response.get("Error", {}).get("Code", "unknown"))
    if code in _TRANSIENT_CLIENT_CODES:
        raise TransientObjectStoreError(f"Transient object-store error {code}") from exc


@dataclass(frozen=True)
class ObjectDescriptor:
    backend: str
    key: str
    sha256: str
    size_bytes: int
    content_type: str
    etag: str | None = None
    version_id: str | None = None


class ObjectStore(Protocol):
    def put_immutable(self, key: str, payload: bytes, *, content_type: str) -> ObjectDescriptor: ...

    def read_verified(self, key: str, expected_sha256: str) -> bytes: ...

    def promote_pointer(self, key: str, payload: dict[str, Any]) -> ObjectDescriptor: ...


class _S3Client(Protocol):
    def put_object(self, **kwargs: Any) -> dict[str, Any]: ...

    def get_object(self, **kwargs: Any) -> dict[str, Any]: ...


def _validate_key(key: str) -> str:
    if not key or "\\" in key:
        raise ValueError("Object key must be a non-empty POSIX path")
    path = PurePosixPath(key)
    if path.is_absolute() or any(part in {"", ".", ".."} for part in path.parts):
        raise ValueError(f"Unsafe object key: {key!r}")
    normalized = path.as_posix()
    if normalized != key:
        raise ValueError(f"Object key is not normalized: {key!r}")
    return normalized


def _sha256(payload: bytes) -> str:
    return hashlib.sha256(payload).hexdigest()


def content_addressed_key(payload: bytes) -> str:
    digest = _sha256(payload)
    return f"objects/sha256/{digest}"


class LocalObjectStore:
    def __init__(self, root: Path) -> None:
        self.root = root.expanduser().resolve()

    def _path(self, key: str) -> Path:
        normalized = _validate_key(key)
        target = (self.root / normalized).resolve()
        if self.root not in target.parents:
            raise ValueError(f"Object key escapes store root: {key!r}")
        return target

    @staticmethod
    def _atomic_write(path: Path, payload: bytes) -> None:
        path.parent.mkdir(parents=True, exist_ok=True)
        temporary_path: Path | None = None
        try:
            with tempfile.NamedTemporaryFile(
                mode="wb",
                dir=path.parent,
                prefix=f".{path.name}.",
                suffix=".tmp",
                delete=False,
            ) as handle:
                temporary_path = Path(handle.name)
                handle.write(payload)
                handle.flush()
                os.fsync(handle.fileno())
            os.replace(temporary_path, path)
            temporary_path = None
        finally:
            if temporary_path is not None:
                temporary_path.unlink(missing_ok=True)

    def put_immutable(self, key: str, payload: bytes, *, content_type: str) -> ObjectDescriptor:
        path = self._path(key)
        digest = _sha256(payload)
        if path.exists():
            existing = path.read_bytes()
            if existing != payload:
                raise ImmutableObjectConflict(f"Immutable object already differs: {key}")
        else:
            self._atomic_write(path, payload)
        return ObjectDescriptor(
            backend="local",
            key=key,
            sha256=digest,
            size_bytes=len(payload),
            content_type=content_type,
        )

    def read_verified(self, key: str, expected_sha256: str) -> bytes:
        payload = self._path(key).read_bytes()
        actual = _sha256(payload)
        if actual != expected_sha256:
            raise ObjectIntegrityError(
                f"Object hash mismatch for {key}: expected {expected_sha256}, observed {actual}"
            )
        return payload

    def promote_pointer(self, key: str, payload: dict[str, Any]) -> ObjectDescriptor:
        path = self._path(key)
        body = (json.dumps(payload, sort_keys=True, separators=(",", ":")) + "\n").encode()
        self._atomic_write(path, body)
        return ObjectDescriptor(
            backend="local",
            key=key,
            sha256=_sha256(body),
            size_bytes=len(body),
            content_type="application/json",
        )


class S3ObjectStore:
    def __init__(
        self,
        *,
        bucket: str,
        prefix: str,
        client: _S3Client,
        server_side_encryption: str,
    ) -> None:
        if not bucket.strip():
            raise ValueError("S3 bucket must not be empty")
        self.bucket = bucket
        self.prefix = prefix.strip("/")
        self.client = client
        self.server_side_encryption = server_side_encryption

    def _full_key(self, key: str) -> str:
        normalized = _validate_key(key)
        return f"{self.prefix}/{normalized}" if self.prefix else normalized

    def put_immutable(self, key: str, payload: bytes, *, content_type: str) -> ObjectDescriptor:
        digest = _sha256(payload)
        full_key = self._full_key(key)
        request = {
            "Bucket": self.bucket,
            "Key": full_key,
            "Body": payload,
            "ContentType": content_type,
            "Metadata": {"sha256": digest},
            "ChecksumSHA256": base64.b64encode(hashlib.sha256(payload).digest()).decode("ascii"),
            "ServerSideEncryption": self.server_side_encryption,
            "IfNoneMatch": "*",
        }
        try:
            response = self.client.put_object(**request)
        except ClientError as exc:
            error = exc.response.get("Error", {})
            _raise_if_transient(exc)
            if error.get("Code") not in {"PreconditionFailed", "412"}:
                raise
            try:
                self.read_verified(key, digest)
            except ObjectIntegrityError as integrity_error:
                raise ImmutableObjectConflict(
                    f"Immutable S3 object already differs: {full_key}"
                ) from integrity_error
            response = {}
        except _TRANSIENT_TRANSPORT_ERRORS as exc:
            raise TransientObjectStoreError(
                f"Transient object-store transport failure: {type(exc).__name__}"
            ) from exc
        return ObjectDescriptor(
            backend="s3",
            key=key,
            sha256=digest,
            size_bytes=len(payload),
            content_type=content_type,
            etag=response.get("ETag"),
            version_id=response.get("VersionId"),
        )

    def read_verified(self, key: str, expected_sha256: str) -> bytes:
        try:
            response = self.client.get_object(Bucket=self.bucket, Key=self._full_key(key))
        except ClientError as exc:
            _raise_if_transient(exc)
            raise
        except _TRANSIENT_TRANSPORT_ERRORS as exc:
            raise TransientObjectStoreError(
                f"Transient object-store transport failure: {type(exc).__name__}"
            ) from exc
        payload = response["Body"].read()
        actual = _sha256(payload)
        if actual != expected_sha256:
            raise ObjectIntegrityError(
                f"Object hash mismatch for {key}: expected {expected_sha256}, observed {actual}"
            )
        return payload

    def promote_pointer(self, key: str, payload: dict[str, Any]) -> ObjectDescriptor:
        body = (json.dumps(payload, sort_keys=True, separators=(",", ":")) + "\n").encode()
        try:
            response = self.client.put_object(
                Bucket=self.bucket,
                Key=self._full_key(key),
                Body=body,
                ContentType="application/json",
                Metadata={"sha256": _sha256(body)},
                ChecksumSHA256=base64.b64encode(hashlib.sha256(body).digest()).decode("ascii"),
                ServerSideEncryption=self.server_side_encryption,
            )
        except ClientError as exc:
            _raise_if_transient(exc)
            raise
        except _TRANSIENT_TRANSPORT_ERRORS as exc:
            raise TransientObjectStoreError(
                f"Transient object-store transport failure: {type(exc).__name__}"
            ) from exc
        return ObjectDescriptor(
            backend="s3",
            key=key,
            sha256=_sha256(body),
            size_bytes=len(body),
            content_type="application/json",
            etag=response.get("ETag"),
            version_id=response.get("VersionId"),
        )
