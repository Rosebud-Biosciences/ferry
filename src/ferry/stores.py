"""Object stores: obstore S3 (AWS, Ceph RGW, any S3 API) and local directories.

A :class:`Location` pairs a store with the key prefix it is rooted at and the
URI that prefix is shown (and registered) as, so the rest of ferry works in
relative paths only.
"""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any

from obstore.store import LocalStore, ObjectStore, S3Store

from .config import StoreConfig


def normalize_key(rel: str) -> str:
    """A relative path as an object key: forward slashes, no empty or dot parts."""
    parts = [
        part for part in rel.replace("\\", "/").split("/") if part not in ("", ".")
    ]
    if ".." in parts:
        raise ValueError(f"key escapes its prefix: {rel!r}")
    return "/".join(parts)


@dataclass(frozen=True)
class Location:
    store: ObjectStore
    base: str = ""
    uri: str = ""

    def key(self, rel: str) -> str:
        rel = normalize_key(rel)
        if self.base and rel:
            return f"{self.base}/{rel}"
        return self.base or rel

    def uri_for(self, rel: str) -> str:
        rel = normalize_key(rel)
        return f"{self.uri}/{rel}" if rel else self.uri

    def child(self, rel: str) -> Location:
        rel = normalize_key(rel)
        if not rel:
            return self
        return Location(self.store, self.key(rel), self.uri_for(rel))


def _s3(cfg: StoreConfig) -> Location:
    bucket, _, prefix = cfg.url.removeprefix("s3://").partition("/")
    if not bucket:
        raise ValueError(f"no bucket in store url {cfg.url!r}")
    config: dict[str, Any] = {}
    if cfg.region:
        config["region"] = cfg.region
    if cfg.endpoint:
        config["endpoint"] = cfg.endpoint
    if cfg.virtual_hosted_style:
        config["virtual_hosted_style_request"] = True
    if cfg.checksum:
        config["checksum_algorithm"] = cfg.checksum
    config.update(cfg.config)
    client_options: dict[str, Any] = dict(cfg.client_options)
    if cfg.allow_http:
        client_options["allow_http"] = True
    credential_provider = None
    if cfg.profile:
        try:
            import boto3
            from obstore.auth.boto3 import Boto3CredentialProvider
        except ImportError as exc:  # pragma: no cover - depends on the extra
            raise RuntimeError(
                "store profiles need boto3: pip install 'ferry-ingest[aws]'"
            ) from exc
        credential_provider = Boto3CredentialProvider(
            boto3.Session(profile_name=cfg.profile)
        )
    store = S3Store(
        bucket,
        config=config,
        client_options=client_options or None,
        credential_provider=credential_provider,
    )
    base = normalize_key(prefix)
    uri = f"s3://{bucket}/{base}" if base else f"s3://{bucket}"
    return Location(store, base, uri)


def _local(cfg: StoreConfig) -> Location:
    directory = Path(cfg.url.removeprefix("file://")).expanduser()
    directory.mkdir(parents=True, exist_ok=True)
    store = LocalStore(prefix=directory, mkdir=True)
    return Location(store, "", directory.resolve().as_uri())


def open_store(cfg: StoreConfig) -> Location:
    return _s3(cfg) if cfg.is_s3 else _local(cfg)
