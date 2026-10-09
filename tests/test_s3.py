"""Against a real S3 API: moto in-process by default, or ``FERRY_TEST_S3_ENDPOINT``
(CI points it at SeaweedFS) with ``FERRY_TEST_S3_KEY`` / ``FERRY_TEST_S3_SECRET``."""

from __future__ import annotations

import hashlib
import json
import os
import socket
import uuid
from collections.abc import Iterator

import boto3
import pytest

from ferry.config import StoreConfig
from ferry.runner import Agent
from ferry.stores import open_store
from ferry.transfer import UploadSettings, upload_file

from .helpers import age_tree, calls, write

KEY = os.environ.get("FERRY_TEST_S3_KEY", "testing")
SECRET = os.environ.get("FERRY_TEST_S3_SECRET", "testing")


@pytest.fixture(scope="module")
def s3_endpoint() -> Iterator[str]:
    external = os.environ.get("FERRY_TEST_S3_ENDPOINT")
    if external:
        yield external
        return
    from moto.server import ThreadedMotoServer

    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        port = sock.getsockname()[1]
    server = ThreadedMotoServer(ip_address="127.0.0.1", port=port, verbose=False)
    server.start()
    try:
        yield f"http://127.0.0.1:{port}"
    finally:
        server.stop()


@pytest.fixture
def bucket(s3_endpoint: str, monkeypatch: pytest.MonkeyPatch) -> str:
    for name in (
        "AWS_PROFILE",
        "AWS_SESSION_TOKEN",
        "AWS_ENDPOINT_URL",
        "AWS_ENDPOINT_URL_S3",
    ):
        monkeypatch.delenv(name, raising=False)
    name = f"ferry-{uuid.uuid4().hex[:10]}"
    boto3.client(
        "s3",
        endpoint_url=s3_endpoint,
        region_name="us-east-1",
        aws_access_key_id=KEY,
        aws_secret_access_key=SECRET,
    ).create_bucket(Bucket=name)
    return name


def store_config(endpoint: str, bucket: str, prefix: str = "") -> dict:
    return {
        "url": f"s3://{bucket}/{prefix}".rstrip("/"),
        "endpoint": endpoint,
        "region": "us-east-1",
        "allow_http": True,
        "config": {"access_key_id": KEY, "secret_access_key": SECRET},
    }


async def test_single_put_and_multipart_round_trip_with_path_style(
    s3_endpoint, bucket, tmp_path
):
    location = open_store(StoreConfig(**store_config(s3_endpoint, bucket, "raw")))
    settings = UploadSettings(
        single_put_max=5 * 1024**2, part_size=5 * 1024**2, part_concurrency=2
    )
    small = write(tmp_path / "small.bin", os.urandom(1000))
    big = write(tmp_path / "big.bin", os.urandom(12 * 1024**2))

    results = {
        "u/small.bin": await upload_file(
            small, location, "u/small.bin", settings=settings
        ),
        "u/big.bin": await upload_file(big, location, "u/big.bin", settings=settings),
    }

    assert {rel: r.multipart for rel, r in results.items()} == {
        "u/small.bin": False,
        "u/big.bin": True,
    }
    for rel, path in {"u/small.bin": small, "u/big.bin": big}.items():
        got = await location.store.get_async(location.key(rel))
        body = bytes(await got.bytes_async())
        assert body == path.read_bytes()
        assert results[rel].sha256 == hashlib.sha256(body).hexdigest()
    assert location.uri_for("u/big.bin") == f"s3://{bucket}/raw/u/big.bin"


async def test_a_pass_registers_from_s3_and_a_fresh_journal_adopts(
    s3_endpoint, bucket, make_config, paths, clock, recorder, tmp_path
):
    stores = {"s3": store_config(s3_endpoint, bucket)}
    registrars = [
        "manifest",
        {"use": "tests.helpers:Recorder", "options": {"name": recorder}},
    ]
    cfg = make_config(
        config={"stores": stores}, store="s3", prefix="raw", registrars=registrars
    )
    write(paths["src"] / "u__1" / "a.tif", b"alpha")
    write(paths["src"] / "u__1" / "sub" / "b.tif", b"bravo")
    age_tree(paths["src"])

    with Agent(cfg, clock=clock) as agent:
        (report,) = await agent.run_once()
        location = agent.stores["s3"].child("raw")
        got = await location.store.get_async(location.key("u__1/_ferry_manifest.json"))
        manifest = json.loads(bytes(await got.bytes_async()))

    assert report.uploaded == 2
    (registered,) = calls(recorder, "register")
    assert registered.uri == f"s3://{bucket}/raw/u__1"
    assert {f["path"]: f["sha256"] for f in manifest["files"]} == {
        "a.tif": hashlib.sha256(b"alpha").hexdigest(),
        "sub/b.tif": hashlib.sha256(b"bravo").hexdigest(),
    }

    fresh = make_config(
        config={"stores": stores, "state_dir": str(tmp_path / "fresh-state")},
        store="s3",
        prefix="raw",
        registrars=registrars,
    )
    with Agent(fresh, clock=clock) as agent:
        (report,) = await agent.run_once()
    assert (report.adopted, report.uploaded) == (2, 0)
