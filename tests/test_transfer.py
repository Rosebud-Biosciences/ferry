from __future__ import annotations

import hashlib
import os
from datetime import datetime
from pathlib import Path
from typing import Any, cast

import pytest
from obstore.store import LocalStore

from ferry.stores import Location
from ferry.transfer import (
    FileChangedError,
    RateLimiter,
    UploadSettings,
    VerifyError,
    move_file,
    prune_empty_dirs,
    upload_file,
)

from .helpers import write

SETTINGS = UploadSettings(single_put_max=1024, part_size=64 * 1024, part_concurrency=2)


class SpyStore:
    """Wraps a store: records puts and can meddle mid-upload or misreport size."""

    def __init__(self, inner: LocalStore, *, during_put=None, head_size=None) -> None:
        self.inner = inner
        self.puts: list[dict[str, Any]] = []
        self.during_put = during_put
        self.head_size = head_size

    async def put_async(self, key, data, **kwargs):
        self.puts.append(kwargs)
        if self.during_put is not None:
            self.during_put()
        return await self.inner.put_async(key, data, **kwargs)

    async def head_async(self, key):
        meta = await self.inner.head_async(key)
        return meta if self.head_size is None else {**meta, "size": self.head_size}


def spy(tmp_path: Path, **kwargs) -> tuple[Location, SpyStore]:
    store = SpyStore(LocalStore(prefix=tmp_path / "remote", mkdir=True), **kwargs)
    return Location(cast(Any, store), "pre", "file:///remote/pre"), store


async def test_small_files_go_up_in_one_put(tmp_path):
    body = os.urandom(100)
    path = write(tmp_path / "a.bin", body)
    location, store = spy(tmp_path)

    result = await upload_file(path, location, "u/a.bin", settings=SETTINGS)

    assert [put["use_multipart"] for put in store.puts] == [False]
    assert (tmp_path / "remote" / "pre" / "u" / "a.bin").read_bytes() == body
    assert result.sha256 == hashlib.sha256(body).hexdigest()
    assert (result.size, result.multipart) == (100, False)


async def test_large_files_stream_as_multipart_with_identical_bytes(tmp_path):
    body = os.urandom(300 * 1024)
    path = write(tmp_path / "big.bin", body)
    location, store = spy(tmp_path)

    result = await upload_file(path, location, "u/big.bin", settings=SETTINGS)

    (put,) = store.puts
    assert (put["use_multipart"], put["chunk_size"]) == (True, 64 * 1024)
    assert (tmp_path / "remote" / "pre" / "u" / "big.bin").read_bytes() == body
    assert result.sha256 == hashlib.sha256(body).hexdigest()
    assert (result.size, result.multipart) == (len(body), True)


async def test_a_file_that_changes_mid_upload_is_rejected(tmp_path):
    path = write(tmp_path / "a.bin", b"first")
    location, _ = spy(tmp_path, during_put=lambda: write(path, b"second!"))

    with pytest.raises(FileChangedError, match="while it was uploading"):
        await upload_file(path, location, "a.bin", settings=SETTINGS)


async def test_a_stale_scan_is_rejected_before_anything_is_read(tmp_path):
    path = write(tmp_path / "a.bin", b"now")
    location, store = spy(tmp_path)

    with pytest.raises(FileChangedError, match="since it was scanned"):
        await upload_file(path, location, "a.bin", settings=SETTINGS, expect=(1, 1))
    assert store.puts == []


async def test_a_short_object_in_the_store_is_caught(tmp_path):
    path = write(tmp_path / "a.bin", b"payload")
    location, _ = spy(tmp_path, head_size=3)

    with pytest.raises(VerifyError, match="holds 3 bytes; sent 7"):
        await upload_file(path, location, "a.bin", settings=SETTINGS)


async def test_the_rate_limiter_paces_to_its_rate():
    now = [0.0]
    slept: list[float] = []

    async def sleep(seconds: float) -> None:
        slept.append(seconds)
        now[0] += seconds

    limiter = RateLimiter([(0, 1440, 100.0)], clock=lambda: now[0], sleep=sleep)
    await limiter.acquire(100)  # the bucket starts empty: a second's worth
    await limiter.acquire(50)
    now[0] += 10  # idle time refills the bucket, up to one second's worth
    await limiter.acquire(100)
    await limiter.acquire(100)

    assert slept == [1.0, 0.5, 1.0]


@pytest.mark.parametrize(
    ("hour", "rate"), [(9, 10.0), (23, 5.0), (3, 5.0), (19, None), (6, None)]
)
def test_rate_windows_apply_only_inside_them(hour, rate):
    windows = [(8 * 60, 18 * 60, 10.0), (22 * 60, 6 * 60, 5.0)]  # 08-18; 22-06
    limiter = RateLimiter(windows, local_now=lambda: datetime(2026, 1, 1, hour, 0))
    assert limiter.rate() == rate


def test_a_move_within_a_file_system_is_a_rename(tmp_path):
    src = write(tmp_path / "a" / "x.bin", b"payload")
    dst = tmp_path / "b" / "deep" / "x.bin"

    assert move_file(src, dst) == 7
    assert dst.read_bytes() == b"payload"
    assert not src.exists()


def test_a_move_across_file_systems_copies_verifies_then_deletes(tmp_path, monkeypatch):
    monkeypatch.setattr("ferry.transfer._same_device", lambda *_: False)
    src = write(tmp_path / "a" / "x.bin", b"payload", age=600)
    mtime = src.stat().st_mtime_ns
    dst = tmp_path / "b" / "x.bin"

    assert move_file(src, dst) == 7
    assert dst.read_bytes() == b"payload"
    assert dst.stat().st_mtime_ns == mtime  # copy2 keeps the timestamps
    assert not src.exists()
    assert list(dst.parent.iterdir()) == [dst]


def test_a_short_copy_keeps_the_source(tmp_path, monkeypatch):
    monkeypatch.setattr("ferry.transfer._same_device", lambda *_: False)
    monkeypatch.setattr(
        "ferry.transfer.shutil.copy2", lambda src, dst: Path(dst).write_bytes(b"pay")
    )
    src = write(tmp_path / "a" / "x.bin", b"payload")
    dst = tmp_path / "b" / "x.bin"

    with pytest.raises(VerifyError, match="is 3 bytes, expected 7"):
        move_file(src, dst)
    assert src.read_bytes() == b"payload"
    assert list(dst.parent.iterdir()) == []


def test_pruning_removes_only_empty_directories(tmp_path):
    (tmp_path / "u" / "a" / "b").mkdir(parents=True)
    write(tmp_path / "u" / "keep" / "f.txt")

    prune_empty_dirs(tmp_path / "u")

    assert not (tmp_path / "u" / "a").exists()
    assert (tmp_path / "u" / "keep" / "f.txt").exists()
