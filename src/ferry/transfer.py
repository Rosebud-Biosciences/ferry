"""Moving bytes: uploads to an object store, and moves to a backup drive."""

from __future__ import annotations

import asyncio
import contextlib
import hashlib
import os
import shutil
import time
from collections.abc import AsyncGenerator, Awaitable, Callable, Mapping
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import TYPE_CHECKING, BinaryIO

from .config import LimitsConfig, parse_window

if TYPE_CHECKING:
    from hashlib import _Hash

    from .stores import Location


class FileChangedError(Exception):
    """The file changed while it was read, so what was sent is not what is there."""


class VerifyError(Exception):
    """A copy does not match what was sent."""


@dataclass(frozen=True)
class UploadSettings:
    single_put_max: int
    part_size: int
    part_concurrency: int

    @classmethod
    def from_limits(cls, limits: LimitsConfig) -> UploadSettings:
        return cls(
            single_put_max=int(limits.single_put_max),
            part_size=int(limits.part_size),
            part_concurrency=limits.part_concurrency,
        )


@dataclass(frozen=True)
class UploadResult:
    size: int
    mtime_ns: int
    sha256: str
    etag: str | None
    duration_ms: int
    multipart: bool


class RateLimiter:
    """A token bucket over bytes per second, optionally only in local time windows.

    Shared by every upload of a source, so the cap is on the source's total rate.
    A chunk bigger than one second's allowance is let through and paid for by
    sleeping afterwards, so a part never has to be split to fit the bucket.
    """

    def __init__(
        self,
        windows: list[tuple[int, int, float]],
        *,
        clock: Callable[[], float] = time.monotonic,
        local_now: Callable[[], datetime] = datetime.now,
        sleep: Callable[[float], Awaitable[object]] = asyncio.sleep,
    ) -> None:
        self._windows = windows
        self._clock = clock
        self._local_now = local_now
        self._sleep = sleep
        self._lock = asyncio.Lock()
        self._tokens = 0.0
        self._stamp = clock()

    @classmethod
    def from_config(
        cls, bandwidth: int | Mapping[str, int] | None
    ) -> RateLimiter | None:
        if bandwidth is None:
            return None
        if isinstance(bandwidth, Mapping):
            windows = [
                (*parse_window(window), float(rate))
                for window, rate in bandwidth.items()
            ]
        else:
            windows = [(0, 24 * 60, float(bandwidth))]
        return cls(windows)

    def rate(self) -> float | None:
        now = self._local_now()
        minute = now.hour * 60 + now.minute
        for start, end, rate in self._windows:
            inside = (
                start <= minute < end
                if start <= end
                else minute >= start or minute < end
            )
            if inside:
                return rate
        return None

    async def acquire(self, nbytes: int) -> None:
        rate = self.rate()
        if not rate:
            return
        async with self._lock:
            now = self._clock()
            self._tokens = min(rate, self._tokens + (now - self._stamp) * rate)
            self._stamp = now
            self._tokens -= nbytes
            if self._tokens < 0:
                await self._sleep(-self._tokens / rate)
                self._tokens = 0.0
                self._stamp = self._clock()


def _read_hashed(fh: BinaryIO, size: int, digest: _Hash) -> bytes:
    chunk = fh.read(size)
    if chunk:
        digest.update(chunk)
    return chunk


def _read_all_hashed(path: Path, digest: _Hash) -> bytes:
    data = path.read_bytes()
    digest.update(data)
    return data


def _open_binary(path: Path) -> BinaryIO:
    return open(path, "rb")


async def _parts(
    fh: BinaryIO,
    digest: _Hash,
    part_size: int,
    limiter: RateLimiter | None,
    sent: list[int],
) -> AsyncGenerator[bytes]:
    while True:
        chunk = await asyncio.to_thread(_read_hashed, fh, part_size, digest)
        if not chunk:
            return
        if limiter is not None:
            await limiter.acquire(len(chunk))
        sent[0] += len(chunk)
        yield chunk


async def upload_file(
    path: Path,
    dest: Location,
    rel: str,
    *,
    settings: UploadSettings,
    limiter: RateLimiter | None = None,
    expect: tuple[int, int] | None = None,
) -> UploadResult:
    """Upload ``path`` to ``rel`` under ``dest``, hashing it on the way.

    Files up to ``single_put_max`` go up in one PUT; bigger ones stream as a
    multipart upload, read part by part on a worker thread. The file is stat'd
    before and after: if it changed (or was not what the journal last saw, per
    ``expect``) the upload raises :class:`FileChangedError` and the caller tries
    again once it settles. The stored size is then checked with a HEAD.
    """
    before = await asyncio.to_thread(os.stat, path)
    stats = (before.st_size, before.st_mtime_ns)
    if expect is not None and stats != expect:
        raise FileChangedError(f"{path} changed since it was scanned")
    key = dest.key(rel)
    digest = hashlib.sha256()
    started = time.perf_counter()
    multipart = before.st_size > settings.single_put_max
    if multipart:
        sent = [0]
        fh = await asyncio.to_thread(_open_binary, path)
        try:
            async with contextlib.aclosing(
                _parts(fh, digest, settings.part_size, limiter, sent)
            ) as parts:
                result = await dest.store.put_async(
                    key,
                    parts,
                    use_multipart=True,
                    chunk_size=settings.part_size,
                    max_concurrency=settings.part_concurrency,
                )
        finally:
            await asyncio.to_thread(fh.close)
        nbytes = sent[0]
    else:
        data = await asyncio.to_thread(_read_all_hashed, path, digest)
        if limiter is not None:
            await limiter.acquire(len(data))
        result = await dest.store.put_async(key, data, use_multipart=False)
        nbytes = len(data)
    after = await asyncio.to_thread(os.stat, path)
    if (after.st_size, after.st_mtime_ns) != stats or nbytes != before.st_size:
        raise FileChangedError(f"{path} changed while it was uploading")
    meta = await dest.store.head_async(key)
    if meta["size"] != nbytes:
        raise VerifyError(
            f"{dest.uri_for(rel)} holds {meta['size']} bytes; sent {nbytes}"
        )
    return UploadResult(
        size=nbytes,
        mtime_ns=before.st_mtime_ns,
        sha256=digest.hexdigest(),
        etag=result["e_tag"],
        duration_ms=int((time.perf_counter() - started) * 1000),
        multipart=multipart,
    )


def _same_device(path: Path, directory: Path) -> bool:
    try:
        return os.stat(path).st_dev == os.stat(directory).st_dev
    except OSError:
        return False


def move_file(src: Path, dst: Path) -> int:
    """Move ``src`` to ``dst`` and return its size.

    Within one file system this is a rename. Across file systems the copy is
    written under a temporary name, fsync'd and size-checked, and only then
    renamed into place and ``src`` deleted, so neither a crash nor a short copy
    can leave the only good copy half-written.
    """
    size = src.stat().st_size
    dst.parent.mkdir(parents=True, exist_ok=True)
    if _same_device(src, dst.parent):
        os.replace(src, dst)
        return size
    partial = dst.with_name(dst.name + ".ferry-partial")
    shutil.copy2(src, partial)
    with open(partial, "rb+") as fh:
        os.fsync(fh.fileno())
    copied = partial.stat().st_size
    if copied != size:
        partial.unlink(missing_ok=True)
        raise VerifyError(f"copy of {src} is {copied} bytes, expected {size}")
    os.replace(partial, dst)
    src.unlink()
    return size


def prune_empty_dirs(top: Path) -> None:
    """Remove empty directories under (and including) ``top``.

    ``os.rmdir`` refuses non-empty directories, so this never deletes data.
    """
    for dirpath, _dirnames, _filenames in os.walk(top, topdown=False):
        with contextlib.suppress(OSError):
            os.rmdir(dirpath)
