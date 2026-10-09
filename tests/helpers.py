"""Test doubles importable from configs as ``tests.helpers:...``."""

from __future__ import annotations

import os
import time
from collections import defaultdict
from pathlib import Path
from typing import Any

from ferry.adapters import Adapter, UnitView
from ferry.manifest import Manifest, TransferEvent
from ferry.registrars.base import Registrar

# Calls per recorder name: (event, manifest, transfers).
CALLS: dict[str, list[tuple[str, Manifest | None, list[TransferEvent]]]] = defaultdict(
    list
)
# Calls to fail before succeeding, per recorder name.
FAILURES: dict[str, int] = defaultdict(int)
# Run inside register(), per recorder name, to simulate the world moving on.
ON_REGISTER: dict[str, Any] = {}


class Recorder(Registrar):
    """Records every call; fails ``FAILURES[name]`` times first."""

    def __init__(self, name: str) -> None:
        super().__init__()
        self.name = name

    def _maybe_fail(self) -> None:
        if FAILURES[self.name] > 0:
            FAILURES[self.name] -= 1
            raise ConnectionError("catalog unavailable")

    def register(self, manifest: Manifest, transfers: list[TransferEvent]) -> None:
        self._maybe_fail()
        hook = ON_REGISTER.pop(self.name, None)
        if hook is not None:
            hook()
        CALLS[self.name].append(("register", manifest, transfers))

    def finalize(self, manifest: Manifest, transfers: list[TransferEvent]) -> None:
        self._maybe_fail()
        CALLS[self.name].append(("finalize", manifest, transfers))

    def record_failures(self, transfers: list[TransferEvent]) -> None:
        CALLS[self.name].append(("failures", None, transfers))


def calls(name: str, event: str) -> list[Manifest]:
    return [m for e, m, _ in CALLS[name] if e == event and m is not None]


class PlateAdapter(Adapter):
    """Reads ``<plate>__<n>`` folder names; tags each file with its stem."""

    def describe(
        self, unit: UnitView
    ) -> tuple[dict[str, Any], dict[str, dict[str, Any]]]:
        plate = unit.key.split("__")[0]
        return (
            {"plate": plate, "files": len(unit.files)},
            {f.within: {"stem": Path(f.within).stem} for f in unit.files},
        )


class FakeClock:
    """Wall-clock seconds that only move when told to."""

    def __init__(self) -> None:
        self.now = time.time()

    def __call__(self) -> float:
        return self.now

    def advance(self, seconds: float) -> None:
        self.now += seconds


def write(
    path: Path,
    data: bytes = b"data",
    *,
    age: float = 3600.0,
    mtime: float | None = None,
) -> Path:
    """Write ``path`` with an mtime ``age`` seconds ago (default: long settled),
    or at ``mtime`` -- e.g. a :class:`FakeClock`'s now, for "just written"."""
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(data)
    stamp = time.time() - age if mtime is None else mtime
    os.utime(path, (stamp, stamp))
    return path


def age_tree(top: Path, age: float = 3600.0, *, mtime: float | None = None) -> None:
    """Backdate every file and directory under (and including) ``top``."""
    stamp = time.time() - age if mtime is None else mtime
    for dirpath, _dirnames, filenames in os.walk(top):
        for name in filenames:
            os.utime(Path(dirpath) / name, (stamp, stamp))
        os.utime(dirpath, (stamp, stamp))


def files_under(top: Path) -> dict[str, bytes]:
    if not top.exists():
        return {}
    return {
        p.relative_to(top).as_posix(): p.read_bytes()
        for p in sorted(top.rglob("*"))
        if p.is_file()
    }
