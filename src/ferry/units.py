"""Units: how a source's files group into acquisitions, and scanning for them.

A unit is either a directory at a fixed depth (``unit.dirs``, one glob per
level) or the set of files whose path yields the same ``unit`` group of
``unit.regex``. A file's *within* path is relative to its unit (for regex units,
its path from the root), and it travels to ``<prefix>/<unit>/<within>`` in the
store, so a directory unit keeps the local layout exactly.
"""

from __future__ import annotations

import os
import re
from collections.abc import Iterable
from dataclasses import dataclass, field
from pathlib import Path

from .config import SourceConfig
from .globs import GlobSet, PathGlob
from .stores import normalize_key


@dataclass(frozen=True)
class FileObs:
    rel: str
    within: str
    size: int
    mtime_ns: int


@dataclass
class UnitObs:
    key: str
    files: dict[str, FileObs] = field(default_factory=dict)
    marker: bool = False
    # The newest mtime of anything in the unit that is not excluded -- files
    # ferry does not upload and the directories themselves included, since a
    # directory's mtime moves whenever an entry is added, removed or renamed.
    newest_ns: int = 0


@dataclass
class Scan:
    units: dict[str, UnitObs]
    # Unit keys this scan looked at in full (``None``: the whole tree), so the
    # journal can tell "deleted" from "not looked at" for whatever it did not see.
    scope: frozenset[str] | None
    errors: int = 0


class Layout:
    """A source's unit grouping and file filters, and the scans built on them."""

    def __init__(self, cfg: SourceConfig) -> None:
        self.root = cfg.root
        self.include = GlobSet(cfg.include)
        self.exclude = GlobSet(cfg.exclude)
        self.marker = PathGlob(cfg.marker) if cfg.marker else None
        self.regex = re.compile(cfg.unit.regex) if cfg.unit.regex else None
        self.levels = (
            [] if self.regex else [PathGlob(level) for level in cfg.unit.levels]
        )

    @property
    def by_dirs(self) -> bool:
        return self.regex is None

    def unit_dir(self, key: str) -> Path | None:
        return self.root / key if self.by_dirs else None

    @staticmethod
    def remote_rel(key: str, within: str) -> str:
        return f"{key}/{within}"

    def key_for(self, rel: str) -> str | None:
        """The unit a root-relative path belongs to, if it can be told from the path."""
        if self.regex is not None:
            match = self.regex.search(rel)
            return self._regex_key(match) if match else None
        parts = rel.split("/")
        depth = len(self.levels)
        if len(parts) < depth:
            return None
        head = parts[:depth]
        if all(glob.match(part) for glob, part in zip(self.levels, head, strict=True)):
            return "/".join(head)
        return None

    @staticmethod
    def _regex_key(match: re.Match[str]) -> str | None:
        key = normalize_key(match.group("unit") or "")
        return key or None

    @staticmethod
    def _entries(path: Path, errors: list[int]) -> list[os.DirEntry[str]]:
        try:
            with os.scandir(path) as it:
                return list(it)
        except OSError:
            errors[0] += 1
            return []

    def unit_keys(self) -> list[str]:
        """Directories at unit depth, without walking into them (cheap discovery)."""
        if not self.by_dirs:
            return []
        return self._unit_dirs([0])

    def _unit_dirs(self, errors: list[int]) -> list[str]:
        frontier = [("", self.root)]
        for level in self.levels:
            deeper: list[tuple[str, Path]] = []
            for prefix, path in frontier:
                for entry in self._entries(path, errors):
                    rel = f"{prefix}/{entry.name}" if prefix else entry.name
                    try:
                        is_dir = entry.is_dir(follow_symlinks=False)
                    except OSError:
                        errors[0] += 1
                        continue
                    if (
                        is_dir
                        and level.match(entry.name)
                        and not self.exclude.match(rel)
                    ):
                        deeper.append((rel, Path(entry.path)))
            frontier = deeper
        return sorted(rel for rel, _ in frontier)

    def scan(self, keys: Iterable[str] | None = None) -> Scan:
        """Observe the whole tree, or just the units in ``keys`` (directory units)."""
        errors = [0]
        if self.regex is not None:
            return Scan(self._scan_regex(errors), None, errors[0])
        if keys is None:
            units = {
                key: self._scan_dir(key, errors) for key in self._unit_dirs(errors)
            }
            return Scan(units, None, errors[0])
        scope = frozenset(keys)
        units = {
            key: self._scan_dir(key, errors)
            for key in sorted(scope)
            if (self.root / key).is_dir()
        }
        return Scan(units, scope, errors[0])

    def _scan_dir(self, key: str, errors: list[int]) -> UnitObs:
        obs = UnitObs(key)
        top = self.root / key
        try:
            obs.newest_ns = top.stat().st_mtime_ns
        except OSError:
            errors[0] += 1
        stack = [(top, "")]
        while stack:
            path, prefix = stack.pop()
            for entry in self._entries(path, errors):
                within = f"{prefix}/{entry.name}" if prefix else entry.name
                if self.exclude.match(f"{key}/{within}"):
                    continue
                try:
                    if entry.is_dir(follow_symlinks=False):
                        stat = entry.stat(follow_symlinks=False)
                        obs.newest_ns = max(obs.newest_ns, stat.st_mtime_ns)
                        stack.append((Path(entry.path), within))
                        continue
                    if not entry.is_file():
                        continue
                    stat = entry.stat()
                except OSError:
                    errors[0] += 1
                    continue
                obs.newest_ns = max(obs.newest_ns, stat.st_mtime_ns)
                if self.marker is not None and self.marker.match(within):
                    obs.marker = True
                if self.include.match(within):
                    rel = f"{key}/{within}"
                    obs.files[rel] = FileObs(
                        rel, within, stat.st_size, stat.st_mtime_ns
                    )
        return obs

    def _scan_regex(self, errors: list[int]) -> dict[str, UnitObs]:
        assert self.regex is not None
        units: dict[str, UnitObs] = {}
        stack = [(self.root, "")]
        while stack:
            path, prefix = stack.pop()
            for entry in self._entries(path, errors):
                rel = f"{prefix}/{entry.name}" if prefix else entry.name
                if self.exclude.match(rel):
                    continue
                try:
                    if entry.is_dir(follow_symlinks=False):
                        stack.append((Path(entry.path), rel))
                        continue
                    if not entry.is_file():
                        continue
                    match = self.regex.search(rel)
                    key = self._regex_key(match) if match else None
                    if key is None:
                        continue
                    stat = entry.stat()
                except OSError:
                    errors[0] += 1
                    continue
                obs = units.setdefault(key, UnitObs(key))
                obs.newest_ns = max(obs.newest_ns, stat.st_mtime_ns)
                if self.marker is not None and self.marker.match(rel):
                    obs.marker = True
                if self.include.match(rel):
                    obs.files[rel] = FileObs(rel, rel, stat.st_size, stat.st_mtime_ns)
        return units
