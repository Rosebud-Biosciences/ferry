"""Glob matching over relative POSIX paths.

``fnmatch`` lets ``*`` cross ``/`` and has no ``**``, and ``PurePath.full_match``
only arrived in Python 3.13, so ferry compiles its own: ``*`` and ``?`` stay
within one path component, ``**/`` matches any number of leading directories,
and a trailing ``/**`` matches everything below.

A pattern without a ``/`` matches a path's last component, the way gitignore
does: ``*.tif`` selects TIFFs at any depth, while ``images/*.tif`` is anchored.
Backslashes are read as separators, so Windows-style patterns work unchanged.
Matching is case-insensitive on Windows, whose file systems are.
"""

from __future__ import annotations

import os
import re
from collections.abc import Iterable
from functools import lru_cache

CASE_SENSITIVE = os.name != "nt"


def normalize_pattern(pattern: str) -> str:
    text = pattern.strip().replace("\\", "/")
    while text.startswith("./"):
        text = text[2:]
    return text.lstrip("/")


@lru_cache(maxsize=512)
def _compile(pattern: str, case_sensitive: bool) -> re.Pattern[str]:
    out: list[str] = []
    i, n = 0, len(pattern)
    while i < n:
        c = pattern[i]
        if c == "*":
            if pattern.startswith("**", i):
                if pattern.startswith("**/", i):
                    out.append("(?:.*/)?")
                    i += 3
                else:
                    out.append(".*")
                    i += 2
                continue
            out.append("[^/]*")
        elif c == "?":
            out.append("[^/]")
        elif c == "[":
            end = pattern.find("]", i + 2)
            if end == -1:
                out.append(re.escape(c))
            else:
                body = pattern[i + 1 : end].replace("\\", "\\\\")
                if body.startswith("!"):
                    body = "^" + body[1:]
                out.append(f"[{body}]")
                i = end + 1
                continue
        else:
            out.append(re.escape(c))
        i += 1
    return re.compile("".join(out) + r"\Z", 0 if case_sensitive else re.IGNORECASE)


class PathGlob:
    """One pattern, matched against a relative POSIX path."""

    def __init__(self, pattern: str, *, case_sensitive: bool = CASE_SENSITIVE):
        text = normalize_pattern(pattern)
        if not text:
            raise ValueError(f"empty glob pattern: {pattern!r}")
        self.pattern = text
        self.anchored = "/" in text
        self._regex = _compile(text, case_sensitive)

    def match(self, rel: str) -> bool:
        target = rel if self.anchored else rel.rsplit("/", 1)[-1]
        return self._regex.match(target) is not None

    def __repr__(self) -> str:
        return f"PathGlob({self.pattern!r})"


class GlobSet:
    """Any-of matching over several patterns; empty matches nothing."""

    def __init__(
        self, patterns: Iterable[str], *, case_sensitive: bool = CASE_SENSITIVE
    ):
        self.globs = [PathGlob(p, case_sensitive=case_sensitive) for p in patterns]

    def match(self, rel: str) -> bool:
        return any(glob.match(rel) for glob in self.globs)

    def __bool__(self) -> bool:
        return bool(self.globs)
