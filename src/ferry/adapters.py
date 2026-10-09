"""Adapters: instrument-specific metadata for a unit's manifest.

An adapter reads what a unit *is* -- the plate, the acquisition time, each
file's well and channel -- typically from its folder and file names. ferry calls
it once per registration, on a worker thread, while the files are still in
place; whatever it returns lands in ``manifest.metadata`` (and each file's
``metadata``) for registrars to use. It must not modify the files.
"""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any


@dataclass(frozen=True)
class UnitFile:
    rel: str
    within: str
    path: Path
    size: int


@dataclass(frozen=True)
class UnitView:
    source: str
    key: str
    root: Path
    # The unit's directory, for directory units.
    local_path: Path | None
    files: list[UnitFile]


class Adapter:
    """Base class; the default describes nothing."""

    def __init__(self, **options: Any) -> None:
        if options:
            raise TypeError(
                f"{type(self).__name__} takes no options; got {sorted(options)}"
            )

    def describe(
        self, unit: UnitView
    ) -> tuple[dict[str, Any], dict[str, dict[str, Any]]]:
        """Return ``(unit metadata, {file within-path: file metadata})``."""
        return {}, {}
