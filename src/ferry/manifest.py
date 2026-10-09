"""What registrars receive: a unit's manifest and its transfer events.

The manifest is also what the built-in ``manifest`` registrar writes to the
store, so it is plain JSON: adapters may return datetimes, paths, enums and
sets, and :func:`to_jsonable` turns them into strings and lists on the way in.
"""

from __future__ import annotations

import enum
import json
from collections.abc import Mapping
from dataclasses import asdict, dataclass, field
from datetime import UTC, date, datetime
from pathlib import PurePath
from typing import Any

SCHEMA = "ferry.manifest/v1"


def to_jsonable(value: Any) -> Any:
    if value is None or isinstance(value, str | int | float | bool):
        return value
    if isinstance(value, Mapping):
        return {str(key): to_jsonable(item) for key, item in value.items()}
    if isinstance(value, set | frozenset):
        return sorted((to_jsonable(item) for item in value), key=str)
    if isinstance(value, list | tuple):
        return [to_jsonable(item) for item in value]
    if isinstance(value, datetime | date):
        return value.isoformat()
    if isinstance(value, PurePath):
        return value.as_posix()
    if isinstance(value, enum.Enum):
        return to_jsonable(value.value)
    return str(value)


def iso(timestamp: float | None) -> str | None:
    """POSIX seconds as ISO 8601 in UTC."""
    if timestamp is None:
        return None
    return datetime.fromtimestamp(timestamp, UTC).isoformat()


@dataclass
class ManifestFile:
    path: str  # within the unit
    rel_path: str  # from the source root
    uri: str
    size: int
    mtime: str | None
    sha256: str | None = None
    etag: str | None = None
    local_path: str | None = None
    backup_path: str | None = None
    metadata: dict[str, Any] = field(default_factory=dict)


@dataclass
class Manifest:
    node: str
    source: str
    unit: str
    uri: str
    root: str
    local_path: str | None
    generation: int
    first_seen: str | None
    completed: str | None
    created: str | None
    finalize: str
    files: list[ManifestFile]
    metadata: dict[str, Any] = field(default_factory=dict)
    agent: str = ""
    schema: str = SCHEMA

    @property
    def total_bytes(self) -> int:
        return sum(f.size for f in self.files)

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)

    def to_json(self, *, indent: int | None = None) -> str:
        return json.dumps(self.to_dict(), indent=indent)

    @classmethod
    def from_dict(cls, data: Mapping[str, Any]) -> Manifest:
        values = dict(data)
        files = [ManifestFile(**item) for item in values.pop("files")]
        return cls(files=files, **values)


@dataclass(frozen=True)
class TransferEvent:
    rel_path: str
    op: str  # upload | adopt | backup | delete
    status: str  # ok | failed | changed
    src: str
    dst: str
    at: str | None
    bytes: int | None = None
    duration_ms: int | None = None
    error: str | None = None

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)

    @classmethod
    def from_dict(cls, data: Mapping[str, Any]) -> TransferEvent:
        return cls(**data)


@dataclass(frozen=True)
class ShippedLog:
    node: str
    name: str
    day: str
    uri: str
    size: int
    sha256: str
