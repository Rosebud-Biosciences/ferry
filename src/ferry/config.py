"""Configuration: one YAML file per node, validated with pydantic.

Durations read as ``90s`` / ``15m`` / ``1h30m`` (or plain seconds), sizes as
``64MiB`` / ``50MB``, and ``${VAR}`` / ``${VAR:-default}`` in any string is
replaced from the environment, so secrets stay out of the file. Relative paths
are resolved against the config file's directory, which keeps a config and its
state directory movable together.

A typo in a key is an error rather than a silently ignored setting: every model
forbids extra fields.
"""

from __future__ import annotations

import os
import re
import socket
from datetime import datetime, timedelta
from pathlib import Path, PureWindowsPath
from typing import Annotated, Any, Literal
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

import yaml
from cronsim import CronSim, CronSimError
from pydantic import (
    BaseModel,
    BeforeValidator,
    ByteSize,
    ConfigDict,
    Field,
    field_validator,
    model_validator,
)

from .globs import PathGlob

_DURATION_PART = re.compile(r"(\d+(?:\.\d+)?)(ms|s|m|h|d|w)", re.IGNORECASE)
_DURATION_UNITS = {
    "ms": 0.001,
    "s": 1.0,
    "m": 60.0,
    "h": 3600.0,
    "d": 86400.0,
    "w": 604800.0,
}
_NUMBER = re.compile(r"\d+(?:\.\d+)?")
_ENV_REF = re.compile(r"\$\{([A-Za-z_][A-Za-z0-9_]*)(?::-(.*?))?\}")
_WINDOW = re.compile(r"(\d{1,2}):(\d{2})\s*-\s*(\d{1,2}):(\d{2})")

# S3 rejects multipart parts under 5 MiB (bar the last) and single PUTs over 5 GiB.
MIN_PART_SIZE = 5 * 1024**2
MAX_SINGLE_PUT = 5 * 1024**3

DEFAULT_EXCLUDE = (
    "*.tmp",
    "*.temp",
    "*.part",
    "*.partial",
    "*.crdownload",
    "~$*",
    ".~lock.*",
    "._*",
    ".DS_Store",
    "Thumbs.db",
    "desktop.ini",
)


def parse_duration(value: object) -> object:
    """``90``, ``"90"``, ``"90s"``, ``"15m"``, ``"1h30m"``, ``"500ms"`` -> timedelta."""
    if isinstance(value, bool | timedelta):
        return value
    if isinstance(value, int | float):
        return timedelta(seconds=value)
    if not isinstance(value, str):
        return value
    text = value.strip().replace(" ", "")
    if _NUMBER.fullmatch(text):
        return timedelta(seconds=float(text))
    total, pos = 0.0, 0
    for match in _DURATION_PART.finditer(text):
        if match.start() != pos:
            break
        total += float(match.group(1)) * _DURATION_UNITS[match.group(2).lower()]
        pos = match.end()
    if not text or pos != len(text):
        raise ValueError(f"not a duration: {value!r} (use e.g. 90s, 15m, 1h30m)")
    return timedelta(seconds=total)


def _per_second(value: object) -> object:
    if isinstance(value, str) and value.strip().lower().endswith("/s"):
        return value.strip()[:-2]
    return value


Duration = Annotated[timedelta, BeforeValidator(parse_duration)]
ByteRate = Annotated[ByteSize, BeforeValidator(_per_second)]


class _Model(BaseModel):
    model_config = ConfigDict(extra="forbid")


def _as_ref(value: object) -> object:
    """A bare plugin name is shorthand for ``{use: name}``."""
    return {"use": value} if isinstance(value, str) else value


class PluginRef(_Model):
    """A plugin: a built-in name, an installed entry point, or ``module:attr``."""

    use: str
    options: dict[str, Any] = Field(default_factory=dict)


class RegistrarRef(PluginRef):
    # Distinguishes two instances of one registrar (say, two manifest targets);
    # it keys the outbox, so renaming one re-delivers to it.
    name: str | None = None
    # Finalizing (moving or deleting source files) waits for every required
    # registrar to confirm the unit. An optional one is retried, never waited on.
    required: bool = True

    @property
    def key(self) -> str:
        return self.name or self.use


class StoreConfig(_Model):
    """Where data goes: ``s3://bucket/prefix``, or a local directory / ``file://``."""

    url: str
    # S3-compatible endpoint (Ceph RGW, SeaweedFS, MinIO, ...); unset means AWS.
    endpoint: str | None = None
    region: str | None = None
    # Path-style addressing (the default) is what most self-hosted stores expect.
    virtual_hosted_style: bool = False
    # An AWS shared-config profile, read through boto3 (the `aws` extra).
    profile: str | None = None
    allow_http: bool = False
    # Ask the server to verify a SHA-256 of every upload. Only enable it where
    # the server validates it: AWS does; Ceph RGW from Tentacle.
    checksum: Literal["sha256"] | None = None
    # Passed through to obstore's S3Config / ClientConfig verbatim.
    config: dict[str, Any] = Field(default_factory=dict)
    client_options: dict[str, Any] = Field(default_factory=dict)

    @property
    def is_s3(self) -> bool:
        return self.url.startswith("s3://")


class UnitConfig(_Model):
    """How files group into units (acquisitions).

    ``dirs`` is a glob per directory level: ``"*__*"`` makes every matching
    top-level folder a unit, ``"*/run*"`` the ``run*`` folders one level down.
    ``regex`` groups files anywhere under the root by its ``unit`` group, e.g.
    ``"^(?P<unit>[^_/]+)_"`` for ``RUN001_quant.csv`` + ``RUN001_melt.csv``.
    """

    dirs: str | None = None
    regex: str | None = None

    @model_validator(mode="after")
    def _one_kind(self) -> UnitConfig:
        if self.dirs is not None and self.regex is not None:
            raise ValueError("unit: set either dirs or regex, not both")
        if self.regex is not None:
            try:
                compiled = re.compile(self.regex)
            except re.error as exc:
                raise ValueError(f"unit.regex does not compile: {exc}") from exc
            if "unit" not in compiled.groupindex:
                raise ValueError("unit.regex needs a named group: (?P<unit>...)")
        else:
            if self.dirs is None:
                self.dirs = "*"
            levels = [level for level in self.dirs.replace("\\", "/").split("/")]
            if not all(levels) or any("**" in level for level in levels):
                raise ValueError(
                    "unit.dirs is one glob per directory level, e.g. '*__*' or "
                    f"'*/run*' (no empty levels or '**'): {self.dirs!r}"
                )
        return self

    @property
    def levels(self) -> list[str]:
        return (self.dirs or "*").replace("\\", "/").split("/")


class FinalizeConfig(_Model):
    """What happens to source files once a unit is uploaded and registered."""

    action: Literal["keep", "move_to_backup", "delete_after_upload"] = "keep"
    backup_root: Path | None = None
    # Files moved concurrently; keep it low when the backup drive spins.
    concurrency: int = Field(default=4, ge=1)
    prune_empty_dirs: bool = True

    @model_validator(mode="after")
    def _backup_root(self) -> FinalizeConfig:
        if self.action == "move_to_backup" and self.backup_root is None:
            raise ValueError("finalize.backup_root is required for move_to_backup")
        return self


class LimitsConfig(_Model):
    files_in_flight: int = Field(default=16, ge=1)
    # Files up to this size go up in one PUT; bigger ones stream as multipart.
    single_put_max: ByteSize = ByteSize(64 * 1024**2)
    part_size: ByteSize = ByteSize(8 * 1024**2)
    part_concurrency: int = Field(default=8, ge=1)
    # Bytes per second, always (``50MB``) or per local time window
    # (``{"08:00-18:00": "50MB"}``, unlimited outside the windows).
    bandwidth: ByteRate | dict[str, ByteRate] | None = None

    @field_validator("part_size")
    @classmethod
    def _part_size(cls, value: ByteSize) -> ByteSize:
        if value < MIN_PART_SIZE:
            raise ValueError("part_size must be at least 5MiB (an S3 minimum)")
        return value

    @field_validator("single_put_max")
    @classmethod
    def _single_put(cls, value: ByteSize) -> ByteSize:
        if value > MAX_SINGLE_PUT:
            raise ValueError("single_put_max must be at most 5GiB (an S3 maximum)")
        return value

    @field_validator("bandwidth")
    @classmethod
    def _windows(cls, value: object) -> object:
        if isinstance(value, dict):
            for window in value:
                parse_window(window)
        return value


def parse_window(text: str) -> tuple[int, int]:
    """``"08:00-18:00"`` -> minutes since midnight ``(480, 1080)``."""
    match = _WINDOW.fullmatch(text.strip())
    if not match:
        raise ValueError(f"not a time window: {text!r} (use HH:MM-HH:MM)")
    h1, m1, h2, m2 = (int(g) for g in match.groups())
    if h1 > 23 or h2 > 24 or m1 > 59 or m2 > 59 or (h2 == 24 and m2 != 0):
        raise ValueError(f"not a time window: {text!r}")
    return h1 * 60 + m1, h2 * 60 + m2


class RetryConfig(_Model):
    initial: Duration = timedelta(seconds=30)
    max: Duration = timedelta(hours=1)
    # Alert once a file, delivery or finalize has failed this many times.
    alert_after: int = Field(default=5, ge=1)

    def backoff(self, attempts: int) -> float:
        """Seconds to wait after the ``attempts``-th consecutive failure."""
        delay = self.initial.total_seconds() * 2 ** max(attempts - 1, 0)
        return min(delay, self.max.total_seconds())


class SourceConfig(_Model):
    """One watched folder and how its units travel."""

    name: str = Field(pattern=r"^[A-Za-z0-9][A-Za-z0-9_.-]*$")
    root: Path
    store: str
    prefix: str = ""
    unit: UnitConfig = Field(default_factory=UnitConfig)
    # Matched against each file's path within its unit (basename if slashless).
    include: list[str] = Field(default_factory=lambda: ["*"])
    # Matched against the path from the root; invisible to ferry entirely.
    exclude: list[str] = Field(default_factory=lambda: list(DEFAULT_EXCLUDE))
    # cron: one pass per `cron` match. stream: watch continuously.
    mode: Literal["cron", "stream"] = "stream"
    cron: str | None = None
    # IANA zone the cron expression is read in; default: the node's local time.
    timezone: str | None = None
    # file: upload each file once it settles. unit: wait for the whole unit.
    # Units register only once complete either way.
    upload: Literal["file", "unit"] = "unit"
    # A file is settled once its mtime is at least this old.
    settle: Duration = timedelta(seconds=60)
    # A unit is complete when all its files have settled and either a marker
    # file is present or nothing in it has changed for `unit_idle`.
    unit_idle: Duration = timedelta(minutes=10)
    marker: str | None = None
    # Alert when a unit with files has stayed incomplete this long.
    max_open: Duration | None = timedelta(hours=24)
    # stream: rescan open units this often, and the whole tree this often.
    poll_interval: Duration = timedelta(seconds=15)
    full_scan_interval: Duration = timedelta(minutes=30)
    # stream: use OS file events as scan hints, or only poll (SMB/NFS roots).
    watch: Literal["events", "poll"] = "events"
    # Windows: treat a file another process holds open as unsettled.
    probe_exclusive: bool = False
    # Before a unit's first upload, list its prefix in the store once and skip
    # files already there with the same size (a lost journal, a migration).
    adopt: bool = True
    finalize: FinalizeConfig = Field(default_factory=FinalizeConfig)
    limits: LimitsConfig = Field(default_factory=LimitsConfig)
    retry: RetryConfig = Field(default_factory=RetryConfig)
    registrars: list[RegistrarRef] = Field(
        default_factory=lambda: [RegistrarRef(use="manifest")]
    )
    adapter: PluginRef | None = None

    @field_validator("registrars", mode="before")
    @classmethod
    def _registrar_refs(cls, value: object) -> object:
        return [_as_ref(v) for v in value] if isinstance(value, list) else value

    @field_validator("adapter", mode="before")
    @classmethod
    def _adapter_ref(cls, value: object) -> object:
        return _as_ref(value)

    @field_validator("include", "exclude")
    @classmethod
    def _globs(cls, patterns: list[str]) -> list[str]:
        for pattern in patterns:
            PathGlob(pattern)
        return patterns

    @field_validator("marker")
    @classmethod
    def _marker(cls, pattern: str | None) -> str | None:
        if pattern is not None:
            PathGlob(pattern)
        return pattern

    @model_validator(mode="after")
    def _schedule(self) -> SourceConfig:
        if self.mode == "cron":
            if not self.cron:
                raise ValueError(
                    f"source {self.name}: mode cron needs a cron expression"
                )
            try:
                CronSim(self.cron, datetime(2000, 1, 1))
            except CronSimError as exc:
                raise ValueError(
                    f"source {self.name}: invalid cron {self.cron!r}: {exc}"
                ) from exc
        elif self.cron is not None:
            raise ValueError(f"source {self.name}: cron only applies to mode: cron")
        if self.timezone is not None:
            try:
                ZoneInfo(self.timezone)
            except (ZoneInfoNotFoundError, ValueError) as exc:
                raise ValueError(
                    f"source {self.name}: unknown timezone {self.timezone!r}"
                ) from exc
        keys = [ref.key for ref in self.registrars]
        if len(keys) != len(set(keys)):
            raise ValueError(
                f"source {self.name}: registrars need distinct names; give "
                "repeated ones a `name:`"
            )
        return self


class ShipConfig(_Model):
    """Copy finished daily log files (and a heartbeat) to a store."""

    store: str
    prefix: str = "_ferry/logs"
    interval: Duration = timedelta(hours=1)
    heartbeat: Duration | None = timedelta(minutes=5)


class LogsConfig(_Model):
    dir: Path | None = None  # default: <state_dir>/logs
    level: Literal["DEBUG", "INFO", "WARNING", "ERROR"] = "INFO"
    keep_days: int = Field(default=30, ge=1)
    ship: ShipConfig | None = None


class FerryConfig(_Model):
    node: str = Field(default_factory=socket.gethostname)
    state_dir: Path
    stores: dict[str, StoreConfig]
    sources: list[SourceConfig]
    logs: LogsConfig = Field(default_factory=LogsConfig)
    notifiers: list[PluginRef] = Field(default_factory=list)

    @field_validator("notifiers", mode="before")
    @classmethod
    def _notifier_refs(cls, value: object) -> object:
        return [_as_ref(v) for v in value] if isinstance(value, list) else value

    @model_validator(mode="after")
    def _references(self) -> FerryConfig:
        names = [source.name for source in self.sources]
        if len(names) != len(set(names)):
            raise ValueError("source names must be unique")
        for source in self.sources:
            if source.store not in self.stores:
                raise ValueError(
                    f"source {source.name}: unknown store {source.store!r} "
                    f"(defined: {', '.join(sorted(self.stores)) or 'none'})"
                )
        if self.logs.ship is not None and self.logs.ship.store not in self.stores:
            raise ValueError(f"logs.ship: unknown store {self.logs.ship.store!r}")
        return self

    def source(self, name: str) -> SourceConfig:
        for source in self.sources:
            if source.name == name:
                return source
        raise KeyError(name)

    @property
    def log_dir(self) -> Path:
        return self.logs.dir or self.state_dir / "logs"

    @property
    def journal_path(self) -> Path:
        return self.state_dir / "journal.sqlite3"


class ConfigError(ValueError):
    """The config file is missing, unreadable, or invalid."""


def interpolate(value: Any, *, env: dict[str, str] | None = None) -> Any:
    """Replace ``${VAR}`` and ``${VAR:-default}`` in every string, recursively."""
    env = dict(os.environ) if env is None else env
    missing: list[str] = []

    def sub(text: str) -> str:
        def repl(match: re.Match[str]) -> str:
            name, default = match.group(1), match.group(2)
            if name in env:
                return env[name]
            if default is not None:
                return default
            missing.append(name)
            return match.group(0)

        return _ENV_REF.sub(repl, text)

    def walk(node: Any) -> Any:
        if isinstance(node, str):
            return sub(node)
        if isinstance(node, list):
            return [walk(item) for item in node]
        if isinstance(node, dict):
            return {key: walk(item) for key, item in node.items()}
        return node

    result = walk(value)
    if missing:
        raise ConfigError(
            "unset environment variable(s): " + ", ".join(sorted(set(missing)))
        )
    return result


def _resolve(base: Path, path: Path) -> Path:
    path = path.expanduser()
    # A Windows config validated elsewhere: C:/... is absolute there, not relative.
    if path.is_absolute() or PureWindowsPath(str(path)).drive:
        return path
    return (base / path).resolve()


def _inside(child: Path, parent: Path) -> bool:
    try:
        child.resolve().relative_to(parent.resolve())
    except ValueError:
        return False
    return True


def resolve_paths(config: FerryConfig, base: Path) -> FerryConfig:
    """Anchor relative paths at ``base`` and reject overlapping roots."""
    config.state_dir = _resolve(base, config.state_dir)
    if config.logs.dir is not None:
        config.logs.dir = _resolve(base, config.logs.dir)
    for store in config.stores.values():
        if not store.is_s3:
            local = store.url.removeprefix("file://")
            store.url = str(_resolve(base, Path(local)))
    for source in config.sources:
        source.root = _resolve(base, source.root)
        backup = source.finalize.backup_root
        if backup is not None:
            backup = source.finalize.backup_root = _resolve(base, backup)
            if _inside(backup, source.root) or _inside(source.root, backup):
                raise ConfigError(
                    f"source {source.name}: backup_root and root must not contain "
                    "each other (ferry would ship its own backups)"
                )
        if _inside(config.state_dir, source.root):
            raise ConfigError(
                f"source {source.name}: state_dir must live outside the root"
            )
    return config


def load_config(path: Path | str, *, env: dict[str, str] | None = None) -> FerryConfig:
    from pydantic import ValidationError

    path = Path(path)
    try:
        raw = yaml.safe_load(path.read_text(encoding="utf-8"))
    except FileNotFoundError as exc:
        raise ConfigError(f"config file not found: {path}") from exc
    except yaml.YAMLError as exc:
        raise ConfigError(f"{path}: not valid YAML: {exc}") from exc
    if not isinstance(raw, dict):
        raise ConfigError(f"{path}: expected a mapping at the top level")
    try:
        config = FerryConfig.model_validate(interpolate(raw, env=env))
    except ValidationError as exc:
        raise ConfigError(f"{path}: {exc}") from exc
    return resolve_paths(config, path.resolve().parent)
