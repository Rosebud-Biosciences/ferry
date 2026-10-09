"""Logging: a console stream plus one JSON-lines file per local day.

A day's file is complete once the day is over, so shipping is just "upload
every file older than today that has not been shipped yet", and the shipped
files are what registrars catalog as the agent's logs.
"""

from __future__ import annotations

import json
import logging
import re
import sys
import time
from datetime import date, datetime
from pathlib import Path
from typing import IO, TYPE_CHECKING, Any

from .manifest import ShippedLog, iso, to_jsonable
from .transfer import UploadSettings, upload_file

if TYPE_CHECKING:
    from .config import FerryConfig
    from .journal import Journal
    from .registrars.base import Registrar
    from .stores import Location

LOGGER_NAME = "ferry"
_DAY_FILE = re.compile(r"^ferry-(\d{4}-\d{2}-\d{2})\.jsonl$")
_STANDARD = set(vars(logging.LogRecord("", 0, "", 0, "", (), None))) | {
    "message",
    "asctime",
    "taskName",
}
_SHIP_SETTINGS = UploadSettings(
    single_put_max=64 * 1024**2, part_size=8 * 1024**2, part_concurrency=4
)


class JsonFormatter(logging.Formatter):
    """One JSON object per record: time, level, logger, message, and any extras."""

    def format(self, record: logging.LogRecord) -> str:
        data: dict[str, Any] = {
            "ts": datetime.fromtimestamp(record.created)
            .astimezone()
            .isoformat(timespec="milliseconds"),
            "level": record.levelname,
            "logger": record.name,
            "message": record.getMessage(),
        }
        for key, value in record.__dict__.items():
            if key not in _STANDARD and not key.startswith("_"):
                data[key] = to_jsonable(value)
        if record.exc_info:
            data["exc"] = self.formatException(record.exc_info)
        return json.dumps(data, ensure_ascii=False)


class DailyFileHandler(logging.Handler):
    """Append to ``<directory>/ferry-YYYY-MM-DD.jsonl`` for the record's local day,
    deleting files older than ``keep_days``."""

    def __init__(self, directory: Path, *, keep_days: int) -> None:
        super().__init__()
        self.directory = directory
        self.keep_days = keep_days
        self._day: date | None = None
        self._fh: IO[str] | None = None
        self.setFormatter(JsonFormatter())

    def emit(self, record: logging.LogRecord) -> None:
        try:
            day = datetime.fromtimestamp(record.created).date()
            if day != self._day or self._fh is None:
                self._open(day)
            assert self._fh is not None
            self._fh.write(self.format(record) + "\n")
            self._fh.flush()
        except Exception:
            self.handleError(record)

    def _open(self, day: date) -> None:
        if self._fh is not None:
            self._fh.close()
        self.directory.mkdir(parents=True, exist_ok=True)
        path = self.directory / f"ferry-{day.isoformat()}.jsonl"
        self._fh = open(path, "a", encoding="utf-8", newline="\n")  # noqa: SIM115
        self._day = day
        for old_day, old in log_files(self.directory):
            if (day - old_day).days >= self.keep_days:
                old.unlink(missing_ok=True)

    def close(self) -> None:
        if self._fh is not None:
            self._fh.close()
            self._fh = None
        super().close()


def log_files(directory: Path) -> list[tuple[date, Path]]:
    if not directory.is_dir():
        return []
    found = []
    for path in directory.iterdir():
        match = _DAY_FILE.match(path.name)
        if match:
            found.append((date.fromisoformat(match.group(1)), path))
    return sorted(found)


_INSTALLED: list[logging.Handler] = []


def reset_logging() -> None:
    """Remove the handlers :func:`setup_logging` installed."""
    logger = logging.getLogger(LOGGER_NAME)
    for handler in _INSTALLED:
        logger.removeHandler(handler)
        handler.close()
    _INSTALLED.clear()


def setup_logging(
    config: FerryConfig, *, console: bool = True, verbose: bool = False
) -> logging.Logger:
    """Point the ``ferry`` logger at the console and the daily file (idempotent)."""
    logger = logging.getLogger(LOGGER_NAME)
    reset_logging()
    logger.setLevel(logging.DEBUG if verbose else config.logs.level)
    _INSTALLED.append(DailyFileHandler(config.log_dir, keep_days=config.logs.keep_days))
    if console:
        stream = logging.StreamHandler(sys.stderr)
        stream.setFormatter(
            logging.Formatter("%(asctime)s %(levelname)s %(name)s: %(message)s")
        )
        _INSTALLED.append(stream)
    for handler in _INSTALLED:
        logger.addHandler(handler)
    return logger


async def ship_logs(
    *,
    journal: Journal,
    location: Location,
    log_dir: Path,
    node: str,
    registrars: list[Registrar],
    today: date | None = None,
) -> list[ShippedLog]:
    """Upload every finished day's log not yet shipped; tell the registrars."""
    import asyncio

    logger = logging.getLogger(f"{LOGGER_NAME}.logs")
    today = today or datetime.now().date()
    shipped: list[ShippedLog] = []
    for day, path in log_files(log_dir):
        if day >= today or journal.shipped(path.name):
            continue
        rel = f"{node}/{path.name}"
        try:
            result = await upload_file(path, location, rel, settings=_SHIP_SETTINGS)
        except Exception:
            logger.warning("Could not ship %s", path.name, exc_info=True)
            continue
        uri = location.uri_for(rel)
        journal.record_shipped(
            path.name, uri=uri, size=result.size, sha256=result.sha256, now=time.time()
        )
        log = ShippedLog(
            node=node,
            name=path.name,
            day=day.isoformat(),
            uri=uri,
            size=result.size,
            sha256=result.sha256,
        )
        shipped.append(log)
        logger.info("Shipped log %s -> %s", path.name, uri)
        for registrar in registrars:
            try:
                await asyncio.to_thread(registrar.register_log, log)
            except Exception:
                logger.warning(
                    "%s could not register log %s",
                    type(registrar).__name__,
                    path.name,
                    exc_info=True,
                )
    return shipped


async def write_heartbeat(
    location: Location, node: str, payload: dict[str, Any]
) -> None:
    body = json.dumps({"node": node, "time": iso(time.time()), **payload}).encode()
    await location.store.put_async(location.key(f"{node}/heartbeat.json"), body)
