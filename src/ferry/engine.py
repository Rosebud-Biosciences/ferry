"""The engine: one source's journey from a watched folder to the store and catalog.

``mode: cron`` runs :meth:`SourceEngine.run_pass` on its schedule (and ``ferry
run`` runs it once); ``mode: stream`` drives the same steps continuously (see
:mod:`ferry.stream`). Every step reads and writes the journal, so a crash, a
reboot or Task Scheduler killing the process at any point leaves a state the
next pass carries on from. Three rules hold throughout:

- A file is uploaded once it has settled, and is recorded as uploaded only if
  it did not change while being read.
- A unit is registered once it is complete and every one of its files is in the
  store -- never with a partial file list.
- Source files are kept, moved or deleted only after every required registrar
  has confirmed the unit, and only if they still match what was uploaded.
"""

from __future__ import annotations

import asyncio
import json
import logging
import os
import time
from collections.abc import Awaitable, Callable, Iterable
from dataclasses import dataclass, field
from typing import Any

from ._version import AGENT
from .adapters import Adapter, UnitFile, UnitView
from .config import SourceConfig
from .journal import (
    COMPLETE,
    FINALIZED,
    GONE,
    OPEN,
    PENDING,
    REGISTERED,
    REGISTERING,
    UPLOADED,
    FileRow,
    Journal,
    OutboxRow,
    ScanChanges,
    TransferRow,
    UnitRow,
)
from .manifest import Manifest, ManifestFile, TransferEvent, iso, to_jsonable
from .notify import Alert, Alerts
from .registrars.base import Registrar
from .stores import Location
from .transfer import (
    FileChangedError,
    RateLimiter,
    UploadSettings,
    move_file,
    prune_empty_dirs,
    upload_file,
)
from .units import Layout
from .winfs import held_open


@dataclass
class BoundRegistrar:
    key: str
    required: bool
    registrar: Registrar


class _ChangedAfterUploadError(Exception):
    def __init__(self, file_id: int, reason: str) -> None:
        super().__init__(reason)
        self.file_id = file_id
        self.reason = reason


def describe_error(exc: BaseException) -> str:
    return f"{type(exc).__name__}: {exc}"


def human_bytes(nbytes: float) -> str:
    for unit in ("B", "KB", "MB", "GB"):
        if abs(nbytes) < 1000:
            return f"{nbytes:.0f} {unit}" if unit == "B" else f"{nbytes:.1f} {unit}"
        nbytes /= 1000
    return f"{nbytes:.1f} TB"


def human_duration(seconds: float) -> str:
    seconds = max(int(seconds), 0)
    if seconds < 120:
        return f"{seconds}s"
    minutes = seconds // 60
    if minutes < 120:
        return f"{minutes}m"
    hours = minutes // 60
    return f"{hours}h" if hours < 48 else f"{hours // 24}d"


def _event(row: TransferRow) -> TransferEvent:
    return TransferEvent(
        rel_path=row.rel,
        op=row.op,
        status=row.status,
        src=row.src,
        dst=row.dst,
        at=iso(row.at),
        bytes=row.bytes,
        duration_ms=row.duration_ms,
        error=row.error,
    )


def _size_or_none(path: os.PathLike[str]) -> int | None:
    try:
        return os.stat(path).st_size
    except OSError:
        return None


@dataclass
class PassReport:
    """What a pass (or, in stream mode, a digest period) did to a source."""

    source: str
    started: float
    finished: float | None = None
    uploaded: int = 0
    uploaded_bytes: int = 0
    adopted: int = 0
    upload_failures: int = 0
    registered: list[str] = field(default_factory=list)
    finalized: list[str] = field(default_factory=list)
    reopened: list[tuple[str, str]] = field(default_factory=list)
    waiting: dict[str, str] = field(default_factory=dict)
    delivery_failures: int = 0
    finalize_failures: int = 0
    errors: list[str] = field(default_factory=list)

    @property
    def failures(self) -> int:
        return (
            self.upload_failures
            + self.delivery_failures
            + self.finalize_failures
            + len(self.errors)
        )

    @property
    def active(self) -> bool:
        return bool(
            self.uploaded
            or self.adopted
            or self.registered
            or self.finalized
            or self.reopened
            or self.failures
        )

    def render(self) -> str:
        head = (
            f"{self.source}: {len(self.registered)} unit(s) registered, "
            f"{len(self.finalized)} finalized; {self.uploaded} file(s) uploaded "
            f"({human_bytes(self.uploaded_bytes)})"
        )
        if self.adopted:
            head += f", {self.adopted} already in the store"
        if self.waiting:
            head += f"; {len(self.waiting)} unit(s) waiting"
        head += f"; {self.failures} failure(s)." if self.failures else "; no failures."
        lines = [head]
        finalized = set(self.finalized)
        for key in self.registered:
            done = ", finalized" if key in finalized else ""
            lines.append(f"• {key} — registered{done}")
        lines.extend(
            f"• {key} — finalized"
            for key in self.finalized
            if key not in self.registered
        )
        lines.extend(f"• {key} — reopened: {why}" for key, why in self.reopened)
        lines.extend(f"• {key} — {why}" for key, why in sorted(self.waiting.items()))
        lines.extend(f"• error: {error}" for error in self.errors)
        return "\n".join(lines)


def report_alert(report: PassReport) -> Alert:
    """A pass's digest as an alert: ``info``, or ``warning`` if anything failed."""
    head, _, rest = report.render().partition("\n")
    return Alert(
        level="warning" if report.failures else "info",
        title=head,
        message=rest,
        source=report.source,
    )


class SourceEngine:
    def __init__(
        self,
        *,
        node: str,
        cfg: SourceConfig,
        location: Location,
        journal: Journal,
        registrars: list[BoundRegistrar],
        adapter: Adapter | None = None,
        alerts: Alerts | None = None,
        dry_run: bool = False,
        clock: Callable[[], float] = time.time,
        logger: logging.Logger | None = None,
    ) -> None:
        self.node = node
        self.cfg = cfg
        self.name = cfg.name
        self.location = location
        self.journal = journal
        self.registrars = registrars
        self._by_key = {bound.key: bound for bound in registrars}
        self.adapter = adapter
        self.dry_run = dry_run
        self.clock = clock
        self.log = logger or logging.getLogger(f"ferry.{cfg.name}")
        self.alerts = alerts or Alerts([], self.log)
        self.layout = Layout(cfg)
        self.settings = UploadSettings.from_limits(cfg.limits)
        self.limiter = RateLimiter.from_config(cfg.limits.bandwidth)
        self._adopt_locks: dict[int, asyncio.Lock] = {}
        self._moves: asyncio.Semaphore | None = None
        self.report = PassReport(cfg.name, started=clock())

    # -- helpers ------------------------------------------------------------
    @property
    def upload_states(self) -> tuple[str, ...]:
        return (COMPLETE,) if self.cfg.upload == "unit" else (OPEN, COMPLETE)

    def _settled_ns(self, now: float) -> int:
        return int((now - self.cfg.settle.total_seconds()) * 1_000_000_000)

    def _extra(self, unit: str | None = None, **values: Any) -> dict[str, Any]:
        return {"source": self.name, "unit": unit, **values}

    def _live(self, unit_id: int) -> list[FileRow]:
        """The unit's files that are on disk now."""
        return [f for f in self.journal.files(unit_id) if f.present and f.state != GONE]

    def _transfer(
        self,
        unit: UnitRow,
        row: FileRow,
        op: str,
        status: str,
        src: str,
        dst: str,
        *,
        nbytes: int | None = None,
        duration_ms: int | None = None,
        error: str | None = None,
    ) -> int:
        return self.journal.add_transfer(
            source=self.name,
            unit_id=unit.id,
            rel=row.rel,
            op=op,
            status=status,
            src=src,
            dst=dst,
            at=self.clock(),
            nbytes=nbytes,
            duration_ms=duration_ms,
            error=error,
        )

    def take_report(self) -> PassReport:
        """Close the current report (with what is still waiting) and start anew."""
        report = self.report
        report.finished = self.clock()
        report.waiting = self.waiting()
        self.report = PassReport(self.name, started=report.finished)
        return report

    # -- scan ---------------------------------------------------------------
    async def scan(self, keys: Iterable[str] | None = None) -> ScanChanges:
        """Observe the tree (or just ``keys``) and fold it into the journal."""
        wanted = None if keys is None else list(keys)
        scan = await asyncio.to_thread(self.layout.scan, wanted)
        changes = self.journal.apply_scan(self.name, scan, self.clock())
        if len(changes.new_units) > 10:
            self.log.info(
                "Found %d new unit(s)", len(changes.new_units), extra=self._extra()
            )
        else:
            for key in changes.new_units:
                self.log.info("New unit %s", key, extra=self._extra(key))
        for key, why in changes.reopened:
            self.log.warning("Reopened %s: %s", key, why, extra=self._extra(key))
            self.report.reopened.append((key, why))
        for rel in changes.gone_files:
            self.log.warning(
                "%s disappeared before it was uploaded", rel, extra=self._extra()
            )
        for key in changes.gone_units:
            self.log.info(
                "Unit %s vanished before any of it was uploaded",
                key,
                extra=self._extra(key),
            )
        if scan.errors:
            self.log.debug(
                "%d entries could not be read", scan.errors, extra=self._extra()
            )
        return changes

    # -- completion ---------------------------------------------------------
    def _why_open(self, unit: UnitRow, files: list[FileRow], now: float) -> str | None:
        """Why ``unit`` is not complete yet, or ``None`` if it is."""
        if not files:
            return "no files yet"
        settled_ns = self._settled_ns(now)
        unsettled = sum(1 for f in files if f.mtime_ns > settled_ns)
        if unsettled:
            return f"{unsettled} of {len(files)} file(s) still settling"
        if unit.forced or unit.marker:
            return None
        quiet = now - unit.last_change
        idle = self.cfg.unit_idle.total_seconds()
        if quiet < idle:
            return (
                f"changed {human_duration(quiet)} ago; complete after "
                f"{human_duration(idle)} without changes"
            )
        return None

    def evaluate(self) -> list[str]:
        """Complete every open unit that qualifies; alert on ones stuck open."""
        now = self.clock()
        completed: list[str] = []
        for unit in self.journal.units(self.name, [OPEN]):
            files = self._live(unit.id)
            why = self._why_open(unit, files, now)
            if why is not None:
                self._alert_if_stuck(unit, files, why, now)
                continue
            generation = self.journal.complete_unit(unit.id, now)
            if generation is None:
                continue
            completed.append(unit.key)
            again = f", generation {generation}" if generation > 1 else ""
            self.log.info(
                "Unit %s is complete (%d file(s)%s)",
                unit.key,
                len(files),
                again,
                extra=self._extra(unit.key),
            )
        return completed

    def _alert_if_stuck(
        self, unit: UnitRow, files: list[FileRow], why: str, now: float
    ) -> None:
        limit = self.cfg.max_open
        if limit is None or unit.alerted or not files:
            return
        age = now - unit.first_seen
        if age < limit.total_seconds():
            return
        self.journal.set_unit(unit.id, alerted=1)
        self.alerts.emit(
            Alert(
                level="warning",
                title=(
                    f"{self.name}: unit {unit.key} has been incomplete for "
                    f"{human_duration(age)}"
                ),
                message=(
                    f"{why}. If it is finished, run `ferry complete {self.name} "
                    f"{unit.key}`; to stop tracking it, `ferry abandon {self.name} "
                    f"{unit.key}`."
                ),
                source=self.name,
                unit=unit.key,
            )
        )

    # -- upload -------------------------------------------------------------
    def upload_candidates(self) -> list[FileRow]:
        now = self.clock()
        return self.journal.upload_candidates(
            self.name,
            now=now,
            settled_ns=self._settled_ns(now),
            unit_states=self.upload_states,
        )

    async def upload(self, file_id: int) -> bool:
        """Upload one pending file; ``True`` if it is now in the store."""
        row = self.journal.file(file_id)
        if row.state != PENDING or not row.present:
            return False
        unit = self.journal.unit(row.unit_id)
        if unit.state not in self.upload_states:
            return False
        path = self.cfg.root / row.rel
        rel = self.layout.remote_rel(unit.key, row.within)
        dst = self.location.uri_for(rel)
        if not unit.adopted and self.cfg.adopt:
            try:
                await self._adopt(unit)
            except Exception as exc:
                self._upload_failed(unit, row, str(path), dst, exc, adopting=True)
                return False
            row = self.journal.file(file_id)
            if row.state != PENDING:
                return row.state == UPLOADED
        now = self.clock()
        if self.cfg.probe_exclusive and await asyncio.to_thread(held_open, path):
            self.journal.defer_file(file_id, now + self.cfg.settle.total_seconds())
            return False
        if self.dry_run:
            self.log.info(
                "[dry run] would upload %s (%s) to %s",
                row.rel,
                human_bytes(row.size),
                dst,
                extra=self._extra(unit.key),
            )
            self.journal.mark_uploaded(
                file_id,
                size=row.size,
                mtime_ns=row.mtime_ns,
                sha256=None,
                etag=None,
                now=now,
            )
            self.report.uploaded += 1
            self.report.uploaded_bytes += row.size
            return True
        try:
            result = await upload_file(
                path,
                self.location,
                rel,
                settings=self.settings,
                limiter=self.limiter,
                expect=(row.size, row.mtime_ns),
            )
        except FileChangedError as exc:
            with self.journal.tx():
                self.journal.defer_file(
                    file_id, self.clock() + self.cfg.settle.total_seconds()
                )
                self._transfer(
                    unit, row, "upload", "changed", str(path), dst, error=str(exc)
                )
            self.log.debug(
                "%s changed while uploading; it will go again once settled", row.rel
            )
            return False
        except FileNotFoundError:
            self.log.debug("%s vanished before it could be uploaded", row.rel)
            return False
        except Exception as exc:
            self._upload_failed(unit, row, str(path), dst, exc)
            return False
        with self.journal.tx():
            self.journal.mark_uploaded(
                file_id,
                size=result.size,
                mtime_ns=result.mtime_ns,
                sha256=result.sha256,
                etag=result.etag,
                now=self.clock(),
            )
            self._transfer(
                unit,
                row,
                "upload",
                "ok",
                str(path),
                dst,
                nbytes=result.size,
                duration_ms=result.duration_ms,
            )
        self.report.uploaded += 1
        self.report.uploaded_bytes += result.size
        self.log.debug(
            "Uploaded %s to %s (%s in %d ms%s)",
            row.rel,
            dst,
            human_bytes(result.size),
            result.duration_ms,
            ", multipart" if result.multipart else "",
            extra=self._extra(
                unit.key, bytes=result.size, duration_ms=result.duration_ms
            ),
        )
        return True

    def _upload_failed(
        self,
        unit: UnitRow,
        row: FileRow,
        src: str,
        dst: str,
        exc: BaseException,
        *,
        adopting: bool = False,
    ) -> None:
        error = describe_error(exc)
        if adopting:
            error = f"listing {self.location.uri_for(unit.key)} failed: {error}"
        attempts = row.attempts + 1
        with self.journal.tx():
            self.journal.file_failed(
                row.id, error, self.clock() + self.cfg.retry.backoff(attempts)
            )
            self._transfer(unit, row, "upload", "failed", src, dst, error=error)
        self.report.upload_failures += 1
        self.log.error(
            "Upload of %s failed (attempt %d): %s",
            row.rel,
            attempts,
            error,
            extra=self._extra(unit.key, error=error),
        )
        if attempts >= self.cfg.retry.alert_after and not row.alerted:
            self.journal.set_file(row.id, alerted=1)
            self.alerts.emit(
                Alert(
                    level="error",
                    title=f"{self.name}: {row.rel} failed to upload {attempts} times",
                    message=error,
                    source=self.name,
                    unit=unit.key,
                )
            )

    async def _adopt(self, unit: UnitRow) -> None:
        """Mark files already in the store (same key and size) as uploaded.

        Run once per unit, before its first upload, so a lost journal or a
        migration from another uploader does not send everything again.
        """
        lock = self._adopt_locks.setdefault(unit.id, asyncio.Lock())
        async with lock:
            if self.journal.unit(unit.id).adopted:
                return
            remote: dict[str, int] = {}
            stream = self.location.store.list(
                prefix=self.location.key(unit.key), chunk_size=1000
            )
            async for batch in stream:
                for meta in batch:
                    remote[meta["path"]] = meta["size"]
            adopted = 0
            with self.journal.tx():
                for row in self.journal.files(unit.id):
                    if row.state != PENDING or not row.present:
                        continue
                    rel = self.layout.remote_rel(unit.key, row.within)
                    if remote.get(self.location.key(rel)) != row.size:
                        continue
                    self.journal.mark_uploaded(
                        row.id,
                        size=row.size,
                        mtime_ns=row.mtime_ns,
                        sha256=None,
                        etag=None,
                        now=self.clock(),
                    )
                    self._transfer(
                        unit,
                        row,
                        "adopt",
                        "ok",
                        str(self.cfg.root / row.rel),
                        self.location.uri_for(rel),
                        nbytes=row.size,
                    )
                    adopted += 1
                self.journal.set_unit(unit.id, adopted=1)
            self._adopt_locks.pop(unit.id, None)
        if adopted:
            self.report.adopted += adopted
            self.log.info(
                "%d file(s) of %s were already in %s; not uploading them again",
                adopted,
                unit.key,
                self.location.uri_for(unit.key),
                extra=self._extra(unit.key),
            )

    async def _run_bounded(
        self,
        rows: list[FileRow],
        limit: int,
        work: Callable[[int], Awaitable[bool]],
        stop: asyncio.Event | None,
    ) -> None:
        pending = iter(rows)

        async def worker() -> None:
            for row in pending:
                if stop is not None and stop.is_set():
                    return
                try:
                    await work(row.id)
                except Exception:
                    self.log.exception("Unexpected error handling %s", row.rel)

        await asyncio.gather(*(worker() for _ in range(min(limit, len(rows)))))

    # -- registration -------------------------------------------------------
    async def _describe(
        self, unit: UnitRow, files: list[FileRow]
    ) -> tuple[dict[str, Any], dict[str, Any]]:
        if self.adapter is None:
            return {}, {}
        view = UnitView(
            source=self.name,
            key=unit.key,
            root=self.cfg.root,
            local_path=self.layout.unit_dir(unit.key),
            files=[
                UnitFile(
                    rel=f.rel, within=f.within, path=self.cfg.root / f.rel, size=f.size
                )
                for f in files
            ],
        )
        try:
            unit_meta, file_meta = await asyncio.to_thread(self.adapter.describe, view)
        except Exception as exc:
            self.log.warning(
                "Adapter could not describe %s; registering without metadata",
                unit.key,
                exc_info=True,
                extra=self._extra(unit.key),
            )
            return {"adapter_error": describe_error(exc)}, {}
        return to_jsonable(unit_meta or {}), to_jsonable(file_meta or {})

    def _manifest(
        self,
        unit: UnitRow,
        files: list[FileRow],
        unit_meta: dict[str, Any],
        file_meta: dict[str, Any],
    ) -> Manifest:
        local = self.layout.unit_dir(unit.key)
        entries = []
        for f in files:
            size = f.up_size if f.up_size is not None else f.size
            mtime_ns = f.up_mtime_ns if f.up_mtime_ns is not None else f.mtime_ns
            rel = self.layout.remote_rel(unit.key, f.within)
            entries.append(
                ManifestFile(
                    path=f.within,
                    rel_path=f.rel,
                    uri=self.location.uri_for(rel),
                    size=size,
                    mtime=iso(mtime_ns / 1e9),
                    sha256=f.sha256,
                    etag=f.etag,
                    local_path=str(self.cfg.root / f.rel),
                    backup_path=f.backup_path,
                    metadata=dict(
                        file_meta.get(f.within) or file_meta.get(f.rel) or {}
                    ),
                )
            )
        return Manifest(
            node=self.node,
            source=self.name,
            unit=unit.key,
            uri=self.location.uri_for(unit.key),
            root=str(self.cfg.root),
            local_path=str(local) if local is not None else None,
            generation=unit.generation,
            first_seen=iso(unit.first_seen),
            completed=iso(unit.completed_at),
            created=iso(self.clock()),
            finalize=self.cfg.finalize.action,
            files=entries,
            metadata=dict(unit_meta),
            agent=AGENT,
        )

    def _payload(self, manifest: Manifest, transfers: list[TransferRow]) -> str:
        return json.dumps(
            {
                "manifest": manifest.to_dict(),
                "transfers": [_event(t).to_dict() for t in transfers],
            }
        )

    async def register_ready(self) -> list[str]:
        """Queue registration for complete units whose files are all uploaded."""
        queued: list[str] = []
        for unit in self.journal.units(self.name, [COMPLETE]):
            files = [f for f in self.journal.files(unit.id) if f.state != GONE]
            if not files or any(f.state == PENDING for f in files):
                continue
            unit_meta, file_meta = await self._describe(unit, files)
            manifest = self._manifest(unit, files, unit_meta, file_meta)
            transfers = self.journal.unreported(
                unit.id, ("upload", "adopt"), status="ok"
            )
            payload = self._payload(manifest, transfers)
            now = self.clock()
            state = REGISTERING if self.registrars else REGISTERED
            with self.journal.tx():
                # Compare-and-set: a finalize or upload worker may have reopened
                # the unit while the adapter ran.
                if not self.journal.set_unit(
                    unit.id,
                    expect=COMPLETE,
                    generation=unit.generation,
                    state=state,
                    metadata=json.dumps({"unit": unit_meta, "files": file_meta}),
                    registered_at=None if self.registrars else now,
                ):
                    continue
                for bound in self.registrars:
                    self.journal.enqueue(
                        unit_id=unit.id,
                        generation=unit.generation,
                        registrar=bound.key,
                        event="register",
                        required=bound.required,
                        payload=payload,
                        now=now,
                    )
                self.journal.mark_reported(t.id for t in transfers)
            if not self.registrars:
                self.report.registered.append(unit.key)
            queued.append(unit.key)
        return queued

    async def deliver(self) -> int:
        """Hand due outbox rows to their registrars; promote confirmed units."""
        delivered = 0
        failing: set[str] = set()
        for row in self.journal.due_outbox(self.name, self.clock()):
            # One failure per registrar per round: a catalog that is down would
            # otherwise cost a timeout for every unit waiting on it.
            if row.registrar in failing:
                continue
            bound = self._by_key.get(row.registrar)
            if bound is None:
                self.log.warning(
                    "Dropping a %s for %r, which is no longer configured",
                    row.event,
                    row.registrar,
                    extra=self._extra(),
                )
                self.journal.outbox_done(row.id, self.clock())
                continue
            data = json.loads(row.payload)
            manifest = Manifest.from_dict(data["manifest"])
            transfers = [TransferEvent.from_dict(item) for item in data["transfers"]]
            if self.dry_run:
                self.log.info(
                    "[dry run] would %s %s with %s",
                    row.event,
                    manifest.unit,
                    row.registrar,
                    extra=self._extra(manifest.unit),
                )
                self.journal.outbox_done(row.id, self.clock())
                delivered += 1
                continue
            call = (
                bound.registrar.register
                if row.event == "register"
                else bound.registrar.finalize
            )
            try:
                await asyncio.to_thread(call, manifest, transfers)
            except Exception as exc:
                failing.add(row.registrar)
                self._delivery_failed(row, manifest, exc)
                continue
            self.journal.outbox_done(row.id, self.clock())
            delivered += 1
        for unit in self.journal.units(self.name, [REGISTERING]):
            if not self.journal.required_done(unit.id, unit.generation, "register"):
                continue
            if self.journal.set_unit(
                unit.id,
                expect=REGISTERING,
                generation=unit.generation,
                state=REGISTERED,
                registered_at=self.clock(),
            ):
                self.report.registered.append(unit.key)
                self.log.info("Registered %s", unit.key, extra=self._extra(unit.key))
        return delivered

    def _delivery_failed(
        self, row: OutboxRow, manifest: Manifest, exc: BaseException
    ) -> None:
        error = describe_error(exc)
        attempts = row.attempts + 1
        self.journal.outbox_failed(
            row.id, error, self.clock() + self.cfg.retry.backoff(attempts)
        )
        self.report.delivery_failures += 1
        self.log.error(
            "Could not %s %s with %s (attempt %d): %s",
            row.event,
            manifest.unit,
            row.registrar,
            attempts,
            error,
            extra=self._extra(manifest.unit, error=error),
        )
        if attempts >= self.cfg.retry.alert_after and not row.alerted:
            self.journal.set_outbox_alerted(row.id)
            self.alerts.emit(
                Alert(
                    level="error",
                    title=(
                        f"{self.name}: cannot {row.event} {manifest.unit} with "
                        f"{row.registrar} ({attempts} attempts)"
                    ),
                    message=error,
                    source=self.name,
                    unit=manifest.unit,
                )
            )

    async def report_failures(self) -> None:
        """Hand failed transfers to registrars' audit trails, once each."""
        if self.dry_run:
            return
        failures = self.journal.unreported_failures(self.name)
        if not failures:
            return
        events = [_event(t) for t in failures]
        for bound in self.registrars:
            try:
                await asyncio.to_thread(bound.registrar.record_failures, events)
            except Exception:
                self.log.warning(
                    "%s could not record %d failed transfer(s)",
                    bound.key,
                    len(events),
                    exc_info=True,
                    extra=self._extra(),
                )
        self.journal.mark_reported(t.id for t in failures)

    # -- finalize -----------------------------------------------------------
    def finalize_candidates(self) -> list[UnitRow]:
        now = self.clock()
        return [
            unit
            for unit in self.journal.units(self.name, [REGISTERED])
            if unit.next_attempt is None or unit.next_attempt <= now
        ]

    async def finalize(self, unit_id: int) -> bool:
        """Keep, move or delete a registered unit's source files."""
        unit = self.journal.unit(unit_id)
        if unit.state != REGISTERED:
            return False
        action = self.cfg.finalize.action
        rows = [f for f in self.journal.files(unit.id) if f.state == UPLOADED]
        if self._moves is None:
            self._moves = asyncio.Semaphore(self.cfg.finalize.concurrency)
        results = await asyncio.gather(
            *(self._finalize_file(unit, row, action) for row in rows),
            return_exceptions=True,
        )
        changed = [r for r in results if isinstance(r, _ChangedAfterUploadError)]
        if changed:
            for change in changed:
                self.journal.reopen_for_file(change.file_id, change.reason)
            self.report.reopened.append((unit.key, changed[0].reason))
            self.log.warning(
                "Not finalizing %s: %s",
                unit.key,
                changed[0].reason,
                extra=self._extra(unit.key),
            )
            return False
        errors = [r for r in results if isinstance(r, BaseException)]
        if errors:
            self._finalize_failed(unit, errors[0])
            return False
        directory = self.layout.unit_dir(unit.key)
        if (
            action != "keep"
            and not self.dry_run
            and self.cfg.finalize.prune_empty_dirs
            and directory is not None
        ):
            await asyncio.to_thread(prune_empty_dirs, directory)
        files = [f for f in self.journal.files(unit.id) if f.state != GONE]
        stored = json.loads(unit.metadata) if unit.metadata else {}
        manifest = self._manifest(
            unit, files, stored.get("unit", {}), stored.get("files", {})
        )
        transfers = self.journal.unreported(unit.id, ("backup", "delete"), status="ok")
        payload = self._payload(manifest, transfers)
        now = self.clock()
        with self.journal.tx():
            if not self.journal.set_unit(
                unit.id,
                expect=REGISTERED,
                generation=unit.generation,
                state=FINALIZED,
                finalized_at=now,
                attempts=0,
                next_attempt=None,
                last_error=None,
            ):
                return False
            for bound in self.registrars:
                self.journal.enqueue(
                    unit_id=unit.id,
                    generation=unit.generation,
                    registrar=bound.key,
                    event="finalize",
                    required=bound.required,
                    payload=payload,
                    now=now,
                )
            self.journal.mark_reported(t.id for t in transfers)
        self.report.finalized.append(unit.key)
        verb = {
            "keep": "kept in place",
            "move_to_backup": f"moved to {self.cfg.finalize.backup_root}",
            "delete_after_upload": "deleted from the source",
        }[action]
        self.log.info(
            "Finalized %s: %d file(s) %s",
            unit.key,
            len(rows),
            verb,
            extra=self._extra(unit.key),
        )
        return True

    async def _finalize_file(self, unit: UnitRow, row: FileRow, action: str) -> None:
        assert self._moves is not None
        src = self.cfg.root / row.rel
        async with self._moves:
            try:
                stat = await asyncio.to_thread(os.stat, src)
            except FileNotFoundError:
                stat = None
            if stat is not None and (stat.st_size, stat.st_mtime_ns) != (
                row.up_size,
                row.up_mtime_ns,
            ):
                raise _ChangedAfterUploadError(
                    row.id, f"{row.within} changed after it was uploaded"
                )
            if action == "keep":
                self.journal.set_file(row.id, state=FINALIZED)
                return
            if self.dry_run:
                verb = "move" if action == "move_to_backup" else "delete"
                self.log.info(
                    "[dry run] would %s %s", verb, src, extra=self._extra(unit.key)
                )
                self.journal.set_file(row.id, state=FINALIZED)
                return
            if action == "delete_after_upload":
                if stat is not None:
                    await asyncio.to_thread(src.unlink)
                with self.journal.tx():
                    self.journal.set_file(row.id, state=FINALIZED, present=0)
                    if stat is not None:
                        self._transfer(unit, row, "delete", "ok", str(src), "")
                return
            backup_root = self.cfg.finalize.backup_root
            assert backup_root is not None
            dst = backup_root / row.rel
            if stat is None:
                # Moved already -- a crash between the move and recording it -- or
                # removed by someone else; it is in the store either way.
                kept = await asyncio.to_thread(_size_or_none, dst)
                self.journal.set_file(
                    row.id,
                    state=FINALIZED,
                    present=0,
                    backup_path=str(dst) if kept == row.up_size else None,
                )
                return
            started = time.perf_counter()
            try:
                nbytes = await asyncio.to_thread(move_file, src, dst)
            except Exception as exc:
                self._transfer(
                    unit,
                    row,
                    "backup",
                    "failed",
                    str(src),
                    str(dst),
                    error=describe_error(exc),
                )
                raise
            with self.journal.tx():
                self.journal.set_file(
                    row.id, state=FINALIZED, present=0, backup_path=str(dst)
                )
                self._transfer(
                    unit,
                    row,
                    "backup",
                    "ok",
                    str(src),
                    str(dst),
                    nbytes=nbytes,
                    duration_ms=int((time.perf_counter() - started) * 1000),
                )

    def _finalize_failed(self, unit: UnitRow, exc: BaseException) -> None:
        error = describe_error(exc)
        attempts = unit.attempts + 1
        self.journal.set_unit(
            unit.id,
            attempts=attempts,
            last_error=error,
            next_attempt=self.clock() + self.cfg.retry.backoff(attempts),
        )
        self.report.finalize_failures += 1
        self.log.error(
            "Finalizing %s failed (attempt %d): %s",
            unit.key,
            attempts,
            error,
            extra=self._extra(unit.key, error=error),
        )
        if attempts >= self.cfg.retry.alert_after and not unit.alerted:
            self.journal.set_unit(unit.id, alerted=1)
            self.alerts.emit(
                Alert(
                    level="error",
                    title=f"{self.name}: cannot finalize {unit.key} ({attempts}x)",
                    message=error,
                    source=self.name,
                    unit=unit.key,
                )
            )

    def next_due(self) -> float | None:
        """The earliest future time a timer -- settle, idle, a retry backoff --
        could make new work ready, so a stream can wake then instead of at its
        next poll."""
        now = self.clock()
        settle = self.cfg.settle.total_seconds()
        idle = self.cfg.unit_idle.total_seconds()
        times: list[float] = []
        for unit in self.journal.units(self.name, [OPEN, COMPLETE]):
            files = self._live(unit.id)
            if not files:
                continue
            if unit.state == OPEN:
                newest = max(f.mtime_ns for f in files) / 1e9
                quiet = 0.0 if unit.marker or unit.forced else idle
                times.append(max(newest + settle, unit.last_change + quiet))
            for f in files:
                if f.state == PENDING:
                    times.append(max(f.mtime_ns / 1e9 + settle, f.next_attempt or 0.0))
        for row in self.journal.due_outbox(self.name, float("inf")):
            if row.next_attempt is not None:
                times.append(row.next_attempt)
        for unit in self.journal.units(self.name, [REGISTERED]):
            if unit.next_attempt is not None:
                times.append(unit.next_attempt)
        future = [t for t in times if t > now]
        return min(future) if future else None

    # -- reporting ----------------------------------------------------------
    def waiting(self) -> dict[str, str]:
        """Why each unit with files is not finalized yet."""
        now = self.clock()
        out: dict[str, str] = {}
        states = [OPEN, COMPLETE, REGISTERING, REGISTERED]
        for unit in self.journal.units(self.name, states):
            if unit.state == OPEN:
                files = self._live(unit.id)
                why = self._why_open(unit, files, now)
                if files and why:
                    out[unit.key] = f"waiting: {why}"
            elif unit.state == COMPLETE:
                files = [f for f in self.journal.files(unit.id) if f.state != GONE]
                pending = [f for f in files if f.state == PENDING]
                failing = [f for f in pending if f.attempts]
                why = f"{len(pending)} of {len(files)} file(s) left to upload"
                if failing:
                    why += f" ({len(failing)} failing: {failing[0].last_error})"
                out[unit.key] = f"waiting: {why}"
            elif unit.state == REGISTERING:
                names = sorted(
                    {
                        o.registrar
                        for o in self.journal.outbox(unit.id)
                        if o.event == "register"
                        and o.done_at is None
                        and not o.superseded
                    }
                )
                out[unit.key] = "waiting on " + (", ".join(names) or "registrars")
            else:
                note = f" ({unit.last_error})" if unit.last_error else ""
                out[unit.key] = f"waiting: finalize pending{note}"
        return out

    # -- one pass -----------------------------------------------------------
    async def run_pass(self, stop: asyncio.Event | None = None) -> PassReport:
        """Scan everything and carry every unit as far as it can go now."""
        self.report = PassReport(self.name, started=self.clock())
        try:
            await self.scan()
            self.evaluate()
            await self._run_bounded(
                self.upload_candidates(),
                self.cfg.limits.files_in_flight,
                self.upload,
                stop,
            )
            await self.register_ready()
            await self.deliver()
            for unit in self.finalize_candidates():
                if stop is not None and stop.is_set():
                    break
                await self.finalize(unit.id)
            await self.deliver()
            await self.report_failures()
        except Exception as exc:
            self.report.errors.append(describe_error(exc))
            self.log.exception("Pass over %s failed", self.name, extra=self._extra())
        finally:
            self.journal.set_meta(f"last_pass:{self.name}", iso(self.clock()) or "")
        return self.take_report()
