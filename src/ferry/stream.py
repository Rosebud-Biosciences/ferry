"""``mode: stream``: drive a source's engine continuously.

Each tick rescans what may have changed, then completes, uploads, registers and
finalizes whatever is ready. Uploads and finalizations run on background
workers, so a large unit never holds up noticing the next one.

What gets rescanned: every unit not yet finalized, plus new unit directories
(a cheap listing), every ``poll_interval``; the whole tree every
``full_scan_interval``. OS file events (``watch: events``) are only hints that
make a rescan happen sooner -- they can be dropped or never arrive (network
shares), and the polling is what guarantees nothing is missed.
"""

from __future__ import annotations

import asyncio
import contextlib
import logging
import time
from collections.abc import Callable
from pathlib import Path

from .engine import SourceEngine, report_alert
from .journal import ACTIVE


class StreamLoop:
    def __init__(
        self,
        engine: SourceEngine,
        stop: asyncio.Event,
        *,
        digest_every: float = 3600.0,
        grace: float = 30.0,
        clock: Callable[[], float] = time.monotonic,
    ) -> None:
        self.engine = engine
        self.stop = stop
        self.digest_every = digest_every
        self.grace = grace
        self.clock = clock
        self.log = logging.getLogger(f"ferry.{engine.name}")
        self._wake = asyncio.Event()
        self._dirty = False
        self._hinted: set[str] = set()
        self._discover = False
        self._uploads: asyncio.Queue[int] = asyncio.Queue()
        self._queued: set[int] = set()
        self._finals: asyncio.Queue[int] = asyncio.Queue()
        self._finalizing: set[int] = set()
        self._busy = 0
        self._idle = asyncio.Event()
        self._idle.set()
        self._tasks: list[asyncio.Task[None]] = []

    async def run(self) -> None:
        cfg = self.engine.cfg
        poll = cfg.poll_interval.total_seconds()
        full_every = cfg.full_scan_interval.total_seconds()
        self._tasks = [
            asyncio.create_task(self._upload_worker(), name=f"ferry:{cfg.name}:upload")
            for _ in range(cfg.limits.files_in_flight)
        ]
        self._tasks.append(
            asyncio.create_task(
                self._finalize_worker(), name=f"ferry:{cfg.name}:finalize"
            )
        )
        watcher = (
            asyncio.create_task(self._watch(), name=f"ferry:{cfg.name}:events")
            if cfg.watch == "events"
            else None
        )
        self.log.info(
            "Streaming %s from %s (upload each %s; rescans every %ss)",
            cfg.name,
            cfg.root,
            "file as it settles" if cfg.upload == "file" else "unit once complete",
            f"{poll:g}",
            extra={"source": cfg.name},
        )
        now = self.clock()
        next_full = next_scan = now
        next_digest = now + self.digest_every
        try:
            while not self.stop.is_set():
                now = self.clock()
                try:
                    if now >= next_full:
                        self._hinted.clear()
                        self._discover = False
                        await self.engine.scan()
                        next_full = now + full_every
                        next_scan = now + poll
                    elif now >= next_scan or self._hinted or self._discover:
                        await self._hot_scan()
                        next_scan = now + poll
                    await self._tick()
                    # Rescan as soon as a settle, idle or backoff timer runs out,
                    # rather than at the next poll.
                    due = self.engine.next_due()
                    if due is not None:
                        wait = max(due - self.engine.clock(), 0.0) + 0.05
                        next_scan = min(next_scan, self.clock() + wait)
                except Exception:
                    self.log.exception("Stream tick for %s failed", cfg.name)
                if now >= next_digest:
                    self._digest()
                    next_digest = now + self.digest_every
                await self._wait(max(0.0, min(next_scan, next_full) - self.clock()))
        finally:
            if watcher is not None:
                watcher.cancel()
                await asyncio.gather(watcher, return_exceptions=True)
            await self._shutdown()
            self._digest()

    async def _hot_scan(self) -> None:
        engine = self.engine
        hinted, self._hinted = self._hinted, set()
        self._discover = False
        if not engine.layout.by_dirs:
            await engine.scan()  # a regex unit's files can be anywhere
            return
        units = engine.journal.units(engine.name)
        known = {unit.key: unit for unit in units}
        keys = {unit.key for unit in units if unit.state in ACTIVE} | hinted
        for key in await asyncio.to_thread(engine.layout.unit_keys):
            unit = known.get(key)
            if unit is None or not unit.present:
                keys.add(key)
        if keys:
            await engine.scan(keys)

    async def _tick(self) -> None:
        engine = self.engine
        engine.evaluate()
        for row in engine.upload_candidates():
            if row.id not in self._queued:
                self._queued.add(row.id)
                self._uploads.put_nowait(row.id)
        await engine.register_ready()
        await engine.deliver()
        for unit in engine.finalize_candidates():
            if unit.id not in self._finalizing:
                self._finalizing.add(unit.id)
                self._finals.put_nowait(unit.id)
        await engine.report_failures()

    def _poke(self) -> None:
        self._dirty = True
        self._wake.set()

    async def _wait(self, delay: float) -> None:
        if not self._wake.is_set() and not self.stop.is_set():
            woken = asyncio.ensure_future(self._wake.wait())
            stopped = asyncio.ensure_future(self.stop.wait())
            try:
                await asyncio.wait(
                    {woken, stopped},
                    timeout=delay,
                    return_when=asyncio.FIRST_COMPLETED,
                )
            finally:
                woken.cancel()
                stopped.cancel()
        self._wake.clear()
        if self._dirty and not self.stop.is_set():
            # Let a burst of finished uploads land before re-evaluating.
            await asyncio.sleep(0.2)
        self._dirty = False

    def _start(self) -> None:
        self._busy += 1
        self._idle.clear()

    def _finish(self) -> None:
        self._busy -= 1
        if not self._busy:
            self._idle.set()
        self._poke()

    async def _upload_worker(self) -> None:
        while True:
            file_id = await self._uploads.get()
            self._start()
            try:
                if not self.stop.is_set():
                    await self.engine.upload(file_id)
            except Exception:
                self.log.exception("Upload worker for %s failed", self.engine.name)
            finally:
                self._queued.discard(file_id)
                self._finish()

    async def _finalize_worker(self) -> None:
        while True:
            unit_id = await self._finals.get()
            self._start()
            try:
                if not self.stop.is_set():
                    await self.engine.finalize(unit_id)
            except Exception:
                self.log.exception("Finalize worker for %s failed", self.engine.name)
            finally:
                self._finalizing.discard(unit_id)
                self._finish()

    async def _watch(self) -> None:
        import watchfiles

        engine = self.engine
        root = engine.cfg.root
        try:
            async for changes in watchfiles.awatch(
                root,
                stop_event=self.stop,
                watch_filter=None,
                debounce=500,
                step=100,
                recursive=True,
                ignore_permission_denied=True,
            ):
                for _change, path in changes:
                    try:
                        rel = Path(path).relative_to(root).as_posix()
                    except ValueError:
                        continue
                    if rel == ".":
                        # FSEvents can report the root itself; look for new units.
                        self._discover = True
                        continue
                    key = engine.layout.key_for(rel)
                    if key is None:
                        self._discover = True
                    else:
                        self._hinted.add(key)
                self._wake.set()
        except Exception as exc:
            self.log.warning(
                "File events are unavailable for %s (%s); relying on polling",
                root,
                exc,
                extra={"source": engine.name},
            )

    async def _shutdown(self) -> None:
        # Let in-flight work finish (no new work starts once stop is set), then
        # cancel; an interrupted upload is simply retried by the next run.
        with contextlib.suppress(TimeoutError):
            await asyncio.wait_for(self._idle.wait(), timeout=self.grace)
        for task in self._tasks:
            task.cancel()
        await asyncio.gather(*self._tasks, return_exceptions=True)

    def _digest(self) -> None:
        report = self.engine.take_report()
        if report.active:
            self.engine.alerts.emit(report_alert(report))
