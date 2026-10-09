"""The agent: everything one ferry process runs (``ferry run``, ``ferry watch``)."""

from __future__ import annotations

import asyncio
import contextlib
import logging
import time
from collections.abc import Callable, Coroutine, Sequence
from typing import Any

from . import plugins
from ._version import AGENT, __version__
from .adapters import Adapter
from .config import FerryConfig
from .engine import BoundRegistrar, PassReport, SourceEngine, report_alert
from .journal import Journal
from .lock import StateLock
from .logs import ship_logs, write_heartbeat
from .notify import Alerts, Notifier
from .registrars.base import Registrar, RegistrarContext
from .schedule import CronSchedule
from .stores import Location, open_store
from .stream import StreamLoop


class Agent:
    """Open with ``with Agent(config) as agent:``; a dry run locks nothing,
    opens no registrar and works on an in-memory copy of the journal."""

    def __init__(
        self,
        config: FerryConfig,
        *,
        dry_run: bool = False,
        clock: Callable[[], float] = time.time,
    ) -> None:
        self.config = config
        self.dry_run = dry_run
        self.clock = clock
        self.log = logging.getLogger("ferry")
        self.engines: list[SourceEngine] = []
        self.stores: dict[str, Location] = {}
        self.alerts = Alerts([], self.log)
        self._journal: Journal | None = None
        self._lock: StateLock | None = None
        self._opened: list[Registrar] = []

    @property
    def journal(self) -> Journal:
        if self._journal is None:
            raise RuntimeError("agent is not open")
        return self._journal

    def __enter__(self) -> Agent:
        self.open()
        return self

    def __exit__(self, *exc: object) -> None:
        self.close()

    def open(self) -> None:
        cfg = self.config
        cfg.state_dir.mkdir(parents=True, exist_ok=True)
        if not self.dry_run:
            self._lock = StateLock(cfg.state_dir / "ferry.lock")
            self._lock.acquire()
        try:
            self._journal = (
                Journal.snapshot(cfg.journal_path)
                if self.dry_run
                else Journal(cfg.journal_path)
            )
            notifiers = (
                []
                if self.dry_run
                else [
                    plugins.build(ref, plugins.NOTIFIERS, Notifier)
                    for ref in cfg.notifiers
                ]
            )
            self.alerts = Alerts(notifiers, self.log)
            self.stores = {
                name: open_store(store) for name, store in cfg.stores.items()
            }
            for source in cfg.sources:
                location = self.stores[source.store].child(source.prefix)
                bound = []
                for ref in source.registrars:
                    registrar = plugins.build(ref, plugins.REGISTRARS, Registrar)
                    if not self.dry_run:
                        registrar.open(
                            RegistrarContext(
                                node=cfg.node,
                                source=source,
                                location=location,
                                store=cfg.stores[source.store],
                                logger=logging.getLogger(
                                    f"ferry.{source.name}.{ref.key}"
                                ),
                            )
                        )
                        self._opened.append(registrar)
                    bound.append(BoundRegistrar(ref.key, ref.required, registrar))
                adapter = (
                    plugins.build(source.adapter, plugins.ADAPTERS, Adapter)
                    if source.adapter
                    else None
                )
                self.engines.append(
                    SourceEngine(
                        node=cfg.node,
                        cfg=source,
                        location=location,
                        journal=self.journal,
                        registrars=bound,
                        adapter=adapter,
                        alerts=self.alerts,
                        dry_run=self.dry_run,
                        clock=self.clock,
                    )
                )
        except BaseException:
            self.close()
            raise

    def close(self) -> None:
        for registrar in self._opened:
            try:
                registrar.close()
            except Exception:
                self.log.warning(
                    "Closing %s failed", type(registrar).__name__, exc_info=True
                )
        self._opened.clear()
        self.engines.clear()
        self.alerts.close()
        if self._journal is not None:
            self._journal.close()
            self._journal = None
        if self._lock is not None:
            self._lock.release()
            self._lock = None

    def engine(self, name: str) -> SourceEngine:
        for engine in self.engines:
            if engine.name == name:
                return engine
        known = ", ".join(engine.name for engine in self.engines)
        raise KeyError(f"no source named {name!r} (configured: {known})")

    def _log_report(self, report: PassReport) -> None:
        # A pass's digest reaches notifiers too (as `info` unless something
        # failed), and is logged by the alert.
        self.alerts.emit(report_alert(report))

    # -- one pass -----------------------------------------------------------
    async def run_once(
        self, sources: Sequence[str] | None = None, stop: asyncio.Event | None = None
    ) -> list[PassReport]:
        engines = [self.engine(name) for name in sources] if sources else self.engines
        reports = []
        for engine in engines:
            if self.dry_run:
                self.log.info(
                    "[dry run] %s: nothing will be uploaded, registered or moved",
                    engine.name,
                )
            report = await engine.run_pass(stop)
            self._log_report(report)
            reports.append(report)
        await self.ship_logs()
        return reports

    # -- the daemon ---------------------------------------------------------
    async def watch(self, stop: asyncio.Event) -> None:
        tasks = []
        for engine in self.engines:
            run = (
                (lambda e=engine: self._cron(e, stop))
                if engine.cfg.mode == "cron"
                else (lambda e=engine: StreamLoop(e, stop).run())
            )
            tasks.append(asyncio.create_task(self._guard(engine.name, run, stop)))
        tasks.append(asyncio.create_task(self._housekeeping(stop)))
        self.log.info(
            "ferry %s on %s: %d source(s) (%s)",
            __version__,
            self.config.node,
            len(self.engines),
            ", ".join(f"{e.name}: {e.cfg.mode}" for e in self.engines),
        )
        await asyncio.gather(*tasks)
        self.log.info("ferry stopped")

    async def _guard(
        self,
        name: str,
        run: Callable[[], Coroutine[Any, Any, None]],
        stop: asyncio.Event,
    ) -> None:
        while not stop.is_set():
            try:
                await run()
                return
            except Exception:
                self.log.exception("%s crashed; restarting it in 30s", name)
                with contextlib.suppress(TimeoutError):
                    await asyncio.wait_for(stop.wait(), timeout=30)

    async def _cron(self, engine: SourceEngine, stop: asyncio.Event) -> None:
        assert engine.cfg.cron is not None
        schedule = CronSchedule(engine.cfg.cron, engine.cfg.timezone)
        target = schedule.next_after(schedule.now())
        self.log.info(
            "%s: next pass at %s", engine.name, target.isoformat(timespec="minutes")
        )
        while await schedule.sleep_until(target, stop):
            report = await engine.run_pass(stop)
            self._log_report(report)
            target = schedule.next_after(schedule.now())
            self.log.info(
                "%s: next pass at %s", engine.name, target.isoformat(timespec="minutes")
            )

    async def _housekeeping(self, stop: asyncio.Event) -> None:
        ship = self.config.logs.ship
        if ship is None or self.dry_run:
            return
        location = self.stores[ship.store].child(ship.prefix)
        next_ship = next_beat = time.monotonic()
        while not stop.is_set():
            now = time.monotonic()
            if now >= next_ship:
                await self.ship_logs()
                next_ship = now + ship.interval.total_seconds()
            if ship.heartbeat is not None and now >= next_beat:
                try:
                    await write_heartbeat(location, self.config.node, self.heartbeat())
                except Exception:
                    self.log.warning("Could not write the heartbeat", exc_info=True)
                next_beat = now + ship.heartbeat.total_seconds()
            wake = (
                min(next_ship, next_beat) if ship.heartbeat is not None else next_ship
            )
            with contextlib.suppress(TimeoutError):
                await asyncio.wait_for(
                    stop.wait(), timeout=max(1.0, wake - time.monotonic())
                )

    def heartbeat(self) -> dict[str, Any]:
        return {
            "agent": AGENT,
            "sources": {e.name: self.journal.summary(e.name) for e in self.engines},
        }

    async def ship_logs(self) -> None:
        ship = self.config.logs.ship
        if ship is None or self.dry_run:
            return
        await ship_logs(
            journal=self.journal,
            location=self.stores[ship.store].child(ship.prefix),
            log_dir=self.config.log_dir,
            node=self.config.node,
            registrars=self._opened,
        )
