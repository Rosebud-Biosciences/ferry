"""Cron schedules for ``mode: cron`` sources."""

from __future__ import annotations

import asyncio
from datetime import datetime
from zoneinfo import ZoneInfo

from cronsim import CronSim

# Waits are taken in naps no longer than this, re-reading the clock each time, so
# a wall-clock jump (DST, an NTP step, a PC waking from sleep) is noticed within
# one nap rather than after a stale monotonic sleep.
MAX_NAP_S = 30.0


class CronSchedule:
    """A cron expression read in an IANA time zone, or in the node's local time.

    Local time is compared as naive wall-clock readings: a match that a DST change
    skips fires when the clock jumps past it, and one it repeats fires once. A
    match the node slept through fires once on wake, not once per missed match.
    """

    def __init__(self, expr: str, timezone: str | None = None) -> None:
        self.expr = expr
        self.tz = ZoneInfo(timezone) if timezone else None

    def now(self) -> datetime:
        return datetime.now(self.tz) if self.tz else datetime.now()

    def next_after(self, when: datetime) -> datetime:
        return next(CronSim(self.expr, when))

    def upcoming(self, count: int, start: datetime | None = None) -> list[datetime]:
        matches = CronSim(self.expr, start or self.now())
        return [next(matches) for _ in range(count)]

    async def sleep_until(self, target: datetime, stop: asyncio.Event) -> bool:
        """Wait until ``target``; ``False`` if ``stop`` was set first."""
        while not stop.is_set():
            remaining = (target - self.now()).total_seconds()
            if remaining <= 0:
                return True
            try:
                await asyncio.wait_for(stop.wait(), timeout=min(remaining, MAX_NAP_S))
            except TimeoutError:
                continue
        return False
