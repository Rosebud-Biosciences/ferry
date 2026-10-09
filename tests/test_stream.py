"""``ferry watch`` in real time, with settle and idle windows under a second."""

from __future__ import annotations

import asyncio
import contextlib
import threading
import time
from collections.abc import AsyncGenerator, Callable
from datetime import datetime, timedelta

from ferry.runner import Agent

from .helpers import age_tree, calls, files_under, write


async def eventually(predicate: Callable[[], bool], within: float = 15.0) -> None:
    deadline = time.monotonic() + within
    while time.monotonic() < deadline:
        if predicate():
            return
        await asyncio.sleep(0.05)
    raise AssertionError(f"not true after {within}s")


@contextlib.asynccontextmanager
async def watching(cfg) -> AsyncGenerator[Agent]:
    stop = asyncio.Event()
    with Agent(cfg) as agent:
        task = asyncio.create_task(agent.watch(stop))
        try:
            yield agent
        finally:
            stop.set()
            await asyncio.wait_for(task, timeout=30)


def shipped(paths) -> dict[str, bytes]:
    return {
        rel: body
        for rel, body in files_under(paths["remote"]).items()
        if not rel.endswith("_ferry_manifest.json")
    }


async def test_each_file_ships_as_it_settles_and_the_unit_registers_once_done(
    make_config, paths, recorder
):
    cfg = make_config(
        mode="stream",
        upload="file",
        settle=0.5,
        unit_idle=1.5,
        poll_interval=0.1,
        full_scan_interval=60,
        watch="poll",
    )
    unit = paths["src"] / "u__1"
    write(unit / "first.tif", b"first", age=0)
    writing = threading.Event()
    writing.set()

    def instrument() -> None:
        with open(unit / "growing.tif", "ab") as fh:
            while writing.is_set():
                fh.write(b"x")
                fh.flush()
                time.sleep(0.05)

    thread = threading.Thread(target=instrument)
    thread.start()
    try:
        async with watching(cfg):
            await eventually(lambda: "u__1/first.tif" in shipped(paths))
            await asyncio.sleep(1.0)
            assert "u__1/growing.tif" not in shipped(paths)
            assert calls(recorder, "register") == []

            writing.clear()
            thread.join()
            size = (unit / "growing.tif").stat().st_size
            await eventually(lambda: len(calls(recorder, "register")) == 1)
            assert shipped(paths)["u__1/growing.tif"] == b"x" * size
            (registered,) = calls(recorder, "register")
            assert sorted(f.path for f in registered.files) == [
                "first.tif",
                "growing.tif",
            ]
    finally:
        writing.clear()
        thread.join()


async def test_whole_unit_uploads_wait_until_the_unit_is_complete(
    make_config, paths, recorder
):
    cfg = make_config(
        mode="stream",
        upload="unit",
        settle=0.2,
        unit_idle=3.0,
        poll_interval=0.1,
        watch="poll",
    )
    write(paths["src"] / "u__1" / "a.tif", age=0)
    age_tree(paths["src"], age=0)

    async with watching(cfg):
        await asyncio.sleep(1.0)  # settled, but not yet quiet for 3 s
        assert shipped(paths) == {}
        await eventually(lambda: len(calls(recorder, "register")) == 1)
        assert shipped(paths) == {"u__1/a.tif": b"data"}


async def test_file_events_wake_a_stream_long_before_its_next_poll(
    make_config, paths, recorder
):
    cfg = make_config(
        mode="stream",
        upload="file",
        settle=0.2,
        unit_idle=0.5,
        poll_interval=600,
        full_scan_interval=600,
        watch="events",
    )
    async with watching(cfg):
        await asyncio.sleep(1.0)  # the watcher is running
        write(paths["src"] / "u__1" / "a.tif", age=0)
        await eventually(lambda: len(calls(recorder, "register")) == 1, within=30)
    assert shipped(paths) == {"u__1/a.tif": b"data"}


async def test_a_stream_wakes_when_a_file_settles_not_at_its_next_poll(
    make_config, paths, recorder
):
    cfg = make_config(
        mode="stream",
        upload="file",
        settle=0.5,
        unit_idle=0.5,
        poll_interval=600,
        full_scan_interval=600,
        watch="poll",
    )
    write(paths["src"] / "u__1" / "a.tif", age=0)
    age_tree(paths["src"], age=0)

    async with watching(cfg):
        await eventually(lambda: len(calls(recorder, "register")) == 1, within=10)
    assert shipped(paths) == {"u__1/a.tif": b"data"}


class EveryFewMoments:
    """A stand-in CronSchedule that matches every 0.2 s."""

    def __init__(self, expr: str, timezone: str | None = None) -> None:
        pass

    def now(self) -> datetime:
        return datetime.now()

    def next_after(self, when: datetime) -> datetime:
        return when + timedelta(seconds=0.2)

    async def sleep_until(self, target: datetime, stop: asyncio.Event) -> bool:
        with contextlib.suppress(TimeoutError):
            await asyncio.wait_for(
                stop.wait(), max((target - datetime.now()).total_seconds(), 0)
            )
        return not stop.is_set()


async def test_a_cron_source_runs_passes_on_its_schedule_inside_watch(
    make_config, paths, recorder, monkeypatch
):
    monkeypatch.setattr("ferry.runner.CronSchedule", EveryFewMoments)
    cfg = make_config(mode="cron", cron="0 2 * * *")
    write(paths["src"] / "u__1" / "a.tif")
    age_tree(paths["src"])

    async with watching(cfg):
        await eventually(lambda: len(calls(recorder, "register")) == 1)
        write(paths["src"] / "u__2" / "b.tif")
        age_tree(paths["src"] / "u__2")
        await eventually(lambda: len(calls(recorder, "register")) == 2)

    assert sorted(m.unit for m in calls(recorder, "register")) == ["u__1", "u__2"]
