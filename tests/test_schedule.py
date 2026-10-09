import asyncio
import time
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

from ferry.schedule import CronSchedule

LA = ZoneInfo("America/Los_Angeles")


def test_the_next_match_is_strictly_after_the_given_time():
    nightly = CronSchedule("0 2 * * *")
    assert nightly.next_after(datetime(2026, 10, 9, 2, 0)) == datetime(
        2026, 10, 10, 2, 0
    )
    assert nightly.next_after(datetime(2026, 10, 9, 1, 59, 30)) == datetime(
        2026, 10, 9, 2, 0
    )


def test_a_zoned_schedule_fires_once_a_day_across_dst_changes():
    # Spring forward: 02:30 does not exist on 2026-03-08, so it fires at the jump.
    spring = CronSchedule("30 2 * * *", "America/Los_Angeles").upcoming(
        3, datetime(2026, 3, 7, 12, 0, tzinfo=LA)
    )
    assert [d.isoformat() for d in spring] == [
        "2026-03-08T03:00:00-07:00",
        "2026-03-09T02:30:00-07:00",
        "2026-03-10T02:30:00-07:00",
    ]
    # Fall back: 01:30 happens twice on 2026-11-01, and fires once.
    fall = CronSchedule("30 1 * * *", "America/Los_Angeles").upcoming(
        2, datetime(2026, 10, 31, 12, 0, tzinfo=LA)
    )
    assert [d.isoformat() for d in fall] == [
        "2026-11-01T01:30:00-07:00",
        "2026-11-02T01:30:00-08:00",
    ]


async def test_a_missed_target_fires_at_once_and_stop_wins_over_waiting():
    schedule = CronSchedule("* * * * *")
    stop = asyncio.Event()
    assert await schedule.sleep_until(schedule.now() - timedelta(hours=3), stop)

    asyncio.get_running_loop().call_later(0.05, stop.set)
    started = time.monotonic()
    assert not await schedule.sleep_until(schedule.now() + timedelta(hours=1), stop)
    assert time.monotonic() - started < 5
