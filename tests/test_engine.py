"""A source's pass end to end, against a local "remote" and a fake clock.

Files are written with real mtimes and the engine reads time from a
:class:`FakeClock`, so "an hour later" is one call rather than an hour.
"""

from __future__ import annotations

import hashlib
import json

from ferry.engine import PassReport
from ferry.journal import Journal
from ferry.runner import Agent

from .helpers import (
    CALLS,
    FAILURES,
    ON_REGISTER,
    age_tree,
    calls,
    files_under,
    write,
)


async def one_pass(cfg, clock, **kwargs) -> PassReport:
    with Agent(cfg, clock=clock, **kwargs) as agent:
        (report,) = await agent.run_once()
    return report


def unit_state(cfg, key: str) -> str | None:
    journal = Journal(cfg.journal_path)
    try:
        unit = journal.find_unit("inst", key)
        return None if unit is None else unit.state
    finally:
        journal.close()


def data_files(top) -> dict[str, bytes]:
    return {
        rel: body
        for rel, body in files_under(top).items()
        if not rel.endswith("_ferry_manifest.json")
    }


async def test_a_settled_unit_is_uploaded_registered_and_moved(
    make_config, paths, clock, recorder
):
    cfg = make_config(
        finalize={"action": "move_to_backup", "backup_root": str(paths["backup"])},
        registrars=[
            "manifest",
            {"use": "tests.helpers:Recorder", "options": {"name": recorder}},
        ],
    )
    write(paths["src"] / "plateA__1" / "img" / "a.tif", b"alpha")
    write(paths["src"] / "plateA__1" / "b.tif", b"bravo")
    age_tree(paths["src"])

    report = await one_pass(cfg, clock)

    unit_uri = paths["remote"].resolve().as_uri() + "/plateA__1"
    assert data_files(paths["remote"]) == {
        "plateA__1/img/a.tif": b"alpha",
        "plateA__1/b.tif": b"bravo",
    }
    assert files_under(paths["backup"]) == {
        "plateA__1/img/a.tif": b"alpha",
        "plateA__1/b.tif": b"bravo",
    }
    assert not (paths["src"] / "plateA__1").exists()  # emptied, then pruned

    (registered,) = calls(recorder, "register")
    assert registered.uri == unit_uri
    assert registered.generation == 1
    assert {f.path: f.sha256 for f in registered.files} == {
        "b.tif": hashlib.sha256(b"bravo").hexdigest(),
        "img/a.tif": hashlib.sha256(b"alpha").hexdigest(),
    }
    (_, _, transfers) = CALLS[recorder][0]
    assert sorted((t.op, t.status, t.rel_path) for t in transfers) == [
        ("upload", "ok", "plateA__1/b.tif"),
        ("upload", "ok", "plateA__1/img/a.tif"),
    ]
    (finalized,) = calls(recorder, "finalize")
    assert {f.path: f.backup_path for f in finalized.files} == {
        "b.tif": str(paths["backup"] / "plateA__1" / "b.tif"),
        "img/a.tif": str(paths["backup"] / "plateA__1" / "img" / "a.tif"),
    }
    # The manifest registrar rewrote the manifest on finalize, backup paths and all.
    stored = json.loads(
        (paths["remote"] / "plateA__1" / "_ferry_manifest.json").read_text()
    )
    assert {f["path"]: f["backup_path"] for f in stored["files"]} == {
        f.path: f.backup_path for f in finalized.files
    }
    assert report.registered == ["plateA__1"]
    assert report.finalized == ["plateA__1"]
    assert report.failures == 0
    assert unit_state(cfg, "plateA__1") == "finalized"


async def test_files_still_being_written_wait_for_settle_and_idle(
    make_config, paths, clock, recorder
):
    cfg = make_config(settle=60, unit_idle=120)
    write(paths["src"] / "u__1" / "a.tif", mtime=clock.now)
    age_tree(paths["src"], mtime=clock.now)

    report = await one_pass(cfg, clock)
    assert files_under(paths["remote"]) == {}
    assert report.waiting == {"u__1": "waiting: 1 of 1 file(s) still settling"}

    clock.advance(61)  # settled, but only 61 s quiet of the 120 s required
    report = await one_pass(cfg, clock)
    assert files_under(paths["remote"]) == {}
    assert "complete after 2m without changes" in report.waiting["u__1"]
    assert calls(recorder, "register") == []

    clock.advance(60)
    report = await one_pass(cfg, clock)
    assert data_files(paths["remote"]) == {"u__1/a.tif": b"data"}
    assert [m.unit for m in calls(recorder, "register")] == ["u__1"]
    assert report.waiting == {}


async def test_upload_unit_sends_nothing_until_the_whole_unit_is_complete(
    make_config, paths, clock, recorder
):
    cfg = make_config(upload="unit", settle=60, unit_idle=0)
    write(paths["src"] / "u__1" / "done.tif", b"done")
    write(paths["src"] / "u__1" / "busy.tif", b"busy", mtime=clock.now)

    await one_pass(cfg, clock)
    assert files_under(paths["remote"]) == {}

    clock.advance(61)
    await one_pass(cfg, clock)
    assert data_files(paths["remote"]) == {
        "u__1/busy.tif": b"busy",
        "u__1/done.tif": b"done",
    }
    (registered,) = calls(recorder, "register")
    assert sorted(f.path for f in registered.files) == ["busy.tif", "done.tif"]


async def test_upload_file_ships_settled_files_but_registers_only_once_complete(
    make_config, paths, clock, recorder
):
    cfg = make_config(upload="file", settle=60, unit_idle=0)
    write(paths["src"] / "u__1" / "done.tif", b"done")
    write(paths["src"] / "u__1" / "busy.tif", b"busy", mtime=clock.now)

    report = await one_pass(cfg, clock)
    assert data_files(paths["remote"]) == {"u__1/done.tif": b"done"}
    assert report.uploaded == 1
    assert calls(recorder, "register") == []

    clock.advance(61)
    report = await one_pass(cfg, clock)
    assert report.uploaded == 1  # only the newly settled file
    (registered,) = calls(recorder, "register")
    assert sorted(f.path for f in registered.files) == ["busy.tif", "done.tif"]


async def test_a_marker_completes_a_unit_without_waiting_for_idle(
    make_config, paths, clock, recorder
):
    cfg = make_config(unit_idle=3600, marker="done.txt", include=["*.tif"])
    write(paths["src"] / "u__1" / "a.tif")

    await one_pass(cfg, clock)
    assert calls(recorder, "register") == []

    write(paths["src"] / "u__1" / "done.txt")
    await one_pass(cfg, clock)
    (registered,) = calls(recorder, "register")
    assert [f.path for f in registered.files] == ["a.tif"]
    assert data_files(paths["remote"]) == {"u__1/a.tif": b"data"}


async def test_a_unit_still_being_written_at_pass_time_is_never_registered_partially(
    make_config, paths, clock, recorder
):
    """The nightly race: a pass that catches an acquisition mid-write must not
    register the files present so far -- the ones written later would never be
    uploaded."""
    cfg = make_config(settle=60, unit_idle=600)
    write(paths["src"] / "u__1" / "t001.tif", b"1")
    write(paths["src"] / "u__1" / "t002.tif", b"2", mtime=clock.now)
    age_tree(paths["src"] / "u__1", mtime=clock.now)

    await one_pass(cfg, clock)
    clock.advance(300)
    write(paths["src"] / "u__1" / "t003.tif", b"3", mtime=clock.now)
    age_tree(paths["src"] / "u__1", mtime=clock.now)
    await one_pass(cfg, clock)
    assert calls(recorder, "register") == []

    clock.advance(601)
    await one_pass(cfg, clock)
    (registered,) = calls(recorder, "register")
    assert sorted(f.path for f in registered.files) == [
        "t001.tif",
        "t002.tif",
        "t003.tif",
    ]


async def test_a_file_added_after_registration_registers_the_unit_again(
    make_config, paths, clock, recorder
):
    cfg = make_config()
    write(paths["src"] / "u__1" / "a.tif", b"a")
    write(paths["src"] / "u__1" / "b.tif", b"b")
    age_tree(paths["src"])
    await one_pass(cfg, clock)

    write(paths["src"] / "u__1" / "c.tif", b"c")
    clock.advance(10)
    report = await one_pass(cfg, clock)

    assert report.reopened == [("u__1", "new file c.tif")]
    first, second = calls(recorder, "register")
    assert (first.generation, second.generation) == (1, 2)
    assert sorted(f.path for f in second.files) == ["a.tif", "b.tif", "c.tif"]
    assert report.uploaded == 1  # only c.tif went up again
    assert unit_state(cfg, "u__1") == "finalized"


async def test_finalize_waits_for_a_required_registrar(
    make_config, paths, clock, recorder
):
    cfg = make_config(
        finalize={"action": "move_to_backup", "backup_root": str(paths["backup"])},
        retry={"initial": 30},
    )
    FAILURES[recorder] = 1
    write(paths["src"] / "u__1" / "a.tif")
    age_tree(paths["src"])

    report = await one_pass(cfg, clock)
    assert report.delivery_failures == 1
    assert data_files(paths["remote"]) == {"u__1/a.tif": b"data"}
    assert (paths["src"] / "u__1" / "a.tif").exists()
    assert files_under(paths["backup"]) == {}
    assert unit_state(cfg, "u__1") == "registering"

    clock.advance(31)
    report = await one_pass(cfg, clock)
    assert report.failures == 0
    assert files_under(paths["backup"]) == {"u__1/a.tif": b"data"}
    assert not (paths["src"] / "u__1").exists()
    assert [event for event, _, _ in CALLS[recorder]] == ["register", "finalize"]


async def test_an_optional_registrar_never_holds_up_finalize(
    make_config, paths, clock, recorder
):
    cfg = make_config(
        finalize={"action": "move_to_backup", "backup_root": str(paths["backup"])},
        registrars=[
            {
                "use": "tests.helpers:Recorder",
                "options": {"name": recorder},
                "required": False,
            }
        ],
        retry={"initial": 30},
    )
    FAILURES[recorder] = 1
    write(paths["src"] / "u__1" / "a.tif")
    age_tree(paths["src"])

    await one_pass(cfg, clock)
    assert files_under(paths["backup"]) == {"u__1/a.tif": b"data"}
    assert CALLS[recorder] == []

    clock.advance(31)
    await one_pass(cfg, clock)
    # The finalize waited behind the failed register, so the order still holds.
    assert [event for event, _, _ in CALLS[recorder]] == ["register", "finalize"]


async def test_files_already_in_the_store_are_adopted_not_uploaded_again(
    make_config, paths, clock, recorder
):
    write(paths["src"] / "u__1" / "a.tif", b"alpha")
    write(paths["src"] / "u__1" / "b.tif", b"bravo")
    age_tree(paths["src"])
    write(paths["remote"] / "u__1" / "a.tif", b"alpha")
    write(paths["remote"] / "u__1" / "b.tif", b"different size")
    cfg = make_config()

    report = await one_pass(cfg, clock)

    assert (report.adopted, report.uploaded) == (1, 1)
    assert (paths["remote"] / "u__1" / "b.tif").read_bytes() == b"bravo"
    (_, _, transfers) = CALLS[recorder][0]
    assert sorted((t.op, t.rel_path) for t in transfers) == [
        ("adopt", "u__1/a.tif"),
        ("upload", "u__1/b.tif"),
    ]


async def test_a_restarted_agent_resumes_without_uploading_or_registering_twice(
    make_config, paths, clock, recorder
):
    cfg = make_config()
    write(paths["src"] / "u__1" / "a.tif")
    write(paths["src"] / "u__1" / "b.tif")
    age_tree(paths["src"])
    with Agent(cfg, clock=clock) as agent:
        engine = agent.engines[0]
        await engine.scan()
        engine.evaluate()
        for row in engine.upload_candidates():
            await engine.upload(row.id)
        # ... and the process dies before registering.

    report = await one_pass(cfg, clock)
    assert report.uploaded == 0
    assert [m.unit for m in calls(recorder, "register")] == ["u__1"]
    report = await one_pass(cfg, clock)
    assert report.active is False
    assert len(calls(recorder, "register")) == 1


async def test_a_file_rewritten_after_upload_is_not_moved_until_resent(
    make_config, paths, clock, recorder
):
    cfg = make_config(
        finalize={"action": "move_to_backup", "backup_root": str(paths["backup"])}
    )
    source = write(paths["src"] / "u__1" / "a.tif", b"first")
    age_tree(paths["src"])
    # The instrument rewrites the file between its upload and the move.
    ON_REGISTER[recorder] = lambda: write(source, b"rewritten")

    report = await one_pass(cfg, clock)
    assert report.reopened == [("u__1", "a.tif changed after it was uploaded")]
    assert files_under(paths["backup"]) == {}
    assert source.read_bytes() == b"rewritten"

    clock.advance(10)
    await one_pass(cfg, clock)
    assert data_files(paths["remote"]) == {"u__1/a.tif": b"rewritten"}
    assert files_under(paths["backup"]) == {"u__1/a.tif": b"rewritten"}
    assert [m.generation for m in calls(recorder, "register")] == [1, 2]


async def test_delete_after_upload_removes_the_source_copy(
    make_config, paths, clock, recorder
):
    cfg = make_config(finalize={"action": "delete_after_upload"})
    write(paths["src"] / "u__1" / "a.tif")
    age_tree(paths["src"])

    await one_pass(cfg, clock)

    assert data_files(paths["remote"]) == {"u__1/a.tif": b"data"}
    assert not (paths["src"] / "u__1").exists()
    (finalized,) = calls(recorder, "finalize")
    assert [f.backup_path for f in finalized.files] == [None]


async def test_regex_units_group_a_flat_folder_by_name(
    make_config, paths, clock, recorder
):
    cfg = make_config(unit={"regex": r"^(?P<unit>[^_/]+)_"}, include=["*.csv"])
    write(paths["src"] / "RUN1_quant.csv", b"q1")
    write(paths["src"] / "RUN1_melt.csv", b"m1")
    write(paths["src"] / "RUN2_quant.csv", b"q2")
    write(paths["src"] / "notes.txt", b"ignored")
    age_tree(paths["src"])

    await one_pass(cfg, clock)

    assert data_files(paths["remote"]) == {
        "RUN1/RUN1_melt.csv": b"m1",
        "RUN1/RUN1_quant.csv": b"q1",
        "RUN2/RUN2_quant.csv": b"q2",
    }
    assert sorted(m.unit for m in calls(recorder, "register")) == ["RUN1", "RUN2"]


async def test_include_and_exclude_choose_what_travels(
    make_config, paths, clock, recorder
):
    cfg = make_config(include=["*.tif"])
    write(paths["src"] / "u__1" / "a.tif")
    write(paths["src"] / "u__1" / "sub" / "b.tif")
    write(paths["src"] / "u__1" / "notes.txt")
    write(paths["src"] / "u__1" / "~$a.tif")  # an Office-style lock file
    age_tree(paths["src"])

    await one_pass(cfg, clock)

    assert data_files(paths["remote"]) == {
        "u__1/a.tif": b"data",
        "u__1/sub/b.tif": b"data",
    }


async def test_a_failing_upload_backs_off_alerts_once_and_is_audited(
    make_config, paths, clock, recorder, monkeypatch
):
    async def unreachable(*args, **kwargs):
        raise ConnectionError("store unreachable")

    monkeypatch.setattr("ferry.engine.upload_file", unreachable)
    cfg = make_config(retry={"initial": 60, "alert_after": 2})
    write(paths["src"] / "u__1" / "a.tif")
    age_tree(paths["src"])

    with Agent(cfg, clock=clock) as agent:
        (first,) = await agent.run_once()
        (backing_off,) = await agent.run_once()
        clock.advance(61)
        (second,) = await agent.run_once()
        clock.advance(121)
        (third,) = await agent.run_once()
        alerts = [a for a in agent.alerts.sent if a.unit == "u__1"]

    assert [first.upload_failures, backing_off.upload_failures] == [1, 0]
    assert [second.upload_failures, third.upload_failures] == [1, 1]
    assert [a.title for a in alerts] == ["inst: u__1/a.tif failed to upload 2 times"]
    audited = [t for event, _, ts in CALLS[recorder] if event == "failures" for t in ts]
    assert [t.error for t in audited] == ["ConnectionError: store unreachable"] * 3
    assert calls(recorder, "register") == []


async def test_a_unit_open_too_long_raises_one_alert(
    make_config, paths, clock, recorder
):
    cfg = make_config(unit_idle=7 * 86400, max_open=60)
    write(paths["src"] / "u__1" / "a.tif")

    with Agent(cfg, clock=clock) as agent:
        await agent.run_once()
        assert [a for a in agent.alerts.sent if a.unit] == []
        clock.advance(61)
        await agent.run_once()
        clock.advance(61)
        await agent.run_once()
        alerts = [a for a in agent.alerts.sent if a.unit]

    (alert,) = alerts
    assert alert.title == "inst: unit u__1 has been incomplete for 61s"
    assert "ferry complete inst u__1" in alert.message


async def test_each_pass_digest_reaches_notifiers_as_one_alert(
    make_config, paths, clock, recorder
):
    cfg = make_config()
    write(paths["src"] / "u__1" / "a.tif")
    age_tree(paths["src"])

    with Agent(cfg, clock=clock) as agent:
        await agent.run_once()
        await agent.run_once()
        digests = [a for a in agent.alerts.sent if a.unit is None]

    first, idle = digests
    assert (first.level, first.source) == ("info", "inst")
    assert first.title == (
        "inst: 1 unit(s) registered, 1 finalized; 1 file(s) uploaded (4 B); "
        "no failures."
    )
    assert first.message == "• u__1 — registered, finalized"
    assert idle.title.startswith("inst: 0 unit(s) registered")


async def test_forcing_completion_skips_the_idle_wait(
    make_config, paths, clock, recorder
):
    cfg = make_config(unit_idle=3600)
    write(paths["src"] / "u__1" / "a.tif")

    await one_pass(cfg, clock)
    assert calls(recorder, "register") == []
    journal = Journal(cfg.journal_path)
    journal.force_complete("inst", "u__1")
    journal.close()

    await one_pass(cfg, clock)
    assert [m.unit for m in calls(recorder, "register")] == ["u__1"]


async def test_a_dry_run_changes_nothing(make_config, paths, clock, recorder):
    cfg = make_config(
        finalize={"action": "move_to_backup", "backup_root": str(paths["backup"])}
    )
    write(paths["src"] / "u__1" / "a.tif")
    age_tree(paths["src"])

    report = await one_pass(cfg, clock, dry_run=True)

    assert (report.uploaded, report.registered, report.finalized) == (
        1,
        ["u__1"],
        ["u__1"],
    )
    assert files_under(paths["remote"]) == {}
    assert files_under(paths["backup"]) == {}
    assert (paths["src"] / "u__1" / "a.tif").exists()
    assert CALLS[recorder] == []
    assert not cfg.journal_path.exists()


async def test_adapter_metadata_reaches_the_registrars(
    make_config, paths, clock, recorder
):
    cfg = make_config(adapter="tests.helpers:PlateAdapter")
    write(paths["src"] / "plateA__3" / "x.tif")
    age_tree(paths["src"])

    await one_pass(cfg, clock)

    (registered,) = calls(recorder, "register")
    assert registered.metadata == {"plate": "plateA", "files": 1}
    assert [f.metadata for f in registered.files] == [{"stem": "x"}]
