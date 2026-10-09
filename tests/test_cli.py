from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import yaml
from typer.testing import CliRunner

from ferry.cli import app
from ferry.lock import StateLock

from .helpers import FAILURES, age_tree, calls, files_under, write

runner = CliRunner()


def config_file(tmp_path: Path, paths, recorder: str, **source: Any) -> Path:
    data = {
        "node": "test-node",
        "state_dir": str(paths["state"]),
        "stores": {"remote": {"url": str(paths["remote"])}},
        "sources": [
            {
                "name": "inst",
                "root": str(paths["src"]),
                "store": "remote",
                "settle": 1,
                "unit_idle": 2,
                "registrars": [
                    {"use": "tests.helpers:Recorder", "options": {"name": recorder}}
                ],
                **source,
            }
        ],
    }
    path = tmp_path / "ferry.yaml"
    path.write_text(yaml.safe_dump(data), encoding="utf-8")
    return path


def test_config_validate_summarizes_each_source(tmp_path, paths, recorder):
    path = config_file(tmp_path, paths, recorder, mode="cron", cron="0 2 * * *")
    result = runner.invoke(app, ["config", "validate", "-c", str(path)])
    assert result.exit_code == 0, result.output
    assert "inst: " in result.output
    assert "cron '0 2 * * *'; upload per unit; finalize keep" in result.output
    assert result.output.rstrip().endswith("ok")


def test_config_validate_names_an_unknown_plugin(tmp_path, paths, recorder):
    path = config_file(tmp_path, paths, recorder, registrars=["nope"])
    result = runner.invoke(app, ["config", "validate", "-c", str(path)])
    assert result.exit_code == 2
    assert "unknown ferry.registrars plugin 'nope'" in result.output


def test_run_ships_a_unit_and_status_reports_it(tmp_path, paths, recorder):
    path = config_file(tmp_path, paths, recorder)
    write(paths["src"] / "u__1" / "a.tif", b"alpha")
    age_tree(paths["src"])

    result = runner.invoke(app, ["run", "-c", str(path)])
    assert result.exit_code == 0, result.output
    assert files_under(paths["remote"]) == {"u__1/a.tif": b"alpha"}

    status = runner.invoke(app, ["status", "-c", str(path), "--json"])
    assert status.exit_code == 0, status.output
    summary = json.loads(status.stdout)["inst"]
    assert summary["units"]["finalized"] == 1
    assert summary["pending_files"] == 0


def test_a_dry_run_exits_cleanly_and_ships_nothing(tmp_path, paths, recorder):
    path = config_file(tmp_path, paths, recorder)
    write(paths["src"] / "u__1" / "a.tif")
    age_tree(paths["src"])

    result = runner.invoke(app, ["run", "-c", str(path), "--dry-run"])

    assert result.exit_code == 0, result.output
    assert files_under(paths["remote"]) == {}
    assert calls(recorder, "register") == []


def test_run_exits_one_when_something_failed(tmp_path, paths, recorder):
    path = config_file(tmp_path, paths, recorder)
    FAILURES[recorder] = 1
    write(paths["src"] / "u__1" / "a.tif")
    age_tree(paths["src"])

    result = runner.invoke(app, ["run", "-c", str(path)])

    assert result.exit_code == 1
    assert "1 failure(s)" in result.output


def test_a_second_agent_on_one_state_directory_is_refused(tmp_path, paths, recorder):
    path = config_file(tmp_path, paths, recorder)
    with StateLock(paths["state"] / "ferry.lock"):
        result = runner.invoke(app, ["run", "-c", str(path)])
    assert result.exit_code == 2
    assert "another ferry agent is using" in result.output


def test_schedule_shows_the_next_passes(tmp_path, paths, recorder):
    path = config_file(tmp_path, paths, recorder, mode="cron", cron="15 2 * * *")
    result = runner.invoke(app, ["schedule", "-c", str(path), "-n", "3"])
    assert result.exit_code == 0, result.output
    lines = result.output.strip().splitlines()
    assert lines[0] == "inst: '15 2 * * *' (local time)"
    assert len(lines) == 4
    assert all(line.strip().endswith("T02:15") for line in lines[1:])


def test_complete_abandon_and_retry(tmp_path, paths, recorder):
    path = config_file(tmp_path, paths, recorder, unit_idle=3600)
    write(paths["src"] / "u__1" / "a.tif")
    write(paths["src"] / "u__2" / "b.tif")
    assert runner.invoke(app, ["run", "-c", str(path)]).exit_code == 0
    assert calls(recorder, "register") == []

    assert (
        runner.invoke(app, ["complete", "inst", "u__1", "-c", str(path)]).exit_code == 0
    )
    assert (
        runner.invoke(app, ["abandon", "inst", "u__2", "-c", str(path)]).exit_code == 0
    )
    assert runner.invoke(app, ["run", "-c", str(path)]).exit_code == 0
    assert [m.unit for m in calls(recorder, "register")] == ["u__1"]

    status = runner.invoke(app, ["status", "-c", str(path), "--json", "--units"])
    summary = json.loads(status.stdout)["inst"]
    assert (summary["units"]["finalized"], summary["units"]["abandoned"]) == (1, 1)

    retried = runner.invoke(app, ["retry", "-c", str(path), "-s", "inst", "-u", "u__2"])
    assert retried.exit_code == 0
    status = runner.invoke(app, ["status", "-c", str(path), "--json", "--units"])
    assert [u["unit"] for u in json.loads(status.stdout)["inst"]["unfinished"]] == [
        "u__2"
    ]

    missing = runner.invoke(app, ["complete", "inst", "nope", "-c", str(path)])
    assert missing.exit_code == 1


def test_service_definitions_run_watch_with_this_config(tmp_path, paths, recorder):
    path = config_file(tmp_path, paths, recorder)

    systemd = runner.invoke(
        app, ["service", "print", "-c", str(path), "--platform", "systemd"]
    )
    assert systemd.exit_code == 0, systemd.output
    assert "-m ferry watch --config" in systemd.output
    assert str(path) in systemd.output
    assert "WantedBy=default.target" in systemd.output

    task = runner.invoke(
        app, ["service", "print", "-c", str(path), "--platform", "windows"]
    )
    assert task.exit_code == 0, task.output
    assert "<BootTrigger>" in task.output
    assert "<LogonType>S4U</LogonType>" in task.output
    assert "<Count>999</Count>" in task.output
    system = runner.invoke(
        app, ["service", "print", "-c", str(path), "--platform", "windows", "--system"]
    )
    assert "<UserId>S-1-5-18</UserId>" in system.output
