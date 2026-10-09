from datetime import timedelta
from pathlib import Path

import pytest
from pydantic import ValidationError

from ferry.config import (
    ConfigError,
    FerryConfig,
    interpolate,
    load_config,
    parse_duration,
    parse_window,
)


def minimal(**source) -> dict:
    return {
        "state_dir": "state",
        "stores": {"s3": {"url": "s3://bucket/raw"}},
        "sources": [{"name": "inst", "root": "data", "store": "s3", **source}],
    }


@pytest.mark.parametrize(
    ("text", "seconds"),
    [
        (90, 90),
        (1.5, 1.5),
        ("90", 90),
        ("90s", 90),
        ("15m", 900),
        ("1h30m", 5400),
        ("2d", 172800),
        ("500ms", 0.5),
        ("1h 30m", 5400),
    ],
)
def test_durations(text, seconds):
    assert parse_duration(text) == timedelta(seconds=seconds)


@pytest.mark.parametrize("text", ["", "fast", "10 parsecs", "5m garbage", "m5"])
def test_bad_durations_are_rejected(text):
    with pytest.raises(ValueError, match="not a duration"):
        parse_duration(text)


def test_sizes_and_bandwidth_read_like_people_write_them():
    cfg = FerryConfig.model_validate(
        minimal(
            limits={
                "single_put_max": "32MiB",
                "part_size": "16MB",
                "bandwidth": {"08:00-18:00": "50MB/s"},
            }
        )
    )
    limits = cfg.sources[0].limits
    assert limits.single_put_max == 32 * 1024**2
    assert limits.part_size == 16_000_000
    assert limits.bandwidth == {"08:00-18:00": 50_000_000}


def test_parts_below_the_s3_minimum_are_rejected():
    with pytest.raises(ValidationError, match="at least 5MiB"):
        FerryConfig.model_validate(minimal(limits={"part_size": "1MiB"}))


@pytest.mark.parametrize(
    ("text", "minutes"),
    [
        ("08:00-18:00", (480, 1080)),
        ("22:30-06:00", (1350, 360)),
        ("0:00-24:00", (0, 1440)),
    ],
)
def test_time_windows(text, minutes):
    assert parse_window(text) == minutes


@pytest.mark.parametrize("text", ["8-18", "25:00-26:00", "08:61-09:00", "24:30-01:00"])
def test_bad_time_windows_are_rejected(text):
    with pytest.raises(ValueError, match="not a time window"):
        parse_window(text)


def test_cron_mode_needs_a_valid_expression_and_stream_mode_none():
    FerryConfig.model_validate(minimal(mode="cron", cron="0 2 * * *"))
    with pytest.raises(ValidationError, match="needs a cron expression"):
        FerryConfig.model_validate(minimal(mode="cron"))
    with pytest.raises(ValidationError, match="invalid cron"):
        FerryConfig.model_validate(minimal(mode="cron", cron="0 25 * * *"))
    with pytest.raises(ValidationError, match="only applies to mode: cron"):
        FerryConfig.model_validate(minimal(mode="stream", cron="0 2 * * *"))


def test_unknown_timezones_are_rejected():
    FerryConfig.model_validate(
        minimal(mode="cron", cron="0 2 * * *", timezone="America/Los_Angeles")
    )
    with pytest.raises(ValidationError, match="unknown timezone"):
        FerryConfig.model_validate(
            minimal(mode="cron", cron="0 2 * * *", timezone="Mars/Olympus")
        )


def test_typos_are_errors_not_silently_ignored():
    with pytest.raises(ValidationError, match="Extra inputs are not permitted"):
        FerryConfig.model_validate(minimal(setle="60s"))


def test_sources_must_name_a_defined_store():
    data = minimal()
    data["sources"][0]["store"] = "nope"
    with pytest.raises(ValidationError, match="unknown store 'nope'"):
        FerryConfig.model_validate(data)


def test_move_to_backup_needs_a_backup_root():
    with pytest.raises(ValidationError, match="backup_root is required"):
        FerryConfig.model_validate(minimal(finalize={"action": "move_to_backup"}))


def test_unit_grouping_takes_dirs_or_a_regex_with_a_unit_group():
    cfg = FerryConfig.model_validate(minimal())
    assert cfg.sources[0].unit.levels == ["*"]
    cfg = FerryConfig.model_validate(minimal(unit={"dirs": "plates/*__*"}))
    assert cfg.sources[0].unit.levels == ["plates", "*__*"]
    with pytest.raises(ValidationError, match="named group"):
        FerryConfig.model_validate(minimal(unit={"regex": "^(RUN\\d+)_"}))
    with pytest.raises(ValidationError, match="either dirs or regex"):
        FerryConfig.model_validate(minimal(unit={"dirs": "*", "regex": "(?P<unit>x)"}))
    with pytest.raises(ValidationError, match="one glob per directory level"):
        FerryConfig.model_validate(minimal(unit={"dirs": "a/**"}))


def test_plugin_shorthand_and_distinct_registrar_names():
    cfg = FerryConfig.model_validate(
        minimal(registrars=["manifest", {"use": "manifest", "name": "copy"}])
    )
    assert [r.key for r in cfg.sources[0].registrars] == ["manifest", "copy"]
    with pytest.raises(ValidationError, match="distinct names"):
        FerryConfig.model_validate(minimal(registrars=["manifest", "manifest"]))


def test_environment_references_are_substituted():
    data = {"a": "${HOST}:${PORT:-9000}", "b": ["${HOST}"], "c": 1}
    assert interpolate(data, env={"HOST": "rgw"}) == {
        "a": "rgw:9000",
        "b": ["rgw"],
        "c": 1,
    }
    with pytest.raises(ConfigError, match="unset environment variable"):
        interpolate({"key": "${SECRET}"}, env={})


def test_loading_resolves_paths_against_the_config_file(tmp_path: Path):
    cfg_dir = tmp_path / "etc"
    cfg_dir.mkdir()
    path = cfg_dir / "ferry.yaml"
    path.write_text(
        "state_dir: state\n"
        "stores:\n"
        "  local: {url: ./out}\n"
        "sources:\n"
        "  - name: inst\n"
        "    root: ../data\n"
        "    store: local\n"
        "    finalize: {action: move_to_backup, backup_root: '${BACKUP}'}\n",
        encoding="utf-8",
    )
    cfg = load_config(path, env={"BACKUP": str(tmp_path / "backup")})
    assert cfg.state_dir == (cfg_dir / "state").resolve()
    assert cfg.stores["local"].url == str((cfg_dir / "out").resolve())
    assert cfg.sources[0].root == (tmp_path / "data").resolve()
    assert cfg.sources[0].finalize.backup_root == tmp_path / "backup"


def test_windows_paths_stay_absolute_when_validated_on_another_os(tmp_path: Path):
    path = tmp_path / "ferry.yaml"
    path.write_text(
        "state_dir: C:/ProgramData/ferry\n"
        "stores: {s3: {url: 's3://bucket'}}\n"
        "sources:\n"
        "  - {name: inst, root: 'D:/Data', store: s3}\n",
        encoding="utf-8",
    )
    cfg = load_config(path)
    assert cfg.state_dir.as_posix() == "C:/ProgramData/ferry"
    assert cfg.sources[0].root.as_posix() == "D:/Data"


def test_a_backup_root_inside_the_source_root_is_rejected(tmp_path: Path):
    path = tmp_path / "ferry.yaml"
    path.write_text(
        "state_dir: state\n"
        "stores: {local: {url: out}}\n"
        "sources:\n"
        "  - {name: inst, root: data, store: local,\n"
        "     finalize: {action: move_to_backup, backup_root: data/backup}}\n",
        encoding="utf-8",
    )
    with pytest.raises(ConfigError, match="must not contain each other"):
        load_config(path)


def test_missing_and_malformed_files_raise_config_errors(tmp_path: Path):
    with pytest.raises(ConfigError, match="not found"):
        load_config(tmp_path / "nope.yaml")
    bad = tmp_path / "bad.yaml"
    bad.write_text("sources: [unclosed", encoding="utf-8")
    with pytest.raises(ConfigError, match="not valid YAML"):
        load_config(bad)
