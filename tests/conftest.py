from __future__ import annotations

import uuid
from collections.abc import Callable
from pathlib import Path
from typing import Any

import pytest

from ferry.config import FerryConfig, resolve_paths
from ferry.logs import reset_logging

from .helpers import CALLS, FAILURES, ON_REGISTER, FakeClock


@pytest.fixture(autouse=True)
def _reset_shared_state():
    yield
    CALLS.clear()
    FAILURES.clear()
    ON_REGISTER.clear()
    reset_logging()


@pytest.fixture
def clock() -> FakeClock:
    return FakeClock()


@pytest.fixture
def recorder() -> str:
    """A fresh recorder name, so tests never see each other's calls."""
    return f"rec-{uuid.uuid4().hex[:8]}"


@pytest.fixture
def paths(tmp_path: Path) -> dict[str, Path]:
    found = {
        "src": tmp_path / "src",
        "remote": tmp_path / "remote",
        "backup": tmp_path / "backup",
        "state": tmp_path / "state",
    }
    found["src"].mkdir()
    return found


@pytest.fixture
def make_config(
    tmp_path: Path, paths: dict[str, Path], recorder: str
) -> Callable[..., FerryConfig]:
    """Build a one-source config over the local ``remote`` store.

    Source settings default to ones that need no waiting: settle 1 s and idle
    2 s, which files written by ``helpers.write`` (an hour old) satisfy.
    """

    def build(*, config: dict[str, Any] | None = None, **source: Any) -> FerryConfig:
        merged: dict[str, Any] = {
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
        data: dict[str, Any] = {
            "node": "test-node",
            "state_dir": str(paths["state"]),
            "stores": {"remote": {"url": str(paths["remote"])}},
            "sources": [merged],
            **(config or {}),
        }
        return resolve_paths(FerryConfig.model_validate(data), tmp_path)

    return build
