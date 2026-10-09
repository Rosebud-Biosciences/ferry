"""The journal's bookkeeping rules, driven with hand-built scans."""

import pytest

from ferry.journal import Journal
from ferry.units import FileObs, Scan, UnitObs


@pytest.fixture
def journal(tmp_path):
    j = Journal(tmp_path / "journal.sqlite3")
    yield j
    j.close()


def scan(*units: UnitObs, scope=None) -> Scan:
    return Scan({u.key: u for u in units}, scope)


def unit(key: str, *files: tuple[str, int, int], newest: int = 1) -> UnitObs:
    obs = UnitObs(key, newest_ns=newest)
    for within, size, mtime in files:
        rel = f"{key}/{within}"
        obs.files[rel] = FileObs(rel, within, size, mtime)
    return obs


def states(journal: Journal, key: str = "u") -> dict[str, str]:
    found = journal.find_unit("s", key)
    assert found is not None
    return {f.within: f.state for f in journal.files(found.id)}


def upload_all(journal: Journal, key: str = "u") -> None:
    found = journal.find_unit("s", key)
    assert found is not None
    for row in journal.files(found.id):
        journal.mark_uploaded(
            row.id, size=row.size, mtime_ns=row.mtime_ns, sha256="h", etag=None, now=1
        )


def finish(journal: Journal, key: str = "u") -> int:
    found = journal.find_unit("s", key)
    assert found is not None
    generation = journal.complete_unit(found.id, now=2)
    assert generation is not None
    journal.set_unit(found.id, state="finalized")
    return found.id


def test_new_units_and_files_start_open_and_pending(journal):
    changes = journal.apply_scan("s", scan(unit("u", ("a", 1, 1), ("b", 2, 2))), now=10)
    assert changes.new_units == ["u"]
    assert changes.new_files == 2
    assert states(journal) == {"a": "pending", "b": "pending"}
    found = journal.find_unit("s", "u")
    assert found is not None
    assert found.state == "open"


def test_an_unchanged_uploaded_file_stays_uploaded(journal):
    journal.apply_scan("s", scan(unit("u", ("a", 1, 1))), now=10)
    upload_all(journal)
    finish(journal)
    changes = journal.apply_scan("s", scan(unit("u", ("a", 1, 1))), now=11)
    assert changes.reopened == []
    assert states(journal) == {"a": "uploaded"}


def test_a_file_changed_after_upload_is_pending_again_and_reopens_its_unit(journal):
    journal.apply_scan("s", scan(unit("u", ("a", 1, 1))), now=10)
    upload_all(journal)
    unit_id = finish(journal)
    journal.enqueue(
        unit_id=unit_id,
        generation=1,
        registrar="r",
        event="finalize",
        required=True,
        payload="{}",
        now=3,
    )

    changes = journal.apply_scan("s", scan(unit("u", ("a", 5, 9))), now=11)

    assert changes.reopened == [("u", "a changed after upload")]
    assert states(journal) == {"a": "pending"}
    assert journal.unit(unit_id).state == "open"
    # The stale delivery is superseded rather than sent after the reopen.
    assert journal.due_outbox("s", now=100) == []


def test_a_pending_file_that_disappears_is_gone_but_an_uploaded_one_is_not(journal):
    journal.apply_scan("s", scan(unit("u", ("a", 1, 1), ("b", 1, 1))), now=10)
    found = journal.find_unit("s", "u")
    assert found is not None
    row = next(r for r in journal.files(found.id) if r.within == "b")
    journal.mark_uploaded(row.id, size=1, mtime_ns=1, sha256=None, etag=None, now=1)

    changes = journal.apply_scan("s", scan(unit("u")), now=11)

    assert changes.gone_files == ["u/a"]
    assert states(journal) == {"a": "gone", "b": "uploaded"}
    assert {r.within: r.present for r in journal.files(found.id)} == {"a": 0, "b": 0}


def test_a_scoped_scan_only_judges_absence_within_its_scope(journal):
    journal.apply_scan("s", scan(unit("u", ("a", 1, 1)), unit("v", ("a", 1, 1))), now=1)
    journal.apply_scan("s", scan(unit("u", ("a", 1, 1)), scope=frozenset({"u"})), now=2)
    assert states(journal, "v") == {"a": "pending"}
    journal.apply_scan("s", scan(scope=frozenset({"v"})), now=3)
    assert states(journal, "v") == {"a": "gone"}
    found = journal.find_unit("s", "v")
    assert found is not None
    assert found.state == "gone"


def test_an_upload_overtaken_by_a_newer_scan_stays_pending(journal):
    journal.apply_scan("s", scan(unit("u", ("a", 1, 1))), now=10)
    found = journal.find_unit("s", "u")
    assert found is not None
    (row,) = journal.files(found.id)
    journal.apply_scan("s", scan(unit("u", ("a", 2, 2))), now=11)

    journal.mark_uploaded(row.id, size=1, mtime_ns=1, sha256="old", etag=None, now=12)

    (row,) = journal.files(found.id)
    assert (row.state, row.up_size) == ("pending", 1)


def test_a_finalize_waits_behind_an_undelivered_register(journal):
    journal.apply_scan("s", scan(unit("u", ("a", 1, 1))), now=1)
    found = journal.find_unit("s", "u")
    assert found is not None
    register = journal.enqueue(
        unit_id=found.id,
        generation=1,
        registrar="r",
        event="register",
        required=True,
        payload="{}",
        now=1,
    )
    finalize = journal.enqueue(
        unit_id=found.id,
        generation=1,
        registrar="r",
        event="finalize",
        required=True,
        payload="{}",
        now=2,
    )
    other = journal.enqueue(
        unit_id=found.id,
        generation=1,
        registrar="q",
        event="register",
        required=False,
        payload="{}",
        now=3,
    )

    assert [row.id for row in journal.due_outbox("s", now=10)] == [register, other]
    journal.outbox_failed(register, "down", next_attempt=50)
    assert [row.id for row in journal.due_outbox("s", now=10)] == [other]
    assert not journal.required_done(found.id, 1, "register")
    journal.outbox_done(register, now=60)
    assert [row.id for row in journal.due_outbox("s", now=60)] == [finalize, other]
    assert journal.required_done(found.id, 1, "register")


def test_abandon_and_retry(journal):
    journal.apply_scan("s", scan(unit("u", ("a", 1, 1))), now=1)
    found = journal.find_unit("s", "u")
    assert found is not None
    (row,) = journal.files(found.id)
    journal.file_failed(row.id, "boom", next_attempt=99)

    journal.abandon("s", "u")
    journal.apply_scan("s", scan(unit("u", ("a", 1, 1), ("b", 1, 1))), now=2)
    assert states(journal) == {"a": "pending"}  # abandoned units are not tracked

    assert journal.retry("s", "u") == 1
    assert journal.unit(found.id).state == "open"
    (row,) = journal.files(found.id)
    assert (row.attempts, row.next_attempt) == (0, None)


def test_a_newer_journal_schema_is_refused(tmp_path):
    path = tmp_path / "journal.sqlite3"
    Journal(path).close()
    j = Journal(path)
    j.set_meta("schema_version", "99")
    j.close()
    with pytest.raises(RuntimeError, match="upgrade ferry"):
        Journal(path)
