"""The journal: ferry's durable record of units, files, deliveries and transfers.

One SQLite database (WAL mode) per state directory. An agent touches it only from
its event-loop thread, between awaits, so every method here is a short
synchronous transaction. Operator commands (``ferry retry``, ``complete``, ...)
write from their own processes; SQLite serializes them against the agent.

Unit lifecycle::

    open -> complete -> registering -> registered -> finalized
      ^________|______________|______________|____________|   (a change reopens)

``abandoned`` (an operator gave up on it) and ``gone`` (it vanished before any
of it was uploaded) take a unit out of play. A file is ``pending`` until it is
``uploaded``, then ``finalized`` once the source copy is kept, moved or deleted;
a pending file that disappears is ``gone``.
"""

from __future__ import annotations

import sqlite3
from collections.abc import Generator, Iterable, Sequence
from contextlib import contextmanager
from dataclasses import dataclass, field, fields
from pathlib import Path
from typing import Any, TypeVar

from .units import Scan, UnitObs

OPEN = "open"
COMPLETE = "complete"
REGISTERING = "registering"
REGISTERED = "registered"
FINALIZED = "finalized"
ABANDONED = "abandoned"
GONE = "gone"
UNIT_STATES = (OPEN, COMPLETE, REGISTERING, REGISTERED, FINALIZED, ABANDONED, GONE)
# Units whose directories a stream rescans every poll.
ACTIVE = (OPEN, COMPLETE, REGISTERING, REGISTERED)

PENDING = "pending"
UPLOADED = "uploaded"
# FINALIZED doubles as the file state.

SCHEMA_VERSION = 1
SCHEMA = """
CREATE TABLE IF NOT EXISTS meta (key TEXT PRIMARY KEY, value TEXT NOT NULL);
CREATE TABLE IF NOT EXISTS unit (
    id INTEGER PRIMARY KEY,
    source TEXT NOT NULL,
    key TEXT NOT NULL,
    state TEXT NOT NULL DEFAULT 'open',
    generation INTEGER NOT NULL DEFAULT 0,
    first_seen REAL NOT NULL,
    last_change REAL NOT NULL,
    marker INTEGER NOT NULL DEFAULT 0,
    present INTEGER NOT NULL DEFAULT 1,
    forced INTEGER NOT NULL DEFAULT 0,
    adopted INTEGER NOT NULL DEFAULT 0,
    completed_at REAL,
    registered_at REAL,
    finalized_at REAL,
    metadata TEXT,
    attempts INTEGER NOT NULL DEFAULT 0,
    next_attempt REAL,
    last_error TEXT,
    alerted INTEGER NOT NULL DEFAULT 0,
    note TEXT,
    UNIQUE (source, key)
);
CREATE INDEX IF NOT EXISTS unit_by_state ON unit (source, state);
CREATE TABLE IF NOT EXISTS file (
    id INTEGER PRIMARY KEY,
    unit_id INTEGER NOT NULL REFERENCES unit (id) ON DELETE CASCADE,
    rel TEXT NOT NULL,
    within TEXT NOT NULL,
    size INTEGER NOT NULL,
    mtime_ns INTEGER NOT NULL,
    present INTEGER NOT NULL DEFAULT 1,
    state TEXT NOT NULL DEFAULT 'pending',
    up_size INTEGER,
    up_mtime_ns INTEGER,
    sha256 TEXT,
    etag TEXT,
    uploaded_at REAL,
    backup_path TEXT,
    attempts INTEGER NOT NULL DEFAULT 0,
    next_attempt REAL,
    last_error TEXT,
    alerted INTEGER NOT NULL DEFAULT 0,
    UNIQUE (unit_id, rel)
);
CREATE INDEX IF NOT EXISTS file_by_unit ON file (unit_id, state);
CREATE TABLE IF NOT EXISTS outbox (
    id INTEGER PRIMARY KEY,
    unit_id INTEGER NOT NULL REFERENCES unit (id) ON DELETE CASCADE,
    generation INTEGER NOT NULL,
    registrar TEXT NOT NULL,
    event TEXT NOT NULL,
    required INTEGER NOT NULL,
    payload TEXT NOT NULL,
    created_at REAL NOT NULL,
    attempts INTEGER NOT NULL DEFAULT 0,
    next_attempt REAL,
    last_error TEXT,
    alerted INTEGER NOT NULL DEFAULT 0,
    done_at REAL,
    superseded INTEGER NOT NULL DEFAULT 0
);
CREATE INDEX IF NOT EXISTS outbox_open ON outbox (done_at, superseded);
CREATE TABLE IF NOT EXISTS transfer (
    id INTEGER PRIMARY KEY,
    source TEXT NOT NULL,
    unit_id INTEGER REFERENCES unit (id) ON DELETE SET NULL,
    rel TEXT NOT NULL,
    op TEXT NOT NULL,
    status TEXT NOT NULL,
    src TEXT NOT NULL,
    dst TEXT NOT NULL,
    bytes INTEGER,
    duration_ms INTEGER,
    error TEXT,
    at REAL NOT NULL,
    reported INTEGER NOT NULL DEFAULT 0
);
CREATE INDEX IF NOT EXISTS transfer_by_unit ON transfer (unit_id, reported);
CREATE TABLE IF NOT EXISTS shipped_log (
    name TEXT PRIMARY KEY,
    uri TEXT NOT NULL,
    size INTEGER NOT NULL,
    sha256 TEXT NOT NULL,
    shipped_at REAL NOT NULL
);
"""


@dataclass(frozen=True)
class UnitRow:
    id: int
    source: str
    key: str
    state: str
    generation: int
    first_seen: float
    last_change: float
    marker: int
    present: int
    forced: int
    adopted: int
    completed_at: float | None
    registered_at: float | None
    finalized_at: float | None
    metadata: str | None
    attempts: int
    next_attempt: float | None
    last_error: str | None
    alerted: int
    note: str | None


@dataclass(frozen=True)
class FileRow:
    id: int
    unit_id: int
    rel: str
    within: str
    size: int
    mtime_ns: int
    present: int
    state: str
    up_size: int | None
    up_mtime_ns: int | None
    sha256: str | None
    etag: str | None
    uploaded_at: float | None
    backup_path: str | None
    attempts: int
    next_attempt: float | None
    last_error: str | None
    alerted: int


@dataclass(frozen=True)
class OutboxRow:
    id: int
    unit_id: int
    generation: int
    registrar: str
    event: str
    required: int
    payload: str
    created_at: float
    attempts: int
    next_attempt: float | None
    last_error: str | None
    alerted: int
    done_at: float | None
    superseded: int


@dataclass(frozen=True)
class TransferRow:
    id: int
    source: str
    unit_id: int | None
    rel: str
    op: str
    status: str
    src: str
    dst: str
    bytes: int | None
    duration_ms: int | None
    error: str | None
    at: float
    reported: int


@dataclass
class ScanChanges:
    new_units: list[str] = field(default_factory=list)
    new_files: int = 0
    changed_files: int = 0
    gone_files: list[str] = field(default_factory=list)
    reopened: list[tuple[str, str]] = field(default_factory=list)
    gone_units: list[str] = field(default_factory=list)


Row = TypeVar("Row", UnitRow, FileRow, OutboxRow, TransferRow)
_UNIT_FIELDS = {f.name for f in fields(UnitRow)}


class Journal:
    def __init__(
        self, path: Path | str, *, connection: sqlite3.Connection | None = None
    ):
        self.path = str(path)
        if connection is None:
            if self.path != ":memory:":
                Path(self.path).parent.mkdir(parents=True, exist_ok=True)
            connection = sqlite3.connect(
                self.path, isolation_level=None, check_same_thread=False, timeout=30
            )
        self.conn = connection
        self.conn.row_factory = sqlite3.Row
        if self.path != ":memory:":
            self.conn.execute("PRAGMA journal_mode=WAL")
        self.conn.execute("PRAGMA synchronous=NORMAL")
        self.conn.execute("PRAGMA foreign_keys=ON")
        self.conn.execute("PRAGMA busy_timeout=30000")
        self._depth = 0
        self._migrate()

    @classmethod
    def snapshot(cls, path: Path | str) -> Journal:
        """An in-memory copy of the journal at ``path``, for dry runs."""
        memory = sqlite3.connect(
            ":memory:", isolation_level=None, check_same_thread=False
        )
        if Path(path).exists():
            source = sqlite3.connect(str(path))
            try:
                source.backup(memory)
            finally:
                source.close()
        return cls(":memory:", connection=memory)

    def close(self) -> None:
        self.conn.close()

    def _migrate(self) -> None:
        # executescript commits any open transaction first, so it runs on its
        # own; every statement in SCHEMA is idempotent.
        self.conn.executescript(SCHEMA)
        with self.tx():
            row = self.conn.execute(
                "SELECT value FROM meta WHERE key = 'schema_version'"
            ).fetchone()
            if row is None:
                self.conn.execute(
                    "INSERT INTO meta (key, value) VALUES ('schema_version', ?)",
                    (str(SCHEMA_VERSION),),
                )
            elif int(row["value"]) > SCHEMA_VERSION:
                raise RuntimeError(
                    f"journal {self.path} has schema {row['value']}; this ferry "
                    f"understands {SCHEMA_VERSION} -- upgrade ferry"
                )

    @contextmanager
    def tx(self) -> Generator[None]:
        """One write transaction; nested calls join the outer one."""
        if self._depth:
            self._depth += 1
            try:
                yield
            finally:
                self._depth -= 1
            return
        self.conn.execute("BEGIN IMMEDIATE")
        self._depth = 1
        try:
            yield
        except BaseException:
            self.conn.execute("ROLLBACK")
            raise
        else:
            self.conn.execute("COMMIT")
        finally:
            self._depth = 0

    def _x(self, sql: str, params: Sequence[Any] = ()) -> sqlite3.Cursor:
        return self.conn.execute(sql, params)

    def _returning(self, sql: str, params: Sequence[Any]) -> sqlite3.Row | None:
        # Drain the cursor: a RETURNING statement left mid-step would make the
        # enclosing COMMIT fail with "SQL statements in progress".
        rows = self.conn.execute(sql, params).fetchall()
        return rows[0] if rows else None

    @staticmethod
    def _all(cls: type[Row], cursor: sqlite3.Cursor) -> list[Row]:
        return [cls(**dict(row)) for row in cursor.fetchall()]

    @staticmethod
    def _one(cls: type[Row], cursor: sqlite3.Cursor) -> Row | None:
        row = cursor.fetchone()
        return None if row is None else cls(**dict(row))

    # -- meta ---------------------------------------------------------------
    def get_meta(self, key: str) -> str | None:
        row = self._x("SELECT value FROM meta WHERE key = ?", (key,)).fetchone()
        return None if row is None else row["value"]

    def set_meta(self, key: str, value: str) -> None:
        self._x(
            "INSERT INTO meta (key, value) VALUES (?, ?) "
            "ON CONFLICT (key) DO UPDATE SET value = excluded.value",
            (key, value),
        )

    # -- reads --------------------------------------------------------------
    def units(self, source: str, states: Iterable[str] | None = None) -> list[UnitRow]:
        if states is None:
            cursor = self._x(
                "SELECT * FROM unit WHERE source = ? ORDER BY id", (source,)
            )
        else:
            wanted = list(states)
            marks = ",".join("?" * len(wanted))
            cursor = self._x(
                f"SELECT * FROM unit WHERE source = ? AND state IN ({marks}) "
                "ORDER BY first_seen, id",
                (source, *wanted),
            )
        return self._all(UnitRow, cursor)

    def unit(self, unit_id: int) -> UnitRow:
        row = self._one(UnitRow, self._x("SELECT * FROM unit WHERE id = ?", (unit_id,)))
        if row is None:
            raise KeyError(unit_id)
        return row

    def find_unit(self, source: str, key: str) -> UnitRow | None:
        return self._one(
            UnitRow,
            self._x("SELECT * FROM unit WHERE source = ? AND key = ?", (source, key)),
        )

    def files(self, unit_id: int) -> list[FileRow]:
        return self._all(
            FileRow,
            self._x("SELECT * FROM file WHERE unit_id = ? ORDER BY rel", (unit_id,)),
        )

    def file(self, file_id: int) -> FileRow:
        row = self._one(FileRow, self._x("SELECT * FROM file WHERE id = ?", (file_id,)))
        if row is None:
            raise KeyError(file_id)
        return row

    def upload_candidates(
        self,
        source: str,
        *,
        now: float,
        settled_ns: int,
        unit_states: Sequence[str],
    ) -> list[FileRow]:
        """Pending files settled by ``settled_ns`` (mtime) and not backing off."""
        marks = ",".join("?" * len(unit_states))
        cursor = self._x(
            "SELECT f.* FROM file f JOIN unit u ON u.id = f.unit_id "
            f"WHERE u.source = ? AND u.state IN ({marks}) "
            "AND f.state = 'pending' AND f.present = 1 AND f.mtime_ns <= ? "
            "AND (f.next_attempt IS NULL OR f.next_attempt <= ?) "
            "ORDER BY u.first_seen, u.id, f.rel",
            (source, *unit_states, settled_ns, now),
        )
        return self._all(FileRow, cursor)

    # -- scans --------------------------------------------------------------
    def apply_scan(self, source: str, scan: Scan, now: float) -> ScanChanges:
        """Fold one scan into the journal and report what it changed."""
        changes = ScanChanges()
        with self.tx():
            known = {unit.key: unit for unit in self.units(source)}
            for key, obs in scan.units.items():
                self._observe_unit(source, known.get(key), obs, now, changes)
            if scan.scope is None:
                unseen = [unit for key, unit in known.items() if key not in scan.units]
            else:
                unseen = [
                    known[key]
                    for key in scan.scope
                    if key in known and key not in scan.units
                ]
            for unit in unseen:
                self._unit_missing(unit, changes)
        return changes

    def _observe_unit(
        self,
        source: str,
        unit: UnitRow | None,
        obs: UnitObs,
        now: float,
        changes: ScanChanges,
    ) -> None:
        newest = obs.newest_ns / 1e9 if obs.newest_ns else now
        if unit is None:
            cursor = self._x(
                "INSERT INTO unit (source, key, first_seen, last_change, marker) "
                "VALUES (?, ?, ?, ?, ?)",
                (source, obs.key, now, newest, int(obs.marker)),
            )
            unit_id, state, existing = cursor.lastrowid, OPEN, {}
            changes.new_units.append(obs.key)
        else:
            if unit.state == ABANDONED:
                return
            unit_id, state = unit.id, unit.state
            self._x(
                "UPDATE unit SET last_change = ?, marker = ?, present = 1 WHERE id = ?",
                (newest, int(obs.marker), unit_id),
            )
            existing = {row.rel: row for row in self.files(unit_id)}
        assert unit_id is not None
        reason = self._observe_files(unit_id, obs, existing, changes)
        if state == GONE and obs.files:
            reason = reason or "reappeared"
        if reason and state != OPEN:
            self._reopen(unit_id, reason)
            changes.reopened.append((obs.key, reason))

    def _observe_files(
        self,
        unit_id: int,
        obs: UnitObs,
        existing: dict[str, FileRow],
        changes: ScanChanges,
    ) -> str | None:
        """Update the unit's file rows; return why completion no longer holds."""
        reason: str | None = None
        for rel, seen in obs.files.items():
            row = existing.pop(rel, None)
            if row is None:
                self._x(
                    "INSERT INTO file (unit_id, rel, within, size, mtime_ns) "
                    "VALUES (?, ?, ?, ?, ?)",
                    (unit_id, rel, seen.within, seen.size, seen.mtime_ns),
                )
                changes.new_files += 1
                reason = reason or f"new file {seen.within}"
                continue
            stats = (seen.size, seen.mtime_ns)
            moved = (row.size, row.mtime_ns) != stats
            matches_upload = (row.up_size, row.up_mtime_ns) == stats
            if row.state == PENDING:
                if moved or not row.present:
                    self._set_stats(row.id, stats, PENDING)
                if moved:
                    changes.changed_files += 1
                    reason = reason or f"{seen.within} changed"
            elif row.state in (UPLOADED, FINALIZED):
                if matches_upload:
                    if moved or not row.present:
                        self._set_stats(row.id, stats, row.state)
                else:
                    self._set_stats(row.id, stats, PENDING, reset=True)
                    changes.changed_files += 1
                    reason = reason or f"{seen.within} changed after upload"
            else:  # gone, and back
                state = UPLOADED if matches_upload else PENDING
                self._set_stats(row.id, stats, state, reset=state == PENDING)
                if state == PENDING:
                    reason = reason or f"{seen.within} reappeared"
        for row in existing.values():
            if not row.present:
                continue
            if row.state == PENDING:
                self._x(
                    "UPDATE file SET state = 'gone', present = 0 WHERE id = ?",
                    (row.id,),
                )
                changes.gone_files.append(row.rel)
                reason = reason or f"{row.within} disappeared"
            else:
                self._x("UPDATE file SET present = 0 WHERE id = ?", (row.id,))
        return reason

    def _set_stats(
        self, file_id: int, stats: tuple[int, int], state: str, *, reset: bool = False
    ) -> None:
        if reset:
            self._x(
                "UPDATE file SET size = ?, mtime_ns = ?, present = 1, state = ?, "
                "attempts = 0, next_attempt = NULL, last_error = NULL, alerted = 0 "
                "WHERE id = ?",
                (*stats, state, file_id),
            )
        else:
            self._x(
                "UPDATE file SET size = ?, mtime_ns = ?, present = 1, state = ? "
                "WHERE id = ?",
                (*stats, state, file_id),
            )

    def _unit_missing(self, unit: UnitRow, changes: ScanChanges) -> None:
        if unit.state in (ABANDONED, GONE) or not unit.present:
            return
        rows = self.files(unit.id)
        if not rows:
            self._x("DELETE FROM unit WHERE id = ?", (unit.id,))
            return
        self._x("UPDATE unit SET present = 0 WHERE id = ?", (unit.id,))
        self._x(
            "UPDATE file SET present = 0, "
            "state = CASE state WHEN 'pending' THEN 'gone' ELSE state END "
            "WHERE unit_id = ?",
            (unit.id,),
        )
        if unit.state == OPEN and not any(
            r.state in (UPLOADED, FINALIZED) for r in rows
        ):
            self._x("UPDATE unit SET state = 'gone' WHERE id = ?", (unit.id,))
            changes.gone_units.append(unit.key)

    def _reopen(self, unit_id: int, reason: str) -> None:
        self._x(
            "UPDATE unit SET state = 'open', completed_at = NULL, forced = 0, "
            "attempts = 0, next_attempt = NULL, last_error = NULL, alerted = 0, "
            "note = ? WHERE id = ?",
            (f"reopened: {reason}", unit_id),
        )
        self._x(
            "UPDATE outbox SET superseded = 1 WHERE unit_id = ? AND done_at IS NULL",
            (unit_id,),
        )

    # -- units --------------------------------------------------------------
    def set_unit(
        self,
        unit_id: int,
        *,
        expect: str | None = None,
        generation: int | None = None,
        **values: Any,
    ) -> bool:
        """Update unit columns, optionally only if it is still in ``expect``.

        The compare-and-set is what keeps a slow finalize from overwriting a
        reopen that a scan recorded while it was moving files.
        """
        unknown = set(values) - _UNIT_FIELDS
        if unknown:
            raise ValueError(f"unknown unit columns: {sorted(unknown)}")
        sets = ", ".join(f"{name} = ?" for name in values)
        sql = f"UPDATE unit SET {sets} WHERE id = ?"
        params: list[Any] = [*values.values(), unit_id]
        if expect is not None:
            sql += " AND state = ?"
            params.append(expect)
        if generation is not None:
            sql += " AND generation = ?"
            params.append(generation)
        return self._x(sql, params).rowcount == 1

    def complete_unit(self, unit_id: int, now: float) -> int | None:
        """Mark an open unit complete; return its new generation."""
        row = self._returning(
            "UPDATE unit SET state = 'complete', generation = generation + 1, "
            "completed_at = ?, note = NULL, alerted = 0 WHERE id = ? "
            "AND state = 'open' RETURNING generation",
            (now, unit_id),
        )
        return None if row is None else int(row["generation"])

    # -- files --------------------------------------------------------------
    def mark_uploaded(
        self,
        file_id: int,
        *,
        size: int,
        mtime_ns: int,
        sha256: str | None,
        etag: str | None,
        now: float,
    ) -> None:
        # Only `uploaded` if a scan has not seen the file move on since; the
        # upload is still recorded, and the next settle uploads the newer bytes.
        self._x(
            "UPDATE file SET up_size = ?, up_mtime_ns = ?, sha256 = ?, etag = ?, "
            "uploaded_at = ?, attempts = 0, next_attempt = NULL, last_error = NULL, "
            "alerted = 0, state = CASE WHEN size = ? AND mtime_ns = ? "
            "THEN 'uploaded' ELSE state END WHERE id = ?",
            (size, mtime_ns, sha256, etag, now, size, mtime_ns, file_id),
        )

    def file_failed(self, file_id: int, error: str, next_attempt: float) -> int:
        row = self._returning(
            "UPDATE file SET attempts = attempts + 1, last_error = ?, next_attempt = ? "
            "WHERE id = ? RETURNING attempts",
            (error, next_attempt, file_id),
        )
        return 0 if row is None else int(row["attempts"])

    def defer_file(self, file_id: int, until: float) -> None:
        self._x("UPDATE file SET next_attempt = ? WHERE id = ?", (until, file_id))

    def set_file(self, file_id: int, **values: Any) -> None:
        allowed = {"state", "present", "backup_path", "alerted", "size", "mtime_ns"}
        unknown = set(values) - allowed
        if unknown:
            raise ValueError(f"unknown file columns: {sorted(unknown)}")
        sets = ", ".join(f"{name} = ?" for name in values)
        self._x(f"UPDATE file SET {sets} WHERE id = ?", [*values.values(), file_id])

    def reopen_for_file(self, file_id: int, reason: str) -> None:
        """A file was found changed outside a scan: pend it and reopen its unit."""
        row = self.file(file_id)
        with self.tx():
            self._x(
                "UPDATE file SET state = 'pending', attempts = 0, next_attempt = NULL, "
                "last_error = NULL WHERE id = ?",
                (file_id,),
            )
            if self.unit(row.unit_id).state != OPEN:
                self._reopen(row.unit_id, reason)

    # -- transfers ----------------------------------------------------------
    def add_transfer(
        self,
        *,
        source: str,
        unit_id: int | None,
        rel: str,
        op: str,
        status: str,
        src: str,
        dst: str,
        at: float,
        nbytes: int | None = None,
        duration_ms: int | None = None,
        error: str | None = None,
    ) -> int:
        cursor = self._x(
            "INSERT INTO transfer (source, unit_id, rel, op, status, src, dst, bytes, "
            "duration_ms, error, at) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
            (
                source,
                unit_id,
                rel,
                op,
                status,
                src,
                dst,
                nbytes,
                duration_ms,
                error,
                at,
            ),
        )
        assert cursor.lastrowid is not None
        return cursor.lastrowid

    def unreported(
        self, unit_id: int, ops: Sequence[str], *, status: str | None = None
    ) -> list[TransferRow]:
        marks = ",".join("?" * len(ops))
        sql = (
            f"SELECT * FROM transfer WHERE unit_id = ? AND reported = 0 "
            f"AND op IN ({marks})"
        )
        params: list[Any] = [unit_id, *ops]
        if status is not None:
            sql += " AND status = ?"
            params.append(status)
        return self._all(TransferRow, self._x(sql + " ORDER BY id", params))

    def unreported_failures(self, source: str) -> list[TransferRow]:
        return self._all(
            TransferRow,
            self._x(
                "SELECT * FROM transfer WHERE source = ? AND status = 'failed' "
                "AND reported = 0 ORDER BY id",
                (source,),
            ),
        )

    def mark_reported(self, transfer_ids: Iterable[int]) -> None:
        self.conn.executemany(
            "UPDATE transfer SET reported = 1 WHERE id = ?",
            [(tid,) for tid in transfer_ids],
        )

    # -- outbox -------------------------------------------------------------
    def enqueue(
        self,
        *,
        unit_id: int,
        generation: int,
        registrar: str,
        event: str,
        required: bool,
        payload: str,
        now: float,
    ) -> int:
        cursor = self._x(
            "INSERT INTO outbox (unit_id, generation, registrar, event, required, "
            "payload, created_at) VALUES (?, ?, ?, ?, ?, ?, ?)",
            (unit_id, generation, registrar, event, int(required), payload, now),
        )
        assert cursor.lastrowid is not None
        return cursor.lastrowid

    def due_outbox(self, source: str, now: float) -> list[OutboxRow]:
        """Undelivered rows whose backoff has passed, oldest first.

        A row waits while an older one for the same unit and registrar is still
        undelivered, so a registrar never sees a unit finalized before it has
        seen it registered.
        """
        return self._all(
            OutboxRow,
            self._x(
                "SELECT o.* FROM outbox o JOIN unit u ON u.id = o.unit_id "
                "WHERE u.source = ? AND o.done_at IS NULL AND o.superseded = 0 "
                "AND (o.next_attempt IS NULL OR o.next_attempt <= ?) "
                "AND NOT EXISTS (SELECT 1 FROM outbox p WHERE p.unit_id = o.unit_id "
                "AND p.registrar = o.registrar AND p.id < o.id AND p.done_at IS NULL "
                "AND p.superseded = 0) ORDER BY o.id",
                (source, now),
            ),
        )

    def outbox_done(self, row_id: int, now: float) -> None:
        self._x(
            "UPDATE outbox SET done_at = ?, last_error = NULL WHERE id = ?",
            (now, row_id),
        )

    def outbox_failed(self, row_id: int, error: str, next_attempt: float) -> int:
        row = self._returning(
            "UPDATE outbox SET attempts = attempts + 1, last_error = ?, "
            "next_attempt = ? WHERE id = ? RETURNING attempts",
            (error, next_attempt, row_id),
        )
        return 0 if row is None else int(row["attempts"])

    def set_outbox_alerted(self, row_id: int) -> None:
        self._x("UPDATE outbox SET alerted = 1 WHERE id = ?", (row_id,))

    def required_done(self, unit_id: int, generation: int, event: str) -> bool:
        row = self._x(
            "SELECT COUNT(*) AS waiting FROM outbox WHERE unit_id = ? "
            "AND generation = ? AND event = ? AND required = 1 AND superseded = 0 "
            "AND done_at IS NULL",
            (unit_id, generation, event),
        ).fetchone()
        return int(row["waiting"]) == 0

    def outbox(self, unit_id: int) -> list[OutboxRow]:
        return self._all(
            OutboxRow,
            self._x("SELECT * FROM outbox WHERE unit_id = ? ORDER BY id", (unit_id,)),
        )

    # -- operator actions ---------------------------------------------------
    def force_complete(self, source: str, key: str) -> UnitRow | None:
        unit = self.find_unit(source, key)
        if unit is not None and unit.state == OPEN:
            self._x(
                "UPDATE unit SET forced = 1, note = 'completion forced' WHERE id = ?",
                (unit.id,),
            )
        return unit

    def abandon(self, source: str, key: str) -> UnitRow | None:
        unit = self.find_unit(source, key)
        if unit is not None:
            with self.tx():
                self._x(
                    "UPDATE unit SET state = 'abandoned', note = 'abandoned' "
                    "WHERE id = ?",
                    (unit.id,),
                )
                self._x(
                    "UPDATE outbox SET superseded = 1 WHERE unit_id = ? "
                    "AND done_at IS NULL",
                    (unit.id,),
                )
        return unit

    def retry(self, source: str | None = None, key: str | None = None) -> int:
        """Clear backoff everywhere in scope; re-open abandoned units in scope."""
        where, params = "1 = 1", []
        if source is not None:
            where += " AND source = ?"
            params.append(source)
        if key is not None:
            where += " AND key = ?"
            params.append(key)
        with self.tx():
            ids = [
                r["id"] for r in self._x(f"SELECT id FROM unit WHERE {where}", params)
            ]
            for unit_id in ids:
                self._x(
                    "UPDATE file SET next_attempt = NULL, attempts = 0, alerted = 0 "
                    "WHERE unit_id = ? AND state = 'pending'",
                    (unit_id,),
                )
                self._x(
                    "UPDATE outbox SET next_attempt = NULL, attempts = 0, alerted = 0 "
                    "WHERE unit_id = ? AND done_at IS NULL AND superseded = 0",
                    (unit_id,),
                )
                self._x(
                    "UPDATE unit SET next_attempt = NULL, attempts = 0, alerted = 0, "
                    "state = CASE state WHEN 'abandoned' THEN 'open' ELSE state END "
                    "WHERE id = ?",
                    (unit_id,),
                )
        return len(ids)

    # -- reporting ----------------------------------------------------------
    def summary(self, source: str) -> dict[str, Any]:
        states = {
            row["state"]: row["n"]
            for row in self._x(
                "SELECT state, COUNT(*) AS n FROM unit WHERE source = ? GROUP BY state",
                (source,),
            )
        }
        files = self._x(
            "SELECT COUNT(*) AS pending, SUM(f.attempts > 0) AS failing, "
            "COALESCE(SUM(f.size), 0) AS pending_bytes FROM file f "
            "JOIN unit u ON u.id = f.unit_id WHERE u.source = ? "
            "AND f.state = 'pending' AND f.present = 1 AND u.state NOT IN "
            "('abandoned', 'gone')",
            (source,),
        ).fetchone()
        outbox = self._x(
            "SELECT COUNT(*) AS waiting, SUM(o.attempts > 0) AS failing FROM outbox o "
            "JOIN unit u ON u.id = o.unit_id WHERE u.source = ? "
            "AND o.done_at IS NULL AND o.superseded = 0",
            (source,),
        ).fetchone()
        return {
            "units": {state: states.get(state, 0) for state in UNIT_STATES},
            "pending_files": int(files["pending"] or 0),
            "pending_bytes": int(files["pending_bytes"] or 0),
            "failing_files": int(files["failing"] or 0),
            "deliveries_waiting": int(outbox["waiting"] or 0),
            "deliveries_failing": int(outbox["failing"] or 0),
            "last_pass": self.get_meta(f"last_pass:{source}"),
        }

    def failing_files(self, source: str) -> list[tuple[str, FileRow]]:
        rows = self._x(
            "SELECT u.key AS unit_key, f.* FROM file f JOIN unit u ON u.id = f.unit_id "
            "WHERE u.source = ? AND f.state = 'pending' AND f.attempts > 0 "
            "ORDER BY f.next_attempt",
            (source,),
        ).fetchall()
        out = []
        for row in rows:
            data = dict(row)
            key = data.pop("unit_key")
            out.append((key, FileRow(**data)))
        return out

    def failing_deliveries(self, source: str) -> list[tuple[str, OutboxRow]]:
        rows = self._x(
            "SELECT u.key AS unit_key, o.* FROM outbox o "
            "JOIN unit u ON u.id = o.unit_id WHERE u.source = ? "
            "AND o.done_at IS NULL AND o.superseded = 0 AND o.attempts > 0 "
            "ORDER BY o.next_attempt",
            (source,),
        ).fetchall()
        out = []
        for row in rows:
            data = dict(row)
            key = data.pop("unit_key")
            out.append((key, OutboxRow(**data)))
        return out

    # -- shipped logs -------------------------------------------------------
    def shipped(self, name: str) -> bool:
        row = self._x("SELECT 1 FROM shipped_log WHERE name = ?", (name,)).fetchone()
        return row is not None

    def record_shipped(
        self, name: str, *, uri: str, size: int, sha256: str, now: float
    ) -> None:
        self._x(
            "INSERT OR REPLACE INTO shipped_log (name, uri, size, sha256, shipped_at) "
            "VALUES (?, ?, ?, ?, ?)",
            (name, uri, size, sha256, now),
        )
