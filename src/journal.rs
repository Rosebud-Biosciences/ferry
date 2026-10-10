//! The journal: ferry's durable record of units, files, copies, deliveries and
//! transfers, in one SQLite database (WAL mode) per state directory.
//!
//! Unit lifecycle:
//!
//! ```text
//! open -> complete -> registering -> registered -> finalized
//!   ^-------------- any change reopens ----------------'
//! abandoned (an operator gave up on it), gone (vanished before any upload)
//! ```
//!
//! A file is `pending` until every destination holds a *current* copy of it --
//! one uploaded with the size and mtime the file has now -- then `uploaded`,
//! then `finalized` once the source copy is kept, moved or deleted. Because a
//! copy only counts while its stats match, a file that changes is pending again
//! for every destination without any bookkeeping beyond the new stats.
//!
//! `receipt_gen` is the last generation of a unit whose receipt was queued for
//! the notifiers, so each generation is announced once.

use std::collections::{BTreeMap, BTreeSet};
use std::path::Path;

use anyhow::{Context, Result, bail};
use rusqlite::types::Value;
use rusqlite::{Connection, OptionalExtension, Row, params, params_from_iter};
use serde::Serialize;

use crate::units::{Scan, UnitObs};

pub const OPEN: &str = "open";
pub const COMPLETE: &str = "complete";
pub const REGISTERING: &str = "registering";
pub const REGISTERED: &str = "registered";
pub const FINALIZED: &str = "finalized";
pub const ABANDONED: &str = "abandoned";
pub const GONE: &str = "gone";
pub const UNIT_STATES: [&str; 7] = [OPEN, COMPLETE, REGISTERING, REGISTERED, FINALIZED, ABANDONED, GONE];
/// Units a stream rescans on every poll.
pub const ACTIVE: [&str; 4] = [OPEN, COMPLETE, REGISTERING, REGISTERED];

pub const PENDING: &str = "pending";
pub const UPLOADED: &str = "uploaded";
pub const DONE: &str = "done";

const SCHEMA_VERSION: i64 = 4;
const SCHEMA: &str = r#"
CREATE TABLE IF NOT EXISTS meta (key TEXT PRIMARY KEY, value TEXT NOT NULL);
CREATE TABLE IF NOT EXISTS unit (
    id INTEGER PRIMARY KEY,
    pipeline TEXT NOT NULL,
    key TEXT NOT NULL,
    state TEXT NOT NULL DEFAULT 'open',
    generation INTEGER NOT NULL DEFAULT 0,
    first_seen REAL NOT NULL,
    last_change REAL NOT NULL,
    marker INTEGER NOT NULL DEFAULT 0,
    present INTEGER NOT NULL DEFAULT 1,
    forced INTEGER NOT NULL DEFAULT 0,
    completed_at REAL,
    registered_at REAL,
    finalized_at REAL,
    attempts INTEGER NOT NULL DEFAULT 0,
    next_attempt REAL,
    last_error TEXT,
    alerted INTEGER NOT NULL DEFAULT 0,
    note TEXT,
    receipt_gen INTEGER,
    scan_error TEXT,
    UNIQUE (pipeline, key)
);
CREATE INDEX IF NOT EXISTS unit_by_state ON unit (pipeline, state);
CREATE INDEX IF NOT EXISTS unit_unannounced ON unit (pipeline)
    WHERE state IN ('registered', 'finalized') AND (receipt_gen IS NULL OR receipt_gen < generation);
CREATE TABLE IF NOT EXISTS file (
    id INTEGER PRIMARY KEY,
    unit_id INTEGER NOT NULL REFERENCES unit (id) ON DELETE CASCADE,
    rel TEXT NOT NULL,
    within TEXT NOT NULL,
    size INTEGER NOT NULL,
    mtime_ns INTEGER NOT NULL,
    present INTEGER NOT NULL DEFAULT 1,
    state TEXT NOT NULL DEFAULT 'pending',
    sha256 TEXT,
    hashed_size INTEGER,
    hashed_mtime_ns INTEGER,
    backup_path TEXT,
    UNIQUE (unit_id, rel)
);
CREATE INDEX IF NOT EXISTS file_by_unit ON file (unit_id, state);
CREATE TABLE IF NOT EXISTS copy (
    file_id INTEGER NOT NULL REFERENCES file (id) ON DELETE CASCADE,
    dest TEXT NOT NULL,
    state TEXT NOT NULL DEFAULT 'pending',
    up_size INTEGER,
    up_mtime_ns INTEGER,
    etag TEXT,
    verified TEXT,
    uploaded_at REAL,
    attempts INTEGER NOT NULL DEFAULT 0,
    next_attempt REAL,
    last_error TEXT,
    alerted INTEGER NOT NULL DEFAULT 0,
    PRIMARY KEY (file_id, dest)
);
CREATE TABLE IF NOT EXISTS destination (
    pipeline TEXT NOT NULL,
    name TEXT NOT NULL,
    identity TEXT NOT NULL,
    verify TEXT NOT NULL,
    PRIMARY KEY (pipeline, name)
);
CREATE TABLE IF NOT EXISTS adoption (
    unit_id INTEGER NOT NULL REFERENCES unit (id) ON DELETE CASCADE,
    dest TEXT NOT NULL,
    PRIMARY KEY (unit_id, dest)
);
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
    pipeline TEXT NOT NULL,
    unit_id INTEGER REFERENCES unit (id) ON DELETE SET NULL,
    rel TEXT NOT NULL,
    dest TEXT,
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
"#;

/// `NOT (current copy)`, for a copy `c` of a file `f`.
const STALE: &str = "NOT (c.state = 'done' AND c.up_size = f.size AND c.up_mtime_ns = f.mtime_ns)";

#[derive(Debug, Clone, Serialize)]
pub struct UnitRow {
    pub id: i64,
    pub pipeline: String,
    pub key: String,
    pub state: String,
    pub generation: i64,
    pub first_seen: f64,
    pub last_change: f64,
    pub marker: bool,
    pub present: bool,
    pub forced: bool,
    pub completed_at: Option<f64>,
    pub registered_at: Option<f64>,
    pub finalized_at: Option<f64>,
    pub attempts: i64,
    pub next_attempt: Option<f64>,
    pub last_error: Option<String>,
    pub alerted: bool,
    pub note: Option<String>,
    pub receipt_gen: Option<i64>,
    /// Why the last scan could not read all of the unit; it neither
    /// completes, registers nor finalizes until a scan can.
    pub scan_error: Option<String>,
}

impl UnitRow {
    fn from_row(r: &Row) -> rusqlite::Result<Self> {
        Ok(Self {
            id: r.get("id")?,
            pipeline: r.get("pipeline")?,
            key: r.get("key")?,
            state: r.get("state")?,
            generation: r.get("generation")?,
            first_seen: r.get("first_seen")?,
            last_change: r.get("last_change")?,
            marker: r.get("marker")?,
            present: r.get("present")?,
            forced: r.get("forced")?,
            completed_at: r.get("completed_at")?,
            registered_at: r.get("registered_at")?,
            finalized_at: r.get("finalized_at")?,
            attempts: r.get("attempts")?,
            next_attempt: r.get("next_attempt")?,
            last_error: r.get("last_error")?,
            alerted: r.get("alerted")?,
            note: r.get("note")?,
            receipt_gen: r.get("receipt_gen")?,
            scan_error: r.get("scan_error")?,
        })
    }
}

#[derive(Debug, Clone, Serialize)]
pub struct FileRow {
    pub id: i64,
    pub unit_id: i64,
    pub rel: String,
    pub within: String,
    pub size: i64,
    pub mtime_ns: i64,
    pub present: bool,
    pub state: String,
    pub sha256: Option<String>,
    pub hashed_size: Option<i64>,
    pub hashed_mtime_ns: Option<i64>,
    pub backup_path: Option<String>,
}

impl FileRow {
    fn from_row(r: &Row) -> rusqlite::Result<Self> {
        Ok(Self {
            id: r.get("id")?,
            unit_id: r.get("unit_id")?,
            rel: r.get("rel")?,
            within: r.get("within")?,
            size: r.get("size")?,
            mtime_ns: r.get("mtime_ns")?,
            present: r.get("present")?,
            state: r.get("state")?,
            sha256: r.get("sha256")?,
            hashed_size: r.get("hashed_size")?,
            hashed_mtime_ns: r.get("hashed_mtime_ns")?,
            backup_path: r.get("backup_path")?,
        })
    }

    /// The SHA-256 of the file as it is now, if one was taken.
    pub fn current_sha256(&self) -> Option<&str> {
        (self.hashed_size == Some(self.size) && self.hashed_mtime_ns == Some(self.mtime_ns))
            .then_some(self.sha256.as_deref())
            .flatten()
    }

    pub fn live(&self) -> bool {
        self.state != GONE
    }
}

#[derive(Debug, Clone, Serialize)]
pub struct CopyRow {
    pub file_id: i64,
    pub dest: String,
    pub state: String,
    pub up_size: Option<i64>,
    pub up_mtime_ns: Option<i64>,
    pub etag: Option<String>,
    pub verified: Option<String>,
    pub uploaded_at: Option<f64>,
    pub attempts: i64,
    pub next_attempt: Option<f64>,
    pub last_error: Option<String>,
    pub alerted: bool,
}

impl CopyRow {
    fn from_row(r: &Row) -> rusqlite::Result<Self> {
        Ok(Self {
            file_id: r.get("file_id")?,
            dest: r.get("dest")?,
            state: r.get("state")?,
            up_size: r.get("up_size")?,
            up_mtime_ns: r.get("up_mtime_ns")?,
            etag: r.get("etag")?,
            verified: r.get("verified")?,
            uploaded_at: r.get("uploaded_at")?,
            attempts: r.get("attempts")?,
            next_attempt: r.get("next_attempt")?,
            last_error: r.get("last_error")?,
            alerted: r.get("alerted")?,
        })
    }

    pub fn is_current(&self, file: &FileRow) -> bool {
        self.state == DONE && self.up_size == Some(file.size) && self.up_mtime_ns == Some(file.mtime_ns)
    }
}

#[derive(Debug, Clone, Serialize)]
pub struct OutboxRow {
    pub id: i64,
    pub unit_id: i64,
    pub generation: i64,
    pub registrar: String,
    pub event: String,
    pub required: bool,
    pub payload: String,
    pub created_at: f64,
    pub attempts: i64,
    pub next_attempt: Option<f64>,
    pub last_error: Option<String>,
    pub alerted: bool,
    pub done_at: Option<f64>,
    pub superseded: bool,
}

impl OutboxRow {
    fn from_row(r: &Row) -> rusqlite::Result<Self> {
        Ok(Self {
            id: r.get("id")?,
            unit_id: r.get("unit_id")?,
            generation: r.get("generation")?,
            registrar: r.get("registrar")?,
            event: r.get("event")?,
            required: r.get("required")?,
            payload: r.get("payload")?,
            created_at: r.get("created_at")?,
            attempts: r.get("attempts")?,
            next_attempt: r.get("next_attempt")?,
            last_error: r.get("last_error")?,
            alerted: r.get("alerted")?,
            done_at: r.get("done_at")?,
            superseded: r.get("superseded")?,
        })
    }
}

#[derive(Debug, Clone, Serialize)]
pub struct TransferRow {
    pub id: i64,
    pub pipeline: String,
    pub unit_id: Option<i64>,
    pub rel: String,
    pub dest: Option<String>,
    pub op: String,
    pub status: String,
    pub src: String,
    pub dst: String,
    pub bytes: Option<i64>,
    pub duration_ms: Option<i64>,
    pub error: Option<String>,
    pub at: f64,
}

impl TransferRow {
    fn from_row(r: &Row) -> rusqlite::Result<Self> {
        Ok(Self {
            id: r.get("id")?,
            pipeline: r.get("pipeline")?,
            unit_id: r.get("unit_id")?,
            rel: r.get("rel")?,
            dest: r.get("dest")?,
            op: r.get("op")?,
            status: r.get("status")?,
            src: r.get("src")?,
            dst: r.get("dst")?,
            bytes: r.get("bytes")?,
            duration_ms: r.get("duration_ms")?,
            error: r.get("error")?,
            at: r.get("at")?,
        })
    }
}

/// One transfer to record (an upload, adoption, backup move or delete).
#[derive(Debug, Clone, Default)]
pub struct NewTransfer<'a> {
    pub pipeline: &'a str,
    pub unit_id: Option<i64>,
    pub rel: &'a str,
    pub dest: Option<&'a str>,
    pub op: &'a str,
    pub status: &'a str,
    pub src: &'a str,
    pub dst: &'a str,
    pub bytes: Option<i64>,
    pub duration_ms: Option<i64>,
    pub error: Option<&'a str>,
    pub at: f64,
}

#[derive(Debug, Clone, Default)]
pub struct ScanChanges {
    pub new_units: Vec<String>,
    pub new_files: usize,
    pub changed_files: usize,
    pub gone_files: Vec<String>,
    pub reopened: Vec<(String, String)>,
    pub gone_units: Vec<String>,
    /// Units the scan newly could not read in full, and why.
    pub unreadable: Vec<(String, String)>,
}

/// Where a destination points, and how it verifies copies.
#[derive(Debug, Clone, PartialEq)]
pub struct Binding {
    pub name: String,
    pub identity: String,
    pub verify: String,
}

/// A destination that now points somewhere other than where its copies are.
#[derive(Debug, Clone, PartialEq)]
pub struct Moved {
    pub name: String,
    pub was: String,
    pub now: String,
}

/// What `ferry destination reset` changes.
#[derive(Debug, Clone, Default, PartialEq)]
pub struct ResetPlan {
    /// Files whose copy there is forgotten, to go up again.
    pub files: i64,
    /// Units already registered or complete, which register again.
    pub reopened: Vec<String>,
    /// Copies of finalized files, kept as a record: their sources are gone.
    pub kept: i64,
}

/// A file ready to upload, and the destinations that still need it.
#[derive(Debug, Clone)]
pub struct Candidate {
    pub file: FileRow,
    pub dests: Vec<String>,
}

#[derive(Debug, Clone, Serialize)]
pub struct Summary {
    pub units: BTreeMap<String, i64>,
    pub pending_files: i64,
    pub pending_bytes: i64,
    pub failing_copies: i64,
    pub deliveries_waiting: i64,
    pub deliveries_failing: i64,
    pub last_pass: Option<String>,
}

fn marks(n: usize) -> String {
    vec!["?"; n].join(",")
}

fn values<S: AsRef<str>>(items: &[S]) -> impl Iterator<Item = Value> + '_ {
    items.iter().map(|s| Value::Text(s.as_ref().to_string()))
}

const UNIT_COLUMNS: &[&str] = &[
    "state",
    "generation",
    "marker",
    "present",
    "forced",
    "completed_at",
    "registered_at",
    "finalized_at",
    "attempts",
    "next_attempt",
    "last_error",
    "alerted",
    "note",
];

/// Bring a journal at schema `from` up to [`SCHEMA_VERSION`], in one
/// transaction. Runs before [`SCHEMA`], whose indexes may need new columns.
fn migrate(conn: &Connection, from: i64) -> Result<()> {
    let tx = conn.unchecked_transaction()?;
    if from < 2 {
        // Which generation of each unit had its receipt queued. Units already
        // registered or finalized count as announced, so an upgrade never
        // sends receipts for them.
        add_column(&tx, "unit", "receipt_gen", "INTEGER")?;
        tx.execute_batch("UPDATE unit SET receipt_gen = generation WHERE state IN ('registered', 'finalized');")?;
    }
    if from < 3 {
        add_column(&tx, "unit", "scan_error", "TEXT")?;
    }
    // 4 adds the `destination` table; the first run after the upgrade records
    // where each destination points.
    tx.execute(
        "UPDATE meta SET value = ? WHERE key = 'schema_version'",
        [SCHEMA_VERSION.to_string()],
    )?;
    tx.commit()?;
    Ok(())
}

fn add_column(conn: &Connection, table: &str, column: &str, declaration: &str) -> Result<()> {
    let present: i64 = conn.query_row(
        &format!("SELECT COUNT(*) FROM pragma_table_info('{table}') WHERE name = ?"),
        [column],
        |r| r.get(0),
    )?;
    if present == 0 {
        conn.execute_batch(&format!("ALTER TABLE {table} ADD COLUMN {column} {declaration};"))?;
    }
    Ok(())
}

pub struct Journal {
    conn: Connection,
}

impl Journal {
    pub fn open(path: &Path) -> Result<Self> {
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent)?;
        }
        let conn = Connection::open(path).with_context(|| format!("cannot open journal {}", path.display()))?;
        conn.pragma_update(None, "journal_mode", "WAL")?;
        Self::init(conn)
    }

    pub fn in_memory() -> Result<Self> {
        Self::init(Connection::open_in_memory()?)
    }

    /// An in-memory copy of the journal at `path` (empty if there is none), for
    /// dry runs.
    pub fn snapshot(path: &Path) -> Result<Self> {
        let mut memory = Connection::open_in_memory()?;
        if path.exists() {
            let source = Connection::open(path)?;
            rusqlite::backup::Backup::new(&source, &mut memory)?.run_to_completion(
                1024,
                std::time::Duration::ZERO,
                None,
            )?;
        }
        Self::init(memory)
    }

    fn init(conn: Connection) -> Result<Self> {
        conn.pragma_update(None, "synchronous", "NORMAL")?;
        conn.pragma_update(None, "foreign_keys", "ON")?;
        conn.busy_timeout(std::time::Duration::from_secs(30))?;
        conn.execute_batch("CREATE TABLE IF NOT EXISTS meta (key TEXT PRIMARY KEY, value TEXT NOT NULL);")?;
        let found: Option<String> = conn
            .query_row("SELECT value FROM meta WHERE key = 'schema_version'", [], |r| r.get(0))
            .optional()?;
        if let Some(v) = &found {
            match v.parse::<i64>().unwrap_or(i64::MAX) {
                newer if newer > SCHEMA_VERSION => {
                    bail!("the journal has schema {v}; this ferry understands {SCHEMA_VERSION} -- upgrade ferry");
                }
                older if older < SCHEMA_VERSION => migrate(&conn, older)?,
                _ => {}
            }
        }
        conn.execute_batch(SCHEMA)?;
        conn.execute(
            "INSERT INTO meta (key, value) VALUES ('schema_version', ?) \
             ON CONFLICT (key) DO UPDATE SET value = excluded.value",
            [SCHEMA_VERSION.to_string()],
        )?;
        Ok(Self { conn })
    }

    // -- meta ---------------------------------------------------------------
    pub fn get_meta(&self, key: &str) -> Result<Option<String>> {
        Ok(self
            .conn
            .query_row("SELECT value FROM meta WHERE key = ?", [key], |r| r.get(0))
            .optional()?)
    }

    pub fn set_meta(&self, key: &str, value: &str) -> Result<()> {
        self.conn.execute(
            "INSERT INTO meta (key, value) VALUES (?, ?) ON CONFLICT (key) DO UPDATE SET value = excluded.value",
            [key, value],
        )?;
        Ok(())
    }

    // -- reads --------------------------------------------------------------
    pub fn units(&self, pipeline: &str, states: Option<&[&str]>) -> Result<Vec<UnitRow>> {
        let (sql, args): (String, Vec<Value>) = match states {
            None => (
                "SELECT * FROM unit WHERE pipeline = ? ORDER BY id".into(),
                vec![Value::from(pipeline.to_string())],
            ),
            Some(states) => (
                format!(
                    "SELECT * FROM unit WHERE pipeline = ? AND state IN ({}) ORDER BY first_seen, id",
                    marks(states.len())
                ),
                std::iter::once(Value::from(pipeline.to_string()))
                    .chain(values(states))
                    .collect(),
            ),
        };
        let mut stmt = self.conn.prepare_cached(&sql)?;
        let rows = stmt.query_map(params_from_iter(args), UnitRow::from_row)?;
        Ok(rows.collect::<rusqlite::Result<_>>()?)
    }

    pub fn unit(&self, id: i64) -> Result<UnitRow> {
        Ok(self
            .conn
            .query_row("SELECT * FROM unit WHERE id = ?", [id], UnitRow::from_row)?)
    }

    pub fn find_unit(&self, pipeline: &str, key: &str) -> Result<Option<UnitRow>> {
        Ok(self
            .conn
            .query_row(
                "SELECT * FROM unit WHERE pipeline = ? AND key = ?",
                [pipeline, key],
                UnitRow::from_row,
            )
            .optional()?)
    }

    pub fn files(&self, unit_id: i64) -> Result<Vec<FileRow>> {
        let mut stmt = self
            .conn
            .prepare_cached("SELECT * FROM file WHERE unit_id = ? ORDER BY rel")?;
        let rows = stmt.query_map([unit_id], FileRow::from_row)?;
        Ok(rows.collect::<rusqlite::Result<_>>()?)
    }

    pub fn file(&self, id: i64) -> Result<FileRow> {
        Ok(self
            .conn
            .query_row("SELECT * FROM file WHERE id = ?", [id], FileRow::from_row)?)
    }

    pub fn copies(&self, file_id: i64) -> Result<Vec<CopyRow>> {
        let mut stmt = self
            .conn
            .prepare_cached("SELECT * FROM copy WHERE file_id = ? ORDER BY dest")?;
        let rows = stmt.query_map([file_id], CopyRow::from_row)?;
        Ok(rows.collect::<rusqlite::Result<_>>()?)
    }

    pub fn unit_copies(&self, unit_id: i64) -> Result<BTreeMap<i64, Vec<CopyRow>>> {
        let mut stmt = self.conn.prepare_cached(
            "SELECT c.* FROM copy c JOIN file f ON f.id = c.file_id WHERE f.unit_id = ? ORDER BY c.dest",
        )?;
        let mut out: BTreeMap<i64, Vec<CopyRow>> = BTreeMap::new();
        for row in stmt.query_map([unit_id], CopyRow::from_row)? {
            let row = row?;
            out.entry(row.file_id).or_default().push(row);
        }
        Ok(out)
    }

    /// Give every file of every unfinished unit a copy row per destination, so a
    /// destination added to the config picks up what is not yet finalized.
    /// Check each destination against what the journal recorded: the first
    /// time, record it. A stronger `verify` than recorded sends the copies it
    /// did not cover again; a destination that now points elsewhere is
    /// returned, and left for an operator to accept or reset.
    pub fn bind_destinations(&mut self, pipeline: &str, bindings: &[Binding], dests: &[String]) -> Result<Vec<Moved>> {
        let tx = self.conn.transaction()?;
        let mut moved = Vec::new();
        for b in bindings {
            let known: Option<(String, String)> = tx
                .query_row(
                    "SELECT identity, verify FROM destination WHERE pipeline = ? AND name = ?",
                    params![pipeline, b.name],
                    |r| Ok((r.get(0)?, r.get(1)?)),
                )
                .optional()?;
            match known {
                None => {
                    tx.execute(
                        "INSERT INTO destination (pipeline, name, identity, verify) VALUES (?, ?, ?, ?)",
                        params![pipeline, b.name, b.identity, b.verify],
                    )?;
                }
                Some((identity, _)) if identity != b.identity => moved.push(Moved {
                    name: b.name.clone(),
                    was: identity,
                    now: b.identity.clone(),
                }),
                Some((_, verify)) if verify != b.verify => {
                    if b.verify == "sha256" {
                        let weaker = live_copies(
                            &tx,
                            pipeline,
                            &b.name,
                            "AND (c.verified IS NULL OR c.verified != 'sha256')",
                        )?;
                        forget_copies(&tx, &weaker, &b.name, dests)?;
                    }
                    tx.execute(
                        "UPDATE destination SET verify = ? WHERE pipeline = ? AND name = ?",
                        params![b.verify, pipeline, b.name],
                    )?;
                }
                Some(_) => {}
            }
        }
        tx.commit()?;
        Ok(moved)
    }

    /// The identity the journal recorded for a destination, if any.
    pub fn destination(&self, pipeline: &str, name: &str) -> Result<Option<String>> {
        Ok(self
            .conn
            .query_row(
                "SELECT identity FROM destination WHERE pipeline = ? AND name = ?",
                params![pipeline, name],
                |r| r.get(0),
            )
            .optional()?)
    }

    /// The destination's new settings reach the same objects: record them and
    /// keep its copies.
    pub fn accept_destination(&mut self, pipeline: &str, binding: &Binding) -> Result<()> {
        self.conn.execute(
            "INSERT INTO destination (pipeline, name, identity, verify) VALUES (?, ?, ?, ?) \
             ON CONFLICT (pipeline, name) DO UPDATE SET identity = excluded.identity, verify = excluded.verify",
            params![pipeline, binding.name, binding.identity, binding.verify],
        )?;
        Ok(())
    }

    /// Forget a destination's copies of every file not yet finalized, so they
    /// go up again; units already complete or registered reopen to register
    /// with the new copies. With `apply` false, only report what would change.
    pub fn reset_destination(
        &mut self,
        pipeline: &str,
        binding: &Binding,
        dests: &[String],
        apply: bool,
    ) -> Result<ResetPlan> {
        let tx = self.conn.transaction()?;
        let files = live_copies(&tx, pipeline, &binding.name, "")?;
        let mut plan = ResetPlan {
            files: files.len() as i64,
            ..Default::default()
        };
        let units: Vec<(i64, String)> = {
            let mut stmt = tx.prepare(
                "SELECT DISTINCT u.id, u.key FROM unit u JOIN file f ON f.unit_id = u.id \
                 JOIN copy c ON c.file_id = f.id WHERE u.pipeline = ? AND c.dest = ? AND c.state = 'done' \
                 AND f.state IN ('pending', 'uploaded') AND u.state IN ('complete', 'registering', 'registered') \
                 ORDER BY u.key",
            )?;
            stmt.query_map(params![pipeline, binding.name], |r| Ok((r.get(0)?, r.get(1)?)))?
                .collect::<rusqlite::Result<_>>()?
        };
        plan.reopened = units.iter().map(|(_, key)| key.clone()).collect();
        plan.kept = tx.query_row(
            "SELECT COUNT(*) FROM copy c JOIN file f ON f.id = c.file_id JOIN unit u ON u.id = f.unit_id \
             WHERE u.pipeline = ? AND c.dest = ? AND c.state = 'done' \
             AND (f.state = 'finalized' OR u.state = 'finalized')",
            params![pipeline, binding.name],
            |r| r.get(0),
        )?;
        if apply {
            forget_copies(&tx, &files, &binding.name, dests)?;
            for (unit_id, _) in &units {
                reopen(&tx, *unit_id, &format!("destination {} was reset", binding.name))?;
            }
            tx.execute(
                "DELETE FROM adoption WHERE dest = ? AND unit_id IN (SELECT id FROM unit WHERE pipeline = ?)",
                params![binding.name, pipeline],
            )?;
            tx.execute(
                "INSERT INTO destination (pipeline, name, identity, verify) VALUES (?, ?, ?, ?) \
                 ON CONFLICT (pipeline, name) DO UPDATE SET identity = excluded.identity, verify = excluded.verify",
                params![pipeline, binding.name, binding.identity, binding.verify],
            )?;
            tx.commit()?;
        }
        Ok(plan)
    }

    pub fn ensure_copies(&mut self, pipeline: &str, dests: &[String]) -> Result<()> {
        let tx = self.conn.transaction()?;
        for dest in dests {
            tx.execute(
                "INSERT OR IGNORE INTO copy (file_id, dest) SELECT f.id, ? FROM file f \
                 JOIN unit u ON u.id = f.unit_id WHERE u.pipeline = ? \
                 AND u.state NOT IN ('finalized', 'abandoned', 'gone') \
                 AND f.state IN ('pending', 'uploaded')",
                params![dest, pipeline],
            )?;
        }
        let ids: Vec<i64> = {
            let mut stmt = tx.prepare(
                "SELECT f.id FROM file f JOIN unit u ON u.id = f.unit_id WHERE u.pipeline = ? \
                 AND f.state = 'uploaded'",
            )?;
            stmt.query_map([pipeline], |r| r.get(0))?
                .collect::<rusqlite::Result<_>>()?
        };
        for id in ids {
            refresh_file(&tx, id, dests)?;
        }
        tx.commit()?;
        Ok(())
    }

    // -- scans --------------------------------------------------------------
    /// Fold one scan into the journal and report what it changed.
    pub fn apply_scan(&mut self, pipeline: &str, scan: &Scan, now: f64, dests: &[String]) -> Result<ScanChanges> {
        let mut changes = ScanChanges::default();
        let tx = self.conn.transaction()?;
        let known: BTreeMap<String, UnitRow> = {
            let mut stmt = tx.prepare("SELECT * FROM unit WHERE pipeline = ?")?;
            stmt.query_map([pipeline], UnitRow::from_row)?
                .map(|r| r.map(|u| (u.key.clone(), u)))
                .collect::<rusqlite::Result<_>>()?
        };
        // An unattributed error could hide any unit's files.
        let everywhere = scan.unattributed.then(|| summarize(&scan.errors));
        for (key, obs) in &scan.units {
            let error = everywhere
                .clone()
                .or_else(|| (!obs.errors.is_empty()).then(|| summarize(&obs.errors)));
            observe_unit(
                &tx,
                pipeline,
                known.get(key),
                obs,
                error.as_deref(),
                now,
                dests,
                &mut changes,
            )?;
        }
        if let Some(why) = &everywhere {
            for unit in known.values().filter(|u| !scan.units.contains_key(&u.key)) {
                if ACTIVE.contains(&unit.state.as_str()) {
                    mark_unreadable(&tx, unit, why, &mut changes)?;
                }
            }
        } else if scan.errors.is_empty() {
            // Only a complete look can tell that a unit is gone.
            let unseen: Vec<&UnitRow> = match &scan.scope {
                None => known.values().filter(|u| !scan.units.contains_key(&u.key)).collect(),
                Some(scope) => scope
                    .iter()
                    .filter(|k| !scan.units.contains_key(*k))
                    .filter_map(|k| known.get(k))
                    .collect(),
            };
            for unit in unseen {
                unit_missing(&tx, unit, &mut changes)?;
            }
        }
        tx.commit()?;
        Ok(changes)
    }

    // -- units --------------------------------------------------------------
    /// Update unit columns, optionally only if it is still in `expect` at
    /// `generation`. The compare-and-set keeps a slow finalize from overwriting
    /// a reopen that a scan recorded meanwhile.
    pub fn set_unit(
        &self,
        id: i64,
        expect: Option<&str>,
        generation: Option<i64>,
        updates: &[(&str, Value)],
    ) -> Result<bool> {
        if let Some((bad, _)) = updates.iter().find(|(c, _)| !UNIT_COLUMNS.contains(c)) {
            bail!("unknown unit column {bad}");
        }
        let sets = updates
            .iter()
            .map(|(c, _)| format!("{c} = ?"))
            .collect::<Vec<_>>()
            .join(", ");
        let mut sql = format!("UPDATE unit SET {sets} WHERE id = ?");
        let mut args: Vec<Value> = updates.iter().map(|(_, v)| v.clone()).collect();
        args.push(id.into());
        if let Some(state) = expect {
            sql.push_str(" AND state = ?");
            args.push(state.to_string().into());
        }
        if let Some(generation) = generation {
            sql.push_str(" AND generation = ?");
            args.push(generation.into());
        }
        Ok(self.conn.execute(&sql, params_from_iter(args))? == 1)
    }

    /// Mark an open unit complete; return its new generation.
    pub fn complete_unit(&self, id: i64, now: f64) -> Result<Option<i64>> {
        Ok(self
            .conn
            .query_row(
                "UPDATE unit SET state = 'complete', generation = generation + 1, completed_at = ?, \
                 note = NULL, alerted = 0 WHERE id = ? AND state = 'open' RETURNING generation",
                params![now, id],
                |r| r.get(0),
            )
            .optional()?)
    }

    /// Whether every live file has a current copy in each of `dests`.
    pub fn copies_current(&self, unit_id: i64, dests: &[String]) -> Result<bool> {
        if dests.is_empty() {
            return Ok(true);
        }
        let sql = format!(
            "SELECT COUNT(*) FROM file f JOIN copy c ON c.file_id = f.id WHERE f.unit_id = ? \
             AND f.state IN ('pending', 'uploaded') AND c.dest IN ({}) AND {STALE}",
            marks(dests.len())
        );
        let args = std::iter::once(Value::from(unit_id)).chain(values(dests));
        let stale: i64 = self.conn.query_row(&sql, params_from_iter(args), |r| r.get(0))?;
        Ok(stale == 0)
    }

    /// When the unit's last copy was made.
    pub fn last_copy_at(&self, unit_id: i64) -> Result<Option<f64>> {
        Ok(self.conn.query_row(
            "SELECT MAX(c.uploaded_at) FROM copy c JOIN file f ON f.id = c.file_id WHERE f.unit_id = ?",
            [unit_id],
            |r| r.get(0),
        )?)
    }

    // -- uploads ------------------------------------------------------------
    /// Settled files with stale copies in `dests`, and which destinations need them.
    pub fn upload_candidates(
        &self,
        pipeline: &str,
        now: f64,
        settled_ns: i64,
        unit_states: &[&str],
        dests: &[String],
    ) -> Result<Vec<Candidate>> {
        let sql = format!(
            "SELECT f.*, group_concat(c.dest, char(31)) AS dests FROM file f \
             JOIN unit u ON u.id = f.unit_id JOIN copy c ON c.file_id = f.id \
             WHERE u.pipeline = ? AND u.state IN ({}) AND f.state = 'pending' AND f.present = 1 \
             AND f.mtime_ns <= ? AND c.dest IN ({}) AND {STALE} \
             AND (c.next_attempt IS NULL OR c.next_attempt <= ?) \
             GROUP BY f.id ORDER BY u.first_seen, u.id, f.rel",
            marks(unit_states.len()),
            marks(dests.len())
        );
        let args: Vec<Value> = std::iter::once(Value::from(pipeline.to_string()))
            .chain(values(unit_states))
            .chain(std::iter::once(Value::from(settled_ns)))
            .chain(values(dests))
            .chain(std::iter::once(Value::from(now)))
            .collect();
        let mut stmt = self.conn.prepare_cached(&sql)?;
        let rows = stmt.query_map(params_from_iter(args), |r| {
            let file = FileRow::from_row(r)?;
            let joined: String = r.get("dests")?;
            Ok(Candidate {
                file,
                dests: joined.split('\u{1f}').map(str::to_string).collect(),
            })
        })?;
        Ok(rows.collect::<rusqlite::Result<_>>()?)
    }

    #[allow(clippy::too_many_arguments)]
    pub fn record_copy(
        &mut self,
        file_id: i64,
        dest: &str,
        size: i64,
        mtime_ns: i64,
        etag: Option<&str>,
        verified: &str,
        now: f64,
        dests: &[String],
    ) -> Result<()> {
        let tx = self.conn.transaction()?;
        tx.execute(
            "INSERT INTO copy (file_id, dest) VALUES (?, ?) ON CONFLICT DO NOTHING",
            params![file_id, dest],
        )?;
        tx.execute(
            "UPDATE copy SET state = 'done', up_size = ?, up_mtime_ns = ?, etag = ?, verified = ?, \
             uploaded_at = ?, attempts = 0, next_attempt = NULL, last_error = NULL, alerted = 0 \
             WHERE file_id = ? AND dest = ?",
            params![size, mtime_ns, etag, verified, now, file_id, dest],
        )?;
        refresh_file(&tx, file_id, dests)?;
        tx.commit()?;
        Ok(())
    }

    pub fn set_hash(&self, file_id: i64, sha256: &str, size: i64, mtime_ns: i64) -> Result<()> {
        self.conn.execute(
            "UPDATE file SET sha256 = ?, hashed_size = ?, hashed_mtime_ns = ? WHERE id = ?",
            params![sha256, size, mtime_ns, file_id],
        )?;
        Ok(())
    }

    /// Record a failed attempt on one copy; return its attempt count.
    pub fn copy_failed(&self, file_id: i64, dest: &str, error: &str, next_attempt: f64) -> Result<i64> {
        Ok(self.conn.query_row(
            "UPDATE copy SET attempts = attempts + 1, last_error = ?, next_attempt = ? \
             WHERE file_id = ? AND dest = ? RETURNING attempts",
            params![error, next_attempt, file_id, dest],
            |r| r.get(0),
        )?)
    }

    pub fn copy_alerted(&self, file_id: i64, dest: &str) -> Result<()> {
        self.conn.execute(
            "UPDATE copy SET alerted = 1 WHERE file_id = ? AND dest = ?",
            params![file_id, dest],
        )?;
        Ok(())
    }

    pub fn defer_copies(&self, file_id: i64, dests: &[String], until: f64) -> Result<()> {
        for dest in dests {
            self.conn.execute(
                "UPDATE copy SET next_attempt = ? WHERE file_id = ? AND dest = ?",
                params![until, file_id, dest],
            )?;
        }
        Ok(())
    }

    pub fn is_adopted(&self, unit_id: i64, dest: &str) -> Result<bool> {
        Ok(self
            .conn
            .query_row(
                "SELECT 1 FROM adoption WHERE unit_id = ? AND dest = ?",
                params![unit_id, dest],
                |_| Ok(()),
            )
            .optional()?
            .is_some())
    }

    pub fn set_adopted(&self, unit_id: i64, dest: &str) -> Result<()> {
        self.conn.execute(
            "INSERT OR IGNORE INTO adoption (unit_id, dest) VALUES (?, ?)",
            params![unit_id, dest],
        )?;
        Ok(())
    }

    /// The file was found changed outside a scan: pend it and reopen its unit.
    pub fn reopen_for_file(&mut self, file_id: i64, reason: &str) -> Result<()> {
        let tx = self.conn.transaction()?;
        let unit_id: i64 = tx.query_row("SELECT unit_id FROM file WHERE id = ?", [file_id], |r| r.get(0))?;
        tx.execute(
            "UPDATE file SET state = 'pending' WHERE id = ? AND state IN ('pending', 'uploaded')",
            [file_id],
        )?;
        tx.execute(
            "UPDATE copy SET state = 'pending', attempts = 0, next_attempt = NULL, last_error = NULL \
             WHERE file_id = ?",
            [file_id],
        )?;
        let state: String = tx.query_row("SELECT state FROM unit WHERE id = ?", [unit_id], |r| r.get(0))?;
        if state != OPEN {
            reopen(&tx, unit_id, reason)?;
        }
        tx.commit()?;
        Ok(())
    }

    pub fn finalize_file(&self, file_id: i64, present: bool, backup_path: Option<&str>) -> Result<()> {
        self.conn.execute(
            "UPDATE file SET state = 'finalized', present = ?, backup_path = COALESCE(?, backup_path) WHERE id = ?",
            params![present, backup_path, file_id],
        )?;
        Ok(())
    }

    // -- transfers ----------------------------------------------------------
    pub fn add_transfer(&self, t: &NewTransfer) -> Result<i64> {
        self.conn.execute(
            "INSERT INTO transfer (pipeline, unit_id, rel, dest, op, status, src, dst, bytes, \
             duration_ms, error, at) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
            params![
                t.pipeline,
                t.unit_id,
                t.rel,
                t.dest,
                t.op,
                t.status,
                t.src,
                t.dst,
                t.bytes,
                t.duration_ms,
                t.error,
                t.at
            ],
        )?;
        Ok(self.conn.last_insert_rowid())
    }

    pub fn unreported(&self, unit_id: i64, ops: &[&str], status: Option<&str>) -> Result<Vec<TransferRow>> {
        let mut sql = format!(
            "SELECT * FROM transfer WHERE unit_id = ? AND reported = 0 AND op IN ({})",
            marks(ops.len())
        );
        let mut args: Vec<Value> = std::iter::once(Value::from(unit_id)).chain(values(ops)).collect();
        if let Some(status) = status {
            sql.push_str(" AND status = ?");
            args.push(status.to_string().into());
        }
        sql.push_str(" ORDER BY id");
        let mut stmt = self.conn.prepare_cached(&sql)?;
        let rows = stmt.query_map(params_from_iter(args), TransferRow::from_row)?;
        Ok(rows.collect::<rusqlite::Result<_>>()?)
    }

    pub fn unreported_failures(&self, pipeline: &str) -> Result<Vec<TransferRow>> {
        let mut stmt = self.conn.prepare_cached(
            "SELECT * FROM transfer WHERE pipeline = ? AND status = 'failed' AND reported = 0 ORDER BY id",
        )?;
        let rows = stmt.query_map([pipeline], TransferRow::from_row)?;
        Ok(rows.collect::<rusqlite::Result<_>>()?)
    }

    pub fn mark_reported(&self, ids: &[i64]) -> Result<()> {
        let mut stmt = self
            .conn
            .prepare_cached("UPDATE transfer SET reported = 1 WHERE id = ?")?;
        for id in ids {
            stmt.execute([id])?;
        }
        Ok(())
    }

    // -- outbox -------------------------------------------------------------
    #[allow(clippy::too_many_arguments)]
    pub fn enqueue(
        &self,
        unit_id: i64,
        generation: i64,
        registrar: &str,
        event: &str,
        required: bool,
        payload: &str,
        now: f64,
    ) -> Result<i64> {
        self.conn.execute(
            "INSERT INTO outbox (unit_id, generation, registrar, event, required, payload, created_at) \
             VALUES (?, ?, ?, ?, ?, ?, ?)",
            params![unit_id, generation, registrar, event, required, payload, now],
        )?;
        Ok(self.conn.last_insert_rowid())
    }

    /// Move a unit from `from` to `to` at `generation` and queue `event` for
    /// every registrar, in one transaction. `false` (and nothing queued) if the
    /// unit moved on meanwhile -- a scan reopened it while files were uploading
    /// or moving.
    #[allow(clippy::too_many_arguments)]
    pub fn advance(
        &mut self,
        unit_id: i64,
        generation: i64,
        from: &str,
        to: &str,
        stamp: Option<&str>,
        event: &str,
        registrars: &[(String, bool)],
        payload: &str,
        transfer_ids: &[i64],
        now: f64,
    ) -> Result<bool> {
        let tx = self.conn.transaction()?;
        let mut sql = String::from("UPDATE unit SET state = ?, attempts = 0, next_attempt = NULL, last_error = NULL");
        if let Some(column) = stamp {
            if !UNIT_COLUMNS.contains(&column) {
                bail!("unknown unit column {column}");
            }
            sql.push_str(&format!(", {column} = ?"));
        }
        sql.push_str(" WHERE id = ? AND state = ? AND generation = ?");
        let mut args: Vec<Value> = vec![to.to_string().into()];
        if stamp.is_some() {
            args.push(now.into());
        }
        args.extend([unit_id.into(), from.to_string().into(), generation.into()]);
        if tx.execute(&sql, params_from_iter(args))? != 1 {
            return Ok(false);
        }
        for (registrar, required) in registrars {
            tx.execute(
                "INSERT INTO outbox (unit_id, generation, registrar, event, required, payload, created_at) \
                 VALUES (?, ?, ?, ?, ?, ?, ?)",
                params![unit_id, generation, registrar, event, required, payload, now],
            )?;
        }
        for id in transfer_ids {
            tx.execute("UPDATE transfer SET reported = 1 WHERE id = ?", [id])?;
        }
        tx.commit()?;
        Ok(true)
    }

    /// Undelivered rows whose backoff has passed, oldest first. A row waits
    /// while an older one for the same unit and registrar is undelivered, so a
    /// registrar never sees a unit finalized before it has seen it registered.
    pub fn due_outbox(&self, pipeline: &str, now: f64) -> Result<Vec<OutboxRow>> {
        let mut stmt = self.conn.prepare_cached(
            "SELECT o.* FROM outbox o JOIN unit u ON u.id = o.unit_id \
             WHERE u.pipeline = ? AND o.done_at IS NULL AND o.superseded = 0 \
             AND (o.next_attempt IS NULL OR o.next_attempt <= ?) \
             AND NOT EXISTS (SELECT 1 FROM outbox p WHERE p.unit_id = o.unit_id \
             AND p.registrar = o.registrar AND p.id < o.id AND p.done_at IS NULL AND p.superseded = 0) \
             ORDER BY o.id",
        )?;
        let rows = stmt.query_map(params![pipeline, now], OutboxRow::from_row)?;
        Ok(rows.collect::<rusqlite::Result<_>>()?)
    }

    /// Every undelivered row of the pipeline, due or not.
    pub fn waiting_outbox(&self, pipeline: &str) -> Result<Vec<OutboxRow>> {
        let mut stmt = self.conn.prepare_cached(
            "SELECT o.* FROM outbox o JOIN unit u ON u.id = o.unit_id \
             WHERE u.pipeline = ? AND o.done_at IS NULL AND o.superseded = 0 ORDER BY o.id",
        )?;
        let rows = stmt.query_map([pipeline], OutboxRow::from_row)?;
        Ok(rows.collect::<rusqlite::Result<_>>()?)
    }

    pub fn outbox_for_unit(&self, unit_id: i64) -> Result<Vec<OutboxRow>> {
        let mut stmt = self
            .conn
            .prepare_cached("SELECT * FROM outbox WHERE unit_id = ? ORDER BY id")?;
        let rows = stmt.query_map([unit_id], OutboxRow::from_row)?;
        Ok(rows.collect::<rusqlite::Result<_>>()?)
    }

    pub fn outbox_done(&self, id: i64, now: f64) -> Result<()> {
        self.conn.execute(
            "UPDATE outbox SET done_at = ?, last_error = NULL WHERE id = ?",
            params![now, id],
        )?;
        Ok(())
    }

    pub fn outbox_failed(&self, id: i64, error: &str, next_attempt: f64) -> Result<i64> {
        Ok(self.conn.query_row(
            "UPDATE outbox SET attempts = attempts + 1, last_error = ?, next_attempt = ? WHERE id = ? \
             RETURNING attempts",
            params![error, next_attempt, id],
            |r| r.get(0),
        )?)
    }

    pub fn outbox_alerted(&self, id: i64) -> Result<()> {
        self.conn.execute("UPDATE outbox SET alerted = 1 WHERE id = ?", [id])?;
        Ok(())
    }

    pub fn required_done(&self, unit_id: i64, generation: i64, event: &str) -> Result<bool> {
        let waiting: i64 = self.conn.query_row(
            "SELECT COUNT(*) FROM outbox WHERE unit_id = ? AND generation = ? AND event = ? \
             AND required = 1 AND superseded = 0 AND done_at IS NULL",
            params![unit_id, generation, event],
            |r| r.get(0),
        )?;
        Ok(waiting == 0)
    }

    /// Registered and finalized units whose current generation has had no
    /// receipt queued.
    pub fn unannounced(&self, pipeline: &str) -> Result<Vec<UnitRow>> {
        let mut stmt = self.conn.prepare_cached(
            "SELECT * FROM unit WHERE pipeline = ? AND state IN ('registered', 'finalized') \
             AND (receipt_gen IS NULL OR receipt_gen < generation) ORDER BY first_seen, id",
        )?;
        let rows = stmt.query_map([pipeline], UnitRow::from_row)?;
        Ok(rows.collect::<rusqlite::Result<_>>()?)
    }

    /// Mark `generation` of a unit announced and queue its receipt for each of
    /// `notifiers`, in one transaction. `false` (and nothing queued) if that
    /// generation was announced already, or the unit moved on.
    pub fn announce(
        &mut self,
        unit_id: i64,
        generation: i64,
        notifiers: &[String],
        payload: &str,
        now: f64,
    ) -> Result<bool> {
        let tx = self.conn.transaction()?;
        let marked = tx.execute(
            "UPDATE unit SET receipt_gen = ?2 WHERE id = ?1 AND generation = ?2 \
             AND state IN ('registered', 'finalized') AND (receipt_gen IS NULL OR receipt_gen < ?2)",
            params![unit_id, generation],
        )?;
        if marked != 1 {
            return Ok(false);
        }
        for notifier in notifiers {
            tx.execute(
                "INSERT INTO outbox (unit_id, generation, registrar, event, required, payload, created_at) \
                 VALUES (?, ?, ?, 'receipt', 0, ?, ?)",
                params![unit_id, generation, notifier, payload, now],
            )?;
        }
        tx.commit()?;
        Ok(true)
    }

    // -- operator actions ---------------------------------------------------
    pub fn force_complete(&self, pipeline: &str, key: &str) -> Result<Option<UnitRow>> {
        let unit = self.find_unit(pipeline, key)?;
        if let Some(u) = &unit
            && u.state == OPEN
        {
            self.conn.execute(
                "UPDATE unit SET forced = 1, note = 'completion forced' WHERE id = ?",
                [u.id],
            )?;
        }
        Ok(unit)
    }

    pub fn abandon(&mut self, pipeline: &str, key: &str) -> Result<Option<UnitRow>> {
        let unit = self.find_unit(pipeline, key)?;
        if let Some(u) = &unit {
            let tx = self.conn.transaction()?;
            tx.execute(
                "UPDATE unit SET state = 'abandoned', note = 'abandoned' WHERE id = ?",
                [u.id],
            )?;
            tx.execute(
                "UPDATE outbox SET superseded = 1 WHERE unit_id = ? AND done_at IS NULL",
                [u.id],
            )?;
            tx.commit()?;
        }
        Ok(unit)
    }

    /// Clear backoff everywhere in scope, and re-open abandoned units in scope.
    pub fn retry(&mut self, pipeline: Option<&str>, key: Option<&str>) -> Result<usize> {
        let mut sql = String::from("SELECT id FROM unit WHERE 1 = 1");
        let mut args: Vec<Value> = Vec::new();
        if let Some(p) = pipeline {
            sql.push_str(" AND pipeline = ?");
            args.push(p.to_string().into());
        }
        if let Some(k) = key {
            sql.push_str(" AND key = ?");
            args.push(k.to_string().into());
        }
        let tx = self.conn.transaction()?;
        let ids: Vec<i64> = {
            let mut stmt = tx.prepare(&sql)?;
            stmt.query_map(params_from_iter(args), |r| r.get(0))?
                .collect::<rusqlite::Result<_>>()?
        };
        for id in &ids {
            tx.execute(
                "UPDATE copy SET next_attempt = NULL, attempts = 0, alerted = 0 \
                 WHERE file_id IN (SELECT id FROM file WHERE unit_id = ?)",
                [id],
            )?;
            tx.execute(
                "UPDATE outbox SET next_attempt = NULL, attempts = 0, alerted = 0 \
                 WHERE unit_id = ? AND done_at IS NULL AND superseded = 0",
                [id],
            )?;
            tx.execute(
                "UPDATE unit SET next_attempt = NULL, attempts = 0, alerted = 0, \
                 state = CASE state WHEN 'abandoned' THEN 'open' ELSE state END WHERE id = ?",
                [id],
            )?;
        }
        tx.commit()?;
        Ok(ids.len())
    }

    // -- reporting ----------------------------------------------------------
    pub fn summary(&self, pipeline: &str, dests: &[String]) -> Result<Summary> {
        let mut units: BTreeMap<String, i64> = UNIT_STATES.iter().map(|s| (s.to_string(), 0)).collect();
        {
            let mut stmt = self
                .conn
                .prepare_cached("SELECT state, COUNT(*) FROM unit WHERE pipeline = ? GROUP BY state")?;
            for row in stmt.query_map([pipeline], |r| Ok((r.get::<_, String>(0)?, r.get::<_, i64>(1)?)))? {
                let (state, n) = row?;
                units.insert(state, n);
            }
        }
        let (pending_files, pending_bytes): (i64, i64) = self.conn.query_row(
            "SELECT COUNT(*), COALESCE(SUM(f.size), 0) FROM file f JOIN unit u ON u.id = f.unit_id \
             WHERE u.pipeline = ? AND f.state = 'pending' AND f.present = 1 \
             AND u.state NOT IN ('abandoned', 'gone')",
            [pipeline],
            |r| Ok((r.get(0)?, r.get(1)?)),
        )?;
        let failing_copies = if dests.is_empty() {
            0
        } else {
            let sql = format!(
                "SELECT COUNT(*) FROM copy c JOIN file f ON f.id = c.file_id JOIN unit u ON u.id = f.unit_id \
                 WHERE u.pipeline = ? AND c.attempts > 0 AND c.dest IN ({}) AND {STALE}",
                marks(dests.len())
            );
            let args = std::iter::once(Value::from(pipeline.to_string())).chain(values(dests));
            self.conn.query_row(&sql, params_from_iter(args), |r| r.get(0))?
        };
        let (deliveries_waiting, deliveries_failing): (i64, i64) = self.conn.query_row(
            "SELECT COUNT(*), COALESCE(SUM(o.attempts > 0), 0) FROM outbox o JOIN unit u ON u.id = o.unit_id \
             WHERE u.pipeline = ? AND o.done_at IS NULL AND o.superseded = 0",
            [pipeline],
            |r| Ok((r.get(0)?, r.get(1)?)),
        )?;
        Ok(Summary {
            units,
            pending_files,
            pending_bytes,
            failing_copies,
            deliveries_waiting,
            deliveries_failing,
            last_pass: self.get_meta(&format!("last_pass:{pipeline}"))?,
        })
    }

    /// Copies that have failed and are not yet current: `(unit key, file, copy)`.
    pub fn failing_copies(&self, pipeline: &str) -> Result<Vec<(String, FileRow, CopyRow)>> {
        let mut stmt = self.conn.prepare_cached(&format!(
            "SELECT u.key AS unit_key, f.*, c.dest AS c_dest FROM copy c JOIN file f ON f.id = c.file_id \
             JOIN unit u ON u.id = f.unit_id WHERE u.pipeline = ? AND c.attempts > 0 AND {STALE} \
             ORDER BY c.next_attempt"
        ))?;
        let rows = stmt.query_map([pipeline], |r| {
            Ok((
                r.get::<_, String>("unit_key")?,
                FileRow::from_row(r)?,
                r.get::<_, String>("c_dest")?,
            ))
        })?;
        let mut out = Vec::new();
        for row in rows {
            let (key, file, dest) = row?;
            let copy = self
                .copies(file.id)?
                .into_iter()
                .find(|c| c.dest == dest)
                .context("copy vanished")?;
            out.push((key, file, copy));
        }
        Ok(out)
    }

    pub fn failing_deliveries(&self, pipeline: &str) -> Result<Vec<(String, OutboxRow)>> {
        let mut stmt = self.conn.prepare_cached(
            "SELECT u.key AS unit_key, o.* FROM outbox o JOIN unit u ON u.id = o.unit_id \
             WHERE u.pipeline = ? AND o.done_at IS NULL AND o.superseded = 0 AND o.attempts > 0 \
             ORDER BY o.next_attempt",
        )?;
        let rows = stmt.query_map([pipeline], |r| {
            Ok((r.get::<_, String>("unit_key")?, OutboxRow::from_row(r)?))
        })?;
        Ok(rows.collect::<rusqlite::Result<_>>()?)
    }

    // -- shipped logs -------------------------------------------------------
    pub fn shipped(&self, name: &str) -> Result<bool> {
        Ok(self
            .conn
            .query_row("SELECT 1 FROM shipped_log WHERE name = ?", [name], |_| Ok(()))
            .optional()?
            .is_some())
    }

    pub fn record_shipped(&self, name: &str, uri: &str, size: i64, sha256: &str, now: f64) -> Result<()> {
        self.conn.execute(
            "INSERT OR REPLACE INTO shipped_log (name, uri, size, sha256, shipped_at) VALUES (?, ?, ?, ?, ?)",
            params![name, uri, size, sha256, now],
        )?;
        Ok(())
    }
}

// -- scan helpers (inside a transaction) -------------------------------------

/// Files not yet finalized with a made copy at `dest` (`extra`: more
/// conditions on that copy, `c`).
fn live_copies(conn: &Connection, pipeline: &str, dest: &str, extra: &str) -> rusqlite::Result<Vec<i64>> {
    let sql = format!(
        "SELECT f.id FROM file f JOIN unit u ON u.id = f.unit_id JOIN copy c ON c.file_id = f.id \
         WHERE u.pipeline = ? AND c.dest = ? AND c.state = 'done' AND f.state IN ('pending', 'uploaded') \
         AND u.state NOT IN ('finalized', 'abandoned', 'gone') {extra}"
    );
    let mut stmt = conn.prepare(&sql)?;
    stmt.query_map(params![pipeline, dest], |r| r.get(0))?.collect()
}

/// Make the copies of `files` at `dest` pending, so they go up again.
fn forget_copies(conn: &Connection, files: &[i64], dest: &str, dests: &[String]) -> rusqlite::Result<()> {
    for id in files {
        conn.execute(
            "UPDATE copy SET state = 'pending', up_size = NULL, up_mtime_ns = NULL, etag = NULL, verified = NULL, \
             uploaded_at = NULL, attempts = 0, next_attempt = NULL, last_error = NULL, alerted = 0 \
             WHERE file_id = ? AND dest = ?",
            params![id, dest],
        )?;
        refresh_file(conn, *id, dests)?;
    }
    Ok(())
}

fn refresh_file(conn: &Connection, file_id: i64, dests: &[String]) -> rusqlite::Result<()> {
    if dests.is_empty() {
        return Ok(());
    }
    let sql = format!(
        "UPDATE file AS f SET state = CASE WHEN EXISTS (SELECT 1 FROM copy c WHERE c.file_id = f.id \
         AND c.dest IN ({}) AND {STALE}) THEN 'pending' ELSE 'uploaded' END \
         WHERE f.id = ? AND f.state IN ('pending', 'uploaded')",
        marks(dests.len())
    );
    let args = values(dests).chain(std::iter::once(Value::from(file_id)));
    conn.execute(&sql, params_from_iter(args))?;
    Ok(())
}

fn reopen(conn: &Connection, unit_id: i64, reason: &str) -> rusqlite::Result<()> {
    conn.execute(
        "UPDATE unit SET state = 'open', completed_at = NULL, forced = 0, attempts = 0, \
         next_attempt = NULL, last_error = NULL, alerted = 0, note = ? WHERE id = ?",
        params![format!("reopened: {reason}"), unit_id],
    )?;
    conn.execute(
        "UPDATE outbox SET superseded = 1 WHERE unit_id = ? AND done_at IS NULL",
        [unit_id],
    )?;
    Ok(())
}

/// The first error, and how many more there were.
pub fn summarize(errors: &[String]) -> String {
    match errors {
        [] => String::new(),
        [only] => only.clone(),
        [first, rest @ ..] => format!("{first} (and {} more)", rest.len()),
    }
}

fn mark_unreadable(tx: &Connection, unit: &UnitRow, why: &str, changes: &mut ScanChanges) -> Result<()> {
    tx.execute("UPDATE unit SET scan_error = ? WHERE id = ?", params![why, unit.id])?;
    if unit.scan_error.is_none() {
        changes.unreadable.push((unit.key.clone(), why.to_string()));
    }
    Ok(())
}

/// Fold one unit's observation in. With `error`, the observation is partial:
/// what it saw counts, but nothing it did not see counts as gone.
#[allow(clippy::too_many_arguments)]
fn observe_unit(
    tx: &Connection,
    pipeline: &str,
    unit: Option<&UnitRow>,
    obs: &UnitObs,
    error: Option<&str>,
    now: f64,
    dests: &[String],
    changes: &mut ScanChanges,
) -> Result<()> {
    let newest = if obs.newest_ns > 0 {
        obs.newest_ns as f64 / 1e9
    } else {
        now
    };
    let (unit_id, state, existing) = match unit {
        None => {
            tx.execute(
                "INSERT INTO unit (pipeline, key, first_seen, last_change, marker, scan_error) \
                 VALUES (?, ?, ?, ?, ?, ?)",
                params![pipeline, obs.key, now, newest, obs.marker, error],
            )?;
            changes.new_units.push(obs.key.clone());
            if let Some(why) = error {
                changes.unreadable.push((obs.key.clone(), why.to_string()));
            }
            (tx.last_insert_rowid(), OPEN.to_string(), BTreeMap::new())
        }
        Some(u) if u.state == ABANDONED => return Ok(()),
        Some(u) => {
            match error {
                Some(why) => {
                    // Part of it unread: never move activity back, never lose a marker.
                    tx.execute(
                        "UPDATE unit SET last_change = MAX(last_change, ?), marker = MAX(marker, ?), \
                         present = 1, scan_error = ? WHERE id = ?",
                        params![newest, obs.marker, why, u.id],
                    )?;
                    if u.scan_error.is_none() {
                        changes.unreadable.push((obs.key.clone(), why.to_string()));
                    }
                }
                None => {
                    tx.execute(
                        "UPDATE unit SET last_change = ?, marker = ?, present = 1, scan_error = NULL WHERE id = ?",
                        params![newest, obs.marker, u.id],
                    )?;
                }
            }
            let mut stmt = tx.prepare_cached("SELECT * FROM file WHERE unit_id = ?")?;
            let files = stmt
                .query_map([u.id], FileRow::from_row)?
                .map(|r| r.map(|f| (f.rel.clone(), f)))
                .collect::<rusqlite::Result<BTreeMap<_, _>>>()?;
            (u.id, u.state.clone(), files)
        }
    };
    let mut reason = observe_files(tx, unit_id, obs, existing, error.is_none(), dests, changes)?;
    if state == GONE && !obs.files.is_empty() {
        reason = reason.or_else(|| Some("reappeared".into()));
    }
    if let Some(reason) = reason
        && state != OPEN
    {
        reopen(tx, unit_id, &reason)?;
        changes.reopened.push((obs.key.clone(), reason));
    }
    Ok(())
}

/// Update a unit's file rows; return why completion no longer holds. Only a
/// `complete` observation can tell that a file is gone.
fn observe_files(
    tx: &Connection,
    unit_id: i64,
    obs: &UnitObs,
    mut existing: BTreeMap<String, FileRow>,
    complete: bool,
    dests: &[String],
    changes: &mut ScanChanges,
) -> Result<Option<String>> {
    let mut reason: Option<String> = None;
    for (rel, seen) in &obs.files {
        let size = seen.size as i64;
        match existing.remove(rel) {
            None => {
                tx.execute(
                    "INSERT INTO file (unit_id, rel, within, size, mtime_ns) VALUES (?, ?, ?, ?, ?)",
                    params![unit_id, rel, seen.within, size, seen.mtime_ns],
                )?;
                let file_id = tx.last_insert_rowid();
                for dest in dests {
                    tx.execute("INSERT INTO copy (file_id, dest) VALUES (?, ?)", params![file_id, dest])?;
                }
                changes.new_files += 1;
                reason.get_or_insert_with(|| format!("new file {}", seen.within));
            }
            Some(row) => {
                let moved = (row.size, row.mtime_ns) != (size, seen.mtime_ns);
                if moved || !row.present || row.state == GONE {
                    let state = if row.state == GONE { PENDING } else { row.state.as_str() };
                    tx.execute(
                        "UPDATE file SET size = ?, mtime_ns = ?, present = 1, state = ? WHERE id = ?",
                        params![size, seen.mtime_ns, state, row.id],
                    )?;
                }
                if moved {
                    // Copies of the old bytes no longer count; back off from a clean slate.
                    tx.execute(
                        "UPDATE copy SET attempts = 0, next_attempt = NULL, last_error = NULL, alerted = 0 \
                         WHERE file_id = ?",
                        [row.id],
                    )?;
                    if row.state == FINALIZED {
                        tx.execute("UPDATE file SET state = 'pending' WHERE id = ?", [row.id])?;
                        for dest in dests {
                            tx.execute(
                                "INSERT OR IGNORE INTO copy (file_id, dest) VALUES (?, ?)",
                                params![row.id, dest],
                            )?;
                        }
                    }
                    changes.changed_files += 1;
                    let what = if row.state == PENDING {
                        "changed"
                    } else {
                        "changed after upload"
                    };
                    reason.get_or_insert_with(|| format!("{} {what}", seen.within));
                }
                if row.state == GONE {
                    reason.get_or_insert_with(|| format!("{} reappeared", seen.within));
                }
                refresh_file(tx, row.id, dests)?;
            }
        }
    }
    if !complete {
        return Ok(reason);
    }
    for row in existing.values() {
        if !row.present {
            continue;
        }
        if row.state == PENDING {
            tx.execute("UPDATE file SET state = 'gone', present = 0 WHERE id = ?", [row.id])?;
            changes.gone_files.push(row.rel.clone());
            reason.get_or_insert_with(|| format!("{} disappeared", row.within));
        } else {
            tx.execute("UPDATE file SET present = 0 WHERE id = ?", [row.id])?;
        }
    }
    Ok(reason)
}

fn unit_missing(tx: &Connection, unit: &UnitRow, changes: &mut ScanChanges) -> Result<()> {
    if unit.state == ABANDONED || unit.state == GONE || !unit.present {
        return Ok(());
    }
    let states: Vec<String> = {
        let mut stmt = tx.prepare_cached("SELECT state FROM file WHERE unit_id = ?")?;
        stmt.query_map([unit.id], |r| r.get(0))?
            .collect::<rusqlite::Result<_>>()?
    };
    if states.is_empty() {
        tx.execute("DELETE FROM unit WHERE id = ?", [unit.id])?;
        return Ok(());
    }
    tx.execute("UPDATE unit SET present = 0 WHERE id = ?", [unit.id])?;
    tx.execute(
        "UPDATE file SET present = 0, state = CASE state WHEN 'pending' THEN 'gone' ELSE state END \
         WHERE unit_id = ?",
        [unit.id],
    )?;
    let anything_uploaded = states.iter().any(|s| s == UPLOADED || s == FINALIZED);
    let any_copy: bool = tx
        .query_row(
            "SELECT 1 FROM copy c JOIN file f ON f.id = c.file_id WHERE f.unit_id = ? AND c.state = 'done' LIMIT 1",
            [unit.id],
            |_| Ok(()),
        )
        .optional()?
        .is_some();
    if unit.state == OPEN && !anything_uploaded && !any_copy {
        tx.execute("UPDATE unit SET state = 'gone' WHERE id = ?", [unit.id])?;
        changes.gone_units.push(unit.key.clone());
    }
    Ok(())
}

/// Distinct values, in order: for building `IN (...)` lists.
pub fn unique(items: impl IntoIterator<Item = String>) -> Vec<String> {
    let mut seen = BTreeSet::new();
    items.into_iter().filter(|i| seen.insert(i.clone())).collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::units::FileObs;

    fn dests() -> Vec<String> {
        vec!["a".into(), "b".into()]
    }

    fn unit(key: &str, files: &[(&str, u64, i64)]) -> UnitObs {
        let mut obs = UnitObs {
            key: key.into(),
            newest_ns: 1,
            ..Default::default()
        };
        for (within, size, mtime) in files {
            let rel = format!("{key}/{within}");
            obs.files.insert(
                rel.clone(),
                FileObs {
                    rel,
                    within: within.to_string(),
                    size: *size,
                    mtime_ns: *mtime,
                },
            );
        }
        obs
    }

    fn scan(units: Vec<UnitObs>, scope: Option<&[&str]>) -> Scan {
        Scan {
            units: units.into_iter().map(|u| (u.key.clone(), u)).collect(),
            scope: scope.map(|s| s.iter().map(|k| k.to_string()).collect()),
            ..Default::default()
        }
    }

    fn states(j: &Journal, key: &str) -> Vec<(String, String)> {
        let u = j.find_unit("p", key).unwrap().unwrap();
        j.files(u.id)
            .unwrap()
            .into_iter()
            .map(|f| (f.within, f.state))
            .collect()
    }

    fn copy_all(j: &mut Journal, key: &str, only: &[&str]) {
        let u = j.find_unit("p", key).unwrap().unwrap();
        for f in j.files(u.id).unwrap() {
            for dest in only {
                j.record_copy(f.id, dest, f.size, f.mtime_ns, None, "size", 1.0, &dests())
                    .unwrap();
            }
        }
    }

    #[test]
    fn a_file_is_uploaded_only_once_every_destination_has_it() {
        let mut j = Journal::in_memory().unwrap();
        let changes = j
            .apply_scan("p", &scan(vec![unit("u", &[("a", 1, 1)])], None), 10.0, &dests())
            .unwrap();
        assert_eq!((changes.new_units.len(), changes.new_files), (1, 1));
        copy_all(&mut j, "u", &["a"]);
        assert_eq!(states(&j, "u"), [("a".into(), "pending".into())]);
        let u = j.find_unit("p", "u").unwrap().unwrap();
        assert!(j.copies_current(u.id, &["a".into()]).unwrap());
        assert!(!j.copies_current(u.id, &dests()).unwrap());
        copy_all(&mut j, "u", &["b"]);
        assert_eq!(states(&j, "u"), [("a".into(), "uploaded".into())]);
        assert!(j.copies_current(u.id, &dests()).unwrap());
    }

    #[test]
    fn a_change_after_upload_makes_every_copy_stale_and_reopens_the_unit() {
        let mut j = Journal::in_memory().unwrap();
        j.apply_scan("p", &scan(vec![unit("u", &[("a", 1, 1)])], None), 10.0, &dests())
            .unwrap();
        copy_all(&mut j, "u", &["a", "b"]);
        let u = j.find_unit("p", "u").unwrap().unwrap();
        j.complete_unit(u.id, 11.0).unwrap();
        j.set_unit(u.id, None, None, &[("state", Value::from(FINALIZED.to_string()))])
            .unwrap();
        j.enqueue(u.id, 1, "r", "finalize", true, "{}", 12.0).unwrap();

        let changes = j
            .apply_scan("p", &scan(vec![unit("u", &[("a", 5, 9)])], None), 13.0, &dests())
            .unwrap();

        assert_eq!(
            changes.reopened,
            [("u".to_string(), "a changed after upload".to_string())]
        );
        assert_eq!(states(&j, "u"), [("a".into(), "pending".into())]);
        assert_eq!(j.unit(u.id).unwrap().state, OPEN);
        assert!(!j.copies_current(u.id, &["a".into()]).unwrap());
        assert!(j.due_outbox("p", 100.0).unwrap().is_empty()); // superseded
        let candidates = j.upload_candidates("p", 100.0, i64::MAX, &[OPEN], &dests()).unwrap();
        assert_eq!(candidates[0].dests, ["a", "b"]);
    }

    #[test]
    fn a_pending_file_that_disappears_is_gone_but_an_uploaded_one_is_not() {
        let mut j = Journal::in_memory().unwrap();
        j.apply_scan(
            "p",
            &scan(vec![unit("u", &[("a", 1, 1), ("b", 1, 1)])], None),
            1.0,
            &dests(),
        )
        .unwrap();
        let u = j.find_unit("p", "u").unwrap().unwrap();
        let b = j.files(u.id).unwrap().into_iter().find(|f| f.within == "b").unwrap();
        for dest in dests() {
            j.record_copy(b.id, &dest, 1, 1, None, "size", 1.0, &dests()).unwrap();
        }

        let changes = j
            .apply_scan("p", &scan(vec![unit("u", &[])], None), 2.0, &dests())
            .unwrap();

        assert_eq!(changes.gone_files, ["u/a"]);
        assert_eq!(
            states(&j, "u"),
            [("a".into(), "gone".into()), ("b".into(), "uploaded".into())]
        );
    }

    #[test]
    fn scoped_scans_only_judge_absence_within_their_scope() {
        let mut j = Journal::in_memory().unwrap();
        j.apply_scan(
            "p",
            &scan(vec![unit("u", &[("a", 1, 1)]), unit("v", &[("a", 1, 1)])], None),
            1.0,
            &dests(),
        )
        .unwrap();
        j.apply_scan("p", &scan(vec![unit("u", &[("a", 1, 1)])], Some(&["u"])), 2.0, &dests())
            .unwrap();
        assert_eq!(states(&j, "v"), [("a".into(), "pending".into())]);
        j.apply_scan("p", &scan(vec![], Some(&["v"])), 3.0, &dests()).unwrap();
        assert_eq!(states(&j, "v"), [("a".into(), "gone".into())]);
        assert_eq!(j.find_unit("p", "v").unwrap().unwrap().state, GONE);
    }

    #[test]
    fn a_copy_made_from_bytes_a_scan_has_since_moved_past_does_not_count() {
        let mut j = Journal::in_memory().unwrap();
        j.apply_scan("p", &scan(vec![unit("u", &[("a", 1, 1)])], None), 1.0, &dests())
            .unwrap();
        let u = j.find_unit("p", "u").unwrap().unwrap();
        let file = j.files(u.id).unwrap().remove(0);
        j.apply_scan("p", &scan(vec![unit("u", &[("a", 2, 2)])], None), 2.0, &dests())
            .unwrap();

        for dest in dests() {
            j.record_copy(file.id, &dest, 1, 1, None, "size", 3.0, &dests())
                .unwrap();
        }

        assert_eq!(states(&j, "u"), [("a".into(), "pending".into())]);
    }

    #[test]
    fn a_new_destination_picks_up_what_is_not_yet_finalized() {
        let mut j = Journal::in_memory().unwrap();
        let one = vec!["a".to_string()];
        j.apply_scan("p", &scan(vec![unit("u", &[("x", 1, 1)])], None), 1.0, &one)
            .unwrap();
        let u = j.find_unit("p", "u").unwrap().unwrap();
        let f = j.files(u.id).unwrap().remove(0);
        j.record_copy(f.id, "a", 1, 1, None, "size", 1.0, &one).unwrap();
        assert_eq!(states(&j, "u"), [("x".into(), "uploaded".into())]);

        j.ensure_copies("p", &dests()).unwrap();

        assert_eq!(states(&j, "u"), [("x".into(), "pending".into())]);
        let candidates = j.upload_candidates("p", 5.0, i64::MAX, &[OPEN], &dests()).unwrap();
        assert_eq!(candidates[0].dests, ["b"]);
    }

    #[test]
    fn a_finalize_waits_behind_an_undelivered_register() {
        let mut j = Journal::in_memory().unwrap();
        j.apply_scan("p", &scan(vec![unit("u", &[("a", 1, 1)])], None), 1.0, &dests())
            .unwrap();
        let u = j.find_unit("p", "u").unwrap().unwrap();
        let register = j.enqueue(u.id, 1, "r", "register", true, "{}", 1.0).unwrap();
        let finalize = j.enqueue(u.id, 1, "r", "finalize", true, "{}", 2.0).unwrap();
        let other = j.enqueue(u.id, 1, "q", "register", false, "{}", 3.0).unwrap();
        let ids = |rows: Vec<OutboxRow>| rows.into_iter().map(|r| r.id).collect::<Vec<_>>();

        assert_eq!(ids(j.due_outbox("p", 10.0).unwrap()), [register, other]);
        j.outbox_failed(register, "down", 50.0).unwrap();
        assert_eq!(ids(j.due_outbox("p", 10.0).unwrap()), [other]);
        assert!(!j.required_done(u.id, 1, "register").unwrap());
        j.outbox_done(register, 60.0).unwrap();
        assert_eq!(ids(j.due_outbox("p", 60.0).unwrap()), [finalize, other]);
        assert!(j.required_done(u.id, 1, "register").unwrap());
    }

    #[test]
    fn a_unit_that_moved_on_is_not_advanced() {
        let mut j = Journal::in_memory().unwrap();
        j.apply_scan("p", &scan(vec![unit("u", &[("a", 1, 1)])], None), 1.0, &dests())
            .unwrap();
        let u = j.find_unit("p", "u").unwrap().unwrap();
        let registrars = [("r".to_string(), true)];
        let advance = |j: &mut Journal, generation| {
            j.advance(
                u.id,
                generation,
                COMPLETE,
                REGISTERING,
                None,
                "register",
                &registrars,
                "{}",
                &[],
                2.0,
            )
        };
        assert_eq!(j.complete_unit(u.id, 2.0).unwrap(), Some(1));
        // A new file reopens the unit between building its manifest and queueing it.
        j.apply_scan(
            "p",
            &scan(vec![unit("u", &[("a", 1, 1), ("b", 1, 1)])], None),
            3.0,
            &dests(),
        )
        .unwrap();
        assert!(!advance(&mut j, 1).unwrap());
        assert_eq!(j.complete_unit(u.id, 4.0).unwrap(), Some(2));
        assert!(!advance(&mut j, 1).unwrap(), "a stale generation");
        assert!(j.outbox_for_unit(u.id).unwrap().is_empty());
        assert!(advance(&mut j, 2).unwrap());
        assert_eq!(j.unit(u.id).unwrap().state, REGISTERING);
        assert_eq!(j.outbox_for_unit(u.id).unwrap().len(), 1);
    }

    #[test]
    fn abandon_and_retry() {
        let mut j = Journal::in_memory().unwrap();
        j.apply_scan("p", &scan(vec![unit("u", &[("a", 1, 1)])], None), 1.0, &dests())
            .unwrap();
        let u = j.find_unit("p", "u").unwrap().unwrap();
        let f = j.files(u.id).unwrap().remove(0);
        j.copy_failed(f.id, "a", "boom", 99.0).unwrap();
        j.abandon("p", "u").unwrap();
        j.apply_scan(
            "p",
            &scan(vec![unit("u", &[("a", 1, 1), ("b", 1, 1)])], None),
            2.0,
            &dests(),
        )
        .unwrap();
        assert_eq!(states(&j, "u"), [("a".into(), "pending".into())]);

        assert_eq!(j.retry(Some("p"), Some("u")).unwrap(), 1);

        assert_eq!(j.unit(u.id).unwrap().state, OPEN);
        let copy = j.copies(f.id).unwrap().into_iter().find(|c| c.dest == "a").unwrap();
        assert_eq!((copy.attempts, copy.next_attempt), (0, None));
    }

    #[test]
    fn a_newer_schema_is_refused() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("journal.sqlite3");
        Journal::open(&path).unwrap().set_meta("schema_version", "99").unwrap();
        let err = Journal::open(&path).err().unwrap().to_string();
        assert!(err.contains("upgrade ferry"), "{err}");
    }

    fn binding(identity: &str, verify: &str) -> Binding {
        Binding {
            name: "a".into(),
            identity: identity.into(),
            verify: verify.into(),
        }
    }

    #[test]
    fn a_destination_that_moved_waits_to_be_accepted_or_reset() {
        let mut j = Journal::in_memory().unwrap();
        j.apply_scan("p", &scan(vec![unit("u", &[("a", 1, 1)])], None), 1.0, &dests())
            .unwrap();
        copy_all(&mut j, "u", &["a", "b"]);
        assert!(
            j.bind_destinations("p", &[binding("s3://x/raw", "size")], &dests())
                .unwrap()
                .is_empty()
        );

        let moved = j
            .bind_destinations("p", &[binding("s3://y/raw", "size")], &dests())
            .unwrap();
        assert_eq!(
            moved,
            [Moved {
                name: "a".into(),
                was: "s3://x/raw".into(),
                now: "s3://y/raw".into()
            }]
        );
        assert_eq!(
            states(&j, "u"),
            [("a".into(), "uploaded".into())],
            "nothing changes on its own"
        );

        let preview = j
            .reset_destination("p", &binding("s3://y/raw", "size"), &dests(), false)
            .unwrap();
        assert_eq!(preview.files, 1);
        assert_eq!(j.destination("p", "a").unwrap().as_deref(), Some("s3://x/raw"));
        j.reset_destination("p", &binding("s3://y/raw", "size"), &dests(), true)
            .unwrap();
        assert_eq!(states(&j, "u"), [("a".into(), "pending".into())]);
        assert!(
            j.bind_destinations("p", &[binding("s3://y/raw", "size")], &dests())
                .unwrap()
                .is_empty()
        );

        copy_all(&mut j, "u", &["a"]);
        j.accept_destination("p", &binding("s3://z/raw", "size")).unwrap();
        assert!(
            j.bind_destinations("p", &[binding("s3://z/raw", "size")], &dests())
                .unwrap()
                .is_empty()
        );
        assert_eq!(
            states(&j, "u"),
            [("a".into(), "uploaded".into())],
            "accepting keeps the copies"
        );
    }

    #[test]
    fn stronger_verification_sends_what_it_did_not_cover_again() {
        let mut j = Journal::in_memory().unwrap();
        j.apply_scan("p", &scan(vec![unit("u", &[("a", 1, 1)])], None), 1.0, &dests())
            .unwrap();
        copy_all(&mut j, "u", &["a", "b"]);
        j.bind_destinations("p", &[binding("s3://x", "size")], &dests())
            .unwrap();

        j.bind_destinations("p", &[binding("s3://x", "sha256")], &dests())
            .unwrap();
        assert_eq!(states(&j, "u"), [("a".into(), "pending".into())]);
        // Weaker again: sha256-verified copies satisfy size.
        copy_all(&mut j, "u", &["a"]);
        j.bind_destinations("p", &[binding("s3://x", "size")], &dests())
            .unwrap();
        assert_eq!(states(&j, "u"), [("a".into(), "uploaded".into())]);
    }

    #[test]
    fn a_partial_look_keeps_what_it_did_not_see() {
        let mut j = Journal::in_memory().unwrap();
        j.apply_scan(
            "p",
            &scan(vec![unit("u", &[("a", 1, 1), ("b", 1, 1)])], None),
            1.0,
            &dests(),
        )
        .unwrap();
        let mut partial = unit("u", &[("a", 1, 1)]);
        partial.errors.push("cannot list /r/u/sub: Permission denied".into());

        let changes = j.apply_scan("p", &scan(vec![partial], None), 2.0, &dests()).unwrap();
        assert_eq!(
            changes.unreadable,
            [("u".to_string(), "cannot list /r/u/sub: Permission denied".to_string())]
        );
        assert!(changes.gone_files.is_empty());
        assert_eq!(
            states(&j, "u"),
            [("a".into(), "pending".into()), ("b".into(), "pending".into())]
        );
        assert!(j.find_unit("p", "u").unwrap().unwrap().scan_error.is_some());

        // Seen in full again: the error clears, and only now is b gone.
        let changes = j
            .apply_scan("p", &scan(vec![unit("u", &[("a", 1, 1)])], None), 3.0, &dests())
            .unwrap();
        assert_eq!(changes.gone_files, ["u/b"]);
        assert!(j.find_unit("p", "u").unwrap().unwrap().scan_error.is_none());
    }

    #[test]
    fn nothing_is_gone_after_a_scan_that_could_not_see_everything() {
        let mut j = Journal::in_memory().unwrap();
        j.apply_scan("p", &scan(vec![unit("u", &[("a", 1, 1)])], None), 1.0, &dests())
            .unwrap();
        let blind = Scan {
            errors: vec!["cannot list /r: Input/output error".into()],
            ..Default::default()
        };
        let changes = j.apply_scan("p", &blind, 2.0, &dests()).unwrap();
        assert!(changes.gone_files.is_empty() && changes.gone_units.is_empty());
        assert!(j.find_unit("p", "u").unwrap().unwrap().present);
        assert_eq!(states(&j, "u"), [("a".into(), "pending".into())]);

        // An error a regex layout cannot place could hide any unit's files.
        let everywhere = Scan {
            errors: vec!["cannot list /r/x: Permission denied".into()],
            unattributed: true,
            ..Default::default()
        };
        let changes = j.apply_scan("p", &everywhere, 3.0, &dests()).unwrap();
        assert_eq!(changes.unreadable.len(), 1);
        assert!(j.find_unit("p", "u").unwrap().unwrap().scan_error.is_some());
    }

    #[test]
    fn each_generation_of_a_unit_is_announced_once() {
        let mut j = Journal::in_memory().unwrap();
        j.apply_scan("p", &scan(vec![unit("u", &[("a", 1, 1)])], None), 1.0, &dests())
            .unwrap();
        let u = j.find_unit("p", "u").unwrap().unwrap();
        assert!(j.unannounced("p").unwrap().is_empty(), "open units are not delivered");
        let registered = [
            ("state", Value::from(REGISTERED.to_string())),
            ("generation", Value::from(1)),
        ];
        j.set_unit(u.id, None, None, &registered).unwrap();

        let chat = ["notifier:chat".to_string()];
        assert!(j.announce(u.id, 1, &chat, "{}", 2.0).unwrap());
        assert!(!j.announce(u.id, 1, &chat, "{}", 3.0).unwrap());
        assert!(j.unannounced("p").unwrap().is_empty());
        let queued: Vec<(String, String, bool)> = j
            .outbox_for_unit(u.id)
            .unwrap()
            .into_iter()
            .map(|o| (o.event, o.registrar, o.required))
            .collect();
        assert_eq!(queued, [("receipt".into(), "notifier:chat".into(), false)]);

        j.set_unit(u.id, None, None, &[("generation", Value::from(2))]).unwrap();
        assert_eq!(j.unannounced("p").unwrap()[0].receipt_gen, Some(1));
        assert!(!j.announce(u.id, 1, &chat, "{}", 4.0).unwrap(), "a stale generation");
        assert!(
            j.announce(u.id, 2, &[], "", 5.0).unwrap(),
            "announced with nobody listening"
        );
        assert_eq!(j.outbox_for_unit(u.id).unwrap().len(), 1);
    }

    #[test]
    fn upgrading_a_version_1_journal_announces_nothing_already_delivered() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("journal.sqlite3");
        Journal::open(&path)
            .unwrap()
            .conn
            .execute_batch(
                "INSERT INTO unit (pipeline, key, state, generation, first_seen, last_change) VALUES \
                 ('p', 'done', 'finalized', 3, 1, 1), ('p', 'waiting', 'registered', 1, 1, 1), \
                 ('p', 'busy', 'open', 1, 1, 1);
                 DROP INDEX unit_unannounced;
                 ALTER TABLE unit DROP COLUMN receipt_gen;
                 UPDATE meta SET value = '1' WHERE key = 'schema_version';",
            )
            .unwrap();

        let j = Journal::open(&path).unwrap();
        assert_eq!(j.get_meta("schema_version").unwrap(), Some(SCHEMA_VERSION.to_string()));
        let gen_of = |key: &str| j.find_unit("p", key).unwrap().unwrap().receipt_gen;
        assert_eq!(
            (gen_of("done"), gen_of("waiting"), gen_of("busy")),
            (Some(3), Some(1), None)
        );
        assert!(j.unannounced("p").unwrap().is_empty());
    }
}
