//! The engine: one pipeline's journey from a watched folder to its
//! destinations and registrars.
//!
//! `mode: cron` runs [`Engine::run_pass`] on its schedule (and `ferry run` runs
//! it once); `mode: stream` drives the same steps continuously (see
//! [`crate::stream`]). Every step reads and writes the journal, so a crash, a
//! reboot or a killed process at any point leaves a state the next pass carries
//! on from. Three rules hold throughout:
//!
//! - A file goes up once it has settled, and a copy is recorded only if the file
//!   did not change while it was read, and the copy verified.
//! - A unit is registered once it is complete and every *required* destination
//!   holds a verified copy of every file -- never with a partial file list.
//! - Source files are kept, moved or deleted only once *every* destination holds
//!   a verified copy, every required registrar has confirmed the unit, any
//!   `finalize.after` grace has passed, and the files still match what was sent.
//!
//! Each generation of a unit then gets one receipt for the notifiers that take
//! receipts: once its source files were dealt with, or as soon as only the
//! grace period is left, so a long grace does not delay the news.

use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::path::PathBuf;
use std::sync::{Arc, Mutex, MutexGuard};

use anyhow::{Context, Result, anyhow, bail};
use futures::StreamExt;
use futures::future::join_all;
use object_store::ObjectStore;
use rusqlite::types::Value;
use serde::Serialize;
use tokio::sync::Semaphore;
use tokio_util::sync::CancellationToken;

use crate::config::{Action, Adopt, Fanout, Level, PipelineConfig, Upload, Verify};
use crate::hooks::{Alerts, Notifier, Registrar};
use crate::journal::{
    Binding, COMPLETE, Candidate, FINALIZED, FileRow, Journal, NewTransfer, OPEN, OutboxRow, REGISTERED, REGISTERING,
    ScanChanges, TransferRow, UPLOADED, UnitRow, summarize,
};
use crate::manifest::{
    Alert, Event, EventKind, FileCopy, Manifest, ManifestDestination, ManifestFile, Original, Receipt, ReceiptCopy,
    TransferEvent,
};
use crate::stores::Location;
use crate::transfer::{self, Fanout as FanoutResult, ReadError, Target, UploadSettings};
use crate::units::Layout;
use crate::util::{Clock, human_bytes, human_duration, iso};
use crate::winfs::held_open;

/// One place a pipeline's files go.
#[derive(Debug, Clone)]
pub struct Destination {
    pub key: String,
    /// The store, rooted at the pipeline's prefix.
    pub location: Location,
    pub required: bool,
    pub verify: Verify,
}

#[derive(Clone)]
pub struct BoundRegistrar {
    pub key: String,
    pub required: bool,
    pub registrar: Arc<dyn Registrar>,
}

/// What a pass (or, in stream mode, a digest period) did to a pipeline.
#[derive(Debug, Clone, Default, Serialize)]
pub struct PassReport {
    pub pipeline: String,
    pub started: f64,
    pub finished: Option<f64>,
    /// Files read and sent (once each, however many destinations).
    pub uploaded: usize,
    pub uploaded_bytes: u64,
    /// Destination copies made, and found already in place.
    pub copies: usize,
    pub adopted: usize,
    pub upload_failures: usize,
    pub registered: Vec<String>,
    pub finalized: Vec<String>,
    pub reopened: Vec<(String, String)>,
    pub waiting: BTreeMap<String, String>,
    pub delivery_failures: usize,
    pub finalize_failures: usize,
    pub errors: Vec<String>,
}

impl PassReport {
    fn new(pipeline: &str, started: f64) -> Self {
        Self {
            pipeline: pipeline.to_string(),
            started,
            ..Default::default()
        }
    }

    pub fn failures(&self) -> usize {
        self.upload_failures + self.delivery_failures + self.finalize_failures + self.errors.len()
    }

    pub fn active(&self) -> bool {
        self.uploaded > 0
            || self.adopted > 0
            || !self.registered.is_empty()
            || !self.finalized.is_empty()
            || !self.reopened.is_empty()
            || self.failures() > 0
    }

    pub fn render(&self) -> String {
        let mut head = format!(
            "{}: {} unit(s) registered, {} finalized; {} file(s) uploaded ({})",
            self.pipeline,
            self.registered.len(),
            self.finalized.len(),
            self.uploaded,
            human_bytes(self.uploaded_bytes)
        );
        if self.adopted > 0 {
            head.push_str(&format!(", {} copies already in place", self.adopted));
        }
        if !self.waiting.is_empty() {
            head.push_str(&format!("; {} unit(s) waiting", self.waiting.len()));
        }
        match self.failures() {
            0 => head.push_str("; no failures."),
            n => head.push_str(&format!("; {n} failure(s).")),
        }
        let mut lines = vec![head];
        let finalized: BTreeSet<&String> = self.finalized.iter().collect();
        for key in &self.registered {
            let done = if finalized.contains(key) { ", finalized" } else { "" };
            lines.push(format!("• {key} — registered{done}"));
        }
        for key in self.finalized.iter().filter(|k| !self.registered.contains(k)) {
            lines.push(format!("• {key} — finalized"));
        }
        for (key, why) in &self.reopened {
            lines.push(format!("• {key} — reopened: {why}"));
        }
        for (key, why) in &self.waiting {
            lines.push(format!("• {key} — {why}"));
        }
        for error in &self.errors {
            lines.push(format!("• error: {error}"));
        }
        lines.join("\n")
    }

    /// The digest as an alert: `info`, or `warning` if anything failed.
    pub fn alert(&self) -> Alert {
        let text = self.render();
        let (title, message) = text.split_once('\n').unwrap_or((&text, ""));
        Alert {
            level: if self.failures() > 0 {
                Level::Warning
            } else {
                Level::Info
            },
            title: title.to_string(),
            message: message.to_string(),
            pipeline: Some(self.pipeline.clone()),
            unit: None,
        }
    }
}

fn transfer_event(row: &TransferRow) -> TransferEvent {
    TransferEvent {
        rel_path: row.rel.clone(),
        destination: row.dest.clone(),
        op: row.op.clone(),
        status: row.status.clone(),
        src: row.src.clone(),
        dst: row.dst.clone(),
        at: iso(row.at),
        bytes: row.bytes,
        duration_ms: row.duration_ms,
        error: row.error.clone(),
    }
}

enum FinalizeError {
    Changed(i64, String),
    Failed(anyhow::Error),
}

/// Who an outbox row is for.
enum Recipient<'a> {
    Registrar(&'a BoundRegistrar),
    Notifier(&'a dyn Notifier),
}

/// The outbox's name for a notifier, kept apart from registrar names.
fn receipt_key(notifier: &dyn Notifier) -> String {
    format!("notifier:{}", notifier.name())
}

/// What delivering `row` does, for logs and alerts.
fn describe(row: &OutboxRow, unit: &str) -> String {
    match row.registrar.strip_prefix("notifier:") {
        Some(notifier) if row.event == EventKind::Receipt.as_str() => {
            format!("post the receipt for {unit} to {notifier}")
        }
        _ => format!("{} {unit} with {}", row.event, row.registrar),
    }
}

pub struct EngineParts {
    pub node: String,
    pub cfg: PipelineConfig,
    pub destinations: Vec<Destination>,
    pub registrars: Vec<BoundRegistrar>,
    /// The notifiers that take receipts.
    pub notifiers: Vec<Arc<dyn Notifier>>,
    /// Where each destination points, checked against the journal.
    pub bindings: Vec<Binding>,
    pub journal: Arc<Mutex<Journal>>,
    pub alerts: Arc<Alerts>,
    pub dry_run: bool,
    pub clock: Arc<dyn Clock>,
}

pub struct Engine {
    pub node: String,
    pub name: String,
    pub cfg: PipelineConfig,
    pub destinations: Vec<Destination>,
    pub registrars: Vec<BoundRegistrar>,
    pub alerts: Arc<Alerts>,
    pub layout: Arc<Layout>,
    pub dry_run: bool,
    pub clock: Arc<dyn Clock>,
    notifiers: Vec<Arc<dyn Notifier>>,
    journal: Arc<Mutex<Journal>>,
    settings: UploadSettings,
    report: Mutex<PassReport>,
    adopting: tokio::sync::Mutex<()>,
    moves: Arc<Semaphore>,
    all_keys: Vec<String>,
    required_keys: Vec<String>,
}

impl Engine {
    pub fn new(parts: EngineParts) -> Result<Self> {
        let layout = Arc::new(Layout::new(&parts.cfg)?);
        let all_keys: Vec<String> = parts.destinations.iter().map(|d| d.key.clone()).collect();
        let required_keys = parts
            .destinations
            .iter()
            .filter(|d| d.required)
            .map(|d| d.key.clone())
            .collect();
        {
            let mut journal = parts.journal.lock().map_err(|_| anyhow!("journal lock poisoned"))?;
            let moved = journal.bind_destinations(&parts.cfg.name, &parts.bindings, &all_keys)?;
            if !moved.is_empty() {
                let pipeline = &parts.cfg.name;
                let lines: Vec<String> = moved
                    .iter()
                    .map(|m| {
                        format!(
                            "destination {name} now points at {now}, but its copies were made at {was}. \
                             If the objects are there too, run `ferry destination accept {pipeline} {name}`; \
                             to send what is not yet finalized again, `ferry destination reset {pipeline} {name}` \
                             (either with --dry-run first).",
                            name = m.name,
                            now = m.now,
                            was = m.was,
                        )
                    })
                    .collect();
                bail!("pipeline {pipeline}: {}", lines.join(" "));
            }
            journal.ensure_copies(&parts.cfg.name, &all_keys)?;
        }
        let started = parts.clock.now();
        Ok(Self {
            name: parts.cfg.name.clone(),
            settings: UploadSettings::from_limits(&parts.cfg.limits),
            moves: Arc::new(Semaphore::new(parts.cfg.finalize.concurrency)),
            report: Mutex::new(PassReport::new(&parts.cfg.name, started)),
            adopting: tokio::sync::Mutex::new(()),
            node: parts.node,
            cfg: parts.cfg,
            destinations: parts.destinations,
            registrars: parts.registrars,
            notifiers: parts.notifiers,
            alerts: parts.alerts,
            layout,
            dry_run: parts.dry_run,
            clock: parts.clock,
            journal: parts.journal,
            all_keys,
            required_keys,
        })
    }

    // -- helpers ------------------------------------------------------------
    pub fn db(&self) -> MutexGuard<'_, Journal> {
        self.journal.lock().unwrap_or_else(|poisoned| poisoned.into_inner())
    }

    fn now(&self) -> f64 {
        self.clock.now()
    }

    fn settled_ns(&self, now: f64) -> i64 {
        ((now - self.cfg.settle.as_secs_f64()) * 1e9) as i64
    }

    fn upload_states(&self) -> &'static [&'static str] {
        match self.cfg.upload {
            Upload::Unit => &[COMPLETE, REGISTERING, REGISTERED],
            Upload::File => &[OPEN, COMPLETE, REGISTERING, REGISTERED],
        }
    }

    fn report<F: FnOnce(&mut PassReport)>(&self, f: F) {
        if let Ok(mut report) = self.report.lock() {
            f(&mut report);
        }
    }

    fn live(&self, unit_id: i64) -> Result<Vec<FileRow>> {
        Ok(self
            .db()
            .files(unit_id)?
            .into_iter()
            .filter(|f| f.present && f.live())
            .collect())
    }

    fn src(&self, file: &FileRow) -> PathBuf {
        self.cfg.root.join(&file.rel)
    }

    fn record(&self, t: NewTransfer) -> Result<i64> {
        self.db().add_transfer(&t)
    }

    pub fn take_report(&self) -> PassReport {
        let now = self.now();
        let waiting = self.waiting().unwrap_or_default();
        let mut report = self
            .report
            .lock()
            .map(|mut r| std::mem::replace(&mut *r, PassReport::new(&self.name, now)))
            .unwrap_or_else(|_| PassReport::new(&self.name, now));
        report.finished = Some(now);
        report.waiting = waiting;
        report
    }

    // -- scan ---------------------------------------------------------------
    pub async fn scan(&self, keys: Option<Vec<String>>) -> Result<ScanChanges> {
        let layout = self.layout.clone();
        let scan = tokio::task::spawn_blocking(move || layout.scan(keys.as_deref())).await?;
        let changes = self.db().apply_scan(&self.name, &scan, self.now(), &self.all_keys)?;
        let pipeline = self.name.as_str();
        if changes.new_units.len() > 10 {
            tracing::info!(pipeline, "found {} new unit(s)", changes.new_units.len());
        } else {
            for key in &changes.new_units {
                tracing::info!(pipeline, unit = key.as_str(), "new unit {key}");
            }
        }
        for (key, why) in &changes.reopened {
            tracing::warn!(pipeline, unit = key.as_str(), "reopened {key}: {why}");
        }
        self.report(|r| r.reopened.extend(changes.reopened.iter().cloned()));
        for rel in &changes.gone_files {
            tracing::warn!(pipeline, "{rel} disappeared before it was uploaded");
        }
        for key in &changes.gone_units {
            tracing::info!(
                pipeline,
                unit = key.as_str(),
                "unit {key} vanished before any of it was uploaded"
            );
        }
        for (key, why) in &changes.unreadable {
            tracing::warn!(pipeline, unit = key.as_str(), "cannot read all of {key}: {why}");
            self.alerts.emit(Alert {
                level: Level::Warning,
                title: format!("{pipeline}: cannot read all of {key}; it waits until it can"),
                message: why.clone(),
                pipeline: Some(self.name.clone()),
                unit: Some(key.clone()),
            });
        }
        if scan.scope.is_none() {
            self.track_scan_errors(&scan.errors)?;
        }
        Ok(changes)
    }

    /// Alert when a full scan first cannot read part of the tree outside any
    /// unit (where no unit then counts as gone), and log when it can again.
    fn track_scan_errors(&self, errors: &[String]) -> Result<()> {
        let key = format!("scan_errors:{}", self.name);
        let before = self.db().get_meta(&key)?.filter(|s| !s.is_empty());
        let now = summarize(errors);
        let root = self.cfg.root.display();
        let pipeline = self.name.as_str();
        match (&before, now.is_empty()) {
            (None, false) => {
                tracing::warn!(pipeline, "cannot read all of {root}: {now}");
                self.alerts.emit(Alert {
                    level: Level::Warning,
                    title: format!("{pipeline}: cannot read all of {root}; no unit counts as gone meanwhile"),
                    message: now.clone(),
                    pipeline: Some(self.name.clone()),
                    unit: None,
                });
            }
            (Some(_), true) => tracing::info!(pipeline, "{root} reads in full again"),
            _ => {}
        }
        if before.as_deref().unwrap_or_default() != now {
            self.db().set_meta(&key, &now)?;
        }
        Ok(())
    }

    // -- completion ---------------------------------------------------------
    /// Why `unit` is not complete yet, or `None` if it is.
    fn why_open(&self, unit: &UnitRow, files: &[FileRow], now: f64) -> Option<String> {
        if let Some(why) = &unit.scan_error {
            return Some(format!("cannot read all of it: {why}"));
        }
        if files.is_empty() {
            return Some("no files yet".into());
        }
        let settled = self.settled_ns(now);
        let unsettled = files.iter().filter(|f| f.mtime_ns > settled).count();
        if unsettled > 0 {
            return Some(format!("{unsettled} of {} file(s) still settling", files.len()));
        }
        if unit.forced || unit.marker {
            return None;
        }
        let quiet = now - unit.last_change;
        let idle = self.cfg.unit_idle.as_secs_f64();
        (quiet < idle).then(|| {
            format!(
                "changed {} ago; complete after {} without changes",
                human_duration(quiet),
                human_duration(idle)
            )
        })
    }

    /// Complete every open unit that qualifies; alert on ones stuck open.
    pub fn evaluate(&self) -> Result<Vec<String>> {
        let now = self.now();
        let mut completed = Vec::new();
        let units = self.db().units(&self.name, Some(&[OPEN]))?;
        for unit in units {
            let files = self.live(unit.id)?;
            if let Some(why) = self.why_open(&unit, &files, now) {
                self.alert_if_stuck(&unit, &files, &why, now)?;
                continue;
            }
            let Some(generation) = self.db().complete_unit(unit.id, now)? else {
                continue;
            };
            let again = if generation > 1 {
                format!(", generation {generation}")
            } else {
                String::new()
            };
            tracing::info!(
                pipeline = self.name.as_str(),
                unit = unit.key.as_str(),
                "unit {} is complete ({} file(s){again})",
                unit.key,
                files.len()
            );
            completed.push(unit.key);
        }
        Ok(completed)
    }

    fn alert_if_stuck(&self, unit: &UnitRow, files: &[FileRow], why: &str, now: f64) -> Result<()> {
        let Some(limit) = self.cfg.max_open else { return Ok(()) };
        if unit.alerted || files.is_empty() || now - unit.first_seen < limit.as_secs_f64() {
            return Ok(());
        }
        self.db().set_unit(unit.id, None, None, &[("alerted", true.into())])?;
        self.alerts.emit(Alert {
            level: Level::Warning,
            title: format!(
                "{}: unit {} has been incomplete for {}",
                self.name,
                unit.key,
                human_duration(now - unit.first_seen)
            ),
            message: format!(
                "{why}. If it is finished, run `ferry complete {name} {key}`; to stop tracking it, `ferry abandon {name} {key}`.",
                name = self.name,
                key = unit.key
            ),
            pipeline: Some(self.name.clone()),
            unit: Some(unit.key.clone()),
        });
        Ok(())
    }

    // -- upload -------------------------------------------------------------
    pub fn upload_candidates(&self) -> Result<Vec<Candidate>> {
        let now = self.now();
        self.db().upload_candidates(
            &self.name,
            now,
            self.settled_ns(now),
            self.upload_states(),
            &self.all_keys,
        )
    }

    /// Upload one file to every destination that still needs it.
    pub async fn upload(&self, candidate: Candidate) -> Result<()> {
        let file = candidate.file;
        let unit = self.db().unit(file.unit_id)?;
        if !self.upload_states().contains(&unit.state.as_str()) {
            return Ok(());
        }
        let mut wanted: Vec<&Destination> = self
            .destinations
            .iter()
            .filter(|d| candidate.dests.contains(&d.key))
            .collect();
        if self.cfg.adopt != Adopt::Off {
            // Size adoption cannot vouch for a hash-verified destination.
            for dest in wanted
                .iter()
                .filter(|d| self.cfg.adopt == Adopt::Sha256 || d.verify == Verify::Size)
            {
                if self.db().is_adopted(unit.id, &dest.key)? {
                    continue;
                }
                if let Err(e) = self.adopt(&unit, dest).await {
                    let error = format!("listing {} failed: {e:#}", dest.location.uri_for(&unit.key));
                    self.copy_failed(&unit, &file, dest, &error)?;
                }
            }
            let fresh = self.db().file(file.id)?;
            let copies = self.db().copies(file.id)?;
            wanted.retain(|d| {
                !copies.iter().any(|c| c.dest == d.key && c.is_current(&fresh))
                    && copies
                        .iter()
                        .find(|c| c.dest == d.key)
                        .is_none_or(|c| c.next_attempt.is_none_or(|t| t <= self.now()))
            });
            if wanted.is_empty() || fresh.state != crate::journal::PENDING {
                return Ok(());
            }
        }
        let path = self.src(&file);
        let now = self.now();
        let keys: Vec<String> = wanted.iter().map(|d| d.key.clone()).collect();
        if self.cfg.probe_exclusive && held_open(&path) {
            self.db()
                .defer_copies(file.id, &keys, now + self.cfg.settle.as_secs_f64())?;
            return Ok(());
        }
        let rel = Layout::remote_rel(&unit.key, &file.within);
        if self.dry_run {
            tracing::info!(
                pipeline = self.name.as_str(),
                unit = unit.key.as_str(),
                "[dry run] would upload {} ({}) to {}",
                file.rel,
                human_bytes(file.size as u64),
                keys.join(", ")
            );
            for dest in &wanted {
                self.db().record_copy(
                    file.id,
                    &dest.key,
                    file.size,
                    file.mtime_ns,
                    None,
                    dest.verify.as_str(),
                    now,
                    &self.all_keys,
                )?;
            }
            self.report(|r| {
                r.uploaded += 1;
                r.uploaded_bytes += file.size as u64;
                r.copies += wanted.len();
            });
            return Ok(());
        }
        let targets: Vec<Target> = wanted
            .iter()
            .map(|d| Target {
                dest: d.key.clone(),
                location: d.location.clone(),
                rel: rel.clone(),
                verify: d.verify,
            })
            .collect();
        let expect = Some((file.size as u64, file.mtime_ns));
        let outcomes: Vec<(Vec<Target>, Result<FanoutResult, ReadError>)> = match self.cfg.fanout {
            Fanout::Shared => vec![(
                targets.clone(),
                transfer::upload(&path, expect, &targets, &self.settings).await,
            )],
            Fanout::Separate => {
                join_all(targets.iter().map(|t| async {
                    let one = vec![t.clone()];
                    let result = transfer::upload(&path, expect, &one, &self.settings).await;
                    (one, result)
                }))
                .await
            }
        };
        for (targets, outcome) in outcomes {
            self.record_outcome(&unit, &file, &targets, outcome)?;
        }
        Ok(())
    }

    fn record_outcome(
        &self,
        unit: &UnitRow,
        file: &FileRow,
        targets: &[Target],
        outcome: Result<FanoutResult, ReadError>,
    ) -> Result<()> {
        let src = self.src(file).to_string_lossy().into_owned();
        let now = self.now();
        let fanout = match outcome {
            Ok(fanout) => fanout,
            Err(ReadError::Changed(message)) => {
                let keys: Vec<String> = targets.iter().map(|t| t.dest.clone()).collect();
                self.db()
                    .defer_copies(file.id, &keys, now + self.cfg.settle.as_secs_f64())?;
                self.record(NewTransfer {
                    pipeline: &self.name,
                    unit_id: Some(unit.id),
                    rel: &file.rel,
                    op: "upload",
                    status: "changed",
                    src: &src,
                    error: Some(&message),
                    at: now,
                    ..Default::default()
                })?;
                tracing::debug!(pipeline = self.name.as_str(), "{message}; it goes again once settled");
                return Ok(());
            }
            Err(ReadError::Io(e)) if e.kind() == std::io::ErrorKind::NotFound => {
                tracing::debug!(
                    pipeline = self.name.as_str(),
                    "{} vanished before it could be uploaded",
                    file.rel
                );
                return Ok(());
            }
            Err(ReadError::Io(e)) => {
                let error = format!("reading {}: {e}", file.rel);
                for target in targets {
                    if let Some(dest) = self.destinations.iter().find(|d| d.key == target.dest) {
                        self.copy_failed(unit, file, dest, &error)?;
                    }
                }
                return Ok(());
            }
        };
        self.db()
            .set_hash(file.id, &fanout.sha256, fanout.size as i64, fanout.mtime_ns)?;
        self.report(|r| {
            r.uploaded += 1;
            r.uploaded_bytes += fanout.size;
        });
        for result in &fanout.targets {
            let Some(dest) = self.destinations.iter().find(|d| d.key == result.dest) else {
                continue;
            };
            match &result.result {
                Ok(copied) => {
                    self.db().record_copy(
                        file.id,
                        &dest.key,
                        fanout.size as i64,
                        fanout.mtime_ns,
                        copied.etag.as_deref(),
                        copied.verified.as_str(),
                        self.now(),
                        &self.all_keys,
                    )?;
                    self.record(NewTransfer {
                        pipeline: &self.name,
                        unit_id: Some(unit.id),
                        rel: &file.rel,
                        dest: Some(&dest.key),
                        op: "upload",
                        status: "ok",
                        src: &src,
                        dst: &result.uri,
                        bytes: Some(fanout.size as i64),
                        duration_ms: Some(fanout.duration_ms as i64),
                        at: self.now(),
                        ..Default::default()
                    })?;
                    self.report(|r| r.copies += 1);
                    tracing::debug!(
                        pipeline = self.name.as_str(),
                        unit = unit.key.as_str(),
                        destination = dest.key.as_str(),
                        bytes = fanout.size,
                        duration_ms = fanout.duration_ms,
                        "uploaded {} to {}",
                        file.rel,
                        result.uri
                    );
                }
                Err(error) => self.copy_failed(unit, file, dest, error)?,
            }
        }
        Ok(())
    }

    fn copy_failed(&self, unit: &UnitRow, file: &FileRow, dest: &Destination, error: &str) -> Result<()> {
        let previous = self.db().copies(file.id)?.into_iter().find(|c| c.dest == dest.key);
        let attempts = previous.as_ref().map_or(0, |c| c.attempts) as u32 + 1;
        let now = self.now();
        self.db()
            .copy_failed(file.id, &dest.key, error, now + self.cfg.retry.backoff(attempts))?;
        let src = self.src(file).to_string_lossy().into_owned();
        let dst = dest.location.uri_for(&Layout::remote_rel(&unit.key, &file.within));
        self.record(NewTransfer {
            pipeline: &self.name,
            unit_id: Some(unit.id),
            rel: &file.rel,
            dest: Some(&dest.key),
            op: "upload",
            status: "failed",
            src: &src,
            dst: &dst,
            error: Some(error),
            at: now,
            ..Default::default()
        })?;
        self.report(|r| r.upload_failures += 1);
        tracing::error!(
            pipeline = self.name.as_str(),
            unit = unit.key.as_str(),
            destination = dest.key.as_str(),
            "upload of {} to {} failed (attempt {attempts}): {error}",
            file.rel,
            dest.key
        );
        if attempts >= self.cfg.retry.alert_after && !previous.is_some_and(|c| c.alerted) {
            self.db().copy_alerted(file.id, &dest.key)?;
            self.alerts.emit(Alert {
                level: Level::Error,
                title: format!(
                    "{}: {} failed to upload to {} {attempts} times",
                    self.name, file.rel, dest.key
                ),
                message: error.to_string(),
                pipeline: Some(self.name.clone()),
                unit: Some(unit.key.clone()),
            });
        }
        Ok(())
    }

    /// Mark copies already in the store (same key and size) as made. Run once
    /// per unit and destination, before the first upload there, so a lost
    /// journal or a migration from another uploader does not send it all again.
    async fn adopt(&self, unit: &UnitRow, dest: &Destination) -> Result<()> {
        let _guard = self.adopting.lock().await;
        if self.db().is_adopted(unit.id, &dest.key)? {
            return Ok(());
        }
        let prefix = dest.location.key(&unit.key)?;
        let store: Arc<dyn ObjectStore> = dest.location.store.inner.clone();
        let mut remote: HashMap<String, (u64, Option<String>)> = HashMap::new();
        let mut listing = store.list(Some(&prefix));
        while let Some(meta) = listing.next().await {
            match meta {
                Ok(meta) => {
                    remote.insert(meta.location.to_string(), (meta.size, meta.e_tag));
                }
                Err(object_store::Error::NotFound { .. }) => break,
                Err(e) => return Err(e.into()),
            }
        }
        let now = self.now();
        let mut adopted = 0;
        let files = self.db().files(unit.id)?;
        for file in files {
            if file.state != crate::journal::PENDING || !file.present {
                continue;
            }
            let rel = Layout::remote_rel(&unit.key, &file.within);
            let key = dest.location.key(&rel)?.to_string();
            let Some((size, etag)) = remote.get(&key) else {
                continue;
            };
            if *size != file.size as u64 {
                continue;
            }
            let current = self
                .db()
                .copies(file.id)?
                .into_iter()
                .any(|c| c.dest == dest.key && c.is_current(&file));
            if current {
                continue;
            }
            let verified = if self.cfg.adopt == Adopt::Sha256 {
                if !self.same_bytes(&file, dest, &rel).await? {
                    tracing::info!(
                        pipeline = self.name.as_str(),
                        unit = unit.key.as_str(),
                        "{} holds other bytes of the same size; uploading over it",
                        dest.location.uri_for(&rel)
                    );
                    continue;
                }
                Verify::Sha256
            } else {
                Verify::Size
            };
            self.db().record_copy(
                file.id,
                &dest.key,
                file.size,
                file.mtime_ns,
                etag.as_deref(),
                verified.as_str(),
                now,
                &self.all_keys,
            )?;
            let src = self.src(&file).to_string_lossy().into_owned();
            let dst = dest.location.uri_for(&rel);
            self.record(NewTransfer {
                pipeline: &self.name,
                unit_id: Some(unit.id),
                rel: &file.rel,
                dest: Some(&dest.key),
                op: "adopt",
                status: "ok",
                src: &src,
                dst: &dst,
                bytes: Some(file.size),
                at: now,
                ..Default::default()
            })?;
            adopted += 1;
        }
        self.db().set_adopted(unit.id, &dest.key)?;
        if adopted > 0 {
            self.report(|r| r.adopted += adopted);
            tracing::info!(
                pipeline = self.name.as_str(),
                unit = unit.key.as_str(),
                "{adopted} file(s) of {} were already in {}; not uploading them again",
                unit.key,
                dest.key
            );
        }
        Ok(())
    }

    /// Whether the object at `rel` in `dest` reads back with the local file's
    /// SHA-256 (hashing the file first if the journal has no current hash). A
    /// file that changes while it is hashed does not match.
    async fn same_bytes(&self, file: &FileRow, dest: &Destination, rel: &str) -> Result<bool> {
        let local = match file.current_sha256() {
            Some(hash) => hash.to_string(),
            None => {
                let path = self.src(file);
                let hashed = tokio::task::spawn_blocking(move || transfer::hash_file(&path)).await??;
                if !hashed.steady_at((file.size as u64, file.mtime_ns)) {
                    return Ok(false);
                }
                self.db().set_hash(file.id, &hashed.sha256, file.size, file.mtime_ns)?;
                hashed.sha256
            }
        };
        let target = Target {
            dest: dest.key.clone(),
            location: dest.location.clone(),
            rel: rel.to_string(),
            verify: Verify::Sha256,
        };
        let remote = transfer::readback_sha256(&target).await.map_err(|e| anyhow!(e))?;
        Ok(remote == local)
    }

    // -- registration -------------------------------------------------------
    fn manifest(&self, unit: &UnitRow, files: &[FileRow]) -> Result<Manifest> {
        let copies = self.db().unit_copies(unit.id)?;
        let primary = self.destinations.first().context("a pipeline without destinations")?;
        let entries = files
            .iter()
            .map(|f| {
                let rel = Layout::remote_rel(&unit.key, &f.within);
                let file_copies = self
                    .destinations
                    .iter()
                    .map(|d| {
                        let copy = copies.get(&f.id).and_then(|cs| cs.iter().find(|c| c.dest == d.key));
                        let current = copy.is_some_and(|c| c.is_current(f));
                        FileCopy {
                            destination: d.key.clone(),
                            uri: d.location.uri_for(&rel),
                            etag: copy.and_then(|c| c.etag.clone()).filter(|_| current),
                            verified: copy.and_then(|c| c.verified.clone()).filter(|_| current),
                            uploaded_at: copy.and_then(|c| c.uploaded_at).filter(|_| current).map(iso),
                        }
                    })
                    .collect();
                ManifestFile {
                    path: f.within.clone(),
                    rel_path: f.rel.clone(),
                    uri: primary.location.uri_for(&rel),
                    size: f.size,
                    mtime: iso(f.mtime_ns as f64 / 1e9),
                    sha256: f.current_sha256().map(str::to_string),
                    local_path: self.src(f).to_string_lossy().into_owned(),
                    backup_path: f.backup_path.clone(),
                    copies: file_copies,
                }
            })
            .collect();
        Ok(Manifest {
            unit: unit.key.clone(),
            generation: unit.generation,
            uri: primary.location.uri_for(&unit.key),
            root: self.cfg.root.to_string_lossy().into_owned(),
            local_path: self
                .layout
                .unit_dir(&unit.key)
                .map(|p| p.to_string_lossy().into_owned()),
            first_seen: iso(unit.first_seen),
            completed: unit.completed_at.map(iso),
            created: iso(self.now()),
            finalize: self.cfg.finalize.action.as_str().into(),
            destinations: self
                .destinations
                .iter()
                .map(|d| ManifestDestination {
                    name: d.key.clone(),
                    uri: d.location.uri_for(&unit.key),
                    required: d.required,
                    verify: d.verify.as_str().into(),
                })
                .collect(),
            files: entries,
        })
    }

    fn event(&self, kind: EventKind, manifest: Option<Manifest>, transfers: &[TransferRow]) -> Event {
        let mut event = Event::new(kind, &self.node, Some(&self.name));
        event.manifest = manifest;
        event.transfers = transfers.iter().map(transfer_event).collect();
        event
    }

    fn registrar_keys(&self) -> Vec<(String, bool)> {
        self.registrars.iter().map(|r| (r.key.clone(), r.required)).collect()
    }

    /// Queue registration for complete units whose required copies are all made.
    pub async fn register_ready(&self) -> Result<Vec<String>> {
        let mut queued = Vec::new();
        let units = self.db().units(&self.name, Some(&[COMPLETE]))?;
        for unit in units {
            let files: Vec<FileRow> = self.db().files(unit.id)?.into_iter().filter(FileRow::live).collect();
            if unit.scan_error.is_some()
                || files.is_empty()
                || !self.db().copies_current(unit.id, &self.required_keys)?
            {
                continue;
            }
            let manifest = self.manifest(&unit, &files)?;
            let transfers = self.db().unreported(unit.id, &["upload", "adopt"], Some("ok"))?;
            let payload = serde_json::to_string(&self.event(EventKind::Register, Some(manifest), &transfers))?;
            let ids: Vec<i64> = transfers.iter().map(|t| t.id).collect();
            let (to, stamp) = if self.registrars.is_empty() {
                (REGISTERED, Some("registered_at"))
            } else {
                (REGISTERING, None)
            };
            let advanced = self.db().advance(
                unit.id,
                unit.generation,
                COMPLETE,
                to,
                stamp,
                "register",
                &self.registrar_keys(),
                &payload,
                &ids,
                self.now(),
            )?;
            if !advanced {
                continue;
            }
            if self.registrars.is_empty() {
                self.report(|r| r.registered.push(unit.key.clone()));
            }
            queued.push(unit.key);
        }
        Ok(queued)
    }

    /// Hand due outbox rows to their registrars and notifiers, promote confirmed
    /// units, and queue (and send) receipts for units now delivered.
    pub async fn deliver(&self) -> Result<usize> {
        let mut delivered = self.deliver_due().await?;
        self.promote_confirmed()?;
        if self.queue_receipts()? > 0 {
            delivered += self.deliver_due().await?;
        }
        Ok(delivered)
    }

    async fn deliver_due(&self) -> Result<usize> {
        let mut delivered = 0;
        let mut failing: BTreeSet<String> = BTreeSet::new();
        let rows = self.db().due_outbox(&self.name, self.now())?;
        for row in rows {
            // One failure per registrar per round: a catalog that is down would
            // otherwise cost a timeout for every unit waiting on it.
            if failing.contains(&row.registrar) {
                continue;
            }
            let recipient = if row.event == EventKind::Receipt.as_str() {
                self.notifiers
                    .iter()
                    .find(|n| receipt_key(n.as_ref()) == row.registrar)
                    .map(|n| Recipient::Notifier(n.as_ref()))
            } else {
                self.registrars
                    .iter()
                    .find(|r| r.key == row.registrar)
                    .map(Recipient::Registrar)
            };
            let Some(recipient) = recipient else {
                tracing::warn!(
                    pipeline = self.name.as_str(),
                    "dropping a {} for {:?}, which is no longer configured",
                    row.event,
                    row.registrar
                );
                self.db().outbox_done(row.id, self.now())?;
                continue;
            };
            let event: Event = serde_json::from_str(&row.payload).context("outbox payload")?;
            let unit_key = event.unit().unwrap_or_default().to_string();
            let what = describe(&row, &unit_key);
            if self.dry_run {
                tracing::info!(
                    pipeline = self.name.as_str(),
                    unit = unit_key.as_str(),
                    "[dry run] would {what}"
                );
                self.db().outbox_done(row.id, self.now())?;
                delivered += 1;
                continue;
            }
            let result = match recipient {
                Recipient::Registrar(bound) => bound.registrar.deliver(&event).await,
                Recipient::Notifier(notifier) => notifier.notify(&event).await,
            };
            match result {
                Ok(()) => {
                    self.db().outbox_done(row.id, self.now())?;
                    delivered += 1;
                }
                Err(e) => {
                    failing.insert(row.registrar.clone());
                    self.delivery_failed(&row, &unit_key, &what, &format!("{e:#}"))?;
                }
            }
        }
        Ok(delivered)
    }

    fn promote_confirmed(&self) -> Result<()> {
        let units = self.db().units(&self.name, Some(&[REGISTERING]))?;
        for unit in units {
            if !self.db().required_done(unit.id, unit.generation, "register")? {
                continue;
            }
            let now = self.now();
            if self.db().set_unit(
                unit.id,
                Some(REGISTERING),
                Some(unit.generation),
                &[("state", REGISTERED.to_string().into()), ("registered_at", now.into())],
            )? {
                self.report(|r| r.registered.push(unit.key.clone()));
                tracing::info!(
                    pipeline = self.name.as_str(),
                    unit = unit.key.as_str(),
                    "registered {}",
                    unit.key
                );
            }
        }
        Ok(())
    }

    fn delivery_failed(&self, row: &OutboxRow, unit: &str, what: &str, error: &str) -> Result<()> {
        let attempts = row.attempts as u32 + 1;
        self.db()
            .outbox_failed(row.id, error, self.now() + self.cfg.retry.backoff(attempts))?;
        self.report(|r| r.delivery_failures += 1);
        tracing::error!(
            pipeline = self.name.as_str(),
            unit,
            "could not {what} (attempt {attempts}): {error}"
        );
        if attempts >= self.cfg.retry.alert_after && !row.alerted {
            self.db().outbox_alerted(row.id)?;
            self.alerts.emit(Alert {
                level: Level::Error,
                title: format!("{}: cannot {what} ({attempts} attempts)", self.name),
                message: error.to_string(),
                pipeline: Some(self.name.clone()),
                unit: Some(unit.to_string()),
            });
        }
        Ok(())
    }

    // -- receipts -----------------------------------------------------------
    /// Queue a receipt for each unit every destination now holds and every
    /// required registrar has confirmed: once its source files were kept,
    /// moved or deleted, or as soon as only `finalize.after` is left to wait.
    /// A unit whose finalizing is failing waits; its alerts tell that story.
    fn queue_receipts(&self) -> Result<usize> {
        let now = self.now();
        let keys: Vec<String> = self.notifiers.iter().map(|n| receipt_key(n.as_ref())).collect();
        let mut queued = 0;
        let units = self.db().unannounced(&self.name)?;
        for unit in units {
            let original = if unit.state == FINALIZED {
                self.original(&unit, true, unit.finalized_at)
            } else {
                match self.finalize_due_at(&unit)? {
                    Some(due) if due > now && unit.last_error.is_none() => self.original(&unit, false, Some(due)),
                    _ => continue,
                }
            };
            let payload = if keys.is_empty() {
                String::new()
            } else {
                let mut event = Event::new(EventKind::Receipt, &self.node, Some(&self.name));
                event.receipt = Some(self.receipt(&unit, original, now)?);
                serde_json::to_string(&event)?
            };
            if self.db().announce(unit.id, unit.generation, &keys, &payload, now)? && !keys.is_empty() {
                queued += 1;
            }
        }
        Ok(queued)
    }

    fn original(&self, unit: &UnitRow, done: bool, at: Option<f64>) -> Original {
        let action = self.cfg.finalize.action;
        let path = match (action, &self.cfg.finalize.backup_root) {
            (Action::Move, Some(root)) => Some(root.join(&unit.key).to_string_lossy().into_owned()),
            _ => None,
        };
        Original {
            action: action.as_str().into(),
            done,
            at: at.map(iso),
            path,
        }
    }

    fn receipt(&self, unit: &UnitRow, original: Original, now: f64) -> Result<Receipt> {
        let files: Vec<FileRow> = self.db().files(unit.id)?.into_iter().filter(FileRow::live).collect();
        let copies = self.db().unit_copies(unit.id)?;
        let destinations = self
            .destinations
            .iter()
            .map(|d| {
                let hashed = files.iter().all(|f| {
                    copies.get(&f.id).into_iter().flatten().any(|c| {
                        c.dest == d.key && c.is_current(f) && c.verified.as_deref() == Some(Verify::Sha256.as_str())
                    })
                });
                ReceiptCopy {
                    name: d.key.clone(),
                    uri: d.location.uri_for(&unit.key),
                    verified: if hashed { Verify::Sha256 } else { Verify::Size }.as_str().into(),
                }
            })
            .collect();
        let confirmed: BTreeSet<String> = self
            .db()
            .outbox_for_unit(unit.id)?
            .into_iter()
            .filter(|o| {
                o.event == EventKind::Register.as_str() && o.generation == unit.generation && o.done_at.is_some()
            })
            .map(|o| o.registrar)
            .collect();
        Ok(Receipt {
            unit: unit.key.clone(),
            generation: unit.generation,
            update: unit.receipt_gen.is_some(),
            files: files.len(),
            bytes: files.iter().map(|f| f.size).sum(),
            first_seen: iso(unit.first_seen),
            delivered: iso(now),
            duration_s: (now - unit.first_seen).max(0.0).round() as u64,
            destinations,
            registrars: self
                .registrars
                .iter()
                .filter(|r| confirmed.contains(&r.key))
                .map(|r| r.key.clone())
                .collect(),
            original,
        })
    }

    /// Hand failed transfers to registrars' audit trails, once each.
    pub async fn report_failures(&self) -> Result<()> {
        if self.dry_run || self.registrars.is_empty() {
            return Ok(());
        }
        let failures = self.db().unreported_failures(&self.name)?;
        if failures.is_empty() {
            return Ok(());
        }
        let event = self.event(EventKind::Failures, None, &failures);
        for bound in &self.registrars {
            if let Err(e) = bound.registrar.deliver(&event).await {
                tracing::warn!(
                    pipeline = self.name.as_str(),
                    "{} could not record {} failed transfer(s): {e:#}",
                    bound.key,
                    failures.len()
                );
            }
        }
        let ids: Vec<i64> = failures.iter().map(|t| t.id).collect();
        self.db().mark_reported(&ids)?;
        Ok(())
    }

    // -- finalize -----------------------------------------------------------
    fn finalize_due_at(&self, unit: &UnitRow) -> Result<Option<f64>> {
        if unit.scan_error.is_some() || !self.db().copies_current(unit.id, &self.all_keys)? {
            return Ok(None);
        }
        let grace = self.cfg.finalize.after.map_or(0.0, |d| d.as_secs_f64());
        let last = self.db().last_copy_at(unit.id)?.unwrap_or(0.0);
        Ok(Some((last + grace).max(unit.next_attempt.unwrap_or(0.0))))
    }

    pub fn finalize_candidates(&self) -> Result<Vec<UnitRow>> {
        let now = self.now();
        let mut out = Vec::new();
        let units = self.db().units(&self.name, Some(&[REGISTERED]))?;
        for unit in units {
            if self.finalize_due_at(&unit)?.is_some_and(|due| due <= now) {
                out.push(unit);
            }
        }
        Ok(out)
    }

    /// Keep, move or delete a registered unit's source files.
    pub async fn finalize(&self, unit_id: i64) -> Result<bool> {
        let unit = self.db().unit(unit_id)?;
        if unit.state != REGISTERED || !self.finalize_due_at(&unit)?.is_some_and(|due| due <= self.now()) {
            return Ok(false);
        }
        let action = self.cfg.finalize.action;
        let rows: Vec<FileRow> = self
            .db()
            .files(unit.id)?
            .into_iter()
            .filter(|f| f.state == UPLOADED)
            .collect();
        let results = join_all(rows.iter().map(|f| self.finalize_file(&unit, f, action))).await;
        let mut changed = Vec::new();
        let mut failure = None;
        for result in results {
            match result {
                Ok(()) => {}
                Err(FinalizeError::Changed(id, reason)) => changed.push((id, reason)),
                Err(FinalizeError::Failed(e)) => failure = failure.or(Some(e)),
            }
        }
        if let Some((_, reason)) = changed.first() {
            for (id, why) in &changed {
                self.db().reopen_for_file(*id, why)?;
            }
            self.report(|r| r.reopened.push((unit.key.clone(), reason.clone())));
            tracing::warn!(
                pipeline = self.name.as_str(),
                unit = unit.key.as_str(),
                "not finalizing {}: {reason}",
                unit.key
            );
            return Ok(false);
        }
        if let Some(error) = failure {
            self.finalize_failed(&unit, &format!("{error:#}"))?;
            return Ok(false);
        }
        if action != Action::Keep
            && !self.dry_run
            && self.cfg.finalize.prune_empty_dirs
            && let Some(dir) = self.layout.unit_dir(&unit.key)
        {
            tokio::task::spawn_blocking(move || transfer::prune_empty_dirs(&dir)).await?;
        }
        let files: Vec<FileRow> = self.db().files(unit.id)?.into_iter().filter(FileRow::live).collect();
        let manifest = self.manifest(&unit, &files)?;
        let transfers = self.db().unreported(unit.id, &["move", "delete"], Some("ok"))?;
        let payload = serde_json::to_string(&self.event(EventKind::Finalize, Some(manifest), &transfers))?;
        let ids: Vec<i64> = transfers.iter().map(|t| t.id).collect();
        let advanced = self.db().advance(
            unit.id,
            unit.generation,
            REGISTERED,
            FINALIZED,
            Some("finalized_at"),
            "finalize",
            &self.registrar_keys(),
            &payload,
            &ids,
            self.now(),
        )?;
        if !advanced {
            return Ok(false);
        }
        self.report(|r| r.finalized.push(unit.key.clone()));
        let verb = match action {
            Action::Keep => "kept in place".to_string(),
            Action::Move => format!(
                "moved to {}",
                self.cfg
                    .finalize
                    .backup_root
                    .as_deref()
                    .unwrap_or(std::path::Path::new("?"))
                    .display()
            ),
            Action::Delete => "deleted from the source".to_string(),
        };
        tracing::info!(
            pipeline = self.name.as_str(),
            unit = unit.key.as_str(),
            "finalized {}: {} file(s) {verb}",
            unit.key,
            rows.len()
        );
        Ok(true)
    }

    async fn finalize_file(&self, unit: &UnitRow, file: &FileRow, action: Action) -> Result<(), FinalizeError> {
        let _permit = self
            .moves
            .acquire()
            .await
            .map_err(|e| FinalizeError::Failed(e.into()))?;
        let src = self.src(file);
        let meta = match tokio::fs::metadata(&src).await {
            Ok(meta) => Some(meta),
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => None,
            Err(e) => return Err(FinalizeError::Failed(e.into())),
        };
        if let Some(meta) = &meta
            && (meta.len() as i64, crate::util::mtime_ns(meta)) != (file.size, file.mtime_ns)
        {
            return Err(FinalizeError::Changed(
                file.id,
                format!("{} changed after it was uploaded", file.within),
            ));
        }
        let fail = |e: anyhow::Error| FinalizeError::Failed(e);
        if action == Action::Keep {
            return self.db().finalize_file(file.id, meta.is_some(), None).map_err(fail);
        }
        if self.dry_run {
            let verb = if action == Action::Move { "move" } else { "delete" };
            tracing::info!(
                pipeline = self.name.as_str(),
                unit = unit.key.as_str(),
                "[dry run] would {verb} {}",
                src.display()
            );
            return self.db().finalize_file(file.id, meta.is_some(), None).map_err(fail);
        }
        let src_text = src.to_string_lossy().into_owned();
        let now = self.now();
        if action == Action::Delete {
            if meta.is_some() {
                tokio::fs::remove_file(&src).await.map_err(|e| fail(e.into()))?;
                self.record(NewTransfer {
                    pipeline: &self.name,
                    unit_id: Some(unit.id),
                    rel: &file.rel,
                    op: "delete",
                    status: "ok",
                    src: &src_text,
                    at: now,
                    ..Default::default()
                })
                .map_err(fail)?;
            }
            return self.db().finalize_file(file.id, false, None).map_err(fail);
        }
        let backup_root = self
            .cfg
            .finalize
            .backup_root
            .clone()
            .ok_or_else(|| fail(anyhow!("move needs a backup_root")))?;
        let dst = backup_root.join(&file.rel);
        let dst_text = dst.to_string_lossy().into_owned();
        if meta.is_none() {
            // Moved already -- a crash between the move and recording it -- or
            // removed by someone else; every destination has it either way.
            let kept = std::fs::metadata(&dst).ok().map(|m| m.len() as i64);
            let backup = (kept == Some(file.size)).then_some(dst_text.as_str());
            return self.db().finalize_file(file.id, false, backup).map_err(fail);
        }
        let started = std::time::Instant::now();
        let (from, to) = (src.clone(), dst.clone());
        let moved = tokio::task::spawn_blocking(move || transfer::move_file(&from, &to))
            .await
            .map_err(|e| fail(e.into()))?;
        match moved {
            Ok(bytes) => {
                self.record(NewTransfer {
                    pipeline: &self.name,
                    unit_id: Some(unit.id),
                    rel: &file.rel,
                    op: "move",
                    status: "ok",
                    src: &src_text,
                    dst: &dst_text,
                    bytes: Some(bytes as i64),
                    duration_ms: Some(started.elapsed().as_millis() as i64),
                    at: self.now(),
                    ..Default::default()
                })
                .map_err(fail)?;
                self.db().finalize_file(file.id, false, Some(&dst_text)).map_err(fail)
            }
            Err(e) => {
                let error = e.to_string();
                self.record(NewTransfer {
                    pipeline: &self.name,
                    unit_id: Some(unit.id),
                    rel: &file.rel,
                    op: "move",
                    status: "failed",
                    src: &src_text,
                    dst: &dst_text,
                    error: Some(&error),
                    at: self.now(),
                    ..Default::default()
                })
                .map_err(fail)?;
                Err(fail(e.into()))
            }
        }
    }

    fn finalize_failed(&self, unit: &UnitRow, error: &str) -> Result<()> {
        let attempts = unit.attempts as u32 + 1;
        let next = self.now() + self.cfg.retry.backoff(attempts);
        self.db().set_unit(
            unit.id,
            None,
            None,
            &[
                ("attempts", (attempts as i64).into()),
                ("last_error", error.to_string().into()),
                ("next_attempt", next.into()),
            ],
        )?;
        self.report(|r| r.finalize_failures += 1);
        tracing::error!(
            pipeline = self.name.as_str(),
            unit = unit.key.as_str(),
            "finalizing {} failed (attempt {attempts}): {error}",
            unit.key
        );
        if attempts >= self.cfg.retry.alert_after && !unit.alerted {
            self.db()
                .set_unit(unit.id, None, None, &[("alerted", Value::from(true))])?;
            self.alerts.emit(Alert {
                level: Level::Error,
                title: format!("{}: cannot finalize {} ({attempts} attempts)", self.name, unit.key),
                message: error.to_string(),
                pipeline: Some(self.name.clone()),
                unit: Some(unit.key.clone()),
            });
        }
        Ok(())
    }

    // -- timing and reporting -----------------------------------------------
    /// The earliest future time a timer -- settle, idle, a backoff, a finalize
    /// grace -- could make new work ready, so a stream can wake then.
    pub fn next_due(&self) -> Result<Option<f64>> {
        let now = self.now();
        let settle = self.cfg.settle.as_secs_f64();
        let idle = self.cfg.unit_idle.as_secs_f64();
        let mut times: Vec<f64> = Vec::new();
        let units = self
            .db()
            .units(&self.name, Some(&[OPEN, COMPLETE, REGISTERING, REGISTERED]))?;
        for unit in units {
            let files = self.live(unit.id)?;
            if unit.state == OPEN && !files.is_empty() {
                let newest = files.iter().map(|f| f.mtime_ns).max().unwrap_or(0) as f64 / 1e9;
                let quiet = if unit.marker || unit.forced { 0.0 } else { idle };
                times.push((newest + settle).max(unit.last_change + quiet));
            }
            let copies = self.db().unit_copies(unit.id)?;
            for file in files.iter().filter(|f| f.state == crate::journal::PENDING) {
                for copy in copies.get(&file.id).into_iter().flatten() {
                    if !copy.is_current(file) {
                        times.push((file.mtime_ns as f64 / 1e9 + settle).max(copy.next_attempt.unwrap_or(0.0)));
                    }
                }
            }
            if unit.state == REGISTERED
                && let Some(due) = self.finalize_due_at(&unit)?
            {
                times.push(due);
            }
        }
        let outbox = self.db().waiting_outbox(&self.name)?;
        for row in outbox {
            if let Some(at) = row.next_attempt {
                times.push(at);
            }
        }
        Ok(times.into_iter().filter(|t| *t > now).reduce(f64::min))
    }

    /// Why each unit with files is not finalized yet.
    pub fn waiting(&self) -> Result<BTreeMap<String, String>> {
        let now = self.now();
        let mut out = BTreeMap::new();
        let units = self
            .db()
            .units(&self.name, Some(&[OPEN, COMPLETE, REGISTERING, REGISTERED]))?;
        for unit in units {
            let why = match unit.state.as_str() {
                OPEN => {
                    let files = self.live(unit.id)?;
                    match self.why_open(&unit, &files, now) {
                        Some(why) if !files.is_empty() => format!("waiting: {why}"),
                        _ => continue,
                    }
                }
                COMPLETE | REGISTERED if unit.scan_error.is_some() => {
                    format!(
                        "waiting: cannot read all of it: {}",
                        unit.scan_error.as_deref().unwrap_or_default()
                    )
                }
                COMPLETE | REGISTERED => {
                    let gate: &[String] = if unit.state == COMPLETE {
                        &self.required_keys
                    } else {
                        &self.all_keys
                    };
                    let files: Vec<FileRow> = self.db().files(unit.id)?.into_iter().filter(FileRow::live).collect();
                    let copies = self.db().unit_copies(unit.id)?;
                    let mut missing: BTreeMap<&str, usize> = BTreeMap::new();
                    let mut error = None;
                    for f in &files {
                        for copy in copies.get(&f.id).into_iter().flatten() {
                            if gate.contains(&copy.dest) && !copy.is_current(f) {
                                *missing.entry(copy.dest.as_str()).or_default() += 1;
                                error = error.or(copy.last_error.clone());
                            }
                        }
                    }
                    if missing.is_empty() {
                        if unit.state == COMPLETE {
                            continue;
                        }
                        match (&unit.last_error, self.finalize_due_at(&unit)?) {
                            (Some(e), _) => format!("waiting: finalize pending ({e})"),
                            (None, Some(due)) if due > now => {
                                format!("waiting: finalize in {}", human_duration(due - now))
                            }
                            _ => "waiting: finalize pending".to_string(),
                        }
                    } else {
                        let parts: Vec<String> = missing.iter().map(|(d, n)| format!("{n} to {d}")).collect();
                        let mut why = format!(
                            "waiting: {} file copy(s) left ({})",
                            missing.values().sum::<usize>(),
                            parts.join(", ")
                        );
                        if let Some(e) = error {
                            why.push_str(&format!("; last error: {e}"));
                        }
                        why
                    }
                }
                _ => {
                    let names: BTreeSet<String> = self
                        .db()
                        .outbox_for_unit(unit.id)?
                        .into_iter()
                        .filter(|o| o.event == "register" && o.done_at.is_none() && !o.superseded)
                        .map(|o| o.registrar)
                        .collect();
                    format!("waiting on {}", names.into_iter().collect::<Vec<_>>().join(", "))
                }
            };
            out.insert(unit.key, why);
        }
        Ok(out)
    }

    // -- one pass -----------------------------------------------------------
    /// Scan everything and carry every unit as far as it can go now.
    pub async fn run_pass(&self, stop: &CancellationToken) -> PassReport {
        self.report(|r| *r = PassReport::new(&self.name, self.now()));
        if let Err(e) = self.pass_steps(stop).await {
            tracing::error!(pipeline = self.name.as_str(), "pass over {} failed: {e:#}", self.name);
            self.report(|r| r.errors.push(format!("{e:#}")));
        }
        let _ = self
            .db()
            .set_meta(&format!("last_pass:{}", self.name), &iso(self.now()));
        self.take_report()
    }

    async fn pass_steps(&self, stop: &CancellationToken) -> Result<()> {
        self.scan(None).await?;
        self.evaluate()?;
        let candidates = self.upload_candidates()?;
        futures::stream::iter(candidates)
            .for_each_concurrent(self.cfg.limits.files_in_flight, |candidate| async move {
                if stop.is_cancelled() {
                    return;
                }
                let rel = candidate.file.rel.clone();
                if let Err(e) = self.upload(candidate).await {
                    tracing::error!(pipeline = self.name.as_str(), "unexpected error uploading {rel}: {e:#}");
                }
            })
            .await;
        self.register_ready().await?;
        self.deliver().await?;
        for unit in self.finalize_candidates()? {
            if stop.is_cancelled() {
                break;
            }
            self.finalize(unit.id).await?;
        }
        self.deliver().await?;
        self.report_failures().await?;
        Ok(())
    }
}

/// The pipeline's destinations, rooted at their prefixes.
pub fn destinations(
    cfg: &PipelineConfig,
    stores: &BTreeMap<String, Arc<crate::stores::Store>>,
) -> Result<Vec<Destination>> {
    cfg.destinations
        .iter()
        .map(|d| {
            let store = stores
                .get(&d.store)
                .with_context(|| format!("unknown store {}", d.store))?;
            Ok(Destination {
                key: d.key().to_string(),
                location: Location::root(store.clone()).child(&cfg.prefix_for(d))?,
                required: d.required,
                verify: d.verify,
            })
        })
        .collect()
}
