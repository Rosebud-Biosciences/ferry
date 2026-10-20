//! `mode: stream`: drive a pipeline's engine continuously.
//!
//! Each tick rescans what may have changed, then completes, uploads, registers
//! and finalizes whatever is ready. Uploads and finalizations run as background
//! tasks, so a large unit never holds up noticing the next one.
//!
//! What gets rescanned: every unit not yet finalized, plus new unit directories
//! (a cheap listing), every `poll_interval`; the whole tree every
//! `full_scan_interval`. OS file events (`watch: events`) are only hints that
//! make a rescan happen sooner -- they can be dropped or never arrive (network
//! shares), and the polling is what guarantees nothing is missed.

use std::collections::{BTreeSet, HashMap, HashSet};
use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use anyhow::Result;
use notify::{RecommendedWatcher, RecursiveMode, Watcher};
use tokio::sync::{Notify, Semaphore};
use tokio::task::JoinSet;
use tokio::time::Instant;
use tokio_util::sync::CancellationToken;

use crate::config::{Upload, Watch};
use crate::engine::Engine;
use crate::journal::{ACTIVE, Candidate};
use crate::units::Layout;

#[derive(Debug, Clone, Copy)]
pub struct StreamOptions {
    /// How often a digest of what happened reaches the log and notifiers.
    pub digest_every: Duration,
    /// How long in-flight work may take to finish once stopped.
    pub grace: Duration,
}

impl Default for StreamOptions {
    fn default() -> Self {
        Self {
            digest_every: Duration::from_secs(3600),
            grace: Duration::from_secs(30),
        }
    }
}

/// State shared with the file watcher and the background tasks.
#[derive(Default)]
struct Shared {
    wake: Notify,
    dirty: AtomicBool,
    discover: AtomicBool,
    hinted: Mutex<BTreeSet<String>>,
    queued: Mutex<HashSet<i64>>,
    finalizing: Mutex<HashSet<i64>>,
}

impl Shared {
    fn poke(&self) {
        self.dirty.store(true, Ordering::SeqCst);
        self.wake.notify_one();
    }

    fn take_hints(&self) -> (BTreeSet<String>, bool) {
        let hinted = std::mem::take(&mut *self.hinted.lock().unwrap_or_else(|p| p.into_inner()));
        (hinted, self.discover.swap(false, Ordering::SeqCst))
    }

    fn has_hints(&self) -> bool {
        self.discover.load(Ordering::SeqCst) || !self.hinted.lock().map(|h| h.is_empty()).unwrap_or(true)
    }
}

/// Drops an id from its in-progress set when its task ends, however it ends.
struct Claim {
    shared: Arc<Shared>,
    id: i64,
    finalize: bool,
}

impl Drop for Claim {
    fn drop(&mut self) {
        let set = if self.finalize {
            &self.shared.finalizing
        } else {
            &self.shared.queued
        };
        set.lock().unwrap_or_else(|p| p.into_inner()).remove(&self.id);
        self.shared.poke();
    }
}

pub async fn run(engine: Arc<Engine>, stop: CancellationToken, options: StreamOptions) -> Result<()> {
    let cfg = &engine.cfg;
    let shared = Arc::new(Shared::default());
    let uploads = Arc::new(Semaphore::new(cfg.limits.files_in_flight));
    let finals = Arc::new(Semaphore::new(1));
    let mut tasks: JoinSet<()> = JoinSet::new();
    let watcher = match cfg.watch {
        Watch::Events => start_watcher(&engine, shared.clone()),
        Watch::Poll => None,
    };
    let each = match cfg.upload {
        Upload::File => "file as it settles",
        Upload::Unit => "unit once complete",
    };
    tracing::info!(
        pipeline = engine.name.as_str(),
        "streaming {} from {} (upload each {each}; rescans every {})",
        engine.name,
        cfg.root.display(),
        humantime::format_duration(cfg.poll_interval)
    );
    let poll = cfg.poll_interval;
    let full_every = cfg.full_scan_interval;
    let start = Instant::now();
    let (mut next_full, mut next_scan, mut next_digest) = (start, start, start + options.digest_every);
    while !stop.is_cancelled() {
        let now = Instant::now();
        let tick = async {
            if now >= next_full {
                shared.take_hints();
                engine.scan(None).await?;
                next_full = now + full_every;
                next_scan = now + poll;
            } else if now >= next_scan || shared.has_hints() {
                let (hinted, _) = shared.take_hints();
                hot_scan(&engine, hinted).await?;
                next_scan = now + poll;
            }
            engine.evaluate()?;
            for candidate in engine.upload_candidates()? {
                spawn_upload(&mut tasks, &engine, &shared, &uploads, &stop, candidate);
            }
            engine.register_ready().await?;
            engine.deliver().await?;
            for unit in engine.finalize_candidates()? {
                spawn_finalize(&mut tasks, &engine, &shared, &finals, &stop, unit.id);
            }
            engine.report_failures().await?;
            // Rescan as soon as a settle, idle or backoff timer runs out, rather
            // than at the next poll.
            if let Some(due) = engine.next_due()? {
                let wait = (due - engine.clock.now()).max(0.0) + 0.05;
                next_scan = next_scan.min(Instant::now() + Duration::from_secs_f64(wait));
            }
            anyhow::Ok(())
        };
        if let Err(e) = tick.await {
            tracing::error!(
                pipeline = engine.name.as_str(),
                "stream tick for {} failed: {e:#}",
                engine.name
            );
        }
        while tasks.try_join_next().is_some() {}
        if now >= next_digest {
            digest(&engine);
            next_digest = now + options.digest_every;
        }
        let deadline = next_scan.min(next_full);
        tokio::select! {
            _ = stop.cancelled() => {}
            _ = shared.wake.notified() => {
                if shared.dirty.swap(false, Ordering::SeqCst) && !stop.is_cancelled() {
                    // Let a burst of finished uploads land before re-evaluating.
                    tokio::time::sleep(Duration::from_millis(200)).await;
                }
            }
            _ = tokio::time::sleep_until(deadline) => {}
        }
    }
    drop(watcher);
    // In-flight work may finish (nothing new starts once stopped); then it is
    // cut off, and an interrupted upload is simply retried by the next run.
    let drained = tokio::time::timeout(options.grace, async { while tasks.join_next().await.is_some() {} }).await;
    if drained.is_err() {
        tasks.abort_all();
        while tasks.join_next().await.is_some() {}
    }
    digest(&engine);
    Ok(())
}

fn digest(engine: &Engine) {
    let report = engine.take_report();
    if report.active() {
        engine.alerts.emit(report.alert());
    }
}

fn spawn_upload(
    tasks: &mut JoinSet<()>,
    engine: &Arc<Engine>,
    shared: &Arc<Shared>,
    permits: &Arc<Semaphore>,
    stop: &CancellationToken,
    candidate: Candidate,
) {
    let id = candidate.file.id;
    if !shared.queued.lock().unwrap_or_else(|p| p.into_inner()).insert(id) {
        return;
    }
    let claim = Claim {
        shared: shared.clone(),
        id,
        finalize: false,
    };
    let (engine, permits, stop) = (engine.clone(), permits.clone(), stop.clone());
    tasks.spawn(async move {
        let _claim = claim;
        let Ok(_permit) = permits.acquire_owned().await else {
            return;
        };
        if stop.is_cancelled() {
            return;
        }
        let rel = candidate.file.rel.clone();
        if let Err(e) = engine.upload(candidate).await {
            tracing::error!(
                pipeline = engine.name.as_str(),
                "uploading {rel} failed unexpectedly: {e:#}"
            );
        }
    });
}

fn spawn_finalize(
    tasks: &mut JoinSet<()>,
    engine: &Arc<Engine>,
    shared: &Arc<Shared>,
    permits: &Arc<Semaphore>,
    stop: &CancellationToken,
    unit_id: i64,
) {
    if !shared
        .finalizing
        .lock()
        .unwrap_or_else(|p| p.into_inner())
        .insert(unit_id)
    {
        return;
    }
    let claim = Claim {
        shared: shared.clone(),
        id: unit_id,
        finalize: true,
    };
    let (engine, permits, stop) = (engine.clone(), permits.clone(), stop.clone());
    tasks.spawn(async move {
        let _claim = claim;
        let Ok(_permit) = permits.acquire_owned().await else {
            return;
        };
        if stop.is_cancelled() {
            return;
        }
        if let Err(e) = engine.finalize(unit_id).await {
            tracing::error!(pipeline = engine.name.as_str(), "finalizing failed unexpectedly: {e:#}");
        }
    });
}

/// Rescan the units that can still change, any the watcher named, and any new
/// unit directories.
async fn hot_scan(engine: &Engine, hinted: BTreeSet<String>) -> Result<()> {
    if !engine.layout.by_dirs() {
        // A regex unit's files can be anywhere under the root.
        engine.scan(None).await?;
        return Ok(());
    }
    let units = engine.db().units(&engine.name, None)?;
    let known: HashMap<&str, bool> = units.iter().map(|u| (u.key.as_str(), u.present)).collect();
    let mut keys: BTreeSet<String> = units
        .iter()
        .filter(|u| ACTIVE.contains(&u.state.as_str()))
        .map(|u| u.key.clone())
        .collect();
    keys.extend(hinted);
    let layout = engine.layout.clone();
    for key in tokio::task::spawn_blocking(move || layout.unit_keys()).await? {
        if !known.get(key.as_str()).copied().unwrap_or(false) {
            keys.insert(key);
        }
    }
    if !keys.is_empty() {
        engine.scan(Some(keys.into_iter().collect())).await?;
    }
    Ok(())
}

fn start_watcher(engine: &Engine, shared: Arc<Shared>) -> Option<RecommendedWatcher> {
    let root = engine.cfg.root.clone();
    // FSEvents reports canonical paths (/private/var/... for /var/...).
    let roots: Vec<PathBuf> = [Some(root.clone()), root.canonicalize().ok()]
        .into_iter()
        .flatten()
        .collect();
    let layout: Arc<Layout> = engine.layout.clone();
    let hints = shared.clone();
    let watcher = notify::recommended_watcher(move |event: notify::Result<notify::Event>| {
        let Ok(event) = event else { return };
        for path in &event.paths {
            let Some(rel) = roots.iter().find_map(|r| path.strip_prefix(r).ok()) else {
                continue;
            };
            let rel = rel
                .components()
                .map(|c| c.as_os_str().to_string_lossy())
                .collect::<Vec<_>>()
                .join("/");
            match (!rel.is_empty()).then(|| layout.key_for(&rel)).flatten() {
                Some(key) => {
                    hints.hinted.lock().unwrap_or_else(|p| p.into_inner()).insert(key);
                }
                // The root itself, or something above unit level: look for new units.
                None => hints.discover.store(true, Ordering::SeqCst),
            }
        }
        hints.wake.notify_one();
    });
    let started = watcher.and_then(|mut w| w.watch(&root, RecursiveMode::Recursive).map(|()| w));
    match started {
        Ok(watcher) => Some(watcher),
        Err(e) => {
            tracing::warn!(
                pipeline = engine.name.as_str(),
                "file events are unavailable for {} ({e}); relying on polling",
                root.display()
            );
            None
        }
    }
}
