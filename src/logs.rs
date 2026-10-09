//! Logging: readable lines on stderr, and JSON lines in one file per UTC day
//! (`ferry.YYYY-MM-DD.jsonl`) under the log directory.
//!
//! A day's file is complete once the day is over, so shipping is "upload every
//! earlier day not yet shipped", and the shipped files are what log registrars
//! catalog as the node's logs.

use std::path::{Path, PathBuf};
use std::sync::Mutex;

use anyhow::{Context, Result};
use aws_lc_rs::digest;
use bytes::Bytes;
use chrono::NaiveDate;
use object_store::{ObjectStoreExt, PutPayload};
use tracing_appender::non_blocking::{NonBlockingBuilder, WorkerGuard};
use tracing_appender::rolling::{RollingFileAppender, Rotation};
use tracing_subscriber::layer::SubscriberExt;
use tracing_subscriber::util::SubscriberInitExt;
use tracing_subscriber::{EnvFilter, Layer, fmt};

use crate::hooks::ExecHook;
use crate::journal::Journal;
use crate::manifest::{Event, EventKind, ShippedLog};
use crate::stores::Location;
use crate::util::{iso, now_secs};

const PREFIX: &str = "ferry";
const SUFFIX: &str = "jsonl";

/// Keeps the file writer flushing; hold it until the process exits.
pub struct LogGuard {
    _file: Option<WorkerGuard>,
}

pub struct LogSetup<'a> {
    pub dir: Option<&'a Path>,
    /// A `tracing` level for ferry's own events: debug, info, warn, error.
    pub level: &'a str,
    pub keep_days: usize,
    pub console: bool,
}

/// Point `tracing` at the console and the daily files. `FERRY_LOG` (an
/// `EnvFilter` directive) overrides the level.
pub fn init(setup: LogSetup) -> Result<LogGuard> {
    let level = setup.level.to_string();
    let filter =
        move || EnvFilter::try_from_env("FERRY_LOG").unwrap_or_else(|_| EnvFilter::new(format!("warn,ferry={level}")));
    let console = setup.console.then(|| {
        fmt::layer()
            .with_writer(std::io::stderr)
            .with_target(false)
            .with_filter(filter())
    });
    let (file, guard) = match setup.dir {
        Some(dir) => {
            std::fs::create_dir_all(dir).with_context(|| format!("cannot create log directory {}", dir.display()))?;
            let appender = RollingFileAppender::builder()
                .rotation(Rotation::DAILY)
                .filename_prefix(PREFIX)
                .filename_suffix(SUFFIX)
                .max_log_files(setup.keep_days.max(1))
                .build(dir)
                .with_context(|| format!("cannot write logs in {}", dir.display()))?;
            let (writer, guard) = NonBlockingBuilder::default().lossy(false).finish(appender);
            let layer = fmt::layer()
                .json()
                .flatten_event(true)
                .with_current_span(false)
                .with_span_list(false)
                .with_writer(writer)
                .with_filter(filter());
            (Some(layer), Some(guard))
        }
        None => (None, None),
    };
    tracing_subscriber::registry()
        .with(console)
        .with(file)
        .try_init()
        .context("logging is already set up")?;
    Ok(LogGuard { _file: guard })
}

/// The daily files in `dir`, oldest first.
pub fn log_files(dir: &Path) -> Vec<(NaiveDate, PathBuf)> {
    let Ok(entries) = std::fs::read_dir(dir) else {
        return Vec::new();
    };
    let mut found: Vec<(NaiveDate, PathBuf)> = entries
        .filter_map(|e| e.ok())
        .filter_map(|e| {
            let name = e.file_name().into_string().ok()?;
            let day = name.strip_prefix("ferry.")?.strip_suffix(".jsonl")?;
            Some((NaiveDate::parse_from_str(day, "%Y-%m-%d").ok()?, e.path()))
        })
        .collect();
    found.sort();
    found
}

/// Upload every finished day's log not yet shipped; tell the log registrars.
pub async fn ship_logs(
    journal: &Mutex<Journal>,
    location: &Location,
    log_dir: &Path,
    node: &str,
    hooks: &[ExecHook],
    today: NaiveDate,
) -> Vec<ShippedLog> {
    let mut shipped = Vec::new();
    for (day, path) in log_files(log_dir) {
        let name = path
            .file_name()
            .map(|n| n.to_string_lossy().into_owned())
            .unwrap_or_default();
        let done = journal
            .lock()
            .map(|j| j.shipped(&name).unwrap_or(false))
            .unwrap_or(true);
        if day >= today || done {
            continue;
        }
        match ship_one(location, node, &path, &name).await {
            Ok((uri, size, sha256)) => {
                if let Ok(j) = journal.lock()
                    && let Err(e) = j.record_shipped(&name, &uri, size as i64, &sha256, now_secs())
                {
                    tracing::warn!("could not record shipping {name}: {e:#}");
                }
                tracing::info!("shipped log {name} to {uri}");
                let log = ShippedLog {
                    node: node.to_string(),
                    name: name.clone(),
                    day: day.to_string(),
                    uri,
                    size: size as i64,
                    sha256,
                };
                let mut event = Event::new(EventKind::Log, node, None);
                event.log = Some(log.clone());
                for hook in hooks {
                    if let Err(e) = hook.run(&event).await {
                        tracing::warn!("{} could not register log {name}: {e:#}", hook.name);
                    }
                }
                shipped.push(log);
            }
            Err(e) => tracing::warn!("could not ship {name}: {e:#}"),
        }
    }
    shipped
}

async fn ship_one(location: &Location, node: &str, path: &Path, name: &str) -> Result<(String, u64, String)> {
    let body = Bytes::from(tokio::fs::read(path).await?);
    let sha256 = hex::encode(digest::digest(&digest::SHA256, &body).as_ref());
    let size = body.len() as u64;
    let rel = format!("{node}/{name}");
    location
        .store
        .inner
        .put(&location.key(&rel)?, PutPayload::from(body))
        .await?;
    Ok((location.uri_for(&rel), size, sha256))
}

/// Overwrite `{node}/heartbeat.json`: proof of life, plus a status summary.
pub async fn write_heartbeat(location: &Location, node: &str, mut payload: serde_json::Value) -> Result<()> {
    if let Some(map) = payload.as_object_mut() {
        map.insert("node".into(), node.into());
        map.insert("time".into(), iso(now_secs()).into());
    }
    let body = serde_json::to_vec_pretty(&payload)?;
    location
        .store
        .inner
        .put(
            &location.key(&format!("{node}/heartbeat.json"))?,
            PutPayload::from(body),
        )
        .await?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::stores::Store;
    use object_store::local::LocalFileSystem;
    use std::sync::Arc;

    fn local(dir: &Path) -> Location {
        Location::root(Arc::new(Store {
            name: "logs".into(),
            inner: Arc::new(LocalFileSystem::new_with_prefix(dir).unwrap()),
            base: String::new(),
            uri: url::Url::from_directory_path(dir)
                .unwrap()
                .to_string()
                .trim_end_matches('/')
                .to_string(),
            limiter: None,
        }))
    }

    #[tokio::test]
    async fn finished_days_ship_once_and_today_stays_home() {
        let logs = tempfile::tempdir().unwrap();
        let remote = tempfile::tempdir().unwrap();
        for day in ["2026-10-06", "2026-10-07", "2026-10-08"] {
            std::fs::write(
                logs.path().join(format!("ferry.{day}.jsonl")),
                format!("{{\"day\":\"{day}\"}}\n"),
            )
            .unwrap();
        }
        std::fs::write(logs.path().join("other.txt"), "x").unwrap();
        let journal = Mutex::new(Journal::in_memory().unwrap());
        let location = local(remote.path()).child("logs").unwrap();
        let today = NaiveDate::from_ymd_opt(2026, 10, 8).unwrap();

        let shipped = ship_logs(&journal, &location, logs.path(), "pc1", &[], today).await;
        assert_eq!(
            shipped.iter().map(|s| s.day.as_str()).collect::<Vec<_>>(),
            ["2026-10-06", "2026-10-07"]
        );
        assert!(remote.path().join("logs/pc1/ferry.2026-10-07.jsonl").exists());
        assert!(!remote.path().join("logs/pc1/ferry.2026-10-08.jsonl").exists());

        assert!(
            ship_logs(&journal, &location, logs.path(), "pc1", &[], today)
                .await
                .is_empty()
        );
    }
}
