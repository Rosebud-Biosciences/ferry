//! Logging: readable lines on stderr, and JSON lines in one file per UTC day
//! (`ferry.YYYY-MM-DD.jsonl`) under the log directory.
//!
//! A day's file is complete once the day is over, so shipping is "upload every
//! earlier day not yet shipped", and the shipped files are what log registrars
//! catalog as the node's logs.

use std::path::{Path, PathBuf};

use anyhow::{Context, Result};
use aws_lc_rs::digest;
use bytes::Bytes;
use chrono::NaiveDate;
use object_store::{ObjectStoreExt, PutPayload, WriteMultipart};
use parking_lot::Mutex;
use tokio::io::AsyncReadExt;
use tracing_appender::non_blocking::{NonBlockingBuilder, WorkerGuard};
use tracing_appender::rolling::{RollingFileAppender, Rotation};
use tracing_subscriber::layer::SubscriberExt;
use tracing_subscriber::util::SubscriberInitExt;
use tracing_subscriber::{EnvFilter, Layer, fmt};

use crate::hooks::ExecHook;
use crate::journal::Journal;
use crate::manifest::{Event, EventKind, ShippedLog, log_event_id};
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

/// Logs up to this size go up in one PUT; bigger ones stream in parts of it.
const LOG_PART: usize = 8 * 1024 * 1024;

/// Upload every finished day's log not yet shipped, then tell the log
/// registrars -- each one any log it has not taken yet, so one that was down
/// hears about it on a later round.
pub async fn ship_logs(
    journal: &Mutex<Journal>,
    location: &Location,
    log_dir: &Path,
    node: &str,
    hooks: &[ExecHook],
    today: NaiveDate,
) -> Vec<ShippedLog> {
    let names: Vec<String> = hooks.iter().map(|h| h.name.clone()).collect();
    let mut shipped = Vec::new();
    for (day, path) in log_files(log_dir) {
        let name = path
            .file_name()
            .map(|n| n.to_string_lossy().into_owned())
            .unwrap_or_default();
        let done = journal.lock().shipped(&name).unwrap_or(false);
        if day >= today || done {
            continue;
        }
        match ship_one(location, node, &path, &name).await {
            Ok((uri, size, sha256)) => {
                let recorded = journal
                    .lock()
                    .record_shipped(&name, &uri, size as i64, &sha256, now_secs(), &names);
                if let Err(e) = recorded {
                    tracing::warn!("could not record shipping {name}: {e:#}");
                }
                tracing::info!("shipped log {name} to {uri}");
                shipped.push(ShippedLog {
                    node: node.to_string(),
                    name: name.clone(),
                    day: day.to_string(),
                    uri,
                    size: size as i64,
                    sha256,
                });
            }
            Err(e) => tracing::warn!("could not ship {name}: {e:#}"),
        }
    }
    let pending = journal.lock().undelivered_logs(&names).unwrap_or_default();
    for (row, hook_name) in pending {
        let Some(hook) = hooks.iter().find(|h| h.name == hook_name) else {
            continue;
        };
        let day = row
            .name
            .strip_prefix("ferry.")
            .and_then(|n| n.strip_suffix(".jsonl"))
            .unwrap_or_default()
            .to_string();
        let log = ShippedLog {
            node: node.to_string(),
            name: row.name.clone(),
            day,
            uri: row.uri,
            size: row.size,
            sha256: row.sha256,
        };
        let mut event = Event::new(EventKind::Log, node, None);
        event.id = log_event_id(&log);
        event.log = Some(log);
        let result = hook.run(&event).await;
        let recorded = {
            let j = journal.lock();
            match &result {
                Ok(()) => j.log_delivered(&row.name, &hook_name, now_secs()),
                Err(e) => j.log_delivery_failed(&row.name, &hook_name, &format!("{e:#}")),
            }
        };
        if let Err(e) = recorded {
            tracing::warn!("could not record telling {hook_name} about {}: {e:#}", row.name);
        }
        if let Err(e) = result {
            tracing::warn!(
                "{hook_name} could not register log {} (retried next round): {e:#}",
                row.name
            );
        }
    }
    shipped
}

/// Upload one log, hashing on the way; small ones in one PUT, others in parts
/// so a big log never sits in memory whole.
async fn ship_one(location: &Location, node: &str, path: &Path, name: &str) -> Result<(String, u64, String)> {
    let rel = format!("{node}/{name}");
    let key = location.key(&rel)?;
    let store = &location.store.inner;
    let mut file = tokio::fs::File::open(path).await?;
    let mut ctx = digest::Context::new(&digest::SHA256);
    let mut chunk = vec![0u8; LOG_PART];
    let first = read_full(&mut file, &mut chunk).await?;
    ctx.update(&chunk[..first]);
    let mut size = first as u64;
    if first < LOG_PART {
        chunk.truncate(first);
        store.put(&key, PutPayload::from(Bytes::from(chunk))).await?;
    } else {
        let mut writer = WriteMultipart::new_with_chunk_size(store.put_multipart(&key).await?, LOG_PART);
        writer.put(Bytes::copy_from_slice(&chunk));
        let streamed: Result<()> = async {
            loop {
                let n = read_full(&mut file, &mut chunk).await?;
                if n == 0 {
                    return Ok(());
                }
                ctx.update(&chunk[..n]);
                size += n as u64;
                writer.wait_for_capacity(2).await?;
                writer.put(Bytes::copy_from_slice(&chunk[..n]));
            }
        }
        .await;
        if let Err(e) = streamed {
            let _ = writer.abort().await;
            return Err(e);
        }
        writer.finish().await?;
    }
    Ok((location.uri_for(&rel), size, hex::encode(ctx.finish().as_ref())))
}

/// Fill `buf` from `file` unless it ends first; how many bytes were read.
async fn read_full(file: &mut tokio::fs::File, buf: &mut [u8]) -> std::io::Result<usize> {
    let mut filled = 0;
    while filled < buf.len() {
        let n = file.read(&mut buf[filled..]).await?;
        if n == 0 {
            break;
        }
        filled += n;
    }
    Ok(filled)
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

    #[tokio::test]
    async fn a_big_log_streams_up_in_parts() {
        let logs = tempfile::tempdir().unwrap();
        let remote = tempfile::tempdir().unwrap();
        let body: Vec<u8> = (0..LOG_PART + LOG_PART / 2).map(|i| (i % 251) as u8).collect();
        std::fs::write(logs.path().join("ferry.2026-10-06.jsonl"), &body).unwrap();
        let journal = Mutex::new(Journal::in_memory().unwrap());
        let location = local(remote.path()).child("logs").unwrap();
        let today = NaiveDate::from_ymd_opt(2026, 10, 8).unwrap();

        let shipped = ship_logs(&journal, &location, logs.path(), "pc1", &[], today).await;
        assert_eq!(shipped[0].size as usize, body.len());
        assert_eq!(
            shipped[0].sha256,
            hex::encode(digest::digest(&digest::SHA256, &body).as_ref())
        );
        assert_eq!(
            std::fs::read(remote.path().join("logs/pc1/ferry.2026-10-06.jsonl")).unwrap(),
            body
        );
    }

    #[tokio::test]
    async fn a_log_registrar_that_was_down_hears_about_it_next_round() {
        let logs = tempfile::tempdir().unwrap();
        let remote = tempfile::tempdir().unwrap();
        std::fs::write(logs.path().join("ferry.2026-10-06.jsonl"), "{}\n").unwrap();
        let journal = Mutex::new(Journal::in_memory().unwrap());
        let location = local(remote.path()).child("logs").unwrap();
        let today = NaiveDate::from_ymd_opt(2026, 10, 8).unwrap();
        let calls = logs.path().join("calls");
        let catalog = |code: i32| {
            let argv: Vec<String> = if cfg!(windows) {
                vec![
                    "powershell".into(),
                    "-NoProfile".into(),
                    "-NonInteractive".into(),
                    "-Command".into(),
                    format!("[IO.File]::AppendAllText('{}', \"x`n\"); exit {code}", calls.display()),
                ]
            } else {
                vec![
                    "sh".into(),
                    "-c".into(),
                    format!("echo x >> '{}'; exit {code}", calls.display()),
                ]
            };
            crate::hooks::exec_hook(
                "catalog".into(),
                &argv,
                &None,
                &Default::default(),
                std::time::Duration::from_secs(30),
            )
        };
        let count = || std::fs::read_to_string(&calls).unwrap_or_default().lines().count();

        ship_logs(&journal, &location, logs.path(), "pc1", &[catalog(1)], today).await;
        assert_eq!(count(), 1);
        ship_logs(&journal, &location, logs.path(), "pc1", &[catalog(0)], today).await;
        assert_eq!(count(), 2, "told again after failing");
        ship_logs(&journal, &location, logs.path(), "pc1", &[catalog(0)], today).await;
        assert_eq!(count(), 2, "and only until it took it");
    }
}
