//! Hooks: where completed units, failures, logs and alerts go.
//!
//! Registrars receive `register` / `finalize` / `failures` / `log` events
//! through a durable outbox, so one that is down delays registration but never
//! the uploads. Delivery is at-least-once: a hook must treat a repeated event
//! for the same unit and generation as a no-op. An `exec` hook is any command:
//! ferry writes the event JSON to its stdin, and a zero exit code means it was
//! handled. Anything else (or the timeout) is a failure, retried with backoff,
//! with the end of stderr kept as the error.

use std::collections::BTreeMap;
use std::path::PathBuf;
use std::process::Stdio;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use anyhow::{Context, Result, anyhow, bail};
use async_trait::async_trait;
use object_store::{ObjectStoreExt, PutPayload};
use tokio::io::AsyncWriteExt;

use crate::config::{HookConfig, Level, NotifierConfig};
use crate::manifest::{Alert, Event, EventKind, ManifestDoc};
use crate::stores::Location;
use crate::util::tail;

#[async_trait]
pub trait Registrar: Send + Sync {
    fn name(&self) -> &str;
    async fn deliver(&self, event: &Event) -> Result<()>;
}

#[derive(Debug, Clone)]
pub struct ExecHook {
    pub name: String,
    pub argv: Vec<String>,
    pub cwd: Option<PathBuf>,
    pub env: BTreeMap<String, String>,
    pub timeout: Duration,
}

impl ExecHook {
    pub async fn run(&self, event: &Event) -> Result<()> {
        let program = self.argv.first().context("exec hook without a command")?;
        let payload = serde_json::to_vec(event)?;
        let mut cmd = tokio::process::Command::new(program);
        cmd.args(&self.argv[1..])
            .envs(&self.env)
            .env("FERRY_EVENT", event.event.as_str())
            .env("FERRY_NODE", &event.node)
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .kill_on_drop(true);
        if let Some(pipeline) = &event.pipeline {
            cmd.env("FERRY_PIPELINE", pipeline);
        }
        if let Some(cwd) = &self.cwd {
            cmd.current_dir(cwd);
        }
        let mut child = cmd
            .spawn()
            .with_context(|| format!("{}: cannot run {program}", self.name))?;
        let mut stdin = child.stdin.take().context("no stdin")?;
        let feed = async move {
            // A hook that does not read stdin is fine; its exit code decides.
            let _ = stdin.write_all(&payload).await;
            drop(stdin);
        };
        let finished = tokio::time::timeout(self.timeout, async {
            let ((), output) = tokio::join!(feed, child.wait_with_output());
            output
        })
        .await;
        let output = match finished {
            Err(_) => bail!(
                "{}: timed out after {}",
                self.name,
                humantime::format_duration(self.timeout)
            ),
            Ok(output) => output.with_context(|| format!("{}: waiting for {program}", self.name))?,
        };
        if output.status.success() {
            return Ok(());
        }
        let stderr = String::from_utf8_lossy(&output.stderr);
        let detail = tail(stderr.trim(), 2000);
        Err(anyhow!(
            "{}: {} for {}{}",
            self.name,
            match output.status.code() {
                Some(code) => format!("exit code {code}"),
                None => "killed by a signal".into(),
            },
            event.event.as_str(),
            if detail.is_empty() {
                String::new()
            } else {
                format!(": {detail}")
            }
        ))
    }
}

pub struct ExecRegistrar(pub ExecHook);

#[async_trait]
impl Registrar for ExecRegistrar {
    fn name(&self) -> &str {
        &self.0.name
    }

    async fn deliver(&self, event: &Event) -> Result<()> {
        self.0.run(event).await
    }
}

/// Writes each unit's manifest as JSON next to its data (or under `key`). It
/// lists every file's size, SHA-256 and copies, so it doubles as an integrity
/// record, and a bucket notification on its key is a catalog-agnostic signal
/// that a unit is complete. Rewritten on finalize, with backup paths.
pub struct ManifestRegistrar {
    pub name: String,
    pub location: Location,
    pub key: String,
}

#[async_trait]
impl Registrar for ManifestRegistrar {
    fn name(&self) -> &str {
        &self.name
    }

    async fn deliver(&self, event: &Event) -> Result<()> {
        if !matches!(event.event, EventKind::Register | EventKind::Finalize) {
            return Ok(());
        }
        let manifest = event.manifest.as_ref().context("event without a manifest")?;
        let pipeline = event.pipeline.as_deref().unwrap_or_default();
        let rel = self
            .key
            .replace("{unit}", &manifest.unit)
            .replace("{pipeline}", pipeline)
            .replace("{node}", &event.node);
        let doc = ManifestDoc {
            schema: crate::manifest::MANIFEST_SCHEMA,
            agent: &event.agent,
            node: &event.node,
            pipeline,
            manifest,
        };
        let body = serde_json::to_vec_pretty(&doc)?;
        self.location
            .store
            .inner
            .put(&self.location.key(&rel)?, PutPayload::from(body))
            .await?;
        Ok(())
    }
}

pub fn exec_hook(
    name: String,
    argv: &[String],
    cwd: &Option<PathBuf>,
    env: &BTreeMap<String, String>,
    timeout: Duration,
) -> ExecHook {
    ExecHook {
        name,
        argv: argv.to_vec(),
        cwd: cwd.clone(),
        env: env.clone(),
        timeout,
    }
}

pub fn registrar_from(hook: &HookConfig, manifest_location: Option<Location>) -> Result<Arc<dyn Registrar>> {
    let name = hook.key();
    if let Some(argv) = &hook.exec {
        return Ok(Arc::new(ExecRegistrar(exec_hook(
            name,
            argv,
            &hook.cwd,
            &hook.env,
            hook.timeout,
        ))));
    }
    let spec = hook.manifest.as_ref().context("registrar without exec or manifest")?;
    Ok(Arc::new(ManifestRegistrar {
        name,
        location: manifest_location.context("manifest registrar without a destination")?,
        key: spec.key.clone(),
    }))
}

// -- alerts -------------------------------------------------------------------

#[async_trait]
pub trait Notifier: Send + Sync {
    fn min_level(&self) -> Level;
    async fn notify(&self, event: &Event) -> Result<()>;
}

/// POSTs `{"text": ...}`, the body Slack incoming webhooks (and Mattermost,
/// Teams workflows, ...) accept.
pub struct WebhookNotifier {
    pub url: String,
    pub min_level: Level,
    client: reqwest::Client,
}

impl WebhookNotifier {
    pub fn new(url: String, min_level: Level) -> Self {
        Self {
            url,
            min_level,
            client: reqwest::Client::builder()
                .timeout(Duration::from_secs(10))
                .build()
                .unwrap_or_default(),
        }
    }
}

#[async_trait]
impl Notifier for WebhookNotifier {
    fn min_level(&self) -> Level {
        self.min_level
    }

    async fn notify(&self, event: &Event) -> Result<()> {
        let alert = event.alert.as_ref().context("alert event without an alert")?;
        self.client
            .post(&self.url)
            .json(&serde_json::json!({ "text": alert.text() }))
            .send()
            .await?
            .error_for_status()?;
        Ok(())
    }
}

pub struct ExecNotifier {
    pub hook: ExecHook,
    pub min_level: Level,
}

#[async_trait]
impl Notifier for ExecNotifier {
    fn min_level(&self) -> Level {
        self.min_level
    }

    async fn notify(&self, event: &Event) -> Result<()> {
        self.hook.run(event).await
    }
}

pub fn notifier_from(cfg: &NotifierConfig, index: usize) -> Arc<dyn Notifier> {
    match (&cfg.webhook, &cfg.exec) {
        (Some(url), _) => Arc::new(WebhookNotifier::new(url.clone(), cfg.min_level)),
        (None, Some(argv)) => Arc::new(ExecNotifier {
            hook: exec_hook(format!("notifier {index}"), argv, &cfg.cwd, &cfg.env, cfg.timeout),
            min_level: cfg.min_level,
        }),
        (None, None) => unreachable!("validated: a notifier has webhook or exec"),
    }
}

/// How many recent alerts [`Alerts::sent`] remembers.
const SENT_KEPT: usize = 1000;

/// Logs every alert and hands it to the notifiers in the background, so a slow
/// or unreachable webhook never stalls transfers.
pub struct Alerts {
    node: String,
    notifiers: Vec<Arc<dyn Notifier>>,
    sent: Mutex<Vec<Alert>>,
    pending: Mutex<Vec<tokio::task::JoinHandle<()>>>,
}

impl Alerts {
    pub fn new(node: &str, notifiers: Vec<Arc<dyn Notifier>>) -> Self {
        Self {
            node: node.to_string(),
            notifiers,
            sent: Mutex::new(Vec::new()),
            pending: Mutex::new(Vec::new()),
        }
    }

    pub fn emit(&self, alert: Alert) {
        let pipeline = alert.pipeline.as_deref().unwrap_or_default();
        let unit = alert.unit.as_deref().unwrap_or_default();
        match alert.level {
            Level::Info => tracing::info!(pipeline, unit, alert = true, "{}", alert.text()),
            Level::Warning => tracing::warn!(pipeline, unit, alert = true, "{}", alert.text()),
            Level::Error => tracing::error!(pipeline, unit, alert = true, "{}", alert.text()),
        }
        if let Ok(mut sent) = self.sent.lock() {
            if sent.len() >= SENT_KEPT {
                sent.drain(..SENT_KEPT / 2);
            }
            sent.push(alert.clone());
        }
        let mut event = Event::new(EventKind::Alert, &self.node, alert.pipeline.as_deref());
        event.alert = Some(alert.clone());
        let event = Arc::new(event);
        for notifier in &self.notifiers {
            if alert.level < notifier.min_level() {
                continue;
            }
            let (notifier, event) = (notifier.clone(), event.clone());
            let handle = tokio::spawn(async move {
                if let Err(e) = notifier.notify(&event).await {
                    tracing::warn!("notifier failed: {e:#}");
                }
            });
            if let Ok(mut pending) = self.pending.lock() {
                pending.retain(|h| !h.is_finished());
                pending.push(handle);
            }
        }
    }

    pub fn sent(&self) -> Vec<Alert> {
        self.sent.lock().map(|s| s.clone()).unwrap_or_default()
    }

    /// Wait (briefly) for alerts still being delivered.
    pub async fn close(&self) {
        let handles: Vec<_> = self
            .pending
            .lock()
            .map(|mut p| p.drain(..).collect())
            .unwrap_or_default();
        let _ = tokio::time::timeout(Duration::from_secs(15), futures::future::join_all(handles)).await;
    }
}

#[cfg(all(test, unix))]
mod tests {
    use super::*;

    fn hook(script: &str, timeout: Duration) -> ExecHook {
        let env = BTreeMap::from([("EXTRA".to_string(), "x".to_string())]);
        exec_hook(
            "catalog".into(),
            &["sh".into(), "-c".into(), script.into()],
            &None,
            &env,
            timeout,
        )
    }

    fn event() -> Event {
        Event::new(EventKind::Register, "pc1", Some("inst"))
    }

    #[tokio::test]
    async fn the_event_arrives_on_stdin_with_its_kind_in_the_environment() {
        let dir = tempfile::tempdir().unwrap();
        let out = dir.path().join("out");
        let script = format!(
            "cat > '{0}'; echo >> '{0}'; echo \"$FERRY_EVENT $FERRY_NODE $FERRY_PIPELINE $EXTRA\" >> '{0}'",
            out.display()
        );
        hook(&script, Duration::from_secs(10)).run(&event()).await.unwrap();
        let text = std::fs::read_to_string(&out).unwrap();
        let (json, env) = text.split_once('\n').unwrap();
        let received: Event = serde_json::from_str(json).unwrap();
        assert_eq!(
            (received.schema.as_str(), received.event),
            ("ferry.event/v1", EventKind::Register)
        );
        assert_eq!(env, "register pc1 inst x\n");
    }

    #[tokio::test]
    async fn a_failing_hook_reports_its_exit_code_and_the_end_of_stderr() {
        let err = hook("echo 'catalog down' >&2; exit 3", Duration::from_secs(10))
            .run(&event())
            .await
            .unwrap_err()
            .to_string();
        assert_eq!(err, "catalog: exit code 3 for register: catalog down");
    }

    #[tokio::test]
    async fn a_hung_hook_is_killed_at_its_timeout() {
        let started = std::time::Instant::now();
        let err = hook("exec sleep 30", Duration::from_millis(300))
            .run(&event())
            .await
            .unwrap_err()
            .to_string();
        assert_eq!(err, "catalog: timed out after 300ms");
        assert!(started.elapsed() < Duration::from_secs(10));
    }

    #[tokio::test]
    async fn a_hook_may_ignore_its_input() {
        hook("exit 0", Duration::from_secs(10)).run(&event()).await.unwrap();
    }
}
