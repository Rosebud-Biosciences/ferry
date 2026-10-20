//! Hooks: where completed units, failures, logs, alerts and receipts go.
//!
//! Registrars receive `register` / `finalize` / `failures` / `log` events
//! through a durable outbox, so one that is down delays registration but never
//! the uploads. Notifiers take alerts, best effort, and receipts, through the
//! same outbox. Delivery is at-least-once: a hook must treat a repeated event
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
use tokio::io::{AsyncReadExt, AsyncWriteExt};

/// How much of a hook's stderr is kept (its end); errors show the last 2000
/// characters of it.
const STDERR_KEPT: usize = 16 * 1024;

use crate::config::{HookConfig, Level, NotifierConfig, NotifierEvent, WebhookFormat};
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
            .stdout(Stdio::null())
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
        let mut stderr = child.stderr.take().context("no stderr")?;
        let collect = async move {
            // Only the end is kept, so a chatty hook cannot fill memory.
            let mut kept: Vec<u8> = Vec::new();
            let mut buf = [0u8; 8192];
            while let Ok(n) = stderr.read(&mut buf).await {
                if n == 0 {
                    break;
                }
                kept.extend_from_slice(&buf[..n]);
                if kept.len() > STDERR_KEPT {
                    kept.drain(..kept.len() - STDERR_KEPT);
                }
            }
            kept
        };
        let finished = tokio::time::timeout(self.timeout, async {
            let ((), stderr, status) = tokio::join!(feed, collect, child.wait());
            (stderr, status)
        })
        .await;
        let (stderr, status) = match finished {
            Err(_) => bail!(
                "{}: timed out after {}",
                self.name,
                humantime::format_duration(self.timeout)
            ),
            Ok((stderr, status)) => (
                stderr,
                status.with_context(|| format!("{}: waiting for {program}", self.name))?,
            ),
        };
        if status.success() {
            return Ok(());
        }
        let stderr = String::from_utf8_lossy(&stderr);
        let detail = tail(stderr.trim(), 2000);
        Err(anyhow!(
            "{}: {} for {}{}",
            self.name,
            match status.code() {
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
    /// Keys the receipts waiting for this notifier in the journal.
    fn name(&self) -> &str;
    fn min_level(&self) -> Level;
    /// Whether it takes `kind` events: alerts only, unless configured otherwise.
    fn wants(&self, kind: EventKind) -> bool {
        kind == EventKind::Alert
    }
    async fn notify(&self, event: &Event) -> Result<()>;
}

fn takes(events: &[NotifierEvent], kind: EventKind) -> bool {
    events.iter().any(|e| match e {
        NotifierEvent::Alert => kind == EventKind::Alert,
        NotifierEvent::Receipt => kind == EventKind::Receipt,
    })
}

/// The body a webhook POSTs for `event`.
pub fn webhook_body(format: WebhookFormat, event: &Event) -> Result<serde_json::Value> {
    if format == WebhookFormat::Event {
        return Ok(serde_json::to_value(event)?);
    }
    let text = if let Some(alert) = &event.alert {
        alert.text()
    } else if let Some(receipt) = &event.receipt {
        receipt.text(&event.node, event.pipeline.as_deref().unwrap_or_default())
    } else {
        bail!("a {} event has no text form", event.event.as_str());
    };
    Ok(serde_json::json!({ "text": text }))
}

/// POSTs each event to a URL, as [`webhook_body`] renders it.
pub struct WebhookNotifier {
    pub name: String,
    pub url: String,
    pub events: Vec<NotifierEvent>,
    pub min_level: Level,
    pub format: WebhookFormat,
    client: reqwest::Client,
}

impl WebhookNotifier {
    pub fn new(name: String, url: String, events: Vec<NotifierEvent>, min_level: Level, format: WebhookFormat) -> Self {
        Self {
            name,
            url,
            events,
            min_level,
            format,
            client: reqwest::Client::builder()
                .timeout(Duration::from_secs(10))
                .build()
                .unwrap_or_default(),
        }
    }
}

#[async_trait]
impl Notifier for WebhookNotifier {
    fn name(&self) -> &str {
        &self.name
    }

    fn min_level(&self) -> Level {
        self.min_level
    }

    fn wants(&self, kind: EventKind) -> bool {
        takes(&self.events, kind)
    }

    async fn notify(&self, event: &Event) -> Result<()> {
        self.client
            .post(&self.url)
            .json(&webhook_body(self.format, event)?)
            .send()
            .await?
            .error_for_status()?;
        Ok(())
    }
}

pub struct ExecNotifier {
    pub hook: ExecHook,
    pub events: Vec<NotifierEvent>,
    pub min_level: Level,
}

#[async_trait]
impl Notifier for ExecNotifier {
    fn name(&self) -> &str {
        &self.hook.name
    }

    fn min_level(&self) -> Level {
        self.min_level
    }

    fn wants(&self, kind: EventKind) -> bool {
        takes(&self.events, kind)
    }

    async fn notify(&self, event: &Event) -> Result<()> {
        self.hook.run(event).await
    }
}

pub fn notifier_from(cfg: &NotifierConfig) -> Arc<dyn Notifier> {
    match (&cfg.webhook, &cfg.exec) {
        (Some(url), _) => Arc::new(WebhookNotifier::new(
            cfg.key(),
            url.clone(),
            cfg.events.clone(),
            cfg.min_level,
            cfg.format.unwrap_or_default(),
        )),
        (None, Some(argv)) => Arc::new(ExecNotifier {
            hook: exec_hook(cfg.key(), argv, &cfg.cwd, &cfg.env, cfg.timeout),
            events: cfg.events.clone(),
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
            if !notifier.wants(EventKind::Alert) || alert.level < notifier.min_level() {
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

#[cfg(test)]
mod notifier_tests {
    use super::*;
    use crate::manifest::{Original, Receipt, ReceiptCopy};

    fn config(yaml: &str) -> NotifierConfig {
        yaml_serde::from_str(yaml).unwrap()
    }

    #[test]
    fn notifiers_take_what_their_config_lists() {
        let alerts = notifier_from(&config("webhook: https://chat.example/a"));
        assert_eq!(alerts.name(), "webhook");
        assert!(alerts.wants(EventKind::Alert) && !alerts.wants(EventKind::Receipt));
        let receipts = notifier_from(&config("{name: arrivals, exec: [post], events: [receipt]}"));
        assert_eq!(receipts.name(), "arrivals");
        assert!(receipts.wants(EventKind::Receipt) && !receipts.wants(EventKind::Alert));
    }

    #[test]
    fn webhooks_post_text_or_the_event_itself() {
        let mut alert = Event::new(EventKind::Alert, "pc1", Some("inst"));
        alert.alert = Some(Alert {
            level: Level::Error,
            title: "inst: u__1 is stuck".into(),
            message: "since noon".into(),
            pipeline: Some("inst".into()),
            unit: Some("u__1".into()),
        });
        assert_eq!(
            webhook_body(WebhookFormat::Text, &alert).unwrap(),
            serde_json::json!({"text": "inst: u__1 is stuck\nsince noon"})
        );
        let raw = webhook_body(WebhookFormat::Event, &alert).unwrap();
        assert_eq!(
            (
                raw["schema"].as_str(),
                raw["event"].as_str(),
                raw["alert"]["unit"].as_str()
            ),
            (Some("ferry.event/v1"), Some("alert"), Some("u__1"))
        );

        let mut receipt = Event::new(EventKind::Receipt, "pc1", Some("inst"));
        receipt.receipt = Some(Receipt {
            unit: "u__1".into(),
            generation: 1,
            update: false,
            files: 2,
            bytes: 2048,
            first_seen: "2026-10-09T08:00:00.000Z".into(),
            delivered: "2026-10-09T08:30:00.000Z".into(),
            duration_s: 1800,
            destinations: vec![ReceiptCopy {
                name: "s3".into(),
                uri: "s3://raw/u__1".into(),
                verified: "size".into(),
            }],
            registrars: Vec::new(),
            original: Original {
                action: "keep".into(),
                done: true,
                at: None,
                path: None,
            },
        });
        let text = webhook_body(WebhookFormat::Text, &receipt).unwrap();
        assert!(
            text["text"]
                .as_str()
                .unwrap()
                .ends_with("original kept\npc1 · inst · delivered 30m after its first file"),
            "{text}"
        );

        let register = Event::new(EventKind::Register, "pc1", Some("inst"));
        assert!(webhook_body(WebhookFormat::Text, &register).is_err());
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A hook running `script` with `sh`, or with PowerShell on Windows.
    fn hook(unix: &str, windows: &str, timeout: Duration) -> ExecHook {
        let env = BTreeMap::from([("EXTRA".to_string(), "x".to_string())]);
        let argv = if cfg!(windows) {
            vec!["powershell", "-NoProfile", "-NonInteractive", "-Command", windows]
        } else {
            vec!["sh", "-c", unix]
        };
        let argv: Vec<String> = argv.into_iter().map(String::from).collect();
        exec_hook("catalog".into(), &argv, &None, &env, timeout)
    }

    fn event() -> Event {
        Event::new(EventKind::Register, "pc1", Some("inst"))
    }

    #[tokio::test]
    async fn the_event_arrives_on_stdin_with_its_kind_in_the_environment() {
        let dir = tempfile::tempdir().unwrap();
        let out = dir.path().join("out");
        let unix = format!(
            "cat > '{0}'; echo >> '{0}'; echo \"$FERRY_EVENT $FERRY_NODE $FERRY_PIPELINE $EXTRA\" >> '{0}'",
            out.display()
        );
        let windows = format!(
            "[IO.File]::WriteAllText('{}', [Console]::In.ReadToEnd() + \"`n$env:FERRY_EVENT $env:FERRY_NODE $env:FERRY_PIPELINE $env:EXTRA`n\")",
            out.display()
        );
        hook(&unix, &windows, Duration::from_secs(30))
            .run(&event())
            .await
            .unwrap();
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
        let err = hook(
            "echo 'catalog down' >&2; exit 3",
            "[Console]::Error.WriteLine('catalog down'); exit 3",
            Duration::from_secs(30),
        )
        .run(&event())
        .await
        .unwrap_err()
        .to_string();
        assert_eq!(err, "catalog: exit code 3 for register: catalog down");
    }

    #[tokio::test]
    async fn a_hung_hook_is_killed_at_its_timeout() {
        let started = std::time::Instant::now();
        let err = hook("exec sleep 30", "Start-Sleep -Seconds 30", Duration::from_millis(300))
            .run(&event())
            .await
            .unwrap_err()
            .to_string();
        assert_eq!(err, "catalog: timed out after 300ms");
        assert!(started.elapsed() < Duration::from_secs(10));
    }

    #[tokio::test]
    async fn a_hook_may_ignore_its_input() {
        hook("exit 0", "exit 0", Duration::from_secs(30))
            .run(&event())
            .await
            .unwrap();
    }

    #[tokio::test]
    async fn a_chatty_hook_costs_only_the_end_of_its_stderr() {
        hook(
            "head -c 20000000 /dev/zero; exit 0",
            "[Console]::Out.Write('x' * 20000000); exit 0",
            Duration::from_secs(60),
        )
        .run(&event())
        .await
        .unwrap();
        let err = hook(
            "i=0; while [ $i -lt 20000 ]; do echo \"noise $i\" >&2; i=$((i+1)); done; echo 'the real reason' >&2; exit 1",
            "for ($i = 0; $i -lt 20000; $i++) { [Console]::Error.WriteLine(\"noise $i\") }; [Console]::Error.WriteLine('the real reason'); exit 1",
            Duration::from_secs(60),
        )
        .run(&event())
        .await
        .unwrap_err()
        .to_string();
        assert!(err.ends_with("the real reason"), "{err}");
        assert!(err.len() < 2200, "{}", err.len());
    }
}
