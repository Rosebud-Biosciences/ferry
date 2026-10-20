//! Test doubles and fixtures shared by the integration tests.
//!
//! Files are written with real mtimes and the engine reads time from a
//! [`FakeClock`], so "an hour later" is one call rather than an hour.

#![allow(dead_code)]

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};
use std::time::{SystemTime, UNIX_EPOCH};

use async_trait::async_trait;
use ferry::config::Config;
use ferry::engine::PassReport;
use ferry::hooks::{Notifier, Registrar};
use ferry::journal::Journal;
use ferry::manifest::{Alert, Event, EventKind, Manifest, Receipt, TransferEvent};
use ferry::runner::{Agent, AgentOptions};
use ferry::util::Clock;
use filetime::FileTime;
use tokio_util::sync::CancellationToken;

pub fn wall() -> f64 {
    SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_secs_f64()
}

/// An hour ago by the wall clock: long settled and long idle.
pub fn hour_ago() -> f64 {
    wall() - 3600.0
}

/// Wall-clock seconds that only move when told to.
pub struct FakeClock(Mutex<f64>);

impl FakeClock {
    pub fn new() -> Arc<Self> {
        Arc::new(Self(Mutex::new(wall())))
    }

    pub fn advance(&self, seconds: f64) {
        *self.0.lock().unwrap() += seconds;
    }
}

impl Clock for FakeClock {
    fn now(&self) -> f64 {
        *self.0.lock().unwrap()
    }
}

type Hook = Box<dyn FnOnce() + Send>;

/// Records every event; fails `failures` deliveries first.
#[derive(Default)]
pub struct Recorder {
    pub events: Mutex<Vec<Event>>,
    pub failures: Mutex<usize>,
    /// Run inside the next register delivery, to simulate the world moving on.
    pub on_register: Mutex<Option<Hook>>,
}

#[async_trait]
impl Registrar for Recorder {
    fn name(&self) -> &str {
        "rec"
    }

    async fn deliver(&self, event: &Event) -> anyhow::Result<()> {
        {
            let mut failures = self.failures.lock().unwrap();
            if *failures > 0 {
                *failures -= 1;
                anyhow::bail!("catalog unavailable");
            }
        }
        if event.event == EventKind::Register {
            let hook = self.on_register.lock().unwrap().take();
            if let Some(hook) = hook {
                hook();
            }
        }
        self.events.lock().unwrap().push(event.clone());
        Ok(())
    }
}

impl Recorder {
    pub fn fail(&self, times: usize) {
        *self.failures.lock().unwrap() = times;
    }

    pub fn kinds(&self) -> Vec<EventKind> {
        self.events.lock().unwrap().iter().map(|e| e.event).collect()
    }

    pub fn manifests(&self, kind: EventKind) -> Vec<Manifest> {
        self.events
            .lock()
            .unwrap()
            .iter()
            .filter(|e| e.event == kind)
            .filter_map(|e| e.manifest.clone())
            .collect()
    }

    pub fn transfers(&self, kind: EventKind) -> Vec<TransferEvent> {
        self.events
            .lock()
            .unwrap()
            .iter()
            .filter(|e| e.event == kind)
            .flat_map(|e| e.transfers.clone())
            .collect()
    }
}

/// Collects what a notifier taking `kinds` would be sent. Its first `failing`
/// deliveries fail, like a chat service that is down.
pub struct Inbox {
    pub name: String,
    pub kinds: Vec<EventKind>,
    pub events: Mutex<Vec<Event>>,
    pub failing: Mutex<usize>,
}

impl Inbox {
    pub fn alerts() -> Arc<Self> {
        Self::taking("alerts", &[EventKind::Alert])
    }

    pub fn receipts() -> Arc<Self> {
        Self::taking("arrivals", &[EventKind::Receipt])
    }

    fn taking(name: &str, kinds: &[EventKind]) -> Arc<Self> {
        Arc::new(Self {
            name: name.into(),
            kinds: kinds.to_vec(),
            events: Mutex::new(Vec::new()),
            failing: Mutex::new(0),
        })
    }

    pub fn alerts_seen(&self) -> Vec<Alert> {
        self.events
            .lock()
            .unwrap()
            .iter()
            .filter_map(|e| e.alert.clone())
            .collect()
    }

    pub fn receipts_seen(&self) -> Vec<Receipt> {
        self.events
            .lock()
            .unwrap()
            .iter()
            .filter_map(|e| e.receipt.clone())
            .collect()
    }
}

#[async_trait]
impl Notifier for Inbox {
    fn name(&self) -> &str {
        &self.name
    }

    fn min_level(&self) -> ferry::config::Level {
        ferry::config::Level::Info
    }

    fn wants(&self, kind: EventKind) -> bool {
        self.kinds.contains(&kind)
    }

    async fn notify(&self, event: &Event) -> anyhow::Result<()> {
        {
            let mut failing = self.failing.lock().unwrap();
            if *failing > 0 {
                *failing -= 1;
                anyhow::bail!("chat is down");
            }
        }
        self.events.lock().unwrap().push(event.clone());
        Ok(())
    }
}

pub struct Harness {
    pub dir: tempfile::TempDir,
    pub src: PathBuf,
    pub remote: PathBuf,
    pub mirror: PathBuf,
    pub backup: PathBuf,
    pub state: PathBuf,
    pub clock: Arc<FakeClock>,
    pub recorder: Arc<Recorder>,
}

impl Harness {
    pub fn new() -> Self {
        let dir = tempfile::tempdir().unwrap();
        let root = dir.path().canonicalize().unwrap();
        let h = Self {
            src: root.join("src"),
            remote: root.join("remote"),
            mirror: root.join("mirror"),
            backup: root.join("backup"),
            state: root.join("state"),
            dir,
            clock: FakeClock::new(),
            recorder: Arc::new(Recorder::default()),
        };
        for d in [&h.src, &h.remote, &h.mirror] {
            std::fs::create_dir_all(d).unwrap();
        }
        h
    }

    /// One pipeline `inst` over `src`, with a recorder registrar `rec`; `extra`
    /// is YAML merged over the pipeline's settings. Defaults need no waiting:
    /// settle 1 s and idle 2 s, which files written an hour ago satisfy.
    pub fn config_with(&self, destinations: &str, registrars: &str, extra: &str) -> Config {
        let base = format!(
            "node: test-node\n\
             state_dir: '{state}'\n\
             stores:\n  remote: {{url: '{remote}'}}\n  mirror: {{url: '{mirror}'}}\n\
             pipelines:\n\
             \x20 - name: inst\n\
             \x20   root: '{src}'\n\
             \x20   destinations: {destinations}\n\
             \x20   settle: 1s\n\
             \x20   unit_idle: 2s\n\
             \x20   registrars: {registrars}\n",
            state = self.state.display(),
            remote = self.remote.display(),
            mirror = self.mirror.display(),
            src = self.src.display(),
        );
        let mut doc: yaml_serde::Value = yaml_serde::from_str(&base).unwrap();
        let overrides: yaml_serde::Value = yaml_serde::from_str(&dedent(extra)).unwrap();
        if let (Some(pipeline), Some(overrides)) = (doc["pipelines"][0].as_mapping_mut(), overrides.as_mapping()) {
            for (k, v) in overrides {
                pipeline.insert(k.clone(), v.clone());
            }
        }
        let text = yaml_serde::to_string(&doc).unwrap();
        Config::from_yaml(&text, self.dir.path()).unwrap_or_else(|e| panic!("{e:#}\n{text}"))
    }

    pub fn config(&self, extra: &str) -> Config {
        self.config_with("[remote]", "[{name: rec, exec: [unused]}]", extra)
    }

    pub fn backup_extra(&self, action: &str) -> String {
        format!(
            "    finalize: {{action: {action}, backup_root: '{}'}}\n",
            self.backup.display()
        )
    }

    pub fn options(&self, dry_run: bool) -> AgentOptions {
        let mut registrars: BTreeMap<String, Arc<dyn Registrar>> = BTreeMap::new();
        registrars.insert("rec".into(), self.recorder.clone());
        AgentOptions {
            dry_run,
            clock: self.clock.clone(),
            registrars,
            notifiers: Some(Vec::new()),
            ..Default::default()
        }
    }

    /// [`Harness::options`], with `inbox` in place of the configured notifiers.
    pub fn notifying(&self, inbox: &Arc<Inbox>) -> AgentOptions {
        AgentOptions {
            notifiers: Some(vec![inbox.clone()]),
            ..self.options(false)
        }
    }

    pub fn agent(&self, cfg: &Config) -> Agent {
        Agent::open(cfg.clone(), self.options(false)).unwrap()
    }

    pub async fn pass(&self, cfg: &Config) -> PassReport {
        self.pass_with(cfg, self.options(false)).await
    }

    pub async fn pass_with(&self, cfg: &Config, options: AgentOptions) -> PassReport {
        let agent = Agent::open(cfg.clone(), options).unwrap();
        let mut reports = agent.run_once(&[], &CancellationToken::new()).await.unwrap();
        reports.remove(0)
    }

    pub fn unit_state(&self, cfg: &Config, key: &str) -> Option<String> {
        let journal = Journal::open(&cfg.journal_path()).unwrap();
        journal.find_unit("inst", key).unwrap().map(|u| u.state)
    }
}

fn dedent(text: &str) -> String {
    let indent = text
        .lines()
        .filter(|l| !l.trim().is_empty())
        .map(|l| l.len() - l.trim_start().len())
        .min()
        .unwrap_or(0);
    let body: Vec<&str> = text.lines().map(|l| l.get(indent..).unwrap_or("")).collect();
    if body.iter().all(|l| l.trim().is_empty()) {
        "{}".into()
    } else {
        body.join("\n")
    }
}

pub fn set_mtime(path: &Path, mtime: f64) {
    let t = FileTime::from_unix_time(mtime.floor() as i64, ((mtime.fract()) * 1e9) as u32);
    filetime::set_file_mtime(path, t).unwrap();
}

/// Write `path` with an mtime of `mtime` (e.g. [`hour_ago`], or a fake clock's now).
pub fn write_at(path: &Path, data: &[u8], mtime: f64) -> PathBuf {
    std::fs::create_dir_all(path.parent().unwrap()).unwrap();
    std::fs::write(path, data).unwrap();
    set_mtime(path, mtime);
    path.to_path_buf()
}

/// Write `path`, long settled.
pub fn write(path: &Path, data: &[u8]) -> PathBuf {
    write_at(path, data, hour_ago())
}

/// Make the directory `path` unreadable; `false` if this process reads it
/// anyway (as root does), so the caller can skip.
#[cfg(unix)]
pub fn lock_out(path: &Path) -> bool {
    use std::os::unix::fs::PermissionsExt;
    std::fs::set_permissions(path, std::fs::Permissions::from_mode(0o000)).unwrap();
    std::fs::read_dir(path).is_err()
}

#[cfg(unix)]
pub fn let_in(path: &Path) {
    use std::os::unix::fs::PermissionsExt;
    std::fs::set_permissions(path, std::fs::Permissions::from_mode(0o755)).unwrap();
}

/// Deny everyone the listing of the directory `path`; `false` if this
/// process lists it anyway, so the caller can skip.
#[cfg(windows)]
pub fn lock_out(path: &Path) -> bool {
    icacls(path, &["/deny", "*S-1-1-0:(RD)"]);
    std::fs::read_dir(path).is_err()
}

#[cfg(windows)]
pub fn let_in(path: &Path) {
    icacls(path, &["/remove:d", "*S-1-1-0"]);
}

#[cfg(windows)]
fn icacls(path: &Path, args: &[&str]) {
    let path = path.to_string_lossy();
    let out = std::process::Command::new("icacls")
        .arg(path.strip_prefix(r"\\?\").unwrap_or(&path))
        .args(args)
        .output()
        .unwrap();
    assert!(
        out.status.success(),
        "icacls {args:?}: {}{}",
        String::from_utf8_lossy(&out.stdout),
        String::from_utf8_lossy(&out.stderr)
    );
}

/// Backdate every file and directory under (and including) `top`.
pub fn age_tree(top: &Path, mtime: f64) {
    for entry in walk(top) {
        set_mtime(&entry, mtime);
    }
    set_mtime(top, mtime);
}

fn walk(top: &Path) -> Vec<PathBuf> {
    let mut out = Vec::new();
    if let Ok(entries) = std::fs::read_dir(top) {
        for entry in entries.flatten() {
            let path = entry.path();
            if path.is_dir() {
                out.extend(walk(&path));
            }
            out.push(path);
        }
    }
    out
}

/// Every file under `top`, by forward-slash relative path.
pub fn files_under(top: &Path) -> BTreeMap<String, Vec<u8>> {
    walk(top)
        .into_iter()
        .filter(|p| p.is_file())
        .map(|p| {
            let rel = p.strip_prefix(top).unwrap().to_string_lossy().replace('\\', "/");
            (rel, std::fs::read(&p).unwrap())
        })
        .collect()
}

/// [`files_under`], minus the manifests the manifest registrar writes.
pub fn data_files(top: &Path) -> BTreeMap<String, Vec<u8>> {
    files_under(top)
        .into_iter()
        .filter(|(rel, _)| !rel.ends_with("_ferry_manifest.json"))
        .collect()
}

pub fn tree(entries: &[(&str, &[u8])]) -> BTreeMap<String, Vec<u8>> {
    entries.iter().map(|(k, v)| (k.to_string(), v.to_vec())).collect()
}

pub fn sha256(data: &[u8]) -> String {
    hex::encode(aws_lc_rs::digest::digest(&aws_lc_rs::digest::SHA256, data).as_ref())
}
