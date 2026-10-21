//! The agent: everything one ferry process runs (`ferry run`, `ferry watch`).

use std::collections::BTreeMap;
use std::future::Future;
use std::sync::Arc;
use std::time::Duration;

use anyhow::{Context, Result, anyhow};
use chrono::Utc;
use parking_lot::Mutex;
use tokio::task::JoinSet;
use tokio_util::sync::CancellationToken;

use crate::config::{Config, Level, Mode};
use crate::engine::{self, BoundRegistrar, Engine, EngineParts, PassReport};
use crate::hooks::{Alerts, ExecHook, Notifier, Registrar, exec_hook, notifier_from, registrar_from};
use crate::journal::{Binding, Journal};
use crate::lock::StateLock;
use crate::logs::{ship_logs, write_heartbeat};
use crate::manifest::{Alert, EventKind};
use crate::metrics::Metrics;
use crate::schedule::{Schedule, sleep_until};
use crate::stores::{Location, Store, open_stores};
use crate::stream::{self, StreamOptions};
use crate::util::{Clock, SystemClock};
use crate::{AGENT, VERSION};

/// How long a crashed pipeline loop waits before it is restarted.
const RESTART_AFTER: Duration = Duration::from_secs(30);

pub struct AgentOptions {
    /// Show what would happen; lock nothing, run no hook, change nothing.
    pub dry_run: bool,
    pub clock: Arc<dyn Clock>,
    /// Registrars to use instead of the configured ones, by registrar name.
    pub registrars: BTreeMap<String, Arc<dyn Registrar>>,
    /// Notifiers to use instead of the configured ones.
    pub notifiers: Option<Vec<Arc<dyn Notifier>>>,
    pub stream: StreamOptions,
}

impl Default for AgentOptions {
    fn default() -> Self {
        Self {
            dry_run: false,
            clock: Arc::new(SystemClock),
            registrars: BTreeMap::new(),
            notifiers: None,
            stream: StreamOptions::default(),
        }
    }
}

pub struct Agent {
    pub config: Config,
    pub dry_run: bool,
    pub journal: Arc<Mutex<Journal>>,
    pub engines: Vec<Arc<Engine>>,
    pub stores: BTreeMap<String, Arc<Store>>,
    pub alerts: Arc<Alerts>,
    stream: StreamOptions,
    log_hooks: Vec<ExecHook>,
    _lock: Option<StateLock>,
}

impl Agent {
    pub fn open(config: Config, options: AgentOptions) -> Result<Self> {
        let dry_run = options.dry_run;
        std::fs::create_dir_all(&config.state_dir)
            .with_context(|| format!("cannot create state_dir {}", config.state_dir.display()))?;
        let lock = if dry_run {
            None
        } else {
            Some(StateLock::acquire(&config.state_dir.join("ferry.lock"))?)
        };
        let journal = if dry_run {
            Journal::snapshot(&config.journal_path())?
        } else {
            Journal::open(&config.journal_path())?
        };
        let journal = Arc::new(Mutex::new(journal));
        let notifiers: Vec<Arc<dyn Notifier>> = match options.notifiers {
            Some(given) => given,
            None => config.notifiers.iter().map(notifier_from).collect(),
        };
        // A dry run sends nothing; it still shows which receipts would go out.
        let alerts = Arc::new(Alerts::new(
            &config.node,
            if dry_run { Vec::new() } else { notifiers.clone() },
        ));
        let receipts: Vec<Arc<dyn Notifier>> = notifiers
            .iter()
            .filter(|n| n.wants(EventKind::Receipt))
            .cloned()
            .collect();
        let stores = open_stores(&config.stores)?;
        let mut engines = Vec::new();
        for p in &config.pipelines {
            let destinations = engine::destinations(p, &stores)?;
            let mut registrars = Vec::new();
            for hook in &p.registrars {
                let key = hook.key();
                let registrar = match options.registrars.get(&key) {
                    Some(given) => given.clone(),
                    None => {
                        let location = hook.manifest.as_ref().map(|m| {
                            let wanted = m.destination.as_deref();
                            destinations
                                .iter()
                                .find(|d| Some(d.key.as_str()) == wanted)
                                .unwrap_or(&destinations[0])
                                .location
                                .clone()
                        });
                        registrar_from(hook, location)?
                    }
                };
                registrars.push(BoundRegistrar {
                    key,
                    required: hook.required,
                    registrar,
                });
            }
            engines.push(Arc::new(Engine::new(EngineParts {
                node: config.node.clone(),
                cfg: p.clone(),
                destinations,
                registrars,
                notifiers: receipts.clone(),
                bindings: p
                    .destinations
                    .iter()
                    .map(|d| Binding {
                        name: d.key().to_string(),
                        identity: config.destination_identity(p, d),
                        verify: d.verify.as_str().into(),
                    })
                    .collect(),
                journal: journal.clone(),
                alerts: alerts.clone(),
                dry_run,
                clock: options.clock.clone(),
            })?));
        }
        let log_hooks = config
            .logs
            .ship
            .iter()
            .flat_map(|ship| &ship.registrars)
            .filter_map(|h| {
                h.exec
                    .as_ref()
                    .map(|argv| exec_hook(h.key(), argv, &h.cwd, &h.env, h.timeout))
            })
            .collect();
        Ok(Self {
            config,
            dry_run,
            journal,
            engines,
            stores,
            alerts,
            stream: options.stream,
            log_hooks,
            _lock: lock,
        })
    }

    pub fn engine(&self, name: &str) -> Result<Arc<Engine>> {
        self.engines.iter().find(|e| e.name == name).cloned().ok_or_else(|| {
            let known: Vec<&str> = self.engines.iter().map(|e| e.name.as_str()).collect();
            anyhow!("no pipeline named {name:?} (configured: {})", known.join(", "))
        })
    }

    // -- one pass -----------------------------------------------------------
    /// One pass over the named pipelines (all if none), whatever their mode.
    pub async fn run_once(&self, names: &[String], stop: &CancellationToken) -> Result<Vec<PassReport>> {
        let engines = if names.is_empty() {
            self.engines.clone()
        } else {
            names.iter().map(|n| self.engine(n)).collect::<Result<_>>()?
        };
        let mut reports = Vec::new();
        for engine in engines {
            if self.dry_run {
                tracing::info!(
                    pipeline = engine.name.as_str(),
                    "[dry run] {}: nothing will be uploaded, registered or moved",
                    engine.name
                );
            }
            let report = engine.run_pass(stop).await;
            // A pass's digest reaches notifiers too (`info` unless something failed).
            self.alerts.emit(report.alert());
            reports.push(report);
        }
        self.ship_logs().await;
        self.alerts.close().await;
        Ok(reports)
    }

    // -- the daemon ---------------------------------------------------------
    /// Run until `stop`: stream pipelines watch, cron pipelines run on schedule.
    pub async fn watch(self: &Arc<Self>, stop: CancellationToken) -> Result<()> {
        let mut tasks = JoinSet::new();
        for engine in &self.engines {
            let name = engine.name.clone();
            let (engine, stop2, options, alerts) = (engine.clone(), stop.clone(), self.stream, self.alerts.clone());
            match engine.cfg.mode {
                Mode::Cron => tasks.spawn(guard(name, stop.clone(), move || {
                    cron_loop(engine.clone(), alerts.clone(), stop2.clone())
                })),
                Mode::Stream => tasks.spawn(guard(name, stop.clone(), move || {
                    stream::run(engine.clone(), stop2.clone(), options)
                })),
            };
        }
        let agent = self.clone();
        let stop2 = stop.clone();
        tasks.spawn(async move { agent.housekeeping(stop2).await });
        if let Some(metrics) = &self.config.metrics {
            match tokio::net::TcpListener::bind(&metrics.listen).await {
                Ok(listener) => {
                    tracing::info!("metrics at http://{}/metrics", metrics.listen);
                    let pipelines = self
                        .engines
                        .iter()
                        .map(|e| (e.name.clone(), e.cfg.destination_keys()))
                        .collect();
                    let served = Arc::new(Metrics::new(
                        &self.config.node,
                        pipelines,
                        self.journal.clone(),
                        crate::util::now_secs(),
                    ));
                    tasks.spawn(crate::metrics::serve(listener, served, stop.clone()));
                }
                // Data keeps moving without its metrics.
                Err(e) => self.alerts.emit(Alert {
                    level: Level::Error,
                    title: format!("metrics: cannot listen on {}: {e}", metrics.listen),
                    message: String::new(),
                    pipeline: None,
                    unit: None,
                }),
            }
        }
        let modes: Vec<String> = self
            .engines
            .iter()
            .map(|e| {
                format!(
                    "{}: {}",
                    e.name,
                    if e.cfg.mode == Mode::Cron { "cron" } else { "stream" }
                )
            })
            .collect();
        tracing::info!(
            "ferry {VERSION} on {}: {} pipeline(s) ({})",
            self.config.node,
            self.engines.len(),
            modes.join(", ")
        );
        while let Some(done) = tasks.join_next().await {
            if let Err(e) = done {
                tracing::error!("a ferry task ended abnormally: {e}");
            }
        }
        self.alerts.close().await;
        tracing::info!("ferry stopped");
        Ok(())
    }

    fn ship_location(&self) -> Option<Location> {
        let ship = self.config.logs.ship.as_ref()?;
        let store = self.stores.get(&ship.store)?;
        Location::root(store.clone()).child(&ship.prefix).ok()
    }

    pub async fn ship_logs(&self) {
        if self.dry_run {
            return;
        }
        let Some(location) = self.ship_location() else { return };
        let today = Utc::now().date_naive();
        ship_logs(
            &self.journal,
            &location,
            &self.config.log_dir(),
            &self.config.node,
            &self.log_hooks,
            today,
        )
        .await;
    }

    pub fn heartbeat(&self) -> serde_json::Value {
        let journal = self.journal.lock();
        let pipelines: serde_json::Map<String, serde_json::Value> = self
            .engines
            .iter()
            .filter_map(|e| {
                let summary = journal.summary(&e.name, &e.cfg.destination_keys()).ok()?;
                Some((e.name.clone(), serde_json::to_value(summary).ok()?))
            })
            .collect();
        serde_json::json!({ "agent": AGENT, "pipelines": pipelines })
    }

    async fn housekeeping(&self, stop: CancellationToken) {
        let Some(ship) = self.config.logs.ship.clone() else {
            return;
        };
        let Some(location) = self.ship_location() else { return };
        if self.dry_run {
            return;
        }
        let start = tokio::time::Instant::now();
        let (mut next_ship, mut next_beat) = (start, start);
        while !stop.is_cancelled() {
            let now = tokio::time::Instant::now();
            if now >= next_ship {
                self.ship_logs().await;
                next_ship = now + ship.interval;
            }
            if let Some(every) = ship.heartbeat
                && now >= next_beat
            {
                if let Err(e) = write_heartbeat(&location, &self.config.node, self.heartbeat()).await {
                    tracing::warn!("could not write the heartbeat: {e:#}");
                }
                next_beat = now + every;
            }
            let wake = if ship.heartbeat.is_some() {
                next_ship.min(next_beat)
            } else {
                next_ship
            };
            tokio::select! {
                _ = stop.cancelled() => {}
                _ = tokio::time::sleep_until(wake.max(tokio::time::Instant::now() + Duration::from_secs(1))) => {}
            }
        }
    }
}

/// Run a pipeline loop, restarting it if it fails or panics, until `stop`.
async fn guard<F, Fut>(name: String, stop: CancellationToken, run: F)
where
    F: Fn() -> Fut + Send + 'static,
    Fut: Future<Output = Result<()>> + Send + 'static,
{
    while !stop.is_cancelled() {
        let outcome = tokio::spawn(run()).await;
        let why = match outcome {
            Ok(Ok(())) => return,
            Ok(Err(e)) => format!("{e:#}"),
            Err(e) => e.to_string(),
        };
        tracing::error!(
            pipeline = name.as_str(),
            "{name} crashed ({why}); restarting it in {}s",
            RESTART_AFTER.as_secs()
        );
        tokio::select! {
            _ = stop.cancelled() => return,
            _ = tokio::time::sleep(RESTART_AFTER) => {}
        }
    }
}

async fn cron_loop(engine: Arc<Engine>, alerts: Arc<Alerts>, stop: CancellationToken) -> Result<()> {
    let expr = engine
        .cfg
        .cron
        .as_deref()
        .context("mode cron needs a cron expression")?;
    let schedule = Schedule::new(expr, engine.cfg.timezone.as_deref())?;
    loop {
        let next = schedule.next_after(Utc::now())?;
        tracing::info!(
            pipeline = engine.name.as_str(),
            "{}: next pass at {}",
            engine.name,
            schedule.show(next)
        );
        if !sleep_until(next, &stop).await {
            return Ok(());
        }
        let report = engine.run_pass(&stop).await;
        alerts.emit(report.alert());
    }
}
