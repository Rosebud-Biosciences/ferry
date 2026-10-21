//! `ferry doctor`: check that a node can run, and say what stops it.
//!
//! It writes nothing but a probe file in the state and log directories,
//! removed at once, unless asked: `--write` puts a probe object in each store
//! (a few bytes at `_ferry/doctor/<node>`, overwritten each time; ferry never
//! deletes from a store), and `--notify` sends each notifier a test message.

use std::collections::BTreeSet;
use std::path::Path;
use std::process::ExitCode;
use std::time::Duration;

use anyhow::Result;
use object_store::{ObjectStoreExt, PutPayload};

use crate::config::{Action, Config, Level};
use crate::engine;
use crate::hooks::notifier_from;
use crate::journal::Journal;
use crate::manifest::{Alert, Event, EventKind};
use crate::stores::{Location, open_stores};

const PROBE_TIMEOUT: Duration = Duration::from_secs(30);

#[derive(Default)]
pub struct DoctorOptions {
    pub write: bool,
    pub notify: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Status {
    Ok,
    Warn,
    Fail,
}

#[derive(Default)]
struct Findings(Vec<(Status, String)>);

impl Findings {
    fn ok(&mut self, what: impl Into<String>) {
        self.0.push((Status::Ok, what.into()));
    }

    fn warn(&mut self, what: impl Into<String>) {
        self.0.push((Status::Warn, what.into()));
    }

    fn fail(&mut self, what: impl Into<String>) {
        self.0.push((Status::Fail, what.into()));
    }
}

pub fn run(config_path: &Path, env_file: Option<&Path>, options: DoctorOptions) -> Result<ExitCode> {
    let cfg = Config::load(config_path)?;
    let mut found = Findings::default();
    found.ok(format!(
        "config {}: node {}, {} pipeline(s)",
        config_path.display(),
        cfg.node,
        cfg.pipelines.len()
    ));
    if let Some(path) = env_file {
        match crate::cli::exposure(path) {
            Some(warning) => found.warn(warning),
            None => found.ok(format!("env file {}", path.display())),
        }
    }
    writable(&mut found, "state dir", &cfg.state_dir);
    let log_dir = cfg.log_dir();
    if !log_dir.starts_with(&cfg.state_dir) {
        writable(&mut found, "log dir", &log_dir);
    }
    journal(&mut found, &cfg);
    for p in &cfg.pipelines {
        match std::fs::read_dir(&p.root) {
            Ok(entries) => found.ok(format!(
                "{}: root {} ({} entries)",
                p.name,
                p.root.display(),
                entries.count()
            )),
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
                found.warn(format!("{}: root {} does not exist (yet)", p.name, p.root.display()))
            }
            Err(e) => found.fail(format!("{}: cannot list root {}: {e}", p.name, p.root.display())),
        }
        if p.finalize.action == Action::Move
            && let Some(backup) = &p.finalize.backup_root
        {
            if backup.exists() {
                writable(&mut found, &format!("{}: backup root", p.name), backup);
            } else {
                found.warn(format!(
                    "{}: backup root {} does not exist yet; the first move creates it",
                    p.name,
                    backup.display()
                ));
            }
        }
        for hook in &p.registrars {
            program(
                &mut found,
                &format!("{}: registrar {}", p.name, hook.key()),
                hook.exec.as_deref(),
                hook.cwd.as_deref(),
            );
        }
    }
    for hook in cfg.logs.ship.iter().flat_map(|ship| &ship.registrars) {
        program(
            &mut found,
            &format!("log registrar {}", hook.key()),
            hook.exec.as_deref(),
            hook.cwd.as_deref(),
        );
    }
    for notifier in &cfg.notifiers {
        let what = format!("notifier {}", notifier.key());
        match &notifier.webhook {
            Some(url) => match url::Url::parse(url) {
                // The URL is often the secret itself: show only where it goes.
                Ok(parsed) => found.ok(format!("{what}: posts to {}", parsed.host_str().unwrap_or("?"))),
                Err(e) => found.fail(format!("{what}: the webhook is not a URL: {e}")),
            },
            None => program(&mut found, &what, notifier.exec.as_deref(), notifier.cwd.as_deref()),
        }
    }
    tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()?
        .block_on(remote(&mut found, &cfg, &options));

    let mut problems = 0;
    for (status, what) in &found.0 {
        let tag = match status {
            Status::Ok => "ok  ",
            Status::Warn => "warn",
            Status::Fail => {
                problems += 1;
                "FAIL"
            }
        };
        println!("{tag}  {what}");
    }
    Ok(if problems == 0 {
        println!("no problems found");
        ExitCode::SUCCESS
    } else {
        println!("{problems} problem(s) found");
        ExitCode::from(1)
    })
}

/// `dir` exists (or can be made) and takes a file.
fn writable(found: &mut Findings, what: &str, dir: &Path) {
    let probe = dir.join(".ferry-doctor");
    let result = std::fs::create_dir_all(dir)
        .and_then(|()| std::fs::write(&probe, b"ferry doctor"))
        .and_then(|()| std::fs::remove_file(&probe));
    match result {
        Ok(()) => found.ok(format!("{what} {} is writable", dir.display())),
        Err(e) => found.fail(format!("{what} {} is not writable: {e}", dir.display())),
    }
}

fn program(found: &mut Findings, what: &str, argv: Option<&[String]>, cwd: Option<&Path>) {
    let Some(program) = argv.and_then(|argv| argv.first()) else {
        found.ok(what.to_string());
        return;
    };
    match crate::cli::find_program(program, cwd) {
        Some(path) => found.ok(format!("{what}: runs {}", path.display())),
        None => found.fail(format!("{what}: cannot find {program:?}")),
    }
}

/// What the journal says would stop ferry from starting, read without
/// changing it (beyond an upgrade to this ferry's schema, as any command does).
fn journal(found: &mut Findings, cfg: &Config) {
    let path = cfg.journal_path();
    if !path.exists() {
        found.ok(format!("no journal yet at {} (ferry has not run)", path.display()));
        return;
    }
    let journal = match Journal::open(&path) {
        Ok(journal) => journal,
        Err(e) => {
            found.fail(format!("journal {}: {e:#}", path.display()));
            return;
        }
    };
    let schema = journal.get_meta("schema_version").ok().flatten().unwrap_or_default();
    found.ok(format!("journal {} (schema {schema})", path.display()));
    for p in &cfg.pipelines {
        for d in &p.destinations {
            let now = cfg.destination_identity(p, d);
            match journal.destination(&p.name, d.key()) {
                Ok(Some(was)) if was != now => found.fail(format!(
                    "{}/{} now points at {now}, but its copies were made at {was}: ferry will not run until \
                     `ferry destination accept {0} {1}` or `ferry destination reset {0} {1}`",
                    p.name,
                    d.key()
                )),
                Ok(_) => {}
                Err(e) => found.fail(format!("{}/{}: {e:#}", p.name, d.key())),
            }
        }
        let configured: Vec<String> = p.registrars.iter().map(|r| r.key()).collect();
        for (registrar, count) in journal.orphaned_outbox(&p.name, &configured).unwrap_or_default() {
            found.fail(format!(
                "{}: {count} undelivered event(s) wait for registrar {registrar}, which is no longer configured: \
                 ferry will not run until it is, or `ferry outbox discard {} {registrar}`",
                p.name, p.name
            ));
        }
        let failing = (
            journal.failing_copies(&p.name).map(|f| f.len()).unwrap_or(0),
            journal.failing_deliveries(&p.name).map(|f| f.len()).unwrap_or(0),
        );
        if failing != (0, 0) {
            found.warn(format!(
                "{}: {} failing copies, {} failing deliveries (`ferry status` shows them)",
                p.name, failing.0, failing.1
            ));
        }
    }
}

async fn remote(found: &mut Findings, cfg: &Config, options: &DoctorOptions) {
    let stores = match open_stores(&cfg.stores) {
        Ok(stores) => stores,
        Err(e) => {
            found.fail(format!("stores: {e:#}"));
            return;
        }
    };
    let mut places: Vec<(String, Location)> = Vec::new();
    for p in &cfg.pipelines {
        match engine::destinations(p, &stores) {
            Ok(destinations) => {
                places.extend(
                    destinations
                        .into_iter()
                        .map(|d| (format!("{}/{}", p.name, d.key), d.location)),
                );
            }
            Err(e) => found.fail(format!("{}: {e:#}", p.name)),
        }
    }
    if let Some(ship) = &cfg.logs.ship
        && let Some(store) = stores.get(&ship.store)
        && let Ok(location) = Location::root(store.clone()).child(&ship.prefix)
    {
        places.push(("logs.ship".into(), location));
    }
    let mut seen = BTreeSet::new();
    for (what, location) in places {
        if !seen.insert(location.uri.clone()) {
            continue;
        }
        let checked = tokio::time::timeout(PROBE_TIMEOUT, probe(&location, &cfg.node, options.write)).await;
        match checked {
            Ok(Ok(done)) => found.ok(format!("{what} {}: {done}", location.uri)),
            // A store's errors already say what caused them.
            Ok(Err(e)) => found.fail(format!("{what} {}: {e}", location.uri)),
            Err(_) => found.fail(format!(
                "{what} {}: no answer in {}s",
                location.uri,
                PROBE_TIMEOUT.as_secs()
            )),
        }
    }
    if !options.notify {
        return;
    }
    for notifier in &cfg.notifiers {
        let mut event = Event::new(EventKind::Alert, &cfg.node, None);
        event.alert = Some(Alert {
            level: Level::Info,
            title: format!("ferry doctor: a test message from {}", cfg.node),
            message: "If you can read this, ferry can reach this notifier.".into(),
            pipeline: None,
            unit: None,
        });
        let what = format!("notifier {}", notifier.key());
        match tokio::time::timeout(PROBE_TIMEOUT, notifier_from(notifier).notify(&event)).await {
            Ok(Ok(())) => found.ok(format!("{what}: took a test message")),
            Ok(Err(e)) => found.fail(format!("{what}: {e:#}")),
            Err(_) => found.fail(format!("{what}: no answer in {}s", PROBE_TIMEOUT.as_secs())),
        }
    }
}

/// Reach the store at `location`: look for the probe object, or with `write`
/// put it, check its size and read it back.
async fn probe(location: &Location, node: &str, write: bool) -> Result<&'static str> {
    let key = location.key(&format!("_ferry/doctor/{node}"))?;
    let store = &location.store.inner;
    if !write {
        // A GET, not a HEAD: only the body of a 404 says whether the object
        // or the whole bucket is missing.
        return match store.get(&key).await {
            Ok(_) => Ok("reachable"),
            Err(e) if e.to_string().contains("NoSuchBucket") => anyhow::bail!("the bucket does not exist"),
            Err(object_store::Error::NotFound { .. }) => Ok("reachable"),
            Err(e) => Err(e.into()),
        };
    }
    let body = format!(
        "ferry doctor on {node} at {}\n",
        crate::util::iso(crate::util::now_secs())
    );
    store.put(&key, PutPayload::from(body.clone().into_bytes())).await?;
    let meta = store.head(&key).await?;
    if meta.size != body.len() as u64 {
        anyhow::bail!("wrote {} bytes, but it holds {}", body.len(), meta.size);
    }
    let back = store.get(&key).await?.bytes().await?;
    if back.as_ref() != body.as_bytes() {
        anyhow::bail!("what it reads back is not what was written");
    }
    Ok("writes, and reads back what it wrote")
}
