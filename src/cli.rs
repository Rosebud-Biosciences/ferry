//! The `ferry` command line.

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use std::process::ExitCode;
use std::sync::Arc;

use anyhow::{Context, Result, bail};
use chrono::Utc;
use clap::{Parser, Subcommand};
use serde_json::{Value, json};
use tokio_util::sync::CancellationToken;

use crate::VERSION;
use crate::config::{Config, Mode};
use crate::journal::{ABANDONED, Binding, FINALIZED, GONE, Journal, OPEN};
use crate::logs::{self, LogGuard, LogSetup};
use crate::runner::{Agent, AgentOptions};
use crate::schedule::Schedule;
use crate::service::{self, Platform, ServiceSpec};
use crate::util::human_bytes;

#[derive(Parser)]
#[command(
    name = "ferry",
    version = VERSION,
    about = "Ship instrument data from watched folders to S3-compatible storage, on a cron schedule or as it lands.",
    arg_required_else_help = true
)]
struct Cli {
    /// Config file.
    #[arg(short, long, env = "FERRY_CONFIG", default_value = "ferry.yaml", global = true)]
    config: PathBuf,
    /// KEY=VALUE lines to set in the environment before anything else, such
    /// as a service's credentials; they replace variables already set.
    #[arg(long, env = "FERRY_ENV_FILE", global = true)]
    env_file: Option<PathBuf>,
    #[command(subcommand)]
    command: Command,
}

#[derive(Subcommand)]
enum Command {
    /// One pass over every pipeline, whatever its mode, then exit.
    ///
    /// Exits 1 if anything failed; failures are retried on the next pass.
    Run {
        /// Only this pipeline (repeatable).
        #[arg(short, long = "pipeline")]
        pipelines: Vec<String>,
        /// Show what a pass would do, changing nothing.
        #[arg(long)]
        dry_run: bool,
        /// Log debug detail.
        #[arg(short, long)]
        verbose: bool,
    },
    /// Run until stopped: stream pipelines watch, cron pipelines run on schedule.
    Watch {
        #[arg(short, long)]
        verbose: bool,
    },
    /// What the journal knows: units by state, backlog and failures.
    Status {
        #[arg(short, long = "pipeline")]
        pipelines: Vec<String>,
        /// List every unfinished unit.
        #[arg(long)]
        units: bool,
        /// Print JSON.
        #[arg(long)]
        json: bool,
    },
    /// Retry failures now, and re-open abandoned units, in the given scope.
    Retry {
        #[arg(short, long)]
        pipeline: Option<String>,
        #[arg(short, long, requires = "pipeline")]
        unit: Option<String>,
    },
    /// Treat an open unit as complete without waiting for it to go quiet.
    Complete { pipeline: String, unit: String },
    /// Stop tracking a unit (`ferry retry -p PIPELINE -u UNIT` resumes it).
    Abandon { pipeline: String, unit: String },
    /// When each cron pipeline runs next.
    Schedule {
        /// Passes to show per pipeline.
        #[arg(short = 'n', long, default_value_t = 5)]
        count: usize,
    },
    /// Settle a destination whose settings now point somewhere new.
    #[command(subcommand)]
    Destination(DestinationCommand),
    /// Events waiting for registrars.
    #[command(subcommand)]
    Outbox(OutboxCommand),
    /// Check a configuration.
    #[command(subcommand)]
    Config(ConfigCommand),
    /// Check that this node can run: its config, state, journal, roots,
    /// stores and hooks. Exits 1 if anything would stop ferry.
    Doctor {
        /// Also put a probe object in each store and read it back.
        #[arg(long)]
        write: bool,
        /// Also send each notifier a test message.
        #[arg(long)]
        notify: bool,
    },
    /// Print the JSON Schema of the events hooks receive, or of the manifests
    /// the `manifest` registrar writes.
    Schema {
        #[arg(value_enum)]
        of: SchemaOf,
    },
    /// Run `ferry watch` as a service that starts at boot.
    #[command(subcommand)]
    Service(ServiceCommand),
}

#[derive(Subcommand)]
enum DestinationCommand {
    /// Its new settings reach the same objects: its copies keep counting.
    Accept {
        pipeline: String,
        destination: String,
        /// Show what would change, changing nothing.
        #[arg(long)]
        dry_run: bool,
    },
    /// Forget its copies: every file not yet finalized goes up again.
    Reset {
        pipeline: String,
        destination: String,
        /// Show what would change, changing nothing.
        #[arg(long)]
        dry_run: bool,
    },
}

#[derive(Subcommand)]
enum OutboxCommand {
    /// Drop a registrar's undelivered events (one renamed or removed from the
    /// config), so its units stop waiting on it.
    Discard {
        pipeline: String,
        registrar: String,
        /// Count them, changing nothing.
        #[arg(long)]
        dry_run: bool,
    },
}

#[derive(Subcommand)]
enum ConfigCommand {
    /// Parse and check the config, and show where each pipeline sends data.
    Validate,
}

#[derive(Clone, Copy, clap::ValueEnum)]
enum SchemaOf {
    Event,
    Manifest,
}

#[derive(Subcommand)]
enum ServiceCommand {
    /// Print the service definition `install` would register.
    Print {
        /// Default: this system's.
        #[arg(long, value_enum)]
        platform: Option<Platform>,
        #[arg(long, default_value = "ferry")]
        name: String,
        /// A system-wide systemd unit rather than a user one.
        #[arg(long)]
        system: bool,
        /// Windows: the account the service runs as (default LocalSystem).
        #[arg(long)]
        account: Option<String>,
    },
    /// Register `ferry watch` to start at boot and restart on failure.
    Install {
        #[arg(long, default_value = "ferry")]
        name: String,
        #[arg(long)]
        system: bool,
        #[arg(long)]
        account: Option<String>,
        #[arg(long, env = "FERRY_SERVICE_PASSWORD", hide_env_values = true)]
        password: Option<String>,
    },
    /// Remove the service `install` registered.
    Uninstall {
        #[arg(long, default_value = "ferry")]
        name: String,
        #[arg(long)]
        system: bool,
    },
    /// How the Windows service manager starts ferry.
    #[command(hide = true)]
    Run {
        #[arg(long, default_value = "ferry")]
        name: String,
    },
}

pub fn main() -> ExitCode {
    let cli = Cli::parse();
    match dispatch(cli) {
        Ok(code) => code,
        Err(e) => {
            eprintln!("error: {e:#}");
            ExitCode::from(2)
        }
    }
}

fn dispatch(cli: Cli) -> Result<ExitCode> {
    let config = std::path::absolute(&cli.config)?;
    let env_file = cli.env_file.as_deref().map(std::path::absolute).transpose()?;
    if let Some(path) = &env_file {
        load_env_file(path)?;
    }
    match cli.command {
        Command::Run {
            pipelines,
            dry_run,
            verbose,
        } => run(&config, pipelines, dry_run, verbose),
        Command::Watch { verbose } => {
            serve_with(&config, CancellationToken::new(), true, verbose, true)?;
            Ok(ExitCode::SUCCESS)
        }
        Command::Status { pipelines, units, json } => status(&config, pipelines, units, json),
        Command::Retry { pipeline, unit } => {
            let mut journal = open_journal(&load(&config)?)?;
            let count = journal.retry(pipeline.as_deref(), unit.as_deref())?;
            println!("{count} unit(s) will be retried on the next pass.");
            Ok(ExitCode::SUCCESS)
        }
        Command::Complete { pipeline, unit } => {
            let journal = open_journal(&load(&config)?)?;
            match journal.force_complete(&pipeline, &unit)? {
                None => fail(&format!("{pipeline} has no unit {unit:?}")),
                Some(row) if row.state != OPEN => fail(&format!("{unit} is {}, not open", row.state)),
                Some(_) => {
                    println!("{unit} will complete on the next pass, once its files have settled.");
                    Ok(ExitCode::SUCCESS)
                }
            }
        }
        Command::Abandon { pipeline, unit } => {
            let mut journal = open_journal(&load(&config)?)?;
            match journal.abandon(&pipeline, &unit)? {
                None => fail(&format!("{pipeline} has no unit {unit:?}")),
                Some(_) => {
                    println!("Abandoned {unit}; its files stay where they are.");
                    Ok(ExitCode::SUCCESS)
                }
            }
        }
        Command::Schedule { count } => schedule(&load(&config)?, count),
        Command::Destination(command) => destination_command(&load(&config)?, command),
        Command::Outbox(OutboxCommand::Discard {
            pipeline,
            registrar,
            dry_run,
        }) => {
            let journal = open_journal(&load(&config)?)?;
            let count = journal.discard_outbox(&pipeline, &registrar, !dry_run)?;
            if dry_run {
                println!("[dry run] would drop {count} undelivered event(s) for {registrar} in {pipeline}.");
            } else {
                println!("Dropped {count} undelivered event(s) for {registrar} in {pipeline}.");
            }
            Ok(ExitCode::SUCCESS)
        }
        Command::Config(ConfigCommand::Validate) => validate(&load(&config)?),
        Command::Doctor { write, notify } => crate::doctor::run(
            &config,
            env_file.as_deref(),
            crate::doctor::DoctorOptions { write, notify },
        ),
        Command::Schema { of } => {
            let name = match of {
                SchemaOf::Event => "event.schema.json",
                SchemaOf::Manifest => "manifest.schema.json",
            };
            let (_, schema) = crate::manifest::schemas()
                .into_iter()
                .find(|(file, _)| *file == name)
                .context("no such schema")?;
            println!("{}", serde_json::to_string_pretty(&schema)?);
            Ok(ExitCode::SUCCESS)
        }
        Command::Service(command) => service_command(&config, env_file.as_deref(), command),
    }
}

fn fail(message: &str) -> Result<ExitCode> {
    eprintln!("{message}");
    Ok(ExitCode::from(1))
}

/// Set the variables in the env file `path`, warning if others can read it.
fn load_env_file(path: &Path) -> Result<()> {
    let text = std::fs::read_to_string(path).with_context(|| format!("cannot read env file {}", path.display()))?;
    let vars = crate::util::parse_env_file(&text).map_err(|e| anyhow::anyhow!("{}: {e}", path.display()))?;
    if let Some(warning) = exposure(path) {
        eprintln!("warning: {warning}");
    }
    for (key, value) in vars {
        // SAFETY: `dispatch` calls this before anything starts another thread.
        unsafe { std::env::set_var(key, value) };
    }
    Ok(())
}

/// Why a file of secrets at `path` is readable by other users, if it is.
pub fn exposure(path: &Path) -> Option<String> {
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        let mode = std::fs::metadata(path).ok()?.permissions().mode() & 0o777;
        if mode & 0o077 != 0 {
            return Some(format!(
                "{} can be read by other users (mode {mode:o}); `chmod 600` it",
                path.display()
            ));
        }
    }
    #[cfg(not(unix))]
    let _ = path;
    None
}

fn destination_command(cfg: &Config, command: DestinationCommand) -> Result<ExitCode> {
    let (pipeline, destination, dry_run, reset) = match command {
        DestinationCommand::Accept {
            pipeline,
            destination,
            dry_run,
        } => (pipeline, destination, dry_run, false),
        DestinationCommand::Reset {
            pipeline,
            destination,
            dry_run,
        } => (pipeline, destination, dry_run, true),
    };
    let Some(p) = cfg.pipeline(&pipeline) else {
        return fail(&format!("no pipeline {pipeline:?}"));
    };
    let Some(d) = p.destinations.iter().find(|d| d.key() == destination) else {
        return fail(&format!("{pipeline} has no destination {destination:?}"));
    };
    let binding = Binding {
        name: destination.clone(),
        identity: cfg.destination_identity(p, d),
        verify: d.verify.as_str().into(),
    };
    let mut journal = open_journal(cfg)?;
    let place = match journal.destination(&pipeline, &destination)? {
        Some(was) if was != binding.identity => format!("{} (was {was})", binding.identity),
        _ => binding.identity.clone(),
    };
    let tag = if dry_run { "[dry run] " } else { "" };
    if reset {
        let plan = journal.reset_destination(&pipeline, &binding, &p.destination_keys(), !dry_run)?;
        let units = if plan.reopened.is_empty() {
            String::new()
        } else {
            format!(
                "; {} unit(s) register again ({})",
                plan.reopened.len(),
                plan.reopened.join(", ")
            )
        };
        println!(
            "{tag}{pipeline}/{destination} points at {place}: {} file(s) go up again{units}; \
             {} copies of finalized files stay as a record.",
            plan.files, plan.kept
        );
    } else {
        if !dry_run {
            journal.accept_destination(&pipeline, &binding)?;
        }
        println!("{tag}{pipeline}/{destination} points at {place}; its copies count there.");
    }
    Ok(ExitCode::SUCCESS)
}

fn load(path: &Path) -> Result<Config> {
    Config::load(path)
}

fn open_journal(cfg: &Config) -> Result<Journal> {
    let path = cfg.journal_path();
    if !path.exists() {
        bail!("no journal at {}: ferry has not run yet", path.display());
    }
    Journal::open(&path)
}

fn runtime() -> Result<tokio::runtime::Runtime> {
    Ok(tokio::runtime::Builder::new_multi_thread().enable_all().build()?)
}

fn logging(cfg: &Config, verbose: bool, console: bool, files: bool) -> Result<LogGuard> {
    let dir = cfg.log_dir();
    logs::init(LogSetup {
        dir: files.then_some(dir.as_path()),
        level: if verbose { "debug" } else { cfg.logs.level.as_filter() },
        keep_days: cfg.logs.keep_days,
        console,
    })
}

/// Cancel `stop` on Ctrl-C (or SIGTERM, or a Windows console close); a second
/// one exits at once.
fn stop_on_signals(stop: CancellationToken) {
    tokio::spawn(async move {
        shutdown_signal().await;
        tracing::info!("stopping: letting in-flight work finish (again to force)");
        stop.cancel();
        shutdown_signal().await;
        std::process::exit(130);
    });
}

async fn shutdown_signal() {
    #[cfg(unix)]
    {
        use tokio::signal::unix::{SignalKind, signal};
        match signal(SignalKind::terminate()) {
            Ok(mut term) => {
                tokio::select! {
                    _ = tokio::signal::ctrl_c() => {}
                    _ = term.recv() => {}
                }
            }
            Err(_) => {
                let _ = tokio::signal::ctrl_c().await;
            }
        }
    }
    #[cfg(windows)]
    {
        use tokio::signal::windows::{ctrl_break, ctrl_close, ctrl_shutdown};
        match (ctrl_break(), ctrl_close(), ctrl_shutdown()) {
            (Ok(mut brk), Ok(mut close), Ok(mut shutdown)) => {
                tokio::select! {
                    _ = tokio::signal::ctrl_c() => {}
                    _ = brk.recv() => {}
                    _ = close.recv() => {}
                    _ = shutdown.recv() => {}
                }
            }
            _ => {
                let _ = tokio::signal::ctrl_c().await;
            }
        }
    }
}

fn run(config: &Path, pipelines: Vec<String>, dry_run: bool, verbose: bool) -> Result<ExitCode> {
    let cfg = load(config)?;
    let _logs = logging(&cfg, verbose, true, !dry_run)?;
    let reports = runtime()?.block_on(async move {
        let agent = Agent::open(
            cfg,
            AgentOptions {
                dry_run,
                ..Default::default()
            },
        )?;
        let stop = CancellationToken::new();
        stop_on_signals(stop.clone());
        agent.run_once(&pipelines, &stop).await
    })?;
    Ok(if reports.iter().any(|r| r.failures() > 0) {
        ExitCode::from(1)
    } else {
        ExitCode::SUCCESS
    })
}

/// `ferry watch` until `stop` (the Windows service's entry point too).
pub fn serve(config: &Path, stop: CancellationToken, console: bool) -> Result<()> {
    serve_with(config, stop, console, false, false)
}

fn serve_with(config: &Path, stop: CancellationToken, console: bool, verbose: bool, signals: bool) -> Result<()> {
    let cfg = load(config)?;
    let _logs = logging(&cfg, verbose, console, true)?;
    runtime()?.block_on(async move {
        let agent = Arc::new(Agent::open(cfg, AgentOptions::default())?);
        if signals {
            stop_on_signals(stop.clone());
        }
        agent.watch(stop).await
    })
}

fn status(config: &Path, pipelines: Vec<String>, units: bool, as_json: bool) -> Result<ExitCode> {
    let cfg = load(config)?;
    let names: Vec<String> = if pipelines.is_empty() {
        cfg.pipelines.iter().map(|p| p.name.clone()).collect()
    } else {
        pipelines
    };
    let journal = open_journal(&cfg)?;
    let mut result: BTreeMap<String, Value> = BTreeMap::new();
    for name in &names {
        let p = cfg
            .pipeline(name)
            .with_context(|| format!("no pipeline named {name:?}"))?;
        let summary = journal.summary(name, &p.destination_keys())?;
        let mut data = serde_json::to_value(&summary)?;
        data["failing_copies"] = journal
            .failing_copies(name)?
            .into_iter()
            .map(|(unit, file, copy)| {
                json!({"unit": unit, "file": file.rel, "destination": copy.dest, "attempts": copy.attempts, "error": copy.last_error})
            })
            .collect();
        data["failing_deliveries"] = journal
            .failing_deliveries(name)?
            .into_iter()
            .map(|(unit, row)| {
                json!({"unit": unit, "registrar": row.registrar, "event": row.event, "attempts": row.attempts, "error": row.last_error})
            })
            .collect();
        if units {
            data["unfinished"] = journal
                .units(name, None)?
                .into_iter()
                .filter(|u| ![FINALIZED, ABANDONED, GONE].contains(&u.state.as_str()))
                .map(|u| json!({"unit": u.key, "state": u.state, "note": u.note, "error": u.last_error}))
                .collect();
        }
        result.insert(name.clone(), data);
    }
    if as_json {
        println!("{}", serde_json::to_string_pretty(&result)?);
        return Ok(ExitCode::SUCCESS);
    }
    for (name, data) in &result {
        let counts: Vec<String> = data["units"]
            .as_object()
            .into_iter()
            .flatten()
            .filter(|(_, n)| n.as_i64().unwrap_or(0) > 0)
            .map(|(state, n)| format!("{n} {state}"))
            .collect();
        println!("{name}");
        println!(
            "  units: {}",
            if counts.is_empty() {
                "none yet".into()
            } else {
                counts.join(", ")
            }
        );
        println!(
            "  waiting to upload: {} file(s), {}",
            data["pending_files"],
            human_bytes(data["pending_bytes"].as_u64().unwrap_or(0))
        );
        println!("  last pass: {}", data["last_pass"].as_str().unwrap_or("never"));
        for item in data["failing_copies"].as_array().into_iter().flatten() {
            println!(
                "  failing: {} to {} ({}x): {}",
                text(&item["file"]),
                text(&item["destination"]),
                item["attempts"],
                text(&item["error"])
            );
        }
        for item in data["failing_deliveries"].as_array().into_iter().flatten() {
            println!(
                "  failing: {} {} with {} ({}x): {}",
                text(&item["event"]),
                text(&item["unit"]),
                text(&item["registrar"]),
                item["attempts"],
                text(&item["error"])
            );
        }
        for item in data["unfinished"].as_array().into_iter().flatten() {
            let note = item["note"].as_str().map(|n| format!(" -- {n}")).unwrap_or_default();
            println!("  {:<12} {}{note}", text(&item["state"]), text(&item["unit"]));
        }
    }
    Ok(ExitCode::SUCCESS)
}

fn text(value: &Value) -> String {
    value.as_str().map(str::to_string).unwrap_or_else(|| value.to_string())
}

fn schedule(cfg: &Config, count: usize) -> Result<ExitCode> {
    for p in &cfg.pipelines {
        match (&p.mode, &p.cron) {
            (Mode::Cron, Some(expr)) => {
                let schedule = Schedule::new(expr, p.timezone.as_deref())?;
                println!(
                    "{}: {expr:?} ({})",
                    p.name,
                    p.timezone.as_deref().unwrap_or("local time")
                );
                for when in schedule.upcoming(Utc::now(), count) {
                    println!("  {}", schedule.show(when));
                }
            }
            _ => println!("{}: streams continuously (upload per {})", p.name, upload_name(p)),
        }
    }
    Ok(ExitCode::SUCCESS)
}

fn upload_name(p: &crate::config::PipelineConfig) -> &'static str {
    match p.upload {
        crate::config::Upload::File => "file",
        crate::config::Upload::Unit => "unit",
    }
}

fn validate(cfg: &Config) -> Result<ExitCode> {
    println!("node {}; state in {}", cfg.node, cfg.state_dir.display());
    let mut warnings = Vec::new();
    for p in &cfg.pipelines {
        let destinations: Vec<String> = p
            .destinations
            .iter()
            .map(|d| {
                let url = cfg.stores[&d.store].url.trim_end_matches('/').to_string();
                let prefix = p.prefix_for(d);
                let target = if prefix.trim_matches('/').is_empty() {
                    url
                } else {
                    format!("{url}/{}", prefix.trim_matches('/'))
                };
                let need = if d.required { "required" } else { "optional" };
                format!("{target} ({need}, verify {})", d.verify.as_str())
            })
            .collect();
        let when = match (&p.mode, &p.cron) {
            (Mode::Cron, Some(expr)) => format!("cron {expr:?}"),
            _ => "stream".into(),
        };
        let registrars: Vec<String> = p.registrars.iter().map(|r| r.key()).collect();
        println!(
            "  {}: {} -> {} [{when}; upload per {}; finalize {}; registrars {}]",
            p.name,
            p.root.display(),
            destinations.join(", "),
            upload_name(p),
            p.finalize.action.as_str(),
            if registrars.is_empty() {
                "none".into()
            } else {
                registrars.join(", ")
            }
        );
        if !p.root.is_dir() {
            warnings.push(format!("{}: root {} does not exist (yet)", p.name, p.root.display()));
        }
        for hook in &p.registrars {
            if let Some(program) = hook.exec.as_ref().and_then(|argv| argv.first())
                && find_program(program, hook.cwd.as_deref()).is_none()
            {
                warnings.push(format!("{}: registrar {}: cannot find {program:?}", p.name, hook.key()));
            }
        }
    }
    for warning in &warnings {
        println!("warning: {warning}");
    }
    println!("ok");
    Ok(ExitCode::SUCCESS)
}

/// Where `program` would be found: a path (relative to `cwd`), or on PATH.
pub(crate) fn find_program(program: &str, cwd: Option<&Path>) -> Option<PathBuf> {
    let candidate = Path::new(program);
    let extensions: Vec<String> = if cfg!(windows) {
        std::env::var("PATHEXT")
            .unwrap_or_else(|_| ".EXE;.BAT;.CMD".into())
            .split(';')
            .map(str::to_string)
            .chain(std::iter::once(String::new()))
            .collect()
    } else {
        vec![String::new()]
    };
    let found = |path: &Path| {
        extensions
            .iter()
            .map(|ext| PathBuf::from(format!("{}{ext}", path.display())))
            .find(|p| p.is_file())
    };
    if candidate.components().count() > 1 || candidate.is_absolute() {
        let full = match cwd {
            Some(dir) if candidate.is_relative() => dir.join(candidate),
            _ => candidate.to_path_buf(),
        };
        return found(&full);
    }
    std::env::split_paths(&std::env::var_os("PATH")?).find_map(|dir| found(&dir.join(program)))
}

fn service_command(config: &Path, env_file: Option<&Path>, command: ServiceCommand) -> Result<ExitCode> {
    let spec = |name: String, system: bool, account: Option<String>, password: Option<String>| -> Result<ServiceSpec> {
        Ok(ServiceSpec {
            name,
            exe: std::env::current_exe()?,
            config: config.to_path_buf(),
            env_file: env_file.map(Path::to_path_buf),
            system,
            account,
            password,
        })
    };
    match command {
        ServiceCommand::Print {
            platform,
            name,
            system,
            account,
        } => {
            let platform = platform
                .or_else(Platform::current)
                .context("pick a --platform (windows or systemd)")?;
            print!("{}", service::render(&spec(name, system, account, None)?, platform));
        }
        ServiceCommand::Install {
            name,
            system,
            account,
            password,
        } => {
            load(config)?;
            println!("{}", service::install(&spec(name, system, account, password)?)?);
        }
        ServiceCommand::Uninstall { name, system } => println!("{}", service::uninstall(&name, system)?),
        ServiceCommand::Run { name } => service::run_service(&name, config)?,
    }
    Ok(ExitCode::SUCCESS)
}
