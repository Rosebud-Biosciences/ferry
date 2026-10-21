//! Running `ferry watch` as a service: a native Windows service, a systemd
//! unit on Linux, or a launchd job on macOS. Each starts ferry at boot (or, as
//! a user's service, at login) and restarts it if it exits.

use std::path::{Path, PathBuf};

use anyhow::{Context, Result, bail};

#[derive(Debug, Clone, Copy, PartialEq, Eq, clap::ValueEnum)]
pub enum Platform {
    Windows,
    Systemd,
    Launchd,
}

impl Platform {
    pub fn current() -> Option<Self> {
        if cfg!(windows) {
            Some(Self::Windows)
        } else if cfg!(target_os = "linux") {
            Some(Self::Systemd)
        } else if cfg!(target_os = "macos") {
            Some(Self::Launchd)
        } else {
            None
        }
    }
}

/// How the service is set up.
#[derive(Debug, Clone)]
pub struct ServiceSpec {
    pub name: String,
    pub exe: PathBuf,
    pub config: PathBuf,
    /// Read into the environment first (`--env-file`): the service's secrets.
    pub env_file: Option<PathBuf>,
    /// A system service (a systemd system unit, a launchd daemon) rather than
    /// a user's. Windows services always run system-wide, as LocalSystem
    /// unless `account` is given.
    pub system: bool,
    /// The account a system service runs as: on Windows the account it logs
    /// on as (`.\user`, `DOMAIN\user`); elsewhere a user name (default root).
    pub account: Option<String>,
    pub password: Option<String>,
}

impl ServiceSpec {
    fn description(&self) -> String {
        format!(
            "ferry edge ingest agent ({}): watch --config {}",
            self.name,
            self.config.display()
        )
    }

    /// `--config` and `--env-file`, as every service passes them.
    fn config_args(&self) -> Vec<String> {
        let mut args = vec!["--config".into(), self.config.to_string_lossy().into_owned()];
        if let Some(env_file) = &self.env_file {
            args.extend(["--env-file".into(), env_file.to_string_lossy().into_owned()]);
        }
        args
    }
}

/// systemd quoting: wrap in double quotes (escaping `\` and `"`) when needed.
fn systemd_quote(arg: &str) -> String {
    if !arg.is_empty()
        && !arg
            .chars()
            .any(|c| c.is_whitespace() || matches!(c, '"' | '\'' | '\\' | '%' | '$'))
    {
        return arg.to_string();
    }
    let escaped = arg
        .replace('\\', "\\\\")
        .replace('"', "\\\"")
        .replace('%', "%%")
        .replace('$', "$$");
    format!("\"{escaped}\"")
}

pub fn systemd_unit(spec: &ServiceSpec) -> String {
    let exec = [spec.exe.to_string_lossy().into_owned(), "watch".into()]
        .into_iter()
        .chain(spec.config_args())
        .map(|a| systemd_quote(&a))
        .collect::<Vec<_>>()
        .join(" ");
    let workdir = spec.config.parent().unwrap_or(Path::new("/"));
    let wanted_by = if spec.system {
        "multi-user.target"
    } else {
        "default.target"
    };
    let user = match (&spec.account, spec.system) {
        (Some(account), true) => format!("User={}\n", systemd_quote(account)),
        _ => String::new(),
    };
    format!(
        "[Unit]\n\
         Description=ferry edge ingest agent ({name})\n\
         Wants=network-online.target\n\
         After=network-online.target\n\
         \n\
         [Service]\n\
         Type=simple\n\
         ExecStart={exec}\n\
         WorkingDirectory={workdir}\n\
         {user}\
         Restart=always\n\
         RestartSec=10\n\
         TimeoutStopSec=60\n\
         \n\
         [Install]\n\
         WantedBy={wanted_by}\n",
        name = spec.name,
        workdir = systemd_quote(&workdir.to_string_lossy()),
    )
}

fn xml_escape(text: &str) -> String {
    text.replace('&', "&amp;")
        .replace('<', "&lt;")
        .replace('>', "&gt;")
        .replace('"', "&quot;")
}

/// A launchd job: a daemon that starts at boot (`system`), or else an agent
/// that starts when its user logs in. Either is restarted if it exits.
pub fn launchd_plist(spec: &ServiceSpec) -> String {
    let args: String = [spec.exe.to_string_lossy().into_owned(), "watch".into()]
        .into_iter()
        .chain(spec.config_args())
        .map(|a| format!("    <string>{}</string>\n", xml_escape(&a)))
        .collect();
    let workdir = spec.config.parent().unwrap_or(Path::new("/"));
    let user = match (&spec.account, spec.system) {
        (Some(account), true) => format!("  <key>UserName</key>\n  <string>{}</string>\n", xml_escape(account)),
        _ => String::new(),
    };
    format!(
        "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n\
         <!DOCTYPE plist PUBLIC \"-//Apple//DTD PLIST 1.0//EN\" \"http://www.apple.com/DTDs/PropertyList-1.0.dtd\">\n\
         <plist version=\"1.0\">\n\
         <dict>\n\
         \x20 <key>Label</key>\n\
         \x20 <string>{label}</string>\n\
         \x20 <key>ProgramArguments</key>\n\
         \x20 <array>\n\
         {args}\
         \x20 </array>\n\
         \x20 <key>WorkingDirectory</key>\n\
         \x20 <string>{workdir}</string>\n\
         {user}\
         \x20 <key>RunAtLoad</key>\n\
         \x20 <true/>\n\
         \x20 <key>KeepAlive</key>\n\
         \x20 <true/>\n\
         \x20 <key>ThrottleInterval</key>\n\
         \x20 <integer>10</integer>\n\
         \x20 <key>ExitTimeOut</key>\n\
         \x20 <integer>60</integer>\n\
         \x20 <key>ProcessType</key>\n\
         \x20 <string>Background</string>\n\
         </dict>\n\
         </plist>\n",
        label = xml_escape(&spec.name),
        workdir = xml_escape(&workdir.to_string_lossy()),
    )
}

/// Windows command-line quoting for one argument (CommandLineToArgvW rules).
fn windows_quote(arg: &str) -> String {
    if !arg.is_empty() && !arg.chars().any(|c| c == ' ' || c == '\t' || c == '"') {
        return arg.to_string();
    }
    let mut out = String::from("\"");
    let mut backslashes = 0;
    for c in arg.chars() {
        match c {
            '\\' => backslashes += 1,
            '"' => {
                out.push_str(&"\\".repeat(backslashes * 2 + 1));
                out.push('"');
                backslashes = 0;
            }
            _ => {
                out.push_str(&"\\".repeat(backslashes));
                out.push(c);
                backslashes = 0;
            }
        }
    }
    out.push_str(&"\\".repeat(backslashes * 2));
    out.push('"');
    out
}

/// The arguments the Windows service starts ferry with.
pub fn windows_arguments(spec: &ServiceSpec) -> Vec<String> {
    let mut args = vec!["service".into(), "run".into(), "--name".into(), spec.name.clone()];
    args.extend(spec.config_args());
    args
}

/// The `sc.exe` commands equivalent to `ferry service install` on Windows.
pub fn windows_commands(spec: &ServiceSpec) -> String {
    let mut bin = vec![windows_quote(&spec.exe.to_string_lossy())];
    bin.extend(windows_arguments(spec).iter().map(|a| windows_quote(a)));
    let account = spec
        .account
        .as_ref()
        .map(|a| format!(" obj= {} password= <password>", windows_quote(a)))
        .unwrap_or_default();
    format!(
        "sc.exe create {name} binPath= {bin} start= delayed-auto DisplayName= {display}{account}\n\
         sc.exe description {name} {description}\n\
         sc.exe failure {name} reset= 86400 actions= restart/60000/restart/60000/restart/60000\n\
         sc.exe failureflag {name} 1\n",
        name = spec.name,
        bin = windows_quote(&bin.join(" ")),
        display = windows_quote(&format!("ferry ({})", spec.name)),
        description = windows_quote(&spec.description()),
    )
}

pub fn render(spec: &ServiceSpec, platform: Platform) -> String {
    match platform {
        Platform::Windows => windows_commands(spec),
        Platform::Systemd => systemd_unit(spec),
        Platform::Launchd => launchd_plist(spec),
    }
}

fn plist_path(name: &str, system: bool) -> Result<PathBuf> {
    Ok(if system {
        PathBuf::from("/Library/LaunchDaemons").join(format!("{name}.plist"))
    } else {
        dirs::home_dir()
            .context("no home directory")?
            .join("Library/LaunchAgents")
            .join(format!("{name}.plist"))
    })
}

/// The launchd domain a job goes in: `system`, or the user's GUI session.
fn launchd_domain(system: bool) -> Result<String> {
    if system {
        return Ok("system".into());
    }
    let out = std::process::Command::new("id")
        .arg("-u")
        .output()
        .context("cannot run `id -u`")?;
    Ok(format!("gui/{}", String::from_utf8_lossy(&out.stdout).trim()))
}

fn systemctl(system: bool) -> std::process::Command {
    let mut cmd = std::process::Command::new("systemctl");
    if !system {
        cmd.arg("--user");
    }
    cmd
}

fn run(mut cmd: std::process::Command) -> Result<()> {
    let status = cmd.status().with_context(|| format!("cannot run {cmd:?}"))?;
    if !status.success() {
        bail!("{cmd:?} failed ({status})");
    }
    Ok(())
}

fn unit_path(name: &str, system: bool) -> Result<PathBuf> {
    Ok(if system {
        PathBuf::from("/etc/systemd/system").join(format!("{name}.service"))
    } else {
        dirs::config_dir()
            .context("no user config directory")?
            .join("systemd/user")
            .join(format!("{name}.service"))
    })
}

pub fn install(spec: &ServiceSpec) -> Result<String> {
    match Platform::current() {
        Some(Platform::Systemd) => {
            let path = unit_path(&spec.name, spec.system)?;
            if let Some(parent) = path.parent() {
                std::fs::create_dir_all(parent)?;
            }
            std::fs::write(&path, systemd_unit(spec)).with_context(|| format!("cannot write {}", path.display()))?;
            let mut reload = systemctl(spec.system);
            reload.arg("daemon-reload");
            run(reload)?;
            let mut enable = systemctl(spec.system);
            enable.args(["enable", "--now", &format!("{}.service", spec.name)]);
            run(enable)?;
            let linger = if spec.system {
                ""
            } else {
                " Run `loginctl enable-linger` so it starts at boot without a login."
            };
            Ok(format!("Installed and started {}.{linger}", path.display()))
        }
        Some(Platform::Launchd) => {
            let path = plist_path(&spec.name, spec.system)?;
            if let Some(parent) = path.parent() {
                std::fs::create_dir_all(parent)?;
            }
            std::fs::write(&path, launchd_plist(spec)).with_context(|| format!("cannot write {}", path.display()))?;
            let domain = launchd_domain(spec.system)?;
            // An earlier install of the same job is replaced.
            let _ = std::process::Command::new("launchctl")
                .args(["bootout", &format!("{domain}/{}", spec.name)])
                .output();
            let mut bootstrap = std::process::Command::new("launchctl");
            bootstrap.args(["bootstrap", &domain]).arg(&path);
            run(bootstrap)?;
            let when = if spec.system {
                "at boot"
            } else {
                "when you log in (pass --system, with sudo, to start it at boot)"
            };
            Ok(format!("Installed and started {}; it starts {when}.", path.display()))
        }
        Some(Platform::Windows) => windows::install(spec),
        None => bail!(
            "ferry can install a service on Windows, Linux (systemd) and macOS (launchd); elsewhere, \
             run `ferry service print --platform systemd` and adapt it"
        ),
    }
}

pub fn uninstall(name: &str, system: bool) -> Result<String> {
    match Platform::current() {
        Some(Platform::Systemd) => {
            let mut disable = systemctl(system);
            disable.args(["disable", "--now", &format!("{name}.service")]);
            let _ = disable.status();
            let path = unit_path(name, system)?;
            if path.exists() {
                std::fs::remove_file(&path)?;
            }
            let mut reload = systemctl(system);
            reload.arg("daemon-reload");
            run(reload)?;
            Ok(format!("Removed {}.", path.display()))
        }
        Some(Platform::Launchd) => {
            let _ = std::process::Command::new("launchctl")
                .args(["bootout", &format!("{}/{name}", launchd_domain(system)?)])
                .output();
            let path = plist_path(name, system)?;
            if path.exists() {
                std::fs::remove_file(&path)?;
            }
            Ok(format!("Stopped {name} and removed {}.", path.display()))
        }
        Some(Platform::Windows) => windows::uninstall(name),
        None => bail!("no service manager ferry knows how to use on this platform"),
    }
}

#[cfg(windows)]
pub use windows::run_service;

#[cfg(not(windows))]
pub fn run_service(_name: &str, _config: &Path) -> Result<()> {
    bail!("`ferry service run` is how the Windows service manager starts ferry; use `ferry watch` here")
}

#[cfg(not(windows))]
mod windows {
    use super::*;

    pub fn install(_spec: &ServiceSpec) -> Result<String> {
        unreachable!("only called on Windows")
    }

    pub fn uninstall(_name: &str) -> Result<String> {
        unreachable!("only called on Windows")
    }
}

#[cfg(windows)]
mod windows {
    use super::*;
    use std::ffi::OsString;
    use std::sync::OnceLock;
    use std::time::Duration;

    use tokio_util::sync::CancellationToken;
    use windows_service::service::{
        ServiceAccess, ServiceAction, ServiceActionType, ServiceControl, ServiceControlAccept, ServiceErrorControl,
        ServiceExitCode, ServiceFailureActions, ServiceFailureResetPeriod, ServiceInfo, ServiceStartType, ServiceState,
        ServiceStatus, ServiceType,
    };
    use windows_service::service_control_handler::{self, ServiceControlHandlerResult};
    use windows_service::service_manager::{ServiceManager, ServiceManagerAccess};
    use windows_service::{define_windows_service, service_dispatcher};

    pub fn install(spec: &ServiceSpec) -> Result<String> {
        let manager = ServiceManager::local_computer(
            None::<&str>,
            ServiceManagerAccess::CONNECT | ServiceManagerAccess::CREATE_SERVICE,
        )
        .context("cannot reach the service manager (run as administrator)")?;
        let info = ServiceInfo {
            name: OsString::from(&spec.name),
            display_name: OsString::from(format!("ferry ({})", spec.name)),
            service_type: ServiceType::OWN_PROCESS,
            start_type: ServiceStartType::AutoStart,
            error_control: ServiceErrorControl::Normal,
            executable_path: spec.exe.clone(),
            launch_arguments: windows_arguments(spec).into_iter().map(OsString::from).collect(),
            dependencies: vec![],
            account_name: spec.account.as_ref().map(OsString::from),
            account_password: spec.password.as_ref().map(OsString::from),
        };
        let service = manager
            .create_service(&info, ServiceAccess::CHANGE_CONFIG | ServiceAccess::START)
            .with_context(|| format!("cannot create service {}", spec.name))?;
        service.set_description(spec.description())?;
        // Start once the network and the rest of boot have settled.
        service.set_delayed_auto_start(true)?;
        let restart = ServiceAction {
            action_type: ServiceActionType::Restart,
            delay: Duration::from_secs(60),
        };
        service.update_failure_actions(ServiceFailureActions {
            reset_period: ServiceFailureResetPeriod::After(Duration::from_secs(86400)),
            reboot_msg: None,
            command: None,
            actions: Some(vec![restart.clone(), restart.clone(), restart]),
        })?;
        // Restart after an error exit too, not only a crash.
        service.set_failure_actions_on_non_crash_failures(true)?;
        service
            .start::<&str>(&[])
            .with_context(|| format!("installed {}, but it did not start", spec.name))?;
        Ok(format!(
            "Installed and started service {name}: it starts at boot and restarts on failure. \
             Manage it with `sc.exe query {name}` or services.msc.",
            name = spec.name
        ))
    }

    pub fn uninstall(name: &str) -> Result<String> {
        let manager = ServiceManager::local_computer(None::<&str>, ServiceManagerAccess::CONNECT)
            .context("cannot reach the service manager (run as administrator)")?;
        let service = manager
            .open_service(
                name,
                ServiceAccess::QUERY_STATUS | ServiceAccess::STOP | ServiceAccess::DELETE,
            )
            .with_context(|| format!("no service {name}"))?;
        service.delete()?;
        if service.query_status()?.current_state != ServiceState::Stopped {
            let _ = service.stop();
        }
        Ok(format!("Removed service {name} (it is deleted once it has stopped)."))
    }

    static SERVICE: OnceLock<(String, PathBuf)> = OnceLock::new();

    define_windows_service!(ffi_service_main, service_main);

    /// Hand the process to the service manager; returns when the service stops.
    pub fn run_service(name: &str, config: &Path) -> Result<()> {
        SERVICE
            .set((name.to_string(), config.to_path_buf()))
            .map_err(|_| anyhow::anyhow!("service already running"))?;
        service_dispatcher::start(name, ffi_service_main)
            .context("not started by the service manager (use `ferry watch` from a console)")?;
        Ok(())
    }

    fn service_main(_arguments: Vec<OsString>) {
        let Some((name, config)) = SERVICE.get().cloned() else {
            return;
        };
        if let Err(e) = serve(&name, &config) {
            tracing::error!("service {name} failed: {e:#}");
        }
    }

    fn serve(name: &str, config: &Path) -> Result<()> {
        let stop = CancellationToken::new();
        let on_stop = stop.clone();
        let handler = move |control| match control {
            ServiceControl::Stop | ServiceControl::Shutdown | ServiceControl::Preshutdown => {
                on_stop.cancel();
                ServiceControlHandlerResult::NoError
            }
            ServiceControl::Interrogate => ServiceControlHandlerResult::NoError,
            _ => ServiceControlHandlerResult::NotImplemented,
        };
        let status = service_control_handler::register(name, handler)?;
        let report = |state: ServiceState, code: u32, wait: Duration| {
            status.set_service_status(ServiceStatus {
                service_type: ServiceType::OWN_PROCESS,
                current_state: state,
                controls_accepted: if state == ServiceState::Running {
                    ServiceControlAccept::STOP | ServiceControlAccept::SHUTDOWN
                } else {
                    ServiceControlAccept::empty()
                },
                exit_code: ServiceExitCode::Win32(code),
                checkpoint: 0,
                wait_hint: wait,
                process_id: None,
            })
        };
        report(ServiceState::Running, 0, Duration::ZERO)?;
        let outcome = crate::cli::serve(config, stop.clone());
        report(ServiceState::StopPending, 0, Duration::from_secs(60))?;
        // A non-zero exit makes the service manager apply the restart actions.
        let code = if outcome.is_ok() { 0 } else { 1 };
        report(ServiceState::Stopped, code, Duration::ZERO)?;
        outcome
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn spec() -> ServiceSpec {
        ServiceSpec {
            name: "ferry".into(),
            exe: PathBuf::from("/opt/ferry/bin/ferry"),
            config: PathBuf::from("/etc/ferry/my config.yaml"),
            env_file: None,
            system: true,
            account: None,
            password: None,
        }
    }

    #[test]
    fn the_systemd_unit_runs_watch_and_restarts_it() {
        let unit = systemd_unit(&spec());
        assert!(
            unit.contains("ExecStart=/opt/ferry/bin/ferry watch --config \"/etc/ferry/my config.yaml\"\n"),
            "{unit}"
        );
        assert!(unit.contains("Restart=always"));
        assert!(unit.contains("WantedBy=multi-user.target"));
    }

    #[test]
    fn the_launchd_job_runs_watch_and_keeps_it_alive() {
        let plist = launchd_plist(&ServiceSpec {
            config: PathBuf::from("/Library/Application Support/ferry/R&D.yaml"),
            account: Some("labdata".into()),
            ..spec()
        });
        assert!(
            plist.contains("  <key>Label</key>\n  <string>ferry</string>\n"),
            "{plist}"
        );
        assert!(
            plist.contains(
                "    <string>/opt/ferry/bin/ferry</string>\n    <string>watch</string>\n    \
                 <string>--config</string>\n    <string>/Library/Application Support/ferry/R&amp;D.yaml</string>\n  </array>"
            ),
            "{plist}"
        );
        assert!(
            plist.contains("<key>UserName</key>\n  <string>labdata</string>"),
            "{plist}"
        );
        assert!(plist.contains("<key>KeepAlive</key>\n  <true/>"), "{plist}");
        if cfg!(target_os = "macos") {
            let dir = tempfile::tempdir().unwrap();
            let path = dir.path().join("ferry.plist");
            std::fs::write(&path, &plist).unwrap();
            let lint = std::process::Command::new("plutil")
                .arg("-lint")
                .arg(&path)
                .output()
                .unwrap();
            assert!(lint.status.success(), "{}", String::from_utf8_lossy(&lint.stdout));
        }
        let agent = launchd_plist(&ServiceSpec {
            system: false,
            account: Some("labdata".into()),
            ..spec()
        });
        assert!(!agent.contains("UserName"), "an agent runs as its user");
    }

    #[test]
    fn a_system_unit_can_run_as_an_account() {
        let unit = systemd_unit(&ServiceSpec {
            account: Some("ferry".into()),
            ..spec()
        });
        assert!(unit.contains("WorkingDirectory=/etc/ferry\nUser=ferry\n"), "{unit}");
        assert!(!systemd_unit(&spec()).contains("User="));
    }

    #[test]
    fn services_read_their_env_file_first() {
        let spec = ServiceSpec {
            env_file: Some(PathBuf::from("/etc/ferry/ferry.env")),
            ..spec()
        };
        assert!(
            systemd_unit(&spec).contains("--env-file /etc/ferry/ferry.env\n"),
            "{}",
            systemd_unit(&spec)
        );
        assert_eq!(
            windows_arguments(&spec)[4..],
            [
                "--config",
                "/etc/ferry/my config.yaml",
                "--env-file",
                "/etc/ferry/ferry.env"
            ]
        );
    }

    #[test]
    fn windows_arguments_survive_spaces_and_quotes() {
        assert_eq!(
            windows_quote(r"C:\Program Files\ferry\ferry.exe"),
            r#""C:\Program Files\ferry\ferry.exe""#
        );
        assert_eq!(windows_quote(r#"say "hi""#), r#""say \"hi\"""#);
        assert_eq!(windows_quote(r"C:\dir with space\"), r#""C:\dir with space\\""#);
        assert_eq!(windows_quote("plain"), "plain");
        let commands = windows_commands(&ServiceSpec {
            exe: PathBuf::from(r"C:\Program Files\ferry\ferry.exe"),
            config: PathBuf::from(r"C:\ProgramData\ferry\ferry.yaml"),
            ..spec()
        });
        assert!(commands.contains("start= delayed-auto"), "{commands}");
        assert!(commands.contains("actions= restart/60000"), "{commands}");
    }
}
