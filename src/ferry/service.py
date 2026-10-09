"""Running ``ferry watch`` as a service: Windows Task Scheduler or systemd.

On Windows a scheduled task that starts at boot and restarts on failure is the
dependable way to keep a Python process alive: it needs no service host (the
pywin32 one is fragile inside virtual environments) and is what lab PCs already
use for nightly scripts.
"""

from __future__ import annotations

import getpass
import os
import shlex
import subprocess
import sys
from pathlib import Path
from xml.sax.saxutils import escape


def default_platform() -> str:
    if sys.platform == "win32":
        return "windows"
    if sys.platform.startswith("linux"):
        return "systemd"
    return "unsupported"


def command(config: Path) -> list[str]:
    return [sys.executable, "-m", "ferry", "watch", "--config", str(config)]


def systemd_unit(config: Path, *, name: str, system: bool = False) -> str:
    return f"""\
[Unit]
Description=ferry edge ingest agent ({name})
Wants=network-online.target
After=network-online.target

[Service]
Type=simple
ExecStart={shlex.join(command(config))}
WorkingDirectory={config.parent}
Restart=always
RestartSec=10
TimeoutStopSec=60
Environment=PYTHONUNBUFFERED=1

[Install]
WantedBy={"multi-user.target" if system else "default.target"}
"""


def windows_user() -> str:
    domain = os.environ.get("USERDOMAIN")
    user = getpass.getuser()
    return f"{domain}\\{user}" if domain else user


def task_xml(
    config: Path, *, name: str, system: bool = False, user: str | None = None
) -> str:
    argv = command(config)
    if system:
        principal = (
            "<UserId>S-1-5-18</UserId>\n      <RunLevel>HighestAvailable</RunLevel>"
        )
    else:
        # S4U: run whether or not the user is logged on, without storing a
        # password. It has no Windows network credentials, which S3 over HTTPS
        # does not need.
        principal = (
            f"<UserId>{escape(user or windows_user())}</UserId>\n"
            "      <LogonType>S4U</LogonType>\n"
            "      <RunLevel>HighestAvailable</RunLevel>"
        )
    description = escape(f"ferry edge ingest agent ({name}): watch --config {config}")
    return f"""\
<?xml version="1.0" encoding="UTF-16"?>
<Task version="1.2" xmlns="http://schemas.microsoft.com/windows/2004/02/mit/task">
  <RegistrationInfo>
    <Description>{description}</Description>
  </RegistrationInfo>
  <Triggers>
    <BootTrigger>
      <Enabled>true</Enabled>
      <Delay>PT1M</Delay>
    </BootTrigger>
  </Triggers>
  <Principals>
    <Principal id="Author">
      {principal}
    </Principal>
  </Principals>
  <Settings>
    <MultipleInstancesPolicy>IgnoreNew</MultipleInstancesPolicy>
    <DisallowStartIfOnBatteries>false</DisallowStartIfOnBatteries>
    <StopIfGoingOnBatteries>false</StopIfGoingOnBatteries>
    <AllowHardTerminate>true</AllowHardTerminate>
    <StartWhenAvailable>true</StartWhenAvailable>
    <RunOnlyIfNetworkAvailable>false</RunOnlyIfNetworkAvailable>
    <IdleSettings>
      <StopOnIdleEnd>false</StopOnIdleEnd>
      <RestartOnIdle>false</RestartOnIdle>
    </IdleSettings>
    <AllowStartOnDemand>true</AllowStartOnDemand>
    <Enabled>true</Enabled>
    <Hidden>false</Hidden>
    <RunOnlyIfIdle>false</RunOnlyIfIdle>
    <WakeToRun>false</WakeToRun>
    <ExecutionTimeLimit>PT0S</ExecutionTimeLimit>
    <Priority>7</Priority>
    <RestartOnFailure>
      <Interval>PT1M</Interval>
      <Count>999</Count>
    </RestartOnFailure>
  </Settings>
  <Actions Context="Author">
    <Exec>
      <Command>{escape(argv[0])}</Command>
      <Arguments>{escape(subprocess.list2cmdline(argv[1:]))}</Arguments>
      <WorkingDirectory>{escape(str(config.parent))}</WorkingDirectory>
    </Exec>
  </Actions>
</Task>
"""


def render(config: Path, *, name: str, platform: str, system: bool = False) -> str:
    if platform == "windows":
        return task_xml(config, name=name, system=system)
    if platform == "systemd":
        return systemd_unit(config, name=name, system=system)
    raise ValueError(f"no service definition for platform {platform!r}")


def _systemctl(system: bool) -> list[str]:
    return ["systemctl"] if system else ["systemctl", "--user"]


def _unit_path(name: str, system: bool) -> Path:
    base = (
        Path("/etc/systemd/system") if system else Path.home() / ".config/systemd/user"
    )
    return base / f"{name}.service"


def install(config: Path, *, name: str, system: bool = False) -> str:
    platform = default_platform()
    if platform == "windows":
        xml = config.parent / f"{name}.task.xml"
        xml.write_text(task_xml(config, name=name, system=system), encoding="utf-16")
        subprocess.run(
            ["schtasks", "/Create", "/TN", name, "/XML", str(xml), "/F"], check=True
        )
        return (
            f"Registered scheduled task {name!r}: starts at boot and restarts on "
            f"failure. Start it now with: schtasks /Run /TN {name}"
        )
    if platform == "systemd":
        path = _unit_path(name, system)
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(
            systemd_unit(config, name=name, system=system), encoding="utf-8"
        )
        subprocess.run([*_systemctl(system), "daemon-reload"], check=True)
        subprocess.run(
            [*_systemctl(system), "enable", "--now", f"{name}.service"], check=True
        )
        linger = (
            ""
            if system
            else " Run `loginctl enable-linger` so it starts at boot without a login."
        )
        return f"Installed and started {path}.{linger}"
    raise RuntimeError(
        "ferry can install a service on Windows (Task Scheduler) and Linux "
        "(systemd); elsewhere, run `ferry service print --platform systemd` and "
        "adapt it"
    )


def uninstall(*, name: str, system: bool = False) -> str:
    platform = default_platform()
    if platform == "windows":
        subprocess.run(["schtasks", "/Delete", "/TN", name, "/F"], check=True)
        return f"Removed scheduled task {name!r}."
    if platform == "systemd":
        subprocess.run(
            [*_systemctl(system), "disable", "--now", f"{name}.service"], check=False
        )
        path = _unit_path(name, system)
        path.unlink(missing_ok=True)
        subprocess.run([*_systemctl(system), "daemon-reload"], check=True)
        return f"Removed {path}."
    raise RuntimeError("no service manager ferry knows how to use on this platform")
