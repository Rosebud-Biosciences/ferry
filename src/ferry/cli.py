"""The ``ferry`` command line."""

from __future__ import annotations

import asyncio
import json
import signal
import sys
from pathlib import Path

import typer

from . import plugins, service
from ._version import __version__
from .adapters import Adapter
from .config import ConfigError, FerryConfig, load_config
from .engine import human_bytes
from .journal import FINALIZED, Journal
from .lock import AlreadyRunning
from .logs import setup_logging
from .notify import Notifier
from .registrars.base import Registrar
from .runner import Agent
from .schedule import CronSchedule

app = typer.Typer(
    help=(
        "Ship instrument data from watched folders to S3-compatible storage, "
        "on a cron schedule or as it lands."
    ),
    no_args_is_help=True,
    add_completion=False,
)
config_app = typer.Typer(help="Check a configuration.", no_args_is_help=True)
service_app = typer.Typer(help="Run `ferry watch` as a service.", no_args_is_help=True)
app.add_typer(config_app, name="config")
app.add_typer(service_app, name="service")

CONFIG = typer.Option(
    None,
    "--config",
    "-c",
    envvar="FERRY_CONFIG",
    help="Config file (default: ./ferry.yaml, or $FERRY_CONFIG).",
)
SOURCES = typer.Option(None, "--source", "-s", help="Only this source (repeatable).")
VERBOSE = typer.Option(False, "--verbose", "-v", help="Log debug detail.")


def _fail(message: str, code: int = 2) -> typer.Exit:
    typer.secho(message, err=True, fg=typer.colors.RED)
    return typer.Exit(code)


def _config_path(path: Path | None) -> Path:
    return (path or Path("ferry.yaml")).resolve()


def _load(path: Path | None) -> FerryConfig:
    try:
        return load_config(_config_path(path))
    except ConfigError as exc:
        raise _fail(str(exc)) from exc


def _version(value: bool) -> None:
    if value:
        typer.echo(f"ferry {__version__}")
        raise typer.Exit()


@app.callback()
def main_options(
    version: bool = typer.Option(
        False, "--version", callback=_version, is_eager=True, help="Show the version."
    ),
) -> None:
    pass


@app.command()
def run(
    config: Path | None = CONFIG,
    source: list[str] | None = SOURCES,
    dry_run: bool = typer.Option(
        False, "--dry-run", help="Show what a pass would do, changing nothing."
    ),
    verbose: bool = VERBOSE,
) -> None:
    """One pass over every source, whatever its mode, then exit.

    Exits 1 if anything failed; failures are retried on the next pass.
    """
    cfg = _load(config)
    setup_logging(cfg, verbose=verbose)
    try:
        with Agent(cfg, dry_run=dry_run) as agent:
            reports = asyncio.run(agent.run_once(source))
    except (AlreadyRunning, plugins.PluginError, KeyError) as exc:
        raise _fail(str(exc)) from exc
    raise typer.Exit(1 if any(report.failures for report in reports) else 0)


@app.command()
def watch(config: Path | None = CONFIG, verbose: bool = VERBOSE) -> None:
    """Run until stopped: stream sources watch, cron sources run on schedule."""
    cfg = _load(config)
    setup_logging(cfg, verbose=verbose)
    try:
        with Agent(cfg) as agent:
            asyncio.run(_watch(agent))
    except (AlreadyRunning, plugins.PluginError) as exc:
        raise _fail(str(exc)) from exc


async def _watch(agent: Agent) -> None:
    stop = asyncio.Event()
    loop = asyncio.get_running_loop()
    signals = [signal.SIGINT, signal.SIGTERM]
    if sys.platform == "win32":
        signals.append(signal.SIGBREAK)
    for sig in signals:
        try:
            loop.add_signal_handler(sig, stop.set)
        except (NotImplementedError, RuntimeError, ValueError):
            signal.signal(sig, lambda *_: loop.call_soon_threadsafe(stop.set))
    await agent.watch(stop)


def _journal(cfg: FerryConfig) -> Journal:
    if not cfg.journal_path.exists():
        raise _fail(f"no journal at {cfg.journal_path}: ferry has not run yet", 1)
    return Journal(cfg.journal_path)


@app.command()
def status(
    config: Path | None = CONFIG,
    source: list[str] | None = SOURCES,
    units: bool = typer.Option(False, "--units", help="List every unfinished unit."),
    as_json: bool = typer.Option(False, "--json", help="Print JSON."),
) -> None:
    """What the journal knows: units by state, backlog and failures."""
    cfg = _load(config)
    names = source or [s.name for s in cfg.sources]
    journal = _journal(cfg)
    try:
        result: dict[str, object] = {}
        for name in names:
            summary = journal.summary(name)
            summary["failing_files"] = [
                {
                    "unit": key,
                    "file": row.rel,
                    "attempts": row.attempts,
                    "error": row.last_error,
                }
                for key, row in journal.failing_files(name)
            ]
            summary["failing_deliveries"] = [
                {
                    "unit": key,
                    "registrar": row.registrar,
                    "event": row.event,
                    "attempts": row.attempts,
                    "error": row.last_error,
                }
                for key, row in journal.failing_deliveries(name)
            ]
            if units:
                summary["unfinished"] = [
                    {
                        "unit": u.key,
                        "state": u.state,
                        "note": u.note,
                        "error": u.last_error,
                    }
                    for u in journal.units(name)
                    if u.state not in (FINALIZED, "abandoned", "gone")
                ]
            result[name] = summary
    finally:
        journal.close()
    if as_json:
        typer.echo(json.dumps(result, indent=2))
        return
    for name, data in result.items():
        assert isinstance(data, dict)
        counts = ", ".join(f"{n} {state}" for state, n in data["units"].items() if n)
        typer.secho(name, bold=True)
        typer.echo(f"  units: {counts or 'none yet'}")
        typer.echo(
            f"  waiting to upload: {data['pending_files']} file(s), "
            f"{human_bytes(data['pending_bytes'])}"
        )
        typer.echo(f"  last pass: {data['last_pass'] or 'never'}")
        for item in data["failing_files"]:
            typer.secho(
                f"  failing: {item['file']} ({item['attempts']}x): {item['error']}",
                fg=typer.colors.RED,
            )
        for item in data["failing_deliveries"]:
            typer.secho(
                f"  failing: {item['event']} {item['unit']} with {item['registrar']} "
                f"({item['attempts']}x): {item['error']}",
                fg=typer.colors.RED,
            )
        for item in data.get("unfinished", []):
            note = f" -- {item['note']}" if item["note"] else ""
            typer.echo(f"  {item['state']:<12} {item['unit']}{note}")


@app.command()
def retry(
    config: Path | None = CONFIG,
    source: str | None = typer.Option(None, "--source", "-s"),
    unit: str | None = typer.Option(None, "--unit", "-u"),
) -> None:
    """Retry failures now, and re-open abandoned units, in the given scope."""
    if unit and not source:
        raise _fail("--unit needs --source")
    cfg = _load(config)
    journal = _journal(cfg)
    try:
        count = journal.retry(source, unit)
    finally:
        journal.close()
    typer.echo(f"{count} unit(s) will be retried on the next pass.")


@app.command()
def complete(source: str, unit: str, config: Path | None = CONFIG) -> None:
    """Treat an open unit as complete without waiting for it to go quiet."""
    cfg = _load(config)
    journal = _journal(cfg)
    try:
        row = journal.force_complete(source, unit)
    finally:
        journal.close()
    if row is None:
        raise _fail(f"{source} has no unit {unit!r}", 1)
    if row.state != "open":
        raise _fail(f"{unit} is {row.state}, not open", 1)
    typer.echo(f"{unit} will complete on the next pass, once its files have settled.")


@app.command()
def abandon(source: str, unit: str, config: Path | None = CONFIG) -> None:
    """Stop tracking a unit (`ferry retry --source S --unit U` resumes it)."""
    cfg = _load(config)
    journal = _journal(cfg)
    try:
        row = journal.abandon(source, unit)
    finally:
        journal.close()
    if row is None:
        raise _fail(f"{source} has no unit {unit!r}", 1)
    typer.echo(f"Abandoned {unit}; its files stay where they are.")


@app.command()
def schedule(
    config: Path | None = CONFIG,
    count: int = typer.Option(5, "--count", "-n", help="Passes to show per source."),
) -> None:
    """When each cron source runs next."""
    cfg = _load(config)
    for source in cfg.sources:
        if source.mode != "cron" or source.cron is None:
            typer.echo(f"{source.name}: streams continuously ({source.upload} uploads)")
            continue
        sched = CronSchedule(source.cron, source.timezone)
        zone = source.timezone or "local time"
        typer.echo(f"{source.name}: {source.cron!r} ({zone})")
        for when in sched.upcoming(count):
            typer.echo(f"  {when.isoformat(timespec='minutes')}")


@config_app.command("validate")
def validate(config: Path | None = CONFIG) -> None:
    """Parse the config and load every plugin it names (opening none)."""
    cfg = _load(config)
    try:
        for ref in cfg.notifiers:
            plugins.build(ref, plugins.NOTIFIERS, Notifier)
        for source in cfg.sources:
            for registrar in source.registrars:
                plugins.build(registrar, plugins.REGISTRARS, Registrar)
            if source.adapter is not None:
                plugins.build(source.adapter, plugins.ADAPTERS, Adapter)
    except plugins.PluginError as exc:
        raise _fail(str(exc)) from exc
    typer.echo(f"node {cfg.node}; state in {cfg.state_dir}")
    for source in cfg.sources:
        store = cfg.stores[source.store]
        target = store.url.rstrip("/") + (
            "/" + source.prefix.strip("/") if source.prefix else ""
        )
        when = f"cron {source.cron!r}" if source.mode == "cron" else "stream"
        typer.echo(
            f"  {source.name}: {source.root} -> {target} [{when}; upload per "
            f"{source.upload}; finalize {source.finalize.action}; registrars "
            f"{', '.join(r.key for r in source.registrars) or 'none'}]"
        )
    typer.echo("ok")


PLATFORM = typer.Option(
    None, "--platform", help="windows or systemd (default: this OS)."
)
NAME = typer.Option("ferry", "--name", help="Task / unit name.")
SYSTEM = typer.Option(
    False, "--system", help="Run as SYSTEM (Windows) or a system unit (Linux)."
)


@service_app.command("print")
def service_print(
    config: Path | None = CONFIG,
    platform: str | None = PLATFORM,
    name: str = NAME,
    system: bool = SYSTEM,
) -> None:
    """Print the service definition `install` would register."""
    path = _config_path(config)
    try:
        text = service.render(
            path,
            name=name,
            platform=platform or service.default_platform(),
            system=system,
        )
    except ValueError as exc:
        raise _fail(str(exc)) from exc
    typer.echo(text, nl=False)


@service_app.command("install")
def service_install(
    config: Path | None = CONFIG, name: str = NAME, system: bool = SYSTEM
) -> None:
    """Register `ferry watch` to start at boot and restart on failure."""
    path = _config_path(config)
    _load(path)
    try:
        typer.echo(service.install(path, name=name, system=system))
    except (RuntimeError, OSError) as exc:
        raise _fail(str(exc)) from exc


@service_app.command("uninstall")
def service_uninstall(name: str = NAME, system: bool = SYSTEM) -> None:
    """Remove the service `install` registered."""
    try:
        typer.echo(service.uninstall(name=name, system=system))
    except (RuntimeError, OSError) as exc:
        raise _fail(str(exc)) from exc


def main() -> None:
    app()
