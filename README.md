# ferry

[![CI](https://github.com/Rosebud-Biosciences/ferry/actions/workflows/ci.yml/badge.svg)](https://github.com/Rosebud-Biosciences/ferry/actions/workflows/ci.yml)
[![PyPI](https://img.shields.io/pypi/v/ferry-ingest)](https://pypi.org/project/ferry-ingest/)
[![License](https://img.shields.io/badge/license-Apache--2.0-blue)](https://github.com/Rosebud-Biosciences/ferry/blob/main/LICENSE)

**Get instrument data off the acquisition PC and into S3-compatible storage --
verified, cataloged, and only then cleaned up.**

ferry is one small binary that runs on the instrument PC (Windows, Linux or
macOS). It watches the folders your instruments write to, waits for each
acquisition to finish, uploads it to one or more stores -- AWS S3, Ceph RGW,
MinIO, SeaweedFS or a network share -- and verifies every copy. Then it tells
your catalog, and only then keeps, moves or deletes the originals.

> **Status: alpha.** `0.1.0-alpha.1` is the first release. Until 1.0, a minor
> release may change behavior; the
> [changelog](https://github.com/Rosebud-Biosciences/ferry/blob/main/CHANGELOG.md)
> says how. Bug reports and feedback are very welcome.

## Why ferry

- **Nothing is deleted on trust:** originals go only once every copy is
  verified and your catalog has confirmed the acquisition.
- **Whole acquisitions:** each is registered once complete, with its full
  file list.
- **One read, many destinations:** each file is read once and streamed to
  every store.
- **Resumable:** a local journal carries every step through crashes, reboots
  and outages.
- **Plugs into your stack:** hooks are plain commands that get JSON on stdin,
  to record acquisitions in a catalog, start an analysis, or post receipts to
  Slack.
- **Made for instrument PCs:** a native Windows service, network shares,
  bandwidth limits by time of day, and nothing else to install.

ferry is for write-once data: acquisitions that land, settle and are left
alone. It is not a backup or sync tool, and it never deletes anything from a
store.

## Install

```bash
uv tool install ferry-ingest    # or pipx / pip install ferry-ingest: a prebuilt binary
```

- **Standalone binaries** for Linux, Windows and macOS, on x86-64 and ARM64,
  come with every [release](https://github.com/Rosebud-Biosciences/ferry/releases),
  along with `SHA256SUMS`. `gh attestation verify FILE -R
  Rosebud-Biosciences/ferry` confirms a file was built by this repository.
- **From source** (Rust 1.89 or later):
  `cargo install --locked --git https://github.com/Rosebud-Biosciences/ferry`

The package is `ferry-ingest`; the command it installs is `ferry`.

## Quickstart

**1. Describe the folder and the store** in `ferry.yaml`:

```yaml
state_dir: ./state                      # ferry's journal, logs and lock
stores:
  lab:
    url: s3://instrument-raw/
    endpoint: https://rgw.lab.internal  # Ceph RGW, MinIO, SeaweedFS; leave out for AWS
    region: default
    options:
      aws_access_key_id: ${FERRY_S3_KEY}
      aws_secret_access_key: ${FERRY_S3_SECRET}
pipelines:
  - name: cytation
    root: C:/Data/Cytation5             # where the instrument writes
    destinations: [lab]
    unit: {dirs: "*"}                   # each top-level folder is one acquisition
    registrars: [manifest]              # write a manifest next to the data
    # finalize: {action: delete, after: 1d}   # once you trust it: free the disk
```

`${FERRY_S3_KEY}` and the like are read from the environment. To try ferry
without S3, point the store at a folder instead (`url: ./bucket`).

**2. Check it**, and see what a pass would do without changing anything:

```bash
ferry config validate    # parse the config; show where each pipeline sends data
ferry doctor --write     # can this PC reach its stores and find its hooks?
ferry run --dry-run      # what a pass would upload, register and finalize
```

**3. Run it:**

```bash
ferry watch              # until stopped
ferry service install    # or run it as a service
ferry status             # what has moved, what is waiting, what failed
```

Every option is annotated in
[`examples/ferry.example.yaml`](https://github.com/Rosebud-Biosciences/ferry/blob/main/examples/ferry.example.yaml).

## How it works

1. **Watch.** Each *pipeline* watches a folder -- continuously, or on a cron
   schedule -- and groups its files into *units*, one per acquisition: a
   folder each (`unit.dirs`), or files grouped by a regex over their paths
   (`unit.regex`).
2. **Wait.** A file has *settled* once it has not changed for `settle` (60s).
   A unit is *complete* once its files have settled and a `marker` file has
   appeared, or nothing in it has changed for `unit_idle` (10m).
3. **Upload and verify.** Each file is read once and sent to every
   *destination*, then checked there: by size, or with `verify: sha256` by
   reading it back.
4. **Register.** Once every required destination holds the whole unit, its
   *registrars* get a JSON event listing every file with its size, SHA-256
   and copies: your catalog, the built-in manifest, or any command.
5. **Finalize.** Once *every* destination holds a verified copy and every
   required registrar has confirmed, the originals are kept, moved to a
   backup drive, or deleted, after an optional grace period.

Every step is recorded in a local SQLite journal, so ferry resumes where it
stopped. [The design notes](https://github.com/Rosebud-Biosciences/ferry/blob/main/docs/design.md)
cover each step in depth.

## What ferry guarantees

- **Verified, or it does not count.** A copy counts only once verified, and
  only if the file did not change while it was read. ferry tells versions of
  a file apart by size and modification time.
- **Never a partial registration.** Files that change or arrive later reopen
  the unit, which registers again with a higher `generation`.
- **Only what it could read.** A folder it cannot list, a share that drops
  out, a name that is not valid Unicode or a symbolic link (never followed)
  holds the unit back, with an alert. Nothing ferry could not see counts as
  gone.
- **Checked again before a move or delete.** ferry rechecks every copy, sets
  each file aside by a rename so a late writer cannot change it, and confirms
  it is the file it uploaded, hashing it again before a delete
  ([the protocol](https://github.com/Rosebud-Biosciences/ferry/blob/main/docs/design.md#deleting-the-source)).
- **Copies follow their destination.** Point a destination at a new bucket,
  endpoint or prefix, and ferry stops until you run `ferry destination
  accept` or `reset`.
- **A lost journal costs a re-upload, never data.** With `adopt: sha256`,
  ferry instead reads back what a destination already holds and keeps each
  object whose hash matches.

## Configuration

### When files travel

| Setting | Default | Alternative |
| --- | --- | --- |
| `mode` | `stream`: watch continuously | `cron`: run on a schedule, e.g. `cron: "0 2 * * *"` in `timezone:`; a pass missed while the PC slept runs on wake |
| `upload` | `unit`: once the whole unit is complete | `file`: each file as soon as it settles |
| `watch` | `events`: OS file events, backed by polling | `poll`: polling only, for network shares |
| `fanout` | `shared`: one read feeds every destination | `separate`: each destination reads for itself |
| `finalize.action` | `keep` | `move` (to `backup_root`) or `delete`; `after:` adds a grace period |

### Destinations and stores

A destination is a store plus an optional `prefix`; files land at
`<prefix>/<unit>/<path within the unit>`. Each verifies by `size` (the
default) or `sha256`, and is `required` unless marked otherwise. An optional
destination catches up later without holding registration back. No two
destinations may share a prefix.

- **S3:** `url: s3://bucket/prefix`, with `endpoint` for anything but AWS.
  Credentials come from `options`, a `profile` or the `AWS_*` variables.
  `bandwidth: 50MB` caps the rate, always or in a window
  (`{"08:00-18:00": 50MB}`).
- **Directories:** `url: /mnt/nas/raw`, or a UNC path.

For **Ceph RGW** and other self-hosted S3:

- Path-style addressing, which RGW expects, is the default. Set `region` to
  the zonegroup if RGW checks it.
- Give each PC its own user, allowed `PutObject`, `GetObject` and
  `ListBucket` on its prefixes. ferry never deletes from a store.
- Add a lifecycle rule that aborts incomplete multipart uploads after a day.
- Enable `checksum: sha256` only where the server validates it (RGW does from
  Tentacle).

## Hooks: telling the rest of your stack

**Registrars** record complete units. A registrar is any command: ferry writes
a JSON event to its stdin (`register`, `finalize` or `failures`), and exit
code 0 means it was handled. Events wait in a durable outbox, so a catalog
that is down delays registration but never uploads. Delivery is
at-least-once and in order per unit, and each event carries an `id` for
dropping repeats. The built-in `manifest` registrar writes
`<unit>/_ferry_manifest.json` -- every file's size, SHA-256 and copies -- next
to the data.

**Notifiers** send alerts -- failures that keep recurring, units stuck open,
a digest per pass -- and, with `events: [receipt]`, one receipt per delivered
unit. A `webhook` posts Slack-compatible text; an `exec` notifier gets the
event JSON. A receipt reads:

```text
acquisition:
• plateA__1 (384 files, 512.3 GB)
copied to:
• s3://instrument-raw/workcell/raw/plateA__1 (size verified)
• s3://lab-archive/cytation5/plateA__1 (SHA-256 verified)
registered with catalog · original moved to F:\Cytation5Raw\plateA__1
workcell1-pc · workcell1_cytation · delivered 2h after its first file
```

[`docs/hooks.md`](https://github.com/Rosebud-Biosciences/ferry/blob/main/docs/hooks.md)
specifies every event, shows an example hook, and explains how to start
downstream work as soon as a unit lands.

## Operating ferry

### Commands

| Command | What it does |
| --- | --- |
| `ferry status [--units] [--json]` | Units by state, the upload backlog, and each failure with its last error |
| `ferry doctor [--write] [--notify]` | Checks the config, directories, journal, roots, stores and hooks; exits 1 if anything would stop ferry |
| `ferry run [--dry-run]` | One pass over every pipeline, then exit -- for OS schedulers and testing |
| `ferry retry [-p PIPELINE] [-u UNIT]` | Retries failures now (they also retry on their own, with backoff) and reopens abandoned units |
| `ferry complete PIPELINE UNIT` | Treats an open unit as complete without waiting for it to go quiet |
| `ferry abandon PIPELINE UNIT` | Stops tracking a unit; its files stay where they are |
| `ferry destination accept\|reset PIPELINE DESTINATION` | Settles a destination that now points somewhere new: its copies still count, or go up again |
| `ferry outbox discard PIPELINE REGISTRAR` | Drops the queued events of a registrar that was renamed or removed |
| `ferry schedule` | When each cron pipeline runs next |
| `ferry schema event\|manifest` | Prints the JSON Schema of what hooks receive |

### Running as a service

`ferry service install` hands `ferry watch` to the OS's service manager, which
restarts it if it fails; `ferry service print` shows the definition first.

- **Windows:** a native service, started at boot. Install it as
  administrator; `--account` runs it as a user that can reach network shares.
- **Linux:** a systemd user unit, or with `--system` a system unit.
- **macOS:** a launchd agent that starts at login, or with `--system` (and
  sudo) a daemon that starts at boot.

### Secrets

Keep credentials and webhook URLs out of the config, in an env file of
`KEY=VALUE` lines that only the service's account can read: `chmod 600`, or
on Windows `icacls FILE /inheritance:r /grant:r SYSTEM:R Administrators:F`
(plus the `--account`, if you set one). Pass it as `--env-file PATH`. ferry
loads it before the config, and `ferry --env-file PATH service install`
makes the service load it too.

### Monitoring

- **Logs:** stderr, and `<state_dir>/logs/ferry.YYYY-MM-DD.jsonl` as JSON
  lines; `FERRY_LOG=debug` adds detail.
- **Metrics:** `metrics: {listen: 127.0.0.1:9187}` serves Prometheus
  `/metrics` and `/healthz`, without authentication: keep it local.
- **Heartbeat:** for a PC nothing can scrape, `logs.ship` copies finished
  logs, and a summary of every pipeline, to a store.

## Documentation

- [Annotated configuration](https://github.com/Rosebud-Biosciences/ferry/blob/main/examples/ferry.example.yaml)
  -- every option
- [Hooks](https://github.com/Rosebud-Biosciences/ferry/blob/main/docs/hooks.md)
  -- events, delivery, receipts and downstream work
- [Design](https://github.com/Rosebud-Biosciences/ferry/blob/main/docs/design.md)
  -- how ferry works, and why
- [Changelog](https://github.com/Rosebud-Biosciences/ferry/blob/main/CHANGELOG.md)

## Compatibility

ferry follows semantic versioning; until 1.0, a minor release may break
things, and the changelog says how. Releases keep these stable:

- **The command line:** commands, flags and exit codes: `0` success, `1`
  failures that will be retried (or an operator command that could not act),
  and `2` an error such as a config that does not load.
- **The config format,** `version: 1`. A config written for a newer format
  is refused, never misread.
- **What hooks receive:** `ferry.event/v1` and `ferry.manifest/v1`
  ([JSON Schemas](https://github.com/Rosebud-Biosciences/ferry/tree/main/schemas)).
  Fields and event kinds may be added, so ignore what you do not recognize.
- **The journal:** a newer ferry upgrades it in place; an older one refuses
  it afterwards.

The Rust library is internal, not an API, and is not published to crates.io.

## Contributing

Contributions are welcome.
[CONTRIBUTING.md](https://github.com/Rosebud-Biosciences/ferry/blob/main/CONTRIBUTING.md)
covers building, testing and what a change needs, and everyone taking part
follows the
[Code of Conduct](https://github.com/Rosebud-Biosciences/ferry/blob/main/CODE_OF_CONDUCT.md).
Please report security problems privately, as
[SECURITY.md](https://github.com/Rosebud-Biosciences/ferry/blob/main/SECURITY.md)
describes.

## License

[Apache-2.0](https://github.com/Rosebud-Biosciences/ferry/blob/main/LICENSE).
