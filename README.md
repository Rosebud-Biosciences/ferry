# ferry

**An edge agent that ships instrument data from watched folders to one or more
S3-compatible stores -- AWS S3, Ceph RGW, SeaweedFS, MinIO -- on a cron
schedule or as it lands, and tells your catalog about every acquisition.**

Lab instruments write acquisitions to a local disk. ferry runs on the
instrument PC (Windows, Linux or macOS) as one small binary. Each *pipeline*
watches a folder, groups its files into *units* (one acquisition each), uploads
them once they have finished being written -- to every destination the
pipeline names, from a single read -- verifies the copies, hands each complete
unit to your *registrars* (any command; the event arrives as JSON on stdin),
and then keeps, moves or deletes the local copies. A local SQLite journal makes
every step resumable, and every action is logged locally as JSON lines.

ferry is for write-once data: acquisitions that land, settle and are then left
alone -- from instruments, sensors, cameras and the like. It is not a backup,
sync or mirror tool for folders whose files keep changing, and it never
deletes from a store.

> Status: alpha (`0.1.0-alpha.1`). The command is `ferry`; the crate and the
> PyPI distribution are `ferry-ingest`.

## Install

```bash
uv tool install ferry-ingest          # or: pip install ferry-ingest (a prebuilt binary wheel)
cargo install --locked --git https://github.com/Rosebud-Biosciences/ferry   # or build it from source
```

## Quickstart

```yaml
# ferry.yaml
state_dir: ./state
stores:
  ceph:
    url: s3://instrument-raw/
    endpoint: https://rgw.lab.internal
    region: default
    options:
      aws_access_key_id: ${FERRY_S3_KEY}
      aws_secret_access_key: ${FERRY_S3_SECRET}
  archive:
    url: s3://lab-archive/
    region: us-west-2
pipelines:
  - name: cytation
    root: C:/Data/Cytation5
    prefix: workcell/raw
    destinations: [ceph, {store: archive, verify: sha256}]
    unit: {dirs: "*__*"}     # each matching top-level folder is one acquisition
    include: ["*.tif"]
    mode: cron
    cron: "0 2 * * *"
    finalize: {action: delete, after: 1d}
    registrars: [manifest]
```

```bash
ferry config validate          # parse and check it; show where data goes
ferry doctor --write           # can this node reach its stores and hooks?
ferry run --dry-run            # what a pass would upload, register and finalize
ferry run                      # one pass over every pipeline, then exit
ferry watch                    # run until stopped (cron and stream pipelines)
ferry service install          # start `ferry watch` at boot, restart on failure
ferry status                   # units by state, backlog, failures
```

[`examples/ferry.example.yaml`](examples/ferry.example.yaml) annotates every
option.

## Pipelines and destinations

A config has any number of `pipelines`, each with its own `root`, schedule and
rules; their roots must not overlap. Each pipeline sends its files to one or
more `destinations` -- stores, each with an optional `prefix`, and:

- `verify: size` (default): a HEAD after the upload confirms the stored size.
  `verify: sha256` also reads the object back and compares its SHA-256 with the
  one computed while uploading.
- `required: true` (default): registration waits for this destination. An
  optional destination is retried until it has the files, but a unit is
  registered without it.

With `fanout: shared` (default) a file is read once and every chunk goes to
every destination that needs it: the chunks are reference-counted, so a second
destination costs no extra disk reads or copies. A destination that fails
mid-file drops out and is retried later with its own read; the slowest
destination paces the rest. `fanout: separate` gives every destination its own
read instead.

## When files travel

Each pipeline picks *when a pass runs* and *what a pass uploads*:

- `mode: cron` runs a pass on a cron expression (`cron: "0 2 * * *"`, read in
  `timezone:` or the node's local time) inside `ferry watch`. A pass missed
  while the PC slept runs once on wake.
- `mode: stream` watches continuously: OS file events (`watch: events`) are
  hints that trigger a rescan, backed by polling every `poll_interval` and a
  full walk every `full_scan_interval`. Use `watch: poll` for network shares,
  which deliver no reliable events.
- `upload: file` uploads each file as soon as it settles. `upload: unit`
  (default) waits until the whole unit is complete, then uploads it.

Either way a unit is **registered only once it is complete**:

- A file has *settled* once its mtime is at least `settle` old. With
  `probe_exclusive: true` (Windows), a file another process holds open has not.
- A unit is *complete* once all its files have settled and either a `marker`
  file is present (`images/index.xml`, `run.done`, ...) or nothing in the unit
  has changed for `unit_idle` -- files ferry does not upload and directory
  mtimes included.
- `ferry complete PIPELINE UNIT` ends the wait by hand; a unit open for
  `max_open` raises an alert.

`ferry run` ignores `mode` and does one pass over every pipeline, for OS
schedulers and testing.

## What ferry guarantees

- A copy counts only if the file did not change while it was read (it is
  stat'd before and after) and the stored object verified. A file whose size
  or mtime changes later is sent again. ferry tells versions of a file apart
  by size and mtime; a rewrite that keeps both is caught before a delete,
  which hashes the file first, and not otherwise.
- ferry acts only on what it could read. If a scan cannot read part of a unit
  -- a permission, a share that drops, a name that is not valid Unicode, a
  symbolic link (links are never followed) -- the unit waits: it does not
  complete, register or finalize, and nothing the scan missed counts as gone.
  An alert says what could not be read; `exclude` it if it should not travel.
- A unit is registered once it is complete and every *required* destination
  holds a verified copy of every file -- never with a partial file list. Files
  that change or appear later reopen the unit and register it again, with a
  higher `generation`.
- Source files are kept, moved or deleted (`finalize.action: keep | move |
  delete`) only once **every** destination holds a verified copy, every
  required registrar has confirmed the unit, and the `finalize.after` grace
  period (if any) has passed. Before a move or delete, ferry then:
  - checks that every copy is still there with the size sent and the ETag
    recorded when it was verified; one that went missing or was overwritten is
    sent again first;
  - sets each file aside by renaming it (`name.ferry-finalizing`), so a writer
    that comes back makes a new file instead of changing the bytes in flight;
  - checks the file set aside is the one uploaded -- the same size and mtime,
    and before a delete the same SHA-256 -- and otherwise puts it back and
    sends it again;
  - moves it (hashing it on the way if the backup is on another file system)
    or deletes it, and records that only while the journal still describes
    those bytes.
- Copies are tied to where a destination points: its bucket, endpoint and
  prefix, or its directory. Change that, and ferry stops until you say what
  happened (see `ferry destination` below). Strengthening `verify` from `size`
  to `sha256` sends again what the weaker check covered.
- Every step is journaled, so a crash, reboot or killed process resumes where
  it stopped -- a move or delete interrupted after the rename included.
- A lost journal means uploading again, unless the pipeline sets `adopt`.
  `adopt: sha256` reads back each object already at a destination and counts
  it if it hashes like the local file. `adopt: size` counts any object with
  the right key and size, so it is allowed only with `finalize.action: keep`.

## Stores

`url: s3://bucket/prefix` uses the `object_store` crate's S3 client: files up
to `limits.single_put_max` (64 MiB) go up in one PUT, bigger ones stream as
multipart uploads (`part_size`, `part_concurrency`), and every file's SHA-256 is
computed on the way and recorded. `options` passes any `object_store` S3
setting by name; credentials otherwise come from `profile` (static keys in
`~/.aws/credentials`) or the usual `AWS_*` environment variables.
`bandwidth: 50MB` caps a store's rate, optionally per local time window
(`{"08:00-18:00": 50MB}`). A local or mounted directory (`url: /mnt/nas/raw`)
also works.

A file goes to `<prefix>/<unit>/<path within the unit>`, its names as the file
system gives them (no Unicode normalization), each percent-encoded only where a
key could not hold it faithfully: `%`, `\` and control characters. So `a\b.tif`
(a legal name on Linux) and `a/b.tif` stay distinct. No two destinations, and
not log shipping, may write under the same prefix of one bucket or directory.

For **Ceph RGW** (and other self-hosted S3):

- Set `endpoint`. Path-style addressing is the default, which is what RGW
  expects. Set `region` to the zonegroup name if your RGW checks it.
- Every request payload is signed (SigV4), so corruption in transit is
  rejected. `checksum: sha256` asks the server to verify a whole-object
  checksum as well; only enable it where the server validates it (RGW does
  from Tentacle). `verify: sha256` checks end to end regardless.
- Give each node its own RGW user, with a bucket policy allowing `PutObject`,
  `GetObject`/`HeadObject` and `ListBucket` on its prefixes. ferry never
  deletes from a store.
- Add a lifecycle rule that aborts incomplete multipart uploads after a day:
  a killed process leaves its in-flight upload behind.

## Registrars and notifiers

**Registrars** record complete units. An `exec` registrar is any command:
ferry writes a versioned JSON event (`register`, `finalize`, `failures`) to its
stdin, and exit code 0 means it was handled. Events go through a durable outbox
-- a catalog that is down delays registration but not uploads, delivery is
at-least-once and in order per unit, and nothing is finalized until every
`required` registrar has confirmed. The built-in `manifest` registrar writes
`<unit>/_ferry_manifest.json` (every file's size, SHA-256 and copies) next to
the data.

**Notifiers** receive alerts: failures that keep recurring, units stuck open,
and each pass's digest (as `info`, or `warning` if anything failed). With
`events: [receipt]`, a notifier gets one receipt per unit once it is delivered
instead: where each copy is, how it was verified, and whether the source files
were kept, moved or deleted. Receipts wait in the outbox until they are taken.
`webhook: URL` posts Slack-compatible text (or, with `format: event`, the event
JSON); `exec:` runs a command with the event on stdin.

[`docs/hooks.md`](docs/hooks.md) specifies the events, with an example hook.

## Operating it

- `ferry doctor` checks that a node can run -- its config, state and log
  directories, the journal (a destination that moved, events waiting for a
  registrar that is gone), each root, every store and every hook's program --
  and exits 1 on anything that would stop ferry. `--write` also puts a probe
  object in each store (`_ferry/doctor/<node>`) and reads it back; `--notify`
  sends each notifier a test message.
- `ferry status [--units] [--json]` shows units by state, the upload backlog,
  and every failing copy or delivery with its last error.
- `metrics: {listen: 127.0.0.1:9187}` (or `ferry watch --metrics ADDR`) serves
  the same numbers to Prometheus at `/metrics` -- units by state, the backlog,
  failing copies and deliveries, each pipeline's last pass, and transfers and
  bytes by destination and outcome -- and `/healthz`, which answers 200 while
  the journal does. Neither asks for credentials: keep them on localhost or a
  management network. A node nothing can scrape can push a heartbeat instead
  (`logs.ship.heartbeat`).
- `ferry retry [-p PIPELINE] [-u UNIT]` clears retry backoff (and re-opens an
  abandoned unit). Failures otherwise retry on their own with exponential
  backoff (`retry.initial`, `retry.max`), and alert after `retry.alert_after`
  attempts.
- `ferry abandon PIPELINE UNIT` stops tracking a unit; its files stay where
  they are.
- `ferry destination accept PIPELINE DESTINATION` -- a destination's new
  settings reach the same objects, so its copies keep counting -- or `ferry
  destination reset PIPELINE DESTINATION` -- they do not, so every file not yet
  finalized goes up again. ferry refuses to run until one is chosen; both take
  `--dry-run`.
- `ferry outbox discard PIPELINE REGISTRAR [--dry-run]` drops the undelivered
  events of a registrar that was renamed or removed. ferry refuses to run while
  any wait for one, rather than finalizing units it never registered.
- Logs go to stderr and to `<state_dir>/logs/ferry.YYYY-MM-DD.jsonl` (UTC
  days), one JSON object per line; `FERRY_LOG=debug` (any `tracing` filter)
  overrides `logs.level`. With `logs.ship`, finished days are copied to a store,
  `logs.ship.registrars` are told about each (and told again on the next round
  if they failed), and a heartbeat object with every pipeline's summary is
  refreshed.
- `ferry service install` registers a native Windows service (delayed
  automatic start, restarted on failure; run it as administrator, and pass
  `--account` to run as a user that can reach network shares), a systemd unit
  (`--system` for a system unit), or a launchd job on macOS (`--system`, with
  sudo, for a daemon that starts at boot; otherwise an agent that starts when
  you log in). `--account` runs a `--system` service as that user. `ferry
  service print` shows the definition without installing it.
- Keep a service's secrets in an env file, not in the config or the machine's
  environment: `KEY=VALUE` lines that `--env-file PATH` (or `FERRY_ENV_FILE`)
  sets before the config loads, so its `${VAR}`s, the `AWS_*` variables and
  hooks all see them. `ferry --env-file PATH service install` makes the
  service read it too. Only the account the service runs as should be able to
  read the file:
  - Linux and macOS: `chmod 600 ferry.env`, owned by that account (root for a
    system service); ferry warns about a file others can read.
  - Windows: `icacls C:\ProgramData\ferry\ferry.env /inheritance:r /grant:r
    SYSTEM:R Administrators:F` (and grant the `--account`, if one is set).

## Compatibility

Until 1.0, a minor release (`0.x`) may break any of the following, and the
[changelog](CHANGELOG.md) says how; from 1.0, only a major release does. What
releases keep stable:

- The command line: commands, flags and exit codes (0 success; 1 a pass with
  failures, which are retried, or an operator command that could not act; 2
  an error, such as a config that does not load).
- The config format, version 1 (`version: 1`, optional). A config written
  for a newer format is refused, never misread.
- What hooks receive: events (`ferry.event/v1`) and manifests
  (`ferry.manifest/v1`), with JSON Schemas in [`schemas/`](schemas). Fields
  and event kinds are added without changing `schema`; a change that would
  break a consumer that ignores what it does not know changes it.
- The journal: a newer ferry upgrades it in place the first time it opens
  it, and an older ferry refuses a journal a newer one upgraded.

The Rust library is ferry's internals, not an API, and is not published to
crates.io.

## Development

```bash
cargo fmt --check && cargo clippy --all-targets -- -D warnings
cargo test
```

`FERRY_BLESS=1 cargo test --lib schemas` regenerates `schemas/` after a change
to what hooks receive.

The S3 tests run when `FERRY_TEST_S3_ENDPOINT` (plus `FERRY_TEST_S3_KEY`,
`FERRY_TEST_S3_SECRET` and an existing `FERRY_TEST_S3_BUCKET`) points at an S3
API, as CI does with SeaweedFS. [`docs/design.md`](docs/design.md) covers the
architecture and the reasoning behind it.

## License

Apache-2.0. See [LICENSE](LICENSE).
