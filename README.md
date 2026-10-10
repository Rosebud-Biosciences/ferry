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
  stat'd before and after) and the stored object verified. A file that changes
  later -- even rewritten in place at the same size -- is sent again.
- A unit is registered once it is complete and every *required* destination
  holds a verified copy of every file -- never with a partial file list. Files
  that change or appear later reopen the unit and register it again, with a
  higher `generation`.
- Source files are kept, moved or deleted (`finalize.action: keep | move |
  delete`) only once **every** destination holds a verified copy, every
  required registrar has confirmed the unit, the `finalize.after` grace period
  (if any) has passed, and the files still match what was uploaded.
- Every step is journaled, so a crash, reboot or killed process resumes where
  it stopped. Before a unit's first upload to a size-verified destination,
  ferry lists its prefix there and adopts files already present with the same
  size, so a lost journal never means uploading everything again.
  (`verify: sha256` destinations never adopt: an object ferry did not hash is
  sent again.)

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
and each pass's digest (as `info`, or `warning` if anything failed).
`webhook: URL` posts Slack-compatible JSON; `exec:` runs a command with the
alert event on stdin.

[`docs/hooks.md`](docs/hooks.md) specifies the events, with an example hook.

## Operating it

- `ferry status [--units] [--json]` shows units by state, the upload backlog,
  and every failing copy or delivery with its last error.
- `ferry retry [-p PIPELINE] [-u UNIT]` clears retry backoff (and re-opens an
  abandoned unit). Failures otherwise retry on their own with exponential
  backoff (`retry.initial`, `retry.max`), and alert after `retry.alert_after`
  attempts.
- `ferry abandon PIPELINE UNIT` stops tracking a unit; its files stay where
  they are.
- Logs go to stderr and to `<state_dir>/logs/ferry.YYYY-MM-DD.jsonl` (UTC
  days), one JSON object per line; `FERRY_LOG=debug` (any `tracing` filter)
  overrides `logs.level`. With `logs.ship`, finished days are copied to a store,
  `logs.ship.registrars` are told about each, and a heartbeat object with every
  pipeline's summary is refreshed.
- `ferry service install` registers a native Windows service (delayed
  automatic start, restarted on failure; run it as administrator, and pass
  `--account` to run as a user that can reach network shares) or a systemd unit
  (`--system` for a system unit). `ferry service print` shows the definition
  without installing it.

## Development

```bash
cargo fmt --check && cargo clippy --all-targets -- -D warnings
cargo test
```

The S3 tests run when `FERRY_TEST_S3_ENDPOINT` (plus `FERRY_TEST_S3_KEY`,
`FERRY_TEST_S3_SECRET` and an existing `FERRY_TEST_S3_BUCKET`) points at an S3
API, as CI does with SeaweedFS. [`docs/design.md`](docs/design.md) covers the
architecture and the reasoning behind it.

## License

Apache-2.0. See [LICENSE](LICENSE).
