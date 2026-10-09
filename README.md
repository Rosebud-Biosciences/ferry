# ferry

**An edge agent that ships instrument data from watched folders to
S3-compatible object storage -- AWS S3, Ceph RGW, SeaweedFS, MinIO -- on a cron
schedule or as it lands, and records every acquisition in your catalog.**

Lab instruments write acquisitions to a local disk. ferry runs on the
instrument PC (Windows, Linux or macOS), groups files into *units* (one
acquisition each), uploads them once they have finished being written,
registers each complete unit with pluggable *registrars* (a catalog database, a
JSON manifest, ...), and then keeps, moves or deletes the local copies. A local
SQLite journal makes every step resumable, and every action is logged locally
as JSON lines.

> Status: alpha (`0.1.0a1`). The import package and the command are `ferry`;
> the distribution is `ferry-ingest`.

## Install

```bash
uv tool install "ferry-ingest[aws]"   # or: pip install "ferry-ingest[aws]"
```

The `aws` extra adds boto3, for AWS shared-config profiles. Static keys and
environment credentials need nothing extra.

## Quickstart

```yaml
# ferry.yaml
state_dir: ./state
stores:
  ceph:
    url: s3://instrument-raw/
    endpoint: https://rgw.lab.internal
    region: default
    config:
      access_key_id: ${FERRY_S3_KEY}
      secret_access_key: ${FERRY_S3_SECRET}
sources:
  - name: cytation
    root: C:/Data/Cytation5
    store: ceph
    prefix: workcell/raw
    unit: {dirs: "*__*"}     # each matching top-level folder is one acquisition
    include: ["*.tif"]
    mode: cron
    cron: "0 2 * * *"
    upload: unit
    finalize: {action: move_to_backup, backup_root: F:/Cytation5Raw}
    registrars: [manifest]
```

```bash
ferry config validate          # parse it, load every plugin it names
ferry run --dry-run            # what a pass would upload, register and move
ferry run                      # one pass over every source, then exit
ferry watch                    # run until stopped (cron and stream sources)
ferry service install          # start `ferry watch` at boot, restart on failure
ferry status                   # units by state, backlog, failures
```

[`examples/ferry.example.yaml`](examples/ferry.example.yaml) annotates every
option.

## When files travel

Each source picks *when a pass runs* and *what a pass uploads*:

- `mode: cron` runs a pass on a cron expression (`cron: "0 2 * * *"`, read in
  `timezone:` or the node's local time) inside `ferry watch`. A pass missed
  while the PC slept runs once on wake.
- `mode: stream` watches continuously: OS file events (`watch: events`) are
  hints that trigger a rescan, backed by polling every `poll_interval` and a
  full walk every `full_scan_interval`. Use `watch: poll` for network shares,
  which deliver no reliable events.
- `upload: file` uploads each file as soon as it settles. `upload: unit` waits
  until the whole unit is complete, then uploads it.

Either way a unit is **registered only once it is complete**:

- A file has *settled* once its mtime is at least `settle` old. With
  `probe_exclusive: true` (Windows), a file another process holds open has not.
- A unit is *complete* once all its files have settled and either a `marker`
  file is present (`images/index.xml`, `run.done`, ...) or nothing in the unit
  has changed for `unit_idle` -- files ferry does not upload and directory
  mtimes included.
- `ferry complete SOURCE UNIT` ends the wait by hand; a unit open for
  `max_open` raises an alert.

`ferry run` ignores `mode` and does one pass over every source, for OS
schedulers and testing.

## What ferry guarantees

- A file is uploaded once it has settled and is recorded as uploaded only if it
  did not change while being read (it is stat'd before and after).
- A unit is registered once it is complete and every file is in the store --
  never with a partial file list. Files that change or appear later reopen the
  unit and register it again, with a higher `generation`.
- Source files are kept, moved or deleted only after every *required* registrar
  has confirmed the unit, and only if they still match what was uploaded.
- Every step is journaled, so a crash, reboot or killed process resumes where
  it stopped. Before a unit's first upload, ferry lists its prefix in the store
  and skips files already there with the same size, so a lost journal never
  means uploading everything again.

## Stores

`url: s3://bucket/prefix` uses obstore's S3 client: files up to
`single_put_max` (64 MiB) go up in one PUT, bigger ones stream as multipart
uploads (`part_size`, `part_concurrency`), and every file's SHA-256 is computed
on the way and recorded. `bandwidth` caps a source's rate, optionally per local
time window. A local directory (`url: /mnt/nas/raw`) also works.

For **Ceph RGW** (and other self-hosted S3):

- Set `endpoint`. Path-style addressing is the default, which is what RGW
  expects. Requests are signed for `us-east-1` unless you set `region`; set it
  to the zonegroup name if your RGW checks the region.
- Every request payload is signed (SigV4), so corruption in transit is
  rejected. `checksum: sha256` asks the server to verify a whole-object SHA-256
  as well. Only enable it where the server validates it: RGW does from
  Tentacle, and older releases accept the header without checking it.
- Give each node its own RGW user, with a bucket policy allowing `PutObject`,
  `GetObject`/`HeadObject` and `ListBucket` on its prefix. ferry never deletes
  from the store.
- Add a lifecycle rule that aborts incomplete multipart uploads after a day:
  a killed process leaves its in-flight upload behind.

## Registrars, adapters and notifiers

ferry's plugin points are plain Python classes, named in the config by a
built-in name, an installed entry point, or `module:attr`:

- **Registrars** (`ferry.registrars`) record complete units. Built in:
  `manifest` writes `<unit>/_ferry_manifest.json` (file list, sizes, SHA-256,
  metadata, backup paths), and `null` records nothing. Calls go through a
  durable outbox: a catalog that is down delays registration but not uploads,
  and nothing is finalized until every `required` registrar has confirmed.
- **Adapters** (`ferry.adapters`) add instrument metadata (plate, timestamps,
  wells, channels) to the manifest, read from folder and file names.
- **Notifiers** (`ferry.notifiers`) receive alerts: failures that keep
  recurring, units stuck open, and each pass's digest (as `info`, or `warning`
  if anything failed). The built-in `webhook` posts Slack-compatible JSON, by
  default for warnings and errors only.

```python
from ferry import Manifest, Registrar, TransferEvent


class CatalogRegistrar(Registrar):
    def __init__(self, url: str) -> None:
        super().__init__()
        self.url = url

    def register(self, manifest: Manifest, transfers: list[TransferEvent]) -> None:
        # Idempotent upsert keyed on manifest.uri; a unit that changed after it
        # was registered arrives again with a higher manifest.generation.
        ...

    def finalize(self, manifest: Manifest, transfers: list[TransferEvent]) -> None:
        # Source copies were kept, moved (see each file's backup_path) or deleted.
        ...
```

```toml
# your package's pyproject.toml
[project.entry-points."ferry.registrars"]
catalog = "my_catalog.ferry:CatalogRegistrar"
```

Delivery is at-least-once, so `register` and `finalize` must be idempotent.
Registrars also receive failed transfers (`record_failures`) and shipped log
files (`register_log`), so logs can be cataloged too.

## Operating it

- `ferry status [--units] [--json]` shows units by state, the upload backlog,
  and every failing file or delivery with its last error.
- `ferry retry [--source S] [--unit U]` clears retry backoff (and re-opens an
  abandoned unit). Failures otherwise retry on their own with exponential
  backoff (`retry.initial`, `retry.max`), and alert after `retry.alert_after`
  attempts.
- `ferry abandon SOURCE UNIT` stops tracking a unit; its files stay where they
  are.
- Logs go to the console and to `<state_dir>/logs/ferry-YYYY-MM-DD.jsonl`, one
  JSON object per line. With `logs.ship`, finished days are copied to the store
  and a heartbeat object is refreshed.
- `ferry service install` registers a Windows scheduled task (at boot, as the
  current user via S4U, restarting on failure; run it from an elevated prompt)
  or a systemd unit (`--system` for a system unit). `ferry service print` shows
  the definition without installing it.

## Development

```bash
uv sync --all-extras
uv run ruff format --check . && uv run ruff check .
uv run ty check
uv run pytest
```

The S3 tests run against an in-process moto server; set
`FERRY_TEST_S3_ENDPOINT` (plus `FERRY_TEST_S3_KEY` / `FERRY_TEST_S3_SECRET`) to
run them against another endpoint, as CI does with SeaweedFS.
[`docs/design.md`](docs/design.md) covers the architecture and the reasoning
behind it.

## License

Apache-2.0. See [LICENSE](LICENSE).
