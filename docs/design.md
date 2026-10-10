# ferry design

## The problem

Instruments write acquisitions to the disk of the PC they are attached to,
usually Windows, often with vendor software that holds files open for minutes
and writes whole folders over hours. The data has to reach object storage (AWS
S3, an on-premises Ceph cluster, or both), be recorded in a catalog that
downstream pipelines read, and then leave the instrument's disk -- moved to a
backup drive or deleted -- without ever losing the only copy.

Required: a nightly-batch (cron) or streaming trigger per folder; several
folders per PC, each with its own destinations; uploads that never send a
half-written file or register a half-written acquisition; deleting the source
only once every copy is verified; local logs; and a seam for registering data
(and logs) in a catalog.

## Existing tools

None covers the whole problem:

- **Arcadia Science Data Hub watcher** (MIT, beta): the closest whole system.
  It is a Windows-service watcher with stability checks and run grouping, but
  it uploads through presigned URLs from its own API to AWS, into its own
  catalog. It has no nightly mode and no backup-drive leg. Its stability period
  and run grouping are the model for ferry's `settle` and units.
- **rclone** (`move --min-age` on a timer): fast and Ceph-aware, with bandwidth
  timetables. It has no watch mode, one destination per invocation, and no hook
  for registering what it moved.
- **Apache MiNiFi C++**, **iRODS automated ingest**: capable dataflow and
  data-management systems, but heavy to operate on an instrument PC, and each
  brings its own catalog model.
- **MinIO `mc mirror --watch`**: the community edition was archived in
  February 2026.
- **Dagster** sensors poll from a central daemon that cannot see an
  instrument's local disk. Downstream orchestration stays where it is, reading
  what ferry registers.

## Why Rust, and one binary

An agent that runs unattended for months on lab PCs should be one file with no
runtime to install or break, small and steady in memory, and fast enough to
hash and upload at disk speed while the instrument keeps writing. ferry is a
single Rust binary built from well-trodden crates: `object_store` (the I/O
layer under obstore, Arrow and Delta) for S3 and local stores, `notify` for
file events, `croner` for cron arithmetic, `rusqlite` (bundled SQLite) for the
journal, `aws-lc-rs` for SHA-256, `tokio` for concurrency, and `windows-service`
for a native Windows service. The same binary is published as a Python wheel
(maturin `bindings = "bin"`) so `pip`/`uv` users install it without a Rust
toolchain.

## Architecture

```mermaid
flowchart LR
  subgraph node [Instrument PC]
    triggers["cron pass / stream rescans"] --> scan["scan: units, files, mtimes"]
    scan --> journal[("SQLite journal")]
    journal --> complete["complete: settled + marker or idle"]
    complete --> upload["upload: one read, fanned out; SHA-256; verify"]
    upload --> journal
    journal --> outbox["outbox: register / finalize"]
    outbox --> finalize["finalize: keep / move / delete"]
  end
  upload --> storeA[("S3 / Ceph RGW")]
  upload --> storeB[("second store")]
  outbox --> registrars["registrars: exec hooks, manifest"]
```

One `Engine` per pipeline owns its steps, and two drivers run it.
`run_pass` does every step once (`mode: cron`, `ferry run`). The stream loop
(`mode: stream`) runs them continuously, with uploads and finalizations as
background tasks. Every step reads and writes the journal, and the journal is
the only state, so a driver can be killed between any two steps. All pipelines
share one journal and one process; each pipeline's loop is supervised and
restarted if it fails.

## Units, settling, completion

A **unit** is one acquisition: a directory at a fixed depth (`unit.dirs`, a
glob per level), or the files whose path gives the same `unit` group of a regex
(`unit.regex`, for instruments that write flat folders). A file's path within
its unit is kept in the store: `<prefix>/<unit>/<within>`.

A file has **settled** when its mtime is at least `settle` old: the same rule
as rclone's `--min-age`. A writer that preserves an old mtime (a copy tool,
say) is caught by the before/after stat around every upload. On Windows,
`probe_exclusive` also treats a file another process holds open (a sharing
violation on an exclusive open) as unsettled.

A unit is **complete** when every one of its files has settled and either its
`marker` is present or nothing in it has changed for `unit_idle`. "Changed"
means the newest mtime of anything in the unit: files that are not uploaded
count, and so do directories, whose mtime moves when entries are added, removed
or renamed. A completed unit gets a new `generation`. Any later change to it --
a new file, a rewritten one -- reopens it, so it completes, uploads and
registers again with a higher generation, never with a partial file list.

## The journal

```text
unit:   open -> complete -> registering -> registered -> finalized
           ^------ any change reopens ---------------------------'
        abandoned (operator), gone (vanished before any upload)
file:   pending -> uploaded -> finalized        pending -> gone
copy:   one row per (file, destination): pending | done, with the size and
        mtime of the bytes that were sent
outbox: (unit, generation, registrar, event) rows, delivered in order
```

- A **copy** is *current* only while its recorded size and mtime equal the
  file's: one SQL predicate decides it everywhere. A file is `uploaded` once
  every destination holds a current copy, so a file that changes is pending
  again for every destination without any other bookkeeping. A destination
  added to the config gets copy rows for everything not yet finalized.
- A unit moves from `complete` to `registering` when every *required*
  destination holds a current copy of every file. The manifest and the
  matching transfer events are frozen into the outbox payload in the same
  transaction, as a compare-and-set on the unit's state and generation.
- `registering` becomes `registered` when every *required* registrar has
  confirmed the current generation. Optional registrars are retried but never
  waited on.
- Finalizing waits for *every* destination (optional ones too), the
  `finalize.after` grace period after the last copy, and a fresh stat of each
  source file. It is a compare-and-set from `registered` at the same
  generation, so a finalize that moved files while a scan reopened the unit
  cannot overwrite the reopen.

Recovery after a crash at each point:

- Mid-upload: the copy is still pending and goes again (the next PUT or
  multipart upload overwrites whatever was left).
- After delivering to a registrar, before recording it: delivered again.
  Registrars are idempotent upserts, which is the contract.
- Mid-move: across file systems the copy is written under a temporary name and
  size-checked before the rename and the source delete. A file missing at
  finalize whose backup copy has the right size is recorded as moved.
- After a lost journal: adoption lists each unit's prefix at each size-verified
  destination before its first upload there, and skips files already present
  with the same size. Hash-verified destinations never adopt, because nothing
  proves an object ferry did not hash is the same file.

## Transfers and fan-out

Files up to `single_put_max` (64 MiB) go up in one PUT. Microscopes write
thousands of small TIFFs, and a multipart upload costs three or four requests
per file. Bigger files stream as multipart parts, read on a blocking thread
that hashes them (SHA-256) as it goes. Each store's token bucket (`bandwidth`,
optionally per time window) paces its requests.

**One read, many destinations.** The reader produces `Bytes` chunks, which are
reference-counted: handing a chunk to a second destination's single PUT or
`WriteMultipart` copies nothing. So N destinations cost one disk read and one
hash, not N. This is what an
[obstore discussion](https://github.com/developmentseed/obstore/discussions/529)
asked for -- one read stream feeding several stores -- where the answer was to
manage it in the caller; in Rust it is a few lines. Two consequences shape the
design:

- The slowest destination paces the others (a chunk is released once every
  destination has taken it; `wait_for_capacity` bounds what is buffered).
  `fanout: separate` trades extra reads for independence.
- A destination that fails mid-file is aborted and drops out; the others carry
  on. Its copy row records the failure and backs off, and the retry reads the
  file again for that destination alone.

After every upload a HEAD checks the stored size; `verify: sha256` also reads
the object back and compares hashes. `object_store` signs every payload
(SigV4), so the server rejects corruption in transit, and `checksum: sha256`
adds a server-verified whole-object checksum where the server supports it.

## Deleting the source

`finalize.action: delete` is the one irreversible step, so it waits for the
strongest evidence ferry has: every destination (not just the required ones)
holds a copy that verified at the file's current size and mtime, every
required registrar has confirmed the unit, `finalize.after` has passed since
the last copy (time for downstream checks or replication), and the source file
still has the stats that were uploaded. Any mismatch reopens the unit instead.
`action: move` follows the same gate and keeps the file on a backup drive.

## Registration: exec hooks

A registrar is a command, not a plugin API: ferry writes one JSON event to its
stdin and reads the exit code (see [hooks.md](hooks.md)). Of the generic
options for an OSS agent, this one needs nothing from the catalog side -- no
server to stand up as an HTTP hook would, no language binding as an in-process
plugin would -- and works with any language, an existing CLI, or a two-line
script, while ferry keeps the durability: the outbox retries with backoff,
delivers in order per unit, supersedes events of a reopened unit, and gates
finalizing on required registrars. An HTTP endpoint is one `curl` away for
those who want it. The built-in `manifest` registrar covers the catalog-less
case by writing the manifest next to the data.

Within one round ferry stops calling a registrar after its first failure, so a
catalog that is down costs one timeout rather than one per waiting unit. An
alert goes out after `retry.alert_after` attempts.

## Scheduling and streaming

Cron expressions are evaluated by `croner` in an IANA zone, or as local
wall-clock time. Waits are taken in naps of at most 30 s that re-read the
clock, so DST changes, NTP steps and sleep are noticed. A time that DST skips
fires at the jump, a time it repeats fires once, and passes missed while the
PC slept collapse into one pass on wake.

A stream rescans every unit not yet finalized, plus any new unit directory (a
cheap listing), every `poll_interval`, and the whole tree every
`full_scan_interval`. File events only make a rescan happen sooner: they can be
dropped, and network shares deliver none. Between rescans the loop sleeps until
the earliest settle, idle, backoff or grace deadline (`next_due`), so a file is
picked up when it settles rather than at the next poll. Uploads are tasks
bounded by `files_in_flight` (oldest first); on stop, in-flight work gets a
grace period and is then cut off, to be retried by the next run.

## Operations

Logs are JSON lines, one file per UTC day, so a finished day is a finished
file. Shipping copies finished days to a store and runs the log registrars,
and a heartbeat object records each pipeline's summary. On Windows, `ferry
service install` registers a native service with the service manager: delayed
automatic start, restart actions on any failure (crash or error exit), stop and
shutdown handled as a graceful stop. On Linux it installs a systemd unit.

## Not (yet) here

Prometheus metrics; registration driven by bucket notifications on the
manifest object; STS / web-identity credentials; an OpenLineage registrar;
self-update; fleet-wide configuration.
