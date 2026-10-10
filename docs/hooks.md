# Hooks: registrars and notifiers

ferry tells the rest of your system what it did by running commands. A hook is
any program, in any language: ferry starts it once per event and writes one
JSON document to its stdin. Exit code 0 means the event was handled; anything
else, or running past `timeout`, is a failure. The last 2000 characters of
stderr become the error that `ferry status` and alerts show.

```yaml
registrars:
  - name: catalog
    exec: [python, -m, my_catalog.ferry_hook]   # argv; no shell involved
    cwd: C:/ferry-hooks                          # default: ferry's working directory
    env: {CATALOG_URL: "${CATALOG_URL}"}         # added to ferry's environment
    timeout: 10m                                 # the default
    required: true                               # the default
```

Each run also gets `FERRY_EVENT` (the event name), `FERRY_NODE` and, for
pipeline events, `FERRY_PIPELINE`, so a hook can dispatch without parsing JSON
first.

## Delivery

Registrar events go through a durable outbox in the journal:

- **At least once.** An event is retried with exponential backoff
  (`retry.initial`, doubling to `retry.max`) until the hook exits 0, across
  restarts. A crash between the hook succeeding and ferry recording it
  delivers it again, so a hook must treat a repeated event for the same unit
  and `generation` as a no-op (an upsert keyed on the unit URI is the usual
  answer).
- **In order per unit.** A `finalize` waits behind an undelivered `register`
  for the same unit and registrar.
- **Stale events are dropped.** A unit that changes after it completed is
  reopened; its undelivered events are superseded, and it is registered again
  with a higher `generation` once complete.
- **Required registrars gate finalizing.** Source files are kept, moved or
  deleted only once every `required: true` registrar has confirmed the unit's
  `register`. Optional registrars are retried but never waited on.
- One failure per registrar per round: a catalog that is down costs one
  timeout, not one per waiting unit. After `retry.alert_after` failures an
  alert goes to the notifiers.

Receipts go to notifiers through the same outbox, retried the same way, but
they never hold anything up. Alerts are best effort: sent once, not retried.

## Events

Every event has the same envelope. Consumers should ignore events and fields
they do not know: new ones are added without changing `schema`.

```json
{
  "schema": "ferry.event/v1",
  "event": "register",
  "agent": "ferry-ingest/0.1.0-alpha.1",
  "node": "workcell1-pc",
  "pipeline": "workcell1_cytation",
  "manifest": { ... },
  "transfers": [ ... ]
}
```

| `event` | Sent to | Carries |
| --- | --- | --- |
| `register` | registrars | `manifest`, and the unit's `upload`/`adopt` transfers |
| `finalize` | registrars | `manifest` (with `backup_path`s), and `move`/`delete` transfers |
| `failures` | registrars | failed `transfers` (best effort, for an audit trail) |
| `log` | `logs.ship.registrars` | `log`: a shipped day of logs |
| `alert` | notifiers (by default) | `alert` |
| `receipt` | notifiers with `events: [receipt]` | `receipt`: a delivered unit |

### `register` and `finalize`

`register` is sent once a unit is complete and every *required* destination
holds a verified copy of every file. `finalize` is sent once the source files
were kept, moved or deleted -- which waits for *every* destination, every
required registrar, and any `finalize.after` grace period.

```json
"manifest": {
  "unit": "plateA__1",
  "generation": 1,
  "uri": "s3://instrument-raw/workcell/raw/plateA__1",
  "root": "C:\\Users\\Lab\\Documents\\Data\\Cytation5",
  "local_path": "C:\\Users\\Lab\\Documents\\Data\\Cytation5\\plateA__1",
  "first_seen": "2026-10-08T23:41:07.112Z",
  "completed": "2026-10-09T02:00:03.410Z",
  "created": "2026-10-09T02:00:41.902Z",
  "finalize": "move",
  "destinations": [
    {"name": "ceph", "uri": "s3://instrument-raw/workcell/raw/plateA__1", "required": true, "verify": "size"},
    {"name": "aws", "uri": "s3://lab-archive/cytation5/plateA__1", "required": false, "verify": "sha256"}
  ],
  "files": [
    {
      "path": "img/A1_01.tif",
      "rel_path": "plateA__1/img/A1_01.tif",
      "uri": "s3://instrument-raw/workcell/raw/plateA__1/img/A1_01.tif",
      "size": 8401230,
      "mtime": "2026-10-08T23:52:10.000Z",
      "sha256": "9f2c...e1",
      "local_path": "C:\\Users\\...\\plateA__1\\img\\A1_01.tif",
      "backup_path": null,
      "copies": [
        {"destination": "ceph", "uri": "s3://instrument-raw/...", "etag": "\"5d41...\"", "verified": "size", "uploaded_at": "2026-10-09T02:00:12.003Z"},
        {"destination": "aws", "uri": "s3://lab-archive/...", "etag": null, "verified": null, "uploaded_at": null}
      ]
    }
  ]
}
```

- `uri` (unit and file) points at the first destination; `copies` lists every
  destination. A copy with `verified: null` is still pending -- an optional
  destination that has not caught up yet.
- `sha256` is computed while the file is read for upload.
- `backup_path` is set in `finalize` events of `action: move` pipelines.
- `transfers` entries: `{rel_path, destination, op, status, src, dst, at,
  bytes, duration_ms, error}` with `op` one of `upload`, `adopt` (already in
  the store with the same size), `move`, `delete`, and `status` one of `ok`,
  `failed`, `changed` (the file changed while it was read).

### `log`

```json
"log": {"node": "workcell1-pc", "name": "ferry.2026-10-08.jsonl", "day": "2026-10-08",
        "uri": "s3://instrument-raw/_ferry/logs/workcell1-pc/ferry.2026-10-08.jsonl",
        "size": 182733, "sha256": "..."}
```

### `alert`

```json
"alert": {"level": "error", "title": "workcell1_cytation: plateA__1/img/A1_01.tif failed to upload to ceph 5 times",
          "message": "...", "pipeline": "workcell1_cytation", "unit": "plateA__1"}
```

Levels are `info` (each pass's digest), `warning` and `error`. A notifier gets
alerts at or above its `min_level` (default `warning`).

### `receipt`

One per unit and `generation`, for people: the unit is in every destination,
every required registrar has confirmed it, and its source files were kept,
moved or deleted. With a `finalize.after` grace period the receipt does not
wait it out; it says when the files will go (`done: false`, `at` the due time),
and a deletion that later fails raises an alert instead.

```json
"receipt": {"unit": "plateA__1", "generation": 1, "update": false, "files": 384, "bytes": 512300000000,
            "first_seen": "2026-10-09T08:02:11.000Z", "delivered": "2026-10-09T10:16:40.000Z", "duration_s": 8069,
            "destinations": [{"name": "ceph", "uri": "s3://instrument-raw/workcell/raw/plateA__1", "verified": "size"},
                             {"name": "aws", "uri": "s3://lab-archive/cytation5/plateA__1", "verified": "sha256"}],
            "registrars": ["catalog"],
            "original": {"action": "move", "done": true, "at": "2026-10-09T10:16:39.501Z", "path": "F:\\Cytation5Raw\\plateA__1"}}
```

- `update` is true when an earlier generation had a receipt: files arrived or
  changed after the unit was announced.
- `verified` is `sha256` when every file was read back and hashed there,
  otherwise `size`.
- `registrars` lists the registrars that had confirmed the unit.
- Units already registered when a journal is upgraded to receipts never get one.

### Notifier settings

```yaml
notifiers:
  - name: arrivals                     # names its receipts in the journal; default: webhook, or the exec program
    webhook: ${FERRY_RECEIPTS_WEBHOOK}
    events: [receipt]                  # alert (the default), receipt, or both
  - webhook: ${FERRY_ALERT_WEBHOOK}
    min_level: warning                 # the least severe alert it takes
    format: text                       # webhooks only: text (the default) or event
```

A webhook POSTs `{"text": ...}` by default, which Slack incoming webhooks
accept, as do Mattermost, Rocket.Chat, Google Chat and Discord's `/slack`
URLs. A receipt reads:

```text
acquisition:
• plateA__1 (384 files, 512.3 GB)
copied to:
• s3://instrument-raw/workcell/raw/plateA__1 (size verified)
• s3://lab-archive/cytation5/plateA__1 (SHA-256 verified)
registered with catalog · original moved to F:\Cytation5Raw\plateA__1
workcell1-pc · workcell1_cytation · delivered 2h after its first file
```

`format: event` POSTs the event JSON instead, for a service rather than a
person; an `exec` notifier always gets the event JSON on stdin. Notifiers that
take receipts need distinct names, since their undelivered receipts are kept by
name.

## The built-in `manifest` registrar

`registrars: [manifest]` writes each unit's manifest (the same document,
wrapped as `{"schema": "ferry.manifest/v1", "agent", "node", "pipeline",
...manifest}`) to `{unit}/_ferry_manifest.json` in the first destination
(`manifest: {destination: aws, key: "_manifests/{pipeline}/{unit}.json"}` to
change either). It is rewritten on finalize. A bucket notification on that key
is a catalog-agnostic "this unit is complete" signal.

## Starting downstream work

To process a unit as soon as it lands, trigger on registration, not on the
receipt (which is for people, and can follow a long `finalize.after`):

- **From a catalog:** a registrar records the unit on `register`, and your
  orchestrator picks it up from there; or the registrar calls the
  orchestrator's API itself.
- **From the store:** a bucket notification on the manifest key (S3 to SQS or
  EventBridge; Ceph RGW to Kafka, AMQP or HTTP) starts the work without
  involving ferry.

Either way:

- Read the objects the manifest lists, from a *required* destination;
  registration does not wait for optional ones. Do not list the prefix, which
  can hold uploads still in flight.
- Key the work on unit and `generation`. A unit that changes after it was
  registered comes back with a higher generation, so process it again. The
  manifest is rewritten on finalize, so a bucket notification fires twice for
  one generation.

## An example hook

```python
"""python -m my_catalog.ferry_hook: record ferry's units in a catalog."""

import json
import sys

from my_catalog import db


def main() -> int:
    event = json.load(sys.stdin)
    kind = event["event"]
    if kind == "register":
        m = event["manifest"]
        # Idempotent: the same unit and generation may arrive again.
        db.upsert_acquisition(uri=m["uri"], generation=m["generation"], files=m["files"])
    elif kind == "finalize":
        db.mark_archived(uri=event["manifest"]["uri"])
    elif kind == "failures":
        db.record_failures(event["transfers"])
    return 0  # unknown events: succeed, so new event kinds never block ferry


if __name__ == "__main__":
    sys.exit(main())
```
