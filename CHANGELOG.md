# Changelog

Notable changes to ferry, newest first. The format follows
[Keep a Changelog](https://keepachangelog.com/en/1.1.0/), and versions follow
[Semantic Versioning](https://semver.org/) as the README's "Compatibility"
section describes: until 1.0, a minor release may break things, and says how
here.

## [Unreleased]

### Fixed

- A unit whose folder name contains `{node}` or `{pipeline}` keeps it in its
  manifest's key, instead of having it filled in like a placeholder.

## [0.1.0-alpha.1] - 2026-10-20

The first release.

### Added

- Pipelines that watch a folder, group its files into units (one acquisition
  each, by directory pattern or regex), and upload them on a cron schedule or
  as they land: per file as each settles, or per unit once it is complete.
- Destinations: S3 (AWS, Ceph RGW, SeaweedFS, MinIO) and local or mounted
  directories, several per pipeline from one read, each verified by size or by
  reading the copy back and hashing it, with bandwidth caps by time of day.
- Registrars -- any command, or the built-in `manifest` -- told about each
  complete unit through a durable outbox; notifiers (webhooks, commands) for
  alerts and for one receipt per delivered unit.
- Finalizing: source files kept, moved to a backup drive or deleted once every
  copy is verified and every required registrar has confirmed, with each copy
  rechecked and each file set aside and checked again first.
- A SQLite journal that makes every step resumable; JSON-lines logs, shipped to
  a store with a heartbeat; `ferry status`, `retry`, `complete`, `abandon`,
  `destination accept|reset`, `outbox discard`, `doctor` and `schema`.
- Services on Windows, Linux (systemd) and macOS (launchd), with secrets in an
  `--env-file`.
- Prometheus `/metrics` and `/healthz` from `ferry watch`.
- Event ids, and JSON Schemas for events and manifests.
- Wheels on PyPI for Linux, Windows and macOS on x86-64 and ARM64, and the same
  binaries standalone with each GitHub release.
