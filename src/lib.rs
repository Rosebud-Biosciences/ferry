//! ferry: ship instrument data from watched folders to one or more
//! S3-compatible stores, on a cron schedule or as it lands.
//!
//! This library is the `ferry` binary's internals, not an API: it may change
//! in any release. What stays stable is the command line, the config format,
//! and the events and manifests hooks receive (see "Compatibility" in the
//! README).

pub mod cli;
pub mod config;
pub mod engine;
pub mod globs;
pub mod hooks;
pub mod journal;
pub mod lock;
pub mod logs;
pub mod manifest;
pub mod runner;
pub mod schedule;
pub mod service;
pub mod stores;
pub mod stream;
pub mod transfer;
pub mod units;
pub mod util;
pub mod winfs;

pub const VERSION: &str = env!("CARGO_PKG_VERSION");
pub const AGENT: &str = concat!("ferry-ingest/", env!("CARGO_PKG_VERSION"));
