//! Configuration: one YAML file per node.
//!
//! Durations read as `90s` / `15m` / `1h30m` (or plain seconds), sizes as
//! `64MiB` / `50MB`, and `${VAR}` / `${VAR:-default}` in any string value is
//! replaced from the environment, so secrets stay out of the file (quote it
//! inside `{...}` flow mappings). Relative paths are resolved against the config
//! file's directory. A misspelled key is an error, never silently ignored.

use std::collections::{BTreeMap, BTreeSet};
use std::fmt;
use std::marker::PhantomData;
use std::path::{Path, PathBuf};
use std::str::FromStr;
use std::time::Duration;

use anyhow::{Context, Result, anyhow, bail};
use bytesize::ByteSize;
use regex::Regex;
use serde::de::{self, Deserializer, MapAccess, Visitor};
use serde::{Deserialize, Serialize};

use crate::globs::PathGlob;

/// S3 rejects multipart parts under 5 MiB (bar the last) and single PUTs over 5 GiB.
pub const MIN_PART_SIZE: u64 = 5 * 1024 * 1024;
pub const MAX_SINGLE_PUT: u64 = 5 * 1024 * 1024 * 1024;

pub const DEFAULT_EXCLUDE: &[&str] = &[
    "*.tmp",
    "*.temp",
    "*.part",
    "*.partial",
    "*.crdownload",
    "~$*",
    ".~lock.*",
    "._*",
    ".DS_Store",
    "Thumbs.db",
    "desktop.ini",
];

// -- value parsers ------------------------------------------------------------

pub fn parse_duration(text: &str) -> Result<Duration, String> {
    let text = text.trim();
    if let Ok(seconds) = text.parse::<f64>()
        && seconds >= 0.0
        && seconds.is_finite()
    {
        return Ok(Duration::from_secs_f64(seconds));
    }
    humantime::parse_duration(text).map_err(|_| format!("not a duration: {text:?} (use e.g. 90s, 15m, 1h30m)"))
}

#[derive(Deserialize)]
#[serde(untagged)]
enum DurationRepr {
    Seconds(f64),
    Text(String),
}

impl DurationRepr {
    fn into_duration<E: de::Error>(self) -> Result<Duration, E> {
        match self {
            Self::Seconds(s) if s >= 0.0 && s.is_finite() => Ok(Duration::from_secs_f64(s)),
            Self::Seconds(s) => Err(E::custom(format!("not a duration: {s}"))),
            Self::Text(t) => parse_duration(&t).map_err(E::custom),
        }
    }
}

fn de_duration<'de, D: Deserializer<'de>>(d: D) -> Result<Duration, D::Error> {
    DurationRepr::deserialize(d)?.into_duration()
}

fn de_opt_duration<'de, D: Deserializer<'de>>(d: D) -> Result<Option<Duration>, D::Error> {
    Option::<DurationRepr>::deserialize(d)?
        .map(DurationRepr::into_duration)
        .transpose()
}

pub fn parse_size(text: &str) -> Result<u64, String> {
    let text = text.trim();
    let text = text
        .strip_suffix("/s")
        .or_else(|| text.strip_suffix("/S"))
        .unwrap_or(text)
        .trim();
    if let Ok(n) = text.parse::<u64>() {
        return Ok(n);
    }
    ByteSize::from_str(text)
        .map(|b| b.as_u64())
        .map_err(|_| format!("not a size: {text:?} (use e.g. 64MiB, 50MB)"))
}

#[derive(Deserialize)]
#[serde(untagged)]
enum SizeRepr {
    Bytes(u64),
    Text(String),
}

impl SizeRepr {
    fn into_bytes<E: de::Error>(self) -> Result<u64, E> {
        match self {
            Self::Bytes(n) => Ok(n),
            Self::Text(t) => parse_size(&t).map_err(E::custom),
        }
    }
}

fn de_size<'de, D: Deserializer<'de>>(d: D) -> Result<u64, D::Error> {
    SizeRepr::deserialize(d)?.into_bytes()
}

/// `"08:00-18:00"` -> minutes since midnight `(480, 1080)`.
pub fn parse_window(text: &str) -> Result<(u16, u16), String> {
    let re = Regex::new(r"^\s*(\d{1,2}):(\d{2})\s*-\s*(\d{1,2}):(\d{2})\s*$").expect("valid regex");
    let bad = || format!("not a time window: {text:?} (use HH:MM-HH:MM)");
    let caps = re.captures(text).ok_or_else(bad)?;
    let n = |i: usize| caps[i].parse::<u16>().map_err(|_| bad());
    let (h1, m1, h2, m2) = (n(1)?, n(2)?, n(3)?, n(4)?);
    if h1 > 23 || h2 > 24 || m1 > 59 || m2 > 59 || (h2 == 24 && m2 != 0) {
        return Err(bad());
    }
    Ok((h1 * 60 + m1, h2 * 60 + m2))
}

/// Bytes per second, always or per local time window (unlimited outside them).
#[derive(Debug, Clone, PartialEq)]
pub enum Bandwidth {
    Always(u64),
    Windows(Vec<(u16, u16, u64)>),
}

impl<'de> Deserialize<'de> for Bandwidth {
    fn deserialize<D: Deserializer<'de>>(d: D) -> Result<Self, D::Error> {
        #[derive(Deserialize)]
        #[serde(untagged)]
        enum Repr {
            Rate(SizeRepr),
            Windows(BTreeMap<String, SizeRepr>),
        }
        match Repr::deserialize(d)? {
            Repr::Rate(rate) => Ok(Self::Always(rate.into_bytes()?)),
            Repr::Windows(map) => map
                .into_iter()
                .map(|(window, rate)| {
                    let (start, end) = parse_window(&window).map_err(de::Error::custom)?;
                    Ok((start, end, rate.into_bytes()?))
                })
                .collect::<Result<Vec<_>, D::Error>>()
                .map(Self::Windows),
        }
    }
}

/// Deserialize a bare string as shorthand for a struct (`"ceph"` for
/// `{store: ceph}`), keeping the struct's own error messages for maps.
fn string_or_struct<'de, T, D>(d: D) -> Result<T, D::Error>
where
    T: Deserialize<'de> + FromStr<Err = String>,
    D: Deserializer<'de>,
{
    struct V<T>(PhantomData<T>);
    impl<'de, T: Deserialize<'de> + FromStr<Err = String>> Visitor<'de> for V<T> {
        type Value = T;
        fn expecting(&self, f: &mut fmt::Formatter) -> fmt::Result {
            f.write_str("a name or a mapping")
        }
        fn visit_str<E: de::Error>(self, v: &str) -> Result<T, E> {
            T::from_str(v).map_err(E::custom)
        }
        fn visit_map<M: MapAccess<'de>>(self, map: M) -> Result<T, M::Error> {
            T::deserialize(de::value::MapAccessDeserializer::new(map))
        }
    }
    d.deserialize_any(V(PhantomData))
}

macro_rules! shorthand_vec {
    ($name:ident, $ty:ty) => {
        fn $name<'de, D: Deserializer<'de>>(d: D) -> Result<Vec<$ty>, D::Error> {
            #[derive(Deserialize)]
            struct Wrap(#[serde(deserialize_with = "string_or_struct")] $ty);
            Ok(Vec::<Wrap>::deserialize(d)?.into_iter().map(|w| w.0).collect())
        }
    };
}

// -- the schema ---------------------------------------------------------------

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Deserialize, Serialize)]
#[serde(rename_all = "lowercase")]
pub enum Mode {
    Cron,
    #[default]
    Stream,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Deserialize, Serialize)]
#[serde(rename_all = "lowercase")]
pub enum Upload {
    File,
    #[default]
    Unit,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Deserialize, Serialize)]
#[serde(rename_all = "lowercase")]
pub enum Fanout {
    #[default]
    Shared,
    Separate,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Deserialize, Serialize)]
#[serde(rename_all = "lowercase")]
pub enum Watch {
    #[default]
    Events,
    Poll,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Deserialize, Serialize)]
#[serde(rename_all = "lowercase")]
pub enum Action {
    #[default]
    Keep,
    Move,
    Delete,
}

impl Action {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Keep => "keep",
            Self::Move => "move",
            Self::Delete => "delete",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Deserialize, Serialize)]
#[serde(rename_all = "lowercase")]
pub enum Verify {
    /// A HEAD after the upload: the stored object has the size sent.
    #[default]
    Size,
    /// Read the object back and compare its SHA-256 with the one hashed on upload.
    Sha256,
}

impl Verify {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Size => "size",
            Self::Sha256 => "sha256",
        }
    }
}

/// Whether an object already at a destination (after a lost journal, or a
/// migration from another uploader) counts as a copy instead of being
/// uploaded again.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize)]
#[serde(rename_all = "lowercase")]
pub enum Adopt {
    /// Upload everything.
    #[default]
    Off,
    /// An object with the same key and size counts. Any object of that size
    /// would pass, so only where source files are kept.
    Size,
    /// The object is read back and must hash like the local file.
    Sha256,
}

/// `off`, `size` or `sha256`; `false` and `true` mean `off` and `size`.
fn de_adopt<'de, D: Deserializer<'de>>(d: D) -> Result<Adopt, D::Error> {
    #[derive(Deserialize)]
    #[serde(untagged)]
    enum Spec {
        Flag(bool),
        Name(String),
    }
    match Spec::deserialize(d)? {
        Spec::Flag(false) => Ok(Adopt::Off),
        Spec::Flag(true) => Ok(Adopt::Size),
        Spec::Name(name) => match name.as_str() {
            "off" => Ok(Adopt::Off),
            "size" => Ok(Adopt::Size),
            "sha256" => Ok(Adopt::Sha256),
            other => Err(de::Error::custom(format!("{other:?} is not off, size or sha256"))),
        },
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Default, Deserialize, Serialize)]
#[serde(rename_all = "lowercase")]
pub enum Level {
    Info,
    #[default]
    Warning,
    Error,
}

impl Level {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Info => "info",
            Self::Warning => "warning",
            Self::Error => "error",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Deserialize)]
#[serde(rename_all = "UPPERCASE")]
pub enum LogLevel {
    #[serde(alias = "debug")]
    Debug,
    #[default]
    #[serde(alias = "info")]
    Info,
    #[serde(alias = "warning", alias = "WARN", alias = "warn")]
    Warning,
    #[serde(alias = "error")]
    Error,
}

impl LogLevel {
    /// As a `tracing` filter directive.
    pub fn as_filter(self) -> &'static str {
        match self {
            Self::Debug => "debug",
            Self::Info => "info",
            Self::Warning => "warn",
            Self::Error => "error",
        }
    }
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct StoreConfig {
    /// `s3://bucket/prefix`, or a local directory / `file://` URL.
    pub url: String,
    /// S3-compatible endpoint (Ceph RGW, SeaweedFS, MinIO, ...); unset means AWS.
    pub endpoint: Option<String>,
    pub region: Option<String>,
    /// Path-style addressing (the default) is what self-hosted stores expect.
    #[serde(default)]
    pub virtual_hosted_style: bool,
    /// An AWS shared-config profile (static keys from `~/.aws/credentials`).
    pub profile: Option<String>,
    #[serde(default)]
    pub allow_http: bool,
    /// `sha256` (or `crc64nvme`): ask the server to verify a checksum of every
    /// request body. Only where the server validates it: AWS does; Ceph RGW
    /// from Tentacle.
    #[serde(default)]
    pub checksum: Option<String>,
    /// Caps every upload to this store, across pipelines.
    pub bandwidth: Option<Bandwidth>,
    /// Passed to object_store's `AmazonS3ConfigKey`s verbatim.
    #[serde(default)]
    pub options: BTreeMap<String, String>,
}

impl StoreConfig {
    pub fn is_s3(&self) -> bool {
        self.url.starts_with("s3://")
    }

    /// Where objects under `prefix` in this store live.
    fn namespace(&self, prefix: &str) -> Namespace {
        let extra = prefix_parts(prefix);
        if self.is_s3() {
            let rest = &self.url["s3://".len()..];
            let (bucket, base) = rest.split_once('/').unwrap_or((rest, ""));
            let mut path = prefix_parts(base);
            path.extend(extra);
            Namespace::S3 {
                bucket: bucket.to_string(),
                endpoint: self
                    .endpoint
                    .as_deref()
                    .map(|e| e.trim_end_matches('/').to_ascii_lowercase()),
                path,
            }
        } else {
            let mut dir = PathBuf::from(&self.url);
            dir.extend(extra);
            Namespace::Local(dir)
        }
    }
}

fn prefix_parts(prefix: &str) -> Vec<String> {
    prefix
        .split(['/', '\\'])
        .filter(|p| !p.is_empty() && *p != ".")
        .map(str::to_string)
        .collect()
}

/// A place objects are written: a bucket (at an endpoint) and a key path in
/// it, or a local directory.
#[derive(Debug, Clone, PartialEq)]
enum Namespace {
    S3 {
        bucket: String,
        endpoint: Option<String>,
        path: Vec<String>,
    },
    Local(PathBuf),
}

impl Namespace {
    /// One holds the other, so objects could collide.
    fn overlaps(&self, other: &Self) -> bool {
        match (self, other) {
            (
                Self::S3 { bucket, endpoint, path },
                Self::S3 {
                    bucket: b,
                    endpoint: e,
                    path: p,
                },
            ) => bucket == b && endpoint == e && (path.starts_with(p) || p.starts_with(path)),
            (Self::Local(a), Self::Local(b)) => inside(a, b) || inside(b, a),
            _ => false,
        }
    }

    fn identity(&self) -> String {
        match self {
            Self::S3 { bucket, endpoint, path } => {
                let at = format!("s3://{bucket}/{}", path.join("/"));
                match endpoint {
                    Some(endpoint) => format!("{at} via {endpoint}"),
                    None => at,
                }
            }
            Self::Local(dir) => dir.display().to_string(),
        }
    }
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct DestinationConfig {
    pub store: String,
    /// Distinguishes two destinations on one store; defaults to the store name.
    pub name: Option<String>,
    /// Overrides the pipeline's `prefix`.
    pub prefix: Option<String>,
    /// Registration waits for every required destination. Finalizing (moving or
    /// deleting the source) waits for all of them, required or not.
    #[serde(default = "yes")]
    pub required: bool,
    #[serde(default)]
    pub verify: Verify,
}

impl DestinationConfig {
    pub fn key(&self) -> &str {
        self.name.as_deref().unwrap_or(&self.store)
    }
}

impl FromStr for DestinationConfig {
    type Err = String;
    fn from_str(store: &str) -> Result<Self, String> {
        Ok(Self {
            store: store.to_string(),
            name: None,
            prefix: None,
            required: true,
            verify: Verify::Size,
        })
    }
}

/// How files group into units (acquisitions): directories at a fixed depth
/// (`dirs`, one glob per level) or a regex whose `unit` group names the unit.
#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct UnitConfig {
    pub dirs: Option<String>,
    pub regex: Option<String>,
}

impl Default for UnitConfig {
    fn default() -> Self {
        Self {
            dirs: Some("*".into()),
            regex: None,
        }
    }
}

impl UnitConfig {
    pub fn levels(&self) -> Vec<String> {
        self.dirs
            .as_deref()
            .unwrap_or("*")
            .replace('\\', "/")
            .split('/')
            .map(str::to_string)
            .collect()
    }
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct FinalizeConfig {
    #[serde(default)]
    pub action: Action,
    pub backup_root: Option<PathBuf>,
    /// Keep the source this long after every copy is verified, then act.
    #[serde(default, deserialize_with = "de_opt_duration")]
    pub after: Option<Duration>,
    #[serde(default = "four")]
    pub concurrency: usize,
    #[serde(default = "yes")]
    pub prune_empty_dirs: bool,
}

impl Default for FinalizeConfig {
    fn default() -> Self {
        Self {
            action: Action::Keep,
            backup_root: None,
            after: None,
            concurrency: 4,
            prune_empty_dirs: true,
        }
    }
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct LimitsConfig {
    #[serde(default = "sixteen")]
    pub files_in_flight: usize,
    /// Files up to this size go up in one PUT; bigger ones stream as multipart.
    #[serde(default = "default_single_put", deserialize_with = "de_size")]
    pub single_put_max: u64,
    #[serde(default = "default_part_size", deserialize_with = "de_size")]
    pub part_size: u64,
    #[serde(default = "eight")]
    pub part_concurrency: usize,
}

impl Default for LimitsConfig {
    fn default() -> Self {
        Self {
            files_in_flight: 16,
            single_put_max: default_single_put(),
            part_size: default_part_size(),
            part_concurrency: 8,
        }
    }
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RetryConfig {
    #[serde(default = "thirty_s", deserialize_with = "de_duration")]
    pub initial: Duration,
    #[serde(default = "one_h", deserialize_with = "de_duration")]
    pub max: Duration,
    /// Alert once a file, delivery or finalize has failed this many times.
    #[serde(default = "five")]
    pub alert_after: u32,
}

impl Default for RetryConfig {
    fn default() -> Self {
        Self {
            initial: thirty_s(),
            max: one_h(),
            alert_after: 5,
        }
    }
}

impl RetryConfig {
    /// Seconds to wait after the `attempts`-th consecutive failure.
    pub fn backoff(&self, attempts: u32) -> f64 {
        let exponent = attempts.saturating_sub(1).min(30) as i32;
        (self.initial.as_secs_f64() * 2f64.powi(exponent)).min(self.max.as_secs_f64())
    }
}

/// A registrar: an `exec` hook, or the built-in `manifest` writer.
#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct HookConfig {
    pub name: Option<String>,
    /// Finalizing waits for every required registrar to confirm the unit.
    #[serde(default = "yes")]
    pub required: bool,
    /// Run this command per event, with the event JSON on stdin.
    pub exec: Option<Vec<String>>,
    pub manifest: Option<ManifestHook>,
    pub cwd: Option<PathBuf>,
    #[serde(default)]
    pub env: BTreeMap<String, String>,
    #[serde(default = "ten_m", deserialize_with = "de_duration")]
    pub timeout: Duration,
}

impl HookConfig {
    pub fn key(&self) -> String {
        if let Some(name) = &self.name {
            return name.clone();
        }
        if self.manifest.is_some() {
            return "manifest".into();
        }
        self.exec
            .as_ref()
            .and_then(|argv| argv.first())
            .map(|program| program_name(program))
            .unwrap_or_else(|| "hook".into())
    }
}

fn program_name(program: &str) -> String {
    Path::new(program)
        .file_stem()
        .map(|s| s.to_string_lossy().into_owned())
        .unwrap_or_else(|| program.to_string())
}

impl FromStr for HookConfig {
    type Err = String;
    fn from_str(name: &str) -> Result<Self, String> {
        if name != "manifest" {
            return Err(format!(
                "unknown registrar {name:?}: write `manifest`, or a mapping with `exec:`"
            ));
        }
        Ok(Self {
            name: None,
            required: true,
            exec: None,
            manifest: Some(ManifestHook::default()),
            cwd: None,
            env: BTreeMap::new(),
            timeout: ten_m(),
        })
    }
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ManifestHook {
    /// Which destination gets the manifest (default: the first).
    pub destination: Option<String>,
    /// Relative to the destination's prefix; `{unit}`, `{pipeline}`, `{node}`.
    #[serde(default = "manifest_key")]
    pub key: String,
}

impl Default for ManifestHook {
    fn default() -> Self {
        Self {
            destination: None,
            key: manifest_key(),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize, Serialize)]
#[serde(rename_all = "lowercase")]
pub enum NotifierEvent {
    /// Recurring failures, units stuck open, and (at `info`) each pass's digest.
    Alert,
    /// One per unit, once every destination holds it (see `docs/hooks.md`).
    Receipt,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Deserialize, Serialize)]
#[serde(rename_all = "lowercase")]
pub enum WebhookFormat {
    /// `{"text": ...}`, which Slack incoming webhooks accept (as do Mattermost,
    /// Rocket.Chat, Google Chat, and Discord's `/slack` webhook URLs).
    #[default]
    Text,
    /// The event JSON itself, as an exec hook gets it.
    Event,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct NotifierConfig {
    /// Names the notifier in the journal, where its receipts wait until it
    /// takes them. Defaults to `webhook`, or the `exec` program's name.
    pub name: Option<String>,
    /// POST each event to this URL, as `format` says.
    pub webhook: Option<String>,
    /// Or run this command per event, with the event JSON on stdin.
    pub exec: Option<Vec<String>>,
    #[serde(default = "alerts_only")]
    pub events: Vec<NotifierEvent>,
    /// The least severe alert it takes.
    #[serde(default)]
    pub min_level: Level,
    /// How a `webhook` posts: `text` (the default) or `event`.
    pub format: Option<WebhookFormat>,
    pub cwd: Option<PathBuf>,
    #[serde(default)]
    pub env: BTreeMap<String, String>,
    #[serde(default = "one_m", deserialize_with = "de_duration")]
    pub timeout: Duration,
}

fn alerts_only() -> Vec<NotifierEvent> {
    vec![NotifierEvent::Alert]
}

impl NotifierConfig {
    pub fn key(&self) -> String {
        if let Some(name) = &self.name {
            return name.clone();
        }
        match self.exec.as_ref().and_then(|argv| argv.first()) {
            Some(program) => program_name(program),
            None => "webhook".into(),
        }
    }
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PipelineConfig {
    pub name: String,
    pub root: PathBuf,
    /// Key prefix under each destination's store (a destination may override it).
    #[serde(default)]
    pub prefix: String,
    #[serde(deserialize_with = "de_destinations")]
    pub destinations: Vec<DestinationConfig>,
    #[serde(default)]
    pub unit: UnitConfig,
    /// Matched against each file's path within its unit (basename if slashless).
    #[serde(default = "include_all")]
    pub include: Vec<String>,
    /// Matched against the path from the root; excluded entries are invisible.
    #[serde(default = "default_exclude")]
    pub exclude: Vec<String>,
    #[serde(default)]
    pub mode: Mode,
    pub cron: Option<String>,
    /// IANA zone the cron expression is read in; default: the node's local time.
    pub timezone: Option<String>,
    #[serde(default)]
    pub upload: Upload,
    #[serde(default)]
    pub fanout: Fanout,
    #[serde(default = "sixty_s", deserialize_with = "de_duration")]
    pub settle: Duration,
    #[serde(default = "ten_m", deserialize_with = "de_duration")]
    pub unit_idle: Duration,
    pub marker: Option<String>,
    #[serde(default = "default_max_open", deserialize_with = "de_opt_duration")]
    pub max_open: Option<Duration>,
    #[serde(default = "fifteen_s", deserialize_with = "de_duration")]
    pub poll_interval: Duration,
    #[serde(default = "thirty_m", deserialize_with = "de_duration")]
    pub full_scan_interval: Duration,
    #[serde(default)]
    pub watch: Watch,
    #[serde(default)]
    pub probe_exclusive: bool,
    #[serde(default, deserialize_with = "de_adopt")]
    pub adopt: Adopt,
    #[serde(default)]
    pub finalize: FinalizeConfig,
    #[serde(default)]
    pub limits: LimitsConfig,
    #[serde(default)]
    pub retry: RetryConfig,
    #[serde(default, deserialize_with = "de_hooks")]
    pub registrars: Vec<HookConfig>,
}

impl PipelineConfig {
    /// The destination's key prefix: its own `prefix`, else the pipeline's.
    pub fn prefix_for(&self, dest: &DestinationConfig) -> String {
        dest.prefix.clone().unwrap_or_else(|| self.prefix.clone())
    }

    pub fn destination_keys(&self) -> Vec<String> {
        self.destinations.iter().map(|d| d.key().to_string()).collect()
    }
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ShipConfig {
    pub store: String,
    #[serde(default = "logs_prefix")]
    pub prefix: String,
    #[serde(default = "one_h", deserialize_with = "de_duration")]
    pub interval: Duration,
    #[serde(default = "five_m_opt", deserialize_with = "de_opt_duration")]
    pub heartbeat: Option<Duration>,
    /// Exec hooks told about each day's log once it is shipped (`event: log`).
    #[serde(default, deserialize_with = "de_hooks")]
    pub registrars: Vec<HookConfig>,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct LogsConfig {
    pub dir: Option<PathBuf>,
    #[serde(default)]
    pub level: LogLevel,
    #[serde(default = "thirty")]
    pub keep_days: usize,
    pub ship: Option<ShipConfig>,
}

impl Default for LogsConfig {
    fn default() -> Self {
        Self {
            dir: None,
            level: LogLevel::Info,
            keep_days: 30,
            ship: None,
        }
    }
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Config {
    #[serde(default = "default_node")]
    pub node: String,
    pub state_dir: PathBuf,
    pub stores: BTreeMap<String, StoreConfig>,
    pub pipelines: Vec<PipelineConfig>,
    #[serde(default)]
    pub logs: LogsConfig,
    #[serde(default)]
    pub notifiers: Vec<NotifierConfig>,
}

shorthand_vec!(de_destinations, DestinationConfig);
shorthand_vec!(de_hooks, HookConfig);

fn yes() -> bool {
    true
}
fn four() -> usize {
    4
}
fn five() -> u32 {
    5
}
fn eight() -> usize {
    8
}
fn sixteen() -> usize {
    16
}
fn thirty() -> usize {
    30
}
fn fifteen_s() -> Duration {
    Duration::from_secs(15)
}
fn thirty_s() -> Duration {
    Duration::from_secs(30)
}
fn sixty_s() -> Duration {
    Duration::from_secs(60)
}
fn one_m() -> Duration {
    Duration::from_secs(60)
}
fn ten_m() -> Duration {
    Duration::from_secs(600)
}
fn thirty_m() -> Duration {
    Duration::from_secs(1800)
}
fn one_h() -> Duration {
    Duration::from_secs(3600)
}
fn five_m_opt() -> Option<Duration> {
    Some(Duration::from_secs(300))
}
fn default_max_open() -> Option<Duration> {
    Some(Duration::from_secs(86400))
}
fn default_single_put() -> u64 {
    64 * 1024 * 1024
}
fn default_part_size() -> u64 {
    8 * 1024 * 1024
}
fn include_all() -> Vec<String> {
    vec!["*".into()]
}
fn default_exclude() -> Vec<String> {
    DEFAULT_EXCLUDE.iter().map(|s| s.to_string()).collect()
}
fn manifest_key() -> String {
    "{unit}/_ferry_manifest.json".into()
}
fn logs_prefix() -> String {
    "_ferry/logs".into()
}
fn default_node() -> String {
    hostname::get()
        .ok()
        .and_then(|h| h.into_string().ok())
        .unwrap_or_else(|| "ferry".into())
}

// -- loading ------------------------------------------------------------------

/// Replace `${VAR}` and `${VAR:-default}` in every string value.
pub fn interpolate(value: &mut yaml_serde::Value, env: &dyn Fn(&str) -> Option<String>) -> Result<()> {
    let re = Regex::new(r"\$\{([A-Za-z_][A-Za-z0-9_]*)(?::-(.*?))?\}").expect("valid regex");
    let mut missing = BTreeSet::new();
    walk(value, &mut |text: &mut String| {
        let replaced = re.replace_all(text, |caps: &regex::Captures| {
            let name = &caps[1];
            env(name)
                .or_else(|| caps.get(2).map(|d| d.as_str().to_string()))
                .unwrap_or_else(|| {
                    missing.insert(name.to_string());
                    caps[0].to_string()
                })
        });
        *text = replaced.into_owned();
    });
    if !missing.is_empty() {
        bail!(
            "unset environment variable(s): {}",
            missing.into_iter().collect::<Vec<_>>().join(", ")
        );
    }
    Ok(())
}

fn walk(value: &mut yaml_serde::Value, f: &mut dyn FnMut(&mut String)) {
    match value {
        yaml_serde::Value::String(s) => f(s),
        yaml_serde::Value::Sequence(items) => items.iter_mut().for_each(|v| walk(v, f)),
        yaml_serde::Value::Mapping(map) => map.iter_mut().for_each(|(_, v)| walk(v, f)),
        yaml_serde::Value::Tagged(tagged) => walk(&mut tagged.value, f),
        _ => {}
    }
}

fn is_windows_absolute(path: &Path) -> bool {
    let text = path.to_string_lossy();
    let bytes = text.as_bytes();
    (bytes.len() >= 3 && bytes[0].is_ascii_alphabetic() && bytes[1] == b':' && (bytes[2] == b'/' || bytes[2] == b'\\'))
        || text.starts_with("\\\\")
}

fn resolve(base: &Path, path: &Path) -> PathBuf {
    let text = path.to_string_lossy();
    let expanded = match (text.strip_prefix("~/"), dirs::home_dir()) {
        (Some(rest), Some(home)) => home.join(rest),
        _ => path.to_path_buf(),
    };
    // A Windows config validated elsewhere: C:/... is absolute there, not relative.
    if expanded.is_absolute() || is_windows_absolute(&expanded) {
        return expanded;
    }
    let joined = base.join(&expanded);
    joined.canonicalize().unwrap_or(joined)
}

fn real(path: &Path) -> PathBuf {
    path.canonicalize().unwrap_or_else(|_| path.to_path_buf())
}

fn inside(child: &Path, parent: &Path) -> bool {
    real(child).starts_with(real(parent))
}

impl Config {
    pub fn load(path: &Path) -> Result<Self> {
        Self::load_with_env(path, &|name| std::env::var(name).ok())
    }

    pub fn load_with_env(path: &Path, env: &dyn Fn(&str) -> Option<String>) -> Result<Self> {
        let text = std::fs::read_to_string(path).with_context(|| format!("cannot read config {}", path.display()))?;
        let mut value: yaml_serde::Value =
            yaml_serde::from_str(&text).map_err(|e| anyhow!("{}: not valid YAML: {e}", path.display()))?;
        interpolate(&mut value, env).map_err(|e| anyhow!("{}: {e}", path.display()))?;
        let mut config: Config = serde_path_to_error::deserialize(value).map_err(|e| {
            let at = e.path().to_string();
            if at == "." {
                anyhow!("{}: {}", path.display(), e.inner())
            } else {
                anyhow!("{}: {at}: {}", path.display(), e.inner())
            }
        })?;
        let base = path
            .canonicalize()
            .ok()
            .and_then(|p| p.parent().map(Path::to_path_buf))
            .unwrap_or_else(|| PathBuf::from("."));
        config.resolve_paths(&base);
        config.validate().map_err(|e| anyhow!("{}: {e}", path.display()))?;
        Ok(config)
    }

    pub fn from_yaml(text: &str, base: &Path) -> Result<Self> {
        let mut value: yaml_serde::Value = yaml_serde::from_str(text)?;
        interpolate(&mut value, &|name| std::env::var(name).ok())?;
        let mut config: Config =
            serde_path_to_error::deserialize(value).map_err(|e| anyhow!("{}: {}", e.path(), e.inner()))?;
        config.resolve_paths(base);
        config.validate()?;
        Ok(config)
    }

    pub fn resolve_paths(&mut self, base: &Path) {
        self.state_dir = resolve(base, &self.state_dir);
        if let Some(dir) = &self.logs.dir {
            self.logs.dir = Some(resolve(base, dir));
        }
        for store in self.stores.values_mut() {
            if !store.is_s3() {
                let local = store.url.strip_prefix("file://").unwrap_or(&store.url);
                store.url = resolve(base, Path::new(local)).to_string_lossy().into_owned();
            }
        }
        for pipeline in &mut self.pipelines {
            pipeline.root = resolve(base, &pipeline.root);
            if let Some(backup) = &pipeline.finalize.backup_root {
                pipeline.finalize.backup_root = Some(resolve(base, backup));
            }
            for hook in &mut pipeline.registrars {
                if let Some(cwd) = &hook.cwd {
                    hook.cwd = Some(resolve(base, cwd));
                }
            }
        }
        for notifier in &mut self.notifiers {
            if let Some(cwd) = &notifier.cwd {
                notifier.cwd = Some(resolve(base, cwd));
            }
        }
        if let Some(ship) = &mut self.logs.ship {
            for hook in &mut ship.registrars {
                if let Some(cwd) = &hook.cwd {
                    hook.cwd = Some(resolve(base, cwd));
                }
            }
        }
    }

    pub fn validate(&self) -> Result<()> {
        let name_re = Regex::new(r"^[A-Za-z0-9][A-Za-z0-9_.-]*$").expect("valid regex");
        if self.pipelines.is_empty() {
            bail!("pipelines: at least one pipeline is required");
        }
        let mut names = BTreeSet::new();
        for pipeline in &self.pipelines {
            let at = |field: &str| format!("pipeline {}: {field}", pipeline.name);
            if !name_re.is_match(&pipeline.name) {
                bail!("pipeline name {:?} must be letters, digits, _ . -", pipeline.name);
            }
            if !names.insert(pipeline.name.clone()) {
                bail!("pipeline names must be unique ({} repeats)", pipeline.name);
            }
            self.validate_pipeline(pipeline, &at)?;
        }
        for (i, a) in self.pipelines.iter().enumerate() {
            for b in &self.pipelines[i + 1..] {
                if inside(&a.root, &b.root) || inside(&b.root, &a.root) {
                    bail!(
                        "pipelines {} and {} watch overlapping roots; give one pipeline \
                         several destinations instead",
                        a.name,
                        b.name
                    );
                }
            }
        }
        let mut receivers = BTreeSet::new();
        for (i, notifier) in self.notifiers.iter().enumerate() {
            if notifier.webhook.is_some() == notifier.exec.is_some() {
                bail!("notifiers[{i}]: set exactly one of `webhook` or `exec`");
            }
            if notifier.exec.as_ref().is_some_and(Vec::is_empty) {
                bail!("notifiers[{i}]: exec needs a command");
            }
            if notifier.events.is_empty() {
                bail!("notifiers[{i}]: events is empty; list `alert`, `receipt` or both");
            }
            if notifier.format.is_some() && notifier.webhook.is_none() {
                bail!("notifiers[{i}]: `format` applies to webhooks; an exec hook always gets the event JSON");
            }
            if notifier.events.contains(&NotifierEvent::Receipt) && !receivers.insert(notifier.key()) {
                bail!(
                    "notifiers that take receipts need distinct names ({} repeats); add `name:`",
                    notifier.key()
                );
            }
        }
        if let Some(ship) = &self.logs.ship {
            if !self.stores.contains_key(&ship.store) {
                bail!("logs.ship: unknown store {:?}", ship.store);
            }
            let mut keys = BTreeSet::new();
            for hook in &ship.registrars {
                let key = hook.key();
                if hook.exec.as_ref().is_none_or(Vec::is_empty) || hook.manifest.is_some() {
                    bail!("logs.ship.registrars: {key}: only `exec` hooks apply to logs");
                }
                if !keys.insert(key.clone()) {
                    bail!("logs.ship.registrars need distinct names ({key} repeats); add `name:`");
                }
            }
        }
        self.validate_places()
    }

    fn validate_pipeline(&self, p: &PipelineConfig, at: &dyn Fn(&str) -> String) -> Result<()> {
        if p.destinations.is_empty() {
            bail!("{}", at("needs at least one destination"));
        }
        let mut keys = BTreeSet::new();
        for dest in &p.destinations {
            if !self.stores.contains_key(&dest.store) {
                let known: Vec<_> = self.stores.keys().cloned().collect();
                bail!(
                    "{}",
                    at(&format!(
                        "unknown store {:?} (defined: {})",
                        dest.store,
                        if known.is_empty() {
                            "none".into()
                        } else {
                            known.join(", ")
                        }
                    ))
                );
            }
            if !keys.insert(dest.key().to_string()) {
                bail!(
                    "{}",
                    at(&format!(
                        "destination {:?} repeats; give repeated stores a `name:`",
                        dest.key()
                    ))
                );
            }
        }
        match (&p.unit.dirs, &p.unit.regex) {
            (Some(_), Some(_)) => bail!("{}", at("unit: set either dirs or regex, not both")),
            (_, Some(re)) => {
                let compiled = Regex::new(re).map_err(|e| anyhow!("{}", at(&format!("unit.regex: {e}"))))?;
                if !compiled.capture_names().any(|n| n == Some("unit")) {
                    bail!("{}", at("unit.regex needs a named group: (?P<unit>...)"));
                }
            }
            _ => {
                let levels = p.unit.levels();
                if levels.iter().any(|l| l.is_empty() || l.contains("**")) {
                    bail!(
                        "{}",
                        at("unit.dirs is one glob per directory level, e.g. '*__*' or '*/run*'")
                    );
                }
                for level in &levels {
                    PathGlob::new(level).map_err(|e| anyhow!("{}", at(&e.to_string())))?;
                }
            }
        }
        for pattern in p.include.iter().chain(&p.exclude).chain(&p.marker) {
            PathGlob::new(pattern).map_err(|e| anyhow!("{}", at(&e.to_string())))?;
        }
        match (p.mode, &p.cron) {
            (Mode::Cron, None) => bail!("{}", at("mode cron needs a cron expression")),
            (Mode::Cron, Some(expr)) => {
                croner::Cron::from_str(expr).map_err(|e| anyhow!("{}", at(&format!("invalid cron {expr:?}: {e}"))))?;
            }
            (Mode::Stream, Some(_)) => bail!("{}", at("cron only applies to mode: cron")),
            (Mode::Stream, None) => {}
        }
        if let Some(zone) = &p.timezone {
            zone.parse::<chrono_tz::Tz>()
                .map_err(|_| anyhow!("{}", at(&format!("unknown timezone {zone:?}"))))?;
        }
        if p.limits.part_size < MIN_PART_SIZE {
            bail!("{}", at("limits.part_size must be at least 5MiB (an S3 minimum)"));
        }
        if p.limits.single_put_max > MAX_SINGLE_PUT {
            bail!("{}", at("limits.single_put_max must be at most 5GiB (an S3 maximum)"));
        }
        if p.limits.files_in_flight == 0 || p.limits.part_concurrency == 0 || p.finalize.concurrency == 0 {
            bail!("{}", at("concurrency limits must be at least 1"));
        }
        if p.finalize.action == Action::Move {
            let Some(backup) = &p.finalize.backup_root else {
                bail!("{}", at("finalize.backup_root is required for action: move"));
            };
            if inside(backup, &p.root) || inside(&p.root, backup) {
                bail!(
                    "{}",
                    at("backup_root and root must not contain each other (ferry would ship its own backups)")
                );
            }
        }
        if inside(&self.state_dir, &p.root) {
            bail!("{}", at("state_dir must live outside the root"));
        }
        if p.adopt == Adopt::Size && p.finalize.action != Action::Keep {
            bail!(
                "{}",
                at(&format!(
                    "adopt: size trusts any object of the right size, so source files cannot be {}d on its \
                     word; use adopt: sha256 (reads each object back) or off",
                    p.finalize.action.as_str()
                ))
            );
        }
        let mut hook_keys = BTreeSet::new();
        for hook in &p.registrars {
            let key = hook.key();
            if hook.exec.is_some() == hook.manifest.is_some() {
                bail!(
                    "{}",
                    at(&format!("registrar {key}: set exactly one of `exec` or `manifest`"))
                );
            }
            if hook.exec.as_ref().is_some_and(Vec::is_empty) {
                bail!("{}", at(&format!("registrar {key}: exec needs a command")));
            }
            if let Some(manifest) = &hook.manifest
                && let Some(dest) = &manifest.destination
                && !p.destinations.iter().any(|d| d.key() == dest)
            {
                bail!("{}", at(&format!("registrar {key}: unknown destination {dest:?}")));
            }
            if !hook_keys.insert(key.clone()) {
                bail!(
                    "{}",
                    at(&format!("registrars need distinct names ({key} repeats); add `name:`"))
                );
            }
        }
        Ok(())
    }

    pub fn pipeline(&self, name: &str) -> Option<&PipelineConfig> {
        self.pipelines.iter().find(|p| p.name == name)
    }

    pub fn log_dir(&self) -> PathBuf {
        self.logs.dir.clone().unwrap_or_else(|| self.state_dir.join("logs"))
    }

    pub fn journal_path(&self) -> PathBuf {
        self.state_dir.join("journal.sqlite3")
    }

    /// Where a destination's objects live (bucket, endpoint and key prefix, or
    /// directory). The journal records it: copies made at one place do not
    /// count at another.
    pub fn destination_identity(&self, p: &PipelineConfig, d: &DestinationConfig) -> String {
        self.stores[&d.store].namespace(&p.prefix_for(d)).identity()
    }

    /// No two destinations (or log shipping) may write where the other does,
    /// and nothing ferry writes locally may sit in a watched root.
    fn validate_places(&self) -> Result<()> {
        let mut places: Vec<(String, Namespace)> = Vec::new();
        for p in &self.pipelines {
            for d in &p.destinations {
                let place = self.stores[&d.store].namespace(&p.prefix_for(d));
                places.push((format!("pipeline {} destination {}", p.name, d.key()), place));
            }
        }
        if let Some(ship) = &self.logs.ship {
            places.push(("logs.ship".into(), self.stores[&ship.store].namespace(&ship.prefix)));
        }
        for (i, (a, x)) in places.iter().enumerate() {
            for (b, y) in &places[i + 1..] {
                if x.overlaps(y) {
                    bail!(
                        "{a} and {b} write to overlapping places ({} and {}); give each its own prefix",
                        x.identity(),
                        y.identity()
                    );
                }
            }
        }
        for p in &self.pipelines {
            let mut local: Vec<(String, PathBuf)> = vec![
                ("state_dir".into(), self.state_dir.clone()),
                ("logs.dir".into(), self.log_dir()),
            ];
            for (name, store) in self.stores.iter().filter(|(_, s)| !s.is_s3()) {
                local.push((format!("store {name}"), PathBuf::from(&store.url)));
            }
            for other in &self.pipelines {
                if let Some(backup) = &other.finalize.backup_root {
                    local.push((format!("pipeline {}'s backup_root", other.name), backup.clone()));
                }
            }
            for (what, dir) in local {
                if inside(&dir, &p.root) || inside(&p.root, &dir) {
                    bail!(
                        "pipeline {}: {what} ({}) and the root must not contain each other \
                         (ferry would ship what it writes)",
                        p.name,
                        dir.display()
                    );
                }
            }
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn base() -> PathBuf {
        std::env::temp_dir()
    }

    fn yaml(destinations: &str, extra: &str) -> String {
        format!(
            "state_dir: /tmp/ferry-test-state\n\
             stores:\n  s3: {{url: 's3://bucket/raw'}}\n  aws: {{url: 's3://other'}}\n\
             pipelines:\n  - name: inst\n    root: /tmp/ferry-test-data\n    destinations: {destinations}\n{extra}"
        )
    }

    fn load(extra: &str) -> Result<Config> {
        Config::from_yaml(&yaml("[s3]", extra), &base())
    }

    #[test]
    fn durations_and_sizes_read_like_people_write_them() {
        assert_eq!(parse_duration("90").unwrap(), Duration::from_secs(90));
        assert_eq!(parse_duration("15m").unwrap(), Duration::from_secs(900));
        assert_eq!(parse_duration("1h30m").unwrap(), Duration::from_secs(5400));
        assert_eq!(parse_duration("500ms").unwrap(), Duration::from_millis(500));
        assert!(parse_duration("fast").is_err());
        assert_eq!(parse_size("64MiB").unwrap(), 64 * 1024 * 1024);
        assert_eq!(parse_size("50MB/s").unwrap(), 50_000_000);
        assert_eq!(parse_window("22:30-06:00").unwrap(), (1350, 360));
        assert!(parse_window("25:00-26:00").is_err());
    }

    #[test]
    fn defaults_and_shorthands() {
        let cfg = load("    registrars: [manifest]\n").unwrap();
        let p = &cfg.pipelines[0];
        assert_eq!(
            (p.mode, p.upload, p.fanout),
            (Mode::Stream, Upload::Unit, Fanout::Shared)
        );
        assert_eq!(p.destinations[0].store, "s3");
        assert!(p.destinations[0].required);
        assert_eq!(p.registrars[0].key(), "manifest");
        assert_eq!(p.settle, Duration::from_secs(60));
        assert_eq!(p.max_open, Some(Duration::from_secs(86400)));
    }

    #[test]
    fn several_destinations_each_with_their_own_prefix_and_checks() {
        let text = yaml(
            "[s3, {store: aws, prefix: archive, required: false, verify: sha256}]",
            "    prefix: raw\n",
        );
        let cfg = Config::from_yaml(&text, &base()).unwrap();
        let p = &cfg.pipelines[0];
        assert_eq!(p.destination_keys(), vec!["s3", "aws"]);
        assert_eq!(p.prefix_for(&p.destinations[0]), "raw");
        assert_eq!(p.prefix_for(&p.destinations[1]), "archive");
        assert_eq!(p.destinations[1].verify, Verify::Sha256);
        assert!(!p.destinations[1].required);
    }

    #[test]
    fn mistakes_name_the_field() {
        let err = load("    setle: 60s\n").unwrap_err().to_string();
        assert!(err.contains("unknown field `setle`"), "{err}");
        let err = load("    finalize: {action: mvoe}\n").unwrap_err().to_string();
        assert!(err.contains("pipelines[0].finalize.action"), "{err}");
        let err = load("    mode: cron\n").unwrap_err().to_string();
        assert!(err.contains("needs a cron expression"), "{err}");
        let err = load("    mode: cron\n    cron: '0 25 * * *'\n")
            .unwrap_err()
            .to_string();
        assert!(err.contains("invalid cron"), "{err}");
        let err = load("    cron: '0 2 * * *'\n").unwrap_err().to_string();
        assert!(err.contains("only applies to mode: cron"), "{err}");
        let err = load("    finalize: {action: move}\n").unwrap_err().to_string();
        assert!(err.contains("backup_root is required"), "{err}");
        let err = load("    limits: {part_size: 1MiB}\n").unwrap_err().to_string();
        assert!(err.contains("at least 5MiB"), "{err}");
        let err = Config::from_yaml(&yaml("[nope]", ""), &base()).unwrap_err().to_string();
        assert!(err.contains("unknown store \"nope\""), "{err}");
        let err = Config::from_yaml(&yaml("[s3, s3]", ""), &base())
            .unwrap_err()
            .to_string();
        assert!(err.contains("give repeated stores a `name:`"), "{err}");
        let err = load("    registrars: [{exec: [a], manifest: {}}]\n")
            .unwrap_err()
            .to_string();
        assert!(err.contains("exactly one of `exec` or `manifest`"), "{err}");
        let err = load("    registrars: [nope]\n").unwrap_err().to_string();
        assert!(err.contains("unknown registrar"), "{err}");
    }

    #[test]
    fn overlapping_pipelines_are_rejected() {
        let text = "state_dir: /tmp/ferry-test-state\nstores: {s3: {url: 's3://b'}}\npipelines:\n  - {name: a, root: /tmp/ferry-x, destinations: [s3]}\n  - {name: b, root: /tmp/ferry-x/sub, destinations: [s3]}\n";
        let err = Config::from_yaml(text, &base()).unwrap_err().to_string();
        assert!(err.contains("overlapping roots"), "{err}");
    }

    #[test]
    fn environment_references_are_substituted() {
        let mut value: yaml_serde::Value =
            yaml_serde::from_str("a: '${HOST}:${PORT:-9000}'\nb: ['${HOST}']\nc: 1").unwrap();
        interpolate(&mut value, &|name| (name == "HOST").then(|| "rgw".to_string())).unwrap();
        assert_eq!(value["a"].as_str(), Some("rgw:9000"));
        assert_eq!(value["b"][0].as_str(), Some("rgw"));
        let mut value: yaml_serde::Value = yaml_serde::from_str("k: '${SECRET}'").unwrap();
        let err = interpolate(&mut value, &|_| None).unwrap_err().to_string();
        assert!(err.contains("SECRET"), "{err}");
    }

    #[test]
    fn windows_paths_stay_absolute_on_other_systems() {
        let text = "state_dir: 'C:/ProgramData/ferry'\nstores: {s3: {url: 's3://b'}}\npipelines:\n  - {name: a, root: 'D:/Data', destinations: [s3]}\n";
        let cfg = Config::from_yaml(text, &base()).unwrap();
        assert_eq!(
            cfg.state_dir.to_string_lossy().replace('\\', "/"),
            "C:/ProgramData/ferry"
        );
        assert_eq!(cfg.pipelines[0].root.to_string_lossy().replace('\\', "/"), "D:/Data");
    }

    #[test]
    fn backoff_doubles_up_to_the_max() {
        let retry = RetryConfig::default();
        assert_eq!(retry.backoff(1), 30.0);
        assert_eq!(retry.backoff(2), 60.0);
        assert_eq!(retry.backoff(10), 3600.0);
    }

    #[test]
    fn adoption_is_off_unless_asked_and_never_by_size_alone_before_a_move() {
        let adopt = |extra: &str| load(extra).map(|cfg| cfg.pipelines[0].adopt);
        assert_eq!(adopt("").unwrap(), Adopt::Off);
        assert_eq!(adopt("    adopt: true\n").unwrap(), Adopt::Size);
        assert_eq!(adopt("    adopt: false\n").unwrap(), Adopt::Off);
        assert_eq!(
            adopt("    adopt: sha256\n    finalize: {action: delete}\n").unwrap(),
            Adopt::Sha256
        );
        let err = adopt("    adopt: size\n    finalize: {action: delete}\n")
            .unwrap_err()
            .to_string();
        assert!(err.contains("cannot be deleted on its word"), "{err}");
        assert!(adopt("    adopt: maybe\n").is_err());
    }

    #[test]
    fn nothing_writes_where_something_else_does() {
        let two = |a: &str, b: &str, extra: &str| {
            Config::from_yaml(
                &format!(
                    "state_dir: /tmp/ferry-test-state\n\
                     stores:\n  raw: {{url: 's3://bucket/raw'}}\n  bucket: {{url: 's3://bucket'}}\n  \
                     rgw: {{url: 's3://bucket', endpoint: 'https://rgw.lab/'}}\n\
                     pipelines:\n\
                     \x20 - {{name: a, root: /tmp/ferry-a, destinations: [{a}]}}\n\
                     \x20 - {{name: b, root: /tmp/ferry-b, destinations: [{b}]}}\n{extra}"
                ),
                &base(),
            )
        };
        let err = two("raw", "bucket", "").unwrap_err().to_string();
        assert!(
            err.contains("overlapping places (s3://bucket/raw and s3://bucket/)"),
            "{err}"
        );
        assert!(two("raw", "raw", "").is_err());
        two("{store: raw, prefix: x}", "{store: raw, prefix: y}", "").unwrap();
        two("raw", "rgw", "").unwrap();
        let shipped = two(
            "raw",
            "{store: bucket, prefix: b}",
            "logs: {ship: {store: bucket, prefix: raw/_ferry}}\n",
        );
        assert!(shipped.unwrap_err().to_string().contains("logs.ship"));

        let cfg = two("raw", "rgw", "").unwrap();
        let (p, d) = (&cfg.pipelines[1], &cfg.pipelines[1].destinations[0]);
        assert_eq!(cfg.destination_identity(p, d), "s3://bucket/ via https://rgw.lab");
    }

    #[test]
    fn a_local_store_may_not_sit_in_a_root() {
        let dir = tempfile::tempdir().unwrap();
        let root = dir.path().join("data");
        std::fs::create_dir_all(root.join("copies")).unwrap();
        let err = Config::from_yaml(
            &format!(
                "state_dir: state\nstores: {{here: {{url: '{}'}}}}\npipelines:\n  - {{name: a, root: '{}', destinations: [here]}}\n",
                root.join("copies").display(),
                root.display()
            ),
            dir.path(),
        )
        .unwrap_err()
        .to_string();
        assert!(err.contains("store here"), "{err}");
    }

    #[test]
    fn notifiers_take_alerts_unless_told_otherwise() {
        let cfg = load(
            "notifiers:\n  - webhook: https://chat.example/a\n  - name: arrivals\n    webhook: https://chat.example/b\n    events: [receipt]\n    format: event\n  - exec: [/opt/bin/page-oncall.sh]\n    events: [alert, receipt]\n",
        )
        .unwrap();
        let summary: Vec<(String, Vec<NotifierEvent>, Option<WebhookFormat>)> = cfg
            .notifiers
            .iter()
            .map(|n| (n.key(), n.events.clone(), n.format))
            .collect();
        assert_eq!(
            summary,
            [
                ("webhook".into(), vec![NotifierEvent::Alert], None),
                (
                    "arrivals".into(),
                    vec![NotifierEvent::Receipt],
                    Some(WebhookFormat::Event)
                ),
                (
                    "page-oncall".into(),
                    vec![NotifierEvent::Alert, NotifierEvent::Receipt],
                    None
                ),
            ]
        );

        let err = |extra: &str| load(extra).unwrap_err().to_string();
        assert!(err("notifiers:\n  - webhook: a\n    events: []\n").contains("events is empty"));
        assert!(err("notifiers:\n  - exec: [notify]\n    format: text\n").contains("`format` applies to webhooks"));
        let twice = "notifiers:\n  - {webhook: a, events: [receipt]}\n  - {webhook: b, events: [receipt]}\n";
        assert!(err(twice).contains("(webhook repeats)"), "{}", err(twice));
        // Names only matter where receipts wait: two alert webhooks are fine.
        load("notifiers:\n  - webhook: a\n  - webhook: b\n").unwrap();
    }
}
