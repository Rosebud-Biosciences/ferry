//! What hooks receive: one versioned JSON event per call (see `docs/hooks.md`).
//!
//! `register` and `finalize` carry the unit's [`Manifest`]; `failures` carries
//! failed transfers for an audit trail; `log` a shipped log file; `alert` an
//! alert (notifiers only). Consumers should ignore events and fields they do not
//! know, so new ones can be added without a schema bump.

use serde::{Deserialize, Serialize};

use crate::config::Level;

pub const EVENT_SCHEMA: &str = "ferry.event/v1";
pub const MANIFEST_SCHEMA: &str = "ferry.manifest/v1";

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum EventKind {
    Register,
    Finalize,
    Failures,
    Log,
    Alert,
}

impl EventKind {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Register => "register",
            Self::Finalize => "finalize",
            Self::Failures => "failures",
            Self::Log => "log",
            Self::Alert => "alert",
        }
    }

    pub fn parse(text: &str) -> Option<Self> {
        Some(match text {
            "register" => Self::Register,
            "finalize" => Self::Finalize,
            "failures" => Self::Failures,
            "log" => Self::Log,
            "alert" => Self::Alert,
            _ => return None,
        })
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Event {
    pub schema: String,
    pub event: EventKind,
    pub agent: String,
    pub node: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub pipeline: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub manifest: Option<Manifest>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub transfers: Vec<TransferEvent>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub log: Option<ShippedLog>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub alert: Option<Alert>,
}

impl Event {
    pub fn new(kind: EventKind, node: &str, pipeline: Option<&str>) -> Self {
        Self {
            schema: EVENT_SCHEMA.into(),
            event: kind,
            agent: crate::AGENT.into(),
            node: node.into(),
            pipeline: pipeline.map(str::to_string),
            manifest: None,
            transfers: Vec::new(),
            log: None,
            alert: None,
        }
    }
}

/// A complete unit: every file, and where each destination holds it.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Manifest {
    pub unit: String,
    /// Bumped each time the unit completes again after a change.
    pub generation: i64,
    /// The unit on the first destination (the primary).
    pub uri: String,
    pub root: String,
    pub local_path: Option<String>,
    pub first_seen: String,
    pub completed: Option<String>,
    pub created: String,
    /// `keep`, `move` or `delete`: what happens to the source files.
    pub finalize: String,
    pub destinations: Vec<ManifestDestination>,
    pub files: Vec<ManifestFile>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ManifestDestination {
    pub name: String,
    pub uri: String,
    pub required: bool,
    pub verify: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ManifestFile {
    /// The path within the unit.
    pub path: String,
    /// The path from the pipeline root.
    pub rel_path: String,
    /// The file on the first destination.
    pub uri: String,
    pub size: i64,
    pub mtime: String,
    pub sha256: Option<String>,
    pub local_path: String,
    pub backup_path: Option<String>,
    pub copies: Vec<FileCopy>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FileCopy {
    pub destination: String,
    pub uri: String,
    pub etag: Option<String>,
    /// `size` or `sha256` for a verified copy; absent while it is still pending.
    pub verified: Option<String>,
    pub uploaded_at: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TransferEvent {
    pub rel_path: String,
    pub destination: Option<String>,
    /// `upload`, `adopt`, `move` or `delete`.
    pub op: String,
    /// `ok`, `failed` or `changed`.
    pub status: String,
    pub src: String,
    pub dst: String,
    pub at: String,
    pub bytes: Option<i64>,
    pub duration_ms: Option<i64>,
    pub error: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Alert {
    pub level: Level,
    pub title: String,
    #[serde(default)]
    pub message: String,
    pub pipeline: Option<String>,
    pub unit: Option<String>,
}

impl Alert {
    pub fn text(&self) -> String {
        if self.message.is_empty() {
            self.title.clone()
        } else {
            format!("{}\n{}", self.title, self.message)
        }
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ShippedLog {
    pub node: String,
    pub name: String,
    pub day: String,
    pub uri: String,
    pub size: i64,
    pub sha256: String,
}

/// The document the `manifest` registrar writes to the store.
#[derive(Serialize)]
pub struct ManifestDoc<'a> {
    pub schema: &'static str,
    pub agent: &'a str,
    pub node: &'a str,
    pub pipeline: &'a str,
    #[serde(flatten)]
    pub manifest: &'a Manifest,
}
