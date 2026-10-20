//! What hooks receive: one versioned JSON event per call (see `docs/hooks.md`).
//!
//! `register` and `finalize` carry the unit's [`Manifest`]; `failures` carries
//! failed transfers for an audit trail; `log` a shipped log file; `alert` an
//! alert and `receipt` a delivered unit's [`Receipt`] (notifiers only).
//! Consumers should ignore events and fields they do not know, so new ones can
//! be added without a schema bump.

use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

use crate::config::Level;
use crate::util::{human_bytes, human_duration};

pub const EVENT_SCHEMA: &str = "ferry.event/v1";
pub const MANIFEST_SCHEMA: &str = "ferry.manifest/v1";

/// What an event is about. Later versions may add kinds: ignore any you do
/// not know.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "lowercase")]
pub enum EventKind {
    Register,
    Finalize,
    Failures,
    Log,
    Alert,
    Receipt,
}

impl EventKind {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Register => "register",
            Self::Finalize => "finalize",
            Self::Failures => "failures",
            Self::Log => "log",
            Self::Alert => "alert",
            Self::Receipt => "receipt",
        }
    }

    pub fn parse(text: &str) -> Option<Self> {
        Some(match text {
            "register" => Self::Register,
            "finalize" => Self::Finalize,
            "failures" => Self::Failures,
            "log" => Self::Log,
            "alert" => Self::Alert,
            "receipt" => Self::Receipt,
            _ => return None,
        })
    }
}

/// One call to a hook. `id` is unique to the event, and a redelivery carries
/// the same one, so a consumer can drop repeats by it.
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[schemars(title = "ferry event", extend("$id" = "https://raw.githubusercontent.com/Rosebud-Biosciences/ferry/main/schemas/event.schema.json"))]
pub struct Event {
    #[schemars(extend("const" = EVENT_SCHEMA))]
    pub schema: String,
    pub id: String,
    pub event: EventKind,
    /// The ferry that sent it, as `ferry-ingest/VERSION`.
    pub agent: String,
    /// The machine it ran on (`node` in the config).
    pub node: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub pipeline: Option<String>,
    /// `register` and `finalize`: the unit.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub manifest: Option<Manifest>,
    /// What the event reports: uploads with `register`, moves or deletes with
    /// `finalize`, failed transfers with `failures`.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub transfers: Vec<TransferEvent>,
    /// `log`: the day's log file, shipped.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub log: Option<ShippedLog>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub alert: Option<Alert>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub receipt: Option<Receipt>,
}

impl Event {
    pub fn new(kind: EventKind, node: &str, pipeline: Option<&str>) -> Self {
        Self {
            schema: EVENT_SCHEMA.into(),
            id: new_event_id(),
            event: kind,
            agent: crate::AGENT.into(),
            node: node.into(),
            pipeline: pipeline.map(str::to_string),
            manifest: None,
            transfers: Vec::new(),
            log: None,
            alert: None,
            receipt: None,
        }
    }

    /// The unit the event is about, if any.
    pub fn unit(&self) -> Option<&str> {
        self.manifest
            .as_ref()
            .map(|m| m.unit.as_str())
            .or_else(|| self.receipt.as_ref().map(|r| r.unit.as_str()))
    }
}

/// 128 random bits, as hex.
pub fn new_event_id() -> String {
    use aws_lc_rs::rand::SecureRandom;
    let mut bytes = [0u8; 16];
    aws_lc_rs::rand::SystemRandom::new()
        .fill(&mut bytes)
        .expect("the system's random number generator");
    hex::encode(bytes)
}

/// A `log` event's id: the same for every delivery of the same file.
pub fn log_event_id(log: &ShippedLog) -> String {
    let key = format!("{}/{}/{}", log.node, log.name, log.sha256);
    hex::encode(&aws_lc_rs::digest::digest(&aws_lc_rs::digest::SHA256, key.as_bytes()).as_ref()[..16])
}

/// The JSON Schemas of what hooks receive: `(file name, schema)`, as checked
/// in under `schemas/`.
pub fn schemas() -> Vec<(&'static str, serde_json::Value)> {
    vec![
        ("event.schema.json", schemars::schema_for!(Event).to_value()),
        ("manifest.schema.json", schemars::schema_for!(ManifestDoc).to_value()),
    ]
}

/// A complete unit: every file, and where each destination holds it.
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
pub struct Manifest {
    pub unit: String,
    /// Bumped each time the unit completes again after a change.
    pub generation: i64,
    /// The unit on the first destination (the primary).
    pub uri: String,
    pub root: String,
    pub local_path: Option<String>,
    #[schemars(extend("format" = "date-time"))]
    pub first_seen: String,
    #[schemars(extend("format" = "date-time"))]
    pub completed: Option<String>,
    #[schemars(extend("format" = "date-time"))]
    pub created: String,
    /// `keep`, `move` or `delete`: what happens to the source files.
    pub finalize: String,
    pub destinations: Vec<ManifestDestination>,
    pub files: Vec<ManifestFile>,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
pub struct ManifestDestination {
    pub name: String,
    pub uri: String,
    pub required: bool,
    pub verify: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
pub struct ManifestFile {
    /// The path within the unit.
    pub path: String,
    /// The path from the pipeline root.
    pub rel_path: String,
    /// The file on the first destination.
    pub uri: String,
    pub size: i64,
    #[schemars(extend("format" = "date-time"))]
    pub mtime: String,
    pub sha256: Option<String>,
    pub local_path: String,
    pub backup_path: Option<String>,
    pub copies: Vec<FileCopy>,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
pub struct FileCopy {
    pub destination: String,
    pub uri: String,
    pub etag: Option<String>,
    /// `size` or `sha256` for a verified copy; absent while it is still pending.
    pub verified: Option<String>,
    #[schemars(extend("format" = "date-time"))]
    pub uploaded_at: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
pub struct TransferEvent {
    pub rel_path: String,
    pub destination: Option<String>,
    /// `upload`, `adopt`, `move` or `delete`.
    pub op: String,
    /// `ok`, `failed` or `changed`.
    pub status: String,
    pub src: String,
    pub dst: String,
    #[schemars(extend("format" = "date-time"))]
    pub at: String,
    pub bytes: Option<i64>,
    pub duration_ms: Option<i64>,
    pub error: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
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

/// A unit every destination holds, whose required registrars confirmed it:
/// one per unit and generation, for people rather than pipelines.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
pub struct Receipt {
    pub unit: String,
    pub generation: i64,
    /// An earlier generation of the unit had a receipt already.
    pub update: bool,
    pub files: usize,
    pub bytes: i64,
    #[schemars(extend("format" = "date-time"))]
    pub first_seen: String,
    #[schemars(extend("format" = "date-time"))]
    pub delivered: String,
    /// From the unit's first file to delivery.
    pub duration_s: u64,
    pub destinations: Vec<ReceiptCopy>,
    /// The registrars that confirmed the unit.
    pub registrars: Vec<String>,
    pub original: Original,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
pub struct ReceiptCopy {
    pub name: String,
    pub uri: String,
    /// `sha256` if every file was read back and hashed; otherwise `size`.
    pub verified: String,
}

/// What became of the source files, or will once `finalize.after` passes.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
pub struct Original {
    /// `keep`, `move` or `delete`.
    pub action: String,
    pub done: bool,
    /// When it was done, or is due.
    #[schemars(extend("format" = "date-time"))]
    pub at: Option<String>,
    /// Where `move` puts the unit.
    pub path: Option<String>,
}

impl Receipt {
    /// The receipt as a chat message.
    pub fn text(&self, node: &str, pipeline: &str) -> String {
        let files = match self.files {
            1 => "1 file".to_string(),
            n => format!("{n} files"),
        };
        let mut lines = vec![
            if self.update {
                "acquisition (updated):"
            } else {
                "acquisition:"
            }
            .to_string(),
            format!("• {} ({files}, {})", self.unit, human_bytes(self.bytes.max(0) as u64)),
            "copied to:".to_string(),
        ];
        for copy in &self.destinations {
            let how = if copy.verified == "sha256" {
                "SHA-256 verified"
            } else {
                "size verified"
            };
            lines.push(format!("• {} ({how})", copy.uri));
        }
        let mut status = Vec::new();
        if !self.registrars.is_empty() {
            status.push(format!("registered with {}", self.registrars.join(", ")));
        }
        status.push(self.original.text());
        lines.push(status.join(" · "));
        lines.push(format!(
            "{node} · {pipeline} · delivered {} after its first file",
            human_duration(self.duration_s as f64)
        ));
        lines.join("\n")
    }
}

impl Original {
    fn text(&self) -> String {
        let path = self.path.as_deref().unwrap_or("its backup root");
        let when = self.at.as_deref().map(minutes_utc).unwrap_or_default();
        match (self.action.as_str(), self.done) {
            ("move", true) => format!("original moved to {path}"),
            ("move", false) => format!("original moves to {path} at {when}"),
            ("delete", true) => "original deleted".to_string(),
            ("delete", false) => format!("original will be deleted at {when}"),
            _ => "original kept".to_string(),
        }
    }
}

fn minutes_utc(timestamp: &str) -> String {
    chrono::DateTime::parse_from_rfc3339(timestamp)
        .map(|t| t.to_utc().format("%Y-%m-%d %H:%M UTC").to_string())
        .unwrap_or_else(|_| timestamp.to_string())
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
pub struct ShippedLog {
    pub node: String,
    pub name: String,
    pub day: String,
    pub uri: String,
    pub size: i64,
    pub sha256: String,
}

/// The document the `manifest` registrar writes to the store.
#[derive(Serialize, JsonSchema)]
#[schemars(title = "ferry manifest", extend("$id" = "https://raw.githubusercontent.com/Rosebud-Biosciences/ferry/main/schemas/manifest.schema.json"))]
pub struct ManifestDoc<'a> {
    #[schemars(extend("const" = MANIFEST_SCHEMA))]
    pub schema: &'static str,
    pub agent: &'a str,
    pub node: &'a str,
    pub pipeline: &'a str,
    #[serde(flatten)]
    pub manifest: &'a Manifest,
}

#[cfg(test)]
mod tests {
    use super::*;

    fn receipt(action: &str, done: bool) -> Receipt {
        Receipt {
            unit: "plateA__1".into(),
            generation: 1,
            update: false,
            files: 384,
            bytes: 512_300_000_000,
            first_seen: "2026-10-09T08:02:11.000Z".into(),
            delivered: "2026-10-09T10:16:40.000Z".into(),
            duration_s: 8069,
            destinations: vec![
                ReceiptCopy {
                    name: "ceph".into(),
                    uri: "s3://raw/workcell/plateA__1".into(),
                    verified: "sha256".into(),
                },
                ReceiptCopy {
                    name: "nas".into(),
                    uri: "file:///mnt/archive/plateA__1".into(),
                    verified: "size".into(),
                },
            ],
            registrars: vec!["catalog".into()],
            original: Original {
                action: action.into(),
                done,
                at: Some("2026-10-16T10:16:40.000Z".into()),
                path: (action == "move").then(|| "F:/Cytation5Raw/plateA__1".into()),
            },
        }
    }

    /// `FERRY_BLESS=1 cargo test` rewrites them.
    #[test]
    fn the_checked_in_schemas_match_the_types() {
        let dir = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("schemas");
        for (name, schema) in schemas() {
            let text = serde_json::to_string_pretty(&schema).unwrap() + "\n";
            let path = dir.join(name);
            if std::env::var_os("FERRY_BLESS").is_some() {
                std::fs::create_dir_all(&dir).unwrap();
                std::fs::write(&path, &text).unwrap();
            }
            let checked_in = std::fs::read_to_string(&path).unwrap_or_default().replace("\r\n", "\n");
            assert!(
                checked_in == text,
                "schemas/{name} is out of date: run `FERRY_BLESS=1 cargo test --lib schemas` and commit it"
            );
        }
    }

    #[test]
    fn event_ids_are_unique_but_a_log_keeps_its_own() {
        let a = Event::new(EventKind::Register, "pc1", Some("inst"));
        let b = Event::new(EventKind::Register, "pc1", Some("inst"));
        assert_ne!(a.id, b.id);
        assert_eq!(a.id.len(), 32);
        let log = ShippedLog {
            node: "pc1".into(),
            name: "ferry.2026-10-08.jsonl".into(),
            day: "2026-10-08".into(),
            uri: "s3://b/logs/pc1/ferry.2026-10-08.jsonl".into(),
            size: 10,
            sha256: "ab".repeat(32),
        };
        assert_eq!(log_event_id(&log), log_event_id(&log.clone()));
        let other = ShippedLog {
            sha256: "cd".repeat(32),
            ..log.clone()
        };
        assert_ne!(log_event_id(&log), log_event_id(&other));
    }

    #[test]
    fn a_receipt_reads_as_one_chat_message() {
        assert_eq!(
            receipt("delete", true).text("workcell1", "cytation"),
            "acquisition:\n\
             • plateA__1 (384 files, 512.3 GB)\n\
             copied to:\n\
             • s3://raw/workcell/plateA__1 (SHA-256 verified)\n\
             • file:///mnt/archive/plateA__1 (size verified)\n\
             registered with catalog · original deleted\n\
             workcell1 · cytation · delivered 2h after its first file"
        );
    }

    #[test]
    fn the_original_line_says_what_happened_or_when_it_will() {
        let status = |r: Receipt| r.text("n", "p").lines().nth(5).unwrap().to_string();
        assert_eq!(
            status(receipt("move", true)),
            "registered with catalog · original moved to F:/Cytation5Raw/plateA__1"
        );
        assert_eq!(
            status(receipt("move", false)),
            "registered with catalog · original moves to F:/Cytation5Raw/plateA__1 at 2026-10-16 10:16 UTC"
        );
        assert_eq!(
            status(receipt("delete", false)),
            "registered with catalog · original will be deleted at 2026-10-16 10:16 UTC"
        );
        let mut kept = receipt("keep", true);
        kept.registrars.clear();
        kept.update = true;
        kept.files = 1;
        let text = kept.text("n", "p");
        assert!(
            text.starts_with("acquisition (updated):\n• plateA__1 (1 file, "),
            "{text}"
        );
        assert_eq!(text.lines().nth(5), Some("original kept"));
    }
}
