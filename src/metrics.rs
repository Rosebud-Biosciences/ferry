//! `/metrics` (Prometheus text format) and `/healthz`, served by `ferry
//! watch` where `metrics.listen` says. The numbers come from the journal, so
//! they say what `ferry status` says; transfer counters total the journal's
//! transfers, which are never deleted.
//!
//! The endpoint has no authentication: bind it to localhost or a management
//! network.

use std::collections::BTreeMap;
use std::fmt::Write as _;
use std::sync::Arc;
use std::time::Duration;

use anyhow::Result;
use parking_lot::{Mutex, MutexGuard};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio_util::sync::CancellationToken;

use crate::VERSION;
use crate::journal::{Journal, Summary};

/// A request has this long to arrive and be answered.
const REQUEST_TIMEOUT: Duration = Duration::from_secs(10);
const MAX_HEAD: usize = 8 * 1024;

/// `(pipeline, destination, op, status)`.
type TransferKey = (String, String, String, String);

/// A gauge per pipeline: its name, help, and value in a summary.
type Gauge = (&'static str, &'static str, fn(&Summary) -> i64);

/// Transfers counted so far: count and bytes by key, to the transfer `last`.
#[derive(Default)]
struct Counted {
    totals: BTreeMap<TransferKey, (i64, i64)>,
    last: i64,
}

pub struct Metrics {
    node: String,
    /// Each pipeline's name and destination names.
    pipelines: Vec<(String, Vec<String>)>,
    journal: Arc<Mutex<Journal>>,
    started: f64,
    transfers: Mutex<Counted>,
}

impl Metrics {
    pub fn new(node: &str, pipelines: Vec<(String, Vec<String>)>, journal: Arc<Mutex<Journal>>, started: f64) -> Self {
        Self {
            node: node.to_string(),
            pipelines,
            journal,
            started,
            transfers: Mutex::default(),
        }
    }

    fn journal(&self) -> MutexGuard<'_, Journal> {
        self.journal.lock()
    }

    /// The journal answers.
    pub fn healthy(&self) -> Result<()> {
        self.journal().get_meta("schema_version")?;
        Ok(())
    }

    pub fn render(&self) -> Result<String> {
        let mut out = Text::default();
        out.family("ferry_build_info", "gauge", "The running ferry.");
        out.sample("ferry_build_info", &[("version", VERSION), ("node", &self.node)], 1.0);
        out.family(
            "ferry_start_time_seconds",
            "gauge",
            "When this ferry started, in Unix time.",
        );
        out.sample("ferry_start_time_seconds", &[], self.started);

        let journal = self.journal();
        let mut summaries = Vec::new();
        for (name, dests) in &self.pipelines {
            summaries.push((name, journal.summary(name, dests)?));
        }
        out.family("ferry_units", "gauge", "Units by state.");
        for (name, summary) in &summaries {
            for (state, count) in &summary.units {
                out.sample("ferry_units", &[("pipeline", name), ("state", state)], *count as f64);
            }
        }
        let gauges: [Gauge; 5] = [
            ("ferry_pending_files", "Files waiting to upload.", |s| s.pending_files),
            ("ferry_pending_bytes", "Bytes waiting to upload.", |s| s.pending_bytes),
            ("ferry_failing_copies", "Copies whose last attempt failed.", |s| {
                s.failing_copies
            }),
            (
                "ferry_deliveries_waiting",
                "Events waiting for a registrar or notifier.",
                |s| s.deliveries_waiting,
            ),
            (
                "ferry_deliveries_failing",
                "Waiting events whose last delivery failed.",
                |s| s.deliveries_failing,
            ),
        ];
        for (metric, help, value) in gauges {
            out.family(metric, "gauge", help);
            for (name, summary) in &summaries {
                out.sample(metric, &[("pipeline", name)], value(summary) as f64);
            }
        }
        out.family(
            "ferry_last_pass_timestamp_seconds",
            "gauge",
            "When the pipeline last finished a pass, in Unix time.",
        );
        for (name, summary) in &summaries {
            let at = summary
                .last_pass
                .as_deref()
                .and_then(|t| chrono::DateTime::parse_from_rfc3339(t).ok());
            if let Some(at) = at {
                out.sample(
                    "ferry_last_pass_timestamp_seconds",
                    &[("pipeline", name)],
                    at.timestamp_millis() as f64 / 1000.0,
                );
            }
        }

        let mut counted = self.transfers.lock();
        let (new, last) = journal.transfer_totals(counted.last)?;
        for t in new {
            let entry = counted.totals.entry((t.pipeline, t.dest, t.op, t.status)).or_default();
            entry.0 += t.count;
            entry.1 += t.bytes;
        }
        counted.last = last;
        out.family(
            "ferry_transfers_total",
            "counter",
            "Uploads, adoptions, moves and deletes, by outcome.",
        );
        for ((pipeline, dest, op, status), (count, _)) in &counted.totals {
            out.sample(
                "ferry_transfers_total",
                &[
                    ("pipeline", pipeline),
                    ("destination", dest),
                    ("op", op),
                    ("status", status),
                ],
                *count as f64,
            );
        }
        out.family(
            "ferry_transferred_bytes_total",
            "counter",
            "Bytes uploaded, adopted, moved or deleted, by transfers that succeeded.",
        );
        for ((pipeline, dest, op, status), (_, bytes)) in &counted.totals {
            if status == "ok" {
                out.sample(
                    "ferry_transferred_bytes_total",
                    &[("pipeline", pipeline), ("destination", dest), ("op", op)],
                    *bytes as f64,
                );
            }
        }
        Ok(out.0)
    }
}

#[derive(Default)]
struct Text(String);

impl Text {
    fn family(&mut self, name: &str, kind: &str, help: &str) {
        let _ = writeln!(self.0, "# HELP {name} {help}\n# TYPE {name} {kind}");
    }

    fn sample(&mut self, name: &str, labels: &[(&str, &str)], value: f64) {
        self.0.push_str(name);
        if !labels.is_empty() {
            let labels: Vec<String> = labels
                .iter()
                .map(|(key, value)| {
                    let value = value.replace('\\', r"\\").replace('"', "\\\"").replace('\n', r"\n");
                    format!("{key}=\"{value}\"")
                })
                .collect();
            let _ = write!(self.0, "{{{}}}", labels.join(","));
        }
        let _ = writeln!(self.0, " {value}");
    }
}

/// Answer requests on `listener` until `stop`.
pub async fn serve(listener: TcpListener, metrics: Arc<Metrics>, stop: CancellationToken) {
    loop {
        let stream = tokio::select! {
            _ = stop.cancelled() => return,
            accepted = listener.accept() => match accepted {
                Ok((stream, _)) => stream,
                Err(e) => {
                    tracing::warn!("metrics: {e}");
                    tokio::time::sleep(Duration::from_millis(100)).await;
                    continue;
                }
            },
        };
        let metrics = metrics.clone();
        tokio::spawn(async move {
            let _ = tokio::time::timeout(REQUEST_TIMEOUT, respond(stream, &metrics)).await;
        });
    }
}

async fn respond(mut stream: TcpStream, metrics: &Metrics) -> std::io::Result<()> {
    let mut head = Vec::new();
    let mut buf = [0u8; 1024];
    while !head.windows(4).any(|w| w == b"\r\n\r\n") && head.len() < MAX_HEAD {
        let n = stream.read(&mut buf).await?;
        if n == 0 {
            break;
        }
        head.extend_from_slice(&buf[..n]);
    }
    let head = String::from_utf8_lossy(&head);
    let mut request = head.lines().next().unwrap_or_default().split_whitespace();
    let method = request.next().unwrap_or_default();
    let path = request.next().unwrap_or_default().split('?').next().unwrap_or_default();
    const TEXT: &str = "text/plain; charset=utf-8";
    let (status, content_type, body) = match (method, path) {
        ("GET", "/metrics") => match metrics.render() {
            Ok(text) => ("200 OK", "text/plain; version=0.0.4; charset=utf-8", text),
            Err(e) => ("500 Internal Server Error", TEXT, format!("{e:#}\n")),
        },
        ("GET", "/healthz") => match metrics.healthy() {
            Ok(()) => ("200 OK", TEXT, "ok\n".to_string()),
            Err(e) => ("503 Service Unavailable", TEXT, format!("{e:#}\n")),
        },
        ("GET", _) => (
            "404 Not Found",
            TEXT,
            "ferry serves /metrics and /healthz\n".to_string(),
        ),
        _ => ("405 Method Not Allowed", TEXT, "only GET\n".to_string()),
    };
    let response = format!(
        "HTTP/1.1 {status}\r\nContent-Type: {content_type}\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
        body.len()
    );
    stream.write_all(response.as_bytes()).await?;
    stream.shutdown().await
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::journal::NewTransfer;

    fn upload(journal: &Journal, pipeline: &str, dest: &str, status: &str, bytes: i64) {
        journal
            .add_transfer(&NewTransfer {
                pipeline,
                unit_id: None,
                rel: "u/a.tif",
                dest: Some(dest),
                op: "upload",
                status,
                src: "/data/u/a.tif",
                dst: "s3://b/u/a.tif",
                bytes: Some(bytes),
                duration_ms: Some(5),
                error: None,
                at: 1.0,
            })
            .unwrap();
    }

    fn metrics() -> Metrics {
        Metrics::new(
            "pc1",
            vec![("inst".into(), vec!["ceph".into()])],
            Arc::new(Mutex::new(Journal::in_memory().unwrap())),
            1_700_000_000.5,
        )
    }

    #[test]
    fn counters_add_up_across_scrapes() {
        let m = metrics();
        upload(&m.journal(), "inst", "ceph", "ok", 100);
        upload(&m.journal(), "inst", "ceph", "failed", 0);
        let first = m.render().unwrap();
        assert!(first.contains("ferry_build_info{version=\""), "{first}");
        assert!(first.contains("ferry_start_time_seconds 1700000000.5\n"), "{first}");
        assert!(
            first.contains("ferry_units{pipeline=\"inst\",state=\"open\"} 0\n"),
            "{first}"
        );
        assert!(first.contains("# TYPE ferry_transfers_total counter\n"), "{first}");
        assert!(
            first.contains(
                "ferry_transfers_total{pipeline=\"inst\",destination=\"ceph\",op=\"upload\",status=\"failed\"} 1\n"
            ),
            "{first}"
        );

        upload(&m.journal(), "inst", "ceph", "ok", 50);
        let second = m.render().unwrap();
        assert!(
            second.contains(
                "ferry_transfers_total{pipeline=\"inst\",destination=\"ceph\",op=\"upload\",status=\"ok\"} 2\n"
            ),
            "{second}"
        );
        assert!(
            second
                .contains("ferry_transferred_bytes_total{pipeline=\"inst\",destination=\"ceph\",op=\"upload\"} 150\n"),
            "{second}"
        );
        assert_eq!(second, m.render().unwrap(), "a scrape counts each transfer once");
    }

    #[test]
    fn label_values_are_escaped() {
        let mut text = Text::default();
        text.sample("m", &[("unit", "a\"b\\c\nd")], 1.0);
        assert_eq!(text.0, "m{unit=\"a\\\"b\\\\c\\nd\"} 1\n");
    }

    async fn get(addr: std::net::SocketAddr, path: &str) -> String {
        let mut stream = TcpStream::connect(addr).await.unwrap();
        stream
            .write_all(format!("GET {path} HTTP/1.1\r\nHost: ferry\r\n\r\n").as_bytes())
            .await
            .unwrap();
        let mut response = String::new();
        stream.read_to_string(&mut response).await.unwrap();
        response
    }

    #[tokio::test]
    async fn it_answers_metrics_health_and_nothing_else() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let stop = CancellationToken::new();
        let server = tokio::spawn(serve(listener, Arc::new(metrics()), stop.clone()));

        let scraped = get(addr, "/metrics").await;
        assert!(scraped.starts_with("HTTP/1.1 200 OK\r\n"), "{scraped}");
        assert!(scraped.contains("Content-Type: text/plain; version=0.0.4"), "{scraped}");
        assert!(scraped.contains("\r\n\r\n# HELP ferry_build_info"), "{scraped}");
        assert!(get(addr, "/healthz").await.ends_with("\r\n\r\nok\n"));
        assert!(get(addr, "/").await.starts_with("HTTP/1.1 404"));

        stop.cancel();
        server.await.unwrap();
    }
}
