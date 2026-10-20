//! `ferry watch` in real time, with settle and idle windows under a second.

mod common;

use std::io::Write;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::{Duration, Instant};

use common::*;
use ferry::config::Config;
use ferry::manifest::EventKind::Register;
use ferry::runner::{Agent, AgentOptions};
use ferry::stream::StreamOptions;
use ferry::util::SystemClock;
use tokio_util::sync::CancellationToken;

async fn eventually(what: &str, within: Duration, mut check: impl FnMut() -> bool) {
    let deadline = Instant::now() + within;
    while Instant::now() < deadline {
        if check() {
            return;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    panic!("not true after {within:?}: {what}");
}

struct Watching {
    stop: CancellationToken,
    task: tokio::task::JoinHandle<anyhow::Result<()>>,
}

impl Watching {
    fn start(h: &Harness, cfg: &Config) -> Self {
        Self::start_with(cfg, h.options(false))
    }

    fn start_with(cfg: &Config, base: AgentOptions) -> Self {
        let options = AgentOptions {
            clock: Arc::new(SystemClock),
            stream: StreamOptions {
                digest_every: Duration::from_secs(3600),
                grace: Duration::from_secs(5),
            },
            ..base
        };
        let agent = Arc::new(Agent::open(cfg.clone(), options).unwrap());
        let stop = CancellationToken::new();
        let token = stop.clone();
        let task = tokio::spawn(async move { agent.watch(token).await });
        Self { stop, task }
    }

    async fn stop(self) {
        self.stop.cancel();
        tokio::time::timeout(Duration::from_secs(30), self.task)
            .await
            .unwrap()
            .unwrap()
            .unwrap();
    }
}

fn registered(h: &Harness) -> usize {
    h.recorder.manifests(Register).len()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_stream_posts_the_receipt_once_the_unit_is_moved() {
    let h = Harness::new();
    let cfg = h.config(&format!(
        "    settle: 300ms\n    unit_idle: 600ms\n    poll_interval: 100ms\n    full_scan_interval: 60s\n    watch: poll\n{}",
        h.backup_extra("move")
    ));
    write_at(&h.src.join("u__1/a.tif"), b"data", wall());
    let inbox = Inbox::receipts();

    let watching = Watching::start_with(&cfg, h.notifying(&inbox));
    eventually("the receipt posted", Duration::from_secs(15), || {
        !inbox.receipts_seen().is_empty()
    })
    .await;
    watching.stop().await;

    let receipts = inbox.receipts_seen();
    assert_eq!(receipts.len(), 1);
    assert_eq!(
        (receipts[0].original.action.as_str(), receipts[0].original.done),
        ("move", true)
    );
    assert!(h.backup.join("u__1/a.tif").exists());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn each_file_ships_as_it_settles_and_the_unit_registers_once_done() {
    let h = Harness::new();
    let cfg = h.config(
        "    upload: file\n    settle: 500ms\n    unit_idle: 1500ms\n    poll_interval: 100ms\n    full_scan_interval: 60s\n    watch: poll\n",
    );
    let unit = h.src.join("u__1");
    write_at(&unit.join("first.tif"), b"first", wall());
    let writing = Arc::new(AtomicBool::new(true));
    let instrument = {
        let (writing, path) = (writing.clone(), unit.join("growing.tif"));
        std::thread::spawn(move || {
            let mut file = std::fs::OpenOptions::new()
                .create(true)
                .append(true)
                .open(path)
                .unwrap();
            while writing.load(Ordering::SeqCst) {
                file.write_all(b"x").unwrap();
                file.flush().unwrap();
                std::thread::sleep(Duration::from_millis(50));
            }
        })
    };

    let watching = Watching::start(&h, &cfg);
    eventually("first.tif shipped", Duration::from_secs(15), || {
        h.remote.join("u__1/first.tif").exists()
    })
    .await;
    tokio::time::sleep(Duration::from_secs(1)).await;
    assert!(
        !h.remote.join("u__1/growing.tif").exists(),
        "a file still growing stays home"
    );
    assert_eq!(registered(&h), 0);

    writing.store(false, Ordering::SeqCst);
    instrument.join().unwrap();
    let size = std::fs::metadata(unit.join("growing.tif")).unwrap().len() as usize;
    eventually("the unit registered", Duration::from_secs(15), || registered(&h) == 1).await;
    watching.stop().await;

    assert_eq!(
        std::fs::read(h.remote.join("u__1/growing.tif")).unwrap(),
        vec![b'x'; size]
    );
    let paths: Vec<String> = h.recorder.manifests(Register)[0]
        .files
        .iter()
        .map(|f| f.path.clone())
        .collect();
    assert_eq!(paths, ["first.tif", "growing.tif"]);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn whole_unit_uploads_wait_until_the_unit_is_complete() {
    let h = Harness::new();
    let cfg =
        h.config("    upload: unit\n    settle: 200ms\n    unit_idle: 3s\n    poll_interval: 100ms\n    watch: poll\n");
    write_at(&h.src.join("u__1/a.tif"), b"data", wall());
    age_tree(&h.src, wall());

    let watching = Watching::start(&h, &cfg);
    tokio::time::sleep(Duration::from_secs(1)).await; // settled, but not yet quiet for 3 s
    assert!(files_under(&h.remote).is_empty());
    eventually("the unit registered", Duration::from_secs(15), || registered(&h) == 1).await;
    watching.stop().await;

    assert_eq!(data_files(&h.remote), tree(&[("u__1/a.tif", b"data")]));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn file_events_wake_a_stream_long_before_its_next_poll() {
    let h = Harness::new();
    let cfg = h.config(
        "    upload: file\n    settle: 200ms\n    unit_idle: 500ms\n    poll_interval: 10m\n    full_scan_interval: 10m\n    watch: events\n",
    );

    let watching = Watching::start(&h, &cfg);
    // The watcher starts in the background, and an event made before it is
    // listening is lost. Keep making fresh ones until the file has gone up:
    // with a 10-minute poll, only an event can get it there in time.
    let file = h.src.join("u__1/a.tif");
    let deadline = Instant::now() + Duration::from_secs(30);
    let mut last_event: Option<Instant> = None;
    while registered(&h) == 0 {
        assert!(Instant::now() < deadline, "the unit did not register within 30s");
        let uploaded = h.remote.join("u__1/a.tif").exists();
        if !uploaded && last_event.is_none_or(|t| t.elapsed() > Duration::from_secs(3)) {
            write_at(&file, b"data", wall());
            last_event = Some(Instant::now());
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    watching.stop().await;

    assert_eq!(data_files(&h.remote), tree(&[("u__1/a.tif", b"data")]));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_stream_wakes_when_a_file_settles_not_at_its_next_poll() {
    let h = Harness::new();
    let cfg = h.config(
        "    upload: file\n    settle: 500ms\n    unit_idle: 500ms\n    poll_interval: 10m\n    full_scan_interval: 10m\n    watch: poll\n",
    );
    write_at(&h.src.join("u__1/a.tif"), b"data", wall());
    age_tree(&h.src, wall());

    let watching = Watching::start(&h, &cfg);
    eventually("the unit registered", Duration::from_secs(10), || registered(&h) == 1).await;
    watching.stop().await;

    assert_eq!(data_files(&h.remote), tree(&[("u__1/a.tif", b"data")]));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_cron_pipeline_runs_passes_on_its_schedule_inside_watch() {
    let h = Harness::new();
    let cfg = h.config("    mode: cron\n    cron: '* * * * * *'\n");
    write(&h.src.join("u__1/a.tif"), b"a");
    age_tree(&h.src, hour_ago());

    let watching = Watching::start(&h, &cfg);
    eventually("u__1 registered", Duration::from_secs(15), || registered(&h) == 1).await;
    write(&h.src.join("u__2/b.tif"), b"b");
    age_tree(&h.src.join("u__2"), hour_ago());
    eventually("u__2 registered", Duration::from_secs(15), || registered(&h) == 2).await;
    watching.stop().await;

    let mut units: Vec<String> = h.recorder.manifests(Register).into_iter().map(|m| m.unit).collect();
    units.sort();
    assert_eq!(units, ["u__1", "u__2"]);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn watch_serves_metrics_from_the_journal() {
    async fn get(port: u16, path: &str) -> String {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};
        let Ok(mut stream) = tokio::net::TcpStream::connect(("127.0.0.1", port)).await else {
            return String::new();
        };
        let request = format!("GET {path} HTTP/1.1\r\nHost: ferry\r\n\r\n");
        let mut response = String::new();
        if stream.write_all(request.as_bytes()).await.is_ok() {
            let _ = stream.read_to_string(&mut response).await;
        }
        response
    }

    let h = Harness::new();
    let mut cfg = h.config("");
    let port = std::net::TcpListener::bind("127.0.0.1:0")
        .unwrap()
        .local_addr()
        .unwrap()
        .port();
    cfg.metrics = Some(ferry::config::MetricsConfig {
        listen: format!("127.0.0.1:{port}"),
    });
    write(&h.src.join("u__1/a.tif"), b"alpha");
    let watching = Watching::start(&h, &cfg);

    let uploaded = "ferry_transfers_total{pipeline=\"inst\",destination=\"remote\",op=\"upload\",status=\"ok\"} 1\n";
    let deadline = Instant::now() + Duration::from_secs(30);
    let mut body = get(port, "/metrics").await;
    while !body.contains(uploaded) && Instant::now() < deadline {
        tokio::time::sleep(Duration::from_millis(100)).await;
        body = get(port, "/metrics").await;
    }
    assert!(body.contains(uploaded), "{body}");
    assert!(body.contains("ferry_pending_files{pipeline=\"inst\"} 0\n"), "{body}");
    assert!(get(port, "/healthz").await.ends_with("\r\n\r\nok\n"));
    watching.stop().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn stopping_lets_the_daemon_exit_promptly() {
    let h = Harness::new();
    let cfg = h.config("    poll_interval: 10m\n    full_scan_interval: 10m\n");
    let watching = Watching::start(&h, &cfg);
    tokio::time::sleep(Duration::from_millis(500)).await;
    let started = Instant::now();
    watching.stop().await;
    assert!(
        started.elapsed() < Duration::from_secs(5),
        "took {:?}",
        started.elapsed()
    );
}
