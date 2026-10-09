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
        let options = AgentOptions {
            clock: Arc::new(SystemClock),
            stream: StreamOptions {
                digest_every: Duration::from_secs(3600),
                grace: Duration::from_secs(5),
            },
            ..h.options(false)
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
    tokio::time::sleep(Duration::from_secs(1)).await; // the watcher is running
    write_at(&h.src.join("u__1/a.tif"), b"data", wall());
    eventually("the unit registered", Duration::from_secs(30), || registered(&h) == 1).await;
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
