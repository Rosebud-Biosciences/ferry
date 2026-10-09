//! Against a real S3 API: set `FERRY_TEST_S3_ENDPOINT` (CI runs SeaweedFS,
//! which like Ceph RGW is reached path-style at a custom endpoint),
//! `FERRY_TEST_S3_KEY`, `FERRY_TEST_S3_SECRET` and `FERRY_TEST_S3_BUCKET` (an
//! existing bucket). Without them these tests pass vacuously.

mod common;

use common::*;
use ferry::manifest::EventKind::Register;
use object_store::ObjectStoreExt;

struct S3 {
    endpoint: String,
    key: String,
    secret: String,
    bucket: String,
}

fn s3() -> Option<S3> {
    let var = |name: &str| std::env::var(name).ok().filter(|v| !v.is_empty());
    Some(S3 {
        endpoint: var("FERRY_TEST_S3_ENDPOINT")?,
        key: var("FERRY_TEST_S3_KEY").unwrap_or_else(|| "ferry".into()),
        secret: var("FERRY_TEST_S3_SECRET").unwrap_or_else(|| "ferry-secret".into()),
        bucket: var("FERRY_TEST_S3_BUCKET").unwrap_or_else(|| "ferry-test".into()),
    })
}

macro_rules! require_s3 {
    () => {
        match s3() {
            Some(s3) => s3,
            None => {
                eprintln!("skipped: set FERRY_TEST_S3_ENDPOINT to run the S3 tests");
                return;
            }
        }
    };
}

/// A unique prefix per test, so runs never see each other's objects.
fn run_prefix() -> String {
    format!(
        "run-{:x}",
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    )
}

/// A config whose `remote` store is the S3 bucket (under a fresh prefix) and
/// whose `mirror` store stays local.
fn s3_config(h: &Harness, s3: &S3, prefix: &str, destinations: &str, extra: &str) -> ferry::config::Config {
    let mut cfg = h.config_with(destinations, "[manifest, {name: rec, exec: [unused]}]", extra);
    let store = cfg.stores.get_mut("remote").unwrap();
    store.url = format!("s3://{}/{prefix}", s3.bucket);
    store.endpoint = Some(s3.endpoint.clone());
    store.region = Some("us-east-1".into());
    store.allow_http = true;
    store.options.insert("aws_access_key_id".into(), s3.key.clone());
    store.options.insert("aws_secret_access_key".into(), s3.secret.clone());
    cfg
}

async fn read(cfg: &ferry::config::Config, rel: &str) -> Vec<u8> {
    let store = ferry::stores::open_store("remote", &cfg.stores["remote"]).unwrap();
    let location = ferry::stores::Location::root(store);
    location
        .store
        .inner
        .get(&location.key(rel).unwrap())
        .await
        .unwrap()
        .bytes()
        .await
        .unwrap()
        .to_vec()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_pipeline_ships_small_and_multipart_files_to_s3_and_a_mirror() {
    let s3 = require_s3!();
    let h = Harness::new();
    let prefix = run_prefix();
    let cfg = s3_config(
        &h,
        &s3,
        &prefix,
        "[{store: remote, verify: sha256}, mirror]",
        "    limits: {single_put_max: 1MiB, part_size: 5MiB}\n",
    );
    let big: Vec<u8> = (0..11 * 1024 * 1024 + 7).map(|i| (i % 253) as u8).collect();
    write(&h.src.join("plate__1/small.tif"), b"small");
    write(&h.src.join("plate__1/big.raw"), &big);
    age_tree(&h.src, hour_ago());

    let report = h.pass(&cfg).await;

    assert_eq!(report.failures(), 0, "{report:?}");
    assert_eq!(report.finalized, ["plate__1"]);
    assert_eq!(read(&cfg, "plate__1/small.tif").await, b"small");
    assert_eq!(read(&cfg, "plate__1/big.raw").await, big);
    assert_eq!(std::fs::read(h.mirror.join("plate__1/big.raw")).unwrap(), big);
    let manifest: serde_json::Value =
        serde_json::from_slice(&read(&cfg, "plate__1/_ferry_manifest.json").await).unwrap();
    assert_eq!(manifest["files"].as_array().unwrap().len(), 2);
    let registered = h.recorder.manifests(Register);
    let big_entry = registered[0].files.iter().find(|f| f.path == "big.raw").unwrap();
    assert_eq!(big_entry.sha256.as_deref(), Some(sha256(&big).as_str()));
    assert_eq!(big_entry.copies[0].verified.as_deref(), Some("sha256"));
    assert!(big_entry.copies[0].etag.is_some());
    assert!(
        big_entry
            .uri
            .starts_with(&format!("s3://{}/{prefix}/plate__1/", s3.bucket)),
        "{}",
        big_entry.uri
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_lost_journal_adopts_what_s3_already_holds() {
    let s3 = require_s3!();
    let h = Harness::new();
    let prefix = run_prefix();
    let cfg = s3_config(&h, &s3, &prefix, "[remote]", "");
    write(&h.src.join("u__1/a.tif"), b"alpha");
    write(&h.src.join("u__1/b.tif"), b"bravo");
    age_tree(&h.src, hour_ago());
    h.pass(&cfg).await;

    std::fs::remove_dir_all(&h.state).unwrap();
    let report = h.pass(&cfg).await;

    assert_eq!((report.adopted, report.uploaded), (2, 0));
}
