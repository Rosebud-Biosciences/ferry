//! A pipeline's pass end to end, against local-directory stores and a fake clock.

mod common;

use common::*;
use ferry::config::Level;
use ferry::journal::Journal;
use ferry::manifest::EventKind::{Failures, Finalize, Register};
use ferry::runner::{Agent, AgentOptions};
use tokio_util::sync::CancellationToken;

#[tokio::test]
async fn a_settled_unit_is_uploaded_registered_and_moved() {
    let h = Harness::new();
    let cfg = h.config_with(
        "[remote]",
        "[manifest, {name: rec, exec: [unused]}]",
        &h.backup_extra("move"),
    );
    write(&h.src.join("plateA__1/img/a.tif"), b"alpha");
    write(&h.src.join("plateA__1/b.tif"), b"bravo");
    age_tree(&h.src, hour_ago());

    let report = h.pass(&cfg).await;

    let both = tree(&[("plateA__1/b.tif", b"bravo"), ("plateA__1/img/a.tif", b"alpha")]);
    assert_eq!(data_files(&h.remote), both);
    assert_eq!(files_under(&h.backup), both);
    assert!(!h.src.join("plateA__1").exists(), "emptied, then pruned");

    let registered = h.recorder.manifests(Register);
    assert_eq!(registered.len(), 1);
    let unit_uri = format!(
        "{}/plateA__1",
        url::Url::from_directory_path(&h.remote)
            .unwrap()
            .as_str()
            .trim_end_matches('/')
    );
    assert_eq!(registered[0].uri, unit_uri);
    assert_eq!(registered[0].generation, 1);
    let hashes: Vec<(String, Option<String>)> = registered[0]
        .files
        .iter()
        .map(|f| (f.path.clone(), f.sha256.clone()))
        .collect();
    assert_eq!(
        hashes,
        [
            ("b.tif".into(), Some(sha256(b"bravo"))),
            ("img/a.tif".into(), Some(sha256(b"alpha")))
        ]
    );
    let mut sent: Vec<(String, String, String)> = h
        .recorder
        .transfers(Register)
        .into_iter()
        .map(|t| (t.op, t.status, t.rel_path))
        .collect();
    sent.sort();
    assert_eq!(
        sent,
        [
            ("upload".into(), "ok".into(), "plateA__1/b.tif".into()),
            ("upload".into(), "ok".into(), "plateA__1/img/a.tif".into())
        ]
    );
    let finalized = h.recorder.manifests(Finalize);
    let backups: Vec<Option<String>> = finalized[0].files.iter().map(|f| f.backup_path.clone()).collect();
    assert_eq!(
        backups,
        [
            Some(h.backup.join("plateA__1").join("b.tif").display().to_string()),
            Some(h.backup.join("plateA__1").join("img/a.tif").display().to_string())
        ]
    );
    // The manifest registrar rewrote the manifest on finalize, backup paths and all.
    let stored: serde_json::Value =
        serde_json::from_slice(&std::fs::read(h.remote.join("plateA__1/_ferry_manifest.json")).unwrap()).unwrap();
    assert_eq!(stored["schema"], "ferry.manifest/v1");
    assert_eq!(stored["files"][0]["backup_path"].as_str(), backups[0].as_deref());
    assert_eq!(report.registered, ["plateA__1"]);
    assert_eq!(report.finalized, ["plateA__1"]);
    assert_eq!(report.failures(), 0);
    assert_eq!(h.unit_state(&cfg, "plateA__1").as_deref(), Some("finalized"));
}

#[tokio::test]
async fn files_still_being_written_wait_for_settle_and_idle() {
    let h = Harness::new();
    let cfg = h.config("    settle: 60s\n    unit_idle: 120s\n");
    let now = ferry::util::Clock::now(&*h.clock);
    write_at(&h.src.join("u__1/a.tif"), b"data", now);
    age_tree(&h.src, now);

    let report = h.pass(&cfg).await;
    assert!(files_under(&h.remote).is_empty());
    assert_eq!(report.waiting["u__1"], "waiting: 1 of 1 file(s) still settling");

    h.clock.advance(61.0); // settled, but only 61 s quiet of the 120 s required
    let report = h.pass(&cfg).await;
    assert!(files_under(&h.remote).is_empty());
    assert!(
        report.waiting["u__1"].contains("complete after 2m without changes"),
        "{:?}",
        report.waiting
    );
    assert!(h.recorder.manifests(Register).is_empty());

    h.clock.advance(60.0);
    let report = h.pass(&cfg).await;
    assert_eq!(data_files(&h.remote), tree(&[("u__1/a.tif", b"data")]));
    assert_eq!(h.recorder.manifests(Register).len(), 1);
    assert!(report.waiting.is_empty(), "{:?}", report.waiting);
}

#[tokio::test]
async fn upload_unit_sends_nothing_until_the_whole_unit_is_complete() {
    let h = Harness::new();
    let cfg = h.config("    upload: unit\n    settle: 60s\n    unit_idle: 0s\n");
    let now = ferry::util::Clock::now(&*h.clock);
    write(&h.src.join("u__1/done.tif"), b"done");
    write_at(&h.src.join("u__1/busy.tif"), b"busy", now);

    h.pass(&cfg).await;
    assert!(files_under(&h.remote).is_empty());

    h.clock.advance(61.0);
    h.pass(&cfg).await;
    assert_eq!(
        data_files(&h.remote),
        tree(&[("u__1/busy.tif", b"busy"), ("u__1/done.tif", b"done")])
    );
    let paths: Vec<String> = h.recorder.manifests(Register)[0]
        .files
        .iter()
        .map(|f| f.path.clone())
        .collect();
    assert_eq!(paths, ["busy.tif", "done.tif"]);
}

#[tokio::test]
async fn upload_file_ships_settled_files_but_registers_only_once_complete() {
    let h = Harness::new();
    let cfg = h.config("    upload: file\n    settle: 60s\n    unit_idle: 0s\n");
    let now = ferry::util::Clock::now(&*h.clock);
    write(&h.src.join("u__1/done.tif"), b"done");
    write_at(&h.src.join("u__1/busy.tif"), b"busy", now);

    let report = h.pass(&cfg).await;
    assert_eq!(data_files(&h.remote), tree(&[("u__1/done.tif", b"done")]));
    assert_eq!(report.uploaded, 1);
    assert!(h.recorder.manifests(Register).is_empty());

    h.clock.advance(61.0);
    let report = h.pass(&cfg).await;
    assert_eq!(report.uploaded, 1, "only the newly settled file");
    let paths: Vec<String> = h.recorder.manifests(Register)[0]
        .files
        .iter()
        .map(|f| f.path.clone())
        .collect();
    assert_eq!(paths, ["busy.tif", "done.tif"]);
}

#[tokio::test]
async fn a_marker_completes_a_unit_without_waiting_for_idle() {
    let h = Harness::new();
    let cfg = h.config("    unit_idle: 1h\n    marker: done.txt\n    include: ['*.tif']\n");
    write(&h.src.join("u__1/a.tif"), b"data");

    h.pass(&cfg).await;
    assert!(h.recorder.manifests(Register).is_empty());

    write(&h.src.join("u__1/done.txt"), b"");
    h.pass(&cfg).await;
    let registered = h.recorder.manifests(Register);
    assert_eq!(
        registered[0].files.iter().map(|f| f.path.as_str()).collect::<Vec<_>>(),
        ["a.tif"]
    );
    assert_eq!(data_files(&h.remote), tree(&[("u__1/a.tif", b"data")]));
}

#[tokio::test]
async fn a_unit_still_being_written_at_pass_time_is_never_registered_partially() {
    // The nightly race: a pass that catches an acquisition mid-write must not
    // register the files present so far -- the ones written later would never
    // be uploaded.
    let h = Harness::new();
    let cfg = h.config("    settle: 60s\n    unit_idle: 600s\n");
    let now = ferry::util::Clock::now(&*h.clock);
    write(&h.src.join("u__1/t001.tif"), b"1");
    write_at(&h.src.join("u__1/t002.tif"), b"2", now);
    set_mtime(&h.src.join("u__1"), now);

    h.pass(&cfg).await;
    h.clock.advance(300.0);
    write_at(&h.src.join("u__1/t003.tif"), b"3", now + 300.0);
    set_mtime(&h.src.join("u__1"), now + 300.0);
    h.pass(&cfg).await;
    assert!(h.recorder.manifests(Register).is_empty());

    h.clock.advance(601.0);
    h.pass(&cfg).await;
    let registered = h.recorder.manifests(Register);
    let paths: Vec<&str> = registered[0].files.iter().map(|f| f.path.as_str()).collect();
    assert_eq!(paths, ["t001.tif", "t002.tif", "t003.tif"]);
}

#[tokio::test]
async fn a_file_added_after_registration_registers_the_unit_again() {
    let h = Harness::new();
    let cfg = h.config("");
    write(&h.src.join("u__1/a.tif"), b"a");
    write(&h.src.join("u__1/b.tif"), b"b");
    age_tree(&h.src, hour_ago());
    h.pass(&cfg).await;

    write(&h.src.join("u__1/c.tif"), b"c");
    h.clock.advance(10.0);
    let report = h.pass(&cfg).await;

    assert_eq!(report.reopened, [("u__1".to_string(), "new file c.tif".to_string())]);
    let registered = h.recorder.manifests(Register);
    assert_eq!(registered.iter().map(|m| m.generation).collect::<Vec<_>>(), [1, 2]);
    assert_eq!(
        registered[1].files.iter().map(|f| f.path.as_str()).collect::<Vec<_>>(),
        ["a.tif", "b.tif", "c.tif"]
    );
    assert_eq!(report.uploaded, 1, "only c.tif went up again");
    assert_eq!(h.unit_state(&cfg, "u__1").as_deref(), Some("finalized"));
}

#[tokio::test]
async fn finalize_waits_for_a_required_registrar() {
    let h = Harness::new();
    let cfg = h.config(&format!("{}    retry: {{initial: 30s}}\n", h.backup_extra("move")));
    h.recorder.fail(1);
    write(&h.src.join("u__1/a.tif"), b"data");
    age_tree(&h.src, hour_ago());

    let report = h.pass(&cfg).await;
    assert_eq!(report.delivery_failures, 1);
    assert_eq!(data_files(&h.remote), tree(&[("u__1/a.tif", b"data")]));
    assert!(h.src.join("u__1/a.tif").exists());
    assert!(files_under(&h.backup).is_empty());
    assert_eq!(h.unit_state(&cfg, "u__1").as_deref(), Some("registering"));

    h.clock.advance(31.0);
    let report = h.pass(&cfg).await;
    assert_eq!(report.failures(), 0);
    assert_eq!(files_under(&h.backup), tree(&[("u__1/a.tif", b"data")]));
    assert!(!h.src.join("u__1").exists());
    assert_eq!(h.recorder.kinds(), [Register, Finalize]);
}

#[tokio::test]
async fn an_optional_registrar_never_holds_up_finalize() {
    let h = Harness::new();
    let cfg = h.config_with(
        "[remote]",
        "[{name: rec, exec: [unused], required: false}]",
        &format!("{}    retry: {{initial: 30s}}\n", h.backup_extra("move")),
    );
    h.recorder.fail(1);
    write(&h.src.join("u__1/a.tif"), b"data");
    age_tree(&h.src, hour_ago());

    h.pass(&cfg).await;
    assert_eq!(files_under(&h.backup), tree(&[("u__1/a.tif", b"data")]));
    assert!(h.recorder.kinds().is_empty());

    h.clock.advance(31.0);
    h.pass(&cfg).await;
    // The finalize waited behind the failed register, so the order still holds.
    assert_eq!(h.recorder.kinds(), [Register, Finalize]);
}

#[tokio::test]
async fn files_already_in_the_store_are_adopted_not_uploaded_again() {
    let h = Harness::new();
    write(&h.src.join("u__1/a.tif"), b"alpha");
    write(&h.src.join("u__1/b.tif"), b"bravo");
    age_tree(&h.src, hour_ago());
    write(&h.remote.join("u__1/a.tif"), b"alpha");
    write(&h.remote.join("u__1/b.tif"), b"different size");
    let cfg = h.config("    adopt: size\n");

    let report = h.pass(&cfg).await;

    assert_eq!((report.adopted, report.uploaded), (1, 1));
    assert_eq!(std::fs::read(h.remote.join("u__1/b.tif")).unwrap(), b"bravo");
    let mut ops: Vec<(String, String)> = h
        .recorder
        .transfers(Register)
        .into_iter()
        .map(|t| (t.op, t.rel_path))
        .collect();
    ops.sort();
    assert_eq!(
        ops,
        [
            ("adopt".into(), "u__1/a.tif".into()),
            ("upload".into(), "u__1/b.tif".into())
        ]
    );
}

#[tokio::test]
async fn a_hash_verified_destination_never_adopts_what_it_cannot_check() {
    let h = Harness::new();
    write(&h.src.join("u__1/a.tif"), b"alpha");
    age_tree(&h.src, hour_ago());
    // Same key and size, different bytes: adopting it by size would lose data.
    write(&h.mirror.join("u__1/a.tif"), b"ALPHA");
    let cfg = h.config_with(
        "[remote, {store: mirror, verify: sha256}]",
        "[{name: rec, exec: [unused]}]",
        "    adopt: size\n",
    );

    let report = h.pass(&cfg).await;

    assert_eq!(report.adopted, 0);
    assert_eq!(std::fs::read(h.mirror.join("u__1/a.tif")).unwrap(), b"alpha");
}

#[tokio::test]
async fn by_default_an_object_already_there_is_uploaded_over() {
    let h = Harness::new();
    write(&h.src.join("u__1/a.tif"), b"alpha");
    age_tree(&h.src, hour_ago());
    write(&h.remote.join("u__1/a.tif"), b"ALPHA");
    let cfg = h.config("");

    let report = h.pass(&cfg).await;

    assert_eq!((report.adopted, report.uploaded), (0, 1));
    assert_eq!(std::fs::read(h.remote.join("u__1/a.tif")).unwrap(), b"alpha");
}

#[tokio::test]
async fn hash_adoption_counts_only_what_reads_back_the_same() {
    let h = Harness::new();
    write(&h.src.join("u__1/a.tif"), b"alpha");
    write(&h.src.join("u__1/b.tif"), b"bravo");
    age_tree(&h.src, hour_ago());
    write(&h.remote.join("u__1/a.tif"), b"alpha");
    write(&h.remote.join("u__1/b.tif"), b"BRAVO");
    let cfg = h.config(&format!("    adopt: sha256\n{}", h.backup_extra("move")));

    let report = h.pass(&cfg).await;

    assert_eq!((report.adopted, report.uploaded), (1, 1));
    assert_eq!(std::fs::read(h.remote.join("u__1/b.tif")).unwrap(), b"bravo");
    let registered = h.recorder.manifests(Register);
    let verified: Vec<Option<String>> = registered[0]
        .files
        .iter()
        .map(|f| f.copies[0].verified.clone())
        .collect();
    assert_eq!(verified, [Some("sha256".into()), Some("size".into())]);
    assert!(
        h.backup.join("u__1/a.tif").exists(),
        "moved on the strength of the readback"
    );
}

#[tokio::test]
async fn a_restarted_agent_resumes_without_uploading_or_registering_twice() {
    let h = Harness::new();
    let cfg = h.config("");
    write(&h.src.join("u__1/a.tif"), b"data");
    write(&h.src.join("u__1/b.tif"), b"data");
    age_tree(&h.src, hour_ago());
    {
        let agent = h.agent(&cfg);
        let engine = agent.engines[0].clone();
        engine.scan(None).await.unwrap();
        engine.evaluate().unwrap();
        for candidate in engine.upload_candidates().unwrap() {
            engine.upload(candidate).await.unwrap();
        }
        // ... and the process dies before registering.
    }

    let report = h.pass(&cfg).await;
    assert_eq!(report.uploaded, 0);
    assert_eq!(
        h.recorder
            .manifests(Register)
            .iter()
            .map(|m| m.unit.as_str())
            .collect::<Vec<_>>(),
        ["u__1"]
    );
    let report = h.pass(&cfg).await;
    assert!(!report.active(), "{report:?}");
    assert_eq!(h.recorder.manifests(Register).len(), 1);
}

#[tokio::test]
async fn a_file_rewritten_after_upload_is_not_moved_until_resent() {
    let h = Harness::new();
    let cfg = h.config(&h.backup_extra("move"));
    let source = write(&h.src.join("u__1/a.tif"), b"first");
    age_tree(&h.src, hour_ago());
    // The instrument rewrites the file between its upload and the move.
    let target = source.clone();
    *h.recorder.on_register.lock().unwrap() = Some(Box::new(move || {
        write_at(&target, b"rewritten", hour_ago() + 1.0);
    }));

    let report = h.pass(&cfg).await;
    assert_eq!(
        report.reopened,
        [("u__1".to_string(), "a.tif changed after it was uploaded".to_string())]
    );
    assert!(files_under(&h.backup).is_empty());
    assert_eq!(std::fs::read(&source).unwrap(), b"rewritten");

    h.clock.advance(10.0);
    h.pass(&cfg).await;
    assert_eq!(data_files(&h.remote), tree(&[("u__1/a.tif", b"rewritten")]));
    assert_eq!(files_under(&h.backup), tree(&[("u__1/a.tif", b"rewritten")]));
    let generations: Vec<i64> = h.recorder.manifests(Register).iter().map(|m| m.generation).collect();
    assert_eq!(generations, [1, 2]);
}

#[tokio::test]
async fn a_same_size_rewrite_after_upload_is_sent_again() {
    // Instruments often rewrite a file in place (a header update): same size,
    // new bytes, new mtime.
    let h = Harness::new();
    let cfg = h.config("");
    let source = write(&h.src.join("u__1/a.tif"), b"alpha");
    age_tree(&h.src, hour_ago());
    h.pass(&cfg).await;
    assert_eq!(std::fs::read(h.remote.join("u__1/a.tif")).unwrap(), b"alpha");

    write_at(&source, b"ALPHA", hour_ago() + 60.0);
    h.clock.advance(10.0);
    let report = h.pass(&cfg).await;

    assert_eq!(
        report.reopened,
        [("u__1".to_string(), "a.tif changed after upload".to_string())]
    );
    assert_eq!(std::fs::read(h.remote.join("u__1/a.tif")).unwrap(), b"ALPHA");
    let generations: Vec<i64> = h.recorder.manifests(Register).iter().map(|m| m.generation).collect();
    assert_eq!(generations, [1, 2]);
}

#[tokio::test]
async fn delete_removes_the_source_once_every_copy_is_verified() {
    let h = Harness::new();
    let cfg = h.config("    finalize: {action: delete}\n");
    write(&h.src.join("u__1/a.tif"), b"data");
    age_tree(&h.src, hour_ago());

    h.pass(&cfg).await;

    assert_eq!(data_files(&h.remote), tree(&[("u__1/a.tif", b"data")]));
    assert!(!h.src.join("u__1").exists());
    let finalized = h.recorder.manifests(Finalize);
    assert_eq!(finalized[0].files[0].backup_path, None);
    assert_eq!(finalized[0].finalize, "delete");
    let deleted: Vec<String> = h.recorder.transfers(Finalize).into_iter().map(|t| t.op).collect();
    assert_eq!(deleted, ["delete"]);
}

#[tokio::test]
async fn delete_waits_out_its_grace_period() {
    let h = Harness::new();
    let cfg = h.config("    finalize: {action: delete, after: 1h}\n");
    write(&h.src.join("u__1/a.tif"), b"data");
    age_tree(&h.src, hour_ago());

    let report = h.pass(&cfg).await;
    assert!(h.src.join("u__1/a.tif").exists());
    assert!(
        report.waiting["u__1"].starts_with("waiting: finalize in"),
        "{:?}",
        report.waiting
    );

    h.clock.advance(3601.0);
    h.pass(&cfg).await;
    assert!(!h.src.join("u__1").exists());
}

#[tokio::test]
async fn every_destination_gets_a_copy_from_one_read() {
    let h = Harness::new();
    let cfg = h.config_with(
        "[remote, {store: mirror, prefix: archive, verify: sha256}]",
        "[{name: rec, exec: [unused]}]",
        "",
    );
    write(&h.src.join("u__1/a.tif"), b"alpha");
    age_tree(&h.src, hour_ago());

    let report = h.pass(&cfg).await;

    assert_eq!(data_files(&h.remote), tree(&[("u__1/a.tif", b"alpha")]));
    assert_eq!(files_under(&h.mirror), tree(&[("archive/u__1/a.tif", b"alpha")]));
    assert_eq!((report.uploaded, report.copies), (1, 2));
    let registered = h.recorder.manifests(Register);
    let copies: Vec<(String, Option<String>)> = registered[0].files[0]
        .copies
        .iter()
        .map(|c| (c.destination.clone(), c.verified.clone()))
        .collect();
    assert_eq!(
        copies,
        [
            ("remote".into(), Some("size".into())),
            ("mirror".into(), Some("sha256".into()))
        ]
    );
    assert!(
        registered[0].files[0].copies[1]
            .uri
            .ends_with("/mirror/archive/u__1/a.tif")
    );
}

#[tokio::test]
async fn separate_reads_reach_every_destination_too() {
    let h = Harness::new();
    let cfg = h.config_with(
        "[remote, mirror]",
        "[{name: rec, exec: [unused]}]",
        "    fanout: separate\n",
    );
    write(&h.src.join("u__1/a.tif"), b"alpha");
    age_tree(&h.src, hour_ago());

    let report = h.pass(&cfg).await;

    assert_eq!(data_files(&h.remote), tree(&[("u__1/a.tif", b"alpha")]));
    assert_eq!(files_under(&h.mirror), tree(&[("u__1/a.tif", b"alpha")]));
    assert_eq!((report.uploaded, report.copies), (2, 2));
}

/// Make every upload under `unit` to the store at `top` fail: a file where the
/// unit's directory would go.
fn block(top: &std::path::Path, unit: &str) {
    std::fs::write(top.join(unit), b"in the way").unwrap();
}

fn unblock(top: &std::path::Path, unit: &str) {
    std::fs::remove_file(top.join(unit)).unwrap();
}

#[tokio::test]
async fn an_optional_destination_that_fails_holds_up_finalize_but_not_registration() {
    let h = Harness::new();
    let cfg = h.config_with(
        "[remote, {store: mirror, required: false}]",
        "[{name: rec, exec: [unused]}]",
        &format!("{}    retry: {{initial: 60s}}\n", h.backup_extra("move")),
    );
    write(&h.src.join("u__1/a.tif"), b"data");
    age_tree(&h.src, hour_ago());
    block(&h.mirror, "u__1");

    let report = h.pass(&cfg).await;
    assert_eq!(report.registered, ["u__1"]);
    assert_eq!(report.upload_failures, 1);
    assert!(report.finalized.is_empty());
    assert!(h.src.join("u__1/a.tif").exists(), "a destination still lacks it");
    assert!(report.waiting["u__1"].contains("1 to mirror"), "{:?}", report.waiting);
    let copies = &h.recorder.manifests(Register)[0].files[0].copies;
    assert_eq!(copies[1].verified, None);

    unblock(&h.mirror, "u__1");
    h.clock.advance(61.0);
    let report = h.pass(&cfg).await;
    assert_eq!(report.finalized, ["u__1"]);
    assert_eq!(files_under(&h.mirror), tree(&[("u__1/a.tif", b"data")]));
    assert_eq!(files_under(&h.backup), tree(&[("u__1/a.tif", b"data")]));
    let finalized = h.recorder.manifests(Finalize);
    assert_eq!(finalized[0].files[0].copies[1].verified.as_deref(), Some("size"));
    assert_eq!(h.recorder.kinds(), [Register, Failures, Finalize]);
}

#[tokio::test]
async fn a_required_destination_that_fails_holds_up_registration() {
    let h = Harness::new();
    let cfg = h.config_with("[remote, mirror]", "[{name: rec, exec: [unused]}]", "");
    write(&h.src.join("u__1/a.tif"), b"data");
    age_tree(&h.src, hour_ago());
    block(&h.mirror, "u__1");

    let report = h.pass(&cfg).await;

    assert!(report.registered.is_empty());
    assert_eq!(data_files(&h.remote), tree(&[("u__1/a.tif", b"data")]));
    assert_eq!(h.unit_state(&cfg, "u__1").as_deref(), Some("complete"));
}

#[tokio::test]
async fn regex_units_group_a_flat_folder_by_name() {
    let h = Harness::new();
    let cfg = h.config("    unit: {regex: '^(?P<unit>[^_/]+)_'}\n    include: ['*.csv']\n");
    write(&h.src.join("RUN1_quant.csv"), b"q1");
    write(&h.src.join("RUN1_melt.csv"), b"m1");
    write(&h.src.join("RUN2_quant.csv"), b"q2");
    write(&h.src.join("notes.txt"), b"ignored");
    age_tree(&h.src, hour_ago());

    h.pass(&cfg).await;

    assert_eq!(
        data_files(&h.remote),
        tree(&[
            ("RUN1/RUN1_melt.csv", b"m1"),
            ("RUN1/RUN1_quant.csv", b"q1"),
            ("RUN2/RUN2_quant.csv", b"q2")
        ])
    );
    let mut units: Vec<String> = h.recorder.manifests(Register).into_iter().map(|m| m.unit).collect();
    units.sort();
    assert_eq!(units, ["RUN1", "RUN2"]);
}

#[tokio::test]
async fn include_and_exclude_choose_what_travels() {
    let h = Harness::new();
    let cfg = h.config("    include: ['*.tif']\n");
    write(&h.src.join("u__1/a.tif"), b"data");
    write(&h.src.join("u__1/sub/b.tif"), b"data");
    write(&h.src.join("u__1/notes.txt"), b"data");
    write(&h.src.join("u__1/~$a.tif"), b"data"); // an Office-style lock file
    age_tree(&h.src, hour_ago());

    h.pass(&cfg).await;

    assert_eq!(
        data_files(&h.remote),
        tree(&[("u__1/a.tif", b"data"), ("u__1/sub/b.tif", b"data")])
    );
}

#[tokio::test]
async fn a_failing_upload_backs_off_alerts_once_and_is_audited() {
    let h = Harness::new();
    let cfg = h.config("    retry: {initial: 60s, alert_after: 2}\n");
    write(&h.src.join("u__1/a.tif"), b"data");
    age_tree(&h.src, hour_ago());
    block(&h.remote, "u__1");

    let agent = h.agent(&cfg);
    let stop = CancellationToken::new();
    let first = agent.run_once(&[], &stop).await.unwrap().remove(0);
    let backing_off = agent.run_once(&[], &stop).await.unwrap().remove(0);
    h.clock.advance(61.0);
    let second = agent.run_once(&[], &stop).await.unwrap().remove(0);
    h.clock.advance(121.0);
    let third = agent.run_once(&[], &stop).await.unwrap().remove(0);
    let alerts: Vec<String> = agent
        .alerts
        .sent()
        .into_iter()
        .filter(|a| a.unit.as_deref() == Some("u__1"))
        .map(|a| a.title)
        .collect();

    assert_eq!([first.upload_failures, backing_off.upload_failures], [1, 0]);
    assert_eq!([second.upload_failures, third.upload_failures], [1, 1]);
    assert_eq!(alerts, ["inst: u__1/a.tif failed to upload to remote 2 times"]);
    let audited = h.recorder.transfers(Failures);
    assert_eq!(audited.len(), 3);
    assert!(
        audited
            .iter()
            .all(|t| t.destination.as_deref() == Some("remote") && t.error.is_some())
    );
    assert!(h.recorder.manifests(Register).is_empty());
}

#[tokio::test]
async fn a_unit_open_too_long_raises_one_alert() {
    let h = Harness::new();
    let cfg = h.config("    unit_idle: 7d\n    max_open: 60s\n");
    write(&h.src.join("u__1/a.tif"), b"data");

    let agent = h.agent(&cfg);
    let stop = CancellationToken::new();
    agent.run_once(&[], &stop).await.unwrap();
    assert!(agent.alerts.sent().iter().all(|a| a.unit.is_none()));
    h.clock.advance(61.0);
    agent.run_once(&[], &stop).await.unwrap();
    h.clock.advance(61.0);
    agent.run_once(&[], &stop).await.unwrap();
    let alerts: Vec<_> = agent.alerts.sent().into_iter().filter(|a| a.unit.is_some()).collect();

    assert_eq!(alerts.len(), 1);
    assert_eq!(alerts[0].title, "inst: unit u__1 has been incomplete for 61s");
    assert!(
        alerts[0].message.contains("ferry complete inst u__1"),
        "{}",
        alerts[0].message
    );
}

#[tokio::test]
async fn each_pass_digest_reaches_notifiers_as_one_alert() {
    let h = Harness::new();
    let cfg = h.config("");
    write(&h.src.join("u__1/a.tif"), b"data");
    age_tree(&h.src, hour_ago());
    let inbox = Inbox::alerts();
    let agent = Agent::open(cfg.clone(), h.notifying(&inbox)).unwrap();
    let stop = CancellationToken::new();
    agent.run_once(&[], &stop).await.unwrap();
    agent.run_once(&[], &stop).await.unwrap();

    assert!(inbox.receipts_seen().is_empty(), "an alert notifier takes no receipts");
    let digests = inbox.alerts_seen();
    assert_eq!(digests.len(), 2);
    assert_eq!(digests[0].level, Level::Info);
    assert_eq!(digests[0].pipeline.as_deref(), Some("inst"));
    assert_eq!(
        digests[0].title,
        "inst: 1 unit(s) registered, 1 finalized; 1 file(s) uploaded (4 B); no failures."
    );
    assert_eq!(digests[0].message, "• u__1 — registered, finalized");
    assert!(
        digests[1].title.starts_with("inst: 0 unit(s) registered"),
        "{}",
        digests[1].title
    );
}

#[tokio::test]
async fn a_delivered_unit_gets_one_receipt_saying_where_it_went() {
    let h = Harness::new();
    let cfg = h.config_with(
        "[remote, {store: mirror, verify: sha256}]",
        "[{name: rec, exec: [unused]}]",
        &h.backup_extra("move"),
    );
    write(&h.src.join("plateA__1/img/a.tif"), b"alpha");
    write(&h.src.join("plateA__1/b.tif"), b"bravo!");
    age_tree(&h.src, hour_ago());
    let inbox = Inbox::receipts();

    h.pass_with(
        &cfg,
        AgentOptions {
            dry_run: true,
            ..h.notifying(&inbox)
        },
    )
    .await;
    assert!(inbox.events.lock().unwrap().is_empty(), "a dry run posts nothing");
    h.pass_with(&cfg, h.notifying(&inbox)).await;
    h.pass_with(&cfg, h.notifying(&inbox)).await;

    assert!(inbox.alerts_seen().is_empty(), "a receipt notifier takes no digests");
    let receipts = inbox.receipts_seen();
    assert_eq!(receipts.len(), 1, "one per unit, however many passes");
    let r = &receipts[0];
    assert_eq!(
        (r.unit.as_str(), r.generation, r.update, r.files, r.bytes),
        ("plateA__1", 1, false, 2, 11)
    );
    let copies: Vec<(&str, &str)> = r
        .destinations
        .iter()
        .map(|d| (d.name.as_str(), d.verified.as_str()))
        .collect();
    assert_eq!(copies, [("remote", "size"), ("mirror", "sha256")]);
    assert_eq!(r.registrars, ["rec"]);
    assert_eq!(
        (r.original.action.as_str(), r.original.done, r.original.path.clone()),
        ("move", true, Some(h.backup.join("plateA__1").display().to_string()))
    );
    let event = inbox.events.lock().unwrap()[0].clone();
    assert_eq!(
        (event.node.as_str(), event.pipeline.as_deref()),
        ("test-node", Some("inst"))
    );
}

#[tokio::test]
async fn a_receipt_does_not_wait_out_the_grace_before_a_delete() {
    let h = Harness::new();
    let cfg = h.config("    finalize: {action: delete, after: 1h}\n");
    write(&h.src.join("u__1/a.tif"), b"data");
    age_tree(&h.src, hour_ago());
    let inbox = Inbox::receipts();

    h.pass_with(&cfg, h.notifying(&inbox)).await;
    assert!(h.src.join("u__1/a.tif").exists());
    let receipts = inbox.receipts_seen();
    assert_eq!(receipts.len(), 1);
    let original = &receipts[0].original;
    assert_eq!((original.action.as_str(), original.done), ("delete", false));
    let secs = |t: &str| chrono::DateTime::parse_from_rfc3339(t).unwrap().timestamp();
    assert_eq!(
        secs(original.at.as_deref().unwrap()) - secs(&receipts[0].delivered),
        3600
    );

    h.clock.advance(3601.0);
    h.pass_with(&cfg, h.notifying(&inbox)).await;
    assert!(!h.src.join("u__1").exists());
    assert_eq!(inbox.receipts_seen().len(), 1, "the deletion itself posts nothing more");
}

#[tokio::test]
async fn a_receipt_waits_in_the_journal_while_chat_is_down() {
    let h = Harness::new();
    let cfg = h.config("");
    write(&h.src.join("u__1/a.tif"), b"data");
    age_tree(&h.src, hour_ago());
    let inbox = Inbox::receipts();
    *inbox.failing.lock().unwrap() = 1;

    let report = h.pass_with(&cfg, h.notifying(&inbox)).await;
    assert_eq!(report.delivery_failures, 1);
    assert_eq!(
        h.unit_state(&cfg, "u__1").as_deref(),
        Some("finalized"),
        "receipts hold nothing up"
    );
    h.pass_with(&cfg, h.notifying(&inbox)).await;
    assert!(inbox.receipts_seen().is_empty(), "backing off");

    h.clock.advance(31.0);
    h.pass_with(&cfg, h.notifying(&inbox)).await;
    assert_eq!(inbox.receipts_seen().len(), 1);
}

#[tokio::test]
async fn files_that_arrive_after_a_receipt_bring_an_update() {
    let h = Harness::new();
    let cfg = h.config("");
    write(&h.src.join("u__1/a.tif"), b"a");
    age_tree(&h.src, hour_ago());
    let inbox = Inbox::receipts();
    h.pass_with(&cfg, h.notifying(&inbox)).await;

    write(&h.src.join("u__1/b.tif"), b"bb");
    h.clock.advance(10.0);
    h.pass_with(&cfg, h.notifying(&inbox)).await;

    let seen: Vec<(i64, bool, usize, i64)> = inbox
        .receipts_seen()
        .iter()
        .map(|r| (r.generation, r.update, r.files, r.bytes))
        .collect();
    assert_eq!(seen, [(1, false, 1, 1), (2, true, 2, 3)]);
}

#[tokio::test]
async fn forcing_completion_skips_the_idle_wait() {
    let h = Harness::new();
    let cfg = h.config("    unit_idle: 1h\n");
    write(&h.src.join("u__1/a.tif"), b"data");

    h.pass(&cfg).await;
    assert!(h.recorder.manifests(Register).is_empty());
    Journal::open(&cfg.journal_path())
        .unwrap()
        .force_complete("inst", "u__1")
        .unwrap();

    h.pass(&cfg).await;
    assert_eq!(h.recorder.manifests(Register).len(), 1);
}

#[tokio::test]
async fn a_dry_run_changes_nothing() {
    let h = Harness::new();
    let cfg = h.config(&h.backup_extra("move"));
    write(&h.src.join("u__1/a.tif"), b"data");
    age_tree(&h.src, hour_ago());

    let report = h.pass_with(&cfg, h.options(true)).await;

    assert_eq!(report.uploaded, 1);
    assert_eq!(report.registered, ["u__1"]);
    assert_eq!(report.finalized, ["u__1"]);
    assert!(files_under(&h.remote).is_empty());
    assert!(files_under(&h.backup).is_empty());
    assert!(h.src.join("u__1/a.tif").exists());
    assert!(h.recorder.kinds().is_empty());
    assert!(!cfg.journal_path().exists());
}

#[tokio::test]
async fn big_files_go_up_in_parts_to_every_destination() {
    let h = Harness::new();
    let cfg = h.config_with(
        "[remote, {store: mirror, verify: sha256}]",
        "[{name: rec, exec: [unused]}]",
        "    limits: {single_put_max: 1MiB, part_size: 5MiB}\n",
    );
    let body: Vec<u8> = (0..12 * 1024 * 1024).map(|i| (i % 251) as u8).collect();
    write(&h.src.join("u__1/big.raw"), &body);
    age_tree(&h.src, hour_ago());

    let report = h.pass(&cfg).await;

    assert_eq!(report.failures(), 0, "{report:?}");
    assert_eq!(std::fs::read(h.remote.join("u__1/big.raw")).unwrap(), body);
    assert_eq!(std::fs::read(h.mirror.join("u__1/big.raw")).unwrap(), body);
    let registered = h.recorder.manifests(Register);
    assert_eq!(registered[0].files[0].sha256.as_deref(), Some(sha256(&body).as_str()));
}

#[tokio::test]
async fn a_copy_that_vanished_is_sent_again_before_the_source_goes() {
    let h = Harness::new();
    let cfg = h.config("    finalize: {action: delete, after: 1h}\n");
    write(&h.src.join("u__1/a.tif"), b"data");
    age_tree(&h.src, hour_ago());
    h.pass(&cfg).await;
    std::fs::remove_file(h.remote.join("u__1/a.tif")).unwrap();

    h.clock.advance(3601.0);
    let report = h.pass(&cfg).await;
    assert!(h.src.join("u__1/a.tif").exists(), "kept while a copy is missing");
    assert!(report.reopened[0].1.contains("is gone"), "{:?}", report.reopened);

    h.pass(&cfg).await;
    assert_eq!(data_files(&h.remote), tree(&[("u__1/a.tif", b"data")]));
    h.clock.advance(3601.0);
    h.pass(&cfg).await;
    assert!(!h.src.join("u__1").exists());
}

#[tokio::test]
async fn an_overwritten_copy_does_not_vouch_for_a_delete() {
    let h = Harness::new();
    let cfg = h.config("    finalize: {action: delete, after: 1h}\n");
    write(&h.src.join("u__1/a.tif"), b"data");
    age_tree(&h.src, hour_ago());
    h.pass(&cfg).await;
    // Same size, other bytes: only its ETag tells.
    std::thread::sleep(std::time::Duration::from_millis(20));
    std::fs::write(h.remote.join("u__1/a.tif"), b"DATA").unwrap();

    h.clock.advance(3601.0);
    let report = h.pass(&cfg).await;
    assert!(h.src.join("u__1/a.tif").exists());
    assert!(
        report.reopened[0].1.contains("was overwritten"),
        "{:?}",
        report.reopened
    );
}

#[tokio::test]
async fn a_rewrite_that_keeps_size_and_time_is_caught_before_a_delete() {
    let h = Harness::new();
    let cfg = h.config("    finalize: {action: delete, after: 1h}\n");
    let path = write(&h.src.join("u__1/a.tif"), b"data");
    age_tree(&h.src, hour_ago());
    h.pass(&cfg).await;
    let mtime = filetime::FileTime::from_last_modification_time(&std::fs::metadata(&path).unwrap());
    std::fs::write(&path, b"DATA").unwrap();
    filetime::set_file_mtime(&path, mtime).unwrap();

    h.clock.advance(3601.0);
    let report = h.pass(&cfg).await;
    assert_eq!(std::fs::read(&path).unwrap(), b"DATA", "put back, not deleted");
    assert!(
        report.reopened[0].1.contains("holds other bytes"),
        "{:?}",
        report.reopened
    );
    assert!(!h.src.join("u__1/a.tif.ferry-finalizing").exists());
}

#[tokio::test]
async fn a_finalize_interrupted_after_setting_a_file_aside_resumes() {
    let h = Harness::new();
    let cfg = h.config(&format!(
        "    finalize: {{action: move, backup_root: '{}', after: 1h}}\n",
        h.backup.display()
    ));
    let path = write(&h.src.join("u__1/a.tif"), b"data");
    age_tree(&h.src, hour_ago());
    h.pass(&cfg).await;
    // As if ferry stopped right after the rename.
    std::fs::rename(&path, h.src.join("u__1/a.tif.ferry-finalizing")).unwrap();

    h.clock.advance(3601.0);
    let report = h.pass(&cfg).await;
    assert_eq!(report.finalized, ["u__1"]);
    assert_eq!(files_under(&h.backup), tree(&[("u__1/a.tif", b"data")]));
    assert!(!h.src.join("u__1").exists());
}

#[tokio::test]
async fn a_move_whose_source_vanished_needs_a_backup_that_matches() {
    let h = Harness::new();
    let cfg = h.config(&format!(
        "    finalize: {{action: move, backup_root: '{}', after: 1h}}\n",
        h.backup.display()
    ));
    let path = write(&h.src.join("u__1/a.tif"), b"data");
    age_tree(&h.src, hour_ago());
    h.pass(&cfg).await;
    std::fs::remove_file(&path).unwrap();

    h.clock.advance(3601.0);
    let report = h.pass(&cfg).await;
    assert_eq!(report.finalize_failures, 1);
    assert_eq!(h.unit_state(&cfg, "u__1").as_deref(), Some("registered"));

    write(&h.backup.join("u__1/a.tif"), b"DATA");
    h.clock.advance(3601.0);
    let report = h.pass(&cfg).await;
    assert_eq!(report.finalize_failures, 1, "a backup with other bytes does not count");

    std::fs::write(h.backup.join("u__1/a.tif"), b"data").unwrap();
    h.clock.advance(3601.0);
    let report = h.pass(&cfg).await;
    assert_eq!(report.finalized, ["u__1"]);
}

#[tokio::test]
async fn a_unit_with_a_folder_it_cannot_read_waits_for_it() {
    let h = Harness::new();
    let cfg = h.config("");
    write(&h.src.join("u__1/a.tif"), b"a");
    write(&h.src.join("u__1/later/b.tif"), b"b");
    age_tree(&h.src, hour_ago());

    let locked = lock_out(&h.src.join("u__1/later"));
    let report = h.pass(&cfg).await;
    let_in(&h.src.join("u__1/later"));
    if !locked {
        return;
    }
    assert!(report.registered.is_empty());
    assert!(
        report.waiting["u__1"].contains("cannot read all of it"),
        "{:?}",
        report.waiting
    );
    assert!(data_files(&h.remote).is_empty());

    h.pass(&cfg).await;
    let registered = h.recorder.manifests(Register);
    assert_eq!(
        registered[0].files.iter().map(|f| f.path.as_str()).collect::<Vec<_>>(),
        ["a.tif", "later/b.tif"]
    );
}

#[tokio::test]
async fn a_complete_unit_that_turns_unreadable_does_not_register() {
    let h = Harness::new();
    let cfg = h.config("");
    write(&h.src.join("u__1/a.tif"), b"a");
    std::fs::create_dir_all(h.src.join("u__1/later")).unwrap();
    age_tree(&h.src, hour_ago());
    block(&h.remote, "u__1");
    h.pass(&cfg).await;
    assert_eq!(h.unit_state(&cfg, "u__1").as_deref(), Some("complete"));

    // A file lands where the next scan cannot look; a.tif stays as it was.
    write(&h.src.join("u__1/later/b.tif"), b"b");
    unblock(&h.remote, "u__1");
    h.clock.advance(31.0);
    let locked = lock_out(&h.src.join("u__1/later"));
    let report = h.pass(&cfg).await;
    let_in(&h.src.join("u__1/later"));
    if !locked {
        return;
    }
    assert!(report.registered.is_empty(), "never with a partial file list");

    h.pass(&cfg).await;
    let registered = h.recorder.manifests(Register);
    assert_eq!(
        registered[0].files.iter().map(|f| f.path.as_str()).collect::<Vec<_>>(),
        ["a.tif", "later/b.tif"]
    );
}

#[tokio::test]
async fn a_registered_unit_that_turns_unreadable_is_not_finalized() {
    let h = Harness::new();
    let cfg = h.config("    finalize: {action: delete, after: 1h}\n");
    write(&h.src.join("u__1/a.tif"), b"a");
    write(&h.src.join("u__1/later/b.tif"), b"b");
    age_tree(&h.src, hour_ago());
    h.pass(&cfg).await;
    assert_eq!(h.unit_state(&cfg, "u__1").as_deref(), Some("registered"));

    h.clock.advance(3601.0);
    let locked = lock_out(&h.src.join("u__1/later"));
    let report = h.pass(&cfg).await;
    let_in(&h.src.join("u__1/later"));
    if !locked {
        return;
    }
    assert!(report.finalized.is_empty());
    assert!(
        h.src.join("u__1/a.tif").exists(),
        "nothing goes while part of the unit is unread"
    );

    h.pass(&cfg).await;
    assert!(!h.src.join("u__1").exists());
}

#[tokio::test]
async fn a_root_that_cannot_be_listed_makes_nothing_gone() {
    let h = Harness::new();
    let cfg = h.config("    unit_idle: 2h\n");
    write(&h.src.join("u__1/a.tif"), b"a");
    age_tree(&h.src, hour_ago());
    h.pass(&cfg).await;

    let locked = lock_out(&h.src);
    let report = h.pass(&cfg).await;
    let_in(&h.src);
    if !locked {
        return;
    }
    assert!(report.reopened.is_empty());
    let journal = Journal::open(&cfg.journal_path()).unwrap();
    let unit = journal.find_unit("inst", "u__1").unwrap().unwrap();
    assert!(unit.present);
    let files = journal.files(unit.id).unwrap();
    assert!(files.iter().all(|f| f.present && f.state == "pending"), "{files:?}");
}

#[cfg(unix)]
#[tokio::test]
async fn names_with_backslashes_keep_keys_of_their_own() {
    let h = Harness::new();
    let cfg = h.config("");
    write(&h.src.join("u__1/a\\b.tif"), b"one name");
    write(&h.src.join("u__1/a/b.tif"), b"two names");
    age_tree(&h.src, hour_ago());

    h.pass(&cfg).await;

    assert_eq!(
        data_files(&h.remote),
        tree(&[("u__1/a%5Cb.tif", b"one name"), ("u__1/a/b.tif", b"two names")])
    );
}
