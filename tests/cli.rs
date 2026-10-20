//! The `ferry` binary end to end, against local-directory stores.

mod common;

use std::path::{Path, PathBuf};

use assert_cmd::Command;
use common::{age_tree, files_under, hour_ago, tree, write};
use predicates::prelude::*;

fn ferry(config: &Path) -> Command {
    let mut cmd = Command::new(env!("CARGO_BIN_EXE_ferry"));
    cmd.arg("--config")
        .arg(config)
        .env_remove("FERRY_CONFIG")
        .env_remove("FERRY_ENV_FILE")
        .env("FERRY_LOG", "warn");
    cmd
}

struct Site {
    _dir: tempfile::TempDir,
    root: PathBuf,
    config: PathBuf,
}

impl Site {
    /// A config with one pipeline `inst` over `src` into the `remote` store.
    fn new(pipeline_extra: &str, top_extra: &str) -> Self {
        let dir = tempfile::tempdir().unwrap();
        let root = dir.path().canonicalize().unwrap();
        for d in ["src", "remote"] {
            std::fs::create_dir_all(root.join(d)).unwrap();
        }
        let config = root.join("ferry.yaml");
        let defaults: String = [("settle", "1s"), ("unit_idle", "2s")]
            .iter()
            .filter(|(key, _)| !pipeline_extra.contains(&format!("{key}:")))
            .map(|(key, value)| format!("    {key}: {value}\n"))
            .collect();
        std::fs::write(
            &config,
            format!(
                "node: test-node\n\
                 state_dir: state\n\
                 stores:\n  remote: {{url: remote}}\n\
                 pipelines:\n\
                 \x20 - name: inst\n\
                 \x20   root: src\n\
                 \x20   destinations: [remote]\n\
                 {defaults}{pipeline_extra}{top_extra}"
            ),
        )
        .unwrap();
        Self {
            _dir: dir,
            root,
            config,
        }
    }

    fn path(&self, rel: &str) -> PathBuf {
        self.root.join(rel)
    }
}

#[test]
fn version() {
    Command::new(env!("CARGO_BIN_EXE_ferry"))
        .arg("--version")
        .assert()
        .success()
        .stdout(predicate::str::starts_with(format!(
            "ferry {}",
            env!("CARGO_PKG_VERSION")
        )));
}

#[test]
fn schemas_print_without_a_config() {
    let out = Command::new(env!("CARGO_BIN_EXE_ferry"))
        .args(["schema", "event"])
        .env_remove("FERRY_CONFIG")
        .assert()
        .success();
    let schema: serde_json::Value = serde_json::from_slice(&out.get_output().stdout).unwrap();
    assert_eq!(schema["properties"]["schema"]["const"], "ferry.event/v1");
    assert!(schema["required"].as_array().unwrap().contains(&"id".into()));
}

#[test]
fn validate_shows_where_each_pipeline_sends_data() {
    let site = Site::new("    finalize: {action: delete}\n", "");
    ferry(&site.config)
        .args(["config", "validate"])
        .assert()
        .success()
        .stdout(predicate::str::contains("inst: "))
        .stdout(predicate::str::contains("(required, verify size)"))
        .stdout(predicate::str::contains("finalize delete"))
        .stdout(predicate::str::ends_with("ok\n"));
}

#[test]
fn config_mistakes_name_the_file_and_field() {
    let site = Site::new("    setle: 5s\n", "");
    ferry(&site.config)
        .args(["config", "validate"])
        .assert()
        .code(2)
        .stderr(predicate::str::contains("ferry.yaml"))
        .stderr(predicate::str::contains("pipelines[0]"))
        .stderr(predicate::str::contains("unknown field `setle`"));
}

#[test]
fn a_dry_run_changes_nothing() {
    let site = Site::new("", "");
    write(&site.path("src/u__1/a.tif"), b"data");
    age_tree(&site.path("src"), hour_ago());

    ferry(&site.config).args(["run", "--dry-run"]).assert().success();

    assert!(files_under(&site.path("remote")).is_empty());
    assert!(!site.path("state/journal.sqlite3").exists());
}

#[test]
fn status_before_any_run_says_so() {
    let site = Site::new("", "");
    ferry(&site.config)
        .arg("status")
        .assert()
        .code(2)
        .stderr(predicate::str::contains("ferry has not run yet"));
}

/// An exec hook, as a YAML list, running `unix` with `sh` or `windows` with
/// PowerShell. Both are YAML single-quoted: write `'` as `''`.
fn shell(unix: &str, windows: &str) -> String {
    if cfg!(windows) {
        format!("[powershell, -NoProfile, -NonInteractive, -Command, '{windows}']")
    } else {
        format!("[sh, -c, '{unix}']")
    }
}

#[test]
fn a_run_ships_registers_with_the_exec_hook_and_finalizes() {
    let hook = shell(
        "cat >> events.jsonl; echo >> events.jsonl",
        "[IO.File]::AppendAllText(''events.jsonl'', [Console]::In.ReadToEnd() + \"`n\")",
    );
    let site = Site::new(
        &format!(
            "    finalize: {{action: delete}}\n    registrars:\n      - {{name: catalog, exec: {hook}, cwd: .}}\n"
        ),
        "",
    );
    write(&site.path("src/u__1/a.tif"), b"alpha");
    age_tree(&site.path("src"), hour_ago());

    ferry(&site.config).arg("run").assert().success();

    assert_eq!(files_under(&site.path("remote")), tree(&[("u__1/a.tif", b"alpha")]));
    assert!(!site.path("src/u__1").exists());
    let events: Vec<serde_json::Value> = std::fs::read_to_string(site.path("events.jsonl"))
        .unwrap()
        .lines()
        .filter(|l| !l.trim().is_empty())
        .map(|l| serde_json::from_str(l).unwrap())
        .collect();
    let kinds: Vec<&str> = events.iter().map(|e| e["event"].as_str().unwrap()).collect();
    assert_eq!(kinds, ["register", "finalize"]);
    assert_eq!(events[0]["manifest"]["files"][0]["path"], "a.tif");
    assert_eq!(events[0]["transfers"][0]["op"], "upload");
    assert_eq!(events[1]["transfers"][0]["op"], "delete");

    let status = ferry(&site.config).args(["status", "--json"]).assert().success();
    let report: serde_json::Value = serde_json::from_slice(&status.get_output().stdout).unwrap();
    assert_eq!(report["inst"]["units"]["finalized"], 1);
}

#[test]
fn a_failing_hook_fails_the_run_and_shows_in_status() {
    let hook = shell("echo down >&2; exit 4", "[Console]::Error.WriteLine(''down''); exit 4");
    let site = Site::new(
        &format!("    registrars:\n      - {{name: catalog, exec: {hook}}}\n"),
        "",
    );
    write(&site.path("src/u__1/a.tif"), b"alpha");
    age_tree(&site.path("src"), hour_ago());

    ferry(&site.config).arg("run").assert().code(1);

    ferry(&site.config)
        .arg("status")
        .assert()
        .success()
        .stdout(predicate::str::contains("1 registering"))
        .stdout(predicate::str::contains(
            "failing: register u__1 with catalog (1x): catalog: exit code 4 for register: down",
        ));
}

#[test]
fn operators_can_complete_abandon_and_retry_units() {
    let site = Site::new("    unit_idle: 1h\n", "");
    write(&site.path("src/u__1/a.tif"), b"alpha");

    ferry(&site.config).arg("run").assert().success();
    ferry(&site.config)
        .args(["complete", "inst", "u__1"])
        .assert()
        .success()
        .stdout(predicate::str::contains("will complete on the next pass"));
    ferry(&site.config)
        .args(["complete", "inst", "nope"])
        .assert()
        .code(1)
        .stderr(predicate::str::contains("inst has no unit \"nope\""));
    ferry(&site.config).args(["abandon", "inst", "u__1"]).assert().success();
    ferry(&site.config)
        .args(["status", "--units"])
        .assert()
        .success()
        .stdout(predicate::str::contains("1 abandoned"));
    ferry(&site.config)
        .args(["retry", "-p", "inst", "-u", "u__1"])
        .assert()
        .success()
        .stdout("1 unit(s) will be retried on the next pass.\n");
    ferry(&site.config).arg("run").assert().success();
    assert_eq!(files_under(&site.path("remote")), tree(&[("u__1/a.tif", b"alpha")]));
}

#[test]
fn events_for_a_renamed_registrar_wait_for_an_operator() {
    let site = Site::new(
        "    registrars: [{name: catalog, exec: [ferry-test-no-such-hook]}]\n",
        "",
    );
    write(&site.path("src/u__1/a.tif"), b"alpha");
    age_tree(&site.path("src"), hour_ago());
    // The hook cannot start, so the registration stays undelivered.
    ferry(&site.config).arg("run").assert().failure();

    let renamed = format!(
        "name: catalogue, exec: ['{}', '--version']",
        env!("CARGO_BIN_EXE_ferry")
    );
    let text = std::fs::read_to_string(&site.config)
        .unwrap()
        .replace("name: catalog, exec: [ferry-test-no-such-hook]", &renamed);
    std::fs::write(&site.config, text).unwrap();
    ferry(&site.config)
        .arg("run")
        .assert()
        .failure()
        .stderr(predicate::str::contains("ferry outbox discard inst catalog"));

    ferry(&site.config)
        .args(["outbox", "discard", "inst", "catalog", "--dry-run"])
        .assert()
        .success()
        .stdout("[dry run] would drop 1 undelivered event(s) for catalog in inst.\n");
    ferry(&site.config)
        .args(["outbox", "discard", "inst", "catalog"])
        .assert()
        .success();
    ferry(&site.config).arg("run").assert().success();
}

#[test]
fn a_destination_that_moved_waits_for_an_operator() {
    let site = Site::new("    upload: file\n    unit_idle: 2h\n", "");
    write(&site.path("src/u__1/a.tif"), b"alpha");
    ferry(&site.config).arg("run").assert().success();
    assert_eq!(files_under(&site.path("remote")), tree(&[("u__1/a.tif", b"alpha")]));

    let text = std::fs::read_to_string(&site.config)
        .unwrap()
        .replace("{url: remote}", "{url: elsewhere}");
    std::fs::write(&site.config, text).unwrap();
    std::fs::create_dir_all(site.path("elsewhere")).unwrap();
    ferry(&site.config)
        .arg("run")
        .assert()
        .failure()
        .stderr(predicate::str::contains("ferry destination accept inst remote"));
    ferry(&site.config)
        .arg("doctor")
        .assert()
        .code(1)
        .stdout(predicate::str::contains("`ferry destination accept inst remote`"));

    ferry(&site.config)
        .args(["destination", "reset", "inst", "remote", "--dry-run"])
        .assert()
        .success()
        .stdout(
            predicate::str::contains("[dry run] inst/remote points at")
                .and(predicate::str::contains("1 file(s) go up again")),
        );
    ferry(&site.config).arg("run").assert().failure();
    ferry(&site.config)
        .args(["destination", "reset", "inst", "remote"])
        .assert()
        .success();
    ferry(&site.config).arg("run").assert().success();
    assert_eq!(files_under(&site.path("elsewhere")), tree(&[("u__1/a.tif", b"alpha")]));
}

#[test]
fn schedule_lists_the_next_passes_in_the_pipeline_zone() {
    let site = Site::new("    mode: cron\n    cron: '0 2 * * *'\n    timezone: Europe/Oslo\n", "");
    let out = ferry(&site.config).args(["schedule", "-n", "2"]).assert().success();
    let text = String::from_utf8(out.get_output().stdout.clone()).unwrap();
    let lines: Vec<&str> = text.lines().collect();
    assert_eq!(lines[0], "inst: \"0 2 * * *\" (Europe/Oslo)");
    assert_eq!(lines.len(), 3);
    assert!(
        lines[1].trim().ends_with("02:00 CET") || lines[1].trim().ends_with("02:00 CEST"),
        "{text}"
    );
}

#[test]
fn doctor_says_what_would_stop_ferry() {
    let site = Site::new(
        "    registrars: [{name: catalog, exec: [ferry-test-no-such-hook]}]\n",
        "",
    );
    ferry(&site.config)
        .arg("doctor")
        .assert()
        .code(1)
        .stdout(predicate::str::contains("ok    state dir "))
        .stdout(predicate::str::contains("ok    inst/remote "))
        .stdout(predicate::str::contains(
            "FAIL  inst: registrar catalog: cannot find \"ferry-test-no-such-hook\"",
        ))
        .stdout(predicate::str::ends_with("1 problem(s) found\n"));
    assert!(
        files_under(&site.path("remote")).is_empty(),
        "nothing written unless asked"
    );
}

#[test]
fn doctor_can_write_to_each_store() {
    let site = Site::new("", "");
    ferry(&site.config)
        .args(["doctor", "--write"])
        .assert()
        .success()
        .stdout(predicate::str::contains("writes, and reads back what it wrote"))
        .stdout(predicate::str::ends_with("no problems found\n"));
    let probes: Vec<String> = files_under(&site.path("remote")).into_keys().collect();
    assert_eq!(probes, ["_ferry/doctor/test-node"]);
}

#[test]
fn an_env_file_fills_in_the_config_and_goes_with_the_service() {
    let site = Site::new("", "");
    let text = std::fs::read_to_string(&site.config)
        .unwrap()
        .replace("node: test-node", "node: ${FERRY_TEST_NODE}");
    std::fs::write(&site.config, text).unwrap();
    let env = site.path("ferry.env");
    std::fs::write(&env, "# the service's secrets\nFERRY_TEST_NODE=from-the-env-file\n").unwrap();

    let validated = ferry(&site.config)
        .arg("--env-file")
        .arg(&env)
        .args(["config", "validate"])
        .assert()
        .success()
        .stdout(predicate::str::starts_with("node from-the-env-file;"));
    if cfg!(unix) {
        validated.stderr(predicate::str::contains("`chmod 600` it"));
    }
    ferry(&site.config)
        .arg("--env-file")
        .arg(&env)
        .args(["service", "print", "--platform", "systemd"])
        .assert()
        .success()
        .stdout(predicate::str::contains(" --env-file "));
    ferry(&site.config)
        .arg("--env-file")
        .arg(site.path("missing.env"))
        .args(["config", "validate"])
        .assert()
        .code(2)
        .stderr(predicate::str::contains("cannot read env file"));
}

#[test]
fn service_print_renders_a_launchd_job() {
    let site = Site::new("", "");
    ferry(&site.config)
        .args(["service", "print", "--platform", "launchd", "--system"])
        .assert()
        .success()
        .stdout(predicate::str::contains("<string>watch</string>"))
        .stdout(predicate::str::contains("<key>KeepAlive</key>"));
}

#[test]
fn service_print_renders_a_systemd_unit() {
    let site = Site::new("", "");
    // systemd needs an argument with a backslash (any Windows path) quoted,
    // each backslash doubled.
    let config = site.config.display().to_string();
    let config = if config.contains('\\') {
        format!("\"{}\"", config.replace('\\', r"\\"))
    } else {
        config
    };
    ferry(&site.config)
        .args(["service", "print", "--platform", "systemd", "--system"])
        .assert()
        .success()
        .stdout(predicate::str::contains(format!("watch --config {config}")))
        .stdout(predicate::str::contains("WantedBy=multi-user.target"));
}
