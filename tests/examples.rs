//! The documented example config stays valid.

use ferry::config::{Action, Adopt, Config, Fanout, Mode, NotifierEvent, Upload, Verify};

#[test]
fn the_annotated_example_config_loads() {
    let path = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("examples/ferry.example.yaml");
    let cfg = Config::load_with_env(&path, &|name| Some(format!("<{name}>"))).unwrap();

    assert_eq!(cfg.pipelines.len(), 3);
    let nightly = &cfg.pipelines[0];
    assert_eq!(
        (nightly.mode, nightly.upload, nightly.fanout),
        (Mode::Cron, Upload::Unit, Fanout::Shared)
    );
    assert_eq!(nightly.destination_keys(), ["ceph", "aws"]);
    assert_eq!(nightly.destinations[1].verify, Verify::Sha256);
    assert!(!nightly.destinations[1].required);
    assert_eq!(nightly.finalize.action, Action::Move);
    assert_eq!(nightly.adopt, Adopt::Sha256);
    assert_eq!(nightly.registrars[1].env["CATALOG_URL"], "<CATALOG_URL>");
    assert_eq!(cfg.pipelines[1].finalize.action, Action::Delete);
    assert_eq!(cfg.stores["ceph"].options["aws_access_key_id"], "<FERRY_S3_KEY>");
    assert_eq!(cfg.logs.ship.as_ref().unwrap().registrars.len(), 1);
    assert_eq!(cfg.notifiers[0].webhook.as_deref(), Some("<FERRY_ALERT_WEBHOOK>"));
    assert_eq!(
        (cfg.notifiers[1].key(), cfg.notifiers[1].events.as_slice()),
        ("arrivals".to_string(), [NotifierEvent::Receipt].as_slice())
    );
}
