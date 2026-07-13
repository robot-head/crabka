//! KIP-1022 `crabka format --feature` end-to-end: a standalone-formatted log
//! dir whose feature levels were set via `--feature` boots a broker that
//! finalizes exactly those levels and surfaces them through `ApiVersions`.
//!
//! `--standalone` is load-bearing: it writes a `VotersRecord`, so the broker
//! uses the formatted voter set and *skips* the in-process self-bootstrap that
//! would otherwise re-seed every feature at the latest release — proving the
//! `--feature` overrides survive boot rather than being clobbered.

use std::process::Command;

use assert2::{assert, check};
use crabka_broker::{Broker, BrokerConfig};
use crabka_client_core::Client;
use crabka_protocol::owned::api_versions_request::ApiVersionsRequest;

mod support;

/// Run `crabka format --standalone … --feature …` as a subprocess. Shelled
/// out via `env!("CARGO")` (the `crabka-cli` `CARGO_BIN_EXE_*` isn't exported
/// to this crate); the `crabka-cli` dev-dep keeps it a cache hit.
fn run_crabka_format_with_features(
    log_dir: &std::path::Path,
    node_id: u64,
    controller_listener: &str,
    features: &[&str],
) {
    let cargo = std::env::var("CARGO").unwrap_or_else(|_| "cargo".to_string());
    let mut args: Vec<String> = [
        "run",
        "--quiet",
        "-p",
        "crabka-cli",
        "--bin",
        "crabka",
        "--",
        "format",
        "--log-dir",
        log_dir.to_str().unwrap(),
        "--standalone",
        "--node-id",
        &node_id.to_string(),
        "--controller-listener",
        controller_listener,
    ]
    .into_iter()
    .map(String::from)
    .collect();
    for f in features {
        args.push("--feature".to_string());
        args.push((*f).to_string());
    }
    let out = Command::new(cargo)
        .args(&args)
        .output()
        .expect("spawn crabka format");
    assert!(
        out.status.success(),
        "crabka format failed: stdout={} stderr={}",
        String::from_utf8_lossy(&out.stdout),
        String::from_utf8_lossy(&out.stderr),
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn standalone_format_feature_overrides_surface_in_api_versions() {
    support::init_tracing();

    // Pre-bind a concrete controller port so it can be baked into the
    // VotersRecord at format time and re-bound by the broker on boot.
    let (client_addrs, controller_addrs, client_listeners, controller_listeners) =
        support::bind_and_hold_ports(1).await;
    let client_addr = client_addrs[0];
    let controller_addr = controller_addrs[0];

    let dir = tempfile::tempdir().unwrap();
    // `crabka format` creates the dir itself and refuses a non-empty one.
    let boot_dir = dir.path().join("boot");
    run_crabka_format_with_features(
        &boot_dir,
        1,
        &controller_addr.to_string(),
        // transaction.version pinned to 1 (default at the latest release is 2);
        // group.version pinned to 0 (default is 1) → omitted → not finalized.
        &["transaction.version=1", "group.version=0"],
    );

    let mut cfg = BrokerConfig::for_tests(boot_dir.clone());
    cfg.broker_id = 1;
    cfg.node_id = crabka_broker::NodeId(1);
    cfg.listen_addr = client_addr;
    cfg.advertised_listener = client_addr.to_string();
    cfg.controller_listen_addr = controller_addr;
    cfg.controller_quorum_voters = vec![(crabka_broker::NodeId(1), controller_addr.to_string())];
    cfg.bootstrap_mode = crabka_broker::BootstrapMode::Bootstrap;

    let data_listener = client_listeners.into_iter().next().unwrap();
    let controller_listener = controller_listeners.into_iter().next().unwrap();
    let handle = Broker::start_with_listeners(cfg, Some(controller_listener), Some(data_listener))
        .await
        .expect("broker start");
    let bootstrap = handle.listen_addr().to_string();
    let client = Client::builder()
        .bootstrap(&bootstrap)
        .client_id("fmt-feature-test")
        .build()
        .await
        .expect("client build");

    let av = client
        .send(ApiVersionsRequest {
            client_software_name: "crabka-test".into(),
            client_software_version: "0.0.0".into(),
            ..Default::default()
        })
        .await
        .expect("ApiVersions");
    assert!(av.error_code == 0, "{av:?}");

    let finalized = |name: &str| {
        av.finalized_features
            .iter()
            .find(|f| f.name == name)
            .map(|f| f.max_version_level)
    };

    // transaction.version=1 took effect (not the release default 2);
    // group.version=0 → omitted → not finalized at all;
    // metadata.version was not overridden → latest stable (25).
    for (feature, want) in [
        ("transaction.version", Some(1)),
        ("group.version", None),
        ("metadata.version", Some(25)),
    ] {
        check!(
            finalized(feature) == want,
            "{feature} must be finalized at {want:?}: {:?}",
            av.finalized_features
        );
    }

    handle.shutdown().await;
}
