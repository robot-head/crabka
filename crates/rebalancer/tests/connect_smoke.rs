//! Slice 43a Connect protocol smoke test. Builds the binary, runs it
//! against a temporary single-broker Crabka, hits the Connect endpoint
//! over HTTP+JSON, asserts a sane response. Proves the axum mount +
//! Connect-axum glue work end-to-end.
//!
//! Route format `/crabka.rebalancer.v1.Rebalancer/GetState` was
//! discovered by reading the `RebalancerServiceBuilder` codegen in
//! `target/debug/build/crabka-rebalancer-*/out/crabka.rebalancer.v1.rs`
//! — it calls `router.route("/crabka.rebalancer.v1.Rebalancer/GetState",
//! ...)` verbatim, matching the canonical Connect/gRPC path format
//! (`<package>.<Service>/<Method>`).

#![cfg(not(target_os = "windows"))]
#![allow(clippy::pedantic)]

use std::time::{Duration, Instant};

use crabka_broker::{Broker, BrokerConfig};

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn connect_get_state_over_http_json() {
    // 1. Boot a broker.
    let dir = tempfile::tempdir().unwrap();
    let cfg = BrokerConfig::for_tests(dir.path().to_path_buf());
    let broker = Broker::start(cfg).await.unwrap();
    let broker_addr = broker.listen_addr();

    // 2. Pick an ephemeral local port for the rebalancer.
    let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let rebal_port = listener.local_addr().unwrap().port();
    drop(listener);
    let rebal_addr = format!("127.0.0.1:{rebal_port}");

    // 3. Spawn the binary. `CARGO_BIN_EXE_<name>` is set automatically by
    // cargo when an integration test in the same crate references the
    // binary target.
    let bin_path = env!("CARGO_BIN_EXE_crabka-rebalancer");
    let mut child = tokio::process::Command::new(bin_path)
        .arg("--bootstrap-servers")
        .arg(broker_addr.to_string())
        .arg("--listen-addr")
        .arg(&rebal_addr)
        .arg("--scrape-interval-secs")
        .arg("1")
        .env("RUST_LOG", "crabka_rebalancer=info,warn")
        .stdout(std::process::Stdio::piped())
        .stderr(std::process::Stdio::piped())
        .kill_on_drop(true)
        .spawn()
        .expect("spawn crabka-rebalancer");

    // 4. Wait for /readyz to become 200. The rebalancer flips /readyz
    // green only once the ingester has written its first snapshot, so a
    // 200 here proves both the HTTP listener and the admin-RPC bootstrap
    // worked.
    let client = reqwest::Client::new();
    let deadline = Instant::now() + Duration::from_secs(15);
    loop {
        match client
            .get(format!("http://{rebal_addr}/readyz"))
            .send()
            .await
        {
            Ok(r) if r.status() == reqwest::StatusCode::OK => break,
            _ => {}
        }
        assert!(
            Instant::now() < deadline,
            "rebalancer /readyz never returned 200"
        );
        tokio::time::sleep(Duration::from_millis(200)).await;
    }

    // 5. POST {} as JSON to the canonical Connect route for GetState.
    let resp = client
        .post(format!(
            "http://{rebal_addr}/crabka.rebalancer.v1.Rebalancer/GetState"
        ))
        .header("Content-Type", "application/json")
        .body("{}")
        .send()
        .await
        .expect("Connect POST");
    let status = resp.status();
    let body_text = resp.text().await.unwrap_or_default();
    assert!(status.is_success(), "got {status}: {body_text}");
    let body: serde_json::Value =
        serde_json::from_str(&body_text).expect("response body parses as JSON");

    // 6. Sanity: response shape matches the proto. pbjson emits
    // `snapshotAtMs` (camelCase) but accept either casing in case
    // codegen settings change later.
    assert!(body.is_object(), "expected JSON object, got {body}");
    assert!(
        body.get("snapshotAtMs").is_some() || body.get("snapshot_at_ms").is_some(),
        "missing snapshotAtMs / snapshot_at_ms: {body}"
    );

    let _ = child.kill().await;
    broker.shutdown().await;
    // Leak the tempdir rather than let `Drop` fight with the broker's
    // background tasks during shutdown; the OS will clean up the
    // tempfile-prefixed dir on next reboot.
    std::mem::forget(dir);
}
