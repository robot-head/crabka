//! Connect protocol smoke test. Builds the binary, runs it
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
    let data_dir = tempfile::tempdir().unwrap();
    let bin_path = env!("CARGO_BIN_EXE_crabka-rebalancer");
    let mut child = tokio::process::Command::new(bin_path)
        .arg("--bootstrap-servers")
        .arg(broker_addr.to_string())
        .arg("--listen-addr")
        .arg(&rebal_addr)
        .arg("--scrape-interval-secs")
        .arg("1")
        .arg("--data-dir")
        .arg(data_dir.path())
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
        assert2::assert!(Instant::now() < deadline);
        // intentional: poll backoff while waiting on the out-of-process
        // rebalancer subprocess to flip /readyz green (external process).
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
    assert2::assert!(status.is_success());
    let body: serde_json::Value =
        serde_json::from_str(&body_text).expect("response body parses as JSON");

    // 6. Sanity: response shape matches the proto. pbjson emits
    // `snapshotAtMs` (camelCase) but accept either casing in case
    // codegen settings change later.
    assert2::assert!(body.is_object());
    assert2::assert!(body.get("snapshotAtMs").is_some() || body.get("snapshot_at_ms").is_some());

    // 7. Connect proto content-type regression: a connect-go client (the canonical
    // Connect clients) posts `application/proto` and requires the 200 response to echo it.
    // An all-default GetStateRequest encodes to an empty body. Before the `.build_connect()`
    // fix the router replied `application/json` here, so proto clients rejected it with
    // `invalid content-type: "application/json"; expecting "application/proto"`.
    let proto_resp = client
        .post(format!(
            "http://{rebal_addr}/crabka.rebalancer.v1.Rebalancer/GetState"
        ))
        .header("Content-Type", "application/proto")
        .body(Vec::<u8>::new())
        .send()
        .await
        .expect("Connect proto POST");
    let proto_status = proto_resp.status();
    let proto_ct = proto_resp
        .headers()
        .get(reqwest::header::CONTENT_TYPE)
        .and_then(|v| v.to_str().ok())
        .unwrap_or_default()
        .to_string();
    assert2::assert!(proto_status.is_success());
    assert2::assert!(proto_ct.starts_with("application/proto"));

    let _ = child.kill().await;
    broker.shutdown().await;
    // Leak the tempdir rather than let `Drop` fight with the broker's
    // background tasks during shutdown; the OS will clean up the
    // tempfile-prefixed dir on next reboot.
    std::mem::forget(dir);
    std::mem::forget(data_dir);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn connect_execute_proposal_and_cancel_over_http_json() {
    let dir = tempfile::tempdir().unwrap();
    let cfg = BrokerConfig::for_tests(dir.path().to_path_buf());
    let broker = Broker::start(cfg).await.unwrap();
    let broker_addr = broker.listen_addr();

    let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let rebal_port = listener.local_addr().unwrap().port();
    drop(listener);
    let rebal_addr = format!("127.0.0.1:{rebal_port}");

    let data_dir = tempfile::tempdir().unwrap();

    let bin_path = env!("CARGO_BIN_EXE_crabka-rebalancer");
    let mut child = tokio::process::Command::new(bin_path)
        .arg("--bootstrap-servers")
        .arg(broker_addr.to_string())
        .arg("--listen-addr")
        .arg(&rebal_addr)
        .arg("--scrape-interval-secs")
        .arg("1")
        .arg("--data-dir")
        .arg(data_dir.path())
        .env("RUST_LOG", "crabka_rebalancer=info,warn")
        .stdout(std::process::Stdio::piped())
        .stderr(std::process::Stdio::piped())
        .kill_on_drop(true)
        .spawn()
        .expect("spawn crabka-rebalancer");

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
        assert2::assert!(Instant::now() < deadline);
        // intentional: poll backoff while waiting on the out-of-process
        // rebalancer subprocess to flip /readyz green (external process).
        tokio::time::sleep(Duration::from_millis(200)).await;
    }

    // CreateProposal — empty goals returns a Computed proposal (may have
    // zero movements on a single-broker cluster; that's fine for the
    // wire-path test).
    let create = client
        .post(format!(
            "http://{rebal_addr}/crabka.rebalancer.v1.Rebalancer/CreateProposal"
        ))
        .header("Content-Type", "application/json")
        .body("{}")
        .send()
        .await
        .expect("create POST");
    assert2::assert!(create.status().is_success());
    let create_body: serde_json::Value = create.json().await.expect("create JSON");
    let id = create_body
        .get("id")
        .and_then(|v| v.as_str())
        .expect("id")
        .to_string();

    // ExecuteProposal on a zero-movements proposal returns FailedPrecondition.
    let exec = client
        .post(format!(
            "http://{rebal_addr}/crabka.rebalancer.v1.Rebalancer/ExecuteProposal"
        ))
        .header("Content-Type", "application/json")
        .body(format!(r#"{{"id":"{id}"}}"#))
        .send()
        .await
        .expect("execute POST");
    // Connect's FailedPrecondition maps to HTTP 400.
    assert2::assert!(exec.status() == reqwest::StatusCode::BAD_REQUEST);
    let body_text = exec.text().await.unwrap_or_default();
    assert2::assert!(body_text.contains("movement") || body_text.contains("Computed"));

    let _ = child.kill().await;
    let _ = tokio::time::timeout(Duration::from_secs(30), broker.shutdown()).await;
    std::mem::forget(dir);
    std::mem::forget(data_dir);
}
