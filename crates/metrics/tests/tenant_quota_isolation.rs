//! Slice 8 hardening: per-tenant ingestion-quota isolation in the distributor.
//!
//! Boots the distributor HTTP router on a real ephemeral `127.0.0.1:0` socket
//! with an in-memory `WalSink` and per-tenant limit overrides. `org-a` gets a
//! TIGHT ingestion-rate quota; `org-b` gets a generous one. We push `remote_write`
//! to `org-a` until it returns HTTP 429, then push the SAME load to `org-b` and
//! assert it still succeeds — proving the token bucket is per-tenant, not global.

use std::{
    net::SocketAddr,
    sync::{Arc, Mutex},
};

use async_trait::async_trait;
use bytes::Bytes;
use crabka_metrics::{
    OverridesProvider, WalRecord,
    distributor::{DistributorState, ProduceError, WalSink, serve},
    wire::pb,
};
use prost::Message;

const ORG_A: &str = "org-a";
const ORG_B: &str = "org-b";

/// In-memory WAL sink: records every appended `WalRecord`, never touches a broker.
#[derive(Default)]
struct RecordingSink {
    records: Mutex<Vec<WalRecord>>,
}

#[async_trait]
impl WalSink for RecordingSink {
    async fn append(&self, _key: Bytes, record: WalRecord) -> Result<(), ProduceError> {
        self.records
            .lock()
            .expect("recording sink poisoned")
            .push(record);
        Ok(())
    }
}

impl RecordingSink {
    fn len(&self) -> usize {
        self.records.lock().expect("recording sink poisoned").len()
    }
}

/// Minimal `remote_write` v1 body: a single `up` series with one sample, snappy
/// compressed (the distributor requires `Content-Encoding: snappy`).
fn remote_write_v1_body() -> Vec<u8> {
    let req = pb::v1::WriteRequest {
        timeseries: vec![pb::v1::TimeSeries {
            labels: vec![pb::v1::Label {
                name: "__name__".into(),
                value: "up".into(),
            }],
            samples: vec![pb::v1::Sample {
                value: 1.0,
                timestamp: 100,
            }],
            ..Default::default()
        }],
        ..Default::default()
    };
    snap::raw::Encoder::new()
        .compress_vec(&req.encode_to_vec())
        .expect("snappy compress")
}

/// Per-tenant overrides: org-a is rate-limited to a single sample of burst;
/// org-b is effectively unlimited. Unlisted tenants fall back to defaults.
fn tenant_overrides() -> OverridesProvider {
    let yaml = "
overrides:
  org-a:
    ingestion_rate: 1
    ingestion_burst_size: 1
  org-b:
    ingestion_rate: 1000000
    ingestion_burst_size: 1000000
";
    OverridesProvider::from_yaml(yaml).expect("parse tenant overrides")
}

async fn boot_distributor() -> (SocketAddr, Arc<RecordingSink>) {
    let sink = Arc::new(RecordingSink::default());
    let state = Arc::new(DistributorState::new(sink.clone()).with_overrides(tenant_overrides()));
    let addr = serve(
        "127.0.0.1:0".parse().expect("socket addr"),
        state,
        std::future::pending(),
    )
    .await
    .expect("serve distributor");
    (addr, sink)
}

async fn push(client: &reqwest::Client, addr: SocketAddr, tenant: &str) -> reqwest::StatusCode {
    client
        .post(format!("http://{addr}/api/v1/push"))
        .header("Content-Type", "application/x-protobuf")
        .header("Content-Encoding", "snappy")
        .header("X-Scope-OrgID", tenant)
        .body(remote_write_v1_body())
        .send()
        .await
        .expect("send push")
        .status()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn per_tenant_quota_is_isolated() {
    let (addr, sink) = boot_distributor().await;
    let client = reqwest::Client::new();

    // Drive org-a until its tight bucket rejects with 429. Bounded loop so a
    // regression (global / never-tripping quota) fails fast instead of hanging.
    let mut org_a_throttled = false;
    for _ in 0..50 {
        let status = push(&client, addr, ORG_A).await;
        if status == reqwest::StatusCode::TOO_MANY_REQUESTS {
            org_a_throttled = true;
            break;
        }
        assert2::assert!(status.is_success());
    }
    assert2::assert!(org_a_throttled);

    // Same load under org-b's own tenant header must still be accepted: the
    // token bucket is per-tenant, so org-a draining its bucket cannot starve
    // org-b. A global bucket would already be empty here and return 429.
    let appends_before_b = sink.len();
    for _index in 0..10 {
        let status = push(&client, addr, ORG_B).await;
        assert2::assert!(status.is_success());
    }

    // org-b's accepted pushes must have reached the WAL sink.
    assert2::assert!(sink.len() >= appends_before_b + 10);

    // Sanity: org-a really is still throttled (its bucket stays drained) while
    // org-b keeps succeeding — confirms the two buckets are independent.
    assert2::assert!(push(&client, addr, ORG_A).await == reqwest::StatusCode::TOO_MANY_REQUESTS);
    assert2::assert!(push(&client, addr, ORG_B).await.is_success());
}
