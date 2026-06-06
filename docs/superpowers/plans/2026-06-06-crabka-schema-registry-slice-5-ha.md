# Crabka Schema Registry — Slice 5 (HA: cp-exact election + write-forwarding) Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Make the registry multi-node HA — registry nodes elect one **primary** via a cp-exact `"sr"` Kafka group; secondaries **forward** mutating REST to the primary and serve reads; failover on primary loss — validated against `cp-schema-registry 7.4.0`.

**Architecture:** A self-contained `election` module joins a `"sr"` group through the Crabka broker's protocol-generic coordinator (over `client-core`'s generic `Client::send`), advertising the node's REST URL in cp's `SchemaRegistryIdentity` JoinGroup metadata; the group leader runs cp's deterministic master-selection and broadcasts the master in every SyncGroup assignment. The election task publishes a `PrimaryState { is_primary, primary_url }` over a `watch` channel; an axum forwarding middleware proxies mutating REST from secondaries to the primary. The `KafkaStore` write-gate is unchanged — only the primary's store ever writes `_schemas`.

**Tech Stack:** Rust 2024; `crabka_client_core::Client` (generic `send`), `crabka_protocol::owned` group-membership codecs (`JoinGroup`/`SyncGroup`/`Heartbeat`/`LeaveGroup`/`FindCoordinator`/`DescribeGroups`), `serde_json` (cp's identity/assignment JSON), `axum` middleware + `tokio::sync::watch`, `reqwest` (forwarding). Tests: pure serde unit tests, broker-backed in-process multi-node `ha.rs`, a `#[ignore]` Docker election capture.

---

## Design reference

Spec: `docs/superpowers/specs/2026-06-06-crabka-schema-registry-slice-5-ha-design.md`. Read it.

### Verified existing signatures (grounded in the current tree)
```rust
// config.rs  — pub struct RegistryConfig { bootstrap, schemas_topic, schemas_topic_rf, client_id }  (all String/i32)
// bin/schema-registry.rs — clap Args { bootstrap_servers, listen_addr: SocketAddr, schemas_topic, schemas_topic_rf, client_id };
//   main: build RegistryConfig → KafkaStore::start(&cfg, shutdown) → rest::router(AppState{store}) → axum::serve(listener, app)
// kafkastore/mod.rs — pub async fn KafkaStore::start(&RegistryConfig, CancellationToken) -> anyhow::Result<Arc<KafkaStore>>
// rest/mod.rs — pub fn router(state: AppState) -> axum::Router ; pub struct AppState { pub store: Arc<KafkaStore> }

// crabka_client_core::Client (bon builder):
//   Client::builder().bootstrap("host:port").client_id("id").build().await -> Result<Client, ClientError>
//   pub async fn send<R: ProtocolRequest>(&self, req: R) -> Result<R::Response, ClientError>   (auto version-negotiation)

// crabka_protocol::owned group-membership codecs (metadata/assignment are ::bytes::Bytes; structs derive Default):
//   find_coordinator_request::FindCoordinatorRequest { key: String, key_type: i8, coordinator_keys: Vec<String>, .. }
//   find_coordinator_response::FindCoordinatorResponse { error_code, node_id, host, port, coordinators: Vec<Coordinator{key,node_id,host,port,error_code,..}>, .. }
//   join_group_request::{JoinGroupRequest { group_id, session_timeout_ms, rebalance_timeout_ms, member_id, group_instance_id: Option<String>, protocol_type, protocols: Vec<JoinGroupRequestProtocol>, reason, .. }, JoinGroupRequestProtocol { name: String, metadata: Bytes, .. }}
//   join_group_response::{JoinGroupResponse { error_code, generation_id, protocol_type: Option<String>, protocol_name: Option<String>, leader: String, member_id: String, members: Vec<JoinGroupResponseMember>, .. }, JoinGroupResponseMember { member_id, group_instance_id, metadata: Bytes, .. }}
//   sync_group_request::{SyncGroupRequest { group_id, generation_id, member_id, group_instance_id, protocol_type: Option<String>, protocol_name: Option<String>, assignments: Vec<SyncGroupRequestAssignment>, .. }, SyncGroupRequestAssignment { member_id, assignment: Bytes, .. }}
//   sync_group_response::SyncGroupResponse { error_code, assignment: Bytes, .. }
//   heartbeat_request::HeartbeatRequest { group_id, generation_id, member_id, group_instance_id, .. }   heartbeat_response::HeartbeatResponse { error_code, .. }
//   leave_group_request::{LeaveGroupRequest { group_id, member_id, members: Vec<MemberIdentity>, .. }, MemberIdentity { member_id, group_instance_id, reason, .. }}   leave_group_response::LeaveGroupResponse { error_code, .. }
//   describe_groups_response::{DescribeGroupsResponse { groups: Vec<DescribedGroup>, .. }, DescribedGroup { group_id, group_state, protocol_type, members: Vec<DescribedGroupMember>, .. }, DescribedGroupMember { member_id, member_metadata: Bytes, member_assignment: Bytes, .. }}
// Kafka error codes (define LOCALLY to avoid a crabka-broker dep): NONE=0, COORDINATOR_LOAD_IN_PROGRESS=14, COORDINATOR_NOT_AVAILABLE=15, NOT_COORDINATOR=16, ILLEGAL_GENERATION=22, UNKNOWN_MEMBER_ID=25, REBALANCE_IN_PROGRESS=27, MEMBER_ID_REQUIRED=79
// Deps already present in schema-registry/Cargo.toml: crabka-client-core, crabka-protocol, crabka-client-admin. reqwest is a DEV-dep (move to deps in Task 3).
```

### Branch / commit / gate discipline (executors read this)
- Worktree: `/Users/mattstone/git/crabka/.claude/worktrees/musing-cartwright-7af144`. Branch: `claude/schema-registry-slice-5` (assert NOT main). Always `git -C <worktree>`. Do NOT push (controller handles push/PR; stacks on slice-4 PR #410).
- Commits: `git -C <worktree> -c user.name="Matthew Stone" -c user.email="matthew.d.stone@gmail.com"`; never `git config`; body ends `Co-Authored-By: Claude Opus 4.8 <noreply@anthropic.com>`.
- **Per change before commit:** `cargo clippy -p crabka-schema-registry --all-targets -- -D warnings` + `cargo fmt -p crabka-schema-registry`. `git add` only the task's files.
- **Greenfield (CLAUDE.md):** clean signature changes, no shims. Every task leaves the crate compiling + all tests green (compat conformance Avro 21 / Protobuf 88 / JSON 92 + slice 1–4 tests stay green).
- **cp is authority** for the election wire — the `SchemaRegistryIdentity`/assignment bytes, the `protocol_type`/protocol-name, and the leader's master-selection rule are **seeded then pinned to the Task-5 Docker capture**.

---

## File structure
```
crates/schema-registry/src/
  config.rs              # + advertised_url, group_id, leader_eligibility
  election/mod.rs        # NEW: PrimaryState, Election::start (spawns the task, returns the watch Receiver)
  election/protocol.rs   # NEW: SchemaRegistryIdentity, SchemaRegistryGroupAssignment, select_master, consts
  election/client.rs     # NEW: the "sr" group-membership loop (FindCoordinator/JoinGroup/SyncGroup/Heartbeat/LeaveGroup)
  rest/forward.rs        # NEW: the forwarding middleware (PrimaryState + reqwest)
  rest/mod.rs            # wire the forwarding layer onto the router
  lib.rs                 # pub mod election;
  bin/schema-registry.rs # CLI args + start election + wrap router with the middleware
crates/schema-registry/Cargo.toml   # reqwest dev-dep -> dep
crates/schema-registry/tests/
  ha.rs                          # NEW: in-process multi-node election + forwarding + failover (no Docker)
  capture_election_fixtures.rs   # NEW: #[ignore] Docker — two cp SR nodes elect via Crabka broker; DescribeGroups capture
  fixtures/election/*.json       # NEW captured cp ground truth
```

## Execution tasks (sequential; one implementer per task)
- **Task 1** — config fields + the `"sr"` protocol types (`SchemaRegistryIdentity`/`SchemaRegistryGroupAssignment` serde + `select_master`) + unit tests.
- **Task 2** — the group-membership client (`election/client.rs`) + `Election::start` + `PrimaryState` (`election/mod.rs`) + a single-node election integration test.
- **Task 3** — the forwarding middleware (`rest/forward.rs`) + router wiring + binary wiring + `reqwest`→dep + forwarding unit tests.
- **Task 4** — in-process multi-node `ha.rs` (2–3 nodes: one primary, secondary-forwards-write, reads-everywhere, failover).
- **Task 5** — cp Docker election capture + byte/protocol-name/selection-rule calibration.

---

## Task 1: config fields + `"sr"` protocol types

**Files:** Modify `src/config.rs`, `src/lib.rs`; Create `src/election/mod.rs`, `src/election/protocol.rs`.

- [ ] **Step 1: Add config fields (`config.rs`).** Append to `RegistryConfig`:
```rust
    /// This node's externally reachable REST URL, advertised to peers for
    /// write-forwarding, e.g. `http://10.0.0.5:8081`.
    pub advertised_url: String,
    /// The primary-election group id (Confluent default: `schema-registry`).
    pub group_id: String,
    /// Whether this node may be elected primary.
    pub leader_eligibility: bool,
```
Update the existing `RegistryConfig { .. }` literals so the crate still compiles: in `bin/schema-registry.rs` (Task 3 wires real values — for now set `advertised_url: format!("http://{}", args.listen_addr)`, `group_id: "schema-registry".into()`, `leader_eligibility: true`) and in EVERY test that constructs `RegistryConfig` (grep `RegistryConfig {` across `tests/`: `interop.rs`, `integration.rs` `boot_registry`, `rest_conformance.rs`, the capture harnesses — add `advertised_url: "http://127.0.0.1:0".into(), group_id: "schema-registry".into(), leader_eligibility: true`).

- [ ] **Step 2: Register the module (`lib.rs`).** Add `pub mod election;` next to the other `pub mod`s.

- [ ] **Step 3: Create `src/election/mod.rs` skeleton** (so the module exists; `Election`/`PrimaryState` filled in Task 2):
```rust
//! Schema Registry primary election (cp-exact `"sr"` Kafka group). A node joins
//! the group; the leader selects the primary and broadcasts it; every node
//! publishes its `PrimaryState` for the forwarding middleware.

pub mod client;
pub mod protocol;

/// Who the primary is, from this node's point of view.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct PrimaryState {
    pub is_primary: bool,
    pub primary_url: Option<String>,
}
```

- [ ] **Step 4: Write failing protocol unit tests (`election/protocol.rs` `mod tests`).**
```rust
#[cfg(test)]
mod tests {
    use super::*;

    fn id(host: &str, port: i32, eligible: bool) -> SchemaRegistryIdentity {
        SchemaRegistryIdentity {
            version: 1,
            host: host.into(),
            port,
            scheme: "http".into(),
            master_eligibility: eligible,
        }
    }

    #[test]
    fn identity_json_round_trips_and_is_field_ordered() {
        let i = id("h", 8081, true);
        let bytes = serde_json::to_vec(&i).unwrap();
        // cp's SchemaRegistryIdentity JSON (field order pinned; calibrated in Task 5).
        assert_eq!(
            String::from_utf8(bytes.clone()).unwrap(),
            r#"{"version":1,"host":"h","port":8081,"master_eligibility":true,"scheme":"http"}"#
        );
        let back: SchemaRegistryIdentity = serde_json::from_slice(&bytes).unwrap();
        assert_eq!(back, i);
    }

    #[test]
    fn assignment_round_trips_with_and_without_master() {
        let a = SchemaRegistryGroupAssignment { error: 0, master: Some(id("h", 8081, true)) };
        let b: SchemaRegistryGroupAssignment =
            serde_json::from_slice(&serde_json::to_vec(&a).unwrap()).unwrap();
        assert_eq!(a, b);
        let none = SchemaRegistryGroupAssignment { error: 1, master: None };
        let n: SchemaRegistryGroupAssignment =
            serde_json::from_slice(&serde_json::to_vec(&none).unwrap()).unwrap();
        assert_eq!(none, n);
    }

    #[test]
    fn select_master_picks_a_deterministic_eligible_member() {
        // Two eligible + one ineligible → the deterministic winner is the same
        // regardless of input order. (Exact cp comparator pinned in Task 5.)
        let a = ("m2".to_string(), id("b", 8081, true));
        let b = ("m1".to_string(), id("a", 8081, true));
        let c = ("m3".to_string(), id("z", 8081, false)); // ineligible
        let pick1 = select_master(&[a.clone(), b.clone(), c.clone()]);
        let pick2 = select_master(&[c, b, a]);
        assert_eq!(pick1, pick2);
        assert!(pick1.as_ref().unwrap().master_eligibility);
    }

    #[test]
    fn select_master_none_when_no_eligible() {
        assert!(select_master(&[("m".into(), id("a", 1, false))]).is_none());
    }

    #[test]
    fn identity_url_builds_scheme_host_port() {
        assert_eq!(id("h", 8081, true).url(), "http://h:8081");
    }
}
```

- [ ] **Step 5: Run — expect FAIL:** `cargo test -p crabka-schema-registry --lib election::protocol`.

- [ ] **Step 6: Implement `election/protocol.rs`.**
```rust
//! cp's `SchemaRegistryProtocol` wire types (the `"sr"` group), serialized
//! byte-exactly to cp-schema-registry 7.4.0 (JSON; calibrated in Task 5).

use serde::{Deserialize, Serialize};

/// Protocol type for the SR election group (cp constant).
pub const SR_PROTOCOL_TYPE: &str = "sr";
/// The single JoinGroup protocol name cp advertises (seed; cp-captured in Task 5).
pub const SR_PROTOCOL_NAME: &str = "v1";

/// A node's identity, serialized into the JoinGroup protocol `metadata`.
/// Field order is fixed to match cp's `SchemaRegistryIdentity` JSON.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SchemaRegistryIdentity {
    pub version: i32,
    pub host: String,
    pub port: i32,
    pub master_eligibility: bool,
    pub scheme: String,
}

impl SchemaRegistryIdentity {
    /// The node's advertised REST base URL.
    #[must_use]
    pub fn url(&self) -> String {
        format!("{}://{}:{}", self.scheme, self.host, self.port)
    }
}

/// The SyncGroup assignment cp's leader broadcasts: which identity is master.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct SchemaRegistryGroupAssignment {
    pub error: i32,
    #[serde(default)]
    pub master: Option<SchemaRegistryIdentity>,
}

/// The leader's deterministic master-selection among the eligible members.
/// Seed rule: the eligible member whose identity sorts first by `(host, port)`;
/// ties broken by `member_id`. The EXACT cp comparator is pinned in Task 5.
#[must_use]
pub fn select_master(
    members: &[(String, SchemaRegistryIdentity)],
) -> Option<SchemaRegistryIdentity> {
    members
        .iter()
        .filter(|(_, id)| id.master_eligibility)
        .min_by(|(am, ai), (bm, bi)| {
            (ai.host.as_str(), ai.port, am.as_str())
                .cmp(&(bi.host.as_str(), bi.port, bm.as_str()))
        })
        .map(|(_, id)| id.clone())
}
```
> NOTE: `master_eligibility`/`scheme` field order + the `SR_PROTOCOL_NAME` + the `select_master` comparator are SEEDS; Task 5's cp capture is authority and re-tunes them (and the Step-4 `identity_json_round_trips` expected string).

- [ ] **Step 7: Run — expect PASS:** `cargo test -p crabka-schema-registry --lib election`.

- [ ] **Step 8: Run the full crate** (config arity ripple): `cargo test -p crabka-schema-registry --lib --test integration --test compat_conformance --test interop` → green (the `RegistryConfig` literals in tests now have the 3 new fields). clippy + fmt.

- [ ] **Step 9: Commit** (`src/config.rs`, `src/lib.rs`, `src/election/{mod,protocol}.rs`, `src/bin/schema-registry.rs`, the test files whose `RegistryConfig` literals were bumped):
`schema-registry: HA config fields + cp "sr" election protocol types (identity/assignment/select_master)`

---

## Task 2: the `"sr"` group-membership client + `Election::start` + `PrimaryState`

**Files:** Create `src/election/client.rs`; Modify `src/election/mod.rs`; Test: `tests/ha.rs` (a single-node election test first; multi-node in Task 4).

- [ ] **Step 1: Write a failing single-node election integration test (`tests/ha.rs`).** Boot one in-process broker; start one election against it; assert the node becomes primary.
```rust
#![cfg(not(target_os = "windows"))]

use std::time::Duration;
use crabka_broker::{Broker, BrokerConfig};
use crabka_schema_registry::config::RegistryConfig;
use crabka_schema_registry::election::{Election, PrimaryState};
use tokio_util::sync::CancellationToken;

fn cfg(bootstrap: &str, port: i32) -> RegistryConfig {
    RegistryConfig {
        bootstrap: bootstrap.into(),
        schemas_topic: "_schemas".into(),
        schemas_topic_rf: 1,
        client_id: format!("sr-{port}"),
        advertised_url: format!("http://127.0.0.1:{port}"),
        group_id: "schema-registry".into(),
        leader_eligibility: true,
    }
}

/// Wait until `pred(state)` holds or `secs` elapses; returns the matching state.
async fn await_state(
    rx: &mut tokio::sync::watch::Receiver<PrimaryState>,
    secs: u64,
    pred: impl Fn(&PrimaryState) -> bool,
) -> PrimaryState {
    let deadline = tokio::time::Instant::now() + Duration::from_secs(secs);
    loop {
        if pred(&rx.borrow()) {
            return rx.borrow().clone();
        }
        tokio::select! {
            _ = tokio::time::sleep_until(deadline) => panic!("state never matched: {:?}", *rx.borrow()),
            r = rx.changed() => { r.expect("election task alive"); }
        }
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn single_node_becomes_primary() {
    let dir = tempfile::tempdir().unwrap();
    let broker = Broker::start(BrokerConfig::for_tests(dir.path().to_path_buf())).await.unwrap();
    let cancel = CancellationToken::new();
    let c = cfg(&broker.listen_addr().to_string(), 8081);
    let mut rx = Election::start(&c, cancel.clone()).await.unwrap();
    let st = await_state(&mut rx, 20, |s| s.is_primary).await;
    assert!(st.is_primary);
    assert_eq!(st.primary_url.as_deref(), Some("http://127.0.0.1:8081"));
    cancel.cancel();
    broker.shutdown().await;
}
```

- [ ] **Step 2: Run — expect FAIL** (`Election::start` missing): `cargo test -p crabka-schema-registry --test ha single_node`.

- [ ] **Step 3: Implement `election/client.rs`** (the group-membership loop). Define the local Kafka error consts + the client.
```rust
//! The `"sr"` group-membership loop: FindCoordinator → JoinGroup → (leader:
//! select+assign) SyncGroup → Heartbeat, rejoining on rebalance and leaving on
//! shutdown. Generic over `protocol_type` + opaque JSON metadata/assignment;
//! models `client-consumer`'s coordinator loop without consumer semantics.

use std::time::Duration;

use bytes::Bytes;
use crabka_client_core::Client;
use crabka_protocol::owned::find_coordinator_request::FindCoordinatorRequest;
use crabka_protocol::owned::heartbeat_request::HeartbeatRequest;
use crabka_protocol::owned::join_group_request::{JoinGroupRequest, JoinGroupRequestProtocol};
use crabka_protocol::owned::leave_group_request::{LeaveGroupRequest, MemberIdentity};
use crabka_protocol::owned::sync_group_request::{SyncGroupRequest, SyncGroupRequestAssignment};
use tokio::sync::watch;
use tokio_util::sync::CancellationToken;

use super::PrimaryState;
use super::protocol::{
    SR_PROTOCOL_NAME, SR_PROTOCOL_TYPE, SchemaRegistryGroupAssignment, SchemaRegistryIdentity,
    select_master,
};

// Kafka group error codes (defined locally to avoid a crabka-broker dependency).
const NONE: i16 = 0;
const COORDINATOR_LOAD_IN_PROGRESS: i16 = 14;
const COORDINATOR_NOT_AVAILABLE: i16 = 15;
const NOT_COORDINATOR: i16 = 16;
const ILLEGAL_GENERATION: i16 = 22;
const UNKNOWN_MEMBER_ID: i16 = 25;
const REBALANCE_IN_PROGRESS: i16 = 27;
const MEMBER_ID_REQUIRED: i16 = 79;

const SESSION_TIMEOUT_MS: i32 = 10_000;
const REBALANCE_TIMEOUT_MS: i32 = 30_000;
const HEARTBEAT_INTERVAL: Duration = Duration::from_secs(3);

pub(super) struct ElectionClient {
    pub bootstrap: String,
    pub client_id: String,
    pub group_id: String,
    pub identity: SchemaRegistryIdentity,
    pub tx: watch::Sender<PrimaryState>,
}

impl ElectionClient {
    /// Run until `cancel` fires. Reconnects + rejoins on any error; publishes
    /// `PrimaryState` after each successful SyncGroup.
    pub async fn run(self, cancel: CancellationToken) {
        let mut member_id = String::new();
        loop {
            if cancel.is_cancelled() {
                return;
            }
            match self.connect_and_run(&mut member_id, &cancel).await {
                Ok(()) => return, // cancelled mid-loop
                Err(e) => {
                    tracing::warn!(error = %e, "election: reconnecting after error");
                    // unknown member on reconnect: rejoin from scratch
                    member_id.clear();
                    let _ = self.tx.send(PrimaryState::default());
                    if cancel
                        .run_until_cancelled(tokio::time::sleep(Duration::from_millis(500)))
                        .await
                        .is_none()
                    {
                        return;
                    }
                }
            }
        }
    }

    async fn connect_and_run(
        &self,
        member_id: &mut String,
        cancel: &CancellationToken,
    ) -> anyhow::Result<()> {
        let coord = self.connect_coordinator().await?;
        loop {
            let (gen, assignment) = self.join_and_sync(&coord, member_id).await?;
            self.publish(&assignment);
            // heartbeat until a rebalance/error forces a rejoin
            loop {
                tokio::select! {
                    () = cancel.cancelled() => {
                        let _ = coord.send(LeaveGroupRequest {
                            group_id: self.group_id.clone(),
                            member_id: member_id.clone(),
                            members: vec![MemberIdentity { member_id: member_id.clone(), ..Default::default() }],
                            ..Default::default()
                        }).await;
                        return Ok(());
                    }
                    () = tokio::time::sleep(HEARTBEAT_INTERVAL) => {
                        let hb = coord.send(HeartbeatRequest {
                            group_id: self.group_id.clone(),
                            generation_id: gen,
                            member_id: member_id.clone(),
                            ..Default::default()
                        }).await?;
                        match hb.error_code {
                            NONE => continue,
                            REBALANCE_IN_PROGRESS | ILLEGAL_GENERATION => break, // rejoin (keep member_id)
                            UNKNOWN_MEMBER_ID => { member_id.clear(); break; }   // rejoin from scratch
                            NOT_COORDINATOR | COORDINATOR_NOT_AVAILABLE | COORDINATOR_LOAD_IN_PROGRESS => {
                                anyhow::bail!("heartbeat coordinator error {}", hb.error_code); // reconnect
                            }
                            other => { tracing::debug!(code = other, "heartbeat transient"); }
                        }
                    }
                }
            }
        }
    }

    /// FindCoordinator via the bootstrap, then a Client to the coordinator.
    async fn connect_coordinator(&self) -> anyhow::Result<Client> {
        let boot = Client::builder()
            .bootstrap(self.bootstrap.clone())
            .client_id(self.client_id.clone())
            .build()
            .await?;
        let fc = boot
            .send(FindCoordinatorRequest {
                key: self.group_id.clone(),
                key_type: 0, // group
                coordinator_keys: vec![self.group_id.clone()],
                ..Default::default()
            })
            .await?;
        let (host, port) = fc
            .coordinators
            .first()
            .map(|c| (c.host.clone(), c.port))
            .filter(|(h, _)| !h.is_empty())
            .or_else(|| (!fc.host.is_empty()).then(|| (fc.host.clone(), fc.port)))
            .ok_or_else(|| anyhow::anyhow!("no coordinator for group {}", self.group_id))?;
        Ok(Client::builder()
            .bootstrap(format!("{host}:{port}"))
            .client_id(self.client_id.clone())
            .build()
            .await?)
    }

    /// JoinGroup (+ MEMBER_ID_REQUIRED two-step) then SyncGroup; as leader,
    /// select the master and assign it to every member. Returns (generation,
    /// our assignment bytes).
    async fn join_and_sync(
        &self,
        coord: &Client,
        member_id: &mut String,
    ) -> anyhow::Result<(i32, Bytes)> {
        let metadata = Bytes::from(serde_json::to_vec(&self.identity)?);
        let mk_join = |mid: String| JoinGroupRequest {
            group_id: self.group_id.clone(),
            session_timeout_ms: SESSION_TIMEOUT_MS,
            rebalance_timeout_ms: REBALANCE_TIMEOUT_MS,
            member_id: mid,
            protocol_type: SR_PROTOCOL_TYPE.to_string(),
            protocols: vec![JoinGroupRequestProtocol {
                name: SR_PROTOCOL_NAME.to_string(),
                metadata: metadata.clone(),
                ..Default::default()
            }],
            ..Default::default()
        };
        let mut jg = coord.send(mk_join(member_id.clone())).await?;
        if jg.error_code == MEMBER_ID_REQUIRED {
            *member_id = jg.member_id.clone();
            jg = coord.send(mk_join(member_id.clone())).await?;
        }
        if jg.error_code != NONE {
            anyhow::bail!("JoinGroup error {}", jg.error_code);
        }
        *member_id = jg.member_id.clone();
        let assignments = if jg.leader == jg.member_id {
            // leader: decode identities, select master, assign to all members
            let ids: Vec<(String, SchemaRegistryIdentity)> = jg
                .members
                .iter()
                .filter_map(|m| {
                    serde_json::from_slice(&m.metadata)
                        .ok()
                        .map(|id| (m.member_id.clone(), id))
                })
                .collect();
            let master = select_master(&ids);
            let assign = Bytes::from(serde_json::to_vec(&SchemaRegistryGroupAssignment {
                error: 0,
                master,
            })?);
            jg.members
                .iter()
                .map(|m| SyncGroupRequestAssignment {
                    member_id: m.member_id.clone(),
                    assignment: assign.clone(),
                    ..Default::default()
                })
                .collect()
        } else {
            Vec::new()
        };
        let sg = coord
            .send(SyncGroupRequest {
                group_id: self.group_id.clone(),
                generation_id: jg.generation_id,
                member_id: member_id.clone(),
                protocol_type: Some(SR_PROTOCOL_TYPE.to_string()),
                protocol_name: jg.protocol_name.clone(),
                assignments,
                ..Default::default()
            })
            .await?;
        if sg.error_code != NONE {
            anyhow::bail!("SyncGroup error {}", sg.error_code);
        }
        Ok((jg.generation_id, sg.assignment))
    }

    fn publish(&self, assignment: &Bytes) {
        let parsed: SchemaRegistryGroupAssignment =
            serde_json::from_slice(assignment).unwrap_or_default();
        let is_primary = parsed.master.as_ref() == Some(&self.identity);
        let primary_url = parsed.master.as_ref().map(SchemaRegistryIdentity::url);
        let _ = self.tx.send(PrimaryState { is_primary, primary_url });
    }
}
```

- [ ] **Step 4: Implement `Election::start` (`election/mod.rs`).** Add below `PrimaryState`:
```rust
use tokio::sync::watch;
use tokio_util::sync::CancellationToken;

use crate::config::RegistryConfig;
use client::ElectionClient;
use protocol::SchemaRegistryIdentity;

/// The election handle: spawns the group-membership task and exposes the
/// `PrimaryState` watch.
pub struct Election;

impl Election {
    /// Parse `advertised_url` into a `SchemaRegistryIdentity`, spawn the `"sr"`
    /// group loop, and return a watch receiver of `PrimaryState`. The task runs
    /// until `cancel` fires (then it `LeaveGroup`s).
    pub async fn start(
        cfg: &RegistryConfig,
        cancel: CancellationToken,
    ) -> anyhow::Result<watch::Receiver<PrimaryState>> {
        let identity = parse_identity(&cfg.advertised_url, cfg.leader_eligibility)?;
        let (tx, rx) = watch::channel(PrimaryState::default());
        let client = ElectionClient {
            bootstrap: cfg.bootstrap.clone(),
            client_id: format!("{}-election", cfg.client_id),
            group_id: cfg.group_id.clone(),
            identity,
            tx,
        };
        tokio::spawn(client.run(cancel));
        Ok(rx)
    }
}

/// Parse `http://host:port` into a `SchemaRegistryIdentity` (version 1).
fn parse_identity(url: &str, eligible: bool) -> anyhow::Result<SchemaRegistryIdentity> {
    let (scheme, rest) = url
        .split_once("://")
        .ok_or_else(|| anyhow::anyhow!("advertised_url missing scheme: {url}"))?;
    let (host, port) = rest
        .rsplit_once(':')
        .ok_or_else(|| anyhow::anyhow!("advertised_url missing port: {url}"))?;
    Ok(SchemaRegistryIdentity {
        version: 1,
        host: host.to_string(),
        port: port.parse()?,
        master_eligibility: eligible,
        scheme: scheme.to_string(),
    })
}
```
Add a unit test for `parse_identity` in `election/mod.rs` `mod tests`:
```rust
#[cfg(test)]
mod tests {
    use super::parse_identity;
    #[test]
    fn parses_advertised_url() {
        let i = parse_identity("http://10.0.0.5:8081", true).unwrap();
        assert_eq!((i.host.as_str(), i.port, i.scheme.as_str(), i.master_eligibility), ("10.0.0.5", 8081, "http", true));
        assert!(parse_identity("nohost", true).is_err());
    }
}
```

- [ ] **Step 5: Run — expect PASS:** `cargo test -p crabka-schema-registry --test ha single_node_becomes_primary --lib election` → the single-node test elects the lone node as primary; the unit tests pass. (If the broker's `FindCoordinator` returns the single broker as coordinator and the lone member is leader+master, `is_primary` becomes true within a couple of heartbeats.)

- [ ] **Step 6: clippy + fmt + commit** (`src/election/{client,mod}.rs`, `tests/ha.rs`):
`schema-registry: "sr" group-membership election client + Election::start + PrimaryState`

---

## Task 3: forwarding middleware + binary wiring

**Files:** Create `src/rest/forward.rs`; Modify `src/rest/mod.rs`, `src/bin/schema-registry.rs`, `Cargo.toml`; tests in `src/rest/forward.rs`.

- [ ] **Step 1: Promote `reqwest` to a dependency (`Cargo.toml`).** Move the `reqwest` line from `[dev-dependencies]` to `[dependencies]` (keep the same `version`/`features`). Confirm it stays listed once.

- [ ] **Step 2: Write a failing forwarding unit test (`rest/forward.rs` `mod tests`).** The middleware's decision logic is unit-testable without a live peer via a small helper `forward_decision`.
```rust
#[cfg(test)]
mod tests {
    use super::*;
    use crate::election::PrimaryState;

    fn primary(url: &str) -> PrimaryState { PrimaryState { is_primary: true, primary_url: Some(url.into()) } }
    fn secondary(url: &str) -> PrimaryState { PrimaryState { is_primary: false, primary_url: Some(url.into()) } }

    #[test]
    fn reads_always_pass_through() {
        assert_eq!(decide(&http::Method::GET, false, &secondary("http://p:8081"), false), Decision::PassThrough);
    }
    #[test]
    fn primary_processes_writes_locally() {
        assert_eq!(decide(&http::Method::POST, false, &primary("http://me:8081"), false), Decision::PassThrough);
    }
    #[test]
    fn secondary_forwards_writes_to_primary() {
        assert_eq!(
            decide(&http::Method::POST, false, &secondary("http://p:8081"), false),
            Decision::Forward("http://p:8081".into())
        );
    }
    #[test]
    fn secondary_without_primary_is_unavailable() {
        let st = PrimaryState { is_primary: false, primary_url: None };
        assert_eq!(decide(&http::Method::DELETE, false, &st, false), Decision::Unavailable);
    }
    #[test]
    fn already_forwarded_to_non_primary_is_retriable() {
        // a forwarded write that lands on a node that is NOT primary → tell the
        // caller to re-resolve (loop-guard + stale-primary).
        assert_eq!(decide(&http::Method::POST, true, &secondary("http://p:8081"), false), Decision::Retriable);
    }
    #[test]
    fn already_forwarded_to_primary_passes_through() {
        assert_eq!(decide(&http::Method::POST, true, &primary("http://me:8081"), false), Decision::PassThrough);
    }
}
```

- [ ] **Step 3: Run — expect FAIL:** `cargo test -p crabka-schema-registry --lib rest::forward`.

- [ ] **Step 4: Implement `rest/forward.rs`.**
```rust
//! Write-forwarding middleware: a secondary proxies mutating REST to the
//! elected primary; reads + primary-side writes pass through. A forwarded
//! request carries `X-Forwarded-For-Registry` so the primary never re-forwards.

use axum::body::Body;
use axum::extract::{Request, State};
use axum::http::{HeaderValue, Method, StatusCode, header};
use axum::middleware::Next;
use axum::response::{IntoResponse, Response};
use tokio::sync::watch;

use crate::election::PrimaryState;

pub const FORWARD_HEADER: &str = "x-forwarded-for-registry";

#[derive(Clone)]
pub struct ForwardState {
    pub primary: watch::Receiver<PrimaryState>,
    pub http: reqwest::Client,
    pub node_id: String,
}

#[derive(Debug, PartialEq, Eq)]
pub(crate) enum Decision {
    PassThrough,
    Forward(String),
    Unavailable,
    Retriable,
}

/// Decide what to do with a request, given the method, whether it already
/// carries the forward header, the current primary state, and a spare flag.
pub(crate) fn decide(
    method: &Method,
    already_forwarded: bool,
    state: &PrimaryState,
    _reserved: bool,
) -> Decision {
    let mutating = matches!(method, &Method::POST | &Method::PUT | &Method::DELETE | &Method::PATCH);
    if !mutating {
        return Decision::PassThrough;
    }
    if state.is_primary {
        return Decision::PassThrough;
    }
    if already_forwarded {
        // forwarded to a non-primary (stale primary / race) → ask caller to retry
        return Decision::Retriable;
    }
    match &state.primary_url {
        Some(url) => Decision::Forward(url.clone()),
        None => Decision::Unavailable,
    }
}

/// axum `from_fn_with_state` middleware.
pub async fn forward_layer(
    State(fwd): State<ForwardState>,
    req: Request,
    next: Next,
) -> Response {
    let already = req.headers().contains_key(FORWARD_HEADER);
    let method = req.method().clone();
    let state = fwd.primary.borrow().clone();
    match decide(&method, already, &state, false) {
        Decision::PassThrough => next.run(req).await,
        Decision::Unavailable => {
            (StatusCode::SERVICE_UNAVAILABLE, "no primary elected").into_response()
        }
        Decision::Retriable => {
            (StatusCode::SERVICE_UNAVAILABLE, "not primary; retry").into_response()
        }
        Decision::Forward(primary_url) => proxy(&fwd, &primary_url, req).await,
    }
}

async fn proxy(fwd: &ForwardState, primary_url: &str, req: Request) -> Response {
    let (parts, body) = req.into_parts();
    let path_q = parts.uri.path_and_query().map_or("", |p| p.as_str());
    let url = format!("{primary_url}{path_q}");
    let bytes = match axum::body::to_bytes(body, 16 * 1024 * 1024).await {
        Ok(b) => b,
        Err(_) => return (StatusCode::BAD_REQUEST, "body read failed").into_response(),
    };
    let method = reqwest::Method::from_bytes(parts.method.as_str().as_bytes())
        .unwrap_or(reqwest::Method::POST);
    let mut rb = fwd.http.request(method, &url).body(bytes.to_vec());
    if let Some(ct) = parts.headers.get(header::CONTENT_TYPE) {
        rb = rb.header(header::CONTENT_TYPE, ct);
    }
    rb = rb.header(FORWARD_HEADER, &fwd.node_id);
    match rb.send().await {
        Ok(resp) => {
            let status = StatusCode::from_u16(resp.status().as_u16())
                .unwrap_or(StatusCode::BAD_GATEWAY);
            let ct = resp
                .headers()
                .get(reqwest::header::CONTENT_TYPE)
                .and_then(|v| HeaderValue::from_bytes(v.as_bytes()).ok());
            let body = resp.bytes().await.unwrap_or_default();
            let mut out = Response::new(Body::from(body));
            *out.status_mut() = status;
            if let Some(ct) = ct {
                out.headers_mut().insert(header::CONTENT_TYPE, ct);
            }
            out
        }
        Err(e) => (StatusCode::BAD_GATEWAY, format!("forward failed: {e}")).into_response(),
    }
}
```

- [ ] **Step 5: Run — expect PASS:** `cargo test -p crabka-schema-registry --lib rest::forward`.

- [ ] **Step 6: Export + wire the layer (`rest/mod.rs`).** Add `pub mod forward;`. Add a helper that wraps an existing router with the forwarding layer:
```rust
/// Wrap the router with the write-forwarding middleware (secondary → primary).
pub fn router_with_forwarding(state: AppState, fwd: forward::ForwardState) -> Router {
    router(state).layer(axum::middleware::from_fn_with_state(fwd, forward::forward_layer))
}
```

- [ ] **Step 7: Wire the binary (`bin/schema-registry.rs`).** Add CLI args + start election + wrap the router:
```rust
    #[arg(long, env = "SCHEMA_REGISTRY_ADVERTISED_URL")]
    advertised_url: Option<String>,
    #[arg(long, env = "SCHEMA_REGISTRY_GROUP_ID", default_value = "schema-registry")]
    group_id: String,
    #[arg(long, env = "SCHEMA_REGISTRY_LEADER_ELIGIBILITY", default_value_t = true)]
    leader_eligibility: bool,
```
In `main`, after building `cfg` (set `advertised_url` to the arg or `format!("http://{}", args.listen_addr)`, plus `group_id`/`leader_eligibility`), and after `KafkaStore::start`:
```rust
    let primary = crabka_schema_registry::election::Election::start(&cfg, shutdown.clone()).await?;
    let fwd = rest::forward::ForwardState {
        primary,
        http: reqwest::Client::new(),
        node_id: cfg.advertised_url.clone(),
    };
    let app = rest::router_with_forwarding(AppState { store }, fwd);
```
(Replace the existing `let app = rest::router(AppState { store });`.)

- [ ] **Step 8: Run + commit.** `cargo build -p crabka-schema-registry` (the binary compiles with the election + forwarding wired) + `cargo test -p crabka-schema-registry --lib --test ha single_node` → green. clippy + fmt. Commit (`Cargo.toml`, `src/rest/forward.rs`, `src/rest/mod.rs`, `src/bin/schema-registry.rs`):
`schema-registry: write-forwarding middleware + binary wiring (election + forward layer)`

---

## Task 4: in-process multi-node HA + failover (`tests/ha.rs`)

**Files:** Modify `tests/ha.rs`.

- [ ] **Step 1: Add a multi-node helper + the conformance test.** Spawn N registry nodes (store + router-with-forwarding + election) on distinct `127.0.0.1` ports against one broker; serve each with `axum`. Reuse `cfg`/`await_state` from Task 2.
```rust
use std::sync::Arc;
use axum::Router;
use crabka_schema_registry::kafkastore::KafkaStore;
use crabka_schema_registry::rest::{self, AppState, forward::ForwardState};

struct Node {
    port: i32,
    store: Arc<KafkaStore>,
    primary: tokio::sync::watch::Receiver<crabka_schema_registry::election::PrimaryState>,
    cancel: CancellationToken,
}

async fn start_node(bootstrap: &str, port: i32) -> Node {
    let c = cfg(bootstrap, port);
    let cancel = CancellationToken::new();
    let store = KafkaStore::start(&c, cancel.clone()).await.unwrap();
    let primary = crabka_schema_registry::election::Election::start(&c, cancel.clone()).await.unwrap();
    let fwd = ForwardState { primary: primary.clone(), http: reqwest::Client::new(), node_id: c.advertised_url.clone() };
    let app: Router = rest::router_with_forwarding(AppState { store: store.clone() }, fwd);
    let listener = tokio::net::TcpListener::bind(("127.0.0.1", u16::try_from(port).unwrap())).await.unwrap();
    let serve_cancel = cancel.clone();
    tokio::spawn(async move {
        axum::serve(listener, app)
            .with_graceful_shutdown(async move { serve_cancel.cancelled().await })
            .await
            .ok();
    });
    Node { port, store, primary, cancel }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn multi_node_elects_one_primary_forwards_writes_and_fails_over() {
    let dir = tempfile::tempdir().unwrap();
    let broker = Broker::start(BrokerConfig::for_tests(dir.path().to_path_buf())).await.unwrap();
    let bootstrap = broker.listen_addr().to_string();
    let mut a = start_node(&bootstrap, 18081).await;
    let mut b = start_node(&bootstrap, 18082).await;
    // exactly one primary
    await_state(&mut a.primary, 25, |s| s.primary_url.is_some()).await;
    await_state(&mut b.primary, 25, |s| s.primary_url.is_some()).await;
    assert_ne!(a.primary.borrow().is_primary, b.primary.borrow().is_primary, "exactly one primary");
    let (primary, secondary) = if a.primary.borrow().is_primary { (&a, &b) } else { (&b, &a) };
    let http = reqwest::Client::new();
    // POST to the SECONDARY → forwarded to the primary → write lands
    let body = r#"{"schema":"{\"type\":\"record\",\"name\":\"U\",\"fields\":[]}"}"#;
    let r = http.post(format!("http://127.0.0.1:{}/subjects/s/versions", secondary.port))
        .header("content-type", "application/vnd.schemaregistry.v1+json")
        .body(body).send().await.unwrap();
    assert_eq!(r.status(), 200, "secondary forwarded the write to the primary");
    // read reflects on BOTH nodes
    for n in [&a, &b] {
        let g = http.get(format!("http://127.0.0.1:{}/subjects/s/versions", n.port)).send().await.unwrap();
        assert_eq!(g.status(), 200);
        assert_eq!(g.text().await.unwrap(), "[1]");
    }
    // FAILOVER: stop the primary; the secondary must become primary and accept writes
    let old_primary_port = primary.port;
    if primary.port == a.port { a.cancel.cancel(); } else { b.cancel.cancel(); }
    let survivor = if old_primary_port == a.port { &mut b } else { &mut a };
    await_state(&mut survivor.primary, 30, |s| s.is_primary).await;
    let r2 = http.post(format!("http://127.0.0.1:{}/subjects/s2/versions", survivor.port))
        .header("content-type", "application/vnd.schemaregistry.v1+json")
        .body(body).send().await.unwrap();
    assert_eq!(r2.status(), 200, "new primary accepts writes after failover");
    survivor.cancel.cancel();
    broker.shutdown().await;
}
```
(If the fixed ports `18081/18082` collide in CI, bind `("127.0.0.1", 0)` and read `local_addr()` for the port — but the advertised URL must then use the real bound port; thread it through `cfg`. Prefer `:0` + real-port if flakiness appears.)

- [ ] **Step 2: Run** `cargo test -p crabka-schema-registry --test ha -- --nocapture` → both `ha` tests pass: one primary, secondary-forwards-write, reads-everywhere, failover→new-primary-accepts-writes. If timing is tight, raise the `await_state` budgets (election needs a JoinGroup→SyncGroup round + a heartbeat); do NOT reduce `SESSION_TIMEOUT_MS` below the broker's minimum.

- [ ] **Step 3: clippy + fmt + commit** (`tests/ha.rs`):
`schema-registry: in-process multi-node HA conformance (election + forwarding + failover)`

---

## Task 5: cp Docker election capture + calibration

**Files:** Create `tests/capture_election_fixtures.rs` + `tests/fixtures/election/*.json`; calibrate `src/election/protocol.rs` (identity field order / protocol name / `select_master`) + the Task-1 byte test if cp differs.

- [ ] **Step 1: Write the `#[ignore]` Docker capture harness** `tests/capture_election_fixtures.rs`, modeled on `tests/capture_admin_fixtures.rs` (copy the `start_host_broker` + docker helpers). Start **two** `cp-schema-registry:7.4.0` containers, both with `SCHEMA_REGISTRY_KAFKASTORE_BOOTSTRAP_SERVERS=PLAINTEXT://host.docker.internal:9092` and the SAME `SCHEMA_REGISTRY_GROUP_ID` + distinct `SCHEMA_REGISTRY_HOST_NAME`/listeners, plus `SCHEMA_REGISTRY_MASTER_ELIGIBILITY=true`. Wait for both `/subjects` to be ready (they only become ready once the group elects a primary — so readiness proves election worked through the Crabka broker). Then read the group via the admin client:
```rust
// DescribeGroups the "sr" group from the Crabka broker (host-side, 127.0.0.1:9092)
let mut admin = crabka_client_admin::AdminClient::connect(&["127.0.0.1:9092".to_string()]).await.unwrap();
// (use AdminClient's describe-groups, or send DescribeGroupsRequest{ groups:["schema-registry"], .. } via a client-core Client)
```
Capture per member: `member_id`, `member_metadata` (the cp `SchemaRegistryIdentity` bytes), `member_assignment` (the cp `SchemaRegistryGroupAssignment` bytes), plus the group's `protocol_type` + each member's protocol name → `tests/fixtures/election/{members,group}.json` (UTF-8-lossy of the byte fields). If `AdminClient` lacks a describe-groups call, send a `DescribeGroupsRequest` (`crabka_protocol::owned::describe_groups_request`) over a `crabka_client_core::Client` to `127.0.0.1:9092` and decode the response (the broker implements api_key 15).

- [ ] **Step 2: Run the capture (Docker):** `cargo test -p crabka-schema-registry --test capture_election_fixtures -- --ignored --nocapture`. **If Docker is unavailable, STOP and report — the controller runs the capture.** Inspect + report: the exact `member_metadata` JSON (cp's `SchemaRegistryIdentity` field order + names — confirm `master_eligibility` vs a renamed field, and whether `scheme` is present/positioned), the `member_assignment` JSON (cp's `SchemaRegistryGroupAssignment` — the `error`/`master` shape), the `protocol_type` (expect `"sr"`), the protocol name (the `SR_PROTOCOL_NAME` seed), and WHICH member cp chose as master (to derive the `select_master` comparator).

- [ ] **Step 3: CALIBRATE `election/protocol.rs` to cp.** For each divergence from the seed:
  - **`SchemaRegistryIdentity`** — fix the struct field set/order/names + any `#[serde(rename = "...")]` so `serde_json::to_vec` reproduces cp's `member_metadata` bytes exactly. Update the Task-1 `identity_json_round_trips_and_is_field_ordered` expected string to the captured bytes.
  - **`SchemaRegistryGroupAssignment`** — match cp's `member_assignment` shape (`error`/`master`, any extra fields).
  - **`SR_PROTOCOL_NAME`** — set to the captured protocol name.
  - **`select_master`** — adjust the comparator so our leader picks the SAME master cp picked given the same member set (derive the rule from the capture; e.g. cp may compare by `(host, port)`, by URL string, or by member-id order). Add a `select_master_matches_cp` unit test asserting the captured master is chosen from the captured identities.
  Report every change (seed → cp).

- [ ] **Step 4: Add a record-fixture confirmation test** in `election/protocol.rs` `mod tests` (mirrors the slice-3/4 cp-byte pins): assert `serde_json::to_vec(&<captured identity>)` equals the exact cp `member_metadata` bytes and `serde_json::to_vec(&<captured assignment>)` equals cp's `member_assignment` bytes.

- [ ] **Step 5: Run everything** (no Docker): `cargo test -p crabka-schema-registry --lib --test ha --test integration --test compat_conformance --test interop` → all green; the in-process `ha.rs` still elects/forwards/fails-over with the calibrated identity/assignment; conformance unchanged. clippy + fmt.

- [ ] **Step 6: Commit** (`tests/capture_election_fixtures.rs`, `tests/fixtures/election/`, `src/election/protocol.rs`, any test updates): `schema-registry: cp-calibrated "sr" election wire (identity/assignment/protocol-name/master-rule) + capture`.

---

## Self-review (completed by plan author)

**Spec coverage:**
- Config (`advertised_url`/`group_id`/`leader_eligibility`) → Task 1.
- cp `"sr"` types (`SchemaRegistryIdentity`/`SchemaRegistryGroupAssignment` serde + `select_master`) → Task 1 (seed) + Task 5 (cp-calibrate).
- Group-membership client (`FindCoordinator`→`JoinGroup`(+MEMBER_ID_REQUIRED)→leader-select→`SyncGroup`→`Heartbeat`→rejoin→`LeaveGroup`) → Task 2.
- `Election::start` + `PrimaryState` watch → Task 2.
- Forwarding middleware (GET passthrough; primary passthrough; secondary forward; no-primary→503; forwarded-to-non-primary→retriable loop-guard) → Task 3.
- Router + binary wiring + `reqwest`→dep → Task 3.
- `KafkaStore` unchanged (only the primary writes; the middleware is the gate) → no facade task (intentional).
- Validation: cp Docker election capture via `DescribeGroups` → Task 5; in-process multi-node election + forwarding + failover → Task 4; serde/byte-shape/selection unit tests → Tasks 1/5; forwarding decision unit tests → Task 3.
- Out of scope honored: no election auth/mTLS, no `_schemas` generation-fencing, the split-brain window documented not engineered.

**Placeholder scan:** the only seed-then-calibrate items — the identity field order, `SR_PROTOCOL_NAME`, and the `select_master` comparator — are explicitly cp-pinned in Task 5 (the spec's authority discipline), not unfilled placeholders. The Kafka error-code consts are defined locally (values from the well-known Kafka protocol). Every code step shows complete code.

**Type consistency:** `SchemaRegistryIdentity { version, host, port, master_eligibility, scheme }` + `.url()`, `SchemaRegistryGroupAssignment { error, master }`, `select_master(&[(String, SchemaRegistryIdentity)]) -> Option<SchemaRegistryIdentity>`, `PrimaryState { is_primary, primary_url }`, `Election::start(&RegistryConfig, CancellationToken) -> watch::Receiver<PrimaryState>`, `ElectionClient { bootstrap, client_id, group_id, identity, tx }`, `ForwardState { primary, http, node_id }`, `decide(&Method, bool, &PrimaryState, bool) -> Decision`, `router_with_forwarding(AppState, ForwardState) -> Router`, and the `crabka_protocol::owned` codec field names (`member_id`/`generation_id`/`metadata`/`assignment`/`leader`/`error_code`/`coordinators`) are used consistently across tasks. The group RPCs use `..Default::default()` for version-specific/`unknown_tagged_fields` (the generated owned structs derive `Default`).

**Gaps fixed during review:** the election error codes are defined locally (not via a `crabka-broker` dep) to keep the registry's dep set clean; `connect_coordinator` handles both the v0–3 single-coordinator and v4+ `coordinators[]` response shapes; the forwarding `decide` is split out as a pure function so the primary/secondary/loop-guard branches are unit-tested without a live peer; the multi-node test notes the fixed-port→`:0` fallback for CI.
