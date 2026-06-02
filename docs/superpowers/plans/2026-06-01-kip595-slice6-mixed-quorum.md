# KIP-595 Slice 6 — mixed JVM+Crabka quorum Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: superpowers:subagent-driven-development (Tasks 1-2) then inline for Task 3. Steps use checkbox (`- [ ]`).

**Goal:** A JVM `apache/kafka:4.0.0` controller joined to a static 3-voter quorum led by a Crabka controller advances its HWM and applies the Crabka leader's committed metadata — proven by an un-ignored Docker acceptance test.

**Architecture:** Two concrete wire fixes on the shared controller listener (promote the spike's ApiVersions tweak to codec-generated+tested; key the v17 Fetch metadata topic by its fixed `topic_id`), then iterate the Docker mixed-quorum acceptance run to election+replication, fixing whatever bootstrap-reconciliation / feature-record gap surfaces.

**Tech Stack:** Rust, `crabka_protocol` generated KIP-595 codecs, tokio engine, Docker `apache/kafka:4.0.0`.

**Spec:** `docs/superpowers/specs/2026-06-01-kip595-slice6-mixed-quorum-design.md`. **Spike findings:** `docs/superpowers/specs/2026-05-31-kip595-static-mixed-quorum-findings.md`.

## Batches
- **Batch A (parallel, disjoint files):** Task 1 (`crates/raft/src/server.rs`) ‖ Task 2 (`crates/raft/src/kraft/transport.rs`).
- **Batch B (inline, iterative):** Task 3 (acceptance run + capture/fix surfaced gaps).
- **Deferred (not this slice):** cluster_id echo in Vote/Fetch — needs threading engine state through the stateless `wire` encoders (signature + call-site churn) for correctness the JVM already tolerates as `None`. Noted; ship in a follow-up if a later gap requires it.

Commit per task with identity overrides + `Co-Authored-By: Claude Opus 4.8 <noreply@anthropic.com>`; `cargo fmt --all` pre-commit; clippy `--all-targets` clean.

---

## Task 1: Promote ApiVersions response to the generated codec + test

**Files:** Modify `crates/raft/src/server.rs` (`api_versions_response_body`); Test: same file.

Context: `api_versions_response_body(req_version: i16) -> Bytes` currently hand-rolls the body with `put_uvarint`/`put_i16`. It must keep the SAME observable behavior: req `v<=2` → non-flexible (i32 array) v0-shaped body; req `v>=3` → flexible (compact array) body; the *response header* stays v0 regardless (handled by the existing `write_response_no_tagged_fields` framing — do NOT change framing). Advertised set unchanged: Fetch(1) 0..=17, ApiVersions(18) 0..=4, Vote(52) 0..=2, BeginQuorumEpoch(53) 0..=1, EndQuorumEpoch(54) 0..=1, FetchSnapshot(59) 0..=1.

- [ ] **Step 1: Failing test** — assert the body decodes via the generated codec and carries the advertised set, for both a non-flexible (v0) and flexible (v4) request.

```rust
#[test]
fn api_versions_body_advertises_kip595_set_both_shapes() {
    use crabka_protocol::owned::api_versions_response::ApiVersionsResponse;
    use crabka_protocol::Decode;
    for req_v in [0i16, 4i16] {
        let body = super::api_versions_response_body(req_v);
        // Decode at the body version we emit (clamped to 0..=4).
        let v = req_v.clamp(0, 4);
        let mut cur = &body[..];
        let resp = ApiVersionsResponse::decode(&mut cur, v).expect("decode body");
        assert!(cur.is_empty(), "no trailing bytes (req_v={req_v})");
        assert!(resp.error_code == 0);
        let keys: std::collections::BTreeSet<i16> =
            resp.api_keys.iter().map(|k| k.api_key).collect();
        for want in [1i16, 18, 52, 53, 54, 59] {
            assert!(keys.contains(&want), "missing api_key {want} at req_v={req_v}");
        }
        let vote = resp.api_keys.iter().find(|k| k.api_key == 52).unwrap();
        assert!(vote.min_version == 0 && vote.max_version == 2);
    }
}
```

Run: `cargo test -p crabka-raft --lib server::tests::api_versions_body_advertises_kip595_set_both_shapes` → expect FAIL (hand-rolled bytes may not decode cleanly via the codec at v4, or the test simply didn't exist).

- [ ] **Step 2: Reimplement via the codec**

Replace the body of `api_versions_response_body` with construction of the generated `ApiVersionsResponse` + `ApiVersionsResponseKey` and `.encode(&mut buf, body_version)`:

```rust
fn api_versions_response_body(req_version: i16) -> Bytes {
    use crabka_protocol::Encode;
    use crabka_protocol::owned::api_versions_response::{ApiVersionsResponse, ApiVersionsResponseKey};
    const KEYS: &[(i16, i16, i16)] = &[
        (1, 0, 17),  // Fetch
        (18, 0, 4),  // ApiVersions
        (52, 0, 2),  // Vote
        (53, 0, 1),  // BeginQuorumEpoch
        (54, 0, 1),  // EndQuorumEpoch
        (59, 0, 1),  // FetchSnapshot
    ];
    let resp = ApiVersionsResponse {
        error_code: 0,
        api_keys: KEYS.iter().map(|&(api_key, min_version, max_version)| ApiVersionsResponseKey {
            api_key, min_version, max_version, ..Default::default()
        }).collect(),
        throttle_time_ms: 0,
        ..Default::default()
    };
    // The JVM dials at v4 (flexible); Crabka's own client at v0 (non-flexible).
    // The generated codec encodes the correct body shape per version. The
    // ApiVersions RESPONSE HEADER stays v0 regardless (Kafka's bootstrap-handshake
    // quirk) — that asymmetry lives in the framing (write_response_no_tagged_fields),
    // not here.
    let body_version = req_version.clamp(0, 4);
    let mut buf = BytesMut::new();
    let _ = resp.encode(&mut buf, body_version);
    buf.freeze()
}
```

Confirm the exact generated names (`ApiVersionsResponseKey` vs `ApiVersionKey`; field names `api_key`/`min_version`/`max_version`) from `crates/protocol/generated/ApiVersionsResponse.owned.rs` and adjust. Remove now-unused `put_uvarint` ONLY if nothing else uses it (grep first — it may be used elsewhere in server.rs; if so, leave it).

- [ ] **Step 3: Run the test** → PASS. Then the Crabka↔Crabka regression: `cargo test -p crabka-raft` and `cargo test -p crabka-broker --test quorum --test role_separation_observer` (the controller-listener ApiVersions path) → green. The v0 body shape Crabka's own client consumes must be unchanged — if `quorum`/observer break, the v0 codec body differs from the prior hand-rolled v0 bytes; reconcile (the prior v0 was "i32 array len + entries, no throttle"; the codec v0 should match — verify byte-for-byte if needed).

- [ ] **Step 4: clippy + fmt + commit**

```bash
cargo clippy -p crabka-raft --all-targets && cargo fmt --all
git add crates/raft/src/server.rs
git -c user.name="Matt Stone" -c user.email="matthew.d.stone@gmail.com" commit -m "feat(raft): codec-generated controller-listener ApiVersions response

Co-Authored-By: Claude Opus 4.8 <noreply@anthropic.com>"
```

---

## Task 2: Key the metadata topic by `topic_id` on the v17 Fetch wire

**Files:** Modify `crates/raft/src/kraft/transport.rs` (`mod wire`); Test: same file.

Context: `wire::PeerRequest::Fetch` encodes `FetchRequest` with `FetchTopic { topic: METADATA_TOPIC.to_string(), .. }` and `wire::PeerResponse::Fetch` encodes `FetchResponse` with `FetchableTopicResponse { topic: METADATA_TOPIC.to_string(), .. }`. Both generated structs ALSO have `topic_id: crate::primitives::uuid::Uuid` (confirmed). The KRaft metadata topic id is `00000000-0000-0000-0000-000000000001`. KRaft Fetch v17 matches the topic by `topic_id`; with Crabka leaving it nil, a JVM follower can't match the leader's response → HWM stuck (spike Gap 2).

- [ ] **Step 1: Add the constant** in `mod wire` near `METADATA_TOPIC`/`METADATA_PARTITION`:

```rust
/// The fixed KRaft `__cluster_metadata` topic id (KIP-595). Fetch v13+ keys the
/// topic by this id, not by name.
const METADATA_TOPIC_ID: crate::primitives_uuid::Uuid = crate::primitives_uuid::Uuid([
    0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1,
]);
```
NOTE: use the correct path to `crabka_protocol::primitives::uuid::Uuid`. The `wire` module already imports generated types from `crabka_protocol::owned::...`; add `use crabka_protocol::primitives::uuid::Uuid as MetaUuid;` and write `const METADATA_TOPIC_ID: MetaUuid = MetaUuid([0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,1]);`. Confirm `Uuid` is a tuple struct `Uuid(pub [u8;16])` (it is) so the const literal compiles.

- [ ] **Step 2: Set `topic_id` on the request** — in `PeerRequest::Fetch` encode, the `FetchTopic { topic: METADATA_TOPIC.to_string(), partitions: ..., ..Default::default() }` gains `topic_id: METADATA_TOPIC_ID,`.

- [ ] **Step 3: Set `topic_id` on the response** — in `PeerResponse::Fetch` encode, the `FetchableTopicResponse { topic: METADATA_TOPIC.to_string(), partitions: ..., ..Default::default() }` gains `topic_id: METADATA_TOPIC_ID,`.

- [ ] **Step 4: Failing-then-passing wire test** — assert the encoded Fetch request+response carry the metadata topic id, and the existing Crabka round-trip still holds (decode is positional, so `decode_fetch` is unaffected):

```rust
#[test]
fn fetch_wire_carries_metadata_topic_id() {
    use crabka_protocol::owned::fetch_request::FetchRequest;
    use crabka_protocol::owned::fetch_response::FetchResponse;
    use crabka_protocol::Decode;
    let req = PeerRequest::Fetch { from: 2, fetch_epoch: 1, fetch_offset: 5 };
    let mut c = &req.encode()[..];
    let dreq = FetchRequest::decode(&mut c, super::wire::tests_fetch_version()).unwrap();
    assert!(dreq.topics[0].topic_id == METADATA_TOPIC_ID);

    let resp = PeerResponse::Fetch {
        leader_id: 1, leader_epoch: 4, diverging: None, snapshot_id: None,
        hwm: 0, records: bytes::Bytes::new(),
    };
    let mut c2 = &resp.encode()[..];
    let dresp = FetchResponse::decode(&mut c2, super::wire::tests_fetch_version()).unwrap();
    assert!(dresp.responses[0].topic_id == METADATA_TOPIC_ID);
}
```
If a `tests_fetch_version()` helper / `FETCH_VERSION` accessor isn't reachable from the test module, hardcode `17` (the captured `FETCH_VERSION`). Match the existing fetch-wire tests' decode style in this module.

- [ ] **Step 5: Run + regression** — `cargo test -p crabka-raft --lib kraft::transport` (new + existing pass); then `cargo test -p crabka-raft` and `cargo test -p crabka-broker --test quorum --test leader_election` (Crabka↔Crabka Fetch must stay green — both sides now send the real topic_id; positional decode is unchanged).

- [ ] **Step 6: clippy + fmt + commit**

```bash
cargo clippy -p crabka-raft --all-targets && cargo fmt --all
git add crates/raft/src/kraft/transport.rs
git -c user.name="Matt Stone" -c user.email="matthew.d.stone@gmail.com" commit -m "feat(raft): key __cluster_metadata Fetch by topic_id (KIP-595 v17)

Co-Authored-By: Claude Opus 4.8 <noreply@anthropic.com>"
```

---

## Task 3: Mixed-quorum acceptance run — election + replication (inline, iterative)

**Files:** Modify `crates/broker/tests/jvm_static_quorum_spike.rs` (rename to `jvm_mixed_quorum.rs`); driven inline with Docker.

This is iterative empirical work (like the spike). The controller (me) drives it; not a single TDD subagent.

- [ ] **Step 1: Extend the test to assert replication.** Keep it `#[ignore]` (Docker-gated). After the existing cross-impl-election assertion, add: wait (bounded, ~60s) for the JVM follower's HWM to advance and its image to reflect the Crabka leader's committed records. Observe the JVM side via `docker logs` (the `MetadataLoader` "high water mark" line advancing, feature/records applied) and/or `kafka-metadata-quorum --bootstrap-controller <jvm> describe --status`. Assert from the Crabka leader's `per_voter_matched_index` that the JVM voter's fetch offset advances past the bootstrap.

- [ ] **Step 2: Run it.** `cargo test -p crabka-broker --test jvm_mixed_quorum -- --ignored --nocapture`. With Tasks 1-2 landed, the JVM should now Fetch real records.

- [ ] **Step 3: Iterate on surfaced gaps (capture + fix each).** Likely, in order:
  - **Bootstrap offset-0 reconciliation.** The JVM's epoch-0 bootstrap differs from the Crabka leader's. Confirm the JVM detects divergence and truncates to the leader's log (watch for `Truncat`/`diverging` in JVM logs). If Crabka's leader doesn't serve a correct `diverging_epoch` for the JVM's offset/epoch, fix the leader Fetch divergence path in `controller.rs`/core.
  - **metadata.version feature record.** The JVM logs "metadata.version is not known yet" until it applies a `FeatureLevelRecord("metadata.version", 25)`. Confirm Crabka's committed leader log contains it (grep the broker bootstrap submit; if absent, ensure the bootstrap submits it). The JVM cannot function as a follower without it.
  - **Record fidelity / version.** If the JVM rejects a specific replicated record version, capture it; raise the apiVersion for that record type at the engine encode (3d-2 left record apiVersion at 0 — a JVM may want a higher PartitionRecord/RegisterBroker version; raise as needed for the records actually in the bootstrap).
  Each fix is a focused commit with a clear message; re-run after each.

- [ ] **Step 4: Done bar met → finalize the test.** Once election + replication hold deterministically (run 3×), keep the test `#[ignore]` (Docker/JVM-gated, not in the default lane) with a clear doc comment, and ensure it compiles + clippy-clean in the normal build.

- [ ] **Step 5: Regression + commit.** `cargo test -p crabka-raft`; `cargo test -p crabka-broker --test quorum --test leader_election --test controlled_shutdown --test role_separation_observer`; the 3d-2 JVM dump-log byte check (`-- --ignored`); clippy `--all-targets`; fmt. Commit the acceptance test (+ any engine fixes from Step 3 already committed individually).

```bash
git -c user.name="Matt Stone" -c user.email="matthew.d.stone@gmail.com" commit -m "test(broker): mixed JVM+Crabka quorum acceptance — election + replication

Co-Authored-By: Claude Opus 4.8 <noreply@anthropic.com>"
```

---

## Final verification
- [ ] Mixed-quorum acceptance test passes (`--ignored`, 3× deterministic).
- [ ] Full `cargo test --workspace` green (modulo known load-flaky TCP suites — re-run in isolation to confirm flake).
- [ ] `cargo clippy --workspace --all-targets` + `cargo fmt --all -- --check` clean.
- [ ] Push to PR #352; update title to "Slices 0-6"; update memory with the end-goal-achieved result.

## Parking lot (documented follow-ups — NOT this slice)
- Reverse direction: Crabka following a JVM leader; admin-tool (`kafka-topics`) changes replicating into Crabka.
- cluster_id echo in Vote/Fetch.
- KIP-853 dynamic voters (confirmed unnecessary for interop) + un-ignoring `dynamic_voters`/`isr_expand`.
- Record apiVersion fidelity beyond what the bootstrap exercises.
