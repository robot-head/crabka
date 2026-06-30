# KIP-595 Slice 6 — mixed JVM+Crabka quorum (election + leader→follower replication)

Date: 2026-06-01
Status: Approved (brainstorming) — pending spec review

## Context

The Slice 5 spike (`docs/superpowers/specs/2026-05-31-kip595-static-mixed-quorum-findings.md`)
proved a **static (kraft.version=0) mixed quorum elects cross-impl** — a JVM
`mirror.gcr.io/apache/kafka:4.0.0` controller attaches as a **Follower of a Crabka leader**
over real `Vote(52 v2)`/`Fetch(1 v17)` — and that **full KIP-853 dynamic
reconfiguration is NOT required**. Replication leader→JVM-follower was blocked by
one remaining wire gap. Slice 6 closes that gap and turns the spike into a real,
un-ignored **acceptance test**: a JVM controller genuinely replicating committed
metadata from a Crabka leader in one quorum — the program's headline goal.

This supersedes the original "Slice 5 = dynamic voters" plan: we stay on static
voters; the `dynamic_voters.rs` / `isr_expand` tests remain `#[ignore]`d
(dynamic reconfig is out of scope and not needed).

## Goal & done bar

**A JVM `mirror.gcr.io/apache/kafka:4.0.0` controller, joined to a static 3-voter quorum led by
a Crabka controller (Crabka holds 2/3), advances its high-watermark and applies
the Crabka leader's committed metadata.** Verified by an un-ignored
Docker-gated acceptance test asserting: (a) a single cross-impl leader (Crabka);
(b) the JVM follower's HWM advances past the bootstrap; (c) the JVM's metadata
image reflects records committed by the Crabka leader.

**Out of scope (follow-ups):** the reverse direction (JVM-led quorum / Crabka
following a JVM leader), admin-tool-driven changes (`kafka-topics` etc.), and
KIP-853 dynamic reconfiguration.

## The three enumerated fixes (from the spike findings)

### Fix 1 — ApiVersions on the controller listener (promote the spike tweak)
The JVM dials peers with `ApiVersions v4` (flexible); Crabka was returning an
empty v0 body → JVM declared `UNSUPPORTED_VERSION: does not support VOTE`. The
spike landed a hand-rolled version-aware body in `crates/raft/src/server.rs`
(`api_versions_response_body`). **Promote it:** regenerate the body via the
generated `ApiVersionsResponse` codec (compact/flexible for req v≥3, non-flexible
i32-array for v≤2) instead of hand-rolled varints, advertising the
controller-listener API set (Fetch 0..=17, ApiVersions 0..=4, Vote 0..=2,
BeginQuorumEpoch/EndQuorumEpoch 0..=1, FetchSnapshot 0..=1, plus the private
SUBMIT_CHANGE/METADATA_FETCH only on the non-flexible path used by Crabka's own
client). Add a unit test asserting the v0-vs-v3+ body shapes + the advertised
set. Keep the existing Crabka↔Crabka behavior byte-identical on the v≤2 path.

### Fix 2 — Fetch metadata-topic identity by topic_id (the replication blocker)
KRaft `Fetch v17` identifies `__cluster_metadata` by **`topicId`** (the fixed
`00000000-0000-0000-0000-000000000001`), not by name. Crabka's
`crates/raft/src/kraft/transport.rs` `wire` keys it by name with a nil topic_id,
so the Crabka leader's Fetch **response** never matches the JVM follower's
requested partition → no records → HWM stuck. Fix in `wire`:
- Define `const METADATA_TOPIC_ID` = `00000000-0000-0000-0000-000000000001`.
- `PeerResponse::Fetch` encode: set `FetchableTopicResponse.topic_id =
  METADATA_TOPIC_ID` (the JVM follower matches the response by id).
- `PeerRequest::Fetch` encode: set `FetchTopic.topic_id = METADATA_TOPIC_ID`
  (v13+ drops the topic *name* in favor of id; so a Crabka follower fetching a
  JVM leader matches too).
- Keep decode positional (`topics.first()`) — unaffected.
- **Regression:** Crabka↔Crabka Fetch must stay green (both sides now use the
  real topic_id; decode is positional).

### Fix 3 — cluster_id echo (cheap correctness)
Crabka sends `cluster_id: None` in Vote/Fetch; the JVM tolerated it for
election. Thread the engine's `cluster_id` (a `uuid::Uuid` → Kafka
base64url-no-pad string) into the `wire` encoders and set it on outbound
Vote/Fetch. Low-risk correctness; not strictly required for the done bar, so it
ships only if it doesn't complicate the wire-state threading — otherwise it is
deferred and noted.

## Expected new gaps the acceptance run may surface (iterate + capture)

The spike could not test past Fix 2; these are the next candidates, each handled
empirically when the test runs:
- **Bootstrap offset-0 reconciliation.** Crabka and the JVM each independently
  bootstrap offset 0 (different bytes, epoch 0). When a Crabka node leads at
  epoch ≥ 1, the JVM follower must detect divergence and truncate its epoch-0
  bootstrap to take the leader's records. Verify the existing diverging-epoch
  path drives this cross-impl; fix if it doesn't.
- **metadata.version feature record.** The JVM follower needs a
  `FeatureLevelRecord("metadata.version", 25)` in the replicated log to function
  ("metadata.version is not known yet" until then). Confirm Crabka's committed
  log carries it (the broker bootstrap submits feature records); if not, ensure
  the leader's log includes it.
- **Record fidelity.** The JVM applies Crabka's KIP-631 records; any field the
  JVM rejects (e.g. a record version it can't read) surfaces here. 3d-2 made the
  records byte-clean vs `kafka-dump-log`, so this is expected to pass.

## Acceptance / testing

- **Headline:** promote `crates/broker/tests/jvm_static_quorum_spike.rs` into a
  real un-ignored acceptance test (rename to `jvm_mixed_quorum.rs` or keep the
  name) that boots 2 Crabka controllers + 1 JVM controller in a static 3-voter
  quorum and asserts election + leader→follower replication (the done bar).
  Docker-gated (`#[ignore]` like the other JVM tests, run explicitly /
  in the JVM CI lane), not in the default `cargo test` lane.
- **Regression:** full `crabka-raft` + the broker controller-path suites
  (quorum / leader_election / controlled_shutdown / role_separation_observer)
  stay green — the ApiVersions + Fetch-topic_id changes are on the shared
  controller wire, so Crabka↔Crabka must be byte-compatible. The 3d-2 JVM
  dump-log byte check stays green.
- `cargo clippy --workspace --all-targets` + `cargo fmt` clean.

## Disposition

Permanent. After Slice 6 a JVM controller and Crabka controllers form one static
metadata quorum with the JVM replicating from a Crabka leader — the KIP-595
program's end goal for this direction. Reverse-direction replication, admin-tool
interop, and KIP-853 dynamic voters are documented follow-ups.

## Execution note

Hybrid: the three fixes are concrete (subagent-driven or inline TDD); the
acceptance run is iterative empirical Docker work (inline, like the spike),
capturing + fixing whatever new gap surfaces until the done bar is met or a hard
gap is documented.
