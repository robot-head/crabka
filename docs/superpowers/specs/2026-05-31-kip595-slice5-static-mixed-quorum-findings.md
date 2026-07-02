# KIP-595 Slice 5 — static mixed JVM+Crabka quorum: SPIKE FINDINGS

Date: 2026-06-01
Status: Findings (throwaway spike complete)
Spike test: `crates/broker/tests/jvm_static_quorum_spike.rs` (`#[ignore]`d, Docker-gated)
Spike tweak under evaluation: `crates/raft/src/server.rs::api_versions_response_body`

## TL;DR

- **Election cross-impl: YES** — with one small Crabka-side tweak. A single
  static (`controller.quorum.voters`, kraft.version=0) quorum of two Crabka
  controllers (ids 1, 2) + one `mirror.gcr.io/apache/kafka:4.0.0` controller (id 3) elects a
  **Crabka** leader, and the **JVM transitions to `FollowerState(leader=<crabka
  node>)`** — it accepts the Crabka leader over the real KIP-595 wire.
- **Replication leader→JVM-follower: NO (blocked).** The JVM follower never
  advances its high-watermark (`MetadataLoader … still don't know the high water
  mark yet`), loops Follower→Prospective→Follower on fetch timeout. The Fetch
  response from the Crabka leader is not being accepted as log progress.
- **cluster_id transform: VERIFIED.** `uuid::Uuid` 16 bytes ⇄ Kafka
  base64-url-no-pad string round-trips exactly; no cluster-id mismatch on the
  wire (JVM logged the identical `clusterId='TWtVM09FVkJOVGN3TlRKRQ'`).
- **Directory ids at v0: CONFIRMED a non-issue for election.** The JVM treats
  the *target* voter's directory id as nil (`voteDirectoryId=AAAA…AA`,
  `directoryId=<undefined>` for all peers) and matches Crabka voters by **node
  id alone**, exactly as predicted.

**Recommendation: (B)** — static kraft.version=0 is sufficient for a mixed
quorum to ELECT cross-impl; a **small, enumerated set of static-mode wire fixes**
is needed for full election+replication. No evidence the JVM demands
kraft.version=1 (rules out **(C)** for the election milestone). See
"Recommendation" below.

## Topology used

- Crabka voters id 1, 2: in-process (`Broker::start`), real TCP controller
  listeners bound `0.0.0.0:p1` / `0.0.0.0:p2`. They hold the 2/3 majority and
  self-elect immediately.
- JVM voter id 3: `mirror.gcr.io/apache/kafka:4.0.0`, `process.roles=controller`, container
  publishing `-p p3:p3`, dialing Crabka voters via
  `--add-host=host.docker.internal:host-gateway` at `host.docker.internal:p1/p2`.
- Shared cluster id, shared static 3-voter list, default `metadata.version`
  (4.0-IV3 = level 25 on both sides), kraft.version unset (= 0).

## What works as-is (no Crabka change)

1. **JVM format + boot** of a static controller with a 3-voter
   `controller.quorum.voters` and the shared `--cluster-id`. Default
   `metadata.version 4.0-IV3` — matches Crabka's bootstrap (`metadata.version`
   level 25, confirmed by `kraft_checkpoint_jvm`).
2. **cluster-id sharing.** `Uuid::from_u128(0x4d6b…4a45)` →
   `base64::URL_SAFE_NO_PAD(uuid.as_bytes())` = `TWtVM09FVkJOVGN3TlRKRQ`, which is
   exactly the string the JVM echoes in its `VoteRequestData(clusterId=…)`. The
   two sides share the identical 16 bytes.
3. **JVM is the dialer.** At v0 the JVM's `RaftManager` actively connects out to
   every other voter (id 1, id 2) and drives `ApiVersions` → `Vote`/`Fetch`. So
   Crabka's *inbound* controller-listener path is what must satisfy the JVM, not
   Crabka's outbound dialer.
4. **Directory-id semantics at v0** (captured from a real JVM Vote v2, below):
   the JVM sends its OWN `replicaDirectoryId` (non-nil) but a nil
   `voteDirectoryId` for the target, and tracks all peers as
   `directoryId=<undefined>`. Voters are matched by node id. Crabka's nil/zero
   directory id on the wire is accepted.

## Gap 1 (BLOCKER for any cross-impl traffic) — controller-listener `ApiVersions`

### Evidence

The JVM dials each peer with **`ApiVersions v4` over a flexible (v2) request
header** (captured raw bytes off a peer port):

```
0012 0004 00000002 000d 726166742d636c69656e742d33 00  | <body>
api_key=18  ver=4  corr=2  client_id="raft-client-3"  hdr-tagged=0
body: compact-string "apache-kafka-java", "4.0.0", tagged=0
```

Crabka's `server.rs` answered this with a v0-shaped `ApiVersionsResponse`
(`error_code=0` + an EMPTY `api_keys` array, written with no tagged-fields byte).
The JVM's `NetworkClient` then concluded the peer supports nothing and
synthesized a client-side `UNSUPPORTED_VERSION`, refusing to put `Vote` on the
wire:

```
ERROR Request OutboundRequest(... data=VoteRequestData(clusterId='TWtVM…',
  voterId=1, … replicaDirectoryId=c6IdakxfikmGqeJYdg1URQ,
  voterDirectoryId=AAAAAAAAAAAAAAAAAAAAAA, preVote=false …))
  failed due to unsupported version error
org.apache.kafka.common.errors.UnsupportedVersionException: The node does not support VOTE
ERROR [RaftManager id=3] Unexpected error UNSUPPORTED_VERSION in VOTE response:
  VoteResponseData(errorCode=35, topics=[], nodeEndpoints=[])
```

### Spike tweak (TRIED — unblocks election; evaluate for promotion)

`crates/raft/src/server.rs::api_versions_response_body(req_version)` now returns
a **version-aware** body:

- `req_version <= 2` → the old non-flexible (i32-array) v0 shape. Crabka's own
  client asks at v0 and ignores the table (it uses hardcoded KIP-595 versions in
  `network.rs::api_version_for`), so Crabka↔Crabka is unaffected.
- `req_version >= 3` → a **flexible (v3+) `ApiVersionsResponse`**: compact-array
  of `{api_key, min, max}` advertising the controller APIs Crabka's engine
  speaks — Fetch(1) `0..=17`, ApiVersions(18) `0..=4`, Vote(52) `0..=2`,
  BeginQuorumEpoch(53) `0..=1`, EndQuorumEpoch(54) `0..=1`, FetchSnapshot(59)
  `0..=1` — plus a trailing `throttle_time_ms` and tagged-field bytes. The
  *response header* stays v0 (no leading tagged byte) per the documented
  `ApiVersions` asymmetry.

This was validated with a Python mock first, then in the real broker.

### Result of the tweak

The JVM proceeds to real `Vote(52 v2)` / `Fetch(1 v17)` and **becomes a follower
of the elected Crabka leader**:

```
Completed transition to FollowerState(epoch=15, leader=1,
  leaderEndpoints=Endpoints({CONTROLLER=host.docker.internal/…:52457}),
  voters=[1, 2, 3], highWatermark=Optional.empty …)
```

Crabka side (asserted): both Crabka nodes agree
`controller_leader_id() == Some(<crabka node>)`, `voter_count == 3`. The JVM's
Vote was REJECTED (Crabka already had a leader at a higher epoch), and the JVM
correctly resolved the leader's CONTROLLER endpoint from Crabka's responses and
attached as a follower. **Cross-impl election success criterion met.**

Crabka↔Crabka regression check: `elect_leaders` (3-node static cluster, real
wire) still passes after the tweak.

## Gap 2 (BLOCKER for replication) — JVM follower can't advance HWM from Crabka

### Evidence

Once a follower, the JVM repeats forever:

```
INFO [MetadataLoader id=3] initializeNewPublishers: the loader is still catching
  up because we still don't know the high water mark yet.
…(2 s later)…
INFO [RaftManager id=3] Transitioning to Prospective state due to fetch timeout
… Completed transition to FollowerState(epoch=15, leader=1, highWatermark=Optional.empty …)
```

`highWatermark` never leaves `Optional.empty`: the JVM's `Fetch` to the Crabka
leader is not yielding accepted log progress. It follows, times out fetching,
briefly goes Prospective (rejected), and re-attaches — a livelock at the
replication layer.

### Likely root cause (high-confidence, not yet byte-confirmed end-to-end)

**Fetch v17 identifies the topic by `topicId` (UUID), not by name.** Captured
from the JVM's real `Fetch v17` request, the `FetchTopic` carries a `topicId`
and an empty `topic` name. Crabka's metadata-Fetch wire codec
(`crates/raft/src/kraft/transport.rs`, `PeerRequest::Fetch` encode and
`PeerResponse::Fetch` encode) sets `topic: "__cluster_metadata"` (a NAME) and
leaves `topic_id` defaulted (nil). At v17 both `FetchRequest::FetchTopic` and
`FetchResponse::FetchableTopicResponse` have a `topic_id: Uuid` field
(`crates/protocol/generated/FetchRequest.owned.rs`,
`…/FetchResponse.owned.rs`). A Crabka leader serving a Fetch response keyed by
name (nil topic_id) almost certainly fails to match the JVM follower's
requested partition (keyed by the metadata topic's well-known UUID), so the JVM
sees no records / no HWM for `__cluster_metadata-0`.

This was NOT fixed in the spike (it is a genuine replication-codec change, not a
"tiny tweak", and is squarely Slice 6 / replication territory). It is the next
concrete blocker after election.

### Secondary suspects to check when fixing Gap 2

- **Bootstrap offset-0 reconciliation.** Both sides independently bootstrapped
  offset 0 (the JVM writes its own `BootstrapMetadata`/feature records; Crabka
  writes its own). When the JVM follows the Crabka leader it must
  truncate/replace its own offset-0 contents with the leader's. The Fetch
  diverging-epoch path must drive this correctly cross-impl. Not reached yet
  because Gap 2's topic-id mismatch precedes it.
- **`cluster_id` in Fetch/Vote.** Crabka sends `cluster_id: None`
  (`transport.rs` lines 354/378/399/554). The JVM *does* send a non-null
  cluster_id and validates it. Election still succeeded with Crabka sending
  None, so the JVM does not hard-require it on inbound RPCs at v0 — but Crabka
  echoing the real cluster_id on responses is worth setting once Gap 2 is
  addressed (cheap correctness).

## Captured wire detail — JVM Vote v2 (decoded)

```
api_key=52 ver=2 client_id="raft-client-3"
cluster_id  = "TWtVM09FVkJOVGN3TlRKRQ"          (non-null; matches ours)
voter_id    = 1                                  (the target peer)
topic_name  = "__cluster_metadata"  partition=0
replica_epoch (candidate epoch) = 0
replica_id  (candidate)         = 3              (the JVM itself)
replicaDirectoryId = 43eb371b6ed3680e7d1181d1ca8cefdf   (JVM's own, NON-nil)
voteDirectoryId    = 00000000000000000000000000000000   (target's — NIL at v0)
lastOffsetEpoch=0  lastOffset=0  preVote=1
```

Confirms: at kraft.version=0 the target's directory id is nil and voters are
matched by node id — Crabka's nil/zero directory id interoperates for election.

## Recommendation

**(B) — static suffices for cross-impl ELECTION; a small enumerated set of
static-mode wire fixes is needed for full election+replication.** There is no
evidence the JVM requires kraft.version=1 to *join and follow* a static quorum
(it elects and attaches as follower at v0). Slice 5 should therefore be a
focused set of static-mode wire fixes, not full KIP-853 dynamic reconfiguration:

1. **[validated, promote] Controller-listener `ApiVersions` must advertise the
   real API set, version-aware on the request version.** This is the Gap-1
   tweak already in `server.rs`. Promote it (with a proper unit test asserting
   the v0 vs v3+ body shapes, and ideally generating the body via the
   `ApiVersionsResponse` codec rather than hand-rolled varints).
2. **[required for replication] Fetch must key the metadata topic by `topicId`
   (UUID), not name, at v17.** Set the well-known `__cluster_metadata` topic id
   on both the Fetch request and the leader's Fetch response in
   `kraft/transport.rs`, and match incoming JVM Fetches by topic_id on the
   serve path. This is the Gap-2 blocker for JVM-follower replication.
3. **[correctness, cheap] Echo the real `cluster_id` on Vote/BeginQuorumEpoch/
   Fetch/FetchSnapshot** (Crabka currently sends `None`) and validate inbound
   cluster_id against the local one.
4. **[verify after #2] Cross-impl bootstrap offset-0 reconciliation** — confirm
   the JVM follower truncates its self-bootstrapped offset 0 to the Crabka
   leader's via the Fetch diverging-epoch path; if the two independently-written
   offset-0 batches diverge in a way the JVM rejects, a shared bootstrap-record
   convention may be needed.

Dynamic reconfiguration (kraft.version=1 / KIP-853) is **not** required to reach
the Slice-5 election milestone and remains a Slice-6+ concern only if the *use
case* needs add/remove-voter at runtime.

## Spike artifacts

- Test: `crates/broker/tests/jvm_static_quorum_spike.rs` (`#[ignore]`d; compiles,
  clippy-clean; asserts cross-impl election + flags the replication gap). Run:
  `cargo test -p crabka-broker --test jvm_static_quorum_spike -- --ignored --nocapture`.
- Tweak: `crates/raft/src/server.rs::api_versions_response_body` (marked "SPIKE
  TWEAK … evaluate for promotion"). Kept in-tree because it is small, validated
  non-breaking for Crabka↔Crabka, and is item (1) of the recommendation.
