# KIP-853 dynamic KRaft voters — Design Spec

## Goal

Make the controller quorum reconfigurable at runtime with full
Apache Kafka wire/semantic fidelity. Voters can be added, removed, and
updated while the cluster is live, the voter set is persisted in the
`@metadata` log as control records, and new controllers auto-join.

Supersedes the manual `change_membership` API from
`2026-05-14-crabka-raft-membership-design.md`, which that spec explicitly
called out KIP-853 as the future direction for. The manual API stays as
the low-level primitive the reconfiguration coordinator drives.

## Decisions (settled during brainstorming)

- **Dynamic-only (`kraft.version=1`).** Crabka is greenfield, so we drop
  the static-voter path (`controller.quorum.voters`) entirely. Every
  controller formats and bootstraps at `kraft.version=1`; the voter set
  always lives in the log. No 0→1 upgrade tooling (per CLAUDE.md: no
  migration shims). `kafka-features describe` reports `kraft.version`
  min/max as `0..1` but the cluster always runs at finalized level 1.
- **Log-driven membership; openraft follows.** A `VotersRecord` in the
  `@metadata` log is the authoritative, Kafka-visible voter set.
  openraft's internal `Membership` is kept in lockstep as the
  quorum-enforcement layer, never the source of truth.
- **Auto-join on.** A controller absent from the voter set discovers the
  leader via `controller.quorum.bootstrap.servers`, catches up as an
  observer, then issues `AddVoter` for itself.

## Background

KIP-853 replaces KRaft's static, config-file voter set with a dynamic
one stored in the metadata log. The mechanism:

- A voter is identified by **node id + replica directory id (UUID)**,
  with a list of listener endpoints and a supported `kraft.version`
  range. The directory id pins a voter to a specific data directory so a
  reformatted/relocated replica is treated as a distinct voter.
- The voter set is materialized by two **control records**:
  `KRaftVersionRecord` (control type 5) and `VotersRecord` (control type
  6, a full snapshot of the voter set). They are written into the
  `@metadata` log and into every metadata snapshot.
- Reconfiguration happens via three RPCs to the leader — `AddRaftVoter`
  (api key 80), `RemoveRaftVoter` (81), `UpdateRaftVoter` (82) — and is
  exposed to operators through `kafka-metadata-quorum
  add-controller/remove-controller`. `DescribeQuorum` (key 55) gains v2
  fields (directory ids, endpoints, `Nodes`).
- Safety: changes are applied **one voter at a time**, and the leader
  uses the latest voter set present in its log (committed or not) for
  quorum decisions.

Crabka's raft is **openraft 0.9**, which manages membership internally
via `EntryPayload::Membership` entries and `change_membership()`. The
core design problem is reconciling openraft's internal membership with
KIP-853's log-resident voter set. We resolve it with the lockstep model
in §"Reconfiguration coordinator".

Current state (from codebase survey): `AddRaftVoterRequest` /
`RemoveRaftVoterRequest` / `UpdateRaftVoterRequest` types are codegen'd
in `crates/protocol/generated/` but unwired; no voter/version metadata
records exist; `MetadataImage` tracks no voter or version state; voters
come from `ControllerConfig::voters` (static); snapshots are stubbed;
`ControllerHandle::{add_learner,change_membership}` exist but are unused.

## Architecture

### 1. Identity model — voter = (id, directory-id, endpoints, version-range)

- Add a **replica directory id** (`Uuid`), generated at `format` time
  and persisted in a `meta.properties`-equivalent file in the controller
  data dir, stable across restarts.
- Replace openraft's `BasicNode { addr }` with a custom `Node` carrying:
  `directory_id: Uuid`, `endpoints: Vec<Endpoint>` (listener name → host,
  port), `kraft_version_range: (u16, u16)`. `NodeId` stays `u64`. This
  `Node` is the single struct that feeds `VotersRecord`, `DescribeQuorum`
  v2, and openraft membership.
- A `VoterSet` value type (ordered map `NodeId → Node`) is the in-memory
  representation; it round-trips to/from `VotersRecord` bytes.

### 2. Control records — the authoritative artifacts

Two new Kafka control records, byte-exact with Kafka:

- **`KRaftVersionRecord`** (control type 5): `version: i16`,
  `kraft_version: i16`.
- **`VotersRecord`** (control type 6): `version: i16`, `voters: []`
  where each voter = `{ voter_id: i32, voter_directory_id: uuid,
  endpoints: [{ name, host, port }], kraft_version_feature: { min, max } }`.

Plumbing:

- New `MetadataRecord` variants `V1KRaftVersion(KRaftVersionRecord)` and
  `V1Voters(VotersRecord)` in `crates/metadata/src/records.rs`.
- `MetadataImage` (`crates/metadata/src/image.rs`) gains `kraft_version:
  u16` and `voters: VoterSet`, updated by `apply()` and checked in
  `validate()`.
- Control-record codecs live in the protocol/records layer so that
  wherever the `@metadata` log or a snapshot is serialized for Kafka
  Fetch/FetchSnapshot, types 5/6 are **byte-exact** Kafka control
  batches. This byte-exactness is the hard external constraint.

### 3. Bootstrap & format

`kafka-storage format` for a dynamic controller writes a **bootstrap
checkpoint** at offset `00000000000000000000-0000000000.checkpoint`
containing, in order: `SnapshotHeaderRecord` → `KRaftVersionRecord`
(level 1) → `VotersRecord` (initial voters) → `SnapshotFooterRecord`.
The bootstrap/startup path must write and read this.

- `--standalone`: format self as the sole initial voter.
- `--initial-controllers <id@host:port:dir-uuid,...>`: explicit initial
  voter set.
- Controllers started without being in the initial set begin empty and
  rely on bootstrap servers + auto-join.

`controller.quorum.bootstrap.servers` (endpoints only, no ids) replaces
`controller.quorum.voters` in config. The old static key is removed.

### 4. Reconfiguration coordinator

A new coordinator in `crates/raft` (e.g. `reconfig.rs`, owned by the
`Controller`) is the only thing that mutates the voter set. It runs on
the leader and enforces KIP-853 safety:

- Precondition: `kraft_version == 1` (always true here, but checked).
- **One change at a time.** Reject a reconfig RPC while another is
  in flight/uncommitted (Kafka error `REQUEST_TIMED_OUT` /
  `INVALID_REQUEST` to match Kafka's behavior).
- **AddVoter.** The candidate must already be a caught-up observer:
  track each observer's last fetch/match offset and require it within a
  configurable lag bound of the leader's HWM. Then: `add_learner` →
  confirm catch-up → `change_membership(old ∪ {new})` → the resulting
  voter set is written as a `V1Voters` record.
- **RemoveVoter.** Refuse if removal would lose quorum. Leader
  self-removal: commit the new set, then step down so a remaining voter
  takes over.
- **UpdateVoter.** Rewrite a single voter's `Node` (endpoints /
  version range) and emit a new `V1Voters`. No quorum membership change.

**Lockstep rule.** Every voter-set change is expressed as *both* a
`V1Voters` metadata record (the Kafka-visible truth) *and* a matching
openraft `change_membership` (quorum accounting). The coordinator
sequences the two so they cannot diverge: it computes the target
`VoterSet`, drives openraft to that membership, and writes the
`V1Voters` record describing the same set. On restart, the latest
`VotersRecord` in the log/snapshot is authoritative, and openraft's
replayed membership is reconciled to it (the bootstrap checkpoint's
`VotersRecord` and the initialized membership are written to agree).

### 5. Wire RPCs & handlers

In `crates/broker`:

- Handlers `add_raft_voter.rs`, `remove_raft_voter.rs`,
  `update_raft_voter.rs` (mirroring `describe_quorum.rs`). Each decodes
  the codegen'd request, forwards to the controller coordinator, and
  returns `error_code`. On a non-leader: return
  `NOT_LEADER_OR_FOLLOWER` with a leader hint (matching Kafka).
- Dispatch wiring for api keys 80/81/82 in
  `crates/broker/src/network/dispatch.rs`, with flexible-version
  handling.
- **`DescribeQuorum` → v2**: add per-voter `voter_directory_id`,
  per-replica state, and the `Nodes` block (id + listener endpoints).
- `ApiVersions` advertises 80/81/82 and `DescribeQuorum` v2.

### 6. Auto-join

On startup, a controller not in the current voter set and configured
with `controller.quorum.auto.join.enable=true`:

1. Connect to a `controller.quorum.bootstrap.servers` endpoint, discover
   the leader.
2. Register as an observer/learner and replicate until caught up.
3. Send `AddRaftVoter` for itself (own id + directory id + endpoints) to
   the leader. Idempotent — a no-op if already a voter.

### 7. Snapshots (scope boundary)

- **In scope:** the bootstrap-checkpoint snapshot format (read + write),
  and the invariant that any snapshot crabka generates **leads with**
  `KRaftVersionRecord` + `VotersRecord` so voter state survives log
  truncation.
- **Out of scope (assumption):** full snapshot *transfer* replication
  (`FetchSnapshot` / `InstallSnapshot`) stays stubbed as today. Dynamic
  voters function without it as long as the `@metadata` log is not
  compacted, which it currently is not. Approved as a follow-up.

## Components

```
crates/metadata/src/
├── records.rs        # MODIFIED — V1KRaftVersion, V1Voters variants
├── image.rs          # MODIFIED — kraft_version + voters (VoterSet) tracking
└── voters.rs         # NEW — VoterSet / Node value types, VotersRecord round-trip

crates/protocol/                # control-record codecs (types 5, 6), byte-exact
                                # DescribeQuorum v2; api keys 80/81/82 already codegen'd

crates/raft/src/
├── types.rs          # MODIFIED — custom Node (directory_id, endpoints, version range)
├── config.rs         # MODIFIED — bootstrap.servers, directory id, auto.join, lag bound
├── controller.rs     # MODIFIED — wire coordinator + auto-join into startup
├── reconfig.rs       # NEW — reconfiguration coordinator (Add/Remove/Update + safety)
├── state_machine.rs  # MODIFIED — apply V1Voters/V1KRaftVersion; bootstrap checkpoint
└── log_store.rs      # MODIFIED — meta.properties (directory id), bootstrap checkpoint I/O

crates/broker/src/
├── handlers/add_raft_voter.rs       # NEW
├── handlers/remove_raft_voter.rs    # NEW
├── handlers/update_raft_voter.rs    # NEW
├── handlers/describe_quorum.rs      # MODIFIED — v2 fields
├── network/dispatch.rs              # MODIFIED — keys 80/81/82, DescribeQuorum v2
└── broker.rs                        # MODIFIED — surface reconfig + auto-join

crates/broker/tests/
└── quorum.rs         # MODIFIED — dynamic membership scenarios
```

## Error handling

- Non-leader reconfig RPC → `NOT_LEADER_OR_FOLLOWER` + leader hint;
  caller (tool/admin) retries against the leader.
- Concurrent reconfig (another change uncommitted) → reject (match
  Kafka's `REQUEST_TIMED_OUT`/`INVALID_REQUEST`).
- AddVoter candidate not caught up → `INVALID_REQUEST` (Kafka returns an
  error and the operator/tool retries after the observer catches up).
- RemoveVoter that would lose quorum → reject; never let the cluster
  become unavailable.
- `kraft.version != 1` (defensive) → `UNSUPPORTED_VERSION`.
- openraft `ForwardToLeader`/`ChangeMembershipError` surfaced through the
  existing `RaftError` → Kafka error-code mapping.

## Testing

- **Unit:** control-record codec byte-exactness against Kafka fixtures
  (types 5/6, bootstrap checkpoint); `VoterSet` add/remove/update +
  `VotersRecord` round-trip; reconfig safety guards (single-change,
  quorum-loss refusal, not-caught-up rejection).
- **Integration** (`crates/broker/tests/quorum.rs`): bootstrap standalone
  → auto-join 2nd and 3rd controllers → `RemoveVoter` → leader
  self-removal/step-down → re-add; assert `DescribeQuorum` v2 reflects
  the live set with directory ids + endpoints.
- **Compatibility:** `kafka-metadata-quorum describe --status`,
  `add-controller`, `remove-controller` against a running crabka
  controller where Docker/JVM tooling is available; byte-compare
  `DescribeQuorum` v2 responses.

## Out of scope / YAGNI

- Static-voter mode (`kraft.version=0`) and the 0→1 upgrade path —
  dropped per dynamic-only decision.
- Full snapshot transfer (`FetchSnapshot`/`InstallSnapshot`) — separate
  follow-up (§7).
- `controller.quorum.voters` static config key — removed.
- Pre-vote / split-vote hardening — unchanged from current behavior; not
  introduced by this work.
