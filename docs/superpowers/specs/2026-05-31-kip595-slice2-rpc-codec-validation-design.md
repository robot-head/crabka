# KIP-595 Slice 2 — RPC codec byte-exact validation

Date: 2026-05-31
Status: Approved (brainstorming) — pending spec review

## Context

Slice 2 of the KIP-595 → true-wire-compatibility program (mixed
`mirror.gcr.io/apache/kafka:4.0.0` JVM + Crabka joint quorum). See
`2026-05-30-kip595-slice0-jvm-fetch-spike-design.md` for the program overview.

**Surprise from exploration:** the KIP-595 RPC wire types are *already
generated* and present in `crates/protocol`: `Vote` (apiKey 52),
`BeginQuorumEpoch` (53), `EndQuorumEpoch` (54), `DescribeQuorum` (55),
`FetchSnapshot` (59), the KRaft-extended `Fetch` (1, with `replica_state` /
`last_fetched_epoch` / `current_leader` / `diverging_epoch` / `snapshot_id`),
and `AddRaftVoter`/`RemoveRaftVoter`/`UpdateRaftVoter` (80–82). Schemas, owned +
borrowed codecs, and the `api_key` enum all exist; `DescribeQuorum` and the
voter RPCs even have broker handlers.

So "generate the RPC codecs" is already done. What remains splits cleanly:

- **Validation** — these specific codecs have never been byte-checked against
  real JVM wire. Slice 1 proved that *generated ≠ byte-correct* until
  round-tripped (it surfaced a wrong frame version and fabricated apiKeys).
- **Wiring** — controller-listener handlers that answer Vote/Begin/EndQuorumEpoch
  are inseparable from the quorum state machine and belong to **Slice 3**.

This slice is **validation only**.

## Goal & scope

Byte-exactly validate the generated KIP-595 RPC codecs by round-tripping real
JVM-captured request **and** response frames.

**In scope:** `Vote` (52), `BeginQuorumEpoch` (53), `EndQuorumEpoch` (54),
`DescribeQuorum` (55), and the raft `Fetch` (1) — request and response each.

**Deferred:** `FetchSnapshot` (59) → Slice 4 (snapshots), where a
lagging-joiner scenario is natural. The voter-reconfig RPCs (80–82) → Slice 5.

**Out of scope:** any handler, state machine, or wiring (Slice 3); any
production `src/` change unless a round-trip mismatch forces a schema/codegen
fix.

## Components

### 1. Capture harness (inline, Docker)

A 3-node JVM controller-only quorum (`process.roles=controller`,
`controller.quorum.voters=1@a:9093,2@b:9093,3@c:9093`) on a shared docker
network. A `tcpdump` sidecar (as in Slice 0) captures a node's controller-port
traffic across three events:

- **cold-start election** → `Vote` (request+response), `BeginQuorumEpoch`
  (request+response), and inter-node `Fetch` (raft).
- **`kafka-metadata-quorum --bootstrap-controller <c> describe --replication`**
  → `DescribeQuorum` (request+response).
- **graceful leader shutdown** (`docker stop` the leader) → `EndQuorumEpoch`
  (request+response).

### 2. Frame extractor (Python)

Parse the pcap → walk length-prefixed Kafka frames → classify request vs
response by direction (dst vs src = controller port) → pair responses to
requests by `correlation_id` (to learn each response's apiKey + api_version) →
emit one fixture per frame: `<rpc>_request.bin` / `<rpc>_response.bin`, each the
frame **minus the 4-byte length prefix** (header + body), plus a recorded
`(api_key, api_version)`.

### 3. Round-trip test (Rust)

`crates/protocol/tests/kraft_rpc_roundtrip.rs`. For each fixture: decode the
header (`RequestHeader` v2 / `ResponseHeader` v1 — these RPCs are flexible) +
the body type at the captured api_version, re-encode header+body, and assert
**byte-identical** to the fixture. A small per-RPC table maps fixture → (header
kind, body type, api_version).

## Data flow

```
3-node JVM controller quorum ──tcpdump──▶ pcap ──extractor──▶ fixtures/*.bin (header+body, per RPC, per direction)
                                                                   │
generated RPC types ◀── round-trip: decode(header+body) → re-encode → assert == fixture
```

## Acceptance / testing

- **Primary:** every captured request and response frame for `Vote`,
  `BeginQuorumEpoch`, `EndQuorumEpoch`, `DescribeQuorum`, and `Fetch` round-trips
  byte-identically.
- A mismatch is a real codec bug → fix the schema/codegen, regenerate, commit
  (the Slice 1 loop).
- Fixtures embedded via `include_bytes!`; the test runs in the normal suite
  (Docker only needed to *capture*, not to run).

## Error handling / risks

- **Capture flakiness:** elections complete in seconds; capture a window and
  retry if an expected frame type is absent.
- **`EndQuorumEpoch`** requires a clean graceful leader resignation; best-effort.
  If it cannot be captured reliably, validate the other four and note
  `EndQuorumEpoch` as carried forward (it is a tiny message — low risk).
- **Whole-frame round-trip** (header+body) is used rather than body-only —
  strictly stronger and not materially harder, since headers are self-describing
  at known flexible versions.
- **No production code change expected.** If the round-trips all pass with zero
  `src/` edits, that is the intended, valuable result: confirmation that the
  generated KIP-595 RPC layer is wire-correct. Any edit would be a
  schema/codegen fix triggered by a real mismatch.

## Disposition

Permanent test + fixtures. Confirms the RPC codec foundation Slice 3 builds on.
