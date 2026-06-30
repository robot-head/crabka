# KIP-595 Slice 0 — JVM-Fetch de-risking spike

Date: 2026-05-30
Status: Approved (brainstorming) — pending spec review

## Context

Crabka's metadata quorum currently runs on the `openraft` crate using a
Crabka-private wire protocol (API keys 1000–1004: AppendEntries, Vote,
InstallSnapshot, SubmitChange, MetadataFetch). This is deliberately **not** the
KIP-595 Kafka-wire protocol. openraft is push-based; KIP-595 replication is
follower-**pull** via an extended `Fetch` (API key 1). A JVM KRaft node cannot
today form a quorum with, or fetch metadata from, a Crabka controller.

The goal of the overall program is **true KIP-595 wire compatibility at the
strictest bar: a mixed JVM + Crabka joint quorum** (a real
`mirror.gcr.io/apache/kafka:4.0.0` controller and a Crabka controller in one quorum, voting,
replicating, and electing across implementations). Because a JVM node in a
joint quorum applies the very log records a Crabka leader appends, this
necessarily couples four layers that must all match the JVM byte-for-byte and
semantically:

1. **KIP-595 consensus** — `Vote` (52), `BeginQuorumEpoch` (53),
   `EndQuorumEpoch` (54), `DescribeQuorum` (55), and the `Fetch` (key 1)
   KRaft extension (pull replication: replica id, diverging epoch, snapshot id,
   high watermark).
2. **KIP-595 state machine** — KRaft roles, leader-epoch semantics, election
   rules, the `quorum-state` file, HWM derived from follower fetch offsets.
3. **KIP-631 metadata records** — the real Kafka control-record formats
   (`RegisterBrokerRecord`, `TopicRecord`, `PartitionRecord`,
   `FeatureLevelRecord`, …) at exact apiKey + version, plus the bootstrap
   checkpoint. Crabka's current `MetadataRecord` enum is not wire-compatible.
4. **KIP-630 snapshots** — byte-exact snapshot artifacts + `FetchSnapshot`
   (59) for cross-implementation catch-up.

The program is sequenced **spike-first incremental** (strategy A):

- **Slice 0 — JVM-Fetch spike (this doc)**
- Slice 1 — KIP-631 real control records + bootstrap checkpoint
- Slice 2 — KIP-595 RPC wire codecs (via protocol-codegen)
- Slice 3 — KRaft consensus state machine (replace openraft)
- Slice 4 — KIP-630 snapshots byte-exact + FetchSnapshot
- Slice 5 — `kraft.version=1` / KIP-853 dynamic voters (only if static v0 is
  insufficient for the bar)
- Slice 6 — mixed-quorum acceptance (JVM controller joins a Crabka quorum)

This document specifies **Slice 0 only**.

## Goal

Prove, against a live `mirror.gcr.io/apache/kafka:4.0.0` JVM, that a single-node Crabka
controller can speak enough real KRaft wire for a **JVM broker observer** to
`Fetch` the `__cluster_metadata-0` log and decode it with zero format errors.

The spike's real deliverable is **knowledge** — a verified wire-format findings
doc that makes slices 1–3 precise instead of speculative. The code is a
throwaway means to that end.

### Success criteria

The spike has succeeded when **all** of the following hold:

- A JVM broker (`process.roles=broker`,
  `controller.quorum.voters=1@<crabka-host>:<port>`) connects to the Crabka
  controller listener.
- It completes `ApiVersions` negotiation and issues a real KRaft `Fetch`
  (key 1) for `__cluster_metadata-0`.
- Crabka replies with the bootstrap records (at minimum a `FeatureLevelRecord`
  setting `metadata.version`) as byte-exact Kafka record batches plus the
  correct KRaft Fetch-response tagged fields (CurrentLeader, HighWatermark).
- The JVM advances its fetch offset past the bootstrap records and logs a
  successful metadata load, with **zero** CRC / format / decode errors in the
  JVM container log.

### Explicitly out of scope (Slice 0)

Broker registration, heartbeats, leader election, `Vote` /
`BeginQuorumEpoch` / `EndQuorumEpoch`, snapshots, multi-voter quorums, dynamic
reconfiguration, and any metadata **writes**. The spike controller is a frozen
single-voter leader serving a static, hand-built log.

## Architecture & disposition

A **feature-gated throwaway spike path** (`kraft-spike`) in the controller
listener, parallel to and not wired into the existing openraft flow. The
openraft controller remains untouched. The spike adds a minimal hand-coded
responder that:

- answers `ApiVersions`,
- answers `Fetch` for `__cluster_metadata-0` from a hardcoded in-memory record
  log,
- self-identifies as the single voter / leader in the Fetch response so the
  observer does not look elsewhere.

The code is disposable. The kept artifact is the findings doc.

## Components

1. **Wire ground-truth capture** *(first task, before any Crabka code)* — stand
   up a pure-JVM KRaft cluster (1 controller + 1 broker, `mirror.gcr.io/apache/kafka:4.0.0`)
   and capture the real controller↔broker metadata `ApiVersions` + `Fetch`
   exchange on the wire (tcpdump/pcap or a transparent TCP tee on loopback).
   Yields ground-truth bytes for: Fetch request/response versions, the
   `__cluster_metadata` topic id, the per-partition tagged fields
   (CurrentLeader, SnapshotId, DivergingEpoch), the bootstrap record set for
   the image's default `metadata.version`, and the exact `ApiVersions` the
   observer path requires. (Per CLAUDE.md: verify the image empirically rather
   than trusting the wiki.)

2. **Bootstrap log builder** — produces the byte-exact initial
   `__cluster_metadata-0` segment + `bootstrap.checkpoint` that
   `kafka-storage format` writes for the negotiated `metadata.version`.

3. **Minimal KRaft Fetch responder** — decodes the JVM's `Fetch`, serves
   records from the bootstrap log, encodes the KRaft Fetch response (records +
   HWM + CurrentLeader tagged field).

4. **ApiVersions responder** — advertises exactly the versions the JVM
   observer path requires (discovered in component 1).

## Data flow

```
JVM broker observer                    Crabka controller (spike)
   | --- TCP connect ----------------->|
   | --- ApiVersions req ------------->|
   |<-- ApiVersions resp (Fetch vN…) --|
   | --- Fetch(__cluster_metadata-0,   |
   |        offset=0, replicaId=brk) ->|
   |<-- Fetch resp: records[0..k],     |
   |      HWM, CurrentLeader=1 --------|
   | (decode + replay; advance offset) |
   | --- Fetch(offset=k) ------------->|
   |<-- Fetch resp: empty, HWM=k ------|
   | (logs "loaded metadata up to k")  |
```

## Error handling

Spike-grade. On any decode mismatch, log the offending bytes and the JVM's
reaction and record it in the findings doc. No production error paths. A JVM
rejection *is* a finding — iterate the bytes until the JVM accepts them.

## Testing / acceptance

One Docker-gated integration test mirroring the existing
`crates/broker/tests/jvm_acceptance.rs` harness:

- boot the spike controller,
- boot a real `mirror.gcr.io/apache/kafka:4.0.0` broker pointed at it,
- assert from the broker container logs that it fetched and loaded the
  metadata with no format errors.

Gated behind the same env var / Docker guard as the existing JVM acceptance
tests so it does not run in the unit suite.

## Kept deliverable

`docs/superpowers/specs/2026-05-30-kraft-wire-findings.md` (written during the
spike), recording:

- exact `Fetch` request/response versions and field layouts,
- the `__cluster_metadata` topic id,
- the bootstrap record set for `mirror.gcr.io/apache/kafka:4.0.0`'s default
  `metadata.version`,
- `ApiVersions` requirements for the observer path,
- any surprises / undocumented behavior.

This is the input that makes slices 1–3 precise.

## Risks

- **Undocumented wire details** — mitigated by the ground-truth capture task.
- **ApiVersions gating** — the JVM may refuse to proceed if a required API/version
  is missing; discovered and closed empirically.
- **metadata.version coupling** — the observer must agree on the feature level;
  the bootstrap `FeatureLevelRecord` must match what the JVM image defaults to.
- **Throwaway temptation creep** — keep the spike strictly decode-only; resist
  pulling registration/election forward. Those are later slices.
