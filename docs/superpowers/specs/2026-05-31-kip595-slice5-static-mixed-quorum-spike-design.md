# KIP-595 Slice 5 spike — static mixed JVM+Crabka controller quorum

Date: 2026-05-31
Status: Approved (brainstorming) — pending spec review
Disposition: **Throwaway spike** (Slice-0 spirit). Produces a findings doc that decides the shape of Slice 5 proper.

## Why

The program's end goal (Slice 6) is a mixed `mirror.gcr.io/apache/kafka:4.0.0` JVM + Crabka joint
metadata quorum. The open question for Slice 5 is whether we need full KIP-853
**dynamic** reconfiguration (kraft.version=1, VotersRecord/KRaftVersionRecord on
the log, AddVoter/RemoveVoter RPCs, the reconfiguration protocol) — a large
slice — or whether a **static** quorum (`controller.quorum.voters`,
kraft.version=0, still fully supported in 4.0) suffices for cross-impl interop.
The memory note framed Slice 5 as "dynamic voters *only if static v0 is
insufficient*". This spike settles that empirically before we commit to building
dynamic reconfig.

Crabka already speaks the real KIP-595 wire (Slice 3c: Vote 52 / BeginQuorumEpoch
53 / EndQuorumEpoch 54 / Fetch 1 v17 over the controller listener) and writes
KIP-631/KIP-630 framed logs/snapshots (Slices 3d-2, 4). So a static mixed quorum
is plausibly within reach; the spike finds out.

## Goal & success criteria

Stand up **one quorum of three voters**: node ids `1,2` = Crabka controllers
(in-process, real TCP controller listener), node id `3` = a JVM
`mirror.gcr.io/apache/kafka:4.0.0` controller (`process.roles=controller`), all sharing one
`cluster_id` and one static `controller.quorum.voters` list, all kraft.version=0
/ metadata.version 4.0. Crabka holds the 2/3 majority.

**Success = BOTH:**
1. **Cross-impl election.** The three voters complete `ApiVersions` + exchange
   `Vote(52)` / `BeginQuorumEpoch(53)` / `Fetch(1 v17)` across impls and a single
   leader emerges (expected: a Crabka node, holding 2/3).
2. **Committed metadata replicates leader→follower.** A record committed on the
   leader (the bootstrap feature-level / `LeaderChange` records at offset 0, or a
   topic created via the leader) appears in a follower's log/image — observed for
   at least one JVM↔Crabka direction.

Partial success (e.g. election works, replication blocks on a specific field) is
a **valid, valuable outcome** — the failure mode is the finding.

## Approach

- **Driver:** a Docker-gated Rust integration test
  `crates/broker/tests/jvm_static_quorum_spike.rs` (`#[ignore]`, like the other
  JVM tests). It (a) starts the 2 Crabka controllers on real TCP ports, (b)
  formats + starts the JVM controller container, (c) waits for a leader and
  observes replication, (d) tears down. Reuses the existing Crabka multi-node
  controller harness where possible; the JVM node is new orchestration.
- **JVM node:** `kafka-storage format --cluster-id <shared> --config <props>`
  with `process.roles=controller`, `controller.quorum.voters=<all three>`,
  `controller.listener.names=CONTROLLER`, kraft.version unset (⇒ 0), an explicit
  `metadata.version` matching Crabka (4.0-IVx — confirm the level Crabka emits).
  Then start `kafka-server-start`/the container entrypoint.
- **Networking (asymmetric advertised addresses):** the JVM container publishes
  its controller port (`-p <p3>:<p3>`); Crabka dials it at `localhost:<p3>`. The
  JVM dials the Crabka voters at `host.docker.internal:<p1>`/`<p2>`. Each side's
  `controller.quorum.voters` therefore lists addresses reachable *from that
  side*. Crabka voters bind `0.0.0.0`. (macOS Docker Desktop: `host.docker.internal`
  resolves to the host; published ports expose the container to the host.)
- **Observation:** Crabka-side `tracing` of the engine (roles, votes, fetch
  offsets, applied records) + the JVM container logs + optionally a `tcpdump`
  pcap of the controller port for byte-level findings. Assert the success
  criteria from the Crabka follower's published `MetadataImage` /
  `quorum_snapshot()`.

## Expected friction (each a finding)

- **Replica directory ids** in Vote/Fetch at kraft.version=0 — does 4.0 require a
  non-nil `replicaDirectoryId`, and does it reject a Crabka voter that sends nil?
  (KIP-853 added directory ids; their role at v0 is the key unknown.)
- **cluster_id** exchange/validation across `Vote`/`Fetch` (Crabka currently
  sends `cluster_id: None`).
- **Controller-listener `ApiVersions`** — does the JVM's controller-to-controller
  client require a specific advertised API set / versions from Crabka?
- **JVM bootstrap records** at offset 0 (`LeaderChange` control batch + feature
  levels) — Crabka must replicate/parse them; and Crabka's own bootstrap must not
  conflict (e.g. divergent offset-0 contents between independently-formatted
  nodes).
- **metadata.version negotiation** and any `kraft.version` feature-record
  expectations even in "static" mode.
- **Voter-set identity:** static config lists `id@host:port`; confirm the JVM
  matches Crabka voters by node id alone at v0 (no directory-id pinning).

Per CLAUDE.md, all of these are checked against the actual `mirror.gcr.io/apache/kafka:4.0.0`
image behavior, not assumed.

## Out of scope

- Any permanent code (the spike is throwaway; if it forces a tiny Crabka wire
  tweak to get further, that tweak is evaluated for promotion separately).
- Dynamic reconfiguration (the whole point is to learn whether we need it).
- Bidirectional / JVM-leader replication and admin-tool-driven changes (Slice 6).

## Deliverable

`docs/superpowers/specs/2026-05-31-kip595-slice5-static-mixed-quorum-findings.md`:
does a static mixed quorum elect + replicate? the concrete gaps (with captured
bytes/errors)? and a recommendation: **(A)** static suffices → Slice 5 is
minimal/none, go to Slice 6; **(B)** small fixed set of static-mode wire fixes
needed → enumerated; or **(C)** the JVM demands kraft.version=1 → full KIP-853
dynamic is required. The spike test is then deleted (or parked `#[ignore]`) per
the Slice-0 precedent.

## Execution note

Driven **inline** (a spike is iterative empirical work, not subagent-TDD).
Time-boxed; partial findings are captured and committed even if reachability or a
hard wire gap stops full success.
