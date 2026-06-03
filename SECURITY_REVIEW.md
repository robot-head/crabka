# Crabka — Security Review

**Date:** 2026-06-03
**Scope:** Whole-codebase review of the Kafka-compatible broker (~450K LOC Rust),
covering authentication, broker request / wire-protocol handling, authorization /
ACLs, raft / inter-broker, storage / operator, plus CI, supply-chain, and
dangerous-primitive sweeps.

> **Bottom line:** The crypto hygiene is genuinely good (constant-time compares,
> CSPRNG nonces/salts, `alg:none` walled off, no path traversal, no command
> injection, hardened operator pods). But the **control plane is effectively
> unauthenticated by default**, and **untrusted client input drives unbounded
> allocations before any authorization check**. Several issues are exploitable by
> an unauthenticated remote attacker. Every Critical/High finding below was
> verified against the source.

## Severity summary

| ID  | Severity | Title |
|-----|----------|-------|
| C-1 | Critical | Controller (raft) listener unauthenticated by default → full cluster takeover |
| C-2 | Critical | Raft engine never checks Vote/BeginQuorumEpoch sender is a voter → leadership hijack |
| C-3 | Critical | Decompression bomb, reachable pre-authorization |
| H-1 | High     | Controller handshake authenticates but does not authorize |
| H-2 | High     | Inter-broker control-plane APIs on data listener have no authorization |
| H-3 | High     | LITERAL resource name `"*"` not treated as the all-resources wildcard |
| H-4 | High     | Unbounded `Vec::with_capacity(n)` from attacker-controlled array length in all decoders |
| H-5 | High     | Unbounded pre-allocation from record `header_count` / `records_count` |
| M-1 | Medium   | Missing authorization on many client-facing APIs |
| M-2 | Medium   | No concurrent-connection cap; IPv6 bypasses the rate quota |
| M-3 | Medium   | Unbounded raft snapshot buffer growth |
| M-4 | Medium   | Rebalancer Helm chart ships no `securityContext` |
| L-1 | Low      | SCRAM server does not verify client-final `r=` nonce / `c=` channel binding |
| L-2 | Low      | OAuth introspection validator has no audience restriction |
| L-3 | Low      | S3 credentials in `#[derive(Debug)]` config structs |
| L-4 | Low      | Unbounded pre-allocation from file-supplied count in leader-epoch checkpoint |
| L-5 | Low      | Operator SSRF via `KafkaRebalance.spec.endpoint` |
| L-6 | Low      | OPA authorizer fail-open is operator-selectable |
| L-7 | Low      | Supply chain: `rsa` crate (RUSTSEC-2023-0071) |

---

## 🔴 Critical

### C-1 — Controller (raft) listener is unauthenticated by default → full cluster takeover

**Files:** `crates/broker/src/broker.rs:1070`, `crates/raft/src/server.rs:293`, `crates/broker/src/config.rs:587`

The default `controller_listener_protocol` is `Plaintext` (`config.rs:587`), and when
it's plaintext the broker sets the inbound handshake to `None` (`broker.rs:1073`).
With no handshake, the raw `TcpStream` is fed straight into `dispatch()`, which
routes `Vote`, `BeginQuorumEpoch`, `EndQuorumEpoch`, `Fetch`, `FetchSnapshot`,
`SubmitChange`, and `MetadataFetch` into the engine with **no authentication and
no authorization**. `dispatch()` takes no principal, and `dispatch_submit_change`
deserializes an attacker-supplied `Vec<MetadataRecord>` and calls
`engine.submit_change`.

**Attacker model:** Any host with TCP reachability to the controller listen
address (default `:9093`).

**Impact:** Inject arbitrary cluster metadata — create/delete topics, **write ACL
records granting itself superuser-equivalent access**, register brokers,
manipulate leadership — and exfiltrate the entire `__cluster_metadata` log (all
ACLs, SCRAM credential records, delegation tokens) via `MetadataFetch`. Kafka
gates all of these behind `CLUSTER_ACTION`.

**Fix:** Refuse to start a controller with an unauthenticated controller listener;
attach the authenticated principal to the connection and authorize every
controller RPC against voter-set membership (default-deny, not default-plaintext).

### C-2 — Raft engine never validates Vote/BeginQuorumEpoch originate from a quorum voter → leadership hijack & log divergence

**Files:** `crates/raft/src/kraft/core.rs:281-318`, `crates/raft/src/kraft/controller.rs:662`, `crates/raft/src/kraft/transport.rs:457`

Peer identities (`candidate`, `leader_id`, `from`) are decoded straight from the
wire and used without checking they belong to `self.state.voters`.
`handle_begin_quorum_epoch` adopts any `leader_id` at a higher epoch
unconditionally:

```rust
// core.rs:289
let accept = leader_epoch > self.state.leader_epoch
    || (leader_epoch == self.state.leader_epoch && self.state.leader_id.is_none());
if !accept { return Vec::new(); }
self.state.leader_epoch = leader_epoch;
self.state.leader_id = Some(leader_id);   // attacker-chosen, not voter-checked
```

The inbound `Vote` path decodes `voter_id` then discards it (`controller.rs:669`),
despite a comment promising the JVM's recipient-targeting check. An attacker sends
`BeginQuorumEpoch{leader_id: attacker, leader_epoch: cur+1}`; the victim adopts
the attacker as leader, then fetches / truncates its metadata log from the
attacker. Spoofed high-epoch Votes can fence the legitimate leader.

**Fix:** Before processing any inbound raft event, verify the authenticated
transport principal maps to the claimed `NodeId` and that `NodeId ∈ voters`;
enforce the inbound `voter_id == self.me` check.

### C-3 — Decompression bomb, reachable pre-authorization

**Files:** `crates/compression/src/{gzip,zstd,lz4,snappy}.rs`; trigger `crates/protocol/src/records/owned.rs:438`

`decompress()` takes no max-output bound — every codec does `read_to_end` into an
unbounded `Vec`:

```rust
// crates/compression/src/gzip.rs:19
pub fn decompress(data: &[u8]) -> Result<Bytes, CompressionError> {
    let mut decoder = GzDecoder::new(data);
    let mut out = Vec::with_capacity(data.len() * 2);
    decoder.read_to_end(&mut out)?;   // no size limit
    Ok(Bytes::from(out))
}
```

It is invoked at **request-decode time**: `ProduceRequest::decode` →
`RecordsPayload::decode` → `from_bytes` → `RecordBatch::decode` → `decompress`,
which runs **before** the ACL preamble in `handlers/produce.rs:76`. A ~MB-scale
compressed batch (well within the 100 MiB frame cap) expands to tens/hundreds of
GB → OOM. On a plaintext listener this is **unauthenticated**; on SASL listeners
any low-privilege user can trigger it (no topic ACL is consulted before decode).

**Fix:** Thread a max-output cap into `decompress` (read through
`Read::take(max + 1)` and error if exceeded), derived from the effective
`message.max.bytes` with a hard absolute ceiling.

---

## 🟠 High

### H-1 — Controller handshake authenticates but does not authorize

**Files:** `crates/broker/src/raft_handshake.rs:71`, `crates/raft/src/server.rs:293`

Even with a non-plaintext controller listener, `BrokerRaftHandshake::upgrade`
terminates SASL/TLS and returns a bare `Box<dyn DuplexStream>` — the
authenticated **principal is dropped** and `dispatch` applies no `CLUSTER_ACTION`
check. SCRAM/PLAIN use the same credential store as client SASL. So **any valid
client credential** (e.g. an ordinary consumer) suffices to drive all the C-2
attacks once it can reach the controller port. Authentication ≠ authorization.

**Fix:** Carry the authenticated principal out of the handshake and gate every
controller RPC on `CLUSTER_ACTION` (or an explicit voter-identity allowlist). Do
not share the client credential namespace with the inter-broker/controller path.

### H-2 — Inter-broker control-plane APIs on the data listener have no authorization

**Files:** `crates/broker/src/handlers/{alter_partition,broker_heartbeat,get_replica_log_info}.rs`, gate `crates/broker/src/network/dispatch.rs:335`

`AlterPartition (56)`, `BrokerHeartbeat (63)`, and `GetReplicaLogInfo (93)` contain
zero `authorize` calls (verified). `AlterPartition` submits ISR-changing
`PartitionRecord`s via `controller.submit_change`; `BrokerHeartbeat` can
fence/unfence brokers. Kafka requires `CLUSTER_ACTION` for all three. The generic
dispatch gate only runs `if is_sasl_listener` (`dispatch.rs:335`), so on a
plaintext data listener it is a no-op anyway. An attacker can shrink ISRs / force
unclean leadership → data loss or availability loss.

**Fix:** Route these through the inline-intercept path with `ctx` and require
`CLUSTER_ACTION` on `Cluster`, returning `CLUSTER_AUTHORIZATION_FAILED (31)`.

### H-3 — LITERAL resource name `"*"` is not treated as the all-resources wildcard

**File:** `crates/metadata/src/image.rs:292`

`matching_acls` does an exact `acls_literal.get(&(rt, rn.to_string()))` for
literals with no special-casing of `"*"` (host `"*"` is handled in
`simple_acl.rs:72`, but resource-name `"*"` is not). Kafka stores cluster-wide
grants as `LITERAL "*"` (what `kafka-acls --topic '*'` produces). The consequence
is bidirectional: an ALLOW-all grant silently grants nothing (lockout), and — the
security-relevant direction — a **DENY-all is silently not applied**, defeating an
intended blanket restriction → authorization bypass.

**Fix:** In `matching_acls`, additionally include literal entries keyed
`(rt, "*")` for every request of that resource type (Kafka `WILDCARD_RESOURCE`
semantics).

### H-4 — Unbounded `Vec::with_capacity(n)` from attacker-controlled array length in every generated decoder

**Files:** `crates/protocol-codegen/src/emit/owned.rs:1782` → e.g. `crates/protocol/generated/MetadataRequest.owned.rs:145`

`get_array_len` (`crates/protocol/src/primitives/array.rs:55`) returns counts up to
`i32::MAX` / `u32::MAX-1` with **no check against the remaining buffer**, and the
codegen template feeds it directly to `Vec::with_capacity(n)`. A few-byte request
declaring a ~2-billion-element array forces an immediate multi-GB allocation
(the subsequent loop fails on EOF, but the allocation already happened). Affects
every request type, including pre-auth ones (Metadata, Produce, Fetch).

**Fix (one place, protects all generated decoders):** bound the length in
`get_array_len` / `get_nullable_array_len` against `buf.remaining()` (minimum
element size ≥ 1 byte), or cap the initial `with_capacity` and let the loop grow.

### H-5 — Unbounded pre-allocation from record `header_count` / `records_count`

**Files:** `crates/protocol/src/records/owned.rs:210,450`, `crates/protocol/src/records/borrowed.rs:224`

Same class, hand-written. `RecordHeader { key: String, value: Option<Bytes> }` is
~48 bytes; a tiny record claiming `header_count = 2^31-1` forces a ~100 GB
`with_capacity` before any element is read. Same pre-auth reachability via Produce.

**Fix:** Cap the initial capacity (`.min(buf.remaining())` or a small constant) and
let the loop grow; the per-element decode already errors on EOF.

---

## 🟡 Medium

### M-1 — Missing authorization on many client-facing APIs

**Files:** `crates/broker/src/handlers/{list_offsets,offset_for_leader_epoch,describe_configs,describe_log_dirs,heartbeat,sync_group,leave_group,consumer_group_heartbeat,share_group_heartbeat,streams_group_heartbeat,find_coordinator}.rs`

These handlers consult no authorizer and are dispatched via the plain
`&Broker`-only table. Kafka requires, e.g., `Describe` on Topic for
`ListOffsets`/`OffsetForLeaderEpoch`, `DescribeConfigs`, `Cluster Describe` for
`DescribeLogDirs`, and `Read` on Group for the membership APIs. Impact:
information disclosure (offsets, broker configs, log-dir layout) and consumer-group
disruption. Lower severity than H-2 because these are not control-plane mutations.

**Fix:** Add the corresponding `Describe` / `DescribeConfigs` / `Read`-on-Group
gates via the inline-intercept path so they receive the principal.

### M-2 — No concurrent-connection cap; IPv6 bypasses the rate quota

**File:** `crates/broker/src/broker.rs:2776`

`accept_loop` spawns one task per accepted socket with no global or per-IP ceiling.
The KIP-612 connection-rate throttle is IPv4-only and only active when an IP quota
is explicitly configured. There is no equivalent of Kafka's
`max.connections` / `max.connections.per.ip`. Amplifies C-3/H-4/H-5 (each
connection can hold a 100 MiB read buffer).

**Fix:** Add a global (and per-IP) connection-count limit backed by a
`tokio::sync::Semaphore` acquired before `tokio::spawn`; extend the rate quota to
IPv6.

### M-3 — Unbounded raft snapshot buffer growth

**File:** `crates/raft/src/kraft/snapshot_fetch.rs:55`

`on_chunk` accepts an attacker-controlled `size` and appends each chunk to
`self.buf` with no maximum-size cap. Reachable from a believed-leader (which an
unauthenticated peer can become via C-1/C-2) → follower OOM.

**Fix:** Enforce a configurable maximum snapshot size and abort when `size` or
accumulated bytes exceed it.

### M-4 — Rebalancer Helm chart ships no `securityContext`

**Files:** `charts/crabka-rebalancer/templates/deployment.yaml`, `charts/crabka-rebalancer/values.yaml`

No pod- or container-level `securityContext` (no `runAsNonRoot`, dropped
capabilities, `readOnlyRootFilesystem`, or `seccompProfile`), in contrast to the
fully hardened operator chart and operator-rendered broker pods. Likely runs as
root with a writable root FS and the full default capability set.

**Fix:** Mirror the operator chart's hardened `podSecurityContext` /
`containerSecurityContext` blocks.

---

## 🟢 Low / Informational

- **L-1** — SCRAM server never verifies the client-final `r=` nonce or `c=`
  channel binding (`crates/security/src/scram/server.rs:141`). Not an auth bypass
  (the proof binds to the server's stored nonce), but an RFC 5802 §5.1 gap; the
  client side does check this.
- **L-2** — OAuth introspection validator (`crates/security/src/oauthbearer.rs:678`)
  has no audience restriction, unlike the signed-JWS path. Add an optional
  `expected_audience` for multi-resource-server deployments.
- **L-3** — S3 credentials sit in `#[derive(Debug)]` config structs
  (`crates/remote-storage/src/s3.rs:84`, `crates/broker/src/file_config.rs:177`);
  latent log-leak if anyone ever `debug!(?config)`. The live client already has a
  redacting `Debug`. Give the carriers a manual redacting `Debug`.
- **L-4** — Unbounded `Vec::with_capacity(count)` from a file-supplied count in
  `crates/log/src/leader_epoch_checkpoint.rs:58` (local / tiered-restore
  reachable, not wire). Clamp against the real line count.
- **L-5** — Operator SSRF: `KafkaRebalance.spec.endpoint` is used verbatim as a
  request URL (`crates/operator/src/controller/rebalance.rs:307`); a privileged
  CR author could point the operator at internal addresses. Validate against an
  allow-list or document as a trusted field.
- **L-6** — OPA authorizer has an operator-selectable fail-open
  (`crates/broker/src/authorizer/opa.rs:196`, `allow_on_error`). Document as
  security-sensitive and default closed.
- **L-7** — Supply chain: the `rsa` crate is present (RUSTSEC-2023-0071 Marvin
  timing attack). Low impact here — used for public-key JWT verification, not
  decryption — but worth tracking.

---

## Verified safe (no finding)

- `alg:none` cannot reach the signed JWT validator; the unsecured validator
  conversely rejects any non-empty signature.
- JWKS matches `alg` → key type (no RSA/HMAC key confusion); RSA verification
  enforces ≥ 2048-bit modulus; absent `kid` is rejected unless exactly one key
  exists.
- SCRAM proof/server-signature and PLAIN password comparisons use
  `subtle::ConstantTimeEq`.
- All nonces/salts use `ring::rand::SystemRandom` (CSPRNG), fail-closed.
- TLS/mTLS never disables certificate or hostname verification; `Required`/
  `Optional` client-auth without a CA path is a hard error.
- **No path traversal**: segment / S3 object paths are built from topic *UUIDs* +
  integer partitions, never from topic names.
- **No runtime command injection** (only `rustfmt`/`protoc`/`docker` in
  codegen/build/test paths).
- Operator injects all secrets via `secretKeyRef` / mounted Secrets — never into
  TOML, CR status, events, or annotations; generated passwords use a CSPRNG;
  ClusterRole is per-resource scoped (no wildcards); operator/broker pods are
  hardened.
- GitHub Actions have no `pull_request_target`-with-secrets and no untrusted-event
  interpolation into `run:`; `claude.yml` is `contents: read` only.
- No hardcoded secrets in runtime code.

---

## Recommended fix order

1. **C-1 / C-2 / H-1** — authenticate the controller listener unconditionally,
   attach the principal, and authorize every raft/controller RPC against
   voter-set membership. Single highest-leverage change (currently enables full
   cluster takeover).
2. **C-3 + H-4 / H-5** — cap decompression output and bound `get_array_len` /
   record-count pre-allocation. Two small changes that close the entire pre-auth
   DoS surface.
3. **H-2 / H-3 / M-1** — add the missing `CLUSTER_ACTION` and per-API ACL gates;
   implement LITERAL `"*"` wildcard semantics.
4. Medium/Low hardening as capacity allows.

> **Methodology note:** This review reflects the codebase at the time of writing
> and is not a guarantee of completeness. Findings were produced by focused
> subsystem audits with manual verification of each Critical/High against the
> source; line numbers may drift as the code evolves.
