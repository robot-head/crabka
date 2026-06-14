# Benchmark Status

**Branch:** `claude/amazing-raman-44dff6`
**Last Updated:** 2026-06-13 (~16:35 PT)

---

## 1. Executive Summary

The `crabka/failover` benchmark is blocked because **the 3 broker pods never form a
joint KRaft quorum** — each runs as an isolated single-node cluster, so replication
factor 3 cannot be satisfied. This is the same root cause described in earlier
revisions of this doc, but two earlier claims were wrong and have been corrected:

* **The previously-documented fix was never committed.** The earlier doc listed
  voter-rendering changes to `file_config.rs`, `listeners.rs`, and `common.rs` as
  "completed". `git grep` finds them on **no branch** — not `HEAD`, not
  `deploy-operator-fixes` (which is the *same commit* as this branch), not
  `origin/main`. The work was lost or never made. The fix has to be written.
* **The "Docker/WSL on Windows" blocker was a dead end.** The deployed images
  (`ghcr.io/robot-head/crabka-{broker,operator}:v0.3.6`) are built by CI, not
  locally — [`.github/workflows/publish-images.yml`](.github/workflows/publish-images.yml)
  runs melange/apko on a GitHub runner and exposes a **`workflow_dispatch`** trigger
  with a `tag` input. No local Docker is needed to rebuild images.

**Good news:** the broker engine *already* supports static multi-voter quorums
([`broker.rs:1221`](crates/broker/src/broker.rs:1221), "KIP-595 static multi-voter
set"). When `controller_quorum_voters.len() > 1`, every node starts with the full
voter set and elects over the real KIP-595 wire. Only the config plumbing and a
listener bind are missing.

---

## 2. Verified Cluster State (GKE `robot-head` / `test-crabka-cluster`, us-central1-b)

* Operator: `operator-crabka-operator` Running, image `:v0.3.6` (Helm release `operator`).
* Brokers: `demo-broker-{0,1,2}-0` Running, image `:v0.3.6`, driven by `Kafka/demo`.
* Strimzi operator also deployed (`strimzi-system`) for comparison runs.
* **Defect:** `demo-broker-0` metadata response lists only itself —
  `resp_brokers=["0@demo-broker-0-0...:9092"] resp_controller_id=0`. The rendered
  `demo-broker-config` ConfigMap has **no `controller_quorum_voters`** key.

---

## 3. Root Cause — three missing pieces

1. **Operator never renders a voter list.** `render_broker_toml` emits listeners and
   server-properties but no `controller_quorum_voters`, so every broker falls back to
   the single self-voter the binary seeds at [`bin/broker.rs:185`](crates/broker/src/bin/broker.rs:185).
2. **Broker can't accept a voter list from TOML.** `FileConfig` has no
   `controller_quorum_voters` field, so even a rendered list would be ignored.
3. **Controller listener binds loopback.** Under `--config-file`, `--listen-addr`
   keeps its `127.0.0.1:9092` default (it `conflicts_with` `config_file`), so the
   controller binds `127.0.0.1:9093` ([`controller.rs:601`](crates/raft/src/controller.rs:601)).
   Peers cannot reach it. Plus the headless Service does not set
   `publishNotReadyAddresses`, so peer DNS would not resolve before readiness — and
   the broker brings the quorum up *before* opening the client listener, which would
   deadlock all three pods.

---

## 4. The Fix (5 changes, 2 disjoint crates)

Wire contract: TOML key `controller_quorum_voters = ["<id>@<host>:9093", ...]`;
controller listener binds `0.0.0.0:9093`.

| # | File | Change | Status |
|---|------|--------|--------|
| 1 | `crates/operator/src/controller/common.rs` (`render_service`) | `publishNotReadyAddresses: true` + controller port 9093 on the headless Service | ✅ |
| 2 | `crates/operator/src/controller/listeners.rs` (`render_broker_toml`) | emit `controller_quorum_voters` (all brokers, `id@fqdn:9093`) | ✅ |
| 3 | `crates/operator/src/controller/common.rs` (`render_configmap`) | build the voter list from `addresses_per_broker`, pass to render | ✅ |
| 4 | `crates/broker/src/file_config.rs` | add `controller_quorum_voters`, parse + DNS-resolve (retry for startup race) into `BrokerConfig` | ✅ |
| 5 | `crates/broker/src/bin/broker.rs` | bind controller `0.0.0.0:9093` under `--config-file` | ✅ |

Changes 1–5 committed in `607d3efc`. A second cold-start bug surfaced on first
multi-node deploy and was fixed in `7cf5ec18`:

| # | File | Change | Status |
|---|------|--------|--------|
| 6 | `crates/broker/src/bin/broker.rs` + `crates/raft/src/{controller,lib}.rs` | `detect_bootstrap_mode` now uses the controller's own `metadata_log_nonempty` (durable `quorum-state`) instead of "segment dir exists". A node killed mid-election (segment dir created by `KraftController::open`, but no committed `quorum-state`) re-Bootstraps instead of dying in a `Rejoin` crashloop. | ✅ |

**Why #6 was needed:** on the first 3-broker deploy, all brokers crashlooped with
`Rejoin mode requires non-empty raft log`. `KraftController::open` creates
`__cluster_metadata/@metadata-0` *before* the election commits; the old detection
keyed Rejoin on that dir existing, disagreeing with the controller's `quorum-state`
check. Single-node never hit it (self-election commits instantly). The operator-side
voter rendering (changes 1–3) was verified working in-cluster before this surfaced.

After #6, brokers reached `Bootstrap` mode and started the controller on
`0.0.0.0:9093`, but leader election never completed — a third bug, fixed in
`3aeceec2`:

| # | File | Change | Status |
|---|------|--------|--------|
| 7 | `crates/broker/src/{file_config,config,broker}.rs` + `crates/operator/src/controller/{listeners,common,kafka}.rs` | Wire controller-quorum **mTLS**. Broker: parse `controller_server_name` + `trust_roots_path`; build the inter-broker connector with `build_client_config_with_identity` (trusts the cluster CA + presents this broker's cert); raft dialer uses the shared headless FQDN as SNI. Operator: render `trust_roots_path = cluster-ca` and `controller_server_name = <kafka>-broker-headless.<ns>.svc.cluster.local`. | ✅ |

**Why #7 was needed:** debug logs showed `tls: received fatal alert: UnknownCA` on
every controller peer handshake. The broker's outbound TLS client config had empty
trust roots (`trust_roots_path: None`) and no client identity, and used SNI
`"localhost"` — not a DNS SAN on the broker certs (which carry IP `127.0.0.1`, the
pod FQDN/name, and the shared headless FQDN). This mTLS peer path had never been
exercised because single-node clusters have no peers.

**Deploy iterations:** `quorum-fix` (initial) → `quorum-fix2` (+#6 detect fix) →
`quorum-fix3` (rebased onto `origin/main`: picks up #512 high-watermark, #504 ext4,
#511 stateright) → `quorum-fix4` (+#7 mTLS). All built via `publish-images.yml`
`workflow_dispatch`. (Commit hashes above are pre-rebase; the branch was rebased at
`quorum-fix3`.)

---

## 5. Deploy & Benchmark Path

1. Implement §4, `cargo fmt`, `cargo check`/`test` for both crates, commit, push.
2. Build+publish images via CI: `gh workflow run publish-images.yml -f tag=<tag>`
   (runs melange/apko on a GitHub runner → ghcr.io + DockerHub). No local Docker.
3. Redeploy: `helm upgrade` the operator to the new tag; bump `Kafka/demo` /
   broker image; **wipe broker PVCs** (greenfield — no state to preserve) so brokers
   re-`format` and re-elect under the new voter config.
4. Verify quorum: a metadata response should list all 3 brokers and a stable
   `controller_id`; create an RF-3 topic and confirm no `INVALID_REPLICATION_FACTOR`.
5. Run `bench/run-matrix.sh`; generate `SUMMARY.md` via `just bench-report`.

---

## 6. Current Status (verified in-cluster, `quorum-fix4`)

**The original blocker is RESOLVED.** On GKE `test-crabka-cluster` with operator +
brokers at `:quorum-fix4`:
* 3-broker KRaft quorum forms — `Kafka/demo` status: `3/3 brokers ready across 3 pool(s)`.
* Both cold-start paths verified: **Bootstrap** (initial, 0 restarts, ready in ~30s)
  and **Rejoin** (rolling restart recovers from committed `quorum-state`).
* mTLS controller handshake succeeds (no more `UnknownCA`).
* **RF-3 topic creates** — `bench-topic` 12p/RF3 `Ready=True`, no `INVALID_REPLICATION_FACTOR`.

## 7. Produce-routing bug found & fixed (`a4f69518`) — client-side, not the broker

Running `bench/scripts/run-scenario.sh crabka failover 3broker-rf3` exposed a
**client-side** routing bug (the broker, leadership, and replication are correct —
JVM `kafka-console-producer` writes RF-3 to all 12 partitions fine, and
`kafka-topics --describe` shows every partition with a valid leader + full ISR
`[0,1,2]`).

**Root cause:** `BrokerPool::refresh_brokers` (`crates/client-core/src/pool.rs`)
parsed each advertised broker address with `parse::<SocketAddr>()`, which only
accepts a **literal IP**. Brokers advertise **DNS names** (pod FQDNs), so the parse
failed silently, the `(id→addr)` registry was never populated, `knows_broker` stayed
false, and `resolve_leader` routed every batch to the round-robin **bootstrap**
connection → permanent `NOT_LEADER_OR_FOLLOWER` for any partition the bootstrap
broker doesn't lead. The producer's recursive re-route (`sender.rs`, bounded by
`cfg.retries` = `i32::MAX`) then recursed until the tokio worker **stack overflowed**
(~67s in). Single-node never hit it (bootstrap == the only broker).

**Fixes (3 crates):**
1. **`client-core/pool.rs`** — `refresh_brokers` is async and resolves the host via
   `tokio::net::lookup_host` (DNS names AND IPs); `client.rs` awaits it. *(root cause)*
2. **`client-producer/sender.rs`** — force a metadata refresh when a `NOT_LEADER`
   hint names a leader whose address the pool doesn't know yet; cap routing re-routes
   at a constant (8), independent of the transport-retry budget, so a never-reachable
   leader fails gracefully instead of overflowing the stack.
3. **`bench-driver/scenario.rs`** — broker-pod prefix `^demo-broker` matches both the
   single-pool e2e naming and the multi-pool bench naming, so the failover kill finds
   the partition-0 leader pod.

Verified at unit level + **in-cluster (`quorum-fix5`)**: no stack overflow, produce
flows across all 3 brokers, and the failover kill correctly deletes the partition-0
leader pod (`demo-broker-0-0`).

## 7b. Failover path: two further gaps (the failover *recovery* doesn't work yet)

With steady-state produce fixed, the `failover` re-run surfaced two **distinct,
substantial** gaps — both in the *recovery* after the leader is killed, neither in
the quorum/steady-state path:

1. **Client has no failover resilience.** After broker-0 (a partition leader) is
   killed, the producer retried dead `leader=0` **4331×** ("connection closed") and
   never recovered, hanging the driver (no result written). Two sub-bugs:
   * `BrokerPool::get` returns the **cached, closed** connection to the dead broker
     instead of evicting + reconnecting.
   * The transport-retry loop (`sender.rs`) retries the *same* (dead) leader and only
     refreshes the broker registry — it never re-resolves the partition leader, so it
     never routes to the new leader the controller elected. JVM clients fast-failover
     in seconds; this loops until the test ends.
2. **Broker can't rejoin after a kill (IP-pinning, see §8).** The killed broker-0
   restarts at a **new pod IP**; survivors have its old IP pinned, so it can't
   re-establish quorum membership and crashloops (exit 137, liveness-killed) in
   `Rejoin` mode. Cluster drops to a degraded 2/3.

Neither is the broker produce path (JVM produce + describe are perfect) nor the
quorum formation (Bootstrap + rolling-restart Rejoin both verified). They are
failover-recovery features that need to be built out for a meaningful `failover`
benchmark number. The 5 **non-failover** cluster scenarios (small-msg-saturate,
fixed-rate-latency, large-msg, fan-out, mixed-acks) should run on the now-working
steady-state path.

## 8. Known follow-up

Peer controller addresses are resolved to IPs at broker boot and pinned (the raft
dialer parses `SocketAddr`, [`network.rs:62`](crates/raft/src/network.rs:62)). A
killed broker that restarts with a new pod IP is reachable by *fetching* from the
leader (KRaft is pull-based) but is not re-dialed by survivors until they restart.
This is sufficient to form the quorum and survive a single failover; full
self-healing rejoin would need per-dial hostname re-resolution in the raft layer.
