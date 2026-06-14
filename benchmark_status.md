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

## 6. Known follow-up

Peer controller addresses are resolved to IPs at broker boot and pinned (the raft
dialer parses `SocketAddr`, [`network.rs:62`](crates/raft/src/network.rs:62)). A
killed broker that restarts with a new pod IP is reachable by *fetching* from the
leader (KRaft is pull-based) but is not re-dialed by survivors until they restart.
This is sufficient to form the quorum and survive a single failover; full
self-healing rejoin would need per-dial hostname re-resolution in the raft layer.
