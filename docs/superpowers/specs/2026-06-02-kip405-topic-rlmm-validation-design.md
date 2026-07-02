# KIP-405 tiered storage: promote & validate the topic-backed RLMM (⚠️ → ✅)

**Date:** 2026-06-02
**Status:** Design approved; ready for implementation plan
**KIP:** [KIP-405](https://cwiki.apache.org/confluence/display/KAFKA/KIP-405) — Kafka tiered storage

## Problem & motivation

Tiered storage is marked ⚠️ *partial* in the README, with the prose
"the `crabka-remote-storage-topic` (KIP-405 production RLMM) crate is in
tree but not yet wired into the broker." **That prose is stale.** PR #227
wired `TopicBasedRemoteLogMetadataManager` into `Broker::start`, and PR
#313 ("Finish Tiered Storage: slices 48m–48r") closed the remaining
functional gaps. What exists and is tested today:

| Piece | State |
|---|---|
| SPI + in-memory RLMM (default) | wired |
| Topic-backed RLMM (`__remote_log_metadata`) | wired, opt-in via `[remote_storage.kafka_metadata]` |
| Copy path, local retention, remote read (incl. read-committed aborted txns), `ListOffsets`-by-timestamp, remote retention, partition-delete | done |
| Snapshots / fast-bootstrap (48p), dynamic per-broker metadata-partition assignment (48q), TLS/SASL on the metadata client (48r) | done |
| Local + S3 (MinIO/R2/GCS) RSM backends | done |
| Operator CRD (`TieredStorage` Local/S3 + `metadataManager`) | done |
| JVM-validated MinIO acceptance (`tiered_storage_round_trip_through_minio`) | done — but uses the **in-memory** RLMM |

The ⚠️ is therefore a **deliberate hold**, not missing wiring. Two real
gaps justify it:

1. **The production topic-backed RLMM is never JVM-validated end-to-end.**
   Only Crabka-internal loopback / in-process tests exercise it. The one
   JVM acceptance test (`tiered_storage_round_trip_through_minio`,
   `crates/broker/tests/jvm_acceptance.rs:7903`) deliberately runs the
   *in-memory* RLMM — it sets `remote_storage_backend: Some(S3(..))` but
   not `remote_log_metadata_kafka`.
2. **A real tiered cluster silently runs the non-durable in-memory RLMM
   by default.** Topic-backed is opt-in (`remote_log_metadata_kafka:
   Option<KafkaRlmmConfig>`, `None` ⇒ in-memory). This is backwards from
   Kafka, where `TopicBasedRemoteLogMetadataManager` *is* the RLMM and
   there is no in-memory option in production.

Plus the fire-and-forget bootstrap has a fail-open hole (below).

## Goal

Make the durable topic-backed RLMM the **first-class, robust, JVM-validated**
path, then flip KIP-405 ⚠️ → ✅ honestly.

### Non-goals (explicit)

- **No** byte-exact `__remote_log_metadata` ↔ JVM `RemoteLogMetadataSerde`
  interop. Crabka's event codec (`crates/remote-storage-topic/src/serde.rs`,
  `WIRE_VERSION = 0`, hand-rolled `tag|payload`) is **not** compatible with
  the JVM's `ApiMessageAndVersion` + generated `RemoteLogSegmentMetadataRecord`
  framing. A Crabka-only tiered cluster works; a **mixed JVM+Crabka tiered
  cluster sharing the internal topic does not**, and that is a deliberate
  non-scenario (real Kafka clusters run one RLMM implementation). The README
  will state this explicitly so the ✅ is not misread as mixed-cluster interop.
- **No** new RSM backends, retention-policy changes, or operator CRD
  redesign. The `metadataManager` surface already exists; we only ensure it
  defaults on.

## Compatibility note

Crabka is greenfield and undeployed (see `CLAUDE.md`). The config-enum change
in W1 is a straight replacement — no `#[serde(default)]`, no kept-around
`Option` variant, no migration shim.

---

## W1 — Topic-backed RLMM as the first-class default

**Today** (`crates/broker/src/config.rs:436-482`): `remote_log_metadata_kafka:
Option<KafkaRlmmConfig>`; `None` ⇒ in-memory placeholder, `Some` ⇒ topic-backed.

**Change:** when tiered storage is enabled (`remote_storage_backend.is_some()`),
default to topic-backed. Replace the `Option` with an explicit enum:

```rust
/// Which RemoteLogMetadataManager the broker runs when tiered storage
/// is enabled. Topic-backed is the production default (matches Kafka's
/// TopicBasedRemoteLogMetadataManager); in-memory is an explicit opt-out
/// for in-process integration tests that have no real listener to loop
/// back to.
pub enum RlmmKind {
    TopicBacked(KafkaRlmmConfig),
    InMemory,
}
```

- **Default:** when a `RemoteStorageBackend` is configured and no explicit
  RLMM kind is given, the broker uses `TopicBacked` with a `KafkaRlmmConfig`
  whose `bootstrap` is **auto-derived** from the broker's own advertised /
  inter-broker listener. The bootstrap task already derives the *security*
  policy from the inter-broker listener (`bootstrap_topic_rlmm`,
  `crates/broker/src/broker.rs:2461-2468`); extend it to derive the *address*
  too when not explicitly set, so single-node "just works".
- **In-memory opt-out:** in-process integration tests (`remote_reader.rs`
  tests and any test harness with no real serving listener) set
  `RlmmKind::InMemory` explicitly.
- **File config:** `crates/broker/src/file_config.rs:880-893` maps
  `[remote_storage.kafka_metadata]` → `RlmmKind::TopicBacked`. With the new
  default, the presence of `[remote_storage]` alone is enough to select
  topic-backed; the `[remote_storage.kafka_metadata]` sub-table only
  overrides `num_partitions` / `replication` / `bootstrap`.
- **Operator:** the `metadataManager` CRD surface
  (`crates/operator/src/crd/kafka.rs:392`) already renders the TOML block;
  ensure topic-backed is the rendered default when tiering is enabled.

This is what makes W3's validation meaningful: the path under test is the
path real clusters run.

---

## W2 — Fail-closed bootstrap robustness

Two defects in the current fire-and-forget bootstrap
(`crates/broker/src/broker.rs:2369-2400` spawn site; `2452-2495` body):

1. **No retry.** If `KafkaMetadataEventLog::start` or
   `TopicBasedRemoteLogMetadataManager::start` fails (listener not yet
   serving, transient connect error), the task logs `warn!` and returns —
   the broker stays on the placeholder **forever**.
2. **Silent metadata-loss window.** The `SwappableRlmm` placeholder is an
   `InmemoryRemoteLogMetadataManager` that *silently accepts writes*
   (`crates/remote-storage-topic/src/swappable.rs`). During the bootstrap
   window the copy task tiers segments to the RSM and records their metadata
   in the placeholder; then `swap()` discards it. Result: **orphaned RSM
   objects with no durable metadata.**

**Fix:**

- **Retry with bounded backoff.** Replace the one-shot start with a loop:
  attempt `KafkaMetadataEventLog::start` + `TopicBasedRemoteLogMetadataManager::start`,
  on error log + back off (capped), retry — until success or
  `shutdown.cancelled()`. The `tokio::select!` on the shutdown token stays.
- **`NotReadyRlmm` placeholder.** Add a stub `RemoteLogMetadataManager` that
  returns `RemoteStorageError::NotReady { partition }`
  (`crates/remote-storage/src/error.rs:70`) for **every** method. Use it as
  the `SwappableRlmm` initial value whenever `RlmmKind::TopicBacked`. Effects,
  both relying on existing infra:
  - The copy path calls `add_remote_log_segment_metadata(CopySegmentStarted)`
    **before** copying data to the RSM. `NotReady` there makes the copy task
    skip the segment — it already `warn!`s + `continue`s on RLMM error
    (`crates/broker/src/remote_log_manager.rs:132`+). **Nothing is tiered
    until durable metadata is available** → no orphaned objects.
  - Remote reads return retryable `NotReady`, already handled in
    `fetch.rs:1139` and `list_offsets.rs:96/127` (and propagated through
    `remote_reader.rs:980`), so clients retry until the real manager swaps in.
- **Observability.** Keep the `tiered_storage_rlmm_topic_backed` 0→1 gauge
  (`crates/broker/src/metrics.rs`); add a bootstrap-attempt counter and
  last-error so a stuck bootstrap is visible.

**Invariant to pin in tests:** `add_remote_log_segment_metadata` precedes the
RSM `copy_log_segment_data` call, so a `NotReady` add cannot leave orphaned
remote data. The TDD plan asserts this.

---

## W3 — JVM-validated acceptance (core deliverable)

Two new tests, both `#[ignore = "requires Docker"]`, extending the existing
MinIO harness (`MinioContainer`, `start_host_broker_with_minio_tier`,
`mirror.gcr.io/confluentinc/cp-kafka:7.8.8`, `kafka-console-producer` / `kafka-console-consumer`
in `crates/broker/tests/jvm_acceptance.rs:7690+`).

### T1 — Durability across restart (single-broker, primary proof, locally runnable)

Proves topic-backed ≠ in-memory, and is single-broker so it is reliable on
dev machines **and** CI:

1. Start one Crabka broker, MinIO S3 backend, **topic-backed RLMM** (the W1
   default).
2. JVM produce ~200 records; small `segment.bytes` + `local.retention.bytes=1`
   so ≥2 segments tier to S3 and evict locally (existing fixture pattern).
3. **Shut the broker down; restart it against the same `log.dir`.** (New
   harness capability — no restart-against-same-dir test exists today.)
4. JVM consume `--from-beginning` → all records read back from the remote tier.

The in-memory RLMM loses all segment metadata at step 3 and fails step 4; the
topic-backed RLMM recovers it from `__remote_log_metadata` + the on-disk
snapshot (48p). This directly validates durability/fast-bootstrap.

### T2 — Multi-broker metadata sharing (the multi-broker case, CI-validated)

1. 2-broker Crabka cluster (reuse the `start_two_sasl_brokers`-style harness,
   `crates/broker/tests/jvm_acceptance.rs:3689`) + MinIO; replicated topic;
   topic-backed RLMM on both brokers.
2. Tier segments on the partition leader (broker A).
3. Force leadership to broker B (controlled shutdown of A, mirroring
   `acks_all_survives_leader_crash`, `:1474`).
4. JVM consumer reads from the remote tier **via broker B**, which serves the
   read from metadata it consumed off `__remote_log_metadata` — having never
   tiered the segment itself.

### Reliability & where validation runs

- **Both T1 and T2 run locally on Docker (macOS)** and on CI — the
  multi-broker JVM harness (`three_node_*` / `acks_all_*` style) works under
  the implementer's local Docker setup, so T2 is a locally-runnable proof,
  not CI-only.
- **Always verifiable locally:** `cargo build`, `cargo test --workspace`
  (non-Docker), `cargo clippy --workspace --all-targets -D warnings`,
  `cargo fmt --check`, the new unit tests (NotReadyRlmm stub, retry loop,
  default RLMM selection, the add-before-copy invariant), plus the in-process
  two-broker unit tests in `crates/remote-storage-topic/src/manager.rs`.
- **Verifiable locally with Docker:** T1 (single-broker restart durability)
  and T2 (multi-broker metadata sharing), both `#[ignore = "requires Docker"]`.

### Test inventory

- Unit: `NotReadyRlmm` returns `NotReady` for all methods; retry loop
  succeeds after N injected failures and stops on shutdown; `RlmmKind` default
  selection (`TopicBacked` when tiered enabled, `InMemory` only when explicit);
  add-precedes-copy invariant in the copy task.
- In-process integration: extend the topic-backed loopback tests
  (`crates/broker/tests/tiered_storage_topic_rlmm.rs`) for the fail-closed
  window (copy task skips while the RLMM is `NotReady`, then tiers after swap).
- JVM acceptance: T1 (local-capable), T2 (CI). Both `#[ignore]`.

---

## W4 — README + STATUS + matrix flip

- Rewrite the stale prose at `README.md:101-111`: drop "not yet wired into the
  broker"; describe the topic-backed RLMM as the **default durable RLMM**;
  state the **Crabka-only-cluster / no `__remote_log_metadata` JVM record
  interop** non-goal explicitly so the ✅ is honest.
- Flip both KIP-405 rows ⚠️ → ✅: the feature table (`README.md:184`) and the
  KIP table (`README.md:405`).
- Add a STATUS.md slice entry documenting what was validated and the deferred
  JVM-record-interop non-goal.

---

## Files touched (anticipated)

- `crates/broker/src/config.rs` — `RlmmKind` enum, default selection.
- `crates/broker/src/file_config.rs` — TOML mapping onto `RlmmKind`.
- `crates/broker/src/broker.rs` — bootstrap retry loop, `NotReadyRlmm`
  placeholder selection, auto-derived bootstrap address.
- `crates/remote-storage-topic/src/swappable.rs` (or a new `not_ready.rs`) —
  `NotReadyRlmm` stub.
- `crates/broker/src/metrics.rs` — bootstrap-attempt / last-error observability.
- `crates/operator/src/crd/kafka.rs` (+ controller render path) — default
  topic-backed when tiering enabled.
- `crates/broker/tests/jvm_acceptance.rs` — T1, T2, restart harness helper.
- `crates/broker/tests/tiered_storage_topic_rlmm.rs` — fail-closed window test.
- `README.md`, `STATUS.md` — prose + matrix flip + slice entry.

## Risks

- **Auto-derived bootstrap address** must resolve correctly for single-node
  loopback *and* multi-broker inter-broker listeners; covered by T1 (loopback)
  and T2 (inter-broker).
- **Making topic-backed the default** changes the existing
  `tiered_storage_round_trip_through_minio` test to run topic-backed; either
  let it ride the new default (extra free validation) or pin it to
  `RlmmKind::InMemory` if we want to preserve its original intent — decide
  during implementation.
