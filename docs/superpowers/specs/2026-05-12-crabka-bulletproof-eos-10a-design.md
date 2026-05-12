# Bulletproof EOS, sub-slice 10a: High Watermark + `acks=all`

Crabka slice-10 closes the slice-8 deferrals that block bulletproof
exactly-once semantics. Slice 8 shipped basic partition replication
(replication_factor-aware leader/follower roles, follower Fetch loop)
without HW tracking, `acks=all` blocking, leader-election-on-failure,
or KIP-101 leader-epoch fencing. Slice 9 shipped the transactional
control plane on top of that, with the explicit caveat that the data
plane could still lose records mid-transaction on a partition-leader
crash.

This is **sub-slice 10a**: the data-plane durability gate. After this
slice, a JVM producer with `acks=all` against Crabka blocks until every
replica in the (static) ISR has replicated the batch, and consumers
under `isolation_level=read_committed` only see records that are
durable across the full replica set. The remaining slice-8 deferrals
— KIP-101 leader-epoch, leader-election-on-failure, and ISR
shrink/expand — ship in **sub-slice 10b** as a separate spec.

## Goal

Two new guarantees:

1. **`acks=all` produce blocks until replication completes.** A
   producer with `acks=-1` (Apache Kafka's "all") sees its request
   complete only after every replica in the partition's ISR has the
   batch on disk. On timeout (the request's `timeout_ms`) the producer
   gets per-partition `NOT_ENOUGH_REPLICAS_AFTER_APPEND` (code 20) for
   any partition whose HW didn't catch up in time. With slice 10a's
   static ISR (= `replicas` from the metadata image, no shrink yet),
   this means *all* replicas. The `min.insync.replicas` knob ships
   with slice 10b alongside ISR shrink.

2. **Consumer fetches clamp at the High Watermark.** A consumer Fetch
   (`replica_id == -1`) returns only records that are durable on every
   ISR replica. For `read_uncommitted`, the visible-range upper bound
   becomes HW; for `read_committed`, the
   bound becomes `min(HW, first_unstable_offset)` (the latter is
   slice-9's LSO). Follower Fetches (`replica_id >= 0`) keep their
   current semantics — they read up to the leader's LEO, which is how
   HW makes progress in the first place.

The JVM-client acceptance gate is a 3-broker Crabka cluster running
`kafka-console-producer --request-required-acks -1` writing 100
records to a 3-replica topic, with a `kafka-console-consumer
--isolation-level read_committed` reading them back. After this slice
that round-trip survives the durability ratchet (every record on
every replica before the producer call returns).

## Non-goals (deferred to slice 10b unless noted)

- **Leader-election on failure.** A leader crashing still results in
  client disconnects; the metadata image's `leader` field is set at
  topic-creation time and never moved. Slice 10b ships controller-side
  liveness detection + election.
- **ISR shrink/expand.** The ISR is the static `replicas` set from
  topic creation. A slow or unreachable follower pins HW and causes
  `acks=all` produces to time out. Slice 10b ships the
  AlterPartition-style ISR maintenance loop.
- **KIP-101 leader-epoch fencing.** Records are not yet tagged with a
  per-leader epoch number; followers can't validate divergence
  cleanly. Slice 10b ships the leader-epoch tagging + the
  `.leader-epoch-checkpoint` byte-compat file + epoch-validated Fetch.
- **`min.insync.replicas` configuration.** With static ISR, hard-coded
  to `replicas.len()`. The configurable knob ships with slice 10b
  where ISR shrink makes it meaningful.
- **HW persistence.** The leader's HW is recomputed from follower
  Fetches after a restart; we don't persist the
  `replication-offset-checkpoint` file yet. Acceptable because:
  any in-flight `acks=all` producer either re-completes (a follower
  catches up) or times out (safe). Persistence is a slice-10b nice-to-have.

## Architecture

A new per-`Partition` `ReplicaState` struct (in a new
`crabka-broker::replica_state` module) tracks every replica's progress
on the leader side. The leader's High Watermark is the minimum LEO
across the ISR — recomputed on every follower Fetch and every
leader-side append, with a `Notify` fired whenever the HW advances.

Three handlers change:

- **Fetch (follower path).** When `replica_id >= 0`, the leader uses
  the incoming `fetch_offset` as the follower's persisted LEO and
  updates the partition's `ReplicaState` before reading. The
  `high_watermark` field on the response now carries the recomputed
  HW.
- **Fetch (consumer path).** When `replica_id == -1`, the leader reads
  the cached HW and clamps returned batches plus
  `last_stable_offset` accordingly. `read_committed` uses
  `min(HW, log.lso())` as the effective LSO; `read_uncommitted` uses
  HW directly.
- **Produce.** When `acks == -1`, after the local append the handler
  awaits `partition.await_hw_at_least(target_offset, deadline)` before
  responding OK. On timeout, the per-partition error_code is
  `NOT_ENOUGH_REPLICAS_AFTER_APPEND`.

The `ReplicaState` lives on the leader's `Partition` instance and is
populated on supervisor reconcile (when the broker materializes a
partition where it's leader). Follower brokers also get a
`ReplicaState` for symmetry, but the data only matters where this
broker is leader; the cost is one `HashMap<NodeId, i64>` per partition
which is negligible.

## Components

### `ReplicaState` (NEW: `crates/broker/src/replica_state.rs`)

```rust
pub struct ReplicaState {
    /// Membership in current ISR. Slice 10a: static = all `replicas`
    /// from the metadata image. Slice 10b will mutate this.
    pub isr: HashSet<NodeId>,
    /// Per-non-leader-replica LEO. The leader's own LEO is fed in
    /// at HW-compute time from `Log::log_end_offset()`.
    pub follower_leo: HashMap<NodeId, i64>,
    /// Cached HW = min(LEO over isr).
    pub hw: i64,
}

impl ReplicaState {
    pub fn new() -> Self;
    pub fn install_isr(&mut self, replicas: Vec<NodeId>, leader: NodeId);
    /// Returns the new HW after applying the follower's reported LEO.
    /// Caller fires `hw_advance_notify` if `new_hw > old_hw`.
    pub fn update_follower_leo(
        &mut self,
        follower: NodeId,
        follower_leo: i64,
        leader_leo: i64,
    ) -> i64;
    /// Recompute HW from the current `follower_leo` map + `leader_leo`.
    /// Called when the leader appends, to advance HW past records the
    /// leader just wrote (only matters when ISR has a single member —
    /// the rf=1 case).
    pub fn recompute_hw_for_leader_append(&mut self, leader_leo: i64) -> i64;
}
```

Unit tests cover: HW advances when the trailing follower catches up;
HW pins at the slowest ISR follower; a non-ISR follower's LEO update
is ignored; leader-only ISR (rf=1) gives HW = leader LEO; and the
follower-overshoot case (follower reports LEO higher than leader, which
means the follower is lying — clamp to leader's LEO).

### `Partition` additions (MODIFIED: `crates/broker/src/partition.rs`)

```rust
pub struct Partition {
    // existing: topic, partition_id, log, writer_tx, append_notify, _writer_handle ...
    pub replica_state: Arc<Mutex<ReplicaState>>,
    pub hw_advance_notify: Arc<Notify>,
}

impl Partition {
    /// Cached HW. Reads `replica_state` briefly.
    pub fn high_watermark(&self) -> i64;

    /// Install ISR membership. Called by the supervisor when this
    /// broker materializes a partition where it's leader.
    pub fn install_isr(&self, replicas: Vec<NodeId>, leader: NodeId);

    /// Wait until cached HW >= `target_offset` or `deadline` elapses.
    pub async fn await_hw_at_least(
        &self,
        target_offset: i64,
        deadline: Instant,
    ) -> Result<(), HwTimeout>;
}

pub struct HwTimeout;
```

The wait primitive uses a `tokio::select!` between
`hw_advance_notify.notified()` and `tokio::time::sleep_until(deadline)`,
re-reading the cached HW each wake. Returns immediately if HW already
satisfies the target.

### Fetch handler changes (MODIFIED: `crates/broker/src/handlers/fetch.rs`)

**Follower path** (`replica_id >= 0`):

Before the existing log read, for each requested partition:

```rust
let leader_leo = partition.log_end_offset();
let new_hw = {
    let mut st = partition.replica_state.lock();
    st.update_follower_leo(req.replica_id as u64, req_part.fetch_offset, leader_leo)
};
if new_hw > prev_hw_for_this_partition {
    partition.hw_advance_notify.notify_waiters();
}
```

The response's per-partition `high_watermark` field reports `new_hw`.

**Consumer path** (`replica_id == -1`):

The current `read_committed` branch (slice 9) becomes:

```rust
let hw = partition.high_watermark();
let lso = partition.log.lock().lso(); // existing
let effective_lso = lso.min(hw);
// existing filter: drop batches with base_offset >= effective_lso
// existing filter: drop control batches
// last_stable_offset = effective_lso (unchanged wire field)
// high_watermark = hw (already wired by slice 8)
```

The `read_uncommitted` branch (default consumer behavior) gains a new
HW clamp:

```rust
let hw = partition.high_watermark();
// drop batches with base_offset >= hw
// last_stable_offset = hw (Apache Kafka convention for read_uncommitted)
// high_watermark = hw
```

### Produce handler changes (MODIFIED: `crates/broker/src/handlers/produce.rs`)

After the existing local append on the leader, for each partition:

```rust
if req.acks == -1 {
    let target = base_offset + i64::from(batch.last_offset_delta) + 1;
    let deadline = Instant::now() + Duration::from_millis(req.timeout_ms as u64);
    match partition.await_hw_at_least(target, deadline).await {
        Ok(()) => {/* normal OK response with base_offset */},
        Err(_timeout) => out.error_code = codes::NOT_ENOUGH_REPLICAS_AFTER_APPEND,
    }
}
// acks == 1 or acks == 0: unchanged
```

The leader-side append also fires `replica_state.recompute_hw_for_leader_append(new_leo)`
in case `isr.len() == 1` (rf=1 case), then notifies `hw_advance_notify`.
This lives in `partition_writer.rs` after the successful `Log::append`.

### Codes (MODIFIED: `crates/broker/src/codes.rs`)

Add (if missing): `NOT_ENOUGH_REPLICAS_AFTER_APPEND = 20` and
`NOT_ENOUGH_REPLICAS = 19`. Maps to `BrokerError::Replication` for
diagnostic encoding; never crossed on the wire from this slice
(handlers set it directly per-partition).

### Supervisor changes (MODIFIED: `crates/broker/src/replicator_supervisor.rs`)

Inside `reconcile`, after `materialize_local_partition` completes for
a partition where this broker is leader:

```rust
let part_record = image.partition(&topic, partition).cloned();
if let Some(pr) = part_record
    && pr.leader == self.node_id
    && let Some(part) = self.partitions.get(&(topic, partition))
{
    part.value().install_isr(pr.replicas.clone(), pr.leader);
}
```

Idempotent — re-installing the same ISR is a no-op. Slice 10b will
replace the static install with image-driven ISR mutation.

### Root README (MODIFIED: `README.md`)

Add a "Status / Slices delivered" section listing slices 1-10a as a
short bulleted timeline. The Status section currently says only
"Pre-1.0, pre-alpha. No production use." — append a sub-section after
that paragraph. The exact bullet list is filled in by the
implementation plan; the spec just commits to the README being
current-state-accurate after slice 10a lands.

## Data flow (annotated)

```
producer (acks=-1)            leader                            follower
─────────────────             ──────                            ────────
ProduceRequest      ─────►    Produce handler
                                ├── append to local Log
                                ├── log_end_offset() = LEO_L
                                ├── (rf=1 only) HW=LEO_L; notify
                                └── await_hw_at_least(LEO_L)
                                     [parks until HW advance]

                                                      ◄──────  FetchRequest
                                                                replica_id=N
                                                                fetch_offset=LEO_F

                              Fetch handler (follower path)
                                ├── replica_state.update_follower_leo(
                                │     N, LEO_F, LEO_L) → new_hw
                                ├── if new_hw > prev_hw:
                                │     hw_advance_notify.notify_waiters()
                                ├── read log @ LEO_F
                                └── return batches + high_watermark=new_hw
                                                      ──────►  appends batches
                                                                advances LEO_F
                                                                next Fetch raises
                                                                  new_hw further

                              await_hw_at_least() wakes
                                ├── re-check: cached HW >= target?
                                └── yes → ProduceResponse OK

                              [timeout path]
                                └── deadline elapsed; reply
                                    NOT_ENOUGH_REPLICAS_AFTER_APPEND
```

## Error handling

- **`NOT_ENOUGH_REPLICAS_AFTER_APPEND` (code 20):** per-partition error
  on `acks=all` Produce response when HW didn't reach the target
  offset before the request's `timeout_ms` expired. The record is on
  the leader's log but not yet on every ISR follower. Producers
  retry; the existing slice-6 idempotence path deduplicates on retry.
- **`NOT_LEADER_OR_FOLLOWER` (already wired):** if a Produce arrives
  for a partition this broker isn't a replica of. Unchanged.
- **Consumer-side HW clamp:** never returns an error. Returns fewer
  records than the consumer's `max_bytes` would allow. The consumer's
  next poll catches up as HW advances.

## Testing

### Unit tests

`crates/broker/src/replica_state.rs::tests`:

- `hw_advances_when_trailing_follower_catches_up`
- `hw_pins_at_slowest_isr_follower`
- `non_isr_follower_leo_update_ignored`
- `single_replica_isr_hw_equals_leader_leo`
- `follower_overshoot_clamps_to_leader_leo`
- `install_isr_resets_follower_leo_to_zero`

`crates/broker/src/partition.rs::tests`:

- `await_hw_at_least_wakes_on_notify`
- `await_hw_at_least_returns_immediately_if_already_satisfied`
- `await_hw_at_least_returns_timeout_on_deadline`
- `install_isr_idempotent`

### Integration tests

NEW: `crates/broker/tests/durability.rs`. Five `#[tokio::test(flavor =
"multi_thread", worker_threads = 4)]` tests. Windows-gated like
slices 7-9's multi-broker tests.

1. **`acks_all_blocks_until_replicated`** — 3-broker cluster, rf=3,
   produce with `acks=-1`. Capture timing: the Produce response
   arrives only after follower Fetches have advanced HW to or past
   the batch's last offset.

2. **`acks_one_returns_before_replication`** — same setup with
   `acks=1`. The response returns immediately after the leader's
   local append; HW is still pinned briefly until followers Fetch.

3. **`consumer_clamps_at_hw`** — produce with `acks=1` so HW lags LEO
   briefly. Consumer Fetch (`replica_id=-1`) returns batches only up
   to HW, not LEO. Then wait for follower Fetches to advance HW;
   re-poll consumer; it now sees the rest.

4. **`read_committed_clamps_at_min_hw_lso`** — single-broker variant
   so the test is deterministic: open a transactional producer,
   begin txn, produce 3 records, commit. The control marker hits the
   leader's log; LSO advances. Use a test-only knob to pin HW below
   the marker offset for one poll cycle; verify
   `read_committed` consumer sees nothing yet. Release the HW pin;
   re-poll; consumer sees the 3 records.

5. **`acks_all_timeout_returns_not_enough_replicas`** — 3-broker
   cluster, kill one of the followers via `Broker::shutdown` after
   topic creation. Produce with `acks=-1` and `timeout_ms=2000`.
   Verify per-partition `error_code == NOT_ENOUGH_REPLICAS_AFTER_APPEND`.

The existing slice-9 transactional tests must continue to pass after
the LSO change: 4 currently-passing tests (commit, abort, fence, plus
`send_offsets_*` which is `#[ignore]`d for slice-5 reasons) and the
`#[ignore]`d interleaved-flake.

### JVM acceptance test

NEW: appended to `crates/broker/tests/jvm_acceptance.rs`:
`acks_all_durability`. 3-broker Crabka cluster on fixed ports
(non-colliding with prior JVM tests). Pipe 100 messages through
`kafka-console-producer --request-required-acks -1` (uses cp-kafka
6.1.1's bundled tool — `--request-required-acks` is stable
across Kafka 0.10+, so the existing global `KAFKA_IMAGE` works).
Consume with `kafka-console-consumer --isolation-level read_committed
--from-beginning --max-messages 100`. Assert all 100 messages are
delivered in order.

`#[ignore = "requires Docker"]` like the rest of the JVM gate.

### Acceptance gate (full)

```bash
cargo fmt --all -- --check
cargo clippy --workspace --all-targets -- -D warnings
cargo test --workspace
RUSTDOCFLAGS="-D warnings" cargo doc --workspace --no-deps
cargo test -p crabka-broker --test jvm_acceptance -- --ignored --nocapture --test-threads=1
```

All clean. Slice 9's `#[ignore]`d tests remain ignored; nothing new
goes onto the ignore list.

## Soft-EOS caveat (post slice 10a)

After this slice, a JVM `kafka-console-producer --acks all` against
Crabka cannot acknowledge a write that hasn't replicated to every ISR
member. `read_committed` consumers cannot see a record that isn't
durable across the ISR.

However: **a partition-leader crash mid-transaction still loses
data**, because leader-election-on-failure ships in slice 10b. The
control plane works (Crabka knows the leader is down — clients
disconnect and reconnect to the bootstrap, which sends them to the
old leader's metadata image entry), but no new leader is elected from
the surviving ISR. Until slice 10b, the bulletproof-EOS promise is
"durable under no-failure" — i.e., a slow follower never returns a
silent partial write, but a crashed leader still requires manual
operator intervention to recover.

This is the gap slice 10b closes.

## Reference

Spec lives at:
`docs/superpowers/specs/2026-05-12-crabka-bulletproof-eos-10a-design.md`

Meta-spec:
`docs/superpowers/specs/2026-05-10-crabka-rust-rewrite-design.md`
(slice 8 in the decomposition table; slice 10a here closes the first
group of slice-8 deferrals).
