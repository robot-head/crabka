# Bulletproof EOS, sub-slice 10b: leader-epoch + leader election + ISR shrink/expand

Sub-slice 10b closes the remaining slice-8 deferrals and completes the
bulletproof exactly-once promise. After this slice, a 3-broker Crabka
cluster survives partition-leader crashes and slow followers: `acks=all`
produces complete after election picks a new leader from ISR;
`read_committed` consumers see all committed records; zombie writes
from fenced ex-leaders are rejected via KIP-101 leader-epoch.

Slice 10a (just merged) shipped HW tracking + `acks=all` blocking +
`read_committed`-at-HW under static ISR. Slice 10b is the
control-plane side: dynamic ISR, leader election, epoch fencing.

## Goal

Three new guarantees:

1. **A partition leader can crash and the cluster keeps serving.** The
   controller detects the dead broker via missed `BrokerHeartbeat`
   RPCs, scans every partition where the dead broker is leader, picks
   the first alive ISR replica as new leader, bumps the partition's
   `leader_epoch`, and propagates the change through the existing
   metadata image. Followers retarget to the new leader; the producer's
   in-flight `acks=all` request retries against the new leader and
   completes. No data loss, no duplicate writes.

2. **A slow follower doesn't block `acks=all` produces indefinitely.**
   The leader tracks each follower's last-fetch time. When a follower's
   lag exceeds `replica.lag.time.max.ms` (default 30s), the leader
   proposes an ISR shrink via the `AlterPartition` RPC. The controller
   commits the change. HW computation now runs over the shrunken ISR,
   so `acks=all` produces make progress over the surviving replicas.
   When the lagging follower catches up, the leader proposes ISR
   expand.

3. **A fenced ex-leader cannot inject zombie writes.** Every appended
   `RecordBatch` carries a `partition_leader_epoch` field. Followers
   validate the leader's epoch on every Fetch (`FENCED_LEADER_EPOCH`,
   wire code 74). A per-partition `.leader-epoch-checkpoint` file
   records `(epoch, start_offset)` history in Apache Kafka byte-compat
   format. On leader change, followers consult the leader's
   `OffsetForLeaderEpoch` response and truncate any local writes past
   the divergence point.

The acceptance gate is a 3-broker JVM kill-the-leader test:
`kafka-console-producer --request-required-acks=-1` writes 100 records;
mid-burst we kill whichever broker is currently the leader for
partition 0; the producer completes (after internal retries) and
`kafka-console-consumer --isolation-level=read_committed --max-messages
100` reads all 100. Plus re-enabling slice-10a's three `#[ignore]`d
multi-broker tests with the slice-10b dynamic ISR in place.

## Non-goals (documented; deferred to slice 11 or later)

- **Unclean leader election (KIP-501 toggle).** When the entire ISR
  is dead, slice 10b leaves the partition unavailable. The future
  `unclean.leader.election.enable` knob lets the operator opt into
  data-loss recovery. Soft-EOS caveat: a full-ISR outage with no
  alive replicas results in a stuck partition until a former replica
  rejoins.
- **Controlled-shutdown handshake** (the `wantShutdown` flag on
  BrokerHeartbeat). `Broker::shutdown` simply stops the heartbeat
  loop; the controller times out the broker the usual way (~9s).
  Future slice can add graceful leader-handoff.
- **Preferred-leader rebalancing** (`auto.leader.rebalance.enable`).
  Slice 10b only re-elects on broker death, not for load
  rebalancing.
- **`min.insync.replicas` knob.** Continues hard-coded to
  `len(replicas)` per slice 10a. Could trivially be added now but
  belongs with slice-11 admin tooling.
- **Broker registration epoch (KIP-500 `broker_epoch`).** Slice 10b
  uses a fixed `epoch=0` for the registration; future slice can add
  the epoch bump on broker restart for fencing of pre-restart
  in-flight requests.

## Architecture

Three loosely-coupled subsystems sharing the existing controller +
metadata image:

```
   ┌─────────────────────────────────────────────────────────────────┐
   │ controller leader broker                                        │
   │                                                                 │
   │  ┌──────────────────┐   ┌────────────────────┐                  │
   │  │ BrokerHeartbeat  │──►│ liveness ticker    │                  │
   │  │ handler          │   │ (1s, fences        │                  │
   │  │                  │   │  stale brokers)    │                  │
   │  └──────────────────┘   └─────────┬──────────┘                  │
   │           ▲                       │                             │
   │           │                       ▼                             │
   │           │                ┌─────────────────┐                  │
   │           │                │ leader_election │                  │
   │           │                │ (scan partitions│                  │
   │           │                │  of dead broker;│                  │
   │           │                │  submit_change) │                  │
   │           │                └────────┬────────┘                  │
   │           │                         │                           │
   │           │                ┌────────▼────────┐                  │
   │           │                │ AlterPartition  │                  │
   │           │                │ handler         │ (ISR proposals  │
   │           │                │ (submit_change) │  from leaders)  │
   │           │                └────────┬────────┘                  │
   │           │                         │                           │
   │           │                ┌────────▼────────┐                  │
   │           │                │ openraft        │                  │
   │           │                │ submit_change   │──► metadata image│
   │           │                └─────────────────┘                  │
   └─────────────────────────────────────────────────────────────────┘
              ▲                                       │
              │ heartbeat (3s)                        │ watch_image
              │ AlterPartition (proposals)            │
              │                                       ▼
   ┌──────────┴───────────────────────────────────────────────────────┐
   │ every broker (incl. controller leader)                          │
   │                                                                 │
   │  ┌────────────────────┐  ┌─────────────────────┐                │
   │  │ heartbeat client   │  │ supervisor reconcile│                │
   │  │ (3s tick)          │  │ on image change:    │                │
   │  └────────────────────┘  │  - install ISR      │                │
   │                          │  - bump leader_epoch│                │
   │                          │  - spawn/cancel     │                │
   │                          │    follower         │                │
   │                          │    replicators on   │                │
   │                          │    leader change    │                │
   │                          └────────┬────────────┘                │
   │                                   │                             │
   │                                   ▼                             │
   │                          ┌─────────────────────┐                │
   │                          │ isr_maintenance     │                │
   │                          │ (per leader-partition,              │
   │                          │  1s tick):          │                │
   │                          │  - propose shrink   │                │
   │                          │  - propose expand   │                │
   │                          └─────────────────────┘                │
   │                                                                 │
   │                          ┌─────────────────────┐                │
   │                          │ replicator (follower)               │
   │                          │  - epoch-validated  │                │
   │                          │    Fetch            │                │
   │                          │  - OffsetForLeader  │                │
   │                          │    Epoch on fence   │                │
   │                          │  - retarget on      │                │
   │                          │    leader change    │                │
   │                          └─────────────────────┘                │
   └─────────────────────────────────────────────────────────────────┘
```

## Components

### `crates/broker/src/heartbeat/`

NEW directory. Three files:

- `mod.rs` — broker-side heartbeat client. Spawned by every broker at
  `Broker::start` (controller leader and followers alike). Loops every
  `heartbeat_interval_ms` (default 3000ms), sending `BrokerHeartbeat`
  request to the controller leader. On failure (controller not yet
  elected, network error, etc.) retries with exponential backoff.

- `controller_state.rs` — controller-side liveness state. Owned by the
  `Broker` (only meaningful on the openraft leader). Structure:
  ```rust
  pub(crate) struct ControllerLivenessState {
      pub(crate) brokers: Mutex<HashMap<NodeId, BrokerLivenessState>>,
      pub(crate) heartbeat_timeout: Duration,
  }

  pub(crate) struct BrokerLivenessState {
      pub(crate) last_heartbeat: Instant,
      pub(crate) alive: bool,
      // Slice 10b leaves this fixed at 0; slice 11 will bump on
      // broker restart for KIP-500 registration-epoch fencing.
      pub(crate) registration_epoch: i32,
  }
  ```
  Plus the 1s ticker task: scans `brokers`, transitions
  `alive→dead` for entries older than `heartbeat_timeout`, fires
  `leader_election::on_broker_dead(node_id)` per transition. On
  heartbeat arrival, transitions `dead→alive` and fires
  `leader_election::on_broker_alive(node_id)` (the latter triggers
  a re-scan in case a partition was waiting for any ISR member to
  come back).

- `handler.rs` — `BrokerHeartbeat` wire handler. Updates the
  controller's liveness state on each request. Returns `NOT_CONTROLLER
  (41)` if this broker isn't the openraft leader. Returns OK with
  `controller_id` so the broker client can redirect.

### `crates/broker/src/leader_election.rs`

NEW. Controller-side election logic.

```rust
pub(crate) async fn on_broker_dead(
    controller: &ControllerHandle,
    dead: NodeId,
    liveness: &ControllerLivenessState,
) -> Result<(), BrokerError> {
    let image = controller.current_image();
    let mut changes: Vec<MetadataRecord> = Vec::new();
    for partition_record in image.partitions_iter() {
        if !partition_record.replicas.contains(&dead) { continue; }
        let needs_election = partition_record.leader == dead;
        if !needs_election && !partition_record.isr.contains(&dead) { continue; }
        // Drop the dead broker from ISR.
        let new_isr: Vec<NodeId> = partition_record.isr.iter()
            .copied()
            .filter(|n| *n != dead && liveness.is_alive(*n))
            .collect();
        if needs_election {
            // Pick the first alive ISR member (excluding the dead one).
            let new_leader = new_isr.first().copied();
            let Some(new_leader) = new_leader else {
                // No live ISR member — partition unavailable. Slice 10b
                // does NOT do unclean leader election.
                continue;
            };
            changes.push(MetadataRecord::V1Partition(PartitionRecord {
                leader: new_leader,
                isr: new_isr,
                leader_epoch: partition_record.leader_epoch + 1,
                ..partition_record.clone()
            }));
        } else if new_isr.len() < partition_record.isr.len() {
            // Dead broker was in ISR but not leader; just shrink ISR.
            changes.push(MetadataRecord::V1Partition(PartitionRecord {
                isr: new_isr,
                ..partition_record.clone()
            }));
        }
    }
    if !changes.is_empty() {
        controller.submit_change(changes).await?;
    }
    Ok(())
}
```

`on_broker_alive` triggers a rescan: for any partition that became
unavailable (no leader, or whose ISR is below `replicas.len()` AND
includes brokers that are now alive), it doesn't change the leader
(no preferred-leader rebalancing in slice 10b) but does propose ISR
shrink/expand. Actually — ISR expand is leader-driven via
`AlterPartition`, so `on_broker_alive` is mostly a no-op except for
the unavailable-partition recovery case. The spec doc keeps the
`on_broker_alive` call as a hook for that case.

### `crates/broker/src/isr_maintenance.rs`

NEW. Per-broker task that runs ISR shrink/expand for every partition
where this broker is the current leader.

```rust
pub(crate) async fn run(
    node_id: NodeId,
    partitions: Arc<DashMap<(String, i32), Arc<Partition>>>,
    controller_client: Arc<ControllerClient>,  // talks to current openraft leader
    replica_lag_time_max: Duration,
    shutdown: CancellationToken,
) {
    let mut tick = tokio::time::interval(Duration::from_secs(1));
    loop {
        tokio::select! {
            _ = tick.tick() => {},
            _ = shutdown.cancelled() => return,
        }
        for entry in partitions.iter() {
            let part = entry.value();
            if part.current_leader.load(Ordering::Acquire) != node_id { continue; }
            let st = part.replica_state.lock().await;
            let mut to_shrink: Vec<NodeId> = Vec::new();
            let mut to_expand: Vec<NodeId> = Vec::new();
            for (n, follower) in &st.per_follower {
                let lag = follower.last_fetch.elapsed();
                if st.isr.contains(n) && lag > replica_lag_time_max {
                    to_shrink.push(*n);
                } else if !st.isr.contains(n) && lag <= replica_lag_time_max
                    && follower.last_caught_up.elapsed() < replica_lag_time_max
                {
                    to_expand.push(*n);
                }
            }
            if to_shrink.is_empty() && to_expand.is_empty() { continue; }
            let new_isr: Vec<NodeId> = compute_new_isr(&st.isr, &to_shrink, &to_expand);
            drop(st);
            // Send AlterPartition to the controller leader.
            controller_client.alter_partition(
                /* topic */, /* partition */, new_isr,
                part.current_leader_epoch.load(Ordering::Acquire),
            ).await.ok();
        }
    }
}
```

### `crates/broker/src/handlers/alter_partition.rs`

NEW. Controller-side wire handler for `AlterPartitionRequest`.

1. Validates `request.leader_epoch == current_partition.leader_epoch`
   (rejects stale proposals via `FENCED_LEADER_EPOCH`).
2. Validates the proposed ISR is a subset of `replicas` and non-empty.
3. Submits a `PartitionRecord` change via `controller.submit_change`.
   `leader_epoch` is NOT bumped here (only changes on leader change).
4. Returns the new `(isr, leader_epoch)` in `AlterPartitionResponse`.

### `crates/broker/src/handlers/offset_for_leader_epoch.rs`

NEW. Wire handler for `OffsetForLeaderEpochRequest`. Per requested
epoch:

1. If `epoch > current_leader_epoch`: return `UNKNOWN_LEADER_EPOCH (75)`.
2. If `epoch == current_leader_epoch`: return `end_offset =
   log_end_offset`.
3. If `epoch < current_leader_epoch`: look up the start_offset of
   `(current_leader_epoch)` in the `.leader-epoch-checkpoint` file
   and return that as `end_offset` (the offset at which the requested
   epoch ENDED is the offset at which the next epoch BEGAN).

### `crates/log/src/leader_epoch_checkpoint.rs`

NEW. Per-partition `.leader-epoch-checkpoint` file in Apache Kafka
byte-compat format:

```
0          <-- header version (always 0)
<n>        <-- row count
<epoch_0> <start_offset_0>
<epoch_1> <start_offset_1>
...
```

Two columns separated by a single space; one row per epoch the leader
has known about. Rows are append-only; the file is rewritten on each
update (it's tiny). Public API:

```rust
pub struct LeaderEpochCheckpoint {
    path: PathBuf,
    entries: Vec<(i32, i64)>,  // (epoch, start_offset)
}

impl LeaderEpochCheckpoint {
    pub fn open(path: PathBuf) -> Result<Self, LogError>;
    pub fn append(&mut self, epoch: i32, start_offset: i64) -> Result<(), LogError>;
    /// End offset of `epoch` = start_offset of `epoch + 1`, or
    /// `log_end_offset` if `epoch` is the current epoch. Used by
    /// `OffsetForLeaderEpoch`.
    pub fn end_offset_for_epoch(&self, epoch: i32, log_end_offset: i64) -> i64;
    pub fn latest_epoch(&self) -> Option<i32>;
    pub fn entries(&self) -> &[(i32, i64)];
}
```

### `crates/metadata/src/records.rs`

MODIFIED. `PartitionRecord` gains:

```rust
pub struct PartitionRecord {
    pub topic: String,
    pub partition: i32,
    pub leader: NodeId,
    pub replicas: Vec<NodeId>,
    pub isr: Vec<NodeId>,
    pub leader_epoch: i32,  // NEW
}
```

Existing call sites at slice-7's CreateTopics handler set
`leader_epoch: 0` on initial PartitionRecord. The serde-wincode
serialization carries the new field automatically; pre-slice-10b
metadata files would deserialize with `leader_epoch: 0` if we used
serde's defaults, but since this is a slice-10b development branch
we don't promise wire-compat with pre-10b log layouts. Document this
as a slice-10b incompatible change with prior on-disk metadata
formats; a `cargo run` against a slice-10a tempdir is expected to
work because Crabka tests always start with a fresh tempdir.

### `crates/broker/src/replica_state.rs`

MODIFIED. `ReplicaState` (currently `{ isr, follower_leo, hw }`)
extends to track per-follower last-fetch timing:

```rust
pub(crate) struct ReplicaState {
    pub(crate) isr: HashSet<NodeId>,
    pub(crate) per_follower: HashMap<NodeId, FollowerStats>,
    pub(crate) hw: i64,
    pub(crate) current_leader_epoch: i32,
}

pub(crate) struct FollowerStats {
    pub(crate) leo: i64,
    pub(crate) last_fetch: Instant,
    pub(crate) last_caught_up: Instant,  // last fetch where leo >= log_end_offset
}
```

`update_follower_leo` is updated to set `last_fetch = Instant::now()`
and to set `last_caught_up = Instant::now()` when the follower's
reported LEO equals leader's. `install_isr` is updated to also accept
a `leader_epoch` argument and store it.

### `crates/broker/src/partition.rs`

MODIFIED. `Partition` gains an atomic for the current leader epoch:

```rust
pub struct Partition {
    /* existing fields */
    pub current_leader_epoch: Arc<AtomicI32>,
    pub current_leader: Arc<AtomicU64>,  // current leader NodeId
}

impl Partition {
    pub async fn install_leader_change(&self, new_leader: NodeId, new_epoch: i32) {
        let mut st = self.replica_state.lock().await;
        st.current_leader_epoch = new_epoch;
        // Reset per-follower stats — old follower-LEO entries are stale
        // under the new leader's view; let them re-converge from each
        // follower's next Fetch.
        st.per_follower.clear();
        self.current_leader.store(new_leader, Ordering::Release);
        self.current_leader_epoch.store(new_epoch, Ordering::Release);
        self.hw_advance_notify.notify_waiters();
    }
}
```

### `crates/broker/src/handlers/produce.rs`

MODIFIED. Before passing the batch to the writer:

```rust
batch.partition_leader_epoch = part.current_leader_epoch.load(Ordering::Acquire);
```

The slice-10a HW-await path stays unchanged.

### `crates/broker/src/handlers/fetch.rs`

MODIFIED. New epoch validation before the existing partition resolve:

```rust
let part_epoch = part.current_leader_epoch.load(Ordering::Acquire);
if fp.current_leader_epoch >= 0 && fp.current_leader_epoch != part_epoch {
    if fp.current_leader_epoch < part_epoch {
        out.error_code = codes::FENCED_LEADER_EPOCH;
    } else {
        out.error_code = codes::UNKNOWN_LEADER_EPOCH;
    }
    pending.push(/* error pending read */);
    continue;
}
```

Also: for follower fetches, restore the slice-10a follower-fetch HW
maintenance block, but ONLY after the epoch check passes. This time
the path doesn't deadlock because (a) the slice-10b follower-side
replicator includes proper leader-change handling so stale fetches
are rejected early, and (b) the ISR shrink loop prevents permanent
follower stalls from pinning leader HW.

### `crates/broker/src/replicator.rs`

MODIFIED. Add two handle_response branches:

```rust
codes::FENCED_LEADER_EPOCH | codes::UNKNOWN_LEADER_EPOCH => {
    // Either stale or future. In both cases the right move is to
    // call OffsetForLeaderEpoch and truncate to the leader's view.
    let truncation_offset = call_offset_for_leader_epoch(cfg).await?;
    if let Some(part) = cfg.partitions.get(&(cfg.topic.clone(), cfg.partition)) {
        part.truncate_to(truncation_offset).await?;
    }
    LoopAction::Continue
}
```

The `current_leader_epoch` on outgoing FetchRequests is set from
`part.current_leader_epoch.load(...)`.

### `crates/broker/src/replicator_supervisor.rs`

MODIFIED. `reconcile` now handles leader changes:

- Old behavior: spawn follower replicator for each partition where
  self is in replicas + not leader.
- New behavior: track the (leader, leader_epoch) tuple per partition.
  On image change: if leader changed, cancel old replicator (or stop
  ISR maintenance for self-was-leader), call
  `Partition::install_leader_change`, spawn new replicator pointed
  at the new leader (or start ISR maintenance for self-is-new-leader).

### `crates/log/src/log.rs`

MODIFIED. `Log::append` accepts the caller's leader_epoch and:

- Stamps `batch.partition_leader_epoch = leader_epoch`.
- If `leader_epoch > last_seen_epoch`, appends
  `(leader_epoch, log_end_offset)` row to the
  `.leader-epoch-checkpoint` file.

### `crates/broker/src/config.rs`

MODIFIED. `BrokerConfig` gains:

```rust
pub struct BrokerConfig {
    /* existing */
    pub heartbeat_interval_ms: u64,         // default 3000
    pub heartbeat_timeout_ms: u64,          // default 9000
    pub replica_lag_time_max_ms: u64,       // default 30000
}
```

Tests override via `BrokerConfig::for_tests` which sets
`heartbeat_timeout_ms=2000`, `replica_lag_time_max_ms=2000` for fast CI
loops.

### Codes

`crates/broker/src/codes.rs` gains:

```rust
pub const FENCED_LEADER_EPOCH: i16 = 74;
pub const UNKNOWN_LEADER_EPOCH: i16 = 75;
pub const NOT_CONTROLLER: i16 = 41;  // if not already present
```

## Data flow

### Leader election on broker death

```
T+0   broker 1 stops responding (e.g. SIGKILL)
T+3s  broker 2's heartbeat client succeeds (talks to controller leader)
T+9s  controller's liveness ticker detects broker 1 has missed 3
        heartbeats; marks alive=false
T+9s  leader_election::on_broker_dead(1):
        for each PartitionRecord where leader=1:
          new_leader = first alive ISR member ≠ 1
          submit_change(PartitionRecord {
            leader: new_leader,
            leader_epoch: prev + 1,
            isr: isr \ {1},
            ...
          })
T+9s  controller commits change via openraft
T+9.1s every broker's metadata watch fires; supervisor.reconcile runs
        each supervisor:
          - if was-leader (broker 1, but it's dead): no-op (irrelevant)
          - if is-new-leader: install_leader_change(new_leader, new_epoch),
            start ISR maintenance for this partition
          - if is-follower: cancel old replicator (targeted old leader),
            spawn new replicator (targeted new leader)
```

### Follower-side KIP-101 truncation on leader change

```
follower's fetch to NEW leader carries old `current_leader_epoch`
   │
   ▼
new leader sees req.current_leader_epoch < part.current_leader_epoch
   → returns FENCED_LEADER_EPOCH
   │
   ▼
follower handle_response sees FENCED_LEADER_EPOCH
   → calls OffsetForLeaderEpoch(epoch=local_last_known_epoch)
   │
   ▼
leader's offset_for_leader_epoch:
   looks up start_offset for (local_last_known_epoch + 1) from
   .leader-epoch-checkpoint
   returns truncation_offset
   │
   ▼
follower calls part.truncate_to(truncation_offset)
   → drops local writes past the divergence point
   │
   ▼
follower resumes Fetch with updated current_leader_epoch + fetch_offset
```

### ISR shrink under follower lag

```
T+0   leader appends 50 records (acks=1)
        — broker 3 stops fetching for 35s
T+30s  leader's isr_maintenance tick:
        follower 3.last_fetch.elapsed() = 35s > replica_lag_time_max (30s)
        and 3 ∈ ISR
        → propose AlterPartition(isr=[1,2,3] \ [3] = [1,2], leader_epoch=N)
T+30.1s controller commits the AlterPartition change
T+30.2s every broker's image fires; supervisor.reconcile runs
        — leader broker re-runs install_isr with isr={1,2}
        HW computation now ranges only over {1,2}
        → HW advances based on broker 2's LEO
T+30.3s any blocked acks=-1 produce completes
```

## Error handling

- **`FENCED_LEADER_EPOCH (74)`** — request's `current_leader_epoch <
  partition.leader_epoch`. Returned by Produce, Fetch, AlterPartition,
  OffsetForLeaderEpoch. Caller should re-fetch metadata or call
  OffsetForLeaderEpoch.
- **`UNKNOWN_LEADER_EPOCH (75)`** — request's `current_leader_epoch >
  partition.leader_epoch`. Returned by Fetch and AlterPartition.
  Caller retries after metadata propagation (typically <100ms).
- **`NOT_CONTROLLER (41)`** — `BrokerHeartbeat` or `AlterPartition`
  sent to a broker that isn't the openraft leader. Caller redirects
  to the `controller_id` in the response.
- **`NOT_LEADER_OR_FOLLOWER (6)`** — Produce or Fetch arrives at a
  broker that's no longer the partition leader (metadata propagation
  hasn't reached the client yet). Caller re-fetches metadata.

**Edge cases:**

- **No live ISR member during election.** Partition becomes
  unavailable. Slice 10b does NOT unclean-elect. When a former replica
  rejoins, its heartbeat ticks alive, controller's `on_broker_alive`
  triggers a rescan, and if the rejoined broker is in the (stale) ISR
  the partition can elect from it. If the controller has shrunk ISR
  to empty (all replicas were unavailable simultaneously), unclean
  election is the only recovery — out of scope for 10b.
- **Single-broker cluster (rf=1).** ISR maintenance has no work,
  election has no candidates. Slice-10a rf=1 paths remain unchanged.
- **Controlled shutdown.** Out of scope; behaves identically to crash.
- **Concurrent AlterPartition + leader election.** AlterPartition's
  leader_epoch check rejects stale proposals; the leader's request
  fails with FENCED_LEADER_EPOCH after election bumps the epoch.
- **Old leader's in-flight Produce against the new leader.** Producer
  has stale metadata; sends to old leader. Old leader appends locally
  with stale `current_leader_epoch`. Old leader is no longer in the
  metadata image's leader role — its supervisor will demote it on the
  next reconcile (within ~100ms of the controller commit). Until then,
  any local writes get `current_leader_epoch = old_epoch`; on next
  follower-Fetch they'd be truncated via KIP-101. After supervisor
  demote, the broker's Produce handler returns NOT_LEADER_OR_FOLLOWER.
- **Slice-9 transactional control plane.** `__transaction_state`
  partitions also go through leader election. The TxnCoordinator
  on the new leader replays records from local log; resumes from last
  persisted state. No special handling needed beyond the standard
  leader-change reconcile.

## Testing

### Unit tests

`crates/broker/src/heartbeat/controller_state.rs::tests`:

- `fresh_broker_alive_on_first_heartbeat`
- `alive_to_dead_after_timeout`
- `dead_to_alive_on_heartbeat_after_gap`
- `ticker_fires_on_alive_to_dead_transition_exactly_once`
- `ticker_fires_on_dead_to_alive_transition`

`crates/broker/src/leader_election.rs::tests`:

- `elects_first_alive_isr_member`
- `skips_dead_broker_when_not_leader_and_not_in_isr`
- `shrinks_isr_when_dead_broker_in_isr_but_not_leader`
- `no_election_when_no_live_isr_member`
- `does_not_bump_epoch_when_only_isr_shrink`
- `bumps_epoch_when_leader_changes`

`crates/broker/src/isr_maintenance.rs::tests`:

- `proposes_shrink_when_follower_exceeds_lag_threshold`
- `proposes_expand_when_lagging_follower_catches_up`
- `proposes_nothing_when_isr_is_stable`
- `does_not_propose_self_shrink`

`crates/log/src/leader_epoch_checkpoint.rs::tests`:

- `round_trip_byte_compat_format`
- `append_preserves_existing_rows`
- `end_offset_for_epoch_returns_next_epoch_start_offset`
- `end_offset_for_current_epoch_returns_log_end_offset`
- `header_format_matches_apache_kafka`

`crates/broker/src/replica_state.rs::tests` (extended from slice-10a):

- `update_follower_leo_advances_last_fetch_time`
- `update_follower_leo_sets_last_caught_up_when_leo_equals_log_end`
- `install_leader_change_clears_per_follower_stats`

### Integration tests

NEW `crates/broker/tests/leader_election.rs`. Windows-gated. Four tests:

1. `broker_death_elects_new_leader`
2. `produce_during_leader_failover`
3. `acks_all_completes_after_isr_shrink`
4. `isr_expand_on_catchup`

NEW `crates/broker/tests/leader_epoch.rs`. Windows-gated. Three tests:

1. `fenced_leader_epoch_truncates_zombie_writes`
2. `epoch_checkpoint_byte_compat`
3. `unknown_leader_epoch_on_metadata_lag`

MODIFIED `crates/broker/tests/durability.rs`:

- Remove `BrokerHandle::test_install_isr` (slice-10a hack) — slice-10b
  has real ISR maintenance.
- Replace `acks_all_times_out_when_no_follower` with a real
  multi-broker variant that exercises ISR shrink.

MODIFIED `crates/broker/tests/replication.rs`:

- Un-`#[ignore]`
  `replication_factor_three_propagates_to_all_followers` and
  `out_of_range_truncates_and_recovers`. Restore their `acks=-1`
  (slice-10a's workaround was `acks=1`).

### JVM acceptance tests

NEW in `crates/broker/tests/jvm_acceptance.rs`:

`acks_all_survives_leader_crash` — 3-broker Crabka cluster on ports
10392/10492/10592 (+ controller 10393/10493/10593). 100-record
`kafka-console-producer --request-required-acks=-1
--request-timeout-ms=30000`. After the 50th record, programmatically
`broker.shutdown()` whichever broker is currently the partition-0
leader. Verify producer completes; `kafka-console-consumer
--isolation-level=read_committed --max-messages=100` reads all 100.

UN-ENV-GATE `three_node_replication_byte_compare` and
`acks_all_durability` (drop their `CRABKA_RUN_*_TEST` gates).

### Acceptance gate

```bash
cargo fmt --all -- --check
cargo clippy --workspace --all-targets -- -D warnings
cargo test --workspace
RUSTDOCFLAGS="-D warnings" cargo doc --workspace --no-deps
cargo test -p crabka-broker --test jvm_acceptance -- --ignored --nocapture --test-threads=1
```

All clean. **No new `#[ignore]`s land in this slice.** The slice-9
`interleaved_commit_and_abort` `#[ignore]` can be removed if the
slice-10b dynamic-ISR fix incidentally addresses the underlying race;
otherwise the slice-9 ignore stays.

## Soft-EOS caveat (post slice 10b)

After this slice, a JVM `kafka-console-producer --acks all` against
Crabka can survive arbitrary single-broker failures (whether the
killed broker is the leader or a follower). The cluster's bulletproof
EOS promise is complete for the no-unclean-election scenario.

Two known limitations:

1. **Full-ISR outage.** If every replica becomes unreachable
   simultaneously, the partition is unavailable until at least one
   former replica rejoins. No data loss but no liveness during the
   outage. Slice 11 will add `unclean.leader.election.enable` for
   data-loss recovery.

2. **Controlled-shutdown handshake.** `Broker::shutdown` works but
   produces a brief unavailability window (~9s) while the controller
   times out the broker. Slice 11 can add KIP-500's `wantShutdown`
   for graceful leader-handoff (sub-second failover).

Both are documented and acceptable.

## Reference

- Spec: this file (`docs/superpowers/specs/2026-05-13-crabka-bulletproof-eos-10b-design.md`)
- Slice 10a spec: `docs/superpowers/specs/2026-05-12-crabka-bulletproof-eos-10a-design.md`
- Meta-spec: `docs/superpowers/specs/2026-05-10-crabka-rust-rewrite-design.md`
  (item #8 in the decomposition table; slices 10a and 10b together
  close all slice-8 deferrals)
- KIP-101: `https://cwiki.apache.org/confluence/display/KAFKA/KIP-101`
- KIP-500: `https://cwiki.apache.org/confluence/display/KAFKA/KIP-500`
