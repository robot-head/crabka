# KIP-1071 streams client — Standby and Warmup Task Materialization

**Status:** design approved (brainstorm)
**Builds on:** #1 membership client, #2 Processor API + stateless engine, #2b runtime (`StreamThread`/`StreamTask`), #3 state stores + changelogs.
**Ground truth:** Apache Kafka Streams 4.1 — KIP-1071 streams client standby / warmup tasks, and lag-based promotion.

## 1. Goal

Implement replication of state stores for assigned standby and warmup tasks. Compute and report `task_offsets` and `task_end_offsets` dynamically in the `StreamsGroupHeartbeatRequest` heartbeat loop. Handle task promotion (warmup -> active transition) once the broker detects the lag is caught up.

## 2. Scope

### In scope
1. **`TaskRole` enum**: Define `TaskRole { Active, Standby, Warmup }` and associate a role with each task.
2. **Heartbeat reporting**: Update `CoordinatorState` and `StreamsMembership` to track standby/warmup assignments and task offset/end-offset maps. Populate `standby_tasks`, `warmup_tasks`, `task_offsets`, and `task_end_offsets` in `StreamsGroupHeartbeatRequest`.
3. **OffsetStore `latest` API**: Add `latest` to `OffsetStore` trait and implement it in `BrokerOffsetStore` to resolve the latest offset of a partition using `ListOffsetsRequest` (timestamp = -1).
4. **Task reconciliation**: In `StreamThread::apply_assignment`, reconcile tasks according to their desired roles. Support promotions/demotions between active, standby, and warmup roles.
5. **Continuous replication**: Implement `restore_step` in `StreamTask` to fetch and apply changelog batches in the background for standby/warmup tasks during `poll_all`.
6. **Task offset tracking**: Create `TaskOffsetTracker` shared via `Arc<Mutex<_>>` between the `StreamThread` and the heartbeat loop to report real-time restored offsets and end offsets.

### Non-goals (deferred)
- Interactive Queries (IQ) integration (separate slice).
- Exactly-Once Semantics (EOS) integration (separate slice).

## 3. Architecture

### 3.1 Task Role Representation
Each task represents a `(subtopology_id, partition)`. We define `TaskRole`:
```rust
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TaskRole {
    Active,
    Standby,
    Warmup,
}
```
`StreamTask` will hold its current `role` and a map of `changelog_offsets: HashMap<String, i64>` to track the next fetch offset for each changelog topic partition.

### 3.2 Dynamic Offset Tracking
To populate `task_offsets` and `task_end_offsets` in the background heartbeat loop, we introduce `TaskOffsetTracker` in `membership/types.rs`:
```rust
#[derive(Debug, Clone, Default)]
pub struct TaskOffsetTracker {
    pub task_offsets: HashMap<(String, i32), i64>,
    pub task_end_offsets: HashMap<(String, i32), i64>,
}
```
The tracker is wrapped in `Arc<Mutex<TaskOffsetTracker>>` and shared:
1. `CoordinatorState` owns the shared tracker and reads from it during `heartbeat_once` to populate the `task_offsets` and `task_end_offsets` fields of the request.
2. `StreamThread` receives a reference to the tracker and updates it on every `poll_all` tick by calling `compute_changelog_offsets()` on all tasks.

### 3.3 OffsetStore `latest` API
To compute task end offsets, we add `latest` to the `OffsetStore` trait:
```rust
async fn latest(&self, topic: &str, partition: i32) -> Result<i64, StreamsClientError>;
```
In `BrokerOffsetStore`, it sends `ListOffsetsRequest` with `timestamp = -1` (LATEST) to fetch the log-end offset of the partition.

### 3.4 Changelog Lag Calculation
For each task `(subtopology_id, partition)`:
- The task's stores are retrieved. Each store has a `changelog_topic`.
- For each store:
  - Its partition is the task's partition index.
  - The store's end offset `end_offset` is queried via `store.latest(&changelog_topic, partition)`.
  - The store's current offset `current_offset` is:
    - If task is `Active`: `end_offset` (lag is 0).
    - If task is `Standby`/`Warmup`: the tracked next fetch offset in `changelog_offsets`.
- The task's cumulative offset is the sum of `current_offset` across all its stores.
- The task's cumulative end offset is the sum of `end_offset` across all its stores.

### 3.5 Continuous Replication
Standby and warmup tasks do not process input records or run punctuators. Instead:
- In `poll_all`:
  - Active tasks call `process_once(fetcher)` and `punctuate_wall_clock`.
  - Standby and warmup tasks call `restore_step(fetcher)`.
- In `restore_step(fetcher)`:
  - For each state store, fetch a single batch from `(changelog_topic, partition)` starting at the tracked offset.
  - Apply the records using `graph.restore_apply()`.
  - Advance the tracked offset.

### 3.6 Task Reconciliation and Promotion/Demotion
In `StreamThread::apply_assignment`:
1. Group desired active, standby, and warmup tasks from the assignment.
2. Any task that is no longer assigned to this member in any role is closed and removed.
3. Any task whose role has changed is transitioned:
   - Transition to `Active`:
     - Run a blocking catch-up `restore(fetcher)` loop to apply any remaining changelog records.
     - Seek source positions (`seek_to_start()`).
     - Initialize processors (`init()`).
     - Change role to `Active`.
   - Transition to `Standby`/`Warmup` (demotion):
     - Close processors (`close_processors()`).
     - Commit offsets (`commit()`).
     - Change role to `Standby`/`Warmup`.
4. Any new task is instantiated with the desired role:
   - If `Active`: seek to start, block-restore, and init.
   - If `Standby`/`Warmup`: initialize tracked changelog offsets to `0` and add to task map.

## 4. Verification Plan

### 4.1 Unit Tests
- Test offset reporting and wire serialization of `task_offsets`, `task_end_offsets`, `standby_tasks`, and `warmup_tasks` in `membership/coordinator.rs` with fake transport.
- Test `restore_step` in `runtime/task.rs` with `ScriptedFetcher` to ensure that standby/warmup tasks incrementally fetch and apply changelog batches.
- Test promotions/demotions in `runtime/thread.rs` by verifying task count, roles, and transitions.

### 4.2 Integration Tests
- Verify that standby/warmup tasks continuously replicate state, report correct offsets/lags in heartbeats, and transition to active once lag drops below `acceptable_recovery_lag`.
