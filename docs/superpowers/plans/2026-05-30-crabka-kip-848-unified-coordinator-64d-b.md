# KIP-848 Slice 64d-B — Unified `GroupCoordinator` skeleton — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking. This is a large, mostly-mechanical refactor with one genuinely hard task (B4, classic parking). Tasks are **sequential** — each leaves the tree compiling and a named suite green, so a regression bisects to one step. Do not parallelize; later tasks consume earlier ones' types.

**Goal:** Collapse the classic `GroupManager` (`coordinator/mod.rs` + `group.rs`) and the next-gen `NextGenCoordinator` (`coordinator/next_gen/`) into one `GroupCoordinator` under `coordinator/unified/`: one per-group actor registry, one persistence path, one `Group` container that holds either a classic or a consumer state machine. **Behavior-preserving** — groups stay single-type, no migration, and every existing classic/next-gen/JVM test passes unmodified.

**Architecture:** See `docs/superpowers/specs/2026-05-30-crabka-kip-848-unified-coordinator-64d-b-design.md`. Actor-per-group model; classic `JoinGroup`/`SyncGroup` parking is re-expressed as a park/wake message protocol where the actor holds the reply `oneshot::Sender` and the handler keeps the `tokio::time::timeout`. State machines move **verbatim** (classic `Group`→`ClassicState`, next-gen `GroupState`→`ConsumerState`) under a `GroupKind` enum.

**Tech Stack:** Rust, tokio (mpsc + oneshot + `time::timeout`), dashmap, crabka workspace crates.

---

## File structure

End state (everything under `crates/broker/src/coordinator/`):

- `unified/mod.rs` — **new.** `GroupCoordinator { groups: Arc<DashMap<String, Arc<GroupActorHandle>>>, config, metadata, offsets_log, seeds, seeds_cache }`; `get_or_create_classic`, `get_or_create_consumer`, `find`, `list_groups`, `describe_group`, `delete_group`, `shutdown_all`, bootstrap-replay methods + `finalize_bootstrap`. Replaces both `GroupManager` and `NextGenCoordinator`.
- `unified/group.rs` — **new.** `Group { group_id, kind: GroupKind, committed_offsets: HashMap<(String,i32), OffsetEntry> }`; `GroupKind::Classic(ClassicState) | Consumer(ConsumerState)`. `ClassicState` is today's `coordinator::group::Group` moved here verbatim (incl. `Member`, `GroupState`, `OffsetEntry`, `AddMemberOutcome`).
- `unified/actor.rs` — **new.** `GroupActorHandle`, `UnifiedMessage`, the actor loop, the parked-joiner / parked-follower registries, the rebalance-deadline + session-timeout `tick`.
- `unified/consumer_ops.rs` — **new.** Next-gen heartbeat/offset-validate/describe + `PendingRecords`/`flush_pending` — moved from `next_gen/group_actor.rs`, operating on `ConsumerState`.
- `unified/classic_ops.rs` — **new.** Classic join/sync/heartbeat/leave logic operating on `ClassicState`, driving the actor's park/wake.
- `unified/persistence.rs` — **new (merge).** One `parse_key` covering versions 0/1/2/3/5/6/7/8; the classic `OffsetCommitValue`/`GroupMetadataValue` encoders (from `coordinator/persistence.rs`) and the next-gen `NextGenKey`/value encoders (from `next_gen/persistence.rs`) moved verbatim.
- `unified/config.rs`, `unified/offsets_log.rs`, `unified/reconciler.rs`, `unified/assignor/**` — **moved unchanged** from `next_gen/`.
- `coordinator/mod.rs` — **shrink.** Re-export `GroupCoordinator`; keep the shared `GroupSnapshot`, `GroupState` (snapshot view), `DeleteGroupError` types the admin handlers use.
- `coordinator/bootstrap.rs` — **rework.** One `replay_records` path feeding `GroupCoordinator`.
- **Deleted:** `coordinator/group.rs`, the whole `coordinator/next_gen/` tree, `GroupManager`, `NextGenCoordinator`, `GroupHandle`, `group_types`, `mark_classic`, `mark_next_gen`.
- Handlers rewired (no signature changes): `join_group.rs`, `sync_group.rs`, `heartbeat.rs`, `leave_group.rs`, `consumer_group_heartbeat.rs`, `consumer_group_describe.rs`, `offset_commit.rs`, `offset_fetch.rs`, `describe_groups.rs`, `list_groups.rs`, `delete_groups.rs`.
- `broker.rs` — construct `GroupCoordinator` in place of `GroupManager` + `set_next_gen` (lines ~48, ~1329-1337); field renamed `group_manager` → `group_coordinator` (or keep the field name to minimize churn — see B5).

Reference facts grounded in the current code (verified during planning):

- Classic `GroupManager`: `coordinator/mod.rs:73`; `GroupHandle { state: Mutex<Group>, join_complete: Notify, sync_complete: Notify }` at `:54`; per-group expiry ticker spawned in `new()` at `:199`.
- Classic `Group`: `coordinator/group.rs:166` (5-state `GroupState` at `:9`; `Member` at `:28`; `OffsetEntry` at `:154`).
- Classic parking: JoinGroup `tokio::time::timeout(wait, handle.join_complete.notified())` (`join_group.rs:271`); `INITIAL_REBALANCE_DELAY = 3s`; `all_members_joined_this_round()` → `notify_waiters()` (`:219`); static-rejoin-to-Stable fast path (`:226`). SyncGroup follower `FOLLOWER_WAIT = 30s` (`sync_group.rs:98`); leader `install_assignments` → `sync_complete.notify_waiters()` (`:93-95`).
- Next-gen `NextGenCoordinator`: `next_gen/mod.rs:27`; `groups: DashMap<_, Arc<GroupActorHandle>>` at `:31`; `group_types: DashMap<_, GroupType>` at `:33`; `get_or_create` (respawn-on-dead) at `:89`; `finalize_bootstrap` at `:236`; `GroupSeed` at `:303`.
- Next-gen actor: `next_gen/group_actor.rs:27` (`GroupActorMessage`), loop at `:95`, `handle_heartbeat` at `:262`, `PendingRecords` at `:590`, `flush_pending` at `:834`.
- Next-gen state: `next_gen/group_state.rs:98` (`GroupState`), `MemberState` at `:14`, `TargetAssignment` at `:92`.
- Persistence: classic `coordinator/persistence.rs:35` (`parse_key`, versions 0/1/2); next-gen `next_gen/persistence.rs:31` (`parse_key`, versions 3/5/6/7/8) and `:16-20` key constants.
- Broker wiring: `broker.rs:48` (`group_manager` field), `:1329-1337` (next-gen construction + `set_next_gen`).
- Handler routing today: `offset_commit.rs:63-68` is the only mixed handler (checks `group_type == NextGen`); `offset_fetch`, `describe_groups`, `list_groups`, `delete_groups` are classic-only (next-gen groups are currently invisible to them — preserve that in B5; do **not** newly expose next-gen groups to admin APIs in this slice).
- Regression suites: `crates/broker/tests/{group_protocol_negotiation,static_membership,consumer_group_next_gen,consumer_group_next_gen_persistence,offset_delete,jvm_acceptance,jvm_consumer_group_next_gen}.rs`.

---

## Conventions for every task

- Commit with: `git -c user.name="Matt Stone" -c user.email="matthew.d.stone@gmail.com" commit -m "<msg>"`.
- After each task: `cargo fmt` then `cargo clippy -p crabka-broker --all-targets -- -D warnings`, then the task's named test command. The tree must compile and the named suite must be green before committing.
- **Never edit a test to make it pass.** If a test needs editing, you have introduced a behavior change — stop and reconsider.
- Use `git mv` for file moves so history is preserved.

---

### Task B0: Green baseline

**Files:** none (verification only).

- [ ] Confirm a clean starting point — expect all PASS:
  - `cargo fmt --all --check`
  - `cargo clippy --workspace --all-targets -- -D warnings`
  - `cargo test -p crabka-broker` (the default, non-ignored group suites must be green)
- [ ] If Docker is available, also bank the JVM gate so post-refactor diffs are meaningful:
  - `cargo test -p crabka-broker -- --include-ignored jvm_acceptance`
  - `cargo test -p crabka-broker -- --include-ignored jvm_consumer_group_next_gen`
  - If Docker is **not** available in the environment, record that the JVM gate must be run by CI / a Docker-capable run before the slice is considered complete, and proceed — the non-JVM suites still gate every step.

Do not commit (no changes).

---

### Task B1: `unified/` scaffolding — move infra + merge persistence

**Files:**
- `git mv` `coordinator/next_gen/config.rs` → `coordinator/unified/config.rs`
- `git mv` `coordinator/next_gen/offsets_log.rs` → `coordinator/unified/offsets_log.rs`
- `git mv` `coordinator/next_gen/reconciler.rs` → `coordinator/unified/reconciler.rs`
- `git mv` `coordinator/next_gen/assignor/` → `coordinator/unified/assignor/`
- Create `coordinator/unified/mod.rs` (module wiring only for now).
- Create `coordinator/unified/persistence.rs` by merging `coordinator/persistence.rs` + `next_gen/persistence.rs`.
- `coordinator/mod.rs` — add `pub(crate) mod unified;` and a temporary `pub(crate) use` shim so the still-live `next_gen` module finds the moved infra (deleted in B5).

Steps:

- [ ] Move the four infra files/dirs with `git mv`. Fix their `use super::…` / `use crate::coordinator::next_gen::…` paths to the new `unified` locations. These files have no logic changes.
- [ ] Create `unified/persistence.rs`. Paste the classic `Key`, `OffsetCommitValue`, `GroupMetadataValue`, `MemberMetadata` (from `coordinator/persistence.rs`) **and** the next-gen `NextGenKey`, key constants, and value types (from `next_gen/persistence.rs`) into it. Define one entry point:
  ```rust
  pub(crate) enum Key {
      OffsetCommit { group_id: String, topic: String, partition: i32 }, // v0/1
      GroupMetadata { group_id: String },                               // v2
      NextGen(NextGenKey),                                              // v3/5/6/7/8
  }
  pub(crate) fn parse_key(buf: &[u8]) -> Result<Key, BrokerError> { /* read i16 version, dispatch */ }
  ```
  Keep both `mod tests` blocks (classic key/value round-trips + next-gen key/value round-trips) — they must pass verbatim.
- [ ] Temporarily re-point `coordinator/persistence.rs` and `next_gen/persistence.rs` to re-export from `unified::persistence` (or leave them and have B5 delete them — pick whichever keeps the build green with least churn; the merge is the durable artifact).
- [ ] Build + the merged persistence unit tests:
  - `cargo test -p crabka-broker --lib coordinator::unified::persistence`
  - `cargo clippy -p crabka-broker --all-targets -- -D warnings`
- [ ] Commit: `refactor(coordinator): move next-gen infra under unified/ + merge persistence parse_key`

---

### Task B2: Unified `Group` + `GroupKind` (rehouse both state machines)

**Files:**
- Create `coordinator/unified/group.rs`.
- `git mv` `coordinator/group.rs` content into `unified/group.rs` as `ClassicState` (+ `Member`, classic `GroupState`, `OffsetEntry`, `AddMemberOutcome`).
- Reference (do not yet move) `next_gen/group_state.rs` as `ConsumerState`.

Steps:

- [ ] In `unified/group.rs`, paste today's `coordinator::group::Group` **verbatim**, renamed `ClassicState` (rename the type only; keep every field/method/`mod tests`). Keep `Member`, the classic 5-state `GroupState` (rename to `ClassicGroupState` to avoid colliding with the next-gen `GroupState`), `OffsetEntry`, `AddMemberOutcome`.
- [ ] `git mv coordinator/next_gen/group_state.rs coordinator/unified/consumer_state.rs`; rename its `GroupState` → `ConsumerState` (type rename only); fix `use` paths. Its `mod tests` move with it and must pass unchanged.
- [ ] Define the container:
  ```rust
  // unified/group.rs
  pub(crate) struct Group {
      pub group_id: String,
      pub kind: GroupKind,
      // k0/k1 committed offsets are protocol-agnostic — live here, not in either kind.
      pub committed_offsets: std::collections::HashMap<(String, i32), OffsetEntry>,
  }
  pub(crate) enum GroupKind {
      Classic(ClassicState),
      Consumer(ConsumerState),
  }
  impl Group {
      pub fn new_classic(group_id: String) -> Self { /* ClassicState::new */ }
      pub fn new_consumer(group_id: String) -> Self { /* ConsumerState::new */ }
      pub fn as_classic_mut(&mut self) -> Option<&mut ClassicState> { /* match */ }
      pub fn as_consumer_mut(&mut self) -> Option<&mut ConsumerState> { /* match */ }
  }
  ```
  Note: today `ClassicState`'s `committed_offsets` lives *inside* the classic `Group`. Move that map up to `Group` and update the classic ops to read/write `group.committed_offsets` instead of `self.committed_offsets`. The `ConsumerState` never owned offsets (offset commits for next-gen groups already append to `__consumer_offsets` via the shared `offset_commit` path), so this is purely a relocation of the classic field — confirm `offset_fetch.rs` (reads classic `committed_offsets`) is updated in B5.
- [ ] Build + state-machine unit tests (the moved `mod tests`):
  - `cargo test -p crabka-broker --lib coordinator::unified::group`
  - `cargo test -p crabka-broker --lib coordinator::unified::consumer_state`
  - `cargo clippy -p crabka-broker --all-targets -- -D warnings`
- [ ] Commit: `refactor(coordinator): unified Group/GroupKind rehousing classic + consumer state machines`

---

### Task B3: Actor with the next-gen arms (port the easy protocol first)

**Files:**
- Create `coordinator/unified/actor.rs`.
- Create `coordinator/unified/consumer_ops.rs` (logic moved from `next_gen/group_actor.rs`).
- `coordinator/unified/mod.rs` — add `GroupCoordinator` with the next-gen surface only (classic added in B4).
- Rewire `consumer_group_heartbeat.rs`, `consumer_group_describe.rs`, and the next-gen branch of `offset_commit.rs` to the new coordinator (behind the same broker field for now).

Steps:

- [ ] In `actor.rs`, define `GroupActorHandle { tx: mpsc::Sender<UnifiedMessage>, _task: JoinHandle<()> }` and the `UnifiedMessage` enum from the spec, but **stub the classic arms** (`unreachable!()` / not yet sent) so this task only exercises the next-gen path.
- [ ] Move `next_gen/group_actor.rs`'s heartbeat/offset-validate/describe/seed/shutdown handling and the `PendingRecords`/`flush_pending`/`snapshot_seed` pipeline into `consumer_ops.rs`, operating on `&mut ConsumerState` (now reached via `group.as_consumer_mut()`). The actor loop's `tokio::select!` over `rx.recv()` + session `tick` (`group_actor.rs:95`) moves into `actor.rs`; for a `Consumer`-kind group the tick calls `ConsumerState::evict_expired`.
- [ ] In `unified/mod.rs`, build `GroupCoordinator` with `groups: DashMap<String, Arc<GroupActorHandle>>`, the `config/metadata/offsets_log/seeds/seeds_cache` fields (from `NextGenCoordinator`), `get_or_create_consumer(group_id)` (spawns a `Consumer`-kind actor; respawn-on-dead logic from `next_gen/mod.rs:89`), `find`, and the next-gen `replay_*` + `finalize_bootstrap` methods moved from `next_gen/mod.rs:137-246`.
- [ ] Rewire the three next-gen handlers to call the new coordinator. The type-lock check `group_type(group_id) == Some(Classic)` (today `consumer_group_heartbeat.rs:36`) becomes: `find(group_id)` returns an actor whose kind is `Classic` → reject with `GROUP_ID_NOT_FOUND`; absent or `Consumer` → proceed. Drop `mark_next_gen` (the actor's kind *is* the lock). For `offset_commit.rs`, the `is_next_gen` check (`:63-68`) becomes "the existing actor is `Consumer`-kind".
- [ ] Keep the classic coordinator (`GroupManager`) alive and wired for classic handlers in this task — both coordinators coexist for exactly one task. The broker constructs **both** temporarily; B4/B5 removes the classic one.
- [ ] Tests — next-gen suites must be green:
  - `cargo test -p crabka-broker --test consumer_group_next_gen`
  - `cargo test -p crabka-broker --test consumer_group_next_gen_persistence`
  - `cargo test -p crabka-broker --lib coordinator::unified`
  - If Docker: `cargo test -p crabka-broker -- --include-ignored jvm_consumer_group_next_gen`
  - `cargo clippy -p crabka-broker --all-targets -- -D warnings`
- [ ] Commit: `refactor(coordinator): port next-gen actor onto unified GroupCoordinator`

---

### Task B4: Classic ops + park/wake on the actor (the hard task)

**Files:**
- Create `coordinator/unified/classic_ops.rs`.
- `coordinator/unified/actor.rs` — add the parked-joiner / parked-follower registries, the rebalance-deadline timer, and the classic `UnifiedMessage` arms.
- `coordinator/unified/mod.rs` — add `get_or_create_classic`, classic `list/describe/delete`, classic bootstrap seed.

Steps:

- [ ] Add to the actor's per-group state:
  ```rust
  // Parked classic JoinGroup waiters: member_id -> reply sender.
  parked_joiners: HashMap<String, oneshot::Sender<JoinOutcome>>,
  // Parked classic SyncGroup followers: member_id -> reply sender.
  parked_followers: HashMap<String, oneshot::Sender<SyncOutcome>>,
  // When Some, the rebalance completes (drains parked_joiners) at this instant.
  rebalance_deadline: Option<Instant>,
  ```
  Extend the actor `tick` to also fire when `rebalance_deadline` elapses → run the same completion the handler runs today (`select_protocol` → `resolve_selected_protocol_metadata` → `complete_rebalance`, `join_group.rs:280-284`) and drain `parked_joiners`, `send`ing each its `JoinOutcome`. Use `tokio::time::sleep_until(deadline)` inside the `select!` (or shorten the tick) so completion is timely.
- [ ] `classic_ops.rs::handle_join(state: &mut ClassicState, committed: &mut .., req, host) -> JoinDisposition` ports `join_group.rs`:
  - `add_member` (`:186`), the `all_members_joined_this_round()` early-complete (`:219`), and the static-rejoin-to-Stable fast path (`:226-253`).
  - Returns `JoinDisposition::Immediate(resp)` (fast paths / errors) or `JoinDisposition::Park` (register the `oneshot::Sender` in `parked_joiners`; set `rebalance_deadline` to `now + min(rebalance_timeout, INITIAL_REBALANCE_DELAY=3s)` if unset).
- [ ] `classic_ops.rs::handle_sync` ports `sync_group.rs`:
  - Validate (member/generation/instance fence, `:47-82`). Leader → `install_assignments` (`:93`) then drain `parked_followers` with the freshly-installed assignments and reply `Immediate` to the leader. Follower → if `Stable`, reply `Immediate` with the stored assignment; else `Park` in `parked_followers`.
- [ ] `handle_heartbeat` (classic) ports `heartbeat.rs`: membership + instance check, bump `last_heartbeat`, return error code. `handle_leave` ports `leave_group.rs`: `remove_member` then, if a rebalance is pending, complete-or-redeadline and drain `parked_joiners` as needed (today: `leave_group.rs` → `join_complete.notify_waiters()`).
- [ ] **Crucially, the handler keeps the timeout.** The handler sends `ClassicJoin { reply }`, then `match tokio::time::timeout(wait, reply_rx).await { Ok(Ok(outcome)) => …, _ => /* deadline path: read back current state via a follow-up Describe-style message or return REBALANCE_IN_PROGRESS exactly as today */ }`. Preserve the exact `wait` computation from `join_group.rs:271` and `FOLLOWER_WAIT=30s` from `sync_group.rs:98`. The actor's `rebalance_deadline` and the handler's `timeout` are belt-and-suspenders, identical to today where the deadline (handler) and the `notify` (state) both exist.
- [ ] Session expiry: the actor `tick` for a `Classic`-kind group calls `ClassicState::expire_dead_members` (`group.rs:374`) and, if any dropped, completes/redeadlines the rebalance and drains parked joiners — replacing the `GroupManager` background ticker (`mod.rs:199`).
- [ ] `unified/mod.rs`: `get_or_create_classic` spawns a `Classic`-kind actor; `list_groups`/`describe_group`/`delete_group` iterate the registry and (for `describe`) send a classic-aware `Describe` message (extend `DescribeView` or add a `ClassicDescribe` arm returning today's `GroupSnapshot` shape).
- [ ] Rewire classic handlers (`join_group`, `sync_group`, `heartbeat`, `leave_group`) to the unified coordinator. Drop `mark_classic` (creating a `Classic` actor *is* the lock; a `ConsumerGroupHeartbeat` hitting a `Classic`-kind actor is rejected, and vice-versa, matching today).
- [ ] Tests — classic suites must be green, **unmodified**:
  - `cargo test -p crabka-broker --test group_protocol_negotiation`
  - `cargo test -p crabka-broker --test static_membership`
  - `cargo test -p crabka-broker --test offset_delete`
  - If Docker: `cargo test -p crabka-broker -- --include-ignored jvm_acceptance`
  - `cargo clippy -p crabka-broker --all-targets -- -D warnings`
- [ ] Commit: `refactor(coordinator): port classic join/sync parking onto unified actor`

---

### Task B5: Single coordinator, reworked bootstrap, delete the old subsystems

**Files:**
- `coordinator/bootstrap.rs` — rework to one replay path.
- `coordinator/mod.rs` — shrink to re-exports + shared admin types.
- `broker.rs` — construct one `GroupCoordinator`.
- Rewire remaining handlers: `offset_commit.rs`, `offset_fetch.rs`, `describe_groups.rs`, `list_groups.rs`, `delete_groups.rs`.
- **Delete:** `coordinator/group.rs`, `coordinator/next_gen/` (all), the temporary persistence re-export shims, `GroupManager`, `GroupHandle`, `NextGenCoordinator`, `GroupType`, `group_types`, `mark_classic`, `mark_next_gen`.

Steps:

- [ ] `bootstrap.rs::replay_records`: for each record, `unified::persistence::parse_key`:
  - `Key::OffsetCommit` → `coordinator.get_or_create_*` for the group (offsets are kind-agnostic; if the group actor does not yet exist, the offset record alone does not fix a kind — buffer offsets in the seed accumulator and attach when the kind is decided, OR default to creating nothing and let the first GroupMetadata/next-gen record pick the kind; match today's behavior where classic `OffsetCommit` replay does `get_or_create` a classic-shaped group — preserve that: an `OffsetCommit`-only group replays Classic, as today via `bootstrap.rs:151-171`).
  - `Key::GroupMetadata` (v2) → seed/create a **Classic** actor and apply `apply_group_metadata` (`bootstrap.rs:259`).
  - `Key::NextGen(_)` → feed the existing `replay_*` seed accumulators; `finalize_bootstrap` spawns **Consumer** actors.
  - Tombstones (`None` value): classic offset/group tombstones honored against the classic seed; next-gen tombstones via the moved `replay_next_gen_tombstone`.
- [ ] `broker.rs`: replace the `GroupManager::new()` + `next_gen` construction (`:1329-1337`) with one `GroupCoordinator::new(config.next_gen_consumer_group.clone(), ImageMetadataProvider{..}, offsets_log)`. Keep the broker field name `group_manager` → rename to `group_coordinator` and update all handler call sites (a mechanical rename; do it here so the diff is one task).
- [ ] Rewire the five remaining handlers. `offset_commit.rs`: collapse the `is_next_gen` fork (`:63-104`) — `find(group_id)` once, branch on the actor kind (`Consumer` → `OffsetValidate` message; `Classic` → classic `validate`), shared append path unchanged (`:238-244`). `offset_fetch.rs`: read committed offsets — now on `Group.committed_offsets` for classic; next-gen groups stay as today (not served here). `describe_groups`/`list_groups`/`delete_groups`: call the unified `describe_group`/`list_groups`/`delete_group`. **Preserve today's visibility**: these admin APIs currently surface classic groups only; keep that (do not newly list next-gen groups in this slice — that is a separate, intentional change, not part of a behavior-preserving port). If the unified `list_groups` would now include consumer groups, filter to `Classic`-kind to hold behavior; leave a `// TODO(64d-C+): surface consumer groups in admin APIs` marker.
- [ ] Delete the old files/types. Then prove they are gone:
  - `rg -n "GroupManager|NextGenCoordinator|GroupHandle|mark_classic|mark_next_gen|group_types" crates/broker/src` → **no hits**.
  - `test ! -e crates/broker/src/coordinator/group.rs && test ! -d crates/broker/src/coordinator/next_gen` → exits 0.
- [ ] Full build + workspace tests:
  - `cargo test --workspace`
  - `cargo clippy --workspace --all-targets -- -D warnings`
- [ ] Commit: `refactor(coordinator): single GroupCoordinator + unified bootstrap; delete classic/next-gen subsystems`

---

### Task B6: Verification gate + docs

**Files:** `STATUS.md`, `README.md`, the roadmap design doc.

- [ ] Full gate — expect all PASS:
  - `cargo fmt --all --check`
  - `cargo clippy --workspace --all-targets -- -D warnings`
  - `cargo test --workspace`
  - `cargo test --workspace -- --include-ignored` (JVM gate; requires Docker — if unavailable locally, ensure CI runs it before merge and note that on the PR).
- [ ] Confirm acceptance items 5–7 from the design spec: the `rg` no-hits check, the deleted-files check, and no diff to any `__consumer_offsets` encoder or wire snapshot (`cargo test -p crabka-protocol` snapshots unchanged; the persistence round-trip tests unchanged).
- [ ] Add a `## Slice 64d-B — Unified GroupCoordinator skeleton (2026-05-30)` entry to `STATUS.md` summarizing: two coordinators merged into one actor-per-group `GroupCoordinator`; classic parking re-expressed as a park/wake message protocol; state machines moved verbatim under `GroupKind`; behavior-preserving (all suites unmodified); single persistence path. List out-of-scope: live migration/policy/mixed membership (Slices C–E).
- [ ] In the migration roadmap design doc (`2026-05-29-crabka-classic-nextgen-migration-roadmap-design.md`), no edit needed to the table, but if a "pending slices" list is maintained elsewhere, mark B done and C next.
- [ ] README: the KIP-848 row stays `⚠️` (migration still pending); optionally note "unified coordinator" in the feature description. No status flip — migration (C–F) is what moves it.
- [ ] Commit: `docs(coordinator): STATUS + README for unified GroupCoordinator (64d-B)`.
- [ ] Push the branch and open a PR titled `KIP-848 64d-B: unified GroupCoordinator skeleton`. PR body: link the design spec, list the suites that gate it, and call out that the JVM gate must be green in CI. Ask the reviewer to confirm the classic parking port (B4) against the `jvm_acceptance` static-membership and cooperative-sticky cases specifically.

---

## Notes for the implementer

- **B3 before B4 is deliberate.** The next-gen path is already an actor; landing
  it first means B4's harder classic-parking port is the only suspect if a
  regression appears between B3 and B4.
- **The park/wake protocol is the crux.** Keep the *timeout* in the handler
  (unchanged durations) and use the actor's `rebalance_deadline` only to *drive
  completion*, exactly as today the handler's `timeout` and the state's `Notify`
  coexist. Do not move timeout durations into the actor — that would risk
  silently changing parking behavior the JVM tests depend on.
- **No test edits.** A required test edit means a behavior change; the slice is
  defined as behavior-preserving. Stop and reconsider the port instead.
- **Greenfield (per `CLAUDE.md`):** delete the old coordinators outright; do not
  leave `#[deprecated]` shells, `V2` enum variants, or compat fallbacks. Wipe
  local raft/data dirs if a stale `__consumer_offsets` confuses a manual run.
