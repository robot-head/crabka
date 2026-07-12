# G8 Split Crash-Anywhere Matrix Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build an exhaustive, fail-closed, real-process crash matrix for two-successor Split across every durable side-effect, receipt, journal, tenant, retirement, and resume transition.

**Architecture:** A dedicated integration-test binary owns the Split crash model, continuous two-table workload, production reconciliation driver, and terminal verifier. Three shell shards enumerate literal case sets and a standalone validator rejects missing or inconsistent evidence. Existing production code is exercised through the real CLI, broker, mTLS control transport, registry, and topic admin without crash hooks.

**Tech Stack:** Rust/Tokio integration tests, `crabka-gres-control`, `crabka-gres-ranges`, `crabka-operator`, real broker process harness, JSON Lines, Bash, Python 3 evidence validation, Stateright model checking.

## Global Constraints

- No production test-only crash hooks and no remote operations.
- Every case uses unique tenant, operation, sentinel-topic, and evidence identities.
- Every ledger record includes table identity, physical rowid where acknowledged, sequence, checksum, kind, and fsynced wall-clock timestamp.
- The authoritative oracle is parsed only after closing and reopening the fsynced ledger.
- The pause bound is elapsed wall-clock time between fsynced ACK timestamps spanning pause/kill/restart.
- Post-publication writes must prove table50 ownership on r2 and table51 rowids at or above 16 on r3.
- r2 and r3 endpoints remain distinct and both sealed generations equal 1.
- Preserve and exclude unrelated `crates/gres-ranges/src/control.rs` formatting changes.
- Each task ends in a focused commit and a reviewable green test gate.

---

### Task 1: Exhaustive Split kill-point model and predicate truth table

**Files:**
- Create: `crates/gres/tests/topology_process_split_crash.rs`
- Modify: `crates/gres/Cargo.toml`

**Interfaces:**
- Produces: `enum SplitKillPoint`, `SplitPredicateState`, `SplitKillPoint::ALL`, `family()`, `name()`, `is_ready()`, `restart_hosted_ranges()`, `pause_bound_ms()`, and `operation_bound_ms()`.
- Consumes later: Tasks 3-6 use the exact 20-case enum and family membership.

- [ ] **Step 1: Add a failing exhaustive-name and family test**

```rust
#[test]
fn split_kill_points_are_exhaustive_unique_and_sharded() {
    let names = SplitKillPoint::ALL.map(SplitKillPoint::name);
    assert_eq!(names.len(), 20);
    assert_eq!(names.into_iter().collect::<BTreeSet<_>>().len(), 20);
    assert_eq!(SplitKillPoint::ALL.iter().filter(|p| p.family() == Family::SourceRestore).count(), 11);
    assert_eq!(SplitKillPoint::ALL.iter().filter(|p| p.family() == Family::Publication).count(), 3);
    assert_eq!(SplitKillPoint::ALL.iter().filter(|p| p.family() == Family::RetirementResume).count(), 6);
}
```

- [ ] **Step 2: Run the test and verify RED**

Run: `cargo test -p crabka-gres --test topology_process_split_crash split_kill_points_are_exhaustive_unique_and_sharded`

Expected: compile failure because `SplitKillPoint` is undefined.

- [ ] **Step 3: Implement the exact enum and family mapping**

```rust
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum SplitKillPoint {
    InitiatedBeforeRunningCas,
    CheckpointReceiptBeforeJournalCas,
    CheckpointedAfterJournalCas,
    PauseReceiptBeforeJournalCas,
    PausedBeforeStage,
    StageReceiptBeforeJournalCas,
    StagedAfterJournalCas,
    MarkerClaimReceiptBeforeJournalCas,
    RestoredAfterJournalCas,
    PrologueReceiptBeforeJournalCas,
    ActivatedAfterJournalCas,
    TenantCasBeforeJournalCas,
    LayoutPublishedAfterJournalCas,
    RetiringAfterJournalCas,
    RetiringBeforeDelete,
    DeleteSuccessBeforeSidecarCas,
    ParkedAfterSidecarCas,
    RetireReceiptBeforeJournalCas,
    ResumingAfterJournalCas,
    CompletedAfterJournalCas,
}
```

Implement `ALL` in this order and exact snake-case names from the design spec. Implement `is_ready` as a total match over `SplitPredicateState`; every arm must assert the precise journal phase plus required receipt, evidence, layout, sidecar, topic, successor-status, and delete-count shape. No wildcard arm is allowed.

- [ ] **Step 4: Add table-driven positive and near-miss predicate tests**

For every `SplitKillPoint`, construct one exact accepted state and then mutate each required field independently (phase, evidence, receipt, layout, sidecar, topic, delete count, target status) and assert rejection. Include explicit ambiguity cases where checkpoint/pause/stage/marker/prologue/retire receipts exist before the journal CAS.

- [ ] **Step 5: Run focused tests and verify GREEN**

Run: `cargo test -p crabka-gres --test topology_process_split_crash split_kill_point -- --nocapture`

Expected: all enum, family, parsing, bounds, and predicate truth-table tests pass.

- [ ] **Step 6: Commit**

```bash
git add crates/gres/Cargo.toml crates/gres/tests/topology_process_split_crash.rs
git commit -m "test(gres): model exhaustive split crash points"
```

### Task 2: Continuous two-table fsynced payload ledger

**Files:**
- Modify: `crates/gres/tests/topology_process_split_crash.rs`

**Interfaces:**
- Produces: `PayloadEvent`, `PayloadLedger`, `WorkloadChild`, `spawn_split_workload`, `parse_closed_payload_ledger`, `ack_gap_ms`, and per-table physical ownership projections.
- Consumes: `ProcessHarness` SQL endpoint and a stop-file path.

- [ ] **Step 1: Write RED parser tests for attempt/ack/recovered_ack and corruption**

```rust
#[test]
fn closed_payload_ledger_is_exact_and_fail_closed() {
    let parsed = parse_payload_ledger(
        r#"{"kind":"attempt","table_id":50,"rowid":null,"seq":1,"checksum":"a","timestamp_ms":10}
{"kind":"ack","table_id":50,"rowid":7,"seq":1,"checksum":"a","timestamp_ms":12}
{"kind":"recovered_ack","table_id":51,"rowid":16,"seq":2,"checksum":"b","timestamp_ms":20}
"#,
    ).unwrap();
    assert_eq!(parsed.acknowledged.len(), 2);
    assert!(parse_payload_ledger("{\"kind\":\"ack\"}").is_err());
}
```

- [ ] **Step 2: Run parser tests and verify RED**

Run: `cargo test -p crabka-gres --test topology_process_split_crash payload_ledger`

Expected: compile failure because parser/types are undefined.

- [ ] **Step 3: Implement strict ledger types and closed-file readback**

```rust
#[derive(Debug, Deserialize, Serialize)]
struct PayloadEvent {
    kind: PayloadKind,
    table_id: u64,
    rowid: Option<u64>,
    seq: u64,
    checksum: String,
    timestamp_ms: u128,
}

fn parse_closed_payload_ledger(path: &Path) -> Result<PayloadLedger, String> {
    std::fs::read_to_string(path)
        .map_err(|e| e.to_string())
        .and_then(|body| parse_payload_ledger(&body))
}
```

Reject duplicate ACK identities, ACKs without attempts, checksum mismatches, timestamp regression, recovered ACKs without a prior ambiguous attempt, unknown kinds, missing fields, and duplicate `(table_id,seq)` keys.

- [ ] **Step 4: Implement the child workload and cleanup contract**

Spawn the current test binary in workload mode. Alternate table50 and table51 attempts; write and `sync_data` the attempt before SQL, then write and `sync_data` either ACK or recovered ACK after querying the exact checksum. After target publication require successful writes in both streams. `WorkloadChild::stop` creates the stop file, waits with a deadline, kills the process group on timeout, and asserts the group no longer exists.

- [ ] **Step 5: Add wall-clock pause and sealed-interval projection tests**

Compute the maximum `later.timestamp_ms - earlier.timestamp_ms` over consecutive reopened ACK records. Project table50 qualifying physical keys to r2 and table51 keys with `rowid >= 16` to r3; reject any acknowledged key outside the sealed plan interval.

- [ ] **Step 6: Run focused tests and commit**

Run: `cargo test -p crabka-gres --test topology_process_split_crash payload_ledger -- --nocapture`

```bash
git add crates/gres/tests/topology_process_split_crash.rs
git commit -m "test(gres): add continuous split payload ledger"
```

### Task 3: Authenticated receipt observation and one-kill production driver

**Files:**
- Modify: `crates/gres/tests/topology_process_split_crash.rs`

**Interfaces:**
- Produces: `ControlObservation`, `RecordingRangeMutationClient`, `SplitCrashObservation`, `probe_completed_receipt`, and `drive_split_with_one_kill`.
- Consumes: production `MtlsRangeMutationClient`, `Registry`, operator reconciliation functions, `CountingRetirementAdmin`, and Task 1 predicates.

- [ ] **Step 1: Add RED tests for receipt classification and exact request identity**

Create fixture requests for all control operations and assert classification records endpoint, tenant, range, generation, operation ID, journal revision/digest, response, and replay count. Assert a changed request digest is rejected rather than recorded as a completed receipt.

- [ ] **Step 2: Run and verify RED**

Run: `cargo test -p crabka-gres --test topology_process_split_crash control_observation`

Expected: compile failure because the recording client is undefined.

- [ ] **Step 3: Implement transparent recording around production mTLS**

```rust
#[async_trait]
impl RangeMutationClient for RecordingRangeMutationClient {
    async fn mutate(&self, endpoint: &str, request: RangeControlReq)
        -> Result<RangeControlResp, SplitReconcileError>
    {
        let response = self.inner.mutate(endpoint, request.clone()).await?;
        self.observations.lock().await.push(ControlObservation {
            endpoint: endpoint.to_owned(),
            request,
            response: response.clone(),
            timestamp_ms: timestamp_ms(),
        });
        Ok(response)
    }
}
```

Only authenticated production responses are recorded. Receipt probes replay the exact sealed request derived from the journal and must receive the durable completed response.

- [ ] **Step 4: Implement the production driver and external predicate loop**

Start the real harness, create filler tables plus table50/table51 schemas, spawn Task 2 workload, invoke the actual CLI once, and loop over durable state. Before each reconcile, build `SplitPredicateState` from registry, tenant, exact receipt probes, r2/r3 authenticated status, topic metadata, marker captures, and counting-admin ledger. When the selected predicate first becomes true: wait for eight fsynced ACKs, record timestamps/PID, SIGKILL source, verify process-group death, restart with the point's hosted ranges, reconstruct registry/mTLS/admin objects, and continue. Assert exactly one kill.

- [ ] **Step 5: Inject post-side-effect ambiguity without crash hooks**

Use existing durable receipt replay and registry/admin ambiguity facilities: allow completed control receipts to precede journal CAS for checkpoint/pause/stage/marker/prologue/retire; allow tenant replacement to commit before its acknowledgement; allow topic deletion to succeed before returning an injected admin error. Predicates observe the durable result externally before SIGKILL.

- [ ] **Step 6: Add driver model tests and commit**

Run: `cargo test -p crabka-gres --test topology_process_split_crash split_driver -- --nocapture`

```bash
git add crates/gres/tests/topology_process_split_crash.rs
git commit -m "test(gres): drive split crashes at durable boundaries"
```

### Task 4: Exact post-restart verifier and measured evidence

**Files:**
- Modify: `crates/gres/tests/topology_process_split_crash.rs`

**Interfaces:**
- Produces: `SplitCrashEvidence`, `verify_completed_split_case`, and exact JSON serialization.
- Consumes: closed Task 2 ledger, Task 3 observations, completed journal/tenant, direct scan responses, SQL, broker metadata, and process ledger.

- [ ] **Step 1: Write RED tests that reject each false terminal invariant**

Table-drive mutations for wrong phase/layout/version, equal endpoints, wrong generations, marker overlap/union/digest, r2/r3 cross-side rows, missing/duplicate payload, absent post-publication stream, excessive ACK pause, predecessor topic present, successor/sentinel topic absent, delete count not one, unrelated delete attempt, unchanged PID, or live process group.

- [ ] **Step 2: Run and verify RED**

Run: `cargo test -p crabka-gres --test topology_process_split_crash completed_split_verifier`

- [ ] **Step 3: Implement exact marker and physical ownership checks**

Require exactly one canonical marker receipt identity (replays may repeat the identical response), explicit left/right partitions, ordered exact union, disjointness, interval membership, and digest equality with journal and retirement checkpoint. Decode every direct tuple and compare `(table_id,rowid,seq,checksum)` to the reopened oracle. Verify table50 qualifying rows only on r2 and table51 `rowid >= 16` only on r3.

- [ ] **Step 4: Implement SQL union, topic, timing, and cleanup checks**

Open a fresh SQL connection after restart and compare exact sorted payloads. Require pre-kill, post-restart, post-publication-r2, and post-publication-r3 ACK timestamps; compute the wall-clock ACK gap and compare it to `pause_bound_ms`. Require exact one predecessor delete, r1 absence, r0/r2/r3 and sentinel presence, distinct old/new PIDs, stopped workload group, and total duration below `operation_bound_ms`.

- [ ] **Step 5: Serialize only measured evidence**

Every boolean is computed from captured values immediately before serialization. Include `schema_version`, family/case, unique IDs, pre-kill predicate snapshot, all receipt identities/replay counts, marker counts/digest, per-table/per-successor rows, ACK kinds/timestamps/gap/bound, journal/tenant/retirement versions, topic set/delete ledger, PIDs/process cleanup, and elapsed/bound fields.

- [ ] **Step 6: Run focused tests and commit**

Run: `cargo test -p crabka-gres --test topology_process_split_crash completed_split_verifier -- --nocapture`

```bash
git add crates/gres/tests/topology_process_split_crash.rs
git commit -m "test(gres): verify exact split crash recovery"
```

### Task 5: Fail-closed evidence validator and three CI shards

**Files:**
- Create: `scripts/tests/validate-gres-split-crash-evidence.py`
- Create: `scripts/tests/gres-topology-process-split-source-restore-ci.sh`
- Create: `scripts/tests/gres-topology-process-split-publication-ci.sh`
- Create: `scripts/tests/gres-topology-process-split-retirement-ci.sh`
- Modify: `crates/gres/tests/topology_process_split_crash.rs`

**Interfaces:**
- Produces: `--validate-family FAMILY DIRECTORY` and `--validate-file FAMILY CASE FILE` validator modes.
- Consumes: Task 4 schema version 1 JSON.

- [ ] **Step 1: Write RED validator self-tests**

Invoke the validator with empty input, incomplete JSON, wrong family/case, duplicate identity, missing expected case, extra case, false invariant, wrong count, wrong bound, and malformed marker partition. Each command must exit nonzero. A complete synthetic 11/3/6 family fixture must exit zero.

- [ ] **Step 2: Implement literal expected sets and strict schema checks**

```python
EXPECTED = {
    "source_restore": [
        "initiated_before_running_cas", "checkpoint_receipt_before_journal_cas",
        "checkpointed_after_journal_cas", "pause_receipt_before_journal_cas",
        "paused_before_stage", "stage_receipt_before_journal_cas",
        "staged_after_journal_cas", "marker_claim_receipt_before_journal_cas",
        "restored_after_journal_cas", "prologue_receipt_before_journal_cas",
        "activated_after_journal_cas",
    ],
    "publication": ["tenant_cas_before_journal_cas", "layout_published_after_journal_cas", "retiring_after_journal_cas"],
    "retirement_resume": ["retiring_before_delete", "delete_success_before_sidecar_cas", "parked_after_sidecar_cas", "retire_receipt_before_journal_cas", "resuming_after_journal_cas", "completed_after_journal_cas"],
}
```

Require exact keys/types, unique tenant/operation identities, exact expected file set, computed count arithmetic, distinct endpoints, generation 1, both post-publication streams, positive bounded timings, exact marker union, exact topic set, one deletion, and process cleanup.

- [ ] **Step 3: Implement family scripts**

Each script builds CLI/GRES once with `--locked`, removes and recreates its target evidence directory, loops over its literal cases, gives each invocation `CRABKA_G8_SPLIT_CRASH=1`, case name, unique evidence path, and `timeout 240s`, then validates the full directory. Use `--exact real_process_split_crash_anywhere --nocapture` so each case is independently reproducible.

- [ ] **Step 4: Run negative and synthetic-positive validator gates**

Run all validator self-tests. Expected: every negative returns nonzero and complete synthetic families return zero.

- [ ] **Step 5: Run one smoke case from each shard**

Run `checkpoint_receipt_before_journal_cas`, `tenant_cas_before_journal_cas`, and `delete_success_before_sidecar_cas`. Expected: each real broker/mTLS/CLI case passes and its single-file validation succeeds.

- [ ] **Step 6: Commit**

```bash
git add crates/gres/tests/topology_process_split_crash.rs scripts/tests/validate-gres-split-crash-evidence.py scripts/tests/gres-topology-process-split-source-restore-ci.sh scripts/tests/gres-topology-process-split-publication-ci.sh scripts/tests/gres-topology-process-split-retirement-ci.sh
git commit -m "test(gres): shard exhaustive split crash evidence"
```

### Task 6: Full matrix, regression gates, published evidence, and independent review

**Files:**
- Create: `docs/superpowers/evidence/2026-07-12-gres-g8-split-crash-matrix.md`
- Modify only if a verified defect is found: files from Tasks 1-5

**Interfaces:**
- Produces: reproducible command/result record and final reviewed commits.
- Consumes: all prior tasks.

- [ ] **Step 1: Run all three real-process shards**

```bash
scripts/tests/gres-topology-process-split-source-restore-ci.sh
scripts/tests/gres-topology-process-split-publication-ci.sh
scripts/tests/gres-topology-process-split-retirement-ci.sh
```

Expected: 11, 3, and 6 unique cases pass; every family validator accepts its exact evidence set.

- [ ] **Step 2: Run existing Split Stateright exhaustive gate**

Run: `cargo test --locked -p crabka-gres-ranges --test split_model -- --nocapture`

Expected: the repository's exhaustive Split Stateright model passes with no counterexample. Record the command and explored-state result verbatim in the evidence document.

- [ ] **Step 3: Run Move and focused compatibility regressions**

```bash
scripts/tests/gres-topology-process-nemesis-ci.sh
cargo test --locked -p crabka-gres-ranges transport::tests --lib
cargo test --locked -p crabka-operator controller::gres_split_operation --lib
cargo check --locked -p crabka-operator
cargo test --locked -p crabka-gres --test topology_process_split_crash
```

Expected: all pass; legacy marker receipt decode remains green.

- [ ] **Step 4: Run formatting and diff integrity gates**

```bash
cargo fmt --all -- --check
git diff --check
```

Stage only semantic hunks. Confirm `git diff --cached -- crates/gres-ranges/src/control.rs` contains no unrelated formatting/replay-test hunks.

- [ ] **Step 5: Publish measured evidence summary**

Document the exact commands, case counts, elapsed ranges, maximum measured ACK gaps, unique identity count, marker evidence, physical table50/r2 and table51/r3 proof, topic/delete proof, Stateright result, Move regression, and validator negative gates. Do not copy success literals; summarize generated evidence values.

- [ ] **Step 6: Commit and request independent review**

```bash
git add docs/superpowers/evidence/2026-07-12-gres-g8-split-crash-matrix.md
git commit -m "docs(gres): publish split crash matrix evidence"
```

Request a read-only independent review covering exhaustive graph-to-case mapping, external predicates, workload oracle, authenticated receipt ambiguity, exact successor ownership, validators, sharding, regressions, and unrelated-dirt preservation. Resolve every actionable finding in a follow-up commit and rerun affected gates before declaring READY.
