# GRES G8 Two-Successor Split Foundation Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Prove a real no-kill two-successor Split end to end with distinct r2/r3 endpoints, ownership, markers, WAL topics, and an exact live ACK ledger.

**Architecture:** Extend the existing process harness with two stable successor proxies backed by the same real child, then add a Split-specific production driver and observational ownership verifier to the topology process nemesis. Keep the no-kill foundation separate from later kill-point parameterization and validate it through a dedicated evidence script.

**Tech Stack:** Rust, Tokio, PostgreSQL wire client, Crabka framed mTLS range RPC, Kafka-compatible broker/admin client, shell CI, Python JSON validation.

## Global Constraints

- Use one real child hosting `r0,r1` before cutover and `r0,r2,r3` after target recovery.
- r2 and r3 must have distinct stable proxy endpoints, range IDs, generations, owners, and WAL topics.
- Ownership must be observed through authenticated direct range scans, not inferred from the registry layout.
- Use globally unique timestamp-and-PID tenant and operation IDs and prove the operation is absent before CLI initiation.
- Do not add Split kill-point parameterization until the no-kill foundation, evidence validator, commit, and independent review are green.
- Perform no remote Git operations.

---

### Task 1: Stable r2/r3 process-harness proxies

**Files:**
- Modify: `crates/gres-ranges/tests/harness/process.rs`
- Test: `crates/gres/tests/topology_process_nemesis.rs`

**Interfaces:**
- Produces: `ProcessHarness::split_successor_endpoints() -> [String; 2]` and restart support for `r0,r2,r3` that retargets both proxies to the current child range listener.
- Consumes: existing `RangeProxy`, `spawn_node`, `restart_with_hosted_ranges`, TLS, and shutdown ownership.

- [ ] **Step 1: Write a failing harness contract test**

Add a focused test that starts all ranges on zero, obtains two non-equal r2/r3 endpoints, restarts range zero with `r0,r2,r3`, and sends authenticated Status requests for `(r2,g1)` and `(r3,g1)` through those exact endpoints.

- [ ] **Step 2: Verify RED**

Run: `cargo test -p crabka-gres --test topology_process_nemesis split_successor_proxies_are_distinct_and_retargeted -- --nocapture`

Expected: compile failure because `split_successor_endpoints` and r2/r3 proxy storage do not exist.

- [ ] **Step 3: Implement the minimal harness surface**

Add harness-owned `r2_proxy` and `r3_proxy`, start them in both harness constructors, expose their endpoints, map `proxy(2)`/`proxy(3)`, and retarget them to `self.r0.range_port` after every range-zero restart. Preserve existing r0/r1 behavior and cleanup.

- [ ] **Step 4: Verify GREEN and regressions**

Run the focused test and `cargo test -p crabka-gres --test topology_process_nemesis retirement_restart_uses_authoritative_target_ranges -- --nocapture`.

Expected: both pass.

- [ ] **Step 5: Commit**

Commit message: `test(gres): add stable split successor proxies`

### Task 2: Real Split CLI and production no-kill driver

**Files:**
- Modify: `crates/gres/tests/topology_process_nemesis.rs`

**Interfaces:**
- Consumes: `split_successor_endpoints`, `reconcile_activated_cutover`, `reconcile_one_rpc_phase`, `reconcile_one_retiring_range_wal`, and the counting retirement admin.
- Produces: `initiate_split_with_cli`, a Split foundation driver that completes the actual sealed operation, and final target record `r0/r2/r3`.

- [ ] **Step 1: Write the failing real-process foundation test**

Add `real_process_split_two_successor_foundation` behind `CRABKA_G8_SPLIT_FOUNDATION=1`. Use a unique `g8-split-<hex timestamp>-p<hex pid>` identity, assert no prior operation, create the unrelated sentinel, and invoke actual CLI arguments `gres split --left-range-id 2 --successor-range-id 3 --left-endpoint <r2> --successor-endpoint <r3> --successor-wal-generation 1` at a fixed table/row boundary.

- [ ] **Step 2: Verify RED**

Run with the environment gate and a 180-second shell timeout.

Expected: failure at missing Split driver/final `r0,r2,r3` assertions while the existing Move test remains unchanged.

- [ ] **Step 3: Implement the production driver path**

Generalize only the operation-independent reconciliation loop needed by the foundation. Drive `Initiated` through `Completed`; require target readiness for both descriptors; keep the exact counting retirement admin; and assert the sidecar reaches `Parked`, the tenant target layout is exactly r0/r2/r3, and the predecessor owner is retired.

- [ ] **Step 4: Verify the operation foundation**

Run the gated foundation test.

Expected: `Completed`, exact target layout, both successor Status requests serving, one predecessor delete, and no process leak.

- [ ] **Step 5: Commit**

Commit message: `test(gres): drive real two-successor split`

### Task 3: Exact ACK ledger, ownership scans, markers, and topics

**Files:**
- Modify: `crates/gres/tests/topology_process_nemesis.rs`

**Interfaces:**
- Consumes: `FramedTcpClient::call`, `RangeRequest::ScanRange`, `ScanRangeReq`, the Split target descriptors, ACK-ledger parsing, admin metadata, and canonical marker digest utilities.
- Produces: per-successor ownership evidence and an exact full-ledger proof.

- [ ] **Step 1: Add failing ownership and marker assertions**

Create a live SHARDED ledger with `(seq, route_key, checksum)` and alternate acknowledged writes below and above the sealed boundary. Add direct authenticated `ScanRange` calls to r2 and r3 and initially assert the exact low/high partitions, disjoint marker partitions, canonical union digest, and final topic set.

- [ ] **Step 2: Verify RED**

Run the gated foundation.

Expected: failure until scan request construction/row decoding and marker partition collection are wired to the exact successor descriptors.

- [ ] **Step 3: Implement exact observational proofs**

Decode direct scan responses into ordered rows; assert r2 contains only low-side ACKed rows and r3 only high-side rows. Open a fresh SQL client and assert its ordered full scan equals the external ACK ledger. Collect successor markers, require disjointness and exact union with the predecessor set, calculate the canonical union digest, and compare it to the journal digest. Require r1 WAL absent; r0, r2.g1, r3.g1, and sentinel present.

- [ ] **Step 4: Verify GREEN and Move regression**

Run the gated Split foundation and the existing no-kill Move foundation.

Expected: both pass; Split evidence contains nonzero low/high counts, exact full-ledger equality, marker union equality, and exact topic preservation.

- [ ] **Step 5: Commit**

Commit message: `test(gres): prove split successor ownership`

### Task 4: Dedicated CI evidence and final review

**Files:**
- Create: `scripts/tests/gres-topology-process-split-foundation-ci.sh`
- Create: `docs/superpowers/evidence/2026-07-12-gres-g8-two-successor-split.md`
- Modify: `crates/gres/tests/topology_process_nemesis.rs`

**Interfaces:**
- Consumes: foundation JSON evidence.
- Produces: a fail-closed one-process CI shard and published evidence.

- [ ] **Step 1: Write the failing evidence validator**

Require nonempty unique tenant/operation IDs, `Completed`, exact r0/r2/r3 layout, distinct endpoints, both serving, nonzero low/high ownership with zero cross-side rows, full ACK equality, marker partition/union/digest equality, exact topic set, sentinel preservation, one predecessor delete, and operation duration within the test deadline.

- [ ] **Step 2: Verify validator RED**

Run the validator against incomplete evidence and require nonzero exit.

- [ ] **Step 3: Emit complete evidence and run the shard**

Build `crabka-cli` and `crabka-gres`, run the gated foundation under `timeout 180s`, and validate the JSON with Python.

- [ ] **Step 4: Run final verification**

Run `git diff --check`, focused harness tests, Split foundation CI, existing Move foundation CI, and `cargo check -p crabka-operator`.

Expected: all exit zero.

- [ ] **Step 5: Commit and review**

Commit message: `test(gres): validate two-successor split foundation`. Request independent review focused on direct ownership observation, marker union, endpoint identity, cleanup, and preservation of Move behavior. Do not begin Split kill parameterization until review is READY.
