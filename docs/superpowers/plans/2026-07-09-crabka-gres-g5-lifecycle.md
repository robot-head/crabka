# Chapter Gres G-5: Lifecycle Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Idle tenants scale to zero and wake transparently on the first connection, with cold-start latency continuously measured and gated.

**Architecture:** The compute self-suspends (final checkpoint → registry `Suspended` → clean exit); the `GresTenant` controller executes scale-to-zero and the `Gres` controller re-routes the tenant to the always-accepting `crabka-gres-activator`, which peeks the Postgres startup prelude, writes idempotent resume-request records, holds the connection until the compute recovers, then pipes bytes transparently.

**Tech Stack:** `crabka-gres-control` registry (state machine grows `ResumeRequested`), `crabka-pgwire` frontend decoding (prelude peek), tokio TCP piping, the G-4 operator controllers, the G-3 checkpointer (suspend checkpoint).

## Global Constraints

- **Prerequisites:** G-2/G-3/G-4 landed. Verify signatures against the landed tree.
- **Spec:** [2026-07-09-crabka-gres-g5-lifecycle-design.md](../specs/2026-07-09-crabka-gres-g5-lifecycle-design.md).
- **State machine (registry `TenantState`):** `Active ↔ Suspended`, plus `ResumeRequested` (written only by activators against `Suspended`; the controller treats it as "scale up now"; the compute's readiness write moves it to `Active`). Every transition is a whole-record upsert with `record_version` bumped; folds stay order-safe.
- **Suspend precondition:** zero open sessions AND idle window elapsed. An in-flight commit can never race suspension (sessions > 0 blocks it).
- **The activator never speaks the protocol past the prelude** — it reads SSLRequest/StartupMessage only, then pipes opaque bytes; auth and everything else terminate at the compute (the G-4 single-credential-store property).
- Lints/format/commit/test conventions as in the G-2 plan; every wait bounded and condition-driven.

---

## Batch 1 — signals (run Tasks 1 and 2 in parallel; disjoint crates)

### Task 1: Registry state semantics in `crabka-gres-control`

**Files:** Modify `crates/gres-control/src/record.rs` (+ `registry.rs` helpers).

Add `TenantState::ResumeRequested`; helper transitions `request_resume(&mut self, tenant)` (upsert only if current state is `Suspended` — read-modify-write with version bump; a lost race with another activator's identical write is harmless by fold semantics) and `mark_active` / `mark_suspended` for the compute's use. Steps: failing fold/transition unit tests (resume-request on Active is a no-op; duplicate resume-requests collapse; suspend→resume→active sequences fold correctly under reordering of distinct versions), implement, nextest/clippy/fmt, commit `feat(gres): tenant lifecycle states in the registry`.

### Task 2: Compute idle tracking + self-suspend

**Files:** Modify `crates/gres/src/main.rs` and (for session accounting) the smallest viable seam in `crates/pgexec`/`crates/pgwire` — inspect the landed tree: the pgwire server tracks per-connection sessions; expose an `Arc<AtomicUsize>` open-session counter + a last-activity `Arc` timestamp updated on statement execution (prefer a counter the server already maintains for CancelRequest bookkeeping; add a narrow public accessor rather than new machinery).

Suspend loop (substrate mode with `idle_seconds > 0` from the registry record): a monitor task checks `sessions == 0 && now - last_activity >= idle_seconds`; on trigger — stop accepting (drop the listener), re-check `sessions == 0` (a connection that raced in aborts the suspend and resumes accepting… simplest correct v1: check-then-close with one recheck after close; a client that connected in the gap gets a clean connection-refused on the next statement and retries into the activator), force a checkpoint through the G-3 control message and await its manifest durability, `mark_suspended`, flush logs, `std::process::exit(0)`. Steps: failing integration test (harness: short idle window; drive workload; observe checkpoint manifest + `Suspended` record + process-scope shutdown; a busy tenant with an open session never suspends), implement, nextest/clippy/fmt, commit `feat(gres): idle self-suspend with a final checkpoint`.

---

## Batch 2 — the activator (serial)

### Task 3: `crabka-gres-activator`

**Files:** Create `crates/gres-activator/` (internal-crate manifest house style; deps: `crabka-pgwire` (message decoding), `crabka-gres-control`, `tokio`, `tracing`, `thiserror`, `clap`; dev: `assert2`, `tokio-postgres`, `crabka-broker`, `tempfile`), `src/{main,lib,peek,hold,pipe}.rs`, `README.md`; release-plz entry; nextest group if broker-heavy tests warrant it.

**Interfaces:**
- Bin `crabka-gres-activator --listen --bootstrap [--registry-poll-ms]`.
- Core per-connection flow (lib, testable without the bin):
```rust
/// Read the connection prelude: answer SSLRequest with 'N' (v1: the PgDog→activator
/// leg is in-cluster plaintext, matching the chapter's in-cluster-plaintext v1 posture
/// for internal legs; the client→PgDog leg carries TLS) and capture the raw
/// StartupMessage bytes + parsed `database` parameter.
pub async fn peek_prelude(stream: &mut TcpStream) -> Result<Prelude, ActivatorError>;
pub struct Prelude { pub database: String, pub raw_startup: Vec<u8> }

/// request_resume → bounded condition-driven wait (registry watch + TCP probe of the
/// compute's endpoint from its Active record) → connect backend → write raw_startup →
/// bidirectional copy until either side closes. On timeout: a proper pgwire
/// ErrorResponse frame (57P03 cannot_connect_now) before close.
pub async fn serve_conn(stream: TcpStream, registry: RegistryHandle, cfg: &ActivatorConfig) -> Result<(), ActivatorError>;
```
- The `Active` record must carry the compute endpoint for the pipe target: add `endpoint: Option<String>` to `TenantRecord` (written by the compute in `mark_active`; the operator renders PgDog against the Service DNS name, so the record's endpoint is the same Service name — decide at execution which single source the activator uses and document it; the Service name derived from the tenant name is the simplest deterministic answer, avoiding record churn).

Steps: failing unit tests over `crabka-pgwire` decoding (goldens: plain StartupMessage; SSLRequest-then-startup; garbage → clean error frame); failing hold/pipe test with a scripted mock backend (bytes round-trip; held startup replays first; timeout produces the 57P03 frame); implement; integration: real compute wakes via a real registry on an in-process broker (activator + suspended record + manual "controller" test double that starts the compute on `ResumeRequested`); nextest/clippy/fmt/README; commit `feat(gres): the activator — accept, peek, resume, pipe`.

---

## Batch 3 — orchestration (serial: operator shared files)

### Task 4: Controllers execute the lifecycle

**Files:** Modify `crates/operator/src/controller/gres_tenant.rs` (+ its test file), `src/controller/gres.rs` (+ its test file), the registry seam on `Context` if the watch surface needs extending.

`GresTenant`: reconcile now also derives desired replicas from the registry state (`Active`/`ResumeRequested` → 1, `Suspended` → 0) — the registry watch (from G-4's `RegistryLike` seam) feeds a requeue channel so state flips reconcile promptly; status surfaces the lifecycle phase. `Gres`: the render input marks suspended tenants → activator endpoint (Task 3's Service), active tenants → compute Service; the activator Deployment/Service joins the fleet's rendered workloads (image from config, replicas from spec). Steps: mock-harness tests first (suspend flip → scale-to-0 + re-render sequence; ResumeRequested → scale-to-1; activator workload present), implement, nextest/clippy/fmt, commit `feat(operator): registry-driven suspend/resume orchestration`.

---

## Batch 4 — proof (run Tasks 5 and 6 in parallel; disjoint files)

### Task 5: Suspend/resume race suite

**Files:** Create `crates/gres-activator/tests/lifecycle.rs` (or extend gres-substrate's harness — pick whichever crate already hosts the in-process broker + compute helpers; keep one home).

Deterministic in-process coverage: full cycle (workload → idle → suspended → connect-through-activator → resumed → same data, the G-2 disposability assertions across the boundary); connect racing suspension-in-progress (client retried into the activator succeeds); two activators, simultaneous first connections (both pipe; registry shows collapsed requests); suspend blocked by an open session; zombie-vs-successor on resume settled by fencing (assert the loser exits). Commit `test(gres): lifecycle race suite`.

### Task 6: The cold-start SLO pipeline

**Files:** Create `scripts/gres-coldstart.sh` + a measurement mode in the e2e driver; modify `.github/workflows/ci.yml` (extend the `gres-e2e` job from G-4 with a cold-start leg + artifact upload; filter additions for `crates/gres-activator/**`).

The leg: provision a small tenant → drive → force suspend (CLI `suspend` for determinism rather than waiting out the idle window) → re-render/RELOAD → measure N=10 first-connection-to-`SELECT 1` latencies through PgDog+activator (fresh scale-up each iteration) → emit `coldstart.json` (p50/p95/max) → assert the environment-qualified ceiling (start generous, e.g. p95 < 30 s in CI; the number lives in one place in the script with a comment that it is a CI-environment backstop, not the product SLO) → upload artifact + step-summary table (the parity-report idiom). Commit `ci: gres cold-start SLO measurement and ceiling`.

## Completion checklist (maps to the G-5 gate)

- Scale-to-zero: idle → final checkpoint → `Suspended` → replicas 0 (Tasks 2, 4).
- Transparent wake: first connection through PgDog+activator succeeds with no client choreography (Tasks 3–5).
- Inherited safety: the race suite shows fencing + checkpointed recovery absorb every interleaving (Task 5).
- The SLO is measured continuously with a CI ceiling and published distributions (Task 6).
