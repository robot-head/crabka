# Gres G-5: Lifecycle — design

**Date:** 2026-07-09
**Status:** Approved
**Type:** Slice design. The fifth slice of [Chapter Gres](2026-07-09-crabka-gres-chapter-design.md): serverless behavior — idle tenants scale to zero and wake on the first connection, with a measured cold-start SLO as the gate.

## Context — the one constraint that shapes everything

PgDog does not queue new connections while a backend is down: a lone primary that refuses TCP errors out after bounded connect attempts (`connect_timeout` × `connect_attempts`), and waiting clients fail at `checkout_timeout`. There is no first-class scale-to-zero, no on-demand hook, and stretching the timeout knobs global-wide is both racy and a tail-latency tax on healthy tenants. The consequence: something on the resume path must **always accept the TCP connection** — converting "backend down" (which PgDog errors on) into "slow backend" (which PgDog and every Postgres client tolerate inside ordinary query/checkout windows). Everything else in this slice is orchestration over machinery earlier slices already landed: G-2's fencing makes any scale-up ordering safe by construction, G-3's checkpoint-on-demand makes resume fast, and G-4's registry + config-render + `RELOAD` loop is the signaling and routing fabric.

## Design Goals

- **Scale to zero:** an idle tenant consumes no compute, and — *corrected after the scaling review* — as little of everything else as the design can arrange: a suspended tenant's WAL topic is **parked** (deleted after the final checkpoint; see below), leaving its checkpoints and one registry record. Without parking, every idle topic costs the brokers continuous follower fetch loops with dedicated connections, ×RF, forever — "idle is free" was false broker-side.
- **Transparent wake:** the first connection to a suspended tenant succeeds (slowly), not errors; no client-side retry choreography required.
- **Inherited safety:** no new correctness machinery — suspend/resume must be safe purely because fencing and checkpointed recovery already are.
- **A measured gate:** cold-start latency is continuously measured and asserted, not asserted once and forgotten.

## Non-goals

- **Predictive/scheduled wake, warm pools, pre-provisioned standbys** — tuning atop this slice's mechanism, driven by data it produces.
- **Session survival across suspend** — suspend requires zero open sessions by definition; long-lived idle *connections* held by PgDog pools count as sessions and block suspend (documented; PgDog-side idle disconnects are the operator's tuning knob).
- **Compute autoscaling beyond 0↔1** — multi-compute tenants are the disaggregated-store follow-on's territory.

## Architecture Overview

```
suspend (idle window elapsed, zero open sessions):
  compute: final checkpoint (G-3) → registry state = suspended → clean exit
  GresTenant controller (registry watch): scale Deployment → 0;
    park the WAL topic (delete — it is empty behind the final checkpoint;
    registry wal_generation += 1)
  Gres controller: re-render pgdog.toml → tenant db targets the ACTIVATOR → RELOAD
    (this render is OFF the wake path — it only has to land before the NEXT wake)

resume (first connection arrives):
  PgDog → activator (always accepting)
    activator: peek pg StartupMessage (crabka-pgwire decode) → tenant name
               write idempotent resume-request to registry
               hold the connection; bounded condition-driven wait for readiness
  GresTenant controller: recreate the WAL topic (current generation); scale → 1
  compute: restore checkpoint (manifest generation < registry generation ⇒
           fresh topic, tail replay from 0) + fence + barrier → state = active
  activator: replay held startup bytes to the compute; PIPE TRANSPARENTLY —
             the wake path ends here; no render, no RELOAD in it
  Gres controller (lazily, batched): re-render pgdog.toml → tenant db targets
             the compute directly → verified RELOAD; activator pipes drain
```

## Key Design Decisions

### Suspend is compute-initiated, controller-executed

The compute is the only component that knows session truth, so it owns the decision: after the configured idle window with zero open sessions (window per tenant in the registry; `0` disables), it takes a final checkpoint, writes `suspended`, and exits cleanly. The `GresTenant` controller — which already tails the registry through `crabka-gres-control`'s reader (surfaced into the controller as a watch-channel requeue trigger) — executes the Kubernetes half: Deployment to zero replicas, and *(added after the scaling review)* **parks the WAL topic**: behind the final checkpoint the topic is empty, so the controller deletes it and bumps the registry's `wal_generation`, eliminating the suspended tenant's standing broker cost (follower fetch loops, connections, metadata weight — the review measured idle topics as decidedly not free). Resume recreates the topic; recovery already distinguishes a fresh topic from a truncated one via the manifest's `wal_generation` (schema landed in G-3), replaying the tail from offset 0 — which is exactly the empty tail. Fencing survives parking untouched: the producer epoch lives in `__transaction_state` under the tenant's transactional id, not in the topic. Suspend is also **size-gated as policy**: a tenant whose checkpoint exceeds a configured threshold stays warm rather than suspending (its cold start would blow the SLO); the threshold is a fleet default with a per-tenant override, and the idle metric plus checkpoint size make the tradeoff visible. Computes never hold kube credentials; every signal is a registry record. A crash *during* suspend is just a crash — the successor path is identical whether the compute exited cleanly or not, because correctness never depended on clean shutdown (G-2's disposability gate).

### The activator converts down into slow

`crabka-gres-activator` is a small, stateless, always-on fleet service (replicable; part of the `Gres` controller's rendered workloads). Per connection: accept immediately; answer an `SSLRequest` with `'N'` — the PgDog→activator leg is in-cluster plaintext, the chapter's stated v1 posture for internal legs, while client-side TLS terminates at PgDog — then read the `StartupMessage` using `crabka-pgwire`'s frontend decoding to learn the target database → tenant; write an idempotent resume-request record; hold the socket while waiting — a bounded, condition-driven wait on the registry flipping to `active` plus a TCP readiness probe of the compute, never a blind sleep — then open the backend connection, replay the held startup bytes, and pipe bytes both ways until either side closes. It terminates nothing else of the protocol: auth and everything after the startup pass through untouched. Wait bounds surface as ordinary Postgres error responses so a stuck wake fails loudly within the client's own timeout budget — a budget G-4's renderer guarantees exceeds the cold-start ceiling.

### The wake path contains no render and no RELOAD

*(Promoted from fallback to the design after the scaling review: the original resume path waited on an O(N-tenants) fleet render, a Secret propagation, and a RELOAD — a serialized pipeline sitting inside every cold start and capping fleet-wide wake throughput at the reconciler's churn rate.)* Routing still flips at the config layer, but **asymmetrically**. Suspend-side (never latency-critical): the tenant's `[[databases]]` entry re-targets the activator; this render only has to land before the *next* wake, and the suspend flow tolerates it lazily. Resume-side (the critical path): the activator pipes the held connection **directly to the recovered compute** — the wake completes with zero config churn — and the flip back to direct PgDog→compute routing happens lazily afterward, batched across recent resumes by the fleet controller's ordinary reconcile. Until that lazy flip lands, new connections keep arriving via the activator, which pipes them straight through to the active compute (a per-connection hop, not a stall). The alternative — always routing through the activator — remains rejected as a permanent second proxy tier; the lazy flip buys the same wake latency without institutionalizing the hop. Races stay benign by construction: an activator connection for an already-active tenant pipes immediately; a connection reaching a just-suspended compute fails and retries into the activator; anything that double-starts computes is what fencing kills.

### Resume requests are records, so wake is at-least-once and idempotent

Multiple activator replicas (or a thundering herd of first connections) produce duplicate resume-request records; the fold in the registry reader collapses them, the controller's reconcile is level-triggered, and scale-up to one replica is idempotent. No leases, no locks, no coordination between activator replicas — the registry's existing semantics absorb it all.

### The SLO is a measurement pipeline, not a number in a doc

The gate harness provisions a small tenant, drives it to suspension, then measures first-connection-to-first-query-result through the full path (PgDog → activator → wake → recovery → pipe). CI asserts a documented, environment-qualified ceiling and publishes the measured distribution as a per-PR artifact (the parity-report idiom), so regressions in any layer — checkpoint size, replay speed, controller latency, render/RELOAD lag — show up as a number moving, with the ceiling as the backstop. Production SLO targets are an operations concern set from this pipeline's data, not hardcoded here.

## Integration

- **`crates/gres`:** idle tracking (sessions + last-statement clock), suspend sequence (checkpoint → registry write → exit), readiness signal on `active`.
- **New crate `crates/gres-activator`** (`crabka-gres-activator`, `publish = false`): the accept/peek/request/hold/pipe loop over `crabka-pgwire` message decoding + `crabka-gres-control` registry access.
- **`crates/gres-control`:** the `suspended`/`ResumeRequested` state transitions, resume-request records, idle-window field, and `wal_generation` — the record schema G-4 seeded is *extended* here by this slice's own plan (Task 1), not merely activated *(corrected after the PR panel review: G-4 seeds the record; G-5's plan adds the lifecycle fields — execution is unblocked either way, but the provenance was wrong)*.
- **`crates/operator`:** `GresTenant` controller gains scale-to-zero/one on registry state; `Gres` controller gains activator workload rendering + suspended-tenant routing in the config render.
- **`crates/cli`:** `crabka gres suspend|resume` become immediate (they write the same records the automation writes).

## Kafka / wire compliance

Nothing new on the Kafka wire (registry records as in G-4). On the Postgres wire the activator is deliberately not a protocol participant beyond reading the standard connection prelude — it proxies bytes, keeping the compute the sole auth/protocol authority (the G-4 single-credential-store property is preserved).

## Testing

- **Activator units:** prelude parsing over `crabka-pgwire` decode (golden startup traces incl. SSLRequest path), hold-then-pipe with a mock backend, bounded-wait timeout surfacing as a proper Postgres error frame.
- **Suspend/resume integration (in-process broker + bucket, deterministic):** drive tenant → idle → assert final checkpoint + `suspended` record + clean exit; connect through a real activator instance → assert resume-request, recovery, first query succeeds; assert no acked loss across the full cycle (the G-2 disposability suite re-run across a suspend/resume boundary).
- **Race coverage:** connect during suspension-in-progress; duplicate simultaneous first-connections through two activator replicas; suspend racing an in-flight commit (must be blocked by the zero-open-sessions rule); scale-up racing a zombie (settled by fencing — assert the loser exits).
- **Operator:** mock-harness reconcile tests for state-driven scaling and routing re-render.
- **The gate (e2e, CI):** the cold-start measurement pipeline with its asserted ceiling and published distribution.

## Risks

- **PgDog pooled idle connections block suspend** — transaction-mode pooling is now the G-4 rendered default with backend idle disconnects pinned (the scaling review's finding: session pooling makes suspend unreachable); the idle metric exposes any residual blocker.
- **The lazy flip leaves a window of activator-hop traffic after wake** — bounded by the fleet controller's reconcile cadence; the activator pipes at line rate, so the cost is one intra-cluster hop per connection during the window, measured by the SLO pipeline.
- **Fleet churn still bounds suspend-side re-renders** — suspends batch through the same lazy reconcile; the chapter's cell posture (~10³ tenants per fleet) is the stated envelope, and the cold-start pipeline publishes wake rates alongside latency so churn saturation is visible before it hurts.
- **Thundering herds on a popular suspended tenant** — absorbed by idempotent records and one Deployment; the held connections all pipe when the single compute wakes.
- **Broker cross-track dependency** *(named after the scaling review)*: topic parking removes the per-idle-tenant broker cost, but fleets of many *active* tenants still lean on broker behaviors that degrade with topic count (per-topic follower fetch connections; metadata scans that are O(partitions) per topic lookup). Replica-fetch multiplexing and indexing partitions-by-topic are broker-track work items this chapter depends on beyond ~10⁴ topics, tracked there — not silently assumed here.

## Resolved decisions

- Suspend: compute-initiated (idle window, zero sessions, checkpoint-size gate) with a final checkpoint; controller executes scale-to-zero and **parks the WAL topic** (delete + `wal_generation` bump; fencing survives in `__transaction_state`); all signaling via registry records.
- Wake: always-accepting activator; `SSLRequest → 'N'` then startup peek via `crabka-pgwire`; idempotent resume-request records; bounded condition-driven hold; transparent byte piping **directly to the recovered compute — no render or RELOAD on the wake path**.
- Routing: asymmetric config-layer flips — suspend-side re-render lands lazily before the next wake; resume-side flip back to direct routing is lazy and batched; no permanent hop.
- Gate: measured cold-start pipeline with an environment-qualified CI ceiling and published distributions (including wake rates, so churn saturation is visible).
