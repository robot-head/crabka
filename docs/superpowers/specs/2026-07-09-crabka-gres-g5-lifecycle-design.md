# Gres G-5: Lifecycle — design

**Date:** 2026-07-09
**Status:** Approved
**Type:** Slice design. The fifth slice of [Chapter Gres](2026-07-09-crabka-gres-chapter-design.md): serverless behavior — idle tenants scale to zero and wake on the first connection, with a measured cold-start SLO as the gate.

## Context — the one constraint that shapes everything

PgDog does not queue new connections while a backend is down: a lone primary that refuses TCP errors out after bounded connect attempts (`connect_timeout` × `connect_attempts`), and waiting clients fail at `checkout_timeout`. There is no first-class scale-to-zero, no on-demand hook, and stretching the timeout knobs global-wide is both racy and a tail-latency tax on healthy tenants. The consequence: something on the resume path must **always accept the TCP connection** — converting "backend down" (which PgDog errors on) into "slow backend" (which PgDog and every Postgres client tolerate inside ordinary query/checkout windows). Everything else in this slice is orchestration over machinery earlier slices already landed: G-2's fencing makes any scale-up ordering safe by construction, G-3's checkpoint-on-demand makes resume fast, and G-4's registry + config-render + `RELOAD` loop is the signaling and routing fabric.

## Design Goals

- **Scale to zero:** an idle tenant consumes no compute — only its topic tail, its checkpoints, and one registry record.
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
  GresTenant controller (registry watch): scale Deployment → 0
  Gres controller: re-render pgdog.toml → tenant db targets the ACTIVATOR → RELOAD

resume (first connection arrives):
  PgDog → activator (always accepting)
    activator: peek pg StartupMessage (crabka-pgwire decode) → tenant name
               write idempotent resume-request to registry
               hold the connection; bounded condition-driven wait for readiness
  GresTenant controller: scale Deployment → 1
  compute: restore checkpoint + replay tail + fence + barrier (G-2/G-3) →
           registry state = active
  Gres controller: re-render pgdog.toml → tenant db targets the compute → RELOAD
  activator: replay held startup bytes to the compute; pipe transparently;
             in-flight pipes drain naturally after re-route
```

## Key Design Decisions

### Suspend is compute-initiated, controller-executed

The compute is the only component that knows session truth, so it owns the decision: after the configured idle window with zero open sessions (window per tenant in the registry; `0` disables), it takes a final checkpoint, writes `suspended`, and exits cleanly. The `GresTenant` controller — which already tails the registry through `crabka-gres-control`'s reader (surfaced into the controller as a watch-channel requeue trigger) — executes the Kubernetes half: Deployment to zero replicas. Computes never hold kube credentials; every signal is a registry record. The final checkpoint is what makes this slice cheap: the next resume replays a near-empty tail, so cold start is dominated by checkpoint download for small tenants. A crash *during* suspend is just a crash — the successor path is identical whether the compute exited cleanly or not, because correctness never depended on clean shutdown (G-2's disposability gate).

### The activator converts down into slow

`crabka-gres-activator` is a small, stateless, always-on fleet service (replicable; part of the `Gres` controller's rendered workloads). Per connection: accept immediately; read exactly the SSLRequest/StartupMessage prelude using `crabka-pgwire`'s frontend decoding to learn the target database → tenant; write an idempotent resume-request record; hold the socket while waiting — a bounded, condition-driven wait on the registry flipping to `active` plus a TCP readiness probe of the compute, never a blind sleep — then open the backend connection, replay the held prelude bytes, and pipe bytes both ways until either side closes. It terminates nothing else of the protocol: auth, TLS-with-the-compute, and everything after the prelude pass through untouched (on the PgDog→backend leg the prelude is plaintext startup unless backend-TLS is configured; if backend TLS is enabled the activator answers the SSLRequest itself and pipes the TLS stream opaquely after wake — it holds bytes, not sessions). Wait bounds surface as ordinary Postgres error responses so a stuck wake fails loudly within the client's own timeout budget.

### Routing flips at the config layer, not a permanent hop

Suspended tenants' `[[databases]]` entries target the activator; active tenants' entries target their compute directly — the `Gres` controller already owns render + `RELOAD` (G-4), so suspend/resume are just two more render triggers from the same registry watch. The alternative — always routing through the activator — was rejected: a permanent extra hop and a second proxy tier to operate, purchased only to avoid config churn that the aggregation controller handles anyway. Races at the flip are benign by construction: a connection that reaches the activator for an already-active tenant just gets piped through immediately; one that reaches a just-suspended compute fails and retries into the activator on the next attempt; and any impossible interleaving that double-starts computes is exactly what fencing already kills.

### Resume requests are records, so wake is at-least-once and idempotent

Multiple activator replicas (or a thundering herd of first connections) produce duplicate resume-request records; the fold in the registry reader collapses them, the controller's reconcile is level-triggered, and scale-up to one replica is idempotent. No leases, no locks, no coordination between activator replicas — the registry's existing semantics absorb it all.

### The SLO is a measurement pipeline, not a number in a doc

The gate harness provisions a small tenant, drives it to suspension, then measures first-connection-to-first-query-result through the full path (PgDog → activator → wake → recovery → pipe). CI asserts a documented, environment-qualified ceiling and publishes the measured distribution as a per-PR artifact (the parity-report idiom), so regressions in any layer — checkpoint size, replay speed, controller latency, render/RELOAD lag — show up as a number moving, with the ceiling as the backstop. Production SLO targets are an operations concern set from this pipeline's data, not hardcoded here.

## Integration

- **`crates/gres`:** idle tracking (sessions + last-statement clock), suspend sequence (checkpoint → registry write → exit), readiness signal on `active`.
- **New crate `crates/gres-activator`** (`crabka-gres-activator`, `publish = false`): the accept/peek/request/hold/pipe loop over `crabka-pgwire` message decoding + `crabka-gres-control` registry access.
- **`crates/gres-control`:** `suspended` state transitions, resume-request records, idle-window field (all landed as schema in G-4; semantics activate here).
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

- **PgDog pooled idle connections block suspend** — transaction-mode pooling plus PgDog-side idle server-connection reaping are the mitigation knobs; documented as an operator tuning concern, with the idle metric exposing it.
- **Render+RELOAD latency sits inside the cold-start budget** — measured by the pipeline; if it dominates, the named optimization is routing suspended tenants through the activator *speculatively* (flip on suspend only, skip the resume-time flip until after wake) — a config-policy change, not an architecture change.
- **Activator as a byte pipe with backend TLS** — the SSLRequest-handling subtlety is pinned by golden-trace tests; if a PgDog behavior makes pass-through infeasible in some mode, the fallback is plaintext-to-activator inside the cluster boundary (PgDog↔activator only), explicitly documented.
- **Thundering herds on a popular suspended tenant** — absorbed by idempotent records and one Deployment; the held connections all pipe when the single compute wakes.

## Resolved decisions

- Suspend: compute-initiated (idle window, zero sessions) with a final checkpoint; controller executes scale-to-zero; all signaling via registry records.
- Wake: always-accepting activator; startup-prelude peek via `crabka-pgwire`; idempotent resume-request records; bounded condition-driven hold; transparent byte piping.
- Routing: config-layer flip between compute and activator per tenant state; no permanent hop.
- Gate: measured cold-start pipeline with an environment-qualified CI ceiling and published distributions.
