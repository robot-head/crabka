# Gres G-5 lifecycle closure evidence

Date: 2026-07-11

Status: implementation and the operator-backed ten-cycle Kind/SLO gate pass.
This document does not claim later Gres chapters.

## Live gate

Command:

```console
CRABKA_GRES_COLDSTART_ITERATIONS=10 \
CRABKA_GRES_KIND_KEEP_CLUSTER=1 \
timeout 1800s scripts/gres-kind-lifecycle.sh
```

Result: `PASS: operator-backed Kind lifecycle and N=10 verified-TLS cold starts`.
After removing fleet-wide `RECONNECT` during final multi-tenant review, the same
gate passed again with `N=1`, including the real size and missing-manifest
scenarios; the route-hash fresh-pod bootstrap requires no reconnect.

The retained `target/gres-kind-lifecycle-artifacts/coldstart.json` reports:

- 10/10 measured cold starts;
- p50 6,016 ms, p95/max 6,613 ms, under the 30,000 ms CI ceiling;
- mean 5,926.0 ms, cold-start-only rate 0.1687/second, and full sustained
  suspend/park/wake lifecycle throughput 0.0192/second;
- strictly advancing physical WAL generations
  `2,3,4,5,6,7,8,9,10,11`;
- pinned official PgDog `ghcr.io/pgdogdev/pgdog:0.1.47`, resolved as
  `sha256:54cafbda4fc8602ae81188db46ad5695038468943c992ad6b0e1de06297e8a66`.

Every iteration proves final checkpoint and self-suspend, compute replicas zero,
generation-qualified WAL deletion, a confirmed PgDog activator route without a
pre-client wake, one verified-mTLS client wake, recovery of the original row,
and an advancing registry version. The wake path additionally asserts that the
PgDog config hash and pod UID do not change before the activator-held first
session completes.

Before the measured loop, the gate writes a real non-empty checkpoint, sets a
zero-byte suspend ceiling, restarts compute to make that manifest authoritative,
and proves the operator leaves replicas at one with the size-gate diagnostic.
After the ten cycles it scales the operator down, deletes the exact latest
manifest from MinIO, restarts the operator, and proves resume is refused while
compute remains at one and the physical WAL is preserved.

## Lifecycle/race closure

- Runtime recovery overlays the authoritative main registry record before
  selecting the physical WAL generation; the operator also provisions and
  deletes that same generation-qualified topic.
- Recovered compute preflights its WAL fence, initializes the single shared
  lifecycle registry and suspend components, then publishes Active immediately
  before polling the pgwire server.
- Activator request coalescing is scoped to concurrent waits. Both successful
  completion and broker-request error paths release the coalescing entry, so a
  later suspend can wake again.
- Suspended PgDog confirmation uses the completed config-hash Deployment
  rollout rather than opening an activator backend pool, which would itself
  violate the no-pre-client-wake invariant.
- Active remains on the byte-identical activator configuration for a bounded
  four-second post-resume grace. The ordinary reconcile then flips direct and
  bootstraps a fresh direct-route PgDog pod with the temporary tenant
  credential through the route-hash rollout. A second bounded `RELOAD` removes
  the credential without rolling that established pool. `RECONNECT` remains
  prohibited fleet-wide because it would rebuild unrelated suspended tenants'
  activator pools. This sequence is not on the held first-session path.
- Both transition deadlines are scheduled independently, and PgDog admin
  reload/route verification is bounded at twenty seconds so a wedged
  admin connection becomes a normal retry rather than stalling reconciliation.

## Focused static evidence

- `cargo test -p crabka-gres-activator --lib`: 5 passed.
- Focused G5 library suites: 44 + 5 + 47 + 106 + 440 passed.
- Lazy route/hash and expired-grace operator tests pass.
- PgDog admin/reload tests, renderer goldens, lifecycle script syntax, and
  `scripts/tests/gres-kind-lifecycle-structure.sh` pass.
- `cargo check -p crabka-operator -p crabka-gres -p crabka-gres-activator`
  passes.
- `cargo check --workspace --all-targets` passes. Stable-toolchain workspace
  clippy reaches unrelated pre-existing `manual_assert_eq` findings in
  metadata/client/log/blockstore tests; focused G5 clippy is required to pass
  with warnings denied.

Repository-wide check/clippy results and independent review disposition are
recorded separately when complete.
