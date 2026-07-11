# PgDog transaction-pooler baseline

Pinned front-door pooler: PgDog 0.1.47 (`f6eea5e7c7c06f62a72e669c3f3f607f4945658b`).

The front-door corpus uses `pooler-baseline.json` (665/688), one match below
the direct-compute baseline (666/688). PgDog accepts the deliberately invalid
`SET TIME ZONE 'Mars/Phobos'` as logical session state without forwarding the
compute's `22023`; the harness reconnects between SQL files so this deliberate
pooler deviation cannot contaminate unrelated cases. The statement is still
executed and remains visible as a mismatch; it is not removed or skipped.

The F-1 live gate configures `tenant-b` with `pool_size = 1`, keeps two logical
clients open concurrently, and proves backend state isolation and replay for the
session-control path implemented by this pinned release.

PgDog records `SET` when it is issued outside an explicit transaction
and before a backend is assigned. A `SET` issued after `BEGIN` is forwarded to
the backend without entering the logical client's parameter map, so it can leak
to the next client. The gate therefore supplies `application_name` as client
one's startup parameter, issues a distinct SET inside
the transaction and observes that changed value directly on the assigned
backend, then restores the tracked startup value before releasing the backend.
The later replay assertion proves startup-parameter replay only; it does not
claim SET-change replay. The gate also executes and observes `SET LOCAL` inside
a transaction and rollback.

PgDog answers `SHOW statement_timeout` from its logical parameter map as
the raw assigned value (`17`) rather than PostgreSQL/Gres canonical output
(`17ms`). The live assertion pins that rendering deviation and separately checks
that rollback returns the next transaction to `0`.

The same release forwards `RESET` without updating the logical parameter map.
The gate keeps `RESET` and its `SHOW` observation in one simple-query checkout,
then verifies the other logical client remains at its default. Reusing the reset
logical client afterward is deliberately not claimed to preserve the reset.

These are pooler-version limitations, not Gres deviations from PostgreSQL 18.
Removing this baseline requires upgrading the pinned PgDog version and first
turning the explicit-transaction `SET` and post-`RESET` reuse probes into passing
live assertions.
