# PgDog transaction-pooler baseline

Pinned pooler: PgDog 0.1.6 (`c99282e`).

The F-1 live gate configures `tenant-b` with `pool_size = 1`, keeps two logical
clients open concurrently, and proves backend state isolation and replay for the
session-control path implemented by this pinned release.

PgDog 0.1.6 only records `SET` when it is issued outside an explicit transaction
and before a backend is assigned. A `SET` issued after `BEGIN` is forwarded to
the backend without entering the logical client's parameter map, so it can leak
to the next client. The gate therefore supplies `application_name` as client
one's startup parameter (which 0.1.6 does track), issues `SET` to that same
value, and verifies its isolation and replay across later transactions. It
still executes and observes `SET LOCAL` inside a transaction and rollback.

PgDog 0.1.6 answers `SHOW statement_timeout` from its logical parameter map as
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
