# PgDog/sqlx protocol debug report

## Outcome

The blocker belonged to Gres's SQL connection preamble compatibility. PgDog
0.1.6 and normal sqlx prepared-statement behavior remain unchanged.

## Control and trace

The smoke gained `--driver all|tokio-postgres|sqlx`, with `all` as the default,
so sqlx was reproduced on a fresh connection. The identical sqlx-only smoke,
including two transactions and persistent prepared `$1` execution, passed
through `ghcr.io/pgdogdev/pgdog:0.1.6` in transaction mode against PostgreSQL
18.

A bounded plaintext proxy recorded only PostgreSQL message codes and frame
lengths. It did not record passwords or message payloads. After the two
tokio-postgres transactions, PostgreSQL accepted the three sanitized sqlx
startup-setting queries as `Q -> C,Z`, `Q -> C,Z`, and `Q -> C,S,Z`. Gres
instead returned `Q -> C,Z`, `Q -> E,Z`, and `Q -> C,Z`. The middle query was
sqlx's standard `extra_float_digits` setting. PgDog discarded its simulated
startup queue after Gres's error; the following Gres `CommandComplete` was then
reported as `ProtocolOutOfSync got: C` with an empty queue.

The later prepared query sequence matched PostgreSQL's expected shape:
`P,D,S -> 1,t,T,Z`, followed by `B,E,C,S -> 2,D,C,3,Z`. Upstream PgDog PR #913
was not used.

The redacted commands, exit statuses, code/length sequences, PostgreSQL 18
integer-GUC oracle results, and top-level final-PASS output are retained in
`.superpowers/sdd/pgdog-sqlx-protocol-debug-evidence.md`.

## RED / GREEN

RED: `sqlx_extra_float_digits_preamble_is_accepted` failed with SQLSTATE 42704.

GREEN: Gres now registers `extra_float_digits`, uses PostgreSQL 18's default of
`1`, reports it as an integer GUC, and accepts PostgreSQL's `-15..=3` range.
The typed integer-GUC parser matches the PostgreSQL 18 oracle for whitespace,
signs, rounded fractional inputs, hexadecimal and `0o` octal forms. Invalid and
out-of-range inputs return SQLSTATE 22023. SET, SET LOCAL, commit, and rollback
behavior is covered by SQL-level tests.

## Live evidence

The complete `scripts/gres-e2e.sh` gate ran with
`target/gres-driver-venv/bin` first in `PATH` and the pinned PgDog 0.1.6 and
PostgreSQL 18 images. It printed final `PASS: Gres front-door e2e completed`.
Extended parity remained 100% (6/6); the Rust smoke passed both tokio-postgres
and sqlx, and the psycopg smoke passed. Retained artifacts are under
`target/gres-e2e-fixed-artifacts`.
