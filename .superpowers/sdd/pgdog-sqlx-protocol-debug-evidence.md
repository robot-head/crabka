# Sanitized PgDog/sqlx protocol evidence

This artifact contains message codes and frame lengths only. No PostgreSQL
payloads, credentials, or bound values were retained.

## PostgreSQL 18 control

Command (URL credentials redacted):

```text
crabka-gres-driver-smoke --driver sqlx --database-url postgresql://REDACTED@127.0.0.1:16432/tenant-b?sslmode=disable
```

Status: `0`; stdout: `PASS: selected Rust parameterized transaction-pooling smoke`.
Pinned components: `postgres:18` and `ghcr.io/pgdogdev/pgdog:0.1.6`, transaction pooling.

PostgreSQL backend sequence after the two tokio-postgres transactions:

```text
frontend: Q(35) Q(37) Q(29) Q(11) P(37) D(16) S(5) B(34) E(10) C(7) S(5) Q(12) Q(11) B(34) E(10) C(7) S(5) Q(12)
backend:  C(9) Z(6) C(9) Z(6) C(9) S(18) Z(6) C(11) Z(6) 1(5) t(11) T(30) Z(6) 2(5) D(15) C(14) 3(5) Z(6) C(12) Z(6) C(11) Z(6) 2(5) D(15) C(14) 3(5) Z(6) C(12) Z(6)
```

## Gres failing trace before repair

Status: `1`; sqlx received SQLSTATE `58000`. PgDog logged
`ProtocolOutOfSync got: C` with `queue: []` and `extended: true`.

```text
frontend: Q(35) Q(37) Q(29)
backend:  C(9) Z(6) E(86) Z(6) C(9) Z(6)
```

PostgreSQL returned `C,Z; C,Z; C,S,Z` for those startup-setting queries. Gres
returned `C,Z; E,Z; C,Z`; the middle command identity (`extra_float_digits`)
was derived from sqlx source, not payload logging.

## PostgreSQL 18 integer-GUC oracle

Command: `psql -X -U bob -d tenant-b -v ON_ERROR_STOP=0 -At` inside the
`postgres:18` control container. Status: `0` (`ON_ERROR_STOP=0` intentionally
continued across invalid cases).

```text
default: 1
pg_settings.vartype: integer
-15 -> -15; 3 -> 3
whitespace and explicit +2 -> 2
1.4 -> 1; 1.6 -> 2; 1.5 -> 2; -1.5 -> -2
0x2 -> 2; +0x2 -> 2; 0o2 -> 2
010 -> rejected as decimal 10, outside -15..3
-16, 4, and non-numeric input -> rejected
```

## Full repaired live gate

Command:

```text
PATH="$PWD/target/gres-driver-venv/bin:$PATH" CRABKA_GRES_E2E_KEEP_ARTIFACTS=1 CRABKA_GRES_E2E_ARTIFACT_DIR=target/gres-e2e-fixed-artifacts CRABKA_GRES_SKIP_BUILD=1 bash scripts/gres-e2e.sh
```

Status: `0`. Top-level stdout tail:

```text
gres-e2e: PASS: tenant A data isolation -> tenant-a
gres-e2e: PASS: tenant B data isolation -> tenant-b
gres-e2e: PASS: tenant B survives tenant A compute death -> tenant-b
gres-e2e: PASS: Gres front-door e2e completed
gres-e2e: kept artifacts in target/gres-e2e-fixed-artifacts
```

Driver artifacts: `PASS` for selected Rust (tokio-postgres and sqlx) and
psycopg smokes. Extended parity: `100.0% (6 / 6 statements match the oracle)`.
