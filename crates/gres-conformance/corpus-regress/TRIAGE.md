# Adopted pg_regress corpus — parity triage

Work queue for M4 (progressive `pg_regress` adoption) and M5 (no `Wave-assigned` row left in [PG_COMPAT_MATRIX.md](../../../docs/PG_COMPAT_MATRIX.md)). Every number here is a measurement against a live PostgreSQL 18.4 oracle, not an estimate.

## 2026-08-02 measurement

```sh
cargo build --locked -p crabka-gres -p crabka-gres-conformance
setsid ./target/debug/crabka-gres --listen 127.0.0.1:54360 >/tmp/gres.log 2>&1 </dev/null &
./target/debug/crabka-gres-conformance \
  --oracle-url "host=127.0.0.1 port=54320 user=postgres dbname=<fresh empty db>" \
  --subject-url "host=127.0.0.1 port=54360 user=crab dbname=crab" \
  --corpus crates/gres-conformance/corpus \
  --corpus-regress crates/gres-conformance/corpus-regress \
  --regress-baseline crates/gres-conformance/corpus-regress/baseline.json \
  --statement-timeout-secs 5 \
  --regress-out regress-parity.json \
  --regress-summary regress-parity.md
```

Both endpoints started clean. The oracle database was empty and the subject was a fresh in-memory process, preventing stale relations from changing later outcomes.

**Result: 9323 / 14272 statements match (65.3%) across 50 adopted files, up 314 matches from the previous 9009 / 14272 floor. The generated report contains zero engine crashes, statement timeouts, or unrecovered connection failures; the current harness retries transient I/O failures without recording a reconnect counter.**

This percentage covers the adopted corpus, not PostgreSQL's complete core regression schedule. PostgreSQL 18.4 schedules 231 core test files; the plan to replace this partial headline with an upstream `pg_regress` gate is in [2026-08-02-gres-pg-regress-100-percent.md](../../../docs/superpowers/plans/2026-08-02-gres-pg-regress-100-percent.md).

## Per-file parity

| file | matched | total | parity |
| --- | ---: | ---: | ---: |
| delete | 10 | 10 | 100.0% |
| comments | 9 | 9 | 100.0% |
| boolean | 94 | 98 | 95.9% |
| limit | 76 | 80 | 95.0% |
| date | 256 | 271 | 94.5% |
| select_distinct | 92 | 101 | 91.1% |
| select_implicit | 40 | 44 | 90.9% |
| prepare | 29 | 33 | 87.9% |
| transactions | 428 | 490 | 87.3% |
| time | 38 | 44 | 86.4% |
| numeric | 899 | 1057 | 85.1% |
| select | 73 | 87 | 83.9% |
| int8 | 145 | 174 | 83.3% |
| select_having | 19 | 23 | 82.6% |
| timetz | 47 | 57 | 82.5% |
| int4 | 73 | 94 | 77.7% |
| subselect | 252 | 327 | 77.1% |
| arrays | 392 | 515 | 76.1% |
| case | 47 | 64 | 73.4% |
| jsonb | 787 | 1084 | 72.6% |
| name | 29 | 40 | 72.5% |
| timestamp | 127 | 177 | 71.8% |
| text | 52 | 73 | 71.2% |
| join | 642 | 920 | 69.8% |
| strings | 345 | 500 | 69.0% |
| timestamptz | 278 | 404 | 68.8% |
| int2 | 51 | 76 | 67.1% |
| select_into | 44 | 67 | 65.7% |
| union | 130 | 198 | 65.7% |
| aggregates | 351 | 545 | 64.4% |
| create_index | 425 | 682 | 62.3% |
| window | 243 | 391 | 62.1% |
| alter_table | 1018 | 1675 | 60.8% |
| update | 171 | 286 | 59.8% |
| truncate | 114 | 193 | 59.1% |
| create_table | 195 | 333 | 58.6% |
| groupingsets | 125 | 217 | 57.6% |
| float4 | 56 | 100 | 56.0% |
| interval | 244 | 446 | 54.7% |
| varchar | 12 | 22 | 54.5% |
| float8 | 99 | 184 | 53.8% |
| char | 16 | 32 | 50.0% |
| sequence | 123 | 261 | 47.1% |
| copy | 44 | 106 | 41.5% |
| json | 194 | 469 | 41.4% |
| insert | 157 | 387 | 40.6% |
| with | 124 | 308 | 40.3% |
| create_view | 87 | 307 | 28.3% |
| expressions | 21 | 79 | 26.6% |
| bit | 0 | 132 | 0.0% |

## Largest gains from the previous floor

| additional matches | file |
| ---: | --- |
| 68 | transactions |
| 61 | alter_table |
| 29 | truncate |
| 28 | join |
| 24 | update |
| 19 | select |
| 16 | create_index |
| 13 | subselect |
| 10 | create_view |

## Mismatches by root cause

The table is ranked by statements unlocked. `25P02` is a cascade: the subject correctly aborted a transaction block after an earlier statement failed, so the first failure in that file is the useful target.

| statements | sqlstate | signature | example |
| ---: | --- | --- | --- |
| 1089 | — | wrong rows | `EXPLAIN (VERBOSE, COSTS OFF) ...` |
| 141 | 25P02 | current transaction is aborted | `SELECT balk(hundred) FROM tenk1` |
| 71 | 42704 | bit-string literal inferred as missing type `b` | `INSERT INTO bit_table VALUES (B'10')` |
| 71 | 42704 | composite type `jsbrec` missing | `jsonb_populate_record(NULL::jsbrec, ...)` |
| 71 | 42704 | composite type `jsrec` missing | `json_populate_record(NULL::jsrec, ...)` |
| 49 | 0A000 | table inheritance unsupported | `CREATE TABLE ... INHERITS (...)` |
| 44 | 22003 | floating-point infinity handled as integer overflow | `power(float8 '1.1', float8 'inf')` |
| 43 | 0A000 | aggregate `ORDER BY` unsupported | `avg(a ORDER BY b)` |
| 41 | 42601 | record-returning function column definition list rejected | `json_populate_record(NULL::record, ...) AS (x int, y int)` |
| 38 | 0A000 | expression indexes unsupported | `CREATE INDEX ... (expression)` |
| 37 | 42883 | `pg_input_error_info` missing | `pg_input_error_info('{1,zed}', 'integer[]')` |
| 33 | 42601 | ordered-set aggregate grammar missing | `percentile_cont(...) WITHIN GROUP (...)` |
| 31 | 0A000 | recursive CTE `SEARCH` and `CYCLE` unsupported | `WITH RECURSIVE ... SEARCH ... CYCLE ...` |
| 30 | 0A000 | stored views over joins or derived tables unsupported | `CREATE VIEW ... AS SELECT ... JOIN ...` |
| 30 | 42846 | text-to-`bytea` input conversion missing | `E'\\xDeAdBeEf'::bytea` |
| 29 | 0A000 | partitioned update with reordered child columns unsupported | `UPDATE partitioned_table ...` |
| 29 | 42601 | array-slice assignment targets unsupported | `INSERT INTO arrtest (a[1:5], ...)` |
| 26 | 42601 | qualified-star composite casts unsupported | `row(i.*::int8_tbl)::nestedcomposite` |
| 25 | 42883 | sequence functions do not accept `regclass` | `nextval('sequence_test'::regclass)` |
| 25 | 42601 | `ALTER INDEX ... ALTER COLUMN` unsupported | `ALTER INDEX idx ALTER COLUMN 1 SET STATISTICS 1000` |

## Next work

Fix shared roots in descending unlock order, but split the broad wrong-row bucket by statement family before implementation. A change is complete only when its focused PostgreSQL regression file improves, the full adopted corpus does not regress, and the matching baseline entry ratchets in the same review.
