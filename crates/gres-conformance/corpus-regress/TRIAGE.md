# Adopted pg_regress corpus — parity triage

Work queue for M4 (progressive pg_regress adoption) and M5 (no `Wave-assigned` row
left in [PG_COMPAT_MATRIX.md](../../../docs/PG_COMPAT_MATRIX.md)). Every number here
is a measurement against a live PostgreSQL 18.4 oracle, not an estimate. It is a
dated snapshot — re-run before acting on a single number.

## How this was measured

```sh
cargo build --locked -p crabka-gres -p crabka-gres-conformance
setsid ./target/debug/crabka-gres --listen 127.0.0.1:54360 >/tmp/gres.log 2>&1 </dev/null &
./target/debug/crabka-gres-conformance \
  --oracle-url "host=127.0.0.1 port=54320 user=postgres dbname=<fresh empty db>" \
  --subject-url "host=127.0.0.1 port=54360 user=crab dbname=crab" \
  --corpus crates/gres-conformance/corpus \
  --corpus-regress crates/gres-conformance/corpus-regress \
  --statement-timeout-secs 5 \
  --regress-out regress-parity.json --regress-summary regress-parity.md
```

Both engines must start clean: the oracle database empty (the harness never resets
it, so a leftover relation turns `CREATE TABLE` into `42P07` on one side only) and
the subject freshly restarted, since it is in-memory and a stale process carries the
previous run's state.

**Result: 6216 / 14272 statements match (43.6%) across 50 adopted files, with zero engine crashes.**

## Per-file parity

| file | matched | total | parity |
| --- | ---: | ---: | ---: |
| comments | 9 | 9 | 100.0% |
| delete | 10 | 10 | 100.0% |
| boolean | 94 | 98 | 95.9% |
| date | 256 | 271 | 94.5% |
| prepare | 29 | 33 | 87.9% |
| int8 | 145 | 174 | 83.3% |
| limit | 64 | 80 | 80.0% |
| int4 | 73 | 94 | 77.7% |
| arrays | 387 | 515 | 75.1% |
| text | 52 | 73 | 71.2% |
| case | 43 | 64 | 67.2% |
| int2 | 51 | 76 | 67.1% |
| float4 | 56 | 100 | 56.0% |
| aggregates | 304 | 545 | 55.8% |
| union | 110 | 198 | 55.6% |
| select_distinct | 56 | 101 | 55.4% |
| strings | 275 | 500 | 55.0% |
| float8 | 99 | 184 | 53.8% |
| groupingsets | 115 | 217 | 53.0% |
| create_index | 344 | 682 | 50.4% |
| char | 16 | 32 | 50.0% |
| interval | 218 | 446 | 48.9% |
| time | 21 | 44 | 47.7% |
| join | 436 | 920 | 47.4% |
| jsonb | 513 | 1084 | 47.3% |
| transactions | 231 | 490 | 47.1% |
| timestamptz | 189 | 404 | 46.8% |
| alter_table | 773 | 1675 | 46.1% |
| timetz | 26 | 57 | 45.6% |
| window | 174 | 391 | 44.5% |
| select | 36 | 87 | 41.4% |
| name | 16 | 40 | 40.0% |
| json | 156 | 469 | 33.3% |
| copy | 33 | 106 | 31.1% |
| timestamp | 44 | 177 | 24.9% |
| create_view | 75 | 307 | 24.4% |
| numeric | 255 | 1057 | 24.1% |
| truncate | 46 | 193 | 23.8% |
| subselect | 74 | 327 | 22.6% |
| create_table | 75 | 333 | 22.5% |
| with | 65 | 308 | 21.1% |
| expressions | 16 | 79 | 20.3% |
| sequence | 48 | 261 | 18.4% |
| insert | 60 | 387 | 15.5% |
| update | 38 | 286 | 13.3% |
| select_into | 8 | 67 | 11.9% |
| varchar | 2 | 22 | 9.1% |
| bit | 0 | 132 | 0.0% |
| select_having | 0 | 23 | 0.0% |
| select_implicit | 0 | 44 | 0.0% |

## Mismatches by root cause

Ranked by statements unlocked. `25P02` is a *cascade*: the subject correctly aborted
the transaction block after an earlier statement failed, so fixing that file's first
failure is what unlocks the bucket. `XXTO` is the harness statement timeout — the
engine is too slow to answer, not wrong.

| statements | sqlstate | signature | example |
| ---: | --- | --- | --- |
| 1172 | - | wrong rows (subject executed the statement) | `explain (verbose, costs off) select s1, s2, sm from generate_series(1, 3) s1,      lateral (select s` |
| 681 | 25P02 | current transaction is aborted, commands ignored until end of transaction block | `SELECT balk(hundred) FROM tenk1` |
| 457 | XXTO | statement did not answer within Ns | `create unique index on fkest(x, x10, x100)` |
| 254 | 42601 | syntax error at position N: expected LParen, found Ident("partition") | `create temp table p_t1_1 partition of p_t1 for values in(1)` |
| 115 | 42601 | syntax error at position N: expected ; or end of input, found Ident("partition") | `create temp table p_t1 (   a int,   b int,   c int,   d int,   primary key(a,b) ) partition by list(` |
| 108 | 42P01 | relation "testjsonb" does not exist | `SELECT count(*) from testjsonb  WHERE j->'array' ? 'bar'` |
| 101 | 42P01 | relation "timestamptz_tbl" does not exist | `INSERT INTO TIMESTAMPTZ_TBL VALUES ('today')` |
| 99 | 42P01 | relation "timestamp_tbl" does not exist | `INSERT INTO TIMESTAMP_TBL VALUES ('today')` |
| 96 | 42P01 | relation "test_jsonb_subscript" does not exist | `insert into test_jsonb_subscript values (1, '{}'),  (2, '{"key": "value"}')` |
| 71 | 42704 | type "b" does not exist | `INSERT INTO BIT_TABLE VALUES (B'10')` |
| 69 | 42704 | type "jsrec" does not exist | `SELECT ia FROM json_populate_record(NULL::jsrec, '{"ia": null}') q` |
| 69 | 42704 | type "jsbrec" does not exist | `SELECT ia FROM jsonb_populate_record(NULL::jsbrec, '{"ia": null}') q` |
| 66 | 42601 | syntax error at position N: expected Keyword(Table), found Ident("type") | `create type t_rec as (x numeric)` |
| 65 | 42601 | syntax error at position N: expected Keyword(Table), found Ident("trigger") | `create trigger ttdummy 	before delete or update on alterlock 	for each row 	execute procedure 	ttdum` |
| 53 | 42P01 | relation "list_parted" does not exist | `CREATE TABLE fail_part (LIKE list_parted)` |
| 52 | 42P01 | relation "num_input_test" does not exist | `INSERT INTO num_input_test(n1) VALUES (' 123')` |
| 50 | 42P01 | relation "empsalary" does not exist | `INSERT INTO empsalary VALUES ('develop', 10, 5200, '2007-08-01'), ('sales', 1, 5000, '2006-10-01'), ` |
| 46 | 42883 | function to_number(...) does not exist | `SELECT to_number('-34,338,492', '99G999G999')` |
| 44 | 22003 | integer out of range | `SELECT power(float8 '1.1', float8 'inf')` |
| 43 | 42P01 | relation "foo" does not exist | `insert into foo values('bb','cc','dd')` |
| 42 | 42P01 | relation "range_parted" does not exist | `ALTER TABLE range_parted ATTACH PARTITION part1 FOR VALUES FROM (1, 1) TO (1, 10)` |
| 41 | 0A000 | CREATE TABLE … INHERITS is not supported: the storage model has no inheritance hierarchy | `create table minmaxtest1() inherits (minmaxtest)` |
| 41 | 42P01 | relation "mlparted" does not exist | `select attrelid::regclass, attname, attnum from pg_attribute where attname = 'a'  and (attrelid = 'm` |
| 40 | 0A000 | aggregate ORDER BY is not supported | `select avg((select avg(a1.col1 order by (select avg(a2.col2) from tenk1 a3))             from tenk1 ` |
| 38 | 42601 | syntax error at position N: expected Keyword(Table), found Ident("domain") | `create domain mytype as text` |
| 38 | 42P01 | relation "test_missing_target" does not exist | `INSERT INTO test_missing_target VALUES (0, 1, 'XXXX', 'A')` |
| 37 | 42P01 | relation "list_partedN" does not exist | `CREATE TABLE part_2 (LIKE list_parted2)` |
| 37 | 42883 | function pg_input_error_info(unknown, unknown) does not exist | `SELECT * FROM pg_input_error_info('{1,zed}', 'integer[]')` |
| 36 | 42601 | syntax error at position N: expected Keyword(Table), found Keyword(Or) | `create or replace view agg_view1 as   select aggfns(distinct a,b,c)     from (values (1,3,'foo'),(0,` |
| 36 | 42P01 | relation "num_result" does not exist | `DELETE FROM num_result` |
| 36 | 42P01 | relation "datetimes" does not exist | `insert into datetimes values (0, '10:00', '10:00 BST', '-infinity', '-infinity', '-infinity'), (1, '` |
| 33 | 42601 | syntax error at position N: expected ; or end of input, found Ident("within") | `select p, percentile_cont(p) within group (order by x::float8) from generate_series(1,5) x,      (va` |
| 33 | 42601 | a column definition list is only allowed for functions returning "record" | `select * from json_populate_recordset(row(0::int),'[{"a":"1","b":"2"},{"a":"3"}]') q (a text, b text` |
| 30 | 0A000 | expression indexes are not supported: index entries store column values, not computed keys | `create unique index on t_opf (a record_image_ops)` |
| 30 | 42P01 | relation "num_data" does not exist | `INSERT INTO num_data VALUES (0, '0')` |
| 29 | 42601 | syntax error at position N: expected RParen, found LBracket | `INSERT INTO arrtest (a[1:5], b[1:1][1:2][1:2], c, d, f, g)    VALUES ('{1,2,3,4,5}', '{{{0,0},{1,2}}` |
| 29 | 42P01 | relation "toasttest" does not exist | `insert into toasttest values(repeat('1234567890',10000))` |
| 28 | 42P01 | sequence "sequence_testN" does not exist | `SELECT nextval('sequence_test2')` |
| 28 | 42P01 | relation "update_test" does not exist | `INSERT INTO update_test VALUES (5, 10, 'foo')` |
| 27 | 42601 | syntax error at position N: unexpected token after ALTER: Ident("type") | `alter type alter1.ctype set schema alter1` |
