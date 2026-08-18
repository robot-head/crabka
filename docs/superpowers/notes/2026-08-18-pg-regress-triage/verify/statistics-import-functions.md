# Verification: statistics-import-functions (stats_import)

Verdict: root cause CONFIRMED, fix location PARTLY WRONG (read-only catalog_fn.rs is not
where a writing + warning-emitting function can live; relstats.rs has no relpages storage),
attribution REASONABLE (645 vs my 707 ceiling / 445 pure), dependencies INCOMPLETE
(five hidden prerequisites).

## 1. Root cause (diff + oracle + Gres out)

* First divergence is hunk 1 (expected line 20): `pg_catalog.pg_restore_relation_stats(...)`
  -> Gres `ERROR: function pg_restore_relation_stats(...) does not exist` (func.rs:501
  `undefined_function`). CREATE SCHEMA/TYPE/TABLE before it succeed. Not a cascade.
* Gres error census (gres-serial/results/stats_import.out): 39x attribute fn missing,
  19x relation fn missing, 1x pg_clear_relation_stats(...) positional missing,
  4x "does not support named arguments here" (parser.rs:3025), 30x `relation "pg_stats"`,
  1x `pg_catalog.pg_stats`, 5x `pg_statistic`, 4x "current transaction is aborted"
  (pg_locks reads inside BEGIN after the failed restore), 1x `unknown query field type
  oid 300136` (CREATE VIEW over composite column; exec.rs:24973 column_type_from_oid,
  caller execute_ddl exec.rs:1114), 1x `relation "pg_temp.stats_temp" does not exist`
  (regclass on pg_temp alias; catalog_fn.rs:1458 resolve_relation_in_scope does not map
  PG_TEMP_ALIAS -> scope.temp_schema(), unlike relname.rs:291), DROP SCHEMA cascade
  DETAIL order/contents.

## 2. Fix location check

* catalog_fn.rs `CatalogFunc`/`catalog_func`/`eval_catalog` exist (L68/L118/L344) but the
  family is read-only: `eval_resolved`/`eval_catalog_reading` take `&EvalCtx` and read
  `ctx.catalog()`. pg_restore_* must WRITE pg_class/pg_statistic and emit WARNINGs. The
  only precedent for a writing scalar function is `ScalarFunc::SetVal` (func.rs:2094)
  via `ctx.sequence.runtime.kv` (Durable: `kv.write_batch`, Replicated: staged into
  `pending`). No expression-level notice sink exists: `EvalCtx` (clock.rs:51) has
  `sequence`, `notify`, `txn`, `catalog` but no `notice_tx`; the session's
  `notice_tx: mpsc::Sender<PgError>` (session.rs:2686, `plpgsql_notice` L3755) is only
  reachable from session methods. => corrected location: func.rs `ScalarFunc` (or a new
  `stats_fn.rs`) + clock.rs `EvalCtx` gets a notice sender and a catalog-write seam +
  session.rs `eval_ctx()` plumbing.
* parser.rs `positional_from_named` L3012: correct. Static table with only
  `make_interval`; fills missing slots with `0` (wrong default for pg_clear_*, which
  have no defaults).
* relstats.rs: stores only reltuples + relhassubclass (TUPLES_PREFIX / SUBCLASS_PREFIX);
  relpages/relallvisible/relallfrozen are hardcoded `int(0)` in `PgClassRow::build`
  (exec.rs ~20839) and PgClassRow has no such fields. So the "new keyspace" is real
  work in relstats.rs AND exec.rs pg_class_rows/PgClassRow.
* pg_stats / pg_statistic: absent from catalog_rel.rs (only pg_statistic_ext). ANALYZE
  (session.rs run_maintenance ~L5299) writes only `set_reltuples_op` (L5460).
* pg_locks: listed in catalog_rel.rs relation lists but `rows()` falls to
  `_ => Ok(Vec::new())` — always empty.
* Scalar builtin in FROM (`CROSS JOIN LATERAL pg_restore_attribute_stats(...) AS r`):
  srf.rs `plan` L456 `classify(name)` -> 42883 for any non-SRF builtin; only user
  routines get the derived-table path (exec.rs:17745). Needed for the clone section.
* pg_attribute has no rows for user indexes (exec.rs:20874 pg_attribute_rows loops
  tables/views/virtual/builtin-catalog indexes only) — needed by the is_odd/is_odd_clone
  "minus" queries (`JOIN pg_attribute a ON a.attrelid = s.starelid`).
* Lateral cache hazard: exec.rs lateral_join caches by specialized TableExpr; a volatile
  writing function called twice with identical args would run once.

## 3. Recount (whole-block rule; script count_stats_import.py) total 726 / 119 blocks

| cat | lines | blocks | note |
|---|---|---|---|
| A restore/clear fn missing (positional) | 427 | 59 | relation 134/20, attribute 293/39 |
| B named args refused (parser) | 18 | 4 | |
| C pg_stats/pg_statistic readbacks | 220 | 36 | need pg_statistic + restore data; 40 of these are the clone section (also ANALYZE, index attrs, scalar-in-FROM) |
| D pg_class readbacks after restore | 18 | 9 | need restore + stored relpages/relallvisible/relallfrozen |
| G pg_locks after abort | 24 | 4 | cascade of A; then fail longer on empty pg_locks |
| E test_i initial `1|0|0|0` | 2 | 1 | NOT this root: CREATE INDEX relstats |
| F part_parent relpages -1 | 2 | 1 | NOT this root: partitioned relpages -1 |
| H CREATE VIEW composite oid | 1 | 1 | NOT this root |
| I pg_temp regclass | 6 | 1 | restore + pg_temp alias in resolve_relation_in_scope |
| J DROP SCHEMA cascade detail | 8 | 3 | NOT this root (5 of 8 are ordering/part_child listing, 3 relate to missing view) |

Attributable to this root as producer: A+B+D+G+I = 493; with C included (as analyst did)
= 713 minus... precisely 427+18+220+18+24+6 = 713. Pure "function exists" lines: 445.
Analyst 645 is within 30% of 713 (0.90) — reasonable.

## 4. Dependencies / hidden prerequisites

Listed by analyst and confirmed: statistics-analyze-pg-statistic, statistics-pg-class-relstats-columns,
statistics-builtin-named-arguments, statistics-pg-locks-relation-rows.
Missed:
1. Expression-level WARNING/NOTICE seam in EvalCtx (with DETAIL support, client_min_messages filter).
2. Expression-level catalog write seam (Durable write_batch vs Replicated staging, as setval).
3. Builtin scalar function as one-row FROM item (srf.rs from_item / plan fallback), plus `r.*` naming.
4. pg_attribute rows for user index relations (attname 'expr' for expression keys).
5. Argument type naming for warnings ("double precision[]", "real[]", "oid", "text") from static arg types (eval::static_arg_types) — likely exists.
6. pg_temp alias in resolve_relation_in_scope (regclass) — separate 1-line defect but blocks 6 lines here.
7. Array-of-composite / range / text[] input parsing for most_common_vals of `comp`, `arange`, `tags` in the clone section.

## 5. Oracle facts
All quoted messages verified verbatim in self-check-serial/results/stats_import.out
(unrecognized argument name, has type X expected type Y, variadic pairs + HINT,
name at variadic position 5 is null, must not be null, relation ... does not exist,
sequences/views DETAIL, attname/attnum rules, system column, inherited null, pair
rules, incorrect number of elements, one-dimensional, invalid input syntax as WARNING,
histogram_bounds/elem_count_histogram null messages (note: the former has no
`argument` prefix, the latter has), not a range type + DETAIL, could not determine
element type + DETAIL). Extra: `schema "nope" does not exist`; 'version' is an accepted
name; relpages accepts negatives; wrong-typed schemaname/relname yields WARNING then
`must not be null` ERROR. Return `t`/`f`, WARNINGs before the table, pg_clear_* void.
ShareUpdateExclusiveLock on relation and on index's parent — confirmed in pg_locks output.
