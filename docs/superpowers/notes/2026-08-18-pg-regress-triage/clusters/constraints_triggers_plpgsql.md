# Cluster constraints_triggers_plpgsql — root-cause triage (2026-08-17)

Files: constraints 668, foreign_key 1300, without_overlaps 450, triggers 984,
plpgsql 1857, domain 898, replica_identity 73 = 6230 changed lines.
Whole-block attribution; numbers are per-hunk estimates (script: hunks.py).

## Per-file summary

| file | first failing stmt | cascade | primary root | planner-only |
|---|---|---|---|---|
| constraints | `CREATE TABLE DEFAULTEXPR_TBL (... i2 int DEFAULT nextval('default_seq'))` -> "sequence functions require a SQL session" | yes (INSERT_TBL block, 131 lines) | ctp-notnull-constraint-catalog (234) | 0 |
| foreign_key | `INSERT INTO FKTABLE VALUES (1, 2)` after `CREATE TABLE FKTABLE (... REFERENCES PKTABLE NOT ENFORCED)` -- NOT ENFORCED ignored | yes (partitioned FK, ~900 lines) | ctp-partitioned-foreign-keys | 8 |
| without_overlaps | `\d temporal_rng` prints `Type -` for range columns (format_type) | PERIOD FK block cascades | ctp-period-temporal-fk (250) + ctp-format-type-range (136) | 0 |
| triggers | `insert into trigtest values(1,'foo')` -> "syntax error at position 0: expected begin" (LANGUAGE C trigger_return_old) | local cascades (DROP COLUMN b allowed -> view block 192; create_table leak 52) | ctp-column-dependency-tracking (192) / ctp-c-regress-trigger-adapter (113) | 0 |
| plpgsql | tg_wslot_biu: `if count(*) = 0 from Room where ...` unparsed; then bpchar `new.backlink != ''` misfires tg_backlink_set | yes: bpchar comparison poisons the whole patchfield schema (~570 lines) | ctp-plpgsql-record-field-types (570) | 45 |
| domain | `comment on domain` unsupported; `create domain d_fail as int4 not null null` silently succeeds | yes: domain-over-composite / arrays of user types (364) | ctp-user-type-arrays-and-domain-over-composite | 6 |
| replica_identity | `CREATE UNIQUE INDEX ... (keya, keyb, (3))` refused | no | ctp-replica-identity (50) | 0 |

## Key fix locations (file that must change)

- ctp-notnull-constraint-catalog: pgcatalog/src/lib.rs `Column.not_null: bool` -> first-class constraint record; pgexec/src/exec.rs `set_column_not_null` (~29233), `no_inherit_not_null_unsupported` (29225), `reject_not_valid` (29092); catalog_rel.rs `check_constraint_rows`/`not_null_constraint_name` (2547-2604).
- ctp-partitioned-foreign-keys: exec.rs `reject_partitioned_foreign_key` (26475); fk.rs `resolve_foreign_key`, `run_child_check`, `find_referencing_rows`, `dependents_blocking_table_drop`; partition.rs attach/detach.
- ctp-fk-referencing-partitioned-pk: fk.rs `run_child_check`/`FkParts::resolve` (1815) do not see rows stored in partitions.
- ctp-period-temporal-fk: exec.rs `reject_temporal_foreign_key` (26489); fk.rs.
- ctp-format-type-range: func.rs `builtin_format_type` (2674).
- ctp-replica-identity: parser.rs `alter_table_action` (3997-4190); exec.rs `Action::Unsupported` (28981); pg_class relreplident (exec.rs 20148).
- ctp-column-default-expression: exec.rs `column_from_ast` (2355), `ensure_default_can_be_persisted` (2794); pgcatalog `ColumnDefault`.
- ctp-user-type-arrays-and-domain-over-composite: pgtypes/src/datum.rs `ElemType` (261), `ColumnType::array_of` (1118); parser.rs 776; usertype.rs `create_domain` (98-106).
- ctp-copy-fires-triggers: session.rs `run_copy_in` (10527) lacks `with_scalar_runtime(.., Some(request_tx))`; trigger.rs `invoke` (132).
- ctp-c-regress-trigger-adapter: routine.rs `STATIC_REGRESS_ENTRYPOINTS`/`call_regression_c_adapter` (4782); trigger.rs `invoke`.
- ctp-plpgsql-record-field-types: plpgsql.rs `rewrite_record_field` (3045); eval.rs `bpchar_to_text_value` (115).
- ctp-plpgsql-expression-sql-tail: pgparser/src/plpgsql.rs `parse_expr_range` (1245) -> parser.rs `parse_expression` (15878).
- ctp-plpgsql-error-context: plpgsql.rs 288/385/425 + pgparser plpgsql AST (no line numbers).
- ctp-trigger-deparse-and-info-schema: catalog_fn.rs 600-705; trigger.rs `when_matches` (1408).
- ctp-whole-row-star-reference: parser.rs `expect_col_id` (510) callers in expr; eval.rs `whole_row_reference` (55).
- ctp-row-order-heap-append: exec.rs `apply_locked_row_update` (10878-10943).
- ctp-detail-failing-row: exec.rs `enforce_not_null` (3240), `enforce_check_constraints` (26688); error.rs 1142-1161.
- ctp-exclusion-constraints-full: parser.rs 7825; exec.rs `enforce_exclusion_constraint` (10249).
- ctp-comment-on-subobjects: parser.rs `comment_on` (8592); exec.rs 31880-31921.
- ctp-alter-table-inherit: parser.rs 4200; exec.rs 25281.
- ctp-fk-misc-semantics: fk.rs `types_are_comparable` (557).
- ctp-domain-integration-gaps: exec.rs `column_type_from_oid` (24973); session.rs 12669; usertype.rs `check_domain` (1357).
- ctp-sql-function-invocation-from-plpgsql: routine.rs `plpgsql_scalar_result_type` (1936).
- ctp-partition-trigger-clone-rules: trigger.rs `drop` (598), `set_table_trigger_mode` (2150).

## Cross-file note

triggers.out hunks 42-47 (52 lines) fail because `create_table` left a table
`parted` behind (its `DROP TABLE parted, ..., range_parted3` failed atomically).
That is a create_table defect, not a triggers one.

## Brief corrections

- foreign_key: the only EXPLAIN (rule r1) is a rules cascade; ~8 planner-only lines.
- plpgsql has ~45 planner-only lines (Gather, Function Scan verbose, Named Tuplestore INFO).
- PG_COMPAT_MATRIX says `ALTER CONSTRAINT` is not in the grammar; parser.rs 4192 has `alter_constraint_action`.
- WITHOUT OVERLAPS PK/UNIQUE already work; what fails is `format_type` for ranges and PERIOD FKs.
- Domain over composite is refused in usertype.rs, not the parser.
- The bpchar problem is not "char type comparison" in general (eval.rs handles it) but lost column types on NEW/OLD fields in PL/pgSQL.
