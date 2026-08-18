# Cluster ddl_objects — root-cause triage (2026-08-17, main certified run)

Files (16, 6,097 changed lines): alter_generic 309, alter_operator 135, create_operator 78,
create_cast 34, create_am 295, create_procedure 275, create_function_sql 349, create_type 197,
typed_table 108, create_table_like 377, create_table 495, drop_if_exists 170, object_address 691,
dependency 96, event_trigger 247, alter_table 1841.

Planner-only lines: create_am ~20 (Index Only Scan using gist2), create_function_sql ~15
(SRF inlining EXPLAIN, `Output: voidtest1(33)`), create_table ~14 (pruning to
`Seq Scan on partitioned1 partitioned`), alter_table ~68 (Append over inheritance children
with constraint exclusion; index-scan output order under enable_seqscan=off; view flattening in
EXPLAIN VERBOSE). Total ~117 of 6,097. Every other line has a concrete non-planner root.

## Per-file first failing statement / cascade

| file | first failing stmt | cascade? | primary root |
|---|---|---|---|
| alter_generic | CREATE AGGREGATE alt_agg1 (sfunc1 = int4pl, basetype = int4, stype1 = int4, ...) -> function int4pl(integer, integer) does not exist | partial | many object families |
| alter_operator | CREATE OPERATOR === (... NEGATOR = !== ...) -> negator operator !== does not exist (no shell operators) | whole file | ddl-operator-shell-and-alter |
| create_operator | SELECT @#@ 24 -> operator does not exist: # integer | no | lex-operator-tokens; privileges grammar |
| create_cast | SELECT casttestfunc('foo'::text) after CREATE CAST (text AS casttesttype) WITHOUT FUNCTION AS IMPLICIT | no | ddl-create-cast-with-function |
| create_am | CREATE ACCESS METHOD gist2 TYPE INDEX HANDLER gisthandler -> 0A000 | whole file | ddl-create-access-method |
| create_procedure | CALL ptest1('a') -> only query statements in a procedure body can take the call's arguments | partial | ddl-sql-routine-executor |
| create_function_sql | SELECT pg_get_functiondef('functest_A_1'::regproc) -> NULL | partial | ddl-pg-get-functiondef, ddl-sql-routine-executor |
| create_type | CREATE FUNCTION widget_in(cstring) RETURNS widget -> type widget does not exist | half | ddl-shell-type-autocreate + ddl-create-type-base-attributes |
| typed_table | CREATE TABLE ttable1 OF nothing -> syntax error | whole file | ddl-typed-tables |
| create_table_like | CREATE TABLE inhe (ee text, LIKE inhx) inherits (ctlb) column order | no | ddl-create-table-like |
| create_table | CREATE TABLE unknowntab (u unknown) msg; cascade from PARTITION BY RANGE (plusone(b)) | partition half | partition-expression-keys |
| drop_if_exists | DROP TABLE IF EXISTS test_exists (missing NOTICE) | no | ddl-if-exists-notices |
| object_address | pg_get_object_address missing | yes | ddl-object-address-functions |
| dependency | CREATE GROUP syntax error; DROP USER should fail | yes | ddl-role-owned-objects |
| event_trigger | drop role regress_evt_user should fail | half | ddl-role-owned-objects, ddl-dependency-graph, ddl-event-trigger-fidelity |
| alter_table | ALTER INDEX attmp_idx ALTER COLUMN 0 SET STATISTICS 1000 syntax error | partial; ATTACH half poisoned by tables leaked from create_table | ~35 roots |

Cross-file leaks: create_table's final DROP TABLE parted, ..., range_parted3 fails as a whole
(range_parted3 never created: expression partition key refusal) so list_parted2, range_parted2,
part1/2/3 survive into alter_table (~150 lines). constraints.out leaks atacc1 (+ serial
sequence) into alter_table (~30 lines). alter_table leaks schema alter1 (SET SCHEMA unsupported).

Full root list with fix locations and oracle facts is in the structured output of the agent.
