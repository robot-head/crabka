# Verification: ctp-partitioned-foreign-keys

Verdict: root cause CONFIRMED for foreign_key, constraints, triggers. NOT the
primary producer for without_overlaps. Fix locations partly wrong
(partition.rs is metadata only). Count within 3% of the analyst's number, but
the composition differs. Several hidden prerequisites.

## 1. Root cause

Refusal sites (read):
- `crates/pgexec/src/exec.rs:718-723` CREATE TABLE: `if (partition_by.is_some()
  || partition_of.is_some()) && let Some(pending) = pending_foreign_keys.first()
  { return Err(reject_partitioned_foreign_key(...)) }`.
- `crates/pgexec/src/exec.rs:29302-29308` `add_foreign_key_constraint`: refuses
  when `partition::is_partitioned(...)` OR `partition::parent_of(...).is_some()`
  -> an FK added by ALTER TABLE on a *leaf partition* is refused too, though PG
  allows it. Paradox: a plain table with an FK can be ATTACHed and keeps the FK
  (`attach_partition_ops` at 29002 never looks at candidate FKs), and that leaf FK
  is then enforced (diff H40: INSERT (1500,1501) into fk_partitioned_fk fails on
  fk_partitioned_fk_2's own FK).
- `reject_partitioned_foreign_key` at 26473 (message matches the diff).

foreign_key.diff: first partitioned failure is H39 (diff line 748) `ALTER TABLE
fk_partitioned_fk ADD CONSTRAINT ... NOT ENFORCED` -> refused. Confirmed.
Everything from H39..H85 (1050 changed lines) is in the partitioned section.

without_overlaps.diff: the two partitioned-FK blocks (H65-H74, 62 lines) fail
first on `ERROR: foreign keys using PERIOD are not supported`
(`reject_temporal_foreign_key`, exec.rs:26488, called at 25994 BEFORE the
partition check at 722). Primary producer = temporal PERIOD FK root; this root
is only a secondary dependency there. Analyst's files_affected is wrong for it.

constraints.diff H13-H14 (10 lines): `parted_fk_naming` CREATE refused; expected
`dummy_constr_1` clone name on the attached partition. Confirmed A.

triggers.diff H53 (30 lines): `create table parent (a int primary key, f int
references parent) partition by list (a)` refused; expected pg_trigger listing
of 8 RI_ConstraintTrigger rows and `disable trigger all` -> tgenabled 'D'.
Cascade from A, but unblocked it fails on RI trigger rows (see prerequisites).

## 2. Fix locations

Exist: exec.rs reject_partitioned_foreign_key (26473), fk.rs resolve_foreign_key
(239), run_child_check (1815), run_parent_check (1889), find_referencing_rows
(2000), dependents_blocking_table_drop (2321). catalog_rel.rs constraint_row
(2669) hard-codes conparentid=0 (and conenforced=true).

Wrong: `crates/pgexec/src/partition.rs` attach_ops (567) / detach_ops (586) only
write partition metadata keys. The attach logic (index clones, trigger clones,
row validation) is `exec.rs::attach_partition_ops` (29002) and the
`Action::DetachPartition` arm (28925). CREATE TABLE ... PARTITION OF is
`exec.rs::partition_definition` (25300), which clones indexes via
`partition_index_clones`; FKs must be cloned there. Model: 
`trigger.rs::clone_partition_triggers` (2050) / `drop_partition_trigger_clones`.

Also needed:
- `crates/pgcatalog/src/lib.rs` ForeignKey (529): add parent link (conparentid),
  and `serde.rs` serialize/deserialize_foreign_key (1626/1663) with a
  FOREIGN_KEY_VERSION bump (its own version byte, not SCHEMA_VERSION).
- exec.rs `drop_foreign_key_constraint` (29390): 'cannot drop inherited
  constraint'; drop on parent removes clones.
- exec.rs `alter_constraint` (~29150-29200): 'cannot alter constraint ... is
  derived from ...' + propagate to clones.
- exec.rs `Action::ValidateConstraint` (28822): validate clones, skip valid ones.
- fk.rs `violation` (735): parent-side violation must name the ROOT table
  ('on table "fk_partitioned_fk"'), Gres names the leaf (H42/H43).
- exec.rs 943: `drop_table_and_dependents_ops(kv, &table, &dropping, ...)` passes
  `dropping` (named targets) instead of `removed` (targets + partition
  descendants, built at 908-913). So `DROP TABLE fk_notpartitioned_pk,
  fk_partitioned_fk` is refused because the FK lives on partition
  fk_partitioned_fk_2. Independent S-size bug, reachable today; it produces the
  'already exists'/'invalid bound specification' cascade in H44-H46 (~120 lines).
- ONLY refusal message does not exist in the source (grep empty).

## 3. Attribution (whole-block rule)

foreign_key H39-H85 = 1050 lines. Non-A inside that range:
E cross-partition UPDATE row movement (check_partition_constraint exec.rs:5047
refuses moves): H42 6, H74 6, H75 4, H80 18 = 34.
C pg_partition_tree missing: H44 50, H54 19, H55 19, H65 19 = 107 (then RI
trigger rows / conparentid needed).
B FK referencing partitioned PK (plain child): H45 30, H63 44, H66 2, H69 2,
H70 6, H77 2, H79 4, H80 12 = 102.
D NOT ENFORCED: H45 3, H47 1 = 4 (many more lines need D as second gap).
F SET CONSTRAINTS schema.name parser: H59 6.
G DROP SCHEMA CASCADE notice lists partitions: H70 5, H76 3, H78 4, H80 10 = 22.
A = 1050 - 275 = 775. Plus constraints 10, triggers 30 -> 815 primary.
without_overlaps 62 secondary (not counted). Analyst 840: within 3%.

## 4. Hidden prerequisites / fail-longer

- NOT ENFORCED: parser drops it (ConstraintAttributes ast.rs:2882 has no
  enforced field; written_constraint_attributes parser.rs:8076 records it only
  for ALTER CONSTRAINT), ForeignKey has no `enforced`, ALTER CONSTRAINT
  [NOT] ENFORCED refused at exec.rs:29188. Needed by H39, H44, H47.
- RI trigger rows in pg_trigger (none synthesized; grep RI_ConstraintTrigger
  finds nothing in catalog_rel.rs/pgcatalog): foreign_key H44 (3 listings),
  triggers H53; DISABLE TRIGGER ALL must flip them to 'D'.
- pg_partition_tree(regclass) SRF (srf.rs has only pg_partition_ancestors).
- Cross-partition UPDATE row movement incl. 'cannot move tuple across
  partitions when a non-root ancestor ...' (fkpart11, H81-H83) and AFTER
  triggers firing on the delete+insert.
- FK referencing a partitioned PK (B) for self-referential cases (selffk,
  parted_self_fk, triggers' parent) and fkpart5/12.
- exec.rs:943 dropping vs removed bug (S).
- SET CONSTRAINTS fkpart3.fkey parser gap (S).
- DROP SCHEMA CASCADE notice must not list partitions (S/M).
- psql \d 'TABLE "parent" CONSTRAINT' comes from psql's query filtering
  conparentid = 0 over pg_partition_ancestors; conparentid must be right.

## 5. Oracle facts

All quoted messages verified in the oracle .out / diff minus side. Parent-side
violation names the root table, child-side names the leaf.

## Size

Core (catalog link + clone/merge/validate on create-partition/attach/detach +
drop/alter/validate propagation + naming + ONLY/attach-referenced refusals):
XL. XXL only if B (referenced-side clones) and RI trigger rows are folded in.
