-- D6: FOREIGN KEY constraints, diffed against PostgreSQL 18.4.
-- DDL validation and its SQLSTATEs, child- and parent-side enforcement, all five
-- referential actions on both sides, MATCH SIMPLE/FULL, NOT VALID and VALIDATE,
-- DEFERRABLE/INITIALLY DEFERRED with SET CONSTRAINTS, TRUNCATE interaction,
-- dependency refusals, and the catalog/information_schema projections.
-- The harness runs every statement over ONE connection, so state accumulates.
--
-- Adding this file moves one statement in pg_catalog.sql from match to mismatch,
-- and that is the correct outcome rather than something to paper over. Every
-- foreign key here makes PostgreSQL create internal referential-integrity
-- triggers -- 96 of them, all tgisinternal -- so pg_catalog.sql's
-- `SELECT count(*) FROM pg_catalog.pg_trigger` answers 96 on the oracle and 0
-- here, Gres having no trigger machinery at all. The divergence was always true;
-- it was unobservable only because nothing in the corpus had created a foreign
-- key before. Narrowing that probe to `WHERE NOT tgisinternal` would hide a real
-- difference to keep a number green, so the mismatch stands and baseline.json
-- carries it.

-- ---------------------------------------------------------------- DDL errors
CREATE TABLE fk_p (id int4 PRIMARY KEY, txt text);
CREATE TABLE fk_nopk (a int4);
CREATE TABLE fk_uniq (a int4);
CREATE UNIQUE INDEX fk_uniq_a_idx ON fk_uniq (a);
CREATE VIEW fk_v AS SELECT 1 AS a;
-- referenced relation missing (42P01)
CREATE TABLE fk_e1 (a int4 REFERENCES fk_nosuch(id));
-- referenced relation is not a table (42809)
CREATE TABLE fk_e2 (a int4 REFERENCES fk_v(a));
-- no unique constraint matching the referenced columns (42830)
CREATE TABLE fk_e3 (a int4 REFERENCES fk_nopk(a));
-- referencing/referenced column count disagree (42830)
CREATE TABLE fk_comp_p (x int4, y int4, PRIMARY KEY (x, y));
CREATE TABLE fk_e4 (a int4, FOREIGN KEY (a) REFERENCES fk_comp_p(x, y));
-- duplicate in the referenced-column list (42830)
CREATE TABLE fk_e5 (a int4, b int4, FOREIGN KEY (a, b) REFERENCES fk_comp_p(x, x));
-- incompatible types (42804)
CREATE TABLE fk_e6 (a text REFERENCES fk_p(id));
-- unknown referencing column (42703)
CREATE TABLE fk_e7 (a int4, FOREIGN KEY (nope) REFERENCES fk_p(id));
-- MATCH PARTIAL is PostgreSQL's own 0A000
CREATE TABLE fk_e8 (a int4, b int4, FOREIGN KEY (a, b) REFERENCES fk_comp_p(x, y) MATCH PARTIAL);
-- ON DELETE SET NULL naming a column outside the key (42P10)
CREATE TABLE fk_e9 (a int4, b int4, FOREIGN KEY (a) REFERENCES fk_p(id) ON DELETE SET NULL (b));
-- a column list is only allowed on ON DELETE (0A000)
CREATE TABLE fk_e10 (a int4, FOREIGN KEY (a) REFERENCES fk_p(id) ON UPDATE SET NULL (a));
-- an FK may target a bare unique index
CREATE TABLE fk_uniq_c (a int4 REFERENCES fk_uniq(a));
-- a constraint name colliding within the relation (42710)
CREATE TABLE fk_e11 (a int4, CONSTRAINT fk_dup CHECK (a > 0), CONSTRAINT fk_dup FOREIGN KEY (a) REFERENCES fk_p(id));

-- ------------------------------------------------------- child-side checking
CREATE TABLE fk_c (a int4 REFERENCES fk_p(id), note text);
INSERT INTO fk_p VALUES (1, 'one');
INSERT INTO fk_p VALUES (2, 'two');
INSERT INTO fk_c VALUES (1, 'ok');
-- 23503: the key is not present
INSERT INTO fk_c VALUES (9, 'bad');
-- NULL satisfies the constraint
INSERT INTO fk_c VALUES (NULL, 'null is fine');
SELECT a, note FROM fk_c ORDER BY a NULLS LAST;
UPDATE fk_c SET a = 2 WHERE note = 'ok';
UPDATE fk_c SET a = 9 WHERE note = 'ok';
SELECT a, note FROM fk_c ORDER BY a NULLS LAST;

-- ------------------------------------------- checks fire after the statement
-- A self-referencing FK with no DEFERRABLE clause: PostgreSQL's RI is an AFTER
-- ROW trigger, so the row exists by the time the check runs.
CREATE TABLE fk_self (id int4 PRIMARY KEY, boss int4 REFERENCES fk_self(id));
INSERT INTO fk_self (id, boss) VALUES (1, 1);
INSERT INTO fk_self (id, boss) VALUES (2, NULL), (3, 2);
SELECT id, boss FROM fk_self ORDER BY id;
INSERT INTO fk_self (id, boss) VALUES (4, 99);

-- ----------------------------------------------------- composite and MATCH
INSERT INTO fk_comp_p VALUES (1, 2);
CREATE TABLE fk_match_c (a int4, b int4, FOREIGN KEY (a, b) REFERENCES fk_comp_p(x, y));
INSERT INTO fk_match_c VALUES (1, 2);
-- MATCH SIMPLE: a partial NULL passes without probing
INSERT INTO fk_match_c VALUES (1, NULL);
INSERT INTO fk_match_c VALUES (3, 4);
SELECT a, b FROM fk_match_c ORDER BY a, b NULLS LAST;
CREATE TABLE fk_full_c (a int4, b int4, FOREIGN KEY (a, b) REFERENCES fk_comp_p(x, y) MATCH FULL);
INSERT INTO fk_full_c VALUES (1, 2);
-- MATCH FULL rejects mixed NULLs
INSERT INTO fk_full_c VALUES (1, NULL);
-- MATCH FULL accepts all-NULL
INSERT INTO fk_full_c VALUES (NULL, NULL);
SELECT a, b FROM fk_full_c ORDER BY a NULLS LAST, b NULLS LAST;

-- ------------------------------------------- permuted composite column order
CREATE TABLE fk_perm_p (x int4, y int4, PRIMARY KEY (x, y));
CREATE TABLE fk_perm_c (a int4, b int4, FOREIGN KEY (b, a) REFERENCES fk_perm_p(y, x));
INSERT INTO fk_perm_p VALUES (1, 2);
-- (a,b)=(1,2) pairs b->y=2, a->x=1, so it needs (x,y)=(1,2)
INSERT INTO fk_perm_c VALUES (1, 2);
-- (a,b)=(2,1) pairs b->y=1, a->x=2, so it needs (x,y)=(2,1), which is absent
INSERT INTO fk_perm_c VALUES (2, 1);
SELECT a, b FROM fk_perm_c ORDER BY a;

-- ------------------------------------------------------ parent-side, default
CREATE TABLE fk_na_p (id int4 PRIMARY KEY);
CREATE TABLE fk_na_c (a int4 REFERENCES fk_na_p(id));
INSERT INTO fk_na_p VALUES (1);
INSERT INTO fk_na_c VALUES (1);
-- 23503: still referenced
DELETE FROM fk_na_p WHERE id = 1;
UPDATE fk_na_p SET id = 5 WHERE id = 1;
-- deleting the child first frees the parent
DELETE FROM fk_na_c;
DELETE FROM fk_na_p;
SELECT count(*) FROM fk_na_p;

-- ------------------------------------------------------- RESTRICT vs NO ACTION
-- RESTRICT reports 23001 where NO ACTION reports 23503, and RESTRICT triggers
-- are never deferred. The re-supply idiom is a property of DEFERRAL, not of
-- NO ACTION: while the constraint is immediate both modes fail at the DELETE,
-- because the check fires at end of statement before the key comes back.
CREATE TABLE fk_re_p (id int4 PRIMARY KEY);
CREATE TABLE fk_re_c (a int4 REFERENCES fk_re_p(id) ON DELETE RESTRICT);
INSERT INTO fk_re_p VALUES (1);
INSERT INTO fk_re_c VALUES (1);
DELETE FROM fk_re_p;
-- immediate NO ACTION also fails at the DELETE, not at COMMIT
CREATE TABLE fk_noact_p (id int4 PRIMARY KEY);
CREATE TABLE fk_noact_c (a int4 REFERENCES fk_noact_p(id));
INSERT INTO fk_noact_p VALUES (1);
INSERT INTO fk_noact_c VALUES (1);
BEGIN;
DELETE FROM fk_noact_p;
ROLLBACK;
-- deferred NO ACTION re-probes at COMMIT and accepts the re-supplied key
CREATE TABLE fk_dna_p (id int4 PRIMARY KEY);
CREATE TABLE fk_dna_c (a int4 REFERENCES fk_dna_p(id) DEFERRABLE INITIALLY DEFERRED);
INSERT INTO fk_dna_p VALUES (1);
INSERT INTO fk_dna_c VALUES (1);
BEGIN;
DELETE FROM fk_dna_p;
INSERT INTO fk_dna_p VALUES (1);
COMMIT;
SELECT count(*) FROM fk_dna_p;
-- deferred RESTRICT ignores the deferral and still fires at end of statement
CREATE TABLE fk_dre_p (id int4 PRIMARY KEY);
CREATE TABLE fk_dre_c (a int4 REFERENCES fk_dre_p(id) ON DELETE RESTRICT DEFERRABLE INITIALLY DEFERRED);
INSERT INTO fk_dre_p VALUES (1);
INSERT INTO fk_dre_c VALUES (1);
BEGIN;
DELETE FROM fk_dre_p;
ROLLBACK;
SELECT count(*) FROM fk_dre_p;

-- --------------------------------------------------------- ON DELETE CASCADE
CREATE TABLE fk_cas_p (id int4 PRIMARY KEY);
CREATE TABLE fk_cas_c (a int4 REFERENCES fk_cas_p(id) ON DELETE CASCADE, tag text);
INSERT INTO fk_cas_p VALUES (1), (2);
INSERT INTO fk_cas_c VALUES (1, 'x'), (1, 'y'), (2, 'z');
DELETE FROM fk_cas_p WHERE id = 1;
SELECT a, tag FROM fk_cas_c ORDER BY a, tag;

-- --------------------------------------------------------- ON UPDATE CASCADE
CREATE TABLE fk_upd_p (id int4 PRIMARY KEY);
CREATE TABLE fk_upd_c (a int4 REFERENCES fk_upd_p(id) ON UPDATE CASCADE);
INSERT INTO fk_upd_p VALUES (1);
INSERT INTO fk_upd_c VALUES (1);
UPDATE fk_upd_p SET id = 7 WHERE id = 1;
SELECT a FROM fk_upd_c ORDER BY a;

-- ------------------------------------------------------------- ON DELETE SET
CREATE TABLE fk_sn_p (id int4 PRIMARY KEY);
CREATE TABLE fk_sn_c (a int4 REFERENCES fk_sn_p(id) ON DELETE SET NULL);
INSERT INTO fk_sn_p VALUES (1);
INSERT INTO fk_sn_c VALUES (1);
DELETE FROM fk_sn_p;
SELECT a FROM fk_sn_c;
-- SET NULL onto a NOT NULL column is 23502
CREATE TABLE fk_nn_p (id int4 PRIMARY KEY);
CREATE TABLE fk_nn_c (a int4 NOT NULL REFERENCES fk_nn_p(id) ON DELETE SET NULL);
INSERT INTO fk_nn_p VALUES (1);
INSERT INTO fk_nn_c VALUES (1);
DELETE FROM fk_nn_p;
-- SET DEFAULT substitutes the column default, which must itself be present
CREATE TABLE fk_sd_p (id int4 PRIMARY KEY);
CREATE TABLE fk_sd_c (a int4 DEFAULT 0 REFERENCES fk_sd_p(id) ON DELETE SET DEFAULT);
INSERT INTO fk_sd_p VALUES (0), (1);
INSERT INTO fk_sd_c VALUES (1);
DELETE FROM fk_sd_p WHERE id = 1;
SELECT a FROM fk_sd_c;
-- the substituted default has no parent, so the re-check fails
CREATE TABLE fk_sd2_p (id int4 PRIMARY KEY);
CREATE TABLE fk_sd2_c (a int4 DEFAULT 42 REFERENCES fk_sd2_p(id) ON DELETE SET DEFAULT);
INSERT INTO fk_sd2_p VALUES (1);
INSERT INTO fk_sd2_c VALUES (1);
DELETE FROM fk_sd2_p;

-- ---------------------------------------------------- NOT VALID and VALIDATE
CREATE TABLE fk_bv_p (id int4 PRIMARY KEY);
CREATE TABLE fk_bv_c (a int4);
INSERT INTO fk_bv_c VALUES (7);
-- back-validation fails on the existing row
ALTER TABLE fk_bv_c ADD CONSTRAINT fk_bv FOREIGN KEY (a) REFERENCES fk_bv_p(id);
-- NOT VALID skips the scan
ALTER TABLE fk_bv_c ADD CONSTRAINT fk_bv FOREIGN KEY (a) REFERENCES fk_bv_p(id) NOT VALID;
-- but still governs new writes
INSERT INTO fk_bv_c VALUES (8);
-- VALIDATE runs the skipped scan
ALTER TABLE fk_bv_c VALIDATE CONSTRAINT fk_bv;
DELETE FROM fk_bv_c;
INSERT INTO fk_bv_p VALUES (7);
INSERT INTO fk_bv_c VALUES (7);
ALTER TABLE fk_bv_c VALIDATE CONSTRAINT fk_bv;
SELECT a FROM fk_bv_c ORDER BY a;

-- ------------------------------------------------------------ deferrability
CREATE TABLE fk_d1 (id int4 PRIMARY KEY, r int4);
CREATE TABLE fk_d2 (id int4 PRIMARY KEY, r int4);
ALTER TABLE fk_d1 ADD CONSTRAINT fk_d1_r FOREIGN KEY (r) REFERENCES fk_d2(id) DEFERRABLE INITIALLY DEFERRED;
ALTER TABLE fk_d2 ADD CONSTRAINT fk_d2_r FOREIGN KEY (r) REFERENCES fk_d1(id) DEFERRABLE INITIALLY DEFERRED;
-- a circular pair commits when each satisfies the other by COMMIT
BEGIN;
INSERT INTO fk_d1 VALUES (1, 1);
INSERT INTO fk_d2 VALUES (1, 1);
COMMIT;
SELECT count(*) FROM fk_d1;
-- a deferred violation surfaces at COMMIT and the row is not committed
BEGIN;
INSERT INTO fk_d1 VALUES (2, 99);
COMMIT;
SELECT count(*) FROM fk_d1;
-- SET CONSTRAINTS ... IMMEDIATE drains mid-transaction
BEGIN;
SET CONSTRAINTS ALL DEFERRED;
INSERT INTO fk_d1 VALUES (3, 98);
SET CONSTRAINTS ALL IMMEDIATE;
ROLLBACK;
SELECT count(*) FROM fk_d1;
-- naming a constraint that does not exist, and one that is not deferrable
BEGIN;
SET CONSTRAINTS fk_nosuch DEFERRED;
ROLLBACK;
BEGIN;
SET CONSTRAINTS fk_na_c_a_fkey DEFERRED;
ROLLBACK;

-- ------------------------------------------------------------------ TRUNCATE
CREATE TABLE fk_tr_p (id int4 PRIMARY KEY);
CREATE TABLE fk_tr_c (a int4 REFERENCES fk_tr_p(id));
INSERT INTO fk_tr_p VALUES (1);
INSERT INTO fk_tr_c VALUES (1);
-- refused while a referencing table is outside the truncate set
TRUNCATE fk_tr_p;
-- naming both together succeeds
TRUNCATE fk_tr_p, fk_tr_c;
SELECT count(*) FROM fk_tr_p;
SELECT count(*) FROM fk_tr_c;
-- CASCADE widens the set rather than firing ON DELETE actions
INSERT INTO fk_tr_p VALUES (1);
INSERT INTO fk_tr_c VALUES (1);
TRUNCATE fk_tr_p CASCADE;
SELECT count(*) FROM fk_tr_c;

-- ------------------------------------------------------------- dependencies
CREATE TABLE fk_dep_p (id int4 PRIMARY KEY);
CREATE TABLE fk_dep_c (a int4 REFERENCES fk_dep_p(id));
-- 2BP01: the referenced table is depended on
DROP TABLE fk_dep_p;
-- dropping the child is always allowed, and then the parent is free
DROP TABLE fk_dep_c;
DROP TABLE fk_dep_p;
-- dropping the index an FK targets is refused
DROP INDEX fk_uniq_a_idx;
-- dropping the constraint releases the dependency
ALTER TABLE fk_uniq_c DROP CONSTRAINT fk_uniq_c_a_fkey;
DROP INDEX fk_uniq_a_idx;

-- ---------------------------------------------------------------- catalog
CREATE TABLE fk_cat_p (id int4 PRIMARY KEY, k int4 UNIQUE);
CREATE TABLE fk_cat_c (
  a int4 REFERENCES fk_cat_p(id),
  b int4, c int4,
  CONSTRAINT fk_cat_full FOREIGN KEY (b) REFERENCES fk_cat_p(id) MATCH FULL ON UPDATE CASCADE ON DELETE SET NULL,
  CONSTRAINT fk_cat_def FOREIGN KEY (c) REFERENCES fk_cat_p(k) ON DELETE SET DEFAULT DEFERRABLE INITIALLY DEFERRED
);
ALTER TABLE fk_cat_c ADD CONSTRAINT fk_cat_nv FOREIGN KEY (a) REFERENCES fk_cat_p(id) ON DELETE RESTRICT NOT VALID;
SELECT conname, contype, condeferrable, condeferred, convalidated, confupdtype, confdeltype, confmatchtype, conkey, confkey, confdelsetcols FROM pg_catalog.pg_constraint WHERE conrelid = 'fk_cat_c'::regclass AND contype = 'f' ORDER BY conname;
SELECT conname, pg_catalog.pg_get_constraintdef(oid) FROM pg_catalog.pg_constraint WHERE conrelid = 'fk_cat_c'::regclass AND contype = 'f' ORDER BY conname;
SELECT constraint_name, unique_constraint_name, match_option, update_rule, delete_rule FROM information_schema.referential_constraints WHERE constraint_name LIKE 'fk_cat%' ORDER BY constraint_name;
SELECT constraint_name, unique_constraint_name FROM information_schema.referential_constraints WHERE constraint_name = 'fk_uniq_c_a_fkey';
SELECT constraint_name, column_name, ordinal_position, position_in_unique_constraint FROM information_schema.key_column_usage WHERE constraint_name = 'fk_cat_full' ORDER BY ordinal_position;
SELECT constraint_name, table_name, column_name FROM information_schema.constraint_column_usage WHERE constraint_name = 'fk_cat_full' ORDER BY column_name;
SELECT constraint_name, constraint_type, is_deferrable, initially_deferred FROM information_schema.table_constraints WHERE constraint_name LIKE 'fk_cat%' AND constraint_type = 'FOREIGN KEY' ORDER BY constraint_name;
SELECT pg_catalog.pg_get_constraintdef(oid) FROM pg_catalog.pg_constraint WHERE conrelid = 'fk_perm_c'::regclass AND contype = 'f';
