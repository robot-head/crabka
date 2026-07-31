-- D1: the ALTER TABLE subcommand family, diffed against PostgreSQL 18.4.
-- ADD/DROP COLUMN, SET/DROP NOT NULL, SET/DROP DEFAULT, ALTER TYPE (with and
-- without USING), ADD/DROP/RENAME/VALIDATE CONSTRAINT, RENAME COLUMN (including
-- the stored-view dependency rewrite), SET/RESET storage parameters, OWNER TO,
-- IF EXISTS, and the multi-subcommand comma form.
-- The harness runs every statement over ONE connection, so state accumulates.
CREATE TABLE at_t (id int4, label text);
INSERT INTO at_t VALUES (1, 'x'), (2, NULL);
-- ADD COLUMN, with and without a DEFAULT: existing rows take the default.
ALTER TABLE at_t ADD COLUMN n int4;
SELECT id, label, n FROM at_t ORDER BY id;
ALTER TABLE at_t ADD COLUMN m int4 DEFAULT 7;
SELECT id, label, n, m FROM at_t ORDER BY id;
ALTER TABLE at_t ADD COLUMN m int4;
ALTER TABLE at_t ADD n int4;
ALTER TABLE at_t ADD COLUMN IF NOT EXISTS m int4;
SELECT id, label, n, m FROM at_t ORDER BY id;
INSERT INTO at_t (id, label) VALUES (3, 'z');
SELECT id, label, n, m FROM at_t ORDER BY id;
-- DROP COLUMN.
ALTER TABLE at_t DROP COLUMN n;
SELECT id, label, m FROM at_t ORDER BY id;
SELECT n FROM at_t;
ALTER TABLE at_t DROP COLUMN nope;
ALTER TABLE at_t DROP COLUMN IF EXISTS nope;
ALTER TABLE at_t DROP COLUMN label RESTRICT;
SELECT id, m FROM at_t ORDER BY id;
-- SET / DROP NOT NULL.
ALTER TABLE at_t ALTER COLUMN m SET NOT NULL;
INSERT INTO at_t (id) VALUES (4);
SELECT id, m FROM at_t ORDER BY id;
INSERT INTO at_t (id, m) VALUES (5, NULL);
ALTER TABLE at_t ALTER COLUMN m DROP NOT NULL;
INSERT INTO at_t (id, m) VALUES (5, NULL);
SELECT id, m FROM at_t ORDER BY id;
ALTER TABLE at_t ALTER COLUMN m SET NOT NULL;
DELETE FROM at_t WHERE id = 5;
ALTER TABLE at_t ALTER COLUMN m SET NOT NULL;
ALTER TABLE at_t ALTER COLUMN nope SET NOT NULL;
-- SET / DROP DEFAULT.
ALTER TABLE at_t ALTER COLUMN m SET DEFAULT 42;
INSERT INTO at_t (id) VALUES (6);
SELECT id, m FROM at_t ORDER BY id;
ALTER TABLE at_t ALTER COLUMN m DROP DEFAULT;
INSERT INTO at_t (id) VALUES (7);
SELECT id, m FROM at_t ORDER BY id;
ALTER TABLE at_t ALTER COLUMN m DROP NOT NULL;
INSERT INTO at_t (id) VALUES (7);
SELECT id, m FROM at_t ORDER BY id;
DELETE FROM at_t WHERE id >= 6;
-- ALTER COLUMN TYPE, with an implicit assignment cast and with USING.
CREATE TABLE at_ty (a int4, b text);
INSERT INTO at_ty VALUES (1, '10'), (2, '20');
ALTER TABLE at_ty ALTER COLUMN a TYPE int8;
SELECT a, b FROM at_ty ORDER BY a;
ALTER TABLE at_ty ALTER COLUMN a TYPE text;
SELECT a, b FROM at_ty ORDER BY a;
ALTER TABLE at_ty ALTER COLUMN b TYPE int4 USING b::int4;
SELECT a, b FROM at_ty ORDER BY b;
ALTER TABLE at_ty ALTER COLUMN a SET DATA TYPE int4 USING a::int4;
SELECT a + 1, b + 1 FROM at_ty ORDER BY a;
ALTER TABLE at_ty ALTER COLUMN nope TYPE int4;
-- ADD CONSTRAINT: UNIQUE and PRIMARY KEY.
CREATE TABLE at_c (a int4, b int4);
INSERT INTO at_c VALUES (1, 1), (2, 2);
ALTER TABLE at_c ADD CONSTRAINT at_c_u UNIQUE (a);
INSERT INTO at_c VALUES (1, 3);
INSERT INTO at_c VALUES (3, 3);
SELECT a, b FROM at_c ORDER BY a;
ALTER TABLE at_c ADD CONSTRAINT at_c_pk PRIMARY KEY (b);
INSERT INTO at_c VALUES (4, 1);
INSERT INTO at_c (a) VALUES (5);
SELECT a, b FROM at_c ORDER BY a;
ALTER TABLE at_c ADD PRIMARY KEY (a);
ALTER TABLE at_c DROP CONSTRAINT at_c_u;
INSERT INTO at_c VALUES (1, 9);
SELECT a, b FROM at_c ORDER BY a, b;
ALTER TABLE at_c DROP CONSTRAINT at_c_u;
ALTER TABLE at_c DROP CONSTRAINT IF EXISTS at_c_u;
-- RENAME COLUMN, and the same rename with a dependent view.
CREATE TABLE at_r (a int4, b int4);
INSERT INTO at_r VALUES (1, 2);
ALTER TABLE at_r RENAME COLUMN a TO a2;
SELECT a2, b FROM at_r;
SELECT a FROM at_r;
ALTER TABLE at_r RENAME b TO b2;
SELECT a2, b2 FROM at_r;
ALTER TABLE at_r RENAME COLUMN nope TO other;
ALTER TABLE at_r RENAME COLUMN a2 TO b2;
CREATE VIEW at_v AS SELECT a2, b2 FROM at_r WHERE a2 > 0;
SELECT a2, b2 FROM at_v;
ALTER TABLE at_r RENAME COLUMN a2 TO a3;
SELECT a3, b2 FROM at_r;
SELECT a2, b2 FROM at_v;
INSERT INTO at_r VALUES (5, 6);
SELECT a2, b2 FROM at_v ORDER BY a2;
DROP VIEW at_v;
-- RENAME CONSTRAINT on an index-backed constraint.
ALTER TABLE at_c RENAME CONSTRAINT at_c_pk TO at_c_pk2;
ALTER TABLE at_c DROP CONSTRAINT at_c_pk;
ALTER TABLE at_c DROP CONSTRAINT at_c_pk2;
INSERT INTO at_c VALUES (9, 1);
SELECT a, b FROM at_c ORDER BY a, b;
-- Storage parameters and ownership: accepted, no queryable effect.
ALTER TABLE at_c SET (fillfactor = 70);
ALTER TABLE at_c RESET (fillfactor);
ALTER TABLE at_c SET (fillfactor = 70, autovacuum_enabled = false);
ALTER TABLE at_c RESET (fillfactor, autovacuum_enabled);
SELECT count(*) FROM at_c;
-- Missing relations, with and without IF EXISTS.
ALTER TABLE at_missing ADD COLUMN z int4;
ALTER TABLE IF EXISTS at_missing ADD COLUMN z int4;
ALTER TABLE at_missing RENAME TO at_other;
ALTER TABLE IF EXISTS at_missing RENAME TO at_other;
ALTER TABLE at_missing RENAME COLUMN a TO b;
ALTER TABLE IF EXISTS at_missing RENAME COLUMN a TO b;
-- The multi-subcommand comma form is one atomic statement.
CREATE TABLE at_multi (a int4);
INSERT INTO at_multi VALUES (1);
ALTER TABLE at_multi ADD COLUMN b int4 DEFAULT 2, ADD COLUMN c text DEFAULT 'c';
SELECT a, b, c FROM at_multi;
ALTER TABLE at_multi DROP COLUMN b, ALTER COLUMN c SET DEFAULT 'd';
INSERT INTO at_multi (a) VALUES (2);
SELECT a, c FROM at_multi ORDER BY a;
ALTER TABLE at_multi ADD COLUMN d int4, DROP COLUMN nope;
SELECT a, c FROM at_multi ORDER BY a;
SELECT d FROM at_multi;
ALTER TABLE at_multi ADD CONSTRAINT at_multi_pos CHECK (a > 0), ALTER COLUMN a SET NOT NULL;
INSERT INTO at_multi (a) VALUES (0);
INSERT INTO at_multi (a) VALUES (NULL);
SELECT a, c FROM at_multi ORDER BY a;
-- Renaming a table keeps its columns, defaults and constraints.
ALTER TABLE at_multi RENAME TO at_multi2;
INSERT INTO at_multi2 (a) VALUES (0);
INSERT INTO at_multi2 (a) VALUES (3);
SELECT a, c FROM at_multi2 ORDER BY a;
SELECT a FROM at_multi;
DROP TABLE at_multi2;
DROP TABLE at_c;
DROP TABLE at_r;
DROP TABLE at_ty;
DROP TABLE at_t;
-- ADD COLUMN carrying a constraint on a table that already has rows: the
-- constraint is validated against the rewritten rows, not the stored ones.
CREATE TABLE at_addc (id int4);
INSERT INTO at_addc VALUES (1);
ALTER TABLE at_addc ADD COLUMN c int4 CHECK (c > 0);
SELECT id, c FROM at_addc;
INSERT INTO at_addc VALUES (2, 0);
INSERT INTO at_addc VALUES (2, 5);
SELECT id, c FROM at_addc ORDER BY id;
ALTER TABLE at_addc ADD COLUMN u int4 UNIQUE;
SELECT id, c, u FROM at_addc ORDER BY id;
INSERT INTO at_addc VALUES (3, 1, 9);
INSERT INTO at_addc VALUES (4, 1, 9);
ALTER TABLE at_addc ADD COLUMN d int4 DEFAULT 5 CHECK (d > 0);
SELECT id, d FROM at_addc ORDER BY id;
ALTER TABLE at_addc ADD COLUMN e int4 DEFAULT 0 CHECK (e > 0);
SELECT e FROM at_addc;
CREATE TABLE at_addpk (id int4);
INSERT INTO at_addpk VALUES (1), (2);
ALTER TABLE at_addpk ADD COLUMN k int4 DEFAULT 1 PRIMARY KEY;
DROP TABLE at_addc;
DROP TABLE at_addpk;
-- ALTER COLUMN TYPE re-encodes every index over the column, so an index scan
-- still finds the rows and PRIMARY KEY / UNIQUE still reject duplicates.
CREATE TABLE at_rt (u int4 UNIQUE, v int4);
INSERT INTO at_rt VALUES (1, 1), (2, 2);
CREATE INDEX at_rt_v ON at_rt (v);
ALTER TABLE at_rt ALTER COLUMN v TYPE int8;
SELECT u, v FROM at_rt WHERE v = 2;
ALTER TABLE at_rt ALTER COLUMN u TYPE int8;
SELECT u, v FROM at_rt WHERE u = 1;
INSERT INTO at_rt VALUES (1, 3);
SELECT u, v FROM at_rt ORDER BY u, v;
DROP TABLE at_rt;
CREATE TABLE at_pkt (u int4 PRIMARY KEY, v text);
INSERT INTO at_pkt VALUES (1, 'a'), (2, 'b');
ALTER TABLE at_pkt ALTER COLUMN u TYPE int8;
INSERT INTO at_pkt VALUES (1, 'dup');
SELECT u, v FROM at_pkt ORDER BY u, v;
SELECT u, v FROM at_pkt WHERE u = 1;
DROP TABLE at_pkt;
-- The type rewrite only has to cast rows that are still live.
CREATE TABLE at_dead (a text);
INSERT INTO at_dead VALUES ('1'), ('bad');
DELETE FROM at_dead WHERE a = 'bad';
ALTER TABLE at_dead ALTER COLUMN a TYPE int4 USING a::int4;
SELECT a FROM at_dead;
CREATE TABLE at_dead2 (a text);
INSERT INTO at_dead2 VALUES ('1'), ('bad');
UPDATE at_dead2 SET a = '2' WHERE a = 'bad';
ALTER TABLE at_dead2 ALTER COLUMN a TYPE int4 USING a::int4;
SELECT a FROM at_dead2 ORDER BY a;
DROP TABLE at_dead;
DROP TABLE at_dead2;
-- ALTER COLUMN TYPE follows PostgreSQL's assignment-cast rule.
CREATE TABLE at_cast (a int4, b text, c float8, d bool, e timestamp);
ALTER TABLE at_cast ALTER COLUMN a TYPE text;
ALTER TABLE at_cast ALTER COLUMN c TYPE int4;
ALTER TABLE at_cast ALTER COLUMN e TYPE date;
ALTER TABLE at_cast ALTER COLUMN b TYPE int4;
ALTER TABLE at_cast ALTER COLUMN d TYPE int4;
ALTER TABLE at_cast ALTER COLUMN b TYPE bogus_type;
DROP TABLE at_cast;
-- A type change that would leave a dependent CHECK unresolvable is refused.
CREATE TABLE at_ck (a int4 CHECK (a > 0));
INSERT INTO at_ck VALUES (5);
ALTER TABLE at_ck ALTER COLUMN a TYPE text;
INSERT INTO at_ck VALUES (6);
SELECT a FROM at_ck ORDER BY a;
DROP TABLE at_ck;
-- NOT VALID skips the existing-row scan but still governs every new row.
CREATE TABLE at_nv (a int4);
INSERT INTO at_nv VALUES (-1);
ALTER TABLE at_nv ADD CONSTRAINT at_nv_pos CHECK (a > 0) NOT VALID;
SELECT conname, convalidated FROM pg_constraint WHERE conname = 'at_nv_pos';
INSERT INTO at_nv VALUES (-2);
SELECT a FROM at_nv ORDER BY a;
ALTER TABLE at_nv VALIDATE CONSTRAINT at_nv_pos;
DELETE FROM at_nv;
SELECT pg_get_constraintdef(oid) FROM pg_constraint WHERE conname = 'at_nv_pos';
ALTER TABLE at_nv VALIDATE CONSTRAINT at_nv_pos;
SELECT conname, convalidated FROM pg_constraint WHERE conname = 'at_nv_pos';
SELECT pg_get_constraintdef(oid) FROM pg_constraint WHERE conname = 'at_nv_pos';
INSERT INTO at_nv VALUES (-3);
INSERT INTO at_nv VALUES (3);
SELECT a FROM at_nv ORDER BY a;
DROP TABLE at_nv;
-- A CHECK predicate is analyzed when the constraint is created, so a table can
-- never end up carrying one that fails every write.
CREATE TABLE at_bad (a int4);
ALTER TABLE at_bad ADD CONSTRAINT at_bad_col CHECK (nope > 0);
INSERT INTO at_bad VALUES (1);
ALTER TABLE at_bad ADD CONSTRAINT at_bad_sq CHECK (a IN (SELECT 1));
INSERT INTO at_bad VALUES (2);
ALTER TABLE at_bad ADD CONSTRAINT at_bad_bool CHECK (a);
ALTER TABLE at_bad ADD CONSTRAINT at_bad_agg CHECK (sum(a) > 0);
SELECT a FROM at_bad ORDER BY a;
DROP TABLE at_bad;
-- ADD PRIMARY KEY builds the unique index first, so a duplicate is reported
-- ahead of a NULL.
CREATE TABLE at_pkord (a int4, b int4);
INSERT INTO at_pkord VALUES (1, 1), (1, 2), (NULL, 3);
ALTER TABLE at_pkord ADD PRIMARY KEY (a);
DELETE FROM at_pkord WHERE b = 2;
ALTER TABLE at_pkord ADD PRIMARY KEY (a);
CREATE UNIQUE INDEX at_pkord_b ON at_pkord (b);
DROP TABLE at_pkord;
-- View dependencies are tracked per column.
CREATE TABLE at_dep (a int4, b int4);
INSERT INTO at_dep VALUES (1, 2);
CREATE VIEW at_dep_v AS SELECT a FROM at_dep;
ALTER TABLE at_dep DROP COLUMN b;
SELECT a FROM at_dep_v;
ALTER TABLE at_dep ALTER COLUMN a TYPE int8;
ALTER TABLE at_dep DROP COLUMN a;
DROP TABLE at_dep;
DROP TABLE at_dep CASCADE;
SELECT a FROM at_dep_v;
-- RENAME TO is unaffected by unrelated views, and carries dependent ones along.
CREATE TABLE at_unrel (a int4);
CREATE VIEW at_unrel_v AS SELECT a FROM at_unrel;
CREATE TABLE at_lonely (b int4);
ALTER TABLE at_lonely RENAME TO at_lonely2;
INSERT INTO at_lonely2 VALUES (7);
SELECT b FROM at_lonely2;
INSERT INTO at_unrel VALUES (5);
ALTER TABLE at_unrel RENAME TO at_unrel2;
SELECT a FROM at_unrel_v;
INSERT INTO at_unrel2 VALUES (6);
SELECT a FROM at_unrel_v ORDER BY a;
DROP VIEW at_unrel_v;
DROP TABLE at_unrel2;
DROP TABLE at_lonely2;
-- An index and a sequence are relations, so a missing one is 42P01.
COMMENT ON INDEX at_no_such_index IS 'x';
COMMENT ON SEQUENCE at_no_such_sequence IS 'x';
-- NOT VALID applies only to constraints PostgreSQL can validate lazily.
CREATE TABLE at_nvk (a int4, b int4);
ALTER TABLE at_nvk ADD CONSTRAINT at_nvk_p PRIMARY KEY (a) NOT VALID;
ALTER TABLE at_nvk ADD CONSTRAINT at_nvk_u UNIQUE (b) NOT VALID;
ALTER TABLE at_nvk ADD PRIMARY KEY (a) NOT VALID;
DROP TABLE at_nvk;
-- A generated column depends on every column its expression reads.
CREATE TABLE at_gd (a int4, b int4 GENERATED ALWAYS AS (a * 2) STORED, c int4);
INSERT INTO at_gd (a, c) VALUES (3, 4);
ALTER TABLE at_gd ALTER COLUMN a TYPE int8;
ALTER TABLE at_gd ALTER COLUMN b TYPE int8;
SELECT a, b, c FROM at_gd;
ALTER TABLE at_gd DROP COLUMN a;
ALTER TABLE at_gd DROP COLUMN a CASCADE;
SELECT c FROM at_gd;
SELECT a FROM at_gd;
SELECT b FROM at_gd;
DROP TABLE at_gd;
-- One ALTER TABLE may not retype the same column twice.
CREATE TABLE at_twice (a int4);
ALTER TABLE at_twice ALTER COLUMN a TYPE int8, ALTER COLUMN a TYPE int2;
ALTER TABLE at_twice ALTER COLUMN a TYPE int8;
ALTER TABLE at_twice ALTER COLUMN a TYPE int2;
INSERT INTO at_twice VALUES (5);
SELECT a FROM at_twice;
DROP TABLE at_twice;
-- A view is a relation, so an ALTER TABLE subcommand it does not support names
-- the action rather than claiming the relation is missing.
CREATE TABLE at_vw (id int4);
CREATE VIEW at_vw_v AS SELECT id FROM at_vw;
ALTER TABLE at_vw_v ADD COLUMN q int4;
ALTER TABLE at_vw_v DROP COLUMN id;
ALTER TABLE at_vw_v ALTER COLUMN id TYPE int8;
ALTER TABLE at_vw_v ALTER COLUMN id SET NOT NULL;
ALTER TABLE at_vw_v ALTER COLUMN id DROP NOT NULL;
ALTER TABLE at_vw_v ADD CONSTRAINT at_vw_c CHECK (id > 0);
ALTER TABLE at_vw_v DROP CONSTRAINT at_vw_c;
ALTER TABLE at_vw_v VALIDATE CONSTRAINT at_vw_c;
DROP VIEW at_vw_v;
DROP TABLE at_vw;
