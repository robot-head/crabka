-- D4: CHECK constraints, diffed against PostgreSQL 18.4.
-- Column and table CHECKs, PostgreSQL's default constraint naming, the
-- three-valued NULL rule, enforcement on INSERT/UPDATE, DEFAULT interaction,
-- explicit CONSTRAINT names, DEFERRABLE syntax acceptance, ALTER TABLE
-- back-validation, and the 23514/42710/42704 error cases.
-- The harness runs every statement over ONE connection, so state accumulates.
CREATE TABLE ck_t (a int4, b int4 CHECK (b > 0), CONSTRAINT ck_ab CHECK (a + b < 100));
INSERT INTO ck_t VALUES (1, 1);
SELECT a, b FROM ck_t ORDER BY a;
INSERT INTO ck_t VALUES (1, -1);
INSERT INTO ck_t VALUES (60, 60);
INSERT INTO ck_t VALUES (2, NULL);
SELECT a, b FROM ck_t ORDER BY a;
INSERT INTO ck_t VALUES (NULL, 5);
SELECT a, b FROM ck_t ORDER BY a NULLS LAST;
UPDATE ck_t SET b = -5 WHERE a = 1;
UPDATE ck_t SET b = 7 WHERE a = 1;
SELECT a, b FROM ck_t ORDER BY a NULLS LAST;
UPDATE ck_t SET a = 99 WHERE a = 1;
DELETE FROM ck_t;
SELECT count(*) FROM ck_t;
-- A CHECK that references no column at all.
CREATE TABLE ck_const (a int4, CHECK (1 = 1));
INSERT INTO ck_const VALUES (1);
SELECT a FROM ck_const;
CREATE TABLE ck_false (a int4, CHECK (false));
INSERT INTO ck_false VALUES (1);
SELECT count(*) FROM ck_false;
-- Boolean and text predicates.
CREATE TABLE ck_text (name text CHECK (name <> ''), flag bool CHECK (flag));
INSERT INTO ck_text VALUES ('ok', true);
INSERT INTO ck_text VALUES ('', true);
INSERT INTO ck_text VALUES ('ok', false);
INSERT INTO ck_text VALUES ('ok', NULL);
SELECT name, flag FROM ck_text ORDER BY name, flag NULLS LAST;
-- A CHECK on a column with a DEFAULT: the default is checked too.
CREATE TABLE ck_def (a int4 DEFAULT -1 CHECK (a > 0));
INSERT INTO ck_def DEFAULT VALUES;
INSERT INTO ck_def (a) VALUES (5);
SELECT a FROM ck_def;
-- DEFERRABLE spellings are accepted (PostgreSQL ignores them for CHECK).
CREATE TABLE ck_defer (a int4, CONSTRAINT ck_d CHECK (a > 0) NOT DEFERRABLE INITIALLY IMMEDIATE);
INSERT INTO ck_defer VALUES (1);
INSERT INTO ck_defer VALUES (0);
SELECT a FROM ck_defer;
-- NOT NULL / NULL column constraints alongside CHECK.
CREATE TABLE ck_nn (a int4 NOT NULL CHECK (a > 0), b int4 NULL);
INSERT INTO ck_nn VALUES (1, NULL);
INSERT INTO ck_nn VALUES (NULL, 1);
INSERT INTO ck_nn VALUES (0, 1);
SELECT a, b FROM ck_nn;
-- ALTER TABLE ADD CONSTRAINT ... CHECK back-validates the stored rows.
CREATE TABLE ck_alter (a int4);
INSERT INTO ck_alter VALUES (1), (2), (-3);
ALTER TABLE ck_alter ADD CONSTRAINT ck_pos CHECK (a > 0);
DELETE FROM ck_alter WHERE a < 0;
ALTER TABLE ck_alter ADD CONSTRAINT ck_pos CHECK (a > 0);
INSERT INTO ck_alter VALUES (-1);
SELECT a FROM ck_alter ORDER BY a;
ALTER TABLE ck_alter ADD CONSTRAINT ck_pos CHECK (a > 0);
ALTER TABLE ck_alter DROP CONSTRAINT ck_pos;
INSERT INTO ck_alter VALUES (-1);
SELECT a FROM ck_alter ORDER BY a;
ALTER TABLE ck_alter DROP CONSTRAINT ck_pos;
ALTER TABLE ck_alter DROP CONSTRAINT IF EXISTS ck_pos;
DELETE FROM ck_alter WHERE a < 0;
ALTER TABLE ck_alter ADD CHECK (a > 0);
INSERT INTO ck_alter VALUES (0);
SELECT a FROM ck_alter ORDER BY a;
-- Renaming a CHECK constraint keeps it enforced under the new name.
ALTER TABLE ck_alter RENAME CONSTRAINT ck_alter_a_check TO ck_alter_positive;
INSERT INTO ck_alter VALUES (0);
ALTER TABLE ck_alter DROP CONSTRAINT ck_alter_positive;
INSERT INTO ck_alter VALUES (0);
SELECT a FROM ck_alter ORDER BY a;
-- VALIDATE CONSTRAINT on an already-valid CHECK is a no-op.
CREATE TABLE ck_valid (a int4, CONSTRAINT ck_v CHECK (a >= 0));
ALTER TABLE ck_valid VALIDATE CONSTRAINT ck_v;
INSERT INTO ck_valid VALUES (0);
SELECT a FROM ck_valid;
ALTER TABLE ck_valid VALIDATE CONSTRAINT no_such;
-- CHECK constraints survive a table rename.
ALTER TABLE ck_valid RENAME TO ck_valid2;
INSERT INTO ck_valid2 VALUES (-1);
SELECT a FROM ck_valid2;
DROP TABLE ck_valid2;
DROP TABLE ck_alter;
DROP TABLE ck_nn;
DROP TABLE ck_defer;
DROP TABLE ck_def;
DROP TABLE ck_text;
DROP TABLE ck_false;
DROP TABLE ck_const;
DROP TABLE ck_t;
-- A CHECK predicate is analyzed when the table is created: an unknown column,
-- a subquery, a non-boolean result, an aggregate, and an unresolvable
-- comparison are all rejected before the table exists.
CREATE TABLE ck_bad1 (a int4, CHECK (nope > 0));
CREATE TABLE ck_bad2 (a int4, CHECK (a IN (SELECT 1)));
CREATE TABLE ck_bad3 (a int4, CHECK (a));
CREATE TABLE ck_bad4 (a int4, CHECK (sum(a) > 0));
CREATE TABLE ck_bad5 (a int4, b text, CHECK (a > b));
SELECT count(*) FROM ck_bad1;
SELECT count(*) FROM ck_bad3;
-- NOT VALID on a CREATE TABLE constraint is ignored: a new table has no rows
-- to skip validating, so the constraint is recorded valid.
CREATE TABLE ck_nv_new (a int4, CONSTRAINT ck_nv_new_pos CHECK (a > 0) NOT VALID);
SELECT conname, convalidated FROM pg_constraint WHERE conname = 'ck_nv_new_pos';
INSERT INTO ck_nv_new VALUES (0);
INSERT INTO ck_nv_new VALUES (1);
SELECT a FROM ck_nv_new;
DROP TABLE ck_nv_new;
