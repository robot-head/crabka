-- D7: temporary tables, diffed against PostgreSQL 18.4 within one session.
-- The harness runs every statement over ONE connection, which is exactly the
-- lifetime a temporary relation is defined over, so every row and SQLSTATE
-- below is identical on both engines. Crabka does NOT drop these relations at
-- session end and does not isolate them per session — see PG_COMPAT_MATRIX.md.
CREATE TEMP TABLE tt_a (id int4, label text);
INSERT INTO tt_a VALUES (1, 'one'), (2, 'two');
SELECT id, label FROM tt_a ORDER BY id;
CREATE TEMPORARY TABLE tt_b (id int4 PRIMARY KEY, n int4 DEFAULT 5);
INSERT INTO tt_b (id) VALUES (1);
INSERT INTO tt_b (id) VALUES (1);
INSERT INTO tt_b VALUES (2, 9);
SELECT id, n FROM tt_b ORDER BY id;
-- ON COMMIT PRESERVE ROWS is the default and is accepted explicitly.
CREATE TEMP TABLE tt_c (a int4) ON COMMIT PRESERVE ROWS;
INSERT INTO tt_c VALUES (1);
SELECT a FROM tt_c;
-- GLOBAL/LOCAL are noise words on a temporary table.
CREATE LOCAL TEMPORARY TABLE tt_d (a int4);
INSERT INTO tt_d VALUES (7);
SELECT a FROM tt_d;
-- A temporary table takes part in transactions like any other relation.
BEGIN;
INSERT INTO tt_a VALUES (3, 'three');
SELECT id, label FROM tt_a ORDER BY id;
ROLLBACK;
SELECT id, label FROM tt_a ORDER BY id;
BEGIN;
INSERT INTO tt_a VALUES (4, 'four');
COMMIT;
SELECT id, label FROM tt_a ORDER BY id;
-- Constraints, defaults, indexes and ALTER all behave as on a permanent table.
CREATE TEMP TABLE tt_e (a int4 NOT NULL, b int4 CHECK (b > 0));
INSERT INTO tt_e VALUES (1, 1);
INSERT INTO tt_e VALUES (NULL, 1);
INSERT INTO tt_e VALUES (1, -1);
SELECT a, b FROM tt_e;
ALTER TABLE tt_e ADD COLUMN c text DEFAULT 'z';
SELECT a, b, c FROM tt_e;
CREATE INDEX tt_e_idx ON tt_e (a);
SELECT a, b, c FROM tt_e WHERE a = 1;
DROP INDEX tt_e_idx;
UPDATE tt_e SET b = 3;
SELECT a, b, c FROM tt_e;
DELETE FROM tt_e;
SELECT count(*) FROM tt_e;
-- A temporary table is dropped explicitly like any other relation.
DROP TABLE tt_e;
SELECT a FROM tt_e;
DROP TABLE tt_e;
DROP TABLE IF EXISTS tt_e;
CREATE TEMP TABLE tt_f (a int4);
DROP TABLE tt_f;
CREATE TEMP TABLE tt_f (a int4, b int4);
INSERT INTO tt_f VALUES (1, 2);
SELECT a, b FROM tt_f;
DROP TABLE tt_f;
DROP TABLE tt_d;
DROP TABLE tt_c;
DROP TABLE tt_b;
DROP TABLE tt_a;
