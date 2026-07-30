-- SP4 UPDATE/DELETE corpus: verifies crabgresql matches PostgreSQL 18 for
-- basic DML on a table with int4 and text columns.
-- Note: SELECT count(*) is intentionally absent — aggregates are out of SP4
-- scope; we use SELECT id ORDER BY id (or SELECT id, name ...) instead.
CREATE TABLE u (id int4, name text);
INSERT INTO u VALUES (1, 'a'), (2, 'b'), (3, 'c');
UPDATE u SET name = 'z' WHERE id > 1;
SELECT id, name FROM u ORDER BY id;
UPDATE u SET id = id + 10;
SELECT id, name FROM u ORDER BY id;
DELETE FROM u WHERE id = 11;
SELECT id, name FROM u ORDER BY id;
DELETE FROM u;
SELECT id FROM u ORDER BY id;
DROP TABLE u;

-- A SET target is `ColId opt_indirection`, so a relation- or schema-qualified
-- target is not a syntax error: PostgreSQL looks for a column named by the
-- FIRST component and does not find one.
CREATE TABLE uq (a int4, b int4);
UPDATE uq SET uq.a = 1;
UPDATE uq SET public.uq.a = 1;
UPDATE uq SET "public".uq.a = 1;
UPDATE uq SET db.public.uq.a = 1;
UPDATE uq SET a = 1;
DROP TABLE uq;
