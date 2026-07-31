-- Q1: CREATE TABLE ... AS and SELECT ... INTO, diffed against PostgreSQL 18.
CREATE TABLE ct_src (id int4, label text, n int4);
INSERT INTO ct_src VALUES (1, 'one', 10), (2, 'two', 20), (3, 'three', 30);
CREATE TABLE ct_all AS SELECT id, label, n FROM ct_src;
SELECT id, label, n FROM ct_all ORDER BY id;
CREATE TABLE ct_filtered AS SELECT id, n * 2 AS doubled FROM ct_src WHERE n > 10;
SELECT id, doubled FROM ct_filtered ORDER BY id;
-- An explicit column list renames the query's output columns.
CREATE TABLE ct_named (a, b) AS SELECT id, label FROM ct_src;
SELECT a, b FROM ct_named ORDER BY a;
-- A shorter list renames only the columns it covers.
CREATE TABLE ct_partial (a) AS SELECT id, label FROM ct_src;
SELECT a, label FROM ct_partial ORDER BY a;
CREATE TABLE ct_toomany (a, b, c) AS SELECT id, label FROM ct_src;
-- WITH NO DATA creates the table empty; WITH DATA is the default.
CREATE TABLE ct_empty AS SELECT id, label FROM ct_src WITH NO DATA;
SELECT count(*) FROM ct_empty;
INSERT INTO ct_empty VALUES (9, 'nine');
SELECT id, label FROM ct_empty ORDER BY id;
CREATE TABLE ct_data AS SELECT id FROM ct_src WITH DATA;
SELECT id FROM ct_data ORDER BY id;
-- IF NOT EXISTS over an existing relation is a notice, not a 42P07.
CREATE TABLE ct_all AS SELECT 1;
CREATE TABLE IF NOT EXISTS ct_all AS SELECT 1 AS other;
SELECT id, label, n FROM ct_all ORDER BY id;
-- Set operations, VALUES, and a FROM-less SELECT all work as the source query.
CREATE TABLE ct_union AS SELECT id FROM ct_src UNION ALL SELECT id + 100 FROM ct_src;
SELECT id FROM ct_union ORDER BY id;
CREATE TABLE ct_values AS VALUES (1, 'a'), (2, 'b');
SELECT column1, column2 FROM ct_values ORDER BY column1;
CREATE TABLE ct_const AS SELECT 42 AS answer, 'hi' AS greeting;
SELECT answer, greeting FROM ct_const;
CREATE TABLE ct_table AS TABLE ct_src;
SELECT id, label, n FROM ct_table ORDER BY id;
-- A CTE feeding the created table.
CREATE TABLE ct_cte AS WITH big AS (SELECT id, n FROM ct_src WHERE n >= 20) SELECT id, n FROM big;
SELECT id, n FROM ct_cte ORDER BY id;
-- SELECT ... INTO is the same statement under another name.
SELECT id, label INTO ct_into FROM ct_src WHERE id <= 2;
SELECT id, label FROM ct_into ORDER BY id;
SELECT * INTO ct_into_star FROM ct_src;
SELECT id, label, n FROM ct_into_star ORDER BY id;
SELECT count(*) AS total INTO ct_into_agg FROM ct_src;
SELECT total FROM ct_into_agg;
SELECT id INTO ct_into_dup FROM ct_src;
SELECT id INTO ct_into_dup FROM ct_src;
-- The created table is an ordinary table afterwards.
INSERT INTO ct_into VALUES (99, 'ninety-nine');
UPDATE ct_into SET label = 'changed' WHERE id = 1;
DELETE FROM ct_into WHERE id = 2;
SELECT id, label FROM ct_into ORDER BY id;
-- A source query that fails leaves no table behind.
CREATE TABLE ct_bad AS SELECT nope FROM ct_src;
SELECT count(*) FROM ct_bad;
-- A runtime failure while evaluating the source query leaves no relation
-- behind, so the ordinary fix-and-retry works.
CREATE TABLE ct_boom AS SELECT 1 / 0 AS b;
SELECT count(*) FROM ct_boom;
CREATE TABLE ct_div (n int4);
INSERT INTO ct_div VALUES (2), (1), (0);
CREATE TABLE ct_retry AS SELECT 100 / n AS r FROM ct_div;
DELETE FROM ct_div WHERE n = 0;
CREATE TABLE ct_retry AS SELECT 100 / n AS r FROM ct_div;
SELECT r FROM ct_retry ORDER BY r;
-- Two output columns of the same name are refused before anything is created.
CREATE TABLE ct_dup1 AS SELECT id, id FROM ct_src;
SELECT * FROM ct_dup1;
CREATE TABLE ct_dup2 AS SELECT 1 + 1, 2 + 2;
CREATE TABLE ct_dup3 (x, x) AS SELECT id, id + 1 FROM ct_src;
-- A view's output columns must be tellable apart too, so CREATE VIEW applies
-- the same 42701 rule CREATE TABLE AS does, before creating anything.
CREATE VIEW ct_vdup AS SELECT id, id FROM ct_src;
CREATE VIEW ct_vdup2 AS SELECT 1 + 1, 2 + 2 FROM ct_src;
CREATE VIEW ct_vdup3 AS SELECT id AS k, label AS k FROM ct_src;
CREATE VIEW ct_vok AS SELECT id AS k, label AS "K" FROM ct_src;
SELECT k, "K" FROM ct_vok ORDER BY 1;
DROP VIEW ct_vok;

DROP TABLE ct_retry;
DROP TABLE ct_div;
DROP TABLE ct_into_dup;
DROP TABLE ct_into_agg;
DROP TABLE ct_into_star;
DROP TABLE ct_into;
DROP TABLE ct_cte;
DROP TABLE ct_table;
DROP TABLE ct_const;
DROP TABLE ct_values;
DROP TABLE ct_union;
DROP TABLE ct_data;
DROP TABLE ct_empty;
DROP TABLE ct_partial;
DROP TABLE ct_named;
DROP TABLE ct_filtered;
DROP TABLE ct_all;
DROP TABLE ct_src;
