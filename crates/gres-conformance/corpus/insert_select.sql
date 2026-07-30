-- Q1: INSERT ... SELECT, multi-row VALUES with DEFAULT, and DEFAULT VALUES,
-- diffed against PostgreSQL 18.
-- The harness compares rows positionally, so every SELECT orders its output and
-- multi-row RETURNING is read back through an ordered SELECT instead.
CREATE TABLE is_src (id int4, label text, n int4);
CREATE TABLE is_dst (id int4, label text, n int4);
INSERT INTO is_src VALUES (1, 'one', 10), (2, 'two', 20), (3, 'three', 30);
INSERT INTO is_dst SELECT id, label, n FROM is_src ORDER BY id;
SELECT id, label, n FROM is_dst ORDER BY id;
INSERT INTO is_dst (id, n) SELECT id + 10, n * 2 FROM is_src WHERE n > 10 ORDER BY id;
SELECT id, label, n FROM is_dst ORDER BY id;
INSERT INTO is_dst SELECT id + 100, upper(label), n FROM is_src ORDER BY id DESC LIMIT 2;
SELECT id, label, n FROM is_dst ORDER BY id;
DELETE FROM is_dst WHERE id > 3;
INSERT INTO is_dst SELECT 4, 'four', 40 UNION ALL SELECT 5, 'five', 50;
SELECT id, label, n FROM is_dst ORDER BY id;
INSERT INTO is_dst VALUES (6, 'six', 60) RETURNING id, label, n;
INSERT INTO is_dst (id, label) VALUES (7, 'seven') RETURNING id, label, n;
-- The feeding query sees the pre-insert snapshot, so this doubles the table
-- rather than looping.
INSERT INTO is_dst SELECT id + 1000, label, n FROM is_dst ORDER BY id;
SELECT count(*) FROM is_dst;
DELETE FROM is_dst WHERE id > 1000;
-- Arity mismatches between the query and the target column list.
INSERT INTO is_dst (id, label) SELECT id, label, n FROM is_src;
INSERT INTO is_dst (id, label, n) SELECT id, label FROM is_src;
-- Column defaults: omitted columns, explicit DEFAULT, and DEFAULT VALUES.
CREATE TABLE is_def (id int4 DEFAULT 7, label text DEFAULT 'dflt', n int4);
INSERT INTO is_def DEFAULT VALUES;
SELECT id, label, n FROM is_def ORDER BY id;
INSERT INTO is_def VALUES (1, DEFAULT, 1), (DEFAULT, 'set', 2), (3, 'three', DEFAULT);
SELECT id, label, n FROM is_def ORDER BY id, n;
INSERT INTO is_def (label) VALUES ('only label') RETURNING id, label, n;
INSERT INTO is_def VALUES (DEFAULT, DEFAULT, DEFAULT) RETURNING id, label, n;
-- INSERT ... SELECT combined with ON CONFLICT.
CREATE TABLE is_up (id int4 PRIMARY KEY, label text, n int4);
INSERT INTO is_up SELECT id, label, n FROM is_src ORDER BY id;
INSERT INTO is_up SELECT id, label || '!', n + 1 FROM is_src ORDER BY id ON CONFLICT (id) DO NOTHING;
SELECT id, label, n FROM is_up ORDER BY id;
INSERT INTO is_up SELECT id, label || '!', n + 1 FROM is_src ORDER BY id ON CONFLICT (id) DO UPDATE SET label = excluded.label, n = excluded.n;
SELECT id, label, n FROM is_up ORDER BY id;
INSERT INTO is_up SELECT 9, 'nine', 90 ON CONFLICT (id) DO UPDATE SET n = excluded.n RETURNING id, label, n;
INSERT INTO is_up SELECT 9, 'nine again', 99 ON CONFLICT (id) DO UPDATE SET n = excluded.n RETURNING id, label, n;
SELECT id, label, n FROM is_up ORDER BY id;
-- A duplicate key from the feeding query itself is still 23505.
INSERT INTO is_up SELECT 50, 'dup', 1 UNION ALL SELECT 50, 'dup', 2;
SELECT count(*) FROM is_up WHERE id = 50;
-- Zero-row feeding queries insert nothing.
INSERT INTO is_dst SELECT id, label, n FROM is_src WHERE false;
SELECT count(*) FROM is_dst;
-- The standalone TABLE statement, including as an INSERT source and a set
-- operation branch.
TABLE is_src;
CREATE TABLE is_copy (id int4, label text, n int4);
INSERT INTO is_copy TABLE is_src;
SELECT id, label, n FROM is_copy ORDER BY id;
TABLE is_src UNION ALL TABLE is_copy ORDER BY id, label;
WITH t AS (TABLE is_src) SELECT id, n FROM t ORDER BY id;
TABLE is_src ORDER BY id DESC LIMIT 1;
TABLE no_such_table_for_table_stmt;
-- With no explicit column list the implicit target list is truncated to the
-- source width, and the columns past it take their defaults.
CREATE TABLE is_wide (a int4, b text, c int4);
INSERT INTO is_wide SELECT id, label FROM is_src;
INSERT INTO is_wide VALUES (7, 'seven');
INSERT INTO is_wide (a, b) VALUES (8, 'eight');
SELECT a, b, c FROM is_wide ORDER BY a;
-- Too many expressions is an error on both paths; too few is one only when the
-- statement wrote an explicit column list.
INSERT INTO is_wide SELECT id, label, n, 1 FROM is_src;
INSERT INTO is_wide VALUES (9, 'nine', 1, 2);
INSERT INTO is_wide (a, b, c) SELECT id, label FROM is_src;
INSERT INTO is_wide (a, b, c) VALUES (9, 'nine');
INSERT INTO is_wide (a) SELECT id, label FROM is_src;
INSERT INTO is_wide VALUES (9, 'nine'), (10);
SELECT count(*) FROM is_wide;
DROP TABLE is_wide;
DROP TABLE is_copy;
DROP TABLE is_up;
DROP TABLE is_def;
DROP TABLE is_dst;
DROP TABLE is_src;

-- DEFAULT is reserved and PostgreSQL's grammar admits it in any expression,
-- leaving parse analysis to refuse every context but an INSERT value and an
-- UPDATE assignment — so a stray DEFAULT is 42601, not an undefined column.
CREATE TABLE is_dflt (a int4, b int4);
INSERT INTO is_dflt SELECT DEFAULT, 1;
SELECT DEFAULT;
SELECT a, DEFAULT FROM is_dflt;
SELECT * FROM is_dflt WHERE a = DEFAULT;
SELECT a FROM is_dflt ORDER BY DEFAULT;
INSERT INTO is_dflt VALUES (DEFAULT, 1);
UPDATE is_dflt SET a = DEFAULT;
SELECT a, b FROM is_dflt ORDER BY 1;
DROP TABLE is_dflt;
