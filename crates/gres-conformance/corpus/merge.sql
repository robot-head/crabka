-- Q1: MERGE, including PostgreSQL 17's WHEN NOT MATCHED BY SOURCE, MERGE
-- RETURNING, and merge_action(), diffed against PostgreSQL 18.
-- MERGE's RETURNING row order is unspecified, so it is read back through an
-- ordered SELECT wherever more than one row can be produced.
CREATE TABLE mg_t (id int4 PRIMARY KEY, v int4, label text);
CREATE TABLE mg_s (id int4, w int4, tag text);
INSERT INTO mg_t VALUES (1, 10, 'one'), (2, 20, 'two'), (3, 30, 'three');
INSERT INTO mg_s VALUES (1, 100, 'x'), (3, 300, 'y'), (5, 500, 'z');
-- Plain upsert.
MERGE INTO mg_t USING mg_s ON mg_t.id = mg_s.id
  WHEN MATCHED THEN UPDATE SET v = mg_s.w, label = mg_s.tag
  WHEN NOT MATCHED THEN INSERT (id, v, label) VALUES (mg_s.id, mg_s.w, mg_s.tag);
SELECT id, v, label FROM mg_t ORDER BY id;
-- WHEN MATCHED ... AND, DELETE, and DO NOTHING.
MERGE INTO mg_t USING mg_s ON mg_t.id = mg_s.id
  WHEN MATCHED AND mg_s.w >= 500 THEN DELETE
  WHEN MATCHED AND mg_s.w >= 300 THEN UPDATE SET v = mg_t.v + 1
  WHEN MATCHED THEN DO NOTHING;
SELECT id, v, label FROM mg_t ORDER BY id;
-- WHEN NOT MATCHED BY SOURCE (PostgreSQL 17).
INSERT INTO mg_t VALUES (7, 70, 'seven');
MERGE INTO mg_t USING mg_s ON mg_t.id = mg_s.id
  WHEN NOT MATCHED BY SOURCE THEN UPDATE SET label = 'orphan';
SELECT id, v, label FROM mg_t ORDER BY id;
MERGE INTO mg_t USING mg_s ON mg_t.id = mg_s.id
  WHEN NOT MATCHED BY SOURCE AND mg_t.id = 7 THEN DELETE;
SELECT id, v, label FROM mg_t ORDER BY id;
-- Explicit BY TARGET, a target alias, and a subquery source.
MERGE INTO mg_t AS t USING (SELECT 11 AS id, 110 AS w) AS s ON t.id = s.id
  WHEN NOT MATCHED BY TARGET THEN INSERT (id, v, label) VALUES (s.id, s.w, 'sub');
SELECT id, v, label FROM mg_t ORDER BY id;
-- RETURNING and merge_action().
MERGE INTO mg_t AS t USING (SELECT 1 AS id, 999 AS w) AS s ON t.id = s.id
  WHEN MATCHED THEN UPDATE SET v = s.w
  RETURNING merge_action(), t.id, t.v;
MERGE INTO mg_t AS t USING (SELECT 21 AS id, 210 AS w) AS s ON t.id = s.id
  WHEN NOT MATCHED THEN INSERT (id, v, label) VALUES (s.id, s.w, 'ret')
  RETURNING merge_action(), t.id, t.v, t.label;
MERGE INTO mg_t AS t USING (SELECT 21 AS id) AS s ON t.id = s.id
  WHEN MATCHED THEN DELETE
  RETURNING merge_action(), t.id, t.v;
SELECT id, v, label FROM mg_t ORDER BY id;
-- RETURNING may project the source relation and OLD/NEW images too.
MERGE INTO mg_t AS t USING (SELECT 1 AS id, 1000 AS w) AS s ON t.id = s.id
  WHEN MATCHED THEN UPDATE SET v = s.w
  RETURNING merge_action(), s.w, old.v, new.v;
MERGE INTO mg_t AS t USING (SELECT 1 AS id, 1 AS w) AS s ON t.id = s.id
  WHEN MATCHED THEN UPDATE SET v = t.v + s.w
  RETURNING *;
SELECT id, v, label FROM mg_t ORDER BY id;
-- A source row matching nothing with no NOT MATCHED clause is a no-op.
MERGE INTO mg_t USING (SELECT 4242 AS id) AS s ON mg_t.id = s.id
  WHEN MATCHED THEN DELETE;
SELECT count(*) FROM mg_t;
-- INSERT ... DEFAULT VALUES and a defaulted column inside MERGE.
CREATE TABLE mg_d (id int4, note text DEFAULT 'auto');
MERGE INTO mg_d USING (SELECT 1 AS id) AS s ON mg_d.id = s.id
  WHEN NOT MATCHED THEN INSERT (id) VALUES (s.id);
SELECT id, note FROM mg_d ORDER BY id;
MERGE INTO mg_d USING (SELECT 2 AS id) AS s ON mg_d.id = s.id
  WHEN NOT MATCHED THEN INSERT DEFAULT VALUES;
SELECT id, note FROM mg_d ORDER BY id, note;
-- Constraint enforcement still applies to a MERGE insert.
MERGE INTO mg_t USING (SELECT 1 AS id, 1 AS w) AS s ON mg_t.id = s.id + 100000
  WHEN NOT MATCHED THEN INSERT (id, v) VALUES (s.id, s.w);
-- Errors: a clause action that does not fit its match condition, and an unknown
-- column in the ON condition.
MERGE INTO mg_t USING mg_s ON mg_t.id = mg_s.id WHEN MATCHED THEN INSERT (id) VALUES (1);
MERGE INTO mg_t USING mg_s ON mg_t.id = mg_s.nope WHEN MATCHED THEN DELETE;
MERGE INTO mg_t USING mg_s ON mg_t.id = mg_s.id;
SELECT id, v, label FROM mg_t ORDER BY id;
-- A MERGE insert action obeys the plain INSERT arity rule: no column list means
-- the target list is truncated to the VALUES width.
CREATE TABLE mg_w (a int4, b text, c int4);
CREATE TABLE mg_k (k int4);
INSERT INTO mg_k VALUES (1);
MERGE INTO mg_w USING mg_k ON mg_w.a = mg_k.k WHEN NOT MATCHED THEN INSERT VALUES (5, 'five');
SELECT a, b, c FROM mg_w ORDER BY a;
MERGE INTO mg_w USING mg_k ON mg_w.a = mg_k.k WHEN NOT MATCHED THEN INSERT (a) VALUES (7, 'seven');
MERGE INTO mg_w USING mg_k ON mg_w.a = mg_k.k WHEN NOT MATCHED THEN INSERT (a, b) VALUES (8);
-- A SET target of a MERGE update action cannot be relation-qualified either.
MERGE INTO mg_w USING mg_k ON mg_w.a = mg_k.k WHEN MATCHED THEN UPDATE SET mg_w.b = 'x';
-- merge_action() is only legal in a MERGE's RETURNING list.
SELECT merge_action();
SELECT merge_action() FROM mg_w;
DROP TABLE mg_k;
DROP TABLE mg_w;
DROP TABLE mg_d;
DROP TABLE mg_s;
DROP TABLE mg_t;
