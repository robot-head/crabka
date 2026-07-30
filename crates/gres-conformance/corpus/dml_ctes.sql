-- Q1: data-modifying CTEs, diffed against PostgreSQL 18. Covers the snapshot
-- rule (a data-modifying CTE never sees its own effects), the run-exactly-once
-- rule (even when the body does not reference it), and DML bodies under a WITH.
CREATE TABLE dc_t (id int4, v int4);
CREATE TABLE dc_log (id int4, note text);
INSERT INTO dc_t VALUES (1, 10), (2, 20), (3, 30);
-- The CTE's RETURNING output is what the body sees.
WITH ins AS (INSERT INTO dc_t VALUES (4, 40), (5, 50) RETURNING id, v)
SELECT id, v FROM ins ORDER BY id;
SELECT id, v FROM dc_t ORDER BY id;
-- The body reads the pre-statement snapshot, so it does not see the CTE's rows.
WITH ins AS (INSERT INTO dc_t VALUES (6, 60) RETURNING id)
SELECT count(*) FROM dc_t;
SELECT count(*) FROM dc_t;
-- An unreferenced data-modifying CTE still runs exactly once.
WITH ins AS (INSERT INTO dc_t VALUES (7, 70) RETURNING id) SELECT 1 AS ignored;
SELECT id, v FROM dc_t ORDER BY id;
-- UPDATE and DELETE CTEs.
WITH upd AS (UPDATE dc_t SET v = v + 1 WHERE id <= 2 RETURNING id, v)
SELECT id, v FROM upd ORDER BY id;
SELECT id, v FROM dc_t ORDER BY id;
WITH del AS (DELETE FROM dc_t WHERE id >= 6 RETURNING id, v)
SELECT id, v FROM del ORDER BY id;
SELECT id, v FROM dc_t ORDER BY id;
-- A column alias list on a data-modifying CTE.
WITH del (k, val) AS (DELETE FROM dc_t WHERE id = 5 RETURNING id, v)
SELECT k, val FROM del ORDER BY k;
SELECT id, v FROM dc_t ORDER BY id;
-- A CTE without RETURNING cannot be referenced.
WITH ins AS (INSERT INTO dc_t VALUES (8, 80)) SELECT * FROM ins;
-- ... but it still runs when it is not referenced.
WITH ins AS (INSERT INTO dc_t VALUES (9, 90)) SELECT 1 AS ok;
SELECT id, v FROM dc_t ORDER BY id;
-- A plain CTE and a data-modifying one in the same list, and a later CTE
-- reading an earlier one's RETURNING output.
WITH picked AS (SELECT id FROM dc_t WHERE v >= 40),
     moved AS (DELETE FROM dc_t WHERE id IN (SELECT id FROM picked) RETURNING id, v)
SELECT id, v FROM moved ORDER BY id;
SELECT id, v FROM dc_t ORDER BY id;
-- The classic move-rows idiom: a DELETE CTE feeding an INSERT body.
INSERT INTO dc_t VALUES (11, 110), (12, 120);
WITH moved AS (DELETE FROM dc_t WHERE id >= 11 RETURNING id, v)
INSERT INTO dc_log SELECT id, 'moved' FROM moved ORDER BY id;
SELECT id, note FROM dc_log ORDER BY id;
SELECT id, v FROM dc_t ORDER BY id;
-- A DML body under a WITH whose CTE is a plain query.
WITH src AS (SELECT 20 AS id, 200 AS v)
INSERT INTO dc_t SELECT id, v FROM src;
SELECT id, v FROM dc_t ORDER BY id;
WITH keys AS (SELECT 20 AS id)
UPDATE dc_t SET v = v + 1 WHERE id IN (SELECT id FROM keys);
SELECT id, v FROM dc_t ORDER BY id;
WITH keys AS (SELECT 20 AS id)
DELETE FROM dc_t WHERE id IN (SELECT id FROM keys);
SELECT id, v FROM dc_t ORDER BY id;
-- Two data-modifying CTEs both run, and neither sees the other.
CREATE TABLE dc_a (id int4);
CREATE TABLE dc_b (id int4);
WITH x AS (INSERT INTO dc_a VALUES (1) RETURNING id),
     y AS (INSERT INTO dc_b VALUES (2) RETURNING id)
SELECT (SELECT count(*) FROM x) AS xn, (SELECT count(*) FROM y) AS yn;
SELECT id FROM dc_a ORDER BY id;
SELECT id FROM dc_b ORDER BY id;
-- The whole statement is one transaction: an error in the body discards the
-- CTE's writes too.
WITH ins AS (INSERT INTO dc_a VALUES (99) RETURNING id) SELECT nope FROM ins;
SELECT id FROM dc_a ORDER BY id;
-- MERGE inside a WITH.
WITH m AS (MERGE INTO dc_b USING (SELECT 3 AS id) AS s ON dc_b.id = s.id
             WHEN NOT MATCHED THEN INSERT (id) VALUES (s.id)
             RETURNING merge_action(), dc_b.id)
SELECT * FROM m;
SELECT id FROM dc_b ORDER BY id;
-- One statement is ONE command: a unique key is enforced across every part of
-- it, so a WITH item and the body cannot both write the same key.
CREATE TABLE dc_pk (id int4 PRIMARY KEY, v int4);
WITH i AS (INSERT INTO dc_pk VALUES (50, 1) RETURNING id) INSERT INTO dc_pk VALUES (50, 2);
SELECT id, v FROM dc_pk ORDER BY id, v;
WITH a AS (INSERT INTO dc_pk VALUES (1, 1) RETURNING id),
     b AS (INSERT INTO dc_pk VALUES (1, 2) RETURNING id)
SELECT (SELECT count(*) FROM a) + (SELECT count(*) FROM b) AS n;
SELECT count(*) FROM dc_pk;
INSERT INTO dc_pk VALUES (1, 1), (2, 2);
WITH u AS (UPDATE dc_pk SET id = 5 WHERE id = 1 RETURNING id)
INSERT INTO dc_pk SELECT 5, 9 FROM u;
SELECT id, v FROM dc_pk ORDER BY id;
-- A key a part frees by deleting its row IS available to a later part.
WITH d AS (DELETE FROM dc_pk WHERE id = 1 RETURNING id) INSERT INTO dc_pk SELECT 1, 7 FROM d;
SELECT id, v FROM dc_pk ORDER BY id;
-- A row one part has modified is never modified again by another.
CREATE TABLE dc_ov (id int4);
INSERT INTO dc_ov VALUES (1), (2), (3);
WITH a AS (DELETE FROM dc_ov WHERE id <= 2 RETURNING id),
     b AS (DELETE FROM dc_ov WHERE id >= 2 RETURNING id)
SELECT (SELECT count(*) FROM a) AS na, (SELECT count(*) FROM b) AS nb;
SELECT count(*) FROM dc_ov;
CREATE TABLE dc_m (id int4, v int4);
INSERT INTO dc_m VALUES (1, 10), (2, 20), (3, 30);
WITH a AS (UPDATE dc_m SET v = v + 1 WHERE id <= 2 RETURNING id),
     b AS (UPDATE dc_m SET v = v + 100 WHERE id >= 2 RETURNING id)
SELECT (SELECT count(*) FROM a) AS na, (SELECT count(*) FROM b) AS nb;
SELECT id, v FROM dc_m ORDER BY id;
-- An item nothing demands runs after the body, in reverse list order, so the
-- LAST item wins a row two items would touch.
CREATE TABLE dc_r (id int4, v int4);
INSERT INTO dc_r VALUES (1, 10), (2, 20), (3, 30);
WITH a AS (UPDATE dc_r SET v = v + 1 WHERE id <= 2 RETURNING id),
     b AS (UPDATE dc_r SET v = v + 100 WHERE id >= 2 RETURNING id)
SELECT 0 AS ignored;
SELECT id, v FROM dc_r ORDER BY id;
CREATE TABLE dc_u (id int4 PRIMARY KEY, v int4);
INSERT INTO dc_u VALUES (1, 1), (2, 2);
WITH a AS (UPDATE dc_u SET v = 10 WHERE id = 1 RETURNING id)
INSERT INTO dc_u VALUES (1, 99) ON CONFLICT (id) DO UPDATE SET v = 100;
SELECT id, v FROM dc_u ORDER BY id;
-- A demanded item runs first, so the upsert and the MERGE below find the row
-- already modified by this command: PostgreSQL's 21000.
WITH a AS (UPDATE dc_u SET v = 11 WHERE id = 1 RETURNING id)
INSERT INTO dc_u SELECT 1, 99 FROM a ON CONFLICT (id) DO UPDATE SET v = 100;
WITH a AS (UPDATE dc_u SET v = 12 WHERE id = 1 RETURNING id)
MERGE INTO dc_u USING (SELECT id AS k FROM a) s ON dc_u.id = s.k
  WHEN MATCHED THEN UPDATE SET v = 50;
SELECT id, v FROM dc_u ORDER BY id;
DROP TABLE dc_u;
DROP TABLE dc_r;
DROP TABLE dc_m;
DROP TABLE dc_ov;
DROP TABLE dc_pk;
DROP TABLE dc_b;
DROP TABLE dc_a;
DROP TABLE dc_log;
DROP TABLE dc_t;
