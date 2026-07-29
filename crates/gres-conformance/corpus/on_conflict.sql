-- Q1: INSERT ... ON CONFLICT, diffed against PostgreSQL 18.
-- DO NOTHING and DO UPDATE, column and `ON CONSTRAINT` arbiters, `excluded`,
-- action WHERE, RETURNING, NULL keys (NULLS DISTINCT), transactional rollback,
-- and the 42P10/42704/21000/42601/42P01/23502 error cases.
-- The conformance harness runs all statements in a file over ONE persistent
-- connection, so the BEGIN..ROLLBACK span below is a real transaction.
-- Inference predicates (`ON CONFLICT (c) WHERE ...`) need partial indexes and
-- are deferred, so they are absent.
CREATE TABLE oc_t (id int4 PRIMARY KEY, label text, n int4);
INSERT INTO oc_t VALUES (1, 'one', 1);
INSERT INTO oc_t VALUES (1, 'dup', 9);
INSERT INTO oc_t VALUES (1, 'dup', 9) ON CONFLICT DO NOTHING;
SELECT id, label, n FROM oc_t ORDER BY id;
INSERT INTO oc_t VALUES (1, 'dup', 9) ON CONFLICT (id) DO NOTHING;
INSERT INTO oc_t VALUES (2, 'two', 2) ON CONFLICT (id) DO NOTHING;
SELECT id, label, n FROM oc_t ORDER BY id;
INSERT INTO oc_t VALUES (1, 'updated', 5) ON CONFLICT (id) DO UPDATE SET label = excluded.label, n = oc_t.n + excluded.n;
SELECT id, label, n FROM oc_t ORDER BY id;
INSERT INTO oc_t VALUES (1, 'skipped', 100) ON CONFLICT (id) DO UPDATE SET n = excluded.n WHERE oc_t.n < 0;
SELECT id, label, n FROM oc_t ORDER BY id;
INSERT INTO oc_t VALUES (1, 'by constraint', 1) ON CONFLICT ON CONSTRAINT oc_t_pkey DO UPDATE SET label = excluded.label RETURNING id, label;
INSERT INTO oc_t VALUES (3, 'three', 3) ON CONFLICT (id) DO UPDATE SET label = excluded.label RETURNING id, label, n;
INSERT INTO oc_t VALUES (3, 'three again', 33) ON CONFLICT (id) DO UPDATE SET label = excluded.label RETURNING *;
INSERT INTO oc_t VALUES (4, 'four', 4) ON CONFLICT (id) DO UPDATE SET n = oc_t.n + 1 RETURNING id;
SELECT id, label, n FROM oc_t ORDER BY id;
INSERT INTO oc_t VALUES (5, 'five', 5) ON CONFLICT (label) DO NOTHING;
INSERT INTO oc_t VALUES (5, 'five', 5) ON CONFLICT ON CONSTRAINT no_such_constraint DO NOTHING;
INSERT INTO oc_t VALUES (6, 'a', 1), (6, 'b', 2) ON CONFLICT (id) DO UPDATE SET label = excluded.label;
INSERT INTO oc_t VALUES (7, 'a', 1), (7, 'b', 2) ON CONFLICT DO NOTHING;
SELECT id, label, n FROM oc_t ORDER BY id;
INSERT INTO oc_t VALUES (8, 'eight', 8) ON CONFLICT DO UPDATE SET n = 1;
INSERT INTO oc_t VALUES (1, 'ret', 1) ON CONFLICT (id) DO UPDATE SET n = 2 RETURNING excluded.n;
INSERT INTO oc_t VALUES (1, 'ret', 1) ON CONFLICT (id) DO UPDATE SET n = nope;
BEGIN;
INSERT INTO oc_t VALUES (1, 'in txn', 42) ON CONFLICT (id) DO UPDATE SET n = excluded.n;
SELECT id, n FROM oc_t WHERE id = 1;
ROLLBACK;
SELECT id, n FROM oc_t WHERE id = 1;
CREATE TABLE oc_u (id int4, code text UNIQUE, hits int4);
INSERT INTO oc_u VALUES (1, 'alpha', 0);
INSERT INTO oc_u VALUES (2, 'alpha', 0) ON CONFLICT (code) DO UPDATE SET hits = oc_u.hits + 1 RETURNING id, code, hits;
INSERT INTO oc_u VALUES (3, NULL, 0) ON CONFLICT (code) DO NOTHING;
INSERT INTO oc_u VALUES (4, NULL, 0) ON CONFLICT (code) DO NOTHING;
SELECT id, code, hits FROM oc_u ORDER BY id;
CREATE TABLE oc_nn (id int4 PRIMARY KEY, label text NOT NULL);
INSERT INTO oc_nn VALUES (1, 'one');
INSERT INTO oc_nn VALUES (1, NULL) ON CONFLICT DO NOTHING;
SELECT id, label FROM oc_nn ORDER BY id;
DROP TABLE oc_nn;
DROP TABLE oc_u;
DROP TABLE oc_t;
