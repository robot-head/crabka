-- Q1: UPDATE ... FROM, DELETE ... USING, multi-column SET, target aliases, and
-- PostgreSQL 18's RETURNING OLD/NEW aliases, diffed against PostgreSQL 18.
-- The harness compares rows positionally, so multi-row RETURNING output is read
-- back through an ordered SELECT rather than returned directly.
CREATE TABLE uf_t (id int4, v int4, label text);
CREATE TABLE uf_s (id int4, w int4, tag text);
INSERT INTO uf_t VALUES (1, 10, 'a'), (2, 20, 'b'), (3, 30, 'c'), (4, 40, 'd');
INSERT INTO uf_s VALUES (1, 100, 'x'), (2, 200, 'y'), (9, 900, 'z');
UPDATE uf_t SET v = v + uf_s.w FROM uf_s WHERE uf_t.id = uf_s.id;
SELECT id, v, label FROM uf_t ORDER BY id;
UPDATE uf_t SET label = uf_s.tag FROM uf_s WHERE uf_t.id = uf_s.id AND uf_s.w > 150;
SELECT id, v, label FROM uf_t ORDER BY id;
UPDATE uf_t AS t SET v = t.v + s.w FROM uf_s AS s WHERE t.id = s.id AND s.id = 1;
SELECT id, v, label FROM uf_t ORDER BY id;
-- A join that matches no target row leaves the table untouched.
UPDATE uf_t SET v = 0 FROM uf_s WHERE uf_t.id = uf_s.id AND uf_s.id = 99;
SELECT id, v FROM uf_t ORDER BY id;
-- A derived table as the FROM item.
UPDATE uf_t SET v = d.total FROM (SELECT id, w * 2 AS total FROM uf_s) AS d WHERE uf_t.id = d.id;
SELECT id, v FROM uf_t ORDER BY id;
-- Multi-column SET, both the ROW() and the parenthesised spellings.
UPDATE uf_t SET (v, label) = ROW(77, 'row') WHERE id = 4;
SELECT id, v, label FROM uf_t ORDER BY id;
UPDATE uf_t SET (v, label) = (88, 'pair') WHERE id = 4;
SELECT id, v, label FROM uf_t ORDER BY id;
UPDATE uf_t SET (v, label) = (SELECT w, tag FROM uf_s WHERE id = 9) WHERE id = 4;
SELECT id, v, label FROM uf_t ORDER BY id;
-- A sub-select with no rows assigns NULL to every target column.
UPDATE uf_t SET (v, label) = (SELECT w, tag FROM uf_s WHERE id = 12345) WHERE id = 4;
SELECT id, v, label FROM uf_t ORDER BY id;
UPDATE uf_t SET (v) = (SELECT 5) WHERE id = 4;
SELECT id, v, label FROM uf_t ORDER BY id;
-- Analysis errors: arity mismatch, unknown column, duplicate assignment.
UPDATE uf_t SET (v, label) = ROW(1, 'a', 2) WHERE id = 1;
UPDATE uf_t SET (v, label) = (SELECT 1) WHERE id = 1;
UPDATE uf_t SET nope = 1 WHERE id = 1;
UPDATE uf_t SET v = 1, v = 2 WHERE id = 1;
UPDATE uf_t SET v = 1 FROM uf_s WHERE uf_t.id = nosuch.id;
-- Once aliased, the target's real name is out of scope.
UPDATE uf_t AS t SET v = uf_t.v WHERE t.id = 1;
-- DELETE ... USING.
CREATE TABLE uf_d (id int4, v int4);
INSERT INTO uf_d VALUES (1, 1), (2, 2), (3, 3), (4, 4), (5, 5);
DELETE FROM uf_d USING uf_s WHERE uf_d.id = uf_s.id;
SELECT id, v FROM uf_d ORDER BY id;
DELETE FROM uf_d AS d USING (SELECT 3 AS id) AS k WHERE d.id = k.id RETURNING d.id, d.v;
SELECT id, v FROM uf_d ORDER BY id;
DELETE FROM uf_d USING uf_s WHERE uf_d.id = uf_s.id AND uf_s.id = 4242;
SELECT id, v FROM uf_d ORDER BY id;
-- RETURNING may project the USING/FROM relation's columns too.
UPDATE uf_d SET v = v + uf_s.w FROM uf_s WHERE uf_d.id = uf_s.id AND uf_s.id = 9 RETURNING uf_d.id, uf_d.v, uf_s.w, uf_s.tag;
DELETE FROM uf_d USING uf_s WHERE uf_d.id = uf_s.id AND uf_s.id = 9 RETURNING uf_d.id, uf_s.tag;
SELECT id, v FROM uf_d ORDER BY id;
-- PostgreSQL 18 RETURNING OLD/NEW.
CREATE TABLE uf_r (id int4 PRIMARY KEY, v int4);
INSERT INTO uf_r VALUES (1, 10);
UPDATE uf_r SET v = v + 1 WHERE id = 1 RETURNING v, old.v, new.v;
UPDATE uf_r SET v = v + 1 WHERE id = 1 RETURNING WITH (OLD AS o, NEW AS n) o.v, n.v, n.v - o.v;
UPDATE uf_r SET v = v * 2 WHERE id = 1 RETURNING *;
UPDATE uf_r SET v = v + 1 WHERE id = 1 RETURNING old.*, new.*;
UPDATE uf_r SET v = v + 1 WHERE id = 1 RETURNING WITH (OLD AS before) before.id, before.v;
INSERT INTO uf_r VALUES (2, 20) RETURNING old.id, old.v, new.id, new.v;
DELETE FROM uf_r WHERE id = 2 RETURNING old.id, old.v, new.id, new.v;
SELECT id, v FROM uf_r ORDER BY id;
UPDATE uf_r SET v = v + 1 WHERE id = 1 RETURNING old.nope;
UPDATE uf_r SET v = v + 1 WHERE id = 1 RETURNING WITH (OLD AS o) new.v;
-- A relation actually named `old` shadows the default alias.
CREATE TABLE old (id int4, v int4);
INSERT INTO old VALUES (1, 5);
UPDATE uf_r SET v = uf_r.v + 1 FROM old WHERE uf_r.id = old.id RETURNING uf_r.v, old.v, new.v;
SELECT id, v FROM uf_r ORDER BY id;
-- A SET target cannot be qualified with the relation name.
UPDATE uf_r SET uf_r.v = 1;
UPDATE uf_r AS t SET t.v = 1;
-- An explicit RETURNING image alias is a relation name: it may not collide with
-- a relation in scope or with the other image.
UPDATE uf_r SET v = v RETURNING WITH (OLD AS o, NEW AS o) o.v;
UPDATE uf_r SET v = v RETURNING WITH (OLD AS uf_r) uf_r.v;
UPDATE uf_r AS t SET v = v RETURNING WITH (NEW AS t) t.v;
-- ... and it suppresses the OTHER image's default spelling.
UPDATE uf_r SET v = v + 1 RETURNING WITH (NEW AS old) old.v;
UPDATE uf_r SET v = v + 1 RETURNING WITH (OLD AS new) new.v;
SELECT id, v FROM uf_r ORDER BY id;
-- Repeated SET targets are 42601 in every spelling.
UPDATE uf_r SET v = 1, v = 2;
UPDATE uf_r SET (v, v) = ROW(1, 2);
UPDATE uf_r SET v = 1, (v, id) = ROW(2, 3);
DROP TABLE old;
DROP TABLE uf_r;
DROP TABLE uf_d;
DROP TABLE uf_s;
DROP TABLE uf_t;
