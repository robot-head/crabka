-- SELECT DISTINCT ON (expr, ...), diffed against PostgreSQL 18.4.
-- Covers the pick-first-per-group semantics, the interaction with ORDER BY
-- (including PostgreSQL's 42P10 when the ON expressions are not a prefix of the
-- sort), the implied sort when there is no ORDER BY, DISTINCT ON over
-- expressions and qualified references, and its composition with WHERE, joins,
-- LIMIT/OFFSET and derived tables.
--
-- Every statement that returns more than one row carries an ORDER BY that fully
-- determines both which row wins each group and the order the groups come out
-- in, so the comparison never depends on either engine's sort stability. The
-- no-ORDER-BY cases are written so each group has exactly one candidate row.

CREATE TABLE q3_don (id int4, grp int4, tag text, val int4);
INSERT INTO q3_don VALUES
  (1, 10, 'a', 100),
  (2, 10, 'b', 300),
  (3, 10, 'c', 200),
  (4, 20, 'a', 50),
  (5, 20, 'b', 50),
  (6, 30, 'z', NULL),
  (7, 30, 'y', 7),
  (8, NULL, 'n', 1),
  (9, NULL, 'm', 2);

-- The basic form: one row per group, chosen by the rest of the ORDER BY.
SELECT DISTINCT ON (grp) grp, val FROM q3_don ORDER BY grp, val;
SELECT DISTINCT ON (grp) grp, val FROM q3_don ORDER BY grp, val DESC;
SELECT DISTINCT ON (grp) grp, id, tag, val FROM q3_don ORDER BY grp, id;
SELECT DISTINCT ON (grp) grp, id, tag, val FROM q3_don ORDER BY grp, id DESC;
SELECT DISTINCT ON (grp) id FROM q3_don ORDER BY grp, id;
SELECT DISTINCT ON (grp) id FROM q3_don ORDER BY grp DESC, id;

-- The group key does not have to be projected at all.
SELECT DISTINCT ON (grp) tag FROM q3_don ORDER BY grp, tag;
SELECT DISTINCT ON (grp) val FROM q3_don ORDER BY grp, val NULLS FIRST;
SELECT DISTINCT ON (grp) val FROM q3_don ORDER BY grp, val NULLS LAST;

-- NULL group keys form one group of their own (NULLs are not distinct here).
SELECT DISTINCT ON (grp) grp, id FROM q3_don WHERE grp IS NULL ORDER BY grp, id;
SELECT DISTINCT ON (grp) grp, id FROM q3_don ORDER BY grp NULLS FIRST, id;
SELECT DISTINCT ON (grp) grp, id FROM q3_don ORDER BY grp DESC NULLS LAST, id;

-- Several ON expressions.
SELECT DISTINCT ON (grp, val) grp, val, id FROM q3_don ORDER BY grp, val, id;
SELECT DISTINCT ON (grp, val) grp, val, id FROM q3_don ORDER BY grp, val, id DESC;
SELECT DISTINCT ON (grp, val) grp, val, id FROM q3_don ORDER BY val, grp, id;
SELECT DISTINCT ON (grp, tag) id FROM q3_don ORDER BY grp, tag, id;

-- Expressions, casts and qualified references as ON keys.
SELECT DISTINCT ON (grp + 0) grp, id FROM q3_don ORDER BY grp + 0, id;
SELECT DISTINCT ON (val % 100) val, id FROM q3_don ORDER BY val % 100, id;
SELECT DISTINCT ON (q3_don.grp) q3_don.grp, q3_don.id FROM q3_don ORDER BY q3_don.grp, q3_don.id;
SELECT DISTINCT ON (d.grp) d.grp, d.id FROM q3_don AS d ORDER BY d.grp, d.id;
SELECT DISTINCT ON (upper(tag)) tag, id FROM q3_don ORDER BY upper(tag), id;
SELECT DISTINCT ON (grp IS NULL) grp IS NULL AS isnull, id FROM q3_don ORDER BY grp IS NULL, id;

-- ON key given as an output ordinal / alias in the ORDER BY.
SELECT DISTINCT ON (grp) grp, id FROM q3_don ORDER BY 1, 2;
SELECT DISTINCT ON (grp) grp AS g, id FROM q3_don ORDER BY g, id;
SELECT DISTINCT ON (grp) grp AS g, id FROM q3_don ORDER BY g DESC, id;

-- No ORDER BY at all: PostgreSQL sorts by the ON keys. Each group here has one
-- candidate row, so the result does not depend on sort stability.
SELECT DISTINCT ON (id) id FROM q3_don;
SELECT DISTINCT ON (grp) grp FROM q3_don;
SELECT DISTINCT ON (grp, val) grp, val FROM q3_don;
SELECT DISTINCT ON (tag) tag FROM q3_don;
SELECT DISTINCT ON (1) 1 AS one;

-- 42P10: the ON expressions must match the leading ORDER BY expressions.
SELECT DISTINCT ON (grp) grp, val FROM q3_don ORDER BY val;
SELECT DISTINCT ON (grp) grp, val FROM q3_don ORDER BY val, grp;
SELECT DISTINCT ON (grp) grp FROM q3_don ORDER BY id;
SELECT DISTINCT ON (grp, val) grp FROM q3_don ORDER BY grp, id;
SELECT DISTINCT ON (grp + 1) grp FROM q3_don ORDER BY grp;
SELECT DISTINCT ON (tag) tag FROM q3_don ORDER BY upper(tag);

-- WHERE filters before the DISTINCT ON.
SELECT DISTINCT ON (grp) grp, id FROM q3_don WHERE id > 3 ORDER BY grp, id;
SELECT DISTINCT ON (grp) grp, id FROM q3_don WHERE val IS NOT NULL ORDER BY grp, id;
SELECT DISTINCT ON (grp) grp, id FROM q3_don WHERE false ORDER BY grp, id;

-- LIMIT / OFFSET apply after the dedup.
SELECT DISTINCT ON (grp) grp, id FROM q3_don ORDER BY grp, id LIMIT 2;
SELECT DISTINCT ON (grp) grp, id FROM q3_don ORDER BY grp, id OFFSET 1;
SELECT DISTINCT ON (grp) grp, id FROM q3_don ORDER BY grp, id LIMIT 2 OFFSET 1;
SELECT DISTINCT ON (grp) grp, id FROM q3_don ORDER BY grp DESC, id LIMIT 1;
SELECT DISTINCT ON (grp) grp, id FROM q3_don ORDER BY grp, id FETCH FIRST 2 ROWS ONLY;

-- Inside a derived table, and over one.
SELECT * FROM (SELECT DISTINCT ON (grp) grp, id FROM q3_don ORDER BY grp, id) t ORDER BY t.grp;
SELECT DISTINCT ON (a) a, b FROM (VALUES (1, 2), (1, 3), (2, 4)) v(a, b) ORDER BY a, b;
SELECT DISTINCT ON (a) a, b FROM (VALUES (1, 2), (1, 3), (2, 4)) v(a, b) ORDER BY a, b DESC;
SELECT DISTINCT ON (a) b FROM (VALUES (1, 2), (1, 3), (2, 4)) v(a, b) ORDER BY a, b DESC;

-- Over a join.
CREATE TABLE q3_don_side (grp int4, label text);
INSERT INTO q3_don_side VALUES (10, 'ten'), (20, 'twenty'), (30, 'thirty');
SELECT DISTINCT ON (d.grp) d.grp, s.label, d.id
  FROM q3_don d JOIN q3_don_side s ON s.grp = d.grp
  ORDER BY d.grp, d.id;
SELECT DISTINCT ON (s.label) s.label, d.id
  FROM q3_don d JOIN q3_don_side s ON s.grp = d.grp
  ORDER BY s.label, d.id DESC;

-- Plain DISTINCT still behaves as before, including with ORDER BY and LIMIT.
SELECT DISTINCT grp FROM q3_don ORDER BY grp;
SELECT DISTINCT grp FROM q3_don ORDER BY grp NULLS FIRST;
SELECT DISTINCT ALL grp FROM q3_don ORDER BY grp;
SELECT ALL grp FROM q3_don ORDER BY grp, id;
SELECT DISTINCT val FROM q3_don ORDER BY val DESC NULLS LAST LIMIT 2;

-- DISTINCT ON is not allowed with a locking clause (0A000).
SELECT DISTINCT ON (grp) grp FROM q3_don ORDER BY grp FOR UPDATE;
SELECT DISTINCT grp FROM q3_don ORDER BY grp FOR SHARE;

-- PostgreSQL's DISTINCT ON / ORDER BY compatibility rule is one-directional:
-- every leading ORDER BY key must be a DISTINCT ON expression, but the ON list
-- may hold expressions the ORDER BY never mentions. Those are appended to the
-- dedup sort with default ASC NULLS LAST semantics, and the survivors are then
-- sorted into the query's own ORDER BY.
SELECT DISTINCT ON (grp, val) grp, val FROM q3_don ORDER BY grp;
SELECT DISTINCT ON (grp, val) grp, val FROM q3_don ORDER BY val, grp;
SELECT DISTINCT ON (grp, tag) grp, tag FROM q3_don ORDER BY grp DESC;
SELECT DISTINCT ON (grp, id) id FROM q3_don ORDER BY grp;
SELECT DISTINCT ON (grp, val, id) grp, val, id FROM q3_don ORDER BY grp, val;
SELECT DISTINCT ON (grp) grp, val FROM q3_don ORDER BY grp NULLS FIRST, val;
SELECT DISTINCT ON (grp, val) grp, val, id FROM q3_don ORDER BY grp DESC, val DESC, id;

-- 42P10 fires once an ORDER BY key has been skipped: both for a later key that
-- IS in the ON list, and for an ON expression that still needs appending.
SELECT DISTINCT ON (grp, val) grp FROM q3_don ORDER BY grp, id;
SELECT DISTINCT ON (grp) grp FROM q3_don ORDER BY val, grp;

-- The ON expressions follow the SQL92 rules: an output ordinal and a bare output
-- alias both name the select-list column they stand for.
SELECT DISTINCT ON (1) grp, id FROM q3_don ORDER BY 1, 2;
SELECT DISTINCT ON (1, 2) grp, id FROM q3_don ORDER BY 1, 2;
SELECT DISTINCT ON (g) grp AS g, id FROM q3_don ORDER BY g, id DESC;
SELECT DISTINCT ON (g) grp AS g, id FROM q3_don ORDER BY g;
SELECT DISTINCT ON (5) grp FROM q3_don ORDER BY 1;
SELECT DISTINCT ON (0) grp FROM q3_don ORDER BY 1;
SELECT DISTINCT ON (-1) grp FROM q3_don ORDER BY 1;
SELECT DISTINCT ON (1.0) grp FROM q3_don;
SELECT DISTINCT ON ('x') grp FROM q3_don;

-- DISTINCT ON over a grouped query dedups the grouped output.
SELECT DISTINCT ON (grp) count(*) FROM q3_don GROUP BY grp ORDER BY grp;
SELECT DISTINCT ON (grp) grp, count(*) FROM q3_don GROUP BY grp ORDER BY grp DESC;
SELECT DISTINCT ON (grp) grp, sum(val) FROM q3_don GROUP BY grp ORDER BY grp, sum(val);

DROP TABLE q3_don_side;
DROP TABLE q3_don;
