-- SP38: set operations — UNION/INTERSECT/EXCEPT [ALL] — diffed against PostgreSQL
-- 18. Every set-op query carries an explicit ORDER BY for deterministic row order.
-- All tables live on one range in the single-engine run (no cross-range set op).
CREATE TABLE a (id int4, label text);
INSERT INTO a VALUES (1, 'x'), (2, 'y'), (2, 'y'), (3, 'z');
CREATE TABLE b (id int4, label text);
INSERT INTO b VALUES (2, 'y'), (3, 'z'), (4, 'w');

-- UNION dedups; UNION ALL keeps duplicates
SELECT id FROM a UNION SELECT id FROM b ORDER BY id;
SELECT id FROM a UNION ALL SELECT id FROM b ORDER BY id;

-- INTERSECT / INTERSECT ALL
SELECT id FROM a INTERSECT SELECT id FROM b ORDER BY id;
SELECT id FROM a INTERSECT ALL SELECT id FROM b ORDER BY id;

-- EXCEPT / EXCEPT ALL
SELECT id FROM a EXCEPT SELECT id FROM b ORDER BY id;
SELECT id FROM a EXCEPT ALL SELECT id FROM b ORDER BY id;

-- multi-column rows; first-branch column names
SELECT id, label FROM a UNION SELECT id, label FROM b ORDER BY id, label;

-- precedence: INTERSECT binds tighter than UNION
SELECT id FROM a UNION SELECT id FROM b INTERSECT SELECT 3 ORDER BY id;

-- result-level LIMIT/OFFSET over the combined output
SELECT id FROM a UNION SELECT id FROM b ORDER BY id LIMIT 2 OFFSET 1;

-- top-N per parenthesized branch
(SELECT id FROM a ORDER BY id LIMIT 1) UNION (SELECT id FROM b ORDER BY id DESC LIMIT 1) ORDER BY id;

-- cross-branch type unification: int4 ∪ int8 → int8
SELECT id FROM a UNION SELECT 9999999999 ORDER BY id;

-- ORDER BY by 1-based position
SELECT id FROM a UNION SELECT id FROM b ORDER BY 1;

-- unknown-literal resolution: a bare NULL / string literal is PG's `unknown`
-- pseudo-type — it takes the other branch's type and is coerced via the cast matrix.
SELECT NULL UNION SELECT 2 ORDER BY 1;
SELECT 1 UNION SELECT '5' ORDER BY 1;
SELECT 'a' UNION SELECT 'b' ORDER BY 1;
SELECT 1.5 UNION SELECT '2' ORDER BY 1;

-- error surface (SQLSTATE matched by the oracle)
SELECT 1 UNION SELECT 1, 2;
-- an explicit ::text is a CONCRETE type (not `unknown`), so int4 vs text → 42804
SELECT 1 UNION SELECT 'x'::text;
-- a bare 'x' is `unknown` → resolves to int4 → the value fails the text→int4 parse,
-- so PG and crabgresql both raise 22P02 (invalid input syntax for type integer)
SELECT 1 UNION SELECT 'x';
