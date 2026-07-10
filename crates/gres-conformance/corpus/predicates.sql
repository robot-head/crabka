-- SP28: predicate + conditional expression breadth, diffed against PostgreSQL 18.
-- IS [NOT] NULL, [NOT] IN, [NOT] BETWEEN, [NOT] LIKE/ILIKE, CASE, SELECT DISTINCT,
-- and OFFSET — with three-valued NULL semantics. Every result-returning query is
-- ORDER BY-stable so the row diff is deterministic.
CREATE TABLE pr (id int4, name text, amount int4);
INSERT INTO pr VALUES (1, 'apple', 10), (2, 'banana', NULL), (3, 'cherry', 30), (4, NULL, 5), (5, 'avocado', 10);

-- IS NULL / IS NOT NULL
SELECT id FROM pr WHERE amount IS NULL ORDER BY id;
SELECT id FROM pr WHERE name IS NOT NULL ORDER BY id;

-- IN / NOT IN (including a NULL in the list -> unknown, never a row)
SELECT id FROM pr WHERE amount IN (10, 30) ORDER BY id;
SELECT id FROM pr WHERE amount NOT IN (10) ORDER BY id;
SELECT id FROM pr WHERE amount NOT IN (10, NULL) ORDER BY id;

-- BETWEEN / NOT BETWEEN (bounds inclusive)
SELECT id FROM pr WHERE amount BETWEEN 5 AND 10 ORDER BY id;
SELECT id FROM pr WHERE amount NOT BETWEEN 5 AND 10 ORDER BY id;

-- LIKE / ILIKE / `_` / `\` escape
SELECT name FROM pr WHERE name LIKE 'a%' ORDER BY name;
SELECT id FROM pr WHERE name ILIKE 'A%' ORDER BY id;
SELECT id FROM pr WHERE name LIKE 'app_e';
SELECT 'a%b' LIKE 'a\%b';

-- CASE (searched + simple; an unmatched simple CASE with no ELSE is NULL)
SELECT id, CASE WHEN amount IS NULL THEN 'none' WHEN amount >= 30 THEN 'big' ELSE 'small' END FROM pr ORDER BY id;
SELECT id, CASE amount WHEN 10 THEN 'ten' WHEN 30 THEN 'thirty' END FROM pr ORDER BY id;

-- SELECT DISTINCT + OFFSET (NULLS LAST under ASC)
SELECT DISTINCT amount FROM pr ORDER BY amount;
SELECT id FROM pr ORDER BY id LIMIT 2 OFFSET 2;
SELECT id FROM pr ORDER BY id OFFSET 4;

-- error parity (same SQLSTATE on both sides): a non-boolean CASE/WHEN is 42804
SELECT CASE WHEN 1 THEN 'x' END FROM pr;
