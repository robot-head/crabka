-- SP29: scalar (row) functions + the `||` operator, diffed against PostgreSQL 18.
-- String (length/upper/lower/btrim/ltrim/rtrim/substr/replace/concat), math
-- (abs/mod), null/conditional (coalesce/nullif/greatest/least), and `||`. Every
-- result-returning query is ORDER BY-stable and ASCII-only so the row diff is
-- deterministic (the slice's lexer treats string-literal bytes verbatim).
CREATE TABLE sf (id int4, name text, qty int4);
INSERT INTO sf VALUES (1, 'Alice', 10), (2, 'bob', NULL), (3, 'Carol', 30), (4, NULL, 5);

-- string length / case folding
SELECT length('hello');
SELECT upper('aBc'), lower('aBc');
SELECT id, upper(name) FROM sf WHERE name IS NOT NULL ORDER BY id;

-- trims (default whitespace + explicit character set)
SELECT btrim('  hi  '), ltrim('xxhi', 'x'), rtrim('hixx', 'x');

-- substr / replace
SELECT substr('abcdef', 2, 3);
SELECT substr('abcdef', 4);
SELECT substr('abcdef', 0, 2);
SELECT replace('a.b.c', '.', '-');

-- concat (renders each non-NULL argument; NULLs skipped; never NULL)
SELECT concat('x', NULL, 'y', 1);
SELECT concat();

-- `||` operator (text || nontext, left-assoc, NULL-propagating)
SELECT 'id=' || 5 || '!';
SELECT 'flag=' || (1 = 1);
SELECT 'x' || NULL IS NULL;

-- math: abs / mod
SELECT abs(-7), abs(7);
SELECT mod(11, 3), mod(-11, 3);

-- null/conditional helpers
SELECT coalesce(NULL, NULL, 'third');
SELECT id, coalesce(qty, 0) FROM sf ORDER BY id;
SELECT nullif(5, 5), nullif(5, 6);
SELECT greatest(3, 7, 2), least(3, 7, 2);
SELECT greatest('b', 'a', 'c'), least('b', 'a', 'c');
SELECT greatest(NULL, 4, NULL);

-- scalar functions compose with WHERE / ORDER BY / aggregates (single range)
SELECT id FROM sf WHERE length(name) = 5 ORDER BY id;
SELECT upper(name) FROM sf WHERE name IS NOT NULL ORDER BY lower(name);
SELECT count(*), abs(0 - sum(qty)) FROM sf;

-- error parity (same SQLSTATE on both sides)
-- `int || int` has no operator (42883)
SELECT 1 || 2;
-- a scalar function applied to the wrong argument type (42883)
SELECT upper(1);
-- DISTINCT is only for aggregates (42809)
SELECT upper(DISTINCT name) FROM sf;
-- COALESCE over incompatible types (42804)
SELECT coalesce(1, 'x');
