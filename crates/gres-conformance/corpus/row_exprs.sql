-- Row constructors, diffed against PostgreSQL 18: the `ROW(a, b)` and bare
-- `(a, b)` spellings, composite text output with PostgreSQL's field quoting,
-- lexicographic comparison, `IN`, and the field-wise `IS [NOT] NULL` rules.
-- Every result-returning query is ORDER BY-stable.
CREATE TABLE rowx (a int4, b int4, c text);
INSERT INTO rowx VALUES (1, 2, 'x'), (1, NULL, 'y'), (3, 4, NULL);

-- Text output. A one-element list is a row only in the ROW spelling; `(1)` is
-- ordinary grouping.
SELECT ROW(1, 2), (1, 2), ROW(1), ROW();
SELECT (1);
SELECT ROW(1, 'a', NULL, true);
SELECT ROW(1.50, true, date '2024-01-01');
SELECT ROW(1, 2)::text, (1, 2)::text;

-- Field quoting: empty, comma, quote, parenthesis, backslash and whitespace.
SELECT ROW('a b', 'a,b', 'c"d', '', ' e');
SELECT ROW('a(b', 'a)b', 'a\b');

-- Lexicographic comparison.
SELECT (1,2) = (1,2), (1,2) = (1,3), (1,2) <> (1,3);
SELECT (1,2) < (1,3), (1,2) <= (1,2), (2,1) > (1,9), (1,2) >= (1,2);
SELECT ROW('a','b') < ROW('a','c'), ROW('b','a') > ROW('a','z');

-- NULL fields: a field that already decided wins over a later NULL.
SELECT (1,NULL) = (1,2), (1,NULL) = (2,2), (1,NULL) < (2,2), (1,2) < (1,NULL);
SELECT (NULL,NULL) = (NULL,NULL);

-- IS [NOT] NULL is field-wise, so the two are not negations of each other.
SELECT ROW(1,NULL) IS NULL, ROW(NULL,NULL) IS NULL, ROW(1,2) IS NULL;
SELECT ROW(1,NULL) IS NOT NULL, ROW(NULL,NULL) IS NOT NULL, ROW(1,2) IS NOT NULL;
SELECT ROW(NULL) IS NULL, ROW(NULL) IS NOT NULL;

-- IN over rows, and the null-safe comparison.
SELECT (1,2) IN ((1,2),(3,4)), (5,6) IN ((1,2),(3,4)), (1,2) NOT IN ((1,2),(3,4));
SELECT (1,2) IS DISTINCT FROM (1,2), ROW(1,2) IS DISTINCT FROM ROW(1,NULL);
SELECT (1,2) IS NOT DISTINCT FROM (1,2), (1,NULL) IS NOT DISTINCT FROM (1,NULL);

-- Over stored rows.
SELECT a, b FROM rowx WHERE (a, b) = (1, 2) ORDER BY a;
SELECT a FROM rowx WHERE (a, b) IN ((1,2), (3,4)) ORDER BY a;
SELECT a, b, (a, b) IS NULL, (a, b) IS NOT NULL FROM rowx ORDER BY a, b;
SELECT ROW(a, b, c) FROM rowx ORDER BY a, b;
SELECT a FROM rowx WHERE (a, c) IS DISTINCT FROM (1, 'x') ORDER BY a;

-- Error parity: comparing rows of different widths is 42601.
SELECT ROW(1,2) = ROW(1,2,3);
SELECT (1,2) IN ((1,2,3));
DROP TABLE rowx;
