-- PostgreSQL's `typename 'string'` constant syntax, diffed against PostgreSQL 18.
-- The form is defined to mean exactly `'string'::typename`, so it must work in
-- every expression position and fail with the same SQLSTATE and message as the
-- cast. Every result-returning query is ORDER BY-stable so the row diff is
-- deterministic.
CREATE TABLE tl (id int4, flag bool, amount numeric, day date, span interval);
INSERT INTO tl VALUES (int4 '1', bool 't', numeric '1.50', date '2024-01-01', interval '1 day');
INSERT INTO tl VALUES (int4 '2', bool 'f', numeric '-2.25', date '2024-03-31', interval '2 hours');

-- Projection position, across the type families.
SELECT bool 't', bool 'f', int4 '0', int8 '90', int2 '5';
SELECT numeric '1.50', float8 '2.5', text 'x';
SELECT date '2024-01-01', timestamp '2024-01-01 12:00:00', interval '1 day 3 hours';
SELECT timestamp with time zone '2024-01-01 00:00:00+00';
SELECT double precision '1.5', character varying 'abc';
SELECT pg_catalog.text 'qualified';

-- The typed constant and the two cast spellings agree.
SELECT bool 't' = 't'::bool, numeric '1.50' = CAST('1.50' AS numeric);

-- WHERE position.
SELECT id FROM tl WHERE flag = bool 't' ORDER BY id;
SELECT id FROM tl WHERE day > date '2024-02-01' ORDER BY id;
SELECT id FROM tl WHERE amount < numeric '0' ORDER BY id;
SELECT id FROM tl WHERE span > interval '3 hours' ORDER BY id;

-- Function-argument position.
SELECT length(text 'abcde'), abs(int4 '-7');
SELECT upper(text 'abc'), date_trunc('month', timestamp '2024-03-31 05:00:00');

-- INSERT VALUES position (rows above), read back.
SELECT id, flag, amount, day, span FROM tl ORDER BY id;

-- A bare type name is still a column reference.
SELECT text FROM (SELECT 'v' AS text) s;

-- Keyword column labels: after AS any keyword is a label, and without AS the
-- bare_label_keyword list applies.
SELECT bool 't' AS true, int4 '1' AS select, text 'x' AS from;
SELECT int4 '1' AS "From", int4 '2' AS order, int4 '3' AS array;
SELECT int4 '1' select, int4 '2' values, int4 '3' table, int4 '4' distinct;

-- Interval field qualifiers: the field supplies the unit AND truncates.
SELECT interval '1' day, interval '1' hour, interval '1' month, interval '2' year;
SELECT interval '90' minute, interval '1.5' second, interval '-1' day;
SELECT interval '1.5' day, interval '1 day 3 hours' day, interval '1 year 2 months' month;

-- Error parity: the constant syntax reports exactly what the cast reports.
SELECT bool 'test';
SELECT int4 'x';
SELECT date 'not-a-date';
SELECT numeric 'zzz';
DROP TABLE tl;
