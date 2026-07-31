-- P2: `CREATE FUNCTION` — the definition surface, the catalog rows it produces,
-- and calling a `LANGUAGE sql` body.
CREATE FUNCTION cf_add(a int, b int) RETURNS int AS 'SELECT $1 + $2' LANGUAGE sql;
SELECT cf_add(1, 2);
SELECT cf_add(-3, 3);
SELECT cf_add(NULL, 2);
-- A body may name its parameters instead of numbering them.
CREATE FUNCTION cf_double(a int) RETURNS int AS 'SELECT a * 2' LANGUAGE sql;
SELECT cf_double(21);
-- Dollar quoting, and a body that is a single expression over several
-- parameters.
CREATE FUNCTION cf_hyp(x double precision, y double precision) RETURNS double precision AS $$ SELECT sqrt(x * x + y * y) $$ LANGUAGE sql;
SELECT cf_hyp(3, 4);
-- STRICT: a NULL argument short-circuits to NULL without running the body.
CREATE FUNCTION cf_strict(a text) RETURNS text AS 'SELECT upper($1)' LANGUAGE sql STRICT;
SELECT cf_strict('abc');
SELECT cf_strict(NULL);
SELECT cf_strict(NULL) IS NULL;
-- CALLED ON NULL INPUT is the default and runs the body.
CREATE FUNCTION cf_lax(a int) RETURNS int AS 'SELECT coalesce($1, 7)' LANGUAGE sql CALLED ON NULL INPUT;
SELECT cf_lax(NULL);
SELECT cf_lax(1);
-- Parameter defaults.
CREATE FUNCTION cf_defaults(a int, b int DEFAULT 10) RETURNS int AS 'SELECT $1 * $2' LANGUAGE sql;
SELECT cf_defaults(3);
SELECT cf_defaults(3, 4);
-- RETURNS void produces one NULL row.
CREATE FUNCTION cf_void() RETURNS void AS 'SELECT 1' LANGUAGE sql;
SELECT cf_void() IS NULL;
-- PostgreSQL 14 SQL bodies.
CREATE FUNCTION cf_atomic(a int) RETURNS int LANGUAGE sql BEGIN ATOMIC SELECT a + 1; END;
SELECT cf_atomic(41);
CREATE FUNCTION cf_return(a int) RETURNS int LANGUAGE sql RETURN a * 3;
SELECT cf_return(5);
-- A multi-statement body returns the last statement's result.
CREATE FUNCTION cf_last() RETURNS int AS 'SELECT 1; SELECT 2;' LANGUAGE sql;
SELECT cf_last();
-- Volatility, parallel safety, cost and security qualifiers are recorded.
CREATE FUNCTION cf_qualified() RETURNS int AS 'SELECT 1' LANGUAGE sql IMMUTABLE STRICT PARALLEL SAFE SECURITY DEFINER COST 5;
SELECT cf_qualified();
SELECT provolatile, proparallel, prosecdef, proisstrict, procost FROM pg_proc WHERE proname = 'cf_qualified';
-- pg_proc rows for the definitions above.
SELECT proname, pronargs, pronargdefaults, prokind, proretset, prolang FROM pg_proc WHERE proname LIKE 'cf\_%' ORDER BY proname;
SELECT proname, prorettype FROM pg_proc WHERE proname LIKE 'cf\_%' ORDER BY proname;
SELECT proname, proargnames FROM pg_proc WHERE proname IN ('cf_add', 'cf_defaults') ORDER BY proname;
SELECT proname, prosrc FROM pg_proc WHERE proname IN ('cf_add', 'cf_double') ORDER BY proname;
SELECT prosrc = '' FROM pg_proc WHERE proname = 'cf_atomic';
-- pg_get_function_* over the same routines.
SELECT pg_get_function_arguments(oid) FROM pg_proc WHERE proname = 'cf_add';
SELECT pg_get_function_arguments(oid) FROM pg_proc WHERE proname = 'cf_defaults';
SELECT pg_get_function_identity_arguments(oid) FROM pg_proc WHERE proname = 'cf_defaults';
SELECT pg_get_function_result(oid) FROM pg_proc WHERE proname = 'cf_add';
SELECT pg_get_function_result(oid) FROM pg_proc WHERE proname = 'cf_void';
SELECT pg_get_functiondef(oid) FROM pg_proc WHERE proname = 'cf_add';
-- CREATE OR REPLACE keeps the identity and swaps the body.
CREATE OR REPLACE FUNCTION cf_add(a int, b int) RETURNS int AS 'SELECT $1 - $2' LANGUAGE sql;
SELECT cf_add(10, 4);
-- Redefining without OR REPLACE is 42723.
CREATE FUNCTION cf_add(a int, b int) RETURNS int AS 'SELECT 0' LANGUAGE sql;
-- Changing the return type of an existing function is 42P13.
CREATE OR REPLACE FUNCTION cf_add(a int, b int) RETURNS bigint AS 'SELECT 0::bigint' LANGUAGE sql;
-- Renaming an input parameter is 42P13.
CREATE OR REPLACE FUNCTION cf_add(x int, y int) RETURNS int AS 'SELECT $1 - $2' LANGUAGE sql;
-- A definition with no language, no body, or an unknown language.
CREATE FUNCTION cf_nolang() RETURNS int AS 'SELECT 1';
CREATE FUNCTION cf_nobody() RETURNS int LANGUAGE sql;
CREATE FUNCTION cf_badlang() RETURNS int AS 'SELECT 1' LANGUAGE nosuchlang;
-- A signature naming a type that does not exist.
CREATE FUNCTION cf_badarg(x nosuchtype) RETURNS int AS 'SELECT 1' LANGUAGE sql;
CREATE FUNCTION cf_badret() RETURNS nosuchtype AS 'SELECT 1' LANGUAGE sql;
-- Set-returning functions in FROM position.
CREATE FUNCTION cf_series(n int) RETURNS SETOF int AS 'SELECT generate_series(1, $1)' LANGUAGE sql;
SELECT * FROM cf_series(3);
SELECT * FROM cf_series(0);
SELECT sum(cf_series) FROM cf_series(4);
CREATE FUNCTION cf_table(n int) RETURNS TABLE(a int, b text) AS $$ SELECT i, 'x' || i FROM generate_series(1, $1) i $$ LANGUAGE sql;
SELECT * FROM cf_table(2);
SELECT a FROM cf_table(3) ORDER BY a DESC;
SELECT pg_get_function_result(oid) FROM pg_proc WHERE proname = 'cf_series';
SELECT pg_get_function_result(oid) FROM pg_proc WHERE proname = 'cf_table';
-- ALTER FUNCTION.
ALTER FUNCTION cf_double(int) IMMUTABLE;
SELECT provolatile FROM pg_proc WHERE proname = 'cf_double';
ALTER FUNCTION cf_double(int) STRICT;
SELECT proisstrict FROM pg_proc WHERE proname = 'cf_double';
ALTER FUNCTION cf_double(int) RENAME TO cf_twice;
SELECT cf_twice(4);
SELECT count(*) FROM pg_proc WHERE proname = 'cf_double';
ALTER FUNCTION cf_nosuch(int) RENAME TO cf_other;
-- DROP FUNCTION.
DROP FUNCTION cf_twice(int);
SELECT count(*) FROM pg_proc WHERE proname = 'cf_twice';
DROP FUNCTION cf_twice(int);
DROP FUNCTION IF EXISTS cf_twice(int);
DROP FUNCTION cf_add(int, int), cf_hyp(double precision, double precision);
SELECT count(*) FROM pg_proc WHERE proname IN ('cf_add', 'cf_hyp');
DROP FUNCTION cf_strict;
DROP FUNCTION IF EXISTS cf_never_existed;
DROP FUNCTION cf_never_existed;
