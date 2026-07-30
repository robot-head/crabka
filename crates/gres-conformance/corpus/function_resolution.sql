-- P2: routine name resolution — overloading, ambiguity, and the exact
-- 42883/42725/42809 conditions PostgreSQL reports.
CREATE FUNCTION fr_one(x int) RETURNS text AS $$ SELECT 'int' $$ LANGUAGE sql;
CREATE FUNCTION fr_one(x text) RETURNS text AS $$ SELECT 'text' $$ LANGUAGE sql;
SELECT fr_one(1);
SELECT fr_one('a');
SELECT fr_one(1::bigint);
-- No overload takes this argument list.
SELECT fr_one(1, 2);
SELECT fr_one();
-- A name that carries no routine at all.
SELECT fr_never_defined(1);
SELECT fr_never_defined();
-- Two overloads equally good for untyped literals is 42725.
CREATE FUNCTION fr_amb(x int, y text) RETURNS int AS 'SELECT 1' LANGUAGE sql;
CREATE FUNCTION fr_amb(x text, y int) RETURNS int AS 'SELECT 2' LANGUAGE sql;
SELECT fr_amb(1, 'a');
SELECT fr_amb('a', 1);
-- Arity-based resolution with defaults.
CREATE FUNCTION fr_def(a int, b int DEFAULT 2, c int DEFAULT 3) RETURNS int AS 'SELECT $1 + $2 + $3' LANGUAGE sql;
SELECT fr_def(1);
SELECT fr_def(1, 10);
SELECT fr_def(1, 10, 100);
SELECT fr_def();
SELECT fr_def(1, 2, 3, 4);
-- DROP with an explicit signature picks one overload.
DROP FUNCTION fr_one(int);
SELECT fr_one('a');
SELECT fr_one(1);
SELECT count(*) FROM pg_proc WHERE proname = 'fr_one';
-- A bare name is only unambiguous while one routine carries it.
DROP FUNCTION fr_one;
SELECT count(*) FROM pg_proc WHERE proname = 'fr_one';
DROP FUNCTION fr_amb;
-- A signature that names no routine.
DROP FUNCTION fr_amb(int, int);
DROP FUNCTION fr_never_defined;
DROP FUNCTION fr_never_defined(int);
-- Kind enforcement between the FUNCTION and PROCEDURE spellings.
CREATE PROCEDURE fr_proc(x int) LANGUAGE sql AS $$ SELECT x $$;
CREATE FUNCTION fr_func(x int) RETURNS int AS 'SELECT $1' LANGUAGE sql;
DROP FUNCTION fr_proc(int);
DROP PROCEDURE fr_func(int);
SELECT fr_proc(1);
CALL fr_func(1);
-- ROUTINE matches either kind.
DROP ROUTINE fr_proc(int);
DROP ROUTINE fr_func(int);
SELECT count(*) FROM pg_proc WHERE proname IN ('fr_proc', 'fr_func');
-- Argument coercion picks the only candidate whose parameter accepts the value.
CREATE FUNCTION fr_coerce(x bigint) RETURNS bigint AS 'SELECT $1 * 2' LANGUAGE sql;
SELECT fr_coerce(3::int);
SELECT fr_coerce(3::bigint);
DROP FUNCTION fr_coerce(bigint);
DROP FUNCTION fr_def(int, int, int);
DROP FUNCTION fr_amb(text, int);
