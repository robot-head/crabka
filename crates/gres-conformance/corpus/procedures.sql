-- P2: `CREATE PROCEDURE`, `CALL`, `DROP PROCEDURE` and `DO`.
CREATE TABLE pr_log (id int, note text);
CREATE PROCEDURE pr_noop() LANGUAGE sql AS $$ SELECT 1 $$;
CALL pr_noop();
SELECT prokind, pronargs, proretset, prorettype FROM pg_proc WHERE proname = 'pr_noop';
SELECT pg_get_function_result(oid) IS NULL FROM pg_proc WHERE proname = 'pr_noop';
SELECT pg_get_functiondef(oid) FROM pg_proc WHERE proname = 'pr_noop';
-- A procedure taking arguments.
CREATE PROCEDURE pr_arg(x int) LANGUAGE sql AS $$ SELECT x + 1 $$;
CALL pr_arg(1);
CALL pr_arg(-1);
SELECT pg_get_function_arguments(oid) FROM pg_proc WHERE proname = 'pr_arg';
-- A procedure whose body writes.
CREATE PROCEDURE pr_insert() LANGUAGE sql AS $$ INSERT INTO pr_log VALUES (1, 'one') $$;
CALL pr_insert();
SELECT id, note FROM pr_log ORDER BY id;
CALL pr_insert();
SELECT count(*) FROM pr_log;
-- A procedure has no RETURNS clause and cannot be selected from.
SELECT pr_noop();
-- A function cannot be CALLed.
CREATE FUNCTION pr_fn(x int) RETURNS int AS 'SELECT $1' LANGUAGE sql;
CALL pr_fn(1);
SELECT pr_fn(1);
-- Calling something that does not exist.
CALL pr_never_defined();
CALL pr_arg('not an int');
-- Redefinition rules match CREATE FUNCTION.
CREATE PROCEDURE pr_arg(x int) LANGUAGE sql AS $$ SELECT 0 $$;
CREATE OR REPLACE PROCEDURE pr_arg(x int) LANGUAGE sql AS $$ SELECT x + 2 $$;
CALL pr_arg(1);
-- ALTER PROCEDURE.
ALTER PROCEDURE pr_arg(int) SECURITY DEFINER;
SELECT prosecdef FROM pg_proc WHERE proname = 'pr_arg';
ALTER PROCEDURE pr_arg(int) RENAME TO pr_arg2;
CALL pr_arg2(1);
SELECT count(*) FROM pg_proc WHERE proname = 'pr_arg';
-- DROP PROCEDURE.
DROP PROCEDURE pr_arg2(int);
SELECT count(*) FROM pg_proc WHERE proname = 'pr_arg2';
DROP PROCEDURE pr_arg2(int);
DROP PROCEDURE IF EXISTS pr_arg2(int);
DROP PROCEDURE pr_noop, pr_insert;
SELECT count(*) FROM pg_proc WHERE proname LIKE 'pr\_%';
-- `DO` has no inline handler for the SQL language, in PostgreSQL or here.
DO $$ SELECT 1 $$ LANGUAGE sql;
DO LANGUAGE sql $$ SELECT 1 $$;
DROP FUNCTION pr_fn(int);
DROP TABLE pr_log;
