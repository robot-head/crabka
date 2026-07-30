-- S2 SQL-level PREPARE / EXECUTE / DEALLOCATE and the pg_prepared_statements view.
CREATE TABLE pe_t (id int4, s text);
INSERT INTO pe_t VALUES (1, 'a'), (2, 'b'), (3, 'c');

PREPARE pe1 AS SELECT id FROM pe_t ORDER BY id;
EXECUTE pe1;
EXECUTE pe1;
PREPARE pe1 AS SELECT 1;

PREPARE pe2(int4) AS SELECT s FROM pe_t WHERE id = $1;
EXECUTE pe2(2);
EXECUTE pe2(99);
EXECUTE pe2;
EXECUTE pe2(1, 2);

PREPARE pe3(int4, text) AS SELECT id, s FROM pe_t WHERE id > $1 AND s <> $2 ORDER BY id;
EXECUTE pe3(1, 'c');

PREPARE pe4 AS SELECT $1::int4 + 1;
EXECUTE pe4(41);

EXECUTE pe_missing;
DEALLOCATE pe_missing;

SELECT name, statement, parameter_types, result_types, from_sql FROM pg_prepared_statements ORDER BY name;
SELECT count(*) FROM pg_prepared_statements;

DEALLOCATE pe1;
DEALLOCATE PREPARE pe2;
SELECT name FROM pg_prepared_statements ORDER BY name;
DEALLOCATE ALL;
SELECT count(*) FROM pg_prepared_statements;

-- A prepared write statement runs through the ordinary DML path.
PREPARE pe_ins(int4, text) AS INSERT INTO pe_t VALUES ($1, $2);
EXECUTE pe_ins(4, 'd');
SELECT id, s FROM pe_t ORDER BY id;
PREPARE pe_upd(text, int4) AS UPDATE pe_t SET s = $1 WHERE id = $2;
EXECUTE pe_upd('D', 4);
PREPARE pe_del(int4) AS DELETE FROM pe_t WHERE id = $1;
EXECUTE pe_del(4);
SELECT id, s FROM pe_t ORDER BY id;
DEALLOCATE ALL;

-- Prepared statements are session state, not transaction state: a rollback
-- keeps them.
BEGIN;
PREPARE pe_txn AS SELECT 1;
ROLLBACK;
EXECUTE pe_txn;
DEALLOCATE ALL;

-- A parameter inside a FROM-clause subquery — lateral or not — is bound like
-- any other, so describing the FROM item never sees the placeholder.
PREPARE pe_sub(int4) AS SELECT z FROM (SELECT $1 AS z) d;
EXECUTE pe_sub(5);
PREPARE pe_lat(int4) AS SELECT q.z FROM pe_t, LATERAL (SELECT pe_t.id + $1 AS z) q ORDER BY 1;
EXECUTE pe_lat(10);
PREPARE pe_join(int4) AS SELECT pe_t.id FROM pe_t JOIN (SELECT $1 AS z) d ON d.z = pe_t.id;
EXECUTE pe_join(2);
PREPARE pe_deep(int4) AS SELECT z FROM (SELECT * FROM (SELECT $1 AS z) e) d;
EXECUTE pe_deep(9);
PREPARE pe_setop(int4) AS SELECT z FROM (SELECT $1 AS z UNION ALL SELECT $1 + 1) d ORDER BY 1;
EXECUTE pe_setop(20);
SELECT name, parameter_types::text FROM pg_prepared_statements WHERE name LIKE 'pe\_%' ORDER BY name;
DEALLOCATE pe_sub;
DEALLOCATE pe_lat;
DEALLOCATE pe_join;
DEALLOCATE pe_deep;
DEALLOCATE pe_setop;

DROP TABLE pe_t;

-- pg_prepared_statements.statement is the query string the PREPARE arrived in.
PREPARE pp_txt AS SELECT 1;
SELECT statement FROM pg_prepared_statements WHERE name = 'pp_txt';
DEALLOCATE pp_txt;
