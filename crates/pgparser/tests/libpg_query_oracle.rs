//! Differential parser oracle: our parser must agree with `libpg_query` on
//! accept/reject for slice-grammar statements and clear syntax errors.
//! Gated behind --features oracle (`libpg_query` is a C build-time dep).
#![cfg(feature = "oracle")]

/// Statements inside the SP2 slice — BOTH parsers must accept.
const ACCEPTED: &[&str] = &[
    "CREATE TABLE t (id int4, name text)",
    "CREATE TABLE t (a integer, b bigint, c boolean, d text)",
    "DROP TABLE t",
    "INSERT INTO t VALUES (1, 'a')",
    "INSERT INTO t (a, b) VALUES (1, 'x'), (2, 'y')",
    "SELECT 1",
    "SELECT 1 + 2 * 3",
    "SELECT a, b AS bee FROM t WHERE a > 1 ORDER BY a DESC, b LIMIT 10",
    "SELECT * FROM t",
    "SELECT NOT a OR b AND c FROM t",
    "SELECT 'it''s' FROM t",
    // SP4: transaction control
    "BEGIN",
    "START TRANSACTION",
    "BEGIN ISOLATION LEVEL REPEATABLE READ",
    "COMMIT",
    "END",
    "ROLLBACK",
    "ABORT",
    // SP4: DML
    "UPDATE t SET a = 1 WHERE id = 5",
    "UPDATE t SET a = 1, b = 2",
    "DELETE FROM t WHERE id > 3",
    "DELETE FROM t",
    // SP6: row-level locking
    "SELECT id FROM t FOR UPDATE",
    "SELECT id FROM t WHERE id > 1 FOR SHARE",
    // SP28: predicate + conditional expression breadth
    "SELECT a FROM t WHERE a IS NULL",
    "SELECT a FROM t WHERE a IS NOT NULL",
    "SELECT a FROM t WHERE a IN (1, 2, 3)",
    "SELECT a FROM t WHERE a NOT IN (1, 2)",
    "SELECT a FROM t WHERE a BETWEEN 1 AND 10",
    "SELECT a FROM t WHERE a NOT BETWEEN 1 AND 10",
    "SELECT a FROM t WHERE name LIKE 'a%'",
    "SELECT a FROM t WHERE name NOT LIKE 'a_c'",
    "SELECT a FROM t WHERE name ILIKE 'A%'",
    "SELECT a FROM t WHERE name NOT ILIKE 'A%'",
    "SELECT NOT a IN (1, 2) FROM t",
    "SELECT a FROM t WHERE a BETWEEN 1 AND 2 AND b",
    "SELECT CASE WHEN a > 0 THEN 'pos' ELSE 'neg' END FROM t",
    "SELECT CASE a WHEN 1 THEN 'one' WHEN 2 THEN 'two' END FROM t",
    "SELECT DISTINCT a FROM t",
    "SELECT a FROM t ORDER BY a LIMIT 5 OFFSET 10",
    "SELECT a FROM t LIMIT 5 OFFSET 2",
    "VALUES (1), (2)",
    "VALUES (1) UNION SELECT 2",
    "SELECT x FROM (VALUES (1), (2)) AS v(x)",
    "SELECT x FROM (SELECT 1 AS x UNION SELECT 2) AS s ORDER BY x",
    "SELECT x FROM (VALUES (2), (1) ORDER BY 1 LIMIT 1) AS v(x)",
    "SELECT (VALUES (1) UNION SELECT 2 ORDER BY 1 LIMIT 1)",
    "SELECT 2 IN (VALUES (1), (2))",
    "SELECT EXISTS (SELECT 1 EXCEPT SELECT 2)",
    "WITH c AS (SELECT 1) SELECT * FROM c",
    "WITH a AS (VALUES (1)), b AS (SELECT * FROM a) SELECT * FROM b",
    "WITH u AS (SELECT 1 UNION SELECT 2) SELECT * FROM u",
    "WITH RECURSIVE r AS (SELECT 1) SELECT * FROM r",
    // SP33: joins — every join type, comma form, USING/NATURAL, aliases,
    // qualified refs, qualified wildcard, multi-way, and derived tables.
    "SELECT t.a FROM t JOIN u ON t.id = u.id",
    "SELECT t.a FROM t INNER JOIN u ON t.id = u.id",
    "SELECT t.a FROM t LEFT JOIN u ON t.id = u.id",
    "SELECT t.a FROM t LEFT OUTER JOIN u ON t.id = u.id",
    "SELECT t.a FROM t RIGHT JOIN u ON t.id = u.id",
    "SELECT t.a FROM t FULL OUTER JOIN u ON t.id = u.id",
    "SELECT t.a FROM t CROSS JOIN u",
    "SELECT a FROM t NATURAL JOIN u",
    "SELECT a FROM t JOIN u USING (id)",
    "SELECT a FROM t, u WHERE t.id = u.id",
    "SELECT t.a, u.b FROM t JOIN u ON t.id = u.id JOIN v ON u.id = v.id",
    "SELECT x.a FROM t AS x",
    "SELECT x.a FROM t x JOIN u y ON x.id = y.id",
    "SELECT t.* FROM t JOIN u ON t.id = u.id",
    "SELECT d.n FROM (SELECT a AS n FROM t) AS d",
    "SELECT d.n FROM (SELECT a AS n FROM t) d",
    // SP37: date/time type names, typed literals, EXTRACT, AT TIME ZONE
    "CREATE TABLE t (a date, b time, c timestamp, d timestamptz, e interval)",
    "CREATE TABLE t (a timestamp with time zone, b time without time zone)",
    "CREATE TABLE t (a timestamp without time zone)",
    "SELECT DATE '2024-01-01'",
    "SELECT TIMESTAMP '2024-01-01 00:00:00'",
    "SELECT INTERVAL '1 day'",
    "SELECT a::timestamp with time zone FROM t",
    "SELECT extract(year FROM a) FROM t",
    "SELECT a AT TIME ZONE 'UTC' FROM t",
    "SELECT a AT TIME ZONE 'UTC' = b FROM t",
    // SP37 Task 13: clock funcs + date/time functions
    "SELECT current_date",
    "SELECT current_timestamp",
    "SELECT now()",
    "SELECT date_part('hour', a) FROM t",
    "SELECT date_trunc('month', a) FROM t",
    // SP38: set operations — UNION / INTERSECT / EXCEPT
    "SELECT 1 UNION SELECT 2",
    "SELECT 1 UNION ALL SELECT 2",
    "SELECT a FROM t UNION SELECT a FROM u ORDER BY a",
    "SELECT 1 INTERSECT SELECT 2",
    "SELECT 1 EXCEPT ALL SELECT 2",
    "SELECT 1 UNION SELECT 2 INTERSECT SELECT 3",
    "(SELECT 1 ORDER BY 1 LIMIT 1) UNION SELECT 2",
    // SP37: SET / SHOW / RESET GUC
    "SET timezone = 'America/New_York'",
    "SET timezone TO 'UTC'",
    "SET TIME ZONE 'America/New_York'",
    "SET LOCAL timezone = 'UTC'",
    "SET TIME ZONE LOCAL",
    "SET TIME ZONE DEFAULT",
    "SET timezone = DEFAULT",
    "SHOW timezone",
    "SHOW TIME ZONE",
    "RESET timezone",
    // SP40: FDW DDL
    "CREATE SERVER s FOREIGN DATA WRAPPER w OPTIONS (a 'b')",
    "CREATE FOREIGN TABLE t (id int4) SERVER s OPTIONS (topic 't')",
    "IMPORT FOREIGN SCHEMA kafka FROM SERVER s INTO public",
    "CREATE USER MAPPING FOR PUBLIC SERVER s OPTIONS (u 'x')",
    "DROP FOREIGN TABLE IF EXISTS t",
];

/// Clear syntax errors — BOTH parsers must reject.
const REJECTED: &[&str] = &[
    "SELECT FROM",
    "CREATE TABLE",
    "INSERT INTO t VALUES",
    "SELECT 1 +",
    "SELECT * FROM",
    "SELECT 1 ORDER BY",
    "(",
    "SELECT 'unterminated",
    // SP28: malformed predicate / CASE grammar
    "SELECT a FROM t WHERE a IN ()",
    "SELECT a FROM t WHERE a BETWEEN 1",
    "SELECT CASE END FROM t",
    // SP33: a non-CROSS/NATURAL JOIN requires an ON/USING qualification, and ON
    // requires a predicate — gram.y rejects both (raw-parse agreement).
    "SELECT a FROM t JOIN u",
    "SELECT a FROM t JOIN u ON",
    // SP40: FDW DDL malformed
    "CREATE FOREIGN TABLE t SERVER",
    "IMPORT FOREIGN SCHEMA FROM SERVER s",
];

fn pg_accepts(sql: &str) -> bool {
    pg_query::parse(sql).is_ok()
}

fn we_accept(sql: &str) -> bool {
    crabka_pgparser::parse(sql).is_ok()
}

#[test]
fn agreement_on_accepted() {
    for &sql in ACCEPTED {
        assert!(pg_accepts(sql), "libpg_query should accept: {sql}");
        assert!(we_accept(sql), "pgparser should accept (PG does): {sql}");
    }
}

#[test]
fn agreement_on_rejected() {
    for &sql in REJECTED {
        assert!(!pg_accepts(sql), "libpg_query should reject: {sql}");
        assert!(!we_accept(sql), "pgparser should reject (PG does): {sql}");
    }
}
