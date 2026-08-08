//! Differential parser oracle: this parser must agree with `libpg_query` on
//! accept/reject for slice-grammar statements and clear syntax errors.
//! --features oracle gates these tests, because `libpg_query` is a C
//! build-time dep.
#![cfg(feature = "oracle")]

/// Statements inside the SP2 slice: BOTH parsers must accept.
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
    "SET SESSION application_name TO 'session-app'",
    "SET extra_float_digits = -15",
    "SET DateStyle TO ISO, MDY",
    "SET DateStyle TO SQL, DMY",
    "SET TIME ZONE LOCAL",
    "SET TIME ZONE DEFAULT",
    "SET timezone = DEFAULT",
    "SHOW timezone",
    "SHOW TIME ZONE",
    "RESET timezone",
    "RESET ALL",
    "SHOW ALL",
    "DISCARD ALL",
    "SET TRANSACTION ISOLATION LEVEL READ COMMITTED",
    "SET TRANSACTION ISOLATION LEVEL REPEATABLE READ",
    // SP40: FDW DDL
    "CREATE SERVER s FOREIGN DATA WRAPPER w OPTIONS (a 'b')",
    "CREATE FOREIGN TABLE t (id int4) SERVER s OPTIONS (topic 't')",
    "IMPORT FOREIGN SCHEMA kafka FROM SERVER s INTO public",
    "CREATE USER MAPPING FOR PUBLIC SERVER s OPTIONS (u 'x')",
    "DROP FOREIGN TABLE IF EXISTS t",
    // jsonb + array types and operators
    "CREATE TABLE t (a jsonb, b int4[], c text[], d numeric(10, 2)[])",
    "CREATE TABLE t (a int4[4])",
    "SELECT a -> 'k' FROM t",
    "SELECT a ->> 'k' FROM t",
    "SELECT a #> '{k,0}' FROM t",
    "SELECT a #>> '{k,0}' FROM t",
    "SELECT a @> b FROM t",
    "SELECT a <@ b FROM t",
    "SELECT a ? 'k' FROM t",
    "SELECT a ?| b FROM t",
    "SELECT a ?& b FROM t",
    "SELECT a && b FROM t",
    "SELECT a - 'k' FROM t",
    "SELECT a ->> 'k' = 'v' FROM t",
    "SELECT ARRAY[1, 2, 3]",
    "SELECT a[1] FROM t",
    "SELECT a[1][2] FROM t",
    "SELECT $1::int4[]",
    "SELECT a = ANY($1) FROM t",
    "SELECT a = ANY(ARRAY[1, 2]) FROM t",
    "SELECT a <> ALL(tags) FROM t",
    "SELECT x FROM unnest(ARRAY[1, 2]) AS u(x)",
    "SELECT * FROM unnest(tags)",
    "SELECT tag FROM t JOIN unnest(t.tags) AS u(tag) ON true",
    // ON CONFLICT
    "INSERT INTO t VALUES (1) ON CONFLICT DO NOTHING",
    "INSERT INTO t VALUES (1) ON CONFLICT (id) DO NOTHING",
    "INSERT INTO t VALUES (1) ON CONFLICT (id, name) DO NOTHING",
    "INSERT INTO t VALUES (1) ON CONFLICT (id) WHERE id > 0 DO NOTHING",
    "INSERT INTO t VALUES (1) ON CONFLICT ON CONSTRAINT t_pkey DO NOTHING",
    "INSERT INTO t (id, v) VALUES (1, 'a') ON CONFLICT (id) DO UPDATE SET v = excluded.v",
    "INSERT INTO t (id, v) VALUES (1, 'a') ON CONFLICT (id) DO UPDATE SET v = excluded.v WHERE t.v <> excluded.v",
    "INSERT INTO t VALUES (1) ON CONFLICT (id) DO UPDATE SET v = 1 RETURNING id",
    // LISTEN / NOTIFY / UNLISTEN
    "LISTEN chan",
    "NOTIFY chan",
    "NOTIFY chan, 'payload'",
    "UNLISTEN chan",
    "UNLISTEN *",
];

/// Clear syntax errors: BOTH parsers must reject.
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
    // jsonb/array operator and constructor grammar
    "SELECT a ->",
    "SELECT a[",
    "SELECT ARRAY[1,",
    "SELECT a = ANY()",
    // ON CONFLICT grammar
    "INSERT INTO t VALUES (1) ON CONFLICT DO",
    "INSERT INTO t VALUES (1) ON CONFLICT () DO NOTHING",
    "INSERT INTO t VALUES (1) ON CONFLICT (id) DO UPDATE",
    "INSERT INTO t VALUES (1) ON CONFLICT ON CONSTRAINT DO NOTHING",
    // LISTEN / NOTIFY / UNLISTEN grammar
    "LISTEN",
    "LISTEN a b",
    "NOTIFY",
    "NOTIFY chan, payload",
    "UNLISTEN",
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
fn compatibility_refusal_representatives_are_postgresql_syntax() {
    let explicit = [
        "ALTER DATABASE postgres RENAME TO other",
        "CREATE DATABASE other",
        "DROP DATABASE other",
        "ALTER EXTENSION plpgsql UPDATE",
        "DROP EXTENSION plpgsql",
        "PREPARE TRANSACTION 'xid-1'",
        "COMMIT PREPARED 'xid-1'",
        "ROLLBACK PREPARED 'xid-1'",
    ];
    for sql in explicit.into_iter().chain(
        crabka_pgparser::ast::NON_GOAL_REFUSALS
            .iter()
            .map(|spec| spec.representative_sql),
    ) {
        assert!(
            pg_accepts(sql),
            "libpg_query rejected refusal representative: {sql}"
        );
        assert!(
            we_accept(sql),
            "pgparser rejected refusal representative: {sql}"
        );
    }
    for spec in crabka_pgparser::ast::NON_GOAL_REFUSALS {
        let variant = refusal_variant(spec.representative_sql);
        assert!(
            pg_accepts(&variant),
            "libpg_query rejected refusal variant for {}: {variant}",
            spec.command.command_name(),
        );
        assert!(
            we_accept(&variant),
            "pgparser rejected refusal variant for {}: {variant}",
            spec.command.command_name(),
        );
    }
}

fn refusal_variant(sql: &str) -> String {
    use crabka_pgparser::token::Token;

    const SLOTS: &[&str] = &[
        "conv",
        "conv2",
        "lang",
        "lang2",
        "postgres",
        "opc",
        "opc2",
        "opf",
        "opf2",
        "pub",
        "r",
        "r2",
        "sub",
        "ts",
        "ts2",
        "p",
        "p2",
        "t",
        "t2",
        "am",
        "handler_fn",
        "func",
        "int4eq",
        "f",
    ];
    let mut parts: Vec<String> = Vec::new();
    for (token, _) in crabka_pgparser::lexer::lex(sql).expect("representative lexes") {
        let part = match token {
            Token::Eof => break,
            Token::Ident(value) if SLOTS.contains(&value.as_str()) => format!("{value}_variant"),
            Token::Ident(value) => value,
            Token::Keyword(keyword) => format!("{keyword:?}").to_ascii_lowercase(),
            Token::StringLit(_) => "'variant'".into(),
            Token::IntLit(_) => "42".into(),
            Token::LParen => "(".into(),
            Token::RParen => ")".into(),
            Token::Comma => ",".into(),
            Token::Eq => "=".into(),
            Token::Lt => "<".into(),
            Token::Plus => "+".into(),
            other => panic!("unexpected refusal token {other:?}"),
        };
        if matches!(part.as_str(), "=" | "+" | "<")
            && parts
                .last()
                .is_some_and(|last| last.chars().all(|character| "=+<".contains(character)))
        {
            parts.last_mut().expect("operator part").push_str(&part);
        } else {
            parts.push(part);
        }
    }
    parts.join(" ")
}

#[test]
fn agreement_on_rejected() {
    for &sql in REJECTED {
        assert!(!pg_accepts(sql), "libpg_query should reject: {sql}");
        assert!(!we_accept(sql), "pgparser should reject (PG does): {sql}");
    }
}

/// Constructs that `PostgreSQL`'s grammar accepts but this parser deliberately
/// refuses (0A000 / 42601). The list keeps the divergence explicit and prevents
/// an accidental acceptance.
#[test]
fn deliberately_unsupported_array_and_conflict_constructs_are_explicitly_bounded() {
    for sql in [
        // Array slices, multidimensional arrays and ARRAY(subquery) are
        // out of scope for the one-dimensional array slice implemented here.
        "SELECT a[1:2] FROM t",
        "SELECT a[:2] FROM t",
        "SELECT a[1:] FROM t",
        "SELECT ARRAY(SELECT 1)",
        "CREATE TABLE t (a int4[][])",
        // Element types with no supported array type.
        "CREATE TABLE t (a varchar(10)[])",
        "SELECT $1::regclass[]",
        // PostgreSQL's raw grammar accepts a target-less DO UPDATE and rejects
        // it in parse analysis; this parser rejects it in the grammar instead.
        "INSERT INTO t VALUES (1) ON CONFLICT DO UPDATE SET v = 1",
    ] {
        assert!(pg_accepts(sql), "PostgreSQL grammar should accept: {sql}");
        assert!(
            !we_accept(sql),
            "pgparser must reject the deliberately unsupported construct: {sql}"
        );
    }
}

#[test]
fn unsupported_discard_variants_are_explicitly_bounded() {
    for sql in ["DISCARD PLANS", "DISCARD SEQUENCES", "DISCARD TEMP"] {
        assert!(pg_accepts(sql), "PostgreSQL grammar should accept: {sql}");
        assert!(
            !we_accept(sql),
            "pgparser must reject the unsupported non-ALL variant: {sql}"
        );
    }
}
