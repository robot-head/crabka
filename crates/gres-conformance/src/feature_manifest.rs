//! Typed behavior manifest for major language-feature matrix rows.

use serde::Serialize;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "kebab-case")]
pub enum FeatureBehavior {
    SessionExecute,
    ExtendedExecute,
    ParserRejectPending,
    SessionRefuse,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
pub struct FeatureProbe {
    pub item: &'static str,
    pub sql: &'static str,
    pub behavior: FeatureBehavior,
    pub setup: &'static [&'static str],
    #[serde(skip_serializing_if = "Option::is_none")]
    pub sqlstate: Option<&'static str>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub message_fragment: Option<&'static str>,
}

const NONE: &[&str] = &[];
const TABLE_ID: &[&str] = &["CREATE TABLE feature_t (id int4)"];
const TABLE_ID_ROW: &[&str] = &[
    "CREATE TABLE feature_t (id int4)",
    "INSERT INTO feature_t VALUES (1)",
];
const SEQUENCE: &[&str] = &["CREATE SEQUENCE feature_seq"];
/// A table and a view over it, so the definition-reconstruction functions have
/// an object to rebuild.
const FEATURE_VIEW: &[&str] = &[
    "CREATE TABLE feature_t (id int4)",
    "CREATE VIEW feature_view AS SELECT id FROM feature_t",
];
const UPSERT_TARGET: &[&str] = &[
    "CREATE TABLE feature_upsert (id int4 PRIMARY KEY, n int4)",
    "INSERT INTO feature_upsert VALUES (1, 1)",
];
/// A referenced parent, a child that cascades on delete, and a self-reference
/// the child row satisfies within its own `INSERT` — so the probe's `DELETE`
/// succeeds only if the referential action actually runs: with the constraint
/// present but the cascade missing, the parent-side check refuses with 23503.
const FOREIGN_KEY_TABLES: &[&str] = &[
    "CREATE TABLE feature_fk_parent (id int4 PRIMARY KEY)",
    "CREATE TABLE feature_fk_child (id int4 PRIMARY KEY, \
     parent_id int4 REFERENCES feature_fk_parent (id) ON DELETE CASCADE, \
     boss int4 REFERENCES feature_fk_child (id))",
    "INSERT INTO feature_fk_parent VALUES (1)",
    "INSERT INTO feature_fk_child VALUES (10, 1, 10)",
];

pub const FEATURE_PROBES: &[FeatureProbe] = &[
    FeatureProbe {
        item: "Advisory lock functions",
        sql: "SELECT pg_advisory_lock(1), pg_try_advisory_lock(2), pg_advisory_unlock_all()",
        behavior: FeatureBehavior::SessionExecute,
        setup: NONE,
        sqlstate: None,
        message_fragment: None,
    },
    FeatureProbe {
        item: "`pg_prepared_statements` view",
        sql: "SELECT name, statement, from_sql FROM pg_prepared_statements",
        behavior: FeatureBehavior::SessionExecute,
        setup: NONE,
        sqlstate: None,
        message_fragment: None,
    },
    FeatureProbe {
        item: "ARRAY expressions and operators",
        sql: "SELECT ARRAY[1, 2]",
        behavior: FeatureBehavior::SessionExecute,
        setup: NONE,
        sqlstate: None,
        message_fragment: None,
    },
    FeatureProbe {
        item: "Non-decimal and separated numeric literals",
        sql: "SELECT 0x1F, 0o17, 0b11, 1_000",
        behavior: FeatureBehavior::SessionExecute,
        setup: NONE,
        sqlstate: None,
        message_fragment: None,
    },
    FeatureProbe {
        // One statement per keyword-argument form, so a regression in any of the
        // four grammars shows up here.
        item: "SQL-standard keyword-argument call forms",
        sql: "SELECT substring('abcdef' FROM 2 FOR 3), substring('abcdef' FROM 'b.d'), \
              trim(leading 'x' from 'xxa'), position('b' in 'abc'), \
              overlay('abcdef' placing 'ZZ' from 2 for 3)",
        behavior: FeatureBehavior::SessionExecute,
        setup: NONE,
        sqlstate: None,
        message_fragment: None,
    },
    FeatureProbe {
        item: "Derived-table alias",
        sql: "SELECT x FROM (SELECT 1 AS x), (SELECT 2 AS y)",
        behavior: FeatureBehavior::SessionExecute,
        setup: NONE,
        sqlstate: None,
        message_fragment: None,
    },
    FeatureProbe {
        item: "Dollar-quoted and escape string literals",
        sql: "SELECT $$dollar$$, E'tab\\there'",
        behavior: FeatureBehavior::SessionExecute,
        setup: NONE,
        sqlstate: None,
        message_fragment: None,
    },
    FeatureProbe {
        item: "Regex-match, bitwise, and arithmetic operators",
        sql: "SELECT 'abc' ~ 'b', 5 & 3, 5 # 3, 1 << 3, 2 ^ 3, 4 % 3, @ -5, |/ 16.0",
        behavior: FeatureBehavior::SessionExecute,
        setup: NONE,
        sqlstate: None,
        message_fragment: None,
    },
    FeatureProbe {
        // `window` is a bare `ColLabel`, `collate` a bare label with no `AS`,
        // and `between` a `ColLabel` after `AS` — one probe per class.
        item: "Keyword classification (`ColId` / `BareColLabel` / `ColLabel`)",
        sql: "SELECT 1 AS window, 2 collate, 3 AS between",
        behavior: FeatureBehavior::SessionExecute,
        setup: NONE,
        sqlstate: None,
        message_fragment: None,
    },
    FeatureProbe {
        item: "COLLATE expression",
        sql: "SELECT 'a' COLLATE \"C\"",
        behavior: FeatureBehavior::SessionExecute,
        setup: NONE,
        sqlstate: None,
        message_fragment: None,
    },
    FeatureProbe {
        item: "Column DEFAULT constraints",
        sql: "CREATE TABLE feature_default (id int4 DEFAULT 1)",
        behavior: FeatureBehavior::SessionExecute,
        setup: NONE,
        sqlstate: None,
        message_fragment: None,
    },
    FeatureProbe {
        item: "Column NOT NULL constraints",
        sql: "CREATE TABLE feature_not_null (id int4 NOT NULL)",
        behavior: FeatureBehavior::SessionExecute,
        setup: NONE,
        sqlstate: None,
        message_fragment: None,
    },
    FeatureProbe {
        item: "Scalar type `jsonb`",
        sql: "SELECT '{\"b\": 1, \"a\": 2}'::jsonb -> 'a'",
        behavior: FeatureBehavior::SessionExecute,
        setup: NONE,
        sqlstate: None,
        message_fragment: None,
    },
    FeatureProbe {
        item: "`json` type as a `jsonb` input alias",
        sql: "SELECT '{\"a\": 1}'::json",
        behavior: FeatureBehavior::SessionExecute,
        setup: NONE,
        sqlstate: None,
        message_fragment: None,
    },
    FeatureProbe {
        item: "INSERT ... ON CONFLICT",
        sql: "INSERT INTO feature_upsert VALUES (1, 2) ON CONFLICT (id) DO UPDATE SET n = excluded.n",
        behavior: FeatureBehavior::SessionExecute,
        setup: UPSERT_TARGET,
        sqlstate: None,
        message_fragment: None,
    },
    FeatureProbe {
        item: "`pg_notify` notification function",
        sql: "SELECT pg_notify('feature_channel', 'payload')",
        behavior: FeatureBehavior::SessionExecute,
        setup: NONE,
        sqlstate: None,
        message_fragment: None,
    },
    FeatureProbe {
        item: "Extended-protocol parameterized queries",
        sql: "SELECT $1::int4",
        behavior: FeatureBehavior::ExtendedExecute,
        setup: NONE,
        sqlstate: None,
        message_fragment: None,
    },
    FeatureProbe {
        item: "`information_schema` views",
        sql: "SELECT constraint_name, constraint_type FROM information_schema.table_constraints",
        behavior: FeatureBehavior::SessionExecute,
        setup: NONE,
        sqlstate: None,
        message_fragment: None,
    },
    FeatureProbe {
        item: "`pg_catalog` introspection relations",
        sql: "SELECT c.relname, c.relkind, am.amname \
              FROM pg_catalog.pg_class c \
              LEFT JOIN pg_catalog.pg_am am ON am.oid = c.relam \
              LEFT JOIN pg_catalog.pg_constraint con ON con.conrelid = c.oid",
        behavior: FeatureBehavior::SessionExecute,
        setup: NONE,
        sqlstate: None,
        message_fragment: None,
    },
    FeatureProbe {
        item: "`pg_catalog` object-definition and identity functions",
        sql: "SELECT pg_catalog.pg_get_viewdef('feature_view'), \
                     pg_catalog.pg_get_userbyid(10), \
                     pg_catalog.pg_size_pretty(10240::int8), \
                     pg_catalog.has_table_privilege('feature_t', 'SELECT')",
        behavior: FeatureBehavior::SessionExecute,
        setup: FEATURE_VIEW,
        sqlstate: None,
        message_fragment: None,
    },
    FeatureProbe {
        item: "Scalar type `varchar(n)` / `character varying(n)`",
        sql: "CREATE TABLE feature_varchar (v varchar(8))",
        behavior: FeatureBehavior::SessionExecute,
        setup: NONE,
        sqlstate: None,
        message_fragment: None,
    },
    FeatureProbe {
        item: "Scalar type `char(n)` / `character(n)`",
        sql: "CREATE TABLE feature_char (v char(8))",
        behavior: FeatureBehavior::SessionExecute,
        setup: NONE,
        sqlstate: None,
        message_fragment: None,
    },
    FeatureProbe {
        item: "Scalar type `uuid`",
        sql: "CREATE TABLE feature_uuid (v uuid)",
        behavior: FeatureBehavior::SessionExecute,
        setup: NONE,
        sqlstate: None,
        message_fragment: None,
    },
    FeatureProbe {
        item: "Scalar type `real` / `float4`",
        sql: "CREATE TABLE feature_real (v real)",
        behavior: FeatureBehavior::SessionExecute,
        setup: NONE,
        sqlstate: None,
        message_fragment: None,
    },
    FeatureProbe {
        item: "Scalar type `smallint` / `int2`",
        sql: "CREATE TABLE feature_smallint (v smallint)",
        behavior: FeatureBehavior::SessionExecute,
        setup: NONE,
        sqlstate: None,
        message_fragment: None,
    },
    FeatureProbe {
        item: "Scalar type `time with time zone` / `timetz`",
        sql: "CREATE TABLE feature_timetz (v time with time zone)",
        behavior: FeatureBehavior::SessionExecute,
        setup: NONE,
        sqlstate: None,
        message_fragment: None,
    },
    FeatureProbe {
        item: "Date/time literal input, special values, and field ranges",
        sql: "SELECT 'infinity'::timestamp, interval '1' year to month, \
              extract(epoch from timestamp '2024-01-15')",
        behavior: FeatureBehavior::SessionExecute,
        setup: NONE,
        sqlstate: None,
        message_fragment: None,
    },
    FeatureProbe {
        item: "CHECK constraints",
        sql: "CREATE TABLE feature_check (id int4 CHECK (id > 0))",
        behavior: FeatureBehavior::SessionExecute,
        setup: NONE,
        sqlstate: None,
        message_fragment: None,
    },
    FeatureProbe {
        item: "Window functions",
        sql: "SELECT id, rank() OVER (PARTITION BY id ORDER BY id \
              ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) FROM feature_t",
        behavior: FeatureBehavior::SessionExecute,
        setup: TABLE_ID_ROW,
        sqlstate: None,
        message_fragment: None,
    },
    FeatureProbe {
        item: "GROUPING SETS / ROLLUP / CUBE",
        sql: "SELECT id, grouping(id), count(*) FROM feature_t GROUP BY ROLLUP(id)",
        behavior: FeatureBehavior::SessionExecute,
        setup: TABLE_ID_ROW,
        sqlstate: None,
        message_fragment: None,
    },
    FeatureProbe {
        item: "Common table expressions and WITH RECURSIVE",
        sql: "WITH RECURSIVE t(n) AS (VALUES (1) UNION ALL SELECT n + 1 FROM t WHERE n < 3) \
              SELECT sum(n) FROM t",
        behavior: FeatureBehavior::SessionExecute,
        setup: NONE,
        sqlstate: None,
        message_fragment: None,
    },
    FeatureProbe {
        item: "JSON_TABLE and SQL/JSON expressions",
        sql: "SELECT * FROM JSON_TABLE('{}', '$' COLUMNS (v int4 PATH '$.v')) AS jt",
        behavior: FeatureBehavior::ParserRejectPending,
        setup: NONE,
        sqlstate: None,
        message_fragment: None,
    },
    FeatureProbe {
        item: "MERGE NOT MATCHED BY SOURCE / RETURNING",
        sql: "MERGE INTO feature_t USING feature_t AS s ON false WHEN NOT MATCHED BY SOURCE THEN DELETE RETURNING *",
        behavior: FeatureBehavior::SessionExecute,
        setup: TABLE_ID,
        sqlstate: None,
        message_fragment: None,
    },
    FeatureProbe {
        item: "OLD/NEW RETURNING aliases",
        sql: "INSERT INTO feature_t VALUES (1) RETURNING WITH (OLD AS o, NEW AS n) n.id",
        behavior: FeatureBehavior::SessionExecute,
        setup: TABLE_ID,
        sqlstate: None,
        message_fragment: None,
    },
    FeatureProbe {
        item: "Recursive CTE SEARCH / CYCLE",
        sql: "WITH RECURSIVE t(n) AS (VALUES (1) UNION ALL SELECT n + 1 FROM t WHERE n < 3) \
              SEARCH DEPTH FIRST BY n SET ordercol SELECT n FROM t",
        behavior: FeatureBehavior::SessionRefuse,
        setup: NONE,
        sqlstate: Some("0A000"),
        message_fragment: Some("SEARCH and CYCLE"),
    },
    FeatureProbe {
        item: "Sequence functions",
        sql: "SELECT nextval('feature_seq')",
        behavior: FeatureBehavior::SessionExecute,
        setup: SEQUENCE,
        sqlstate: None,
        message_fragment: None,
    },
    FeatureProbe {
        item: "SQL identity / generated columns",
        sql: "CREATE TABLE feature_identity (id int4 GENERATED ALWAYS AS IDENTITY)",
        behavior: FeatureBehavior::SessionExecute,
        setup: NONE,
        sqlstate: None,
        message_fragment: None,
    },
    FeatureProbe {
        item: "Row locking NOWAIT / SKIP LOCKED / KEY SHARE",
        sql: "SELECT id FROM feature_t FOR UPDATE NOWAIT",
        behavior: FeatureBehavior::SessionExecute,
        setup: TABLE_ID,
        sqlstate: None,
        message_fragment: None,
    },
    FeatureProbe {
        item: "SQL/JSON constructors and aggregates",
        sql: "SELECT JSON_OBJECT('a' VALUE 1 RETURNING jsonb), JSON_ARRAY(1, 2 RETURNING jsonb), \
              JSON_VALUE(jsonb '{\"a\": 1}', '$.a'), '1' IS JSON SCALAR",
        behavior: FeatureBehavior::SessionExecute,
        setup: NONE,
        sqlstate: None,
        message_fragment: None,
    },
    FeatureProbe {
        item: "Table PRIMARY KEY / UNIQUE constraints",
        sql: "CREATE TABLE feature_unique (id int4 PRIMARY KEY, value int4 UNIQUE)",
        behavior: FeatureBehavior::SessionExecute,
        setup: NONE,
        sqlstate: None,
        message_fragment: None,
    },
    FeatureProbe {
        item: "FOREIGN KEY constraints",
        sql: "DELETE FROM feature_fk_parent WHERE id = 1",
        behavior: FeatureBehavior::SessionExecute,
        setup: FOREIGN_KEY_TABLES,
        sqlstate: None,
        message_fragment: None,
    },
    FeatureProbe {
        item: "WITH ORDINALITY / ROWS FROM",
        sql: "SELECT * FROM generate_series(1, 2) WITH ORDINALITY",
        behavior: FeatureBehavior::SessionExecute,
        setup: NONE,
        sqlstate: None,
        message_fragment: None,
    },
    FeatureProbe {
        item: "SELECT DISTINCT ON",
        sql: "SELECT DISTINCT ON (id) id FROM feature_t ORDER BY id",
        behavior: FeatureBehavior::SessionExecute,
        setup: TABLE_ID,
        sqlstate: None,
        message_fragment: None,
    },
    FeatureProbe {
        item: "LATERAL FROM items",
        sql: "SELECT t.id, g FROM feature_t t, LATERAL generate_series(1, t.id) g",
        behavior: FeatureBehavior::SessionExecute,
        setup: TABLE_ID,
        sqlstate: None,
        message_fragment: None,
    },
    FeatureProbe {
        item: "ORDER BY / row-count clause breadth",
        sql: "SELECT id FROM feature_t ORDER BY id USING < NULLS FIRST LIMIT '1'",
        behavior: FeatureBehavior::SessionExecute,
        setup: TABLE_ID,
        sqlstate: None,
        message_fragment: None,
    },
    FeatureProbe {
        item: "TABLESAMPLE",
        sql: "SELECT id FROM feature_t TABLESAMPLE BERNOULLI (100)",
        behavior: FeatureBehavior::SessionExecute,
        setup: TABLE_ID,
        sqlstate: None,
        message_fragment: None,
    },
    FeatureProbe {
        // `\d` is a psql meta-command, so the executable surface behind the row
        // is the catalog query psql actually issues for it.
        item: "`psql` `\\d` family",
        sql: "SELECT c.relname, c.relkind FROM pg_catalog.pg_class c \
              JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace \
              WHERE n.nspname = 'public' ORDER BY c.relname",
        behavior: FeatureBehavior::SessionExecute,
        setup: TABLE_ID,
        sqlstate: None,
        message_fragment: None,
    },
    FeatureProbe {
        item: "`reg*` object-identifier types",
        sql: "SELECT 'int4'::regtype",
        behavior: FeatureBehavior::ParserRejectPending,
        setup: NONE,
        sqlstate: None,
        message_fragment: None,
    },
];
