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

pub const FEATURE_PROBES: &[FeatureProbe] = &[
    FeatureProbe {
        item: "ARRAY expressions and operators",
        sql: "SELECT ARRAY[1, 2]",
        behavior: FeatureBehavior::ParserRejectPending,
        setup: NONE,
        sqlstate: None,
        message_fragment: None,
    },
    FeatureProbe {
        item: "COLLATE expression",
        sql: "SELECT 'a' COLLATE \"C\"",
        behavior: FeatureBehavior::ParserRejectPending,
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
        item: "Extended-protocol parameterized queries",
        sql: "SELECT $1::int4",
        behavior: FeatureBehavior::ExtendedExecute,
        setup: NONE,
        sqlstate: None,
        message_fragment: None,
    },
    FeatureProbe {
        item: "`information_schema` starter views",
        sql: "SELECT schema_name FROM information_schema.schemata",
        behavior: FeatureBehavior::SessionExecute,
        setup: NONE,
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
        behavior: FeatureBehavior::ParserRejectPending,
        setup: NONE,
        sqlstate: None,
        message_fragment: None,
    },
    FeatureProbe {
        item: "Scalar type `smallint` / `int2`",
        sql: "CREATE TABLE feature_smallint (v smallint)",
        behavior: FeatureBehavior::ParserRejectPending,
        setup: NONE,
        sqlstate: None,
        message_fragment: None,
    },
    FeatureProbe {
        item: "CHECK constraints",
        sql: "CREATE TABLE feature_check (id int4 CHECK (id > 0))",
        behavior: FeatureBehavior::SessionRefuse,
        setup: NONE,
        sqlstate: Some("0A000"),
        message_fragment: Some("CHECK constraints in CREATE TABLE are parsed but not enforced yet"),
    },
    FeatureProbe {
        item: "GROUPING SETS / ROLLUP / CUBE",
        sql: "SELECT count(*) FROM feature_t GROUP BY ROLLUP(id)",
        behavior: FeatureBehavior::SessionRefuse,
        setup: TABLE_ID_ROW,
        sqlstate: Some("42883"),
        message_fragment: Some("function rollup"),
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
        behavior: FeatureBehavior::ParserRejectPending,
        setup: TABLE_ID,
        sqlstate: None,
        message_fragment: None,
    },
    FeatureProbe {
        item: "OLD/NEW RETURNING aliases",
        sql: "INSERT INTO feature_t VALUES (1) RETURNING WITH (OLD AS o, NEW AS n) n.id",
        behavior: FeatureBehavior::ParserRejectPending,
        setup: TABLE_ID,
        sqlstate: None,
        message_fragment: None,
    },
    FeatureProbe {
        item: "Recursive CTE SEARCH / CYCLE",
        sql: "WITH RECURSIVE t(n) AS (VALUES (1)) SEARCH DEPTH FIRST BY n SET ordercol SELECT n FROM t",
        behavior: FeatureBehavior::ParserRejectPending,
        setup: NONE,
        sqlstate: None,
        message_fragment: None,
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
        behavior: FeatureBehavior::ParserRejectPending,
        setup: NONE,
        sqlstate: None,
        message_fragment: None,
    },
    FeatureProbe {
        item: "Row locking NOWAIT / SKIP LOCKED / KEY SHARE",
        sql: "SELECT id FROM feature_t FOR UPDATE NOWAIT",
        behavior: FeatureBehavior::ParserRejectPending,
        setup: TABLE_ID,
        sqlstate: None,
        message_fragment: None,
    },
    FeatureProbe {
        item: "SQL/JSON constructors and aggregates",
        sql: "SELECT JSON_OBJECT('a' VALUE 1)",
        behavior: FeatureBehavior::ParserRejectPending,
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
        item: "WITH ORDINALITY / ROWS FROM",
        sql: "SELECT * FROM generate_series(1, 2) WITH ORDINALITY",
        behavior: FeatureBehavior::ParserRejectPending,
        setup: NONE,
        sqlstate: None,
        message_fragment: None,
    },
];
