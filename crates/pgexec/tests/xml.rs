//! The `xml` type end to end: input and output, the two well-formedness
//! grammars, `XMLPARSE`/`XMLSERIALIZE`/`IS DOCUMENT`, the small function family,
//! storage, and the many things `PostgreSQL` refuses to do with a type that has
//! no comparison operator.
//!
//! Every expected value here was taken from `PostgreSQL` 18.4 built with libxml,
//! including the ones that look wrong: `'<?xml version="1.0"?><foo/>'::xml`
//! really does *display* as `<foo/>` while the same value cast to `text` keeps
//! its declaration, because `xml_out` re-renders the declaration and the
//! `xml → text` cast is binary-coercible and does not.

use assert2::assert;
use crabka_pgexec::{SqlEngine, SqlSession};
use crabka_pgwire::engine::{Cell, Engine, QueryResult, Session};

/// `pg_type.oid` of `xml` and `xml[]`.
const XML_OID: u32 = 142;
const XMLARRAY_OID: u32 = 143;

// ---------------------------------------------------------------------------
// Harness
// ---------------------------------------------------------------------------

fn cell_text(cell: Option<&Cell>) -> Option<String> {
    cell.map(|c| String::from_utf8(c.text.to_vec()).expect("utf8"))
}

async fn run(s: &mut SqlSession, sql: &str) -> Vec<QueryResult> {
    s.simple_query(sql)
        .await
        .unwrap_or_else(|e| panic!("`{sql}` should succeed: {e:?}"))
}

async fn rows(s: &mut SqlSession, sql: &str) -> Vec<Vec<Option<String>>> {
    match &run(s, sql).await[0] {
        QueryResult::Rows { rows, .. } => rows
            .iter()
            .map(|row| row.iter().map(|c| cell_text(c.as_ref())).collect())
            .collect(),
        other => panic!("`{sql}` should return rows, got {other:?}"),
    }
}

async fn scalar(s: &mut SqlSession, expr: &str) -> Option<String> {
    let sql = format!("SELECT {expr}");
    let rows = rows(s, &sql).await;
    assert!(rows.len() == 1 && rows[0].len() == 1, "{sql}");
    rows[0][0].clone()
}

/// The value and the OID the `RowDescription` reports for it.
async fn typed_scalar(s: &mut SqlSession, expr: &str) -> (Option<String>, u32) {
    let sql = format!("SELECT {expr}");
    match &run(s, &sql).await[0] {
        QueryResult::Rows { rows, fields, .. } => {
            (cell_text(rows[0][0].as_ref()), fields[0].type_oid)
        }
        other => panic!("`{sql}` should return rows, got {other:?}"),
    }
}

/// The SQLSTATE, message and DETAIL a statement that must fail reports.
async fn err(s: &mut SqlSession, sql: &str) -> (String, String, Option<String>) {
    let e = s
        .simple_query(sql)
        .await
        .expect_err("statement should fail");
    (e.code, e.message, e.diagnostics.and_then(|d| d.detail))
}

fn session() -> (SqlEngine, SqlSession) {
    let engine = SqlEngine::new();
    let s = engine.connect();
    (engine, s)
}

// ---------------------------------------------------------------------------
// The type
// ---------------------------------------------------------------------------

/// `xml` is a real type at `PostgreSQL`'s own OID, and it keeps the bytes it was
/// given. Without the type this is `type "xml" does not exist`, so every
/// assertion here fails on an engine that has not got one.
#[tokio::test]
async fn xml_is_a_type_that_keeps_its_input_text() {
    let (_engine, mut s) = session();

    assert!(typed_scalar(&mut s, "'<a/>'::xml").await == (Some("<a/>".into()), XML_OID));
    assert!(
        typed_scalar(&mut s, "ARRAY['<a/>'::xml]").await == (Some("{<a/>}".into()), XMLARRAY_OID)
    );
    assert!(scalar(&mut s, "pg_typeof('<a/>'::xml)").await == Some("xml".into()));
    assert!(scalar(&mut s, "pg_typeof(ARRAY['<a/>'::xml])").await == Some("xml[]".into()));
    assert!(scalar(&mut s, "format_type(142, -1)").await == Some("xml".into()));
    assert!(scalar(&mut s, "format_type(143, -1)").await == Some("xml[]".into()));

    // Whitespace, attribute spelling and quoting all survive a round trip:
    // `xml_in` validates and stores, it does not rewrite.
    let verbatim = "  spaced  <a  b = ''1'' />  ";
    assert!(
        scalar(&mut s, &format!("'{verbatim}'::xml")).await
            == Some("  spaced  <a  b = '1' />  ".into())
    );

    // `xml` and `_xml` are in `pg_type` with the shape PostgreSQL gives them.
    assert!(
        rows(
            &mut s,
            "SELECT typname, typlen, typcategory, typelem, typarray FROM pg_type \
             WHERE oid IN (142, 143) ORDER BY oid"
        )
        .await
            == vec![
                vec![
                    Some("xml".into()),
                    Some("-1".into()),
                    Some("U".into()),
                    Some("0".into()),
                    Some("143".into())
                ],
                vec![
                    Some("_xml".into()),
                    Some("-1".into()),
                    Some("A".into()),
                    Some("142".into()),
                    Some("0".into())
                ],
            ]
    );
}

/// `xml_out` re-renders the XML declaration; the `xml → text` cast is
/// binary-coercible and does not. The same stored value therefore prints two
/// different ways depending on which one you ask for.
#[tokio::test]
async fn output_rewrites_the_declaration_and_the_text_cast_does_not() {
    let (_engine, mut s) = session();

    let cases = [
        // (stored text, xml_out, xml::text)
        (
            r#"<?xml version="1.0"?><foo/>"#,
            "<foo/>",
            r#"<?xml version="1.0"?><foo/>"#,
        ),
        (
            r#"<?xml version="1.0" encoding="UTF-8"?><foo/>"#,
            "<foo/>",
            r#"<?xml version="1.0" encoding="UTF-8"?><foo/>"#,
        ),
        // A non-default version survives, and so does an explicit standalone.
        (
            r#"<?xml version="1.1"?><foo/>"#,
            r#"<?xml version="1.1"?><foo/>"#,
            r#"<?xml version="1.1"?><foo/>"#,
        ),
        (
            r#"<?xml version="1.0" standalone="yes"?><foo/>"#,
            r#"<?xml version="1.0" standalone="yes"?><foo/>"#,
            r#"<?xml version="1.0" standalone="yes"?><foo/>"#,
        ),
        ("<foo/>", "<foo/>", "<foo/>"),
    ];
    for (stored, displayed, as_text) in cases {
        assert!(
            scalar(&mut s, &format!("'{stored}'::xml")).await == Some(displayed.into()),
            "{stored}"
        );
        assert!(
            scalar(&mut s, &format!("'{stored}'::xml::text")).await == Some(as_text.into()),
            "{stored}"
        );
    }
}

// ---------------------------------------------------------------------------
// XMLPARSE and the two grammars
// ---------------------------------------------------------------------------

/// DOCUMENT wants one root; CONTENT takes a fragment, bare text, or nothing.
#[tokio::test]
async fn xmlparse_document_and_content_accept_different_shapes() {
    let (_engine, mut s) = session();

    let cases = [
        // (value, valid as CONTENT, valid as DOCUMENT)
        ("", true, false),
        ("  ", true, false),
        ("abc", true, false),
        ("<abc>x</abc>", true, true),
        ("<a/><b/>", true, false),
        (" <a/> ", true, true),
        ("<!DOCTYPE a><a/>", true, true),
        // A leading doctype promotes CONTENT to the document grammar, so a
        // second root is rejected under both spellings.
        ("<!DOCTYPE a><a/><b/>", false, false),
        // Namespaces are not checked at parse time -- only xpath cares.
        ("<nosuchprefix:tag/>", true, true),
        ("<invalidns xmlns='&lt;'/>", true, true),
        ("<wrong", false, false),
        ("<123/>", false, false),
    ];
    for (value, content_ok, document_ok) in cases {
        let quoted = value.replace('\'', "''");
        for (mode, ok) in [("CONTENT", content_ok), ("DOCUMENT", document_ok)] {
            let sql = format!("SELECT XMLPARSE({mode} '{quoted}')");
            let result = s.simple_query(&sql).await;
            assert!(result.is_ok() == ok, "{sql}");
            if ok {
                // A parsed value is its input, byte for byte.
                assert!(
                    cell_text(match &result.expect("rows")[0] {
                        QueryResult::Rows { rows, .. } => rows[0][0].as_ref(),
                        other => panic!("{other:?}"),
                    }) == Some(value.to_string()),
                    "{sql}"
                );
            }
        }
    }
    assert!(scalar(&mut s, "XMLPARSE(CONTENT NULL) IS NULL").await == Some("t".into()));
    // The value is coerced to text by the grammar, so a number parses.
    assert!(scalar(&mut s, "XMLPARSE(CONTENT 1)").await == Some("1".into()));
}

/// A malformed value is 2200M or 2200N with libxml's own DETAIL, and libxml
/// keeps going after a recoverable fault so one value can report two.
#[tokio::test]
async fn malformed_xml_carries_postgres_sqlstate_and_libxml_detail() {
    let (_engine, mut s) = session();

    let cases = [
        (
            "SELECT XMLPARSE(CONTENT '<undefinedentity>&idontexist;</undefinedentity>')",
            "2200N",
            "invalid XML content",
            "line 1: Entity 'idontexist' not defined",
        ),
        (
            "SELECT XMLPARSE(DOCUMENT 'abc')",
            "2200M",
            "invalid XML document",
            "line 1: Start tag expected, '<' not found",
        ),
        (
            "SELECT XMLPARSE(CONTENT '<a b=\"1\" b=\"2\"/>')",
            "2200N",
            "invalid XML content",
            "line 1: Attribute b redefined",
        ),
        (
            "SELECT XMLPARSE(CONTENT '<?xml version=\"1.0\" standalone=\"y\"?><foo/>')",
            "2200N",
            "invalid XML content: invalid XML declaration",
            "standalone accepts only 'yes' or 'no'.",
        ),
    ];
    for (sql, code, message, detail_head) in cases {
        let (got_code, got_message, detail) = err(&mut s, sql).await;
        assert!(got_code == code, "{sql}");
        assert!(got_message == message, "{sql}");
        assert!(
            detail
                .as_deref()
                .is_some_and(|d| d.starts_with(detail_head)),
            "{sql}: {detail:?}"
        );
    }

    // Two faults, both reported, in libxml's order.
    let (_, _, detail) = err(
        &mut s,
        "SELECT XMLPARSE(CONTENT '<twoerrors>&idontexist;</unbalanced>')",
    )
    .await;
    let detail = detail.expect("detail");
    assert!(
        detail.contains("Entity 'idontexist' not defined"),
        "{detail}"
    );
    assert!(
        detail.contains("Opening and ending tag mismatch: twoerrors line 1 and unbalanced"),
        "{detail}"
    );
}

/// The security property: an external entity is declared, referenced, echoed
/// back unexpanded, and never fetched.
///
/// A parser with entity expansion left on would make `XMLPARSE` an
/// arbitrary-file-read primitive reachable from any client that can `SELECT`.
/// The two probes below are `xml.sql`'s own, plus the external-DTD one; the
/// pass condition is not merely "no crash" but that the file's contents do not
/// appear, that no filesystem error surfaces, and that a *missing* file behaves
/// identically to a present one — which is only possible if neither was opened.
#[tokio::test]
async fn external_entities_are_never_resolved() {
    let (_engine, mut s) = session();

    let present = r#"<!DOCTYPE foo [<!ENTITY c SYSTEM "/etc/passwd">]><foo>&c;</foo>"#;
    let missing = r#"<!DOCTYPE foo [<!ENTITY c SYSTEM "/etc/no.such.file">]><foo>&c;</foo>"#;
    for probe in [present, missing] {
        for mode in ["DOCUMENT", "CONTENT"] {
            let value = scalar(&mut s, &format!("XMLPARSE({mode} '{probe}')")).await;
            assert!(value.as_deref() == Some(probe), "{mode} {probe}");
        }
    }
    // A file that exists and one that does not are indistinguishable, which no
    // implementation that opened either could manage.
    assert!(
        scalar(&mut s, &format!("XMLPARSE(DOCUMENT '{present}')")).await
            != scalar(&mut s, &format!("XMLPARSE(DOCUMENT '{missing}')")).await
    );
    assert!(
        scalar(
            &mut s,
            &format!("length(XMLPARSE(DOCUMENT '{present}')::text)")
        )
        .await
            == Some(present.len().to_string())
    );

    // Serialising rebuilds the tree, so it is the other path a leak could take.
    // The reference expands to nothing at all.
    let indented = scalar(
        &mut s,
        &format!("XMLSERIALIZE(DOCUMENT '{present}' AS text INDENT)"),
    )
    .await
    .expect("indent");
    assert!(!indented.contains("root:"), "{indented}");
    assert!(!indented.contains("/bin/"), "{indented}");
    assert!(indented.ends_with("<foo/>"), "{indented}");

    // An external DTD is not fetched either, and an entity it might have
    // defined is accepted rather than reported undefined.
    let docbook = concat!(
        r#"<!DOCTYPE chapter PUBLIC "-//OASIS//DTD DocBook XML V4.1.2//EN" "#,
        r#""http://www.oasis-open.org/docbook/xml/4.1.2/docbookx.dtd"><chapter>&nbsp;</chapter>"#
    );
    assert!(
        scalar(&mut s, &format!("XMLPARSE(DOCUMENT '{docbook}')")).await
            == Some(docbook.to_string())
    );

    // An entity nothing declared is still an error: the permissiveness above is
    // scoped to references the document itself declared.
    assert!(err(&mut s, "SELECT '<a>&nope;</a>'::xml").await.0 == "2200N");
}

// ---------------------------------------------------------------------------
// XMLSERIALIZE
// ---------------------------------------------------------------------------

#[tokio::test]
async fn xmlserialize_returns_the_value_and_indent_reformats_it() {
    let (_engine, mut s) = session();

    // Without INDENT the value comes back unchanged, whatever its spacing.
    assert!(
        scalar(&mut s, "XMLSERIALIZE(CONTENT '<a  b=''1'' />' AS text)").await
            == Some("<a  b='1' />".into())
    );
    assert!(
        scalar(&mut s, "XMLSERIALIZE(CONTENT '<a/>' AS text)").await
            == scalar(&mut s, "XMLSERIALIZE(CONTENT '<a/>' AS text NO INDENT)").await
    );
    // DOCUMENT without INDENT still has to *be* a document.
    let (code, message, _) = err(&mut s, "SELECT XMLSERIALIZE(DOCUMENT 'bad' AS text)").await;
    assert!(code == "2200L");
    assert!(message == "not an XML document");

    let cases = [
        (
            "DOCUMENT",
            r#"<foo><bar><val x="y">42</val></bar></foo>"#,
            "<foo>\n  <bar>\n    <val x=\"y\">42</val>\n  </bar>\n</foo>",
        ),
        // Blank text nodes between elements are dropped; an element's own
        // whitespace-only content is not.
        (
            "DOCUMENT",
            "<foo>   <bar></bar>    </foo>",
            "<foo>\n  <bar/>\n</foo>",
        ),
        // Mixed content is never broken across lines.
        (
            "CONTENT",
            r#"<foo><bar><val x="y">text node<val>73</val></val></bar></foo>"#,
            "<foo>\n  <bar>\n    <val x=\"y\">text node<val>73</val></val>\n  </bar>\n</foo>",
        ),
        // Several roots: a newline before each node that is not character data.
        (
            "CONTENT",
            r#"<foo>73</foo><bar><val x="y">42</val></bar>"#,
            "<foo>73</foo>\n<bar>\n  <val x=\"y\">42</val>\n</bar>",
        ),
        ("CONTENT", "", ""),
        ("CONTENT", "  ", "  "),
        // A declaration is re-rendered from the parsed document, so `encoding`
        // appears even though the input had none.
        (
            "DOCUMENT",
            r#"<?xml version="1.0"?><foo/>"#,
            "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n<foo/>",
        ),
    ];
    for (mode, value, expected) in cases {
        let quoted = value.replace('\'', "''");
        assert!(
            scalar(
                &mut s,
                &format!("XMLSERIALIZE({mode} '{quoted}' AS text INDENT)")
            )
            .await
                == Some(expected.into()),
            "{mode} {value}"
        );
    }

    // The target must be a character string type, and the result carries it.
    assert!(
        typed_scalar(&mut s, "XMLSERIALIZE(CONTENT 'good' AS text)").await
            == (Some("good".into()), 25)
    );
    assert!(
        scalar(&mut s, "XMLSERIALIZE(CONTENT 'good' AS char(10))").await
            == Some("good      ".into())
    );
    let (code, message, _) = err(&mut s, "SELECT XMLSERIALIZE(CONTENT '<a/>' AS int)").await;
    assert!(code == "42846");
    assert!(message == "cannot cast XMLSERIALIZE result to integer");
}

// ---------------------------------------------------------------------------
// IS DOCUMENT
// ---------------------------------------------------------------------------

#[tokio::test]
async fn is_document_answers_without_raising_and_only_over_xml() {
    let (_engine, mut s) = session();

    let cases = [
        ("<foo>bar</foo>", "t"),
        ("<foo>bar</foo><bar>foo</bar>", "f"),
        ("<abc/>", "t"),
        ("abc", "f"),
        ("", "f"),
        ("<!DOCTYPE a><a/>", "t"),
    ];
    for (value, expected) in cases {
        assert!(
            scalar(&mut s, &format!("xml '{value}' IS DOCUMENT")).await == Some(expected.into()),
            "{value}"
        );
        let negated = if expected == "t" { "f" } else { "t" };
        assert!(
            scalar(&mut s, &format!("xml '{value}' IS NOT DOCUMENT")).await == Some(negated.into()),
            "{value}"
        );
    }
    // NULL in, NULL out -- unlike `IS TRUE`, this is not a definedness test.
    assert!(scalar(&mut s, "(NULL::xml IS DOCUMENT) IS NULL").await == Some("t".into()));
    // `document` stays a perfectly good column name.
    run(&mut s, "CREATE TABLE isdoc (document int)").await;
    run(&mut s, "INSERT INTO isdoc VALUES (1)").await;
    assert!(rows(&mut s, "SELECT document FROM isdoc").await == vec![vec![Some("1".into())]]);

    let (code, message, _) = err(&mut s, "SELECT 1 IS DOCUMENT").await;
    assert!(code == "42804");
    assert!(message == "argument of IS DOCUMENT must be type xml, not type integer");
}

// ---------------------------------------------------------------------------
// xmlcomment / xmltext / xmlconcat
// ---------------------------------------------------------------------------

#[tokio::test]
async fn the_small_function_family_matches_postgres() {
    let (_engine, mut s) = session();

    assert!(
        typed_scalar(&mut s, "xmlcomment('test')").await == (Some("<!--test-->".into()), XML_OID)
    );
    assert!(scalar(&mut s, "xmlcomment('-test')").await == Some("<!---test-->".into()));
    assert!(scalar(&mut s, "xmlcomment('te st')").await == Some("<!--te st-->".into()));
    for bad in ["--test", "test-"] {
        let (code, message, _) = err(&mut s, &format!("SELECT xmlcomment('{bad}')")).await;
        assert!(code == "2200S", "{bad}");
        assert!(message == "invalid XML comment", "{bad}");
    }

    // `xmlEncodeSpecialChars` escapes the double quote and leaves `>` alone.
    assert!(scalar(&mut s, "xmltext('foo & <bar>')").await == Some("foo &amp; &lt;bar&gt;".into()));
    assert!(scalar(&mut s, "xmltext('a\"b')").await == Some("a&quot;b".into()));

    assert!(
        typed_scalar(&mut s, "xmlconcat('hello', 'you')").await
            == (Some("helloyou".into()), XML_OID)
    );
    assert!(scalar(&mut s, "xmlconcat(NULL) IS NULL").await == Some("t".into()));
    assert!(scalar(&mut s, "xmlconcat(NULL, NULL) IS NULL").await == Some("t".into()));
    assert!(
        scalar(
            &mut s,
            "xmlconcat(xmlcomment('hello'), xmlcomment('world'))"
        )
        .await
            == Some("<!--hello--><!--world-->".into())
    );
    // One part with no declaration silences the merged one entirely.
    assert!(
        scalar(
            &mut s,
            r#"xmlconcat('<foo/>', NULL, '<?xml version="1.1" standalone="no"?><bar/>')"#
        )
        .await
            == Some("<foo/><bar/>".into())
    );
    assert!(
        scalar(
            &mut s,
            r#"xmlconcat('<?xml version="1.1"?><foo/>', '<?xml version="1.1" standalone="no"?><bar/>')"#
        )
        .await
            == Some(r#"<?xml version="1.1"?><foo/><bar/>"#.into())
    );

    // Both refuse a non-string argument, each in its own wording.
    let (code, message, _) = err(&mut s, "SELECT xmlconcat(1, 2)").await;
    assert!(code == "42804");
    assert!(message == "argument of XMLCONCAT must be type xml, not type integer");
    assert!(err(&mut s, "SELECT xmlcomment(1)").await.0 == "42883");
    // A malformed part is rejected when it is coerced.
    assert!(err(&mut s, "SELECT xmlconcat('bad', '<syntax')").await.0 == "2200N");
}

// ---------------------------------------------------------------------------
// No comparison operator
// ---------------------------------------------------------------------------

/// `xml` has no equality operator, no ordering operator and no operator class,
/// so every construct that reaches for one must refuse it — and refuse it with
/// `PostgreSQL`'s own wording, which differs per construct.
#[tokio::test]
async fn xml_has_no_comparison_operator_anywhere() {
    let (_engine, mut s) = session();

    let cases = [
        (
            "SELECT '<a/>'::xml = '<a/>'::xml",
            "operator does not exist: xml = xml",
        ),
        (
            "SELECT '<a/>'::xml <> '<a/>'::xml",
            "operator does not exist: xml <> xml",
        ),
        (
            "SELECT '<a/>'::xml < '<a/>'::xml",
            "operator does not exist: xml < xml",
        ),
        // An untyped literal beside it adopts nothing, so it stays `unknown`.
        (
            "SELECT '<a/>'::xml = '<a/>'",
            "operator does not exist: xml = unknown",
        ),
        (
            "SELECT '<a/>'::xml IS DISTINCT FROM '<a/>'::xml",
            "operator does not exist: xml = xml",
        ),
        (
            "SELECT nullif('<a/>'::xml, '<a/>'::xml)",
            "operator does not exist: xml = xml",
        ),
        (
            "SELECT DISTINCT x FROM (VALUES ('<a/>'::xml)) t(x)",
            "could not identify an equality operator for type xml",
        ),
        (
            "SELECT x FROM (VALUES ('<a/>'::xml)) t(x) GROUP BY x",
            "could not identify an equality operator for type xml",
        ),
        (
            "SELECT x FROM (VALUES ('<a/>'::xml)) t(x) ORDER BY x",
            "could not identify an ordering operator for type xml",
        ),
        (
            "SELECT '<a/>'::xml UNION SELECT '<b/>'::xml",
            "could not identify an equality operator for type xml",
        ),
        (
            "SELECT greatest('<a/>'::xml, '<b/>'::xml)",
            "could not identify a comparison function for type xml",
        ),
        (
            "SELECT least('<a/>'::xml, '<b/>'::xml)",
            "could not identify a comparison function for type xml",
        ),
    ];
    for (sql, message) in cases {
        let (_, got, _) = err(&mut s, sql).await;
        assert!(got == message, "{sql}: got {got}");
    }

    // `min`/`max` are missing *functions*, because there is no btree opclass to
    // declare them over.
    for aggregate in ["min", "max"] {
        let sql = format!("SELECT {aggregate}(x) FROM (VALUES ('<a/>'::xml)) t(x)");
        assert!(err(&mut s, &sql).await.0 == "42883", "{sql}");
    }
    // `array_agg` and `count` need no comparison and still work.
    assert!(
        scalar(
            &mut s,
            "array_agg(x)::text FROM (VALUES ('<a/>'::xml)) t(x)"
        )
        .await
            == Some("{<a/>}".into())
    );

    // No opclass means no index and no unique constraint.
    run(&mut s, "CREATE TABLE xi (x xml)").await;
    let (code, message, _) = err(&mut s, "CREATE INDEX ON xi(x)").await;
    assert!(code == "42704");
    assert!(message == "data type xml has no default operator class for access method \"btree\"");

    // COALESCE picks a value without comparing, so it is unaffected.
    assert!(scalar(&mut s, "coalesce(NULL::xml, '<a/>'::xml)").await == Some("<a/>".into()));
}

// ---------------------------------------------------------------------------
// Storage and DDL
// ---------------------------------------------------------------------------

#[tokio::test]
async fn xml_columns_store_and_return_their_text_unchanged() {
    let (_engine, mut s) = session();

    run(
        &mut s,
        "CREATE TABLE xmlt (id int, data xml DEFAULT '<d/>')",
    )
    .await;
    run(&mut s, "INSERT INTO xmlt VALUES (1, '<value>one</value>')").await;
    run(&mut s, "INSERT INTO xmlt VALUES (2, NULL)").await;
    run(&mut s, "INSERT INTO xmlt(id) VALUES (3)").await;
    run(
        &mut s,
        "INSERT INTO xmlt VALUES (4, '  spaced  <a  b = ''1'' />  ')",
    )
    .await;
    // A malformed value never reaches storage.
    assert!(err(&mut s, "INSERT INTO xmlt VALUES (5, '<wrong')").await.0 == "2200N");

    assert!(
        rows(&mut s, "SELECT id, data FROM xmlt ORDER BY id").await
            == vec![
                vec![Some("1".into()), Some("<value>one</value>".into())],
                vec![Some("2".into()), None],
                vec![Some("3".into()), Some("<d/>".into())],
                vec![Some("4".into()), Some("  spaced  <a  b = '1' />  ".into())],
            ]
    );
    // The DEFAULT survives the catalog round trip rather than being refused at
    // DDL time or silently lost.
    assert!(
        scalar(
            &mut s,
            "column_default FROM information_schema.columns \
             WHERE table_name = 'xmlt' AND column_name = 'data'"
        )
        .await
            == Some("'<d/>'::xml".into())
    );
    assert!(
        scalar(
            &mut s,
            "data_type FROM information_schema.columns \
             WHERE table_name = 'xmlt' AND column_name = 'data'"
        )
        .await
            == Some("xml".into())
    );

    run(&mut s, "UPDATE xmlt SET data = '<updated/>' WHERE id = 1").await;
    assert!(scalar(&mut s, "data FROM xmlt WHERE id = 1").await == Some("<updated/>".into()));

    // An array column, and a domain over the type.
    run(&mut s, "CREATE TABLE xmlarr (a xml[])").await;
    run(&mut s, "INSERT INTO xmlarr VALUES (ARRAY['<a/>'::xml])").await;
    assert!(scalar(&mut s, "a::text FROM xmlarr").await == Some("{<a/>}".into()));
    run(&mut s, "CREATE DOMAIN xmldom AS xml").await;
    assert!(scalar(&mut s, "pg_typeof('<a/>'::xmldom)").await == Some("xmldom".into()));
}

/// `pg_cast` gives `xml` six entries and no more: the string family in both
/// directions, and nothing else.
#[tokio::test]
async fn xml_casts_only_to_and_from_the_string_family() {
    let (_engine, mut s) = session();

    assert!(scalar(&mut s, "'<a/>'::xml::text").await == Some("<a/>".into()));
    assert!(scalar(&mut s, "'<a/>'::xml::varchar").await == Some("<a/>".into()));
    assert!(scalar(&mut s, "'<a/>'::text::xml").await == Some("<a/>".into()));
    assert!(scalar(&mut s, "'<a/>'::varchar::xml").await == Some("<a/>".into()));
    // A malformed value is rejected by the cast, not stored.
    assert!(err(&mut s, "SELECT '<wrong'::text::xml").await.0 == "2200N");

    for target in ["int", "boolean", "float8", "jsonb", "json"] {
        let sql = format!("SELECT '<a/>'::xml::{target}");
        assert!(err(&mut s, &sql).await.0 == "42846", "{sql}");
    }
    for source in ["1::int", "true", "1.5::float8", "'{}'::jsonb"] {
        let sql = format!("SELECT ({source})::xml");
        assert!(err(&mut s, &sql).await.0 == "42846", "{sql}");
    }
}

/// The deliberately-unimplemented XML surface keeps reporting a missing
/// function rather than half-answering.
#[tokio::test]
async fn the_out_of_scope_xml_functions_still_report_that_they_are_missing() {
    let (_engine, mut s) = session();

    for sql in [
        "SELECT xpath('/a', '<a/>'::xml)",
        "SELECT xpath_exists('/a', '<a/>'::xml)",
        "SELECT xml_is_well_formed('<a/>')",
        "SELECT xmlagg(x) FROM (VALUES ('<a/>'::xml)) t(x)",
        "SELECT query_to_xml('SELECT 1', false, false, '')",
    ] {
        let (code, _, _) = err(&mut s, sql).await;
        assert!(code.starts_with("42") || code == "0A000", "{sql}: {code}");
    }
}
