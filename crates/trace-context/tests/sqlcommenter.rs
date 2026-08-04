use assert2::{assert, check};
use crabka_trace_context::{SqlCommenterTrace, TraceCarrier, extract_sqlcommenter};

const TRACEPARENT: &str = "00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01";

fn found<'a>(traceparent: &'a str, tracestate: Option<&'a str>) -> SqlCommenterTrace<'a> {
    SqlCommenterTrace {
        traceparent,
        tracestate,
    }
}

#[test]
fn extract_sqlcommenter_reads_only_genuine_comment_regions() {
    let trailing = format!("SELECT * FROM orders /*traceparent='{TRACEPARENT}'*/");
    let leading = format!("/*traceparent='{TRACEPARENT}'*/ SELECT 1");
    let with_state =
        format!("SELECT 1 /*traceparent='{TRACEPARENT}',tracestate='congo=t61rcWkgMzE'*/");
    let sqlcommenter_neighbours =
        format!("SELECT 1 /*db_driver='psycopg2',route='%2Forders',traceparent='{TRACEPARENT}'*/");
    let nested = format!("SELECT 1 /* outer /*traceparent='{TRACEPARENT}'*/ still comment */");
    let line_comment = format!("SELECT 1 --traceparent='{TRACEPARENT}'\n");
    let unquoted = format!("SELECT 1 /*traceparent={TRACEPARENT}*/");
    let string_literal = format!("SELECT '/*traceparent={TRACEPARENT}*/'");
    let dollar_quoted = format!("SELECT $body$/*traceparent='{TRACEPARENT}'*/$body$");
    let quoted_identifier = format!(r#"SELECT 1 AS "/*traceparent='{TRACEPARENT}'*/""#);
    let after_a_literal =
        format!("SELECT 'it''s fine' /*traceparent='{TRACEPARENT}'*/ FROM orders");
    let malformed = "SELECT 1 /*traceparent='00-nothex-b7ad6b7169203331-01'*/".to_owned();
    let zero_trace_id = format!(
        "SELECT 1 /*traceparent='00-{}-b7ad6b7169203331-01'*/",
        "0".repeat(32)
    );
    let second_comment_wins = format!(
        "SELECT 1 /*traceparent='00-nothex-b7ad6b7169203331-01'*/ /*traceparent='{TRACEPARENT}'*/"
    );
    let prefixed_key = format!("SELECT 1 /*db_traceparent='{TRACEPARENT}'*/");

    let cases: [(&str, &str, Option<SqlCommenterTrace<'_>>); 16] = [
        (
            "trailing block comment",
            &trailing,
            Some(found(TRACEPARENT, None)),
        ),
        (
            "leading block comment",
            &leading,
            Some(found(TRACEPARENT, None)),
        ),
        (
            "traceparent and tracestate",
            &with_state,
            Some(found(TRACEPARENT, Some("congo=t61rcWkgMzE"))),
        ),
        (
            "alongside other sqlcommenter keys",
            &sqlcommenter_neighbours,
            Some(found(TRACEPARENT, None)),
        ),
        (
            "nested block comment",
            &nested,
            Some(found(TRACEPARENT, None)),
        ),
        (
            "line comment",
            &line_comment,
            Some(found(TRACEPARENT, None)),
        ),
        ("unquoted value", &unquoted, Some(found(TRACEPARENT, None))),
        (
            "after a string literal with a doubled quote",
            &after_a_literal,
            Some(found(TRACEPARENT, None)),
        ),
        (
            "a later comment is used when the first is malformed",
            &second_comment_wins,
            Some(found(TRACEPARENT, None)),
        ),
        // The traps: none of these is a comment, or none is a traceparent.
        ("inside a string literal", &string_literal, None),
        ("inside a dollar-quoted body", &dollar_quoted, None),
        ("inside a quoted identifier", &quoted_identifier, None),
        ("no comment at all", "SELECT 1 FROM orders", None),
        ("malformed traceparent", &malformed, None),
        ("all-zero trace id", &zero_trace_id, None),
        ("a different key ending in traceparent", &prefixed_key, None),
    ];

    for (name, sql, expected) in cases {
        check!(extract_sqlcommenter(sql) == expected, "{name}");
    }
}

#[test]
fn extract_sqlcommenter_survives_unterminated_input() {
    // A truncated statement must not panic or spin; it simply yields nothing.
    let cases = [
        format!("SELECT 1 /*traceparent='{TRACEPARENT}'"),
        format!("SELECT 'unterminated /*traceparent='{TRACEPARENT}'*/"),
        format!("SELECT $body$ /*traceparent='{TRACEPARENT}'*/"),
        format!("SELECT 1 /*traceparent='{}", &TRACEPARENT[..20]),
    ];

    for sql in cases {
        check!(extract_sqlcommenter(&sql).is_none(), "{sql}");
    }
}

#[test]
fn extract_sqlcommenter_feeds_a_validated_carrier() {
    let sql = format!("SELECT 1 /*traceparent='{TRACEPARENT}',tracestate='congo=t61'*/");
    assert!(let Some(trace) = extract_sqlcommenter(&sql));
    assert!(let Ok(carrier) = TraceCarrier::from_w3c(trace.traceparent, trace.tracestate));

    check!(carrier.traceparent.as_deref() == Some(TRACEPARENT));
    check!(carrier.tracestate.as_deref() == Some("congo=t61"));
}

#[test]
fn a_sqlcommenter_tag_changes_no_parsed_statement() {
    // The ingress path never rewrites the SQL it hands to the parser, so the
    // claim that the tag is invisible to the AST has to hold behaviourally.
    let statements = [
        "SELECT id, total FROM orders WHERE id = $1",
        "INSERT INTO orders (id, total) VALUES (1, 2)",
        "UPDATE orders SET total = 3 WHERE id = 1",
        "SELECT '/*not a comment*/' AS literal",
    ];

    for sql in statements {
        let tagged = format!("{sql} /*traceparent='{TRACEPARENT}'*/");
        assert!(let Ok(plain_ast) = crabka_pgparser::parse(sql), "{sql}");
        assert!(let Ok(tagged_ast) = crabka_pgparser::parse(&tagged), "{sql}");
        check!(plain_ast == tagged_ast, "{sql}");
    }
}
