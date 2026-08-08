use assert2::{assert, check};
use crabka_trace_context::{SqlCommenterTrace, TraceCarrier, extract_sqlcommenter};

const TRACEPARENT: &str = "00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01";
/// A second well-formed traceparent. Some cases must show *which* tag the
/// scanner read, not only that it read one.
const OTHER: &str = "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01";

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
    // Operators and parameters the scanner must step over one byte at a time.
    // Each is a single character that *begins* a construct the scanner also
    // handles, so a guard that stops discriminating swallows the rest of the
    // statement and the real comment behind it is never reached.
    let subtraction = format!("SELECT a - b FROM t /*traceparent='{TRACEPARENT}'*/");
    let division = format!("SELECT a / b FROM t /*traceparent='{TRACEPARENT}'*/");
    let positional_param = format!("SELECT * FROM t WHERE id = $1 /*traceparent='{TRACEPARENT}'*/");
    // The comment sits *after* the dollar-quoted body, so the skip has to land
    // exactly past the closing delimiter — too short re-reads the body, too far
    // steps over the comment.
    let after_dollar_body = format!("SELECT $body$hi$body$ /*traceparent='{TRACEPARENT}'*/");
    // The tag is only reachable if nesting is honoured: without it the first
    // `*/` closes the comment and `traceparent=` is left sitting in bare SQL.
    let after_nested_close =
        format!("SELECT 1 /* outer /* inner */ traceparent='{TRACEPARENT}' */");
    // `key = value` with padding, and a key carrying no value at all — the two
    // ends of the space-skipping that sits between the key and its `=`.
    let padded_equals = format!("SELECT 1 /*traceparent = '{TRACEPARENT}'*/");
    let key_without_value = "SELECT 1 /*traceparent*/";
    // An unquoted value has to stop at the delimiter: run past it and the
    // trailing `,tracestate=…` is swallowed into the traceparent and fails to
    // parse. The existing unquoted case ends at the comment, so it never tests
    // termination.
    let unquoted_then_tracestate =
        format!("SELECT 1 /*traceparent={TRACEPARENT},tracestate=congo=t61*/");
    // An empty value is absent, not `Some("")`.
    let empty_tracestate = format!("SELECT 1 /*traceparent='{TRACEPARENT}',tracestate=*/");
    // A line comment closed by end-of-input rather than a newline, and one at
    // offset 0 with nothing before it to step back over.
    let line_comment_at_eof = format!("SELECT 1 --traceparent='{TRACEPARENT}'");
    let line_comment_at_start = format!("--traceparent='{TRACEPARENT}'\nSELECT 1");
    // A `$tag` that runs to end-of-input, reached only because the comment
    // before it carried a malformed tag and the scan continued.
    let dollar_at_eof = "SELECT 1 /*traceparent='00-nothex-b7ad6b7169203331-01'*/ $abc".to_owned();
    // A string literal whose *contents* look like a tag, behind a subtraction.
    // If `-` is mistaken for a line-comment opener the rest of the line is read
    // as comment text and the literal's tag wins — so this pins which one was
    // read, which a single-traceparent statement cannot.
    let literal_tag_behind_subtraction =
        format!("SELECT a - b, 'traceparent={OTHER}' FROM t /*traceparent='{TRACEPARENT}'*/");
    // A literal whose *last* character is an escaped quote (`'a'''` is the SQL
    // value `a'`). Mid-literal doubled quotes cannot catch a mis-read of the
    // escape: closing early at the first quote of the pair and reopening at the
    // second covers exactly the same span. Only a pair at the very end leaves
    // the reopened literal unterminated, which abandons the scan.
    let literal_ending_in_escaped_quote = format!("SELECT 'a''' /*traceparent='{TRACEPARENT}'*/");

    let cases: [(&str, &str, Option<SqlCommenterTrace<'_>>); 30] = [
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
        (
            "subtraction before the comment",
            &subtraction,
            Some(found(TRACEPARENT, None)),
        ),
        (
            "division before the comment",
            &division,
            Some(found(TRACEPARENT, None)),
        ),
        (
            "positional parameter before the comment",
            &positional_param,
            Some(found(TRACEPARENT, None)),
        ),
        (
            "after a dollar-quoted body",
            &after_dollar_body,
            Some(found(TRACEPARENT, None)),
        ),
        (
            "after a nested comment closes",
            &after_nested_close,
            Some(found(TRACEPARENT, None)),
        ),
        (
            "spaces around the equals",
            &padded_equals,
            Some(found(TRACEPARENT, None)),
        ),
        (
            "unquoted value stops at the delimiter",
            &unquoted_then_tracestate,
            Some(found(TRACEPARENT, Some("congo=t61"))),
        ),
        (
            "line comment closed by end of input",
            &line_comment_at_eof,
            Some(found(TRACEPARENT, None)),
        ),
        (
            "line comment at offset zero",
            &line_comment_at_start,
            Some(found(TRACEPARENT, None)),
        ),
        (
            "literal ending in an escaped quote",
            &literal_ending_in_escaped_quote,
            Some(found(TRACEPARENT, None)),
        ),
        (
            "a literal that looks like a tag, behind a subtraction",
            &literal_tag_behind_subtraction,
            Some(found(TRACEPARENT, None)),
        ),
        (
            "empty tracestate is absent, not empty",
            &empty_tracestate,
            Some(found(TRACEPARENT, None)),
        ),
        // The traps: none of these is a comment, or none is a traceparent.
        ("key with no value", key_without_value, None),
        ("dollar tag running to end of input", &dollar_at_eof, None),
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
