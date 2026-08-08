//! The geometric OPERATORS and SUBSCRIPTING, end to end.
//!
//! `PostgreSQL` declares these pair by pair rather than uniformly: `box <-> lseg`
//! exists and `box <-> circle` does not; `point` has `<>` but no `=`; `box` has
//! `=` but no `<>`; `polygon` has neither. The matrix has about as many holes as
//! entries, and an engine that answers a hole is as wrong as one that answers it
//! with the wrong number — so every negative below is as load-bearing as every
//! positive.
//!
//! Every expected value was read from a live `PostgreSQL` 18.4 server, including
//! the ones that look like typos: `lseg '[(0,0),(1,1)]' = lseg '[(5,5),(6,6)]'`
//! really is false while `box '(0,0),(1,1)' = box '(5,5),(6,6)'` really is true,
//! because `lseg_eq` compares endpoints and `box_eq` compares areas.

use assert2::assert;
use crabka_pgexec::{SqlEngine, SqlSession};
use crabka_pgwire::engine::{Cell, Engine, QueryResult, Session};

// ---------------------------------------------------------------------------
// Harness
// ---------------------------------------------------------------------------

fn session() -> (SqlEngine, SqlSession) {
    let engine = SqlEngine::new();
    let s = engine.connect();
    (engine, s)
}

fn cell_text(cell: Option<&Cell>) -> Option<String> {
    cell.map(|c| String::from_utf8(c.text.to_vec()).expect("utf8"))
}

async fn rows(s: &mut SqlSession, sql: &str) -> Vec<Vec<Option<String>>> {
    let results = s
        .simple_query(sql)
        .await
        .unwrap_or_else(|e| panic!("`{sql}` should succeed: {e:?}"));
    match &results[0] {
        QueryResult::Rows { rows, .. } => rows
            .iter()
            .map(|row| row.iter().map(|c| cell_text(c.as_ref())).collect())
            .collect(),
        other => panic!("`{sql}` should return rows, got {other:?}"),
    }
}

/// One expression's rendered value (`None` is SQL NULL).
async fn scalar(s: &mut SqlSession, expr: &str) -> Option<String> {
    let sql = format!("SELECT {expr}");
    let rows = rows(s, &sql).await;
    assert!(rows.len() == 1 && rows[0].len() == 1, "{sql}");
    rows[0][0].clone()
}

/// One expression's value and the type OID its `RowDescription` promises.
async fn typed_scalar(s: &mut SqlSession, expr: &str) -> (Option<String>, u32) {
    let sql = format!("SELECT {expr}");
    let results = s
        .simple_query(&sql)
        .await
        .unwrap_or_else(|e| panic!("`{sql}` should succeed: {e:?}"));
    match &results[0] {
        QueryResult::Rows { rows, fields, .. } => {
            (cell_text(rows[0][0].as_ref()), fields[0].type_oid)
        }
        other => panic!("`{sql}` should return rows, got {other:?}"),
    }
}

/// The SQLSTATE and message a `SELECT <expr>` that must fail reports.
async fn expr_err(s: &mut SqlSession, expr: &str) -> (String, String) {
    let sql = format!("SELECT {expr}");
    let e = s
        .simple_query(&sql)
        .await
        .expect_err(&format!("`{sql}` should fail"));
    (e.code, e.message)
}

/// `pg_type.oid` of the types these operators produce.
const BOOL_OID: u32 = 16;
const INT4_OID: u32 = 23;
const FLOAT8_OID: u32 = 701;
const POINT_OID: u32 = 600;
const BOX_OID: u32 = 603;
const CIRCLE_OID: u32 = 718;
const PATH_OID: u32 = 602;

/// Every operand pair `PostgreSQL` does NOT declare, checked as one table so a
/// dispatch that silently widens is caught by whichever hole it fell into.
async fn assert_all_undefined(s: &mut SqlSession, cases: &[(&str, &str)]) {
    for (expr, message) in cases {
        let (code, actual) = expr_err(s, expr).await;
        assert!(
            (code.as_str(), actual.as_str()) == ("42883", *message),
            "{expr}: got {code} {actual}"
        );
    }
}

// ---------------------------------------------------------------------------
// `<->` — the operator the bounding-box reduction got WRONG
// ---------------------------------------------------------------------------

/// Every declared `<->` pair, with the distance `PostgreSQL` 18.4 answers.
///
/// `point <-> circle` is the case a bounding-box reduction gets numerically
/// wrong rather than merely differently: `PostgreSQL` measures the gap to the
/// CIRCUMFERENCE (4.505678886386311 for `(3.5,-4.25)` and `<(0,0),1>`), while
/// the box around the circle gives 4.100304866714181.
#[tokio::test]
async fn distance_is_measured_per_pair_not_by_bounding_box() {
    let (_engine, mut s) = session();

    let cases: [(&str, &str); 21] = [
        (
            "point '(3.5,-4.25)' <-> circle '<(0,0),1>'",
            "4.505678886386311",
        ),
        ("point '(1,2)' <-> point '(4,6)'", "5"),
        ("point '(0,0)' <-> box '(2,2),(3,3)'", "2.8284271247461903"),
        ("box '(2,2),(3,3)' <-> point '(0,0)'", "2.8284271247461903"),
        ("point '(0,0)' <-> lseg '[(1,1),(1,-1)]'", "1"),
        ("point '(0,0)' <-> line '{1,1,-2}'", "1.4142135623730951"),
        (
            "point '(0,0)' <-> path '[(1,1),(2,2)]'",
            "1.4142135623730951",
        ),
        (
            "point '(0,0)' <-> polygon '((1,1),(2,1),(2,2))'",
            "1.4142135623730951",
        ),
        (
            "box '(0,0),(1,1)' <-> box '(3,3),(4,4)'",
            "4.242640687119286",
        ),
        (
            "box '(0,0),(1,1)' <-> lseg '[(3,3),(4,4)]'",
            "2.8284271247461903",
        ),
        (
            "lseg '[(3,3),(4,4)]' <-> box '(0,0),(1,1)'",
            "2.8284271247461903",
        ),
        ("circle '<(0,0),1>' <-> circle '<(5,0),1>'", "3"),
        (
            "circle '<(0,0),1>' <-> polygon '((3,3),(4,3),(4,4))'",
            "3.2426406871192857",
        ),
        (
            "polygon '((3,3),(4,3),(4,4))' <-> circle '<(0,0),1>'",
            "3.2426406871192857",
        ),
        ("line '{1,0,0}' <-> line '{1,0,-5}'", "5"),
        ("line '{1,0,0}' <-> lseg '[(3,3),(4,4)]'", "3"),
        ("lseg '[(3,3),(4,4)]' <-> line '{1,0,0}'", "3"),
        ("lseg '[(0,0),(1,0)]' <-> lseg '[(0,3),(1,3)]'", "3"),
        ("path '[(0,0),(1,0)]' <-> path '[(0,3),(1,3)]'", "3"),
        ("path '[(0,0),(1,0)]' <-> point '(0,3)'", "3"),
        (
            "polygon '((0,0),(1,0),(1,1))' <-> polygon '((5,5),(6,5),(6,6))'",
            "5.656854249492381",
        ),
    ];
    for (expr, expected) in cases {
        assert!(
            typed_scalar(&mut s, expr).await == (Some(expected.into()), FLOAT8_OID),
            "{expr}"
        );
    }
}

/// The pairs `<->` is NOT declared over. A bounding-box reduction answers every
/// one of these, which is exactly what makes them worth testing.
#[tokio::test]
async fn an_undeclared_distance_pair_is_undefined_not_approximated() {
    let (_engine, mut s) = session();

    assert_all_undefined(
        &mut s,
        &[
            (
                "box '(0,0),(1,1)' <-> circle '<(5,5),1>'",
                "operator does not exist: box <-> circle",
            ),
            (
                "circle '<(0,0),1>' <-> box '(5,5),(6,6)'",
                "operator does not exist: circle <-> box",
            ),
            (
                "box '(0,0),(1,1)' <-> line '{1,0,0}'",
                "operator does not exist: box <-> line",
            ),
            (
                "circle '<(0,0),1>' <-> lseg '[(3,3),(4,4)]'",
                "operator does not exist: circle <-> lseg",
            ),
            (
                "lseg '[(3,3),(4,4)]' <-> circle '<(0,0),1>'",
                "operator does not exist: lseg <-> circle",
            ),
            (
                "path '[(0,0),(1,1)]' <-> box '(0,0),(1,1)'",
                "operator does not exist: path <-> box",
            ),
            (
                "polygon '((0,0),(1,0),(1,1))' <-> box '(0,0),(1,1)'",
                "operator does not exist: polygon <-> box",
            ),
        ],
    )
    .await;
}

// ---------------------------------------------------------------------------
// The positional family
// ---------------------------------------------------------------------------

/// `<<`, `>>`, `<<|`, `|>>`, `&<`, `&>`, `&<|`, `|&>`, `&&`, `~=`, `<^` and
/// `>^` over each family that declares them.
#[tokio::test]
async fn the_positional_operators_answer_their_declared_families() {
    let (_engine, mut s) = session();

    let cases: [(&str, bool); 40] = [
        // `<<` `>>` `<<|` `|>>` — box, circle, point and polygon.
        ("box '(0,0),(1,1)' << box '(3,3),(4,4)'", true),
        ("box '(0,0),(3,3)' << box '(1,1),(4,4)'", false),
        ("circle '<(0,0),1>' << circle '<(5,0),1>'", true),
        ("point '(0,0)' << point '(1,1)'", true),
        ("point '(1,0)' << point '(1,1)'", false),
        (
            "polygon '((0,0),(1,0),(1,1))' << polygon '((3,3),(4,3),(4,4))'",
            true,
        ),
        ("box '(3,3),(4,4)' >> box '(0,0),(1,1)'", true),
        ("circle '<(5,0),1>' >> circle '<(0,0),1>'", true),
        ("point '(1,1)' >> point '(0,0)'", true),
        (
            "polygon '((3,3),(4,3),(4,4))' >> polygon '((0,0),(1,0),(1,1))'",
            true,
        ),
        ("box '(0,0),(1,1)' <<| box '(3,3),(4,4)'", true),
        ("circle '<(0,0),1>' <<| circle '<(0,5),1>'", true),
        ("point '(0,0)' <<| point '(1,1)'", true),
        ("point '(0,1)' <<| point '(1,1)'", false),
        (
            "polygon '((0,0),(1,0),(1,1))' <<| polygon '((3,3),(4,3),(4,4))'",
            true,
        ),
        ("box '(3,3),(4,4)' |>> box '(0,0),(1,1)'", true),
        ("circle '<(0,5),1>' |>> circle '<(0,0),1>'", true),
        ("point '(1,1)' |>> point '(0,0)'", true),
        (
            "polygon '((3,3),(4,3),(4,4))' |>> polygon '((0,0),(1,0),(1,1))'",
            true,
        ),
        // `&&` `&<` `&>` `&<|` `|&>` — box, circle and polygon only.
        ("box '(0,0),(2,2)' && box '(1,1),(3,3)'", true),
        ("box '(0,0),(1,1)' && box '(5,5),(6,6)'", false),
        ("circle '<(0,0),1>' && circle '<(1,0),1>'", true),
        ("circle '<(0,0),1>' && circle '<(5,0),1>'", false),
        (
            "polygon '((0,0),(2,0),(2,2))' && polygon '((1,1),(3,1),(3,3))'",
            true,
        ),
        (
            "polygon '((0,0),(1,0),(1,1))' && polygon '((5,5),(6,5),(6,6))'",
            false,
        ),
        ("box '(0,0),(1,1)' &< box '(0,0),(2,2)'", true),
        ("box '(0,0),(3,3)' &< box '(0,0),(2,2)'", false),
        ("circle '<(0,0),1>' &< circle '<(0,0),2>'", true),
        (
            "polygon '((0,0),(1,0),(1,1))' &< polygon '((0,0),(2,0),(2,2))'",
            true,
        ),
        ("box '(0,0),(1,1)' &> box '(0,0),(2,2)'", true),
        ("box '(-1,0),(1,1)' &> box '(0,0),(2,2)'", false),
        ("circle '<(0,0),1>' &> circle '<(0,0),2>'", true),
        ("box '(0,0),(1,1)' &<| box '(0,0),(2,2)'", true),
        ("box '(0,0),(3,3)' &<| box '(0,0),(2,2)'", false),
        ("box '(0,0),(1,1)' |&> box '(0,0),(2,2)'", true),
        ("box '(-1,-1),(1,1)' |&> box '(0,0),(2,2)'", false),
        // `~=` is structural for every family it takes.
        ("box '(0,0),(1,1)' ~= box '(0,0),(1,1)'", true),
        ("box '(0,0),(1,1)' ~= box '(0,0),(2,2)'", false),
        ("point '(1,2)' ~= point '(1,2)'", true),
        (
            "polygon '((0,0),(1,0),(1,1))' ~= polygon '((1,0),(1,1),(0,0))'",
            true,
        ),
    ];
    for (expr, expected) in cases {
        let rendered = if expected { "t" } else { "f" };
        assert!(
            typed_scalar(&mut s, expr).await == (Some(rendered.into()), BOOL_OID),
            "{expr}"
        );
    }
}

/// `<^` and `>^` share a spelling across two different relations: `box_below_eq`
/// is "below OR level with", `point_below` is strictly below.
#[tokio::test]
async fn below_eq_and_above_eq_are_two_relations_under_one_spelling() {
    let (_engine, mut s) = session();

    let cases: [(&str, bool); 10] = [
        ("box '(0,0),(1,1)' <^ box '(0,3),(1,4)'", true),
        ("box '(0,3),(1,4)' <^ box '(0,0),(1,1)'", false),
        // A box that only reaches the other's low edge still counts.
        ("box '(0,0),(1,1)' <^ box '(0,1),(1,4)'", true),
        ("box '(0,3),(1,4)' >^ box '(0,0),(1,1)'", true),
        ("box '(0,0),(1,1)' >^ box '(0,3),(1,4)'", false),
        ("box '(0,1),(1,4)' >^ box '(0,0),(1,1)'", true),
        // Points, by contrast, are strict on both sides.
        ("point '(0,0)' <^ point '(1,1)'", true),
        ("point '(0,1)' <^ point '(1,1)'", false),
        ("point '(1,1)' >^ point '(0,0)'", true),
        ("point '(1,1)' >^ point '(0,1)'", false),
    ];
    for (expr, expected) in cases {
        let rendered = if expected { "t" } else { "f" };
        assert!(
            typed_scalar(&mut s, expr).await == (Some(rendered.into()), BOOL_OID),
            "{expr}"
        );
    }
}

/// The positional holes. Reducing both operands to a bounding box answers every
/// one of these; `PostgreSQL` refuses them.
#[tokio::test]
async fn the_positional_operators_refuse_the_families_they_do_not_declare() {
    let (_engine, mut s) = session();

    assert_all_undefined(
        &mut s,
        &[
            // `point` has none of the five non-strict positional tests.
            (
                "point '(0,0)' && point '(1,1)'",
                "operator does not exist: point && point",
            ),
            (
                "point '(0,0)' &< point '(1,1)'",
                "operator does not exist: point &< point",
            ),
            (
                "point '(0,0)' &> point '(1,1)'",
                "operator does not exist: point &> point",
            ),
            (
                "point '(0,0)' &<| point '(1,1)'",
                "operator does not exist: point &<| point",
            ),
            (
                "point '(0,0)' |&> point '(1,1)'",
                "operator does not exist: point |&> point",
            ),
            // `lseg` has NO positional operator at all.
            (
                "lseg '[(0,0),(1,1)]' && lseg '[(0,0),(1,1)]'",
                "operator does not exist: lseg && lseg",
            ),
            (
                "lseg '[(0,0),(1,1)]' << lseg '[(3,3),(4,4)]'",
                "operator does not exist: lseg << lseg",
            ),
            (
                "lseg '[(0,0),(1,1)]' ~= lseg '[(0,0),(1,1)]'",
                "operator does not exist: lseg ~= lseg",
            ),
            // Nor does any positional operator cross families.
            (
                "box '(0,0),(1,1)' && circle '<(0,0),1>'",
                "operator does not exist: box && circle",
            ),
            (
                "box '(0,0),(1,1)' << circle '<(5,5),1>'",
                "operator does not exist: box << circle",
            ),
            (
                "point '(0,0)' <^ box '(0,0),(1,1)'",
                "operator does not exist: point <^ box",
            ),
            (
                "circle '<(0,0),1>' <^ circle '<(0,0),1>'",
                "operator does not exist: circle <^ circle",
            ),
            (
                "polygon '((0,0),(1,0),(1,1))' <^ polygon '((0,0),(1,0),(1,1))'",
                "operator does not exist: polygon <^ polygon",
            ),
        ],
    )
    .await;
}

// ---------------------------------------------------------------------------
// Containment
// ---------------------------------------------------------------------------

/// `@>` and `<@` are NOT mirror images of one another: four `<@` pairs have no
/// `@>` spelling at all.
#[tokio::test]
async fn containment_is_declared_one_direction_at_a_time() {
    let (_engine, mut s) = session();

    let cases: [(&str, bool); 20] = [
        ("box '(0,0),(3,3)' <@ box '(0,0),(4,4)'", true),
        ("box '(0,0),(5,5)' <@ box '(0,0),(4,4)'", false),
        ("circle '<(0,0),1>' <@ circle '<(0,0),2>'", true),
        ("lseg '[(1,1),(2,2)]' <@ box '(0,0),(4,4)'", true),
        ("lseg '[(1,1),(9,9)]' <@ box '(0,0),(4,4)'", false),
        ("lseg '[(1,1),(2,2)]' <@ line '{1,-1,0}'", true),
        ("lseg '[(1,1),(2,3)]' <@ line '{1,-1,0}'", false),
        ("point '(1,1)' <@ box '(0,0),(4,4)'", true),
        ("point '(9,9)' <@ box '(0,0),(4,4)'", false),
        ("point '(1,1)' <@ circle '<(0,0),2>'", true),
        ("point '(1,1)' <@ line '{1,-1,0}'", true),
        ("point '(1,2)' <@ line '{1,-1,0}'", false),
        ("point '(1,1)' <@ lseg '[(0,0),(2,2)]'", true),
        ("point '(1,1)' <@ path '[(0,0),(2,2)]'", true),
        ("point '(1,1)' <@ polygon '((0,0),(4,0),(4,4),(0,4))'", true),
        (
            "polygon '((1,1),(2,1),(2,2))' <@ polygon '((0,0),(4,0),(4,4),(0,4))'",
            true,
        ),
        ("box '(0,0),(4,4)' @> point '(1,1)'", true),
        ("box '(0,0),(4,4)' @> point '(9,9)'", false),
        ("circle '<(0,0),2>' @> point '(1,1)'", true),
        ("path '[(0,0),(2,2)]' @> point '(1,1)'", true),
    ];
    for (expr, expected) in cases {
        let rendered = if expected { "t" } else { "f" };
        assert!(
            scalar(&mut s, expr).await == Some(rendered.into()),
            "{expr}"
        );
    }

    assert_all_undefined(
        &mut s,
        &[
            // The four `<@` pairs with no commutator.
            (
                "box '(0,0),(4,4)' @> lseg '[(1,1),(2,2)]'",
                "operator does not exist: box @> lseg",
            ),
            (
                "line '{1,-1,0}' @> lseg '[(1,1),(2,2)]'",
                "operator does not exist: line @> lseg",
            ),
            (
                "line '{1,-1,0}' @> point '(1,1)'",
                "operator does not exist: line @> point",
            ),
            (
                "lseg '[(0,0),(2,2)]' @> point '(1,1)'",
                "operator does not exist: lseg @> point",
            ),
            // And the pairs neither direction declares.
            (
                "point '(0,0)' @> point '(1,1)'",
                "operator does not exist: point @> point",
            ),
            (
                "point '(0,0)' <@ point '(1,1)'",
                "operator does not exist: point <@ point",
            ),
            (
                "box '(0,0),(1,1)' <@ point '(0,0)'",
                "operator does not exist: box <@ point",
            ),
            (
                "point '(0,0)' @> box '(0,0),(1,1)'",
                "operator does not exist: point @> box",
            ),
            (
                "box '(0,0),(1,1)' @> circle '<(0,0),1>'",
                "operator does not exist: box @> circle",
            ),
            (
                "lseg '[(0,0),(1,1)]' <@ circle '<(0,0),5>'",
                "operator does not exist: lseg <@ circle",
            ),
            (
                "circle '<(0,0),5>' @> lseg '[(0,0),(1,1)]'",
                "operator does not exist: circle @> lseg",
            ),
        ],
    )
    .await;
}

// ---------------------------------------------------------------------------
// The `?` family and `#` / `##`
// ---------------------------------------------------------------------------

/// `?#`, `?-`, `?|`, `?-|` and `?||`.
#[tokio::test]
async fn the_question_mark_operators_answer_their_declared_pairs() {
    let (_engine, mut s) = session();

    let cases: [(&str, bool); 24] = [
        ("box '(0,0),(2,2)' ?# box '(1,1),(3,3)'", true),
        ("box '(0,0),(1,1)' ?# box '(5,5),(6,6)'", false),
        ("line '{1,-1,0}' ?# box '(0,0),(2,2)'", true),
        ("line '{1,-1,-100}' ?# box '(0,0),(2,2)'", false),
        ("line '{1,-1,0}' ?# line '{1,1,-2}'", true),
        ("line '{1,-1,0}' ?# line '{1,-1,-5}'", false),
        ("lseg '[(0,0),(2,2)]' ?# box '(1,1),(3,3)'", true),
        ("lseg '[(0,0),(1,1)]' ?# box '(5,5),(6,6)'", false),
        ("lseg '[(0,0),(2,2)]' ?# line '{1,1,-2}'", true),
        ("lseg '[(0,0),(1,1)]' ?# line '{1,1,-100}'", false),
        ("lseg '[(0,0),(2,2)]' ?# lseg '[(0,2),(2,0)]'", true),
        ("lseg '[(0,0),(1,1)]' ?# lseg '[(5,5),(6,6)]'", false),
        ("path '[(0,0),(2,2)]' ?# path '[(0,2),(2,0)]'", true),
        ("path '[(0,0),(1,1)]' ?# path '[(5,5),(6,6)]'", false),
        ("point '(1,1)' ?- point '(5,1)'", true),
        ("point '(1,1)' ?- point '(5,2)'", false),
        ("point '(1,1)' ?| point '(1,5)'", true),
        ("point '(1,1)' ?| point '(2,5)'", false),
        ("line '{1,-1,0}' ?-| line '{1,1,-2}'", true),
        ("line '{1,-1,0}' ?-| line '{1,-1,-5}'", false),
        ("lseg '[(0,0),(2,2)]' ?-| lseg '[(0,2),(2,0)]'", true),
        ("lseg '[(0,0),(1,1)]' ?-| lseg '[(5,5),(6,6)]'", false),
        ("line '{1,-1,0}' ?|| line '{1,-1,-5}'", true),
        ("lseg '[(0,0),(1,1)]' ?|| lseg '[(5,5),(6,6)]'", true),
    ];
    for (expr, expected) in cases {
        let rendered = if expected { "t" } else { "f" };
        assert!(
            typed_scalar(&mut s, expr).await == (Some(rendered.into()), BOOL_OID),
            "{expr}"
        );
    }

    assert_all_undefined(
        &mut s,
        &[
            (
                "point '(1,1)' ?# point '(1,1)'",
                "operator does not exist: point ?# point",
            ),
            (
                "circle '<(0,0),1>' ?# circle '<(0,0),1>'",
                "operator does not exist: circle ?# circle",
            ),
            (
                "box '(0,0),(1,1)' ?# line '{1,-1,0}'",
                "operator does not exist: box ?# line",
            ),
            (
                "lseg '[(0,0),(1,1)]' ?- lseg '[(0,0),(1,1)]'",
                "operator does not exist: lseg ?- lseg",
            ),
            (
                "point '(1,1)' ?-| point '(1,1)'",
                "operator does not exist: point ?-| point",
            ),
            (
                "path '[(0,0),(1,1)]' ?|| path '[(0,0),(1,1)]'",
                "operator does not exist: path ?|| path",
            ),
        ],
    )
    .await;
}

/// `#` intersects two shapes and `##` finds the closest point on the RIGHT
/// operand. Both are NULL where the construction is undefined.
#[tokio::test]
async fn intersection_and_closest_point_construct_or_yield_null() {
    let (_engine, mut s) = session();

    assert!(
        typed_scalar(&mut s, "box '(0,0),(2,2)' # box '(1,1),(3,3)'").await
            == (Some("(2,2),(1,1)".into()), BOX_OID)
    );
    assert!(
        scalar(&mut s, "box '(0,0),(1,1)' # box '(5,5),(6,6)'")
            .await
            .is_none()
    );
    assert!(
        typed_scalar(&mut s, "line '{1,-1,0}' # line '{1,1,-2}'").await
            == (Some("(1,1)".into()), POINT_OID)
    );
    assert!(
        scalar(&mut s, "line '{1,-1,0}' # line '{1,-1,-5}'")
            .await
            .is_none()
    );
    assert!(
        scalar(&mut s, "lseg '[(0,0),(2,2)]' # lseg '[(0,2),(2,0)]'").await == Some("(1,1)".into())
    );
    assert!(
        scalar(&mut s, "lseg '[(0,0),(1,1)]' # lseg '[(5,5),(6,6)]'")
            .await
            .is_none()
    );

    let closest: [(&str, &str); 5] = [
        ("line '{1,-1,0}' ## lseg '[(0,3),(2,3)]'", "(2,3)"),
        ("lseg '[(0,0),(1,0)]' ## box '(2,2),(3,3)'", "(2,2)"),
        ("lseg '[(0,0),(1,0)]' ## lseg '[(0,3),(2,5)]'", "(0,3)"),
        ("point '(0,0)' ## line '{1,1,-2}'", "(1,1)"),
        ("point '(0,0)' ## lseg '[(1,1),(1,-1)]'", "(1,0)"),
    ];
    for (expr, expected) in closest {
        assert!(
            typed_scalar(&mut s, expr).await == (Some(expected.into()), POINT_OID),
            "{expr}"
        );
    }
    // Parallel segments have no closest point, which is NULL rather than an
    // error.
    assert!(
        scalar(&mut s, "lseg '[(0,0),(1,0)]' ## lseg '[(0,3),(2,3)]'")
            .await
            .is_none()
    );

    assert_all_undefined(
        &mut s,
        &[
            (
                "point '(1,1)' # point '(1,1)'",
                "operator does not exist: point # point",
            ),
            (
                "circle '<(0,0),1>' # circle '<(0,0),1>'",
                "operator does not exist: circle # circle",
            ),
            (
                "box '(0,0),(1,1)' ## box '(0,0),(1,1)'",
                "operator does not exist: box ## box",
            ),
            (
                "box '(0,0),(1,1)' ## lseg '[(0,0),(1,1)]'",
                "operator does not exist: box ## lseg",
            ),
            (
                "point '(1,1)' ## point '(1,1)'",
                "operator does not exist: point ## point",
            ),
            (
                "point '(1,1)' ## polygon '((0,0),(1,0),(1,1))'",
                "operator does not exist: point ## polygon",
            ),
        ],
    )
    .await;
}

// ---------------------------------------------------------------------------
// Arithmetic
// ---------------------------------------------------------------------------

/// `+`, `-`, `*` and `/`. Every one translates a shape by a POINT and keeps the
/// shape's type; `path + path` alone concatenates, and is NULL when either
/// operand is closed.
#[tokio::test]
async fn geometric_arithmetic_translates_by_a_point() {
    let (_engine, mut s) = session();

    let cases: [(&str, &str, u32); 16] = [
        ("box '(0,0),(1,1)' + point '(1,2)'", "(2,3),(1,2)", BOX_OID),
        (
            "circle '<(0,0),1>' + point '(1,2)'",
            "<(1,2),1>",
            CIRCLE_OID,
        ),
        (
            "path '[(0,0),(1,1)]' + path '[(2,2),(3,3)]'",
            "[(0,0),(1,1),(2,2),(3,3)]",
            PATH_OID,
        ),
        (
            "path '[(0,0),(1,1)]' + point '(1,1)'",
            "[(1,1),(2,2)]",
            PATH_OID,
        ),
        ("point '(1,2)' + point '(3,4)'", "(4,6)", POINT_OID),
        (
            "box '(0,0),(1,1)' - point '(1,2)'",
            "(0,-1),(-1,-2)",
            BOX_OID,
        ),
        (
            "circle '<(0,0),1>' - point '(1,2)'",
            "<(-1,-2),1>",
            CIRCLE_OID,
        ),
        (
            "path '[(0,0),(1,1)]' - point '(1,1)'",
            "[(-1,-1),(0,0)]",
            PATH_OID,
        ),
        ("point '(1,2)' - point '(3,4)'", "(-2,-2)", POINT_OID),
        ("box '(0,0),(1,1)' * point '(2,0)'", "(2,2),(0,0)", BOX_OID),
        (
            "circle '<(1,1),1>' * point '(2,0)'",
            "<(2,2),2>",
            CIRCLE_OID,
        ),
        (
            "path '[(0,0),(1,1)]' * point '(2,0)'",
            "[(0,0),(2,2)]",
            PATH_OID,
        ),
        // `point * point` is COMPLEX multiplication, not componentwise.
        ("point '(1,2)' * point '(3,4)'", "(-5,10)", POINT_OID),
        ("box '(0,0),(2,2)' / point '(2,0)'", "(1,1),(0,0)", BOX_OID),
        (
            "path '[(0,0),(2,2)]' / point '(2,0)'",
            "[(0,0),(1,1)]",
            PATH_OID,
        ),
        ("point '(1,2)' / point '(3,4)'", "(0.44,0.08)", POINT_OID),
    ];
    for (expr, expected, oid) in cases {
        assert!(
            typed_scalar(&mut s, expr).await == (Some(expected.into()), oid),
            "{expr}"
        );
    }

    // `path_add` is NULL when EITHER operand is a closed path.
    for expr in [
        "path '((0,0),(1,0),(1,1))' + path '((2,2),(3,3))'",
        "path '[(0,0),(1,1)]' + path '((2,2),(3,3))'",
        "path '((0,0),(1,0),(1,1))' + path '[(2,2),(3,3)]'",
    ] {
        assert!(scalar(&mut s, expr).await.is_none(), "{expr}");
    }
}

/// The arithmetic operators `PostgreSQL` does not declare, and the three numeric
/// domain errors `geometry.rs` reports for the ones it does.
#[tokio::test]
async fn geometric_arithmetic_refuses_undeclared_pairs_and_reports_domain_errors() {
    let (_engine, mut s) = session();

    assert_all_undefined(
        &mut s,
        &[
            (
                "point '(1,2)' + box '(0,0),(1,1)'",
                "operator does not exist: point + box",
            ),
            (
                "point '(1,2)' + path '[(0,0),(1,1)]'",
                "operator does not exist: point + path",
            ),
            (
                "point '(1,2)' * box '(0,0),(1,1)'",
                "operator does not exist: point * box",
            ),
            (
                "polygon '((0,0),(1,0),(1,1))' + point '(1,1)'",
                "operator does not exist: polygon + point",
            ),
            (
                "lseg '[(0,0),(1,1)]' + point '(1,1)'",
                "operator does not exist: lseg + point",
            ),
            (
                "box '(0,0),(1,1)' + box '(0,0),(1,1)'",
                "operator does not exist: box + box",
            ),
            (
                "circle '<(0,0),1>' + circle '<(0,0),1>'",
                "operator does not exist: circle + circle",
            ),
            (
                "point '(1,2)' + 5",
                "operator does not exist: point + integer",
            ),
            (
                "5 - point '(1,2)'",
                "operator does not exist: integer - point",
            ),
            (
                "path '[(0,0),(1,1)]' - path '[(0,0),(1,1)]'",
                "operator does not exist: path - path",
            ),
        ],
    )
    .await;

    let domain: [(&str, &str, &str); 4] = [
        (
            "point '(1e300,1e300)' * point '(1e300,1e300)'",
            "22003",
            "value out of range: overflow",
        ),
        (
            "point '(1e-300,1e-300)' * point '(1e-300,1e-300)'",
            "22003",
            "value out of range: underflow",
        ),
        ("point '(1,2)' / point '(0,0)'", "22012", "division by zero"),
        (
            "point '(1e300,1e300)' / point '(1e-300,1e-300)'",
            "22003",
            "value out of range: underflow",
        ),
    ];
    for (expr, code, message) in domain {
        assert!(
            expr_err(&mut s, expr).await == (code.into(), message.into()),
            "{expr}"
        );
    }
}

// ---------------------------------------------------------------------------
// Comparison
// ---------------------------------------------------------------------------

/// The btree surface each geometric type has, verified against `PostgreSQL` 18.4.
/// The table is deliberately inconsistent — `point` has `<>` and no `=`, `box`
/// has `=` and no `<>` — and every hole is a 42883 rather than the 42804
/// "cannot compare" a type-level comparison would report.
#[tokio::test]
async fn each_geometric_types_comparison_operators_are_exactly_postgres_own() {
    let (_engine, mut s) = session();

    // (type name, one literal, `=`, `<>`, ordering)
    let types: [(&str, &str, bool, bool, bool); 7] = [
        ("point", "point '(1,2)'", false, true, false),
        ("box", "box '(0,0),(1,1)'", true, false, true),
        ("circle", "circle '<(0,0),1>'", true, true, true),
        ("line", "line '{1,-1,0}'", true, false, false),
        ("lseg", "lseg '[(0,0),(1,1)]'", true, true, true),
        ("path", "path '[(0,0),(1,1)]'", true, false, true),
        (
            "polygon",
            "polygon '((0,0),(1,0),(1,1))'",
            false,
            false,
            false,
        ),
    ];
    for (name, literal, has_eq, has_ne, has_order) in types {
        for (op, declared) in [
            ("=", has_eq),
            ("<>", has_ne),
            ("<", has_order),
            ("<=", has_order),
            (">", has_order),
            (">=", has_order),
        ] {
            let expr = format!("{literal} {op} {literal}");
            if declared {
                assert!(
                    typed_scalar(&mut s, &expr).await.1 == BOOL_OID,
                    "{name} {op} should exist"
                );
            } else {
                assert!(
                    expr_err(&mut s, &expr).await
                        == (
                            "42883".into(),
                            format!("operator does not exist: {name} {op} {name}")
                        ),
                    "{name} {op} should not exist"
                );
            }
        }
    }
}

/// What the comparison operators MEAN also varies by type: `box` and `circle`
/// compare AREA, `lseg` compares length for the orderings but ENDPOINTS for
/// equality, `path` compares the number of points, and `line` compares its
/// coefficients UP TO A SCALE FACTOR.
#[tokio::test]
async fn a_geometric_comparison_uses_its_own_types_ordering_key() {
    let (_engine, mut s) = session();

    let cases: [(&str, bool); 19] = [
        // `box_eq` is area, so two boxes in different places are equal.
        ("box '(0,0),(1,1)' = box '(5,5),(6,6)'", true),
        ("box '(0,0),(1,1)' = box '(0,0),(2,2)'", false),
        ("box '(0,0),(1,1)' < box '(0,0),(2,2)'", true),
        ("box '(0,0),(2,2)' <= box '(0,0),(2,2)'", true),
        ("circle '<(0,0),1>' = circle '<(9,9),1>'", true),
        ("circle '<(0,0),1>' <> circle '<(0,0),2>'", true),
        ("circle '<(0,0),1>' < circle '<(0,0),2>'", true),
        ("line '{1,-1,0}' = line '{1,-1,0}'", true),
        ("line '{1,-1,0}' = line '{1,1,0}'", false),
        // `line_eq` is PROPORTIONAL: the same line written at any scale, or
        // negated, is equal. Nothing normalizes on input — `line '{2,-2,0}'`
        // still prints `{2,-2,0}` — so this is decided at comparison time.
        ("line '{1,-1,0}' = line '{2,-2,0}'", true),
        ("line '{1,-1,0}' = line '{-1,1,0}'", true),
        ("line '{0,-1,5}' = line '{0,-2,10}'", true),
        // Parallel but offset is a different line, at any scale.
        ("line '{1,-1,0}' = line '{1,-1,5}'", false),
        // A NaN coefficient falls back to exact equality, under which a NaN
        // equals itself.
        ("line '{NaN,NaN,NaN}' = line '{NaN,NaN,NaN}'", true),
        // `lseg_eq` compares endpoints, NOT the length the orderings use.
        ("lseg '[(0,0),(1,1)]' = lseg '[(5,5),(6,6)]'", false),
        ("lseg '[(0,0),(1,1)]' <> lseg '[(5,5),(6,6)]'", true),
        ("lseg '[(0,0),(1,1)]' < lseg '[(0,0),(2,2)]'", true),
        // `path_n_eq` is the point count and nothing else.
        ("path '[(0,0),(1,1)]' = path '[(5,5),(6,6)]'", true),
        ("path '[(0,0),(1,1)]' < path '[(0,0),(1,1),(2,2)]'", true),
    ];
    for (expr, expected) in cases {
        let rendered = if expected { "t" } else { "f" };
        assert!(
            scalar(&mut s, expr).await == Some(rendered.into()),
            "{expr}"
        );
    }

    // `point` has `<>` and nothing else.
    assert!(scalar(&mut s, "point '(1,2)' <> point '(1,2)'").await == Some("f".into()));
    assert!(scalar(&mut s, "point '(1,2)' <> point '(1,3)'").await == Some("t".into()));
}

/// A comparison across two different geometric types has no operator at all,
/// and `IS DISTINCT FROM` is resolved through the type's `=` — so it is named
/// with `=` when there is none.
#[tokio::test]
async fn cross_type_and_distinct_comparisons_name_the_missing_operator() {
    let (_engine, mut s) = session();

    assert_all_undefined(
        &mut s,
        &[
            (
                "point '(1,2)' = box '(0,0),(1,1)'",
                "operator does not exist: point = box",
            ),
            (
                "box '(0,0),(1,1)' = circle '<(0,0),1>'",
                "operator does not exist: box = circle",
            ),
            (
                "lseg '[(0,0),(1,1)]' < path '[(0,0),(1,1)]'",
                "operator does not exist: lseg < path",
            ),
            (
                "point '(1,2)' IS DISTINCT FROM point '(1,2)'",
                "operator does not exist: point = point",
            ),
            (
                "polygon '((0,0),(1,0),(1,1))' IS DISTINCT FROM polygon '((0,0),(1,0),(1,1))'",
                "operator does not exist: polygon = polygon",
            ),
        ],
    )
    .await;

    // The types that DO have `=` keep `IS DISTINCT FROM`, and a NULL operand
    // resolves the operator from its sibling rather than being refused — even
    // for the types that have no `=` at all.
    // `IN (…)`, a simple `CASE` and a row comparison all expand into the type's
    // `=` or `<`, so each names the same missing operator.
    assert_all_undefined(
        &mut s,
        &[
            (
                "point '(1,2)' IN (point '(1,2)')",
                "operator does not exist: point = point",
            ),
            (
                "polygon '((0,0),(1,0),(1,1))' IN (polygon '((0,0),(1,0),(1,1))')",
                "operator does not exist: polygon = polygon",
            ),
            (
                "CASE point '(1,2)' WHEN point '(1,2)' THEN 1 END",
                "operator does not exist: point = point",
            ),
            (
                "ROW(point '(1,2)') = ROW(point '(1,2)')",
                "operator does not exist: point = point",
            ),
            (
                "ROW(point '(1,2)') < ROW(point '(1,2)')",
                "operator does not exist: point < point",
            ),
            (
                "point '(1,2)' BETWEEN point '(0,0)' AND point '(9,9)'",
                "operator does not exist: point >= point",
            ),
        ],
    )
    .await;

    // The same constructs over a type that HAS the operator still work.
    for (expr, expected) in [
        ("box '(0,0),(1,1)' IN (box '(0,0),(1,1)')", "t"),
        ("ROW(box '(0,0),(1,1)') = ROW(box '(0,0),(1,1)')", "t"),
        ("ROW(box '(0,0),(1,1)') < ROW(box '(0,0),(2,2)')", "t"),
        (
            "box '(0,0),(1,1)' BETWEEN box '(0,0),(0,0)' AND box '(0,0),(9,9)'",
            "t",
        ),
        ("box '(0,0),(1,1)' IS DISTINCT FROM box '(0,0),(1,1)'", "f"),
        ("box '(0,0),(1,1)' IS DISTINCT FROM NULL", "t"),
        ("NULL::box IS DISTINCT FROM NULL", "f"),
        ("point '(1,2)' IS DISTINCT FROM NULL", "t"),
        (
            "polygon '((0,0),(1,0),(1,1))' IS NOT DISTINCT FROM NULL",
            "f",
        ),
    ] {
        assert!(
            scalar(&mut s, expr).await == Some(expected.into()),
            "{expr}"
        );
    }
}

// ---------------------------------------------------------------------------
// The prefix operators
// ---------------------------------------------------------------------------

/// `#`, `@-@`, `@@`, `?-` and `?|` over the operands `PostgreSQL` declares them
/// for. `@@` is the one that surprises: a `path` has a length and a point count
/// but NO centre.
#[tokio::test]
async fn the_geometric_prefix_operators_answer_their_declared_operands() {
    let (_engine, mut s) = session();

    let cases: [(&str, &str, u32); 14] = [
        ("# path '[(0,0),(1,1),(2,2)]'", "3", INT4_OID),
        ("# polygon '((0,0),(1,0),(1,1))'", "3", INT4_OID),
        ("@-@ lseg '[(0,0),(3,4)]'", "5", FLOAT8_OID),
        ("@-@ path '[(0,0),(3,4),(3,0)]'", "9", FLOAT8_OID),
        ("@@ box '(0,0),(2,2)'", "(1,1)", POINT_OID),
        ("@@ circle '<(1,2),3>'", "(1,2)", POINT_OID),
        ("@@ lseg '[(0,0),(2,2)]'", "(1,1)", POINT_OID),
        ("@@ polygon '((0,0),(2,0),(2,2),(0,2))'", "(1,1)", POINT_OID),
        ("?- line '{0,1,-5}'", "t", BOOL_OID),
        ("?- line '{1,0,-5}'", "f", BOOL_OID),
        ("?- lseg '[(0,0),(2,0)]'", "t", BOOL_OID),
        ("?| line '{1,0,-5}'", "t", BOOL_OID),
        ("?| line '{0,1,-5}'", "f", BOOL_OID),
        ("?| lseg '[(0,0),(0,2)]'", "t", BOOL_OID),
    ];
    for (expr, expected, oid) in cases {
        assert!(
            typed_scalar(&mut s, expr).await == (Some(expected.into()), oid),
            "{expr}"
        );
    }
}

/// Every operand a geometric prefix operator is NOT declared over, including
/// `integer` — `@-@ 5` is one operator applied to an integer, not `@(-(@5))`.
#[tokio::test]
async fn a_geometric_prefix_operator_over_the_wrong_operand_is_undefined() {
    let (_engine, mut s) = session();

    assert_all_undefined(
        &mut s,
        &[
            // `@@` has no `path` overload, open or closed.
            (
                "@@ path '[(0,0),(1,1)]'",
                "operator does not exist: @@ path",
            ),
            (
                "@@ path '((0,0),(1,0),(1,1))'",
                "operator does not exist: @@ path",
            ),
            ("@@ point '(1,1)'", "operator does not exist: @@ point"),
            // `#` counts points, so only a path or a polygon has one.
            ("# lseg '[(0,0),(1,1)]'", "operator does not exist: # lseg"),
            ("# box '(0,0),(1,1)'", "operator does not exist: # box"),
            ("# circle '<(0,0),1>'", "operator does not exist: # circle"),
            ("# line '{1,-1,0}'", "operator does not exist: # line"),
            ("# point '(1,1)'", "operator does not exist: # point"),
            ("@-@ box '(0,0),(1,1)'", "operator does not exist: @-@ box"),
            (
                "@-@ polygon '((0,0),(1,0),(1,1))'",
                "operator does not exist: @-@ polygon",
            ),
            ("?- box '(0,0),(1,1)'", "operator does not exist: ?- box"),
            ("?| box '(0,0),(1,1)'", "operator does not exist: ?| box"),
            ("?- point '(1,1)'", "operator does not exist: ?- point"),
            ("?| point '(1,1)'", "operator does not exist: ?| point"),
            // And on a non-geometric operand. `@-@ 5` returned 5 before this
            // wave, because nothing claimed the spelling.
            ("@-@ 5", "operator does not exist: @-@ integer"),
            ("# 5", "operator does not exist: # integer"),
            ("@@ 5", "operator does not exist: @@ integer"),
            ("?- 5", "operator does not exist: ?- integer"),
            ("?| 5", "operator does not exist: ?| integer"),
            ("@-@ 'abc'::text", "operator does not exist: @-@ text"),
        ],
    )
    .await;
}

// ---------------------------------------------------------------------------
// Subscripting
// ---------------------------------------------------------------------------

/// `point`, `box`, `lseg` and `line` subscript into their own fields, 0-based
/// where the array rule is 1-based. Out of range — a negative index included —
/// is SQL NULL rather than an error.
#[tokio::test]
async fn the_four_subscriptable_geometric_types_index_their_own_fields() {
    let (_engine, mut s) = session();

    let cases: [(&str, &str, u32); 9] = [
        ("(point '(1,2)')[0]", "1", FLOAT8_OID),
        ("(point '(1,2)')[1]", "2", FLOAT8_OID),
        // `box_subscript` reads the HIGH corner first.
        ("(box '(1,2),(3,4)')[0]", "(3,4)", POINT_OID),
        ("(box '(1,2),(3,4)')[1]", "(1,2)", POINT_OID),
        ("(lseg '[(1,2),(3,4)]')[0]", "(1,2)", POINT_OID),
        ("(lseg '[(1,2),(3,4)]')[1]", "(3,4)", POINT_OID),
        ("(line '{1,-2,3}')[0]", "1", FLOAT8_OID),
        ("(line '{1,-2,3}')[1]", "-2", FLOAT8_OID),
        ("(line '{1,-2,3}')[2]", "3", FLOAT8_OID),
    ];
    for (expr, expected, oid) in cases {
        assert!(
            typed_scalar(&mut s, expr).await == (Some(expected.into()), oid),
            "{expr}"
        );
    }

    for expr in [
        "(point '(1,2)')[2]",
        "(point '(1,2)')[-1]",
        "(box '(1,2),(3,4)')[2]",
        "(lseg '[(1,2),(3,4)]')[2]",
        "(line '{1,-2,3}')[3]",
        "(point '(1,2)')[NULL::int]",
        // A fixed-length type has ONE dimension, so a two-dimensional
        // reference is NULL — `box[0][1]` is not `(box[0])[1]`.
        "(box '(1,2),(3,4)')[0][1]",
        "(NULL::point)[0]",
    ] {
        assert!(scalar(&mut s, expr).await.is_none(), "{expr}");
    }
}

/// `circle`, `path` and `polygon` have no subscript handler at all, and a
/// fixed-length type cannot be sliced.
#[tokio::test]
async fn the_unsubscriptable_geometric_types_and_slices_are_refused() {
    let (_engine, mut s) = session();

    for (expr, name) in [
        ("(circle '<(0,0),1>')[0]", "circle"),
        ("(path '[(0,0),(1,1)]')[0]", "path"),
        ("(polygon '((0,0),(1,0),(1,1))')[0]", "polygon"),
        // The NULL value still knows its type, so the rejection is the same.
        ("(NULL::circle)[0]", "circle"),
    ] {
        assert!(
            expr_err(&mut s, expr).await
                == (
                    "42804".into(),
                    format!(
                        "cannot subscript type {name} because it does not support subscripting"
                    )
                ),
            "{expr}"
        );
    }

    for expr in ["(point '(1,2)')[0:1]", "(box '(1,2),(3,4)')[0:1]"] {
        assert!(
            expr_err(&mut s, expr).await
                == (
                    "0A000".into(),
                    "slices of fixed-length arrays not implemented".into()
                ),
            "{expr}"
        );
    }
}

/// Subscripting is one shared helper, and teaching it about geometry must not
/// change what every OTHER unsubscriptable type reports.
#[tokio::test]
async fn non_geometric_subscripting_is_unchanged() {
    let (_engine, mut s) = session();

    for (expr, name) in [
        ("(5)[1]", "integer"),
        ("('abc'::text)[1]", "text"),
        ("(now())[1]", "timestamp with time zone"),
        ("('1.5'::float8)[1]", "double precision"),
        ("(true)[1]", "boolean"),
        ("(interval '1 day')[1]", "interval"),
        ("(int4range(1,5))[1]", "int4range"),
        ("('{\"a\":1}'::json)[0]", "json"),
    ] {
        assert!(
            expr_err(&mut s, expr).await
                == (
                    "42804".into(),
                    format!(
                        "cannot subscript type {name} because it does not support subscripting"
                    )
                ),
            "{expr}"
        );
    }

    // The array and jsonb rules themselves are untouched.
    assert!(scalar(&mut s, "(ARRAY[10,20,30])[2]").await == Some("20".into()));
    assert!(scalar(&mut s, "(ARRAY[10,20,30])[2:3]").await == Some("{20,30}".into()));
    assert!(scalar(&mut s, "(ARRAY[10,20,30])[9]").await.is_none());
    assert!(scalar(&mut s, "('{\"a\": 1}'::jsonb)['a']").await == Some("1".into()));
    assert!(scalar(&mut s, "('[10,20]'::jsonb)[1]").await == Some("20".into()));
}

// ---------------------------------------------------------------------------
// NULL, columns and untyped literals
// ---------------------------------------------------------------------------

/// Every geometric operator is strict, and the operators work over stored
/// columns and bare literals just as they do over typed ones.
#[tokio::test]
async fn geometric_operators_are_strict_and_work_over_columns() {
    let (_engine, mut s) = session();

    for expr in [
        "point '(1,2)' <-> NULL",
        "point '(1,2)' + NULL::point",
        "NULL::box # box '(0,0),(1,1)'",
        "NULL::point ## lseg '[(0,0),(1,1)]'",
        "point '(1,2)' ?- NULL::point",
        "NULL::box <^ box '(0,0),(1,1)'",
        "# NULL::path",
        "@-@ NULL::lseg",
        "@@ NULL::box",
        "?- NULL::line",
        "box '(0,0),(1,1)' = NULL",
    ] {
        assert!(scalar(&mut s, expr).await.is_none(), "{expr}");
    }

    s.simple_query(
        "CREATE TABLE geo (p point, b box, s lseg, l line, h path, g polygon, c circle)",
    )
    .await
    .expect("create");
    s.simple_query(
        "INSERT INTO geo VALUES ('(1,2)', '(0,0),(4,4)', '[(0,0),(2,2)]', '{1,-1,0}', \
         '[(0,0),(1,1),(2,2)]', '((0,0),(4,0),(4,4),(0,4))', '<(0,0),2>')",
    )
    .await
    .expect("insert");

    let row = rows(
        &mut s,
        "SELECT p <@ b, p <-> b, @@ b, # h, p ## b, s ?# l, g @> p, b <^ b, p[0], \
         s <@ b, c @> p, ?- l, @-@ s FROM geo",
    )
    .await;
    assert!(
        row[0]
            == vec![
                Some("t".into()),
                Some("0".into()),
                Some("(2,2)".into()),
                Some("3".into()),
                Some("(1,2)".into()),
                // A segment lying ALONG a line does not `?#` it — `inter_sl`
                // asks for a crossing, and PostgreSQL answers false here.
                Some("f".into()),
                Some("t".into()),
                // `box_below_eq` wants this box's TOP at or below the other's
                // BOTTOM, so a box is not below-or-equal to itself.
                Some("f".into()),
                Some("1".into()),
                Some("t".into()),
                Some("f".into()),
                Some("f".into()),
                Some("2.8284271247461903".into()),
            ]
    );

    // A bare literal beside a geometric operand adopts the type the operator's
    // one surviving candidate wants: its own for `<<`, but a POINT for `+`.
    assert!(scalar(&mut s, "point '(1,2)' << '(5,5)'").await == Some("t".into()));
    assert!(scalar(&mut s, "box '(0,0),(1,1)' + '(1,2)'").await == Some("(2,3),(1,2)".into()));
    assert!(
        scalar(&mut s, "box '(0,0),(2,2)' # '(1,1),(3,3)'").await == Some("(2,2),(1,1)".into())
    );
    assert!(
        scalar(
            &mut s,
            "polygon '((0,0),(1,0),(1,1))' ~= '((0,0),(1,0),(1,1))'"
        )
        .await
            == Some("t".into())
    );
    assert!(scalar(&mut s, "point '(1,1)' ?- '(5,1)'").await == Some("t".into()));
    assert!(scalar(&mut s, "point '(1,1)' <^ '(5,2)'").await == Some("t".into()));
    assert!(scalar(&mut s, "line '{1,-1,0}' ?-| '{1,1,-2}'").await == Some("t".into()));
}

// ---------------------------------------------------------------------------
// Shadowing — the families that share these spellings
// ---------------------------------------------------------------------------

/// The geometric dispatch runs SECOND in `apply_binary`'s prelude, ahead of the
/// network, bit-string, money and system-identifier families, and it shares
/// `<<`, `>>`, `&&`, `#`, `+`, `-`, `*`, `/`, `@>`, `<@`, `<->`, `?|` and `@@`
/// with them. Every expression here has NO geometric operand and must be
/// answered by whichever family owns the spelling.
#[tokio::test]
async fn the_geometric_dispatch_shadows_no_other_family() {
    let (_engine, mut s) = session();

    let cases: [(&str, &str); 20] = [
        ("inet '10.0.0.0/8' << inet '10.0.0.0/7'", "t"),
        ("inet '10.0.0.0/8' >>= inet '10.0.0.0/8'", "t"),
        ("inet '10.0.0.0/8' && inet '10.0.0.0/7'", "t"),
        ("B'1010' # B'0110'", "1100"),
        ("B'1010' | B'0110'", "1110"),
        ("money '1.00' < money '2.00'", "t"),
        ("(money '3.00' - money '1.00')::text", "$2.00"),
        ("(pg_lsn '0/16B3748' - pg_lsn '0/16B3740')::text", "8"),
        ("'1'::xid = '1'::xid", "t"),
        ("int4range(1,5) &< int4range(6,9)", "t"),
        ("int4range(1,5) @> 3", "t"),
        ("(int4range(1,5) + int4range(4,9))::text", "[1,9)"),
        ("ARRAY[1,2] && ARRAY[2,3]", "t"),
        ("'[1,2,3]'::jsonb @> '[1]'::jsonb", "t"),
        ("'{\"a\":1}'::jsonb ?| ARRAY['a']", "t"),
        ("to_tsvector('cats') @@ to_tsquery('cat')", "t"),
        (
            "(to_tsquery('cat') <-> to_tsquery('dog'))::text",
            "'cat' <-> 'dog'",
        ),
        ("(5 # 3)::text", "6"),
        ("(5 | 3)::text", "7"),
        ("((1 << 4) + (256 >> 4))::text", "32"),
    ];
    for (expr, expected) in cases {
        assert!(
            scalar(&mut s, expr).await == Some(expected.into()),
            "{expr}"
        );
    }

    // The comparison gate is geometry-only too: the pre-existing
    // `cannot compare` behaviour for the non-geometric types is unchanged.
    assert!(scalar(&mut s, "5 < 6").await == Some("t".into()));
    assert!(scalar(&mut s, "'a' < 'b'").await == Some("t".into()));
    assert!(scalar(&mut s, "'1'::xid = '1'::xid").await == Some("t".into()));
    assert!(
        expr_err(&mut s, "'1'::xid < '2'::xid").await
            == ("42883".into(), "operator does not exist: xid < xid".into())
    );
}

/// A geometric operand beside a NON-geometric one has no operator at all, so
/// the geometric dispatch must report 42883 rather than let a later family
/// answer with its own meaning.
#[tokio::test]
async fn a_geometric_operand_beside_a_foreign_one_is_undefined() {
    let (_engine, mut s) = session();

    assert_all_undefined(
        &mut s,
        &[
            (
                "point '(1,2)' << 4",
                "operator does not exist: point << integer",
            ),
            (
                "box '(0,0),(1,1)' && ARRAY[1,2]",
                "operator does not exist: box && integer[]",
            ),
            (
                "point '(1,2)' <-> '[1,2]'::jsonb",
                "operator does not exist: point <-> jsonb",
            ),
            (
                "circle '<(0,0),1>' @> 5",
                "operator does not exist: circle @> integer",
            ),
        ],
    )
    .await;
}

// ---------------------------------------------------------------------------
// Deparsing
// ---------------------------------------------------------------------------

/// `pg_get_viewdef` has to spell every operator a view body can contain, or the
/// definition it prints does not parse back. Before this wave the new geometric
/// spellings printed as `?`.
#[tokio::test]
async fn a_view_over_the_geometric_operators_round_trips() {
    let (_engine, mut s) = session();

    s.simple_query("CREATE TABLE gv (p point, b box, s lseg, l line, h path)")
        .await
        .expect("create");
    // `b # b` is `box`-valued, so this also covers `column_type_from_oid`
    // carrying a geometric query field type through `RowDescription`.
    s.simple_query(
        "CREATE VIEW vg AS SELECT p ## b AS a, s ?# l AS bb, p ?- p AS c, l ?-| l AS d, \
         l ?|| l AS e, b <^ b AS f, b >^ b AS g2, p <-> b AS i, b ~= b AS j, \
         b &<| b AS k, b |&> b AS m, b <<| b AS n, b |>> b AS o, p ?| p AS q, \
         # h AS r, @-@ s AS t, @@ b AS u, ?- l AS v, ?| l AS w, p[0] AS x, \
         b # b AS h2 FROM gv",
    )
    .await
    .expect("create view");

    let definition = scalar(&mut s, "pg_get_viewdef('vg'::regclass, true)")
        .await
        .expect("definition");
    for fragment in [
        "p ## b AS a",
        "s ?# l AS bb",
        "p ?- p AS c",
        "l ?-| l AS d",
        "l ?|| l AS e",
        "b <^ b AS f",
        "b >^ b AS g2",
        "p <-> b AS i",
        "b ~= b AS j",
        "b &<| b AS k",
        "b |&> b AS m",
        "b <<| b AS n",
        "b |>> b AS o",
        "p ?| p AS q",
        "# h AS r",
        "@-@ s AS t",
        "@@ b AS u",
        "?- l AS v",
        "?| l AS w",
        "p[0] AS x",
        "b # b AS h2",
    ] {
        assert!(
            definition.contains(fragment),
            "`{fragment}` missing from:\n{definition}"
        );
    }
    assert!(
        !definition.contains('?') || !definition.contains(" ? "),
        "{definition}"
    );

    // The un-pretty form parenthesizes every operator node, exactly as
    // `get_rule_expr` does.
    let plain = scalar(&mut s, "pg_get_viewdef('vg'::regclass)")
        .await
        .expect("definition");
    for fragment in ["(p ## b) AS a", "(# h) AS r", "(@-@ s) AS t", "(@@ b) AS u"] {
        assert!(
            plain.contains(fragment),
            "`{fragment}` missing from:\n{plain}"
        );
    }
}

// ---------------------------------------------------------------------------
// Catalog visibility
// ---------------------------------------------------------------------------

/// psql's `\d` renders every column through `format_type`, and `pg_type` is
/// what an introspection query joins against. Both tables were keyed by a
/// literal oid list that only ever held `point` and `path` of the seven, so a
/// `box` or `polygon` column printed its type as `-` and had no `pg_type` row
/// at all.
///
/// Every expected value here came from `PostgreSQL` 18.4.
#[tokio::test]
async fn every_geometric_type_is_visible_to_catalog_introspection() {
    let (_engine, mut s) = session();

    // (type name, oid, typlen). `typcategory` is 'G' for all seven.
    let types: [(&str, u32, i32); 7] = [
        ("point", 600, 16),
        ("lseg", 601, 32),
        ("path", 602, -1),
        ("box", 603, 32),
        ("polygon", 604, -1),
        ("line", 628, 24),
        ("circle", 718, 24),
    ];

    for (name, oid, len) in types {
        // `format_type` names it, so `\d` prints the type instead of `-`.
        assert!(
            scalar(&mut s, &format!("format_type({oid}, -1)")).await == Some(name.into()),
            "format_type({oid}, -1)"
        );
        // A `pg_type` row exists, with the length and category upstream records.
        let row = rows(
            &mut s,
            &format!("SELECT typname, typlen::text, typcategory FROM pg_type WHERE oid = {oid}"),
        )
        .await;
        assert!(
            row == vec![vec![
                Some(name.into()),
                Some(len.to_string()),
                Some("G".into())
            ]],
            "pg_type row for {name}"
        );
    }

    // And through a real column, which is the path `\d` actually takes.
    s.simple_query(
        "CREATE TABLE gcat (a point, b lseg, c path, d box, e polygon, f line, g circle)",
    )
    .await
    .expect("create");
    let named = rows(
        &mut s,
        "SELECT format_type(atttypid, atttypmod) FROM pg_attribute \
         WHERE attrelid = 'gcat'::regclass AND attnum > 0 ORDER BY attnum",
    )
    .await;
    let expected: Vec<Vec<Option<String>>> = types
        .iter()
        .map(|(name, _, _)| vec![Some((*name).into())])
        .collect();
    assert!(named == expected, "{named:?}");

    // None of them accepts a modifier, so a typmod can only arrive from a
    // direct call — where PostgreSQL's `printTypmod` fallback still prints it.
    assert!(scalar(&mut s, "format_type(603, 4)").await == Some("box(4)".into()));
}

/// `EXPLAIN`'s `Filter:` line kept a SECOND, partial copy of the operator
/// spelling table that ended in `_ => "?"`, so every operator it had not been
/// taught printed as a literal `?`. That silently covered the whole jsonb family
/// long before geometry existed; adding 7 more operators widened it.
///
/// The deparser now delegates to the one exhaustive table, so this test is
/// really a guard on that: a catch-all cannot fail loudly, and the only durable
/// fix is a table the compiler forces someone to extend.
#[tokio::test]
async fn explain_spells_every_operator_it_filters_on() {
    let (_engine, mut s) = session();
    s.simple_query("CREATE TABLE ge (p point, b box, g polygon, j jsonb)")
        .await
        .expect("create");

    // (predicate, the spelling EXPLAIN must print). The jsonb rows carry no
    // geometric type at all — they were broken by the same catch-all.
    let cases: [(&str, &str); 12] = [
        ("b && b", "&&"),
        ("b &< b", "&<"),
        ("b &> b", "&>"),
        ("b &<| b", "&<|"),
        ("b |&> b", "|&>"),
        ("b <^ b", "<^"),
        ("b >^ b", ">^"),
        ("b ~= b", "~="),
        ("g @> g", "@>"),
        ("p <@ g", "<@"),
        ("j ? 'k'", "?"),
        ("j @> '{}'::jsonb", "@>"),
    ];
    for (predicate, spelling) in cases {
        let plan = rows(
            &mut s,
            &format!("EXPLAIN (COSTS OFF) SELECT 1 FROM ge WHERE {predicate}"),
        )
        .await;
        let text: String = plan
            .iter()
            .filter_map(|row| row[0].clone())
            .collect::<Vec<_>>()
            .join("\n");
        let filter = text
            .lines()
            .find(|line| line.trim_start().starts_with("Filter:"))
            .unwrap_or_else(|| panic!("no Filter line for `{predicate}`:\n{text}"))
            .to_string();
        assert!(
            filter.contains(spelling),
            "`{predicate}` should print `{spelling}`, got: {filter}"
        );
        // `?` is the catch-all's output, so only the row that really is `?` may
        // contain one.
        if spelling != "?" {
            assert!(!filter.contains('?'), "`{predicate}` deparsed to: {filter}");
        }
    }
}
