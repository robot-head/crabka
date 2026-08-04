//! `PostgreSQL` geometric scalar values.

use std::hash::{Hash, Hasher};

use crate::TypeError;

/// `PostgreSQL` `point`: two IEEE-754 double-precision coordinates.
#[derive(Debug, Clone, Copy)]
pub struct Point {
    pub x: f64,
    pub y: f64,
}

/// `PostgreSQL` `path`: an ordered series of points, either open or closed.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct Path {
    pub closed: bool,
    pub points: Vec<Point>,
}

/// `PostgreSQL` `lseg`: a line segment between two endpoints.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct Lseg {
    pub start: Point,
    pub end: Point,
}

impl Lseg {
    /// Parse `lseg_in`'s spellings: `[(x,y),(x,y)]`, `((x,y),(x,y))`,
    /// `(x,y),(x,y)` and the bare `x,y,x,y`, each optionally bracketed.
    ///
    /// # Errors
    ///
    /// Returns `22P02` for anything that is not exactly two points, and the
    /// same coordinate errors as [`Point::parse`].
    pub fn parse(input: &str) -> Result<Self, TypeError> {
        let value = input.trim();
        // Strip one optional outer delimiter pair. `lseg_in` accepts `[…]` and
        // `(…)` alike, but only one level — and a leading `(` may instead open
        // the *first point*, as in `(1,2),(3,4)`, so parentheses count as a
        // wrapper only when the opening one closes at the very end.
        let inner = value
            .strip_prefix('[')
            .and_then(|inner| inner.strip_suffix(']'))
            .or_else(|| parenthesis_wraps_whole(value).then(|| &value[1..value.len() - 1]))
            .unwrap_or(value);
        let points = split_points(inner).ok_or_else(|| invalid_lseg(input))?;
        let [start, end] = points.as_slice() else {
            return Err(invalid_lseg(input));
        };
        Ok(Self {
            start: Point::parse(start).map_err(|_| invalid_lseg(input))?,
            end: Point::parse(end).map_err(|_| invalid_lseg(input))?,
        })
    }
}

/// `PostgreSQL` `circle`: a centre point and a radius.
#[derive(Debug, Clone, Copy)]
pub struct Circle {
    pub center: Point,
    pub radius: f64,
}

impl Circle {
    /// Parse `circle_in`: `<(x,y),r>`, `((x,y),r)`, `(x,y),r` or `x,y,r`.
    ///
    /// # Errors
    ///
    /// `22P02` for malformed input and for a negative radius; `22003` for a
    /// coordinate that overflows `float8`.
    pub fn parse(input: &str) -> Result<Self, TypeError> {
        let value = input.trim();
        // One optional wrapper: `<…>` always wraps, `(…)` only when its opening
        // parenthesis closes at the very end — otherwise it opens the centre.
        let inner = value
            .strip_prefix('<')
            .and_then(|inner| inner.strip_suffix('>'))
            .or_else(|| parenthesis_wraps_whole(value).then(|| &value[1..value.len() - 1]))
            .unwrap_or(value)
            .trim();
        let (center, radius) = if inner.starts_with('(') {
            let end = inner.find(')').ok_or_else(|| invalid_circle(input))?;
            let rest = inner[end + 1..]
                .trim_start()
                .strip_prefix(',')
                .ok_or_else(|| invalid_circle(input))?;
            (&inner[..=end], rest)
        } else {
            // `x,y,r`: the radius is everything after the second comma.
            let first = inner.find(',').ok_or_else(|| invalid_circle(input))?;
            let second = first
                + 1
                + inner[first + 1..]
                    .find(',')
                    .ok_or_else(|| invalid_circle(input))?;
            (&inner[..second], &inner[second + 1..])
        };
        let circle = Self {
            center: Point::parse(center).map_err(|error| match error {
                TypeError::InvalidText { .. } => invalid_circle(input),
                other => other,
            })?,
            radius: coordinate(radius, input).map_err(|error| match error {
                TypeError::InvalidText { .. } => invalid_circle(input),
                other => other,
            })?,
        };
        // `circle_in` rejects a negative radius as bad input; zero and NaN are
        // values.
        if circle.radius < 0.0 {
            return Err(invalid_circle(input));
        }
        Ok(circle)
    }

    /// `area(circle)` — `pi * r^2`, which is also how circles order.
    #[must_use]
    pub fn area(self) -> f64 {
        std::f64::consts::PI * self.radius * self.radius
    }

    /// SQL ordering for circles: every comparison operator works on *area*
    /// through `PostgreSQL`'s `FPeq`/`FPlt` macros, so areas within `EPSILON`
    /// compare equal and the centres are ignored entirely. `None` means the
    /// comparison is undefined because an area is NaN, where `PostgreSQL`'s C
    /// macros make every operator false.
    #[must_use]
    pub fn compare(self, other: Self) -> Option<std::cmp::Ordering> {
        /// `PostgreSQL`'s `EPSILON` from `geo_decls.h`.
        const EPSILON: f64 = 1.0E-06;
        let (left, right) = (self.area(), other.area());
        if left.is_nan() || right.is_nan() {
            return None;
        }
        if (left - right).abs() < EPSILON {
            return Some(std::cmp::Ordering::Equal);
        }
        left.partial_cmp(&right)
    }

    /// `circle <-> circle`: the gap between their boundaries, clamped at zero
    /// when they overlap, as `circle_distance` computes it.
    #[must_use]
    pub fn distance(self, other: Self) -> f64 {
        // `circle_distance` subtracts the radii as one sum, and the centre
        // distance comes from `pg_hypot`; both groupings are load-bearing for
        // the last digit.
        let gap = pg_hypot(
            self.center.x - other.center.x,
            self.center.y - other.center.y,
        ) - (self.radius + other.radius);
        if gap < 0.0 { 0.0 } else { gap }
    }
}

/// `PostgreSQL` `line`: the infinite line `Ax + By + C = 0`.
#[derive(Debug, Clone, Copy)]
pub struct Line {
    pub a: f64,
    pub b: f64,
    pub c: f64,
}

impl Line {
    /// Parse `line_in`: the coefficient form `{A,B,C}`, or any two-point
    /// spelling `lseg_in` accepts, which is converted to coefficients.
    ///
    /// # Errors
    ///
    /// `22P02` for malformed input, for `A` and `B` both zero, and for two
    /// equal points; `22003` for a coefficient or coordinate that overflows
    /// `float8`, which passes through rather than being reported as syntax.
    pub fn parse(input: &str) -> Result<Self, TypeError> {
        let value = input.trim();
        if let Some(inner) = value
            .strip_prefix('{')
            .and_then(|inner| inner.strip_suffix('}'))
        {
            let fields: Vec<&str> = inner.split(',').collect();
            let [a, b, c] = fields.as_slice() else {
                return Err(invalid_line(input));
            };
            let line = Self {
                a: line_coordinate(a, input)?,
                b: line_coordinate(b, input)?,
                c: line_coordinate(c, input)?,
            };
            if line.a == 0.0 && line.b == 0.0 {
                return Err(line_specification(
                    "invalid line specification: A and B cannot both be zero",
                ));
            }
            return Ok(line);
        }
        let inner = value
            .strip_prefix('[')
            .and_then(|inner| inner.strip_suffix(']'))
            .or_else(|| parenthesis_wraps_whole(value).then(|| &value[1..value.len() - 1]))
            .unwrap_or(value);
        let points = split_points(inner).ok_or_else(|| invalid_line(input))?;
        let [start, end] = points.as_slice() else {
            return Err(invalid_line(input));
        };
        Self::from_points(line_point(start, input)?, line_point(end, input)?)
    }

    /// The line through two distinct points, as `line(point, point)` builds it.
    ///
    /// # Errors
    ///
    /// `22P02` when the points are equal.
    pub fn from_points(start: Point, end: Point) -> Result<Self, TypeError> {
        if start == end {
            return Err(line_specification(
                "invalid line specification: must be two distinct points",
            ));
        }
        // A vertical line has no slope, so it is written directly; every other
        // line is normalized to `B = -1` the way `line_construct_pts` does.
        // `partial_cmp` is IEEE `==` without the float-comparison lint: a NaN
        // abscissa is *not* equal to itself, so it takes the slope branch and
        // produces NaN coefficients, exactly as PostgreSQL does.
        if start.x.partial_cmp(&end.x) == Some(std::cmp::Ordering::Equal) {
            return Ok(Self {
                a: -1.0,
                b: 0.0,
                c: start.x,
            });
        }
        let a = (end.y - start.y) / (end.x - start.x);
        Ok(Self {
            a,
            b: -1.0,
            c: start.y - a * start.x,
        })
    }
}

/// A coefficient or coordinate inside a `line`: an overflow is the float's own
/// `22003`, while bad syntax names `line` rather than the inner type.
fn line_coordinate(value: &str, whole: &str) -> Result<f64, TypeError> {
    coordinate(value, whole).map_err(|error| match error {
        TypeError::InvalidText { .. } => invalid_line(whole),
        other => other,
    })
}

fn line_point(value: &str, whole: &str) -> Result<Point, TypeError> {
    Point::parse(value).map_err(|error| match error {
        TypeError::InvalidText { .. } => invalid_line(whole),
        other => other,
    })
}

/// Whether the value's leading `(` is closed by its final `)`, which is what
/// distinguishes the wrapper in `((1,2),(3,4))` from the first point's own
/// parenthesis in `(1,2),(3,4)`.
fn parenthesis_wraps_whole(value: &str) -> bool {
    let mut depth = 0_usize;
    for (index, byte) in value.bytes().enumerate() {
        match byte {
            b'(' => depth += 1,
            b')' => match depth.checked_sub(1) {
                Some(remaining) => {
                    depth = remaining;
                    if depth == 0 {
                        return index + 1 == value.len();
                    }
                }
                None => return false,
            },
            _ => {}
        }
    }
    false
}

/// Split a two-point body into its two point texts. A parenthesized body
/// splits on the comma *between* the points; a bare `x,y,x,y` splits at its
/// second comma so each half is an `x,y` pair.
fn split_points(inner: &str) -> Option<Vec<&str>> {
    let trimmed = inner.trim();
    if !trimmed.starts_with('(') {
        let first = trimmed.find(',')?;
        let second = first + 1 + trimmed[first + 1..].find(',')?;
        return Some(vec![&trimmed[..second], &trimmed[second + 1..]]);
    }
    let mut points = Vec::new();
    let mut rest = trimmed;
    while !rest.is_empty() {
        let start = rest.strip_prefix('(')?;
        let end = start.find(')')?;
        points.push(&start[..end]);
        rest = start[end + 1..].trim_start();
        if rest.is_empty() {
            break;
        }
        rest = rest.strip_prefix(',')?.trim_start();
        if rest.is_empty() {
            return None;
        }
    }
    Some(points)
}

impl Path {
    /// Parse `path_in`'s bracketed (open) or parenthesized (closed) spelling.
    ///
    /// # Errors
    ///
    /// Returns the same coordinate errors as [`Point::parse`], or `22P02` for
    /// malformed delimiters and point lists.
    pub fn parse(input: &str) -> Result<Self, TypeError> {
        let value = input.trim();
        let (closed, inner) = if let Some(inner) = value
            .strip_prefix('[')
            .and_then(|inner| inner.strip_suffix(']'))
        {
            (false, inner)
        } else if let Some(inner) = value
            .strip_prefix('(')
            .and_then(|inner| inner.strip_suffix(')'))
        {
            (true, inner)
        } else {
            return Err(invalid_path(input));
        };
        let mut points = Vec::new();
        let mut rest = inner.trim();
        while !rest.is_empty() {
            let Some(point_start) = rest.strip_prefix('(') else {
                return Err(invalid_path(input));
            };
            let Some(end) = point_start.find(')') else {
                return Err(invalid_path(input));
            };
            points.push(Point::parse(&point_start[..end])?);
            rest = point_start[end + 1..].trim_start();
            if rest.is_empty() {
                break;
            }
            let Some(tail) = rest.strip_prefix(',') else {
                return Err(invalid_path(input));
            };
            rest = tail.trim_start();
            if rest.is_empty() {
                return Err(invalid_path(input));
            }
        }
        if points.is_empty() {
            return Err(invalid_path(input));
        }
        Ok(Self { closed, points })
    }
}

impl Point {
    /// Parse `point_in`'s ordinary `(x,y)` or `x,y` spelling.
    ///
    /// # Errors
    ///
    /// Returns `22P02` for malformed input and `22003` for a finite coordinate
    /// literal that overflows `float8`.
    pub fn parse(input: &str) -> Result<Self, TypeError> {
        let trimmed = input.trim();
        let inner = trimmed
            .strip_prefix('(')
            .and_then(|value| value.strip_suffix(')'))
            .unwrap_or(trimmed);
        let Some((x, y)) = inner.split_once(',') else {
            return Err(invalid(input));
        };
        if y.contains(',') || (trimmed.starts_with('(') != trimmed.ends_with(')')) {
            return Err(invalid(input));
        }
        Ok(Self {
            x: coordinate(x, input)?,
            y: coordinate(y, input)?,
        })
    }
}

fn coordinate(value: &str, whole: &str) -> Result<f64, TypeError> {
    let value = value.trim();
    let parsed = value.parse::<f64>().map_err(|_| invalid(whole))?;
    let explicit_infinity = matches!(
        value.to_ascii_lowercase().as_str(),
        "inf" | "+inf" | "-inf" | "infinity" | "+infinity" | "-infinity"
    );
    if parsed.is_infinite() && !explicit_infinity {
        return Err(TypeError::float_text_out_of_range(
            value,
            "double precision",
        ));
    }
    Ok(parsed)
}

fn invalid(value: &str) -> TypeError {
    TypeError::InvalidText {
        type_name: "point",
        value: value.to_string(),
    }
}

/// `PostgreSQL`'s own `pg_hypot`, which is `x * sqrt(1 + (y/x)^2)` rather than
/// libm's `hypot`. The two disagree in the last digit, and `PostgreSQL`'s
/// expected output records its own.
fn pg_hypot(x: f64, y: f64) -> f64 {
    if x.is_infinite() || y.is_infinite() {
        return f64::INFINITY;
    }
    if x.is_nan() || y.is_nan() {
        return f64::NAN;
    }
    let (mut larger, mut smaller) = (x.abs(), y.abs());
    if larger < smaller {
        std::mem::swap(&mut larger, &mut smaller);
    }
    if larger == 0.0 {
        return 0.0;
    }
    let ratio = smaller / larger;
    // Deliberately not `mul_add`: C rounds the multiply and the add
    // separately, and a fused multiply-add can differ in the last digit.
    let squared = ratio * ratio;
    larger * (1.0 + squared).sqrt()
}

fn invalid_circle(value: &str) -> TypeError {
    TypeError::InvalidText {
        type_name: "circle",
        value: value.to_string(),
    }
}

fn invalid_line(value: &str) -> TypeError {
    TypeError::InvalidText {
        type_name: "line",
        value: value.to_string(),
    }
}

/// `line_in`'s two rejections that describe the line rather than its syntax.
/// Both are 22P02 with a fixed message, as `PostgreSQL` writes them.
fn line_specification(message: &'static str) -> TypeError {
    TypeError::Domain {
        sqlstate: "22P02",
        message,
    }
}

fn invalid_lseg(value: &str) -> TypeError {
    TypeError::InvalidText {
        type_name: "lseg",
        value: value.to_string(),
    }
}

fn invalid_path(value: &str) -> TypeError {
    TypeError::InvalidText {
        type_name: "path",
        value: value.to_string(),
    }
}

impl PartialEq for Circle {
    /// Structural identity, for storage keys and hashing. SQL's `=` is a
    /// different relation — see [`Circle::compare`] — so the two are kept
    /// apart: an epsilon relation is not transitive and cannot back `Hash`.
    fn eq(&self, other: &Self) -> bool {
        self.center == other.center && float_eq(self.radius, other.radius)
    }
}

impl Eq for Circle {}

impl Hash for Circle {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.center.hash(state);
        float_bits(self.radius).hash(state);
    }
}

impl PartialEq for Line {
    /// `line_eq` compares coefficients directly, so a `NaN` coefficient equals
    /// itself — the upstream `line` file asserts exactly that.
    fn eq(&self, other: &Self) -> bool {
        float_eq(self.a, other.a) && float_eq(self.b, other.b) && float_eq(self.c, other.c)
    }
}

impl Eq for Line {}

impl Hash for Line {
    fn hash<H: Hasher>(&self, state: &mut H) {
        float_bits(self.a).hash(state);
        float_bits(self.b).hash(state);
        float_bits(self.c).hash(state);
    }
}

impl PartialEq for Point {
    fn eq(&self, other: &Self) -> bool {
        float_eq(self.x, other.x) && float_eq(self.y, other.y)
    }
}

impl Eq for Point {}

impl Hash for Point {
    fn hash<H: Hasher>(&self, state: &mut H) {
        float_bits(self.x).hash(state);
        float_bits(self.y).hash(state);
    }
}

fn float_eq(left: f64, right: f64) -> bool {
    left == right || (left.is_nan() && right.is_nan())
}

fn float_bits(value: f64) -> u64 {
    if value.is_nan() {
        f64::NAN.to_bits()
    } else if value == 0.0 {
        0
    } else {
        value.to_bits()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn point_input_accepts_coordinates_and_rejects_bad_shapes() {
        assert_eq!(
            Point::parse(" ( -10, Infinity ) "),
            Ok(Point {
                x: -10.0,
                y: f64::INFINITY
            })
        );
        assert_eq!(Point::parse("10,20"), Ok(Point { x: 10.0, y: 20.0 }));
        assert_eq!(Point::parse("(10 20)").unwrap_err().sqlstate(), "22P02");
        assert_eq!(Point::parse("(10,1e500)").unwrap_err().sqlstate(), "22003");
    }

    /// `lseg_in` accepts every spelling `PostgreSQL` does and rejects anything
    /// that is not exactly two points.
    #[test]
    fn lseg_input_accepts_every_two_point_spelling() {
        let expected = Lseg {
            start: Point { x: 1.0, y: 2.0 },
            end: Point { x: 3.0, y: 4.0 },
        };
        for spelling in [
            "[(1,2),(3,4)]",
            "((1,2),(3,4))",
            "(1,2),(3,4)",
            "1,2,3,4",
            "[1,2,3, 4]",
            "  [ (1,2) , (3,4) ]  ",
        ] {
            assert_eq!(Lseg::parse(spelling), Ok(expected), "{spelling}");
        }
        // NaN coordinates are values, not errors.
        let nan = Lseg::parse("[(NaN,1),(NaN,90)]").expect("NaN endpoints");
        assert!(nan.start.x.is_nan() && nan.end.x.is_nan());

        for bad in [
            "(3asdf,2 ,3,4r2)",
            "[1,2,3, 4",
            "[(,2),(3,4)]",
            "[(1,2),(3,4)",
            "[(1,2),(3)]",
            "[(1,2),(3,4),(5,6)]",
            "[(1,2)]",
        ] {
            let error = Lseg::parse(bad).expect_err(bad);
            assert_eq!(error.sqlstate(), "22P02", "{bad}");
            assert_eq!(
                error.to_string(),
                format!("invalid input syntax for type lseg: \"{bad}\""),
                "{bad}"
            );
        }
    }

    /// `line_in` takes coefficients directly and converts any two-point
    /// spelling, normalizing to `B = -1` except for a vertical line.
    /// `circle_in` accepts every spelling, keeps a zero or NaN radius as a
    /// value, and rejects a negative one. Comparison is by area through
    /// `PostgreSQL`'s epsilon macros, so the centres are ignored.
    #[test]
    fn circle_input_and_area_comparison_match_postgres() {
        use std::cmp::Ordering;

        use assert2::assert;

        let expected = Circle {
            center: Point { x: 1.0, y: 2.0 },
            radius: 3.0,
        };
        for spelling in [
            "<(1,2),3>",
            "((1,2),3)",
            "(1,2),3",
            "1,2,3",
            " ( ( 1 , 2 ) , 3 ) ",
        ] {
            assert!(Circle::parse(spelling) == Ok(expected), "{spelling}");
        }
        // A zero radius is a value, not a rejection.
        assert!(
            Circle::parse("<(3,5),0>")
                .expect("zero radius")
                .radius
                .to_bits()
                == 0.0_f64.to_bits()
        );
        assert!(
            Circle::parse("<(3,5),NaN>")
                .expect("NaN radius")
                .radius
                .is_nan()
        );
        for bad in [
            "<(-100,0),-100>",
            "<(100,200),10",
            "<(100,200),10> x",
            "1abc,3,5",
            "(3,(1,2),3)",
        ] {
            let error = Circle::parse(bad).expect_err(bad);
            assert!(error.sqlstate() == "22P02", "{bad}");
            assert!(
                error.to_string() == format!("invalid input syntax for type circle: \"{bad}\""),
                "{bad}"
            );
        }

        let unit = Circle::parse("<(0,0),1>").expect("unit");
        // Areas within EPSILON are equal however far apart the centres are.
        assert!(
            unit.compare(Circle::parse("<(9,9),1.0000001>").expect("near"))
                == Some(Ordering::Equal)
        );
        assert!(unit.compare(Circle::parse("<(9,9),1.001>").expect("far")) == Some(Ordering::Less));
        // A NaN area leaves every operator undefined.
        assert!(unit.compare(Circle::parse("<(3,5),NaN>").expect("nan")) == None);
    }

    #[test]
    fn line_input_converts_points_to_coefficients() {
        for (spelling, expected) in [
            (
                "{0,-1,5}",
                Line {
                    a: 0.0,
                    b: -1.0,
                    c: 5.0,
                },
            ),
            (
                "(0,0), (6,6)",
                Line {
                    a: 1.0,
                    b: -1.0,
                    c: 0.0,
                },
            ),
            (
                "10,-10 ,-5,-4",
                Line {
                    a: -0.4,
                    b: -1.0,
                    c: -6.0,
                },
            ),
            // Horizontal and vertical are the two normalized special cases.
            (
                "[(1,3),(2,3)]",
                Line {
                    a: 0.0,
                    b: -1.0,
                    c: 3.0,
                },
            ),
            (
                "[(3,1),(3,2)]",
                Line {
                    a: -1.0,
                    b: 0.0,
                    c: 3.0,
                },
            ),
        ] {
            assert_eq!(Line::parse(spelling), Ok(expected), "{spelling}");
        }
        // A NaN coefficient equals itself, which `line_eq` relies on.
        assert_eq!(Line::parse("{NaN,NaN,NaN}"), Line::parse("{NaN,NaN,NaN}"));

        for (bad, message) in [
            ("{}", "invalid input syntax for type line: \"{}\""),
            ("{0,0}", "invalid input syntax for type line: \"{0,0}\""),
            (
                "{1, 1, a}",
                "invalid input syntax for type line: \"{1, 1, a}\"",
            ),
            (
                "{0,0,1}",
                "invalid line specification: A and B cannot both be zero",
            ),
            (
                "[(1,2),(1,2)]",
                "invalid line specification: must be two distinct points",
            ),
        ] {
            let error = Line::parse(bad).expect_err(bad);
            assert_eq!(error.sqlstate(), "22P02", "{bad}");
            assert_eq!(error.to_string(), message, "{bad}");
        }
        // A coordinate overflow is the float's own 22003, not a syntax error.
        let overflow = Line::parse("{1, 1, 1e400}").expect_err("1e400");
        assert_eq!(overflow.sqlstate(), "22003");
        assert_eq!(
            overflow.to_string(),
            "\"1e400\" is out of range for type double precision"
        );
    }

    #[test]
    fn path_input_distinguishes_open_and_closed_paths() {
        let open = Path::parse("[(-1,2),(3,4)]").unwrap();
        assert!(!open.closed && open.points.len() == 2);
        let closed = Path::parse("((0,0),(1,1))").unwrap();
        assert!(closed.closed && closed.points.len() == 2);
        assert_eq!(Path::parse("[(0,0),]").unwrap_err().sqlstate(), "22P02");
    }
}
