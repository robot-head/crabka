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

    #[test]
    fn path_input_distinguishes_open_and_closed_paths() {
        let open = Path::parse("[(-1,2),(3,4)]").unwrap();
        assert!(!open.closed && open.points.len() == 2);
        let closed = Path::parse("((0,0),(1,1))").unwrap();
        assert!(closed.closed && closed.points.len() == 2);
        assert_eq!(Path::parse("[(0,0),]").unwrap_err().sqlstate(), "22P02");
    }
}
