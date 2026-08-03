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

    #[test]
    fn path_input_distinguishes_open_and_closed_paths() {
        let open = Path::parse("[(-1,2),(3,4)]").unwrap();
        assert!(!open.closed && open.points.len() == 2);
        let closed = Path::parse("((0,0),(1,1))").unwrap();
        assert!(closed.closed && closed.points.len() == 2);
        assert_eq!(Path::parse("[(0,0),]").unwrap_err().sqlstate(), "22P02");
    }
}
