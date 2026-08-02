//! PostgreSQL geometric scalar values.

use std::hash::{Hash, Hasher};

use crate::TypeError;

/// PostgreSQL `point`: two IEEE-754 double-precision coordinates.
#[derive(Debug, Clone, Copy)]
pub struct Point {
    pub x: f64,
    pub y: f64,
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
        assert!(
            Point::parse(" ( -10, Infinity ) ")
                == Ok(Point {
                    x: -10.0,
                    y: f64::INFINITY
                })
        );
        assert!(Point::parse("10,20") == Ok(Point { x: 10.0, y: 20.0 }));
        assert!(Point::parse("(10 20)").unwrap_err().sqlstate() == "22P02");
        assert!(Point::parse("(10,1e500)").unwrap_err().sqlstate() == "22003");
    }
}
