//! Errors from the type layer, each carrying the PostgreSQL SQLSTATE the
//! executor maps onto a wire ErrorResponse.

#![expect(
    clippy::pedantic,
    reason = "vendored PostgreSQL-compatible error surface kept structurally close to donor"
)]

#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum TypeError {
    #[error("integer out of range")]
    Overflow,
    #[error("division by zero")]
    DivisionByZero,
    #[error("invalid input syntax for type {type_name}: \"{value}\"")]
    InvalidText {
        type_name: &'static str,
        value: String,
    },
    #[error("{message}")]
    TypeMismatch { message: String },
    #[error("value too long for type character varying")]
    StringDataRightTruncation,
    /// SP28: a `LIKE`/`ILIKE` pattern ending in a lone escape `\` (22025).
    #[error("LIKE pattern must not end with escape character")]
    InvalidEscape,
    /// SP31: an explicit `CAST`/`::` between two types with no defined cast
    /// (42846) — e.g. `double precision` → `boolean`.
    #[error("cannot cast type {from} to {to}")]
    CannotCast {
        from: &'static str,
        to: &'static str,
    },
    /// a math/string domain error carrying its own PostgreSQL SQLSTATE —
    /// e.g. `ln(0)` (2201E), `sqrt(-1)` (2201F), `chr(0)` (54000). One
    /// code-carrying variant rather than one per domain.
    #[error("{message}")]
    Domain {
        sqlstate: &'static str,
        message: &'static str,
    },
    /// SP37: malformed date/time/interval literal or text (22007).
    #[error("invalid input syntax for type {type_name}: \"{value}\"")]
    InvalidDatetimeFormat {
        type_name: &'static str,
        value: String,
    },
    /// SP37: a date/time field out of range (e.g. month 13) (22008).
    #[error("date/time field value out of range: \"{value}\"")]
    DatetimeFieldOverflow { value: String },
}

impl TypeError {
    /// The five-character SQLSTATE for this error.
    pub fn sqlstate(&self) -> &'static str {
        match self {
            TypeError::Overflow => "22003",
            TypeError::DivisionByZero => "22012",
            TypeError::InvalidText { .. } => "22P02",
            TypeError::TypeMismatch { .. } => "42804",
            TypeError::StringDataRightTruncation => "22001",
            TypeError::InvalidEscape => "22025",
            TypeError::CannotCast { .. } => "42846",
            TypeError::Domain { sqlstate, .. } => sqlstate,
            TypeError::InvalidDatetimeFormat { .. } => "22007",
            TypeError::DatetimeFieldOverflow { .. } => "22008",
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn each_error_maps_to_its_postgres_sqlstate() {
        assert_eq!(TypeError::Overflow.sqlstate(), "22003");
        assert_eq!(TypeError::DivisionByZero.sqlstate(), "22012");
        assert_eq!(
            TypeError::InvalidText {
                type_name: "int4",
                value: "x".into(),
            }
            .sqlstate(),
            "22P02"
        );
        assert_eq!(
            TypeError::TypeMismatch {
                message: "boom".into(),
            }
            .sqlstate(),
            "42804"
        );
        assert_eq!(TypeError::StringDataRightTruncation.sqlstate(), "22001");
        assert_eq!(TypeError::InvalidEscape.sqlstate(), "22025");
        assert_eq!(
            TypeError::CannotCast {
                from: "double precision",
                to: "boolean",
            }
            .sqlstate(),
            "42846"
        );
        assert_eq!(
            TypeError::Domain {
                sqlstate: "2201E",
                message: "cannot take logarithm of a negative number",
            }
            .sqlstate(),
            "2201E"
        );
        assert_eq!(
            TypeError::InvalidDatetimeFormat {
                type_name: "date",
                value: "not-a-date".into(),
            }
            .sqlstate(),
            "22007"
        );
        assert_eq!(
            TypeError::DatetimeFieldOverflow {
                value: "2023-02-29".into(),
            }
            .sqlstate(),
            "22008"
        );
    }
}
