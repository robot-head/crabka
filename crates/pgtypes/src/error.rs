//! Errors from the type layer, each with the PostgreSQL SQLSTATE that the
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
    #[error("value too long for type {type_name}")]
    StringDataRightTruncation { type_name: String },
    /// SP28: a `LIKE`/`ILIKE` pattern ending in a lone escape `\` (22025).
    #[error("LIKE pattern must not end with escape character")]
    InvalidEscape,
    /// An `ESCAPE` clause whose string is neither empty nor a single character
    /// (22025), for example `LIKE 'x' ESCAPE 'ab'`.
    #[error("invalid escape string")]
    InvalidEscapeString,
    /// SP31: an explicit `CAST`/`::` between two types with no defined cast
    /// (42846), for example `double precision` → `boolean`.
    #[error("cannot cast type {from} to {to}")]
    CannotCast {
        from: &'static str,
        to: &'static str,
    },
    /// A math or string domain error that carries its own PostgreSQL SQLSTATE,
    /// for example `ln(0)` (2201E), `sqrt(-1)` (2201F), or `chr(0)` (54000).
    /// There is one code-carrying variant instead of one variant per domain.
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
    /// SP37: an `interval` field out of range (22015). SQL99 gives interval its
    /// own SQLSTATE, so `interval_in` promotes every field overflow the shared
    /// decoder reports into this one — `date/time field value out of range` is
    /// never what an `interval` literal raises.
    #[error("interval field value out of range: \"{value}\"")]
    IntervalFieldOverflow { value: String },
    /// A date/time value that leaves its type's range, carrying PostgreSQL's
    /// exact message for the context — `timestamp out of range`, `interval out
    /// of range`, `cannot subtract infinite dates` (22008).
    #[error("{message}")]
    DatetimeOutOfRange { message: String },
    /// A date/time literal whose UTC offset is outside ±15:59:59 (22009).
    #[error("time zone displacement out of range: \"{value}\"")]
    TimezoneDisplacementOverflow { value: String },
    /// A date/time literal naming a zone the zone database does not know
    /// (22023).
    #[error("time zone \"{name}\" not recognized")]
    UnknownTimeZone { name: String },
    /// A type-layer feature crabka deliberately does not implement (0A000),
    /// for example an array of an unsupported element type, or a
    /// multidimensional array literal.
    #[error("{message}")]
    FeatureNotSupported { message: String },
    /// Out of range (22003) like [`TypeError::Overflow`], but with the exact
    /// message PostgreSQL uses for that type and context: `smallint out of
    /// range`, `value out of range: overflow`, `value "99999" is out of range
    /// for type smallint`, `"1e39" is out of range for type real`. The bare
    /// `Overflow` variant hard-codes `integer out of range`, which is correct
    /// only for `int4`.
    #[error("{message}")]
    OutOfRange { message: String },
    /// A condition the named variants do not cover. It carries `PostgreSQL`'s
    /// own SQLSTATE and message. The array layer uses it for `array_in`'s
    /// `malformed array literal` (22P02), the subscript/dimension errors
    /// (2202E), and the dimension limit (54000).
    #[error("{message}")]
    Coded {
        sqlstate: &'static str,
        message: String,
    },
    #[error("malformed range literal: \"{value}\"")]
    RangeMalformed { value: String, detail: &'static str },
    /// `cidr_in`'s own rejection (22P02): the text parses as an address, but a
    /// bit is set to the right of the netmask, which no `cidr` may have. Its
    /// message and DETAIL differ from the generic `invalid input syntax`.
    #[error("invalid cidr value: \"{value}\"")]
    InvalidCidr { value: String },
    /// Like [`TypeError::Coded`], but carrying `PostgreSQL`'s HINT as well —
    /// the `macaddr8` → `macaddr` narrowing is the one type-layer error that
    /// spells out which values are eligible.
    #[error("{message}")]
    CodedWithHint {
        sqlstate: &'static str,
        message: String,
        hint: &'static str,
    },
    /// `json_in` / `jsonb_in`'s rejection of malformed JSON. Alone among the
    /// type-layer errors it carries a CONTEXT as well as a DETAIL, because
    /// `PostgreSQL` reports the offending token *and* an excerpt of the line it
    /// sits on, and both are per-value rather than per-variant. The message is
    /// `invalid input syntax for type json` for `jsonb` too — the two types
    /// share one lexer, so they share its complaints.
    #[error("{message}")]
    JsonSyntax {
        sqlstate: &'static str,
        message: &'static str,
        detail: String,
        context: String,
    },
    /// `xml_in` / `XMLPARSE`'s rejection of a malformed value (`2200M` for a
    /// document, `2200N` for content). The DETAIL is multi-line — libxml's
    /// complaint, the offending input line and a caret under the column, once
    /// per fault — because libxml keeps parsing after a recoverable error and
    /// `PostgreSQL` prints everything it reported.
    #[error("{message}")]
    XmlSyntax {
        sqlstate: &'static str,
        message: &'static str,
        detail: String,
    },
}

impl TypeError {
    /// The five-character SQLSTATE for this error.
    pub fn sqlstate(&self) -> &'static str {
        match self {
            TypeError::Overflow => "22003",
            TypeError::DivisionByZero => "22012",
            TypeError::InvalidText { .. } => "22P02",
            TypeError::TypeMismatch { .. } => "42804",
            TypeError::StringDataRightTruncation { .. } => "22001",
            TypeError::InvalidEscape | TypeError::InvalidEscapeString => "22025",
            TypeError::CannotCast { .. } => "42846",
            TypeError::Domain { sqlstate, .. } => sqlstate,
            TypeError::InvalidDatetimeFormat { .. } => "22007",
            TypeError::DatetimeFieldOverflow { .. } => "22008",
            TypeError::IntervalFieldOverflow { .. } => "22015",
            TypeError::DatetimeOutOfRange { .. } => "22008",
            TypeError::TimezoneDisplacementOverflow { .. } => "22009",
            TypeError::UnknownTimeZone { .. } => "22023",
            TypeError::FeatureNotSupported { .. } => "0A000",
            TypeError::OutOfRange { .. } => "22003",
            TypeError::Coded { sqlstate, .. } => sqlstate,
            TypeError::RangeMalformed { .. } => "22P02",
            TypeError::InvalidCidr { .. } => "22P02",
            TypeError::CodedWithHint { sqlstate, .. } => sqlstate,
            TypeError::JsonSyntax { sqlstate, .. } => sqlstate,
            TypeError::XmlSyntax { sqlstate, .. } => sqlstate,
        }
    }

    /// `PostgreSQL`'s DETAIL for this error. Borrowed where the wording is fixed
    /// per variant, owned where it names the offending value — `json_in`'s
    /// `Token "x" is invalid.` cannot be a `&'static str`.
    #[must_use]
    pub fn detail(&self) -> Option<std::borrow::Cow<'_, str>> {
        match self {
            TypeError::RangeMalformed { detail, .. } => Some(std::borrow::Cow::Borrowed(*detail)),
            TypeError::InvalidCidr { .. } => Some(std::borrow::Cow::Borrowed(
                "Value has bits set to right of mask.",
            )),
            TypeError::JsonSyntax { detail, .. } | TypeError::XmlSyntax { detail, .. } => {
                Some(std::borrow::Cow::Borrowed(detail.as_str()))
            }
            _ => None,
        }
    }

    /// `PostgreSQL`'s CONTEXT for this error — the excerpt of the input line the
    /// JSON lexer stopped on. No other type-layer error has one.
    #[must_use]
    pub fn context(&self) -> Option<&str> {
        match self {
            TypeError::JsonSyntax { context, .. } => Some(context),
            _ => None,
        }
    }

    /// `PostgreSQL`'s HINT for this error, when it has one.
    #[must_use]
    pub fn hint(&self) -> Option<&'static str> {
        match self {
            TypeError::CodedWithHint { hint, .. } => Some(hint),
            _ => None,
        }
    }

    /// `PostgreSQL`'s `2202E` array subscript error, the code every array
    /// dimension, bound, and slice-shape complaint carries.
    #[must_use]
    pub fn array_subscript(message: impl Into<String>) -> TypeError {
        TypeError::Coded {
            sqlstate: "2202E",
            message: message.into(),
        }
    }

    /// `smallint out of range` or `real out of range`: PostgreSQL's message for
    /// an arithmetic or narrowing result that leaves `type_name`'s range.
    #[must_use]
    pub fn out_of_range_for(type_name: &str) -> TypeError {
        TypeError::OutOfRange {
            message: format!("{type_name} out of range"),
        }
    }

    /// `value "32768" is out of range for type smallint`: PostgreSQL's message
    /// when an integer *input string* parses but does not fit.
    #[must_use]
    pub fn value_out_of_range(value: &str, type_name: &str) -> TypeError {
        TypeError::OutOfRange {
            message: format!("value \"{value}\" is out of range for type {type_name}"),
        }
    }

    /// `"1e39" is out of range for type real`: PostgreSQL's message when a
    /// float *input string* parses but overflows or underflows the type.
    #[must_use]
    pub fn float_text_out_of_range(value: &str, type_name: &str) -> TypeError {
        TypeError::OutOfRange {
            message: format!("\"{value}\" is out of range for type {type_name}"),
        }
    }

    /// `value out of range: overflow` or `: underflow`: PostgreSQL's message
    /// for a float *computation or cast* that leaves the target's range.
    #[must_use]
    pub fn float_overflow() -> TypeError {
        TypeError::OutOfRange {
            message: "value out of range: overflow".to_string(),
        }
    }

    /// The underflow companion of [`TypeError::float_overflow`].
    #[must_use]
    pub fn float_underflow() -> TypeError {
        TypeError::OutOfRange {
            message: "value out of range: underflow".to_string(),
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
        assert_eq!(
            TypeError::StringDataRightTruncation {
                type_name: "character varying(3)".into(),
            }
            .sqlstate(),
            "22001"
        );
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
        assert_eq!(
            TypeError::FeatureNotSupported {
                message: "multidimensional arrays are not supported".into(),
            }
            .sqlstate(),
            "0A000"
        );
    }
}
