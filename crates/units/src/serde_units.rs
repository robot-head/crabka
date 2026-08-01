//! `serde` adapters, so a config or API struct can hold quantities.
//!
//! `uom`'s own `Serialize`/`Deserialize` encodes the raw base-unit float — a
//! timeout as `30.0`, a size as `536870912.0` — which is neither what an operator
//! writes in YAML nor what an admin API should return. These modules are used
//! through `#[serde(with = ...)]` and give a choice of two encodings:
//!
//! - [`human`] — the operator-facing string form (`"512MiB"`, `"30s"`,
//!   `"10MiB/s"`), for config files.
//! - [`numeric`] — an exact integer in a named unit (`30000` milliseconds,
//!   `536870912` bytes), for JSON APIs and anything mirroring a Kafka wire field.
//!
//! ```
//! use crabka_units::{prelude::*, serde_units};
//! use serde::{Deserialize, Serialize};
//!
//! #[derive(Serialize, Deserialize)]
//! struct TopicConfig {
//!     #[serde(with = "serde_units::human::byte_size")]
//!     segment_size: ByteSize,
//!     #[serde(with = "serde_units::numeric::millis_i64")]
//!     retention: Time,
//! }
//!
//! let parsed: TopicConfig =
//!     serde_json::from_str(r#"{"segment_size":"512MiB","retention":604800000}"#)?;
//! assert_eq!(parsed.segment_size, mebibytes(512));
//! assert_eq!(parsed.retention, days(7));
//! # Ok::<_, serde_json::Error>(())
//! ```
//!
//! A dimensioned field in [`human`] form must carry its unit: a bare number is
//! rejected rather than assumed to be seconds or bytes, since guessing is the
//! failure this crate exists to prevent. [`human::ratio`] is the exception — a
//! fraction's unit is "none", so `0.25` is accepted alongside `"25%"`.

/// The operator-facing string encoding: `"512MiB"`, `"30s"`, `"10MiB/s"`, `"25%"`.
pub mod human {
    /// Defines a `#[serde(with = ...)]` module over a quantity's human form, plus
    /// an `option_`-prefixed sibling for `Option` fields.
    macro_rules! human_module {
        ($(#[$meta:meta])* $name:ident / $option_name:ident, $quantity:ty, $parse:path) => {
            $(#[$meta])*
            pub mod $name {
                use serde::de::{Deserialize as _, Error as _};
                use serde::{Deserializer, Serializer};

                use crate::fmt::Human as _;

                /// Writes the quantity as its human string form.
                ///
                /// # Errors
                ///
                /// Whatever the serializer reports for a string.
                pub fn serialize<S: Serializer>(
                    value: &$quantity,
                    serializer: S,
                ) -> Result<S::Ok, S::Error> {
                    serializer.collect_str(&value.human())
                }

                /// Reads the quantity from its human string form.
                ///
                /// # Errors
                ///
                /// If the value is not a string, or not a quantity of this
                /// dimension with an explicit unit.
                pub fn deserialize<'de, D: Deserializer<'de>>(
                    deserializer: D,
                ) -> Result<$quantity, D::Error> {
                    let raw = String::deserialize(deserializer)?;
                    $parse(&raw).map_err(D::Error::custom)
                }
            }

            $(#[$meta])*
            ///
            /// The `Option` form of the sibling module: `null` is `None`.
            pub mod $option_name {
                use serde::de::{Deserialize as _, Error as _};
                use serde::{Deserializer, Serializer};

                use crate::fmt::Human as _;

                /// Writes the quantity as its human string form, or `null`.
                ///
                /// # Errors
                ///
                /// Whatever the serializer reports for an optional string.
                pub fn serialize<S: Serializer>(
                    value: &Option<$quantity>,
                    serializer: S,
                ) -> Result<S::Ok, S::Error> {
                    match value {
                        Some(value) => serializer.collect_str(&value.human()),
                        None => serializer.serialize_none(),
                    }
                }

                /// Reads the quantity from its human string form, or `null`.
                ///
                /// # Errors
                ///
                /// If the value is neither `null` nor a string holding a quantity
                /// of this dimension with an explicit unit.
                pub fn deserialize<'de, D: Deserializer<'de>>(
                    deserializer: D,
                ) -> Result<Option<$quantity>, D::Error> {
                    Option::<String>::deserialize(deserializer)?
                        .map(|raw| $parse(&raw).map_err(D::Error::custom))
                        .transpose()
                }
            }
        };
    }

    human_module!(
        /// A byte count as `"512MiB"`.
        byte_size / option_byte_size,
        crate::ByteSize,
        crate::parse::byte_size
    );
    human_module!(
        /// A time extent as `"30s"`.
        time / option_time,
        crate::Time,
        crate::parse::time
    );
    human_module!(
        /// A byte throughput as `"10MiB/s"`.
        byte_rate / option_byte_rate,
        crate::ByteRate,
        crate::parse::byte_rate
    );
    human_module!(
        /// An event rate as `"100/s"`.
        frequency / option_frequency,
        crate::Frequency,
        crate::parse::frequency
    );

    /// A dimensionless fraction as `"25%"` or `0.25`.
    pub mod ratio {
        use serde::{
            Deserializer, Serializer,
            de::{Deserialize as _, Error as _, Unexpected},
        };

        use crate::{Ratio, fmt::Human as _, fraction};

        /// Writes the fraction as a percentage string.
        ///
        /// # Errors
        ///
        /// Whatever the serializer reports for a string.
        pub fn serialize<S: Serializer>(value: &Ratio, serializer: S) -> Result<S::Ok, S::Error> {
            serializer.collect_str(&value.human())
        }

        /// Reads the fraction from a percentage string or a bare number.
        ///
        /// # Errors
        ///
        /// If the value is neither a number nor a string holding a fraction.
        pub fn deserialize<'de, D: Deserializer<'de>>(deserializer: D) -> Result<Ratio, D::Error> {
            match Encoded::deserialize(deserializer)? {
                Encoded::Text(raw) => crate::parse::ratio(&raw).map_err(D::Error::custom),
                Encoded::Number(value) if value.is_finite() => Ok(fraction(value)),
                Encoded::Number(value) => Err(D::Error::invalid_value(
                    Unexpected::Float(value),
                    &"a finite fraction",
                )),
            }
        }

        /// Either encoding a fraction may arrive in.
        #[derive(serde::Deserialize)]
        #[serde(untagged)]
        pub(super) enum Encoded {
            /// `"25%"`.
            Text(String),
            /// `0.25`.
            Number(f64),
        }
    }

    /// A dimensionless fraction as `"25%"` or `0.25`, or `null`.
    pub mod option_ratio {
        use serde::{
            Deserializer, Serializer,
            de::{Deserialize as _, Error as _, Unexpected},
        };

        use super::ratio::Encoded;
        use crate::{Ratio, fmt::Human as _, fraction};

        /// Writes the fraction as a percentage string, or `null`.
        ///
        /// # Errors
        ///
        /// Whatever the serializer reports for an optional string.
        pub fn serialize<S: Serializer>(
            value: &Option<Ratio>,
            serializer: S,
        ) -> Result<S::Ok, S::Error> {
            match value {
                Some(value) => serializer.collect_str(&value.human()),
                None => serializer.serialize_none(),
            }
        }

        /// Reads the fraction from a percentage string, a bare number, or `null`.
        ///
        /// # Errors
        ///
        /// If the value is neither `null` nor a number nor a string holding a
        /// fraction.
        pub fn deserialize<'de, D: Deserializer<'de>>(
            deserializer: D,
        ) -> Result<Option<Ratio>, D::Error> {
            match Option::<Encoded>::deserialize(deserializer)? {
                None => Ok(None),
                Some(Encoded::Text(raw)) => crate::parse::ratio(&raw)
                    .map(Some)
                    .map_err(D::Error::custom),
                Some(Encoded::Number(value)) if value.is_finite() => Ok(Some(fraction(value))),
                Some(Encoded::Number(value)) => Err(D::Error::invalid_value(
                    Unexpected::Float(value),
                    &"a finite fraction",
                )),
            }
        }
    }
}

/// The exact integer encoding, in an explicitly named unit.
pub mod numeric {
    /// Defines a `#[serde(with = ...)]` module encoding a quantity as an integer
    /// in one named unit, plus an `option_`-prefixed sibling.
    ///
    /// `$into` and `$from` are named as fully-qualified trait-method paths, so the
    /// generated modules need no trait imports.
    macro_rules! numeric_module {
        (
            $(#[$meta:meta])*
            $name:ident / $option_name:ident,
            $quantity:ty, $raw:ty, $into:path, $from:path
        ) => {
            $(#[$meta])*
            pub mod $name {
                use serde::de::Deserialize as _;
                use serde::{Deserializer, Serialize as _, Serializer};

                /// Writes the quantity as an integer in this module's unit.
                ///
                /// # Errors
                ///
                /// Whatever the serializer reports for an integer.
                pub fn serialize<S: Serializer>(
                    value: &$quantity,
                    serializer: S,
                ) -> Result<S::Ok, S::Error> {
                    let raw: $raw = $into(*value);
                    raw.serialize(serializer)
                }

                /// Reads the quantity from an integer in this module's unit.
                ///
                /// # Errors
                ///
                /// If the value is not an integer of the underlying width.
                pub fn deserialize<'de, D: Deserializer<'de>>(
                    deserializer: D,
                ) -> Result<$quantity, D::Error> {
                    Ok($from(<$raw>::deserialize(deserializer)?))
                }
            }

            $(#[$meta])*
            ///
            /// The `Option` form of the sibling module: `null` is `None`.
            pub mod $option_name {
                use serde::de::Deserialize as _;
                use serde::{Deserializer, Serialize as _, Serializer};

                /// Writes the quantity as an integer in this module's unit, or `null`.
                ///
                /// # Errors
                ///
                /// Whatever the serializer reports for an optional integer.
                pub fn serialize<S: Serializer>(
                    value: &Option<$quantity>,
                    serializer: S,
                ) -> Result<S::Ok, S::Error> {
                    value.map($into).serialize(serializer)
                }

                /// Reads the quantity from an integer in this module's unit, or `null`.
                ///
                /// # Errors
                ///
                /// If the value is neither `null` nor an integer of the underlying width.
                pub fn deserialize<'de, D: Deserializer<'de>>(
                    deserializer: D,
                ) -> Result<Option<$quantity>, D::Error> {
                    Ok(Option::<$raw>::deserialize(deserializer)?.map($from))
                }
            }
        };
    }

    numeric_module!(
        /// A time extent as whole milliseconds — Kafka's unit for retention,
        /// timeout, and expiry fields.
        millis_i64 / option_millis_i64,
        crate::Time,
        i64,
        crate::convert::TimeExt::millis_i64,
        crate::convert::TimeExt::from_millis
    );
    numeric_module!(
        /// A time extent as whole milliseconds, **truncated** on the way out.
        ///
        /// For mirroring an external format that integer-divides rather than
        /// rounds, such as Tempo's `durationMs`. Reading is exact, so this is
        /// deliberately asymmetric — see
        /// [`TimeExt::millis_i64_trunc`](crate::convert::TimeExt::millis_i64_trunc).
        millis_i64_trunc / option_millis_i64_trunc,
        crate::Time,
        i64,
        crate::convert::TimeExt::millis_i64_trunc,
        crate::convert::TimeExt::from_millis
    );
    numeric_module!(
        /// A time extent as whole seconds.
        secs_i64 / option_secs_i64,
        crate::Time,
        i64,
        crate::convert::TimeExt::secs_i64,
        crate::convert::TimeExt::from_secs
    );
    numeric_module!(
        /// A time extent as whole nanoseconds.
        nanos_i64 / option_nanos_i64,
        crate::Time,
        i64,
        crate::convert::TimeExt::nanos_i64,
        crate::convert::TimeExt::from_nanos
    );
    numeric_module!(
        /// A byte count as an unsigned total.
        bytes_u64 / option_bytes_u64,
        crate::ByteSize,
        u64,
        crate::convert::ByteSizeExt::bytes_u64,
        crate::convert::ByteSizeExt::from_bytes
    );
    numeric_module!(
        /// A byte count as Kafka's `int64` byte fields.
        bytes_i64 / option_bytes_i64,
        crate::ByteSize,
        i64,
        crate::convert::ByteSizeExt::bytes_i64,
        crate::convert::ByteSizeExt::from_bytes_i64
    );
    numeric_module!(
        /// A byte throughput as Kafka's `int64` quota fields.
        bytes_per_sec_i64 / option_bytes_per_sec_i64,
        crate::ByteRate,
        i64,
        crate::convert::ByteRateExt::bytes_per_sec_i64,
        crate::convert::ByteRateExt::from_bytes_per_sec
    );
}
