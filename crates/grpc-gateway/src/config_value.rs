//! Validated scalar values accepted at gateway configuration boundaries.

use std::str::FromStr;

use refined_type::rule::{
    GreaterEqualI64, GreaterI16, GreaterI32, GreaterI64, GreaterU32, GreaterU64, MinMaxU32,
};

macro_rules! refined_integer {
    (
        $(#[$meta:meta])*
        $name:ident($primitive:ty) => $rule:ty
    ) => {
        $(#[$meta])*
        #[derive(Clone, Copy, Debug, Eq, PartialEq)]
        pub struct $name($primitive);

        impl $name {
            /// Validate and construct the value.
            ///
            /// # Errors
            ///
            /// Returns an error when `value` violates the documented range.
            pub fn new(value: $primitive) -> Result<Self, String> {
                <$rule>::new(value)
                    .map(|value| Self(value.into_value()))
                    .map_err(|error| error.to_string())
            }

            /// Consume the validated value.
            #[must_use]
            pub const fn into_value(self) -> $primitive {
                self.0
            }
        }

        impl FromStr for $name {
            type Err = String;

            fn from_str(value: &str) -> Result<Self, Self::Err> {
                value
                    .parse()
                    .map_err(|error: std::num::ParseIntError| error.to_string())
                    .and_then(Self::new)
            }
        }
    };
}

refined_integer!(
    /// A 64-bit unsigned integer greater than zero.
    PositiveU64(u64) => GreaterU64<0>
);
refined_integer!(
    /// A 64-bit signed integer greater than zero.
    PositiveI64(i64) => GreaterI64<0>
);
refined_integer!(
    /// A 32-bit signed integer greater than zero.
    PositiveI32(i32) => GreaterI32<0>
);
refined_integer!(
    /// A 16-bit signed integer greater than zero.
    PositiveI16(i16) => GreaterI16<0>
);
refined_integer!(
    /// A 32-bit unsigned integer greater than zero.
    PositiveU32(u32) => GreaterU32<0>
);
refined_integer!(
    /// A 64-bit signed integer greater than or equal to zero.
    NonNegativeI64(i64) => GreaterEqualI64<0>
);
refined_integer!(
    /// A partition count representable by Kafka's signed 32-bit field.
    PartitionCount(u32) => MinMaxU32<1, 2_147_483_647>
);
refined_integer!(
    /// A ratio expressed as inclusive basis points from zero through 10,000.
    DirtyRatioBasisPoints(u32) => MinMaxU32<0, 10_000>
);

#[cfg(test)]
mod tests {
    use assert2::assert;

    use super::*;

    #[test]
    fn refined_integer_boundaries() {
        assert!(PositiveU64::new(0).is_err());
        assert!(PositiveU64::new(1).is_ok());
        assert!(PositiveI64::new(0).is_err());
        assert!(PositiveI64::new(1).is_ok());
        assert!(PositiveI32::new(0).is_err());
        assert!(PositiveI32::new(1).is_ok());
        assert!(PositiveI16::new(0).is_err());
        assert!(PositiveI16::new(1).is_ok());
        assert!(PositiveU32::new(0).is_err());
        assert!(PositiveU32::new(1).is_ok());
        assert!(NonNegativeI64::new(-1).is_err());
        assert!(NonNegativeI64::new(0).is_ok());
        assert!(PartitionCount::new(0).is_err());
        assert!(PartitionCount::new(2_147_483_647).is_ok());
        assert!(PartitionCount::new(2_147_483_648).is_err());
        assert!(DirtyRatioBasisPoints::new(10_000).is_ok());
        assert!(DirtyRatioBasisPoints::new(10_001).is_err());
    }

    #[test]
    fn refined_integer_from_str_is_checked() {
        assert!("1".parse::<PositiveU64>().is_ok());
        assert!("0".parse::<PositiveU64>().is_err());
        assert!("not-an-integer".parse::<PositiveU64>().is_err());
    }
}
