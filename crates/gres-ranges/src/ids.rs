use std::{fmt, str::FromStr};

use derive_more::{Display, From, FromStr, Into};
use serde::{Deserialize, Serialize};
use thiserror::Error;

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize)]
#[serde(transparent)]
pub struct TenantName(String);

#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub enum TenantNameError {
    #[error("tenant name is empty")]
    Empty,

    #[error("tenant name contains an invalid character: {invalid}")]
    InvalidCharacter { invalid: char },
}

impl TenantName {
    #[must_use]
    pub fn as_str(&self) -> &str {
        self.0.as_str()
    }

    pub fn parse(value: impl Into<String>) -> Result<Self, TenantNameError> {
        let value = value.into();

        if value.is_empty() {
            return Err(TenantNameError::Empty);
        }

        if let Some(invalid) = value.chars().find(|character| !is_topic_safe(*character)) {
            return Err(TenantNameError::InvalidCharacter { invalid });
        }

        Ok(Self(value))
    }
}

impl fmt::Display for TenantName {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(self.as_str())
    }
}

impl FromStr for TenantName {
    type Err = TenantNameError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        Self::parse(value)
    }
}

impl<'de> Deserialize<'de> for TenantName {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let value = String::deserialize(deserializer)?;
        Self::parse(value).map_err(serde::de::Error::custom)
    }
}

#[derive(
    Debug,
    Clone,
    Copy,
    PartialEq,
    Eq,
    PartialOrd,
    Ord,
    Hash,
    Display,
    From,
    Into,
    FromStr,
    Serialize,
    Deserialize,
)]
#[serde(transparent)]
pub struct RangeId(u32);

impl RangeId {
    pub const COORDINATOR: Self = Self(0);

    #[must_use]
    pub const fn new(value: u32) -> Self {
        Self(value)
    }

    #[must_use]
    pub const fn as_u32(self) -> u32 {
        self.0
    }

    #[must_use]
    pub const fn is_coordinator(self) -> bool {
        self.0 == Self::COORDINATOR.0
    }
}

#[derive(
    Debug,
    Clone,
    Copy,
    PartialEq,
    Eq,
    PartialOrd,
    Ord,
    Hash,
    Display,
    From,
    Into,
    FromStr,
    Serialize,
    Deserialize,
)]
#[serde(transparent)]
pub struct TableId(u64);

impl TableId {
    pub const ZERO: Self = Self(0);

    #[must_use]
    pub const fn new(value: u64) -> Self {
        Self(value)
    }

    #[must_use]
    pub const fn as_u64(self) -> u64 {
        self.0
    }
}

#[derive(
    Debug,
    Clone,
    Copy,
    PartialEq,
    Eq,
    PartialOrd,
    Ord,
    Hash,
    Display,
    From,
    Into,
    FromStr,
    Serialize,
    Deserialize,
)]
#[serde(transparent)]
pub struct ShardId(u32);

impl ShardId {
    pub const ZERO: Self = Self(0);

    #[must_use]
    pub const fn new(value: u32) -> Self {
        Self(value)
    }
}

#[derive(
    Debug,
    Clone,
    Copy,
    PartialEq,
    Eq,
    PartialOrd,
    Ord,
    Hash,
    Display,
    From,
    Into,
    FromStr,
    Serialize,
    Deserialize,
)]
#[serde(transparent)]
pub struct MapEpoch(u64);

impl MapEpoch {
    pub const ZERO: Self = Self(0);

    #[must_use]
    pub const fn new(value: u64) -> Self {
        Self(value)
    }

    #[must_use]
    pub const fn as_u64(self) -> u64 {
        self.0
    }
}

#[derive(
    Debug,
    Clone,
    Copy,
    PartialEq,
    Eq,
    PartialOrd,
    Ord,
    Hash,
    Display,
    From,
    Into,
    FromStr,
    Serialize,
    Deserialize,
)]
#[serde(transparent)]
pub struct KeyHash(u64);

impl KeyHash {
    #[must_use]
    pub const fn new(value: u64) -> Self {
        Self(value)
    }
}

const fn is_topic_safe(character: char) -> bool {
    character.is_ascii_alphanumeric() || matches!(character, '-' | '_')
}

#[cfg(test)]
mod tests {
    use assert2::assert;

    use super::*;

    #[test]
    fn tenant_name_rejects_empty_names() {
        assert!(TenantName::parse("").is_err());
    }

    #[test]
    fn tenant_name_rejects_separator_characters() {
        assert!(TenantName::parse("tenant.with.dot").is_err());
    }
}
