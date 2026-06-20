//! The 5-part profile-type string carried as the `__profile_type__` label.

use std::fmt;

use crate::error::ProfileError;

/// The profile type `name:sample_type:sample_unit:period_type:period_unit`.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ProfileType {
    pub name: String,
    pub sample_type: String,
    pub sample_unit: String,
    pub period_type: String,
    pub period_unit: String,
}

impl ProfileType {
    /// Parse exactly five colon-separated non-empty parts.
    pub fn parse(input: &str) -> Result<Self, ProfileError> {
        let parts: Vec<&str> = input.split(':').collect();
        if parts.len() != 5 || parts.iter().any(|part| part.is_empty()) {
            return Err(ProfileError::Decode(format!(
                "invalid profile_type {input:?}: expected name:sample_type:sample_unit:period_type:period_unit"
            )));
        }
        Ok(Self {
            name: parts[0].to_string(),
            sample_type: parts[1].to_string(),
            sample_unit: parts[2].to_string(),
            period_type: parts[3].to_string(),
            period_unit: parts[4].to_string(),
        })
    }
}

impl fmt::Display for ProfileType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "{}:{}:{}:{}:{}",
            self.name, self.sample_type, self.sample_unit, self.period_type, self.period_unit
        )
    }
}

#[cfg(test)]
mod tests {
    use assert2::assert;

    use super::*;

    #[test]
    fn parses_go_cpu() {
        let pt = ProfileType::parse("process_cpu:cpu:nanoseconds:cpu:nanoseconds").unwrap();
        assert!(pt.name == "process_cpu");
        assert!(pt.sample_type == "cpu");
        assert!(pt.sample_unit == "nanoseconds");
        assert!(pt.period_type == "cpu");
        assert!(pt.period_unit == "nanoseconds");
    }

    #[test]
    fn display_round_trips() {
        for input in [
            "process_cpu:cpu:nanoseconds:cpu:nanoseconds",
            "memory:alloc_space:bytes:space:bytes",
            "wall:wall:nanoseconds:wall:nanoseconds",
        ] {
            let pt = ProfileType::parse(input).unwrap();
            assert!(format!("{pt}") == input);
        }
    }

    #[test]
    fn rejects_wrong_part_count() {
        assert!(ProfileType::parse("a:b:c:d").is_err());
        assert!(ProfileType::parse("a:b:c:d:e:f").is_err());
        assert!(ProfileType::parse("a:b::d:e").is_err());
    }
}
