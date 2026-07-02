//! The profile-type string carried as the `__profile_type__` label.

use std::fmt;

use crate::error::ProfileError;

/// The profile type `name:sample_type:sample_unit:period_type:period_unit[:delta]`.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ProfileType {
    pub name: String,
    pub sample_type: String,
    pub sample_unit: String,
    pub period_type: String,
    pub period_unit: String,
    pub delta: bool,
}

impl ProfileType {
    /// Parse five colon-separated non-empty parts plus an optional `:delta` suffix.
    pub fn parse(input: &str) -> Result<Self, ProfileError> {
        let parts: Vec<&str> = input.split(':').collect();
        let delta = matches!(parts.as_slice(), [_, _, _, _, _, "delta"]);
        if !(parts.len() == 5 || delta) || parts.iter().any(|part| part.is_empty()) {
            return Err(ProfileError::Decode(format!(
                "invalid profile_type {input:?}: expected name:sample_type:sample_unit:period_type:period_unit[:delta]"
            )));
        }
        Ok(Self {
            name: parts[0].to_string(),
            sample_type: parts[1].to_string(),
            sample_unit: parts[2].to_string(),
            period_type: parts[3].to_string(),
            period_unit: parts[4].to_string(),
            delta,
        })
    }
}

impl fmt::Display for ProfileType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "{}:{}:{}:{}:{}",
            self.name, self.sample_type, self.sample_unit, self.period_type, self.period_unit
        )?;
        if self.delta {
            f.write_str(":delta")?;
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use assert2::assert;

    use super::*;

    #[test]
    fn parses_go_cpu() {
        let pt = ProfileType::parse("process_cpu:cpu:nanoseconds:cpu:nanoseconds").unwrap();
        assert!(
            pt == ProfileType {
                name: "process_cpu".to_string(),
                sample_type: "cpu".to_string(),
                sample_unit: "nanoseconds".to_string(),
                period_type: "cpu".to_string(),
                period_unit: "nanoseconds".to_string(),
                delta: false,
            }
        );
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
    fn parses_optional_delta_suffix() {
        let input = "process_cpu:cpu:nanoseconds:cpu:nanoseconds:delta";
        let pt = ProfileType::parse(input).unwrap();

        assert!(
            pt == ProfileType {
                name: "process_cpu".to_string(),
                sample_type: "cpu".to_string(),
                sample_unit: "nanoseconds".to_string(),
                period_type: "cpu".to_string(),
                period_unit: "nanoseconds".to_string(),
                delta: true,
            }
        );
        assert!(format!("{pt}") == input);
    }

    #[test]
    fn rejects_wrong_part_count() {
        for input in ["a:b:c:d", "a:b:c:d:e:f", "a:b::d:e", "a:b:c:d:e:cumulative"] {
            assert!(ProfileType::parse(input).is_err(), "{input}");
        }
    }
}
