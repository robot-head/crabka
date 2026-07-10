//! pprof wire model wrapper.

use prost::Message;

use crate::ProfileError;

/// A decoded perftools.profiles profile.
#[derive(Clone, Debug, PartialEq)]
pub struct PprofProfile {
    inner: crate::proto::Profile,
}

impl PprofProfile {
    pub fn decode(bytes: &[u8]) -> Result<Self, ProfileError> {
        crate::proto::Profile::decode(bytes)
            .map(|inner| Self { inner })
            .map_err(ProfileError::from)
    }

    #[must_use]
    pub fn encode(&self) -> Vec<u8> {
        self.inner.encode_to_vec()
    }

    #[must_use]
    pub fn inner(&self) -> &crate::proto::Profile {
        &self.inner
    }

    #[must_use]
    pub fn into_inner(self) -> crate::proto::Profile {
        self.inner
    }

    #[must_use]
    pub fn string(&self, index: i64) -> Option<&str> {
        usize::try_from(index)
            .ok()
            .and_then(|idx| self.inner.string_table.get(idx))
            .map(String::as_str)
    }

    #[must_use]
    pub fn sample_types(&self) -> Vec<(String, String)> {
        self.inner
            .sample_type
            .iter()
            .map(|sample_type| {
                (
                    self.string(sample_type.r#type).unwrap_or("").to_string(),
                    self.string(sample_type.unit).unwrap_or("").to_string(),
                )
            })
            .collect()
    }

    #[must_use]
    pub fn period_type_strings(&self) -> (String, String) {
        self.inner.period_type.map_or_else(
            || (String::new(), String::new()),
            |period_type| {
                (
                    self.string(period_type.r#type).unwrap_or("").to_string(),
                    self.string(period_type.unit).unwrap_or("").to_string(),
                )
            },
        )
    }

    #[must_use]
    pub fn samples(&self) -> &[crate::proto::Sample] {
        &self.inner.sample
    }
}

impl From<crate::proto::Profile> for PprofProfile {
    fn from(inner: crate::proto::Profile) -> Self {
        Self { inner }
    }
}

impl From<PprofProfile> for crate::proto::Profile {
    fn from(profile: PprofProfile) -> Self {
        profile.inner
    }
}

#[cfg(test)]
mod tests {
    use assert2::{assert, check};
    use prost::Message;

    use super::*;

    fn sample_pprof() -> crate::proto::Profile {
        crate::proto::Profile {
            sample_type: vec![crate::proto::ValueType { r#type: 1, unit: 2 }],
            period_type: Some(crate::proto::ValueType { r#type: 3, unit: 4 }),
            sample: vec![crate::proto::Sample {
                location_id: vec![1],
                value: vec![42],
                label: Vec::new(),
            }],
            string_table: vec![
                String::new(),
                "cpu".to_string(),
                "nanoseconds".to_string(),
                "wall".to_string(),
                "milliseconds".to_string(),
            ],
            ..Default::default()
        }
    }

    #[test]
    fn decode_and_encode_round_trip_profile_bytes() {
        let bytes = sample_pprof().encode_to_vec();
        let profile = PprofProfile::decode(&bytes).unwrap();

        assert!(profile.inner() == &sample_pprof());
        assert!(
            crate::proto::Profile::decode(profile.encode().as_slice()).unwrap() == sample_pprof()
        );
    }

    #[test]
    fn invalid_bytes_report_decode_error() {
        let error = PprofProfile::decode(&[0xff]).unwrap_err();

        assert!(matches!(error, ProfileError::Decode(_)));
    }

    #[test]
    fn from_conversions_preserve_inner_profile() {
        let inner = sample_pprof();
        let profile = PprofProfile::from(inner.clone());

        assert!(crate::proto::Profile::from(profile) == inner);
    }

    #[test]
    fn accessors_return_decoded_profile_contents() {
        let inner = sample_pprof();
        let profile = PprofProfile::from(inner.clone());

        assert_eq!(profile.string(1), Some("cpu"));
        assert_eq!(profile.string(-1), None);
        assert_eq!(profile.string(99), None);
        assert_eq!(
            profile.sample_types(),
            vec![("cpu".to_string(), "nanoseconds".to_string())]
        );
        assert_eq!(
            profile.period_type_strings(),
            ("wall".to_string(), "milliseconds".to_string())
        );
        assert_eq!(
            profile.samples(),
            &[crate::proto::Sample {
                location_id: vec![1],
                value: vec![42],
                label: Vec::new(),
            }][..]
        );
        check!(profile.into_inner() == inner);
    }
}
