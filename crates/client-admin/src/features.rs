//! Cluster feature administration.

use crabka_protocol::owned::update_features_request::{FeatureUpdateKey, UpdateFeaturesRequest};
use crabka_units::{Time, convert::TimeExt as _};

use crate::{AdminClient, AdminError, MetadataVersionUpdate, kafka_error_name};

const METADATA_VERSION_FEATURE: &str = "metadata.version";
const UPGRADE: i8 = 1;
const SAFE_DOWNGRADE: i8 = 2;

fn metadata_update_error(
    response: &crabka_protocol::owned::update_features_response::UpdateFeaturesResponse,
) -> Option<AdminError> {
    if response.error_code != 0 {
        return Some(AdminError::Broker {
            api: "UpdateFeatures",
            code: response.error_code,
            name: kafka_error_name(response.error_code),
            message: response.error_message.clone(),
        });
    }
    response
        .results
        .iter()
        .find(|result| result.feature == METADATA_VERSION_FEATURE && result.error_code != 0)
        .map(|error| AdminError::Broker {
            api: "UpdateFeatures",
            code: error.error_code,
            name: kafka_error_name(error.error_code),
            message: error.error_message.clone(),
        })
}

fn metadata_update_request(
    level: i16,
    safe_downgrade: bool,
    timeout: Time,
) -> UpdateFeaturesRequest {
    UpdateFeaturesRequest {
        timeout_ms: timeout.millis_i32(),
        feature_updates: vec![FeatureUpdateKey {
            feature: METADATA_VERSION_FEATURE.into(),
            max_version_level: level,
            allow_downgrade: safe_downgrade,
            upgrade_type: if safe_downgrade {
                SAFE_DOWNGRADE
            } else {
                UPGRADE
            },
            ..Default::default()
        }],
        ..Default::default()
    }
}

impl AdminClient {
    /// Finalize `metadata.version` through `UpdateFeatures`.
    ///
    /// # Errors
    /// Returns a transport, protocol, or broker error when Kafka rejects the
    /// requested level.
    pub async fn update_metadata_version(
        &mut self,
        level: i16,
        safe_downgrade: bool,
        timeout: Time,
    ) -> Result<MetadataVersionUpdate, AdminError> {
        let request = || metadata_update_request(level, safe_downgrade, timeout);
        let first = self.conn.send(request()).await?;
        if matches!(
            metadata_update_error(&first),
            Some(AdminError::Broker {
                code: crate::NOT_CONTROLLER,
                ..
            })
        ) {
            self.refresh_controller_connection().await?;
            let second = self.conn.send(request()).await?;
            if matches!(
                metadata_update_error(&second),
                Some(AdminError::Broker {
                    code: crate::NOT_CONTROLLER,
                    ..
                })
            ) {
                return Err(AdminError::NotControllerExhausted);
            }
            if let Some(error) = metadata_update_error(&second) {
                return Err(error);
            }
        } else if let Some(error) = metadata_update_error(&first) {
            return Err(error);
        }
        Ok(MetadataVersionUpdate { level })
    }
}

#[cfg(test)]
mod tests {
    use bytes::BytesMut;
    use crabka_protocol::{Decode, Encode};

    use super::*;

    #[test]
    fn metadata_update_selects_safe_downgrade() {
        let request = metadata_update_request(15, true, crabka_units::secs(30));
        let update = &request.feature_updates[0];

        assert2::assert!(request.timeout_ms == 30_000);
        assert2::assert!(update.allow_downgrade);
        assert2::assert!(update.upgrade_type == 2);
        assert2::assert!(update.max_version_level == 15);
    }

    #[test]
    fn metadata_safe_downgrade_survives_v0_encoding() {
        let request = metadata_update_request(15, true, crabka_units::secs(30));
        let mut bytes = BytesMut::new();
        request.encode(&mut bytes, 0).unwrap();

        let mut encoded = bytes.freeze();
        let decoded = UpdateFeaturesRequest::decode(&mut encoded, 0).unwrap();
        assert2::assert!(decoded.feature_updates[0].allow_downgrade);
    }

    #[test]
    fn metadata_update_surfaces_row_error_from_v0_or_v1() {
        use crabka_protocol::owned::update_features_response::{
            UpdatableFeatureResult, UpdateFeaturesResponse,
        };

        let response = UpdateFeaturesResponse {
            results: vec![UpdatableFeatureResult {
                feature: METADATA_VERSION_FEATURE.into(),
                error_code: 95,
                error_message: Some("not supported by every broker".into()),
                ..Default::default()
            }],
            ..Default::default()
        };

        assert2::assert!(matches!(
            metadata_update_error(&response),
            Some(AdminError::Broker { code: 95, .. })
        ));
    }

    #[test]
    fn metadata_update_recognizes_not_controller_for_retry() {
        let response = crabka_protocol::owned::update_features_response::UpdateFeaturesResponse {
            error_code: crate::NOT_CONTROLLER,
            ..Default::default()
        };

        assert2::assert!(matches!(
            metadata_update_error(&response),
            Some(AdminError::Broker {
                code: crate::NOT_CONTROLLER,
                ..
            })
        ));
    }
}
