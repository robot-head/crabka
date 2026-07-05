//! `DescribeUserScramCredentials` (`api_key` 50, KIP-554 read half).

use bytes::Bytes;
use crabka_metadata::{MetadataImage, ResourceType};
use crabka_protocol::{
    Encode,
    owned::{
        describe_user_scram_credentials_request::DescribeUserScramCredentialsRequest,
        describe_user_scram_credentials_response::{
            CredentialInfo, DescribeUserScramCredentialsResponse,
            DescribeUserScramCredentialsResult,
        },
    },
};
use crabka_security::SaslMechanism;

use crate::authorizer::{AuthorizationRequest, AuthorizationResult};
use crate::broker::Broker;
use crate::codes::{CLUSTER_AUTHORIZATION_FAILED, DESCRIBE_USER_SCRAM_RESOURCE_NOT_FOUND};

#[allow(clippy::unused_async)]
#[tracing::instrument(
    name = "handle_describe_user_scram_credentials",
    level = "info",
    skip_all,
    fields(api = "DescribeUserScramCredentials"),
    err
)]
pub(crate) async fn handle(
    broker: &Broker,
    req: DescribeUserScramCredentialsRequest,
    ctx: &crate::handlers::RequestContext<'_>,
    api_version: i16,
) -> Result<Bytes, crate::error::BrokerError> {
    let image = broker.controller.current_image();

    let allow = broker.config.authorizer.authorize(
        &*image,
        &AuthorizationRequest {
            principal: ctx.principal,
            host: ctx.peer,
            resource_type: ResourceType::Cluster,
            resource_name: crate::handlers::acl_wire::CLUSTER_RESOURCE_NAME,
            operation: crabka_metadata::AclOperation::Alter,
        },
    );
    if matches!(allow, AuthorizationResult::Deny) {
        let resp = DescribeUserScramCredentialsResponse {
            throttle_time_ms: 0,
            error_code: CLUSTER_AUTHORIZATION_FAILED,
            error_message: Some("describe-user-scram-credentials denied".into()),
            results: vec![],
            ..Default::default()
        };
        return encode_response(&resp, api_version);
    }

    let known_users: std::collections::HashSet<String> =
        image.scram_credentials_users().into_iter().collect();
    let targets: Vec<String> = match req.users.as_deref() {
        None | Some([]) => {
            let mut v: Vec<String> = known_users.iter().cloned().collect();
            v.sort();
            v
        }
        Some(filter) => filter.iter().map(|u| u.name.clone()).collect(),
    };

    let results = build_results(&image, &known_users, targets);

    let resp = DescribeUserScramCredentialsResponse {
        throttle_time_ms: 0,
        error_code: 0,
        error_message: None,
        results,
        ..Default::default()
    };
    encode_response(&resp, api_version)
}

fn build_results(
    image: &MetadataImage,
    known_users: &std::collections::HashSet<String>,
    targets: Vec<String>,
) -> Vec<DescribeUserScramCredentialsResult> {
    targets
        .into_iter()
        .map(|user| {
            let pairs = image.scram_credentials_for_user(&user);
            if pairs.is_empty() && !known_users.contains(&user) {
                DescribeUserScramCredentialsResult {
                    user,
                    error_code: DESCRIBE_USER_SCRAM_RESOURCE_NOT_FOUND,
                    error_message: Some("no such SCRAM user".into()),
                    credential_infos: vec![],
                    ..Default::default()
                }
            } else {
                let credential_infos: Vec<CredentialInfo> = pairs
                    .into_iter()
                    .map(|(mech, iters)| CredentialInfo {
                        mechanism: sasl_mechanism_to_byte(mech),
                        iterations: iters.cast_signed(),
                        ..Default::default()
                    })
                    .collect();
                DescribeUserScramCredentialsResult {
                    user,
                    error_code: 0,
                    error_message: None,
                    credential_infos,
                    ..Default::default()
                }
            }
        })
        .collect()
}

#[must_use]
fn sasl_mechanism_to_byte(m: SaslMechanism) -> i8 {
    match m {
        SaslMechanism::ScramSha256 => 1,
        SaslMechanism::ScramSha512 => 2,
        // Non-SCRAM mechanisms never own SCRAM credential records; map to the
        // KIP-554 UNKNOWN sentinel (0).
        SaslMechanism::Plain | SaslMechanism::OAuthBearer | SaslMechanism::Gssapi => 0,
    }
}

fn encode_response<R: Encode>(
    resp: &R,
    api_version: i16,
) -> Result<Bytes, crate::error::BrokerError> {
    let mut body = Vec::new();
    resp.encode(&mut body, api_version).map_err(|e| {
        crate::error::BrokerError::Replication(format!("encode DescribeUserScramCredentials: {e}"))
    })?;
    Ok(Bytes::from(body))
}

#[cfg(test)]
mod tests {
    use assert2::assert;
    use crabka_metadata::{MetadataRecord, ScramCredentialRecord};
    use crabka_protocol::UnknownTaggedFields;

    use super::*;

    fn img_with_scram(users: &[(&str, SaslMechanism, u32)]) -> MetadataImage {
        let mut img = MetadataImage::new(uuid::Uuid::nil());
        for (user, mech, iters) in users {
            img.apply(&MetadataRecord::V1ScramCredential(ScramCredentialRecord {
                user: (*user).into(),
                mechanism: *mech,
                iterations: *iters,
                salt: vec![1, 2, 3],
                server_key: vec![4, 5, 6],
                stored_key: vec![7, 8, 9],
            }));
        }
        img
    }

    fn process_targets_for_test(
        image: &MetadataImage,
        users_filter: Option<
            &[crabka_protocol::owned::describe_user_scram_credentials_request::UserName],
        >,
    ) -> DescribeUserScramCredentialsResponse {
        let known_users: std::collections::HashSet<String> =
            image.scram_credentials_users().into_iter().collect();
        let targets: Vec<String> = match users_filter {
            None | Some([]) => {
                let mut v: Vec<String> = known_users.iter().cloned().collect();
                v.sort();
                v
            }
            Some(filter) => filter.iter().map(|u| u.name.clone()).collect(),
        };
        let results = build_results(image, &known_users, targets);
        DescribeUserScramCredentialsResponse {
            throttle_time_ms: 0,
            error_code: 0,
            error_message: None,
            results,
            ..Default::default()
        }
    }

    fn run_handle_filter(
        users_filter: Option<Vec<String>>,
        seeded: &[(&str, SaslMechanism, u32)],
    ) -> DescribeUserScramCredentialsResponse {
        use crabka_protocol::owned::describe_user_scram_credentials_request::UserName;
        let req = DescribeUserScramCredentialsRequest {
            users: users_filter.map(|v| {
                v.into_iter()
                    .map(|n| UserName {
                        name: n,
                        ..Default::default()
                    })
                    .collect()
            }),
            ..Default::default()
        };
        let image = img_with_scram(seeded);
        process_targets_for_test(&image, req.users.as_deref())
    }

    #[test]
    fn describe_all_users_when_filter_none() {
        let resp = run_handle_filter(
            None,
            &[
                ("alice", SaslMechanism::ScramSha512, 4096),
                ("bob", SaslMechanism::ScramSha512, 8192),
            ],
        );
        assert!(resp.results.len() == 2);
        let users: Vec<&str> = resp.results.iter().map(|r| r.user.as_str()).collect();
        assert!(users.contains(&"alice") && users.contains(&"bob"));
    }

    #[test]
    fn describe_filter_returns_only_listed_users() {
        let resp = run_handle_filter(
            Some(vec!["alice".into()]),
            &[
                ("alice", SaslMechanism::ScramSha512, 4096),
                ("bob", SaslMechanism::ScramSha512, 8192),
            ],
        );
        let expected = vec![DescribeUserScramCredentialsResult {
            user: "alice".to_string(),
            error_code: 0,
            error_message: None,
            credential_infos: vec![CredentialInfo {
                mechanism: 2,
                iterations: 4096,
                unknown_tagged_fields: UnknownTaggedFields(Vec::new()),
            }],
            unknown_tagged_fields: UnknownTaggedFields(Vec::new()),
        }];
        assert!(resp.results == expected);
    }

    #[test]
    fn unknown_user_returns_resource_not_found() {
        let resp = run_handle_filter(
            Some(vec!["ghost".into()]),
            &[("alice", SaslMechanism::ScramSha512, 4096)],
        );
        let expected = vec![DescribeUserScramCredentialsResult {
            user: "ghost".to_string(),
            error_code: DESCRIBE_USER_SCRAM_RESOURCE_NOT_FOUND,
            error_message: Some("no such SCRAM user".to_string()),
            credential_infos: Vec::new(),
            unknown_tagged_fields: UnknownTaggedFields(Vec::new()),
        }];
        assert!(resp.results == expected);
    }

    #[test]
    fn sasl_mechanism_byte_mapping() {
        for (mechanism, want) in [
            (SaslMechanism::ScramSha256, 1),
            (SaslMechanism::ScramSha512, 2),
            (SaslMechanism::Plain, 0),
        ] {
            assert!(sasl_mechanism_to_byte(mechanism) == want, "{mechanism:?}");
        }
    }
}
