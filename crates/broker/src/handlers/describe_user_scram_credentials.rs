//! `DescribeUserScramCredentials` (`api_key` 50, KIP-554 read half).

use bytes::Bytes;
use crabka_metadata::{MetadataImage, ResourceType};
use crabka_protocol::{
    Encode,
    owned::{
        describe_user_scram_credentials_request::{DescribeUserScramCredentialsRequest, UserName},
        describe_user_scram_credentials_response::{
            CredentialInfo, DescribeUserScramCredentialsResponse,
            DescribeUserScramCredentialsResult,
        },
    },
};
use crabka_security::SaslMechanism;

use crate::{
    authorizer::{AuthorizationRequest, AuthorizationResult},
    broker::Broker,
    codes::{CLUSTER_AUTHORIZATION_FAILED, DUPLICATE_RESOURCE, RESOURCE_NOT_FOUND},
};

const DESCRIBE_DUPLICATE_USER: &str =
    "Cannot describe SCRAM credentials for the same user twice in a single request";

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
            operation: crabka_metadata::AclOperation::Describe,
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
    let targets = requested_targets(&known_users, req.users.as_deref());

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
    targets: Vec<DescribeTarget>,
) -> Vec<DescribeUserScramCredentialsResult> {
    targets
        .into_iter()
        .map(|target| {
            let user = target.user;
            if target.is_duplicate {
                return DescribeUserScramCredentialsResult {
                    error_message: Some(format!("{DESCRIBE_DUPLICATE_USER}: {user}")),
                    user,
                    error_code: DUPLICATE_RESOURCE,
                    credential_infos: vec![],
                    ..Default::default()
                };
            }

            let mut pairs = image.scram_credentials_for_user(&user);
            if pairs.is_empty() && !known_users.contains(&user) {
                DescribeUserScramCredentialsResult {
                    user,
                    error_code: RESOURCE_NOT_FOUND,
                    error_message: Some("no such SCRAM user".into()),
                    credential_infos: vec![],
                    ..Default::default()
                }
            } else {
                pairs.sort_by_key(|(mech, _)| sasl_mechanism_to_byte(*mech));
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

#[derive(Debug, Clone, PartialEq, Eq)]
struct DescribeTarget {
    user: String,
    is_duplicate: bool,
}

fn requested_targets(
    known_users: &std::collections::HashSet<String>,
    users_filter: Option<&[UserName]>,
) -> Vec<DescribeTarget> {
    let Some(filter) = users_filter else {
        return all_known_user_targets(known_users);
    };
    if filter.is_empty() {
        return all_known_user_targets(known_users);
    }

    let mut requested_users = Vec::new();
    let mut duplicate_flags = std::collections::HashMap::new();
    for requested_user in filter {
        let user = requested_user.name.clone();
        match duplicate_flags.entry(user.clone()) {
            std::collections::hash_map::Entry::Vacant(entry) => {
                entry.insert(false);
                requested_users.push(user);
            }
            std::collections::hash_map::Entry::Occupied(mut entry) => {
                entry.insert(true);
            }
        }
    }

    requested_users
        .into_iter()
        .map(|user| DescribeTarget {
            is_duplicate: duplicate_flags.get(&user).copied().unwrap_or(false),
            user,
        })
        .collect()
}

fn all_known_user_targets(known_users: &std::collections::HashSet<String>) -> Vec<DescribeTarget> {
    let mut users: Vec<String> = known_users.iter().cloned().collect();
    users.sort();
    users
        .into_iter()
        .map(|user| DescribeTarget {
            user,
            is_duplicate: false,
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
    crate::handlers::encode_response_with_context(
        resp,
        api_version,
        "encode DescribeUserScramCredentials",
    )
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use assert2::assert;
    use crabka_metadata::{AclOperation, MetadataRecord, ScramCredentialRecord};
    use crabka_protocol::UnknownTaggedFields;

    #[derive(Debug)]
    struct ClusterDescribeOnly;

    impl crate::authorizer::Authorizer for ClusterDescribeOnly {
        fn authorize(
            &self,
            _source: &dyn crabka_authz::AclSource,
            req: &crate::authorizer::AuthorizationRequest<'_>,
        ) -> AuthorizationResult {
            if req.resource_type == ResourceType::Cluster
                && req.resource_name == crate::handlers::acl_wire::CLUSTER_RESOURCE_NAME
                && req.operation == AclOperation::Describe
            {
                return AuthorizationResult::Allow;
            }

            AuthorizationResult::Deny
        }
    }

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
        let targets = requested_targets(&known_users, users_filter);
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
            error_code: 91,
            error_message: Some("no such SCRAM user".to_string()),
            credential_infos: Vec::new(),
            unknown_tagged_fields: UnknownTaggedFields(Vec::new()),
        }];
        assert!(resp.results == expected);
    }

    #[test]
    fn duplicate_requested_user_returns_single_duplicate_resource_row() {
        const KAFKA_DUPLICATE_RESOURCE: i16 = 92;

        let resp = run_handle_filter(
            Some(vec!["alice".into(), "bob".into(), "alice".into()]),
            &[
                ("alice", SaslMechanism::ScramSha512, 4096),
                ("bob", SaslMechanism::ScramSha512, 8192),
            ],
        );

        assert!(
            resp.results.len() == 2,
            "duplicate users collapse to one row"
        );
        let alice_rows: Vec<_> = resp.results.iter().filter(|r| r.user == "alice").collect();
        assert!(
            alice_rows.len() == 1,
            "alice should appear once: {:?}",
            resp.results
        );
        assert!(alice_rows[0].error_code == KAFKA_DUPLICATE_RESOURCE);
        assert!(alice_rows[0].credential_infos.is_empty());

        let bob = resp
            .results
            .iter()
            .find(|r| r.user == "bob")
            .expect("distinct users remain in the response");
        assert!(bob.error_code == 0);
        assert!(
            bob.credential_infos
                == vec![CredentialInfo {
                    mechanism: 2,
                    iterations: 8192,
                    unknown_tagged_fields: UnknownTaggedFields(Vec::new()),
                }]
        );
    }

    #[test]
    fn credential_infos_are_ordered_by_kafka_scram_mechanism_type() {
        let resp = run_handle_filter(
            Some(vec!["alice".into()]),
            &[
                ("alice", SaslMechanism::ScramSha512, 8192),
                ("alice", SaslMechanism::ScramSha256, 4096),
            ],
        );

        let alice = resp
            .results
            .iter()
            .find(|row| row.user == "alice")
            .expect("alice result exists");
        let mechanisms: Vec<i8> = alice
            .credential_infos
            .iter()
            .map(|info| info.mechanism)
            .collect();

        assert!(mechanisms == vec![1, 2]);
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

    #[tokio::test]
    async fn handle_allows_cluster_describe_authorization() {
        let (broker_handle, _dir) =
            crate::test_support::start_broker_with_authorizer(Arc::new(ClusterDescribeOnly)).await;
        let broker = broker_handle.broker_arc_for_test();
        let principal = crate::test_support::principal("alice");
        let peer = crate::test_support::peer();
        let ctx = crate::test_support::request_context(&principal, &peer, "scram-describe-test");

        let bytes = handle(
            &broker,
            DescribeUserScramCredentialsRequest::default(),
            &ctx,
            0,
        )
        .await
        .expect("describe should encode");
        let resp: DescribeUserScramCredentialsResponse =
            crate::test_support::decode_response(&bytes, 0);

        assert!(resp.error_code == 0, "Cluster Describe should authorize");
        assert!(resp.results.is_empty());
        broker_handle.shutdown().await;
    }

    #[tokio::test]
    async fn handle_rejects_without_cluster_describe_authorization() {
        let (broker_handle, _dir) = crate::test_support::start_broker_with_authorizer(Arc::new(
            crate::test_support::DenyAll,
        ))
        .await;
        let broker = broker_handle.broker_arc_for_test();
        let principal = crate::test_support::principal("alice");
        let peer = crate::test_support::peer();
        let ctx = crate::test_support::request_context(&principal, &peer, "scram-describe-test");

        let bytes = handle(
            &broker,
            DescribeUserScramCredentialsRequest::default(),
            &ctx,
            0,
        )
        .await
        .expect("describe denial should encode");
        let resp: DescribeUserScramCredentialsResponse =
            crate::test_support::decode_response(&bytes, 0);

        assert!(resp.error_code == CLUSTER_AUTHORIZATION_FAILED);
        assert!(resp.results.is_empty());
        broker_handle.shutdown().await;
    }
}
