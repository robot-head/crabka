// rustc 1.95 clippy::pedantic ICEs on this file (the same upstream bug
// in clippy's body-analysis / doc lint pass that already triggers on
// `tests/admin_handlers.rs`). Disable pedantic locally; the rest of the
// workspace still enforces the full pedantic gate.
#![allow(clippy::pedantic)]

//! Broker-side ACL integration tests. No Docker.
//!
//! T22 — the first of three integration test batches — drives the
//! `CreateAcls` / `DescribeAcls` / `DeleteAcls` flow over a real
//! `SASL_PLAINTEXT` listener with the wire-typed `crabka-protocol`
//! request/response codecs. The SASL framing helpers (`drive_*`,
//! `round_trip`) are copied inline rather than shared via `mod common`
//! because Rust integration tests don't easily allow sibling-module
//! reuse across files in `tests/`.
//!
//! Gated to non-Windows to match the multi-broker test convention
//! (the SASL listener startup is fine on Windows, but keeping
//! the gate uniform avoids one-off CI matrix surprises).

use std::{io, net::SocketAddr};

use assert2::{assert, check};
use bytes::{Buf, BufMut, BytesMut};
use crabka_broker::{Broker, BrokerConfig, authorizer::SimpleAclAuthorizer, config::ListenerSpec};
use crabka_protocol::{
    Decode, Encode,
    owned::{
        api_versions_request::ApiVersionsRequest,
        api_versions_response::ApiVersionsResponse,
        create_acls_request::{AclCreation, CreateAclsRequest},
        create_acls_response::CreateAclsResponse,
        create_topics_request::{CreatableTopic, CreateTopicsRequest},
        create_topics_response::CreateTopicsResponse,
        delete_acls_request::{DeleteAclsFilter, DeleteAclsRequest},
        delete_acls_response::DeleteAclsResponse,
        describe_acls_request::DescribeAclsRequest,
        describe_acls_response::DescribeAclsResponse,
        fetch_request::{FetchPartition, FetchRequest, FetchTopic},
        fetch_response::FetchResponse,
        init_producer_id_request::InitProducerIdRequest,
        init_producer_id_response::InitProducerIdResponse,
        join_group_request::{JoinGroupRequest, JoinGroupRequestProtocol},
        join_group_response::JoinGroupResponse,
        metadata_request::{MetadataRequest, MetadataRequestTopic},
        metadata_response::MetadataResponse,
        produce_request::{PartitionProduceData, ProduceRequest, TopicProduceData},
        produce_response::ProduceResponse,
        sasl_authenticate_request::SaslAuthenticateRequest,
        sasl_authenticate_response::SaslAuthenticateResponse,
        sasl_handshake_request::SaslHandshakeRequest,
        sasl_handshake_response::SaslHandshakeResponse,
    },
    records::{Record, RecordBatch},
};
use crabka_security::{ListenerProtocol, SaslMechanism};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::TcpStream,
};

// Wire `i8` discriminants for the Kafka ACL enums. Kept inline (rather
// than imported from `crabka-broker::handlers::acl_wire`, which is
// crate-private) so the tests exercise the same byte values JVM clients
// would send. Sourced from `crates/broker/src/handlers/acl_wire.rs`.
const RESOURCE_TYPE_TOPIC: i8 = 2;
const PATTERN_TYPE_ANY: i8 = 1;
const PATTERN_TYPE_LITERAL: i8 = 3;
const OPERATION_ANY: i8 = 1;
const OPERATION_READ: i8 = 3;
const OPERATION_WRITE: i8 = 4;
const PERMISSION_ANY: i8 = 1;
const PERMISSION_ALLOW: i8 = 3;

// API versions chosen so the request header is the flexible v2 form
// (matches what's exercised by the `drive_sasl_plain_session`
// helper for any flexible body). All three ACL APIs went flexible at v2.
const CREATE_ACLS_VERSION: i16 = 3;
const DESCRIBE_ACLS_VERSION: i16 = 3;
const DELETE_ACLS_VERSION: i16 = 3;

// Versions chosen for the T23 Produce/Fetch integration tests:
//   * CreateTopics v7 — flexible (FLEXIBLE_MIN=5), topic id round-trips
//     so the admin path matches what JVM clients send.
//   * Produce v11 — flexible (FLEXIBLE_MIN=9) and still uses topic
//     `name` rather than topic_id (the latter is v ≥ 13).
//   * Fetch v12 — flexible (FLEXIBLE_MIN=12) and still uses topic
//     `name` rather than topic_id, and predates KIP-903's tagged
//     `replica_state` (v ≥ 15) so the request stays a simple shape.
const CREATE_TOPICS_VERSION: i16 = 7;
const PRODUCE_VERSION: i16 = 11;
const FETCH_VERSION: i16 = 12;

// T24 versions:
//   * Metadata v9 — first flexible version (FLEXIBLE_MIN=9), still uses
//     topic `name` rather than `topic_id` (the latter is v ≥ 10), and
//     predates the `topic_authorized_operations` per-topic field (v ≥ 8
//     in request, but we don't request it).
//   * JoinGroup v9 — flexible (FLEXIBLE_MIN=6) and max supported by the
//     handler. Carries `skip_assignment` (v ≥ 9) but we don't read it.
//   * InitProducerId v4 — flexible (FLEXIBLE_MIN=2). Past v3 we have
//     producer_id + producer_epoch on the wire but no enable2_pc fields
//     (those are v ≥ 6).
const METADATA_VERSION: i16 = 9;
const JOIN_GROUP_VERSION: i16 = 9;
const INIT_PRODUCER_ID_VERSION: i16 = 4;

// Kafka error codes consumed by the T23/T24 assertions.
const ERR_TOPIC_AUTHORIZATION_FAILED: i16 = 29;
const ERR_GROUP_AUTHORIZATION_FAILED: i16 = 30;
const ERR_TRANSACTIONAL_ID_AUTHORIZATION_FAILED: i16 = 53;
const ERR_MEMBER_ID_REQUIRED: i16 = 79;

/// Build a `BrokerConfig` with a single `SASL_PLAINTEXT` listener, PLAIN
/// enabled, and the given super-user. The non-super-user case still
/// declares a super-user so the cluster-Alter gate applies. It also
/// installs `SimpleAclAuthorizer` explicitly so the broker enforces ACLs
/// (the new default is `AllowAllAuthorizer`, which would silently let
/// every test through).
fn sasl_plain_broker_config(
    log_dir: &std::path::Path,
    creds: &[(&str, &str)],
    super_user: Option<&str>,
) -> BrokerConfig {
    let mut cfg = BrokerConfig::for_tests(log_dir.to_path_buf());
    cfg.listeners = vec![ListenerSpec {
        name: "SASL_PLAINTEXT".to_string(),
        bind_addr: "127.0.0.1:0".parse().unwrap(),
        advertised: "127.0.0.1:0".to_string(),
        protocol: ListenerProtocol::SaslPlaintext,
        tls_config: None,
        sasl_mechanisms: None,
    }];
    cfg.inter_broker_listener_name = "SASL_PLAINTEXT".to_string();
    cfg.enabled_sasl_mechanisms = vec![SaslMechanism::Plain];
    for (u, p) in creds {
        cfg.plain_credentials
            .insert((*u).to_string(), (*p).to_string());
    }
    cfg.super_users = super_user.map(str::to_string).into_iter().collect();
    cfg.authorizer = std::sync::Arc::new(SimpleAclAuthorizer::new(cfg.super_users.clone()));
    cfg
}

/// Like `sasl_plain_broker_config` but accepts multiple super-users. Used by
/// the `multi_super_user_both_can_provision` test to verify that
/// any principal in the `super_users` set can drive privileged admin APIs.
fn sasl_plain_broker_config_multi_super(
    log_dir: &std::path::Path,
    creds: &[(&str, &str)],
    super_users: &[&str],
) -> BrokerConfig {
    let mut cfg = BrokerConfig::for_tests(log_dir.to_path_buf());
    cfg.listeners = vec![ListenerSpec {
        name: "SASL_PLAINTEXT".to_string(),
        bind_addr: "127.0.0.1:0".parse().unwrap(),
        advertised: "127.0.0.1:0".to_string(),
        protocol: ListenerProtocol::SaslPlaintext,
        tls_config: None,
        sasl_mechanisms: None,
    }];
    cfg.inter_broker_listener_name = "SASL_PLAINTEXT".to_string();
    cfg.enabled_sasl_mechanisms = vec![SaslMechanism::Plain];
    for (u, p) in creds {
        cfg.plain_credentials
            .insert((*u).to_string(), (*p).to_string());
    }
    cfg.super_users = super_users.iter().map(|s| (*s).to_string()).collect();
    cfg.authorizer = std::sync::Arc::new(SimpleAclAuthorizer::new(cfg.super_users.clone()));
    cfg
}

/// Shorthand for `Allow <op> on Topic LITERAL <name> for <principal> from *`.
/// Every test in this file uses literal Topic ACLs with host `*`, so the only
/// dimensions that vary per binding are `resource_name`, `principal`, and
/// `operation` — wrap them up here to keep the test bodies short.
fn topic_allow_creation(name: &str, principal: &str, operation: i8) -> AclCreation {
    AclCreation {
        resource_type: RESOURCE_TYPE_TOPIC,
        resource_name: name.to_string(),
        resource_pattern_type: PATTERN_TYPE_LITERAL,
        principal: principal.to_string(),
        host: "*".to_string(),
        operation,
        permission_type: PERMISSION_ALLOW,
        ..Default::default()
    }
}

/// Permissive `DescribeAclsRequest` for `Topic` — every other axis is wildcard.
fn describe_all_topic_acls() -> DescribeAclsRequest {
    DescribeAclsRequest {
        resource_type_filter: RESOURCE_TYPE_TOPIC,
        resource_name_filter: None,
        pattern_type_filter: PATTERN_TYPE_ANY,
        principal_filter: None,
        host_filter: None,
        operation: OPERATION_ANY,
        permission_type: PERMISSION_ANY,
        ..Default::default()
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn create_acls_super_user_can_provision_and_describe() {
    let log_dir = tempfile::tempdir().unwrap();
    let cfg = sasl_plain_broker_config(log_dir.path(), &[("admin", "admin-secret")], Some("admin"));

    let handle = Broker::start(cfg).await.expect("broker must start");
    let addr = handle.listen_addr();

    // Provision: Allow Read on Topic LITERAL "foo" for User:alice from *.
    let create_req = CreateAclsRequest {
        creations: vec![topic_allow_creation("foo", "User:alice", OPERATION_READ)],
        ..Default::default()
    };
    let create_resp = drive_create_acls_as_plain(addr, "admin", b"admin-secret", create_req)
        .await
        .expect("CreateAcls as super-user must succeed");
    assert!(
        create_resp.results.len() == 1,
        "one result per creation: {create_resp:?}"
    );
    assert!(
        create_resp.results[0].error_code == 0,
        "super-user creation must return error_code=0, got {:?}",
        create_resp.results[0]
    );

    // Describe with a permissive filter (resource_type=Topic, everything
    // else any/null) — must return exactly one resource entry carrying
    // one ACL description for User:alice / Read / Allow.
    let describe_resp =
        drive_describe_acls_as_plain(addr, "admin", b"admin-secret", describe_all_topic_acls())
            .await
            .expect("DescribeAcls as super-user must succeed");
    handle.shutdown().await;

    assert!(
        describe_resp.error_code == 0,
        "DescribeAcls must succeed, got {describe_resp:?}"
    );
    assert!(
        describe_resp.resources.len() == 1,
        "expected exactly one matching resource, got {:?}",
        describe_resp.resources
    );
    let resource = &describe_resp.resources[0];
    check!(resource.resource_type == RESOURCE_TYPE_TOPIC);
    check!(resource.resource_name == "foo");
    check!(resource.pattern_type == PATTERN_TYPE_LITERAL);
    assert!(
        resource.acls.len() == 1,
        "expected exactly one ACL description, got {:?}",
        resource.acls
    );
    let acl = &resource.acls[0];
    check!(acl.principal == "User:alice");
    check!(acl.host == "*");
    check!(acl.operation == OPERATION_READ);
    check!(acl.permission_type == PERMISSION_ALLOW);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn create_acls_non_super_user_rejected() {
    let log_dir = tempfile::tempdir().unwrap();
    // alice is NOT the super-user. admin is configured as super-user so
    // the compat shim stays off and the cluster-Alter gate applies.
    let cfg = sasl_plain_broker_config(
        log_dir.path(),
        &[("admin", "admin-secret"), ("alice", "wonderland")],
        Some("admin"),
    );

    let handle = Broker::start(cfg).await.expect("broker must start");
    let addr = handle.listen_addr();

    let req = CreateAclsRequest {
        creations: vec![
            topic_allow_creation("foo", "User:bob", OPERATION_READ),
            topic_allow_creation("bar", "User:carol", OPERATION_WRITE),
        ],
        ..Default::default()
    };
    let resp = drive_create_acls_as_plain(addr, "alice", b"wonderland", req)
        .await
        .expect("CreateAcls request must round-trip even when denied");
    handle.shutdown().await;

    assert!(resp.results.len() == 2, "one result row per creation");
    for (i, r) in resp.results.iter().enumerate() {
        assert!(
            r.error_code == 31, /* CLUSTER_AUTHORIZATION_FAILED */
            "binding {i} must be denied with CLUSTER_AUTHORIZATION_FAILED, got {r:?}"
        );
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn delete_acls_removes_matching() {
    let log_dir = tempfile::tempdir().unwrap();
    let cfg = sasl_plain_broker_config(log_dir.path(), &[("admin", "admin-secret")], Some("admin"));

    let handle = Broker::start(cfg).await.expect("broker must start");
    let addr = handle.listen_addr();

    // Provision two ACLs (Read on "foo", Write on "bar").
    let create_req = CreateAclsRequest {
        creations: vec![
            topic_allow_creation("foo", "User:alice", OPERATION_READ),
            topic_allow_creation("bar", "User:alice", OPERATION_WRITE),
        ],
        ..Default::default()
    };
    let create_resp = drive_create_acls_as_plain(addr, "admin", b"admin-secret", create_req)
        .await
        .expect("provisioning CreateAcls must succeed");
    assert!(create_resp.results.len() == 2);
    for r in &create_resp.results {
        assert!(r.error_code == 0, "provisioning must succeed, got {r:?}");
    }

    // Delete only the Read-on-foo binding via a precisely-targeted filter.
    let delete_req = DeleteAclsRequest {
        filters: vec![DeleteAclsFilter {
            resource_type_filter: RESOURCE_TYPE_TOPIC,
            resource_name_filter: Some("foo".to_string()),
            pattern_type_filter: PATTERN_TYPE_LITERAL,
            principal_filter: Some("User:alice".to_string()),
            host_filter: Some("*".to_string()),
            operation: OPERATION_READ,
            permission_type: PERMISSION_ALLOW,
            ..Default::default()
        }],
        ..Default::default()
    };
    let delete_resp = drive_delete_acls_as_plain(addr, "admin", b"admin-secret", delete_req)
        .await
        .expect("DeleteAcls must succeed");
    assert!(
        delete_resp.filter_results.len() == 1,
        "one filter result row per filter"
    );
    assert!(
        delete_resp.filter_results[0].error_code == 0,
        "filter must succeed, got {:?}",
        delete_resp.filter_results[0]
    );
    let matching = &delete_resp.filter_results[0].matching_acls;
    assert!(
        matching.len() == 1,
        "exactly one ACL must match the precise filter, got {matching:?}"
    );
    check!(matching[0].resource_name == "foo");
    check!(matching[0].operation == OPERATION_READ);
    check!(matching[0].error_code == 0);

    // Describe — only the Write-on-bar binding should remain.
    let describe_resp =
        drive_describe_acls_as_plain(addr, "admin", b"admin-secret", describe_all_topic_acls())
            .await
            .expect("DescribeAcls must succeed");
    handle.shutdown().await;

    assert!(describe_resp.error_code == 0);
    // Flatten all (resource, acl) pairs so the assertion doesn't depend
    // on whether the broker groups by resource or emits one resource per
    // ACL — the contract is "the deleted binding is gone, the other one
    // is still there".
    let mut surviving: Vec<(String, i8, i8)> = Vec::new();
    for r in &describe_resp.resources {
        for a in &r.acls {
            surviving.push((r.resource_name.clone(), a.operation, a.permission_type));
        }
    }
    assert!(
        surviving.len() == 1,
        "exactly one binding must remain, got {surviving:?}"
    );
    assert!(
        surviving[0] == ("bar".to_string(), OPERATION_WRITE, PERMISSION_ALLOW),
        "the surviving binding must be Write-on-bar, got {:?}",
        surviving[0]
    );
}

// ────────────────────────────────────────────────────────────────────────
// T23: Produce / Fetch enforcement.
//
// Each test boots a fresh single-broker SASL_PLAINTEXT cluster with admin
// as the super-user. Admin (over SASL) drives a CreateTopics request to
// materialise the partition, the test seeds whatever ACL records are
// needed via the controller-direct test helper, then alice authenticates
// (a separate connection) and drives a Produce / Fetch. The assertions
// look at the per-partition `error_code` row of the response and, on the
// happy path, also check the broker's local log end offset via the
// existing `BrokerHandle::local_log_end_offset` helper.
// ────────────────────────────────────────────────────────────────────────

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn produce_denied_without_topic_acl() {
    let log_dir = tempfile::tempdir().unwrap();
    let cfg = sasl_plain_broker_config(
        log_dir.path(),
        &[("admin", "admin-secret"), ("alice", "wonderland")],
        Some("admin"),
    );

    let handle = Broker::start(cfg).await.expect("broker must start");
    let addr = handle.listen_addr();

    // Admin creates topic "foo" with one partition (rf=1, single-node).
    create_topic_as_admin(addr, "foo", 1).await;

    // Seed a meaningless ACL via direct controller write. The super-user
    // is already set so `authorize()`'s compat shim is off, but populating
    // at least one ACL makes the test read closer to a "real" cluster
    // post-bootstrap.
    handle
        .submit_metadata_record_for_test(crabka_metadata::MetadataRecord::V1AccessControlEntry(
            crabka_metadata::AclEntry {
                resource_type: crabka_metadata::ResourceType::Topic,
                resource_name: "_nothing".into(),
                pattern_type: crabka_metadata::PatternType::Literal,
                principal: "User:admin".into(),
                host: "*".into(),
                operation: crabka_metadata::AclOperation::Read,
                permission_type: crabka_metadata::PermissionType::Allow,
            },
        ))
        .await
        .expect("seed dummy ACL");

    // alice has NO Write-on-foo binding → Produce must return 29.
    let resp = drive_produce_as_plain(
        addr,
        "alice",
        b"wonderland",
        single_record_produce_request("foo", 0, b"hello"),
    )
    .await
    .expect("Produce must round-trip");
    handle.shutdown().await;

    assert!(resp.responses.len() == 1, "one topic in response");
    assert!(
        resp.responses[0].partition_responses.len() == 1,
        "one partition row in response"
    );
    let p = &resp.responses[0].partition_responses[0];
    assert!(
        p.error_code == ERR_TOPIC_AUTHORIZATION_FAILED,
        "alice has no Write ACL on foo, expected TOPIC_AUTHORIZATION_FAILED (29), got {p:?}"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn produce_allowed_with_topic_write_acl() {
    let log_dir = tempfile::tempdir().unwrap();
    let cfg = sasl_plain_broker_config(
        log_dir.path(),
        &[("admin", "admin-secret"), ("alice", "wonderland")],
        Some("admin"),
    );

    let handle = Broker::start(cfg).await.expect("broker must start");
    let addr = handle.listen_addr();

    create_topic_as_admin(addr, "foo", 1).await;

    // Provision Allow Write Topic LITERAL "foo" User:alice host=* via a
    // direct controller write. (CreateAcls as admin would also work,
    // but `submit_metadata_record_for_test` is one fewer round-trip and
    // exercises the same authorizer state.)
    handle
        .submit_metadata_record_for_test(crabka_metadata::MetadataRecord::V1AccessControlEntry(
            crabka_metadata::AclEntry {
                resource_type: crabka_metadata::ResourceType::Topic,
                resource_name: "foo".into(),
                pattern_type: crabka_metadata::PatternType::Literal,
                principal: "User:alice".into(),
                host: "*".into(),
                operation: crabka_metadata::AclOperation::Write,
                permission_type: crabka_metadata::PermissionType::Allow,
            },
        ))
        .await
        .expect("seed Write-on-foo ACL for alice");

    // The ACL submit above is committed via the controller's raft path
    // then applied into the in-memory `MetadataImage` asynchronously, so
    // Produce reads from that image. Retry on Deny for up to 10 s — on
    // CI the commit-then-apply gap is usually a few ms but can spike.
    let resp = retry_produce_until_allowed(addr, "alice", b"wonderland", "foo")
        .await
        .expect("Produce must round-trip");

    assert!(resp.responses.len() == 1);
    assert!(resp.responses[0].partition_responses.len() == 1);
    let p = &resp.responses[0].partition_responses[0];
    assert!(
        p.error_code == 0,
        "alice has Write ACL on foo, expected error_code=0, got {p:?}"
    );

    // Verify the record actually landed in the local log.
    let leo = handle
        .local_log_end_offset("foo", 0)
        .await
        .expect("foo-0 must be hosted on this broker");
    handle.shutdown().await;
    assert!(
        leo >= 1,
        "log_end_offset must advance after a successful Produce, got {leo}"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn fetch_denied_without_topic_read_acl() {
    let log_dir = tempfile::tempdir().unwrap();
    let cfg = sasl_plain_broker_config(
        log_dir.path(),
        &[("admin", "admin-secret"), ("alice", "wonderland")],
        Some("admin"),
    );

    let handle = Broker::start(cfg).await.expect("broker must start");
    let addr = handle.listen_addr();

    create_topic_as_admin(addr, "foo", 1).await;

    // Seed a dummy ACL via direct controller write. Same rationale as in
    // produce_denied_without_topic_acl.
    handle
        .submit_metadata_record_for_test(crabka_metadata::MetadataRecord::V1AccessControlEntry(
            crabka_metadata::AclEntry {
                resource_type: crabka_metadata::ResourceType::Topic,
                resource_name: "_nothing".into(),
                pattern_type: crabka_metadata::PatternType::Literal,
                principal: "User:admin".into(),
                host: "*".into(),
                operation: crabka_metadata::AclOperation::Read,
                permission_type: crabka_metadata::PermissionType::Allow,
            },
        ))
        .await
        .expect("seed dummy ACL");

    // alice has NO Read-on-foo binding → Fetch must return 29 on the
    // partition row.
    let req = FetchRequest {
        max_wait_ms: 0,
        min_bytes: 1,
        max_bytes: 1_048_576,
        topics: vec![FetchTopic {
            topic: "foo".to_string(),
            partitions: vec![FetchPartition {
                partition: 0,
                fetch_offset: 0,
                partition_max_bytes: 1_048_576,
                ..Default::default()
            }],
            ..Default::default()
        }],
        ..Default::default()
    };
    let resp = drive_fetch_as_plain(addr, "alice", b"wonderland", req)
        .await
        .expect("Fetch must round-trip");
    handle.shutdown().await;

    assert!(resp.responses.len() == 1, "one topic in response");
    assert!(
        resp.responses[0].partitions.len() == 1,
        "one partition row in response"
    );
    let p = &resp.responses[0].partitions[0];
    assert!(
        p.error_code == ERR_TOPIC_AUTHORIZATION_FAILED,
        "alice has no Read ACL on foo, expected TOPIC_AUTHORIZATION_FAILED (29), got {p:?}"
    );
}

// ────────────────────────────────────────────────────────────────────────
// T23 helpers — CreateTopics-as-admin, Produce/Fetch over SASL, and a
// tiny poll helper. All three share `sasl_plain_authenticate` + the
// existing `round_trip` framing primitive defined further down.
// ────────────────────────────────────────────────────────────────────────

/// Build a `ProduceRequest` carrying a single record (`value`) for
/// `(topic, partition)`. `acks=-1` (all-ISR) matches the JVM client's
/// default for durable producers.
fn single_record_produce_request(topic: &str, partition: i32, value: &[u8]) -> ProduceRequest {
    ProduceRequest {
        transactional_id: None,
        acks: -1,
        timeout_ms: 5_000,
        topic_data: vec![TopicProduceData {
            name: topic.to_string(),
            partition_data: vec![PartitionProduceData {
                index: partition,
                records: Some(
                    RecordBatch {
                        last_offset_delta: 0,
                        records: vec![Record {
                            offset_delta: 0,
                            value: Some(bytes::Bytes::copy_from_slice(value)),
                            ..Default::default()
                        }],
                        ..Default::default()
                    }
                    .into(),
                ),
                ..Default::default()
            }],
            ..Default::default()
        }],
        ..Default::default()
    }
}

/// Drive a single `CreateTopics` against `addr` authenticated as
/// `admin` / `admin-secret`. Asserts the response has `error_code=0`
/// for the requested topic. Used by the T23 tests to materialise a
/// partition before producing / fetching against it.
async fn create_topic_as_admin(addr: SocketAddr, name: &str, partitions: i32) {
    let req = CreateTopicsRequest {
        topics: vec![CreatableTopic {
            name: name.to_string(),
            num_partitions: partitions,
            replication_factor: 1,
            ..Default::default()
        }],
        timeout_ms: 5_000,
        ..Default::default()
    };
    let resp = drive_create_topics_as_plain(addr, "admin", b"admin-secret", req)
        .await
        .expect("CreateTopics as super-user must round-trip");
    assert!(resp.topics.len() == 1, "one topic in response");
    assert!(
        resp.topics[0].error_code == 0,
        "CreateTopics({name}) must succeed: {:?}",
        resp.topics[0].error_message
    );
}

async fn drive_create_topics_as_plain(
    addr: SocketAddr,
    user: &str,
    password: &[u8],
    req: CreateTopicsRequest,
) -> Result<CreateTopicsResponse, io::Error> {
    let mut stream = sasl_plain_authenticate(addr, user, password).await?;
    let mut body = BytesMut::new();
    req.encode(&mut body, CREATE_TOPICS_VERSION)
        .map_err(|e| io::Error::other(format!("CreateTopics encode: {e}")))?;
    let resp_bytes = round_trip(&mut stream, 19, CREATE_TOPICS_VERSION, 4, true, &body).await?;
    let mut cur: &[u8] = &resp_bytes;
    CreateTopicsResponse::decode(&mut cur, CREATE_TOPICS_VERSION)
        .map_err(|e| io::Error::other(format!("CreateTopics decode: {e}")))
}

async fn drive_produce_as_plain(
    addr: SocketAddr,
    user: &str,
    password: &[u8],
    req: ProduceRequest,
) -> Result<ProduceResponse, io::Error> {
    let mut stream = sasl_plain_authenticate(addr, user, password).await?;
    let mut body = BytesMut::new();
    req.encode(&mut body, PRODUCE_VERSION)
        .map_err(|e| io::Error::other(format!("Produce encode: {e}")))?;
    let resp_bytes = round_trip(&mut stream, 0, PRODUCE_VERSION, 4, true, &body).await?;
    let mut cur: &[u8] = &resp_bytes;
    ProduceResponse::decode(&mut cur, PRODUCE_VERSION)
        .map_err(|e| io::Error::other(format!("Produce decode: {e}")))
}

async fn drive_fetch_as_plain(
    addr: SocketAddr,
    user: &str,
    password: &[u8],
    req: FetchRequest,
) -> Result<FetchResponse, io::Error> {
    let mut stream = sasl_plain_authenticate(addr, user, password).await?;
    let mut body = BytesMut::new();
    req.encode(&mut body, FETCH_VERSION)
        .map_err(|e| io::Error::other(format!("Fetch encode: {e}")))?;
    let resp_bytes = round_trip(&mut stream, 1, FETCH_VERSION, 4, true, &body).await?;
    let mut cur: &[u8] = &resp_bytes;
    FetchResponse::decode(&mut cur, FETCH_VERSION)
        .map_err(|e| io::Error::other(format!("Fetch decode: {e}")))
}

/// Retry `drive_produce_as_plain` against `topic`/partition-0 until the
/// per-partition `error_code` is no longer `TOPIC_AUTHORIZATION_FAILED`
/// (i.e. the ACL submit has been applied into the metadata image) or a
/// 10 s deadline elapses. Used by the happy-path Produce test to absorb
/// the raft commit-then-apply gap. The final response (whichever the
/// caller gets) is what we return.
async fn retry_produce_until_allowed(
    addr: SocketAddr,
    user: &str,
    password: &[u8],
    topic: &str,
) -> Result<ProduceResponse, io::Error> {
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(10);
    loop {
        let resp = drive_produce_as_plain(
            addr,
            user,
            password,
            single_record_produce_request(topic, 0, b"hello"),
        )
        .await?;
        let part = resp
            .responses
            .first()
            .and_then(|t| t.partition_responses.first());
        if part.is_some_and(|p| p.error_code != ERR_TOPIC_AUTHORIZATION_FAILED) {
            return Ok(resp);
        }
        if std::time::Instant::now() > deadline {
            return Ok(resp);
        }
        // intentional: bounded RPC-response poll. The ground truth is the
        // broker's authorization decision (and partition-writer readiness for
        // acks=-1), observed by re-driving Produce — not an image/metric an
        // awaiter exposes. wait_for_image watches the controller's committed
        // image, which can lead the request path's applied copy, so an image
        // wait would race; re-driving absorbs the commit-then-apply gap.
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
    }
}

// ────────────────────────────────────────────────────────────────────────
// T24: Metadata / JoinGroup / InitProducerId enforcement.
//
// Each test boots a fresh single-broker SASL_PLAINTEXT cluster with
// admin as the super-user. The test seeds whatever ACL records the
// scenario requires via the controller-direct test helper (which keeps
// the compat shim off because at least one ACL exists), then alice
// authenticates separately and drives the typed request.
// ────────────────────────────────────────────────────────────────────────

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn metadata_silent_filter_on_fetch_all() {
    let log_dir = tempfile::tempdir().unwrap();
    let cfg = sasl_plain_broker_config(
        log_dir.path(),
        &[("admin", "admin-secret"), ("alice", "wonderland")],
        Some("admin"),
    );

    let handle = Broker::start(cfg).await.expect("broker must start");
    let addr = handle.listen_addr();

    create_topic_as_admin(addr, "t1", 1).await;
    create_topic_as_admin(addr, "t2", 1).await;

    // Seed Allow Describe Topic LITERAL "t1" User:alice. The presence of
    // any ACL in the image also disables the compat shim, so the
    // authorizer evaluates every request rather than short-circuiting to
    // Allow.
    handle
        .submit_metadata_record_for_test(crabka_metadata::MetadataRecord::V1AccessControlEntry(
            crabka_metadata::AclEntry {
                resource_type: crabka_metadata::ResourceType::Topic,
                resource_name: "t1".into(),
                pattern_type: crabka_metadata::PatternType::Literal,
                principal: "User:alice".into(),
                host: "*".into(),
                operation: crabka_metadata::AclOperation::Describe,
                permission_type: crabka_metadata::PermissionType::Allow,
            },
        ))
        .await
        .expect("seed Describe-on-t1 ACL for alice");

    // Wait for the ACL to propagate before issuing Metadata as alice —
    // until then alice sees no topics at all. Once t1 appears, the image
    // has applied the binding.
    let resp = retry_metadata_until_topic_visible(addr, "alice", b"wonderland", "t1", None)
        .await
        .expect("Metadata must round-trip");
    handle.shutdown().await;

    let names: Vec<&str> = resp
        .topics
        .iter()
        .filter_map(|t| t.name.as_deref())
        .collect();
    assert!(
        names.contains(&"t1"),
        "t1 must be visible to alice, got {names:?}"
    );
    assert!(
        !names.contains(&"t2"),
        "t2 must be silently filtered out of fetch-all, got {names:?}"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn metadata_explicit_deny_on_named_topic() {
    let log_dir = tempfile::tempdir().unwrap();
    let cfg = sasl_plain_broker_config(
        log_dir.path(),
        &[("admin", "admin-secret"), ("alice", "wonderland")],
        Some("admin"),
    );

    let handle = Broker::start(cfg).await.expect("broker must start");
    let addr = handle.listen_addr();

    create_topic_as_admin(addr, "t1", 1).await;
    create_topic_as_admin(addr, "t2", 1).await;

    // Seed Allow Describe on t1 for alice. This both turns the compat
    // shim off and gives alice *something* she's authorized to see, so
    // the Deny on t2 isn't merely "no ACLs anywhere".
    handle
        .submit_metadata_record_for_test(crabka_metadata::MetadataRecord::V1AccessControlEntry(
            crabka_metadata::AclEntry {
                resource_type: crabka_metadata::ResourceType::Topic,
                resource_name: "t1".into(),
                pattern_type: crabka_metadata::PatternType::Literal,
                principal: "User:alice".into(),
                host: "*".into(),
                operation: crabka_metadata::AclOperation::Describe,
                permission_type: crabka_metadata::PermissionType::Allow,
            },
        ))
        .await
        .expect("seed Describe-on-t1 ACL for alice");

    // Ask Metadata for t2 *by name*. The named-topic path returns an
    // error row instead of silently filtering. Use the retry helper so
    // we don't race the raft commit-then-apply gap on the seeded ACL.
    let resp = retry_metadata_until_topic_visible(
        addr,
        "alice",
        b"wonderland",
        "t2",
        Some(vec!["t2".to_string()]),
    )
    .await
    .expect("Metadata must round-trip");
    handle.shutdown().await;

    assert!(resp.topics.len() == 1, "one topic row in response");
    let row = &resp.topics[0];
    assert!(row.name.as_deref() == Some("t2"));
    assert!(
        row.error_code == ERR_TOPIC_AUTHORIZATION_FAILED,
        "alice has no ACL on t2, expected TOPIC_AUTHORIZATION_FAILED (29), got {row:?}"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn join_group_denied_without_group_read_acl() {
    let log_dir = tempfile::tempdir().unwrap();
    let cfg = sasl_plain_broker_config(
        log_dir.path(),
        &[("admin", "admin-secret"), ("alice", "wonderland")],
        Some("admin"),
    );

    let handle = Broker::start(cfg).await.expect("broker must start");
    let addr = handle.listen_addr();

    // Seed a meaningless ACL so the compat shim is off. Without this the
    // authorizer would short-circuit to Allow on every check and the
    // Deny assertion below would never fire.
    handle
        .submit_metadata_record_for_test(crabka_metadata::MetadataRecord::V1AccessControlEntry(
            crabka_metadata::AclEntry {
                resource_type: crabka_metadata::ResourceType::Topic,
                resource_name: "_nothing".into(),
                pattern_type: crabka_metadata::PatternType::Literal,
                principal: "User:admin".into(),
                host: "*".into(),
                operation: crabka_metadata::AclOperation::Read,
                permission_type: crabka_metadata::PermissionType::Allow,
            },
        ))
        .await
        .expect("seed dummy ACL");

    // alice has NO Group Read ACL on "cg-1" → JoinGroup must return 30.
    let denied =
        drive_join_group_as_plain(addr, "alice", b"wonderland", join_group_request("cg-1"))
            .await
            .expect("JoinGroup must round-trip");
    assert!(
        denied.error_code == ERR_GROUP_AUTHORIZATION_FAILED,
        "alice has no Group Read on cg-1, expected GROUP_AUTHORIZATION_FAILED (30), got {denied:?}"
    );

    // Provision Allow Read Group LITERAL "cg-1" User:alice.
    handle
        .submit_metadata_record_for_test(crabka_metadata::MetadataRecord::V1AccessControlEntry(
            crabka_metadata::AclEntry {
                resource_type: crabka_metadata::ResourceType::Group,
                resource_name: "cg-1".into(),
                pattern_type: crabka_metadata::PatternType::Literal,
                principal: "User:alice".into(),
                host: "*".into(),
                operation: crabka_metadata::AclOperation::Read,
                permission_type: crabka_metadata::PermissionType::Allow,
            },
        ))
        .await
        .expect("seed Read-on-cg-1 ACL for alice");

    // Retry until the ACL is applied (not 30 any more). The first
    // non-denied response is MEMBER_ID_REQUIRED (79) — JoinGroup with
    // empty member_id gets a broker-generated id and tells the client to
    // retry with it. Capture that id and call again to complete the
    // join.
    let bootstrap = retry_join_group_until_allowed(addr, "alice", b"wonderland", "cg-1")
        .await
        .expect("JoinGroup retry must round-trip");
    assert!(
        bootstrap.error_code == ERR_MEMBER_ID_REQUIRED,
        "first authorized JoinGroup must return MEMBER_ID_REQUIRED (79) with a generated member_id, got {bootstrap:?}"
    );
    assert!(
        !bootstrap.member_id.is_empty(),
        "broker must return a non-empty generated member_id on MEMBER_ID_REQUIRED"
    );

    // Second call with the generated member_id should complete the
    // rebalance (single-member group) and return error_code=0.
    let mut req2 = join_group_request("cg-1");
    req2.member_id = bootstrap.member_id;
    let joined = drive_join_group_as_plain(addr, "alice", b"wonderland", req2)
        .await
        .expect("second JoinGroup must round-trip");
    handle.shutdown().await;

    assert!(
        joined.error_code == 0,
        "JoinGroup must succeed with alice's Group Read ACL on cg-1, got {joined:?}"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn init_producer_id_denied_without_txn_acl() {
    let log_dir = tempfile::tempdir().unwrap();
    let cfg = sasl_plain_broker_config(
        log_dir.path(),
        &[("admin", "admin-secret"), ("alice", "wonderland")],
        Some("admin"),
    );

    let handle = Broker::start(cfg).await.expect("broker must start");
    let addr = handle.listen_addr();

    // Seed a dummy ACL to disable the compat shim.
    handle
        .submit_metadata_record_for_test(crabka_metadata::MetadataRecord::V1AccessControlEntry(
            crabka_metadata::AclEntry {
                resource_type: crabka_metadata::ResourceType::Topic,
                resource_name: "_nothing".into(),
                pattern_type: crabka_metadata::PatternType::Literal,
                principal: "User:admin".into(),
                host: "*".into(),
                operation: crabka_metadata::AclOperation::Read,
                permission_type: crabka_metadata::PermissionType::Allow,
            },
        ))
        .await
        .expect("seed dummy ACL");

    let req = InitProducerIdRequest {
        transactional_id: Some("tx-1".to_string()),
        transaction_timeout_ms: 60_000,
        producer_id: -1,
        producer_epoch: -1,
        ..Default::default()
    };
    let resp = drive_init_producer_id_as_plain(addr, "alice", b"wonderland", req)
        .await
        .expect("InitProducerId must round-trip");
    handle.shutdown().await;

    assert!(
        resp.error_code == ERR_TRANSACTIONAL_ID_AUTHORIZATION_FAILED,
        "alice has no TransactionalId Write ACL on tx-1, expected TRANSACTIONAL_ID_AUTHORIZATION_FAILED (53), got {resp:?}"
    );
}

// ────────────────────────────────────────────────────────────────────────
// Operation-implication + multi-super-user integration tests.
//
// These cover the end-to-end implications: a Read or Write ACL on a topic
// also grants Describe (so Metadata-by-name no longer needs a separate
// Describe seed), and a CreateAcls request from any principal in the
// `super_users` set succeeds while a non-super principal still gets
// CLUSTER_AUTHORIZATION_FAILED per binding.
// ────────────────────────────────────────────────────────────────────────

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn implication_metadata_describes_after_read_acl() {
    let log_dir = tempfile::tempdir().unwrap();
    let cfg = sasl_plain_broker_config(
        log_dir.path(),
        &[("admin", "admin-secret"), ("alice", "wonderland")],
        Some("admin"),
    );

    let handle = Broker::start(cfg).await.expect("broker must start");
    let addr = handle.listen_addr();

    create_topic_as_admin(addr, "foo", 1).await;

    // Seed Allow READ Topic LITERAL "foo" User:alice host=*. No explicit
    // Describe ACL — relies on the Read→Describe implication for
    // the Metadata-by-name visibility check.
    handle
        .submit_metadata_record_for_test(crabka_metadata::MetadataRecord::V1AccessControlEntry(
            crabka_metadata::AclEntry {
                resource_type: crabka_metadata::ResourceType::Topic,
                resource_name: "foo".into(),
                pattern_type: crabka_metadata::PatternType::Literal,
                principal: "User:alice".into(),
                host: "*".into(),
                operation: crabka_metadata::AclOperation::Read,
                permission_type: crabka_metadata::PermissionType::Allow,
            },
        ))
        .await
        .expect("seed Read-on-foo ACL for alice");

    // Wait for raft commit-then-apply, then ask Metadata for foo by name.
    // Pre-13b would have returned TOPIC_AUTHORIZATION_FAILED (29).
    let resp = retry_metadata_until_topic_visible(
        addr,
        "alice",
        b"wonderland",
        "foo",
        Some(vec!["foo".to_string()]),
    )
    .await
    .expect("Metadata must round-trip");
    handle.shutdown().await;

    assert!(resp.topics.len() == 1, "one topic row in response");
    let row = &resp.topics[0];
    assert!(row.name.as_deref() == Some("foo"));
    assert!(
        row.error_code == 0,
        "Read implies Describe, foo must be visible to alice with error_code=0, got {row:?}"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn implication_metadata_describes_after_write_acl() {
    let log_dir = tempfile::tempdir().unwrap();
    let cfg = sasl_plain_broker_config(
        log_dir.path(),
        &[("admin", "admin-secret"), ("alice", "wonderland")],
        Some("admin"),
    );

    let handle = Broker::start(cfg).await.expect("broker must start");
    let addr = handle.listen_addr();

    create_topic_as_admin(addr, "foo", 1).await;

    // Seed Allow WRITE Topic LITERAL "foo" User:alice host=*. No explicit
    // Describe ACL — relies on the Write→Describe implication.
    handle
        .submit_metadata_record_for_test(crabka_metadata::MetadataRecord::V1AccessControlEntry(
            crabka_metadata::AclEntry {
                resource_type: crabka_metadata::ResourceType::Topic,
                resource_name: "foo".into(),
                pattern_type: crabka_metadata::PatternType::Literal,
                principal: "User:alice".into(),
                host: "*".into(),
                operation: crabka_metadata::AclOperation::Write,
                permission_type: crabka_metadata::PermissionType::Allow,
            },
        ))
        .await
        .expect("seed Write-on-foo ACL for alice");

    let resp = retry_metadata_until_topic_visible(
        addr,
        "alice",
        b"wonderland",
        "foo",
        Some(vec!["foo".to_string()]),
    )
    .await
    .expect("Metadata must round-trip");
    handle.shutdown().await;

    assert!(resp.topics.len() == 1, "one topic row in response");
    let row = &resp.topics[0];
    assert!(row.name.as_deref() == Some("foo"));
    assert!(
        row.error_code == 0,
        "Write implies Describe, foo must be visible to alice with error_code=0, got {row:?}"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn multi_super_user_both_can_provision() {
    let log_dir = tempfile::tempdir().unwrap();
    // Two super-users: admin + ops-bot. alice has PLAIN creds but is NOT
    // in the super-users set, so her CreateAcls must hit the cluster gate.
    let cfg = sasl_plain_broker_config_multi_super(
        log_dir.path(),
        &[
            ("admin", "admin-secret"),
            ("ops-bot", "ops-secret"),
            ("alice", "wonderland"),
        ],
        &["admin", "ops-bot"],
    );

    let handle = Broker::start(cfg).await.expect("broker must start");
    let addr = handle.listen_addr();

    // admin (super-user #1) must succeed.
    let admin_req = CreateAclsRequest {
        creations: vec![topic_allow_creation("t-admin", "User:bob", OPERATION_READ)],
        ..Default::default()
    };
    let admin_resp = drive_create_acls_as_plain(addr, "admin", b"admin-secret", admin_req)
        .await
        .expect("CreateAcls as admin must round-trip");
    assert!(admin_resp.results.len() == 1);
    assert!(
        admin_resp.results[0].error_code == 0,
        "admin is a super-user, CreateAcls must succeed: {:?}",
        admin_resp.results[0]
    );

    // ops-bot (super-user #2) must also succeed.
    let ops_req = CreateAclsRequest {
        creations: vec![topic_allow_creation("t-ops", "User:carol", OPERATION_WRITE)],
        ..Default::default()
    };
    let ops_resp = drive_create_acls_as_plain(addr, "ops-bot", b"ops-secret", ops_req)
        .await
        .expect("CreateAcls as ops-bot must round-trip");
    assert!(ops_resp.results.len() == 1);
    assert!(
        ops_resp.results[0].error_code == 0,
        "ops-bot is a super-user, CreateAcls must succeed: {:?}",
        ops_resp.results[0]
    );

    // alice (not in super-set, no Cluster Alter ACL) must be denied per
    // binding with CLUSTER_AUTHORIZATION_FAILED (31).
    let alice_req = CreateAclsRequest {
        creations: vec![
            topic_allow_creation("t-x", "User:dave", OPERATION_READ),
            topic_allow_creation("t-y", "User:eve", OPERATION_WRITE),
        ],
        ..Default::default()
    };
    let alice_resp = drive_create_acls_as_plain(addr, "alice", b"wonderland", alice_req)
        .await
        .expect("CreateAcls request must round-trip even when denied");
    handle.shutdown().await;

    assert!(alice_resp.results.len() == 2);
    for (i, r) in alice_resp.results.iter().enumerate() {
        assert!(
            r.error_code == 31, /* CLUSTER_AUTHORIZATION_FAILED */
            "binding {i} must be denied for alice (not in super_users), got {r:?}"
        );
    }
}

// ────────────────────────────────────────────────────────────────────────
// T24 helpers — Metadata / JoinGroup / InitProducerId drivers and a
// couple of retry-until-allowed polls. All three share
// `sasl_plain_authenticate` + `round_trip` (defined further down).
// ────────────────────────────────────────────────────────────────────────

async fn drive_metadata_as_plain(
    addr: SocketAddr,
    user: &str,
    password: &[u8],
    req: MetadataRequest,
) -> Result<MetadataResponse, io::Error> {
    let mut stream = sasl_plain_authenticate(addr, user, password).await?;
    let mut body = BytesMut::new();
    req.encode(&mut body, METADATA_VERSION)
        .map_err(|e| io::Error::other(format!("Metadata encode: {e}")))?;
    let resp_bytes = round_trip(&mut stream, 3, METADATA_VERSION, 4, true, &body).await?;
    let mut cur: &[u8] = &resp_bytes;
    MetadataResponse::decode(&mut cur, METADATA_VERSION)
        .map_err(|e| io::Error::other(format!("Metadata decode: {e}")))
}

async fn drive_join_group_as_plain(
    addr: SocketAddr,
    user: &str,
    password: &[u8],
    req: JoinGroupRequest,
) -> Result<JoinGroupResponse, io::Error> {
    let mut stream = sasl_plain_authenticate(addr, user, password).await?;
    let mut body = BytesMut::new();
    req.encode(&mut body, JOIN_GROUP_VERSION)
        .map_err(|e| io::Error::other(format!("JoinGroup encode: {e}")))?;
    let resp_bytes = round_trip(&mut stream, 11, JOIN_GROUP_VERSION, 4, true, &body).await?;
    let mut cur: &[u8] = &resp_bytes;
    JoinGroupResponse::decode(&mut cur, JOIN_GROUP_VERSION)
        .map_err(|e| io::Error::other(format!("JoinGroup decode: {e}")))
}

async fn drive_init_producer_id_as_plain(
    addr: SocketAddr,
    user: &str,
    password: &[u8],
    req: InitProducerIdRequest,
) -> Result<InitProducerIdResponse, io::Error> {
    let mut stream = sasl_plain_authenticate(addr, user, password).await?;
    let mut body = BytesMut::new();
    req.encode(&mut body, INIT_PRODUCER_ID_VERSION)
        .map_err(|e| io::Error::other(format!("InitProducerId encode: {e}")))?;
    let resp_bytes = round_trip(&mut stream, 22, INIT_PRODUCER_ID_VERSION, 4, true, &body).await?;
    let mut cur: &[u8] = &resp_bytes;
    InitProducerIdResponse::decode(&mut cur, INIT_PRODUCER_ID_VERSION)
        .map_err(|e| io::Error::other(format!("InitProducerId decode: {e}")))
}

/// Build a single-protocol JoinGroup request with an empty `member_id`
/// (so the broker will first respond with MEMBER_ID_REQUIRED + a
/// generated id), proposing the `range` assignor (the only one the
/// broker negotiates in MVP).
fn join_group_request(group_id: &str) -> JoinGroupRequest {
    JoinGroupRequest {
        group_id: group_id.to_string(),
        session_timeout_ms: 30_000,
        rebalance_timeout_ms: 60_000,
        member_id: String::new(),
        group_instance_id: None,
        protocol_type: "consumer".to_string(),
        protocols: vec![JoinGroupRequestProtocol {
            name: "range".to_string(),
            metadata: bytes::Bytes::new(),
            ..Default::default()
        }],
        ..Default::default()
    }
}

/// Retry `drive_metadata_as_plain` until `topic` shows up in the
/// response (i.e. the Allow Describe ACL has been applied) or a 10 s
/// deadline elapses. `req_topics` is forwarded as-is to the inner
/// `MetadataRequest::topics` so callers can poll either the fetch-all
/// or named-topic path.
async fn retry_metadata_until_topic_visible(
    addr: SocketAddr,
    user: &str,
    password: &[u8],
    topic: &str,
    req_topics: Option<Vec<String>>,
) -> Result<MetadataResponse, io::Error> {
    let req = MetadataRequest {
        topics: req_topics.as_ref().map(|names| {
            names
                .iter()
                .map(|n| MetadataRequestTopic {
                    name: Some(n.clone()),
                    ..Default::default()
                })
                .collect()
        }),
        ..Default::default()
    };
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(10);
    loop {
        let resp = drive_metadata_as_plain(addr, user, password, req.clone()).await?;
        let visible = resp.topics.iter().any(|t| t.name.as_deref() == Some(topic));
        if visible {
            return Ok(resp);
        }
        if std::time::Instant::now() > deadline {
            return Ok(resp);
        }
        // intentional: bounded RPC-response poll. The awaited signal is the
        // broker's authorization decision (topic visible to alice), observed by
        // re-driving Metadata — not an image/metric an awaiter exposes.
        // wait_for_image watches the controller's committed image, which can
        // lead the request path's applied copy, so an image wait would race;
        // re-driving absorbs the raft commit-then-apply gap.
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
    }
}

/// Retry `drive_join_group_as_plain` against `group_id` (empty
/// member_id) until the response stops being `GROUP_AUTHORIZATION_FAILED`
/// (i.e. the Allow Read ACL has been applied) or a 10 s deadline
/// elapses. The next code in the success ladder is
/// `MEMBER_ID_REQUIRED (79)`; the caller follows up with the generated
/// `member_id` to actually complete the join.
async fn retry_join_group_until_allowed(
    addr: SocketAddr,
    user: &str,
    password: &[u8],
    group_id: &str,
) -> Result<JoinGroupResponse, io::Error> {
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(10);
    loop {
        let resp =
            drive_join_group_as_plain(addr, user, password, join_group_request(group_id)).await?;
        if resp.error_code != ERR_GROUP_AUTHORIZATION_FAILED {
            return Ok(resp);
        }
        if std::time::Instant::now() > deadline {
            return Ok(resp);
        }
        // intentional: bounded RPC-response poll. The awaited signal is the
        // broker's authorization decision (JoinGroup no longer denied),
        // observed by re-driving JoinGroup — not an image/metric an awaiter
        // exposes. wait_for_image watches the controller's committed image,
        // which can lead the request path's applied copy, so an image wait
        // would race; re-driving absorbs the raft commit-then-apply gap.
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
    }
}

// ────────────────────────────────────────────────────────────────────────
// SASL/PLAIN + ACL wire helpers.
//
// Same shape as `drive_alter_user_scram_credentials_as_plain`:
// one ApiVersions warm-up, one SaslHandshake, one SaslAuthenticate, then
// the typed ACL request. Each helper authenticates fresh on a new TCP
// stream because that's the simplest model for "a client doing one
// admin action"; reuse is unnecessary for these tests.
// ────────────────────────────────────────────────────────────────────────

async fn drive_create_acls_as_plain(
    addr: SocketAddr,
    user: &str,
    password: &[u8],
    req: CreateAclsRequest,
) -> Result<CreateAclsResponse, io::Error> {
    let mut stream = sasl_plain_authenticate(addr, user, password).await?;
    let mut body = BytesMut::new();
    req.encode(&mut body, CREATE_ACLS_VERSION)
        .map_err(|e| io::Error::other(format!("CreateAcls encode: {e}")))?;
    let resp_bytes = round_trip(&mut stream, 30, CREATE_ACLS_VERSION, 4, true, &body).await?;
    let mut cur: &[u8] = &resp_bytes;
    CreateAclsResponse::decode(&mut cur, CREATE_ACLS_VERSION)
        .map_err(|e| io::Error::other(format!("CreateAcls decode: {e}")))
}

async fn drive_describe_acls_as_plain(
    addr: SocketAddr,
    user: &str,
    password: &[u8],
    req: DescribeAclsRequest,
) -> Result<DescribeAclsResponse, io::Error> {
    let mut stream = sasl_plain_authenticate(addr, user, password).await?;
    let mut body = BytesMut::new();
    req.encode(&mut body, DESCRIBE_ACLS_VERSION)
        .map_err(|e| io::Error::other(format!("DescribeAcls encode: {e}")))?;
    let resp_bytes = round_trip(&mut stream, 29, DESCRIBE_ACLS_VERSION, 4, true, &body).await?;
    let mut cur: &[u8] = &resp_bytes;
    DescribeAclsResponse::decode(&mut cur, DESCRIBE_ACLS_VERSION)
        .map_err(|e| io::Error::other(format!("DescribeAcls decode: {e}")))
}

async fn drive_delete_acls_as_plain(
    addr: SocketAddr,
    user: &str,
    password: &[u8],
    req: DeleteAclsRequest,
) -> Result<DeleteAclsResponse, io::Error> {
    let mut stream = sasl_plain_authenticate(addr, user, password).await?;
    let mut body = BytesMut::new();
    req.encode(&mut body, DELETE_ACLS_VERSION)
        .map_err(|e| io::Error::other(format!("DeleteAcls encode: {e}")))?;
    let resp_bytes = round_trip(&mut stream, 31, DELETE_ACLS_VERSION, 4, true, &body).await?;
    let mut cur: &[u8] = &resp_bytes;
    DeleteAclsResponse::decode(&mut cur, DELETE_ACLS_VERSION)
        .map_err(|e| io::Error::other(format!("DeleteAcls decode: {e}")))
}

/// Open a TCP stream to `addr` and drive `ApiVersions` → `SaslHandshake(PLAIN)`
/// → `SaslAuthenticate(\0user\0password)`. Returns the authenticated stream
/// for the caller to issue follow-up requests on. Mirrors the first three
/// steps of `drive_sasl_plain_session` in `auth_handlers.rs`.
async fn sasl_plain_authenticate(
    addr: SocketAddr,
    user: &str,
    password: &[u8],
) -> Result<TcpStream, io::Error> {
    let mut stream = TcpStream::connect(addr).await?;

    // ── 1. ApiVersions (v0, non-flexible).
    let av_req = ApiVersionsRequest::default();
    let mut av_body = BytesMut::new();
    av_req
        .encode(&mut av_body, 0)
        .map_err(|e| io::Error::other(format!("ApiVersions encode: {e}")))?;
    let av_resp_bytes = round_trip(&mut stream, 18, 0, 1, false, &av_body).await?;
    let mut cur: &[u8] = &av_resp_bytes;
    let _av_resp = ApiVersionsResponse::decode(&mut cur, 0)
        .map_err(|e| io::Error::other(format!("ApiVersions decode: {e}")))?;

    // ── 2. SaslHandshake v1 (non-flexible, mechanism="PLAIN").
    let mut sh_body = BytesMut::new();
    SaslHandshakeRequest {
        mechanism: "PLAIN".to_string(),
        ..Default::default()
    }
    .encode(&mut sh_body, 1)
    .map_err(|e| io::Error::other(format!("SaslHandshake encode: {e}")))?;
    let sh_resp_bytes = round_trip(&mut stream, 17, 1, 2, false, &sh_body).await?;
    let mut cur: &[u8] = &sh_resp_bytes;
    let sh_resp = SaslHandshakeResponse::decode(&mut cur, 1)
        .map_err(|e| io::Error::other(format!("SaslHandshake decode: {e}")))?;
    if sh_resp.error_code != 0 {
        return Err(io::Error::other(format!(
            "SaslHandshake failed: error_code={}",
            sh_resp.error_code
        )));
    }

    // ── 3. SaslAuthenticate v2 (flexible). auth_bytes = \0user\0password.
    let mut payload = Vec::with_capacity(2 + user.len() + password.len());
    payload.push(0); // authzid (empty)
    payload.extend_from_slice(user.as_bytes());
    payload.push(0);
    payload.extend_from_slice(password);
    let mut auth_body = BytesMut::new();
    SaslAuthenticateRequest {
        auth_bytes: bytes::Bytes::from(payload),
        ..Default::default()
    }
    .encode(&mut auth_body, 2)
    .map_err(|e| io::Error::other(format!("SaslAuthenticate encode: {e}")))?;
    let auth_resp_bytes = round_trip(&mut stream, 36, 2, 3, true, &auth_body).await?;
    let mut cur: &[u8] = &auth_resp_bytes;
    let auth_resp = SaslAuthenticateResponse::decode(&mut cur, 2)
        .map_err(|e| io::Error::other(format!("SaslAuthenticate decode: {e}")))?;
    if auth_resp.error_code != 0 {
        return Err(io::Error::other(format!(
            "SaslAuthenticate failed: error_code={} error_message={:?}",
            auth_resp.error_code, auth_resp.error_message
        )));
    }

    Ok(stream)
}

/// Same shape as `auth_handlers::round_trip`. Encodes a request header
/// (v1 non-flexible / v2 flexible), prepends a 4-byte length prefix,
/// writes the frame, reads one response frame and strips the response
/// header (v0 for ApiVersions or any non-flexible response, v1 with a
/// trailing tagged-fields byte for every other flexible response).
async fn round_trip(
    stream: &mut TcpStream,
    api_key: i16,
    api_version: i16,
    corr_id: i32,
    flexible: bool,
    body: &[u8],
) -> Result<Vec<u8>, io::Error> {
    let mut frame = BytesMut::with_capacity(16 + body.len());
    frame.put_i16(api_key);
    frame.put_i16(api_version);
    frame.put_i32(corr_id);
    let client_id = "crabka-acl-test";
    frame.put_i16(i16::try_from(client_id.len()).expect("client_id fits in i16"));
    frame.put_slice(client_id.as_bytes());
    if flexible {
        frame.put_u8(0); // empty header tagged-fields byte
    }
    frame.put_slice(body);

    stream
        .write_u32(u32::try_from(frame.len()).expect("frame size fits in u32"))
        .await?;
    stream.write_all(&frame).await?;
    stream.flush().await?;

    let resp_len = stream.read_u32().await?;
    let mut resp = vec![0u8; resp_len as usize];
    stream.read_exact(&mut resp).await?;

    let mut cur = &resp[..];
    let _resp_corr_id = cur.get_i32();
    let uses_v1_header = flexible && api_key != 18;
    if uses_v1_header {
        if cur.is_empty() {
            return Err(io::Error::other(
                "flexible response missing tagged-fields byte",
            ));
        }
        let _tagged = cur.get_u8();
    }
    Ok(cur.to_vec())
}
