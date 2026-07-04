#![allow(clippy::duration_suboptimal_units)]

use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

use crabka_admin_ui::auth::{LoginBroker, LoginRequest};
use crabka_admin_ui::config::{AdminUiConfig, BrokerSecurityConfig};
use crabka_admin_ui::dto::{
    AclRequestDto, AlterConfigRequestDto, ConfigEntryDto, CreatePartitionsRequestDto,
    CreateTopicRequestDto, DeleteTopicRequestDto, GroupRow, LogDirMoveRequestDto, LogDirRow,
    QuotaDeleteDto, QuotaUpsertDto, ResourceOutcome, ScramUserDeleteDto, ScramUserUpsertDto,
    TopicRow,
};
use crabka_admin_ui::error::UiError;
use crabka_admin_ui::server::AppState;
use crabka_admin_ui::server_fns::{
    AdminMutationSeam, AdminReadSeam, AdminSeamFactory, ServerFunctionContext,
};
use crabka_admin_ui::session::{SessionCredentials, SessionRecord, SessionStore, SessionUser};
use std::future::Future;
use std::pin::Pin;

const LOGIN_PASSWORD_SENTINEL: &str = "server-fn-password-sentinel";
const SESSION_SENTINEL: &str = "server-fn-session-sentinel";

#[test]
fn app_state_carries_config_and_sessions() {
    let cfg = AdminUiConfig {
        cluster_name: "task-six-cluster".to_string(),
        bootstrap_addrs: vec!["127.0.0.1:9092".to_string()],
        security: BrokerSecurityConfig::SaslPlaintext,
        session_ttl_seconds: 37,
        ..AdminUiConfig::default()
    };

    let state = AppState::new(cfg.clone());

    assert_eq!(state.cfg.cluster_name, "task-six-cluster");
    assert_eq!(state.cfg.bootstrap_addrs, ["127.0.0.1:9092"]);
    assert_eq!(state.sessions_ttl_seconds(), 37);

    let sessions = Arc::new(SessionStore::new(Duration::from_secs(5)));
    let state = AppState::from_parts(Arc::new(cfg), sessions);

    assert_eq!(state.cfg.cluster_name, "task-six-cluster");
    assert_eq!(state.sessions_ttl_seconds(), 5);
}

#[tokio::test]
async fn login_seam_rejects_without_exposing_password() {
    let result = crabka_admin_ui::server_fns::login(LoginRequest {
        username: "alice".to_string(),
        password: LOGIN_PASSWORD_SENTINEL.to_string(),
    })
    .await;

    assert!(result.is_err());
    assert_debug_does_not_contain_secret(
        &format_result_debug(&result),
        LOGIN_PASSWORD_SENTINEL,
        "password",
    );
}

#[tokio::test]
async fn login_with_context_calls_broker_and_stores_session() {
    let sessions = SessionStore::new(Duration::from_secs(60));
    let broker = RecordingLoginBroker::default();
    let cfg = AdminUiConfig {
        bootstrap_addrs: vec!["127.0.0.1:9092".to_string()],
        ..AdminUiConfig::default()
    };

    let success = crabka_admin_ui::server_fns::login_with_context(
        &cfg,
        &sessions,
        &broker,
        LoginRequest {
            username: "alice".to_string(),
            password: LOGIN_PASSWORD_SENTINEL.to_string(),
        },
    )
    .await
    .expect("login succeeds through injected broker");

    assert_eq!(broker.calls.load(Ordering::SeqCst), 1);
    assert_eq!(success.username, "alice");
    assert_eq!(success.principal, "User:alice");
    let current_session = crabka_admin_ui::server_fns::current_session_with_store(
        &sessions,
        Some(&success.session_id),
    )
    .expect("session exists after successful login");
    assert_eq!(current_session.username, "alice");
    assert_debug_does_not_contain_secret(
        &format_result_debug(&Ok::<_, UiError>(success)),
        LOGIN_PASSWORD_SENTINEL,
        "password",
    );
}

#[tokio::test]
async fn session_seams_reject_without_exposing_session_values() {
    let logout_result = crabka_admin_ui::server_fns::logout().await;
    let current_session_result = crabka_admin_ui::server_fns::current_session().await;

    assert!(matches!(logout_result, Err(UiError::NotAuthenticated)));
    assert!(matches!(
        current_session_result,
        Err(UiError::NotAuthenticated)
    ));
    assert_debug_does_not_contain_secret(
        &format!("{logout_result:?} {current_session_result:?}"),
        SESSION_SENTINEL,
        "session value",
    );
}

#[test]
fn current_session_with_store_rejects_missing_invalid_and_expired_sessions() {
    let valid_store = SessionStore::new(Duration::from_secs(60));
    let expired_store = SessionStore::new(Duration::ZERO);
    let valid_id = valid_store.create_user("alice", "User:alice");
    let expired_id = expired_store.create_user("bob", "User:bob");

    let valid_session = crabka_admin_ui::server_fns::current_session_with_store(
        &valid_store,
        Some(valid_id.expose_for_cookie()),
    )
    .expect("valid session resolves");

    assert_eq!(valid_session.username, "alice");
    assert!(matches!(
        crabka_admin_ui::server_fns::current_session_with_store(&valid_store, None),
        Err(UiError::NotAuthenticated)
    ));
    assert!(matches!(
        crabka_admin_ui::server_fns::current_session_with_store(&valid_store, Some("not-a-uuid")),
        Err(UiError::NotAuthenticated)
    ));
    assert!(matches!(
        crabka_admin_ui::server_fns::current_session_with_store(
            &expired_store,
            Some(expired_id.expose_for_cookie()),
        ),
        Err(UiError::NotAuthenticated)
    ));
}

#[tokio::test]
async fn resource_seams_are_callable_and_require_authentication() {
    let topics = crabka_admin_ui::server_fns::list_topics().await;
    let groups = crabka_admin_ui::server_fns::list_groups().await;
    let acls = crabka_admin_ui::server_fns::list_acls().await;
    let users = crabka_admin_ui::server_fns::list_users().await;
    let quotas = crabka_admin_ui::server_fns::list_quotas().await;
    let log_dirs = crabka_admin_ui::server_fns::list_log_dirs().await;

    assert!(matches!(topics, Err(UiError::NotAuthenticated)));
    assert!(matches!(groups, Err(UiError::NotAuthenticated)));
    assert!(matches!(acls, Err(UiError::NotAuthenticated)));
    assert!(matches!(users, Err(UiError::NotAuthenticated)));
    assert!(matches!(quotas, Err(UiError::NotAuthenticated)));
    assert!(matches!(log_dirs, Err(UiError::NotAuthenticated)));
}

#[tokio::test]
async fn authenticated_read_seams_validate_session_and_call_admin_reader() {
    let sessions = SessionStore::new(Duration::from_secs(60));
    let session_id = sessions.create_user("alice", "User:alice");
    let reader = RecordingAdminReadSeam::default();

    let topics = crabka_admin_ui::server_fns::list_topics_with_reader(
        &sessions,
        Some(session_id.expose_for_cookie()),
        &reader,
    )
    .await
    .expect("topics read succeeds");
    let groups = crabka_admin_ui::server_fns::list_groups_with_reader(
        &sessions,
        Some(session_id.expose_for_cookie()),
        &reader,
    )
    .await
    .expect("groups read succeeds");
    let log_dirs = crabka_admin_ui::server_fns::list_log_dirs_with_reader(
        &sessions,
        Some(session_id.expose_for_cookie()),
        &reader,
    )
    .await
    .expect("log dirs read succeeds");

    assert_eq!(topics[0].name, "orders");
    assert_eq!(groups[0].group_id, "consumer-a");
    assert_eq!(log_dirs[0].log_dir, "/var/lib/crabka");
    assert_eq!(reader.topics.load(Ordering::SeqCst), 1);
    assert_eq!(reader.groups.load(Ordering::SeqCst), 1);
    assert_eq!(reader.log_dirs.load(Ordering::SeqCst), 1);
    assert!(matches!(
        crabka_admin_ui::server_fns::list_topics_with_reader(&sessions, None, &reader).await,
        Err(UiError::NotAuthenticated)
    ));
}

#[tokio::test]
async fn public_context_reads_validate_session_and_call_admin_reader() {
    let sessions = SessionStore::new(Duration::from_secs(60));
    let session_id = authenticated_session(&sessions);
    let cfg = AdminUiConfig::default();
    let factory = RecordingAdminSeamFactory::default();
    let context = ServerFunctionContext::new(
        &cfg,
        &sessions,
        Some(session_id.expose_for_cookie()),
        &factory,
    );

    let topics = crabka_admin_ui::server_fns::list_topics_with_context(&context)
        .await
        .expect("topics public context read succeeds");
    let groups = crabka_admin_ui::server_fns::list_groups_with_context(&context)
        .await
        .expect("groups public context read succeeds");
    let log_dirs = crabka_admin_ui::server_fns::list_log_dirs_with_context(&context)
        .await
        .expect("log dirs public context read succeeds");

    assert_eq!(topics[0].name, "orders");
    assert_eq!(groups[0].group_id, "consumer-a");
    assert_eq!(log_dirs[0].log_dir, "/var/lib/crabka");
    assert_eq!(factory.read_seam_calls.load(Ordering::SeqCst), 3);
    assert_eq!(factory.reader.topics.load(Ordering::SeqCst), 1);
    assert_eq!(factory.reader.groups.load(Ordering::SeqCst), 1);
    assert_eq!(factory.reader.log_dirs.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn public_context_current_session_uses_exported_context_path() {
    let sessions = SessionStore::new(Duration::from_secs(60));
    let session_id = authenticated_session(&sessions);
    let cfg = AdminUiConfig::default();
    let factory = RecordingAdminSeamFactory::default();
    let context = ServerFunctionContext::new(
        &cfg,
        &sessions,
        Some(session_id.expose_for_cookie()),
        &factory,
    );

    let current_session = crabka_admin_ui::server_fns::current_session_with_context(&context)
        .expect("public context current session resolves");

    assert_eq!(current_session.username, "alice");
    assert_eq!(current_session.principal, "User:alice");
    assert_eq!(factory.read_seam_calls.load(Ordering::SeqCst), 0);
}

#[tokio::test]
async fn public_context_reads_reject_unauthenticated_sessions() {
    let sessions = SessionStore::new(Duration::from_secs(60));
    let cfg = AdminUiConfig::default();
    let factory = RecordingAdminSeamFactory::default();
    let context = ServerFunctionContext::new(&cfg, &sessions, None, &factory);

    assert!(matches!(
        crabka_admin_ui::server_fns::list_topics_with_context(&context).await,
        Err(UiError::NotAuthenticated)
    ));
    assert!(matches!(
        crabka_admin_ui::server_fns::list_groups_with_context(&context).await,
        Err(UiError::NotAuthenticated)
    ));
    assert!(matches!(
        crabka_admin_ui::server_fns::list_log_dirs_with_context(&context).await,
        Err(UiError::NotAuthenticated)
    ));
    assert_eq!(factory.read_seam_calls.load(Ordering::SeqCst), 0);
}

#[tokio::test]
async fn mutation_seams_validate_requests_before_requiring_authentication() {
    let invalid_topic = crabka_admin_ui::server_fns::create_topic(CreateTopicRequestDto {
        name: "orders".to_string(),
        partitions: 0,
        replicas: 1,
        configs: Vec::new(),
    })
    .await;
    let invalid_scram = crabka_admin_ui::server_fns::upsert_scram_sha512_user(ScramUserUpsertDto {
        username: "alice".to_string(),
        password: String::new(),
        iterations: 4096,
    })
    .await;

    assert!(matches!(invalid_topic, Err(UiError::Admin(_))));
    assert!(matches!(invalid_scram, Err(UiError::Admin(_))));
}

#[tokio::test]
async fn mutation_seams_require_authentication_after_validation_without_leaking_password() {
    let password_sentinel = "server-fn-scram-password-sentinel";
    let create_topic = crabka_admin_ui::server_fns::create_topic(CreateTopicRequestDto {
        name: "orders".to_string(),
        partitions: 3,
        replicas: 1,
        configs: Vec::new(),
    })
    .await;
    let upsert_scram = crabka_admin_ui::server_fns::upsert_scram_sha512_user(ScramUserUpsertDto {
        username: "alice".to_string(),
        password: password_sentinel.to_string(),
        iterations: 4096,
    })
    .await;

    assert!(matches!(create_topic, Err(UiError::NotAuthenticated)));
    assert!(matches!(upsert_scram, Err(UiError::NotAuthenticated)));
    assert_debug_does_not_contain_secret(
        &format!("{upsert_scram:?}"),
        password_sentinel,
        "SCRAM password",
    );
}

#[tokio::test]
async fn authenticated_mutation_seam_validates_then_calls_admin_mutation() {
    let sessions = SessionStore::new(Duration::from_secs(60));
    let session_id = sessions.create_user("alice", "User:alice");
    let mutations = RecordingAdminMutationSeam::default();

    let outcomes = crabka_admin_ui::server_fns::create_topic_with_mutations(
        &sessions,
        Some(session_id.expose_for_cookie()),
        &mutations,
        CreateTopicRequestDto {
            name: "orders".to_string(),
            partitions: 3,
            replicas: 1,
            configs: Vec::new(),
        },
    )
    .await
    .expect("authenticated mutation succeeds");

    assert_eq!(outcomes, vec![ResourceOutcome::ok("orders")]);
    assert_eq!(mutations.create_topic.load(Ordering::SeqCst), 1);
    assert!(matches!(
        crabka_admin_ui::server_fns::create_topic_with_mutations(
            &sessions,
            None,
            &mutations,
            CreateTopicRequestDto {
                name: "orders".to_string(),
                partitions: 3,
                replicas: 1,
                configs: Vec::new(),
            },
        )
        .await,
        Err(UiError::NotAuthenticated)
    ));
}

#[tokio::test]
async fn public_context_create_topic_validates_session_and_calls_admin_mutation() {
    let sessions = SessionStore::new(Duration::from_secs(60));
    let session_id = authenticated_session(&sessions);
    let cfg = AdminUiConfig::default();
    let factory = RecordingAdminSeamFactory::default();
    let context = ServerFunctionContext::new(
        &cfg,
        &sessions,
        Some(session_id.expose_for_cookie()),
        &factory,
    );

    let outcomes = crabka_admin_ui::server_fns::create_topic_with_context(
        &context,
        CreateTopicRequestDto {
            name: "orders".to_string(),
            partitions: 3,
            replicas: 1,
            configs: Vec::new(),
        },
    )
    .await
    .expect("public context mutation succeeds");

    assert_eq!(outcomes, vec![ResourceOutcome::ok("orders")]);
    assert_eq!(factory.mutation_seam_calls.load(Ordering::SeqCst), 1);
    assert_eq!(factory.mutations.create_topic.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn public_context_create_topic_rejects_unauthenticated_after_validation() {
    let sessions = SessionStore::new(Duration::from_secs(60));
    let cfg = AdminUiConfig::default();
    let factory = RecordingAdminSeamFactory::default();
    let context = ServerFunctionContext::new(&cfg, &sessions, None, &factory);

    let result = crabka_admin_ui::server_fns::create_topic_with_context(
        &context,
        CreateTopicRequestDto {
            name: "orders".to_string(),
            partitions: 3,
            replicas: 1,
            configs: Vec::new(),
        },
    )
    .await;

    assert!(matches!(result, Err(UiError::NotAuthenticated)));
    assert_eq!(factory.mutation_seam_calls.load(Ordering::SeqCst), 0);
}

#[tokio::test]
async fn public_context_create_topic_validates_before_authentication() {
    let sessions = SessionStore::new(Duration::from_secs(60));
    let cfg = AdminUiConfig::default();
    let factory = RecordingAdminSeamFactory::default();
    let context = ServerFunctionContext::new(&cfg, &sessions, None, &factory);

    let result = crabka_admin_ui::server_fns::create_topic_with_context(
        &context,
        CreateTopicRequestDto {
            name: "orders".to_string(),
            partitions: 0,
            replicas: 1,
            configs: Vec::new(),
        },
    )
    .await;

    assert!(matches!(result, Err(UiError::Admin(_))));
    assert_eq!(factory.mutation_seam_calls.load(Ordering::SeqCst), 0);
}

#[tokio::test]
async fn logout_with_context_removes_authenticated_session() {
    let sessions = SessionStore::new(Duration::from_secs(60));
    let session_id = authenticated_session(&sessions);
    let cfg = AdminUiConfig::default();
    let factory = RecordingAdminSeamFactory::default();
    let context = ServerFunctionContext::new(
        &cfg,
        &sessions,
        Some(session_id.expose_for_cookie()),
        &factory,
    );

    crabka_admin_ui::server_fns::logout_with_context(&context)
        .await
        .expect("authenticated logout succeeds");

    assert!(sessions.get(&session_id).is_none());
    assert!(matches!(
        crabka_admin_ui::server_fns::current_session_with_store(
            &sessions,
            Some(session_id.expose_for_cookie())
        ),
        Err(UiError::NotAuthenticated)
    ));
}

#[tokio::test]
async fn public_context_mutations_validate_session_and_call_admin_mutation() {
    let sessions = SessionStore::new(Duration::from_secs(60));
    let session_id = authenticated_session(&sessions);
    let cfg = AdminUiConfig::default();
    let factory = RecordingAdminSeamFactory::default();
    let context = ServerFunctionContext::new(
        &cfg,
        &sessions,
        Some(session_id.expose_for_cookie()),
        &factory,
    );

    crabka_admin_ui::server_fns::delete_topic_with_context(&context, delete_topic_request())
        .await
        .expect("delete topic mutation succeeds");
    crabka_admin_ui::server_fns::create_partitions_with_context(
        &context,
        create_partitions_request(),
    )
    .await
    .expect("create partitions mutation succeeds");
    crabka_admin_ui::server_fns::alter_configs_with_context(&context, alter_config_request())
        .await
        .expect("alter config mutation succeeds");
    crabka_admin_ui::server_fns::create_acl_with_context(&context, acl_request())
        .await
        .expect("create ACL mutation succeeds");
    crabka_admin_ui::server_fns::delete_acl_with_context(&context, acl_request())
        .await
        .expect("delete ACL mutation succeeds");
    crabka_admin_ui::server_fns::upsert_scram_sha512_user_with_context(
        &context,
        scram_upsert_request(),
    )
    .await
    .expect("SCRAM upsert mutation succeeds");
    crabka_admin_ui::server_fns::delete_scram_user_with_context(&context, scram_delete_request())
        .await
        .expect("SCRAM delete mutation succeeds");
    crabka_admin_ui::server_fns::upsert_quota_with_context(&context, quota_upsert_request())
        .await
        .expect("quota upsert mutation succeeds");
    crabka_admin_ui::server_fns::delete_quota_with_context(&context, quota_delete_request())
        .await
        .expect("quota delete mutation succeeds");
    crabka_admin_ui::server_fns::move_log_dir_with_context(&context, log_dir_move_request())
        .await
        .expect("log-dir move mutation succeeds");

    assert_eq!(factory.mutation_seam_calls.load(Ordering::SeqCst), 10);
    assert_eq!(factory.mutations.delete_topic.load(Ordering::SeqCst), 1);
    assert_eq!(
        factory.mutations.create_partitions.load(Ordering::SeqCst),
        1
    );
    assert_eq!(factory.mutations.alter_configs.load(Ordering::SeqCst), 1);
    assert_eq!(factory.mutations.create_acl.load(Ordering::SeqCst), 1);
    assert_eq!(factory.mutations.delete_acl.load(Ordering::SeqCst), 1);
    assert_eq!(factory.mutations.upsert_scram.load(Ordering::SeqCst), 1);
    assert_eq!(factory.mutations.delete_scram.load(Ordering::SeqCst), 1);
    assert_eq!(factory.mutations.upsert_quota.load(Ordering::SeqCst), 1);
    assert_eq!(factory.mutations.delete_quota.load(Ordering::SeqCst), 1);
    assert_eq!(factory.mutations.move_log_dir.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn public_context_mutations_reject_unauthenticated_without_calling_admin_mutation() {
    let sessions = SessionStore::new(Duration::from_secs(60));
    let cfg = AdminUiConfig::default();
    let factory = RecordingAdminSeamFactory::default();
    let context = ServerFunctionContext::new(&cfg, &sessions, None, &factory);

    assert!(matches!(
        crabka_admin_ui::server_fns::delete_topic_with_context(&context, delete_topic_request())
            .await,
        Err(UiError::NotAuthenticated)
    ));
    assert!(matches!(
        crabka_admin_ui::server_fns::create_partitions_with_context(
            &context,
            create_partitions_request()
        )
        .await,
        Err(UiError::NotAuthenticated)
    ));
    assert!(matches!(
        crabka_admin_ui::server_fns::alter_configs_with_context(&context, alter_config_request())
            .await,
        Err(UiError::NotAuthenticated)
    ));
    assert!(matches!(
        crabka_admin_ui::server_fns::create_acl_with_context(&context, acl_request()).await,
        Err(UiError::NotAuthenticated)
    ));
    assert!(matches!(
        crabka_admin_ui::server_fns::delete_acl_with_context(&context, acl_request()).await,
        Err(UiError::NotAuthenticated)
    ));
    assert!(matches!(
        crabka_admin_ui::server_fns::upsert_scram_sha512_user_with_context(
            &context,
            scram_upsert_request()
        )
        .await,
        Err(UiError::NotAuthenticated)
    ));
    assert!(matches!(
        crabka_admin_ui::server_fns::delete_scram_user_with_context(
            &context,
            scram_delete_request()
        )
        .await,
        Err(UiError::NotAuthenticated)
    ));
    assert!(matches!(
        crabka_admin_ui::server_fns::upsert_quota_with_context(&context, quota_upsert_request())
            .await,
        Err(UiError::NotAuthenticated)
    ));
    assert!(matches!(
        crabka_admin_ui::server_fns::delete_quota_with_context(&context, quota_delete_request())
            .await,
        Err(UiError::NotAuthenticated)
    ));
    assert!(matches!(
        crabka_admin_ui::server_fns::move_log_dir_with_context(&context, log_dir_move_request())
            .await,
        Err(UiError::NotAuthenticated)
    ));

    assert_eq!(factory.mutation_seam_calls.load(Ordering::SeqCst), 0);
    assert_eq!(factory.mutations.total_calls(), 0);
}

#[derive(Default)]
struct RecordingLoginBroker {
    calls: AtomicUsize,
}

impl LoginBroker for RecordingLoginBroker {
    fn check_login<'a>(
        &'a self,
        _cfg: &'a AdminUiConfig,
        username: &'a str,
        password: &'a str,
    ) -> Pin<Box<dyn Future<Output = Result<(), UiError>> + Send + 'a>> {
        Box::pin(async move {
            assert_eq!(username, "alice");
            assert_eq!(password, LOGIN_PASSWORD_SENTINEL);
            self.calls.fetch_add(1, Ordering::SeqCst);
            Ok(())
        })
    }
}

#[derive(Default)]
struct RecordingAdminReadSeam {
    topics: AtomicUsize,
    groups: AtomicUsize,
    log_dirs: AtomicUsize,
}

impl AdminReadSeam for RecordingAdminReadSeam {
    fn topics<'a>(
        &'a self,
    ) -> Pin<Box<dyn Future<Output = Result<Vec<TopicRow>, UiError>> + Send + 'a>> {
        Box::pin(async move {
            self.topics.fetch_add(1, Ordering::SeqCst);
            Ok(vec![TopicRow {
                name: "orders".to_string(),
                topic_id: None,
                partition_count: 3,
                replication_factor: 1,
                error: None,
            }])
        })
    }

    fn groups<'a>(
        &'a self,
    ) -> Pin<Box<dyn Future<Output = Result<Vec<GroupRow>, UiError>> + Send + 'a>> {
        Box::pin(async move {
            self.groups.fetch_add(1, Ordering::SeqCst);
            Ok(vec![GroupRow {
                group_id: "consumer-a".to_string(),
            }])
        })
    }

    fn log_dirs<'a>(
        &'a self,
    ) -> Pin<Box<dyn Future<Output = Result<Vec<LogDirRow>, UiError>> + Send + 'a>> {
        Box::pin(async move {
            self.log_dirs.fetch_add(1, Ordering::SeqCst);
            Ok(vec![LogDirRow {
                log_dir: "/var/lib/crabka".to_string(),
                topic: "orders".to_string(),
                partition: 0,
                partition_size: 10,
                offset_lag: 0,
                is_future_key: false,
                error: None,
            }])
        })
    }
}

#[derive(Default)]
struct RecordingAdminMutationSeam {
    create_topic: AtomicUsize,
    delete_topic: AtomicUsize,
    create_partitions: AtomicUsize,
    alter_configs: AtomicUsize,
    create_acl: AtomicUsize,
    delete_acl: AtomicUsize,
    upsert_scram: AtomicUsize,
    delete_scram: AtomicUsize,
    upsert_quota: AtomicUsize,
    delete_quota: AtomicUsize,
    move_log_dir: AtomicUsize,
}

impl RecordingAdminMutationSeam {
    fn total_calls(&self) -> usize {
        self.create_topic.load(Ordering::SeqCst)
            + self.delete_topic.load(Ordering::SeqCst)
            + self.create_partitions.load(Ordering::SeqCst)
            + self.alter_configs.load(Ordering::SeqCst)
            + self.create_acl.load(Ordering::SeqCst)
            + self.delete_acl.load(Ordering::SeqCst)
            + self.upsert_scram.load(Ordering::SeqCst)
            + self.delete_scram.load(Ordering::SeqCst)
            + self.upsert_quota.load(Ordering::SeqCst)
            + self.delete_quota.load(Ordering::SeqCst)
            + self.move_log_dir.load(Ordering::SeqCst)
    }
}

#[derive(Default)]
struct RecordingAdminSeamFactory {
    reader: RecordingAdminReadSeam,
    mutations: RecordingAdminMutationSeam,
    read_seam_calls: AtomicUsize,
    mutation_seam_calls: AtomicUsize,
}

impl AdminSeamFactory for RecordingAdminSeamFactory {
    type Reader<'a> = &'a RecordingAdminReadSeam;
    type Mutations<'a> = &'a RecordingAdminMutationSeam;

    fn read_seam<'a>(
        &'a self,
        _cfg: &AdminUiConfig,
        record: &SessionRecord,
    ) -> Result<Self::Reader<'a>, UiError> {
        assert_eq!(record.user.username, "alice");
        self.read_seam_calls.fetch_add(1, Ordering::SeqCst);
        Ok(&self.reader)
    }

    fn mutation_seam<'a>(
        &'a self,
        _cfg: &AdminUiConfig,
        record: &SessionRecord,
    ) -> Result<Self::Mutations<'a>, UiError> {
        assert_eq!(record.user.username, "alice");
        self.mutation_seam_calls.fetch_add(1, Ordering::SeqCst);
        Ok(&self.mutations)
    }
}

impl AdminReadSeam for &RecordingAdminReadSeam {
    fn topics<'a>(
        &'a self,
    ) -> Pin<Box<dyn Future<Output = Result<Vec<TopicRow>, UiError>> + Send + 'a>> {
        (*self).topics()
    }

    fn groups<'a>(
        &'a self,
    ) -> Pin<Box<dyn Future<Output = Result<Vec<GroupRow>, UiError>> + Send + 'a>> {
        (*self).groups()
    }

    fn log_dirs<'a>(
        &'a self,
    ) -> Pin<Box<dyn Future<Output = Result<Vec<LogDirRow>, UiError>> + Send + 'a>> {
        (*self).log_dirs()
    }
}

impl AdminMutationSeam for &RecordingAdminMutationSeam {
    fn create_topic<'a>(
        &'a self,
        request: CreateTopicRequestDto,
    ) -> Pin<Box<dyn Future<Output = Result<Vec<ResourceOutcome>, UiError>> + Send + 'a>> {
        (*self).create_topic(request)
    }

    fn delete_topic<'a>(
        &'a self,
        request: DeleteTopicRequestDto,
    ) -> Pin<Box<dyn Future<Output = Result<Vec<ResourceOutcome>, UiError>> + Send + 'a>> {
        (*self).delete_topic(request)
    }

    fn create_partitions<'a>(
        &'a self,
        request: CreatePartitionsRequestDto,
    ) -> Pin<Box<dyn Future<Output = Result<Vec<ResourceOutcome>, UiError>> + Send + 'a>> {
        (*self).create_partitions(request)
    }

    fn alter_configs<'a>(
        &'a self,
        request: AlterConfigRequestDto,
    ) -> Pin<Box<dyn Future<Output = Result<Vec<ResourceOutcome>, UiError>> + Send + 'a>> {
        (*self).alter_configs(request)
    }

    fn create_acl<'a>(
        &'a self,
        request: AclRequestDto,
    ) -> Pin<Box<dyn Future<Output = Result<Vec<ResourceOutcome>, UiError>> + Send + 'a>> {
        (*self).create_acl(request)
    }

    fn delete_acl<'a>(
        &'a self,
        request: AclRequestDto,
    ) -> Pin<Box<dyn Future<Output = Result<Vec<ResourceOutcome>, UiError>> + Send + 'a>> {
        (*self).delete_acl(request)
    }

    fn upsert_scram_sha512_user<'a>(
        &'a self,
        request: ScramUserUpsertDto,
    ) -> Pin<Box<dyn Future<Output = Result<Vec<ResourceOutcome>, UiError>> + Send + 'a>> {
        (*self).upsert_scram_sha512_user(request)
    }

    fn delete_scram_user<'a>(
        &'a self,
        request: ScramUserDeleteDto,
    ) -> Pin<Box<dyn Future<Output = Result<Vec<ResourceOutcome>, UiError>> + Send + 'a>> {
        (*self).delete_scram_user(request)
    }

    fn upsert_quota<'a>(
        &'a self,
        request: QuotaUpsertDto,
    ) -> Pin<Box<dyn Future<Output = Result<Vec<ResourceOutcome>, UiError>> + Send + 'a>> {
        (*self).upsert_quota(request)
    }

    fn delete_quota<'a>(
        &'a self,
        request: QuotaDeleteDto,
    ) -> Pin<Box<dyn Future<Output = Result<Vec<ResourceOutcome>, UiError>> + Send + 'a>> {
        (*self).delete_quota(request)
    }

    fn move_log_dir<'a>(
        &'a self,
        request: LogDirMoveRequestDto,
    ) -> Pin<Box<dyn Future<Output = Result<Vec<ResourceOutcome>, UiError>> + Send + 'a>> {
        (*self).move_log_dir(request)
    }
}

fn authenticated_session(sessions: &SessionStore) -> crabka_admin_ui::session::SessionId {
    sessions.create_authenticated(
        SessionUser {
            username: "alice".to_string(),
            principal: "User:alice".to_string(),
        },
        SessionCredentials::scram_sha512("password".to_string()),
    )
}

impl AdminMutationSeam for RecordingAdminMutationSeam {
    fn create_topic<'a>(
        &'a self,
        request: CreateTopicRequestDto,
    ) -> Pin<Box<dyn Future<Output = Result<Vec<ResourceOutcome>, UiError>> + Send + 'a>> {
        Box::pin(async move {
            self.create_topic.fetch_add(1, Ordering::SeqCst);
            Ok(vec![ResourceOutcome::ok(request.name)])
        })
    }

    fn delete_topic<'a>(
        &'a self,
        request: DeleteTopicRequestDto,
    ) -> Pin<Box<dyn Future<Output = Result<Vec<ResourceOutcome>, UiError>> + Send + 'a>> {
        Box::pin(async move {
            self.delete_topic.fetch_add(1, Ordering::SeqCst);
            Ok(vec![ResourceOutcome::ok(request.name)])
        })
    }

    fn create_partitions<'a>(
        &'a self,
        request: CreatePartitionsRequestDto,
    ) -> Pin<Box<dyn Future<Output = Result<Vec<ResourceOutcome>, UiError>> + Send + 'a>> {
        Box::pin(async move {
            self.create_partitions.fetch_add(1, Ordering::SeqCst);
            Ok(vec![ResourceOutcome::ok(request.topic)])
        })
    }

    fn alter_configs<'a>(
        &'a self,
        request: AlterConfigRequestDto,
    ) -> Pin<Box<dyn Future<Output = Result<Vec<ResourceOutcome>, UiError>> + Send + 'a>> {
        Box::pin(async move {
            self.alter_configs.fetch_add(1, Ordering::SeqCst);
            Ok(vec![ResourceOutcome::ok(request.resource_name)])
        })
    }

    fn create_acl<'a>(
        &'a self,
        request: AclRequestDto,
    ) -> Pin<Box<dyn Future<Output = Result<Vec<ResourceOutcome>, UiError>> + Send + 'a>> {
        Box::pin(async move {
            self.create_acl.fetch_add(1, Ordering::SeqCst);
            Ok(vec![ResourceOutcome::ok(request.principal)])
        })
    }

    fn delete_acl<'a>(
        &'a self,
        request: AclRequestDto,
    ) -> Pin<Box<dyn Future<Output = Result<Vec<ResourceOutcome>, UiError>> + Send + 'a>> {
        Box::pin(async move {
            self.delete_acl.fetch_add(1, Ordering::SeqCst);
            Ok(vec![ResourceOutcome::ok(request.principal)])
        })
    }

    fn upsert_scram_sha512_user<'a>(
        &'a self,
        request: ScramUserUpsertDto,
    ) -> Pin<Box<dyn Future<Output = Result<Vec<ResourceOutcome>, UiError>> + Send + 'a>> {
        Box::pin(async move {
            self.upsert_scram.fetch_add(1, Ordering::SeqCst);
            Ok(vec![ResourceOutcome::ok(request.username)])
        })
    }

    fn delete_scram_user<'a>(
        &'a self,
        request: ScramUserDeleteDto,
    ) -> Pin<Box<dyn Future<Output = Result<Vec<ResourceOutcome>, UiError>> + Send + 'a>> {
        Box::pin(async move {
            self.delete_scram.fetch_add(1, Ordering::SeqCst);
            Ok(vec![ResourceOutcome::ok(request.username)])
        })
    }

    fn upsert_quota<'a>(
        &'a self,
        request: QuotaUpsertDto,
    ) -> Pin<Box<dyn Future<Output = Result<Vec<ResourceOutcome>, UiError>> + Send + 'a>> {
        Box::pin(async move {
            self.upsert_quota.fetch_add(1, Ordering::SeqCst);
            Ok(vec![ResourceOutcome::ok(request.entity)])
        })
    }

    fn delete_quota<'a>(
        &'a self,
        request: QuotaDeleteDto,
    ) -> Pin<Box<dyn Future<Output = Result<Vec<ResourceOutcome>, UiError>> + Send + 'a>> {
        Box::pin(async move {
            self.delete_quota.fetch_add(1, Ordering::SeqCst);
            Ok(vec![ResourceOutcome::ok(request.entity)])
        })
    }

    fn move_log_dir<'a>(
        &'a self,
        request: LogDirMoveRequestDto,
    ) -> Pin<Box<dyn Future<Output = Result<Vec<ResourceOutcome>, UiError>> + Send + 'a>> {
        Box::pin(async move {
            self.move_log_dir.fetch_add(1, Ordering::SeqCst);
            Ok(vec![ResourceOutcome::ok(request.topic)])
        })
    }
}

fn delete_topic_request() -> DeleteTopicRequestDto {
    DeleteTopicRequestDto {
        name: "orders".to_string(),
    }
}

fn create_partitions_request() -> CreatePartitionsRequestDto {
    CreatePartitionsRequestDto {
        topic: "orders".to_string(),
        total_count: 6,
    }
}

fn alter_config_request() -> AlterConfigRequestDto {
    AlterConfigRequestDto {
        resource_type: "topic".to_string(),
        resource_name: "orders".to_string(),
        configs: vec![ConfigEntryDto {
            name: "cleanup.policy".to_string(),
            value: "compact".to_string(),
        }],
    }
}

fn acl_request() -> AclRequestDto {
    AclRequestDto {
        resource_type: "topic".to_string(),
        resource_name: "orders".to_string(),
        principal: "User:alice".to_string(),
        operation: "Read".to_string(),
        permission: "Allow".to_string(),
        host: "*".to_string(),
    }
}

fn scram_upsert_request() -> ScramUserUpsertDto {
    ScramUserUpsertDto {
        username: "alice".to_string(),
        password: "redacted-password".to_string(),
        iterations: 4096,
    }
}

fn scram_delete_request() -> ScramUserDeleteDto {
    ScramUserDeleteDto {
        username: "alice".to_string(),
    }
}

fn quota_upsert_request() -> QuotaUpsertDto {
    QuotaUpsertDto {
        entity: "user=alice".to_string(),
        quota_type: "producer_byte_rate".to_string(),
        value: 1024.0,
    }
}

fn quota_delete_request() -> QuotaDeleteDto {
    QuotaDeleteDto {
        entity: "user=alice".to_string(),
        quota_type: "producer_byte_rate".to_string(),
    }
}

fn log_dir_move_request() -> LogDirMoveRequestDto {
    LogDirMoveRequestDto {
        topic: "orders".to_string(),
        partition: 0,
        destination_log_dir: "/var/lib/crabka-1".to_string(),
    }
}

fn format_result_debug<T: std::fmt::Debug>(result: &Result<T, UiError>) -> String {
    format!("{result:?}")
}

fn assert_debug_does_not_contain_secret(debug: &str, secret: &str, label: &str) {
    assert!(!debug.contains(secret), "debug output leaked {label}");
}
