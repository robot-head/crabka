//! Server-function seam for the Dioxus admin UI shell.

#![allow(clippy::unused_async)]

use std::collections::BTreeMap;
use std::future::Future;
use std::pin::Pin;
use std::sync::OnceLock;
use std::time::Duration;

use crabka_client_admin::{AdminClient, CreateTopicSpec};
use serde::{Deserialize, Serialize};

use crate::admin::AdminFacade;
use crate::auth::{
    AdminClientLoginBroker, LoginBroker, LoginRequest, LoginSuccess, build_scram_sha512_security,
};
use crate::config::AdminUiConfig;
use crate::dto::{
    AclRequestDto, AlterConfigRequestDto, CreatePartitionsRequestDto, CreateTopicRequestDto,
    DeleteTopicRequestDto, GroupRow, LogDirMoveRequestDto, LogDirRow, QuotaDeleteDto,
    QuotaUpsertDto, ResourceOutcome, ScramUserDeleteDto, ScramUserUpsertDto, TopicRow,
};
use crate::error::UiError;
use crate::session::{SessionId, SessionRecord, SessionStore};

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CurrentSession {
    pub username: String,
    pub principal: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AclRow {
    pub resource: String,
    pub principal: String,
    pub operation: String,
    pub permission: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct UserRow {
    pub username: String,
    pub principal: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct QuotaRow {
    pub entity: String,
    pub quota_type: String,
    pub value: String,
}

pub async fn login(request: LoginRequest) -> Result<LoginSuccess, UiError> {
    let cfg = AdminUiConfig::from_env().map_err(|error| UiError::Admin(error.to_string()))?;

    login_with_context(&cfg, runtime_sessions(), &AdminClientLoginBroker, request).await
}

pub async fn logout<F>(context: &ServerFunctionContext<'_, F>) -> Result<(), UiError> {
    logout_with_context(context).await
}

pub async fn create_topic<F: AdminSeamFactory>(
    context: &ServerFunctionContext<'_, F>,
    request: CreateTopicRequestDto,
) -> Result<Vec<ResourceOutcome>, UiError> {
    create_topic_with_context(context, request).await
}

pub async fn delete_topic<F: AdminSeamFactory>(
    context: &ServerFunctionContext<'_, F>,
    request: DeleteTopicRequestDto,
) -> Result<Vec<ResourceOutcome>, UiError> {
    delete_topic_with_context(context, request).await
}

pub async fn create_partitions<F: AdminSeamFactory>(
    context: &ServerFunctionContext<'_, F>,
    request: CreatePartitionsRequestDto,
) -> Result<Vec<ResourceOutcome>, UiError> {
    create_partitions_with_context(context, request).await
}

pub async fn alter_configs<F: AdminSeamFactory>(
    context: &ServerFunctionContext<'_, F>,
    request: AlterConfigRequestDto,
) -> Result<Vec<ResourceOutcome>, UiError> {
    alter_configs_with_context(context, request).await
}

pub async fn create_acl<F: AdminSeamFactory>(
    context: &ServerFunctionContext<'_, F>,
    request: AclRequestDto,
) -> Result<Vec<ResourceOutcome>, UiError> {
    create_acl_with_context(context, request).await
}

pub async fn delete_acl<F: AdminSeamFactory>(
    context: &ServerFunctionContext<'_, F>,
    request: AclRequestDto,
) -> Result<Vec<ResourceOutcome>, UiError> {
    delete_acl_with_context(context, request).await
}

pub async fn upsert_scram_sha512_user<F: AdminSeamFactory>(
    context: &ServerFunctionContext<'_, F>,
    request: ScramUserUpsertDto,
) -> Result<Vec<ResourceOutcome>, UiError> {
    upsert_scram_sha512_user_with_context(context, request).await
}

pub async fn delete_scram_user<F: AdminSeamFactory>(
    context: &ServerFunctionContext<'_, F>,
    request: ScramUserDeleteDto,
) -> Result<Vec<ResourceOutcome>, UiError> {
    delete_scram_user_with_context(context, request).await
}

pub async fn upsert_quota<F: AdminSeamFactory>(
    context: &ServerFunctionContext<'_, F>,
    request: QuotaUpsertDto,
) -> Result<Vec<ResourceOutcome>, UiError> {
    upsert_quota_with_context(context, request).await
}

pub async fn delete_quota<F: AdminSeamFactory>(
    context: &ServerFunctionContext<'_, F>,
    request: QuotaDeleteDto,
) -> Result<Vec<ResourceOutcome>, UiError> {
    delete_quota_with_context(context, request).await
}

pub async fn move_log_dir<F: AdminSeamFactory>(
    context: &ServerFunctionContext<'_, F>,
    request: LogDirMoveRequestDto,
) -> Result<Vec<ResourceOutcome>, UiError> {
    move_log_dir_with_context(context, request).await
}

pub trait AdminReadSeam {
    fn topics<'a>(
        &'a self,
    ) -> Pin<Box<dyn Future<Output = Result<Vec<TopicRow>, UiError>> + Send + 'a>>;

    fn groups<'a>(
        &'a self,
    ) -> Pin<Box<dyn Future<Output = Result<Vec<GroupRow>, UiError>> + Send + 'a>>;

    fn acls<'a>(
        &'a self,
    ) -> Pin<Box<dyn Future<Output = Result<Vec<AclRow>, UiError>> + Send + 'a>>;

    fn users<'a>(
        &'a self,
    ) -> Pin<Box<dyn Future<Output = Result<Vec<UserRow>, UiError>> + Send + 'a>>;

    fn quotas<'a>(
        &'a self,
    ) -> Pin<Box<dyn Future<Output = Result<Vec<QuotaRow>, UiError>> + Send + 'a>>;

    fn log_dirs<'a>(
        &'a self,
    ) -> Pin<Box<dyn Future<Output = Result<Vec<LogDirRow>, UiError>> + Send + 'a>>;
}

pub trait AdminMutationSeam {
    fn create_topic<'a>(
        &'a self,
        request: CreateTopicRequestDto,
    ) -> Pin<Box<dyn Future<Output = Result<Vec<ResourceOutcome>, UiError>> + Send + 'a>>;

    fn delete_topic<'a>(
        &'a self,
        request: DeleteTopicRequestDto,
    ) -> Pin<Box<dyn Future<Output = Result<Vec<ResourceOutcome>, UiError>> + Send + 'a>>;

    fn create_partitions<'a>(
        &'a self,
        request: CreatePartitionsRequestDto,
    ) -> Pin<Box<dyn Future<Output = Result<Vec<ResourceOutcome>, UiError>> + Send + 'a>>;

    fn alter_configs<'a>(
        &'a self,
        request: AlterConfigRequestDto,
    ) -> Pin<Box<dyn Future<Output = Result<Vec<ResourceOutcome>, UiError>> + Send + 'a>>;

    fn create_acl<'a>(
        &'a self,
        request: AclRequestDto,
    ) -> Pin<Box<dyn Future<Output = Result<Vec<ResourceOutcome>, UiError>> + Send + 'a>>;

    fn delete_acl<'a>(
        &'a self,
        request: AclRequestDto,
    ) -> Pin<Box<dyn Future<Output = Result<Vec<ResourceOutcome>, UiError>> + Send + 'a>>;

    fn upsert_scram_sha512_user<'a>(
        &'a self,
        request: ScramUserUpsertDto,
    ) -> Pin<Box<dyn Future<Output = Result<Vec<ResourceOutcome>, UiError>> + Send + 'a>>;

    fn delete_scram_user<'a>(
        &'a self,
        request: ScramUserDeleteDto,
    ) -> Pin<Box<dyn Future<Output = Result<Vec<ResourceOutcome>, UiError>> + Send + 'a>>;

    fn upsert_quota<'a>(
        &'a self,
        request: QuotaUpsertDto,
    ) -> Pin<Box<dyn Future<Output = Result<Vec<ResourceOutcome>, UiError>> + Send + 'a>>;

    fn delete_quota<'a>(
        &'a self,
        request: QuotaDeleteDto,
    ) -> Pin<Box<dyn Future<Output = Result<Vec<ResourceOutcome>, UiError>> + Send + 'a>>;

    fn move_log_dir<'a>(
        &'a self,
        request: LogDirMoveRequestDto,
    ) -> Pin<Box<dyn Future<Output = Result<Vec<ResourceOutcome>, UiError>> + Send + 'a>>;
}

pub trait AdminSeamFactory {
    type Reader<'a>: AdminReadSeam
    where
        Self: 'a;
    type Mutations<'a>: AdminMutationSeam
    where
        Self: 'a;

    fn read_seam<'a>(
        &'a self,
        cfg: &AdminUiConfig,
        record: &SessionRecord,
    ) -> Result<Self::Reader<'a>, UiError>;

    fn mutation_seam<'a>(
        &'a self,
        cfg: &AdminUiConfig,
        record: &SessionRecord,
    ) -> Result<Self::Mutations<'a>, UiError>;
}

#[derive(Clone, Copy, Debug, Default)]
pub struct BrokerAdminSeamFactory;

impl AdminSeamFactory for BrokerAdminSeamFactory {
    type Reader<'a> = BrokerAdminReadSeam;
    type Mutations<'a> = BrokerAdminMutationSeam;

    fn read_seam<'a>(
        &'a self,
        cfg: &AdminUiConfig,
        record: &SessionRecord,
    ) -> Result<Self::Reader<'a>, UiError> {
        BrokerAdminReadSeam::from_session(cfg, record)
    }

    fn mutation_seam<'a>(
        &'a self,
        cfg: &AdminUiConfig,
        record: &SessionRecord,
    ) -> Result<Self::Mutations<'a>, UiError> {
        BrokerAdminMutationSeam::from_session(cfg, record)
    }
}

pub struct ServerFunctionContext<'a, F = BrokerAdminSeamFactory> {
    pub cfg: &'a AdminUiConfig,
    pub sessions: &'a SessionStore,
    pub raw_session_id: Option<&'a str>,
    pub seam_factory: &'a F,
}

impl<'a, F> ServerFunctionContext<'a, F> {
    #[must_use]
    pub const fn new(
        cfg: &'a AdminUiConfig,
        sessions: &'a SessionStore,
        raw_session_id: Option<&'a str>,
        seam_factory: &'a F,
    ) -> Self {
        Self {
            cfg,
            sessions,
            raw_session_id,
            seam_factory,
        }
    }
}

pub async fn login_with_context<B: LoginBroker>(
    cfg: &AdminUiConfig,
    sessions: &SessionStore,
    broker: &B,
    request: LoginRequest,
) -> Result<LoginSuccess, UiError> {
    crate::auth::AuthService::new_with_broker(cfg, sessions, broker)
        .login(request)
        .await
}

pub async fn logout_with_context<F>(context: &ServerFunctionContext<'_, F>) -> Result<(), UiError> {
    let session_id = require_session_id(context.sessions, context.raw_session_id)?;

    context.sessions.remove(&session_id);
    Ok(())
}

pub fn current_session_with_store(
    sessions: &SessionStore,
    raw_session_id: Option<&str>,
) -> Result<CurrentSession, UiError> {
    let record = require_session(sessions, raw_session_id)?;

    Ok(CurrentSession {
        username: record.user.username,
        principal: record.user.principal,
    })
}

pub fn current_session_with_context<F>(
    context: &ServerFunctionContext<'_, F>,
) -> Result<CurrentSession, UiError> {
    current_session_with_store(context.sessions, context.raw_session_id)
}

pub async fn list_topics_with_reader<R: AdminReadSeam>(
    sessions: &SessionStore,
    raw_session_id: Option<&str>,
    reader: &R,
) -> Result<Vec<TopicRow>, UiError> {
    require_session(sessions, raw_session_id)?;

    reader.topics().await
}

pub async fn list_topics_with_context<F: AdminSeamFactory>(
    context: &ServerFunctionContext<'_, F>,
) -> Result<Vec<TopicRow>, UiError> {
    let reader = read_seam_from_context(context)?;

    reader.topics().await
}

pub async fn list_groups_with_reader<R: AdminReadSeam>(
    sessions: &SessionStore,
    raw_session_id: Option<&str>,
    reader: &R,
) -> Result<Vec<GroupRow>, UiError> {
    require_session(sessions, raw_session_id)?;

    reader.groups().await
}

pub async fn list_groups_with_context<F: AdminSeamFactory>(
    context: &ServerFunctionContext<'_, F>,
) -> Result<Vec<GroupRow>, UiError> {
    let reader = read_seam_from_context(context)?;

    reader.groups().await
}

pub async fn list_acls_with_reader<R: AdminReadSeam>(
    sessions: &SessionStore,
    raw_session_id: Option<&str>,
    reader: &R,
) -> Result<Vec<AclRow>, UiError> {
    require_session(sessions, raw_session_id)?;

    reader.acls().await
}

pub async fn list_acls<F: AdminSeamFactory>(
    context: &ServerFunctionContext<'_, F>,
) -> Result<Vec<AclRow>, UiError> {
    let reader = read_seam_from_context(context)?;

    reader.acls().await
}

pub async fn list_users_with_reader<R: AdminReadSeam>(
    sessions: &SessionStore,
    raw_session_id: Option<&str>,
    reader: &R,
) -> Result<Vec<UserRow>, UiError> {
    require_session(sessions, raw_session_id)?;

    reader.users().await
}

pub async fn list_users<F: AdminSeamFactory>(
    context: &ServerFunctionContext<'_, F>,
) -> Result<Vec<UserRow>, UiError> {
    let reader = read_seam_from_context(context)?;

    reader.users().await
}

pub async fn list_quotas_with_reader<R: AdminReadSeam>(
    sessions: &SessionStore,
    raw_session_id: Option<&str>,
    reader: &R,
) -> Result<Vec<QuotaRow>, UiError> {
    require_session(sessions, raw_session_id)?;

    reader.quotas().await
}

pub async fn list_quotas<F: AdminSeamFactory>(
    context: &ServerFunctionContext<'_, F>,
) -> Result<Vec<QuotaRow>, UiError> {
    let reader = read_seam_from_context(context)?;

    reader.quotas().await
}

pub async fn list_log_dirs_with_reader<R: AdminReadSeam>(
    sessions: &SessionStore,
    raw_session_id: Option<&str>,
    reader: &R,
) -> Result<Vec<LogDirRow>, UiError> {
    require_session(sessions, raw_session_id)?;

    reader.log_dirs().await
}

pub async fn list_log_dirs_with_context<F: AdminSeamFactory>(
    context: &ServerFunctionContext<'_, F>,
) -> Result<Vec<LogDirRow>, UiError> {
    let reader = read_seam_from_context(context)?;

    reader.log_dirs().await
}

pub async fn create_topic_with_mutations<M: AdminMutationSeam>(
    sessions: &SessionStore,
    raw_session_id: Option<&str>,
    mutations: &M,
    request: CreateTopicRequestDto,
) -> Result<Vec<ResourceOutcome>, UiError> {
    ensure_valid_request(request.validate())?;
    require_session(sessions, raw_session_id)?;

    mutations.create_topic(request).await
}

pub async fn create_topic_with_context<F: AdminSeamFactory>(
    context: &ServerFunctionContext<'_, F>,
    request: CreateTopicRequestDto,
) -> Result<Vec<ResourceOutcome>, UiError> {
    ensure_valid_request(request.validate())?;
    let mutations = mutation_seam_from_context(context)?;

    mutations.create_topic(request).await
}

pub async fn delete_topic_with_context<F: AdminSeamFactory>(
    context: &ServerFunctionContext<'_, F>,
    request: DeleteTopicRequestDto,
) -> Result<Vec<ResourceOutcome>, UiError> {
    ensure_valid_request(request.validate())?;
    let mutations = mutation_seam_from_context(context)?;

    mutations.delete_topic(request).await
}

pub async fn create_partitions_with_context<F: AdminSeamFactory>(
    context: &ServerFunctionContext<'_, F>,
    request: CreatePartitionsRequestDto,
) -> Result<Vec<ResourceOutcome>, UiError> {
    ensure_valid_request(request.validate())?;
    let mutations = mutation_seam_from_context(context)?;

    mutations.create_partitions(request).await
}

pub async fn alter_configs_with_context<F: AdminSeamFactory>(
    context: &ServerFunctionContext<'_, F>,
    request: AlterConfigRequestDto,
) -> Result<Vec<ResourceOutcome>, UiError> {
    ensure_valid_request(request.validate())?;
    let mutations = mutation_seam_from_context(context)?;

    mutations.alter_configs(request).await
}

pub async fn create_acl_with_context<F: AdminSeamFactory>(
    context: &ServerFunctionContext<'_, F>,
    request: AclRequestDto,
) -> Result<Vec<ResourceOutcome>, UiError> {
    ensure_valid_request(request.validate())?;
    let mutations = mutation_seam_from_context(context)?;

    mutations.create_acl(request).await
}

pub async fn delete_acl_with_context<F: AdminSeamFactory>(
    context: &ServerFunctionContext<'_, F>,
    request: AclRequestDto,
) -> Result<Vec<ResourceOutcome>, UiError> {
    ensure_valid_request(request.validate())?;
    let mutations = mutation_seam_from_context(context)?;

    mutations.delete_acl(request).await
}

pub async fn upsert_scram_sha512_user_with_context<F: AdminSeamFactory>(
    context: &ServerFunctionContext<'_, F>,
    request: ScramUserUpsertDto,
) -> Result<Vec<ResourceOutcome>, UiError> {
    ensure_valid_request(request.validate())?;
    let mutations = mutation_seam_from_context(context)?;

    mutations.upsert_scram_sha512_user(request).await
}

pub async fn delete_scram_user_with_context<F: AdminSeamFactory>(
    context: &ServerFunctionContext<'_, F>,
    request: ScramUserDeleteDto,
) -> Result<Vec<ResourceOutcome>, UiError> {
    ensure_valid_request(request.validate())?;
    let mutations = mutation_seam_from_context(context)?;

    mutations.delete_scram_user(request).await
}

pub async fn upsert_quota_with_context<F: AdminSeamFactory>(
    context: &ServerFunctionContext<'_, F>,
    request: QuotaUpsertDto,
) -> Result<Vec<ResourceOutcome>, UiError> {
    ensure_valid_request(request.validate())?;
    let mutations = mutation_seam_from_context(context)?;

    mutations.upsert_quota(request).await
}

pub async fn delete_quota_with_context<F: AdminSeamFactory>(
    context: &ServerFunctionContext<'_, F>,
    request: QuotaDeleteDto,
) -> Result<Vec<ResourceOutcome>, UiError> {
    ensure_valid_request(request.validate())?;
    let mutations = mutation_seam_from_context(context)?;

    mutations.delete_quota(request).await
}

pub async fn move_log_dir_with_context<F: AdminSeamFactory>(
    context: &ServerFunctionContext<'_, F>,
    request: LogDirMoveRequestDto,
) -> Result<Vec<ResourceOutcome>, UiError> {
    ensure_valid_request(request.validate())?;
    let mutations = mutation_seam_from_context(context)?;

    mutations.move_log_dir(request).await
}

pub struct BrokerAdminReadSeam {
    cfg: AdminUiConfig,
    username: String,
    password: String,
}

impl BrokerAdminReadSeam {
    pub fn from_session(cfg: &AdminUiConfig, record: &SessionRecord) -> Result<Self, UiError> {
        let Some(credentials) = &record.credentials else {
            return Err(UiError::NotAuthenticated);
        };

        Ok(Self {
            cfg: cfg.clone(),
            username: record.user.username.clone(),
            password: credentials.password().to_string(),
        })
    }

    async fn facade(&self) -> Result<AdminFacade, UiError> {
        let security = build_scram_sha512_security(&self.cfg, &self.username, &self.password);
        let client =
            AdminClient::connect_secured(&self.cfg.bootstrap_addrs, Some(security)).await?;

        Ok(AdminFacade::new(client))
    }
}

impl AdminReadSeam for BrokerAdminReadSeam {
    fn topics<'a>(
        &'a self,
    ) -> Pin<Box<dyn Future<Output = Result<Vec<TopicRow>, UiError>> + Send + 'a>> {
        Box::pin(async move {
            let mut facade = self.facade().await?;
            Ok(facade.topics().await?)
        })
    }

    fn groups<'a>(
        &'a self,
    ) -> Pin<Box<dyn Future<Output = Result<Vec<GroupRow>, UiError>> + Send + 'a>> {
        Box::pin(async move {
            let mut facade = self.facade().await?;
            Ok(facade.groups().await?)
        })
    }

    fn acls<'a>(
        &'a self,
    ) -> Pin<Box<dyn Future<Output = Result<Vec<AclRow>, UiError>> + Send + 'a>> {
        Box::pin(async {
            Err(UiError::Admin(
                "list ACLs is unsupported by the broker admin seam yet".to_string(),
            ))
        })
    }

    fn users<'a>(
        &'a self,
    ) -> Pin<Box<dyn Future<Output = Result<Vec<UserRow>, UiError>> + Send + 'a>> {
        Box::pin(async {
            Err(UiError::Admin(
                "list SCRAM users is unsupported by the broker admin seam yet".to_string(),
            ))
        })
    }

    fn quotas<'a>(
        &'a self,
    ) -> Pin<Box<dyn Future<Output = Result<Vec<QuotaRow>, UiError>> + Send + 'a>> {
        Box::pin(async {
            Err(UiError::Admin(
                "list quotas is unsupported by the broker admin seam yet".to_string(),
            ))
        })
    }

    fn log_dirs<'a>(
        &'a self,
    ) -> Pin<Box<dyn Future<Output = Result<Vec<LogDirRow>, UiError>> + Send + 'a>> {
        Box::pin(async move {
            let mut facade = self.facade().await?;
            Ok(facade.log_dirs().await?)
        })
    }
}

pub struct BrokerAdminMutationSeam(BrokerAdminReadSeam);

impl BrokerAdminMutationSeam {
    pub fn from_session(cfg: &AdminUiConfig, record: &SessionRecord) -> Result<Self, UiError> {
        Ok(Self(BrokerAdminReadSeam::from_session(cfg, record)?))
    }
}

impl AdminMutationSeam for BrokerAdminMutationSeam {
    fn create_topic<'a>(
        &'a self,
        request: CreateTopicRequestDto,
    ) -> Pin<Box<dyn Future<Output = Result<Vec<ResourceOutcome>, UiError>> + Send + 'a>> {
        Box::pin(async move {
            let mut facade = self.0.facade().await?;
            let configs = request
                .configs
                .into_iter()
                .map(|config| (config.name, config.value))
                .collect::<BTreeMap<_, _>>();
            let outcomes = facade
                .client_mut()
                .create_topics(
                    &[CreateTopicSpec {
                        name: request.name,
                        partitions: request.partitions,
                        replicas: request.replicas,
                        configs,
                    }],
                    30_000,
                )
                .await?;

            Ok(outcomes
                .into_iter()
                .map(|outcome| ResourceOutcome {
                    resource: outcome.name,
                    error: outcome.error.as_ref().map(crate::dto::KafkaErrorDto::from),
                })
                .collect())
        })
    }

    fn delete_topic<'a>(
        &'a self,
        request: DeleteTopicRequestDto,
    ) -> Pin<Box<dyn Future<Output = Result<Vec<ResourceOutcome>, UiError>> + Send + 'a>> {
        Box::pin(async move { unsupported_mutation_outcome(&request.name, "delete topic") })
    }

    fn create_partitions<'a>(
        &'a self,
        request: CreatePartitionsRequestDto,
    ) -> Pin<Box<dyn Future<Output = Result<Vec<ResourceOutcome>, UiError>> + Send + 'a>> {
        Box::pin(async move { unsupported_mutation_outcome(&request.topic, "create partitions") })
    }

    fn alter_configs<'a>(
        &'a self,
        request: AlterConfigRequestDto,
    ) -> Pin<Box<dyn Future<Output = Result<Vec<ResourceOutcome>, UiError>> + Send + 'a>> {
        Box::pin(
            async move { unsupported_mutation_outcome(&request.resource_name, "alter configs") },
        )
    }

    fn create_acl<'a>(
        &'a self,
        request: AclRequestDto,
    ) -> Pin<Box<dyn Future<Output = Result<Vec<ResourceOutcome>, UiError>> + Send + 'a>> {
        Box::pin(async move { unsupported_mutation_outcome(&request.principal, "create ACL") })
    }

    fn delete_acl<'a>(
        &'a self,
        request: AclRequestDto,
    ) -> Pin<Box<dyn Future<Output = Result<Vec<ResourceOutcome>, UiError>> + Send + 'a>> {
        Box::pin(async move { unsupported_mutation_outcome(&request.principal, "delete ACL") })
    }

    fn upsert_scram_sha512_user<'a>(
        &'a self,
        request: ScramUserUpsertDto,
    ) -> Pin<Box<dyn Future<Output = Result<Vec<ResourceOutcome>, UiError>> + Send + 'a>> {
        Box::pin(
            async move { unsupported_mutation_outcome(&request.username, "upsert SCRAM user") },
        )
    }

    fn delete_scram_user<'a>(
        &'a self,
        request: ScramUserDeleteDto,
    ) -> Pin<Box<dyn Future<Output = Result<Vec<ResourceOutcome>, UiError>> + Send + 'a>> {
        Box::pin(
            async move { unsupported_mutation_outcome(&request.username, "delete SCRAM user") },
        )
    }

    fn upsert_quota<'a>(
        &'a self,
        request: QuotaUpsertDto,
    ) -> Pin<Box<dyn Future<Output = Result<Vec<ResourceOutcome>, UiError>> + Send + 'a>> {
        Box::pin(async move { unsupported_mutation_outcome(&request.entity, "upsert quota") })
    }

    fn delete_quota<'a>(
        &'a self,
        request: QuotaDeleteDto,
    ) -> Pin<Box<dyn Future<Output = Result<Vec<ResourceOutcome>, UiError>> + Send + 'a>> {
        Box::pin(async move { unsupported_mutation_outcome(&request.entity, "delete quota") })
    }

    fn move_log_dir<'a>(
        &'a self,
        request: LogDirMoveRequestDto,
    ) -> Pin<Box<dyn Future<Output = Result<Vec<ResourceOutcome>, UiError>> + Send + 'a>> {
        Box::pin(async move { unsupported_mutation_outcome(&request.topic, "move log dir") })
    }
}

fn unsupported_mutation_outcome<T>(resource: &str, operation: &'static str) -> Result<T, UiError> {
    Err(UiError::Admin(format!(
        "{operation} is not supported by the broker admin seam yet for {resource}"
    )))
}

fn ensure_valid_request(validation: Result<(), String>) -> Result<(), UiError> {
    validation.map_err(UiError::Admin)
}

fn require_session_id(
    sessions: &SessionStore,
    raw_session_id: Option<&str>,
) -> Result<SessionId, UiError> {
    let Some(raw_session_id) = raw_session_id else {
        return Err(UiError::NotAuthenticated);
    };
    let Ok(session_id) = SessionId::try_from(raw_session_id) else {
        return Err(UiError::NotAuthenticated);
    };

    if sessions.get(&session_id).is_none() {
        return Err(UiError::NotAuthenticated);
    }

    Ok(session_id)
}

fn require_session(
    sessions: &SessionStore,
    raw_session_id: Option<&str>,
) -> Result<SessionRecord, UiError> {
    let Some(raw_session_id) = raw_session_id else {
        return Err(UiError::NotAuthenticated);
    };
    let Ok(session_id) = SessionId::try_from(raw_session_id) else {
        return Err(UiError::NotAuthenticated);
    };

    sessions.get(&session_id).ok_or(UiError::NotAuthenticated)
}

fn read_seam_from_context<'a, F: AdminSeamFactory>(
    context: &'a ServerFunctionContext<'_, F>,
) -> Result<F::Reader<'a>, UiError> {
    let record = require_session(context.sessions, context.raw_session_id)?;

    context.seam_factory.read_seam(context.cfg, &record)
}

fn mutation_seam_from_context<'a, F: AdminSeamFactory>(
    context: &'a ServerFunctionContext<'_, F>,
) -> Result<F::Mutations<'a>, UiError> {
    let record = require_session(context.sessions, context.raw_session_id)?;

    context.seam_factory.mutation_seam(context.cfg, &record)
}

fn runtime_sessions() -> &'static SessionStore {
    static SESSIONS: OnceLock<SessionStore> = OnceLock::new();

    SESSIONS.get_or_init(|| SessionStore::new(Duration::from_hours(8)))
}
