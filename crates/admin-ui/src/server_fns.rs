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

pub async fn logout() -> Result<(), UiError> {
    Err(UiError::NotAuthenticated)
}

pub async fn current_session() -> Result<CurrentSession, UiError> {
    let factory = BrokerAdminSeamFactory;
    let cfg = AdminUiConfig::default();
    let context = ServerFunctionContext::new(&cfg, runtime_sessions(), None, &factory);

    current_session_with_context(&context)
}

pub async fn list_topics() -> Result<Vec<TopicRow>, UiError> {
    let factory = BrokerAdminSeamFactory;
    let cfg = AdminUiConfig::default();
    let context = ServerFunctionContext::new(&cfg, runtime_sessions(), None, &factory);

    list_topics_with_context(&context).await
}

pub async fn list_groups() -> Result<Vec<GroupRow>, UiError> {
    let factory = BrokerAdminSeamFactory;
    let cfg = AdminUiConfig::default();
    let context = ServerFunctionContext::new(&cfg, runtime_sessions(), None, &factory);

    list_groups_with_context(&context).await
}

pub async fn list_acls() -> Result<Vec<AclRow>, UiError> {
    let factory = BrokerAdminSeamFactory;
    let cfg = AdminUiConfig::default();
    let context = ServerFunctionContext::new(&cfg, runtime_sessions(), None, &factory);

    current_session_with_context(&context)?;
    Ok(Vec::new())
}

pub async fn list_users() -> Result<Vec<UserRow>, UiError> {
    let factory = BrokerAdminSeamFactory;
    let cfg = AdminUiConfig::default();
    let context = ServerFunctionContext::new(&cfg, runtime_sessions(), None, &factory);

    current_session_with_context(&context)?;
    Ok(Vec::new())
}

pub async fn list_quotas() -> Result<Vec<QuotaRow>, UiError> {
    let factory = BrokerAdminSeamFactory;
    let cfg = AdminUiConfig::default();
    let context = ServerFunctionContext::new(&cfg, runtime_sessions(), None, &factory);

    current_session_with_context(&context)?;
    Ok(Vec::new())
}

pub async fn list_log_dirs() -> Result<Vec<LogDirRow>, UiError> {
    let factory = BrokerAdminSeamFactory;
    let cfg = AdminUiConfig::default();
    let context = ServerFunctionContext::new(&cfg, runtime_sessions(), None, &factory);

    list_log_dirs_with_context(&context).await
}

pub async fn create_topic(request: CreateTopicRequestDto) -> Result<Vec<ResourceOutcome>, UiError> {
    ensure_valid_request(request.validate())?;

    let factory = BrokerAdminSeamFactory;
    let cfg = AdminUiConfig::default();
    let context = ServerFunctionContext::new(&cfg, runtime_sessions(), None, &factory);

    create_topic_with_context(&context, request).await
}

pub async fn delete_topic(request: DeleteTopicRequestDto) -> Result<Vec<ResourceOutcome>, UiError> {
    ensure_valid_request(request.validate())?;

    reject_missing_session().await
}

pub async fn create_partitions(
    request: CreatePartitionsRequestDto,
) -> Result<Vec<ResourceOutcome>, UiError> {
    ensure_valid_request(request.validate())?;

    reject_missing_session().await
}

pub async fn alter_configs(
    request: AlterConfigRequestDto,
) -> Result<Vec<ResourceOutcome>, UiError> {
    ensure_valid_request(request.validate())?;

    reject_missing_session().await
}

pub async fn create_acl(request: AclRequestDto) -> Result<Vec<ResourceOutcome>, UiError> {
    ensure_valid_request(request.validate())?;

    reject_missing_session().await
}

pub async fn delete_acl(request: AclRequestDto) -> Result<Vec<ResourceOutcome>, UiError> {
    ensure_valid_request(request.validate())?;

    reject_missing_session().await
}

pub async fn upsert_scram_sha512_user(
    request: ScramUserUpsertDto,
) -> Result<Vec<ResourceOutcome>, UiError> {
    ensure_valid_request(request.validate())?;

    reject_missing_session().await
}

pub async fn delete_scram_user(
    request: ScramUserDeleteDto,
) -> Result<Vec<ResourceOutcome>, UiError> {
    ensure_valid_request(request.validate())?;

    reject_missing_session().await
}

pub async fn upsert_quota(request: QuotaUpsertDto) -> Result<Vec<ResourceOutcome>, UiError> {
    ensure_valid_request(request.validate())?;

    reject_missing_session().await
}

pub async fn delete_quota(request: QuotaDeleteDto) -> Result<Vec<ResourceOutcome>, UiError> {
    ensure_valid_request(request.validate())?;

    reject_missing_session().await
}

pub async fn move_log_dir(request: LogDirMoveRequestDto) -> Result<Vec<ResourceOutcome>, UiError> {
    ensure_valid_request(request.validate())?;

    reject_missing_session().await
}

pub trait AdminReadSeam {
    fn topics<'a>(
        &'a self,
    ) -> Pin<Box<dyn Future<Output = Result<Vec<TopicRow>, UiError>> + Send + 'a>>;

    fn groups<'a>(
        &'a self,
    ) -> Pin<Box<dyn Future<Output = Result<Vec<GroupRow>, UiError>> + Send + 'a>>;

    fn log_dirs<'a>(
        &'a self,
    ) -> Pin<Box<dyn Future<Output = Result<Vec<LogDirRow>, UiError>> + Send + 'a>>;
}

pub trait AdminMutationSeam {
    fn create_topic<'a>(
        &'a self,
        request: CreateTopicRequestDto,
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
}

fn ensure_valid_request(validation: Result<(), String>) -> Result<(), UiError> {
    validation.map_err(UiError::Admin)
}

async fn reject_missing_session<T>() -> Result<T, UiError> {
    Err(UiError::NotAuthenticated)
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
