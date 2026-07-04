//! Server-function seam for the Dioxus admin UI shell.

#![allow(clippy::unused_async)]

use serde::{Deserialize, Serialize};

use crate::auth::{LoginRequest, LoginSuccess};
use crate::dto::{
    AclRequestDto, AlterConfigRequestDto, CreatePartitionsRequestDto, CreateTopicRequestDto,
    DeleteTopicRequestDto, GroupRow, LogDirMoveRequestDto, LogDirRow, QuotaDeleteDto,
    QuotaUpsertDto, ResourceOutcome, ScramUserDeleteDto, ScramUserUpsertDto, TopicRow,
};
use crate::error::UiError;

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
    let LoginRequest {
        username: _,
        password: _,
    } = request;

    unauthenticated()
}

pub async fn logout() -> Result<(), UiError> {
    unauthenticated()
}

pub async fn current_session() -> Result<CurrentSession, UiError> {
    unauthenticated()
}

pub async fn list_topics() -> Result<Vec<TopicRow>, UiError> {
    unauthenticated()
}

pub async fn list_groups() -> Result<Vec<GroupRow>, UiError> {
    unauthenticated()
}

pub async fn list_acls() -> Result<Vec<AclRow>, UiError> {
    unauthenticated()
}

pub async fn list_users() -> Result<Vec<UserRow>, UiError> {
    unauthenticated()
}

pub async fn list_quotas() -> Result<Vec<QuotaRow>, UiError> {
    unauthenticated()
}

pub async fn list_log_dirs() -> Result<Vec<LogDirRow>, UiError> {
    unauthenticated()
}

pub async fn create_topic(request: CreateTopicRequestDto) -> Result<Vec<ResourceOutcome>, UiError> {
    ensure_valid_request(request.validate())?;

    unauthenticated()
}

pub async fn delete_topic(request: DeleteTopicRequestDto) -> Result<Vec<ResourceOutcome>, UiError> {
    ensure_valid_request(request.validate())?;

    unauthenticated()
}

pub async fn create_partitions(
    request: CreatePartitionsRequestDto,
) -> Result<Vec<ResourceOutcome>, UiError> {
    ensure_valid_request(request.validate())?;

    unauthenticated()
}

pub async fn alter_configs(
    request: AlterConfigRequestDto,
) -> Result<Vec<ResourceOutcome>, UiError> {
    ensure_valid_request(request.validate())?;

    unauthenticated()
}

pub async fn create_acl(request: AclRequestDto) -> Result<Vec<ResourceOutcome>, UiError> {
    ensure_valid_request(request.validate())?;

    unauthenticated()
}

pub async fn delete_acl(request: AclRequestDto) -> Result<Vec<ResourceOutcome>, UiError> {
    ensure_valid_request(request.validate())?;

    unauthenticated()
}

pub async fn upsert_scram_sha512_user(
    request: ScramUserUpsertDto,
) -> Result<Vec<ResourceOutcome>, UiError> {
    ensure_valid_request(request.validate())?;

    unauthenticated()
}

pub async fn delete_scram_user(
    request: ScramUserDeleteDto,
) -> Result<Vec<ResourceOutcome>, UiError> {
    ensure_valid_request(request.validate())?;

    unauthenticated()
}

pub async fn upsert_quota(request: QuotaUpsertDto) -> Result<Vec<ResourceOutcome>, UiError> {
    ensure_valid_request(request.validate())?;

    unauthenticated()
}

pub async fn delete_quota(request: QuotaDeleteDto) -> Result<Vec<ResourceOutcome>, UiError> {
    ensure_valid_request(request.validate())?;

    unauthenticated()
}

pub async fn move_log_dir(request: LogDirMoveRequestDto) -> Result<Vec<ResourceOutcome>, UiError> {
    ensure_valid_request(request.validate())?;

    unauthenticated()
}

fn ensure_valid_request(validation: Result<(), String>) -> Result<(), UiError> {
    validation.map_err(UiError::Admin)
}

fn unauthenticated<T>() -> Result<T, UiError> {
    Err(UiError::NotAuthenticated)
}
