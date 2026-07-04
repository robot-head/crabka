//! Server-function seam for the Dioxus admin UI shell.

#![allow(clippy::unused_async)]

use serde::{Deserialize, Serialize};

use crate::auth::{LoginRequest, LoginSuccess};
use crate::dto::{GroupRow, LogDirRow, TopicRow};
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

fn unauthenticated<T>() -> Result<T, UiError> {
    Err(UiError::NotAuthenticated)
}
