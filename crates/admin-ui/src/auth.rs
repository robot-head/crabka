//! Broker-backed login for the admin UI.

use std::{fmt, future::Future, pin::Pin};

use crabka_client_admin::AdminClient;
use crabka_client_core::security::{ClientSecurity, SaslCredentials};
use crabka_security::SaslMechanism;
use serde::{Deserialize, Serialize};

use crate::{
    config::AdminUiConfig,
    error::UiError,
    session::{SessionCredentials, SessionStore, SessionUser},
};

#[derive(Clone, PartialEq, Eq, Deserialize)]
pub struct LoginRequest {
    pub username: String,
    pub password: String,
}

#[derive(Clone, PartialEq, Eq, Serialize)]
pub struct LoginSuccess {
    pub username: String,
    pub principal: String,
    pub session_id: String,
}

impl fmt::Debug for LoginSuccess {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("LoginSuccess")
            .field("username", &self.username)
            .field("principal", &self.principal)
            .field("session_id", &"<redacted>")
            .finish()
    }
}

#[must_use]
pub fn build_scram_sha512_security(
    cfg: &AdminUiConfig,
    username: &str,
    password: &str,
) -> ClientSecurity {
    ClientSecurity {
        protocol: cfg.security.listener_protocol(),
        tls: cfg.security.tls(),
        sasl: Some(SaslCredentials::Scram {
            mechanism: SaslMechanism::ScramSha512,
            username: username.to_string(),
            password: password.to_string(),
        }),
        sasl_host: None,
    }
}

#[derive(Debug, Clone, Copy, Default)]
pub struct AdminClientLoginBroker;

pub trait LoginBroker {
    fn check_login<'a>(
        &'a self,
        cfg: &'a AdminUiConfig,
        username: &'a str,
        password: &'a str,
    ) -> Pin<Box<dyn Future<Output = Result<(), UiError>> + Send + 'a>>;
}

impl LoginBroker for AdminClientLoginBroker {
    fn check_login<'a>(
        &'a self,
        cfg: &'a AdminUiConfig,
        username: &'a str,
        password: &'a str,
    ) -> Pin<Box<dyn Future<Output = Result<(), UiError>> + Send + 'a>> {
        Box::pin(async move {
            let security = build_scram_sha512_security(cfg, username, password);
            AdminClient::connect_secured(&cfg.bootstrap_addrs, Some(security)).await?;
            Ok(())
        })
    }
}

impl<T: LoginBroker + ?Sized> LoginBroker for &T {
    fn check_login<'a>(
        &'a self,
        cfg: &'a AdminUiConfig,
        username: &'a str,
        password: &'a str,
    ) -> Pin<Box<dyn Future<Output = Result<(), UiError>> + Send + 'a>> {
        (*self).check_login(cfg, username, password)
    }
}

pub struct AuthService<'a, B = AdminClientLoginBroker> {
    cfg: &'a AdminUiConfig,
    sessions: &'a SessionStore,
    broker: B,
}

impl<'a> AuthService<'a, AdminClientLoginBroker> {
    #[must_use]
    pub const fn new(cfg: &'a AdminUiConfig, sessions: &'a SessionStore) -> Self {
        Self {
            cfg,
            sessions,
            broker: AdminClientLoginBroker,
        }
    }
}

impl<'a, B: LoginBroker> AuthService<'a, B> {
    #[must_use]
    pub const fn new_with_broker(
        cfg: &'a AdminUiConfig,
        sessions: &'a SessionStore,
        broker: B,
    ) -> Self {
        Self {
            cfg,
            sessions,
            broker,
        }
    }

    pub async fn login(&self, request: LoginRequest) -> Result<LoginSuccess, UiError> {
        self.broker
            .check_login(self.cfg, &request.username, &request.password)
            .await?;

        let principal = format!("User:{}", request.username);
        let session_id = self.sessions.create_authenticated(
            SessionUser {
                username: request.username.clone(),
                principal: principal.clone(),
            },
            SessionCredentials::scram_sha512(request.password),
        );

        Ok(LoginSuccess {
            username: request.username,
            principal,
            session_id: session_id.expose_for_cookie().to_string(),
        })
    }
}
