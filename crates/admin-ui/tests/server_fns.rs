#![allow(clippy::duration_suboptimal_units)]

use std::sync::Arc;
use std::time::Duration;

use crabka_admin_ui::auth::LoginRequest;
use crabka_admin_ui::config::{AdminUiConfig, BrokerSecurityConfig};
use crabka_admin_ui::error::UiError;
use crabka_admin_ui::server::AppState;
use crabka_admin_ui::session::SessionStore;

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

    assert!(matches!(result, Err(UiError::NotAuthenticated)));
    assert_debug_does_not_contain_secret(
        &format_result_debug(&result),
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

fn format_result_debug<T: std::fmt::Debug>(result: &Result<T, UiError>) -> String {
    format!("{result:?}")
}

fn assert_debug_does_not_contain_secret(debug: &str, secret: &str, label: &str) {
    assert!(!debug.contains(secret), "debug output leaked {label}");
}
