#![allow(clippy::duration_suboptimal_units)]

use std::time::Duration;

use crabka_admin_ui::session::{SessionId, SessionStore, SessionUser};

#[test]
fn session_store_creates_and_retrieves_user() {
    let store = SessionStore::new(Duration::from_secs(60));

    let id = store.create(SessionUser {
        username: "alice".to_string(),
        principal: "User:alice".to_string(),
    });

    let record = store.get(&id).expect("session exists");
    assert_eq!(record.user.username, "alice");
    assert_eq!(record.user.principal, "User:alice");
}

#[test]
fn logout_removes_session() {
    let store = SessionStore::new(Duration::from_secs(60));
    let id = store.create(SessionUser {
        username: "bob".to_string(),
        principal: "User:bob".to_string(),
    });

    assert!(store.remove(&id));
    assert!(store.get(&id).is_none());
}

#[test]
fn expired_session_returns_none_without_panicking() {
    let store = SessionStore::new(Duration::ZERO);
    let id = store.create(SessionUser {
        username: "carol".to_string(),
        principal: "User:carol".to_string(),
    });

    assert!(store.get(&id).is_none());
}

#[test]
fn session_id_try_from_accepts_cookie_uuid_and_rejects_invalid_value() {
    let id = SessionId::new();
    let cookie_value = id.expose_for_cookie();

    let parsed = SessionId::try_from(cookie_value).expect("uuid cookie value is valid");

    assert_eq!(parsed, id);
    assert!(SessionId::try_from("not-a-uuid").is_err());
}

#[test]
fn session_id_debug_redacts_cookie_value() {
    let id = SessionId::new();

    let debug_output = format!("{id:?}");

    assert!(!debug_output.contains(id.expose_for_cookie()));
}

#[test]
fn oversized_ttl_session_creation_does_not_panic() {
    let result = std::panic::catch_unwind(|| {
        let store = SessionStore::new(Duration::MAX);
        store.create(SessionUser {
            username: "dave".to_string(),
            principal: "User:dave".to_string(),
        })
    });

    assert!(result.is_ok());
}

#[test]
fn session_store_debug_does_not_include_session_storage() {
    let store = SessionStore::new(Duration::from_secs(60));
    store.create(SessionUser {
        username: "erin".to_string(),
        principal: "User:erin".to_string(),
    });

    let debug_output = format!("{store:?}");

    assert!(!debug_output.contains("sessions"));
    assert!(!debug_output.contains("erin"));
}
