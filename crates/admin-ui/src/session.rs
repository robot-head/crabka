use std::collections::HashMap;
use std::fmt;
use std::time::{Duration, Instant};

use parking_lot::RwLock;
use uuid::Uuid;

#[derive(Clone, PartialEq, Eq, Hash)]
pub struct SessionId(String);

impl SessionId {
    #[must_use]
    pub fn new() -> Self {
        Self(Uuid::new_v4().to_string())
    }

    #[must_use]
    pub fn expose_for_cookie(&self) -> &str {
        &self.0
    }
}

impl fmt::Debug for SessionId {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("SessionId(<redacted>)")
    }
}

impl Default for SessionId {
    fn default() -> Self {
        Self::new()
    }
}

impl TryFrom<&str> for SessionId {
    type Error = uuid::Error;

    fn try_from(value: &str) -> Result<Self, Self::Error> {
        let parsed_uuid = Uuid::parse_str(value)?;

        Ok(Self(parsed_uuid.to_string()))
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SessionUser {
    pub username: String,
    pub principal: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SessionRecord {
    pub user: SessionUser,
    pub expires_at: Instant,
}

impl SessionRecord {
    #[must_use]
    pub fn is_expired(&self, now: Instant) -> bool {
        now >= self.expires_at
    }
}

pub struct SessionStore {
    ttl: Duration,
    sessions: RwLock<HashMap<SessionId, SessionRecord>>,
}

impl fmt::Debug for SessionStore {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("SessionStore")
            .field("ttl", &self.ttl)
            .field("session_count", &self.sessions.read().len())
            .finish()
    }
}

impl SessionStore {
    #[must_use]
    pub fn new(ttl: Duration) -> Self {
        Self {
            ttl,
            sessions: RwLock::new(HashMap::new()),
        }
    }

    pub fn create(&self, user: SessionUser) -> SessionId {
        let session_id = SessionId::new();
        let now = Instant::now();
        let session_record = SessionRecord {
            user,
            expires_at: now.checked_add(self.ttl).unwrap_or(now),
        };

        self.sessions
            .write()
            .insert(session_id.clone(), session_record);

        session_id
    }

    #[must_use]
    pub fn get(&self, id: &SessionId) -> Option<SessionRecord> {
        let now = Instant::now();
        let record = self.sessions.read().get(id).cloned()?;

        if !record.is_expired(now) {
            return Some(record);
        }

        self.sessions.write().remove(id);
        None
    }

    pub fn remove(&self, id: &SessionId) -> bool {
        self.sessions.write().remove(id).is_some()
    }
}
