use std::collections::HashMap;
use std::time::{Duration, Instant};

use parking_lot::RwLock;
use uuid::Uuid;

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
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

#[derive(Debug)]
pub struct SessionStore {
    ttl: Duration,
    sessions: RwLock<HashMap<SessionId, SessionRecord>>,
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
        let session_record = SessionRecord {
            user,
            expires_at: Instant::now() + self.ttl,
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
