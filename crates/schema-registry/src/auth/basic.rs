//! HTTP Basic credential store (the only new auth primitive; the rest reuses
//! `crabka-security`). A plaintext credential is cp `PropertyFileLoginModule`
//! parity; a `$2…` value is bcrypt-verified.
use std::collections::HashMap;

/// Username → credential map for HTTP Basic auth. A stored value beginning with
/// `$2` is a bcrypt hash; anything else is a plaintext password.
#[derive(Debug, Clone, Default)]
pub struct BasicAuthStore {
    users: HashMap<String, String>,
}

impl BasicAuthStore {
    /// Build directly from an in-memory `user -> credential` map.
    #[must_use]
    pub fn from_users(users: HashMap<String, String>) -> Self {
        Self { users }
    }

    /// Build from config: htpasswd-style `user:cred` file lines, then the inline
    /// `users` map layered on top (inline wins).
    ///
    /// # Errors
    ///
    /// Returns the underlying [`std::io::Error`] if `cfg.file` is set but cannot
    /// be read.
    pub fn load(cfg: &crate::config::BasicAuthConfig) -> std::io::Result<Self> {
        let mut users = HashMap::new();
        if let Some(path) = &cfg.file {
            for line in std::fs::read_to_string(path)?.lines() {
                let line = line.trim();
                if line.is_empty() || line.starts_with('#') {
                    continue;
                }
                if let Some((u, c)) = line.split_once(':') {
                    users.insert(u.to_string(), c.to_string());
                }
            }
        }
        users.extend(cfg.users.clone());
        Ok(Self { users })
    }

    /// Verify `user`/`pass`. `$2…` stored values are bcrypt; otherwise a
    /// constant-time plaintext compare.
    #[must_use]
    pub fn verify(&self, user: &str, pass: &str) -> bool {
        let Some(stored) = self.users.get(user) else {
            return false;
        };
        if stored.starts_with("$2") {
            return bcrypt::verify(pass, stored).unwrap_or(false);
        }
        constant_time_eq(stored.as_bytes(), pass.as_bytes())
    }
}

/// Length-independent constant-time byte compare (no extra deps).
fn constant_time_eq(a: &[u8], b: &[u8]) -> bool {
    if a.len() != b.len() {
        return false;
    }
    let mut diff = 0u8;
    for (x, y) in a.iter().zip(b.iter()) {
        diff |= x ^ y;
    }
    diff == 0
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn plaintext_verify() {
        let s = BasicAuthStore::from_users(
            [("alice".to_string(), "pw".to_string())]
                .into_iter()
                .collect(),
        );
        assert!(s.verify("alice", "pw"));
        assert!(!s.verify("alice", "bad"));
        assert!(!s.verify("bob", "pw"));
    }

    #[test]
    fn bcrypt_verify() {
        let hash = bcrypt::hash("pw", 4).unwrap();
        let s = BasicAuthStore::from_users([("alice".to_string(), hash)].into_iter().collect());
        assert!(s.verify("alice", "pw"));
        assert!(!s.verify("alice", "bad"));
    }

    #[test]
    fn load_inline_users_win_over_file() {
        // htpasswd file says alice:filepw; inline config says alice:inlinepw.
        // Inline (CLI/config) is more explicit and must win the conflict.
        let dir = std::env::temp_dir();
        let path = dir.join(format!(
            "crabka-sr-basic-auth-{}.htpasswd",
            std::process::id()
        ));
        std::fs::write(&path, "# comment\n\nalice:filepw\nbob:bobpw\n").unwrap();

        let cfg = crate::config::BasicAuthConfig {
            users: [("alice".to_string(), "inlinepw".to_string())]
                .into_iter()
                .collect(),
            file: Some(path.clone()),
        };
        let store = BasicAuthStore::load(&cfg).unwrap();
        std::fs::remove_file(&path).ok();

        assert!(store.verify("alice", "inlinepw"), "inline credential wins");
        assert!(
            !store.verify("alice", "filepw"),
            "file credential is overridden"
        );
        // File-only entries (no inline override) still load.
        assert!(store.verify("bob", "bobpw"), "file-only entry preserved");
    }
}
