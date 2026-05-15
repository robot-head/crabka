# Slice 12: Auth & security — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add TLS, SASL/PLAIN, SASL/SCRAM-SHA-512, multi-listener configuration, inter-broker auth, and KIP-554 `AlterUserScramCredentials` so JVM Kafka clients can connect to a Crabka broker over `SASL_SSL` and the broker authenticates inter-broker replication / raft / heartbeat traffic.

**Architecture:** New `crabka-security` crate holds the pure-logic SCRAM + PLAIN + TLS-config types (shared by broker and CLI). New `crabka-cli` crate ships a single binary with a `format --add-scram` subcommand. `crabka-broker` gains a listener registry (one accept loop per `ListenerSpec`), a per-connection `ConnectionAuth` state machine, three new wire handlers (api_keys 17, 36, 51), and an `InterBrokerClient` that runs TLS + outbound SASL before handing the stream to existing fetch/raft RPC code. `crabka-metadata` gets two new records (`V1ScramCredential`, `V1DeleteScramCredential`).

**Tech Stack:** Rust 1.95.0; `rustls` 0.23 + `tokio-rustls` 0.26 for TLS; `pbkdf2` 0.12 + `hmac` 0.12 + `sha2` 0.10 + `ring` 0.17 (constant-time compare + RNG) for crypto; existing `openraft 0.9.24` + `serde_wincode` for new metadata records; existing `crabka_protocol::owned::*` generated types (SaslHandshake / SaslAuthenticate / AlterUserScramCredentials all already exist on disk).

**Reference spec:** [`docs/superpowers/specs/2026-05-14-crabka-auth-security-12-design.md`](../specs/2026-05-14-crabka-auth-security-12-design.md).

**Working directory:** `C:\Users\Matt Stone\git\crabka`. Implementation runs on `feature/auth-security-12` (create off main at the start of task 1).

---

## File structure

```
crates/security/                          # NEW crate
├── Cargo.toml
└── src/
    ├── lib.rs                            # public re-exports
    ├── listener.rs                       # ListenerProtocol enum
    ├── mechanism.rs                      # SaslMechanism enum
    ├── principal.rs                      # Principal + AuthError
    ├── tls.rs                            # TlsConfig + rustls server/client builders
    ├── scram/
    │   ├── mod.rs                        # ScramCredential, hash_scram_password
    │   ├── server.rs                     # ScramServerExchange
    │   └── client.rs                     # ScramClientExchange
    └── plain.rs                          # verify_plain (constant-time compare)

crates/cli/                               # NEW crate
├── Cargo.toml
└── src/
    ├── main.rs                           # clap entry point
    └── format.rs                         # `crabka format --add-scram` impl

crates/metadata/src/
├── records.rs                            # MODIFIED — V1ScramCredential + V1DeleteScramCredential
├── image.rs                              # MODIFIED — scram_credentials field + accessor + apply/validate
└── error.rs                              # MODIFIED — UnknownUser/UnknownMechanism variants

crates/broker/src/
├── config.rs                             # MODIFIED — ListenerSpec, InterBrokerCredentials, super_user_name, etc.
├── broker.rs                             # MODIFIED — spawn per-listener accept loops; TLS termination
├── error.rs                              # MODIFIED — ListenerConflict, InvalidInterBrokerListener, Tls, etc.
├── network/
│   ├── dispatch.rs                       # MODIFIED — ConnectionAuth state + pre-auth gate
│   ├── auth.rs                           # NEW — SaslHandshake + SaslAuthenticate handler bodies
│   └── client.rs                         # NEW — InterBrokerClient (TLS + outbound SASL)
├── replicator.rs                         # MODIFIED — route through InterBrokerClient
├── heartbeat/controller_state.rs         # MODIFIED — heartbeat client through InterBrokerClient
└── handlers/
    ├── mod.rs                            # MODIFIED — register api_keys 17, 36, 51
    ├── api_versions.rs                   # MODIFIED — add 17, 36, 51 to supported_apis
    └── alter_user_scram_credentials.rs   # NEW — api_key 51 (KIP-554)

crates/raft/src/
└── transport.rs                          # MODIFIED — outbound raft RPC dials via InterBrokerClient

crates/broker/tests/
├── auth_handlers.rs                      # NEW — broker-side integration tests
└── jvm_acceptance.rs                     # MODIFIED — 4 new JVM tests
```

The plan is structured in **ten batches** mirroring the slice-11 cadence. Each batch is a self-contained set of tasks ending in a commit. Batches build sequentially.

---

## Batch 1 — `crabka-security` crate

### Task 1: Create the `crabka-security` crate skeleton

**Files:**
- Create: `crates/security/Cargo.toml`
- Create: `crates/security/src/lib.rs`
- Create: `crates/security/src/listener.rs`
- Create: `crates/security/src/mechanism.rs`
- Create: `crates/security/src/principal.rs`
- Modify: `Cargo.toml` (workspace deps)

- [ ] **Step 1: Create branch + crate directory**

```bash
git checkout main && git pull --ff-only origin main
git checkout -b feature/auth-security-12
mkdir -p crates/security/src/scram
```

- [ ] **Step 2: Add workspace deps**

Edit `Cargo.toml` workspace `[workspace.dependencies]` block, append:

```toml
rustls = { version = "0.23", default-features = false, features = ["std", "tls12"] }
rustls-pemfile = "2"
tokio-rustls = { version = "0.26", default-features = false, features = ["tls12"] }
pbkdf2 = { version = "0.12", default-features = false, features = ["std", "hmac"] }
hmac = "0.12"
sha2 = "0.10"
ring = "0.17"
subtle = "2"
base64 = "0.22"
```

(`subtle` is used for constant-time comparison; `ring` is the RNG; `base64` is needed because SCRAM messages are framed in base64.)

- [ ] **Step 3: Write `crates/security/Cargo.toml`**

```toml
[package]
name = "crabka-security"
version.workspace = true
edition.workspace = true
license.workspace = true
authors.workspace = true
rust-version.workspace = true

[lints]
workspace = true

[dependencies]
thiserror.workspace = true
serde.workspace = true
rustls.workspace = true
rustls-pemfile.workspace = true
pbkdf2.workspace = true
hmac.workspace = true
sha2.workspace = true
ring.workspace = true
subtle.workspace = true
base64.workspace = true
tracing.workspace = true

[dev-dependencies]
hex.workspace = true
```

- [ ] **Step 4: Write `crates/security/src/lib.rs`**

```rust
//! Pure-logic auth primitives used by the broker and CLI.
//!
//! No I/O, no async, no networking. The broker plumbs streams in; this
//! crate produces verifiers, hashes, and TLS configs.

mod listener;
mod mechanism;
mod plain;
mod principal;
pub mod scram;
mod tls;

pub use listener::ListenerProtocol;
pub use mechanism::SaslMechanism;
pub use plain::verify_plain;
pub use principal::{AuthError, Principal};
pub use scram::{
    ScramClientExchange, ScramCredential, ScramServerExchange, StepResult,
    hash_scram_password,
};
pub use tls::TlsConfig;
```

- [ ] **Step 5: Write `crates/security/src/listener.rs`**

```rust
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum ListenerProtocol {
    Plaintext,
    Ssl,
    SaslPlaintext,
    SaslSsl,
}

impl ListenerProtocol {
    #[must_use]
    pub fn requires_tls(self) -> bool {
        matches!(self, Self::Ssl | Self::SaslSsl)
    }

    #[must_use]
    pub fn requires_sasl(self) -> bool {
        matches!(self, Self::SaslPlaintext | Self::SaslSsl)
    }
}
```

- [ ] **Step 6: Write `crates/security/src/mechanism.rs`**

```rust
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum SaslMechanism {
    Plain,
    ScramSha512,
}

impl SaslMechanism {
    #[must_use]
    pub fn wire_name(self) -> &'static str {
        match self {
            Self::Plain => "PLAIN",
            Self::ScramSha512 => "SCRAM-SHA-512",
        }
    }

    #[must_use]
    pub fn from_wire(s: &str) -> Option<Self> {
        match s {
            "PLAIN" => Some(Self::Plain),
            "SCRAM-SHA-512" => Some(Self::ScramSha512),
            _ => None,
        }
    }
}
```

- [ ] **Step 7: Write `crates/security/src/principal.rs`**

```rust
use thiserror::Error;
use crate::SaslMechanism;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Principal {
    pub name: String,
    pub mechanism: SaslMechanism,
}

#[derive(Debug, Error, Clone, PartialEq, Eq)]
pub enum AuthError {
    #[error("unknown user")]
    UnknownUser,
    #[error("bad password")]
    BadPassword,
    #[error("bad proof")]
    BadProof,
    #[error("malformed message")]
    MalformedMessage,
    #[error("unsupported mechanism")]
    UnsupportedMechanism,
}
```

- [ ] **Step 8: Stub the remaining modules so `cargo check` passes**

Write `crates/security/src/plain.rs`:

```rust
use std::collections::HashMap;
use crate::{AuthError, Principal, SaslMechanism};

pub fn verify_plain(
    _creds: &HashMap<String, String>,
    _user: &str,
    _password: &[u8],
) -> Result<Principal, AuthError> {
    let _ = SaslMechanism::Plain;
    Err(AuthError::UnknownUser)
}
```

Write `crates/security/src/tls.rs`:

```rust
//! TLS config — implemented in task 4.

use std::path::PathBuf;

#[derive(Debug, Clone)]
pub struct TlsConfig {
    pub cert_chain_path: PathBuf,
    pub private_key_path: PathBuf,
    pub trust_roots_path: Option<PathBuf>,
}
```

Write `crates/security/src/scram/mod.rs`:

```rust
//! SCRAM-SHA-512 — implemented in tasks 2-3.

mod server;
mod client;

pub use server::{ScramServerExchange, StepResult};
pub use client::ScramClientExchange;

use crate::SaslMechanism;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ScramCredential {
    pub mechanism: SaslMechanism,
    pub salt: Vec<u8>,
    pub stored_key: Vec<u8>,
    pub server_key: Vec<u8>,
    pub iterations: u32,
}

#[must_use]
pub fn hash_scram_password(
    _password: &[u8],
    mechanism: SaslMechanism,
    iterations: u32,
) -> ScramCredential {
    ScramCredential {
        mechanism,
        salt: vec![],
        stored_key: vec![],
        server_key: vec![],
        iterations,
    }
}
```

Write `crates/security/src/scram/server.rs`:

```rust
//! ScramServerExchange — implemented in task 3.

use crate::{AuthError, Principal};
use super::ScramCredential;

#[derive(Debug)]
pub struct ScramServerExchange {
    _credential: ScramCredential,
}

#[derive(Debug)]
pub enum StepResult {
    Continue(Vec<u8>),
    Done(Principal, Vec<u8>),
    Failed(AuthError),
}

impl ScramServerExchange {
    #[must_use]
    pub fn new(credential: ScramCredential) -> Self {
        Self { _credential: credential }
    }

    pub fn step(&mut self, _client_bytes: &[u8]) -> StepResult {
        StepResult::Failed(AuthError::MalformedMessage)
    }
}
```

Write `crates/security/src/scram/client.rs`:

```rust
//! ScramClientExchange — implemented in task 3.

use crate::AuthError;

#[derive(Debug)]
pub struct ScramClientExchange {
    _username: String,
    _password: Vec<u8>,
}

impl ScramClientExchange {
    #[must_use]
    pub fn new(username: String, password: Vec<u8>) -> Self {
        Self { _username: username, _password: password }
    }

    pub fn step(&mut self, _server_bytes: &[u8]) -> Result<Vec<u8>, AuthError> {
        Err(AuthError::MalformedMessage)
    }
}
```

- [ ] **Step 9: Verify it builds**

Run: `cargo check -p crabka-security`
Expected: clean.

- [ ] **Step 10: Commit**

```bash
git add Cargo.toml crates/security/
git commit -m "feat(security): scaffold crabka-security crate"
```

---

### Task 2: PBKDF2 + `hash_scram_password`

**Files:**
- Modify: `crates/security/src/scram/mod.rs`
- Test: same file (`#[cfg(test)] mod tests`)

- [ ] **Step 1: Write the failing test**

Append to `crates/security/src/scram/mod.rs`:

```rust
#[cfg(test)]
mod tests {
    use super::*;
    use sha2::{Digest, Sha512};

    /// RFC 7677 vector for SCRAM-SHA-256 doesn't translate directly,
    /// but we can verify PBKDF2-HMAC-SHA-512 with a known vector and
    /// then assert stored_key = H(client_key), server_key = HMAC(salted, "Server Key").
    #[test]
    fn hash_scram_password_produces_expected_keys() {
        let password = b"pencil";
        let cred = hash_scram_password(password, SaslMechanism::ScramSha512, 4096);
        assert_eq!(cred.mechanism, SaslMechanism::ScramSha512);
        assert_eq!(cred.salt.len(), 16, "salt must be 16 bytes");
        assert_eq!(cred.stored_key.len(), 64, "SHA-512 output is 64 bytes");
        assert_eq!(cred.server_key.len(), 64);
        assert_eq!(cred.iterations, 4096);
        // stored_key = H(client_key) — verify by recomputing
        let salted = pbkdf2::pbkdf2_hmac_array::<sha2::Sha512, 64>(
            password,
            &cred.salt,
            cred.iterations,
        );
        let client_key = {
            use hmac::{Hmac, Mac};
            let mut m = <Hmac<Sha512>>::new_from_slice(&salted).unwrap();
            m.update(b"Client Key");
            m.finalize().into_bytes()
        };
        let expected_stored = Sha512::digest(&client_key);
        assert_eq!(cred.stored_key, expected_stored.as_slice());
    }

    #[test]
    fn hash_scram_password_is_deterministic_given_salt() {
        // Internal helper that takes a fixed salt for reproducibility.
        // We can't assert against a public hash_scram_password (which generates
        // random salt), so smoke-test via two calls producing different salts.
        let a = hash_scram_password(b"x", SaslMechanism::ScramSha512, 4096);
        let b = hash_scram_password(b"x", SaslMechanism::ScramSha512, 4096);
        assert_ne!(a.salt, b.salt, "fresh salt each call");
    }
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cargo test -p crabka-security hash_scram_password`
Expected: FAIL — stub returns empty vecs.

- [ ] **Step 3: Implement `hash_scram_password`**

Replace the stub `hash_scram_password` in `crates/security/src/scram/mod.rs`:

```rust
use hmac::{Hmac, Mac};
use sha2::{Digest, Sha512};
use ring::rand::{SecureRandom, SystemRandom};

#[must_use]
pub fn hash_scram_password(
    password: &[u8],
    mechanism: SaslMechanism,
    iterations: u32,
) -> ScramCredential {
    let mut salt = vec![0u8; 16];
    SystemRandom::new()
        .fill(&mut salt)
        .expect("system RNG must succeed");
    hash_scram_password_with_salt(password, mechanism, iterations, salt)
}

/// Test-only entry that lets callers fix the salt (for golden vectors).
#[must_use]
pub fn hash_scram_password_with_salt(
    password: &[u8],
    mechanism: SaslMechanism,
    iterations: u32,
    salt: Vec<u8>,
) -> ScramCredential {
    assert_eq!(
        mechanism,
        SaslMechanism::ScramSha512,
        "only SCRAM-SHA-512 supported in slice 12"
    );
    let salted: [u8; 64] = pbkdf2::pbkdf2_hmac_array::<Sha512, 64>(
        password,
        &salt,
        iterations,
    );
    let mut client_key_mac = <Hmac<Sha512>>::new_from_slice(&salted)
        .expect("hmac accepts any key length");
    client_key_mac.update(b"Client Key");
    let client_key = client_key_mac.finalize().into_bytes();
    let stored_key = Sha512::digest(&client_key).to_vec();

    let mut server_key_mac = <Hmac<Sha512>>::new_from_slice(&salted)
        .expect("hmac accepts any key length");
    server_key_mac.update(b"Server Key");
    let server_key = server_key_mac.finalize().into_bytes().to_vec();

    ScramCredential {
        mechanism,
        salt,
        stored_key,
        server_key,
        iterations,
    }
}
```

- [ ] **Step 4: Run test to verify it passes**

Run: `cargo test -p crabka-security hash_scram_password`
Expected: PASS (2 tests).

- [ ] **Step 5: Commit**

```bash
git add crates/security/src/scram/mod.rs
git commit -m "feat(security): PBKDF2-HMAC-SHA-512 SCRAM credential hashing"
```

---

### Task 3: `ScramServerExchange` + `ScramClientExchange` round-trip

**Files:**
- Modify: `crates/security/src/scram/server.rs`
- Modify: `crates/security/src/scram/client.rs`
- Test: integration test in `crates/security/src/scram/mod.rs`

SCRAM is a multi-round-trip auth protocol (RFC 5802). The server and client each have a small state machine. Reference: client-first → server-first → client-final → server-final.

Wire formats (with `,`-separated attributes):
- `client-first-message`: `n,,n=<username>,r=<client-nonce>`
- `server-first-message`: `r=<client-nonce><server-nonce>,s=<base64-salt>,i=<iterations>`
- `client-final-message`: `c=<base64-channel-binding>,r=<combined-nonce>,p=<base64-proof>`
- `server-final-message`: `v=<base64-server-signature>` or `e=<error>`

- [ ] **Step 1: Write the round-trip test**

Append to `crates/security/src/scram/mod.rs` `mod tests`:

```rust
    use crate::scram::client::ScramClientExchange;
    use crate::scram::server::{ScramServerExchange, StepResult};

    #[test]
    fn scram_server_and_client_round_trip() {
        let password = b"hunter2";
        let cred = hash_scram_password_with_salt(
            password,
            SaslMechanism::ScramSha512,
            4096,
            (0..16).collect::<Vec<u8>>(),
        );
        let mut server = ScramServerExchange::new("alice".to_string(), cred);
        let mut client = ScramClientExchange::new("alice".to_string(), password.to_vec());

        // Client first
        let c1 = client.client_first().expect("client first");
        // Server step 1 -> server-first
        let s1 = match server.step(&c1) {
            StepResult::Continue(b) => b,
            other => panic!("server step 1 must continue, got {other:?}"),
        };
        // Client final
        let c2 = client.step(&s1).expect("client final");
        // Server step 2 -> done
        let (principal, s2) = match server.step(&c2) {
            StepResult::Done(p, b) => (p, b),
            other => panic!("server step 2 must Done, got {other:?}"),
        };
        assert_eq!(principal.name, "alice");
        assert_eq!(principal.mechanism, SaslMechanism::ScramSha512);
        // Client verifies server signature
        let final_check = client.verify_server_final(&s2);
        assert!(final_check.is_ok(), "server signature must verify");
    }

    #[test]
    fn scram_server_rejects_bad_proof() {
        let cred = hash_scram_password_with_salt(
            b"correct",
            SaslMechanism::ScramSha512,
            4096,
            vec![0u8; 16],
        );
        let mut server = ScramServerExchange::new("alice".to_string(), cred);
        let mut client = ScramClientExchange::new("alice".to_string(), b"wrong".to_vec());
        let c1 = client.client_first().unwrap();
        let s1 = match server.step(&c1) {
            StepResult::Continue(b) => b,
            _ => panic!(),
        };
        let c2 = client.step(&s1).unwrap();
        match server.step(&c2) {
            StepResult::Failed(crate::AuthError::BadProof) => {}
            other => panic!("expected BadProof, got {other:?}"),
        }
    }
```

- [ ] **Step 2: Replace `crates/security/src/scram/server.rs`**

```rust
use base64::Engine;
use base64::engine::general_purpose::STANDARD as B64;
use hmac::{Hmac, Mac};
use ring::rand::{SecureRandom, SystemRandom};
use sha2::{Digest, Sha512};
use subtle::ConstantTimeEq;

use crate::{AuthError, Principal, SaslMechanism};
use super::ScramCredential;

#[derive(Debug)]
enum State {
    AwaitingClientFirst,
    AwaitingClientFinal {
        client_first_bare: String,
        server_first: String,
    },
    Finished,
}

#[derive(Debug)]
pub struct ScramServerExchange {
    username: String,
    credential: ScramCredential,
    state: State,
}

#[derive(Debug)]
pub enum StepResult {
    Continue(Vec<u8>),
    Done(Principal, Vec<u8>),
    Failed(AuthError),
}

impl ScramServerExchange {
    #[must_use]
    pub fn new(username: String, credential: ScramCredential) -> Self {
        Self {
            username,
            credential,
            state: State::AwaitingClientFirst,
        }
    }

    pub fn step(&mut self, client_bytes: &[u8]) -> StepResult {
        match std::mem::replace(&mut self.state, State::Finished) {
            State::AwaitingClientFirst => self.step_first(client_bytes),
            State::AwaitingClientFinal {
                client_first_bare,
                server_first,
            } => self.step_final(client_bytes, &client_first_bare, &server_first),
            State::Finished => StepResult::Failed(AuthError::MalformedMessage),
        }
    }

    fn step_first(&mut self, client_bytes: &[u8]) -> StepResult {
        let s = match std::str::from_utf8(client_bytes) {
            Ok(s) => s,
            Err(_) => return StepResult::Failed(AuthError::MalformedMessage),
        };
        // GS2 header "n,," then bare client-first
        let bare = match s.strip_prefix("n,,") {
            Some(rest) => rest,
            None => return StepResult::Failed(AuthError::MalformedMessage),
        };
        let mut user = None;
        let mut nonce = None;
        for attr in bare.split(',') {
            if let Some(v) = attr.strip_prefix("n=") {
                user = Some(v.to_string());
            } else if let Some(v) = attr.strip_prefix("r=") {
                nonce = Some(v.to_string());
            }
        }
        let (Some(u), Some(c_nonce)) = (user, nonce) else {
            return StepResult::Failed(AuthError::MalformedMessage);
        };
        if u != self.username {
            return StepResult::Failed(AuthError::UnknownUser);
        }
        let mut server_nonce_bytes = [0u8; 18];
        SystemRandom::new().fill(&mut server_nonce_bytes).expect("rng");
        let server_nonce = B64.encode(server_nonce_bytes);
        let combined_nonce = format!("{c_nonce}{server_nonce}");
        let server_first = format!(
            "r={},s={},i={}",
            combined_nonce,
            B64.encode(&self.credential.salt),
            self.credential.iterations,
        );
        let response = server_first.clone().into_bytes();
        self.state = State::AwaitingClientFinal {
            client_first_bare: bare.to_string(),
            server_first,
        };
        StepResult::Continue(response)
    }

    fn step_final(
        &mut self,
        client_bytes: &[u8],
        client_first_bare: &str,
        server_first: &str,
    ) -> StepResult {
        let s = match std::str::from_utf8(client_bytes) {
            Ok(s) => s,
            Err(_) => return StepResult::Failed(AuthError::MalformedMessage),
        };
        let mut channel_binding = None;
        let mut nonce = None;
        let mut proof_b64 = None;
        for attr in s.split(',') {
            if let Some(v) = attr.strip_prefix("c=") {
                channel_binding = Some(v);
            } else if let Some(v) = attr.strip_prefix("r=") {
                nonce = Some(v);
            } else if let Some(v) = attr.strip_prefix("p=") {
                proof_b64 = Some(v);
            }
        }
        let (Some(_cb), Some(_nonce), Some(proof_b64)) = (channel_binding, nonce, proof_b64) else {
            return StepResult::Failed(AuthError::MalformedMessage);
        };
        let proof = match B64.decode(proof_b64) {
            Ok(b) if b.len() == 64 => b,
            _ => return StepResult::Failed(AuthError::MalformedMessage),
        };

        // client-final-without-proof = everything before ",p="
        let cf_no_proof_end = match s.rfind(",p=") {
            Some(i) => i,
            None => return StepResult::Failed(AuthError::MalformedMessage),
        };
        let client_final_no_proof = &s[..cf_no_proof_end];

        let auth_message = format!(
            "{client_first_bare},{server_first},{client_final_no_proof}",
        );

        // client_signature = HMAC(stored_key, auth_message)
        let mut mac = match <Hmac<Sha512>>::new_from_slice(&self.credential.stored_key) {
            Ok(m) => m,
            Err(_) => return StepResult::Failed(AuthError::MalformedMessage),
        };
        mac.update(auth_message.as_bytes());
        let client_signature = mac.finalize().into_bytes();

        // client_key = client_signature XOR proof
        let client_key: Vec<u8> = client_signature
            .iter()
            .zip(proof.iter())
            .map(|(a, b)| a ^ b)
            .collect();
        // stored_key = H(client_key)
        let computed_stored = Sha512::digest(&client_key);
        if computed_stored.ct_eq(&self.credential.stored_key).unwrap_u8() != 1 {
            return StepResult::Failed(AuthError::BadProof);
        }
        // server_signature = HMAC(server_key, auth_message)
        let mut server_mac = <Hmac<Sha512>>::new_from_slice(&self.credential.server_key)
            .expect("hmac");
        server_mac.update(auth_message.as_bytes());
        let server_signature = server_mac.finalize().into_bytes();
        let server_final = format!("v={}", B64.encode(&server_signature));
        StepResult::Done(
            Principal {
                name: self.username.clone(),
                mechanism: SaslMechanism::ScramSha512,
            },
            server_final.into_bytes(),
        )
    }
}
```

- [ ] **Step 3: Replace `crates/security/src/scram/client.rs`**

```rust
use base64::Engine;
use base64::engine::general_purpose::STANDARD as B64;
use hmac::{Hmac, Mac};
use ring::rand::{SecureRandom, SystemRandom};
use sha2::{Digest, Sha512};
use subtle::ConstantTimeEq;

use crate::AuthError;

#[derive(Debug)]
enum State {
    Initial,
    AwaitingServerFirst { client_first_bare: String, client_nonce: String },
    AwaitingServerFinal {
        auth_message: String,
        server_key: Vec<u8>,
    },
    Finished,
}

#[derive(Debug)]
pub struct ScramClientExchange {
    username: String,
    password: Vec<u8>,
    state: State,
}

impl ScramClientExchange {
    #[must_use]
    pub fn new(username: String, password: Vec<u8>) -> Self {
        Self {
            username,
            password,
            state: State::Initial,
        }
    }

    pub fn client_first(&mut self) -> Result<Vec<u8>, AuthError> {
        let mut nonce_bytes = [0u8; 18];
        SystemRandom::new().fill(&mut nonce_bytes).map_err(|_| AuthError::MalformedMessage)?;
        let client_nonce = B64.encode(nonce_bytes);
        let bare = format!("n={},r={}", self.username, client_nonce);
        let msg = format!("n,,{bare}");
        self.state = State::AwaitingServerFirst {
            client_first_bare: bare,
            client_nonce,
        };
        Ok(msg.into_bytes())
    }

    pub fn step(&mut self, server_bytes: &[u8]) -> Result<Vec<u8>, AuthError> {
        let State::AwaitingServerFirst { client_first_bare, client_nonce } =
            std::mem::replace(&mut self.state, State::Finished)
        else {
            return Err(AuthError::MalformedMessage);
        };
        let s = std::str::from_utf8(server_bytes).map_err(|_| AuthError::MalformedMessage)?;
        let mut nonce = None;
        let mut salt = None;
        let mut iterations = None;
        for attr in s.split(',') {
            if let Some(v) = attr.strip_prefix("r=") {
                nonce = Some(v.to_string());
            } else if let Some(v) = attr.strip_prefix("s=") {
                salt = Some(B64.decode(v).map_err(|_| AuthError::MalformedMessage)?);
            } else if let Some(v) = attr.strip_prefix("i=") {
                iterations = Some(v.parse::<u32>().map_err(|_| AuthError::MalformedMessage)?);
            }
        }
        let (Some(combined_nonce), Some(salt), Some(iters)) = (nonce, salt, iterations) else {
            return Err(AuthError::MalformedMessage);
        };
        if !combined_nonce.starts_with(&client_nonce) {
            return Err(AuthError::BadProof);
        }

        let salted: [u8; 64] = pbkdf2::pbkdf2_hmac_array::<Sha512, 64>(
            &self.password,
            &salt,
            iters,
        );
        let mut ck_mac = <Hmac<Sha512>>::new_from_slice(&salted)
            .map_err(|_| AuthError::MalformedMessage)?;
        ck_mac.update(b"Client Key");
        let client_key = ck_mac.finalize().into_bytes();
        let stored_key = Sha512::digest(&client_key);
        let mut sk_mac = <Hmac<Sha512>>::new_from_slice(&salted)
            .map_err(|_| AuthError::MalformedMessage)?;
        sk_mac.update(b"Server Key");
        let server_key = sk_mac.finalize().into_bytes().to_vec();

        let channel_binding = B64.encode(b"n,,");
        let client_final_no_proof = format!("c={channel_binding},r={combined_nonce}");
        let auth_message = format!(
            "{client_first_bare},{s},{client_final_no_proof}",
        );
        let mut cs_mac = <Hmac<Sha512>>::new_from_slice(&stored_key)
            .map_err(|_| AuthError::MalformedMessage)?;
        cs_mac.update(auth_message.as_bytes());
        let client_signature = cs_mac.finalize().into_bytes();
        let proof: Vec<u8> = client_key
            .iter()
            .zip(client_signature.iter())
            .map(|(a, b)| a ^ b)
            .collect();
        let client_final = format!("{client_final_no_proof},p={}", B64.encode(&proof));
        self.state = State::AwaitingServerFinal {
            auth_message,
            server_key,
        };
        Ok(client_final.into_bytes())
    }

    pub fn verify_server_final(&mut self, server_bytes: &[u8]) -> Result<(), AuthError> {
        let State::AwaitingServerFinal { auth_message, server_key } =
            std::mem::replace(&mut self.state, State::Finished)
        else {
            return Err(AuthError::MalformedMessage);
        };
        let s = std::str::from_utf8(server_bytes).map_err(|_| AuthError::MalformedMessage)?;
        let v_b64 = s.strip_prefix("v=").ok_or(AuthError::MalformedMessage)?;
        let v = B64.decode(v_b64).map_err(|_| AuthError::MalformedMessage)?;
        let mut mac = <Hmac<Sha512>>::new_from_slice(&server_key)
            .map_err(|_| AuthError::MalformedMessage)?;
        mac.update(auth_message.as_bytes());
        let expected = mac.finalize().into_bytes();
        if expected.ct_eq(&v).unwrap_u8() != 1 {
            return Err(AuthError::BadProof);
        }
        Ok(())
    }
}
```

- [ ] **Step 4: Run tests**

Run: `cargo test -p crabka-security scram`
Expected: PASS (4 tests total).

- [ ] **Step 5: Commit**

```bash
git add crates/security/src/scram/
git commit -m "feat(security): SCRAM-SHA-512 server + client state machines"
```

---

### Task 4: `verify_plain` (constant-time) + `TlsConfig` builders

**Files:**
- Modify: `crates/security/src/plain.rs`
- Modify: `crates/security/src/tls.rs`

- [ ] **Step 1: Write the failing tests**

Replace `crates/security/src/plain.rs`:

```rust
use std::collections::HashMap;
use subtle::ConstantTimeEq;

use crate::{AuthError, Principal, SaslMechanism};

/// Verifies a SASL/PLAIN auth attempt against a static credential map.
///
/// On a known user, password comparison is constant-time. On an unknown
/// user, returns `UnknownUser` (but the wire response collapses both to
/// `SASL_AUTHENTICATION_FAILED` upstream).
pub fn verify_plain(
    creds: &HashMap<String, String>,
    user: &str,
    password: &[u8],
) -> Result<Principal, AuthError> {
    let Some(expected) = creds.get(user) else {
        return Err(AuthError::UnknownUser);
    };
    if expected.as_bytes().ct_eq(password).unwrap_u8() == 1 {
        Ok(Principal {
            name: user.to_string(),
            mechanism: SaslMechanism::Plain,
        })
    } else {
        Err(AuthError::BadPassword)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn creds() -> HashMap<String, String> {
        let mut m = HashMap::new();
        m.insert("alice".into(), "wonderland".into());
        m
    }

    #[test]
    fn correct_creds_pass() {
        let p = verify_plain(&creds(), "alice", b"wonderland").unwrap();
        assert_eq!(p.name, "alice");
        assert_eq!(p.mechanism, SaslMechanism::Plain);
    }

    #[test]
    fn wrong_password_fails() {
        assert_eq!(
            verify_plain(&creds(), "alice", b"hunter2"),
            Err(AuthError::BadPassword),
        );
    }

    #[test]
    fn unknown_user_fails() {
        assert_eq!(
            verify_plain(&creds(), "bob", b"anything"),
            Err(AuthError::UnknownUser),
        );
    }
}
```

- [ ] **Step 2: Replace `crates/security/src/tls.rs`**

```rust
use std::fs::File;
use std::io::BufReader;
use std::path::PathBuf;
use std::sync::Arc;

use rustls::pki_types::{CertificateDer, PrivateKeyDer};
use thiserror::Error;

#[derive(Debug, Clone)]
pub struct TlsConfig {
    pub cert_chain_path: PathBuf,
    pub private_key_path: PathBuf,
    pub trust_roots_path: Option<PathBuf>,
}

#[derive(Debug, Error)]
pub enum TlsError {
    #[error("io: {0}")]
    Io(#[from] std::io::Error),
    #[error("rustls: {0}")]
    Rustls(#[from] rustls::Error),
    #[error("no private key in {0}")]
    NoPrivateKey(PathBuf),
    #[error("no certificates in {0}")]
    NoCerts(PathBuf),
}

impl TlsConfig {
    pub fn build_server_config(&self) -> Result<Arc<rustls::ServerConfig>, TlsError> {
        let certs = load_certs(&self.cert_chain_path)?;
        let key = load_private_key(&self.private_key_path)?;
        let cfg = rustls::ServerConfig::builder()
            .with_no_client_auth()
            .with_single_cert(certs, key)?;
        Ok(Arc::new(cfg))
    }

    pub fn build_client_config(&self) -> Result<Arc<rustls::ClientConfig>, TlsError> {
        let mut roots = rustls::RootCertStore::empty();
        if let Some(path) = &self.trust_roots_path {
            for cert in load_certs(path)? {
                roots.add(cert)?;
            }
        } else {
            // Native roots — for production. Tests typically supply trust_roots_path.
            // We don't pull in rustls-native-certs to keep deps tight; missing
            // trust_roots_path on a TLS client config is treated as a config
            // error upstream.
        }
        let cfg = rustls::ClientConfig::builder()
            .with_root_certificates(roots)
            .with_no_client_auth();
        Ok(Arc::new(cfg))
    }
}

fn load_certs(path: &PathBuf) -> Result<Vec<CertificateDer<'static>>, TlsError> {
    let f = File::open(path)?;
    let mut r = BufReader::new(f);
    let certs: Result<Vec<_>, _> = rustls_pemfile::certs(&mut r).collect();
    let certs = certs?;
    if certs.is_empty() {
        return Err(TlsError::NoCerts(path.clone()));
    }
    Ok(certs)
}

fn load_private_key(path: &PathBuf) -> Result<PrivateKeyDer<'static>, TlsError> {
    let f = File::open(path)?;
    let mut r = BufReader::new(f);
    rustls_pemfile::private_key(&mut r)?
        .ok_or_else(|| TlsError::NoPrivateKey(path.clone()))
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write;

    fn write_self_signed(dir: &std::path::Path) -> (PathBuf, PathBuf) {
        // Reuse a deterministic dev cert; for the unit test we just need
        // valid PEM. We embed pre-generated PEMs as constants.
        // (Generated with: openssl req -x509 -newkey ed25519 -nodes -days 365 \
        //   -subj "/CN=test" -keyout key.pem -out cert.pem)
        let cert_pem = include_str!("../tests/fixtures/dev_cert.pem");
        let key_pem = include_str!("../tests/fixtures/dev_key.pem");
        let cert_path = dir.join("cert.pem");
        let key_path = dir.join("key.pem");
        File::create(&cert_path).unwrap().write_all(cert_pem.as_bytes()).unwrap();
        File::create(&key_path).unwrap().write_all(key_pem.as_bytes()).unwrap();
        (cert_path, key_path)
    }

    #[test]
    fn valid_cert_and_key_loads() {
        let dir = tempfile::tempdir().unwrap();
        let (cert_path, key_path) = write_self_signed(dir.path());
        let cfg = TlsConfig {
            cert_chain_path: cert_path,
            private_key_path: key_path,
            trust_roots_path: None,
        };
        cfg.build_server_config().expect("build server cfg");
    }

    #[test]
    fn missing_cert_errors() {
        let cfg = TlsConfig {
            cert_chain_path: PathBuf::from("/nonexistent/cert.pem"),
            private_key_path: PathBuf::from("/nonexistent/key.pem"),
            trust_roots_path: None,
        };
        assert!(cfg.build_server_config().is_err());
    }
}
```

- [ ] **Step 3: Generate dev cert + key fixtures**

```bash
mkdir -p crates/security/tests/fixtures
openssl req -x509 -newkey ed25519 -nodes -days 36500 \
    -subj "/CN=crabka-dev" \
    -keyout crates/security/tests/fixtures/dev_key.pem \
    -out crates/security/tests/fixtures/dev_cert.pem
```

(If `openssl` is not available on Windows, run this in WSL or use `rustls` programmatic generation; document in a comment. The fixtures are dev-only, embedded for unit-test reproducibility.)

- [ ] **Step 4: Add `tempfile` to dev-dependencies**

In `crates/security/Cargo.toml`, append:

```toml
[dev-dependencies]
hex.workspace = true
tempfile.workspace = true
```

- [ ] **Step 5: Run tests**

Run: `cargo test -p crabka-security`
Expected: PASS (all tests — 4 SCRAM, 3 PLAIN, 2 TLS).

- [ ] **Step 6: Commit**

```bash
git add crates/security/
git commit -m "feat(security): PLAIN verifier + rustls TLS config builders"
```

---

## Batch 2 — Metadata records

### Task 5: `V1ScramCredential` + `V1DeleteScramCredential` records

**Files:**
- Modify: `crates/metadata/src/records.rs`
- Modify: `crates/metadata/Cargo.toml` (add `crabka-security`)
- Modify: workspace `Cargo.toml` (add internal crate to workspace deps)

- [ ] **Step 1: Wire `crabka-security` as a workspace dep**

In workspace `Cargo.toml` `[workspace.dependencies]`:

```toml
crabka-security = { path = "crates/security" }
```

- [ ] **Step 2: Add to `crates/metadata/Cargo.toml`**

```toml
[dependencies]
# ... existing ...
crabka-security.workspace = true
```

- [ ] **Step 3: Write the failing round-trip test**

Append to `crates/metadata/src/records.rs` `mod tests`:

```rust
    #[test]
    fn scram_credential_round_trip() {
        let r = MetadataRecord::V1ScramCredential(ScramCredentialRecord {
            user: "alice".into(),
            mechanism: crabka_security::SaslMechanism::ScramSha512,
            salt: vec![1u8; 16],
            stored_key: vec![2u8; 64],
            server_key: vec![3u8; 64],
            iterations: 4096,
        });
        assert_eq!(round_trip(&r), r);
    }

    #[test]
    fn delete_scram_credential_round_trip() {
        let r = MetadataRecord::V1DeleteScramCredential(DeleteScramCredentialRecord {
            user: "alice".into(),
            mechanism: crabka_security::SaslMechanism::ScramSha512,
        });
        assert_eq!(round_trip(&r), r);
    }
```

- [ ] **Step 4: Add the record types**

In `crates/metadata/src/records.rs`, add after `DeleteTopicRecord`:

```rust
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ScramCredentialRecord {
    pub user: String,
    pub mechanism: crabka_security::SaslMechanism,
    pub salt: Vec<u8>,
    pub stored_key: Vec<u8>,
    pub server_key: Vec<u8>,
    pub iterations: u32,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DeleteScramCredentialRecord {
    pub user: String,
    pub mechanism: crabka_security::SaslMechanism,
}
```

Append two variants to `MetadataRecord`:

```rust
pub enum MetadataRecord {
    V1Topic(TopicRecord),
    V1Partition(PartitionRecord),
    V1BrokerRegistration(BrokerRegistrationRecord),
    V1DeleteTopic(DeleteTopicRecord),
    V1TopicConfig(TopicConfigRecord),       // existing from slice 11
    V1ScramCredential(ScramCredentialRecord),
    V1DeleteScramCredential(DeleteScramCredentialRecord),
}
```

- [ ] **Step 5: Run the round-trip tests**

Run: `cargo test -p crabka-metadata records::tests`
Expected: PASS (2 new tests).

- [ ] **Step 6: Commit**

```bash
git add Cargo.toml crates/metadata/Cargo.toml crates/metadata/src/records.rs
git commit -m "feat(metadata): V1ScramCredential + V1DeleteScramCredential records"
```

---

### Task 6: `MetadataImage` SCRAM credential storage

**Files:**
- Modify: `crates/metadata/src/image.rs`

- [ ] **Step 1: Write failing tests**

Append to `crates/metadata/src/image.rs` `mod tests`:

```rust
    #[test]
    fn apply_scram_credential_stores() {
        let mut m = img();
        m.apply(&MetadataRecord::V1ScramCredential(ScramCredentialRecord {
            user: "alice".into(),
            mechanism: crabka_security::SaslMechanism::ScramSha512,
            salt: vec![1; 16],
            stored_key: vec![2; 64],
            server_key: vec![3; 64],
            iterations: 4096,
        }));
        let got = m.scram_credential("alice", crabka_security::SaslMechanism::ScramSha512);
        assert!(got.is_some());
        assert_eq!(got.unwrap().iterations, 4096);
    }

    #[test]
    fn apply_scram_credential_last_write_wins() {
        let mut m = img();
        let mech = crabka_security::SaslMechanism::ScramSha512;
        m.apply(&MetadataRecord::V1ScramCredential(ScramCredentialRecord {
            user: "alice".into(), mechanism: mech,
            salt: vec![1; 16], stored_key: vec![2; 64], server_key: vec![3; 64],
            iterations: 4096,
        }));
        m.apply(&MetadataRecord::V1ScramCredential(ScramCredentialRecord {
            user: "alice".into(), mechanism: mech,
            salt: vec![9; 16], stored_key: vec![9; 64], server_key: vec![9; 64],
            iterations: 8192,
        }));
        let got = m.scram_credential("alice", mech).unwrap();
        assert_eq!(got.iterations, 8192);
        assert_eq!(got.salt, vec![9; 16]);
    }

    #[test]
    fn delete_scram_credential_removes() {
        let mut m = img();
        let mech = crabka_security::SaslMechanism::ScramSha512;
        m.apply(&MetadataRecord::V1ScramCredential(ScramCredentialRecord {
            user: "alice".into(), mechanism: mech,
            salt: vec![1; 16], stored_key: vec![2; 64], server_key: vec![3; 64],
            iterations: 4096,
        }));
        m.apply(&MetadataRecord::V1DeleteScramCredential(DeleteScramCredentialRecord {
            user: "alice".into(), mechanism: mech,
        }));
        assert!(m.scram_credential("alice", mech).is_none());
    }
```

- [ ] **Step 2: Extend `MetadataImage`**

In `crates/metadata/src/image.rs`:

```rust
use crabka_security::{SaslMechanism, ScramCredential};
use crate::records::{
    DeleteScramCredentialRecord, ScramCredentialRecord, // ... existing imports
};
```

Add to the `MetadataImage` struct (after `topic_configs`):

```rust
    scram_credentials: HashMap<(String, SaslMechanism), ScramCredential>,
```

Update `MetadataImage::new` to initialize the field with `HashMap::new()`.

Add accessor:

```rust
    #[must_use]
    pub fn scram_credential(
        &self,
        user: &str,
        mechanism: SaslMechanism,
    ) -> Option<&ScramCredential> {
        self.scram_credentials.get(&(user.to_string(), mechanism))
    }
```

Extend `apply`:

```rust
            MetadataRecord::V1ScramCredential(r) => {
                self.scram_credentials.insert(
                    (r.user.clone(), r.mechanism),
                    ScramCredential {
                        mechanism: r.mechanism,
                        salt: r.salt.clone(),
                        stored_key: r.stored_key.clone(),
                        server_key: r.server_key.clone(),
                        iterations: r.iterations,
                    },
                );
            }
            MetadataRecord::V1DeleteScramCredential(r) => {
                self.scram_credentials.remove(&(r.user.clone(), r.mechanism));
            }
```

Extend `validate` (these records have no preconditions):

```rust
            MetadataRecord::V1ScramCredential(_) | MetadataRecord::V1DeleteScramCredential(_) => {
                Ok(())
            }
```

- [ ] **Step 3: Run tests**

Run: `cargo test -p crabka-metadata image::tests`
Expected: PASS.

- [ ] **Step 4: Verify workspace still builds**

Run: `cargo build --workspace`
Expected: clean.

- [ ] **Step 5: Commit**

```bash
git add crates/metadata/src/image.rs
git commit -m "feat(metadata): MetadataImage SCRAM credential store + accessors"
```

---

## Batch 3 — `BrokerConfig` extension

### Task 7: Listener types + `BrokerConfig` new fields

**Files:**
- Modify: `crates/broker/src/config.rs` (or wherever `BrokerConfig` lives — verify before editing)
- Modify: `crates/broker/Cargo.toml` (add `crabka-security` workspace dep)

- [ ] **Step 1: Locate `BrokerConfig`**

Run: `rg "pub struct BrokerConfig" crates/broker/src/`
The file is likely `crates/broker/src/config.rs` or `lib.rs`. Edit the file that contains the struct.

- [ ] **Step 2: Add `crabka-security` dep to broker Cargo.toml**

```toml
crabka-security.workspace = true
tokio-rustls.workspace = true
```

- [ ] **Step 3: Define new types and extend `BrokerConfig`**

At the top of the config file:

```rust
use std::collections::HashMap;
use std::net::SocketAddr;
use crabka_security::{ListenerProtocol, SaslMechanism, TlsConfig};

#[derive(Debug, Clone)]
pub struct ListenerSpec {
    pub name: String,
    pub bind_addr: SocketAddr,
    pub advertised: String,
    pub protocol: ListenerProtocol,
}

#[derive(Debug, Clone)]
pub struct InterBrokerCredentials {
    pub mechanism: SaslMechanism,
    pub username: String,
    pub password: String,
}
```

Append to `BrokerConfig`:

```rust
pub struct BrokerConfig {
    // ... existing fields stay as-is ...
    pub listeners: Vec<ListenerSpec>,
    pub inter_broker_listener_name: String,
    pub inter_broker_credentials: Option<InterBrokerCredentials>,
    pub plain_credentials: HashMap<String, String>,
    pub super_user_name: Option<String>,
    pub tls_config: Option<TlsConfig>,
    pub enabled_sasl_mechanisms: Vec<SaslMechanism>,
}
```

- [ ] **Step 4: Provide a `Default` that preserves current behavior**

Where `BrokerConfig::default()` (or the existing test constructor) lives, ensure the new fields default to:

```rust
listeners: vec![],   // empty -> synthesized from listen_addr below
inter_broker_listener_name: "PLAINTEXT".to_string(),
inter_broker_credentials: None,
plain_credentials: HashMap::new(),
super_user_name: None,
tls_config: None,
enabled_sasl_mechanisms: vec![],
```

- [ ] **Step 5: Synthesize default listener from legacy fields**

Add a method on `BrokerConfig`:

```rust
impl BrokerConfig {
    /// If `listeners` is empty, synthesize a single PLAINTEXT listener
    /// matching the legacy `listen_addr` + `advertised_listener` fields.
    /// Preserves backward compatibility with all existing tests.
    #[must_use]
    pub fn effective_listeners(&self) -> Vec<ListenerSpec> {
        if !self.listeners.is_empty() {
            return self.listeners.clone();
        }
        vec![ListenerSpec {
            name: "PLAINTEXT".to_string(),
            bind_addr: self.listen_addr,
            advertised: self.advertised_listener.clone(),
            protocol: ListenerProtocol::Plaintext,
        }]
    }
}
```

- [ ] **Step 6: Verify workspace still builds**

Run: `cargo build --workspace`
Expected: clean.

- [ ] **Step 7: Commit**

```bash
git add crates/broker/Cargo.toml crates/broker/src/config.rs
git commit -m "feat(broker): listener registry + auth fields on BrokerConfig (no behavior change)"
```

---

### Task 8: `BrokerConfig` validation at startup

**Files:**
- Modify: same config file
- Modify: `crates/broker/src/error.rs` (likely)

- [ ] **Step 1: Write failing tests**

In the config file's `#[cfg(test)] mod tests`:

```rust
    fn base() -> BrokerConfig {
        let mut c = BrokerConfig::default();
        c.listeners = vec![
            ListenerSpec {
                name: "INTERNAL".to_string(),
                bind_addr: "127.0.0.1:9093".parse().unwrap(),
                advertised: "127.0.0.1:9093".to_string(),
                protocol: ListenerProtocol::Plaintext,
            },
            ListenerSpec {
                name: "EXTERNAL".to_string(),
                bind_addr: "0.0.0.0:9092".parse().unwrap(),
                advertised: "host.docker.internal:9092".to_string(),
                protocol: ListenerProtocol::SaslSsl,
            },
        ];
        c.inter_broker_listener_name = "INTERNAL".to_string();
        c.enabled_sasl_mechanisms = vec![SaslMechanism::Plain, SaslMechanism::ScramSha512];
        c
    }

    #[test]
    fn rejects_bind_collision() {
        let mut c = base();
        c.listeners[1].bind_addr = c.listeners[0].bind_addr;
        assert!(matches!(c.validate(), Err(BrokerStartError::ListenerConflict { .. })));
    }

    #[test]
    fn rejects_missing_inter_broker_listener() {
        let mut c = base();
        c.inter_broker_listener_name = "NONESUCH".to_string();
        assert!(matches!(c.validate(), Err(BrokerStartError::InvalidInterBrokerListener { .. })));
    }

    #[test]
    fn rejects_sasl_listener_without_mechanisms() {
        let mut c = base();
        c.enabled_sasl_mechanisms.clear();
        assert!(c.validate().is_err());
    }

    #[test]
    fn legacy_default_passes() {
        let c = BrokerConfig::default();
        c.validate().expect("legacy default must validate");
    }
```

- [ ] **Step 2: Extend `BrokerError` / `BrokerStartError`**

In `crates/broker/src/error.rs` (locate via `rg "pub enum BrokerError"`), add variants:

```rust
    #[error("listener bind conflict: {a} and {b} share bind_addr")]
    ListenerConflict { a: String, b: String },
    #[error("inter_broker_listener_name {name} not in listeners list")]
    InvalidInterBrokerListener { name: String },
    #[error("SASL listener {name} declared but enabled_sasl_mechanisms is empty")]
    SaslListenerNoMechanisms { name: String },
    #[error("tls: {0}")]
    Tls(String),
```

(If there's a separate `BrokerStartError` alias for startup-only failures, add there. Otherwise `BrokerError` is the single error type.)

- [ ] **Step 3: Implement `BrokerConfig::validate`**

```rust
impl BrokerConfig {
    pub fn validate(&self) -> Result<(), BrokerError> {
        let listeners = self.effective_listeners();
        // bind collisions
        for i in 0..listeners.len() {
            for j in (i + 1)..listeners.len() {
                if listeners[i].bind_addr == listeners[j].bind_addr {
                    return Err(BrokerError::ListenerConflict {
                        a: listeners[i].name.clone(),
                        b: listeners[j].name.clone(),
                    });
                }
            }
        }
        // inter-broker listener must exist
        if !listeners.iter().any(|l| l.name == self.inter_broker_listener_name) {
            return Err(BrokerError::InvalidInterBrokerListener {
                name: self.inter_broker_listener_name.clone(),
            });
        }
        // every SASL listener requires at least one mechanism
        for l in &listeners {
            if l.protocol.requires_sasl() && self.enabled_sasl_mechanisms.is_empty() {
                return Err(BrokerError::SaslListenerNoMechanisms { name: l.name.clone() });
            }
        }
        Ok(())
    }
}
```

- [ ] **Step 4: Call `validate()` from `Broker::start`**

In `crates/broker/src/broker.rs`, near the top of `Broker::start`:

```rust
        config.validate()?;
```

(Adjust based on actual signature; `?` requires `BrokerError` to be the error type.)

- [ ] **Step 5: Run tests**

Run: `cargo test -p crabka-broker config`
Expected: PASS (4 new tests).

- [ ] **Step 6: Run full workspace tests**

Run: `cargo test --workspace --exclude crabka-client-core --exclude crabka-log --exclude crabka-broker`
Expected: PASS. Existing broker tests should still pass because they use `BrokerConfig::default()`.

Run: `cargo test -p crabka-broker --lib`
Expected: PASS.

- [ ] **Step 7: Commit**

```bash
git add crates/broker/src/config.rs crates/broker/src/error.rs crates/broker/src/broker.rs
git commit -m "feat(broker): validate listener + auth config at Broker::start"
```

---

## Batch 4 — Multi-listener accept + TLS

### Task 9: Per-listener accept loops

**Files:**
- Modify: `crates/broker/src/broker.rs`

Today there's one `accept_loop` spawned against a single `TcpListener` bound to `config.listen_addr`. Replace with one loop per `ListenerSpec`. The plaintext path stays identical; SSL/SASL paths are stubbed and added in tasks 10 and 12-14.

- [ ] **Step 1: Refactor accept loop signature to carry listener context**

In `crates/broker/src/broker.rs`, change `accept_loop`:

```rust
async fn accept_loop(
    broker: Arc<Broker>,
    listener: TcpListener,
    listener_spec: crate::config::ListenerSpec,
    shutdown: CancellationToken,
) {
    loop {
        tokio::select! {
            _ = shutdown.cancelled() => break,
            accept = listener.accept() => {
                match accept {
                    Ok((stream, peer)) => {
                        tracing::debug!(%peer, name = %listener_spec.name, "accepted connection");
                        let b = broker.clone();
                        let spec = listener_spec.clone();
                        tokio::spawn(async move {
                            crate::network::dispatch::serve_connection_on_listener(b, stream, spec).await;
                        });
                    }
                    Err(e) => {
                        tracing::warn!(error = %e, name = %listener_spec.name, "accept failed");
                    }
                }
            }
        }
    }
}
```

The previous `serve_connection(broker, stream)` becomes `serve_connection_on_listener(broker, stream, spec)`. The new function (added below) is a thin wrapper that records the listener protocol on the per-connection state and calls the existing serve path.

- [ ] **Step 2: Add the wrapper in `crates/broker/src/network/dispatch.rs`**

```rust
/// Per-listener entrypoint. Records the listener protocol on the
/// per-connection state and delegates to the regular dispatch loop.
/// TLS termination happens here for SSL / SASL_SSL listeners (task 10).
pub async fn serve_connection_on_listener(
    broker: std::sync::Arc<crate::broker::Broker>,
    stream: tokio::net::TcpStream,
    spec: crate::config::ListenerSpec,
) {
    // Task 10 wraps `stream` in tokio_rustls when spec.protocol.requires_tls().
    // For now: plaintext path identical to old `serve_connection`.
    serve_connection_plaintext(broker, stream, spec).await;
}

async fn serve_connection_plaintext(
    broker: std::sync::Arc<crate::broker::Broker>,
    stream: tokio::net::TcpStream,
    _spec: crate::config::ListenerSpec,
) {
    // Move the body of the existing `serve_connection` here.
    // ...
}
```

Rename the existing `serve_connection` body to `serve_connection_plaintext` (keep behavior identical). The old public `serve_connection` name can stay as a passthrough if any other code calls it; otherwise remove it.

- [ ] **Step 3: Spawn one accept loop per listener in `Broker::start`**

Replace the single `TcpListener::bind` + `tokio::spawn(accept_loop(...))` block in `Broker::start` with:

```rust
        let listeners_spec = config.effective_listeners();
        let mut listener_tasks = Vec::with_capacity(listeners_spec.len());
        let mut bound_addrs = Vec::with_capacity(listeners_spec.len());
        for spec in &listeners_spec {
            let listener = TcpListener::bind(spec.bind_addr).await?;
            let actual = listener.local_addr()?;
            bound_addrs.push((spec.name.clone(), actual));
            let task = tokio::spawn(accept_loop(
                broker.clone(),
                listener,
                spec.clone(),
                shutdown.clone(),
            ));
            listener_tasks.push(task);
        }
        // Pick "primary" listen_addr for BrokerHandle: first PLAINTEXT or the inter-broker one.
        let listen_addr = bound_addrs
            .iter()
            .find(|(name, _)| *name == config.inter_broker_listener_name)
            .map(|(_, a)| *a)
            .unwrap_or(bound_addrs[0].1);
```

- [ ] **Step 4: Update `BrokerHandle` to hold all listener tasks**

```rust
pub struct BrokerHandle {
    listen_addr: SocketAddr,
    shutdown: CancellationToken,
    listener_tasks: Vec<JoinHandle<()>>,
    _broker: Arc<Broker>,
}
```

Update `BrokerHandle::shutdown` to await all tasks.

- [ ] **Step 5: Verify existing tests still pass**

Run: `cargo test -p crabka-broker --lib`
Expected: PASS — legacy single-listener path is preserved via `effective_listeners`.

Run: `cargo test -p crabka-broker --test smoke` (or whichever integration test names exist; list via `ls crates/broker/tests/`)
Expected: PASS.

- [ ] **Step 6: Commit**

```bash
git add crates/broker/src/broker.rs crates/broker/src/network/dispatch.rs
git commit -m "refactor(broker): per-listener accept loops (plaintext path unchanged)"
```

---

### Task 10: TLS termination in accept loop

**Files:**
- Modify: `crates/broker/src/network/dispatch.rs`
- Modify: `crates/broker/src/broker.rs`

- [ ] **Step 1: Make `serve_connection_plaintext` generic over `AsyncRead+AsyncWrite`**

Change the signature:

```rust
async fn serve_connection_stream<S>(
    broker: std::sync::Arc<crate::broker::Broker>,
    stream: S,
    _spec: crate::config::ListenerSpec,
)
where
    S: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin + Send + 'static,
{
    // existing serve_connection body, no changes other than generic typing
}
```

Rename and adjust callers. `serve_connection_plaintext` becomes:

```rust
async fn serve_connection_plaintext(
    broker: std::sync::Arc<crate::broker::Broker>,
    stream: tokio::net::TcpStream,
    spec: crate::config::ListenerSpec,
) {
    serve_connection_stream(broker, stream, spec).await;
}
```

- [ ] **Step 2: Add TLS wrapper**

In `serve_connection_on_listener`:

```rust
pub async fn serve_connection_on_listener(
    broker: std::sync::Arc<crate::broker::Broker>,
    stream: tokio::net::TcpStream,
    spec: crate::config::ListenerSpec,
) {
    use crabka_security::ListenerProtocol;
    if spec.protocol.requires_tls() {
        let Some(acceptor) = broker.tls_acceptor.clone() else {
            tracing::error!(
                listener = %spec.name,
                "TLS listener configured but broker has no TlsAcceptor"
            );
            return;
        };
        match acceptor.accept(stream).await {
            Ok(tls_stream) => serve_connection_stream(broker, tls_stream, spec).await,
            Err(e) => tracing::debug!(error = %e, "TLS handshake failed"),
        }
    } else {
        serve_connection_plaintext(broker, stream, spec).await;
    }
}
```

- [ ] **Step 3: Store `TlsAcceptor` on `Broker`**

In `crates/broker/src/broker.rs`, add to `Broker` struct:

```rust
pub(crate) tls_acceptor: Option<tokio_rustls::TlsAcceptor>,
```

In `Broker::start`, build the acceptor when `tls_config.is_some()`:

```rust
        let tls_acceptor = match &config.tls_config {
            Some(tls) => {
                let server_cfg = tls.build_server_config()
                    .map_err(|e| BrokerError::Tls(e.to_string()))?;
                Some(tokio_rustls::TlsAcceptor::from(server_cfg))
            }
            None => None,
        };
```

Set on the `Broker` instance during construction.

- [ ] **Step 4: Write a smoke test for TLS termination**

In `crates/broker/tests/auth_handlers.rs` (create the file):

```rust
//! Slice 12 broker-side auth tests. No Docker.

use crabka_broker::{Broker, BrokerConfig};
use crabka_security::{ListenerProtocol, SaslMechanism, TlsConfig};
use std::sync::Arc;
use tokio_rustls::TlsConnector;
use tokio_rustls::rustls::{ClientConfig, RootCertStore};
use tokio_rustls::rustls::pki_types::{CertificateDer, ServerName};

const DEV_CERT: &str = include_str!("../../../crates/security/tests/fixtures/dev_cert.pem");
const DEV_KEY: &str = include_str!("../../../crates/security/tests/fixtures/dev_key.pem");

fn write_dev_pem(dir: &std::path::Path) -> (std::path::PathBuf, std::path::PathBuf) {
    let cp = dir.join("cert.pem");
    let kp = dir.join("key.pem");
    std::fs::write(&cp, DEV_CERT).unwrap();
    std::fs::write(&kp, DEV_KEY).unwrap();
    (cp, kp)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn tls_listener_accepts_tls_handshake_only() {
    let dir = tempfile::tempdir().unwrap();
    let (cert_path, key_path) = write_dev_pem(dir.path());
    let mut cfg = BrokerConfig::default();
    cfg.listeners = vec![crabka_broker::config::ListenerSpec {
        name: "SSL".to_string(),
        bind_addr: "127.0.0.1:0".parse().unwrap(),
        advertised: "127.0.0.1:0".to_string(),
        protocol: ListenerProtocol::Ssl,
    }];
    cfg.inter_broker_listener_name = "SSL".to_string();
    cfg.tls_config = Some(TlsConfig {
        cert_chain_path: cert_path.clone(),
        private_key_path: key_path,
        trust_roots_path: None,
    });
    let handle = Broker::start(cfg).await.expect("start");
    let addr = handle.listen_addr();

    // Build a client config trusting our dev cert
    let mut roots = RootCertStore::empty();
    let cert_pem = std::fs::read(&cert_path).unwrap();
    let cert: Vec<CertificateDer> =
        rustls_pemfile::certs(&mut std::io::Cursor::new(cert_pem))
            .collect::<Result<_, _>>().unwrap();
    for c in cert { roots.add(c).unwrap(); }
    let client_cfg = ClientConfig::builder()
        .with_root_certificates(roots)
        .with_no_client_auth();
    let connector = TlsConnector::from(Arc::new(client_cfg));
    let tcp = tokio::net::TcpStream::connect(addr).await.unwrap();
    let server_name = ServerName::try_from("crabka-dev").unwrap();
    let _tls = connector.connect(server_name, tcp).await
        .expect("TLS handshake must succeed");
    handle.shutdown().await;
}
```

Add `rustls-pemfile`, `tokio-rustls`, `rustls`, `tempfile` to broker `[dev-dependencies]`:

```toml
[dev-dependencies]
# ... existing ...
rustls.workspace = true
rustls-pemfile.workspace = true
tokio-rustls.workspace = true
tempfile.workspace = true
```

- [ ] **Step 5: Run the test**

Run: `cargo test -p crabka-broker --test auth_handlers tls_listener_accepts`
Expected: PASS.

- [ ] **Step 6: Commit**

```bash
git add crates/broker/Cargo.toml crates/broker/src/ crates/broker/tests/auth_handlers.rs
git commit -m "feat(broker): TLS termination per-listener via tokio-rustls"
```

---

### Task 11: `BrokerEndpoint` in metadata + per-listener Metadata response

**Files:**
- Modify: `crates/metadata/src/records.rs`
- Modify: `crates/metadata/src/image.rs`
- Modify: `crates/broker/src/handlers/metadata.rs` (locate via `rg "fn handle_metadata"`)

- [ ] **Step 1: Add `BrokerEndpoint` to `BrokerRegistrationRecord`**

In `crates/metadata/src/records.rs`:

```rust
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct BrokerEndpoint {
    pub name: String,                                       // listener name
    pub host: String,
    pub port: u16,
    pub protocol: crabka_security::ListenerProtocol,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct BrokerRegistrationRecord {
    pub node_id: NodeId,
    pub host: String,                                       // legacy / inter-broker default
    pub port: u16,
    pub rack: Option<String>,
    pub endpoints: Vec<BrokerEndpoint>,                     // NEW
}
```

Write a round-trip test in the same file's `mod tests`:

```rust
    #[test]
    fn broker_registration_with_endpoints_round_trip() {
        let r = MetadataRecord::V1BrokerRegistration(BrokerRegistrationRecord {
            node_id: 1,
            host: "h".into(),
            port: 9092,
            rack: None,
            endpoints: vec![BrokerEndpoint {
                name: "EXTERNAL".into(),
                host: "ext.example.com".into(),
                port: 9092,
                protocol: crabka_security::ListenerProtocol::SaslSsl,
            }],
        });
        assert_eq!(round_trip(&r), r);
    }
```

Run: `cargo test -p crabka-metadata broker_registration_with_endpoints_round_trip`
Expected: PASS.

- [ ] **Step 2: Update every construction site of `BrokerRegistrationRecord`**

Run: `rg "BrokerRegistrationRecord \{" crates/`. At each site, add `endpoints: vec![]` (legacy default) or — in `Broker::start` where the broker registers itself — populate from `effective_listeners()`:

```rust
let endpoints: Vec<BrokerEndpoint> = config
    .effective_listeners()
    .iter()
    .map(|l| {
        let (host, port) = parse_host_port(&l.advertised);
        BrokerEndpoint {
            name: l.name.clone(),
            host,
            port,
            protocol: l.protocol,
        }
    })
    .collect();
```

`parse_host_port` helper splits on last `:`. If `crates/broker/src/network/` already has something similar, reuse it.

- [ ] **Step 3: Project endpoints into Metadata response**

In the Metadata response handler, where each broker is rendered, populate the v9+ `endpoints` field. The exact wire field name depends on `crabka_protocol::owned::metadata_response_v9::MetadataResponseBroker`; verify via `rg "endpoints" crates/protocol/generated/` and adjust.

If the codec already supports `endpoints` (it should — that's a v9+ field), the handler change is:

```rust
let endpoints = broker_record.endpoints.iter().map(|e| /* build BrokerEndpoint wire struct */).collect();
```

If `endpoints` is not currently populated for older API versions, fall back to a single-element list derived from `host`+`port`.

- [ ] **Step 4: Add a unit test for Metadata response endpoints**

In broker integration tests (e.g. `crates/broker/tests/auth_handlers.rs`):

```rust
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn metadata_response_carries_listener_endpoints() {
    // Spin up a 2-listener broker (PLAINTEXT + SSL), issue Metadata,
    // assert both endpoints appear.
    // (Full test body — write the actual TCP+codec call here.)
    todo!("see task body — write a real test, not a todo");
}
```

NOTE: the `todo!` must be replaced with a real test before commit. Pattern: use `crabka_client_core::Connection` to dial the broker, send `MetadataRequest::v12`, decode, assert `response.brokers[0].endpoints.len() == 2`.

- [ ] **Step 5: Run all metadata + broker tests**

```bash
cargo test -p crabka-metadata
cargo test -p crabka-broker --lib
cargo test -p crabka-broker --test auth_handlers
```

Expected: PASS.

- [ ] **Step 6: Commit**

```bash
git add crates/metadata/ crates/broker/
git commit -m "feat(metadata): per-listener endpoints on BrokerRegistration"
```

---

## Batch 5 — SASL handshake

### Task 12: `ConnectionAuth` state + pre-auth gate

**Files:**
- Modify: `crates/broker/src/network/dispatch.rs`
- Create: `crates/broker/src/network/auth.rs`

- [ ] **Step 1: Create `crates/broker/src/network/auth.rs`**

```rust
//! Per-connection SASL authentication state machine.
//!
//! Slice 12. Drives SaslHandshake (17) and SaslAuthenticate (36).

use crabka_security::{Principal, SaslMechanism, ScramServerExchange};

#[derive(Debug)]
pub enum ConnectionAuth {
    /// PLAINTEXT / SSL listener, or pre-handshake on a SASL listener.
    Anonymous,
    /// SaslHandshake received; awaiting (possibly multiple) SaslAuthenticate.
    Negotiating {
        mechanism: SaslMechanism,
        exchange: SaslExchange,
    },
    Authenticated { principal: Principal },
}

#[derive(Debug)]
pub enum SaslExchange {
    Plain,                                  // single-RTT
    Scram(ScramServerExchange),
}

impl ConnectionAuth {
    #[must_use]
    pub fn is_authenticated(&self) -> bool {
        matches!(self, Self::Authenticated { .. })
    }

    #[must_use]
    pub fn principal(&self) -> Option<&Principal> {
        if let Self::Authenticated { principal } = self {
            Some(principal)
        } else {
            None
        }
    }
}

/// Pre-auth allowlist: api_keys clients may send before completing SASL.
#[must_use]
pub fn is_pre_auth_allowed(api_key: i16) -> bool {
    matches!(api_key, 17 | 36 | 18) // SaslHandshake, SaslAuthenticate, ApiVersions
}
```

Add `mod auth;` to `crates/broker/src/network/mod.rs`.

- [ ] **Step 2: Plumb `ConnectionAuth` into `serve_connection_stream`**

In `crates/broker/src/network/dispatch.rs`, inside `serve_connection_stream`, before the request loop:

```rust
    let mut auth = if spec.protocol.requires_sasl() {
        crate::network::auth::ConnectionAuth::Anonymous
    } else {
        // PLAINTEXT / SSL: implicit anonymous, treated as authenticated for gating purposes.
        crate::network::auth::ConnectionAuth::Authenticated {
            principal: crabka_security::Principal {
                name: "ANONYMOUS".to_string(),
                mechanism: crabka_security::SaslMechanism::Plain,
            },
        }
    };
```

Inside the request-handling block, before dispatching to handlers:

```rust
    let is_sasl_listener = spec.protocol.requires_sasl();
    if is_sasl_listener
        && !auth.is_authenticated()
        && !crate::network::auth::is_pre_auth_allowed(api_key)
    {
        // ILLEGAL_SASL_STATE (34) + close
        let resp_bytes = build_error_response(api_key, api_version, correlation_id, 34)?;
        write_response(&mut writer, &resp_bytes).await?;
        tracing::info!(%api_key, "rejecting pre-auth request, closing");
        return;
    }
```

`build_error_response` may already exist; if not, add a small helper that writes a minimal error response for the given api_key. (For most api_keys the response body has an `error_code` field at a known offset; for slice 12 we close after 34, so a generic short response is fine.)

- [ ] **Step 3: Add unit test for the gate**

In `crates/broker/src/network/auth.rs`:

```rust
#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn pre_auth_allowlist() {
        assert!(is_pre_auth_allowed(17)); // SaslHandshake
        assert!(is_pre_auth_allowed(36)); // SaslAuthenticate
        assert!(is_pre_auth_allowed(18)); // ApiVersions
        assert!(!is_pre_auth_allowed(0));  // Produce
        assert!(!is_pre_auth_allowed(1));  // Fetch
        assert!(!is_pre_auth_allowed(3));  // Metadata
    }

    #[test]
    fn anonymous_is_not_authenticated() {
        let a = ConnectionAuth::Anonymous;
        assert!(!a.is_authenticated());
        assert!(a.principal().is_none());
    }

    #[test]
    fn authenticated_returns_principal() {
        let a = ConnectionAuth::Authenticated {
            principal: Principal {
                name: "alice".into(),
                mechanism: SaslMechanism::ScramSha512,
            },
        };
        assert!(a.is_authenticated());
        assert_eq!(a.principal().unwrap().name, "alice");
    }
}
```

- [ ] **Step 4: Verify builds + existing tests pass**

```bash
cargo test -p crabka-broker --lib auth
cargo test -p crabka-broker --test auth_handlers
```

Expected: PASS. Existing PLAINTEXT tests continue working because they go through the `Authenticated{ANONYMOUS}` shortcut.

- [ ] **Step 5: Commit**

```bash
git add crates/broker/src/network/
git commit -m "feat(broker): ConnectionAuth state + pre-auth allowlist gate"
```

---

### Task 13: SaslHandshake (17) + SaslAuthenticate (36) — PLAIN

**Files:**
- Modify: `crates/broker/src/network/auth.rs`
- Modify: `crates/broker/src/network/dispatch.rs`
- Modify: `crates/broker/src/handlers/api_versions.rs`
- Modify: `crates/broker/src/network/dispatch.rs` (flexible-body table)

- [ ] **Step 1: Register the api_keys in dispatch tables**

In `crates/broker/src/network/dispatch.rs::handler_body_flexible`:

```rust
        (17, _) => false,        // SaslHandshake v1 — non-flexible body
        (36, v) => v >= 2,       // SaslAuthenticate flexible from v2
```

In `crates/broker/src/handlers/api_versions.rs::supported_apis`:

```rust
        ApiSupport { api_key: 17, min: 0, max: 1 },
        ApiSupport { api_key: 36, min: 0, max: 2 },
```

- [ ] **Step 2: Implement the SaslHandshake handler**

Append to `crates/broker/src/network/auth.rs`:

```rust
use crabka_protocol::owned::sasl_handshake_request_v1::SaslHandshakeRequest;
use crabka_protocol::owned::sasl_handshake_response_v1::SaslHandshakeResponse;

/// Handles SaslHandshake (api_key 17). Transitions `auth` to Negotiating on
/// success. On unsupported mechanism, returns SaslHandshakeResponse with
/// error_code=33 and the enabled list; connection stays open per Kafka.
pub fn handle_handshake(
    req: &SaslHandshakeRequest,
    auth: &mut ConnectionAuth,
    enabled: &[SaslMechanism],
) -> SaslHandshakeResponse {
    let requested = SaslMechanism::from_wire(&req.mechanism);
    let enabled_names: Vec<String> = enabled.iter().map(|m| m.wire_name().to_string()).collect();
    match requested {
        Some(m) if enabled.contains(&m) => {
            *auth = ConnectionAuth::Negotiating {
                mechanism: m,
                exchange: match m {
                    SaslMechanism::Plain => SaslExchange::Plain,
                    SaslMechanism::ScramSha512 => {
                        // SCRAM exchange built lazily on first authenticate
                        // because we don't have the credential yet.
                        SaslExchange::Plain // placeholder; replaced in task 14
                    }
                },
            };
            SaslHandshakeResponse {
                error_code: 0,
                mechanisms: enabled_names,
            }
        }
        _ => SaslHandshakeResponse {
            error_code: 33, // UNSUPPORTED_SASL_MECHANISM
            mechanisms: enabled_names,
        },
    }
}
```

- [ ] **Step 3: Implement PLAIN branch of SaslAuthenticate**

Append to `crates/broker/src/network/auth.rs`:

```rust
use crabka_protocol::owned::sasl_authenticate_request_v2::SaslAuthenticateRequest;
use crabka_protocol::owned::sasl_authenticate_response_v2::SaslAuthenticateResponse;
use std::collections::HashMap;

/// Handles SaslAuthenticate (api_key 36) for the PLAIN mechanism.
/// On success, transitions `auth` to Authenticated. Caller closes
/// connection if returned error_code != 0.
pub fn handle_authenticate_plain(
    req: &SaslAuthenticateRequest,
    auth: &mut ConnectionAuth,
    plain_credentials: &HashMap<String, String>,
) -> SaslAuthenticateResponse {
    // wire bytes: \0<authzid>\0<authcid>\0<password>  (we ignore authzid)
    let parts: Vec<&[u8]> = req.auth_bytes.split(|&b| b == 0).collect();
    if parts.len() != 3 {
        return fail_authenticate("malformed PLAIN payload");
    }
    let user = match std::str::from_utf8(parts[1]) {
        Ok(s) => s,
        Err(_) => return fail_authenticate("non-utf8 username"),
    };
    let password = parts[2];
    match crabka_security::verify_plain(plain_credentials, user, password) {
        Ok(p) => {
            *auth = ConnectionAuth::Authenticated { principal: p };
            SaslAuthenticateResponse {
                error_code: 0,
                error_message: None,
                auth_bytes: vec![],
                session_lifetime_ms: 0,
            }
        }
        Err(_) => fail_authenticate("authentication failed"),
    }
}

fn fail_authenticate(msg: &str) -> SaslAuthenticateResponse {
    tracing::debug!(reason = msg, "SASL authenticate failed");
    SaslAuthenticateResponse {
        error_code: 58, // SASL_AUTHENTICATION_FAILED
        error_message: Some("authentication failed".to_string()),
        auth_bytes: vec![],
        session_lifetime_ms: 0,
    }
}
```

(Field names like `auth_bytes`, `error_message`, `session_lifetime_ms` are best-guess from KIP-152; verify against `crates/protocol/generated/sasl_authenticate_request_v2.rs` and `..._response_v2.rs` before compiling.)

- [ ] **Step 4: Wire the handlers into `serve_connection_stream`**

In `crates/broker/src/network/dispatch.rs`, add dispatch arms for api_key 17 and 36 *before* the regular handler-table lookup, since they update `auth`:

```rust
            17 => {
                let req: crabka_protocol::owned::sasl_handshake_request_v1::SaslHandshakeRequest =
                    decode_body(api_version, body_bytes)?;
                let resp = crate::network::auth::handle_handshake(
                    &req,
                    &mut auth,
                    &broker.config.enabled_sasl_mechanisms,
                );
                let bytes = encode_response(api_key, api_version, correlation_id, &resp)?;
                write_response(&mut writer, &bytes).await?;
                continue;
            }
            36 => {
                let req: crabka_protocol::owned::sasl_authenticate_request_v2::SaslAuthenticateRequest =
                    decode_body(api_version, body_bytes)?;
                let mech = match &auth {
                    crate::network::auth::ConnectionAuth::Negotiating { mechanism, .. } => *mechanism,
                    _ => {
                        // 34 ILLEGAL_SASL_STATE + close
                        let resp = build_error_response(36, api_version, correlation_id, 34)?;
                        write_response(&mut writer, &resp).await?;
                        return;
                    }
                };
                let resp = match mech {
                    crabka_security::SaslMechanism::Plain => {
                        crate::network::auth::handle_authenticate_plain(
                            &req,
                            &mut auth,
                            &broker.config.plain_credentials,
                        )
                    }
                    crabka_security::SaslMechanism::ScramSha512 => {
                        // Implemented in task 14.
                        crate::network::auth::handle_authenticate_scram(
                            &req,
                            &mut auth,
                            &broker,
                        )
                    }
                };
                let was_success = resp.error_code == 0;
                let bytes = encode_response(36, api_version, correlation_id, &resp)?;
                write_response(&mut writer, &bytes).await?;
                if !was_success {
                    return; // close connection on auth failure
                }
                continue;
            }
```

- [ ] **Step 5: Add stub for `handle_authenticate_scram` so it compiles**

Append to `crates/broker/src/network/auth.rs`:

```rust
pub fn handle_authenticate_scram(
    _req: &SaslAuthenticateRequest,
    _auth: &mut ConnectionAuth,
    _broker: &crate::broker::Broker,
) -> SaslAuthenticateResponse {
    fail_authenticate("SCRAM not yet implemented")
}
```

- [ ] **Step 6: Integration test for SASL/PLAIN**

In `crates/broker/tests/auth_handlers.rs`:

```rust
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn sasl_plain_happy_path() {
    let mut cfg = BrokerConfig::default();
    cfg.listeners = vec![crabka_broker::config::ListenerSpec {
        name: "SASL_PLAINTEXT".to_string(),
        bind_addr: "127.0.0.1:0".parse().unwrap(),
        advertised: "127.0.0.1:0".to_string(),
        protocol: ListenerProtocol::SaslPlaintext,
    }];
    cfg.inter_broker_listener_name = "SASL_PLAINTEXT".to_string();
    cfg.enabled_sasl_mechanisms = vec![SaslMechanism::Plain];
    cfg.plain_credentials.insert("alice".to_string(), "wonderland".to_string());
    let handle = Broker::start(cfg).await.expect("start");
    let addr = handle.listen_addr();

    // Dial, send ApiVersions, SaslHandshake(PLAIN), SaslAuthenticate, Metadata.
    // (Full client-side codec dance — write actual bytes via crabka_protocol.)
    // This test asserts the connection survives past auth and Metadata responds.
    let result = drive_sasl_plain_session(addr, "alice", "wonderland").await;
    assert!(result.is_ok(), "session must succeed: {result:?}");
    handle.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn sasl_plain_wrong_password_closes_connection() {
    let mut cfg = BrokerConfig::default();
    // ... same setup as above but pass bad password ...
    let result = drive_sasl_plain_session(addr, "alice", "wrong").await;
    assert!(result.is_err(), "wrong password must fail");
}
```

`drive_sasl_plain_session` is a helper at the bottom of the file: dials the broker, runs the 3-frame dance via `tokio::net::TcpStream` + raw protocol bytes, returns `Result<(), io::Error>`. Implement it using `crabka_protocol::owned::*` types and a length-prefixed framing helper.

- [ ] **Step 7: Run tests**

```bash
cargo test -p crabka-broker --test auth_handlers sasl_plain
```

Expected: PASS.

- [ ] **Step 8: Commit**

```bash
git add crates/broker/
git commit -m "feat(broker): SaslHandshake (17) + SaslAuthenticate PLAIN (36)"
```

---

### Task 14: SaslAuthenticate — SCRAM-SHA-512

**Files:**
- Modify: `crates/broker/src/network/auth.rs`

- [ ] **Step 1: Replace the SCRAM stub with a real handler**

In `crates/broker/src/network/auth.rs`:

```rust
pub fn handle_authenticate_scram(
    req: &SaslAuthenticateRequest,
    auth: &mut ConnectionAuth,
    broker: &crate::broker::Broker,
) -> SaslAuthenticateResponse {
    let exchange = match auth {
        ConnectionAuth::Negotiating { exchange: SaslExchange::Scram(e), .. } => e,
        ConnectionAuth::Negotiating { exchange: SaslExchange::Plain, mechanism } => {
            // First SCRAM call: parse the client-first message to learn the
            // username, look up the credential, init ScramServerExchange.
            let username = match parse_scram_username(&req.auth_bytes) {
                Some(u) => u,
                None => return fail_authenticate("malformed SCRAM client-first"),
            };
            let cred = match broker
                .controller
                .current_image()
                .scram_credential(&username, *mechanism)
                .cloned()
            {
                Some(c) => c,
                None => return fail_authenticate("unknown user"),
            };
            let server = crabka_security::ScramServerExchange::new(username, cred);
            *exchange = SaslExchange::Scram(server);
            let SaslExchange::Scram(e) = exchange else { unreachable!() };
            e
        }
        _ => return fail_authenticate("not in SCRAM negotiation"),
    };
    match exchange.step(&req.auth_bytes) {
        crabka_security::StepResult::Continue(bytes) => SaslAuthenticateResponse {
            error_code: 0,
            error_message: None,
            auth_bytes: bytes,
            session_lifetime_ms: 0,
        },
        crabka_security::StepResult::Done(principal, bytes) => {
            *auth = ConnectionAuth::Authenticated { principal };
            SaslAuthenticateResponse {
                error_code: 0,
                error_message: None,
                auth_bytes: bytes,
                session_lifetime_ms: 0,
            }
        }
        crabka_security::StepResult::Failed(_) => fail_authenticate("SCRAM failed"),
    }
}

fn parse_scram_username(bytes: &[u8]) -> Option<String> {
    let s = std::str::from_utf8(bytes).ok()?;
    let bare = s.strip_prefix("n,,")?;
    for attr in bare.split(',') {
        if let Some(v) = attr.strip_prefix("n=") {
            return Some(v.to_string());
        }
    }
    None
}
```

NOTE: the `handle_handshake` in task 13 sets `SaslExchange::Plain` as a placeholder even for SCRAM. The SCRAM handler upgrades that to `SaslExchange::Scram(...)` on first call, when the username is parseable from `auth_bytes`. Fix `handle_handshake` so SCRAM starts as `SaslExchange::Plain` (or rename to a dedicated `SaslExchange::ScramPending` variant for clarity):

```rust
// Replace SaslExchange enum:
pub enum SaslExchange {
    Plain,
    ScramPending,        // SCRAM handshake done; no exchange built yet (needs username)
    Scram(ScramServerExchange),
}
```

Update `handle_handshake` SCRAM branch:

```rust
                SaslMechanism::ScramSha512 => SaslExchange::ScramPending,
```

And the SCRAM handler match: when in `ScramPending` state, build the exchange.

- [ ] **Step 2: Add an integration test**

In `crates/broker/tests/auth_handlers.rs`:

```rust
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn sasl_scram_sha512_happy_path() {
    let mut cfg = BrokerConfig::default();
    cfg.listeners = vec![crabka_broker::config::ListenerSpec {
        name: "SASL_PLAINTEXT".to_string(),
        bind_addr: "127.0.0.1:0".parse().unwrap(),
        advertised: "127.0.0.1:0".to_string(),
        protocol: ListenerProtocol::SaslPlaintext,
    }];
    cfg.inter_broker_listener_name = "SASL_PLAINTEXT".to_string();
    cfg.enabled_sasl_mechanisms = vec![SaslMechanism::ScramSha512];
    let handle = Broker::start(cfg).await.expect("start");

    // Provision alice/wonderland directly via the controller (bypassing
    // AlterUserScramCredentials — that's task 15).
    let cred = crabka_security::hash_scram_password(
        b"wonderland",
        SaslMechanism::ScramSha512,
        4096,
    );
    handle.submit_metadata_record_for_test(
        crabka_metadata::MetadataRecord::V1ScramCredential(
            crabka_metadata::records::ScramCredentialRecord {
                user: "alice".into(),
                mechanism: SaslMechanism::ScramSha512,
                salt: cred.salt,
                stored_key: cred.stored_key,
                server_key: cred.server_key,
                iterations: cred.iterations,
            },
        ),
    ).await.unwrap();

    let result = drive_sasl_scram_session(
        handle.listen_addr(),
        "alice",
        "wonderland",
    ).await;
    assert!(result.is_ok(), "session must succeed: {result:?}");
    handle.shutdown().await;
}
```

`submit_metadata_record_for_test` is a new test-only accessor on `BrokerHandle`:

```rust
#[cfg(any(test, feature = "test-helpers"))]
pub async fn submit_metadata_record_for_test(
    &self,
    rec: crabka_metadata::MetadataRecord,
) -> Result<(), BrokerError> {
    self._broker.controller.submit_change(rec)
        .await
        .map_err(|e| BrokerError::Replication(format!("submit: {e}")))
}
```

`drive_sasl_scram_session` is a helper that runs the full SCRAM dance using `crabka_security::ScramClientExchange` on the client side and bytes-over-the-wire.

- [ ] **Step 3: Run tests**

```bash
cargo test -p crabka-broker --test auth_handlers sasl_scram
```

Expected: PASS.

- [ ] **Step 4: Commit**

```bash
git add crates/broker/src/ crates/broker/tests/auth_handlers.rs
git commit -m "feat(broker): SaslAuthenticate SCRAM-SHA-512 branch"
```

---

## Batch 6 — Provisioning + inter-broker auth

### Task 15: `AlterUserScramCredentials` handler (api_key 51)

**Files:**
- Create: `crates/broker/src/handlers/alter_user_scram_credentials.rs`
- Modify: `crates/broker/src/handlers/mod.rs`
- Modify: `crates/broker/src/handlers/api_versions.rs`
- Modify: `crates/broker/src/network/dispatch.rs` (flexible-body table)
- Modify: `crates/broker/src/codes.rs` (RESOURCE_NOT_FOUND, UNACCEPTABLE_CREDENTIAL, DUPLICATE_RESOURCE)

- [ ] **Step 1: Add Kafka error code constants**

In `crates/broker/src/codes.rs`, add:

```rust
pub const CLUSTER_AUTHORIZATION_FAILED: i16 = 31;
pub const RESOURCE_NOT_FOUND: i16 = 66;
pub const UNACCEPTABLE_CREDENTIAL: i16 = 74;
pub const DUPLICATE_RESOURCE: i16 = 84;
```

- [ ] **Step 2: Register the api_key**

In `dispatch.rs::handler_body_flexible`:

```rust
        (51, _) => true,         // AlterUserScramCredentials flexible from v0
```

In `api_versions.rs::supported_apis`:

```rust
        ApiSupport { api_key: 51, min: 0, max: 0 },
```

- [ ] **Step 3: Write the handler**

Create `crates/broker/src/handlers/alter_user_scram_credentials.rs`:

```rust
//! AlterUserScramCredentials handler (api_key 51, KIP-554).
//!
//! Per-request validation: iterations >= 4096, non-empty salt, salted_password
//! length must match mechanism hash size. Authorization stand-in: principal
//! must equal super_user_name.

use std::collections::HashSet;

use crabka_metadata::records::{
    DeleteScramCredentialRecord, MetadataRecord, ScramCredentialRecord,
};
use crabka_protocol::owned::alter_user_scram_credentials_request_v0::*;
use crabka_protocol::owned::alter_user_scram_credentials_response_v0::*;
use crabka_security::{Principal, SaslMechanism};

use crate::broker::Broker;
use crate::codes;

const MIN_ITERATIONS: u32 = 4096;
const SHA512_OUTPUT_LEN: usize = 64;

pub async fn handle(
    broker: &Broker,
    req: AlterUserScramCredentialsRequest,
    principal: &Principal,
) -> AlterUserScramCredentialsResponse {
    let authorized = broker
        .config
        .super_user_name
        .as_deref()
        .is_some_and(|s| s == principal.name);

    let mut seen: HashSet<(String, i8)> = HashSet::new();
    let mut user_results: Vec<AlterUserScramCredentialsResult> = vec![];
    let mut records: Vec<MetadataRecord> = vec![];

    for d in req.deletions {
        let key = (d.name.clone(), d.mechanism);
        if !seen.insert(key.clone()) {
            user_results.push(err(d.name, codes::DUPLICATE_RESOURCE, "duplicate resource"));
            continue;
        }
        let mech = match wire_to_mech(d.mechanism) {
            Some(m) => m,
            None => {
                user_results.push(err(d.name, codes::UNACCEPTABLE_CREDENTIAL, "unknown mechanism"));
                continue;
            }
        };
        if !authorized {
            user_results.push(err(d.name, codes::CLUSTER_AUTHORIZATION_FAILED, "not super-user"));
            continue;
        }
        if broker.controller.current_image().scram_credential(&d.name, mech).is_none() {
            user_results.push(err(d.name, codes::RESOURCE_NOT_FOUND, "credential not found"));
            continue;
        }
        records.push(MetadataRecord::V1DeleteScramCredential(DeleteScramCredentialRecord {
            user: d.name.clone(),
            mechanism: mech,
        }));
        user_results.push(ok(d.name));
    }

    for u in req.upsertions {
        let key = (u.name.clone(), u.mechanism);
        if !seen.insert(key) {
            user_results.push(err(u.name, codes::DUPLICATE_RESOURCE, "duplicate resource"));
            continue;
        }
        let mech = match wire_to_mech(u.mechanism) {
            Some(m) => m,
            None => {
                user_results.push(err(u.name, codes::UNACCEPTABLE_CREDENTIAL, "unknown mechanism"));
                continue;
            }
        };
        if u.iterations < MIN_ITERATIONS as i32 {
            user_results.push(err(u.name, codes::UNACCEPTABLE_CREDENTIAL, "iterations < 4096"));
            continue;
        }
        if u.salt.is_empty() {
            user_results.push(err(u.name, codes::UNACCEPTABLE_CREDENTIAL, "empty salt"));
            continue;
        }
        if u.salted_password.len() != SHA512_OUTPUT_LEN {
            user_results.push(err(u.name, codes::UNACCEPTABLE_CREDENTIAL, "wrong salted_password length"));
            continue;
        }
        if !authorized {
            user_results.push(err(u.name, codes::CLUSTER_AUTHORIZATION_FAILED, "not super-user"));
            continue;
        }
        // Reconstruct stored_key + server_key from salted_password.
        // Client (KIP-554) sent us H_i(password, salt, iters) directly.
        let (stored_key, server_key) =
            crabka_security::derive_keys_from_salted(&u.salted_password);
        records.push(MetadataRecord::V1ScramCredential(ScramCredentialRecord {
            user: u.name.clone(),
            mechanism: mech,
            salt: u.salt,
            stored_key,
            server_key,
            iterations: u.iterations as u32,
        }));
        user_results.push(ok(u.name));
    }

    // Submit records sequentially. Per-user errors above already populated
    // the response; on submit failure we replace OKs with that error.
    for rec in records {
        if let Err(e) = broker.controller.submit_change(rec).await {
            tracing::warn!(error = %e, "submit V1ScramCredential failed");
            // Convert any pending OK rows to a generic 0 error_code is wrong;
            // surface a CLUSTER_AUTHORIZATION_FAILED with the submit error.
            for r in user_results.iter_mut().filter(|r| r.error_code == 0) {
                r.error_code = codes::CLUSTER_AUTHORIZATION_FAILED;
                r.error_message = Some(format!("submit failed: {e}"));
            }
            break;
        }
    }

    AlterUserScramCredentialsResponse {
        throttle_time_ms: 0,
        results: user_results,
    }
}

fn wire_to_mech(wire: i8) -> Option<SaslMechanism> {
    // KIP-554: 0 = unknown, 1 = SCRAM-SHA-256, 2 = SCRAM-SHA-512
    match wire {
        2 => Some(SaslMechanism::ScramSha512),
        _ => None,
    }
}

fn ok(name: String) -> AlterUserScramCredentialsResult {
    AlterUserScramCredentialsResult {
        user: name,
        error_code: 0,
        error_message: None,
    }
}

fn err(name: String, code: i16, msg: &str) -> AlterUserScramCredentialsResult {
    AlterUserScramCredentialsResult {
        user: name,
        error_code: code,
        error_message: Some(msg.to_string()),
    }
}
```

`derive_keys_from_salted` is a new helper added to `crabka-security`:

```rust
// In crates/security/src/scram/mod.rs:
pub fn derive_keys_from_salted(salted: &[u8]) -> (Vec<u8>, Vec<u8>) {
    use hmac::{Hmac, Mac};
    use sha2::{Digest, Sha512};
    let mut ck_mac = <Hmac<Sha512>>::new_from_slice(salted).expect("hmac");
    ck_mac.update(b"Client Key");
    let client_key = ck_mac.finalize().into_bytes();
    let stored_key = Sha512::digest(&client_key).to_vec();
    let mut sk_mac = <Hmac<Sha512>>::new_from_slice(salted).expect("hmac");
    sk_mac.update(b"Server Key");
    let server_key = sk_mac.finalize().into_bytes().to_vec();
    (stored_key, server_key)
}
```

- [ ] **Step 4: Register the handler in `handlers/mod.rs`**

```rust
mod alter_user_scram_credentials;
// in build_table or wherever the existing slice-11 entries are:
table.insert(51, Handler::AlterUserScramCredentials);
```

Dispatch in `dispatch.rs`:

```rust
            51 => {
                let req = decode_body(api_version, body_bytes)?;
                let principal = auth.principal()
                    .cloned()
                    .unwrap_or_else(|| crabka_security::Principal {
                        name: "ANONYMOUS".into(),
                        mechanism: crabka_security::SaslMechanism::Plain,
                    });
                let resp = crate::handlers::alter_user_scram_credentials::handle(
                    &broker, req, &principal,
                ).await;
                let bytes = encode_response(51, api_version, correlation_id, &resp)?;
                write_response(&mut writer, &bytes).await?;
                continue;
            }
```

- [ ] **Step 5: Write integration tests**

In `crates/broker/tests/auth_handlers.rs`:

```rust
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn alter_scram_creds_super_user_can_provision() {
    // Set up a broker with super_user_name="admin" + admin's PLAIN creds.
    // Auth as admin via PLAIN. Send AlterUserScramCredentials upserting "alice".
    // Confirm response carries 0 error_code, then auth as alice with SCRAM.
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn alter_scram_creds_non_super_user_rejected() {
    // Same setup, auth as a regular user, send upsertion. Expect error_code 31.
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn alter_scram_creds_low_iterations_rejected() {
    // Auth as super-user, send upsertion with iterations=1. Expect 74.
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn alter_scram_creds_duplicate_resource_rejected() {
    // Two upsertions for same (user, mech) in one request. Expect 84 on second.
}
```

Write actual bodies — full client-side codec dance via shared helpers from the SASL/PLAIN integration tests.

- [ ] **Step 6: Run tests**

```bash
cargo test -p crabka-broker --test auth_handlers alter_scram
```

Expected: PASS (4 tests).

- [ ] **Step 7: Commit**

```bash
git add crates/security/ crates/broker/
git commit -m "feat(broker): AlterUserScramCredentials handler (api_key 51, KIP-554)"
```

---

### Task 16: `InterBrokerClient` (outbound TLS + SASL)

**Files:**
- Create: `crates/broker/src/network/client.rs`
- Modify: `crates/broker/src/network/mod.rs`

- [ ] **Step 1: Write `crates/broker/src/network/client.rs`**

```rust
//! Outbound inter-broker client. Establishes TCP, optionally wraps in TLS,
//! optionally runs SASL client handshake. Returns a generic `AsyncRead +
//! AsyncWrite` stream the caller uses for normal RPCs.

use std::sync::Arc;

use crabka_security::{ListenerProtocol, SaslMechanism, ScramClientExchange};
use thiserror::Error;
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio_rustls::TlsConnector;

use crate::config::InterBrokerCredentials;

#[derive(Debug, Error)]
pub enum InterBrokerError {
    #[error("io: {0}")]
    Io(#[from] std::io::Error),
    #[error("tls: {0}")]
    Tls(String),
    #[error("sasl: {0}")]
    Sasl(String),
    #[error("config: {0}")]
    Config(String),
}

pub struct InterBrokerClient {
    tls_connector: Option<TlsConnector>,
    creds: Option<InterBrokerCredentials>,
}

impl InterBrokerClient {
    #[must_use]
    pub fn new(tls_connector: Option<TlsConnector>, creds: Option<InterBrokerCredentials>) -> Self {
        Self { tls_connector, creds }
    }

    pub async fn connect(
        &self,
        host: &str,
        port: u16,
        listener_protocol: ListenerProtocol,
        server_name: &str,
    ) -> Result<Box<dyn DuplexStream>, InterBrokerError> {
        let tcp = TcpStream::connect((host, port)).await?;
        let mut stream: Box<dyn DuplexStream> = if listener_protocol.requires_tls() {
            let connector = self.tls_connector.clone().ok_or_else(|| {
                InterBrokerError::Config("TLS listener without TlsConnector".into())
            })?;
            let server_name = tokio_rustls::rustls::pki_types::ServerName::try_from(
                server_name.to_string(),
            )
            .map_err(|e| InterBrokerError::Tls(format!("invalid server name: {e}")))?;
            let tls = connector
                .connect(server_name, tcp)
                .await
                .map_err(|e| InterBrokerError::Tls(e.to_string()))?;
            Box::new(tls)
        } else {
            Box::new(tcp)
        };
        if listener_protocol.requires_sasl() {
            let creds = self.creds.clone().ok_or_else(|| {
                InterBrokerError::Config("SASL listener without inter_broker_credentials".into())
            })?;
            run_outbound_sasl(&mut *stream, &creds).await?;
        }
        Ok(stream)
    }
}

/// Trait alias for boxed duplex streams.
pub trait DuplexStream: AsyncRead + AsyncWrite + Unpin + Send {}
impl<T: AsyncRead + AsyncWrite + Unpin + Send + ?Sized> DuplexStream for T {}

async fn run_outbound_sasl(
    stream: &mut (dyn DuplexStream),
    creds: &InterBrokerCredentials,
) -> Result<(), InterBrokerError> {
    // Frame helpers — length-prefixed Kafka requests.
    // Step 1: ApiVersions (optional but JVM-compatible). We skip for simplicity.
    // Step 2: SaslHandshake with the chosen mechanism.
    send_sasl_handshake(stream, creds.mechanism).await?;
    // Step 3: SaslAuthenticate (one round for PLAIN, two rounds for SCRAM).
    match creds.mechanism {
        SaslMechanism::Plain => send_plain_authenticate(stream, &creds.username, &creds.password).await,
        SaslMechanism::ScramSha512 => run_scram_client(stream, &creds.username, &creds.password).await,
    }
}

async fn send_sasl_handshake(
    _stream: &mut (dyn DuplexStream),
    _mechanism: SaslMechanism,
) -> Result<(), InterBrokerError> {
    // Build SaslHandshakeRequest v1 via crabka_protocol::owned, frame it,
    // send, read response, check error_code==0.
    // ... codec details ...
    todo!("send SaslHandshakeRequest v1 and verify response — write this body fully before commit")
}

async fn send_plain_authenticate(
    _stream: &mut (dyn DuplexStream),
    _user: &str,
    _pass: &str,
) -> Result<(), InterBrokerError> {
    todo!("send SaslAuthenticate v2 with \\0user\\0password auth_bytes — write body before commit")
}

async fn run_scram_client(
    stream: &mut (dyn DuplexStream),
    user: &str,
    pass: &str,
) -> Result<(), InterBrokerError> {
    let mut exch = ScramClientExchange::new(user.to_string(), pass.as_bytes().to_vec());
    let c1 = exch.client_first().map_err(|e| InterBrokerError::Sasl(format!("{e:?}")))?;
    // Frame, send, recv server-first via SaslAuthenticate v2.
    // ...
    let _ = c1;
    let _ = stream;
    todo!("frame + send + recv SaslAuthenticate frames — write body before commit")
}
```

NOTE: the three `todo!` calls must each become a full body before the commit step. Pattern after the integration-test helpers from task 13's `drive_sasl_plain_session` — same framing, same codec types, just from the client side.

- [ ] **Step 2: Add `mod client;` to `crates/broker/src/network/mod.rs`**

- [ ] **Step 3: Add a unit test using a paired in-process server**

In `crates/broker/tests/auth_handlers.rs`:

```rust
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn inter_broker_client_authenticates_via_plain() {
    // Spin up a broker with SASL_PLAINTEXT listener + plain_credentials["broker"]=...
    // Then construct an InterBrokerClient with the matching creds and call connect().
    // Assert the returned stream survives a subsequent ApiVersions exchange.
}
```

- [ ] **Step 4: Run tests + commit**

```bash
cargo test -p crabka-broker --test auth_handlers inter_broker
git add crates/broker/
git commit -m "feat(broker): InterBrokerClient (outbound TLS + SASL handshake)"
```

---

### Task 17: Wire `InterBrokerClient` into replicator + raft transport + heartbeat

**Files:**
- Modify: `crates/broker/src/replicator.rs`
- Modify: `crates/raft/src/transport.rs` (locate first)
- Modify: `crates/broker/src/heartbeat/controller_state.rs` (or wherever outbound heartbeat lives)

- [ ] **Step 1: Build an `Arc<InterBrokerClient>` once on `Broker`**

In `Broker::start`, after constructing `TlsAcceptor`:

```rust
        let tls_connector = config.tls_config.as_ref()
            .map(|t| t.build_client_config()
                .map(tokio_rustls::TlsConnector::from)
                .map_err(|e| BrokerError::Tls(e.to_string())))
            .transpose()?;
        let inter_broker_client = Arc::new(crate::network::client::InterBrokerClient::new(
            tls_connector,
            config.inter_broker_credentials.clone(),
        ));
```

Store on `Broker`.

- [ ] **Step 2: Wire into replicator**

In `crates/broker/src/replicator.rs`, where replicator currently constructs a `TcpStream` to dial a peer, replace with `broker.inter_broker_client.connect(host, port, protocol, server_name).await`. Get the peer's host/port/protocol from `metadata_image.broker(peer).endpoints` filtered by `inter_broker_listener_name`.

- [ ] **Step 3: Wire into raft transport**

`crates/raft/src/transport.rs` — same pattern. Raft transport needs to take an `Arc<dyn OutboundDialer>` injected from the broker (since `crabka-raft` shouldn't depend on `crabka-broker`). Define a small trait:

```rust
// In crates/raft/src/transport.rs:
#[async_trait::async_trait]
pub trait OutboundDialer: Send + Sync {
    async fn dial(&self, target: NodeId) -> Result<Box<dyn DuplexStream>, RaftError>;
}
```

The broker provides an impl that wraps `InterBrokerClient`. Existing raft transport code switches to using `OutboundDialer::dial` instead of `TcpStream::connect`.

- [ ] **Step 4: Wire into heartbeat / controller liveness**

Same pattern. The controller heartbeat loop dials the controller leader; use `InterBrokerClient`.

- [ ] **Step 5: Test — multi-broker SASL replication**

In `crates/broker/tests/auth_handlers.rs`:

```rust
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn two_broker_sasl_plaintext_replication() {
    // Spin up two brokers with SASL_PLAINTEXT inter-broker.
    // Create a topic rf=2. Produce. Verify follower has the records.
    // (Use the existing 2-broker test scaffolding from slice 10b as the
    // base; just flip the listener config.)
}
```

- [ ] **Step 6: Run all broker tests**

```bash
cargo test -p crabka-broker
```

Expected: PASS. Existing PLAINTEXT-only multi-broker tests still green (no creds set → `requires_sasl()` returns false → SASL handshake skipped).

- [ ] **Step 7: Commit**

```bash
git add crates/broker/ crates/raft/
git commit -m "feat(broker): replicator + raft + heartbeat dial via InterBrokerClient"
```

---

## Batch 7 — `crabka-cli` (format CLI)

### Task 18: `crabka format --add-scram`

**Files:**
- Create: `crates/cli/Cargo.toml`
- Create: `crates/cli/src/main.rs`
- Create: `crates/cli/src/format.rs`

- [ ] **Step 1: Cargo.toml**

```toml
[package]
name = "crabka-cli"
version.workspace = true
edition.workspace = true
license.workspace = true
authors.workspace = true
rust-version.workspace = true

[[bin]]
name = "crabka"
path = "src/main.rs"

[lints]
workspace = true

[dependencies]
clap.workspace = true
crabka-security.workspace = true
crabka-metadata.workspace = true
crabka-raft.workspace = true
thiserror.workspace = true
tokio = { workspace = true, features = ["rt-multi-thread", "macros", "fs"] }
serde_wincode.workspace = true
wincode.workspace = true
tracing.workspace = true
tracing-subscriber.workspace = true
uuid.workspace = true
```

- [ ] **Step 2: Write `crates/cli/src/main.rs`**

```rust
//! Crabka CLI. Slice 12: only the `format` subcommand exists.

use clap::{Parser, Subcommand};

mod format;

#[derive(Parser)]
#[command(name = "crabka", version, about = "Crabka operator CLI")]
struct Cli {
    #[command(subcommand)]
    command: Command,
}

#[derive(Subcommand)]
enum Command {
    /// Format a fresh log directory, optionally seeding SCRAM credentials.
    Format(format::FormatArgs),
}

#[tokio::main]
async fn main() {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info")),
        )
        .init();
    let cli = Cli::parse();
    let rc = match cli.command {
        Command::Format(args) => format::run(args).await,
    };
    std::process::exit(rc);
}
```

- [ ] **Step 3: Write `crates/cli/src/format.rs`**

```rust
use std::path::PathBuf;

use clap::Args;
use crabka_metadata::records::{MetadataRecord, ScramCredentialRecord};
use crabka_security::{SaslMechanism, hash_scram_password_with_salt};
use ring::rand::{SecureRandom, SystemRandom};
use uuid::Uuid;

#[derive(Args, Debug)]
pub struct FormatArgs {
    /// Directory to format. Must be empty or non-existent.
    #[arg(long)]
    log_dir: PathBuf,
    /// Cluster id. Generated if not provided.
    #[arg(long)]
    cluster_id: Option<Uuid>,
    /// Seed a SCRAM credential. May be repeated.
    /// Format: `SCRAM-SHA-512=[name=<u>,password=<p>,iterations=<n>]`
    #[arg(long, value_parser = parse_scram_spec)]
    add_scram: Vec<ScramSpec>,
}

#[derive(Debug, Clone)]
pub struct ScramSpec {
    mechanism: SaslMechanism,
    name: String,
    password: String,
    iterations: u32,
}

fn parse_scram_spec(s: &str) -> Result<ScramSpec, String> {
    let s = s.trim();
    let s = s.strip_prefix("SCRAM-SHA-512=[").ok_or("must start with SCRAM-SHA-512=[")?;
    let s = s.strip_suffix(']').ok_or("must end with ]")?;
    let mut name = None;
    let mut password = None;
    let mut iterations = 4096u32;
    for attr in s.split(',') {
        let (k, v) = attr.split_once('=').ok_or_else(|| format!("malformed attr: {attr}"))?;
        match k.trim() {
            "name" => name = Some(v.trim().to_string()),
            "password" => password = Some(v.trim().to_string()),
            "iterations" => iterations = v.trim().parse().map_err(|e| format!("iterations: {e}"))?,
            other => return Err(format!("unknown attr: {other}")),
        }
    }
    Ok(ScramSpec {
        mechanism: SaslMechanism::ScramSha512,
        name: name.ok_or("missing name")?,
        password: password.ok_or("missing password")?,
        iterations,
    })
}

pub async fn run(args: FormatArgs) -> i32 {
    // Refuse to overwrite a non-empty directory.
    if args.log_dir.exists()
        && std::fs::read_dir(&args.log_dir)
            .map(|mut it| it.next().is_some())
            .unwrap_or(false)
    {
        eprintln!("crabka format: refusing to overwrite non-empty log_dir {:?}", args.log_dir);
        return 3;
    }
    std::fs::create_dir_all(&args.log_dir).expect("create log_dir");

    let cluster_id = args.cluster_id.unwrap_or_else(Uuid::new_v4);
    let mut records: Vec<MetadataRecord> = vec![];
    for spec in &args.add_scram {
        if spec.iterations < 4096 {
            eprintln!("crabka format: iterations must be >= 4096, got {}", spec.iterations);
            return 2;
        }
        let mut salt = vec![0u8; 16];
        SystemRandom::new().fill(&mut salt).expect("rng");
        let cred = hash_scram_password_with_salt(
            spec.password.as_bytes(),
            spec.mechanism,
            spec.iterations,
            salt,
        );
        records.push(MetadataRecord::V1ScramCredential(ScramCredentialRecord {
            user: spec.name.clone(),
            mechanism: spec.mechanism,
            salt: cred.salt,
            stored_key: cred.stored_key,
            server_key: cred.server_key,
            iterations: cred.iterations,
        }));
    }

    // Append bootstrap records to a fresh raft log at args.log_dir.
    // We don't start a full broker — just write the on-disk records via
    // crabka_raft's bootstrap API.
    if let Err(e) = crabka_raft::bootstrap_log_dir(&args.log_dir, cluster_id, &records).await {
        eprintln!("crabka format: bootstrap failed: {e}");
        return 4;
    }
    println!("Formatted {} with cluster-id {}", args.log_dir.display(), cluster_id);
    0
}
```

NOTE: `crabka_raft::bootstrap_log_dir` is a new public function. Add it as a thin wrapper around the existing bootstrap path the broker uses on first start — it writes the initial raft snapshot/log files containing the cluster-id record and any extra metadata records, then returns. If the existing bootstrap is tightly coupled inside `Broker::start`, factor out the file-writing piece into a free function in `crabka-raft`.

- [ ] **Step 4: Add `crates/cli` to the workspace**

Workspace `Cargo.toml` already says `members = ["crates/*"]` so this is automatic.

- [ ] **Step 5: Add a smoke test**

Create `crates/cli/tests/format_smoke.rs`:

```rust
//! Smoke test: run `crabka format --add-scram` and assert the credential
//! is readable in a freshly-started broker.

use std::process::Command;

#[test]
fn format_with_add_scram_writes_credential_record() {
    let bin = env!("CARGO_BIN_EXE_crabka");
    let dir = tempfile::tempdir().unwrap();
    let out = Command::new(bin)
        .args([
            "format",
            "--log-dir", dir.path().to_str().unwrap(),
            "--add-scram", "SCRAM-SHA-512=[name=admin,password=admin-secret,iterations=4096]",
        ])
        .output()
        .expect("run crabka format");
    assert!(
        out.status.success(),
        "format failed: stdout={} stderr={}",
        String::from_utf8_lossy(&out.stdout),
        String::from_utf8_lossy(&out.stderr),
    );
    // Open the bootstrap log dir and assert at least one file exists.
    let entries: Vec<_> = std::fs::read_dir(dir.path()).unwrap().collect();
    assert!(!entries.is_empty(), "format must write something");
}

#[test]
fn format_low_iterations_fails() {
    let bin = env!("CARGO_BIN_EXE_crabka");
    let dir = tempfile::tempdir().unwrap();
    let out = Command::new(bin)
        .args([
            "format",
            "--log-dir", dir.path().to_str().unwrap(),
            "--add-scram", "SCRAM-SHA-512=[name=admin,password=p,iterations=1]",
        ])
        .output()
        .expect("run crabka format");
    assert!(!out.status.success(), "must fail with iterations < 4096");
    assert_eq!(out.status.code(), Some(2));
}
```

- [ ] **Step 6: Run tests + commit**

```bash
cargo build -p crabka-cli
cargo test -p crabka-cli
git add crates/cli/ crates/raft/
git commit -m "feat(cli): crabka format --add-scram subcommand"
```

---

## Batch 8 — Integration tests (no Docker)

### Task 19: Broker-side integration test sweep

**Files:**
- Modify: `crates/broker/tests/auth_handlers.rs`

Consolidate and round out the no-Docker test coverage. Earlier tasks added focused tests; this task fills in the matrix from the spec's "Integration tests" section.

- [ ] **Step 1: Add missing tests**

Verify all of these exist in `auth_handlers.rs` (added across prior tasks). Add any that are missing:

```rust
// From task 10:
//   tls_listener_accepts_tls_handshake_only
// From task 13:
//   sasl_plain_happy_path
//   sasl_plain_wrong_password_closes_connection
// From task 14:
//   sasl_scram_sha512_happy_path
// From task 15:
//   alter_scram_creds_super_user_can_provision
//   alter_scram_creds_non_super_user_rejected
//   alter_scram_creds_low_iterations_rejected
//   alter_scram_creds_duplicate_resource_rejected
// From task 16:
//   inter_broker_client_authenticates_via_plain
// From task 17:
//   two_broker_sasl_plaintext_replication

// Add now:
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn api_versions_reachable_pre_auth_on_sasl_listener() {
    // SASL_PLAINTEXT listener. Dial. Send ApiVersions WITHOUT auth.
    // Assert response decodes successfully and lists api_keys 17, 36.
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn metadata_rejected_pre_auth_on_sasl_listener() {
    // SASL_PLAINTEXT listener. Dial. Send Metadata WITHOUT auth.
    // Assert response carries error_code 34 (ILLEGAL_SASL_STATE) and
    // that the connection closes after.
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn unsupported_mechanism_rejected_but_handshake_retryable() {
    // Send SaslHandshake with mechanism="GSSAPI" (not enabled).
    // Assert response carries error_code 33 + the enabled list,
    // and that the connection is still open afterwards (try a new SaslHandshake).
}
```

- [ ] **Step 2: Run the full file**

```bash
cargo test -p crabka-broker --test auth_handlers
```

Expected: all tests PASS.

- [ ] **Step 3: Commit**

```bash
git add crates/broker/tests/auth_handlers.rs
git commit -m "test(broker): fill in remaining slice-12 integration test matrix"
```

---

## Batch 9 — JVM acceptance

### Task 20: JVM SASL/PLAIN produce/consume

**Files:**
- Modify: `crates/broker/tests/jvm_acceptance.rs`

- [ ] **Step 1: Add a helper for SASL JAAS-config strings**

Append to `jvm_acceptance.rs`:

```rust
fn plain_jaas(user: &str, pass: &str) -> String {
    format!(
        "org.apache.kafka.common.security.plain.PlainLoginModule required \
         username=\"{user}\" password=\"{pass}\";",
    )
}

fn scram_jaas(user: &str, pass: &str) -> String {
    format!(
        "org.apache.kafka.common.security.scram.ScramLoginModule required \
         username=\"{user}\" password=\"{pass}\";",
    )
}

async fn start_sasl_plaintext_broker(
    users: &[(&str, &str)],
) -> (crabka_broker::BrokerHandle, tempfile::TempDir) {
    // Similar to `start_host_broker` but with SASL_PLAINTEXT listener +
    // plain_credentials populated from `users`.
    // (See existing `start_host_broker` body for the pattern; this is a
    // small variant.)
}
```

- [ ] **Step 2: Write the test**

```rust
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "requires Docker"]
async fn jvm_sasl_plain_produce_consume() {
    const TOPIC: &str = "crabka-sasl-plain-itest";
    const USER: &str = "alice";
    const PASS: &str = "wonderland";

    let (_broker, _dir) = start_sasl_plaintext_broker(&[(USER, PASS)]).await;
    nc_check_connectivity();

    // Write a tmp file with the client properties for the JVM tools.
    let props = format!(
        "security.protocol=SASL_PLAINTEXT\n\
         sasl.mechanism=PLAIN\n\
         sasl.jaas.config={}\n",
        plain_jaas(USER, PASS),
    );
    let tmp = tempfile::NamedTempFile::new().expect("tmp");
    std::fs::write(tmp.path(), &props).expect("write props");
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        std::fs::set_permissions(tmp.path(), std::fs::Permissions::from_mode(0o644))
            .expect("chmod props");
    }
    let mount = format!("{}:/client.properties:ro", tmp.path().display());

    // Create topic
    docker_run_kafka_tool_with_mount(&mount, &[
        "kafka-topics", "--create", "--if-not-exists",
        "--topic", TOPIC,
        "--partitions", "1", "--replication-factor", "1",
        "--bootstrap-server", BOOTSTRAP,
        "--command-config", "/client.properties",
    ]);

    // Produce 10 records
    let prod = std::process::Command::new("docker")
        .args(["run", "--rm", "-i",
            "-v", &mount,
            "--add-host=host.docker.internal:host-gateway",
            KAFKA_IMAGE,
            "kafka-console-producer",
            "--bootstrap-server", BOOTSTRAP,
            "--topic", TOPIC,
            "--producer.config", "/client.properties",
        ])
        .stdin(std::process::Stdio::piped())
        .stderr(std::process::Stdio::piped())
        .spawn().expect("spawn producer");
    {
        use std::io::Write;
        let stdin = prod.stdin.as_ref().expect("stdin");
        // ... write 10 lines, drop stdin, wait ...
    }
    // Consume + assert 10 lines
    // ... mirror existing produce/consume helpers ...
}

fn docker_run_kafka_tool_with_mount(mount: &str, args: &[&str]) -> std::process::Output {
    let out = std::process::Command::new("docker")
        .arg("run").arg("--rm")
        .arg("-v").arg(mount)
        .arg("--add-host=host.docker.internal:host-gateway")
        .arg(KAFKA_IMAGE)
        .args(args)
        .output()
        .expect("spawn docker run");
    assert!(out.status.success(),
        "docker run failed: stdout={} stderr={}",
        String::from_utf8_lossy(&out.stdout),
        String::from_utf8_lossy(&out.stderr));
    out
}
```

- [ ] **Step 3: Run in WSL**

```bash
wsl bash -c "cd /mnt/c/Users/Matt\ Stone/git/crabka && cargo test -p crabka-broker --test jvm_acceptance jvm_sasl_plain -- --ignored --nocapture --test-threads=1"
```

Expected: PASS.

- [ ] **Step 4: Commit**

```bash
git add crates/broker/tests/jvm_acceptance.rs
git commit -m "test(jvm): SASL_PLAINTEXT + PLAIN produce/consume"
```

---

### Task 21: JVM SASL/SCRAM-SHA-512 produce/consume

**Files:**
- Modify: `crates/broker/tests/jvm_acceptance.rs`

- [ ] **Step 1: Add the test**

```rust
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "requires Docker"]
async fn jvm_sasl_scram_sha512_produce_consume() {
    const TOPIC: &str = "crabka-sasl-scram-itest";
    const ADMIN: &str = "admin";
    const ADMIN_PASS: &str = "admin-secret";
    const ALICE: &str = "alice";
    const ALICE_PASS: &str = "alice-secret";

    // Broker config: super_user = admin (provisioned via plain_credentials),
    // SASL_PLAINTEXT listener, enabled mechanisms = [PLAIN, SCRAM-SHA-512].
    let (broker, _dir) = start_dual_mech_broker(ADMIN, ADMIN_PASS).await;
    nc_check_connectivity();

    // Step A: Auth as admin via PLAIN, provision alice via
    //   `kafka-configs --alter --entity-type users --entity-name alice
    //    --add-config 'SCRAM-SHA-512=[password=alice-secret]'`
    // This translates to api_key 51 in cp-kafka:7.5.0+; cp-kafka:6.1.1
    // uses IncrementalAlterConfigs USER entity which we don't implement.
    // So this test requires KAFKA_IMAGE_TXN (cp-kafka:7.5.0).
    let admin_props = write_client_props(&format!(
        "security.protocol=SASL_PLAINTEXT\n\
         sasl.mechanism=PLAIN\n\
         sasl.jaas.config={}\n",
        plain_jaas(ADMIN, ADMIN_PASS),
    ));
    docker_run_kafka_tool_with_image_and_mount(
        KAFKA_IMAGE_TXN,
        &admin_props.mount(),
        &[
            "kafka-configs", "--alter",
            "--entity-type", "users", "--entity-name", ALICE,
            "--add-config", &format!("SCRAM-SHA-512=[password={ALICE_PASS}]"),
            "--bootstrap-server", BOOTSTRAP,
            "--command-config", "/client.properties",
        ],
    );

    // Step B: Produce as alice via SCRAM-SHA-512
    let alice_props = write_client_props(&format!(
        "security.protocol=SASL_PLAINTEXT\n\
         sasl.mechanism=SCRAM-SHA-512\n\
         sasl.jaas.config={}\n",
        scram_jaas(ALICE, ALICE_PASS),
    ));
    docker_run_kafka_tool_with_image_and_mount(
        KAFKA_IMAGE_TXN,
        &alice_props.mount(),
        &[
            "kafka-topics", "--create", "--if-not-exists",
            "--topic", TOPIC, "--partitions", "1", "--replication-factor", "1",
            "--bootstrap-server", BOOTSTRAP,
            "--command-config", "/client.properties",
        ],
    );

    // Produce + consume, mirror task 20.
}
```

`write_client_props` writes the file + chmods 0644 + returns an object whose `.mount()` returns the `-v` string.

- [ ] **Step 2: Run + commit**

```bash
wsl bash -c "cd /mnt/c/Users/Matt\ Stone/git/crabka && cargo test -p crabka-broker --test jvm_acceptance jvm_sasl_scram -- --ignored --nocapture --test-threads=1"
git add crates/broker/tests/jvm_acceptance.rs
git commit -m "test(jvm): SASL_PLAINTEXT + SCRAM-SHA-512 produce/consume"
```

---

### Task 22: JVM TLS handshake

**Files:**
- Modify: `crates/broker/tests/jvm_acceptance.rs`

- [ ] **Step 1: Add the test**

```rust
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "requires Docker"]
async fn jvm_ssl_handshake_succeeds() {
    // Start broker with SSL listener (no SASL) using dev cert/key.
    let (_broker, _dir) = start_ssl_broker().await;
    nc_check_connectivity();

    // Build a JKS truststore from our PEM cert (use openssl + keytool inside
    // a one-shot cp-kafka container or precompute).
    let truststore_path = prepare_jks_truststore();

    // Run kafka-broker-api-versions with SSL config.
    let props = format!(
        "security.protocol=SSL\n\
         ssl.truststore.location=/truststore.jks\n\
         ssl.truststore.password=changeit\n\
         ssl.endpoint.identification.algorithm=\n",  // disable hostname verify for the dev cert
    );
    let props_tmp = write_client_props(&props);
    let ts_mount = format!("{}:/truststore.jks:ro", truststore_path.display());

    let out = std::process::Command::new("docker")
        .args([
            "run", "--rm",
            "-v", &props_tmp.mount_str(),
            "-v", &ts_mount,
            "--add-host=host.docker.internal:host-gateway",
            KAFKA_IMAGE,
            "kafka-broker-api-versions",
            "--bootstrap-server", BOOTSTRAP,
            "--command-config", "/client.properties",
        ])
        .output().expect("spawn");
    assert!(out.status.success(),
        "ssl handshake failed: stdout={} stderr={}",
        String::from_utf8_lossy(&out.stdout),
        String::from_utf8_lossy(&out.stderr));
}

fn prepare_jks_truststore() -> std::path::PathBuf {
    // openssl + keytool inside a one-shot docker run that takes
    // crates/security/tests/fixtures/dev_cert.pem and emits /tmp/ts.jks.
    todo!("write the keytool pipeline; result must be a JKS truststore on host fs")
}
```

The `prepare_jks_truststore` body is the trickiest piece. Approach:

```bash
docker run --rm -v <fixtures>:/in -v <host_tmp>:/out openjdk:17 \
  bash -c "keytool -import -alias crabka -file /in/dev_cert.pem -keystore /out/ts.jks -storepass changeit -noprompt"
```

- [ ] **Step 2: Run + commit**

```bash
wsl bash -c "cd /mnt/c/Users/Matt\ Stone/git/crabka && cargo test -p crabka-broker --test jvm_acceptance jvm_ssl -- --ignored --nocapture --test-threads=1"
git add crates/broker/tests/jvm_acceptance.rs
git commit -m "test(jvm): SSL-only listener TLS handshake"
```

---

### Task 23: JVM SASL_SSL + inter-broker replication

**Files:**
- Modify: `crates/broker/tests/jvm_acceptance.rs`

- [ ] **Step 1: Add the SASL_SSL test**

```rust
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "requires Docker"]
async fn jvm_sasl_ssl_full_stack() {
    const TOPIC: &str = "crabka-sasl-ssl-itest";
    const ADMIN: &str = "admin";
    const ADMIN_PASS: &str = "admin-secret";
    const ALICE: &str = "alice";
    const ALICE_PASS: &str = "alice-secret";

    let (broker, _dir) = start_sasl_ssl_broker(ADMIN, ADMIN_PASS).await;
    let truststore = prepare_jks_truststore();
    nc_check_connectivity();

    // Provision alice
    let admin_props = write_client_props(&format!(
        "security.protocol=SASL_SSL\n\
         sasl.mechanism=PLAIN\n\
         sasl.jaas.config={}\n\
         ssl.truststore.location=/truststore.jks\n\
         ssl.truststore.password=changeit\n\
         ssl.endpoint.identification.algorithm=\n",
        plain_jaas(ADMIN, ADMIN_PASS),
    ));
    docker_run_kafka_tool_with_image_and_mounts(
        KAFKA_IMAGE_TXN,
        &[&admin_props.mount_str(), &format!("{}:/truststore.jks:ro", truststore.display())],
        &[
            "kafka-configs", "--alter",
            "--entity-type", "users", "--entity-name", ALICE,
            "--add-config", &format!("SCRAM-SHA-512=[password={ALICE_PASS}]"),
            "--bootstrap-server", BOOTSTRAP,
            "--command-config", "/client.properties",
        ],
    );

    // Produce as alice via SASL_SSL + SCRAM
    // ... mirror task 21 with SASL_SSL settings ...
}
```

- [ ] **Step 2: Add the inter-broker replication test**

```rust
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "requires Docker"]
async fn jvm_inter_broker_replication_authed() {
    // Spin up two brokers (in-process) both configured with SASL_PLAINTEXT
    // for inter-broker, with admin/admin-secret credentials and rf=2 topic.
    // Produce 50 records via JVM kafka-console-producer authed as admin.
    // Kill broker 0 (the leader). Wait for failover.
    // Re-bootstrap to broker 1, consume, assert 50 records.

    // Most of this scaffolding exists in slice-10b's failover test; this is
    // the same minus PLAINTEXT, plus SASL config on each broker.
}
```

- [ ] **Step 3: Run + commit**

```bash
wsl bash -c "cd /mnt/c/Users/Matt\ Stone/git/crabka && cargo test -p crabka-broker --test jvm_acceptance jvm_sasl_ssl jvm_inter_broker -- --ignored --nocapture --test-threads=1"
git add crates/broker/tests/jvm_acceptance.rs
git commit -m "test(jvm): SASL_SSL full stack + inter-broker SASL replication"
```

---

## Batch 10 — Final acceptance sweep

### Task 24: Final acceptance sweep + docs + PR

**Files:**
- Modify: `README.md`
- Modify: `STATUS.md`

- [ ] **Step 1: Run the full workspace test suite**

```bash
cargo build --workspace
cargo test --workspace --exclude crabka-client-core --exclude crabka-log --exclude crabka-broker -- --include-ignored
cargo test -p crabka-broker --lib
cargo test -p crabka-broker --test auth_handlers
cargo fmt --check
cargo clippy --workspace --all-targets -- -D warnings
```

Expected: all green.

- [ ] **Step 2: Run JVM acceptance in WSL**

```bash
wsl bash -c "cd /mnt/c/Users/Matt\ Stone/git/crabka && cargo test -p crabka-broker --test jvm_acceptance -- --ignored --nocapture --test-threads=1"
```

Expected: all green (existing pre-slice-12 tests + 4 new ones = ~18 total).

- [ ] **Step 3: Update `README.md`**

Append a new bullet to the "Slices delivered" list:

```markdown
- **Slice 12** — auth & security: TLS via rustls; SASL/PLAIN +
  SASL/SCRAM-SHA-512 client auth; per-listener protocol multiplexing
  (PLAINTEXT/SSL/SASL_PLAINTEXT/SASL_SSL); inter-broker auth (TLS +
  SASL); KIP-554 `AlterUserScramCredentials`; `crabka format
  --add-scram` bootstrap CLI. JVM clients connect over `SASL_SSL`.
```

- [ ] **Step 4: Add a Slice 12 section to `STATUS.md`**

Append:

```markdown
## Slice 12 — auth & security (2026-05-14)

- 2 new crates: `crabka-security` (pure-logic SCRAM/PLAIN/TLS), `crabka-cli`
  (`crabka format --add-scram`).
- 3 new wire handlers: `SaslHandshake` (17), `SaslAuthenticate` (36),
  `AlterUserScramCredentials` (51, KIP-554).
- 2 new metadata records: `V1ScramCredential`, `V1DeleteScramCredential`.
- Per-listener accept loops with TLS termination via `tokio_rustls`.
- Per-connection `ConnectionAuth` state machine; pre-auth allowlist gate.
- `InterBrokerClient` runs TLS + outbound SASL for replication, raft, and
  controller-heartbeat traffic.
- 4 new JVM acceptance tests: SASL/PLAIN, SASL/SCRAM-SHA-512, SSL-only,
  SASL_SSL + inter-broker replication.
- Out of scope: ACLs, delegation tokens, OAUTHBEARER, GSSAPI,
  SCRAM-SHA-256, mTLS client-auth, quotas.
```

- [ ] **Step 5: Commit docs**

```bash
git add README.md STATUS.md
git commit -m "docs(slice-12): README + STATUS entry"
```

- [ ] **Step 6: Push + open PR**

```bash
git push -u origin feature/auth-security-12
gh pr create --title "Slice 12: Auth & security (TLS + SASL/PLAIN + SCRAM-SHA-512)" \
  --body "$(cat <<'EOF'
## Summary

- TLS (rustls) per-listener; multi-listener config (PLAINTEXT/SSL/SASL_PLAINTEXT/SASL_SSL)
- SASL/PLAIN + SASL/SCRAM-SHA-512 client auth via SaslHandshake (17) + SaslAuthenticate (36)
- Inter-broker auth (TLS + SASL) on replication, raft, controller heartbeat
- KIP-554 AlterUserScramCredentials (51) for dynamic SCRAM provisioning
- New `crabka-security` crate (pure-logic primitives, shared with CLI)
- New `crabka-cli` crate with `crabka format --add-scram` bootstrap
- V1ScramCredential + V1DeleteScramCredential metadata records
- Per-listener BrokerEndpoint plumbing through MetadataImage + Metadata response

## Out of scope

ACLs (only a super-user-name stand-in), delegation tokens, OAUTHBEARER, GSSAPI,
SCRAM-SHA-256, mTLS client-auth, quotas.

## Test plan

- [x] cargo fmt --check
- [x] cargo clippy --workspace --all-targets -D warnings
- [x] cargo test --workspace (no Docker)
- [x] WSL JVM acceptance: SASL/PLAIN, SASL/SCRAM-SHA-512, SSL-only, SASL_SSL inter-broker

🤖 Generated with [Claude Code](https://claude.com/claude-code)
EOF
)"
```

- [ ] **Step 7: Confirm CI passes**

Wait for the CI run on GitHub. Address any Linux-specific failures (most likely candidates: `openssl` not on PATH for fixture generation — generate fixtures in a build script or pre-commit them; clippy strictness differences).

---

## Notes for the executing agent

1. **Branch:** all work is on `feature/auth-security-12`. Do NOT push to main.
2. **Subagent dispatch boundary:** every task is a self-contained dispatch unit. Run tests at the end of each task before commit; if a test fails, fix it within the same task (don't carry failures into the next task).
3. **Backward compatibility:** every behavioral change must preserve existing PLAINTEXT-only tests. The `effective_listeners()` shim (task 7) is the load-bearing piece — never remove the empty-listeners→synthesize-PLAINTEXT path.
4. **Crypto carefulness:** the SCRAM state machine has subtle bugs to avoid: forgetting the GS2 header `"n,,"`, using base64 vs raw bytes inconsistently, mixing up `client_key` vs `client_signature`. Stick to the RFC 5802 names verbatim in code comments to keep yourself oriented.
5. **`todo!()` is a plan failure:** every `todo!()` in this plan must be replaced with a real body before commit. If you can't fill one in, stop and ask before continuing.
6. **JVM tests on Linux CI:** if you write any tempfile and mount it into a container, chmod 0644 (slice-11 ran into this — `tempfile::NamedTempFile` is 0600 by default on Linux).
7. **JAAS string escaping:** Java JAAS configs end with `;` and use `=` for attribute separators. Quote values with `"..."`. Test with the actual JVM tool to catch quoting bugs early.
