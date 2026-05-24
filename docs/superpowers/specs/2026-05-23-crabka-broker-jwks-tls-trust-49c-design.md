# Slice 49c — Broker: Custom TLS trust to IdP for JWKS

Status: Draft
Date: 2026-05-23
Slice: 49c
Pairs with operator slice(s): 50b (deferred; will land separately)
Umbrella: [OAUTHBEARER full-parity roadmap](2026-05-23-crabka-oauth-parity-roadmap-design.md)

## Goal

When the JWKS endpoint runs on a host whose server certificate is not
signed by a public webpki root — the common case in private clusters
that use an internal CA — the broker today fails to fetch the key set
because slice 49b's reqwest client trusts only webpki-roots. Slice 49c
adds an opt-in broker config knob that supplies a PEM bundle of CA
certificates to use as the *exclusive* trust store for the JWKS HTTPS
connection (Strimzi-shaped "replace, don't extend" semantic).

This unblocks slice 50b (operator `tlsTrustedCertificates` CRD field on
the listener OAuth config) and turns the slice 50 Keycloak kind e2e
from HTTP-only into a realistic HTTPS-to-IdP test.

## Deliverables

1. **New helper** in `crates/security`: `build_client_config_from_pem`
   that returns a `rustls::ClientConfig` whose `RootCertStore` is loaded
   exclusively from a user-supplied PEM file.
2. **New TOML key** `[oauthbearer].jwks_tls_trust` (a single `PathBuf`)
   in `crates/broker/src/file_config.rs::FileOAuthBearerConfig`.
3. **New runtime field** `BrokerConfig.oauthbearer_jwks_tls_trust:
   Option<PathBuf>`.
4. **`JwksRefresher::run` updated** to thread the path through, build
   the reqwest client with `use_preconfigured_tls(ClientConfig)` when
   the path is set, and hard-stop the refresher on PEM-load failure
   (mirrors the existing reqwest-builder-failure hard stop).
5. **Tests** as listed under "Test plan" below.

No CRD changes, no operator changes, no wire-protocol changes. Single
slice, broker-only.

## Non-deliverables (out of scope)

| Item | Lands in |
|------|----------|
| Operator CRD field `tlsTrustedCertificates` on listener OAuth config | 50b |
| Operator Secret-mounting + path threading into broker TOML | 50b |
| Custom TLS trust for opaque-token introspection (49d) HTTPS | reuses this helper, but the config key is `introspection_tls_trust` and lands in 49d |
| Hot reload of the trust bundle without broker restart | future, if real demand |
| Multiple PEM paths in one TOML key | operator concatenates; broker reads one file |
| Pinning the IdP cert SHA / public-key fingerprint | explicit non-goal — rustls chain verification only |
| mTLS *to* the IdP (broker authenticating itself) | not in any roadmap slice |
| `jwks_tls_trust` set without `jwks_endpoint_uri` rejected at config-load | silently no-op (matches Strimzi permissive posture during operator rollout) |

## Trust semantic — replace, not extend

When `[oauthbearer].jwks_tls_trust` is set, the rustls `ClientConfig`
used by the JWKS refresher trusts **only** the certificates in that PEM
bundle. The default webpki-roots are not consulted. This matches
Strimzi's `tlsTrustedCertificates` semantic (which sets the JVM
`ssl.truststore.location` — the JVM treats that as a full replacement).

When `[oauthbearer].jwks_tls_trust` is unset, the refresher continues
to use reqwest's default rustls feature, which trusts webpki-roots —
exactly slice 49b's behavior. No regression.

The trade-off accepted: ops teams pointing the broker at a public IdP
that uses a Let's Encrypt cert and *also* set `jwks_tls_trust` for an
unrelated private CA must concatenate LE roots into the PEM. The
alternative (additive: webpki + user) was rejected for Strimzi-parity.

## Code surface

### `crates/security/src/jwks_trust.rs` (new)

```rust
//! Build a rustls `ClientConfig` whose trust roots are exclusively the
//! certificates in a user-supplied PEM bundle. Slice 49c: backs the
//! broker's outbound HTTPS to the JWKS endpoint when the operator
//! configures a private IdP CA (Strimzi-shaped tlsTrustedCertificates
//! "replace" semantic).

use std::path::Path;
use std::sync::Arc;
use rustls::pki_types::CertificateDer;
use rustls::pki_types::pem::PemObject;

#[derive(Debug, thiserror::Error)]
pub enum JwksTrustError {
    #[error("read {0}: {1}")]
    Io(std::path::PathBuf, std::io::Error),
    #[error("no certificates in {0}")]
    Empty(std::path::PathBuf),
    #[error("rustls add cert: {0}")]
    Rustls(#[from] rustls::Error),
}

pub fn build_client_config_from_pem(
    path: &Path,
) -> Result<Arc<rustls::ClientConfig>, JwksTrustError> {
    let certs: Vec<CertificateDer<'static>> = CertificateDer::pem_file_iter(path)
        .map_err(|e| JwksTrustError::Io(path.into(), std::io::Error::other(e.to_string())))?
        .collect::<Result<Vec<_>, _>>()
        .map_err(|e| JwksTrustError::Io(path.into(), std::io::Error::other(e.to_string())))?;
    if certs.is_empty() {
        return Err(JwksTrustError::Empty(path.into()));
    }
    let mut roots = rustls::RootCertStore::empty();
    for cert in certs {
        roots.add(cert)?;
    }
    Ok(Arc::new(
        rustls::ClientConfig::builder()
            .with_root_certificates(roots)
            .with_no_client_auth(),
    ))
}
```

Re-exported from `crates/security/src/lib.rs` as
`pub use jwks_trust::{build_client_config_from_pem, JwksTrustError};`.

A new file rather than extending `crates/security/src/tls.rs` (which
covers inter-broker mTLS) keeps each file single-purpose. The helper is
generic over "PEM path → ClientConfig" — slice 49d's introspection
client will call the same function with its own config key.

### `crates/broker/src/file_config.rs`

```rust
#[derive(Debug, Clone, Default, Deserialize, PartialEq)]
pub struct FileOAuthBearerConfig {
    // ...existing fields unchanged...

    /// Slice 49c: PEM file containing the CA certificate(s) used to
    /// verify the JWKS endpoint's TLS certificate when
    /// `jwks_endpoint_uri` is an `https://` URL. When set, these are
    /// the *only* trust roots used for the JWKS fetch (replaces the
    /// default webpki-roots — Strimzi-shaped). When unset, the
    /// refresher uses reqwest's default rustls webpki-roots. Inert if
    /// `jwks_endpoint_uri` is unset.
    #[serde(default)]
    pub jwks_tls_trust: Option<std::path::PathBuf>,
}
```

`FileOAuthBearerConfig::apply_to` threads the value to
`BrokerConfig.oauthbearer_jwks_tls_trust`.

### `crates/broker/src/config.rs` (or wherever `BrokerConfig` lives)

```rust
pub struct BrokerConfig {
    // ...existing fields...
    /// Slice 49c: optional PEM path for the JWKS HTTPS trust store.
    /// `None` → reqwest's default webpki-roots.
    pub oauthbearer_jwks_tls_trust: Option<std::path::PathBuf>,
}
```

Default value: `None`.

### `crates/broker/src/oauth_jwks.rs`

```rust
pub(crate) struct JwksRefresher {
    pub endpoint: String,
    pub handle: JwksHandle,
    pub interval: Duration,
    pub shutdown: CancellationToken,
    /// Slice 49c: optional PEM path; replaces webpki-roots when Some.
    pub tls_trust: Option<std::path::PathBuf>,
}

impl JwksRefresher {
    pub(crate) async fn run(self) {
        let mut builder = reqwest::Client::builder().timeout(Duration::from_secs(10));
        if let Some(path) = &self.tls_trust {
            match crabka_security::build_client_config_from_pem(path) {
                Ok(cfg) => {
                    builder = builder.use_preconfigured_tls((*cfg).clone());
                }
                Err(e) => {
                    tracing::error!(
                        error = %e,
                        path = %path.display(),
                        "failed to load OAUTHBEARER JWKS TLS trust bundle; refresher will not start",
                    );
                    return;
                }
            }
        }
        let client = match builder.build() { /* existing handling */ };
        // ...existing tick loop unchanged...
    }
}
```

The `(*cfg).clone()` strips the outer `Arc` because reqwest's
`use_preconfigured_tls` takes a `rustls::ClientConfig` by value (it
wraps it in its own `Arc` internally). A clone of the rustls config is
cheap — it's a small struct of `Arc`s around the actual trust roots.

### `crates/broker/src/broker.rs`

In `Broker::start`, where the refresher is constructed (currently
around line 1238), pass the new field:

```rust
let refresher = crate::oauth_jwks::JwksRefresher {
    endpoint,
    handle,
    interval: config.oauthbearer_jwks_refresh_interval,
    shutdown: supervisor_shutdown.child_token(),
    tls_trust: config.oauthbearer_jwks_tls_trust.clone(),
};
tokio::spawn(refresher.run());
```

## File-level change map

| File | Change |
|------|--------|
| `crates/security/src/jwks_trust.rs` | New file (helper + 5 unit tests) |
| `crates/security/src/lib.rs` | `mod jwks_trust;` + re-exports |
| `crates/broker/src/file_config.rs` | Add `jwks_tls_trust` field + apply_to threading + 1 test |
| `crates/broker/src/config.rs` | Add `oauthbearer_jwks_tls_trust` runtime field |
| `crates/broker/src/oauth_jwks.rs` | Add `tls_trust` field, wire reqwest builder, 2 new HTTPS integration tests |
| `crates/broker/src/broker.rs` | Thread `tls_trust` into `JwksRefresher` literal (1 line) |

All changes are file-disjoint enough to run as a single batch (file_config + oauth_jwks share a crate but not a function), but the dependency graph is sequential (`security` → `file_config` → `oauth_jwks` for the type to flow through). One implementer, three commits suggested below.

## Test plan

### Unit tests in `crates/security/src/jwks_trust.rs`

- `build_client_config_from_pem_loads_single_cert` — write a self-signed PEM to a tempfile, call helper, assert `Ok(_)`.
- `build_client_config_from_pem_loads_concatenated_chain` — PEM with two `-----BEGIN CERTIFICATE-----` blocks; assert both end up in the trust store (verify by attempting to verify a leaf signed by each, or by checking `RootCertStore.len() == 2` via whichever API rustls exposes).
- `build_client_config_from_pem_rejects_missing_file` — nonexistent path, assert `Err(JwksTrustError::Io(_, _))`.
- `build_client_config_from_pem_rejects_empty_pem` — file exists but has no `BEGIN CERTIFICATE`, assert `Err(JwksTrustError::Empty(_))`.
- `build_client_config_from_pem_rejects_non_pem_garbage` — file with arbitrary bytes; expect `Err` (likely `Io` from PEM parser, or `Empty` if the parser returns zero certs without erroring — adjust assertion to whichever rustls-pki-types does).

Reuse the existing test fixture pattern (`crates/security/tests/fixtures/dev_cert.pem` exists; either reuse or write a fresh one for these tests).

### Unit test in `crates/broker/src/file_config.rs`

- `apply_to_oauthbearer_threads_jwks_tls_trust_to_broker_config` — TOML with `[oauthbearer]` + `jwks_tls_trust = "/some/path.pem"`; assert `BrokerConfig.oauthbearer_jwks_tls_trust == Some(PathBuf::from("/some/path.pem"))`.

### Integration tests in `crates/broker/src/oauth_jwks.rs`

- `refresher_fetches_jwks_over_https_with_custom_trust` — spin up an HTTPS axum server with a self-signed cert (use the existing dev cert + key fixtures from `crates/security/tests/fixtures/` or generate fresh via `rcgen`). Point a `JwksRefresher` at it with `tls_trust = Some(<dev-ca-pem>)`. Assert the handle gets populated.
- `refresher_https_fetch_fails_when_custom_trust_doesnt_match_server_cert` — same HTTPS server, but `tls_trust = Some(<different-ca-pem>)`. Assert the handle stays empty (the warning-log-and-continue path fires).

The existing tests (`fetch_jwks_parses_served_keyset`, `fetch_jwks_errors_on_dead_endpoint`, `refresher_populates_handle_then_stops_on_shutdown`) continue to use plain HTTP and are unchanged — they exercise the `tls_trust: None` path.

If `axum-server` or the equivalent rustls-on-axum harness isn't already a dev-dep of the broker crate, this slice adds it as a `[dev-dependencies]` entry. (Check first; the existing serve_jwks pattern uses plain `axum`. The HTTPS variant needs `axum-server` or a hand-rolled `tokio-rustls` accept loop. Either is acceptable; the implementation plan picks one.)

### What is *not* tested here

- Operator-side rendering of `jwks_tls_trust` from a Secret mount — slice 50b.
- Kind-cluster end-to-end Keycloak with HTTPS — slice 50b adds the e2e job that exercises the full path.
- Hot reload of the trust bundle — explicit non-goal.

## Acceptance criteria

1. `cargo build -p crabka-security -p crabka-broker` succeeds.
2. `cargo test -p crabka-security -p crabka-broker` passes.
3. `cargo fmt --check` and `cargo clippy --workspace --all-targets -- -D warnings` pass.
4. New unit tests in `crates/security` and `crates/broker/src/file_config.rs` all pass.
5. New HTTPS integration tests in `crates/broker/src/oauth_jwks.rs` pass.
6. No regression in the existing OAUTHBEARER unit tests, the slice-49b `auth_handlers` integration tests, or any other workspace test.
7. STATUS.md updated with a `## Slice 49c` entry.

## Open questions resolved during brainstorming

- **Trust semantic.** Replace (Strimzi-parity), not additive. Documented in the field doc.
- **Single path vs. list.** Single path; operator concatenates. Matches `client_ca_path` shape elsewhere in the broker.
- **Hot reload.** Out of scope. CA rotations are years-cadence; a broker restart is acceptable.
- **Cross-check with `jwks_endpoint_uri`.** Silently no-op if set without the endpoint; no config-load error.
- **Helper placement.** New file `crates/security/src/jwks_trust.rs`, not an extension of `tls.rs` (which serves inter-broker mTLS).
