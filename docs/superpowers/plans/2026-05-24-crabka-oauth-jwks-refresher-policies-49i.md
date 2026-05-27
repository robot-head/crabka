# Slice 49i — OAUTHBEARER JWKS refresher policies Implementation Plan

## Implementation status

**Slice tracked in STATUS.md as:** ## Slice 49i — Operator + Broker: OAUTHBEARER JWKS refresher policies (2026-05-24)

**Incomplete / deferred steps (out-of-scope follow-ups):**

- Per-listener JWKS refreshers (broker still has one global [oauthbearer] block)
- Reconcile-time validation against the actual IdP (operator just renders)
- Slice 49f (PLAIN-with-OAuth-token) — indefinitely skipped

---

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Final OAUTHBEARER umbrella slice. Add Strimzi's `jwksMinRefreshPauseSeconds` (on-demand JWKS refresh rate-limit), `jwksExpirySeconds` (hard cache expiry — fails closed when IdP unreachable), and `jwksIgnoreKeyUse` (filter toggle for JWKS `use=enc` keys) on the broker + operator surfaces.

**Architecture:** Six tasks across four batches (mirrors slice 49g/49h). T1 reworks the slice-49b JWKS refresher loop (adds `tokio::select!` over periodic-tick + new signal channel, expiry tracking, parse-filter toggle), extends `SignedJwsValidator` with expiry-check + signal-on-verify-failure logic, threads 3 new fields through `FileOAuthBearerConfig.apply_to` (including the signal channel wiring from `apply_to` → `Broker::start`). T2 + T3 each touch one operator file (CRD + reconciler) with new cross-mode validation rejecting the 3 JWT-only fields on introspection-mode listeners. T4 + T5 file-disjoint parallel (operator integration tests + sample + CRD regen ‖ kind-oauth e2e — introspection job NOT touched). T6 ships STATUS + final gate + OAUTHBEARER umbrella completion note.

**Tech Stack:** Rust, tokio (`mpsc` for fire-and-forget signal, `select!` for refresher loop, `time::pause/advance` for tests), arc-swap (existing JwksHandle pattern), jsonpath-rust (already in workspace from 49g — unrelated to this slice).

**Spec:** `docs/superpowers/specs/2026-05-24-crabka-oauth-jwks-refresher-policies-49i-design.md` (commit `9ddb559`).

**Worktree:** `/Users/mattstone/git/crabka/.worktrees/slice-49i-oauth-jwks-policies` on branch `slice-49i-oauth-jwks-policies`. Verify with `git branch --show-current`. Commit with `-c user.name="Matt Stone" -c user.email="matthew.d.stone@gmail.com"`.

---

## File structure

| File | Responsibility | Touched by |
|---|---|---|
| `crates/security/src/jwks.rs` | `Jwks::from_json` gains `ignore_key_use` parameter (filter `use=enc` only when false); `JwksHandle` gains `last_successful_fetch_ms` + `signal_tx` fields + accessor methods | T1 |
| `crates/broker/src/oauth_jwks.rs` | `JwksRefresher` gains `last_successful_fetch`, `last_on_demand_refresh`, `min_on_demand_pause`, `signal_rx`, `ignore_key_use` fields; `run()` loop becomes 3-arm `tokio::select!` (periodic / signal / cancel); unit tests for rate-limit + expiry-tracking | T1 |
| `crates/security/src/oauthbearer.rs` | `SignedJwsValidator.expiry_ms` field; `validate()` body cache-expiry check + signal-on-verify-failure; unit tests | T1 |
| `crates/broker/src/file_config.rs` | 3 new `FileOAuthBearerConfig` fields; `apply_to` creates the signal mpsc pair, threads `signal_tx` into validator's `JwksHandle`, returns `signal_rx` for refresher (via new `BrokerConfig` carry-field or apply_to return shape — see Step 12) | T1 |
| `crates/broker/src/config.rs` | New `BrokerConfig.oauthbearer_jwks_signal_rx: Option<Mutex<Option<Receiver<()>>>>` carry field for the refresher's signal channel | T1 |
| `crates/broker/src/broker.rs` (or wherever `Broker::start` builds the refresher) | Pass the carried `signal_rx` + new policy fields into `JwksRefresher::new` | T1 |
| `crates/operator/src/crd/listener.rs` | 3 new `ListenerAuthenticationOAuth` fields + hand-rolled schema + own-file fixture sweep + round-trip tests | T2 |
| `crates/operator/src/controller/listeners.rs` | render_broker_toml emits 3 new keys; `ValidationError::ListenerOauthJwksFieldsRejectedInIntrospectionMode` variant + check; divergence walk extension; own-file + sibling-file fixture sweep + unit tests | T3 |
| `crates/operator/src/controller/kafka.rs` + `kafka_node_pool.rs` | Fixture sweep (4 + 3 sites) | T3 |
| `crates/operator/tests/reconcile_*.rs` | Fixture sweep (5 + 2 + 1 sites) + 2 new integration tests | T4 |
| `crates/operator/sample/oauth-listener.yaml` | Commented-out hint lines for the 3 new fields | T4 |
| `deploy/crds/crabka.io_kafkas.yaml` | Regenerated CRD | T4 |
| `.github/workflows/operator-e2e.yml` | `kind-oauth` job only (introspection NOT touched per cross-mode validator rejection) | T5 |
| `STATUS.md` | Slice 49i entry + OAUTHBEARER umbrella completion note | T6 |

---

## Batches

### Batch 1 — T1 (broker, alone, large)

#### Task T1: JWKS refresher rework + validator integration + FileOAuthBearerConfig wiring

**Files:**
- Modify: `crates/security/src/jwks.rs`
- Modify: `crates/broker/src/oauth_jwks.rs`
- Modify: `crates/security/src/oauthbearer.rs`
- Modify: `crates/broker/src/file_config.rs`
- Modify: `crates/broker/src/config.rs`
- Modify: `crates/broker/src/broker.rs` (or wherever `Broker::start` constructs `JwksRefresher`)

**Context:** T1 is the largest task. Three coordinated changes:
1. `Jwks::from_json` + `JwksHandle` gain filter-toggle + signal-channel support.
2. `JwksRefresher` loop adds on-demand-refresh arm + cache-expiry timestamp tracking.
3. `SignedJwsValidator` adds cache-expiry pre-check + signal-on-verify-failure trigger.
4. `FileOAuthBearerConfig.apply_to` creates the signal mpsc pair + threads through to both ends.

- [ ] **Step 1: Find where `Broker::start` constructs `JwksRefresher`**

```bash
cd /Users/mattstone/git/crabka/.worktrees/slice-49i-oauth-jwks-policies
grep -rn "JwksRefresher\|jwks_refresher\|jwks.*spawn" crates/broker/src --include="*.rs"
```

Note the exact file + line where `JwksRefresher { ... }` or `JwksRefresher::new(...)` is constructed. This is where T1 step 12-13 will wire the signal_rx + the 3 new policy fields.

- [ ] **Step 2: Extend `JwksHandle` with timestamp + signal accessors**

Edit `crates/security/src/jwks.rs`. Replace the existing `JwksHandle` struct (lines 210-232):

```rust
/// A cheaply-clonable, atomically-swappable holder for the live [`Jwks`]
/// plus slice 49i fields for the refresher coordination.
#[derive(Debug, Clone)]
pub struct JwksHandle {
    keys: Arc<ArcSwap<Jwks>>,
    /// Slice 49i: epoch ms of last successful refresh. Validators
    /// check this against `expiry_ms` to fail closed on stale cache.
    /// 0 sentinel = never successfully fetched (initial state).
    last_successful_fetch_ms: Arc<std::sync::atomic::AtomicI64>,
    /// Slice 49i: fire-and-forget signal sender to the refresher.
    /// Validator calls `signal_refresh()` on verify failure (unknown
    /// kid or bad signature). `None` when the validator isn't paired
    /// with a refresher (e.g., default-constructed `JwksHandle` in
    /// non-signed validators or pre-`apply_to` state).
    signal_tx: Option<tokio::sync::mpsc::Sender<()>>,
}

impl JwksHandle {
    /// Wrap an initial key set with NO refresher coordination.
    /// Used by default constructors + the `apply_to` placeholder
    /// before the real handle is wired (T1 step 12 replaces this).
    #[must_use]
    pub fn new(jwks: Jwks) -> Self {
        Self {
            keys: Arc::new(ArcSwap::from_pointee(jwks)),
            last_successful_fetch_ms: Arc::new(std::sync::atomic::AtomicI64::new(0)),
            signal_tx: None,
        }
    }

    /// Slice 49i: wrap an initial key set WITH the timestamp counter
    /// + signal sender pre-wired. The refresher constructs its own
    /// `(signal_tx, signal_rx)` pair and passes `signal_tx` here; the
    /// refresher holds `signal_rx` and the shared
    /// `Arc<AtomicI64>` for timestamp updates.
    #[must_use]
    pub fn new_with_refresher_handles(
        jwks: Jwks,
        last_successful_fetch_ms: Arc<std::sync::atomic::AtomicI64>,
        signal_tx: tokio::sync::mpsc::Sender<()>,
    ) -> Self {
        Self {
            keys: Arc::new(ArcSwap::from_pointee(jwks)),
            last_successful_fetch_ms,
            signal_tx: Some(signal_tx),
        }
    }

    /// Atomically replace the key set — called by the refresher after
    /// a successful fetch. Lock-free.
    pub fn store(&self, jwks: Jwks) {
        self.keys.store(Arc::new(jwks));
    }

    /// Load the current key set. Cheap (an `Arc` clone).
    #[must_use]
    pub fn load(&self) -> Arc<Jwks> {
        self.keys.load_full()
    }

    /// Slice 49i: epoch-ms timestamp of last successful JWKS fetch.
    /// 0 if no fetch has succeeded yet. Validators compare against
    /// `now - expiry_ms` to enforce hard cache expiry.
    #[must_use]
    pub fn last_successful_fetch_ms(&self) -> i64 {
        self.last_successful_fetch_ms.load(std::sync::atomic::Ordering::Relaxed)
    }

    /// Slice 49i: fire-and-forget signal to the refresher that an
    /// on-demand refresh is requested (e.g., unknown-kid token).
    /// Non-blocking — drops silently if the channel is full (signals
    /// coalesce; one is enough). No-op when `signal_tx` is `None`.
    pub fn signal_refresh(&self) {
        if let Some(tx) = &self.signal_tx {
            let _ = tx.try_send(());
        }
    }
}

impl Default for JwksHandle {
    fn default() -> Self {
        Self::new(Jwks::default())
    }
}
```

- [ ] **Step 3: Update `Jwks::from_json` to take `ignore_key_use`**

Edit `crates/security/src/jwks.rs`. Find `Jwks::from_json` (it calls `parse_one_jwk` per the explore). The current `parse_one_jwk` hard-codes the `use=enc` filter at lines 155-157. Add an `ignore_key_use: bool` parameter:

```rust
impl Jwks {
    /// Parse a JWKS JSON document into a `Jwks` keyset.
    ///
    /// Slice 49i: when `ignore_key_use` is `false` (the default),
    /// keys with `use=enc` are filtered out (matches Strimzi and
    /// general JWS practice — encryption-only keys aren't suitable
    /// for signature verification). When `true`, all keys are kept
    /// regardless of `use` value.
    pub fn from_json(json: &str, ignore_key_use: bool) -> Result<Self, ParseError> {
        // existing JSON parsing ...
        for jwk in jwks_array {
            if let Some((kid, key)) = parse_one_jwk(jwk, ignore_key_use) {
                keys.insert(kid, key);
            }
        }
        // ... etc.
    }
}

fn parse_one_jwk(jwk: &Value, ignore_key_use: bool) -> Option<(String, JwkKey)> {
    // Slice 49i: filter use=enc keys unless ignore_key_use is true.
    // Keys with `use` absent are always kept (existing behavior).
    if !ignore_key_use && jwk.get("use").and_then(Value::as_str) == Some("enc") {
        return None;
    }
    // ... existing kid/kty/RSA/EC parsing unchanged ...
}
```

ALL callers of `Jwks::from_json` need updating. Grep:

```bash
grep -rn "Jwks::from_json\|jwks::from_json" crates/ --include="*.rs"
```

Each caller needs to either pass `false` (default behavior) or thread the actual config through.

- [ ] **Step 4: Write failing unit tests for the JWKS filter**

Add to `crates/security/src/jwks.rs` test module:

```rust
#[test]
fn parse_jwks_filters_use_enc_by_default() {
    // Two keys: one with use=sig (kept), one with use=enc (dropped).
    let json = r#"{
        "keys": [
            {"kty": "RSA", "kid": "sig-key", "use": "sig",
             "n": "0Z...", "e": "AQAB"},
            {"kty": "RSA", "kid": "enc-key", "use": "enc",
             "n": "0Y...", "e": "AQAB"}
        ]
    }"#;
    let jwks = Jwks::from_json(json, false).expect("parses");
    assert!(jwks.contains_kid("sig-key"));
    assert!(!jwks.contains_kid("enc-key"));
}

#[test]
fn parse_jwks_keeps_use_enc_when_ignore_key_use_true() {
    let json = r#"{
        "keys": [
            {"kty": "RSA", "kid": "sig-key", "use": "sig",
             "n": "0Z...", "e": "AQAB"},
            {"kty": "RSA", "kid": "enc-key", "use": "enc",
             "n": "0Y...", "e": "AQAB"}
        ]
    }"#;
    let jwks = Jwks::from_json(json, true).expect("parses");
    assert!(jwks.contains_kid("sig-key"));
    assert!(jwks.contains_kid("enc-key"));
}

#[test]
fn parse_jwks_keeps_keys_with_absent_use_field_regardless() {
    let json = r#"{
        "keys": [
            {"kty": "RSA", "kid": "no-use",
             "n": "0Z...", "e": "AQAB"}
        ]
    }"#;
    assert!(Jwks::from_json(json, false).unwrap().contains_kid("no-use"));
    assert!(Jwks::from_json(json, true).unwrap().contains_kid("no-use"));
}
```

`Jwks::contains_kid` may need adding as a `pub fn` if not present (alternatively use `Jwks::keys().contains_key("...")` if there's a public accessor). The implementer's call. Use realistic RSA modulus values from existing JWKS test fixtures — grep:

```bash
grep -n "fn parse_jwks\|0Z\|fixture.*jwks" crates/security/src/jwks.rs | head
```

- [ ] **Step 5: Run JWKS filter tests — verify they pass**

```bash
cd /Users/mattstone/git/crabka/.worktrees/slice-49i-oauth-jwks-policies
cargo test -p crabka-security jwks::tests::parse_jwks_filters 2>&1 | tail
cargo test -p crabka-security jwks::tests::parse_jwks_keeps 2>&1 | tail
```

Expected: all pass (filter toggle is mechanical).

- [ ] **Step 6: Extend `JwksRefresher` struct**

Edit `crates/broker/src/oauth_jwks.rs`. Replace the existing struct (lines 46-64):

```rust
pub(crate) struct JwksRefresher {
    /// JWKS endpoint URL.
    pub endpoint: String,
    /// Shared key cell read by the validator; this task `store`s into it.
    pub handle: JwksHandle,
    /// Re-fetch cadence (periodic).
    pub interval: Duration,
    /// Cancels the task on broker shutdown.
    pub shutdown: CancellationToken,
    /// Slice 49c (renamed in 49d): optional PEM path.
    pub tls_trust: Option<PathBuf>,
    /// Slice 49i: receives signals from validators on verify-failure
    /// to trigger an on-demand refresh (subject to rate-limit).
    pub signal_rx: tokio::sync::mpsc::Receiver<()>,
    /// Slice 49i: minimum pause between on-demand refreshes.
    /// Strimzi default 1 second. Periodic refresh is unaffected.
    pub min_on_demand_pause: Duration,
    /// Slice 49i: shared timestamp counter. Refresher updates after
    /// each successful fetch; validators read for cache-expiry check.
    pub last_successful_fetch_ms: Arc<std::sync::atomic::AtomicI64>,
    /// Slice 49i: tracks the last on-demand-refresh epoch ms for
    /// rate-limiting. Independent of periodic refresh.
    pub last_on_demand_refresh_ms: Arc<std::sync::atomic::AtomicI64>,
    /// Slice 49i: when true, accept JWKS keys regardless of `use`
    /// field. Default false (filter to `use=sig` or absent).
    pub ignore_key_use: bool,
}
```

Add imports as needed at the top of the file:

```rust
use std::sync::atomic::{AtomicI64, Ordering};
use std::sync::Arc;
```

- [ ] **Step 7: Refactor `JwksRefresher::run()` to add on-demand refresh arm**

Replace the `run()` method (lines 71-119):

```rust
pub(crate) async fn run(mut self) {
    // existing builder/client construction unchanged ...
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
    let client = match builder.build() {
        Ok(c) => c,
        Err(e) => {
            tracing::error!(error = %e, "failed to build JWKS HTTP client; OAUTHBEARER signed tokens will not validate");
            return;
        }
    };

    let mut tick = tokio::time::interval(self.interval);
    tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);

    loop {
        tokio::select! {
            _ = tick.tick() => {
                self.refresh_and_swap(&client).await;
            }
            // Slice 49i: on-demand refresh triggered by validator
            // signal. Subject to min_on_demand_pause rate-limit.
            // Signals coalesce via mpsc capacity 1 + try_send.
            Some(()) = self.signal_rx.recv() => {
                let now_ms = current_epoch_ms();
                let last_on_demand = self.last_on_demand_refresh_ms.load(Ordering::Relaxed);
                let elapsed_ms = now_ms - last_on_demand;
                let pause_ms = self.min_on_demand_pause.as_millis() as i64;
                if elapsed_ms >= pause_ms {
                    self.last_on_demand_refresh_ms.store(now_ms, Ordering::Relaxed);
                    tracing::debug!(
                        endpoint = %self.endpoint,
                        elapsed_ms,
                        "on-demand JWKS refresh triggered by validator signal",
                    );
                    self.refresh_and_swap(&client).await;
                } else {
                    tracing::debug!(
                        endpoint = %self.endpoint,
                        elapsed_ms,
                        pause_ms,
                        "on-demand JWKS refresh rate-limited; signal dropped",
                    );
                }
            }
            () = self.shutdown.cancelled() => return,
        }
    }
}

/// Slice 49i: extracted from the loop so the periodic + on-demand
/// arms can both call it. Updates `last_successful_fetch_ms` only
/// on success (failure leaves the timestamp untouched so the cache
/// ages toward expiry).
async fn refresh_and_swap(&self, client: &reqwest::Client) {
    match fetch_jwks(client, &self.endpoint, self.ignore_key_use).await {
        Ok(jwks) => {
            tracing::debug!(
                endpoint = %self.endpoint,
                keys = jwks.len(),
                "refreshed OAUTHBEARER JWKS",
            );
            self.handle.store(jwks);
            self.last_successful_fetch_ms
                .store(current_epoch_ms(), Ordering::Relaxed);
        }
        Err(e) => tracing::warn!(
            endpoint = %self.endpoint,
            error = %e,
            "failed to refresh OAUTHBEARER JWKS; keeping previous key set",
        ),
    }
}
```

Add a `current_epoch_ms()` helper (or use whatever the broker's existing convention is — grep for `SystemTime::now\|epoch_ms`):

```rust
fn current_epoch_ms() -> i64 {
    use std::time::{SystemTime, UNIX_EPOCH};
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |d| i64::try_from(d.as_millis()).unwrap_or(i64::MAX))
}
```

If a workspace helper already exists (e.g., `crabka_security::now_unix_millis`), prefer that.

- [ ] **Step 8: Update `fetch_jwks` to take `ignore_key_use`**

Replace `fetch_jwks` (lines 31-42):

```rust
pub(crate) async fn fetch_jwks(
    client: &reqwest::Client,
    endpoint: &str,
    ignore_key_use: bool,
) -> Result<Jwks, FetchError> {
    let body = client
        .get(endpoint)
        .send()
        .await?
        .error_for_status()?
        .text()
        .await?;
    Jwks::from_json(&body, ignore_key_use).map_err(|_| FetchError::Parse)
}
```

- [ ] **Step 9: Write failing refresher unit tests**

Add to `crates/broker/src/oauth_jwks.rs` test module (create if not present):

```rust
#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::AtomicI64;
    use tokio::sync::mpsc;
    use tokio_util::sync::CancellationToken;

    /// Spin up a refresher pointed at a mock HTTP server; control time
    /// via `tokio::time::pause` + `advance`. Returns the
    /// `last_on_demand_refresh_ms` Arc so tests can assert on it, plus
    /// the signal sender for triggering refreshes from the test thread.
    fn make_refresher_with_mock(
        endpoint: String,
    ) -> (
        JwksRefresher,
        Arc<AtomicI64>, // last_on_demand_refresh_ms
        mpsc::Sender<()>,
        CancellationToken,
    ) {
        let (signal_tx, signal_rx) = mpsc::channel::<()>(1);
        let shutdown = CancellationToken::new();
        let last_successful = Arc::new(AtomicI64::new(0));
        let last_on_demand = Arc::new(AtomicI64::new(0));
        let handle = JwksHandle::new_with_refresher_handles(
            Jwks::default(),
            last_successful.clone(),
            signal_tx.clone(),
        );
        let refresher = JwksRefresher {
            endpoint,
            handle,
            interval: Duration::from_secs(3600), // long — only on-demand matters here
            shutdown: shutdown.clone(),
            tls_trust: None,
            signal_rx,
            min_on_demand_pause: Duration::from_secs(5),
            last_successful_fetch_ms: last_successful,
            last_on_demand_refresh_ms: last_on_demand.clone(),
            ignore_key_use: false,
        };
        (refresher, last_on_demand, signal_tx, shutdown)
    }

    #[tokio::test(flavor = "current_thread", start_paused = true)]
    async fn refresher_signal_triggers_on_demand_refresh_when_pause_elapsed() {
        let server = mockito::Server::new_async().await;
        let _m = server
            .mock("GET", "/jwks")
            .with_status(200)
            .with_body(r#"{"keys":[]}"#)
            .create_async()
            .await;
        let endpoint = format!("{}/jwks", server.url());
        let (refresher, last_on_demand, signal_tx, shutdown) =
            make_refresher_with_mock(endpoint);

        // Start the refresher in the background.
        let task = tokio::spawn(async move { refresher.run().await });

        // Advance just enough for the periodic-interval tokio internals
        // to settle (not 3600s — we don't want a periodic tick), then
        // fire the signal.
        tokio::time::advance(Duration::from_millis(10)).await;
        assert_eq!(last_on_demand.load(Ordering::Relaxed), 0);
        signal_tx.send(()).await.expect("send signal");

        // Let the refresher's select! arm run.
        tokio::task::yield_now().await;
        tokio::time::advance(Duration::from_millis(10)).await;
        tokio::task::yield_now().await;

        assert!(
            last_on_demand.load(Ordering::Relaxed) > 0,
            "refresher should have stored on-demand refresh timestamp",
        );

        shutdown.cancel();
        let _ = task.await;
    }

    #[tokio::test(flavor = "current_thread", start_paused = true)]
    async fn refresher_signal_dropped_when_within_min_pause_window() {
        let server = mockito::Server::new_async().await;
        let _m = server
            .mock("GET", "/jwks")
            .with_status(200)
            .with_body(r#"{"keys":[]}"#)
            .expect(1) // Only the FIRST refresh should fire.
            .create_async()
            .await;
        let endpoint = format!("{}/jwks", server.url());
        let (refresher, last_on_demand, signal_tx, shutdown) =
            make_refresher_with_mock(endpoint);

        let task = tokio::spawn(async move { refresher.run().await });

        // First signal fires.
        signal_tx.send(()).await.expect("send signal 1");
        tokio::task::yield_now().await;
        tokio::time::advance(Duration::from_millis(10)).await;
        tokio::task::yield_now().await;
        let first_ts = last_on_demand.load(Ordering::Relaxed);
        assert!(first_ts > 0);

        // Second signal within min_pause (5s) — dropped.
        tokio::time::advance(Duration::from_secs(2)).await;
        signal_tx.send(()).await.expect("send signal 2");
        tokio::task::yield_now().await;
        tokio::time::advance(Duration::from_millis(10)).await;
        tokio::task::yield_now().await;
        assert_eq!(
            last_on_demand.load(Ordering::Relaxed),
            first_ts,
            "second signal within min_pause should not update timestamp",
        );

        shutdown.cancel();
        let _ = task.await;
        // _m.assert_async().await ensures only 1 HTTP fetch happened.
    }

    #[tokio::test(flavor = "current_thread", start_paused = true)]
    async fn refresher_successful_refresh_updates_last_successful_fetch_timestamp() {
        let server = mockito::Server::new_async().await;
        let _m = server
            .mock("GET", "/jwks")
            .with_status(200)
            .with_body(r#"{"keys":[]}"#)
            .create_async()
            .await;
        let endpoint = format!("{}/jwks", server.url());
        let (refresher, _last_on_demand, signal_tx, shutdown) =
            make_refresher_with_mock(endpoint);
        let last_successful = refresher.last_successful_fetch_ms.clone();

        let task = tokio::spawn(async move { refresher.run().await });
        signal_tx.send(()).await.unwrap();
        tokio::task::yield_now().await;
        tokio::time::advance(Duration::from_millis(10)).await;
        tokio::task::yield_now().await;
        assert!(last_successful.load(Ordering::Relaxed) > 0);
        shutdown.cancel();
        let _ = task.await;
    }

    #[tokio::test(flavor = "current_thread", start_paused = true)]
    async fn refresher_failed_refresh_does_not_advance_last_successful_fetch() {
        let server = mockito::Server::new_async().await;
        let _m = server
            .mock("GET", "/jwks")
            .with_status(500)
            .with_body("boom")
            .create_async()
            .await;
        let endpoint = format!("{}/jwks", server.url());
        let (refresher, _last_on_demand, signal_tx, shutdown) =
            make_refresher_with_mock(endpoint);
        let last_successful = refresher.last_successful_fetch_ms.clone();

        let task = tokio::spawn(async move { refresher.run().await });
        signal_tx.send(()).await.unwrap();
        tokio::task::yield_now().await;
        tokio::time::advance(Duration::from_millis(10)).await;
        tokio::task::yield_now().await;
        assert_eq!(
            last_successful.load(Ordering::Relaxed),
            0,
            "failed refresh should leave timestamp at sentinel 0",
        );
        shutdown.cancel();
        let _ = task.await;
    }
}
```

`mockito` is already in workspace dev-deps (used by other broker tests — verify with `grep mockito Cargo.toml`); if absent, add `mockito.workspace = true` to `crates/broker/Cargo.toml`'s `[dev-dependencies]`.

- [ ] **Step 10: Run refresher tests — verify pass**

```bash
cargo test -p crabka-broker --lib oauth_jwks::tests 2>&1 | tail -15
```

Expected: all 4 new refresher tests pass.

- [ ] **Step 11: Extend `SignedJwsValidator` with `expiry_ms` field + body changes**

Edit `crates/security/src/oauthbearer.rs`. Replace `SignedJwsValidator` struct (lines 374-398) — add `expiry_ms` field:

```rust
#[derive(Debug, Clone)]
pub struct SignedJwsValidator {
    pub principal_claim_name: String,
    pub allowable_clock_skew_ms: i64,
    pub valid_issuer: Option<String>,
    pub expected_audience: Option<String>,
    pub custom_claim_check: Option<JpQuery>,
    pub valid_token_type: Option<String>,
    pub fallback_user_name_claim: Option<String>,
    pub fallback_user_name_prefix: Option<String>,
    pub groups_claim: Option<JpQuery>,
    pub groups_claim_delimiter: Option<String>,
    /// Slice 49i: hard cache-expiry threshold in milliseconds. When
    /// set, validators reject tokens if the JWKS has not been
    /// successfully refreshed within this window. `None` = no expiry
    /// check (slice 49b behavior).
    pub expiry_ms: Option<i64>,
    /// The live JWKS, swapped in by the broker's refresher.
    keys: JwksHandle,
}
```

Replace `validate()` (lines 433-469) — add expiry check + signal-on-verify-failure:

```rust
pub fn validate(&self, token: &str, now_ms: i64) -> Result<AuthOutcome, AuthError> {
    // existing: structural parse ...
    let mut segs = token.split('.');
    let header_b64 = segs.next().ok_or(AuthError::InvalidToken)?;
    let payload_b64 = segs.next().ok_or(AuthError::InvalidToken)?;
    let sig_b64 = segs.next().ok_or(AuthError::InvalidToken)?;
    if segs.next().is_some() || sig_b64.is_empty() {
        return Err(AuthError::InvalidToken);
    }

    let header: Value = decode_json_segment(header_b64)?;
    let alg = header
        .get("alg")
        .and_then(Value::as_str)
        .ok_or(AuthError::InvalidToken)?;
    if alg != "RS256" && alg != "ES256" {
        return Err(AuthError::InvalidToken);
    }
    if let Some(expected_typ) = &self.valid_token_type
        && header.get("typ").and_then(Value::as_str) != Some(expected_typ.as_str())
    {
        return Err(AuthError::InvalidToken);
    }

    // Slice 49i: hard cache-expiry. If the last successful fetch is
    // older than expiry_ms, reject all tokens until next successful
    // refresh. Fails closed when the IdP is unreachable for too long.
    if let Some(expiry_ms) = self.expiry_ms {
        let last_fetch = self.keys.last_successful_fetch_ms();
        if last_fetch > 0 && (now_ms - last_fetch) > expiry_ms {
            tracing::debug!(
                last_fetch_ms = last_fetch,
                now_ms = now_ms,
                expiry_ms = expiry_ms,
                "JWKS cache expired; rejecting token until next successful refresh"
            );
            return Err(AuthError::InvalidToken);
        }
    }

    let kid = header.get("kid").and_then(Value::as_str);
    let signing_input = format!("{header_b64}.{payload_b64}");
    let sig = B64URL
        .decode(sig_b64)
        .map_err(|_| AuthError::InvalidToken)?;

    // Slice 49i: signal-on-verify-failure. The `verify()` returns
    // InvalidToken for both unknown-kid AND bad-signature; we signal
    // in both cases. The min_pause rate-limit caps the cost; the
    // current token rejects regardless.
    if let Err(e) = self.keys.load().verify(kid, alg, signing_input.as_bytes(), &sig) {
        self.keys.signal_refresh();
        return Err(e);
    }

    let claims: Value = decode_json_segment(payload_b64)?;
    self.validate_claims(&self, claims, now_ms)
}
```

The body change is minimal: insert the expiry check after the `typ` check, and restructure the verify call from `?` to `if let Err(e) { signal_refresh(); return Err(e); }`.

- [ ] **Step 12: Write failing validator unit tests**

Add to `crates/security/src/oauthbearer.rs` test module (the `tests` mod that already has the signed-validator tests):

```rust
#[test]
fn signed_validate_rejects_when_jwks_cache_expired() {
    // Build a validator with expiry_ms = 1000 (1s), then artificially
    // set last_successful_fetch_ms to now-2s so the cache "is" expired.
    let now_ms = 10_000_000_i64;
    let (token, jwks) = mint_rs256("kid1", &serde_json::json!({"sub": "alice", "exp": (now_ms / 1000) + 60}));
    let last_successful = Arc::new(AtomicI64::new(now_ms - 2000));
    let (signal_tx, _signal_rx) = mpsc::channel::<()>(1);
    let handle = JwksHandle::new_with_refresher_handles(jwks, last_successful, signal_tx);
    let mut v = SignedJwsValidator::new(handle);
    v.expiry_ms = Some(1000);
    let result = v.validate(&token, now_ms);
    assert_eq!(result, Err(AuthError::InvalidToken));
}

#[test]
fn signed_validate_accepts_when_jwks_cache_within_expiry() {
    let now_ms = 10_000_000_i64;
    let (token, jwks) = mint_rs256("kid1", &serde_json::json!({"sub": "alice", "exp": (now_ms / 1000) + 60}));
    let last_successful = Arc::new(AtomicI64::new(now_ms - 500));
    let (signal_tx, _signal_rx) = mpsc::channel::<()>(1);
    let handle = JwksHandle::new_with_refresher_handles(jwks, last_successful, signal_tx);
    let mut v = SignedJwsValidator::new(handle);
    v.expiry_ms = Some(1000);
    let outcome = v.validate(&token, now_ms).expect("valid");
    assert_eq!(outcome.principal.name, "alice");
}

#[test]
fn signed_validate_accepts_when_expiry_unset_regardless_of_cache_age() {
    let now_ms = 10_000_000_i64;
    let (token, jwks) = mint_rs256("kid1", &serde_json::json!({"sub": "alice", "exp": (now_ms / 1000) + 60}));
    // Cache "is" very stale, but expiry_ms = None.
    let last_successful = Arc::new(AtomicI64::new(now_ms - 999_999_999));
    let (signal_tx, _signal_rx) = mpsc::channel::<()>(1);
    let handle = JwksHandle::new_with_refresher_handles(jwks, last_successful, signal_tx);
    let v = SignedJwsValidator::new(handle); // expiry_ms = None
    assert!(v.validate(&token, now_ms).is_ok());
}

#[tokio::test]
async fn signed_validate_signals_refresh_on_unknown_kid() {
    let now_ms = 10_000_000_i64;
    // Token signed with kid="missing-kid"; JWKS only contains kid="present-kid".
    let (token, _) = mint_rs256("missing-kid", &serde_json::json!({"sub": "alice", "exp": (now_ms / 1000) + 60}));
    let (_, jwks) = mint_rs256("present-kid", &serde_json::json!({}));
    let last_successful = Arc::new(AtomicI64::new(now_ms));
    let (signal_tx, mut signal_rx) = mpsc::channel::<()>(1);
    let handle = JwksHandle::new_with_refresher_handles(jwks, last_successful, signal_tx);
    let v = SignedJwsValidator::new(handle);
    let result = v.validate(&token, now_ms);
    assert_eq!(result, Err(AuthError::InvalidToken));
    // The validator should have fired a signal — try_recv should succeed.
    assert!(signal_rx.try_recv().is_ok(), "validator should signal refresh on verify failure");
}
```

Adapt `mint_rs256` to the actual helper name used by existing signed-validator tests (grep `fn mint_rs256\|fn signed_jws`). The `mpsc` import + `AtomicI64` import come from the new `JwksHandle::new_with_refresher_handles` signature.

- [ ] **Step 13: Run validator tests — verify pass**

```bash
cargo test -p crabka-security oauthbearer::tests::signed_validate_rejects_when_jwks 2>&1 | tail
cargo test -p crabka-security oauthbearer::tests::signed_validate_accepts_when_jwks 2>&1 | tail
cargo test -p crabka-security oauthbearer::tests::signed_validate_accepts_when_expiry 2>&1 | tail
cargo test -p crabka-security oauthbearer::tests::signed_validate_signals_refresh 2>&1 | tail
```

Expected: all 4 new validator tests pass.

- [ ] **Step 14: Extend `FileOAuthBearerConfig` with 3 new fields**

Edit `crates/broker/src/file_config.rs`. AFTER the existing `groups_claim_delimiter` field (last field post-49h, around line 162), ADD:

```rust
    /// Slice 49i: minimum pause (seconds) between on-demand JWKS
    /// refreshes triggered by unknown-kid tokens. Strimzi default 1.
    #[serde(default)]
    pub jwks_min_refresh_pause_seconds: Option<u32>,

    /// Slice 49i: maximum age (seconds) of the cached JWKS before
    /// validators reject tokens until next successful refresh.
    /// Strimzi default 360 (6 minutes). Fails closed on IdP outage.
    #[serde(default)]
    pub jwks_expiry_seconds: Option<u32>,

    /// Slice 49i: when true, accept JWKS keys regardless of `use`
    /// field. Default false matches Strimzi (filter out `use=enc`).
    #[serde(default)]
    pub jwks_ignore_key_use: Option<bool>,
```

- [ ] **Step 15: Add `BrokerConfig.oauthbearer_jwks_signal_rx` carry field**

The signal channel needs to flow from `apply_to` (where validator is constructed) to `Broker::start` (where the refresher is constructed). Use a `Mutex<Option<>>` for the receiver — `Broker::start` `take()`s it.

Edit `crates/broker/src/config.rs`. Find the section with `oauthbearer_*` fields (post-50d, search for `oauthbearer_max_session_lifetime_seconds`). Add adjacent:

```rust
    /// Slice 49i: receiver half of the JWKS refresher signal channel.
    /// `apply_to` creates the channel pair: the sender is wired into
    /// the signed validator's JwksHandle; the receiver is parked here
    /// for `Broker::start` to `take()` and pass to `JwksRefresher`.
    /// `None` when JWKS validation isn't configured.
    pub oauthbearer_jwks_signal_rx:
        std::sync::Mutex<Option<tokio::sync::mpsc::Receiver<()>>>,

    /// Slice 49i: shared timestamp for cache-expiry. Validator + refresher
    /// share this. apply_to creates it; Broker::start hands a clone to
    /// the refresher.
    pub oauthbearer_jwks_last_successful_fetch_ms:
        std::sync::Arc<std::sync::atomic::AtomicI64>,

    /// Slice 49i: shared on-demand-refresh timestamp for rate-limiting.
    /// apply_to creates it; Broker::start hands a clone to the refresher.
    pub oauthbearer_jwks_last_on_demand_refresh_ms:
        std::sync::Arc<std::sync::atomic::AtomicI64>,

    /// Slice 49i: minimum pause between on-demand refreshes. apply_to
    /// sets it from FileOAuthBearerConfig; Broker::start reads it.
    pub oauthbearer_jwks_min_on_demand_pause: std::time::Duration,

    /// Slice 49i: when true, refresher and validator skip the `use=enc`
    /// filter on JWKS keys.
    pub oauthbearer_jwks_ignore_key_use: bool,
```

Update the `Default` impl for `BrokerConfig` to set sensible defaults (`None` for the receiver, `Arc::new(AtomicI64::new(0))` for timestamps, `Duration::from_secs(1)` for pause, `false` for ignore_key_use).

If `BrokerConfig` doesn't derive `Default` and uses a constructor, add init lines there.

**Why Mutex<Option<Receiver>>**: `BrokerConfig` is `Clone` (probably), but `Receiver` isn't. Wrapping in `Mutex<Option<>>` lets `Broker::start` `lock().take()` to consume the receiver without needing to thread it as a separate function parameter through whatever construction call chain exists.

- [ ] **Step 16: Update `apply_to` signed-branch to create + wire the signal channel**

Edit `crates/broker/src/file_config.rs`. Replace the signed-branch in `apply_to` (lines 327-360):

```rust
                (Some(_), None) => {
                    // Signed-JWT validation (slice 49b). The empty key handle is
                    // populated by the refresher `Broker::start` spawns.
                    let jwks_uri = oauth.jwks_endpoint_uri.clone().unwrap();

                    // Slice 49i: signal channel + shared timestamps.
                    // Channel capacity 1 — signals coalesce via try_send.
                    let (signal_tx, signal_rx) = tokio::sync::mpsc::channel::<()>(1);
                    let last_successful = std::sync::Arc::new(
                        std::sync::atomic::AtomicI64::new(0),
                    );
                    let last_on_demand = std::sync::Arc::new(
                        std::sync::atomic::AtomicI64::new(0),
                    );

                    let handle = crabka_security::JwksHandle::new_with_refresher_handles(
                        crabka_security::Jwks::default(),
                        last_successful.clone(),
                        signal_tx,
                    );

                    let mut v = crabka_security::SignedJwsValidator::new(handle);
                    if let Some(name) = oauth.principal_claim_name {
                        v.principal_claim_name = name;
                    }
                    if let Some(skew) = oauth.allowable_clock_skew_ms {
                        v.allowable_clock_skew_ms = skew;
                    }
                    v.valid_issuer = oauth.valid_issuer_uri;
                    v.expected_audience = oauth.expected_audience;
                    v.custom_claim_check
                        .clone_from(&custom_claim_check_compiled);
                    v.valid_token_type.clone_from(&oauth.valid_token_type);
                    v.fallback_user_name_claim
                        .clone_from(&oauth.fallback_user_name_claim);
                    v.fallback_user_name_prefix
                        .clone_from(&oauth.fallback_user_name_prefix);
                    v.groups_claim.clone_from(&groups_claim_compiled);
                    v.groups_claim_delimiter
                        .clone_from(&oauth.groups_claim_delimiter);
                    // Slice 49i: cache-expiry threshold.
                    v.expiry_ms = oauth.jwks_expiry_seconds.map(|s| i64::from(s) * 1000);

                    cfg.oauthbearer_validator = crabka_security::OAuthBearerValidator::Signed(v);
                    cfg.oauthbearer_jwks_endpoint = Some(jwks_uri);
                    if let Some(ms) = oauth.jwks_refresh_interval_ms {
                        cfg.oauthbearer_jwks_refresh_interval =
                            std::time::Duration::from_millis(ms);
                    }

                    // Slice 49i: park signal_rx + shared state for Broker::start.
                    *cfg.oauthbearer_jwks_signal_rx.lock().unwrap() = Some(signal_rx);
                    cfg.oauthbearer_jwks_last_successful_fetch_ms = last_successful;
                    cfg.oauthbearer_jwks_last_on_demand_refresh_ms = last_on_demand;
                    cfg.oauthbearer_jwks_min_on_demand_pause = std::time::Duration::from_secs(
                        u64::from(oauth.jwks_min_refresh_pause_seconds.unwrap_or(1)),
                    );
                    cfg.oauthbearer_jwks_ignore_key_use =
                        oauth.jwks_ignore_key_use.unwrap_or(false);
                }
```

- [ ] **Step 17: Wire the carried state into `Broker::start`'s refresher construction**

Find the `Broker::start` site where `JwksRefresher` is constructed (per T1 Step 1 grep). The current code probably looks like:

```rust
// Pre-49i:
let refresher = JwksRefresher {
    endpoint: jwks_uri.clone(),
    handle: jwks_handle.clone(),
    interval: cfg.oauthbearer_jwks_refresh_interval,
    shutdown: shutdown.clone(),
    tls_trust: cfg.oauthbearer_idp_tls_trust.clone(),
};
tokio::spawn(refresher.run());
```

Replace with (preserving the existing surrounding code):

```rust
// Slice 49i: consume the parked signal_rx + shared state from cfg.
let signal_rx = cfg
    .oauthbearer_jwks_signal_rx
    .lock()
    .unwrap()
    .take()
    .expect("apply_to wired signal_rx when configuring signed validator");
let refresher = JwksRefresher {
    endpoint: jwks_uri.clone(),
    handle: jwks_handle.clone(),
    interval: cfg.oauthbearer_jwks_refresh_interval,
    shutdown: shutdown.clone(),
    tls_trust: cfg.oauthbearer_idp_tls_trust.clone(),
    signal_rx,
    min_on_demand_pause: cfg.oauthbearer_jwks_min_on_demand_pause,
    last_successful_fetch_ms: cfg.oauthbearer_jwks_last_successful_fetch_ms.clone(),
    last_on_demand_refresh_ms: cfg.oauthbearer_jwks_last_on_demand_refresh_ms.clone(),
    ignore_key_use: cfg.oauthbearer_jwks_ignore_key_use,
};
tokio::spawn(refresher.run());
```

The `jwks_handle` variable is whatever the existing code holds — it should be the SAME handle that's in the validator (via `oauthbearer_validator` extraction or stored separately during `apply_to`). Read the existing construction carefully.

If the existing code stores `jwks_handle` in some `BrokerConfig` field (e.g., a `cfg.oauthbearer_jwks_handle: JwksHandle`), THAT field should now carry the handle with `new_with_refresher_handles` (the same handle that's in the validator). The refresher's `handle` field is just `cfg.oauthbearer_jwks_handle.clone()`.

If the existing code accesses the handle via `oauthbearer_validator` introspection, that pattern stays — both validator and refresher share via `JwksHandle::clone()` (cheap Arc clone).

- [ ] **Step 18: Build the workspace**

```bash
cargo build --workspace 2>&1 | tail -15
```

Expected: clean broker + security. Operator likely fails E0063 in `controller/listeners.rs` (T2/T3 territory).

- [ ] **Step 19: fmt + clippy on broker side**

```bash
cargo fmt -p crabka-security -p crabka-broker -- --check
cargo clippy -p crabka-security -p crabka-broker --lib --tests -- -D warnings 2>&1 | tail
```

Expected: clean.

- [ ] **Step 20: Commit**

```bash
git add crates/security/src/jwks.rs \
        crates/security/src/oauthbearer.rs \
        crates/broker/src/oauth_jwks.rs \
        crates/broker/src/file_config.rs \
        crates/broker/src/config.rs \
        crates/broker/src/broker.rs
git -c user.name="Matt Stone" -c user.email="matthew.d.stone@gmail.com" commit -m "$(cat <<'EOF'
T1: broker — JWKS refresher policies (slice 49i)

Adds three new operational tuning fields on the broker JWKS refresher
(slice-49b) loop:

- jwks_min_refresh_pause_seconds: rate-limit on on-demand refreshes
  triggered by validator signals (unknown kid / bad signature).
- jwks_expiry_seconds: hard cache expiry — SignedJwsValidator rejects
  tokens when last_successful_fetch is older than this. Fails closed
  on IdP outage.
- jwks_ignore_key_use: when true, JWKS parser keeps keys with any
  `use` value (default behavior filters out use=enc).

JwksHandle gains last_successful_fetch_ms + signal_tx (Option<>) so
validators can read the cache-age timestamp and trigger on-demand
refresh via fire-and-forget mpsc try_send (capacity 1; signals
coalesce). JwksRefresher loop becomes 3-arm select! (periodic tick /
signal_rx.recv / cancellation). Signal arm checks
min_on_demand_pause before refreshing.

SignedJwsValidator.validate body: cache-expiry check after typ/alg
checks; signal_refresh() fired on keys.verify() failure (any kind —
the min_pause rate-limit caps the cost). The verify call restructured
from ? to if-let-Err for the signal-then-reject pattern.

FileOAuthBearerConfig.apply_to creates the signal mpsc pair, wires
signal_tx into the validator's JwksHandle, and parks signal_rx +
shared timestamps in BrokerConfig for Broker::start to consume when
constructing JwksRefresher.

7 new unit tests: 3 JWKS parser filter tests, 4 refresher behavior
tests (signal-triggers-on-pause-elapsed, signal-dropped-within-pause,
last_successful_fetch-updates-on-success, no-update-on-fetch-fail).
4 new validator tests: cache-expired-rejects, within-expiry-accepts,
expiry-unset-no-check, unknown-kid-signals-refresh.

Co-Authored-By: Claude Opus 4.7 (1M context) <noreply@anthropic.com>
EOF
)"
```

---

### Batch 2 — T2 then T3 (sequential within batch)

**Dispatch order:** T2 first; wait for commit; then T3.

#### Task T2: Operator CRD — 3 new fields + own-file fixture sweep + round-trip tests

**Files:**
- Modify: `crates/operator/src/crd/listener.rs`

- [ ] **Step 1: Write 2 failing round-trip tests**

In the test module:

```rust
#[test]
fn oauth_round_trip_with_jwks_policy_fields() {
    let yaml = r#"
type: oauth
validIssuerUri: https://issuer.example/
jwksEndpointUri: https://issuer.example/jwks
jwksMinRefreshPauseSeconds: 1
jwksExpirySeconds: 3600
jwksIgnoreKeyUse: false
"#;
    let parsed: ListenerAuthentication = serde_yaml::from_str(yaml).expect("yaml must parse");
    let ListenerAuthentication::OAuth(oauth) = &parsed else {
        panic!("expected oauth variant");
    };
    assert_eq!(oauth.jwks_min_refresh_pause_seconds, Some(1));
    assert_eq!(oauth.jwks_expiry_seconds, Some(3600));
    assert_eq!(oauth.jwks_ignore_key_use, Some(false));
}

#[test]
fn oauth_round_trip_without_jwks_policy_fields_omits_them() {
    let cfg = ListenerAuthenticationOAuth {
        // existing 22 fields per the post-49h shape, all defaults ...
        valid_issuer_uri: "https://issuer.example/".into(),
        jwks_endpoint_uri: Some("https://issuer.example/jwks".into()),
        valid_audience: None,
        user_name_claim: None,
        custom_claim_check: None,
        jwks_refresh_seconds: None,
        max_clock_skew_seconds: None,
        enable_oauth_bearer: true,
        tls_trusted_certificates: vec![],
        access_token_is_jwt: true,
        introspection_endpoint_uri: None,
        user_info_endpoint_uri: None,
        client_id: None,
        client_secret: None,
        introspection_http_timeout_seconds: None,
        max_seconds_without_reauthentication: None,
        valid_token_type: None,
        fallback_user_name_claim: None,
        fallback_user_name_prefix: None,
        groups_claim: None,
        groups_claim_delimiter: None,
        jwks_min_refresh_pause_seconds: None,
        jwks_expiry_seconds: None,
        jwks_ignore_key_use: None,
    };
    let auth = ListenerAuthentication::OAuth(cfg);
    let yaml = serde_yaml::to_string(&auth).expect("yaml must serialize");
    for key in ["jwksMinRefreshPauseSeconds", "jwksExpirySeconds", "jwksIgnoreKeyUse"] {
        assert!(!yaml.contains(key), "{key} must be omitted; got:\n{yaml}");
    }
}
```

- [ ] **Step 2: Run tests — verify failure**

```bash
cd /Users/mattstone/git/crabka/.worktrees/slice-49i-oauth-jwks-policies
cargo test -p crabka-operator --lib crd::listener::auth_tests::oauth_round_trip_with_jwks_policy 2>&1 | tail
```

Expected: compile error — fields missing.

- [ ] **Step 3: Add 3 new fields to `ListenerAuthenticationOAuth`**

In `crates/operator/src/crd/listener.rs`, AFTER the existing `groups_claim_delimiter` field (last field post-49h, around line 195), ADD:

```rust
    /// Slice 49i: minimum pause (seconds) between on-demand JWKS
    /// refreshes triggered by tokens with unknown `kid`. When the
    /// broker receives a token whose `kid` isn't in the cached
    /// JWKS, it triggers an immediate refresh; this field
    /// rate-limits to protect the IdP from being hammered by a
    /// stream of bad tokens. Strimzi default: 1. CRD-validated
    /// `minimum: 0` (0 = no rate-limit). JWT-mode only — rejected
    /// when `accessTokenIsJwt: false`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub jwks_min_refresh_pause_seconds: Option<u32>,

    /// Slice 49i: maximum age (seconds) of the cached JWKS before
    /// validators reject tokens until next successful refresh.
    /// Distinct from `jwksRefreshSeconds` (the periodic cadence) —
    /// this is the HARD expiry that fails closed if the IdP is
    /// unreachable for too long. Strimzi default: 360 (6 minutes).
    /// CRD-validated `minimum: 1`. JWT-mode only — rejected when
    /// `accessTokenIsJwt: false`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub jwks_expiry_seconds: Option<u32>,

    /// Slice 49i: when true, accept JWKS keys with any `use` field
    /// value (not just `sig`). Default false matches Strimzi/JWS
    /// behavior of filtering out encryption-only keys. Set true for
    /// IdPs that mis-tag their signing keys. JWT-mode only —
    /// rejected when `accessTokenIsJwt: false`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub jwks_ignore_key_use: Option<bool>,
```

- [ ] **Step 4: Add 3 schema entries**

Find `listener_authentication_schema` (around line 296). After the existing 49h entries (around line 378), ADD:

```rust
            "jwksMinRefreshPauseSeconds": { "type": "integer", "format": "int32", "minimum": 0 },
            "jwksExpirySeconds":          { "type": "integer", "format": "int32", "minimum": 1 },
            "jwksIgnoreKeyUse":           { "type": "boolean" },
```

- [ ] **Step 5: Sweep `ListenerAuthenticationOAuth { ... }` literal sites in this file's tests**

```bash
grep -n "ListenerAuthenticationOAuth {" crates/operator/src/crd/listener.rs
```

Per the explore: ~12 sites. For each, add the 3 new defaults at the end:

```rust
        jwks_min_refresh_pause_seconds: None,
        jwks_expiry_seconds: None,
        jwks_ignore_key_use: None,
```

- [ ] **Step 6: Update the schema-regression test**

Find `oauth_listener_authentication_schema_smoke` (or similar). Extend its expected-properties list with the 3 new keys.

- [ ] **Step 7: Run tests — verify pass**

```bash
cargo test -p crabka-operator --lib crd::listener 2>&1 | tail -10
```

Expected: 2 new round-trip tests pass + existing pass + schema regression test passes.

- [ ] **Step 8: fmt + clippy (scoped)**

```bash
cargo fmt -p crabka-operator -- --check
cargo clippy -p crabka-operator --lib -- -D warnings 2>&1 | tail
```

Expected: lib clean. `--tests` will fail E0063 in `controller/listeners.rs` (T3 territory).

- [ ] **Step 9: Commit**

```bash
git add crates/operator/src/crd/listener.rs
git -c user.name="Matt Stone" -c user.email="matthew.d.stone@gmail.com" commit -m "$(cat <<'EOF'
T2: operator CRD — 3 new JWKS refresher policy fields

Adds jwksMinRefreshPauseSeconds + jwksExpirySeconds + jwksIgnoreKeyUse
to ListenerAuthenticationOAuth. First two are Option<u32> (minimum: 0
and 1 respectively in CRD validation); third is Option<bool>. All
JWT-mode only — T3 adds cross-mode validation rejecting them on
introspection-mode listeners.

2 new round-trip tests (with-fields-set, without-fields-omits).
~12 struct-literal fixture sites in this file's tests swept with the
3 new None defaults. Schema-regression test extended with the 3 new
property keys.

T3 follows up with reconciler render + cross-mode validation +
sibling-file sweeps.

Co-Authored-By: Claude Opus 4.7 (1M context) <noreply@anthropic.com>
EOF
)"
```

---

#### Task T3: Operator reconciler — render + cross-mode validation + divergence + sibling-file sweep

**Files:**
- Modify: `crates/operator/src/controller/listeners.rs`
- Modify: `crates/operator/src/controller/kafka.rs`
- Modify: `crates/operator/src/controller/kafka_node_pool.rs`

**Prerequisite:** T2 committed first.

- [ ] **Step 1: Sweep sibling files**

```bash
cd /Users/mattstone/git/crabka/.worktrees/slice-49i-oauth-jwks-policies
grep -n "ListenerAuthenticationOAuth {" crates/operator/src/controller/kafka.rs \
    crates/operator/src/controller/kafka_node_pool.rs
```

Per the explore: 4 + 3 = 7 sites. Add the 3 new defaults to each.

- [ ] **Step 2: Sweep `controller/listeners.rs` fixtures (NOT divergence-walk perturbations)**

```bash
grep -n "ListenerAuthenticationOAuth {" crates/operator/src/controller/listeners.rs
```

Per the explore: 36 matches, but most are divergence-walk perturbation entries using `..base.clone()` (those don't need the new defaults — `..base.clone()` handles them). The real construction sites needing the sweep are:
- The `base` fixture in the divergence walk (~line 1507).
- The fixture builders (`oauth_full_cfg()`, etc.).
- The render-test cfg literals (per slice 49g/49h's added tests).

The implementer should grep + visually distinguish construction sites from `..base.clone()` perturbations. The compiler will catch any missed construction site (E0063).

- [ ] **Step 3: Run `cargo build -p crabka-operator` — confirm workspace unbroke**

```bash
cargo build -p crabka-operator 2>&1 | tail
```

Expected: clean. If E0063 remains, find the missed site.

- [ ] **Step 4: Add 3 render-emit lines to `render_broker_toml`**

Edit `crates/operator/src/controller/listeners.rs`. Find the `[oauthbearer]` block. After the existing 49h emissions (around line 2735, ends with `groups_claim_delimiter`), AFTER the `out.push('\n')` at the very END of the block, REVERT to BEFORE the newline — add:

```rust
        if let Some(s) = oauth_cfg.jwks_min_refresh_pause_seconds {
            let _ = writeln!(out, "jwks_min_refresh_pause_seconds = {s}");
        }
        if let Some(s) = oauth_cfg.jwks_expiry_seconds {
            let _ = writeln!(out, "jwks_expiry_seconds = {s}");
        }
        if let Some(b) = oauth_cfg.jwks_ignore_key_use {
            let _ = writeln!(out, "jwks_ignore_key_use = {b}");
        }
```

All plain values (integers + bool). No escaping needed.

- [ ] **Step 5: Add new `ValidationError` variant + cross-mode check**

Find the `ValidationError` enum (around line 53). After the existing `ListenerOauthValidTokenTypeRejectedInIntrospectionMode` variant (added by 49g, around line 73), ADD:

```rust
    /// Slice 49i: any of `jwksMinRefreshPauseSeconds`,
    /// `jwksExpirySeconds`, `jwksIgnoreKeyUse` set on an
    /// `accessTokenIsJwt: false` listener. Introspection mode
    /// doesn't use JWKS; setting these fields is a configuration
    /// error. The String carries the listener name + which field(s)
    /// were rejected.
    ListenerOauthJwksFieldsRejectedInIntrospectionMode(String),
```

Find `validate_listeners` (around line 202). In the introspection-mode branch (the `else` after `if cfg.access_token_is_jwt`), AFTER the existing 49g `valid_token_type` check, ADD:

```rust
    // Slice 49i: JWKS-only fields are rejected in introspection mode.
    let mut jwks_fields_set = Vec::new();
    if cfg.jwks_min_refresh_pause_seconds.is_some() {
        jwks_fields_set.push("jwksMinRefreshPauseSeconds");
    }
    if cfg.jwks_expiry_seconds.is_some() {
        jwks_fields_set.push("jwksExpirySeconds");
    }
    if cfg.jwks_ignore_key_use.is_some() {
        jwks_fields_set.push("jwksIgnoreKeyUse");
    }
    if !jwks_fields_set.is_empty() {
        return Err(ValidationError::ListenerOauthJwksFieldsRejectedInIntrospectionMode(
            format!(
                "listener '{}': accessTokenIsJwt=false forbids JWKS-only fields ({})",
                l.name,
                jwks_fields_set.join(", "),
            ),
        ));
    }
```

Find where `ValidationError::reason()` returns the variant name (per slice 49g/50d pattern). Add the new variant's reason string mapping (same as variant name).

```bash
grep -n "ListenerOauthValidTokenTypeRejectedInIntrospectionMode" crates/operator/src/controller/listeners.rs
```

Look at the existing reason() match arm and add an entry for `ListenerOauthJwksFieldsRejectedInIntrospectionMode`.

- [ ] **Step 6: Add 4 perturbation entries to the divergence walk**

In `validate_listeners_rejects_two_oauth_listeners_with_divergent_config_in_any_canonical_field` (around line 1495), at the END of the `perturbations` vec (after the existing 49h perturbations), ADD:

```rust
        (
            "jwks_min_refresh_pause_seconds",
            crate::crd::ListenerAuthenticationOAuth {
                jwks_min_refresh_pause_seconds: Some(5),
                ..base.clone()
            },
        ),
        (
            "jwks_expiry_seconds",
            crate::crd::ListenerAuthenticationOAuth {
                jwks_expiry_seconds: Some(3600),
                ..base.clone()
            },
        ),
        (
            "jwks_ignore_key_use",
            crate::crd::ListenerAuthenticationOAuth {
                jwks_ignore_key_use: Some(true),
                ..base.clone()
            },
        ),
```

(3 perturbations, not 4 — the spec mistakenly said 4 in one place. Three new fields = three perturbations.)

- [ ] **Step 7: Add 4 new render unit tests + 1 cross-mode validation test**

```rust
#[test]
fn render_broker_toml_emits_jwks_min_refresh_pause_seconds_when_set() {
    let mut oauth = oauth_full_cfg();
    oauth.jwks_min_refresh_pause_seconds = Some(2);
    let listeners = vec![oauth_listener_for_render("oauth", 9096, false, oauth)];
    let toml = render_broker_toml(&listeners /* match existing args */);
    assert!(toml.contains("jwks_min_refresh_pause_seconds = 2"));
}

#[test]
fn render_broker_toml_emits_jwks_expiry_seconds_when_set() {
    let mut oauth = oauth_full_cfg();
    oauth.jwks_expiry_seconds = Some(3600);
    let listeners = vec![oauth_listener_for_render("oauth", 9096, false, oauth)];
    let toml = render_broker_toml(&listeners /* args */);
    assert!(toml.contains("jwks_expiry_seconds = 3600"));
}

#[test]
fn render_broker_toml_emits_jwks_ignore_key_use_when_set() {
    let mut oauth = oauth_full_cfg();
    oauth.jwks_ignore_key_use = Some(true);
    let listeners = vec![oauth_listener_for_render("oauth", 9096, false, oauth)];
    let toml = render_broker_toml(&listeners /* args */);
    assert!(toml.contains("jwks_ignore_key_use = true"));
}

#[test]
fn render_broker_toml_omits_jwks_policy_fields_when_unset() {
    let oauth = oauth_full_cfg(); // all 3 None
    let listeners = vec![oauth_listener_for_render("oauth", 9096, false, oauth)];
    let toml = render_broker_toml(&listeners /* args */);
    assert!(!toml.contains("jwks_min_refresh_pause_seconds"));
    assert!(!toml.contains("jwks_expiry_seconds"));
    assert!(!toml.contains("jwks_ignore_key_use"));
}

#[test]
fn validate_listeners_rejects_jwks_fields_in_introspection_mode() {
    let cfg = crate::crd::ListenerAuthenticationOAuth {
        valid_issuer_uri: "https://iss.example/".into(),
        jwks_endpoint_uri: None,
        valid_audience: None,
        user_name_claim: None,
        custom_claim_check: None,
        jwks_refresh_seconds: None,
        max_clock_skew_seconds: None,
        enable_oauth_bearer: true,
        tls_trusted_certificates: vec![],
        access_token_is_jwt: false,
        introspection_endpoint_uri: Some("https://iss.example/introspect".into()),
        user_info_endpoint_uri: None,
        client_id: Some("kafka-broker".into()),
        client_secret: Some(crate::crd::OauthClientSecretRef {
            secret_name: "creds".into(),
            key: "client-secret".into(),
        }),
        introspection_http_timeout_seconds: None,
        max_seconds_without_reauthentication: None,
        valid_token_type: None,
        fallback_user_name_claim: None,
        fallback_user_name_prefix: None,
        groups_claim: None,
        groups_claim_delimiter: None,
        jwks_min_refresh_pause_seconds: Some(1), // The violation
        jwks_expiry_seconds: None,
        jwks_ignore_key_use: None,
    };
    let listeners = vec![crate::crd::Listener {
        name: "oauth".into(),
        port: 9096,
        type_: crate::crd::ListenerType::Internal,
        tls: false,
        authentication: Some(crate::crd::ListenerAuthentication::OAuth(cfg)),
        configuration: None,
        network_policy_peers: None,
    }];
    let result = validate_listeners(&listeners);
    assert!(matches!(
        result,
        Err(ValidationError::ListenerOauthJwksFieldsRejectedInIntrospectionMode(_))
    ));
}
```

Adapt `oauth_full_cfg()` + `oauth_listener_for_render()` to actual helper names in this file.

- [ ] **Step 8: Run listeners tests — verify pass**

```bash
cargo test -p crabka-operator --lib controller::listeners 2>&1 | tail -15
```

Expected: all green, including the 4 new render tests + 1 new validation test + extended divergence walk (now 18 perturbations).

- [ ] **Step 9: fmt + clippy**

```bash
cargo fmt -p crabka-operator -- --check
cargo clippy -p crabka-operator --lib --tests -- -D warnings 2>&1 | tail
```

Expected: clean for `controller/*` files. Integration tests (`tests/reconcile_*.rs`) may still fail E0063 — T4 territory.

- [ ] **Step 10: Commit**

```bash
git add crates/operator/src/controller/listeners.rs \
        crates/operator/src/controller/kafka.rs \
        crates/operator/src/controller/kafka_node_pool.rs
git -c user.name="Matt Stone" -c user.email="matthew.d.stone@gmail.com" commit -m "$(cat <<'EOF'
T3: operator reconciler — render JWKS policies + cross-mode + divergence

render_broker_toml emits 3 new TOML keys under [oauthbearer] when set:
jwks_min_refresh_pause_seconds, jwks_expiry_seconds, jwks_ignore_key_use
(all plain integers/bool — no JsonPath escape concerns).

Cross-mode validation: new
ListenerOauthJwksFieldsRejectedInIntrospectionMode ValidationError
variant fires when any of the 3 fields is set on an
accessTokenIsJwt:false listener. Aggregates the names of all set
fields in the error message for ops feedback.

Cross-listener divergence walk extended with 3 new perturbations
(now 18 total). oauth_canonical requires no change — PartialEq picks
up the new fields automatically.

Sweep: 4 + 3 sibling-file sites (controller/kafka.rs +
controller/kafka_node_pool.rs) + own-file construction-site fixtures
in controller/listeners.rs picked up the 3 new None defaults.

4 new render unit tests + 1 new cross-mode validation test. T2's
struct change cascades here per the 49g/49h pattern.

Co-Authored-By: Claude Opus 4.7 (1M context) <noreply@anthropic.com>
EOF
)"
```

---

### Batch 3 — T4 ‖ T5 (file-disjoint parallel)

#### Task T4: Operator integration tests + sample + CRD regen

**Files:**
- Modify: `crates/operator/tests/reconcile_listener_oauth.rs`
- Modify: `crates/operator/tests/reconcile_oauth_introspection.rs`
- Modify: `crates/operator/tests/reconcile_oauth_trust.rs`
- Modify: `crates/operator/sample/oauth-listener.yaml`
- Modify: `deploy/crds/crabka.io_kafkas.yaml` (regenerated)

**Race awareness:** T5 is running in parallel; touches only `.github/workflows/operator-e2e.yml`. File-disjoint.

- [ ] **Step 1: Sweep integration-test fixtures**

```bash
cd /Users/mattstone/git/crabka/.worktrees/slice-49i-oauth-jwks-policies
grep -n "ListenerAuthenticationOAuth {" crates/operator/tests/reconcile_listener_oauth.rs \
    crates/operator/tests/reconcile_oauth_introspection.rs \
    crates/operator/tests/reconcile_oauth_trust.rs
```

Per the explore: 5 + 2 + 1 = 8 sites. Add the 3 new `None` defaults to each.

- [ ] **Step 2: Add 2 new integration tests in `reconcile_listener_oauth.rs`**

```rust
#[tokio::test]
async fn oauth_listener_with_jwks_policies_renders_broker_toml_keys() {
    let mut cfg = oauth_cfg_minimal();
    cfg.jwks_min_refresh_pause_seconds = Some(2);
    cfg.jwks_expiry_seconds = Some(3600);
    cfg.jwks_ignore_key_use = Some(true);
    // ... build Kafka CR, reconcile, extract broker0 TOML
    let toml = /* extract */;
    assert!(toml.contains("jwks_min_refresh_pause_seconds = 2"));
    assert!(toml.contains("jwks_expiry_seconds = 3600"));
    assert!(toml.contains("jwks_ignore_key_use = true"));
}

#[tokio::test]
async fn oauth_listener_jwks_fields_in_introspection_mode_rejected_with_listeners_valid_false() {
    let mut cfg = oauth_cfg_minimal();
    cfg.access_token_is_jwt = false;
    cfg.jwks_endpoint_uri = None;
    cfg.introspection_endpoint_uri = Some("https://iss.example/introspect".into());
    cfg.client_id = Some("kafka-broker".into());
    cfg.client_secret = Some(crabka_operator::crd::OauthClientSecretRef {
        secret_name: "creds".into(),
        key: "client-secret".into(),
    });
    cfg.jwks_expiry_seconds = Some(3600); // The violation
    // ... reconcile, expect ListenersValid=False reason
    //     ListenerOauthJwksFieldsRejectedInIntrospectionMode
    // Mirror the existing slice 49g/49h cross-mode-rejection test pattern.
}
```

Mirror exact `reconcile` + extract-broker0-toml pattern from existing tests (look at slice 49h's `oauth_listener_with_groups_claim_renders_broker_toml_key` for the template).

- [ ] **Step 3: Run new integration tests — verify pass**

```bash
cargo test -p crabka-operator --test reconcile_listener_oauth oauth_listener_with_jwks_policies 2>&1 | tail
cargo test -p crabka-operator --test reconcile_listener_oauth oauth_listener_jwks_fields_in_introspection 2>&1 | tail
```

Expected: both pass.

- [ ] **Step 4: Update sample manifest**

Edit `crates/operator/sample/oauth-listener.yaml`. AFTER the existing 49h commented-out hints (around line 28), ADD:

```yaml
        # Slice 49i: JWKS refresher tuning (JWT-mode only — rejected on
        # introspection-mode listeners).
        #
        # jwksMinRefreshPauseSeconds: 1   # rate-limit on on-demand refreshes
        # jwksExpirySeconds: 3600         # hard cache expiry; fails closed on IdP outage
        # jwksIgnoreKeyUse: false         # default; set true for IdPs that mis-tag signing keys
```

Verify YAML still parses to 3 docs:

```bash
cat crates/operator/sample/oauth-listener.yaml | python3 -c "import sys, yaml; docs = list(yaml.safe_load_all(sys.stdin)); print(f'{len(docs)} docs: {[d.get(\"kind\") for d in docs]}')"
```

Expected: `3 docs: ['Kafka', 'KafkaNodePool', 'KafkaUser']`.

- [ ] **Step 5: Regenerate CRDs**

```bash
bash tools/regen-crds.sh 2>&1 | tail -10
git diff --stat deploy/crds/
git diff deploy/crds/crabka.io_kafkas.yaml | grep -B 1 -A 5 "jwksMinRefreshPauseSeconds\|jwksExpirySeconds\|jwksIgnoreKeyUse" | head -40
```

Expected: ONLY `deploy/crds/crabka.io_kafkas.yaml` changed. 3 new property entries with correct types + minimums.

- [ ] **Step 6: Run full operator test suite + fmt + clippy**

```bash
cargo test -p crabka-operator 2>&1 | tail -15
cargo fmt -p crabka-operator -- --check
cargo clippy -p crabka-operator --tests -- -D warnings 2>&1 | tail
bash tools/regen-crds.sh && git diff --exit-code -- deploy/crds/ ; echo "exit: $?"
```

All clean.

- [ ] **Step 7: Commit**

```bash
git add crates/operator/tests/reconcile_listener_oauth.rs \
        crates/operator/tests/reconcile_oauth_introspection.rs \
        crates/operator/tests/reconcile_oauth_trust.rs \
        crates/operator/sample/oauth-listener.yaml \
        deploy/crds/crabka.io_kafkas.yaml
git -c user.name="Matt Stone" -c user.email="matthew.d.stone@gmail.com" commit -m "$(cat <<'EOF'
T4: operator integration tests + sample + CRD regen for slice 49i

Sweeps 8 fixture sites across 3 integration test files (5 in
reconcile_listener_oauth, 2 in reconcile_oauth_introspection, 1 in
reconcile_oauth_trust): adds the 3 new None defaults.

2 new integration tests:
- oauth_listener_with_jwks_policies_renders_broker_toml_keys
- oauth_listener_jwks_fields_in_introspection_mode_rejected_with_listeners_valid_false

Sample manifest: commented-out examples for all 3 new fields with
explanatory comments (rate-limit, fail-closed-on-outage,
mis-tagged-key fallback).

CRDs regenerated; only deploy/crds/crabka.io_kafkas.yaml changed
(3 new properties: 2 integers with minimums + 1 boolean).

Co-Authored-By: Claude Opus 4.7 (1M context) <noreply@anthropic.com>
EOF
)"
```

---

#### Task T5: kind-oauth e2e CR YAML extension

**Files:**
- Modify: `.github/workflows/operator-e2e.yml`

**Race awareness:** T4 in parallel; file-disjoint.

**CRITICAL: Do NOT touch the `kind-oauth-introspection` job.** The 3 fields are JWT-mode only; cross-mode validator (T3 step 5) rejects them on introspection-mode listeners. Adding to introspection would break the e2e at the reconcile step.

- [ ] **Step 1: Locate the kind-oauth job's CR YAML**

```bash
cd /Users/mattstone/git/crabka/.worktrees/slice-49i-oauth-jwks-policies
grep -n "^  kind-oauth:\|authentication:\|groupsClaim:" .github/workflows/operator-e2e.yml | head -20
```

Find the `kind-oauth` job's `authentication:` block in the `kubectl apply -f -` heredoc (post-49h includes `groupsClaim: "$.realm_access.roles[*]"`).

- [ ] **Step 2: Add 3 new fields to kind-oauth's CR YAML**

After the existing `groupsClaim:` line (per slice 49h), ADD (matching 18-space indent of siblings):

```yaml
                  jwksMinRefreshPauseSeconds: 1
                  jwksExpirySeconds: 3600
                  jwksIgnoreKeyUse: false
```

**Values chosen:**
- `jwksMinRefreshPauseSeconds: 1`: Strimzi default; exercises the wire without rate-limiting anything in normal flow.
- `jwksExpirySeconds: 3600`: 1 hour — far longer than the e2e runtime (~minutes). Keycloak stays reachable throughout; the field exercises the wire without ever triggering the fail-closed path.
- `jwksIgnoreKeyUse: false`: explicit default; exercises the wire.

DO NOT add these to the `kind-oauth-introspection` job (lines 2618+ per slice 49h's e2e layout). The cross-mode validator rejects them on introspection-mode listeners.

- [ ] **Step 3: Verify YAML parses + actionlint clean**

```bash
python3 -c "
import yaml
w = yaml.safe_load(open('.github/workflows/operator-e2e.yml'))
print('jobs:', list(w['jobs'].keys()))
"
which actionlint && actionlint .github/workflows/operator-e2e.yml 2>&1 | head -20 || echo "actionlint not installed"
```

Expected: parses cleanly. Pre-existing warnings fine; no new warnings near edit site.

- [ ] **Step 4: Commit**

```bash
git add .github/workflows/operator-e2e.yml
git -c user.name="Matt Stone" -c user.email="matthew.d.stone@gmail.com" commit -m "$(cat <<'EOF'
T5: kind-oauth e2e — JWKS refresher policy fields end-to-end

Adds jwksMinRefreshPauseSeconds + jwksExpirySeconds + jwksIgnoreKeyUse
to the kind-oauth job's Kafka CR YAML. Values chosen so the e2e never
triggers fail-closed paths (jwksExpirySeconds: 3600 >> e2e runtime;
jwksIgnoreKeyUse: false matches Keycloak's default `use=sig` keys).

DO NOT add these to kind-oauth-introspection (separate job at ~line
2618+): the 3 fields are JWT-mode only; cross-mode validator from T3
rejects them on accessTokenIsJwt:false listeners. Adding would break
the introspection e2e at reconcile.

Producer Jobs unchanged.

Co-Authored-By: Claude Opus 4.7 (1M context) <noreply@anthropic.com>
EOF
)"
```

---

### Batch 4 — T6 (alone)

#### Task T6: STATUS.md + final gate + OAUTHBEARER umbrella completion note

**Files:**
- Modify: `STATUS.md`

- [ ] **Step 1: Read slice 49h's STATUS entry for tone**

```bash
grep -n "^## Slice 49h " STATUS.md
```

- [ ] **Step 2: Append slice 49i entry + umbrella completion note**

Append:

```markdown
## Slice 49i — Operator + Broker: OAUTHBEARER JWKS refresher policies (2026-05-24)

LAST OAUTHBEARER umbrella slice. After this lands, Strimzi field parity
reached (modulo the explicitly-skipped slice 49f, PLAIN-with-OAuth-token).

Adds 3 Strimzi-shape JWKS operational tuning fields on the listener
OAuth CRD + broker JWKS refresher:

- **`jwksMinRefreshPauseSeconds`**: rate-limits on-demand JWKS refresh
  triggered by tokens with unknown `kid`. Strimzi default 1.
- **`jwksExpirySeconds`**: hard cache expiry. `SignedJwsValidator`
  rejects tokens when the JWKS hasn't been successfully refreshed
  within this window. Fails closed on IdP outage. Strimzi default
  360 (6 minutes).
- **`jwksIgnoreKeyUse`**: filter toggle. Default `false` filters out
  JWKS keys with `use=enc`; setting `true` keeps all keys regardless.

- **Broker JWKS refresher (`crates/broker/src/oauth_jwks.rs`)**:
  `JwksRefresher` loop becomes 3-arm `tokio::select!` over
  periodic-tick + `signal_rx.recv()` + cancellation. On-demand
  refresh fires only when `now - last_on_demand_refresh >=
  min_on_demand_pause`; signals coalesce via mpsc capacity-1
  `try_send`. `last_successful_fetch_ms` advances only on success
  so the cache ages toward expiry on persistent failure.
- **JwksHandle (`crates/security/src/jwks.rs`)**: gained
  `last_successful_fetch_ms: Arc<AtomicI64>` and
  `signal_tx: Option<mpsc::Sender<()>>` fields. New constructor
  `JwksHandle::new_with_refresher_handles` for the wired path; the
  default constructor leaves both `None`/sentinel for non-paired
  validators.
- **SignedJwsValidator (`crates/security/src/oauthbearer.rs`)**:
  new `expiry_ms: Option<i64>` field. `validate()` pre-checks
  `now - last_successful_fetch > expiry_ms` (reject if stale);
  signal-on-verify-failure pattern (any `verify()` error fires
  `signal_refresh()` then returns the error). Unsecured-JWS +
  Introspection validators untouched (don't consult JWKS).
- **Operator CRD (`crates/operator/src/crd/listener.rs`)**: 3 new
  `Option<>` fields. Hand-rolled schema: 2 integers with
  `minimum: 0` / `minimum: 1` + 1 boolean.
- **Operator reconciler (`crates/operator/src/controller/listeners.rs`)**:
  `render_broker_toml` emits 3 new TOML keys when set. New cross-mode
  validation `ListenerOauthJwksFieldsRejectedInIntrospectionMode`
  fires when any of the 3 fields is set on an `accessTokenIsJwt:
  false` listener (operator-side feedback rather than silent broker-side
  no-op). Cross-listener divergence walk extended with 3 new
  perturbations.
- **`ListenerAuthenticationOAuth` cascade (CLAUDE.md greenfield rule):**
  T2/T3/T4 swept ~21 fixture sites for the 3 new `None` defaults:
  ~12 in `crd/listener.rs` + own-file construction sites in
  `controller/listeners.rs` + 4 + 3 in sibling controller files +
  5 + 2 + 1 across `tests/reconcile_*.rs`.
- **E2E (`.github/workflows/operator-e2e.yml`)**: `kind-oauth` job's
  Kafka CR YAML adds the 3 fields. `kind-oauth-introspection` job
  NOT touched — cross-mode validator would reject.
- **Tests**: ~15 new (3 JWKS parser filter + 4 refresher behavior +
  4 SignedJwsValidator + 2 CRD round-trip + 4 reconciler unit + 1
  cross-mode validation + 2 operator integration). Extended
  divergence walk (now 18 perturbations). Workspace fmt + clippy
  `-D warnings` + tests + CRD drift gate all green.
- **Reference doc**:
  `[docs/superpowers/specs/2026-05-24-crabka-oauth-jwks-refresher-policies-49i-design.md]`
- **Architecture choice**: Approach A (fire-and-forget mpsc signal).
  Validator stays sync; refresher consumes signals in its
  `tokio::select!` loop. Rejected Approach B (async-await on
  validator) and Approach C (skip on-demand refresh) for API-shape
  and Strimzi-parity reasons.
- **Out of scope (deferred or never):** per-listener JWKS refreshers
  (broker still has one global `[oauthbearer]` block); reconcile-time
  validation against the actual IdP (operator just renders); slice 49f
  (PLAIN-with-OAuth-token, indefinitely skipped).

### OAUTHBEARER umbrella complete

After 49i lands, the OAUTHBEARER umbrella shipped 9 slices over
the past month:

- 49 / 49b: wire + JWKS validator (broker).
- 50: KafkaUser tls-external + listener OAuth surface (operator).
- 49c / 50b: TLS trust to IdP (broker + operator).
- 49d / 50c: opaque-token introspection (broker + operator).
- 49e / 50d: KIP-368 SASL re-auth + session-lifetime cap (broker + operator).
- 49g: customClaimCheck JsonPath + validTokenType (broker + operator).
- 49h: claims mapping — fallback chain + groups extraction (broker + operator).
- 49i: JWKS refresher policies (broker + operator). **THIS SLICE.**

Strimzi `KafkaListenerAuthenticationOAuth` field parity reached
(modulo intentionally-skipped slice 49f PLAIN-with-OAuth-token,
which gates clients that can't speak OAUTHBEARER — re-evaluate if
a user reports needing it).

Next umbrella per the operator roadmap: slices 51+ (delegation
tokens, GSSAPI/Kerberos, OPA/Keycloak authorizer plugins). Slices
53/54 will CONSUME the scaffolding 49g/49h/49d laid down
(`Principal.groups`, `customClaimCheck` evaluation results,
introspection metadata).
```

- [ ] **Step 3: Final gate**

```bash
cd /Users/mattstone/git/crabka/.worktrees/slice-49i-oauth-jwks-policies
cargo fmt --check 2>&1 | tail -3
cargo clippy --workspace --all-targets -- -D warnings 2>&1 | tail -30
cargo test --workspace 2>&1 | tail -30
bash tools/regen-crds.sh && git diff --exit-code -- deploy/crds/ ; echo "exit: $?"
```

All four must be green. Known pre-existing flake:
`auto_rebalance_restores_preferred_leader` in
`crates/broker/tests/elect_leaders.rs`. Re-run in isolation if it
fires:

```bash
cargo test -p crabka-broker --test elect_leaders auto_rebalance_restores_preferred_leader 2>&1 | tail
```

If out-of-slice clippy errors exist, flag (don't fix in T6).

- [ ] **Step 4: Commit**

```bash
git add STATUS.md
git -c user.name="Matt Stone" -c user.email="matthew.d.stone@gmail.com" commit -m "$(cat <<'EOF'
Slice 49i: STATUS.md entry + final gate + OAUTHBEARER umbrella complete

Documents the new operator + broker JWKS refresher policy surface:
jwksMinRefreshPauseSeconds (on-demand refresh rate-limit),
jwksExpirySeconds (hard cache expiry), jwksIgnoreKeyUse (filter
toggle). SignedJwsValidator + JwksRefresher + JwksHandle wiring.

Also documents OAUTHBEARER umbrella completion: 9 slices over the
past month bring the operator's listener OAuth surface to Strimzi
field parity (modulo intentionally-skipped 49f).

fmt + clippy + workspace tests + CRD drift gate all green.

Co-Authored-By: Claude Opus 4.7 (1M context) <noreply@anthropic.com>
EOF
)"
```

---

## Notes

- **Dependency chain:** T1 → T2 → T3 → (T4 ‖ T5) → T6. Six tasks, four batches. T2 + T3 file-disjoint but dispatched sequentially (T3's fixture sweep needs T2's struct field present). T4 + T5 truly parallel.
- **T1 is largest** — re-works the slice-49b refresher loop, threads a new mpsc channel from `apply_to` through `BrokerConfig` to `Broker::start`, AND adds the validator-side expiry-check + signal-on-failure plumbing. ~6 broker files touched. Allocate plenty of subagent budget.
- **T5 e2e lesson from 49g + 49h**: introspection-job CR YAML is a sibling that must be deliberately considered. For 49i: explicitly do NOT touch it (cross-mode validator rejects). T5 prompt must state this loudly.
- **CLAUDE.md greenfield**: no compat shims. The 3 new fields are required-OR-default on the broker side (`apply_to` reads `.unwrap_or(...)`). The CRD fields are all `Option<>`; absent means "use broker default".
- **Channel capacity 1 + try_send**: this is the coalescing design. If 100 bad tokens arrive in 1ms, the FIRST one fills the channel; the next 99 `try_send` calls fail silently. The refresher consumes one signal + does one refresh. That's correct behavior (we only need to know "at least one bad token happened recently").
- **`refresh_and_swap` called from BOTH arms**: the periodic-tick arm and the on-demand-signal arm both call the same `refresh_and_swap` helper (extracted from the original inline body). The periodic arm doesn't check `last_on_demand_refresh` — periodic is independent of on-demand and runs on its own cadence.
- **After 49i lands**: the OAUTHBEARER umbrella is done. Next brainstorming should pull from the operator roadmap (slices 51+). The natural next slice depends on user demand — delegation tokens (51) probably most universal-broker-compat-relevant.
