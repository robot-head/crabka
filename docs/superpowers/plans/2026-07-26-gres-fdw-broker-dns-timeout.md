# Gres FDW Broker DNS Timeout Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Bound every Kafka broker DNS lookup made by the Gres FDW with one validated process policy exposed through Gres CLI/environment configuration and `Gres.spec.compute`.

**Architecture:** Reuse `crabka_client_core::ClientDnsTimeout` from the configuration boundary through the process-owned `KafkaFdw`. Add one secured admin constructor that changes only DNS policy, and bound the FDW raw `lookup_host` before connecting by `SocketAddr`; keep catalog connection profiles and existing public default entry points compatible.

**Tech Stack:** Rust, Tokio, Clap, `refined_type`, kube/schemars CRDs, Cargo tests and Clippy.

## Global Constraints

- Preserve `ClientDnsTimeout::default()` at exactly 10,000 ms.
- Use exact CLI name `--fdw-broker-dns-timeout-ms`.
- Use exact environment name `CRABKA_GRES_FDW_BROKER_DNS_TIMEOUT_MS`.
- Use exact CRD field `spec.compute.fdwBrokerDnsTimeoutMs`.
- CLI precedence is CLI over environment over typed default.
- The setting is valid in local and substrate modes; do not add `requires = "substrate_bootstrap"`.
- Zero must fail at the parsing/CRD validation boundary before broker or Kubernetes resource I/O.
- Do not add a foreign-server option, schema-registry HTTP DNS policy, resolver abstraction, dependency, or new validation type.
- Preserve TLS/SASL, bootstrap ordering, first-address selection, and existing catalog semantics.
- Preserve unrelated dirty and untracked files; stage only files listed by the current task.
- Run every Cargo command with `CARGO_PROFILE_DEV_DEBUG=0 CARGO_INCREMENTAL=0`.
- Add `--locked` to every Cargo command that accepts it.

---

### Task 1: Secured Admin DNS Constructor

**Files:**
- Modify: `crates/client-admin/src/lib.rs`

**Interfaces:**
- Consumes: existing `AdminClient::opts`, `AdminClient::connect_with_options`, `ClientSecurity`, and `ClientDnsTimeout`.
- Produces:
  ```rust
  pub async fn AdminClient::connect_secured_with_dns_timeout(
      bootstrap_addrs: &[String],
      security: Option<crabka_client_core::security::ClientSecurity>,
      dns_timeout: crabka_client_core::ClientDnsTimeout,
  ) -> Result<Self, AdminError>
  ```
- Preserves `connect_secured` and `connect_with_dns_timeout`; the latter delegates to the new method with `None` security.

- [ ] **Step 1: Add the failing secured-options test**

  Extend the existing admin test module with a live `ObservedAdminBroker` test:

  ```rust
  #[tokio::test]
  async fn secured_dns_timeout_preserves_security_and_admin_defaults() {
      let live = ObservedAdminBroker::start(Duration::ZERO).await;
      let timeout = crabka_client_core::ClientDnsTimeout::new(Duration::from_millis(37))
          .expect("positive timeout");
      let security = ClientSecurity {
          protocol: ListenerProtocol::SaslPlaintext,
          tls: None,
          sasl: Some(SaslCredentials::Plain {
              username: "u".into(),
              password: "p".into(),
          }),
          sasl_host: Some("broker.example".into()),
      };
      let admin = AdminClient::connect_secured_with_dns_timeout(
          &[live.addr.to_string()],
          Some(security),
          timeout,
      )
      .await
      .expect("secured admin connects");

      assert2::assert!(admin.options.dns_timeout == timeout);
      assert2::assert!(admin.options.security.is_some());
      assert2::assert!(admin.options.client_id == "crabka-operator");
      assert2::assert!(admin.options.connect_timeout == Duration::from_secs(5));
      assert2::assert!(admin.options.request_timeout == Duration::from_secs(30));
      live.stop();
  }
  ```

- [ ] **Step 2: Run the test and confirm the missing method fails compilation**

  Run:

  ```bash
  CARGO_PROFILE_DEV_DEBUG=0 CARGO_INCREMENTAL=0 cargo test \
    -p crabka-client-admin --locked \
    secured_dns_timeout_preserves_security_and_admin_defaults
  ```

  Expected: compile failure naming `connect_secured_with_dns_timeout`.

- [ ] **Step 3: Implement the minimal constructor and delegation**

  Add beside the existing constructors:

  ```rust
  pub async fn connect_secured_with_dns_timeout(
      bootstrap_addrs: &[String],
      security: Option<crabka_client_core::security::ClientSecurity>,
      dns_timeout: crabka_client_core::ClientDnsTimeout,
  ) -> Result<Self, AdminError> {
      let mut options = Self::opts(security);
      options.dns_timeout = dns_timeout;
      Self::connect_with_options(bootstrap_addrs, options).await
  }
  ```

  Change `connect_with_dns_timeout` to:

  ```rust
  Self::connect_secured_with_dns_timeout(bootstrap_addrs, None, dns_timeout).await
  ```

  Document that the secured constructor preserves the standard admin identity,
  TCP-connect timeout, request timeout, and supplied security.

- [ ] **Step 4: Run the complete package gates**

  ```bash
  CARGO_PROFILE_DEV_DEBUG=0 CARGO_INCREMENTAL=0 cargo test \
    -p crabka-client-admin --all-targets --locked
  CARGO_PROFILE_DEV_DEBUG=0 CARGO_INCREMENTAL=0 cargo clippy \
    -p crabka-client-admin --all-targets --locked -- -D warnings
  CARGO_PROFILE_DEV_DEBUG=0 CARGO_INCREMENTAL=0 cargo fmt --all -- --check
  git diff --check
  ```

  Expected: all commands exit 0.

- [ ] **Step 5: Commit only the admin change**

  ```bash
  git add -- crates/client-admin/src/lib.rs
  git commit -m "feat(admin): secure custom DNS timeout"
  ```

---

### Task 2: Carry and Enforce FDW Broker DNS Policy

**Files:**
- Modify: `crates/gres-fdw/src/lib.rs`
- Modify: `crates/gres-fdw/src/source.rs`

**Interfaces:**
- Consumes: Task 1
  `AdminClient::connect_secured_with_dns_timeout` and existing
  `ClientDnsTimeout`.
- Produces:
  ```rust
  pub fn KafkaFdw::with_broker_dns_timeout(
      self,
      timeout: crabka_client_core::ClientDnsTimeout,
  ) -> Self

  pub fn KafkaFdw::broker_dns_timeout(
      &self,
  ) -> crabka_client_core::ClientDnsTimeout

  pub async fn source::scan_topic_with_dns_timeout(
      profile: &ConnProfile,
      topic: &str,
      bounds: &ScanBounds,
      dns_timeout: crabka_client_core::ClientDnsTimeout,
  ) -> Result<Vec<RawRecord>, KafkaFdwError>
  ```
- Preserves `KafkaFdw::with_defaults(default_bootstrap)` and
  `source::scan_topic(profile, topic, bounds)` by delegating with the typed
  default.

- [ ] **Step 1: Add failing policy and raw-timeout tests**

  In `crates/gres-fdw/src/lib.rs`, add:

  ```rust
  #[test]
  fn fdw_carries_typed_broker_dns_timeout() {
      let timeout = crabka_client_core::ClientDnsTimeout::new(
          std::time::Duration::from_millis(37),
      )
      .expect("positive timeout");
      let fdw = KafkaFdw::with_defaults(Some("broker:9092".into()))
          .with_broker_dns_timeout(timeout);

      assert_eq!(fdw.default_bootstrap(), Some("broker:9092"));
      assert_eq!(fdw.broker_dns_timeout(), timeout);
  }
  ```

  In `crates/gres-fdw/src/source.rs`, add a paused-clock test for the extracted
  lookup seam:

  ```rust
  #[tokio::test(start_paused = true)]
  async fn raw_dns_lookup_stops_at_configured_deadline() {
      let timeout = crabka_client_core::ClientDnsTimeout::new(
          Duration::from_millis(37),
      )
      .expect("positive timeout");
      let started = tokio::time::Instant::now();
      let pending =
          std::future::pending::<std::io::Result<std::vec::IntoIter<std::net::SocketAddr>>>();

      let error = lookup_first("broker.example:9092", timeout, pending)
          .await
          .expect_err("lookup times out");

      assert_eq!(tokio::time::Instant::now() - started, Duration::from_millis(37));
      assert!(error.to_string().contains(
          "DNS lookup broker.example:9092 timed out after 37 ms"
      ));
  }
  ```

- [ ] **Step 2: Run the focused tests and confirm missing interfaces fail**

  ```bash
  CARGO_PROFILE_DEV_DEBUG=0 CARGO_INCREMENTAL=0 cargo test \
    -p crabka-gres-fdw --lib --locked fdw_carries_typed_broker_dns_timeout
  CARGO_PROFILE_DEV_DEBUG=0 CARGO_INCREMENTAL=0 cargo test \
    -p crabka-gres-fdw --lib --locked raw_dns_lookup_stops_at_configured_deadline
  ```

  Expected: compile failures naming the missing builder/accessor and
  `lookup_first`.

- [ ] **Step 3: Store the typed process policy on `KafkaFdw`**

  Extend the struct without changing the existing constructor:

  ```rust
  pub struct KafkaFdw {
      default_bootstrap: Option<String>,
      broker_dns_timeout: crabka_client_core::ClientDnsTimeout,
  }
  ```

  `with_defaults` initializes `broker_dns_timeout` with
  `ClientDnsTimeout::default()`. Implement the builder and accessor signatures
  above. Do not put the timeout into `ConnProfile`.

  In `ForeignScanner::scan`, call `scan_topic_with_dns_timeout` with
  `self.broker_dns_timeout`. In `import_schema`, replace
  `AdminClient::connect_secured` with
  `AdminClient::connect_secured_with_dns_timeout`, passing the same field.

- [ ] **Step 4: Bound both scan DNS paths**

  Preserve `scan_topic` as a default wrapper:

  ```rust
  pub async fn scan_topic(
      profile: &ConnProfile,
      topic: &str,
      bounds: &ScanBounds,
  ) -> Result<Vec<RawRecord>, KafkaFdwError> {
      scan_topic_with_dns_timeout(
          profile,
          topic,
          bounds,
          crabka_client_core::ClientDnsTimeout::default(),
      )
      .await
  }
  ```

  Move the current body to `scan_topic_with_dns_timeout`. Use
  `AdminClient::connect_secured_with_dns_timeout` for metadata.

  Extract the raw seam:

  ```rust
  async fn lookup_first<F, I>(
      host_port: &str,
      dns_timeout: crabka_client_core::ClientDnsTimeout,
      lookup: F,
  ) -> Result<std::net::SocketAddr, KafkaFdwError>
  where
      F: std::future::Future<Output = std::io::Result<I>>,
      I: Iterator<Item = std::net::SocketAddr>,
  {
      let mut addrs = tokio::time::timeout(dns_timeout.duration(), lookup)
          .await
          .map_err(|_| {
              KafkaFdwError::Other(format!(
                  "DNS lookup {host_port} timed out after {} ms",
                  dns_timeout.milliseconds(),
              ))
          })?
          .map_err(|error| {
              KafkaFdwError::Other(format!("DNS lookup {host_port}: {error}"))
          })?;
      addrs
          .next()
          .ok_or_else(|| KafkaFdwError::Other(format!("no addresses for {host_port}")))
  }
  ```

  Pass the timeout into `open_connection`, call:

  ```rust
  let addr = lookup_first(host_port, dns_timeout, tokio::net::lookup_host(host_port)).await?;
  ```

  Remove the misleading explicit post-resolution `dns_timeout` assignment.
  Preserve every effective non-DNS option with this exact literal:

  ```rust
  let options = crabka_client_core::ConnectionOptions {
      client_id: "crabka-fdw".to_string(),
      connect_timeout: std::time::Duration::from_secs(10),
      request_timeout: std::time::Duration::from_secs(30),
      security: profile.security.clone().map(Box::new),
      ..crabka_client_core::ConnectionOptions::default()
  };
  ```

- [ ] **Step 5: Run package gates including the unchanged roundtrip API**

  The existing roundtrip helper remains on the default policy:

  ```rust
  KafkaFdw::with_defaults(default_bootstrap)
  ```

  No roundtrip behavior or catalog SQL changes are expected.

  ```bash
  CARGO_PROFILE_DEV_DEBUG=0 CARGO_INCREMENTAL=0 cargo test \
    -p crabka-gres-fdw --all-targets --locked
  CARGO_PROFILE_DEV_DEBUG=0 CARGO_INCREMENTAL=0 cargo clippy \
    -p crabka-gres-fdw --all-targets --locked -- -D warnings
  CARGO_PROFILE_DEV_DEBUG=0 CARGO_INCREMENTAL=0 cargo fmt --all -- --check
  git diff --check
  ```

  Expected: all commands exit 0.

- [ ] **Step 6: Commit only the FDW runtime change**

  ```bash
  git add -- \
    crates/gres-fdw/src/lib.rs \
    crates/gres-fdw/src/source.rs
  git commit -m "fix(gres-fdw): bound broker DNS"
  ```

---

### Task 3: Expose Standalone Gres Configuration

**Files:**
- Modify: `crates/gres/src/lib.rs`
- Modify: `crates/gres/tests/runtime.rs`

**Interfaces:**
- Consumes: Task 2 `KafkaFdw::with_broker_dns_timeout`.
- Produces:
  ```rust
  pub ServeArgs::fdw_broker_dns_timeout_ms: Option<PositiveMillis>

  fn effective_fdw_broker_dns_timeout(
      args: &ServeArgs,
  ) -> std::io::Result<crabka_client_core::ClientDnsTimeout>
  ```
- `register_kafka_scanner_with_default_bootstrap` gains a
  `ClientDnsTimeout` argument; `register_kafka_scanner` remains a typed-default
  compatibility entry point.

- [ ] **Step 1: Add failing CLI boundary and precedence tests**

  Add a local-mode child-process environment test following the existing WAL
  DNS test pattern:

  ```rust
  #[test]
  fn fdw_broker_dns_timeout_uses_default_environment_and_cli_precedence() {
      const CHILD: &str = "CRABKA_TEST_GRES_FDW_BROKER_DNS_TIMEOUT_CHILD";
      const ENV: &str = "CRABKA_GRES_FDW_BROKER_DNS_TIMEOUT_MS";
      if std::env::var_os(CHILD).is_none() {
          for mode in ["defaults", "environment"] {
              let mut child =
                  std::process::Command::new(std::env::current_exe().expect("test exe"));
              child
                  .args([
                      "--exact",
                      "tests::fdw_broker_dns_timeout_uses_default_environment_and_cli_precedence",
                  ])
                  .env(CHILD, mode)
                  .env_remove(ENV);
              if mode == "environment" {
                  child.env(ENV, "27");
              }
              assert!(child.status().expect("child test").success());
          }
          return;
      }

      let args = <Cli as clap::Parser>::try_parse_from(["crabka-gres"])
          .expect("default FDW DNS timeout")
          .serve;
      let expected_ms = if std::env::var(CHILD).as_deref() == Ok("environment") {
          27
      } else {
          crabka_client_core::ClientDnsTimeout::default().milliseconds()
      };
      assert_eq!(
          effective_fdw_broker_dns_timeout(&args)
              .expect("valid FDW DNS timeout")
              .milliseconds(),
          expected_ms
      );

      let args = <Cli as clap::Parser>::try_parse_from([
          "crabka-gres",
          "--fdw-broker-dns-timeout-ms=37",
      ])
      .expect("CLI FDW DNS timeout")
      .serve;
      assert_eq!(
          effective_fdw_broker_dns_timeout(&args)
              .expect("valid FDW DNS timeout")
              .milliseconds(),
          37
      );
  }
  ```

  The finished test must use the same `std::process::Command` isolation as
  `wal_producer_dns_timeout_uses_defaults_environment_and_cli_precedence`, but
  it must parse local mode without substrate arguments.

  Add:

  ```rust
  #[test]
  fn fdw_broker_dns_timeout_rejects_zero_but_allows_local_mode() {
      Cli::try_parse_from(["crabka-gres", "--fdw-broker-dns-timeout-ms=0"])
          .expect_err("zero DNS timeout");
      Cli::try_parse_from(["crabka-gres", "--fdw-broker-dns-timeout-ms=1"])
          .expect("local FDW policy");
  }
  ```

- [ ] **Step 2: Run focused tests and confirm the flag is missing**

  ```bash
  CARGO_PROFILE_DEV_DEBUG=0 CARGO_INCREMENTAL=0 cargo test \
    -p crabka-gres --lib --locked \
    fdw_broker_dns_timeout_uses_default_environment_and_cli_precedence
  CARGO_PROFILE_DEV_DEBUG=0 CARGO_INCREMENTAL=0 cargo test \
    -p crabka-gres --lib --locked \
    fdw_broker_dns_timeout_rejects_zero_but_allows_local_mode
  ```

  Expected: failures because the new CLI option/interface does not exist.

- [ ] **Step 3: Add the validated CLI/environment field and effective helper**

  Add to `ServeArgs`, without a substrate requirement:

  ```rust
  /// Timeout for resolving Kafka broker hostnames used by the FDW.
  #[arg(
      long = "fdw-broker-dns-timeout-ms",
      env = "CRABKA_GRES_FDW_BROKER_DNS_TIMEOUT_MS"
  )]
  pub fdw_broker_dns_timeout_ms: Option<PositiveMillis>,
  ```

  Implement:

  ```rust
  fn effective_fdw_broker_dns_timeout(
      args: &ServeArgs,
  ) -> std::io::Result<crabka_client_core::ClientDnsTimeout> {
      args.fdw_broker_dns_timeout_ms.map_or_else(
          || Ok(crabka_client_core::ClientDnsTimeout::default()),
          |timeout| {
              crabka_client_core::ClientDnsTimeout::new(Duration::from_millis(
                  timeout.into_value(),
              ))
              .map_err(|error| {
                  std::io::Error::new(std::io::ErrorKind::InvalidInput, error)
              })
          },
      )
  }
  ```

  Call this helper from existing serve-argument validation so programmatic
  callers cannot bypass it.

- [ ] **Step 4: Carry the policy into scanner registration**

  At runtime startup, resolve once and pass the value:

  ```rust
  register_kafka_scanner_with_default_bootstrap(
      &mut runtime.engine,
      kafka_scanner_default_bootstrap(&effective_args),
      effective_fdw_broker_dns_timeout(&effective_args)?,
  );
  ```

  Configure the scanner with:

  ```rust
  crabka_gres_fdw::KafkaFdw::with_defaults(default_bootstrap)
      .with_broker_dns_timeout(broker_dns_timeout)
  ```

  Keep `register_kafka_scanner(&mut SqlEngine)` using
  `ClientDnsTimeout::default()`. Update the one live runtime-test call to
  `register_kafka_scanner_with_default_bootstrap` with the typed default.

- [ ] **Step 5: Update complete `ServeArgs` literals and verify**

  Add `fdw_broker_dns_timeout_ms: None` to complete literals in
  `crates/gres/src/lib.rs` and `crates/gres/tests/runtime.rs`; do not alter
  unrelated policy values.

  ```bash
  CARGO_PROFILE_DEV_DEBUG=0 CARGO_INCREMENTAL=0 cargo test \
    -p crabka-gres --all-targets --locked
  CARGO_PROFILE_DEV_DEBUG=0 CARGO_INCREMENTAL=0 cargo clippy \
    -p crabka-gres --all-targets --locked -- -D warnings
  CARGO_PROFILE_DEV_DEBUG=0 CARGO_INCREMENTAL=0 cargo run -q \
    -p crabka-gres --locked -- --help |
    rg -- '--fdw-broker-dns-timeout-ms'
  CARGO_PROFILE_DEV_DEBUG=0 CARGO_INCREMENTAL=0 cargo fmt --all -- --check
  git diff --check
  ```

  Expected: tests and Clippy pass; help contains the exact flag once.

- [ ] **Step 6: Commit only standalone Gres changes**

  ```bash
  git add -- crates/gres/src/lib.rs crates/gres/tests/runtime.rs
  git commit -m "feat(gres): expose FDW broker DNS"
  ```

---

### Task 4: Expose Operator CRD and Compute Rendering

**Files:**
- Modify: `crates/operator/src/crd/gres.rs`
- Modify: `crates/operator/src/controller/gres_tenant.rs`
- Modify: `deploy/crds/crabka.io_greses.yaml`

**Interfaces:**
- Consumes: Task 3 exact CLI flag and `ClientDnsTimeout`.
- Produces:
  ```rust
  pub GresComputeSpec::fdw_broker_dns_timeout_ms: Option<u64>
  pub(crate) EffectiveGresComputePolicy::fdw_broker_dns_timeout:
      crabka_client_core::ClientDnsTimeout
  ```

- [ ] **Step 1: Add failing CRD policy tests**

  Add a focused test in `crates/operator/src/crd/gres.rs`:

  ```rust
  #[test]
  fn fdw_broker_dns_timeout_has_exact_schema_default_override_and_error() {
      let crd = serde_json::to_value(Gres::crd()).expect("serialize Gres CRD");
      let field = &crd["spec"]["versions"][0]["schema"]["openAPIV3Schema"]
          ["properties"]["spec"]["properties"]["compute"]["properties"]
          ["fdwBrokerDnsTimeoutMs"];
      assert_eq!(field["minimum"].as_u64(), Some(1));

      let defaults = GresComputeSpec::default()
          .effective_policy()
          .expect("default policy");
      assert_eq!(
          defaults.fdw_broker_dns_timeout,
          crabka_client_core::ClientDnsTimeout::default()
      );

      let overridden = GresComputeSpec {
          fdw_broker_dns_timeout_ms: Some(37),
          ..GresComputeSpec::default()
      }
      .effective_policy()
      .expect("override");
      assert_eq!(overridden.fdw_broker_dns_timeout.milliseconds(), 37);

      let error = GresComputeSpec {
          fdw_broker_dns_timeout_ms: Some(0),
          ..GresComputeSpec::default()
      }
      .effective_policy()
      .expect_err("zero must fail");
      assert!(error.starts_with("spec.compute.fdwBrokerDnsTimeoutMs:"));
  }
  ```

- [ ] **Step 2: Add the failing exact-once render test**

  Add beside the WAL DNS rendering test in
  `crates/operator/src/controller/gres_tenant.rs`:

  ```rust
  #[test]
  fn fdw_broker_dns_timeout_is_exact_once_in_single_and_two_range_deployments() {
      let mut obj = tenant();
      obj.metadata.namespace = Some("ns".into());
      obj.metadata.uid = Some("uid".into());
      let ranges = [
          GresTenantRangeSpec {
              range_id: 0,
              end_key: Some(GresTenantRangeKey {
                  table_id: 10,
                  bucket: None,
                  rowid: 0,
              }),
          },
          GresTenantRangeSpec {
              range_id: 1,
              end_key: None,
          },
      ];
      let operator_config = ConfigArgs::parse_from(["operator"]).config;

      for (spec, pair) in [
          (
              crate::crd::gres::GresComputeSpec::default(),
              ["--fdw-broker-dns-timeout-ms", "10000"],
          ),
          (
              crate::crd::gres::GresComputeSpec {
                  fdw_broker_dns_timeout_ms: Some(37),
                  ..crate::crd::gres::GresComputeSpec::default()
              },
              ["--fdw-broker-dns-timeout-ms", "37"],
          ),
      ] {
          let compute_policy = spec.effective_policy().expect("compute policy");
          for (range_control_enabled, active_ranges) in
              [(false, &ranges[..1]), (true, &ranges[..])]
          {
              for range in active_ranges {
                  let wal_topic = format!("__gres_wal.tenant-a.r{}", range.range_id);
                  let deployment = render_deployment(
                      &obj,
                      range,
                      &DeploymentRenderConfig {
                          all_ranges: active_ranges,
                          image: "image",
                          readiness_probe_period_seconds: 5,
                          bootstrap: "k:9092",
                          wal_topic: &wal_topic,
                          config_topic: "__gres_cfg.tenant-a",
                          policy: &crabka_gres_control::RegistryPolicy::default(),
                          compute_policy,
                          replicas: 1,
                          operator_config: &operator_config,
                          kafka_sasl: false,
                          range_control_enabled,
                          range_tls_hash: None,
                      },
                  )
                  .expect("render deployment");
                  let args = deployment.spec.unwrap().template.spec.unwrap().containers[0]
                      .args
                      .expect("compute args");
                  assert!(
                      args.windows(2).filter(|window| *window == pair).count() == 1,
                      "expected {pair:?} exactly once, got: {args:?}"
                  );
              }
          }
      }
  }
  ```

  Extend `invalid_compute_policy_is_rejected_before_kafka_or_resource_io` with:

  ```rust
  GresComputeSpec {
      fdw_broker_dns_timeout_ms: Some(0),
      ..GresComputeSpec::default()
  }
  ```

  Require the existing fake Kubernetes/Kafka I/O counters to remain zero and
  the error to contain `spec.compute.fdwBrokerDnsTimeoutMs`.

- [ ] **Step 3: Run focused tests and confirm the field is missing**

  ```bash
  CARGO_PROFILE_DEV_DEBUG=0 CARGO_INCREMENTAL=0 cargo test \
    -p crabka-operator --lib --locked \
    fdw_broker_dns_timeout_has_exact_schema_default_override_and_error
  CARGO_PROFILE_DEV_DEBUG=0 CARGO_INCREMENTAL=0 cargo test \
    -p crabka-operator --lib --locked \
    fdw_broker_dns_timeout_is_exact_once_in_single_and_two_range_deployments
  ```

  Expected: compile failures naming the absent CRD/effective-policy fields.

- [ ] **Step 4: Implement CRD validation and effective policy**

  Add to `GresComputeSpec`:

  ```rust
  /// Timeout for resolving Kafka broker hostnames used by the FDW.
  #[serde(default, skip_serializing_if = "Option::is_none")]
  #[schemars(range(min = 1))]
  pub fdw_broker_dns_timeout_ms: Option<u64>,
  ```

  Add `fdw_broker_dns_timeout: ClientDnsTimeout` to
  `EffectiveGresComputePolicy`, then construct it with:

  ```rust
  fdw_broker_dns_timeout: crabka_client_core::ClientDnsTimeout::new(
      Duration::from_millis(self.fdw_broker_dns_timeout_ms.unwrap_or_else(|| {
          crabka_client_core::ClientDnsTimeout::default().milliseconds()
      })),
  )
  .map_err(|error| format!("spec.compute.fdwBrokerDnsTimeoutMs: {error}"))?,
  ```

- [ ] **Step 5: Render the exact argument pair**

  Add once to the base compute argument vector:

  ```rust
  "--fdw-broker-dns-timeout-ms".to_owned(),
  u64::to_string(&compute_policy.fdw_broker_dns_timeout.milliseconds()),
  ```

  Do not add it to per-range conditional extensions or any other container.

- [ ] **Step 6: Regenerate and compare all nine CRDs**

  ```bash
  crd_dir=$(mktemp -d /tmp/crabka-fdw-dns-crds.XXXXXX)
  CARGO_PROFILE_DEV_DEBUG=0 CARGO_INCREMENTAL=0 cargo run -q \
    -p crabka-operator --locked -- gen-crds "$crd_dir"
  test "$(find "$crd_dir" -maxdepth 1 -type f | wc -l)" -eq 9
  cp "$crd_dir/crabka.io_greses.yaml" deploy/crds/crabka.io_greses.yaml
  rm -rf -- "$crd_dir"
  ```

  Confirm only `crabka.io_greses.yaml` changes and it contains
  `fdwBrokerDnsTimeoutMs` with `minimum: 1`.

- [ ] **Step 7: Run full operator gates**

  ```bash
  CARGO_PROFILE_DEV_DEBUG=0 CARGO_INCREMENTAL=0 cargo test \
    -p crabka-operator --all-targets --locked
  CARGO_PROFILE_DEV_DEBUG=0 CARGO_INCREMENTAL=0 cargo clippy \
    -p crabka-operator --all-targets --locked -- -D warnings
  CARGO_PROFILE_DEV_DEBUG=0 CARGO_INCREMENTAL=0 cargo fmt --all -- --check
  git diff --check
  ```

  Expected: all commands exit 0.

- [ ] **Step 8: Commit only operator and generated-schema changes**

  ```bash
  git add -- \
    crates/operator/src/crd/gres.rs \
    crates/operator/src/controller/gres_tenant.rs \
    deploy/crds/crabka.io_greses.yaml
  git commit -m "feat(operator): expose FDW broker DNS"
  ```

---

### Task 5: Audit Evidence, Whole-Slice Review, and Publication

**Files:**
- Modify: `docs/configuration-audit.md`

**Interfaces:**
- Consumes: Tasks 1-4 complete runtime/configuration path.
- Produces: an auditable closure record for only this FDW broker DNS slice and
  identifies the next unresolved owner without claiming the repository-wide
  goal is complete.

- [ ] **Step 1: Run the repository runtime-value scanner**

  ```bash
  tools/audit-runtime-values.sh
  ```

  Record the exact line and file totals from current output.

- [ ] **Step 2: Classify the focused DNS search**

  ```bash
  rg -n \
    "lookup_host|ToSocketAddrs|ClientDnsTimeout|dns[_-]timeout|DnsTimeout|fdw-broker-dns-timeout|fdwBrokerDnsTimeoutMs" \
    crates deploy/crds docs/configuration-audit.md
  ```

  Classify every match as production, checked-in schema, test/harness, prior
  audit evidence, completed unrelated DNS policy, or unresolved owner. State
  the exact totals and name the next coherent unresolved owner from current
  evidence.

- [ ] **Step 3: Append the audit section**

  Add `## Gres FDW Broker DNS Timeout` covering:

  - exact CLI/environment/CRD names and 10,000-ms default;
  - CLI > environment > default precedence;
  - `Gres.spec.compute -> EffectiveGresComputePolicy -> rendered CLI ->
    ServeArgs -> KafkaFdw -> admin/raw lookup` flow;
  - scan and import coverage, TLS/SASL preservation, and timeout errors;
  - scanner and focused-search totals;
  - verification evidence from Tasks 1-4;
  - explicit statement that other FDW operational values and the
    repository-wide goal remain open.

- [ ] **Step 4: Run fresh final verification on the exact slice head**

  ```bash
  CARGO_PROFILE_DEV_DEBUG=0 CARGO_INCREMENTAL=0 cargo test \
    -p crabka-client-admin \
    -p crabka-gres-fdw \
    -p crabka-gres \
    -p crabka-operator \
    --all-targets --locked

  CARGO_PROFILE_DEV_DEBUG=0 CARGO_INCREMENTAL=0 cargo clippy \
    -p crabka-client-admin \
    -p crabka-gres-fdw \
    -p crabka-gres \
    -p crabka-operator \
    --all-targets --locked -- -D warnings

  CARGO_PROFILE_DEV_DEBUG=0 CARGO_INCREMENTAL=0 cargo run -q \
    -p crabka-gres --locked -- --help |
    rg -- '--fdw-broker-dns-timeout-ms'

  CARGO_PROFILE_DEV_DEBUG=0 CARGO_INCREMENTAL=0 cargo fmt --all -- --check
  git diff --check
  ```

  Generate two fresh nine-file CRD directories, require both counts to equal
  nine, compare them with `diff -ru`, compare one with `deploy/crds`, and
  remove only those exact temporary directories.

- [ ] **Step 5: Commit the audit evidence**

  ```bash
  git add -- docs/configuration-audit.md
  git commit -m "docs(gres): record FDW broker DNS"
  ```

- [ ] **Step 6: Review the frozen complete diff**

  Freeze the diff from the plan commit through Task 5. Review requirements
  against the approved design:

  - all FDW Kafka broker DNS paths are bounded;
  - one typed process value reaches scan metadata, raw scan resolution, and
    import metadata;
  - exact CLI/environment/CRD names and precedence;
  - zero fails before I/O;
  - default remains 10,000 ms;
  - security and ordering remain intact;
  - no foreign-server option or unrelated refactor;
  - tests prove behavior rather than only field presence.

  Resolve all Critical and Important findings, rerun affected gates, and
  repeat review until clean. Ledger non-blocking Minor findings explicitly.

- [ ] **Step 7: Publish to the existing draft PR**

  Confirm `git status -sb`, exact commits, exact file scope, `gh auth status`,
  and current branch. Push `configuration_expose` normally; do not force-push.
  Verify local `HEAD`, `git ls-remote origin
  refs/heads/configuration_expose`, and draft PR #904 `head_sha` are identical.
  Require PR #904 to remain open, draft, and mergeable.

- [ ] **Step 8: Continue the repository-wide audit**

  Do not mark the persistent goal complete. Start a new design cycle for the
  next coherent unresolved operational owner identified in Step 2.
