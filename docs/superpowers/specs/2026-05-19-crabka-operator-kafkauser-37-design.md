# Crabka Operator — Slice 37: KafkaUser mTLS authentication (design)

**Date:** 2026-05-19
**Status:** Plan-ready
**Scope:** Add the `tls` authentication variant to `KafkaUser`. Operator
issues a per-user X.509 client cert from a per-cluster clients CA and
publishes it via a Strimzi-shaped Secret (`user.crt`, `user.key`,
`ca.crt`). ACLs and quotas key off the cert's Subject DN.
**Depends on:** slice 36 (KafkaUser CRD + SCRAM + ACLs), slice 38
(KafkaUser quotas; landed 2026-05-18 — slice 37 reuses the
`describe → diff → alter` plumbing, just keyed by DN).

## Goal

Land the second authentication mechanism on `KafkaUser`: mutual TLS.
The operator generates an ECDSA P-256 client certificate signed by a
lazily-bootstrapped per-cluster clients CA, drops it in a per-user
`Secret` shaped the same way Strimzi does (`user.crt`, `user.key`,
`ca.crt`), and pins ACLs / quotas to the cert's Subject DN. Bare
`CN=<KafkaUser name>` as the entire DN — no `O`, no `OU` — so the
principal string the broker observes is unambiguous and stable across
RFC 2253 vs 4514 ordering quirks.

## Decisions captured

- **CA scope.** Lazy bootstrap of a per-cluster clients CA when the
  first TLS user reconciles. Slice 30 owns the full CA lifecycle
  (rotation, renewal CronJob, multi-generation trust). Per CLAUDE.md
  the project is greenfield and undeployed, so slice 30 just replaces
  the bootstrap logic — no migration shims, no compatibility kept on
  either side.
- **Subject DN.** Bare `CN=<name>`. No `O=crabka`, no `OU`. The fewer
  RDNs the cert carries, the fewer ways the various string encodings
  of the broker, the JVM `kafka-acls` tooling, and our `User:` prefix
  parser can disagree. The clients-CA cert itself does carry
  `O=crabka` since it's never used as a principal.
- **ACL principal.** `User:CN=<name>` (the `User:` prefix Kafka stamps
  on every authorizer principal, followed by the bare-CN DN). Lives
  behind a single `principal_for(&name, &Authentication)` that ACL
  reconcile, ACL finalizer-delete, and quotas (after stripping the
  `User:` prefix) all call.
- **Quotas.** Keyed off the DN as the broker's `entity_name`. The
  broker treats `entity_name` as an opaque string — `alice` works and
  `CN=alice` works, they just route to different quota slots. Slice 37
  uses the DN consistently so SCRAM-set quotas don't leak onto TLS
  users sharing a name and vice versa.
- **Secret keys.** `user.crt`, `user.key`, `ca.crt` (PEM). No
  `user.p12`, no `user.password` (PKCS#12 keystore bundle) in this
  slice; defer to a slice 37 follow-up if/when a JVM-client consumer
  asks for it. PEM covers the Rust + Go client stories we care about
  today.
- **CRD schema.** Hand-rolled JSON schema on `Authentication` via
  `schema_with`. kube-rs 3.x's `StructuralSchemaRewriter` panics on
  multi-variant tagged enums where the discriminator's `enum` values
  differ across `oneOf` branches (the default schemars output). Same
  workaround as `Storage` in `kafka_node_pool.rs`. Cross-variant
  constraints ("`iterations` is only valid when
  `type=scram-sha-512`") stay in the operator at reconcile time.
- **Crypto.** ECDSA P-256 via rcgen 0.13 (`PKCS_ECDSA_P256_SHA256`).
  Matches the algorithm choice in slice 29 and slice 33 (listener
  certs); the security crate stays single-algorithm so test
  certificates are stable.
- **Renewal.** Default 365-day validity, 30-day renewal window
  (`renewalDays`). Every reconcile parses the existing cert's
  `notAfter`; if `notAfter - now <= renewalDays`, the operator issues
  a new cert and overwrites `user.crt` / `user.key` in the Secret.
- **Requeue cadence.** SCRAM users requeue every 1 minute (catches
  external ACL drift via `DescribeAcls`). TLS users requeue every
  6 hours — cert renewal needs daily-ish, not minutely; ACL drift is
  still caught by the slice 36 watch + 6h floor; and high-frequency
  reconciles on TLS users would burn the clients-CA key (each
  reconcile re-parses the CA PEM) for no benefit.

## Architecture

### Module placement

- **`crates/security/src/ca.rs` — pure rcgen helpers.** No kube types,
  no `async`, no I/O. Two public functions:
  - `generate_clients_ca(cn, validity_days) -> CaMaterial { cert_pem,
    key_pem }`. Self-signed, `CA:TRUE`, `keyCertSign + cRLSign`.
  - `issue_user_cert(ca_cert_pem, ca_key_pem, cn, validity_days) ->
    UserCert { cert_pem, key_pem, not_after }`. Leaf with
    `digitalSignature + keyEncipherment` and `EKU = clientAuth`.
  Reusable verbatim by slice 30 (inter-broker CA) and slice 33's
  test-cert generation. Keeping this in `crabka-security` keeps the
  operator crate kube-only.

- **`crates/operator/src/controller/user_tls.rs` — controller-side
  helpers.** Owns the per-cluster clients-CA bootstrap, the per-user
  cert reuse/renew/issue decision, the Secret render, and
  `is_cert_expiring_soon`. Called from `controller/user.rs` step 6
  when `spec.authentication` matches `Authentication::Tls(_)`.

- **`crates/operator/src/crd/user.rs` — CRD types.** Grows
  `TlsAuth { validity_days, renewal_days }`, an `Authentication::Tls`
  variant, three new status fields (`tls`, `tls_cert_not_after`,
  `tls_principal`), and the `schema_with` shim.

### Reconcile pipeline (delta from slice 36)

Slice 36's pipeline is unchanged for SCRAM users. The TLS arm slots
in at step 6 (credentials):

1-5. Cluster label / spec validate / cluster-ready / finalizer / add
finalizer — unchanged.

6. **Credentials.** Match on `&obj.spec.authentication`:
   - `Authentication::ScramSha512` — slice 36 path
     (`ensure_password_secret`, `AlterUserScramCredentials` upsert).
   - `Authentication::Tls(tls_auth)` —
     `user_tls::ensure_clients_ca(secret_api, kafka)` then
     `user_tls::ensure_user_cert_secret(secret_api, &obj, &ca,
     tls_auth)`. The TLS arm makes no broker call: the broker learns
     the user identity from the certificate at mTLS handshake time.

7. **ACL reconcile.** Unchanged shape; the principal is now
   `principal_for(&name, &auth)` so the DN ends up in the ACL filter
   for TLS users.

8. **Quota reconcile.** Unchanged shape; entity name is
   `principal.strip_prefix("User:").unwrap_or(&principal)` so quotas
   route to the DN's slot for TLS users.

9. **Status patch.** `Ready=True` plus `tls`, `tls_cert_not_after`,
   `tls_principal` for TLS users; `scram_sha512` for SCRAM users.
   `quotas_in_sync` covers both.

10. **Requeue.** `match auth { ScramSha512 => 1min, Tls => 6h }`.

### `principal_for` as single source of truth

```rust
pub(crate) fn principal_for(name: &str, auth: &Authentication) -> String {
    match auth {
        Authentication::ScramSha512(_) => format!("User:{name}"),
        Authentication::Tls(_) => format!("User:CN={name}"),
    }
}
```

Called from: ACL reconcile (`expand_spec_acls`), ACL finalizer-delete
(`DeleteAcls` principal filter), quotas reconcile + finalizer-delete
(after stripping `User:`). Every place the operator constructs a
principal string goes through this one function.

### Status delta

Three new fields on `KafkaUserStatus`:

| Field                | Type             | Emitted when         |
|----------------------|------------------|----------------------|
| `tls`                | `bool` (default false) | always (`#[serde(default)]`) |
| `tlsCertNotAfter`    | `Option<String>` | TLS user is provisioned |
| `tlsPrincipal`       | `Option<String>` | User is provisioned (SCRAM gets `User:<name>`, TLS gets `User:CN=<name>`) |

`tlsPrincipal` is the field operators read when debugging "why isn't
my ACL matching?" — it's the exact string the broker compares
against.

### Requeue cadence rationale

SCRAM users hold their existing 1-minute cadence: ACL drift detection
relies on `DescribeAcls` polling, and SCRAM has no other state with a
long natural cadence. TLS users have two things to check on each
reconcile: cert expiry (sub-daily is overkill) and ACL drift (the
finalizer + create watch already catch CRD-driven changes; 6h covers
external `kafka-acls` drift). 6h gives renewal a daily-ish heartbeat
and avoids hammering the clients-CA PEM parse on every TLS user every
minute.

## Secret shapes

Three Secret kinds, all server-side-applied with field-manager
`crabka-operator`, owner-ref'd as noted.

| Secret name                        | Keys                | Owner ref     | Lifetime       |
|------------------------------------|---------------------|---------------|----------------|
| `<cluster>-clients-ca`             | `ca.key` (PEM)      | `Kafka`       | 10y CA (slice 30 takes over rotation) |
| `<cluster>-clients-ca-cert`        | `ca.crt` (PEM)      | `Kafka`       | same as above  |
| `<user>` (= `KafkaUser.name`)      | `user.crt`, `user.key`, `ca.crt` (PEM) | `KafkaUser` | `validityDays`, default 365 |

The clients-CA is split across two Secrets the same way Strimzi
splits it: the cert lives in the world-readable `*-clients-ca-cert`
Secret (operator workloads, prometheus exporters, etc. can mount
it), while the private key lives in `*-clients-ca` (only the
operator should ever mount it). Both are owner-ref'd to the parent
`Kafka` so a cluster delete cascades.

The per-user Secret carries a copy of `ca.crt` so consumers can
build a trust store without separately mounting the cluster-wide
Secret.

Labels on all three:
- `app.kubernetes.io/managed-by: crabka-operator`
- `crabka.io/cluster: <cluster>`
- on the per-user Secret only: `crabka.io/user: <name>`

## CRD wire shape

```yaml
apiVersion: crabka.io/v1alpha1
kind: KafkaUser
metadata:
  name: alice
  namespace: kafka
  labels:
    crabka.io/cluster: demo
spec:
  authentication:
    type: tls
    validityDays: 180        # optional, default 365
    renewalDays: 14          # optional, default 30
  authorization:
    type: simple
    acls:
      - resource: { type: topic, name: orders, patternType: literal }
        operations: [Read, Describe]
      - resource: { type: group, name: alice, patternType: literal }
        operations: [Read]
  quotas:
    producerByteRate: 1048576
    consumerByteRate: 2097152
status:
  conditions:
    - type: Ready
      status: "True"
      reason: Ready
  observedGeneration: 1
  username: alice
  secret: alice
  scramSha512: false
  tls: true
  tlsCertNotAfter: "2027-05-19T16:38:45Z"
  tlsPrincipal: "User:CN=alice"
  quotasInSync: true
```

Compare to a SCRAM user — only the `authentication.type` and
`status.{tls*,scramSha512}` block change.

## What's missing for full end-to-end (deferred)

- **Slice 30 — full CA lifecycle.** This slice's bootstrap is
  one-shot: if both Secrets exist and parse, reuse them; otherwise
  regenerate both. Slice 30 owns rotation (multi-generation trust
  during a roll), CRL/OCSP if we want it, and the `renew now`
  annotation. No compat shim from slice 37 → slice 30 — when slice 30
  lands it just replaces the bootstrap path.
- **Slice 31 — listener mTLS.** This slice generates client certs,
  but the broker doesn't yet listen on a port that requests them at
  handshake time. Slice 31 wires `Kafka.spec.listeners[].auth: mtls`
  into the broker's listener config so the certs become usable. Until
  slice 31, the e2e job only verifies the Secret content and the
  cert chain, not a live SASL/SSL handshake.
- **Slice 37 follow-up — PKCS#12 keystore bundle.** `user.p12` +
  `user.password` (random 32-byte hex) for JVM-client consumers that
  don't want to convert PEM at deployment time. Add when a real
  consumer asks; until then the Rust `rustls` PEM path is enough.
- **Slice 33 — cert hot-reload (shipped 2026-05-16).** The broker
  already watches its listener cert Secrets and reloads on change;
  consumer-side hot-reload is the consumer's responsibility, but the
  Strimzi-shaped Secret means existing rotation tooling (cert-manager
  etc.) interoperates if a user prefers it.
- **Slice 38 — quotas (shipped 2026-05-18).** Slice 37 quotas-by-DN
  plumbing reuses the `describe_user_quotas` /
  `alter_user_quotas` admin RPCs unchanged; only the `entity_name`
  argument changes.

## Testing strategy

### Unit — `crates/security/src/ca.rs`

Five tests, all working over the in-memory PEM output (no I/O):

- `generate_clients_ca_round_trips` — round-trip parse, assert
  `Subject = CN=root,O=crabka`, assert `BasicConstraints.ca = true`,
  assert validity span within ±60s of `validity_days * 86400`.
- `issue_user_cert_signed_by_ca_and_bare_cn` — parse the leaf, assert
  `Subject = CN=alice` exactly (bare RDN), assert
  `verify_signature` against the CA public key.
- `issue_user_cert_dn_matches_extract_principal` — the leaf DN
  observed via `crabka_security::extract_principal_from_cert` (the
  function the broker uses on the SSL session) must equal
  `CN=alice`. Pins the wire round-trip.
- `extended_key_usage_is_client_auth_on_leaf` — assert leaf EKU has
  `client_auth = true`. Pins that broker mTLS will accept the cert.
- `each_generate_is_unique` — two `generate_clients_ca` calls must
  produce distinct `cert_pem` and `key_pem`. Catches a regression
  where a stray static-keypair shortcut snuck in.

### Unit — `crates/operator/src/crd/user.rs`

Five new tests on top of slice 36's CRD tests:

- `tls_auth_round_trips` — `Authentication::Tls(TlsAuth::default())`
  serializes to `{"type": "tls"}`.
- `tls_auth_with_validity_days_round_trips` — both optional fields
  emit `camelCase` keys.
- `authentication_scram_round_trips_unchanged` — the
  `schema_with`-hand-rolled enum still serializes SCRAM users
  identically to slice 36 (no behaviour regression).
- `status_tls_fields_omit_when_unset` — `tlsCertNotAfter` /
  `tlsPrincipal` are absent from the JSON when `None`; `tls: false`
  is still emitted (it's a plain `bool`, not `Option<bool>`).
- `status_tls_fields_emit_when_set` — populated `Some(…)` round-trip.

### Unit — `crates/operator/src/controller/user.rs`

- `principal_for_dispatches_on_auth_type` —
  `principal_for("alice", &scram) == "User:alice"`;
  `principal_for("alice", &tls) == "User:CN=alice"`.

### Unit — `crates/operator/src/controller/user_tls.rs`

- `is_cert_expiring_soon_boundary_cases` — `notAfter - now` at
  `renewal_days - 1`, `renewal_days`, `renewal_days + 1`. The middle
  case (exact equality) reissues — the comparator is `<=`.

### Reconcile-level — `crates/operator/tests/reconcile_user.rs`

Five new tests, all using `FakeAdminClient` + an in-process clients-CA:

1. **`first_reconcile_tls_provisions_certs_and_acls`** — apply a TLS
   `KafkaUser` with two ACLs. Assert clients-CA Secrets get created,
   the per-user Secret contains `user.crt`/`user.key`/`ca.crt`, the
   fake admin sees `CreateAcls` with `principal = User:CN=alice`, and
   status reaches `Ready=True` with `tls=true`, `tlsPrincipal=
   User:CN=alice`, and `tlsCertNotAfter` set.
2. **`tls_reconcile_reuses_existing_cert_when_not_near_expiry`** —
   apply, reconcile twice. The second reconcile must observe the same
   `user.crt` bytes as the first (parse cert, compare serial).
3. **`tls_reconcile_reissues_cert_near_expiry`** — set
   `validityDays=10`, `renewalDays=30`. First reconcile issues; second
   reconcile must reissue (cert is always inside the renewal window).
   New `user.crt` bytes; `status.tlsCertNotAfter` updates.
4. **`tls_finalizer_filters_acls_by_dn`** — delete a TLS user with
   ACLs. Finalizer-delete must call `DeleteAcls` with
   `principal_filter = User:CN=alice`, not `User:alice`. Same shape
   for quotas-by-DN.
5. **`tls_user_with_quotas_alters_quotas_by_dn`** — TLS user with
   `spec.quotas.producerByteRate = 1MB`; fake admin's
   `alter_user_quotas` must be called with entity name `CN=alice`,
   not `alice`.

Brings `tests/reconcile_user.rs` from 8 → 13 tests.

### e2e (extend existing `kind-kafkauser` job)

Apply a TLS `KafkaUser` after the existing SCRAM test, then:

- `kubectl get secret alice -o jsonpath='{.data.user\.crt}' | base64 -d`
  parses as a valid X.509 cert.
- The cert chains to the contents of the `ca.crt` key.
- The cert's Subject DN is exactly `CN=alice`.
- The clients-CA Secret (`<cluster>-clients-ca-cert`) exists and is
  owner-ref'd to the parent `Kafka`.

**No broker handshake.** That's slice 31's job; this slice ships
the cert material and the Secret shape, not the listener.

## Open questions resolved

- **"Should we generate the CA or require user-provided?"** Generate
  now. Lazy bootstrap on first TLS user reconcile, owner-ref'd to the
  `Kafka`. User-provided CAs (via a labelled Secret) is on slice 30's
  surface; deferring keeps the slice 37 surface bounded.
- **"PKCS#12 keystore in this slice?"** No. PEM is enough for the
  Rust + Go consumer story we care about; PKCS#12 is a follow-up if a
  JVM consumer materializes.
- **"Bare CN vs full DN with O?"** Bare CN on the leaf. Avoids the
  RFC 2253 vs 4514 ordering trap (different libraries emit
  `O=...,CN=...` vs `CN=...,O=...`), keeps the ACL principal stable
  across broker-version upgrades, and matches Strimzi. The CA cert
  itself keeps `O=crabka` since it's never used as a principal.
- **"Reject quotas for TLS users?"** No — support them. The broker
  accepts any string as a quota entity name, and slice 38's plumbing
  is reusable verbatim with the DN as the entity name.

## Acceptance criteria

- `cargo build --workspace` and `cargo clippy --workspace
  --all-targets -- -D warnings` clean.
- All five `crates/security/src/ca.rs` tests green; all CRD round-trip
  tests green; reconcile tests go from 8 → 13 and all green.
- `cargo xtask gen-crds` produces drift-clean
  `deploy/crds/crabka.io_kafkausers.yaml`; the `Authentication` schema
  shows both `scram-sha-512` and `tls` in the discriminator enum
  with `validityDays` and `renewalDays` properties.
- Manual: apply a TLS `KafkaUser` to a kind cluster, verify the
  per-user Secret has `user.crt`/`user.key`/`ca.crt`, verify the cert
  subject is `CN=<name>`, verify the chain validates against
  `ca.crt`.

## Non-goals

- Full CA rotation / renewal (→ slice 30).
- Listener-side mTLS so the broker actually requests a client cert
  (→ slice 31).
- PKCS#12 keystore bundle (`user.p12` + `user.password`)
  (→ slice 37 follow-up if a JVM-client user asks).
- User-provided clients-CA (BYO-CA via a labelled Secret)
  (→ slice 30).
- ACL or quota semantics beyond what slice 36 / slice 38 ship; this
  slice only changes the principal string passed in.
