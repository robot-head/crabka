# Slice 50b — Operator: Listener OAuth `tlsTrustedCertificates`

Status: Draft
Date: 2026-05-23
Slice: 50b
Pairs with broker slice(s): 49c (already shipped)
Umbrella: [OAUTHBEARER full-parity roadmap](2026-05-23-crabka-oauth-parity-roadmap-design.md)

## Goal

Surface broker slice 49c's `[oauthbearer].jwks_tls_trust` knob on the
operator CRD as a Strimzi-shaped `tlsTrustedCertificates` field, build
the trust bundle into a managed Kubernetes `Secret`, mount it into
broker pods, and upgrade the slice-50 Keycloak kind e2e from HTTP to
HTTPS to prove the full stack works end-to-end against a real IdP whose
TLS cert isn't signed by a public webpki root.

## Deliverables

1. **CRD field** `tlsTrustedCertificates: [{secretName, certificate}, …]`
   on `Kafka.spec.listeners[].authentication` (oauth-typed listeners).
2. **Cross-listener canonical update**: `tls_trusted_certificates` joins
   the existing oauth-conflict tuple. Two OAuth listeners with same
   JWKS/issuer but different trust bundles are rejected — same shape as
   slice 50's existing per-field conflict check.
3. **Managed Secret** `{kafka.name}-oauth-jwks-trust` (single key
   `ca.crt`, concatenated PEM bytes), owner-ref to the `Kafka` CR.
4. **Broker pod plumbing**: conditional volume + mount at
   `/etc/crabka/oauth-jwks-trust/` when any OAuth listener has trust
   certs.
5. **Broker TOML rendering**: `[oauthbearer].jwks_tls_trust =
   "/etc/crabka/oauth-jwks-trust/ca.crt"` emitted when trust certs are
   set.
6. **Three new `Ready=False` reasons** for trust-bundle assembly
   failures.
7. **Sample manifest** updated with a `tlsTrustedCertificates` example.
8. **Regenerated CRD YAML**.
9. **`kind-oauth` e2e upgraded** from HTTP to HTTPS using Bitnami
   Keycloak's auto-generated TLS.
10. **STATUS.md entry**.

## Non-deliverables (out of scope)

| Item | Status |
|------|--------|
| Source-Secret reflector for instant rotation pickup | Out — periodic reconcile is sufficient for slice 50b. Future slice if real demand. |
| Cross-namespace Secret references | Out — `secretName` constrained to same ns as the `Kafka` CR. |
| mTLS from broker *to* IdP (client cert auth) | Out — not in any roadmap slice. |
| Per-listener `[oauthbearer]` config (allowing divergent trust per listener) | Still rejected at validation by the existing slice-50 cross-listener guard; lifts in future slice 49h. |
| Operator-managed truststore in the JVM producer Job | E2E concern only — handled inline in the workflow (keytool import of Keycloak CA into the existing cluster-CA JKS). |
| Multiple PEM paths threaded into the broker (broker reads one file only) | Out — broker accepts one path (per slice 49c); operator concatenates. |
| Pinning the IdP cert SHA / public-key fingerprint | Out — rustls chain verification only (matches 49c). |

## CRD shape

```yaml
# Kafka.spec.listeners[].authentication
type: oauth
validIssuerUri: https://keycloak.example/realms/kafka
jwksEndpointUri: https://keycloak.example/realms/kafka/protocol/openid-connect/certs
validAudience: kafka-broker
userNameClaim: preferred_username
customClaimCheck:
  scope: kafka.write
tlsTrustedCertificates:
  - secretName: keycloak-ca
    certificate: tls.crt
  - secretName: corp-root-ca
    certificate: ca.pem
```

Rust:

```rust
// crates/operator/src/crd/listener.rs

pub struct ListenerAuthenticationOAuth {
    // ...existing fields unchanged...
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub tls_trusted_certificates: Vec<TlsTrustedCertificate>,
}

#[derive(Debug, Clone, Deserialize, Serialize, JsonSchema, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct TlsTrustedCertificate {
    pub secret_name: String,
    pub certificate: String,
}
```

The hand-rolled `listener_authentication_schema` extends to include
`tlsTrustedCertificates` as an array sibling property whose items are
`{type: object, required: ["secretName", "certificate"], properties:
{secretName: {type: string, minLength: 1}, certificate: {type: string,
minLength: 1}}}`.

## Reconciler behavior

### Order

In `controller/kafka.rs::reconcile_kafka`, after the existing listener
validation step and before ConfigMap/StatefulSet render:

1. Compute the canonical OAuth config (already done for the broker-global `[oauthbearer]` block — reuse).
2. If canonical config exists and `tls_trusted_certificates` is non-empty:
   - For each entry, fetch the source Secret. If missing → `Ready=False reason=MissingOauthTrustSecret message="Secret '<name>' not found in namespace '<ns>'"`.
   - Read the named key. If absent → `Ready=False reason=MissingOauthTrustKey message="Secret '<name>' has no key '<key>'"`.
   - If the key value is empty → `Ready=False reason=EmptyOauthTrustValue message="Secret '<name>' key '<key>' is empty"`.
   - Concatenate the byte values, separated by `\n` (only added when the previous PEM didn't end with one — safety net against missing trailing newlines).
   - Create/update managed Secret `{kafka.name}-oauth-jwks-trust` with one key `ca.crt = bundle`, owner-ref to the `Kafka` CR, standard operator labels.
   - Thread the managed Secret's name into the downstream render steps.
3. If canonical config has empty `tls_trusted_certificates`: do not create the managed Secret. Existing managed Secret (from a prior reconcile that did have trust certs) is left in place — the StatefulSet stops mounting it, and the owner-ref will cascade-delete it when the `Kafka` CR is deleted. **Possible future improvement**: actively delete the managed Secret when no listener wants it. Deferred to keep this slice tight.

### Cross-listener canonical update

The existing `oauth_canonical` helper in `controller/listeners.rs:181`
masks `enable_oauth_bearer` for the cross-listener conflict check.
Slice 50b adds `tls_trusted_certificates` to the canonical tuple
(no masking — divergent trust bundles are a real conflict because the
broker `[oauthbearer]` block is global).

`tls_trusted_certificates: Vec<TlsTrustedCertificate>` already derives
`Eq`/`PartialEq` via the new struct's derives — no manual masking
needed; the canonical clone includes the field as-is.

### Failure-mode summary

| Reason string (Ready=False) | Trigger |
|------|---------|
| `MissingOauthTrustSecret` | Source Secret name doesn't exist in the Kafka CR's namespace |
| `MissingOauthTrustKey` | Source Secret exists; key name doesn't |
| `EmptyOauthTrustValue` | Key exists but value is zero bytes |
| `ConflictingOAuthConfig` (existing — extended) | Two OAuth listeners declare different trust bundles |

## Pod template

In `controller/kafka_node_pool.rs`:

- `render_storage(..., oauth_jwks_trust_secret: Option<&str>)` — new parameter. When `Some(name)`, append:
  ```json
  { "name": "oauth-jwks-trust", "secret": { "secretName": name, "defaultMode": 0o400 } }
  ```
  to the `volumes` array. (Mirrors the existing `cluster_ca_cert_vol` / `clients_ca_cert_vol` pattern.)

- `render_main_container(..., oauth_jwks_trust_mount: Option<&str>)` — new parameter. When `Some(mount_path)`, append:
  ```json
  { "name": "oauth-jwks-trust", "mountPath": mount_path, "readOnly": true }
  ```
  to `volumeMounts`. Constant: `/etc/crabka/oauth-jwks-trust`.

`controller/kafka.rs` decides Some-vs-None based on the
`reconcile_oauth_jwks_trust` result and threads through both rendering
calls.

## Broker TOML

In `controller/listeners.rs::render_broker_toml`, where the existing
`[oauthbearer]` block emits the seven keys from slice 50:

```toml
jwks_endpoint_uri
valid_issuer_uri
expected_audience           (optional)
principal_claim_name        (optional)
scope_claim_name            (optional)
required_scope              (optional)
jwks_refresh_interval_ms    (optional)
allowable_clock_skew_ms     (optional)
```

Add one more line when the canonical config has `tls_trusted_certificates` non-empty:

```toml
jwks_tls_trust = "/etc/crabka/oauth-jwks-trust/ca.crt"
```

Placed at the end of the block, after `allowable_clock_skew_ms`,
matching the slice-49b/49c byte-stable ordering of `FileOAuthBearerConfig`
fields.

The mount path is a constant (`/etc/crabka/oauth-jwks-trust/ca.crt`),
not derived from the managed Secret name. The Secret name is operator
internal; the broker only sees the file path.

## File-level change map

| File | Change |
|------|--------|
| `crates/operator/src/crd/listener.rs` | New `TlsTrustedCertificate` struct, new field on `ListenerAuthenticationOAuth`, extend `listener_authentication_schema` |
| `crates/operator/src/controller/listeners.rs` | Render `jwks_tls_trust` line; canonical-tuple includes `tls_trusted_certificates` (no helper change needed — `Eq` derives carry through) |
| `crates/operator/src/controller/kafka.rs` | New `reconcile_oauth_jwks_trust` helper; 3 new failure paths; thread `Option<String>` (managed Secret name) into render calls |
| `crates/operator/src/controller/kafka_node_pool.rs` | Add `oauth_jwks_trust_secret: Option<&str>` to `render_storage` + `render_main_container`; conditional volume + mount entries |
| `crates/operator/src/controller/common.rs` (or wherever `ReconcileError` is defined) | New error variants (or new `Ready=False` reason strings) for the three failure modes |
| `crates/operator/sample/oauth-listener.yaml` | Add `tlsTrustedCertificates` example |
| `deploy/crds/crabka.io_kafkas.yaml` | Regenerated |
| `crates/operator/tests/reconcile_listener_oauth.rs` | Extend cross-listener-divergence test with `tls_trusted_certificates` perturbation; new TOML-render tests |
| `crates/operator/tests/reconcile_oauth_trust.rs` (new) | Reconcile-level integration tests for managed-Secret creation + failure modes + pod-mount assertions |
| `.github/workflows/operator-e2e.yml` | `kind-oauth` job: install Keycloak with TLS, copy auto-generated CA into default ns, point Kafka CR at HTTPS + declare trust certs, drop WeakAuth assertion, update producer-Job truststore |
| `STATUS.md` | New `## Slice 50b` entry |

## Test plan

### Unit tests (in `crd/listener.rs`)
- `oauth_with_tls_trusted_certificates_round_trips`
- `oauth_tls_trusted_certificates_default_omitted_on_serialize`
- `tls_trusted_certificate_minimum_required_fields_parse`

### Validation + render tests (in `controller/listeners.rs`)
- Extend `validate_listeners_rejects_two_oauth_listeners_with_divergent_config_in_any_canonical_field` to add a perturbation entry for `tls_trusted_certificates`.
- `render_broker_toml_emits_jwks_tls_trust_when_trust_certs_present`
- `render_broker_toml_omits_jwks_tls_trust_when_no_trust_certs`
- `render_broker_toml_oauthbearer_block_byte_order_with_trust_certs` — pin the exact `[oauthbearer]` block including `jwks_tls_trust` at the bottom (extends the existing canonical-order test from slice-50 T3 polish).

### Reconciler integration tests (in `tests/reconcile_oauth_trust.rs` — new file)
- `oauth_trust_creates_managed_secret_from_concatenated_pems`
- `oauth_trust_missing_source_secret_rejects_with_missing_oauth_trust_secret`
- `oauth_trust_missing_key_in_source_secret_rejects_with_missing_oauth_trust_key`
- `oauth_trust_empty_key_value_rejects_with_empty_oauth_trust_value`
- `oauth_trust_no_trust_certs_does_not_create_managed_secret`
- `oauth_trust_managed_secret_updates_when_source_changes`
- `statefulset_mounts_oauth_jwks_trust_secret_when_trust_certs_present`
- `statefulset_omits_oauth_jwks_trust_volume_when_no_trust_certs`

### Kind e2e (`kind-oauth` in `.github/workflows/operator-e2e.yml`)
- Keycloak installed with TLS on.
- Auto-generated `kc-keycloak-crt` Secret copied into `default` ns as `keycloak-ca`.
- Kafka CR has `tlsTrustedCertificates: [{secretName: keycloak-ca, certificate: tls.crt}]`.
- All URLs are `https://`.
- Producer Job's JKS truststore contains both the cluster CA (broker TLS) and the Keycloak CA (token-endpoint HTTPS).
- WeakAuth Event assertion removed (no `http://` in any URL).
- Happy-path producer (scoped token) and negative producer (no scope) still pass / fail as expected.

## E2E upgrade details

The `kind-oauth` job currently (slice 50) installs Keycloak with TLS
off and points the broker + producer at HTTP. The upgrade:

1. **Chart install:**
   ```bash
   helm install kc bitnami/keycloak --namespace keycloak --create-namespace \
     --version 25.2.0 \
     --set auth.adminUser=admin --set auth.adminPassword=admin \
     --set tls.enabled=true --set tls.autoGenerated=true \
     --set service.type=ClusterIP \
     --set production=false --set proxy=edge \
     --wait --timeout=600s
   ```
   Bitnami `tls.autoGenerated=true` produces a Secret named like
   `kc-keycloak-crt` in the `keycloak` namespace with `tls.crt` and
   `tls.key`. The exact name is chart-version dependent — verify at
   implementation time via `kubectl get secret -n keycloak`.

2. **Copy CA into the Kafka CR's namespace:**
   ```bash
   kubectl get secret kc-keycloak-crt -n keycloak -o json \
     | jq '.metadata = {"name":"keycloak-ca","namespace":"default"}' \
     | kubectl apply -f -
   ```

3. **`kcadm.sh` bootstrap script** — call against `https://localhost:8443/`. kcadm needs to trust the loopback cert. Either:
   - `--insecure` (acceptable for the in-pod bootstrap — it's loopback, not the wire).
   - Or supply `--truststore /opt/bitnami/keycloak/conf/keystore.jks --trustpass <pw>` if the chart sets a JKS truststore with the cert.

   Use `--insecure` for simplicity; document the choice in a comment.

4. **Kafka CR URLs all `https://`:**
   ```yaml
   validIssuerUri: https://kc-keycloak.keycloak.svc.cluster.local/realms/kafka
   jwksEndpointUri: https://kc-keycloak.keycloak.svc.cluster.local/realms/kafka/protocol/openid-connect/certs
   tlsTrustedCertificates:
     - secretName: keycloak-ca
       certificate: tls.crt
   ```

5. **Producer Job's JKS truststore** — extend the existing keytool-import
   init step to also import the Keycloak CA. Pull `keycloak-ca`'s
   `tls.crt`, convert with `keytool -importcert -alias keycloak-ca
   -file /tmp/keycloak.crt -keystore /etc/truststore/truststore.jks
   -storepass <pw> -noprompt`. The producer's JAAS config keeps a
   single truststore for both broker TLS and token-endpoint HTTPS.

6. **Producer JAAS:**
   ```
   sasl.oauthbearer.token.endpoint.url=https://kc-keycloak.keycloak.svc.cluster.local/realms/kafka/protocol/openid-connect/token
   ```

7. **Drop the WeakAuth assertion** — no `http://` URLs anywhere, so no
   WeakAuth Event. Replace with the inverse: assert zero items returned
   by `kubectl get events -n default --field-selector reason=WeakAuth`
   (or remove the assertion entirely).

8. **Bootstrap-server hostname unchanged** — `demo-broker-headless.default.svc.cluster.local:9096`. That's broker-client TLS via the operator-issued cluster CA, unrelated to the Keycloak HTTPS work.

## Acceptance criteria

1. `cargo build -p crabka-operator` + `cargo test -p crabka-operator` pass.
2. `cargo fmt --check` + `cargo clippy --workspace --all-targets -- -D warnings` pass.
3. CRD-drift gate (`tools/regen-crds.sh` + `git diff --exit-code -- deploy/crds/`) clean.
4. New unit + integration tests above all pass.
5. Sample `crates/operator/sample/oauth-listener.yaml` validates against the regenerated CRD.
6. Kind e2e `kind-oauth` job passes on `push: main` and on PRs labeled `e2e-oauth`: Keycloak boots with TLS, realm bootstrap succeeds, Kafka CR reaches `Ready=True`, producer with valid scoped token succeeds, producer without scope fails with `SaslAuthenticationException`, zero `WeakAuth` Events on the `Kafka` resource.
7. STATUS.md updated.

## Open questions resolved during brainstorming

- **CRD shape.** Strimzi-shaped list of `{secretName, certificate}` entries (not single, not secretName-only).
- **Concat strategy.** Reconciler builds + maintains a managed output Secret (not init-container concat, not ConfigMap).
- **E2E upgrade scope.** Upgrade `kind-oauth` from HTTP to HTTPS in this slice (not deferred, not parallel HTTP+HTTPS jobs).
- **Source-Secret rotation.** Eventually consistent via periodic reconcile. Reflector wiring deferred until real demand.
- **Cross-namespace Secret refs.** Same-namespace only.
- **Conditional pod mount.** Threaded via `Option<&str>` parameter on `render_storage` / `render_main_container`, mirroring the existing `clients_ca_path` pattern.
- **Managed-Secret cleanup when trust certs removed.** Left in place; cascades on `Kafka` CR delete via owner-ref. Active deletion deferred.
- **JVM producer truststore.** Single JKS containing both cluster CA and Keycloak CA. keytool-import in the existing init step. Avoids the JVM client's separate per-listener truststore knobs.
