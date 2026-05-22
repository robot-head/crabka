# Slice 31 — Operator: Listener auth wiring (TLS + SCRAM)

**Date:** 2026-05-21
**Author:** brainstormed with Claude / matthew.d.stone
**Roadmap entry:** Phase 4, Slice 31 — *Listener auth wiring (TLS + SCRAM-SHA-512). Surface existing Crabka auth as CRD listener config.*

## Goal

Expose Crabka's existing per-listener authentication primitives (transport
TLS, mTLS client-cert auth, SASL/SCRAM-SHA-512, SASL/SCRAM-SHA-256) through
the `Kafka.spec.listeners[]` CRD so a user can declaratively configure a
listener's auth posture in their `Kafka` resource. After this slice, a
`KafkaUser` (slice 36/37) is end-to-end usable — the broker actually
requests / accepts the user's credentials on the listener port.

Slice 30 already mounts the cluster CA, clients CA, and per-broker leaf
cert into the broker pod (the `clients-ca` mount was explicitly noted as
"reserved for slice 31"). This slice consumes those mounts and adds the
per-listener TOML emission, plus the CRD schema and validation.

## Decisions captured during brainstorm

1. **Auth types in scope:** `tls`, `scram-sha-512`, `scram-sha-256`. The
   broker already has all three (slices 12, 29, 32) — exposing all three
   here is ~30 LOC and keeps the slice self-contained. `oauth` and
   `custom` are out of scope (slice 49 / future).
2. **CRD shape:** Strimzi-shape `authentication: { type: ... }` block,
   matching the existing `KafkaUser.spec.authentication` shape in
   `crates/operator/src/crd/user.rs`. Same `tag = "type"` serde pattern.
   Extensible: future variants can carry per-type config.
3. **`tls: bool` stays separate from `authentication`.** Transport TLS
   and auth are independent dimensions. A listener can be plaintext, TLS
   anonymous, SASL-over-plaintext, SASL-over-TLS, or mTLS.
4. **External listener TLS in scope.** Slice 30's per-broker cert only
   has internal pod-DNS SANs. To make `tls: true` work on a NodePort or
   LoadBalancer listener (JVM hostname verification), extend the SAN
   computation to include external advertised addrs. Reissue per-broker
   cert when the SAN list changes.
5. **Client trust on mTLS listeners = clients CA only.** Matches Strimzi
   exactly. The cluster CA is broker-server / inter-broker only; data-plane
   client certs must be signed by the clients CA.
6. **Empty SCRAM credential set is fine.** SCRAM credentials live in the
   metadata log, not in rendered operator config. Listener boots; users
   added via `KafkaUser` later without restart.
7. **Listener auth change → rolling restart via slice-21 config-hash.**
   Listener protocol is structural (accept-loop, pre-auth gate bound at
   broker start). Free — the slice-21 config-hash already incorporates
   the rendered broker TOML; adding `[listeners.tls_config]` /
   `[listeners.sasl_config]` blocks changes the hash automatically.
8. **No BYO server cert in scope.** Cluster-CA-signed per-broker cert
   only. BYO (`brokerCertChainAndKey`) is a follow-up.
9. **`tls: false` + SCRAM (SaslPlaintext) is valid but emits a `WeakAuth`
   Warning Event.** Accept, but flag.

## Architecture

### CRD shape (`Kafka.spec.listeners[].authentication`)

Add to `crates/operator/src/crd/listener.rs`:

```rust
#[derive(Debug, Clone, Deserialize, Serialize, JsonSchema, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct Listener {
    pub name: String,
    pub port: i32,
    #[serde(rename = "type")]
    pub type_: ListenerType,
    #[serde(default)]
    pub tls: bool,
    /// Slice 31. Optional per-listener authentication. When absent and
    /// `tls: true`, the listener is anonymous over TLS. When absent and
    /// `tls: false`, the listener is plaintext.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub authentication: Option<ListenerAuthentication>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub configuration: Option<ListenerConfiguration>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub network_policy_peers: Option<Vec<crate::crd::NetworkPolicyPeer>>,
}

#[derive(Debug, Clone, Deserialize, Serialize, JsonSchema, PartialEq, Eq)]
#[serde(tag = "type", rename_all = "kebab-case")]
pub enum ListenerAuthentication {
    /// Mutual TLS — client must present a cert signed by the clients CA.
    /// Requires `tls: true`. Principal = `User:CN=<subject CN>`.
    Tls,
    /// SASL/SCRAM-SHA-512. Credentials live in the metadata log,
    /// provisioned by KafkaUser. Principal = `User:<username>`.
    ScramSha512,
    /// SASL/SCRAM-SHA-256.
    ScramSha256,
}
```

Comment on `Listener.tls` is updated:
```
Transport-level TLS. When true, the listener uses the per-broker
keystore signed by the cluster CA. Independent from `authentication`:
a `tls: true` listener with no `authentication` is anonymous over TLS.
```

The existing comment "Must be `false` in this slice; reconcile rejects
`true` until Phase 4" is removed.

### Listener → broker protocol mapping

In `crates/operator/src/controller/listeners.rs`:

```rust
fn listener_protocol(l: &Listener) -> ListenerProtocol {
    use ListenerAuthentication::*;
    use ListenerProtocol::*;
    match (l.tls, l.authentication.as_ref()) {
        (false, None)                          => Plaintext,
        (true,  None)                          => Ssl,
        (false, Some(ScramSha512 | ScramSha256)) => SaslPlaintext,
        (true,  Some(ScramSha512 | ScramSha256)) => SaslSsl,
        (true,  Some(Tls))                     => Ssl,
        (false, Some(Tls))                     => unreachable!("validation rejects"),
    }
}

fn sasl_mechanism(auth: &ListenerAuthentication) -> Option<SaslMechanism> {
    use ListenerAuthentication::*;
    match auth {
        ScramSha512 => Some(SaslMechanism::ScramSha512),
        ScramSha256 => Some(SaslMechanism::ScramSha256),
        Tls         => None,
    }
}
```

mTLS (`authentication.type: tls`) and anonymous-TLS both map to broker
`ListenerProtocol::Ssl`. The broker distinguishes them via the
per-listener `client_auth` field, not via the protocol enum.

### Validation rules (`controller::listeners::validate_listener`)

Add to existing validation pass:

| `tls`  | `authentication.type`     | Result                                              |
|--------|---------------------------|-----------------------------------------------------|
| `false`| absent                    | OK — `Plaintext`                                    |
| `true` | absent                    | OK — `Ssl` anonymous                                |
| `false`| `scram-sha-512`           | OK — `SaslPlaintext`; emit `WeakAuth` Warning Event |
| `false`| `scram-sha-256`           | OK — same                                           |
| `true` | `scram-sha-512`           | OK — `SaslSsl`                                      |
| `true` | `scram-sha-256`           | OK — `SaslSsl`                                      |
| `true` | `tls`                     | OK — `Ssl` with `client_auth=Required`              |
| `false`| `tls`                     | **REJECT** — `ListenerMtlsRequiresTransportTls`     |

Existing slice-25 hard rejection `if l.tls { return Err(TlsNotYetSupported) }`
is removed.

On validation failure, the existing slice-25 pattern applies: the broker
ConfigMap is not re-rendered, the StatefulSet pod-template hash is not
bumped, and no rolling restart is triggered. The cluster keeps running its
last-valid config; the user fixes the manifest.

### Rendered broker TOML — per-listener TLS + SASL blocks

`render_broker_toml` in `controller/listeners.rs` emits, for each listener:

```toml
[[listeners]]
name      = "external"
bind_addr = "0.0.0.0:9094"
advertised = "broker-0.example.com:31090"
protocol  = "SaslSsl"

[listeners.tls_config]                                  # only when tls=true
cert_path      = "/etc/crabka/broker-tls/0.crt"
key_path       = "/etc/crabka/broker-tls/0.key"
client_ca_path = "/etc/crabka/clients-ca/ca.crt"        # only when auth=tls
client_auth    = "Required"                             # only when auth=tls

[listeners.sasl_config]                                 # only when auth=SCRAM
enabled_mechanisms = ["SCRAM-SHA-512"]
```

Emission rules:

- `[listeners.tls_config]` emitted iff `tls: true`.
- `client_ca_path` + `client_auth` emitted iff `authentication.type: tls`.
- `[listeners.sasl_config]` emitted iff `authentication` is a SCRAM variant;
  `enabled_mechanisms` is the single mechanism the listener exposes.

The top-level `[tls_config]` block (slice 30, controller listener) is
unchanged. Per-listener blocks coexist with it.

### Broker-side: per-listener TLS + SASL config

`crates/broker/src/file_config.rs` — extend `ListenerSpec`'s
deserialization to accept per-listener `tls_config` and `sasl_config`
blocks. Today `ListenerSpec` parses `name`, `bind_addr`, `advertised`,
`protocol` only; the top-level `[tls_config]` is used for any TLS
listener (only inter-broker today).

```rust
#[derive(Debug, Deserialize)]
pub struct ListenerSpec {
    pub name: String,
    pub bind_addr: SocketAddr,
    pub advertised: String,
    pub protocol: ListenerProtocol,
    #[serde(default)]
    pub tls_config: Option<ListenerTlsConfig>,    // NEW
    #[serde(default)]
    pub sasl_config: Option<ListenerSaslConfig>,  // NEW
}

#[derive(Debug, Deserialize)]
pub struct ListenerTlsConfig {
    pub cert_path: PathBuf,
    pub key_path: PathBuf,
    pub client_ca_path: Option<PathBuf>,
    pub client_auth: ClientAuth,                  // None | Optional | Required
}

#[derive(Debug, Deserialize)]
pub struct ListenerSaslConfig {
    pub enabled_mechanisms: Vec<SaslMechanism>,
}
```

In `crates/broker/src/network/listener.rs` (accept loop):

- When binding a TLS listener, resolve TLS config in this order:
  1. `ListenerSpec.tls_config` if `Some`,
  2. otherwise the top-level `BrokerConfig.tls_config` (back-compat for
     the slice-30 inter-broker setup),
  3. otherwise error at start (config invariant: `Ssl`/`SaslSsl`
     listener with no TLS config is a hard error).
- When a connection comes in on a SASL listener, the per-connection
  `ConnectionAuth` is initialized with `enabled_mechanisms` from
  `ListenerSpec.sasl_config` (falling back to
  `BrokerConfig.enabled_sasl_mechanisms` for back-compat).
- `client_auth = Required` configures the `TlsAcceptor`'s
  `ClientCertVerifier` against `client_ca_path` (clients CA). This is
  identical to slice 30's inter-broker setup except the truststore is
  the clients CA, not the cluster CA.

### Cluster CA cert SAN expansion (external listeners)

`crates/operator/src/controller/cluster_ca.rs::issue_broker_cert` —
extend signature:

```rust
pub fn issue_broker_cert(
    cluster: &str,
    namespace: &str,
    node_id: i32,
    extra_sans: &[SubjectAltName],   // NEW
    ca: &ClusterCa,
    validity_days: u32,
) -> Result<BrokerCert>;
```

Final SAN list = internal pod-DNS names (existing) ∪ `extra_sans`.

`extra_sans` for broker N is computed by `controller/listeners.rs` from
the per-listener Services already reconciled in slice 25:

- **Internal listener:** no extra SANs.
- **NodePort listener:** add `DnsName(node_external_dns)` for each Node
  in the cluster, plus `IpAddress(node_external_ip)`. Today's slice-25
  advertised-addr logic already enumerates Node addresses; reuse it.
  Also add the per-broker `BrokerOverride.advertised_host` if set.
- **LoadBalancer listener:** add `IpAddress(svc.status.loadBalancer.ingress[].ip)`
  for the per-broker LB Service AND for the bootstrap LB Service.
  Add `DnsName(ingress[].hostname)` if the cloud provider returns one.
- **Ingress/Route listener:** out of scope (slice 27).

When a LoadBalancer ingress is not yet assigned, return
`SansNotReady(broker_id)` and requeue with a status condition
`WaitingForLoadBalancerIp=True` on the affected listener. Internal +
NodePort listeners are unaffected.

### Cert reissue trigger (`controller/ca.rs::ensure_broker_certs`)

Slice 30: reissue on scale-up (new broker) or expiry; reuse otherwise.

Slice 31: also reissue when the SAN list for a broker changes vs the
SAN list embedded in the existing per-broker Secret entry. Compare
deterministically (sort + dedup before comparison).

Reissue path: same as slice 30 — write new entry into the per-cluster
`<cluster>-kafka-brokers` Secret. The slice-21 config-hash includes the
cert PEMs (per slice 30's hash inputs), so cert change triggers a
rolling restart automatically.

### Pod / StatefulSet template

**No changes.** Slice 30 already mounts:

```
/etc/crabka/cluster-ca/ca.crt
/etc/crabka/broker-tls/{id}.{crt,key}
/etc/crabka/clients-ca/ca.crt    # reserved for slice 31 — now consumed
```

The per-broker init step that selects the broker's own cert by node id
is unchanged.

### Reconcile pipeline (delta from slice 30)

```
1. reconcile_listener_services           (existing; slice 25)
   → assigns NodePort, observes LB ingress
   → returns per-broker advertised_addrs: BTreeMap<i32, Vec<String>>
        (May be partial if LB ingress not yet ready.)
2. reconcile_ca                          (extended slice 30)
   → for each broker:
        san_list = internal_dns ∪ advertised_addrs[id]
        if san_list != cert.sans: reissue
3. reconcile_broker_config               (existing; extended slice 31)
   → render_broker_toml emits per-listener tls_config / sasl_config
        (validation per Section "Validation rules")
4. reconcile_statefulset                 (existing; slice 21)
   → bump pod-template annotation on config-hash change
   → ordered rolling restart
```

Ordering: listener Services reconciled before CA so the SAN list is
current. CA reconciled before broker config so the cert PEMs entering
the config-hash are stable.

### Status surfacing

New status conditions on `Kafka.status`:

- `WaitingForLoadBalancerIp=True` — at least one LB listener's ingress is
  empty; per-broker cert issuance is paused for affected brokers.
- `ListenerValidationFailed=True` — at least one listener failed validation
  (e.g., `tls: false + auth: tls`). Reason: `ListenerMtlsRequiresTransportTls`.
  Message lists the offending listener names.

New Events:

- `Warning WeakAuth` on the Kafka resource — emitted once per listener
  per reconcile when SCRAM is configured without TLS
  (`SaslPlaintext` path). Message: `listener '<name>' has SCRAM auth
  without transport TLS; credentials traverse the network in cleartext
  during the SCRAM exchange. Consider tls: true.`

`KafkaListenerStatus` (already exists per slice 25) gains no new fields
in this slice. The `bootstrap_servers` URL prefix continues to be
inferred from `tls: bool` — TLS listeners are reported via
`SSL://host:port` / SASL-TLS listeners via `SASL_SSL://...`.

## Out of scope

- **BYO server cert** (`brokerCertChainAndKey`) — a user-supplied
  cert/key/chain for a specific listener. Follow-up.
- **OAuth / OAUTHBEARER** — slice 49.
- **Custom authentication plugin** (Strimzi `type: custom`) — no
  near-term roadmap; future.
- **External advertised hostnames** beyond what the cloud provider
  surfaces in `Service.status.loadBalancer.ingress`. Custom hostnames
  belong with BYO cert.
- **Ingress / Route listeners** — slice 27 territory; SNI/TLS for those
  has different semantics.
- **PKCS#12 user keystore bundle** (`user.p12`) — slice 37 follow-up,
  not slice 31.
- **Hot-reload of listener protocol** (no restart on auth change).
  Listener structure is bound at broker start; substantial broker
  refactor required and out of scope.
- **Per-listener SCRAM credential scoping** (a listener restricted to a
  subset of SCRAM users). Not a Kafka concept; Strimzi doesn't have it.
- **Mixed SCRAM mechanisms on one listener** (e.g., both 256 and 512
  on the same port). `enabled_mechanisms` is single-valued per listener
  in slice 31; multi-valued is a one-line follow-up if demand emerges.

## Testing strategy

### Operator unit tests

`crates/operator/src/crd/listener.rs`:
- `listener_authentication_round_trips` — three variants + None.

`crates/operator/src/controller/listeners.rs`:
- `validate_listener_mtls_requires_tls` — `(tls=false, auth=tls)` → error.
- `validate_listener_scram_without_tls_allowed` — accepts, no error.
- `validate_listener_tls_without_auth_allowed` — accepts.
- `validate_listener_scram_with_tls_allowed` — accepts.
- `listener_protocol_table` — all six legal `(tls, auth)` tuples →
  expected `ListenerProtocol`.
- `render_broker_toml_scram_sha_512_over_tls` — snapshot.
- `render_broker_toml_mtls` — snapshot, including `client_auth=Required`.
- `render_broker_toml_mixed_listeners` — plaintext inter-broker +
  SCRAM-SSL external + mTLS internal, snapshot.

`crates/operator/src/controller/cluster_ca.rs`:
- `san_list_internal_only_unchanged_from_slice30`.
- `san_list_with_nodeport_includes_node_external_addrs`.
- `san_list_with_loadbalancer_includes_ingress_ip`.
- `san_list_returns_sans_not_ready_when_loadbalancer_pending`.

`crates/operator/src/controller/ca.rs`:
- `ensure_broker_certs_reissues_on_san_list_change`.
- `ensure_broker_certs_reuses_when_san_list_unchanged`.

### Operator integration tests (FIFO-mock harness)

`crates/operator/tests/reconcile_listener_auth.rs` (new file):
- `scram_sha_512_internal_listener_renders_sasl_ssl`.
- `mtls_internal_listener_renders_client_auth_required`.
- `scram_sha_256_internal_listener_renders_sasl_ssl_with_256`.
- `scram_listener_without_kafkausers_still_reconciles`.
- `listener_mtls_requires_tls_validation_error_surfaces_status`.
- `auth_change_bumps_config_hash` — SCRAM → mTLS transition.
- `nodeport_listener_external_san_added_to_per_broker_cert`.

### Broker unit tests

`crates/broker/src/file_config.rs`:
- `per_listener_tls_config_deserializes`.
- `per_listener_sasl_config_deserializes`.
- `top_level_tls_config_still_parses_back_compat`.

`crates/broker/src/network/listener.rs`:
- `per_listener_tls_config_overrides_top_level`.
- `per_listener_sasl_mechanism_gates_handshake` —
  `SaslHandshake` for SCRAM-256 against a SCRAM-512-only listener →
  `UnsupportedSaslMechanism`.
- `clients_ca_truststore_rejects_cluster_ca_signed_cert` — data-plane
  mTLS listener rejects a broker cert (cluster-CA-signed).

### JVM acceptance tests

Existing slice-12/12b/29 differential tests against the cp-kafka JVM
oracle continue to cover the wire-level SCRAM and mTLS handshakes. No
new oracle tests required in slice 31 (no new on-wire surface).

### kind e2e (`.github/workflows/kind-e2e.yml` + helpers)

Three new scenarios:

1. **`scram_sha_512_tls_internal`**: deploy Kafka CR with internal
   listener `tls: true, authentication.type: scram-sha-512`; create
   KafkaUser SCRAM-512. Run `kafka-console-producer` Job using the
   user's password Secret — expect success. Run anonymous Job — expect
   `SaslAuthenticationException`.
2. **`mtls_internal`**: deploy Kafka CR with internal listener
   `tls: true, authentication.type: tls`; create KafkaUser TLS. Job
   mounting `user.crt`/`user.key` from the user Secret + the
   clients-CA — expect produce success. Job without client cert —
   expect TLS handshake rejection.
3. **`scram_sha_512_tls_nodeport`**: deploy Kafka CR with NodePort
   listener `tls: true, authentication.type: scram-sha-512`. Run
   `kafka-console-producer` on a host pod (network-namespace outside
   the cluster) connecting via NodePort hostname — validates SAN
   expansion end-to-end.

## Risks and mitigations

| Risk                                                              | Mitigation                                                                |
|-------------------------------------------------------------------|---------------------------------------------------------------------------|
| LB ingress can take minutes; cert issuance blocked → broker pod won't start until cert exists | Skip SAN expansion for that broker, issue with internal SANs only, requeue + status condition. Broker can run on the internal listener while LB is pending. |
| SAN-change reissue on every reconcile (churn)                     | Compare SAN lists deterministically (sorted, deduped) — only reissue on real change. Existing slice-30 idempotency tests catch regressions. |
| Per-listener TLS config change in the broker-side `file_config.rs` is a structural change to a slice-30-stable struct | Keep top-level `[tls_config]` working as fallback so slice-30 broker configs still parse. New per-listener TOML is purely additive. |
| Confusing failure mode when user sets `tls: false + auth: scram` and credentials leak in cleartext | `WeakAuth` Warning Event surfaces this on every reconcile; loud in `kubectl describe kafka`. |
| `WaitingForLoadBalancerIp` condition stuck if cloud provider misconfigured | Existing slice-25 reconcile behavior; status condition makes it diagnostically obvious. |

## Migration / compatibility

**Greenfield project — no migration.** Slice 30's broker config TOML
format gains per-listener subkeys; the slice-30 inter-broker top-level
`[tls_config]` block keeps working. Existing slice-30 configs parse
unchanged.

The slice-25 hard rejection of `tls: true` on data-plane listeners is
removed. Manifests that previously included `tls: true` and were
rejected will now reconcile successfully — they'll get an anonymous-TLS
listener unless they also add `authentication`.

## Implementation order (will become the plan)

1. **CRD schema** — add `ListenerAuthentication` enum + `authentication`
   field to `Listener`; update CRD schemars output. Snapshot the
   regenerated CRD YAML.
2. **Listener validation** — implement validation rules + status
   condition surfacing in `controller/listeners.rs`. Unit tests.
3. **`listener_protocol` mapping + render_broker_toml extension** —
   emit per-listener `[listeners.tls_config]` and
   `[listeners.sasl_config]` blocks. Unit tests + snapshots.
4. **Broker-side per-listener config parsing** — extend
   `ListenerSpec` deserialization, fallback logic, per-listener
   `TlsAcceptor` + `ConnectionAuth` wiring. Unit tests.
5. **Cluster CA SAN expansion** — extend `issue_broker_cert` signature,
   build SAN list in listener reconciler, plumb through.
6. **Cert reissue on SAN change** — extend `ensure_broker_certs`
   comparison logic. Unit tests.
7. **Reconcile pipeline integration** — connect listener-services →
   advertised-addrs → CA → broker-config flow. Integration tests
   (FIFO-mock harness).
8. **`WeakAuth` Warning Event + `WaitingForLoadBalancerIp` status
   condition** — wire into existing reconcile error paths.
9. **kind e2e scenarios** — three scenarios in
   `.github/workflows/kind-e2e.yml`.
10. **STATUS.md slice-31 entry + Helm chart sample update** — final
    documentation.
