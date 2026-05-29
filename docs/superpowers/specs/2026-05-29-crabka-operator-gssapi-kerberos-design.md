# Operator GSSAPI (Kerberos) — Listener & Inter-Broker Authentication

**Date:** 2026-05-29
**Status:** Design — approved, pending implementation plan

## Summary

SASL/GSSAPI (Kerberos) is fully implemented in the broker *library* (commit
`701bea7`, #295) but is unreachable through the configuration surfaces the
Kubernetes operator drives. The broker can accept GSSAPI from clients and
initiate GSSAPI to peer brokers only when its in-process `BrokerConfig` is
constructed directly (as the e2e tests do) — there is **no TOML surface** for
either path, and the operator has **no CRD surface** to express either.

This work closes that gap end to end, for both:

1. **Client-facing listener authentication** — clients authenticate to brokers
   via GSSAPI (`ListenerAuthentication::Gssapi`).
2. **Inter-broker authentication** — brokers authenticate to each other via
   Kerberos, derived from a GSSAPI inter-broker listener.

It does not change any broker handshake logic — that already works. It adds
broker TOML config surface, operator CRD surface, reconciliation/mounting, and
fixes stale documentation.

## The gap

| Layer | Client-listener GSSAPI | Inter-broker GSSAPI |
|---|---|---|
| Broker handshake logic | ✅ `network/auth.rs` accept path | ✅ `network/client.rs` initiate path (`run_gssapi_client`) |
| Broker programmatic config | ✅ `BrokerConfig.gssapi` | ✅ `BrokerConfig.inter_broker_credentials` (`InterBrokerCredentials::Gssapi`) |
| Broker **TOML** (`FileConfig`) | ❌ no `[gssapi]` block | ❌ no inter-broker credentials at all |
| Operator **CRD** | ❌ no enum variant | ❌ no field |
| Operator **render / mount** | ❌ | ❌ |
| README feature tables | ❌ stale (`SASL/GSSAPI … ❌`) | partial |

Verified facts the design relies on:

- `InterBrokerCredentials::Gssapi` has exactly `{ keytab_path, client_principal,
  service_name, kdc_url }` (`crates/broker/src/config.rs`). The dialed peer host
  is supplied separately at connect time and combined into the target SPN
  `service_name/host` inside `run_gssapi_client` — it is **not** a config field.
- `GssapiConfig` (`crates/security/src/gssapi/mod.rs`) is
  `{ keytab_path, service_name, principal_to_local_rules: Vec<name::Rule>, realm:
  Option<String>, kdc: Option<String> }`.
- `FileConfig` (`crates/broker/src/file_config.rs`) has `inter_broker_listener_name`
  but no inter-broker *credentials* field of any kind (not PLAIN, SCRAM, or
  GSSAPI). This `[inter_broker_credentials]` block is the first such surface.
- The server *accept* path does not require KDC contact (the broker decrypts the
  AP-REQ with its own service key from the keytab); `kdc`/`realm` are carried for
  parity and for the initiate path.

## Design decisions (settled during brainstorming)

- **Scope:** both the broker TOML surface and the operator CRD surface, as one
  coherent feature, end to end.
- **Inter-broker is in scope** (not just client listeners).
- **CRD field set:** full `GssapiConfig` parity for the client listener.
- **Inter-broker activation:** derived from the inter-broker listener — when
  `inter_broker_listener_name` resolves to a `type: gssapi` listener, reuse that
  listener's keytab Secret for initiating.
- **Client principal:** a single shared principal cluster-wide (no per-broker
  host-templated SPNs).
- **KDC discovery:** explicit `kdcUrl` by default, with an *optional*
  process-wide `krb5.conf` mount for advanced setups.

## 1. Broker TOML surface — `crates/broker/src/file_config.rs`

### 1a. `[gssapi]` block → `BrokerConfig.gssapi: Option<GssapiConfig>`

```toml
[gssapi]
keytab_path = "/etc/crabka/gssapi-keytab/keytab"
service_name = "kafka"                 # default "kafka"
principal_to_local_rules = ["RULE:[1:$1@$0](.*@EXAMPLE.COM)s/@.*//", "DEFAULT"]
realm = "EXAMPLE.COM"                  # optional
kdc = "tcp://kdc:88"                   # optional
```

- New `FileGssapiConfig` struct (`#[serde(deny_unknown_fields)]`, matching the
  surrounding file-config structs).
- `principal_to_local_rules` is `Vec<String>` in TOML, parsed into
  `Vec<name::Rule>` during `apply_to()`. On a malformed rule, panic with context
  (`"[gssapi]: invalid principal_to_local rule {rule:?}: {e}"`), mirroring how
  `[oauthbearer]` validates its JsonPath expressions at load time.
- `service_name` defaults to `"kafka"` when omitted.

### 1b. `[inter_broker_credentials]` block → `BrokerConfig.inter_broker_credentials`

The first inter-broker credentials TOML surface. Only the `gssapi` variant is
added now (PLAIN/SCRAM inter-broker over TOML is not requested — see Non-goals).

```toml
[inter_broker_credentials]
type = "gssapi"
keytab_path = "/etc/crabka/gssapi-keytab/keytab"   # reused from the IB listener's keytab
client_principal = "kafka@EXAMPLE.COM"
service_name = "kafka"
kdc_url = "tcp://kdc:88"
```

- New `FileInterBrokerCredentials` struct with a `type` discriminator; maps to
  `InterBrokerCredentials::Gssapi { .. }` in `apply_to()`.
- An unknown/other `type` value is a load-time error.

## 2. Operator CRD additions

### 2a. Client listener — `ListenerAuthentication::Gssapi` (`crates/operator/src/crd/listener.rs`)

Full `GssapiConfig` parity. New variant on the `ListenerAuthentication` enum
(serde tag `"gssapi"`) carrying a `ListenerAuthenticationGssapi` struct:

```yaml
authentication:
  type: gssapi
  keytabSecretRef: { secretName: kafka-keytab, key: kafka.keytab }
  serviceName: kafka                    # default "kafka"
  principalToLocalRules: ["RULE:[1:$1@$0](.*@EXAMPLE.COM)s/@.*//", "DEFAULT"]
  realm: EXAMPLE.COM                    # optional
  kdc: tcp://kdc:88                     # optional
```

- `keytabSecretRef` is a `{ secretName, key }` reference (same shape as the OAuth
  `clientSecret` ref) to a Secret in the `Kafka` CR's namespace.
- Add `"gssapi"` to the `enum` discriminator list and add the GSSAPI sibling
  fields to the hand-written `listener_authentication_schema()` (the same
  `schema_with` workaround the enum already uses for the OAuth variant).

### 2b. Inter-broker — `Kafka.spec.interBrokerKerberos`

Derived-from-the-listener model: the keytab comes from the inter-broker
listener's GSSAPI config; this block supplies only the initiate-specific bits.

```yaml
spec:
  interBrokerKerberos:                  # required iff the IB listener is type:gssapi
    clientPrincipal: kafka@EXAMPLE.COM  # single shared principal
    serviceName: kafka                  # default "kafka"
    kdcUrl: tcp://kdc:88
```

Placed as a dedicated `spec` block (not hung off the listener) because it is
initiate-only configuration that does not belong on the accept-side listener
auth struct.

### 2c. Process-wide krb5.conf — `Kafka.spec.krb5ConfSecretRef` (optional)

```yaml
spec:
  krb5ConfSecretRef: { secretName: krb5-conf, key: krb5.conf }   # optional
```

A single cluster-level reference because `krb5.conf` is process-global and serves
both the accept and initiate paths. When set, the operator mounts it and sets
`KRB5_CONFIG` on broker pods. When unset, `kdcUrl` / per-listener `kdc` drives
KDC discovery.

## 3. Reconciliation & secret mounting

### `crates/operator/src/controller/listeners.rs`

- `sasl_mechanism()`: add a `ListenerAuthentication::Gssapi(_) => Some(SaslMechanism::Gssapi)` arm.
- `listener_protocol()`: `gssapi` + `tls: false` → `SASL_PLAINTEXT`; `gssapi` +
  `tls: true` → `SASL_SSL`. Mirrors SCRAM. GSSAPI carries its own RFC 4752
  security layer, so TLS is not required.
- `render_broker_toml()`:
  - emit the single broker-global `[gssapi]` block from the GSSAPI listener's
    config (see "Broker-global GSSAPI config" below);
  - append `GSSAPI` to that listener's `sasl_mechanisms`;
  - emit `[inter_broker_credentials]` (type `gssapi`) when the resolved
    inter-broker listener is a GSSAPI listener, reusing its keytab path and
    pulling `client_principal` / `service_name` / `kdc_url` from
    `spec.interBrokerKerberos`.
  - The function signature grows an inter-broker-kerberos parameter (and whatever
    the keytab mount path constant is).

### `crates/operator/src/controller/kafka_node_pool.rs`

- Keytab mount: reuse the OAuth `clientSecret` **projected-items** pattern — mount
  the user's keytab Secret at a fixed path (`/etc/crabka/gssapi-keytab/keytab`)
  regardless of the user's key name, so the broker reads a stable path.
- krb5.conf mount (when `krb5ConfSecretRef` set): mount at a fixed path and set
  the `KRB5_CONFIG` env var on the broker container.

### Broker-global GSSAPI config (important constraint)

`BrokerConfig.gssapi` is a single broker-global `Option<GssapiConfig>` — there is
**one** `[gssapi]` block per broker, not one per listener. The CRD nonetheless
carries the GSSAPI config inside each listener's `authentication` (where it
belongs ergonomically and where `keytabSecretRef` lives). The reconciler must
collapse these to one effective config:

- All `type: gssapi` listeners on a cluster must agree on `keytabSecretRef`,
  `serviceName`, `principalToLocalRules`, `realm`, and `kdc`. Divergent values →
  `ListenersValid=False` (reason `ListenerGssapiConfigConflict`).
- The single agreed config is rendered as the broker-global `[gssapi]` block;
  each GSSAPI listener still gets `GSSAPI` appended to its own `sasl_mechanisms`.
- The inter-broker `[inter_broker_credentials]` keytab path reuses this same
  agreed keytab mount.

## 4. Validation rules (reconciler `ListenersValid` condition)

New error reason strings follow the existing `ListenerOauth*` naming convention
(e.g. `ListenerGssapiKeytabSecretMissing`).

- `type: gssapi` requires `keytabSecretRef` with both `secretName` and `key`
  non-empty; the referenced Secret (and key) must exist — mirrors the OAuth
  `clientSecret` existence check.
- If the resolved inter-broker listener is `type: gssapi`, `spec.interBrokerKerberos`
  is required and must carry `clientPrincipal` and `kdcUrl`; otherwise
  `ListenersValid=False` with a GSSAPI-specific reason.
- `principalToLocalRules` entries must parse as `name::Rule`; bad syntax →
  `ListenersValid=False`.
- If `krb5ConfSecretRef` is set, the referenced Secret/key must exist.
- Multiple `type: gssapi` listeners must agree on every GSSAPI config field
  (broker-global `[gssapi]` constraint above); divergence →
  `ListenerGssapiConfigConflict`.

## 5. Documentation — the "operator table" and friends

- README "Security" table: `SASL/GSSAPI (Kerberos)` `❌` → `✅`.
- README "Kubernetes operator" table: `Listener auth wiring (TLS / SCRAM)` →
  `(TLS / SCRAM / OAuth / Kerberos)` — this also corrects the already-stale
  omission of OAuth, which the operator has supported since slices 49/50.
- README `crabka-security` crate row: add GSSAPI to the listed mechanisms.
- README roadmap line: remove SASL/GSSAPI from "still cooking".
- KIP-12 (SSL & SASL/Kerberos) row: `⚠️` → `✅` once both client and inter-broker
  GSSAPI land.

## 6. Testing

- **Broker (`file_config.rs` unit tests):** parse `[gssapi]` and
  `[inter_broker_credentials]` blocks and assert the resulting `BrokerConfig`
  (`gssapi`, `inter_broker_credentials`); assert malformed
  `principal_to_local_rules` and unknown inter-broker `type` fail at load.
- **Operator (`crates/operator/tests/reconcile_listener_auth.rs`):**
  - a GSSAPI listener renders the expected `[gssapi]` TOML, appends `GSSAPI` to
    `sasl_mechanisms`, and mounts the keytab Secret;
  - an inter-broker GSSAPI listener emits `[inter_broker_credentials]`;
  - validation rejects: missing keytab Secret, missing `interBrokerKerberos` when
    the IB listener is GSSAPI, and unparseable `principalToLocalRules`.
- Reuse the existing MIT-KDC fixture (`crates/security/tests/fixtures/kdc/`) only
  where an e2e check adds value; introduce no new KDC infrastructure.

## 7. Non-goals

- No PLAIN/SCRAM inter-broker TOML surface — only the `gssapi`
  `[inter_broker_credentials]` variant.
- No per-broker host-templated SPNs — a single shared client principal only.
- No operator-managed keytab *generation* — the admin supplies the keytab
  Secret (and any krb5.conf).
- No changes to broker GSSAPI handshake logic — it already works.

## Files touched (anticipated)

- `crates/broker/src/file_config.rs` — `[gssapi]` + `[inter_broker_credentials]`
  parsing and `apply_to()` mapping.
- `crates/operator/src/crd/listener.rs` — `ListenerAuthentication::Gssapi`,
  `ListenerAuthenticationGssapi`, schema function.
- `crates/operator/src/crd/kafka.rs` — `spec.interBrokerKerberos`,
  `spec.krb5ConfSecretRef`.
- `crates/operator/src/controller/listeners.rs` — `sasl_mechanism()`,
  `listener_protocol()`, `render_broker_toml()`, validation.
- `crates/operator/src/controller/kafka_node_pool.rs` — keytab + krb5.conf mounts,
  `KRB5_CONFIG` env.
- `crates/operator/tests/reconcile_listener_auth.rs` — operator tests.
- `crates/broker/src/file_config.rs` tests — broker TOML tests.
- `README.md` — feature/KIP tables.
