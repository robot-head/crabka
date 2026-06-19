# Crabka — FedRAMP 20x MLA Audit & Compliance Subsystem

**Date:** 2026-06-18
**Status:** Design (approved for planning)
**Scope:** Full FedRAMP 20x *Monitoring, Logging, and Auditing* (MLA) Key Security Indicator suite, delivered as implementation slices.

---

## 1. Purpose & framing

FedRAMP 20x defines a set of **Key Security Indicators (KSIs)** for *Monitoring, Logging, and Auditing*. A cloud service offering (CSO) that embeds or deploys Crabka must be able to demonstrate these indicators *for the Crabka component*. This design adds the **pre-built support** a CSO needs: Crabka ships the audit-grade event trail, tamper-evidence, SIEM-ready export, log-access controls, and config-posture tooling that the KSIs depend on.

### The five MLA KSIs

| KSI | Title | Requirement (abridged) |
|---|---|---|
| **KSI-MLA-LET** | Logging Event Types | Maintain a list of resources/event types to log, monitor, audit — and do so. |
| **KSI-MLA-OSM** | Operating SIEM Capability | Centralized, **tamper-resistant** logging of events, activities, changes. |
| **KSI-MLA-RVL** | Reviewing Logs | Persistently review and audit logs. |
| **KSI-MLA-EVC** | Evaluating Configurations | Persistently evaluate/test configuration, especially IaC. |
| **KSI-MLA-ALA** | Authorizing Log Access | Least-privilege, role/attribute-based, just-in-time access to log data. |

### Boundary of responsibility

A *broker* cannot satisfy every KSI on its own. **Operating** the SIEM, performing **human log review**, and running the **IaC scanner** remain CSO responsibilities. Crabka's job is to provide the raw material and hooks those activities consume. This design draws that line explicitly per KSI (see §11).

**Out of scope (noted for operators):** FIPS-validated cryptographic *provider* selection (e.g. rustls FIPS backend) belongs to the FedRAMP **Cryptography** KSI family, not MLA — flagged but tracked separately. Audit *signing* algorithms used here are nonetheless chosen to be FIPS-approved (§5).

### Current state (baseline)

Crabka today has structured JSON application logs (`tracing` + `logfmt`), Prometheus metrics (`/metrics`), OTLP tracing (`crates/telemetry`), and a full auth/authz stack (`crates/security`, `crates/authz`). It has **no audit-grade security event trail** — no structured "who did what to which resource, when, allowed or denied" record, no tamper-evidence, no SIEM export, no log-access controls, no config-posture evaluation. That gap is what this design fills.

---

## 2. Design decisions (locked)

| Area | Decision |
|---|---|
| Delivery model | **Kafka-native**: a dedicated internal `__crabka_audit` topic is the primary audit interface. |
| Failure policy (AU-5) | **Spool + async replay** by default; **fail-closed opt-in** per event class. |
| Event coverage (LET) | **Control-plane always**; **data-plane configurable** (`off`/`deny_only`/`all`), deny-only default when enabled. |
| Tamper-evidence (OSM) | **Per-broker hash-chain + periodic signed checkpoints**. |
| Record schema | **OCSF** (Open Cybersecurity Schema Framework) JSON on the topic. |
| Config evaluation (EVC) | **Hardening baseline + `check-config` tool + runtime posture events/metric**. |
| Log access (ALA) | **Dedicated audit roles, super-user excluded, write-locked topic, JIT via delegation tokens**. |

---

## 3. Architecture

### 3.1 New crate: `crates/audit`

A self-contained crate owning the portable, broker-agnostic core so the integrity-critical logic is unit- and model-testable in isolation:

- **Event model** — an internal `AuditEvent` enum (the source of truth for the LET catalog).
- **OCSF serializer** — maps each `AuditEvent` to its OCSF class + fields as JSON.
- **Hash-chain + signer** — per-broker chain state, checkpoint emission, signature creation/verification.
- **Spool** — durable local append-only buffer for the AU-5 degraded path.
- **`AuditLog` handle** — the writer API the broker calls.

The crate has **no dependency on broker internals**. The broker passes in an append sink (the internal partition-append closure) and a signing-key provider. This keeps chain/signing/OCSF logic portable and testable without a running broker.

### 3.2 Instrumentation points (broker → `AuditLog`)

| Location | Events emitted |
|---|---|
| `crates/broker/src/network/auth.rs` | Authentication success / failure (mechanism, principal, source endpoint). |
| **Authorizer decorator** wrapping `authz::Authorizer::authorize` | Authorization **denies** centrally — no admin handler can forget to audit a denial. |
| `crates/broker/src/handlers/*` (admin) | Operation semantics: topic create/delete, partition & config changes, ACL CRUD, SCRAM credentials, delegation-token ops, reassignments, quota/leadership changes — with before/after values where meaningful. |
| Broker lifecycle | Start/stop, config apply, TLS reload. |
| EVC subsystem (§6) | Config-posture / drift events. |

The authorizer decorator is the key design lever: routing *all* deny decisions through one wrapper guarantees coverage independent of per-handler discipline. Admin handlers additionally emit operation-semantic events (the *what changed*, not just *was it allowed*).

### 3.3 Kafka-native write path

The `__crabka_audit` topic is partitioned by **broker affinity**: it has ≥ N partitions and **each broker leads its own partition**. A broker writes its records to *its own* partition through the **internal partition-append path** — the same path replication uses — so the common case is a **local append with no network round-trip**, then replicated to followers at `acks=all` / `min.insync.replicas=2`.

Consequences:
- The **hash-chain is per-broker** (one clean monotonic chain per partition) — no cross-broker ordering coordination needed.
- The leader's local log is the **durability floor**: an appended record is never lost even before replication completes.
- Replication provides availability + tamper-resistance (a single-host segment rewrite is detectable against replicas + the chain).

### 3.4 Topic properties

- Internal (`__` prefix), auto-created at controller bootstrap.
- Partitions ≥ broker count (broker-affinity); RF `min(3, brokers)`; `min.insync.replicas = 2`.
- **Compaction off** (audit is an append-only event log, never keyed-collapsed).
- Retention: a compliant default (operator-tunable), with optional **KIP-405 remote-storage tiering** to object storage for long-term retention (AU-11).
- **Write-locked** to the internal broker principal — no external producer can forge records.

### 3.5 Consumption & circular-audit rule

SIEMs ingest OCSF JSON directly off the topic (native consumer or Kafka Connect). To avoid an audit-write feedback loop, a hard rule applies:

> **Operations on `__crabka_audit` are never data-plane-audited** — writing audit records produces no audit records. **Administrative** changes to the audit topic (retention, ACL, delete) **are** audited (by the `audit-admin` role, §7).

---

## 4. Data flow

```
auth / authz-decorator / admin handler / lifecycle / EVC
        │  AuditEvent
        ▼
   crates/audit::AuditLog
        │  OCSF-serialize → assign seq + prev_hash → record headers
        ▼
   internal partition-append (this broker's __crabka_audit partition)
        │
   ┌────┴───────────── durable & replicated? ───────────────┐
   │ yes                                                      │ no (under-replicated /
   ▼                                                          │  leader move / disk)
 committed to topic                                           ▼
   │                                              durable local spool
   │                                                          │  background replayer
   │                                                          ▼  drains on recovery
   │                                              re-append to topic (chain continuous)
   ▼
 SIEM consumers (audit-reader role) ← OCSF JSON
   ▲
 every N records / T seconds: signed checkpoint record anchors chain head
```

For **fail-closed event classes**, if the record cannot be durably persisted to topic *or* spool, the **originating operation is rejected** instead of proceeding unaudited.

---

## 5. Tamper-evidence (KSI-MLA-OSM)

- **Per-record chaining.** Each record carries `seq` (monotonic per broker) and `prev_hash` (hash of the prior record in this broker's chain) in its Kafka record headers. Any insertion, deletion, or reorder breaks the chain and is detectable — even against on-disk segment rewrites — up to the most recent signed checkpoint. Records written after the last checkpoint (the unsigned window) are chain-continuous but not signature-attested; a cleanly-stopped broker emits a final checkpoint covering the tail. Chain-only mode (no signing key configured) provides continuity detection only, with no signature attestation over any records.
- **Signed checkpoints.** Every *N* records or *T* seconds the broker emits a checkpoint record: a signature over `{broker_id, seq_range, chain_head_hash, timestamp, key_id}` using the broker's audit signing key.
- **Algorithm.** FIPS-approved — **Ed25519 (FIPS 186-5)** or **ECDSA P-256** (final selection in the plan).
- **Key management.** Keys sourced from config / file / KMS. **Rotation** via `key_id` carried on each checkpoint, so a chain spans key epochs verifiably.
- **Offline verification.** A `crabka-audit verify` CLI walks a partition, validates chain continuity and every checkpoint signature, and reports the first break (seq + reason).

---

## 6. Durability & AU-5 response

The degraded path triggers when a record cannot reach a **durable, replicated** state (under-replication below `min.insync.replicas`, partition leadership moving, disk pressure) or when local append itself fails:

1. The record is written to a **durable local spool** (append-only file; the hash-chain stays continuous across spool → topic).
2. A **background replayer** drains the spool into the topic on recovery.
3. `crabka_broker_audit_*` metrics and a log alert fire (spool depth, replay lag, drop count).
4. Event classes flagged **fail-closed** in config (e.g. ACL changes, super-user actions) instead **reject the originating operation** if the record can't be durably persisted to topic-or-spool.

Everything not flagged fail-closed stays available; the spool bounds loss to catastrophic local-disk failure, which the replicated topic + per-broker chain make evident.

---

## 7. Log access model (KSI-MLA-ALA + AC-5)

- **Dedicated authorizations** `audit-reader` and `audit-admin` gate read and administration of `__crabka_audit`.
- **Super-user exclusion.** The authorizer treats the audit topic specially: **cluster super-user status does not grant** the audit roles. A broker admin cannot silently read or purge the trail — enforcing **separation of duties (AC-5)**.
- **Write-lock.** The topic is write-locked to the internal broker principal; no external principal (super-user included) can produce to it.
- **Audited administration.** Retention / delete / ACL changes on the audit topic are themselves audited, attributed to `audit-admin`.
- **JIT access.** Short-lived **delegation tokens scoped to audit-read** (reusing KIP-48) give a reviewer time-boxed access without a standing grant.

---

## 8. Config evaluation (KSI-MLA-EVC)

1. **Hardening baseline.** A documented secure-by-default profile (TLS required on client listeners, anonymous auth off, ACL authorizer on with deny-by-default, audit enabled, fail-closed set for sensitive classes, …), each rule annotated with its NIST control.
2. **`crabka-broker check-config`.** Evaluates the running/declared config against the baseline, prints a pass/fail/drift report with control references, and **exits non-zero on failure** so it drops into operator CI as the IaC-evaluation gate.
3. **Runtime posture.** The broker re-evaluates periodically, emits a `compliance_posture` audit event **on change** (drift is itself auditable/alertable), and exposes a `crabka_broker_compliance_posture` gauge.

---

## 9. Event catalog (KSI-MLA-LET) & OCSF mapping

The maintained "list of event types" is the `AuditEvent` enum, mapped to OCSF:

| Crabka event | OCSF class |
|---|---|
| SASL / mTLS authn success & failure | Authentication (3002) |
| Authorization denial (any resource) | Authorize Session (3003) / activity disposition |
| Topic create/delete, partition/config change | API Activity (6003) |
| ACL create/delete | Account Change (3001) / API Activity |
| SCRAM credential & delegation-token ops | Account Change (3001) |
| Reassignments, quota/leadership changes | API Activity (6003) |
| Broker start/stop, config apply, TLS reload | API Activity (6003) |
| Config-posture / drift | Compliance / Config State |

**LET-completeness guarantee:** a conformance test asserts **every catalog entry actually emits a record**, and the catalog doc is **generated from the same source of truth** (the enum) — so the documented list and the code cannot silently drift apart.

---

## 10. Configuration surface

New `[audit]` section in `broker.toml`:

```toml
[audit]
enabled = true                       # part of the hardening baseline
topic = "__crabka_audit"
partitions = "per-broker"            # broker-affinity
replication_factor = 3
min_insync_replicas = 2
retention = "..."                    # compliant default; remote-tiering optional
data_plane = "deny_only"             # off | deny_only | all
data_plane_resources = []            # optional selectors when data_plane != off
fail_closed_classes = ["acl_change", "super_user_action"]

[audit.signing]
algorithm = "ed25519"                # FIPS-approved
key_source = "..."                   # config | file | kms
key_id = "..."

[audit.checkpoint]
every_n = 1000
every_secs = 60

[audit.spool]
dir = "..."
max_bytes = "..."
```

Defaults are compliant out of the box; `enabled = true` is part of the hardening baseline (§8).

---

## 11. KSI → NIST control → evidence

| KSI | Primary NIST controls | Crabka evidence |
|---|---|---|
| MLA-LET | AU-2, AU-12, AC-6.9 | OCSF catalog + completeness test; data-plane toggle |
| MLA-OSM | AU-4, AU-5, AU-8, AU-9, SI-7.7 | Audit topic; hash-chain + signed checkpoints; `verify` CLI; spool/AU-5 policy; remote-storage tiering |
| MLA-RVL | AU-6, SI-4 | OCSF-on-topic for SIEM review; posture/drift events |
| MLA-EVC | CA-7, CM-2, CM-6, SI-7.7 | Hardening baseline; `check-config`; posture gauge |
| MLA-ALA | SI-11, AC-5, AC-6 | Dedicated audit roles; super-user exclusion; JIT tokens; write-lock |

---

## 12. Implementation slices

Each slice gets its own plan. File sets are mostly disjoint, so several can batch in parallel (per `CLAUDE.md` parallel-subagent guidance).

1. **Audit core + write path** — `crates/audit` (event model, OCSF serializer, `AuditLog`); `__crabka_audit` topic + broker-affinity append; control-plane instrumentation (authn, authorizer-decorator denies, admin handlers, lifecycle). *Foundation; everything depends on it.*
2. **Tamper-evidence** — per-broker hash-chain, signed checkpoints, key management/rotation, `crabka-audit verify` CLI.
3. **Durability / AU-5** — spool + replay, fail-closed classes, metrics/alerts.
4. **ALA access model** — `audit-reader`/`audit-admin` roles, super-user exclusion, write-lock, audited retention/delete, JIT delegation tokens.
5. **Data-plane auditing** — configurable `deny_only`/`all`, resource selectors, sampling (high-volume, optional path).
6. **EVC** — hardening baseline, `check-config`, posture events + gauge.
7. **Compliance pack** — KSI→control mapping doc + generated LET catalog (finalized last, drafted alongside).

**Dependency spine:** 1 → 2 → 3 → 4. Slices 5, 6, 7 hang off slice 1 and can run in parallel batches.

---

## 13. Testing strategy

- **Unit (`crates/audit`, no broker):** OCSF mapping golden tests; hash-chain continuity; signature verify; key rotation across epochs; spool replay ordering.
- **Integration:** each catalog event type produces the expected record; ACL enforcement on the audit topic; **super-user exclusion** (negative test); fail-closed actually rejects the originating op under simulated unavailability; spool → topic replay after recovery.
- **Conformance:** LET-completeness (every catalog entry emits) + `check-config` golden reports.
- **Model-checking opportunity** (fits the existing stateright program): two invariants under interleavings —
  1. *No fail-closed operation ever commits without a durable audit record.*
  2. *The per-broker chain stays continuous across spool / replay / failover.*

  Proposed, not mandated.

---

## 14. Open items for planning

- Final signing algorithm choice (Ed25519 vs ECDSA P-256) and KMS integration surface.
- Exact OCSF field-level mapping per event class (class UIDs fixed in §9; field mapping in the plan).
- Compliant retention default value and remote-tiering interaction with the audit topic.
- `audit-reader` / `audit-admin` representation in the existing ACL model (new resource type vs reserved resource name).
