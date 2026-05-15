# Slice 13: ACLs — Design Spec

## Goal

Replace slice 12's `super_user_name` stand-in with a real Kafka ACL
authorizer. Operators provision ACLs via `kafka-acls.sh`; every
relevant handler enforces the configured permissions. After this slice
a deployment can run in production with no super-user — fine-grained
per-principal, per-resource access control via the standard JVM
tooling.

Production-shape scope:
- Resources: `Topic`, `Group`, `Cluster`, `TransactionalId`.
- Patterns: `Literal` + `Prefixed`.
- Permissions: `Allow` + `Deny` (DENY wins).
- Default action: deny if any ACLs exist (or any super-user configured).
- Host filter: per-ACL IPv4 or `*` wildcard.

Out of scope: delegation tokens, `SCRAM-SHA-256` ACL principals,
IPv6 host filters, ACL audit log destinations beyond standard
`tracing`, ACL caches with TTL, the no-authorizer-configured behavior
toggle (`allow.everyone.if.no.acl.found=true`).

## Background

Slices 1–12b shipped a broker with authenticated identities (slice 12)
and a `super_user_name` config that maps a single authenticated
principal to "may do anything". That stand-in was always meant to be
replaced by a real authorizer. Real Kafka uses the `Authorizer`
plugin interface; KRaft's bundled `StandardAuthorizer` reads ACL
records from the metadata image and answers `authorize(principal,
host, resource, operation) -> Allow | Deny`.

This slice ports that behavior to Crabka: one `MetadataRecord`
variant per ACL entry (additive), one delete-by-filter variant, and a
pure-logic `crabka_broker::authorizer::authorize` function that every
gated handler consults before doing real work.

The slice also surfaces three new wire api_keys for `kafka-acls.sh`
to talk to: `CreateAcls` (30), `DeleteAcls` (31), `DescribeAcls` (29).
The schemas for these messages already exist in
`crates/protocol/generated/` from the slice-1 protocol-coverage work.

## Architecture

### Crates touched

| Crate | Change |
|-------|--------|
| `crabka-metadata` | New `AclEntry` + `AclEntryFilter`; new enums (`ResourceType`, `PatternType`, `AclOperation`, `PermissionType`); two new `MetadataRecord` variants; image storage + accessors. |
| `crabka-broker` | New `authorizer.rs` (pure-logic decision algorithm); 3 new wire handlers (`CreateAcls`, `DeleteAcls`, `DescribeAcls`); handler wiring across ~16 existing handlers. |
| `crabka-cli` | Optional `--add-acl` flag on `format` to seed ACLs at bootstrap. |
| `crabka-protocol` | No code change — the 3 ACL message types are already generated. |

### ACL entry shape

```rust
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum ResourceType { Topic, Group, Cluster, TransactionalId }

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum PatternType { Literal, Prefixed }

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum PermissionType { Allow, Deny }

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum AclOperation {
    All, Read, Write, Create, Delete, Alter, Describe,
    ClusterAction, DescribeConfigs, AlterConfigs, IdempotentWrite,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AclEntry {
    pub resource_type: ResourceType,
    pub resource_name: String,         // "foo" or "foo-" (prefix)
    pub pattern_type: PatternType,
    pub principal: String,             // "User:alice" or "User:*"
    pub host: String,                  // "*" or "192.168.1.1"
    pub operation: AclOperation,
    pub permission_type: PermissionType,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AclEntryFilter {
    pub resource_type: Option<ResourceType>,
    pub resource_name: Option<String>,
    pub pattern_type: Option<PatternType>,
    pub principal: Option<String>,
    pub host: Option<String>,
    pub operation: Option<AclOperation>,
    pub permission_type: Option<PermissionType>,
}
```

`AclEntryFilter` uses `Option<...>` for "match anything"; the wire-level
`Any` / `Match` sentinels from Kafka's schemas map to/from `None` in
the `CreateAcls` / `DeleteAcls` / `DescribeAcls` handlers.

### Decision algorithm

```rust
pub fn authorize(
    image: &MetadataImage,
    super_user_name: Option<&str>,
    req: &AuthorizationRequest,
) -> AuthorizationResult;
```

Algorithm:

1. **Compatibility shim**: if `super_user_name.is_none()` **and**
   `image.all_acls().next().is_none()` → ALLOW (preserves slice 11/12
   pre-ACL behavior; once ACLs or a super-user appear, deny-by-default
   kicks in).
2. If `principal.name == super_user_name` → ALLOW.
3. Collect matching ACLs:
   - `resource_type` exact match.
   - `resource_name` matches per `pattern_type`:
     `Literal` → equal; `Prefixed` → `request.resource_name.starts_with(acl.resource_name)`.
   - `principal` matches: literal equality or `acl.principal == "User:*"`.
   - `host` matches: `acl.host == "*"` or literal equality of the
     client's remote IP (string form).
   - `operation` matches: literal equality or `acl.operation ==
     AclOperation::All`.
4. If any matched ACL has `permission_type = Deny` → DENY.
5. Else if any matched ACL has `permission_type = Allow` → ALLOW.
6. Else → DENY.

### Image storage

```rust
acls_literal:  HashMap<(ResourceType, String), Vec<AclEntry>>,
acls_prefixed: HashMap<ResourceType, Vec<AclEntry>>,
```

Two collections; LITERAL hits the map directly, PREFIXED iterates the
per-resource-type vec (typically <100 entries per type). Lookup
function:

```rust
pub fn matching_acls<'a>(
    &'a self,
    rt: ResourceType,
    rn: &str,
) -> impl Iterator<Item = &'a AclEntry>;
```

Returns LITERAL entries at `(rt, rn)` plus PREFIXED entries whose
`resource_name` is a prefix of `rn`.

### Handler wiring matrix

| Handler | Operation | Resource | Failure code |
|---|---|---|---|
| `Produce` | Write | Topic (per topic) | `TOPIC_AUTHORIZATION_FAILED (29)` per-partition |
| `Fetch` | Read | Topic (per topic) | 29 per-partition |
| `Metadata` (per topic) | Describe | Topic | 29 (named) or silent-filter (fetch-all) |
| `CreateTopics` | Create | Cluster or Topic | `CLUSTER_AUTHORIZATION_FAILED (31)` or 29 |
| `DeleteTopics` | Delete | Topic | 29 per-topic |
| `AlterConfigs` / `IncrementalAlterConfigs` | AlterConfigs | Topic or Cluster | 29 or 31 |
| `CreatePartitions` | Alter | Topic | 29 |
| `DeleteRecords` | Delete | Topic | 29 |
| `ListGroups` (filter) | Describe | Group | silent filter |
| `DescribeGroups` | Describe | Group | `GROUP_AUTHORIZATION_FAILED (30)` |
| `DeleteGroups` | Delete | Group | 30 |
| `JoinGroup` | Read | Group | 30 |
| `OffsetCommit` | Read | Group + Read on Topic | 30 / 29 |
| `OffsetFetch` | Describe | Group + Read on Topic | 30 / 29 |
| `DescribeCluster` | Describe | Cluster | 31 |
| `AlterUserScramCredentials` | Alter | Cluster | 31 |
| `InitProducerId` (txn variant) | Write | TransactionalId | `TRANSACTIONAL_ID_AUTHORIZATION_FAILED (53)` |
| `AddPartitionsToTxn` / `EndTxn` / `Produce` (txn) | Write | TransactionalId + Write on Topic | 53 / 29 |
| `CreateAcls` | Alter | Cluster | 31 (whole-request) |
| `DeleteAcls` | Alter | Cluster | 31 |
| `DescribeAcls` | Describe | Cluster | 31 |

## Components

### `crabka-metadata`

- `src/acl.rs` (new) — enums + `AclEntry` + `AclEntryFilter`.
- `src/records.rs` — append:
  - `V1AccessControlEntry(AclEntry)`
  - `V1DeleteAccessControlEntry(AclEntryFilter)`
- `src/image.rs`:
  - Two new fields (`acls_literal`, `acls_prefixed`).
  - Initialize in `new`.
  - `apply` handles both new variants (LITERAL → insert into map, PREFIXED → append to vec; delete iterates and removes by filter match).
  - `validate` returns `Ok(())` for both (no pre-conditions; idempotent under last-write semantics).
  - Accessors: `matching_acls(rt, rn)`, `all_acls()`.

### `crabka-broker::authorizer` (new file, ~150 lines)

```rust
pub struct AuthorizationRequest<'a> {
    pub principal: &'a Principal,
    pub host: &'a SocketAddr,
    pub resource_type: ResourceType,
    pub resource_name: &'a str,
    pub operation: AclOperation,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AuthorizationResult { Allow, Deny }

pub fn authorize(
    image: &MetadataImage,
    super_user_name: Option<&str>,
    req: &AuthorizationRequest,
) -> AuthorizationResult;

/// Batch helper for `Produce` / `Fetch` / `Metadata` per-topic.
pub fn authorize_topics<'a>(
    image: &MetadataImage,
    super_user_name: Option<&str>,
    principal: &Principal,
    host: &SocketAddr,
    operation: AclOperation,
    topic_names: impl Iterator<Item = &'a str>,
) -> HashMap<&'a str, AuthorizationResult>;
```

### `crabka-broker` handlers

Three new handlers:

- `handlers/create_acls.rs` (api_key 30).
- `handlers/delete_acls.rs` (api_key 31).
- `handlers/describe_acls.rs` (api_key 29).

Existing handlers (16 sites listed above) get an authorization
preamble. The preamble is small (~10 lines) and consistent — only the
operation + resource type + error code vary.

Dispatch table additions: `network/dispatch.rs::handler_body_flexible`
gains 29/30/31 entries (all flexible from v2+). `api_versions.rs`
`supported_apis()` adds the three api_keys.

### `crabka-cli`

`format` subcommand gains a `--add-acl` flag, parseable as:

```
--add-acl 'principal=User:admin,host=*,operation=All,permission=Allow,resource=Cluster:kafka-cluster'
```

Parser builds an `AclEntry`; serialization piggybacks on the slice-12b
`bootstrap.records.bin` framing — ACL records get serialized
alongside SCRAM records in the same file. `Broker::start` already
reads and submits them on first start (slice 12b T8).

## Data flow

### Authorizing a Produce request

1. Authed client sends Produce covering topics `t1`, `t2`, `t3`.
2. Handler pulls `principal` from `ConnectionAuth::Authenticated{...}`
   and remote `host: &SocketAddr` from per-connection state.
3. For each topic, handler calls `authorize` with op=Write.
4. The authorizer walks: compatibility shim → super-user → matching
   ACLs (LITERAL hits map, PREFIXED iterates vec) → DENY-wins →
   ALLOW-wins → default DENY.
5. Per-topic result: `t1=Allow`, `t2=Deny`, `t3=Allow`.
6. Handler proceeds with `t1` + `t3`; `t2` partitions get
   `error_code=29` in the per-partition response row. Wire shape
   unchanged.

### Authorizing a Metadata request

**Client-requested topics** (`request.topics = Some([...])`):

1. Authorize each named topic with `Describe`.
2. Per topic: Allow → include with `error_code=0`. Deny → include
   with `error_code=29`. The client asked by name; it's told it
   can't see it.

**Fetch-all** (`request.topics = None`):

1. Authorize each topic in the image with `Describe`.
2. Only Allow-list topics included. Deny entries silently omitted —
   don't leak existence.

### Authorizing a JoinGroup + OffsetCommit

1. `JoinGroup` requires `Read` on the GROUP. Deny → `error_code=30`,
   connection stays open.
2. `OffsetCommit` requires `Read` on GROUP **and** `Read` on TOPIC
   for each partition's topic. Deny on GROUP → 30 on the whole
   request. Deny on a TOPIC → 29 per affected partition row.
3. `OffsetFetch` requires `Describe` on GROUP. With `topics = None`,
   per-topic `Read` filters silently.

### `CreateAcls`

1. Authorize caller with `Alter` on `Cluster`. Deny → 31 whole-request.
2. For each binding, validate (non-empty `resource_name` for
   non-wildcard patterns; `User:<name>` form; known enum values).
3. Submit each valid binding as `V1AccessControlEntry` in one
   batched `controller.submit_change(records)`.
4. Response: per-binding result row with `error_code=0` on success,
   per-binding specific code on validation failure.

### `DeleteAcls`

1. Same `Alter` on `Cluster` gate.
2. For each filter, scan the image; gather matched entries.
3. Submit one `V1DeleteAccessControlEntry(filter)` per filter (not
   per matched entry — the apply path deletes everything matching).
4. Response includes the matched entries so the client knows what
   was removed.

### `DescribeAcls`

1. `Describe` on `Cluster`. Deny → 31.
2. Walk image, return entries matching the filter. No metadata writes.

### Bootstrap with `crabka format --add-acl`

1. Operator: `crabka format --log-dir D --add-scram '...' --add-acl '...'`.
2. CLI parses the ACL spec, builds an `AclEntry`, serializes into
   `bootstrap.records.bin` after any SCRAM records.
3. On first start (slice 12b T8 path), the broker reads the file
   and submits the records. ACLs land in the image before clients
   connect. The seeded ACL is immediately enforced.

## Error handling

### Per-request authorization failures

| Scenario | Wire response |
|---|---|
| TOPIC op DENY | `TOPIC_AUTHORIZATION_FAILED (29)` per-partition / per-topic |
| GROUP op DENY | `GROUP_AUTHORIZATION_FAILED (30)` per-group |
| CLUSTER op DENY | `CLUSTER_AUTHORIZATION_FAILED (31)` whole-request |
| TRANSACTIONAL_ID op DENY | `TRANSACTIONAL_ID_AUTHORIZATION_FAILED (53)` |
| Anonymous on SASL listener | Cannot happen (pre-auth gate from slice 12 closes connection) |
| No matching ACL + no super-user + ACL store non-empty | DENY (default) — same wire shape as explicit DENY |

Logged at `debug` level — auth-denies are normal traffic. Operators
wanting an audit trail filter for `crabka_broker::authorizer=debug`.

### `CreateAcls` validation

Per-binding, returned in the response row (rest of the batch still
proceeds):

- Empty `resource_name` with `Literal` pattern → `INVALID_REQUEST (42)`.
- `resource_name` containing `\x00` → `INVALID_REQUEST (42)`.
- Unknown wire enum (`resource_type` / `pattern_type` / `operation` /
  `permission_type`) → `UNSUPPORTED_VERSION (35)`.
- Principal not in `User:<name>` form → `INVALID_REQUEST (42)`.

### `DeleteAcls` filter behavior

- Filter matching no entries → empty `matching_acls` with
  `error_code=0`. Not an error.
- Filter validation failure → per-filter `INVALID_REQUEST`.

### Submit-to-controller failures

- `submit_change` errors → `COORDINATOR_NOT_AVAILABLE (15)` on the
  failing binding(s). Subsequent bindings get `OPERATION_NOT_ATTEMPTED
  (55)`. Matches Kafka.

### Bootstrap-file load with ACL records

- Older broker, newer CLI: `V1AccessControlEntry` unknown variant →
  `serde_wincode` decode fails → `BrokerError::BootstrapFile`, broker
  refuses to start. Same behavior as slice 12b SCRAM path.

### Compatibility shim

When `super_user_name.is_none()` AND the image has zero ACL records →
authorizer returns ALLOW unconditionally. This keeps slice 11/12 tests
green without modification. Once an operator provisions one ACL or
sets `super_user_name`, deny-by-default kicks in.

## Testing

### Unit tests (`crabka-broker::authorizer`)

The matrix lives here — pure-logic, no I/O:

- `super_user_bypass_grants_everything`
- `compatibility_shim_allows_when_no_acls_and_no_super_user`
- `empty_image_denies_when_super_user_set_but_principal_mismatches`
- `literal_allow_matches_exact_name`
- `prefixed_allow_matches_prefix`
- `deny_wins_over_allow`
- `principal_wildcard_matches_any_user`
- `host_filter_matches_specific_ip`
- `host_wildcard_matches_any_ip`
- `operation_All_matches_any_op`
- `operation_specific_does_not_match_others`
- `mixed_pattern_types_independent`
- `authorize_topics_batch_returns_per_topic_decisions`

### Unit tests (`crabka-metadata::acl` + `image`)

- `acl_entry_round_trip` for each enum variant.
- `acl_entry_filter_round_trip`.
- `apply_v1_access_control_entry_literal_stores_in_literal_map`.
- `apply_v1_access_control_entry_prefixed_stores_in_prefixed_vec`.
- `apply_v1_delete_access_control_entry_removes_matching`.
- `apply_v1_delete_access_control_entry_no_match_is_noop`.
- `matching_acls_combines_literal_and_prefixed`.

### Integration tests (`crates/broker/tests/acl_handlers.rs`, no Docker)

- `create_acls_super_user_can_provision_and_describe`.
- `create_acls_non_super_user_rejected`.
- `delete_acls_removes_matching`.
- `produce_denied_without_topic_acl`.
- `produce_allowed_with_topic_write_acl`.
- `metadata_silent_filter_on_fetch_all`.
- `metadata_explicit_deny_on_named_topic`.
- `join_group_denied_without_group_read_acl`.
- `init_producer_id_denied_without_txn_acl`.

### JVM acceptance tests (Docker)

cp-kafka:7.5.0 only (KIP-554 + ACL flag set both require it):

- `jvm_kafka_acls_provision_via_cli` — `kafka-acls.sh --add` then `--list` round-trips.
- `jvm_authorized_produce_consume` — provisioned alice produces + consumes 10 records.
- `jvm_unauthorized_produce_fails` — bob with no ACLs gets `TopicAuthorizationException`.
- `jvm_unauthorized_consumer_fails_group_check` — alice with no group ACL gets `GroupAuthorizationException`.
- `jvm_prefixed_topic_acl_works` — `--resource-pattern-type prefixed` works end-to-end.

### Regression guards

- Slice 11 admin handler tests pass without modification (compat shim).
- Slice 12 SASL tests pass without modification (super-user-only path
  still works — slice 12's tests configure `super_user_name`).
- Slice 12b raft_sasl + bootstrap_consumption tests pass.

### Out of scope for tests

- IPv6 host filters.
- ACL rotation under live raft traffic.
- Audit log content assertions.

## Wire-protocol additions

| api_key | Name | Versions |
|---------|------|----------|
| 29 | DescribeAcls | v0–v3 |
| 30 | CreateAcls | v0–v3 |
| 31 | DeleteAcls | v0–v3 |

Schemas already generated in `crates/protocol/generated/`. Flexible-body
table in `dispatch.rs` adds entries; `api_versions.rs::supported_apis()`
exposes them.

## Out of scope

- Delegation tokens.
- `SCRAM-SHA-256` ACLs (single-mechanism principal naming only).
- IPv6 host filters.
- Audit log destinations beyond `tracing`.
- ACL caching with TTL — the metadata image is already the cache.
- `allow.everyone.if.no.acl.found=true` toggle — we have the compat
  shim instead (auto-disable when zero ACLs and no super-user).
- KIP-580 / KIP-679 ACL evolution (Kafka 3.5+ field additions).
