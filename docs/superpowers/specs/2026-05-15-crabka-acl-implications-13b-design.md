# Slice 13b: ACL implications + multi-super-user — Design Spec

## Goal

Two small polish items on top of slice 13:

1. **Operation implications.** Real Kafka's `StandardAuthorizer` treats
   `Read`, `Write`, `Delete`, `Alter` as implying `Describe`, and
   `AlterConfigs` as implying `DescribeConfigs`. Our slice-13
   authorizer has neither; every existing test workaround that seeds an
   explicit `Describe` ACL alongside `Read`/`Write`/etc. exists only
   because of this gap. Add the implications and remove the
   workarounds.
2. **Multi-super-user.** Replace `BrokerConfig::super_user_name:
   Option<String>` with `super_users: HashSet<String>` so deployments
   can configure multiple privileged identities, matching real Kafka's
   `super.users=User:a;User:b` config.

Out of scope: ACL audit logging (debug log → operator-facing channel),
`User:` prefix handling in super_user config strings, ACL caching,
delegation tokens.

## Background

Slice 13 shipped a Kafka-compatible ACL system but explicitly did NOT
implement Kafka's operation-implication semantics. The slice-13 T25
report flagged this: "Crabka's authorizer does not implement Kafka's
'Read/Write implies Describe' operation implication. Tests work around
by seeding explicit Describe ACLs alongside Read/Write — production
deployments should do the same until an implication slice lands."

The gap visibly affects every standard `kafka-acls.sh` workflow: an
operator running `kafka-acls --add --operation Read --topic foo
--allow-principal User:alice` expects alice to be able to call
`Metadata foo` immediately. In Kafka semantics that works because
`Read` implies `Describe`. In Crabka 13 it doesn't — alice needs a
separate `--operation Describe` ACL.

The slice-12 SCRAM JVM tests (after the slice-13 T27 super-user-bypass
fix) and slice-13's own ACL JVM tests both carry this workaround.

The `super_users` rename is a smaller item but lands cleanly here: it's
a workspace-internal config-field change with the same shape as the
implication update (touch every ACL call site once).

## Architecture

### Operation implications

A small `implies(stored, requested) -> bool` helper lives in
`crabka-broker::authorizer`. It returns `true` for the table:

| Stored operation | Implies (matches when requested is...) |
|---|---|
| `Read` | `Describe` |
| `Write` | `Describe` |
| `Delete` | `Describe` |
| `Alter` | `Describe` |
| `AlterConfigs` | `DescribeConfigs` |
| `All` | Everything (existing semantics, kept) |

`matches_operation(entry, requested)` calls `implies` after the exact-
match and `All` checks fail. The decision algorithm in `authorize`
otherwise stays exactly the same.

The implication table is one-way:
- `Describe` does NOT imply `Read`.
- `DescribeConfigs` does NOT imply `AlterConfigs`.

The table is not resource-type-scoped — `Read implies Describe` works
on `Topic`, `Group`, `Cluster`, and `TransactionalId`. (Kafka behaves
the same way.)

### Multi-super-user

```rust
pub struct BrokerConfig {
    // ... existing fields ...
    pub super_users: std::collections::HashSet<String>,
}
```

`HashSet<String>` over `Vec<String>`:
- O(1) `.contains` on every `authorize` call.
- Dedupes free.

Empty set is the "no super-user" state, equivalent to slice-12's
`super_user_name = None`. Compat shim's "no super-user" check uses
`super_users.is_empty()`.

No wire format change. Config string format kept as bare name (no
`User:` prefix) for slice-12/13 backward compatibility.

### `authorize` signature

```rust
pub fn authorize(
    image: &MetadataImage,
    super_users: &HashSet<String>,
    req: &AuthorizationRequest,
) -> AuthorizationResult;
```

`&HashSet<String>` so call sites pass `&broker.config.super_users`
directly without allocation. `authorize_topics` (the batch helper)
takes the same signature shape.

### Decision flow (post-implications)

1. **Compat shim**: `super_users.is_empty() && image.all_acls().next().is_none()` → ALLOW.
2. **Super-user bypass**: `super_users.contains(&req.principal.name)` → ALLOW.
3. **Match loop** on `image.matching_acls(rt, rn)`:
   - For each entry, `matches_operation(entry, req.operation)` — exact
     match OR `All` OR `implies(entry.operation, req.operation)`.
   - Principal + host axes unchanged.
   - DENY-wins, ALLOW second, default DENY.

### Workaround removal

After implications land, the explicit `Describe` ACLs in these tests
are redundant:

- **Slice-13 JVM tests** (`tests/jvm_acceptance.rs`):
  `jvm_kafka_acls_provision_via_cli`, `jvm_authorized_produce_consume`,
  `jvm_unauthorized_produce_fails`,
  `jvm_unauthorized_consumer_fails_group_check`,
  `jvm_prefixed_topic_acl_works`.
- **Slice-12 JVM tests after T27**:
  `jvm_sasl_scram_sha512_produce_consume`, `jvm_sasl_ssl_full_stack`,
  `jvm_inter_broker_sasl_ssl_raft_replication`.
- **Slice-13 broker integration tests**
  (`tests/acl_handlers.rs`):
  `produce_allowed_with_topic_write_acl`,
  `fetch_allowed_with_topic_read_acl`, plus any equivalent.

The `enable.idempotence=false` workaround in slice-12 SCRAM tests
stays. That's about `Cluster IdempotentWrite` (a real ACL gap from
operator missing config), not an implication.

## Components

### `crabka-broker/src/config.rs`

- Replace `pub super_user_name: Option<String>` with
  `pub super_users: std::collections::HashSet<String>`.
- `Default` initializer: `super_users: HashSet::new()`.
- `for_tests` initializer: same.
- No new validation. Empty entries (whitespace-only) skipped at startup
  via a `.trim().is_empty()` filter in any future config-string parser
  (no parser exists yet — slice 13's `super_user_name` is set
  programmatically).

### `crabka-broker/src/authorizer.rs`

- `super_user_name: Option<&str>` parameter → `super_users: &HashSet<String>`.
- `if super_user_name == Some(&req.principal.name)` → `if super_users.contains(&req.principal.name)`.
- Compat-shim check: `super_user_name.is_none()` → `super_users.is_empty()`.
- New `implies(stored, requested) -> bool` helper.
- New `matches_operation(entry, requested) -> bool` helper that calls
  `implies` after the exact-match and `All` checks.

### Handler call sites (~22 files)

Every call site that today passes
`broker.config.super_user_name.as_deref()` becomes
`&broker.config.super_users`. Mechanical one-line change per call
site. Search: `rg "super_user_name" crates/broker/`.

Files touched (slice-13's full handler-wiring matrix):
- `produce.rs`, `fetch.rs`, `metadata.rs`
- `create_topics.rs`, `delete_topics.rs`, `alter_configs.rs`,
  `incremental_alter_configs.rs`, `create_partitions.rs`,
  `delete_records.rs`
- `list_groups.rs`, `describe_groups.rs`, `delete_groups.rs`,
  `join_group.rs`, `offset_commit.rs`, `offset_fetch.rs`
- `describe_cluster.rs`, `alter_user_scram_credentials.rs`
- `init_producer_id.rs`, `txn/handlers/{add_partitions_to_txn,
  end_txn, txn_offset_commit}.rs`
- `create_acls.rs`, `delete_acls.rs`, `describe_acls.rs`

### Test setup sites

Every test that built `super_user_name: Some("admin".to_string())`
becomes `super_users: HashSet::from(["admin".to_string()])`:

- `crates/broker/tests/auth_handlers.rs`
- `crates/broker/tests/acl_handlers.rs`
- `crates/broker/tests/jvm_acceptance.rs` (`start_dual_mech_broker`,
  `start_sasl_ssl_broker`,
  `start_two_sasl_ssl_brokers_with_controller_protocol`,
  `start_sasl_plaintext_broker_with_super_user`)

Optional convenience helper in test files:

```rust
fn one_super_user(name: &str) -> HashSet<String> {
    let mut s = HashSet::new();
    s.insert(name.to_string());
    s
}
```

Inside `#[cfg(test)]`; not part of the public API.

## Data flow

### Metadata for alice after `Allow Read Topic foo` grant

1. Alice authenticates as `alice` via PLAIN. Image has one ACL: `Allow
   Read Topic LITERAL "foo" User:alice host=*`.
2. JVM producer dials, sends Metadata for `foo` (or fetch-all).
   Handler calls `authorize` with `operation: Describe`.
3. Compat shim: `super_users.is_empty()` may be true or false depending
   on broker config; `image.all_acls()` non-empty → skip shim regardless.
4. Super-user bypass: `super_users.contains("alice")` → false.
5. Match loop on `image.matching_acls(Topic, "foo")`:
   - One entry: `Allow Read`.
     - `matches_operation(Read, Describe)`:
       - exact match? No.
       - `All`? No.
       - `implies(Read, Describe)` → **true**.
     - Principal `User:*` or `User:alice` match? Yes.
     - Host wildcard? Yes.
     - DENY? no. ALLOW? saw_allow = true.
6. **Result: Allow**. Metadata returns `foo` normally (`error_code=0`).

Pre-13b: same setup denied because no `Describe` ACL existed; JVM
producer received TOPIC_AUTHORIZATION_FAILED on Metadata and never
even attempted Produce.

### Multi-super-user authorize

- `BrokerConfig.super_users = {"admin", "ops-bot"}`. Image has zero ACLs.
- Compat shim: `super_users.is_empty()` → false → shim does NOT fire.
- `admin` calls anything → super-user bypass → ALLOW.
- `ops-bot` calls anything → super-user bypass → ALLOW.
- `alice` calls anything → bypass false, match loop has no entries →
  default DENY.

### Workaround removal flow

After implications land, an operator (or a test) running:

```
kafka-acls --add --operation Read --topic foo --allow-principal User:alice
```

…can immediately have alice consume from `foo`. No separate
`--operation Describe` is needed. The implication `Read → Describe`
auto-grants the Metadata/Fetch path.

### Backward compatibility

- **Empty `super_users` + zero ACLs**: compat shim → ALLOW. Slice
  11/12/12b tests stay green.
- **`super_users = {"admin"}` + zero ACLs**: super-user bypass for
  admin; default-DENY for everyone else. Slice 12 SCRAM tests (which
  set the single super-user) keep working — `HashSet::from(["admin"])`
  is the rename of `Some("admin")`.
- **`super_users = {}` + ACLs exist**: no super-user, normal ACL
  evaluation. Slice 13 tests that explicitly avoid super-user (e.g.
  `create_acls_non_super_user_rejected`) stay green.

## Error handling

### Authorizer

No new error paths. The implication changes the **decision** for
already-matched ACLs but doesn't introduce new failure modes. The
existing wire error codes (29/30/31/53) flow from handler-side gate
checks unchanged.

### `BrokerConfig` validation

- Empty `super_users` set is **valid**; meaning equivalent to slice
  12's `None`.
- Duplicate entries collapse silently via `HashSet` semantics.
- Empty/whitespace-only principal entries (e.g. from a future config
  string parser): silent skip. No `BrokerError::InvalidSuperUserList`
  variant.

### Workaround-removal failure modes

If a test fails after dropping the Describe ACL:

1. **`TOPIC_AUTHORIZATION_FAILED` on Metadata still appears**: the
   `matches_operation` helper isn't being called on that path. Re-grep
   for any direct `entry.operation == requested` comparisons that
   weren't migrated.
2. **Test fails on Group instead of Topic**: implications work for any
   resource type, so this would indicate a Group-specific Read→Describe
   path that was previously satisfied by an explicit Describe. Same
   fix.
3. **Test fails on a totally different op pair**: there's a hidden
   workaround (e.g. `kafka-console-consumer` needs Read on Group AND
   Read on Topic — neither implies the other). Leave the Group ACL in
   place; only Describe variants are redundant.

### Out-of-scope safety nets

- We do NOT add a deprecation alias for `super_user_name`. The rename
  is workspace-internal; pre-1.0 surface.
- We do NOT add migration logic to read an old persisted config. There
  is no persisted broker config — slice 12's CLI flag work hasn't
  reached `super_users` yet.

## Testing

### Unit tests — `crabka-broker::authorizer`

New implication tests:

- `read_implies_describe_on_topic`
- `write_implies_describe_on_topic`
- `delete_implies_describe`
- `alter_implies_describe`
- `alter_configs_implies_describe_configs`
- `describe_does_not_imply_read` (one-way check)
- `implication_works_on_group_resource`
- `implication_works_on_cluster_resource`
- `implication_works_on_transactional_id_resource`
- `all_operation_still_matches_anything` (regression for `op::All`)

New super-user tests:

- `multi_super_user_all_bypass` — `{"admin", "ops-bot"}` both ALLOW,
  alice DENY.
- `empty_super_user_set_engages_compat_shim_when_no_acls`
- `empty_super_user_set_denies_when_acls_present`

### Unit tests — `crabka-broker::config`

- `super_users_default_is_empty`
- `super_users_for_tests_is_empty`

### Integration tests — no Docker

`crates/broker/tests/acl_handlers.rs`:

- `implication_metadata_describes_after_read_acl` — provision one
  `Allow Read Topic LITERAL "foo"`; Metadata for `foo` returns
  `error_code=0`.
- `implication_metadata_describes_after_write_acl` — same with Write.
- `multi_super_user_both_can_provision` — broker with `super_users =
  {"admin", "ops-bot"}`; both authenticate (via PLAIN) and call
  `CreateAcls` successfully; alice (not in set) gets 31.

### Workaround removal (modify existing tests)

`crates/broker/tests/acl_handlers.rs`:

- `produce_allowed_with_topic_write_acl` — drop the explicit Describe ACL seed; test should pass.
- `fetch_allowed_with_topic_read_acl` (or equivalent) — same.
- Any other slice-13 broker-side test with redundant Describe — clean up.

`crates/broker/tests/jvm_acceptance.rs`:

- `jvm_authorized_produce_consume` (slice 13 T25) — drop `Describe`
  `kafka-acls --add` calls. Test still passes.
- `jvm_prefixed_topic_acl_works` — same.
- `jvm_sasl_scram_sha512_produce_consume` (slice 12 T21, fixed in
  slice 13 T27) — drop the Describe `kafka-acls --add` loop iteration;
  keep Read + Write.
- `jvm_sasl_ssl_full_stack` — same.
- `jvm_inter_broker_sasl_ssl_raft_replication` — same.

### Regression guards

- Slice 11/12/12b tests: empty `super_users` + zero ACLs path. Compat
  shim continues to ALLOW. Should run unchanged.
- Slice 13 ACL flow tests
  (`create_acls_super_user_can_provision_and_describe`,
  `create_acls_non_super_user_rejected`, etc.): the rename from
  `super_user_name: Some("admin".to_string())` to
  `super_users: HashSet::from(["admin".to_string()])` is the only
  required test edit.

### Out of scope for tests

- ACL audit logging (deferred).
- Performance benchmarks for the implications hop (table is tiny).
- IPv6 host filter (slice 13's gap).
- `ClusterAction` implications (Kafka uses it for inter-broker; Crabka
  uses static SASL credentials for inter-broker traffic so we never
  check ACLs on that path).

## Wire-protocol additions

None. This slice is internal-only.

## Out of scope

- ACL audit logging beyond `tracing`.
- `User:` prefix handling in super-user config strings.
- Persisted broker config / CLI flag for `super_users`.
- Delegation tokens.
- ACL caching with TTL.
- `ClusterAction` implications.
- Operation implications for other op pairs Kafka has but Crabka
  doesn't model (`IdempotentWrite` semantics, etc.).
