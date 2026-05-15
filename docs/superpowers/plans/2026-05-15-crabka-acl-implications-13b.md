# Slice 13b: ACL implications + multi-super-user — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add Kafka's operation-implication semantics to the slice-13 authorizer (Read/Write/Delete/Alter → Describe; AlterConfigs → DescribeConfigs) and rename `BrokerConfig::super_user_name: Option<String>` to `super_users: HashSet<String>` so deployments can configure multiple privileged identities.

**Architecture:** Two surgical changes. (1) A new `implies(stored, requested) -> bool` helper in `crabka-broker::authorizer` is called from a refactored `matches_operation` after the exact-match and `All` checks. (2) The `super_users` rename touches ~22 handler call sites mechanically. After both land, the redundant `Describe` ACLs in slice-12/13 JVM tests and slice-13 broker integration tests are removed and verified.

**Tech Stack:** Rust 1.95.0; no new dependencies. The slice 13 `crabka_metadata::AclOperation` enum is the load-bearing type — `implies` matches on it.

**Reference spec:** [`docs/superpowers/specs/2026-05-15-crabka-acl-implications-13b-design.md`](../specs/2026-05-15-crabka-acl-implications-13b-design.md).

**Working directory:** `C:\Users\Matt Stone\git\crabka`. Implementation runs on `feature/acl-implications-13b` (already created off `main`; spec is committed).

---

## File structure

```
crates/broker/src/
├── config.rs          # MODIFIED — super_user_name → super_users: HashSet<String>
├── authorizer.rs      # MODIFIED — signature + implies() helper + matches_operation()
└── handlers/          # MODIFIED — 22 call sites: super_user_name.as_deref() → &super_users
    ├── alter_configs.rs
    ├── alter_user_scram_credentials.rs
    ├── create_acls.rs
    ├── create_partitions.rs
    ├── create_topics.rs
    ├── delete_acls.rs
    ├── delete_groups.rs
    ├── delete_records.rs
    ├── delete_topics.rs
    ├── describe_acls.rs
    ├── describe_cluster.rs
    ├── describe_groups.rs
    ├── fetch.rs
    ├── incremental_alter_configs.rs
    ├── init_producer_id.rs
    ├── join_group.rs
    ├── list_groups.rs
    ├── metadata.rs
    ├── offset_commit.rs
    ├── offset_fetch.rs
    └── produce.rs
crates/broker/src/txn/handlers/
    ├── add_partitions_to_txn.rs   # MODIFIED — call-site rename
    ├── end_txn.rs                  # MODIFIED — call-site rename
    └── txn_offset_commit.rs        # MODIFIED — call-site rename

crates/broker/tests/
├── auth_handlers.rs    # MODIFIED — super_user_name: Some(...) → super_users: HashSet::from(...)
├── acl_handlers.rs     # MODIFIED — same rename + drop redundant Describe ACLs
└── jvm_acceptance.rs   # MODIFIED — same rename + drop redundant Describe ACLs
```

Total: 7 tasks across 4 batches.

---

## Batch 1 — Multi-super-user rename (atomic)

### Task 1: Atomic `super_user_name` → `super_users: HashSet<String>` rename

**Files:** 27 across `crates/broker/src/` and `crates/broker/tests/`.

This is a single mechanical refactor that MUST land in one commit — every consumer of `super_user_name` must migrate at the same time or the crate won't compile.

- [ ] **Step 1: Update `BrokerConfig` field**

In `crates/broker/src/config.rs`, find the struct definition (around line 128):

```rust
// REMOVE:
pub super_user_name: Option<String>,

// REPLACE WITH:
pub super_users: std::collections::HashSet<String>,
```

In the `Default` impl (around line 171):

```rust
// REMOVE:
super_user_name: None,

// REPLACE WITH:
super_users: std::collections::HashSet::new(),
```

In `for_tests` (around line 282):

```rust
// REMOVE:
super_user_name: None,

// REPLACE WITH:
super_users: std::collections::HashSet::new(),
```

- [ ] **Step 2: Update `authorizer.rs` signature**

In `crates/broker/src/authorizer.rs`, change `authorize`:

```rust
// REMOVE:
pub fn authorize(
    image: &MetadataImage,
    super_user_name: Option<&str>,
    req: &AuthorizationRequest,
) -> AuthorizationResult {

// REPLACE WITH:
pub fn authorize(
    image: &MetadataImage,
    super_users: &std::collections::HashSet<String>,
    req: &AuthorizationRequest,
) -> AuthorizationResult {
```

Update the body:

```rust
// REMOVE the compat-shim check:
if super_user_name.is_none() && image.all_acls().next().is_none() {
    return AuthorizationResult::Allow;
}

// REPLACE WITH:
if super_users.is_empty() && image.all_acls().next().is_none() {
    return AuthorizationResult::Allow;
}

// REMOVE the super-user bypass:
if let Some(name) = super_user_name
    && req.principal.name == name
{
    return AuthorizationResult::Allow;
}

// REPLACE WITH:
if super_users.contains(&req.principal.name) {
    return AuthorizationResult::Allow;
}
```

Update `authorize_topics` signature the same way:

```rust
pub fn authorize_topics<'a>(
    image: &MetadataImage,
    super_users: &std::collections::HashSet<String>,
    principal: &Principal,
    host: &SocketAddr,
    operation: AclOperation,
    topic_names: impl IntoIterator<Item = &'a str>,
) -> HashMap<&'a str, AuthorizationResult> {
    topic_names
        .into_iter()
        .map(|name| {
            let req = AuthorizationRequest { /* ... */ };
            (name, authorize(image, super_users, &req))
        })
        .collect()
}
```

Update the unit tests in `mod tests` (~5 sites) — `authorize(&img, None, &req(...))` → `authorize(&img, &HashSet::new(), &req(...))`, and `Some("admin")` → `&HashSet::from(["admin".to_string()])`.

Helper for terse test calls:

```rust
fn no_super() -> std::collections::HashSet<String> { std::collections::HashSet::new() }
fn one_super(name: &str) -> std::collections::HashSet<String> {
    let mut s = std::collections::HashSet::new();
    s.insert(name.to_string());
    s
}
```

- [ ] **Step 3: Update handler call sites**

22 files in `crates/broker/src/handlers/` plus 3 in `crates/broker/src/txn/handlers/`. The pattern is the same in every site:

```rust
// REMOVE:
broker.config.super_user_name.as_deref(),

// REPLACE WITH:
&broker.config.super_users,
```

Search via `rg "super_user_name" crates/broker/src/handlers/ crates/broker/src/txn/handlers/`. Each occurrence is a single-line replace. Some handlers also have docstring comments mentioning `super_user_name` — update those too.

`alter_user_scram_credentials.rs` has a slice-12-historical docstring at line 19 and 67 that says "super_user_name, which short-circuits inside authorize → ALLOW". Update to "super_users set, which short-circuits inside authorize → ALLOW".

`init_producer_id.rs` (around line 60) has `let super_user = broker.config.super_user_name.as_deref();` — replace with `let super_users = &broker.config.super_users;` and update the call site.

- [ ] **Step 4: Update test fixture sites**

In `crates/broker/tests/auth_handlers.rs`, `acl_handlers.rs`, and `jvm_acceptance.rs`, search via `rg "super_user_name" crates/broker/tests/`. Every site looks like:

```rust
// REMOVE:
super_user_name: Some("admin".to_string()),

// REPLACE WITH:
super_users: std::collections::HashSet::from(["admin".to_string()]),
```

Some sites also use `super_user_name: None` — replace with `super_users: std::collections::HashSet::new()`. Or use a small test helper inside each file:

```rust
fn super_users_set(names: &[&str]) -> std::collections::HashSet<String> {
    names.iter().map(|s| s.to_string()).collect()
}
```

- [ ] **Step 5: Pre-commit gates**

Run in order:

```
cargo build --workspace
cargo test -p crabka-broker --lib
cargo test -p crabka-broker --tests
cargo fmt --check
cargo clippy --workspace --all-targets -- -D warnings
```

Expected:
- Build clean (every `super_user_name` reference migrated; no compile errors).
- All slice-11/12/12b/13 tests still pass (the rename is semantically a no-op for single-super-user and zero-super-user cases — the new code paths are byte-for-byte equivalent).

If any test fails: most likely a test fixture site was missed. `rg "super_user_name" crates/` should return zero hits except in comments / spec / plan docs.

- [ ] **Step 6: Commit**

```bash
git add -A
git commit -m "refactor(broker): super_user_name -> super_users: HashSet<String>

Replaces BrokerConfig::super_user_name (Option<String>) with
super_users (HashSet<String>) so deployments can configure multiple
privileged identities, matching real Kafka's
\`super.users=User:a;User:b\`. Authorizer + 22 handler call sites +
4 test fixture files migrated atomically.

Semantically a no-op for single-super-user and zero-super-user cases:
- HashSet::from([\"admin\"]) replaces Some(\"admin\")
- HashSet::new() replaces None

Co-Authored-By: Claude Opus 4.7 <noreply@anthropic.com>"
```

---

## Batch 2 — Operation implications

### Task 2: `implies()` helper + `matches_operation()` refactor + matrix tests

**Files:**
- Modify: `crates/broker/src/authorizer.rs`

The decision algorithm gains one new step: after `entry.operation == requested` and `entry.operation == All` fail to match, consult an `implies(stored, requested) -> bool` table.

- [ ] **Step 1: Write the failing matrix tests**

Append to `crates/broker/src/authorizer.rs::mod tests`:

```rust
    fn topic_acl_op(
        permission: PermissionType,
        op: AclOperation,
        name: &str,
    ) -> AclEntry {
        AclEntry {
            resource_type: ResourceType::Topic,
            resource_name: name.into(),
            pattern_type: PatternType::Literal,
            principal: "User:alice".into(),
            host: "*".into(),
            operation: op,
            permission_type: permission,
        }
    }

    #[test]
    fn read_implies_describe_on_topic() {
        let mut img = img();
        img.apply(&MetadataRecord::V1AccessControlEntry(topic_acl_op(
            PermissionType::Allow, AclOperation::Read, "foo",
        )));
        let a = alice();
        let h = addr();
        assert_eq!(
            authorize(&img, &no_super(), &req(&a, &h, "foo", AclOperation::Describe)),
            AuthorizationResult::Allow,
        );
    }

    #[test]
    fn write_implies_describe_on_topic() {
        let mut img = img();
        img.apply(&MetadataRecord::V1AccessControlEntry(topic_acl_op(
            PermissionType::Allow, AclOperation::Write, "foo",
        )));
        let a = alice();
        let h = addr();
        assert_eq!(
            authorize(&img, &no_super(), &req(&a, &h, "foo", AclOperation::Describe)),
            AuthorizationResult::Allow,
        );
    }

    #[test]
    fn delete_implies_describe() {
        let mut img = img();
        img.apply(&MetadataRecord::V1AccessControlEntry(topic_acl_op(
            PermissionType::Allow, AclOperation::Delete, "foo",
        )));
        let a = alice();
        let h = addr();
        assert_eq!(
            authorize(&img, &no_super(), &req(&a, &h, "foo", AclOperation::Describe)),
            AuthorizationResult::Allow,
        );
    }

    #[test]
    fn alter_implies_describe() {
        let mut img = img();
        img.apply(&MetadataRecord::V1AccessControlEntry(topic_acl_op(
            PermissionType::Allow, AclOperation::Alter, "foo",
        )));
        let a = alice();
        let h = addr();
        assert_eq!(
            authorize(&img, &no_super(), &req(&a, &h, "foo", AclOperation::Describe)),
            AuthorizationResult::Allow,
        );
    }

    #[test]
    fn alter_configs_implies_describe_configs() {
        let mut img = img();
        img.apply(&MetadataRecord::V1AccessControlEntry(topic_acl_op(
            PermissionType::Allow, AclOperation::AlterConfigs, "foo",
        )));
        let a = alice();
        let h = addr();
        assert_eq!(
            authorize(&img, &no_super(), &req(&a, &h, "foo", AclOperation::DescribeConfigs)),
            AuthorizationResult::Allow,
        );
    }

    #[test]
    fn describe_does_not_imply_read() {
        let mut img = img();
        img.apply(&MetadataRecord::V1AccessControlEntry(topic_acl_op(
            PermissionType::Allow, AclOperation::Describe, "foo",
        )));
        let a = alice();
        let h = addr();
        assert_eq!(
            authorize(&img, &no_super(), &req(&a, &h, "foo", AclOperation::Read)),
            AuthorizationResult::Deny,
        );
    }
```

- [ ] **Step 2: Run tests to verify they fail**

```
cargo test -p crabka-broker --lib authorizer::tests
```

Expected: 6 new tests FAIL (no implication logic yet). All existing tests still PASS.

- [ ] **Step 3: Add `implies` helper + refactor `matches_operation`**

Replace the existing operation-match logic in `crates/broker/src/authorizer.rs`. Find the part of `authorize`'s loop body that says something like `entry.operation == req.operation || entry.operation == AclOperation::All` — extract into a helper:

```rust
/// Returns true when an ACL with `stored` operation grants access for
/// an authorization request with `requested` operation. Beyond exact
/// match and the `All` wildcard, applies Kafka's operation-implication
/// table:
///
/// | stored          | implies                |
/// |-----------------|------------------------|
/// | Read            | Describe               |
/// | Write           | Describe               |
/// | Delete          | Describe               |
/// | Alter           | Describe               |
/// | AlterConfigs    | DescribeConfigs        |
/// | All             | Everything             |
///
/// The table is one-way: Describe does NOT imply Read, etc.
fn matches_operation(stored: AclOperation, requested: AclOperation) -> bool {
    if stored == requested {
        return true;
    }
    if matches!(stored, AclOperation::All) {
        return true;
    }
    implies(stored, requested)
}

fn implies(stored: AclOperation, requested: AclOperation) -> bool {
    matches!(
        (stored, requested),
        (AclOperation::Read, AclOperation::Describe)
            | (AclOperation::Write, AclOperation::Describe)
            | (AclOperation::Delete, AclOperation::Describe)
            | (AclOperation::Alter, AclOperation::Describe)
            | (AclOperation::AlterConfigs, AclOperation::DescribeConfigs)
    )
}
```

Update the match loop inside `authorize` to call `matches_operation`:

```rust
for entry in image.matching_acls(req.resource_type, req.resource_name) {
    if !principal_matches(&entry.principal, &principal_pattern) {
        continue;
    }
    if !host_matches(&entry.host, &host_str) {
        continue;
    }
    if !matches_operation(entry.operation, req.operation) {  // <-- new call
        continue;
    }
    match entry.permission_type {
        PermissionType::Deny => return AuthorizationResult::Deny,
        PermissionType::Allow => saw_allow = true,
    }
}
```

(The `principal_matches` and `host_matches` helper names may differ in the slice-13 code — check the existing implementation and use whatever names are there.)

- [ ] **Step 4: Run tests to verify all pass**

```
cargo test -p crabka-broker --lib authorizer::tests
```

Expected: 6 new + all existing tests PASS.

- [ ] **Step 5: Lints + commit**

```
cargo fmt --check -p crabka-broker
cargo clippy -p crabka-broker --all-targets -- -D warnings
git add crates/broker/src/authorizer.rs
git commit -m "feat(broker): operation implications in authorizer

Read/Write/Delete/Alter on any resource now imply Describe;
AlterConfigs implies DescribeConfigs. Matches Kafka's
StandardAuthorizer semantics. Decision algorithm grows one helper
(\`matches_operation\`) and one table (\`implies\`).

The implication is one-way (Describe doesn't imply Read). All other
slice-13 algorithm steps unchanged.

Co-Authored-By: Claude Opus 4.7 <noreply@anthropic.com>"
```

---

### Task 3: Resource-type independence + broker integration tests

**Files:**
- Modify: `crates/broker/src/authorizer.rs` (3 more unit tests)
- Modify: `crates/broker/tests/acl_handlers.rs` (3 new integration tests)

- [ ] **Step 1: Add resource-type unit tests**

Append to `crates/broker/src/authorizer.rs::mod tests`:

```rust
    fn acl_op_on(
        rt: ResourceType,
        permission: PermissionType,
        op: AclOperation,
        name: &str,
    ) -> AclEntry {
        AclEntry {
            resource_type: rt,
            resource_name: name.into(),
            pattern_type: PatternType::Literal,
            principal: "User:alice".into(),
            host: "*".into(),
            operation: op,
            permission_type: permission,
        }
    }

    fn req_on<'a>(
        p: &'a Principal,
        host: &'a SocketAddr,
        rt: ResourceType,
        name: &'a str,
        op: AclOperation,
    ) -> AuthorizationRequest<'a> {
        AuthorizationRequest {
            principal: p,
            host,
            resource_type: rt,
            resource_name: name,
            operation: op,
        }
    }

    #[test]
    fn implication_works_on_group_resource() {
        let mut img = img();
        img.apply(&MetadataRecord::V1AccessControlEntry(acl_op_on(
            ResourceType::Group, PermissionType::Allow, AclOperation::Read, "cg-1",
        )));
        let a = alice();
        let h = addr();
        assert_eq!(
            authorize(&img, &no_super(), &req_on(&a, &h, ResourceType::Group, "cg-1", AclOperation::Describe)),
            AuthorizationResult::Allow,
        );
    }

    #[test]
    fn implication_works_on_cluster_resource() {
        let mut img = img();
        img.apply(&MetadataRecord::V1AccessControlEntry(acl_op_on(
            ResourceType::Cluster, PermissionType::Allow, AclOperation::Alter, "kafka-cluster",
        )));
        let a = alice();
        let h = addr();
        assert_eq!(
            authorize(&img, &no_super(), &req_on(&a, &h, ResourceType::Cluster, "kafka-cluster", AclOperation::Describe)),
            AuthorizationResult::Allow,
        );
    }

    #[test]
    fn implication_works_on_transactional_id_resource() {
        let mut img = img();
        img.apply(&MetadataRecord::V1AccessControlEntry(acl_op_on(
            ResourceType::TransactionalId, PermissionType::Allow, AclOperation::Write, "tx-1",
        )));
        let a = alice();
        let h = addr();
        assert_eq!(
            authorize(&img, &no_super(), &req_on(&a, &h, ResourceType::TransactionalId, "tx-1", AclOperation::Describe)),
            AuthorizationResult::Allow,
        );
    }

    #[test]
    fn multi_super_user_all_bypass() {
        let img = img();
        let h = addr();
        let supers = {
            let mut s = std::collections::HashSet::new();
            s.insert("admin".to_string());
            s.insert("ops-bot".to_string());
            s
        };
        let admin = Principal { name: "admin".into(), mechanism: SaslMechanism::Plain };
        let ops = Principal { name: "ops-bot".into(), mechanism: SaslMechanism::Plain };
        let alice = alice();
        assert_eq!(
            authorize(&img, &supers, &req(&admin, &h, "foo", AclOperation::Write)),
            AuthorizationResult::Allow,
        );
        assert_eq!(
            authorize(&img, &supers, &req(&ops, &h, "foo", AclOperation::Write)),
            AuthorizationResult::Allow,
        );
        // alice would hit the compat shim with empty image; force a non-empty
        // image by adding an unrelated ACL.
        let mut img2 = img;
        img2.apply(&MetadataRecord::V1AccessControlEntry(topic_acl_op(
            PermissionType::Allow, AclOperation::Read, "_other",
        )));
        assert_eq!(
            authorize(&img2, &supers, &req(&alice, &h, "foo", AclOperation::Write)),
            AuthorizationResult::Deny,
        );
    }
```

- [ ] **Step 2: Add 3 broker integration tests**

In `crates/broker/tests/acl_handlers.rs`, find the existing slice-13 tests like `create_acls_super_user_can_provision_and_describe`. Add three more tests at the bottom of the file using the same SASL/PLAIN driver helpers:

```rust
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn implication_metadata_describes_after_read_acl() {
    // SASL_PLAINTEXT broker, super-user admin, alice with PLAIN creds.
    // Provision ONE ACL: Allow Read Topic LITERAL "foo" for alice.
    // No Describe ACL — relies on Read->Describe implication.
    // Create topic foo via admin.
    // Send Metadata for foo as alice. Expect error_code = 0 (topic visible).
    //
    // Pre-13b: this would have returned TOPIC_AUTHORIZATION_FAILED (29).
    //
    // Body shape matches `create_acls_super_user_can_provision_and_describe`.
    // Use `submit_metadata_record_for_test` to inject the ACL directly
    // (bypassing kafka-acls.sh which would itself need admin auth + ACLs).
    //
    // Concrete assertions:
    //   let meta = drive_metadata_as_plain(addr, "alice", "alice-pass", &["foo"]).await;
    //   assert_eq!(meta.topics[0].error_code, 0);
    //   assert_eq!(meta.topics[0].name.as_deref(), Some("foo"));

    unimplemented!("see body sketch above — model on create_acls_super_user_can_provision_and_describe");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn implication_metadata_describes_after_write_acl() {
    // Same as above with `Write` stored.
    unimplemented!("model on implication_metadata_describes_after_read_acl");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn multi_super_user_both_can_provision() {
    // Broker with super_users = {"admin", "ops-bot"}. Both have PLAIN creds.
    // Auth as "admin"; send CreateAcls; expect error_code = 0.
    // Auth as "ops-bot"; send CreateAcls; expect error_code = 0.
    // Auth as "alice" (not in super-set, no Cluster Alter ACL); send CreateAcls;
    //   expect error_code = CLUSTER_AUTHORIZATION_FAILED (31).

    unimplemented!("see body sketch above");
}
```

Replace each `unimplemented!()` with a real body that follows the pattern of slice-13's `create_acls_super_user_can_provision_and_describe` (uses the existing `drive_create_acls_as_plain` / `drive_describe_acls_as_plain` / `drive_metadata_as_plain` helpers — verify those names match the actual slice-13 helpers; `drive_metadata_as_plain` was added in slice-13 T24).

- [ ] **Step 3: Run tests**

```
cargo test -p crabka-broker --lib authorizer
cargo test -p crabka-broker --test acl_handlers
```

Expected: all PASS. 13 new tests total (4 unit + 3 integration). The acl_handlers integration tests must be run via WSL since they are `#![cfg(not(target_os = "windows"))]`:

```bash
wsl bash -c "cd /mnt/c/Users/Matt\\ Stone/git/crabka && RUSTC_WRAPPER= cargo test -p crabka-broker --test acl_handlers implication multi_super_user -- --nocapture --test-threads=1"
```

- [ ] **Step 4: Lints + commit**

```
cargo fmt --check -p crabka-broker
cargo clippy -p crabka-broker --all-targets -- -D warnings
git add crates/broker/src/authorizer.rs crates/broker/tests/acl_handlers.rs
git commit -m "test(broker): operation-implication + multi-super-user tests

Adds 4 authorizer unit tests covering implications on Group, Cluster,
and TransactionalId resource types plus multi-super-user bypass, and
3 broker integration tests verifying Read/Write->Describe implications
work end-to-end via Metadata and that CreateAcls accepts any principal
in the super_users set.

Co-Authored-By: Claude Opus 4.7 <noreply@anthropic.com>"
```

---

## Batch 3 — Workaround removal

### Task 4: Remove redundant Describe from slice-13 broker integration tests

**Files:**
- Modify: `crates/broker/tests/acl_handlers.rs`

The slice-13 T22-T24 tests seeded explicit Describe ACLs alongside Read/Write/Delete/Alter. With implications live, those Describe seeds are redundant.

- [ ] **Step 1: Inventory the redundant seeds**

```bash
rg "AclOperation::Describe" crates/broker/tests/acl_handlers.rs
```

For each hit, check the surrounding test context:
- If the test ALSO seeds Read/Write/Delete/Alter on the same resource for the same principal → Describe seed is redundant. Drop it.
- If the test seeds ONLY Describe (no Read/Write etc.) → keep it. The test is exercising the Describe operation directly.
- If a test seeds Describe-only as a fixture to disable the compat shim → keep it.

Specifically check (these are the slice-13 tests likely to have redundant seeds):
- `produce_allowed_with_topic_write_acl` (slice 13 T23) — seeded `Allow Write Topic` AND `Allow Describe Topic`. Drop Describe.
- Any other test with the `Allow Read/Write/...Delete/Alter` + `Allow Describe` pair on the same `(principal, resource_type, resource_name)`.

- [ ] **Step 2: Remove the redundant seeds**

For each test identified in step 1, delete the lines that submit the redundant Describe `V1AccessControlEntry`. Update the test's docstring to note that the Describe path now relies on slice-13b's implications.

- [ ] **Step 3: Run tests via WSL**

```bash
wsl bash -c "cd /mnt/c/Users/Matt\\ Stone/git/crabka && RUSTC_WRAPPER= cargo test -p crabka-broker --test acl_handlers -- --nocapture --test-threads=1"
```

Expected: all tests still PASS. If any test fails with TOPIC_AUTHORIZATION_FAILED, the implementation may have a path that wasn't migrated to `matches_operation` — re-check the call sites.

- [ ] **Step 4: Lints + commit**

```
cargo fmt --check -p crabka-broker
cargo clippy -p crabka-broker --all-targets -- -D warnings
git add crates/broker/tests/acl_handlers.rs
git commit -m "test(broker): drop redundant Describe ACLs from slice-13 tests

With slice-13b implications live, Read/Write/Delete/Alter ACLs
auto-grant Describe on the same resource. The explicit Describe seeds
in slice-13's broker integration tests are now redundant.

Co-Authored-By: Claude Opus 4.7 <noreply@anthropic.com>"
```

---

### Task 5: Remove redundant Describe from slice-13 JVM tests

**Files:**
- Modify: `crates/broker/tests/jvm_acceptance.rs`

The slice-13 T25/T26 JVM tests provision ACLs via `kafka-acls.sh --add` and iterate over `["Read", "Write", "Describe"]`. With implications live, the `Describe` iteration is redundant.

- [ ] **Step 1: Find the slice-13 JVM tests with the iteration pattern**

```bash
rg -B 2 -A 6 'for op in \["Read", "Write", "Describe"\]' crates/broker/tests/jvm_acceptance.rs
```

Or the broader pattern:

```bash
rg -B 1 -A 8 'for op in.*Describe' crates/broker/tests/jvm_acceptance.rs
```

Likely candidates (slice 13 T25-T26):
- `jvm_kafka_acls_provision_via_cli` — seeds Read + Describe? If so, drop Describe.
- `jvm_authorized_produce_consume` — seeds Read+Write+Describe. Drop Describe.
- `jvm_prefixed_topic_acl_works` — seeds Read+Describe (prefixed). Drop Describe.
- Other slice-13 JVM ACL tests with similar loops.

- [ ] **Step 2: Drop the Describe iteration**

For each test, change:

```rust
// REMOVE:
for op in ["Read", "Write", "Describe"] {
    // kafka-acls --add ...
}

// REPLACE WITH:
for op in ["Read", "Write"] {
    // kafka-acls --add ...
}
```

Or if the loop is `["Read", "Describe"]`, drop to just `["Read"]`.

Single-call cases (no loop, one `--operation Describe` invocation alongside a `--operation Read` invocation): delete the Describe call.

- [ ] **Step 3: Run the affected tests via WSL**

```bash
wsl bash -c "cd /mnt/c/Users/Matt\\ Stone/git/crabka && RUSTC_WRAPPER= cargo test -p crabka-broker --test jvm_acceptance -- jvm_authorized_produce_consume jvm_kafka_acls_provision_via_cli jvm_prefixed_topic_acl_works --ignored --nocapture --test-threads=1"
```

Expected: PASS. The producer's `Write` ACL now also grants Describe; the consumer's `Read` ACL also grants Describe.

- [ ] **Step 4: Lints + commit**

```
cargo fmt --check -p crabka-broker
cargo clippy -p crabka-broker --all-targets -- -D warnings
git add crates/broker/tests/jvm_acceptance.rs
git commit -m "test(jvm): drop redundant Describe ACLs from slice-13 ACL tests

With slice-13b implications live, kafka-acls --add --operation Read
also grants Describe on the same topic. The explicit
--operation Describe iteration is redundant.

Co-Authored-By: Claude Opus 4.7 <noreply@anthropic.com>"
```

---

### Task 6: Remove redundant Describe from slice-12 SCRAM JVM tests

**Files:**
- Modify: `crates/broker/tests/jvm_acceptance.rs`

The slice-13 T27 fix added an `["Read", "Write", "Describe"]` ACL provisioning loop to:
- `jvm_sasl_scram_sha512_produce_consume`
- `jvm_sasl_ssl_full_stack`
- `jvm_inter_broker_sasl_ssl_raft_replication`

With implications live, drop the Describe iteration in each.

- [ ] **Step 1: Find the three slice-12 SCRAM tests' ACL loops**

```bash
rg -B 3 -A 12 'for op in \["Read", "Write", "Describe"\]' crates/broker/tests/jvm_acceptance.rs
```

(There should be 3 hits in the slice-12 tests; if slice-13 T25/T26 tests use the same pattern they may show up too — check the surrounding test name to filter.)

- [ ] **Step 2: Drop Describe in each loop**

For each of the three tests, change `["Read", "Write", "Describe"]` to `["Read", "Write"]`.

- [ ] **Step 3: Run via WSL**

```bash
wsl bash -c "cd /mnt/c/Users/Matt\\ Stone/git/crabka && RUSTC_WRAPPER= cargo test -p crabka-broker --test jvm_acceptance -- jvm_sasl_scram_sha512_produce_consume jvm_sasl_ssl_full_stack jvm_inter_broker_sasl_ssl_raft_replication --ignored --nocapture --test-threads=1"
```

Expected: all 3 PASS. (Note: `jvm_inter_broker_sasl_ssl_raft_replication` may hit the WSL `host.docker.internal` issue documented in slice 12b — if so, CI will validate.)

- [ ] **Step 4: Lints + commit**

```
cargo fmt --check -p crabka-broker
cargo clippy -p crabka-broker --all-targets -- -D warnings
git add crates/broker/tests/jvm_acceptance.rs
git commit -m "test(jvm): drop redundant Describe ACLs from slice-12 SCRAM tests

With slice-13b implications live, the Describe iteration in the
kafka-acls --add loop is redundant — Read/Write each auto-grant
Describe on the same topic.

Co-Authored-By: Claude Opus 4.7 <noreply@anthropic.com>"
```

---

## Batch 4 — Final acceptance sweep

### Task 7: Sweep + docs + PR

**Files:**
- Modify: `README.md`
- Modify: `STATUS.md`

- [ ] **Step 1: Full local test matrix**

```bash
cargo fmt --check
cargo clippy --workspace --all-targets -- -D warnings
cargo build --workspace
cargo test --workspace --exclude crabka-client-core --exclude crabka-log --exclude crabka-broker
cargo test -p crabka-broker --lib
cargo test -p crabka-broker --tests
```

All clean. The compat shim guarantees slice 11/12/12b tests continue passing without changes.

- [ ] **Step 2: WSL JVM acceptance (full suite)**

```bash
wsl bash -c "cd /mnt/c/Users/Matt\\ Stone/git/crabka && RUSTC_WRAPPER= cargo test -p crabka-broker --test jvm_acceptance -- --ignored --nocapture --test-threads=1"
```

All green. If `jvm_inter_broker_sasl_ssl_raft_replication` times out under WSL with the `host.docker.internal` /etc/hosts issue, document and rely on CI.

- [ ] **Step 3: Update `README.md`**

Append under "Slices delivered":

```markdown
- **Slice 13b** — ACL polish: operation implications (Read/Write/Delete/Alter
  → Describe; AlterConfigs → DescribeConfigs) match Kafka's
  `StandardAuthorizer` semantics. Multi-super-user config:
  `BrokerConfig::super_users` is now a `HashSet<String>` so deployments
  can grant `super.users`-style privileges to multiple identities. The
  workaround Describe-ACL seeds in slice-12 SCRAM and slice-13 ACL JVM
  tests removed; standard `kafka-acls.sh --operation Read ...` now works
  end-to-end without extra Describe grants.
```

- [ ] **Step 4: Append `STATUS.md` section**

```markdown
## Slice 13b — ACL implications + multi-super-user (2026-05-15)

- `crabka_broker::authorizer::matches_operation` calls new `implies`
  helper: `Read`/`Write`/`Delete`/`Alter` on any resource imply
  `Describe`; `AlterConfigs` implies `DescribeConfigs`. One-way table
  (`Describe` does not imply `Read`). Resource-type independent.
- `BrokerConfig::super_user_name: Option<String>` renamed to
  `super_users: HashSet<String>`. Authorizer + 22 handler call sites +
  4 test fixture files migrated atomically. Semantically a no-op for
  pre-13b single-/zero-super-user cases.
- Authorizer unit tests: 10 new (6 implication matrix, 3 resource-type
  independence, 1 multi-super-user bypass).
- Broker integration tests: 3 new in `tests/acl_handlers.rs`
  (implication via Metadata after Read/Write seed; multi-super-user
  CreateAcls).
- Workaround removal: redundant Describe ACL seeds dropped from
  slice-12 SCRAM JVM tests (3), slice-13 ACL JVM tests (5), and
  slice-13 broker integration tests. Standard `kafka-acls.sh --add
  --operation Read|Write|...` now works end-to-end without separate
  Describe grants.
- Out of scope: ACL audit logging, `User:` prefix in super-user config
  strings, `ClusterAction` implication, persisted broker config.
```

- [ ] **Step 5: Commit docs**

```bash
git add README.md STATUS.md
git commit -m "docs(slice-13b): README + STATUS entry

Co-Authored-By: Claude Opus 4.7 <noreply@anthropic.com>"
```

- [ ] **Step 6: Push + open PR**

```bash
git push -u origin feature/acl-implications-13b
gh pr create --base main --head feature/acl-implications-13b \
  --title "Slice 13b: ACL operation implications + multi-super-user" \
  --body "$(cat <<'EOF'
## Summary

Two slice-13 polish items, both small but visible to operators:

1. **Operation implications** — Kafka's `StandardAuthorizer` semantics: `Read`/`Write`/`Delete`/`Alter` on any resource imply `Describe`; `AlterConfigs` implies `DescribeConfigs`. One-way table (`Describe` does not imply `Read`). After this slice, standard `kafka-acls.sh --add --operation Read --topic foo` workflows work end-to-end without a separate `--operation Describe` grant.

2. **Multi-super-user** — `BrokerConfig::super_user_name: Option<String>` becomes `super_users: HashSet<String>`. Matches real Kafka's `super.users=User:a;User:b`. The slice 13 super-user bypass becomes `super_users.contains(&principal.name)`.

## Verified

- 10 new authorizer unit tests (implication matrix on Topic/Group/Cluster/TransactionalId + multi-super-user bypass).
- 3 new broker integration tests (`tests/acl_handlers.rs`).
- Workaround Describe-ACL seeds dropped from slice-12 SCRAM JVM tests + slice-13 ACL JVM tests + slice-13 broker integration tests. All tests still pass.
- Workspace fmt/clippy/test all green.
- Slice 11/12/12b/13 regression tests pass unchanged. The compat shim continues to fire when no ACLs and zero super-users; super-user bypass works on a 1-entry set the same as slice-13's `Option<String>`.

## Out of scope

ACL audit log channel, `User:` prefix in super-user config strings, `ClusterAction` implications, persisted broker config, delegation tokens, ACL caching.

## Plan / spec

- Spec: `docs/superpowers/specs/2026-05-15-crabka-acl-implications-13b-design.md`
- Plan: `docs/superpowers/plans/2026-05-15-crabka-acl-implications-13b.md` (7 tasks across 4 batches)

🤖 Generated with [Claude Code](https://claude.com/claude-code)
EOF
)"
```

- [ ] **Step 7: Confirm CI passes**

Watch for the same lint shape as slice 12b post-rebase (doc-comment backticks, etc.). Slice 13's `super_users` rename touches a lot of comments — re-run `cargo clippy --workspace --all-targets -- -D warnings` one more time before push if uncertain.

---

## Notes for the executing agent

1. **Branch:** all work is on `feature/acl-implications-13b`. Do NOT push to main.
2. **Atomicity of T1:** the `super_users` rename MUST land in one commit. The intermediate states (e.g., field renamed but call sites still reference `super_user_name`) won't compile. Trust the find-and-replace approach; the workspace build at the end of step 5 is the validation.
3. **Compat shim still load-bearing:** after T1, the shim's condition is `super_users.is_empty() && image.all_acls().next().is_none()`. Verify both halves migrated correctly — a typo (`is_some` instead of `is_empty`) breaks every slice 11/12/12b test.
4. **`unimplemented!()` placeholders in T3 tests must be replaced with real bodies before commit.** The plan sketches the assertions; the real bodies model on slice-13's `drive_create_acls_as_plain` / `drive_describe_acls_as_plain` / `drive_metadata_as_plain` helpers (verify their names match what's actually in `tests/acl_handlers.rs`).
5. **WSL `host.docker.internal` setup:** if `jvm_inter_broker_sasl_ssl_raft_replication` times out locally, that's the documented slice-12b /etc/hosts issue. CI runs the test correctly.
6. **No `todo!()` in committed code:** every helper-body sketch must become real code before its commit step.
