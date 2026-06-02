# KIP-595 Slice 3d-1 — KIP-631 Records Foundation Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Generate the 9 missing Kafka metadata-record schemas and extend the `KraftMetadataRecord` dispatch so all of `MetadataRecord`'s value-record equivalents have a byte-exact KIP-631 counterpart, validated by round-trip + JVM `kafka-dump-log`.

**Architecture:** A near-exact repeat of the Slice-1 workflow — fetch Kafka `common/metadata/*.json` at the pinned sha, transform to the codegen grammar, regenerate, and add a `KraftMetadataRecord` variant per record keyed on its real (non-sequential) apiKey. Pure additive: `MetadataImage`, the engine, and the broker are untouched.

**Tech Stack:** the `crabka-protocol-codegen` pipeline, `crates/protocol/src/records/metadata/{record,envelope}.rs`, Docker + `apache/kafka:4.0.0` for byte-validation.

**Spec:** [docs/superpowers/specs/2026-05-31-kip595-slice3d1-records-foundation-design.md](../specs/2026-05-31-kip595-slice3d1-records-foundation-design.md)

---

## Background the implementer needs

- This mirrors Slice 1 (`docs/superpowers/plans/2026-05-31-kip631-metadata-records.md`) — read it for the exact mechanics. Same pinned sha `a9ce3221537b8653448750697915607dc7936cf3`, same transform, same `tools/regenerate.sh`, same dispatch pattern.
- **The 9 records + their REAL apiKeys** (fetched & confirmed — non-sequential; use these exactly): `UnregisterBrokerRecord`=1, `ConfigRecord`=4, `DelegationTokenRecord`=10, `UserScramCredentialRecord`=11, `ClientQuotaRecord`=14, `AccessControlEntryRecord`=18, `RemoveAccessControlEntryRecord`=19, `RemoveUserScramCredentialRecord`=22, `RemoveDelegationTokenRecord`=26. All `validVersions` 0.
- Existing `KraftMetadataRecord` dispatch (Slice 1, `crates/protocol/src/records/metadata/record.rs`): RegisterBroker=0, Topic=2, Partition=3, RemoveTopic=9, FeatureLevel=12, BrokerRegistrationChange=17, NoOp=20, BeginTransaction=23, EndTransaction=24, RegisterController=27, + `Unknown { api_key, api_version, body }`. After this slice it gains the 9 above (→ 19 modeled).
- **Do NOT add `VotersRecord`/`KRaftVersionRecord` to this dispatch.** They are KIP-853 *raft control records* (own framing, not the metadata-record value envelope) — `V1Voters`/`V1KRaftVersion` are handled by the engine's voter/control path (Slice 5), not `KraftMetadataRecord`.
- **Slice-1 escapee reminder:** `tools/regenerate.sh` creates `crates/protocol/src/{owned,borrowed}/<record>.rs` wrapper modules referenced by `mod.rs` — `git add` them or a clean checkout fails to compile.
- Transform per schema: `"type":"metadata"`→`"type":"data"`, strip the top-level `"apiKey"` line (the apiKey lives in the dispatch). Preserve the Apache license header.

## File Structure

| Path | Change |
|------|--------|
| `crates/protocol/schemas/{ConfigRecord,AccessControlEntryRecord,RemoveAccessControlEntryRecord,ClientQuotaRecord,UserScramCredentialRecord,RemoveUserScramCredentialRecord,DelegationTokenRecord,RemoveDelegationTokenRecord,UnregisterBrokerRecord}.json` | new (codegen input) |
| `crates/protocol/generated/*`, `crates/protocol/src/{owned,borrowed}/*` | regenerated (committed) |
| `crates/protocol/src/records/metadata/record.rs` | +9 `KraftMetadataRecord` variants (encode map + decode arms) + unit tests |
| `crates/protocol/tests/kraft_metadata_roundtrip.rs` | (existing) + a JVM-captured rare-record round-trip fixture/test |

---

## Task 1: Fetch + transform the 9 schemas and regenerate

**Driven inline by the controller** (network + `regenerate.sh`), as in Slice 1.

- [ ] **Step 1: Fetch + transform**

```bash
cd /Users/mattstone/git/crabka/.claude/worktrees/jovial-beaver-54b766
SHA=a9ce3221537b8653448750697915607dc7936cf3
META="https://raw.githubusercontent.com/apache/kafka/$SHA/metadata/src/main/resources/common/metadata"
for r in ConfigRecord AccessControlEntryRecord RemoveAccessControlEntryRecord \
         ClientQuotaRecord UserScramCredentialRecord RemoveUserScramCredentialRecord \
         DelegationTokenRecord RemoveDelegationTokenRecord UnregisterBrokerRecord; do
  curl -s --max-time 30 "$META/$r.json" -o "/tmp/$r.raw.json"
  python3 - "$r" <<'PY'
import sys, re
r = sys.argv[1]
src = open(f"/tmp/{r}.raw.json").read()
out = [l for l in src.splitlines() if not re.match(r'\s*"apiKey"\s*:', l)]
out = [re.sub(r'("type"\s*:\s*)"metadata"', r'\1"data"', l) for l in out]
open(f"crates/protocol/schemas/{r}.json", "w").write("\n".join(out) + "\n")
print(f"wrote crates/protocol/schemas/{r}.json")
PY
done
```

- [ ] **Step 2: Sanity-check** each parses as `type:data` with no apiKey:

```bash
for f in ConfigRecord AccessControlEntryRecord RemoveAccessControlEntryRecord ClientQuotaRecord UserScramCredentialRecord RemoveUserScramCredentialRecord DelegationTokenRecord RemoveDelegationTokenRecord UnregisterBrokerRecord; do
  python3 -c "import json; lines=open('crates/protocol/schemas/$f.json').read().splitlines(); d=json.loads('\n'.join(l for l in lines if not l.lstrip().startswith('//'))); print('$f', d['type'], 'apiKey' in d)"
done
```
Expected: each prints `<name> data False`.

- [ ] **Step 3: Regenerate**

Run: `tools/regenerate.sh`
Expected: completes; new `generated/<Record>.owned.rs`/`.borrowed.rs` + `pub mod <record>;` lines added to `src/owned/mod.rs` / `src/borrowed/mod.rs`. If codegen errors on an unseen field attribute, extend `crates/protocol-codegen/src/ir.rs` minimally and note it.

- [ ] **Step 4: Build + commit**

Run: `cargo build -p crabka-protocol` (build.rs sha assertion passes — `schemas/VERSION` unchanged).

```bash
git add crates/protocol/schemas crates/protocol/generated crates/protocol/src/owned crates/protocol/src/borrowed
git commit -m "feat(protocol): generate ConfigRecord/ACL/quota/SCRAM/token/UnregisterBroker schemas"
```

(Confirm via `git status` that the new `src/{owned,borrowed}/<record>.rs` wrapper files are staged — the Slice-1 escapee.)

---

## Task 2: Extend the `KraftMetadataRecord` dispatch

**Files:** `crates/protocol/src/records/metadata/record.rs`

- [ ] **Step 1: Write failing tests** (append to the `tests` module)

```rust
#[test]
fn config_record_value_roundtrips() {
    use crate::owned::config_record::ConfigRecord;
    let rec = KraftMetadataRecord::Config(ConfigRecord::default());
    let (decoded, ver) = KraftMetadataRecord::decode_value(&rec.encode_value(0).unwrap()).unwrap();
    assert!(decoded.encode_value(ver).unwrap() == rec.encode_value(0).unwrap());
    assert!(decoded.api_key() == 4);
}

#[test]
fn all_new_records_have_correct_api_keys() {
    use crate::owned::{access_control_entry_record::AccessControlEntryRecord,
        remove_access_control_entry_record::RemoveAccessControlEntryRecord,
        client_quota_record::ClientQuotaRecord, user_scram_credential_record::UserScramCredentialRecord,
        remove_user_scram_credential_record::RemoveUserScramCredentialRecord,
        delegation_token_record::DelegationTokenRecord, remove_delegation_token_record::RemoveDelegationTokenRecord,
        unregister_broker_record::UnregisterBrokerRecord, config_record::ConfigRecord};
    assert!(KraftMetadataRecord::UnregisterBroker(UnregisterBrokerRecord::default()).api_key() == 1);
    assert!(KraftMetadataRecord::Config(ConfigRecord::default()).api_key() == 4);
    assert!(KraftMetadataRecord::DelegationToken(DelegationTokenRecord::default()).api_key() == 10);
    assert!(KraftMetadataRecord::UserScramCredential(UserScramCredentialRecord::default()).api_key() == 11);
    assert!(KraftMetadataRecord::ClientQuota(ClientQuotaRecord::default()).api_key() == 14);
    assert!(KraftMetadataRecord::AccessControlEntry(AccessControlEntryRecord::default()).api_key() == 18);
    assert!(KraftMetadataRecord::RemoveAccessControlEntry(RemoveAccessControlEntryRecord::default()).api_key() == 19);
    assert!(KraftMetadataRecord::RemoveUserScramCredential(RemoveUserScramCredentialRecord::default()).api_key() == 22);
    assert!(KraftMetadataRecord::RemoveDelegationToken(RemoveDelegationTokenRecord::default()).api_key() == 26);
}
```

(Confirm the generated module paths from `crates/protocol/src/owned/mod.rs` after Task 1 — snake_case of each name.)

- [ ] **Step 2: Run** → FAIL (variants undefined). `cargo test -p crabka-protocol record::tests`.

- [ ] **Step 3: Add the 9 variants**

Add to the `KraftMetadataRecord` enum (with `use` imports for each generated type), to `api_key()` (real apiKeys), to `encode_value` (one match arm each, `r.encode(&mut body, version)`), and to `decode_value` (one arm each keyed on the apiKey):

```rust
// enum variants
UnregisterBroker(UnregisterBrokerRecord),            // 1
Config(ConfigRecord),                                // 4
DelegationToken(DelegationTokenRecord),              // 10
UserScramCredential(UserScramCredentialRecord),      // 11
ClientQuota(ClientQuotaRecord),                      // 14
AccessControlEntry(AccessControlEntryRecord),        // 18
RemoveAccessControlEntry(RemoveAccessControlEntryRecord), // 19
RemoveUserScramCredential(RemoveUserScramCredentialRecord), // 22
RemoveDelegationToken(RemoveDelegationTokenRecord),  // 26
```

In `api_key()`: map each to its number above. In `encode_value`: `Self::Config(r) => r.encode(&mut body, version)?,` etc. In `decode_value`: `4 => Self::Config(ConfigRecord::decode(&mut cur, v)?),` etc. Preserve the trailing-bytes guard for the new modeled arms.

- [ ] **Step 4: Run** → PASS. `cargo test -p crabka-protocol record::tests`.

- [ ] **Step 5: Commit**

```bash
git add crates/protocol/src/records/metadata/record.rs
git commit -m "feat(protocol): KraftMetadataRecord dispatch covers config/ACL/quota/SCRAM/token records"
```

---

## Task 3: JVM byte round-trip for the rare records

**Driven inline by the controller** (Docker). Create a fixture + test.

- [ ] **Step 1: Capture a JVM metadata log containing the rare records**

Boot a single-node `apache/kafka:4.0.0`; provoke the records, then dump+copy the log:

```bash
# topic config (ConfigRecord), client quota (ClientQuotaRecord), an ACL (AccessControlEntryRecord),
# a SCRAM cred (UserScramCredentialRecord):
docker exec <node> /opt/kafka/bin/kafka-configs.sh --bootstrap-server localhost:9092 \
  --entity-type topics --entity-name t --alter --add-config retention.ms=999999
docker exec <node> /opt/kafka/bin/kafka-configs.sh --bootstrap-server localhost:9092 \
  --entity-type users --entity-name u --alter --add-config 'SCRAM-SHA-256=[password=pw]'
docker exec <node> /opt/kafka/bin/kafka-acls.sh --bootstrap-server localhost:9092 \
  --add --allow-principal User:u --operation Read --topic t
docker exec <node> /opt/kafka/bin/kafka-configs.sh --bootstrap-server localhost:9092 \
  --entity-type clients --entity-name c --alter --add-config producer_byte_rate=1000
# copy the metadata log, cut to a clean batch boundary (Slice-0/1 walker)
```

Save the captured `.log` slice as `crates/protocol/tests/fixtures/rare_records_log.bin`. (Some commands need a SASL/authorizer-enabled broker config; if a record can't be provoked in this environment, capture the ones that can and note which rely on the unit round-trip only — esp. delegation tokens.)

- [ ] **Step 2: Add a round-trip test** in `crates/protocol/tests/kraft_metadata_roundtrip.rs`

Reuse the existing `assert_log_roundtrips` helper (decode each batch, round-trip each record value through `KraftMetadataRecord`, assert byte-identical):

```rust
#[test]
fn rare_records_log_roundtrips() {
    assert_log_roundtrips(include_bytes!("fixtures/rare_records_log.bin"));
}
```

- [ ] **Step 3: Run**

Run: `cargo test -p crabka-protocol --test kraft_metadata_roundtrip`
Expected: PASS (incl. the rare records). A mismatch = a schema/codegen byte bug → fix + regenerate (the Slice-1 loop).

- [ ] **Step 4: Commit**

```bash
git add crates/protocol/tests/fixtures/rare_records_log.bin crates/protocol/tests/kraft_metadata_roundtrip.rs
git commit -m "test(protocol): byte-identity round-trip of rare KIP-631 records from a JVM log"
```

---

## Task 4: Capstone — fmt, clippy, regression

- [ ] **Step 1:** `cargo fmt --all && cargo fmt --all --check` → clean.
- [ ] **Step 2:** `cargo clippy -p crabka-protocol --tests` → clean.
- [ ] **Step 3:** `cargo test -p crabka-protocol` → all green (the existing Slice-1/2 round-trips + the new dispatch tests). `cargo build -p crabka-protocol` → build.rs sha assertion green.
- [ ] **Step 4:** Commit any fmt fixes.

---

## Self-Review Notes

- **Spec coverage:** 9 schemas generated → Task 1; dispatch extended to all value-record equivalents + real apiKeys → Task 2; per-record round-trip → Task 2 tests; JVM byte round-trip → Task 3; capstone → Task 4. `VotersRecord`/`KRaftVersionRecord` correctly excluded (KIP-853 control records, Slice 5). Additive — image/engine/handlers untouched (no task touches them).
- **Real apiKeys** (1/4/10/11/14/18/19/22/26) are fetched-and-confirmed, baked into Task 2, not guessed.
- **Type consistency:** the 9 variant names + their generated-type imports + apiKeys are used identically across the enum, `api_key()`, `encode_value`, `decode_value`, and the tests.
- **Escapee guard:** Task 1 Step 4 explicitly stages the `src/{owned,borrowed}/<record>.rs` wrapper modules.
- **Validation honesty:** delegation-token records may not be provokable in a plain JVM cluster; Task 3 notes those fall back to the per-record unit round-trip (Task 2).
