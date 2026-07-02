# KIP-595 Slice 3d-1 — KIP-631 records foundation (complete the dispatch)

Date: 2026-05-31
Status: Approved (brainstorming) — pending spec review

## Context

Slice 3d migrates the live metadata path off the wincode `crabka_metadata::MetadataRecord`
enum onto the real KIP-631 `crabka_protocol::records::metadata::KraftMetadataRecord`
(full handler migration — the chosen end-state: handlers speak genuine Kafka
records, no wincode enum, fully byte-real log for the mixed JVM+Crabka quorum,
Slice 6). That is a 4-part mini-program (incremental, tree green at each step):

- **3d-1 — records foundation (this doc):** generate the missing record schemas
  + extend the `KraftMetadataRecord` dispatch to cover all 17 `MetadataRecord`
  equivalents. Pure additive.
- 3d-2 — rewrite `MetadataImage::{validate,apply,to_records}` + engine submit/
  apply + snapshot onto `KraftMetadataRecord`; change `submit_change` to
  `Vec<KraftMetadataRecord>`.
- 3d-3 — migrate the ~25 broker handlers + bootstrap to construct
  `KraftMetadataRecord`.
- 3d-4 — delete the wincode `MetadataRecord` enum + `kafka_record` bridge +
  `SerdeCompat` usage.

Exploration found only 5 of 17 `MetadataRecord` variants have KIP-631
counterparts today (Topic, Partition, RegisterBroker, RemoveTopic,
FeatureLevel); combined with the other already-generated `KraftMetadataRecord`
variants the common cluster path is covered, but the rare/advanced records
(configs, ACLs, quotas, SCRAM, delegation tokens, unregister) have no schema yet.

## Goal & scope

Generate the remaining KIP-631 metadata record schemas and extend the
`KraftMetadataRecord` dispatch enum so every `MetadataRecord` variant has a real
Kafka-control-record counterpart, validated by byte round-trip + JVM
`kafka-dump-log` (the Slice-1/2 method).

**In scope:** the ~9 new record schemas (codegen), the extended dispatch enum,
unit + JVM byte-validation tests.

**Out of scope:** `MetadataImage`/engine/snapshot migration (3d-2), handler
migration (3d-3), wincode-enum deletion (3d-4). Nothing in the live path
changes; `MetadataImage`, the engine, the broker, and `submit_change` are
untouched.

## Records to add

Fetched verbatim from apache/kafka at the pinned sha
`a9ce3221537b8653448750697915607dc7936cf3` (same as the existing schema set),
mapping the unmapped `MetadataRecord` variants:

| MetadataRecord variant(s) | Kafka schema |
|---------------------------|--------------|
| `V1TopicConfig`, `V1BrokerConfig` | `ConfigRecord` |
| `V1AccessControlEntry` | `AccessControlEntryRecord` |
| `V1DeleteAccessControlEntry` | `RemoveAccessControlEntryRecord` |
| `V1ClientQuota` | `ClientQuotaRecord` |
| `V1ScramCredential` | `UserScramCredentialRecord` |
| `V1DeleteScramCredential` | `RemoveUserScramCredentialRecord` |
| `V1DelegationToken` | `DelegationTokenRecord` |
| `V1DeleteDelegationToken` | `RemoveDelegationTokenRecord` |
| `V1UnregisterBroker` | `UnregisterBrokerRecord` |

(`V1Voters`→`VotersRecord` and `V1KRaftVersion`→`KRaftVersionRecord` were
generated in Slice 1; Topic/Partition/RegisterBroker/RemoveTopic/FeatureLevel
are already in the dispatch.) Extend `KraftMetadataRecord` from 10 → ~19
variants (plus the retained `Unknown` arm) so all 17 `MetadataRecord`
equivalents are representable.

## Approach

The established Slice-1 workflow:

1. Fetch each schema; transform `"type":"metadata"`→`"data"` and strip the
   top-level `"apiKey"`, **recording the schema's real apiKey** for the dispatch
   mapping (do NOT guess a sequential apiKey — the Slice-1 lesson; the real
   keys are non-sequential, e.g. ConfigRecord/ACL/quota have their own values).
2. Run `tools/regenerate.sh`; commit the regenerated `generated/` tree + the new
   `src/{owned,borrowed}/<record>.rs` wrapper modules (the Slice-1 escapee
   reminder: git-add the wrapper files too).
3. Add a `KraftMetadataRecord` variant per record keyed on its real apiKey in
   both the encode (`api_key`/`encode_value`) and decode (`decode_value`) paths.
   The envelope codec (frameVersion + apiKey + apiVersion + body) is unchanged.

## Validation

- **Unit:** per-record envelope round-trip (`encode_value(ver)` →
  `decode_value` → byte-identical) for all ~19 modeled variants, plus the
  existing `Unknown`-arm + truncation tests.
- **JVM byte round-trip (Docker-gated):** drive an `mirror.gcr.io/apache/kafka:4.0.0` cluster
  to emit the rare records into its `__cluster_metadata` log — `kafka-configs`
  (ConfigRecord, ClientQuota), `kafka-acls` (AccessControlEntry),
  `kafka-configs --alter --entity-type users` (UserScramCredential) — capture
  the log, and round-trip those real bytes through the extended dispatch
  byte-identically (the Slice-1/2 gold standard). Records that are hard to
  provoke deterministically (delegation tokens) rely on the per-record
  round-trip unit test; note the gap.

## Error handling

Decode of a still-unmodeled apiKey falls to the `Unknown` arm (forward-compat).
A modeled record with trailing bytes after its body errors (the Slice-1
trailing-byte guard). Out-of-range apiVersion → typed `ProtocolError`.

## Disposition

Permanent. Completes the `KraftMetadataRecord` dispatch so 3d-2/3/4 can migrate
the image, engine, and handlers onto it. Additive — zero live-path change.
