# Crabka — project-specific guidance

## Compatibility

**Crabka is greenfield and undeployed.** There are no production users, no persisted state to migrate, no clients pinned to a specific build. Don't write backwards-compatibility shims:

- No `#[serde(default)]` on metadata fields "to keep old raft logs readable"
- No `V2` enum variants kept around alongside `V1` to support replay
- No feature flags that gate new behavior behind a default-off switch
- No migration code or one-shot upgraders for on-disk format changes
- No deprecated-but-kept API surfaces

When a schema, enum, wire format, or interface changes, just change it. Wipe local raft logs / data dirs if needed during development.

**Kafka compatibility is the constraint that matters.** Always preserve:

- Apache Kafka wire-protocol byte exactness (request/response shapes, field order, error codes, version negotiation)
- KIP semantics for whatever feature is being implemented
- Behavior the JVM admin tools (`kafka-topics`, `kafka-acls`, `kafka-leader-election`, `kafka-reassign-partitions`, etc.) rely on

When in doubt, match Kafka. When Kafka's behavior is undocumented or version-dependent, check the latest released cp-kafka image's behavior empirically rather than reading the wiki.
