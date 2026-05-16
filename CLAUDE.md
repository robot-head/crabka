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

## Execution

When executing implementation plans, always use **subagent-driven development in parallel batches** where the per-task file sets don't overlap. The plan groups tasks into batches; dispatch all tasks within a batch concurrently (single message, multiple Agent calls), wait for the batch to complete, review, then move to the next batch. Sequential dispatch one-task-at-a-time is wasted wall-clock — use it only when later tasks genuinely depend on earlier ones in the same batch.

A "conflict" between parallel implementers requires the same file being edited by both. Tasks like "add wire codes" (codes.rs) and "add metadata fields" (records.rs) don't conflict and should run together. When in doubt, list the file set each task touches before deciding.
