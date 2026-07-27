# Share Consumer Acquire Mode Design

Expose Kafka share-fetch acquisition policy through the existing share-consumer builder while preserving batch-optimized behavior by default.

## Scope

`ShareFetchRequest` version 2 defines `share_acquire_mode`, but the client currently relies on its raw wire default of `0`. Callers need a typed choice between Kafka's batch-optimized behavior and its strict record-limit behavior.

This is a library-owned policy. No deployed binary currently constructs `ShareConsumer`, so this slice does not add an inert command-line, environment, CRD, or operator setting.

## API and Data Flow

Add a public `ShareAcquireMode` enum with:

- `BatchOptimized`, the default and wire value `0`.
- `RecordLimit`, wire value `1`.

`ShareConsumer::builder()` accepts the enum and stores it on `ShareConsumer`. Polling passes the semantic value to the share-fetch request builder, which converts it to `i8` only at the protocol boundary.

The generated protocol encoder already omits `share_acquire_mode` before request version 2, so version 1 compatibility remains unchanged.

## Key Decisions

### Preserve Existing Behavior

The builder defaults to `BatchOptimized`, matching the current raw default. Existing callers therefore require no changes.

### Use a Closed Enum

The two protocol-defined modes form a closed set. An enum prevents invalid values without a numeric newtype, parser, or new dependency. `refined_type` remains appropriate for validated scalar newtypes, but adds nothing to this finite choice.

### Keep Wire Values at the Boundary

`ShareConsumer` stores `ShareAcquireMode`, not `i8`. A private conversion supplies the exact wire value when constructing `ShareFetchRequest`. This keeps protocol representation out of the public builder and internal policy state.

## Testing

Focused tests prove:

- `BatchOptimized` is the default.
- The two variants map exactly to wire values `0` and `1`.
- A configured `RecordLimit` reaches `ShareFetchRequest`.
- Existing default request behavior remains unchanged.

The configuration audit is updated with the completed owner, focused scanner evidence, and the next unresolved candidate.

Formatting, strict Clippy, targeted tests, and the workspace test suite must pass before publication.

## Out of Scope

No string parsing, raw numeric input, protocol-generator change, CLI/environment option, CRD field, or operator wiring is added. A deployment surface belongs in a later slice only if a deployed process begins constructing `ShareConsumer`.
