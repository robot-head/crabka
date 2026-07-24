## gRPC Gateway Task 2 report

### RED

- `cargo test -p crabka-grpc-gateway internal_topic_policy` exited 101:
  `InternalTopicPolicy` and `internal_topic_policy` were unresolved.
- `cargo test -p crabka-grpc-gateway ownership_is_warm` exited 101:
  `ownership_is_warm` was unresolved.
- `cargo test -p crabka-grpc-gateway schema_registry_cache` exited 101:
  `SchemaRegistryClient::new_with_policy` was missing.
- The frame-raw construction test also failed to compile because
  `build_schema_registry_codec` was missing.

### Implementation

- Added one internal-topic policy shared by dedup and membership topic creation.
  Replication factor, fallback, create timeout, segment time, and dirty ratio
  now come from `GatewayRuntimeConfig`; the old RF constants and topic literals
  are gone.
- Routed the configured consumer poll timeout through membership, ownership,
  outbound, and streaming consumers; routed the ownership warmup threshold and
  readiness polling interval.
- Routed Schema Registry latest-cache TTL and `frame_raw` through production
  client/codec construction; removed `LATEST_TTL`.
- Preserved topic partition counts, cleanup policies, retention semantics, and
  existing protocol behavior.

### Verification

- New behavior tests: 4 passed.
- Focused `dedup`, `outbound`, `streaming`, and `schema` test filters: passed.
- `cargo test -p crabka-grpc-gateway`: 145 passed, 2 ignored, 0 failed.
- `cargo clippy -p crabka-grpc-gateway --all-targets -- -D warnings`: passed.
- `cargo +nightly fmt --all -- --check`: passed.
- `git diff --check`: passed.

### Concerns

None.
