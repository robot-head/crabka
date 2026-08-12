# Crabka unfinished-work catalog

Baseline: `origin/main` at
`f28a79d4ac52ac5cdf3edd3750bd6b703fee97e4` on 2026-08-11. This catalog was
reconciled against the shared worktree on 2026-08-12.

Only work inside the requested scope appears here. Work outside that scope is
absent from the rows and from every total.

The catalog distinguishes three states:

- **Closed**: the repository contains the bounded behavior and its acceptance
  evidence.
- **Verification pending**: the implementation exists, but the final named gate
  has not yet completed in this worktree.
- **Directional or limited**: the repository records an aspiration or a known
  boundary, but no finite implementation commitment with an acceptance gate.

## Finite outcomes awaiting final verification

None. Every eligible finite outcome discovered at the baseline is closed below.

## Closed finite outcomes

Rows remain here so completed markers are not rediscovered as unfinished work.

### Broker, protocol, storage, and clients

| ID | Closed behavior | Evidence |
| --- | --- | --- |
| K-01 | Repository-local metadata-version downgrade semantics are complete: capability gating, lossy-state projection before publishing the lower version, all-quorum-node checkpoints, fail-closed retry, restart recovery, and conservative rejection when capability is absent. | `crates/broker/src/handlers/update_features.rs`; `crates/metadata/src/image.rs`; `crates/raft/src/kraft/{controller,log}.rs`; `crates/client-admin/src/features.rs`; operator version reconciliation. **PASS:** UpdateFeatures 28/28, downgrade checkpoint/reload/failure/retry/restart gates, Admin 3/3, operator version 18/18, reconcile 2/2, raft library 173 passed / 1 ignored, strict raft clippy. |
| K-02 | A prepared producer transaction survives restart and can be committed, aborted, or force-terminated through Admin. | `crates/client-producer/src/{producer,transactional}.rs`; `crates/client-admin/src/transactions.rs`; `transactions_2pc_client.rs`. **PASS:** live recovery gate 1/1. |
| K-04 | Local remote storage writes Kafka-compatible artifact names/layout plus producer snapshots, with pinned JVM byte and parser verification. | `crates/remote-storage/src/{storage_manager,local}.rs`; `crates/broker/src/remote_log_manager.rs`; `crates/remote-storage/tests/jvm_tiered_storage.rs`. **PASS:** storage 64/64, broker RLM 30/30, JVM interop 1/1, strict clippy. |
| K-05 | Controller bootstrap advertises and routes its defined Admin subset, validates endpoints, serves quorum APIs, and gives clients controller discovery plus unsupported-API preflight. | `crates/broker/src/controller_admin.rs`; `crates/raft/src/server.rs`; `crates/client-admin/tests/controller_bootstrap.rs`. **PASS:** bootstrap 2/2 plus startup-API bypass. |
| K-06 | Core, producer, and Admin clients rebootstrap through learned endpoints, stale metadata, error 129, retired seeds, and retained controllers. | `crates/client-core/src/{client,pool}.rs`; producer/Admin clients. **PASS:** client-core 78/78 and strict clippy, client-admin 81/81, live retired-seed gate. |
| K-07 | Tiered `ListOffsets` handles latest-tiered, earliest-pending-upload, exact positive-timestamp lookup, concurrent remote work, and request/config timeout precedence. | `crates/broker/src/handlers/list_offsets.rs`; `crates/broker/src/remote_reader.rs`. **PASS:** handler 11/11, sparse timestamp 4/4, wire 1/1, strict broker clippy. |
| K-09 | Replication output, offset sync, and source checkpoint share one stable transaction and restore committed position after restart. | `crates/replicator/src/`; `crates/replicator/tests/recovery.rs`. **PASS:** live restart gate 1/1. |
| K-10 | Shared outbound broker/controller connections support OAUTHBEARER and reread the token file for each connection. | `crates/broker/src/network/client.rs`; `crates/client-core/src/sasl.rs`; `crates/broker/tests/raft_sasl.rs`. |
| K-11 | `ListTransactions` implements the v1 strict duration filter and the v2 whole-transactional-ID regular-expression filter; negative duration and null/empty pattern disable their filters, and malformed expressions return `INVALID_REGULAR_EXPRESSION` (128). | `crates/broker/src/handlers/list_transactions.rs`; `crates/broker/src/codes.rs`. **PASS:** focused handler suite 6/6. |
| S-06 | Diskless Slice 6d uses three brokers, one sole classic owner, two concurrent appenders, and three WAL voters. Both non-replica voters durably checkpoint the acknowledged prefix before the owner crashes and its canonical log is erased; an explicitly promoted voter byte-exactly adopts only its durable follower prefix. Readback succeeds while the object store remains blocked, then a fresh replacement-leader PUT failure, exact object recovery, JVM byte comparison, and history linearizability complete the witness. | `crates/integration-tests/tests/diskless_jepsen.rs`; `crates/broker/src/wal/quorum/follower.rs`; bounded crash model 17,701 unique / 56,343 generated states; combined Raft/WAL model 230,591 / 938,679. **PASS:** final RF=1 live gate retained all 8 acknowledged records through abrupt sole-owner and controller loss, direct Rust readback, replacement object retry, and exact JVM bytes. |

### Operator and Schema Registry

| ID | Closed behavior | Evidence |
| --- | --- | --- |
| O-01 | Multi-replica node pools allocate durable IDs, bootstrap once, join and remove voters safely, and retain IDs still present in StatefulSets or Pods. | `crates/operator/src/controller/kafka_node_pool.rs`; topology and pool scale/delete tests. |
| O-02 | Combined, controller-only, and broker-only pools render separate roles; broker-only pools wait for, but do not join, the controller quorum. | `kafka_node_pool.rs`; focused pool reconciliation tests. |
| O-03 | Topic replication-factor changes submit Admin reassignment and expose submitted/in-flight state. | `crates/operator/src/controller/topic.rs`; `reconcile_topic.rs`. |
| O-04 | Schema Registry CLI security accepts GSSAPI credentials and an explicit SASL host and maps both into runtime security. | `crates/schema-registry/src/{cli.rs,bin/schema-registry.rs}`. **PASS:** library 2/2, binary 1/1. |
| O-05 | Clients-CA replacement is staged trust-first, waits for exact rollout convergence, promotes/reissues all managed users, synchronizes trust bundles, and prunes only after convergence. | `crates/operator/src/controller/{cluster_ca,kafka,user_tls}.rs`; CA/user reconciliation tests. **PASS:** operator aggregate 74 tests and strict clippy. |

### Messaging and gateway

| ID | Closed behavior | Evidence |
| --- | --- | --- |
| F-01 | Subscription filters compile as SQL and execute through Arrow/DataFusion over schema-decoded records, including nested and repeated fields, enum names, schema caching, schema evolution, numeric values beyond floating-point precision, nulls, and delivery of the original bytes. Arrow is enabled by default for the gateway. | `crates/grpc-gateway/src/{filter,streaming}.rs`; `crates/client-streams/src/columnar/serde/arrow.rs`. **PASS:** gateway all-target suite 172/172, focused Arrow suite 11/11, and strict gateway/client-streams clippy. |
| MSG-01 | Record headers preserve order, duplicate names, and null values across input, consume, streaming, webhook, and outbound paths. | `gateway.proto`; `crates/grpc-gateway/src/{consume,streaming,webhook,outbound}.rs`; header-shape conformance vector. |
| MSG-02 | CloudEvents 1.0 binary and structured modes round-trip through HTTP and streaming paths, reject unsupported batches/media types, and preserve event data and attributes. | `crates/grpc-gateway/src/ce_translate.rs`; `crates/grpc-gateway/tests/cloudevents_roundtrip.rs`. **PASS:** gateway all-target suite 172/172, including round-trip coverage. |
| MSG-03 | Subscribe acknowledgements commit only the highest contiguous delivered prefix; filtered or out-of-order acknowledgements cannot skip undelivered records. | `crates/grpc-gateway/src/streaming.rs`; consumer commit tests. **PASS:** commit suite 10/10 and streaming 4/4 at the completed gate. |
| MSG-04 | Share backlog aggregates only authoritative partition results, preserves unknown state instead of false zero, drops stale ownership, and exposes deployment/configuration for KEDA consumption. | Broker share-backlog handler and tests; `crates/grpc-gateway/src/config.rs`; KEDA deployment/configuration assets. **PASS:** share backlog 2/2 and strict broker clippy. |
| MSG-05 | The plaintext gateway listener supports both HTTP/1 and prior-knowledge h2c, enabling bidi SDK subscriptions without breaking unary calls. | `crates/grpc-gateway/src/serve.rs`; raw HTTP/2 transport test. |
| MSG-06 | Queue acquisition sessions are reusable across polls, retain acquired coordinates until disposition, isolate delivery state and counts per consumer group, and reject invalid acknowledgement types or coordinates not owned by that session. Live SDK results use the gateway's authoritative entry, preserve retryability, and map access denials consistently. | `crates/grpc-gateway/src/queue.rs`; all five SDK queue implementations; v1.1 queue-session conformance vector; `crates/grpc-gateway/tests/scripts/jvm_queue_cross_consumer.sh`. **PASS:** five-adapter mock and live matrices plus focused SDK queue tests. |

### Validation, CI, packaging, and tracking

| ID | Closed behavior | Evidence |
| --- | --- | --- |
| SDK-01 | The shared v1.1 JSON-lines conformance harness defines versioned messaging, CloudEvents, filter, header, auth, error, and queue vectors and boots a live broker/gateway substrate. | `crates/sdk-conformance/`. **PASS:** harness 9/9; all five adapters each pass 15 mock vectors with 1 live-only skip and 12 live-compatible vectors with 4 deliberately unsupported skips. |
| SDK-02 | The Go SDK exposes the v1.1 messaging and queue contract plus a conformance adapter, including group-isolated queue state, strict acknowledgement validation, authoritative result-entry mapping, and stable error taxonomy. | `sdks/go/`; `.github/workflows/sdk-go.yml`. **PASS:** format, vet, tests, adapter build, and both shared matrices. |
| SDK-03 | The Node TypeScript SDK exposes the v1.1 messaging and queue contract plus a conformance adapter, with bigint offsets and the same queue/error guarantees as the shared contract. | `sdks/ts/`; `.github/workflows/sdk-ts.yml`. **PASS:** 26 tests, type-check, build, zero-vulnerability audit, and both shared matrices. |
| SDK-04 | The Java-first SDK with Kotlin transport exposes the v1.1 messaging and queue contract plus a conformance adapter, preserving authoritative entries and per-entry retryability. | `sdks/java/`; `.github/workflows/sdk-java.yml`. **PASS:** 27 tests, Gradle build and `installDist`, and both shared matrices. |
| SDK-05 | The Rust application SDK exposes the v1.1 messaging and queue contract plus a conformance adapter with per-group queue state. | `crates/app-sdk/`; `.github/workflows/sdk-rust.yml`. **PASS:** 19/19 tests, strict all-feature clippy, and both shared matrices. |
| SDK-06 | The Linux C++ SDK exposes the v1.1 messaging and queue contract through an nghttp2 transport plus a conformance adapter with per-group queue state. | `sdks/cpp/`; `.github/workflows/sdk-cpp.yml`. **PASS:** ASan/UBSan CTest 4/4, TSan CTest 4/4, and both shared matrices. |
| D-01 | The compatibility matrix separates implemented behavior, finite missing work, tracked horizons, and deliberate non-goals. | `docs/KIP_MATRIX.md`. |
| N-01 | The bounded newtype-safety rollout is complete; its old survey is explicitly retained as history rather than an unchecked backlog. | `docs/newtype-safety-rollout.md`; six shared identifiers and their byte-compatibility gates. |
| P-01 | The gateway has melange/apko packaging, multi-architecture image publication with signing and attestations, release metadata, and SDK workflows triggered by gateway protocol changes. | `packaging/melange/crabka.yaml`; `packaging/apko/crabka-gateway.yaml`; `.github/workflows/{publish-images,sdk-go,sdk-ts,sdk-java,sdk-rust,sdk-cpp}.yml`; `release-plz.toml`. **PASS:** workflow YAML/action structure, shell syntax, and publish allowlist gates. |
| V-01 | ISR catch-up/expansion is enabled and no longer ignored. | `crates/broker/tests/leader_election.rs::isr_expand_on_catchup`. |
| V-02 | Rust/JVM log interoperability runs in both directions under a dedicated CI job. | `crates/log/tests/integration.rs`; `.github/workflows/ci.yml` `log-integration`. |
| V-03 | The diskless live fault-injection binary is selected as a shipping gate and changes to its crate trigger that lane. | `crates/integration-tests/tests/diskless_jepsen.rs`; `.github/workflows/ci.yml`. This row closes CI wiring only; S-06 records the completed live result. |
| V-04 | The serialized JVM broker job runs both follower-divergence directions, wire conformance, and conservative downgrade rejection; replication fences in-flight responses by leader ID and epoch. | broker replicator/supervisor and JVM divergence test. **PASS:** replicator 19/19, supervisor 31/31, ignored JVM suite 4/4. |
| V-05 | Rebalancer state-topic recovery runs explicitly with ignored tests enabled in CI. | `crates/rebalancer/tests/state_topic.rs`; `.github/workflows/ci.yml`. |
| V-06 | The JVM gateway differential runs its ignored Docker test in a dedicated CI step. | `crates/grpc-gateway/tests/jvm_differential.rs`; `.github/workflows/ci.yml`. |
| V-07 | Real Confluent Schema Registry record interoperability runs after the in-process suite. | `crates/schema-registry/tests/interop.rs`; `.github/workflows/ci.yml`. |
| V-08 | Admin UI login has a Playwright lane with the matching Chromium revision and a health gate. | `crates/admin-ui/tests/e2e.rs`; `.github/workflows/ci.yml`. |
| V-09 | JVM transactional smoke commits through a Java producer and verifies read-committed visibility. | `crates/broker/tests/jvm_acceptance.rs`; broker JVM CI lane. |
| V-10 | JVM `DescribeGroups` calibration covers classic and next-generation groups and pins the authority response. | `crates/broker/tests/describe_groups_jvm.rs`; `fixtures/describe_groups/real_kafka_next_gen.json`. |
| V-11 | Delegation-token operator E2E produces with `acks=all`, consumes the persisted record, and compares the exact payload. | `.github/workflows/operator-e2e.yml`. |

Environment-heavy CI rows are wiring evidence unless the row explicitly says
**PASS**. They are not reported as local executions.

## Directional horizons, not finite commitments

These remain unfinished in the broad product sense, but their source documents
do not define a bounded repository outcome that can be closed here.

| ID | Directional horizon | Boundary still open |
| --- | --- | --- |
| H-01 | Full drop-in JVM Kafka Streams library parity. | `crabka-client-streams` is a separately scoped Rust API; no finite parity set or replacement acceptance gate exists. |
| H-02 | Full KIP-1150 diskless GA beyond Slice 6d. | The umbrella does not pin the benchmark profile, cluster topology, object-store class, fault window, sample size, percentile, numeric meanings of “near-zero” or “seconds-scale,” or the operator/API design for per-topic configuration. Without those inputs, its performance and elasticity statements cannot form a reproducible acceptance gate. |
| H-04 | Chapter 2 lakehouse-native topics. | Topic-to-Parquet materialization, Iceberg metadata and consistency, in-process topic SQL/Flight SQL, and external catalogs remain separate future designs. |
| H-05 | Chapter 4 eventing qualification. | The roadmap does not pin an upstream version or manifests, cluster topology, container references, scaler implementation/API, workload, polling windows, thresholds, timing/retry bounds, or a pass/fail harness. Its named qualification checkpoints therefore remain directional until a bounded test plan supplies those parameters. |
| H-06 | User-facing blob service. | An S3-compatible API, tenant namespace and policy model, object metadata/index plane, and presigned/multipart behavior are deliberately late follow-ons. |
| H-07 | Messaging/gateway/Schema Registry tenant control plane. | Unified provisioning for topics, gateway access policy, schema namespaces, tenant isolation, credentials, usage rollup/billing, and a local developer bootstrap remains an unscoped product program. |
| H-08 | Realtime fan-out surface. | Many-subscriber fan-out, WebSocket/SSE bindings, presence/broadcast channels, and per-subscription offsets are not yet finite commitments. |
| H-09 | General production hardening. | The README names performance, broader JVM edge compatibility, operator depth, generic native Connect depth, and full Streams depth as themes, not acceptance-gated tasks. |
| H-10 | Successful mixed-JVM rolling software downgrade. | Completion depends on an upstream release advertising the required downgrade capability; no repository-local change can make an older peer advertise it. |
| H-11 | Five-SDK release engineering. | Registry targets, package/version policy, publication credentials, and release acceptance gates are not yet defined across all five ecosystems. |
| H-12 | Geo-replication schema policy lane. | Schema Registry integration, schema-aware redact/mask/tokenize transforms, record-level routing, and fail-closed decode remain a separately staged follow-on without their own accepted implementation plan. |
| H-13 | Geo-replication operator integration. | A `GeoReplication` CRD plus workload, configuration, credential, and connection-security reconciliation remains a separately staged follow-on. |
| H-14 | Geo-replication compliance extensions. | Audit/erasure propagation and region-scoped encryption or key residency are explicit future capabilities, not bounded commitments. |
| H-15 | Generic Connect operator surface. | A generic `KafkaConnect` worker CRD and declarative plugin/image-build surface remain roadmap items; the current operator has no such workload manager. |
| H-16 | Operator distribution and migration tooling. | An OLM bundle and a manifest migration tool remain roadmap items without accepted implementation gates. |

The eligible Chapter 1 and Chapter 4 sections of
`docs/superpowers/specs/2026-07-05-crabka-north-star-roadmap-design.md` are
explicitly a 24+ month, undated vision. Only the messaging, blob, realtime, and
gateway/client-control passages of
`docs/superpowers/specs/2026-07-06-crabka-serverless-backend-vision-design.md`
are used here; no adjacent product program is imported into scope.

## Documented limitations, not finite commitments

| ID | Current limitation | Evidence |
| --- | --- | --- |
| L-03 | Native Connect does not load arbitrary source or sink plugins. | Compile-time `Source`/`Sink` composition in `crates/connect/src/{lib,runtime}.rs`; no runtime plugin-loader surface. |
| L-04 | JVM Kafka Connect plugins cannot be loaded. | Native Rust `Source`/`Sink` traits in `crates/connect/src/{source,sink}.rs`; no JVM plugin boundary. |
| L-05 | The Kafka Connect distributed-worker protocol is absent. | `crates/connect/src/{lib,runtime}.rs`; the runtime is explicitly single-process. |
| L-06 | The Kafka Connect REST API is absent. | `crates/connect/src/{lib,runtime}.rs`; lifecycle is an embeddable Rust API. |
| L-07 | A connector cannot run multiple tasks. | `crates/connect/src/runtime.rs`; one runtime owns one source and one sink. |
| L-08 | A connector cannot run more than one coordinated worker replica. | `crates/connect/src/runtime.rs`; no distributed assignment or shared worker-membership layer. |
| L-09 | Broker JVM acceptance has no validated Windows CI/runtime path; its dedicated CI lane runs only on Ubuntu. | `crates/broker/tests/KNOWN_ISSUES.md`; `.github/workflows/ci.yml`. |
| L-10 | Operator Admin RPCs use the default plaintext internal client and do not load TLS/SASL credentials for a secured internal listener. | `crates/operator/src/context.rs`; topic internal-listener selection. |
| L-11 | The TypeScript v1 target is Node-only; browser transport and npm publication are deferred. | `docs/superpowers/specs/2026-07-06-crabka-sdk-ts-design.md`. |
| L-12 | The Java v1 target excludes Android, reactive bindings, and Maven Central publication. | `docs/superpowers/specs/2026-07-06-crabka-sdk-java-design.md`. |
| L-13 | The Rust application SDK excludes wasm and crates.io publication. | `docs/superpowers/specs/2026-07-06-crabka-sdk-rust-design.md`. |
| L-14 | The C++ v1 target is Linux/plaintext only and has no package-manager distribution. | `docs/superpowers/specs/2026-07-06-crabka-sdk-cpp-design.md`. |
| L-15 | Diskless partitions reject transactional record batches with `INVALID_TXN_STATE`. | `crates/broker/src/handlers/produce.rs`; diskless Slice 4 design. |
| L-16 | The Streams source path cannot ingest null-valued source records, so source-row tombstones are not supported. | `crates/client-streams/tests/fk_join_broker.rs`; KIP-1071 Streams client design. |
| L-17 | Rolling membership that mixes eager and cooperative classic-group protocols remains unsupported. | `STATUS.md` Slice 64 follow-up boundary; classic coordinator and cooperative assignor tests. |
| L-18 | The native consumer exposes subscription-based flows but no JVM-style manual `assign()` API. | `crates/client-consumer/README.md`; `crates/client-consumer/src/consumer.rs`. |
| L-19 | The native Admin client covers the repository's current operator needs rather than the full JVM AdminClient surface; log-directory calls target the connected broker and do not retry through controller discovery. | `crates/client-admin/README.md`; `crates/client-admin/src/log_dirs.rs`. |
| L-20 | The standalone geo-replicator resolves source and target clients with plaintext security; its worker accepts security objects, but the supervisor always supplies `None`. | `crates/replicator/src/supervisor.rs`; `crates/replicator/src/worker.rs`. |
| L-21 | Queue delivery is unary pull only, uses the group-level fixed lock duration, has no dead-letter queue, and requires single-gateway session affinity. | `docs/superpowers/specs/2026-07-06-crabka-msg6-queue-rpc-design.md`; `crates/grpc-gateway/src/queue.rs`. |
| L-22 | All five application SDKs expose equality-only filters over structured records rather than the gateway's richer SQL predicate surface. | `crates/app-sdk/src/messaging.rs`; `sdks/{go,ts,java,cpp}` filter adapters. |
| L-23 | All five application SDKs default subscriptions to auto-commit and do not expose public manual per-offset acknowledgement. | SDK designs under `docs/superpowers/specs/2026-07-06-crabka-sdk-*-design.md`; SDK subscription adapters. |
| L-24 | The application SDKs expose unary publish but not the gateway's bidirectional `SendStream` batch-produce RPC. | `docs/superpowers/specs/2026-07-06-crabka-polyglot-messaging-sdk-design.md`; SDK messaging clients. |
| L-25 | Gateway bearer-token configuration uses unsecured development JWS material and is not a production authentication surface. | `sdks/go/README.md`; `crates/grpc-gateway/src/config.rs`. |
| L-26 | The Go SDK does not expose topic auto-provision or typed CloudEvents consumption. | `sdks/go/README.md`. |
| L-27 | The application SDKs expose blob and identity/control-plane calls only as typed stubs. | `crates/app-sdk/src/stubs.rs`; `sdks/{go,ts,java,cpp}` stub modules. |
| L-28 | The Go live-compose SDK gate is manual-only because its broker and gateway images are not guaranteed to be available in ordinary CI runs. | `sdks/go/README.md`; `.github/workflows/sdk-go.yml`. |
| L-29 | Delegation-token master-key changes require restart; hot reload is unsupported. | `STATUS.md` delegation-token known limitations; broker delegation-token configuration. |
| L-30 | Delegation-token renewal extends the existing credential; it does not rotate the token ID and HMAC. | `STATUS.md` delegation-token known limitations; `crates/client-admin/src/delegation_tokens.rs`. |
| L-31 | Operator delegation-token renewal describes every token and filters locally, making each renewal O(all tokens). | `STATUS.md` delegation-token known limitations; operator delegation-token controller. |
| L-32 | Operator delegation-token act-as requires a super-user, while the rendered population is hardcoded to `ANONYMOUS`; authenticated internal principals need a missing super-users CRD surface. | `STATUS.md` delegation-token known limitations; operator Kafka rendering. |
| L-33 | The OPA authorizer has no mutual-TLS client-certificate plumbing. | `STATUS.md` OPA known limitations; `crates/broker/src/authorizer/opa.rs`. |
| L-34 | The OPA authorizer has no policy-bundle management awareness; policy distribution is external. | `STATUS.md` OPA known limitations; `crates/broker/src/authorizer/opa.rs`. |
| L-35 | OPA cache misses bridge synchronously into async work, and concurrent misses for the same key can serialize without single-flight coalescing. | `STATUS.md` OPA known limitations; `crates/broker/src/authorizer/opa.rs`. |
| L-36 | The operator's OPA cluster test proves rendering and startup but not an end-to-end allow/deny produce path. | `STATUS.md` OPA known limitations; `.github/workflows/operator-e2e.yml`. |
| L-37 | General PLAIN/SCRAM connection-lifetime reauthentication, minimum accepted bearer-token lifetime, and a native-client reauthentication scheduler are absent. | `STATUS.md` OAuth parity follow-ups; broker connection auth state and native client SASL path. |
| L-38 | PLAIN carrying an OAuth token and `tokenEndpointUri`-based token acquisition are not implemented. | `STATUS.md` OAuth parity follow-ups; `crates/client-core/src/sasl.rs`. |
| L-39 | OAuth and GSSAPI validation settings are broker-global, so separate listeners cannot use divergent identity-provider or Kerberos configurations. | `crates/operator/src/controller/listeners.rs`; broker auth configuration. |
| L-40 | Schema Registry exposes version-specific compatibility checks but no all-versions compatibility endpoint. | `crates/schema-registry/src/rest/{mod,compatibility}.rs`. |
| L-41 | Schema Registry `GET /schemas` does not implement `offset` and `limit` pagination. | `crates/schema-registry/src/rest/schemas.rs`; Schema Registry REST slice designs. |
| L-42 | Schema Registry contexts are not implemented. | `crates/schema-registry/src/rest/mod.rs`; Schema Registry REST slice designs. |
| L-43 | JSON Schema remote or dangling `$ref` values are unresolved and therefore treated permissively during compatibility checking. | `crates/schema-registry/src/format/json/{mod,diff}.rs`. |
| L-44 | Schema Registry mode handling supports `READWRITE`, `READONLY`, and `IMPORT`, but not `READONLY_OVERRIDE`. | `crates/schema-registry/src/kafkastore/mod.rs`. |
| L-45 | Schema Registry write forwarding trusts the forwarding header after initial request authorization; the inter-node hop has no cryptographic forwarding identity. | `crates/schema-registry/src/rest/forward.rs`; Schema Registry HA security design. |
| L-46 | Schema Registry's outbound broker client cannot acquire OAuth tokens; its CLI credential construction covers PLAIN, SCRAM, and GSSAPI. | `crates/schema-registry/src/{cli.rs,bin/schema-registry.rs}`. |
| L-47 | The Schema Registry operator does not mint serving certificates or render workload autoscaling, disruption-budget, and network-policy resources. | `crates/operator/src/controller/schema_registry.rs`; Schema Registry operator design. |
| L-48 | Lazily allocated per-entity quota buckets have no eviction path and can grow with the set of encountered entities. | `crates/broker/src/quota/buckets.rs`. |
| L-49 | `AlterClientQuotas` validates IP entity names as IPv4 only. | `crates/broker/src/handlers/alter_client_quotas.rs`. |
| L-50 | APIs outside the dispatch throttle-patch table are delayed by request quotas but do not echo that delay in `throttle_time_ms`. | `crates/broker/src/network/dispatch.rs`. |
| L-51 | IP byte-rate quota entries pass validation but are not enforced. | `STATUS.md` IP quota known limitations; broker quota lookup and connection-rate paths. |
| L-52 | The rebalancer is single-replica and has no Lease-based leader election or multi-replica HA. | `docs/superpowers/specs/2026-05-17-crabka-rebalancer-roadmap-design.md`; `crates/rebalancer/src/bin/rebalancer.rs`. |
| L-53 | Operator network policy has no `ipBlock` peers, outbound policy, or per-node-pool override. | `docs/superpowers/specs/2026-05-17-crabka-operator-network-policy-23-design.md`; `crates/operator/src/crd/network_policy.rs`. |
| L-54 | The operator manages one Kubernetes cluster and has no CRD conversion webhooks for divergent schema versions. | `docs/superpowers/specs/2026-05-15-crabka-operator-roadmap-design.md`; operator controller and CRD inventories. |
| L-55 | The Admin UI manages one configured cluster, authenticates only with SCRAM-SHA-512, and has no OIDC/OAuth, reverse-proxy, mTLS-only, PLAIN, SCRAM-SHA-256, or public non-Dioxus REST surface. | `docs/superpowers/specs/2026-07-04-dioxus-broker-admin-ui-design.md`; `crates/admin-ui/src/{config,auth,server}.rs`. |

## Closure rule

Move a verification-pending row to **Closed** only after its named gate passes.
A directional horizon or limitation becomes finite work only when a later spec
defines bounded behavior and an acceptance gate.
