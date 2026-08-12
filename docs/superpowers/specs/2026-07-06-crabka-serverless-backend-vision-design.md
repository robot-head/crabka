# Crabka as a Serverless Application Backend on the Unified Substrate

**Date:** 2026-07-06
**Status:** Approved
**Type:** North-star vision / roadmap chapter (data-plane BaaS on the substrate) — *not* a subsystem design. Sub-services are directional; each becomes its own spec → plan cycle. The spine is honesty: on every individual face Crabka is parity-or-behind a specialist (Supabase most of all); the only defensible moat is the shared log + object bucket and near-free cross-service integration.

## North star

**Build a serverless application backend — messaging, a relational database, blobs, realtime, and auth — where every service is a face of one durable log and one columnar object bucket, so that a write is a stream is a table with no connector, no ETL, and no second copy in between.**

### The unifying thesis: one substrate, many data services

The thesis is deliberately narrow, because the honest version is the only defensible one. Crabka is **not** a better Supabase, a better Neon, a better SQS, or a better S3 — on each of those axes, in isolation, it is parity-or-behind a specialist that already exists. The wedge is one level down, in the storage architecture itself.

Modern serverless data services have quietly converged on the same shape: **a durable append-only log for ordering and recovery, object storage for the bulk of the bytes, and a thin access layer on top.** Neon's Postgres streams its WAL into safekeepers and materializes pages onto S3. Aurora's mantra is literally "the log is the database." Kafka is a durable log that already spills to object storage. A columnar lakehouse is object storage plus a manifest. These are not four storage architectures — they are one architecture wearing four product skins, and each incumbent rebuilds the log-plus-object-plus-access-layer stack privately, then bolts a connector onto the next service to move data between them.

Crabka's bet is to build that shared layer **once, in the open, as the substrate** — a quorum-durable WAL over one object bucket, with a DataFusion/Parquet query engine that already scans that bucket — and then hang all the data services off it. The payoff is not per-service excellence. It is that **cross-service data movement becomes free**: because messages, database changes, blob manifests, and telemetry all land on the same durability tier in the same bucket, an event that is a Kafka topic record is *simultaneously* a change stream, a queryable columnar table, and (once the lakehouse lands) an Iceberg data file — with no Debezium, no separate WAL-tailer, no S3-to-warehouse export pipeline, and no CDC connector to stand up between any two faces.

```
                        ONE APPLICATION, ONE PROJECT IDENTITY
   ┌──────────┬──────────────┬──────────────┬──────────────┬────────────┐
   │ Messaging│  Serverless  │     Blob      │  Realtime +  │  Auth /    │
   │ pub/sub  │   Postgres    │   storage     │     CDC      │   RLS      │
   │ + queues │  (Neon-shaped)│              │              │            │
   └────┬─────┴──────┬───────┴──────┬───────┴──────┬───────┴─────┬──────┘
        │            │              │              │             │
        │   the access layer (gateway · schema registry · SDK)  │
        └────────────┴──────────────┴──────────────┴────────────┘
                                   │
        ┌──────────────────────────┴───────────────────────────┐
        │        SHARED ACCESS ENGINE — DataFusion / Parquet    │
        │        (blockstore already scans this bucket)         │
        └──────────────────────────┬───────────────────────────┘
                                   │
        ┌──────────────────────────┴───────────────────────────┐
        │   QUORUM-DURABLE WAL   ·   2f+1 AZ fsync-quorum        │  (spec-only)
        │   payload-blind consensus kernel (kraft-core)         │  (kernel: built)
        └──────────────────────────┬───────────────────────────┘
                                   │
        ┌──────────────────────────┴───────────────────────────┐
        │        ONE OBJECT BUCKET  —  S3 / GCS / R2 / local     │  (built)
        │  topic segments · DB pages · blobs · signals · Iceberg │
        └───────────────────────────────────────────────────────┘

   the same physical log entry is, at once:
       a produce record  =  a change-stream event  =  a columnar row
                          =  (post-Ch.2) an Iceberg data file
```

**Read the diagram top-down and it looks like Supabase. Read it bottom-up and it is the point.** Supabase is Postgres *plus* an S3-backed Storage service *plus* an Elixir Realtime service that tails the WAL *plus* GoTrue for auth — four systems, three auth models, and connectors between them. Crabka's faces are not bolted together; they are projections of one log on one bucket. That single fact — not any individual face — is the entire chapter.

---

## Where Crabka is today for this

Honesty first, because the readiness across these faces is wildly uneven and the framing must never average them. **Messaging is real and nearly shippable; everything else ranges from a genuine head start to a starting line.**

**What is built and load-bearing today:** a wire-exact Kafka broker (pub/sub transport, produce/fetch, classic + KIP-848 consumer groups, transactional/EOS) with **fully implemented, JVM-validated KIP-932 share groups** — real competing-consumer queues, not a roadmap item (`crates/broker/src/handlers/share_fetch.rs`, 767 lines, zero production stubs; `crates/broker/src/share_coordinator/`, `crates/broker/src/share_partition/`; `crates/broker/tests/jvm_share_groups.rs`). On top sits a stateless, Deployment-shaped gRPC/Connect + HTTP gateway with a real pub/sub proto (`crates/grpc-gateway/`), a Confluent-compatible Schema Registry over the `_schemas` compacted topic (`crates/schema-registry/`, `crates/schema-serde/src/wire.rs`), and a reusable identity plane — SCRAM, mTLS, OAUTHBEARER/JWKS validation, delegation tokens, principals, and an ACL authorizer, with the gateway already resolving a per-request bearer JWT to an overriding `Principal` (`crates/security/`, `crates/authz/`, `crates/grpc-gateway/src/authz/auth_layer.rs`). Change-data ingest is real too: `crates/connect-postgres/` does genuine `pgoutput` logical decoding (Begin/Commit/Relation/Insert/Update/Delete, replication slots, LSN-resumable checkpoints), and the single-process at-least-once Connect runtime pipes that into a topic (`crates/connect/src/runtime.rs`). Everything sits on a **unified object-store constructor** — S3/GCS/R2/MinIO/local/in-memory behind one `Arc<dyn ObjectStore>` (`crates/object-store/src/lib.rs`, `build_object_store` at `build.rs:21`) — and a working **columnar materialization engine on that bucket**: Parquet blocks with an index that prunes queries to candidate blocks, DataFusion row-group pushdown, and point-read-by-key (`crates/blockstore/`). Multi-tenancy is proven *in exactly one plane*: the observability path has Mimir-style tenant-ID validation, per-tenant limits, and tenant-as-physical-object-prefix (`crates/metrics/src/tenant.rs`, `crates/blockstore/src/store.rs`).

**What is bounded and landed, but not yet production-complete:** diskless slices 1–6d provide the WAL seam, quorum durability, object-store flushing, ordered publication, restart recovery, metadata-only reads, crash-model coverage, and a three-broker fault witness. This is an M1–M3 foundation, not the M4 claim: production cost and latency envelopes, broad soak and chaos evidence, and operator-driven elasticity remain open.

**What is net-new — most of the BaaS product surface:** the Neon-shaped **pageserver** (physical WAL-redo to versioned 8 KB pages) and the Postgres `smgr`/`get_page@LSN` compute integration; **end-user auth** (signup/login/JWT issuance — a GoTrue equivalent) and **row-level security** (0% built, tree-wide); an **S3-compatible blob server API** and per-tenant bucket/metadata plane (0% built — `object-store` *consumes* object storage, it does not *serve* an S3 API); the **lakehouse/Iceberg** surface (0% built — no `iceberg` dependency in any `Cargo.toml`); a **realtime fan-out engine** and client subscription protocol beyond one-consumer-group-per-caller gRPC; the **project/tenant control plane**, unified polyglot **SDK**, and usage **metering/billing** (the CLI has exactly one subcommand, `format`; there is no `Project` CRD; a tenant is currently a whole cluster).

---

## The services as chapters

Each face below is a *consumer* of the substrate wedge. On its own merits, each is parity-or-behind a specialist — that is stated plainly and repeatedly, because it is true. The differentiation is always the shared log and shared bucket, never the face.

### Chapter A — Messaging (pub/sub + queues) — *the only near-term-shippable face*

**Thesis.** Messaging is the proof-of-concept for the entire integration wedge, and it is almost entirely already built. Crabka is a wire-exact Kafka broker with real KIP-932 competing-consumer queues, a stateless polyglot gateway, and a schema plane — so a "serverless messaging" product is a thin DX/CloudEvents wrapper over landed plumbing. **The sharp wedge:** the *same* topic on the *same* bucket is simultaneously a pub/sub channel, a KIP-932 work queue, a CDC change stream, and an observability WAL — four primitives, one log entry, zero connectors — with a broker-native queue-backlog signal no consumer-group-only system exposes.

**What we stand on today.**
- Wire-exact Kafka pub/sub: topics, produce/fetch, classic + KIP-848 groups, transactional/EOS (`crates/broker/src/handlers/{produce,fetch}.rs`, `crates/broker/src/coordinator/unified/`, `crates/broker/src/txn/`).
- Real competing-consumer queues via KIP-932 share groups, JVM-validated, zero production stubs (`crates/broker/src/handlers/share_fetch.rs`, `crates/broker/src/share_coordinator/`, `crates/broker/src/share_partition/`, `crates/broker/tests/jvm_share_groups.rs`).
- Broker-computed queue backlog as a native autoscaling metric: `lag = (hwm − start_offset).max(0)` (`crates/broker/src/handlers/describe_share_group_offsets.rs:229`).
- Stateless Connect-RPC + HTTP gateway with a real pub/sub proto — `Send`/`SendStream`/`Subscribe` (`crates/grpc-gateway/proto/crabka/gateway/v1/gateway.proto`, `crates/grpc-gateway/src/lib.rs`).
- HTTP produce ingress + signed webhook ingress (HMAC, replay guard, idempotency key, JSONPath key extraction, schema-bound bodies) and outbound push delivery with per-subscription DLQ, exponential backoff+jitter, and JSONPath filtering (`crates/grpc-gateway/src/{webhook,outbound}.rs`).
- Confluent Schema Registry + Confluent wire framing for typed messaging (`crates/schema-registry/src/rest/`, `crates/schema-serde/src/wire.rs`).
- Ordered record headers end-to-end, including duplicate keys and null values, through the proto, transport-agnostic gateway seam, consumers, webhooks, and outbound delivery (`crates/grpc-gateway/proto/crabka/gateway/v1/gateway.proto`, `crates/grpc-gateway/src/{types.rs,consume.rs,outbound.rs}`).
- CloudEvents 1.0 binary and structured HTTP ingress/egress, with binding validation and byte-preserving Kafka payload translation (`crates/grpc-gateway/src/{ce_translate.rs,webhook.rs,outbound.rs}`).
- Explicit subscription acknowledgements advance only a contiguous per-topic-partition frontier; filtered records close known gaps without allowing a later ack to skip an unacknowledged delivery (`crates/grpc-gateway/src/consume.rs`).
- Fleet-complete share-group backlog sampling resolves the partition leader, prunes stale series, and publishes a KEDA-ready gauge plus example `ScaledObject` (`crates/broker/src/share_partition/backlog_poller.rs`, `docs/examples/keda-sharegroup-scaledobject.yaml`).
- The plaintext gateway listener accepts HTTP/1.1 and HTTP/2 prior knowledge (h2c), so bidirectional Connect streams work for SDKs without TLS (`crates/grpc-gateway/src/serve.rs`).

**What landed in this cycle.**
- Thin messaging and queue SDKs in Go, TypeScript, Java, Rust, and C++, all driven by the same versioned conformance vectors and live-compatible gateway harness (`crates/sdk-conformance/`, `sdks/{go,ts,java,cpp}/`, `crates/app-sdk/`).
- The CloudEvents binding, lossless header shape, contiguous explicit-ack semantics, fleet-complete share backlog gauge/KEDA bridge, and plaintext h2c transport listed above are implemented rather than remaining packaging placeholders.

**Moat / wedge.** Strong and substrate-derived: one topic = pub/sub + KIP-932 queue + CDC feed + observability WAL, all landed code on one bucket, plus a native HWM−SPSO backlog signal for autoscaling. Per *primitive*, this is parity-or-behind SQS / Pub/Sub / Supabase Queues — the moat is the combinatorial one-substrate integration, not any single messaging feature. **The fourth leg — "= an Iceberg table" — is aspirational and gated on the unbuilt Ch.2 lakehouse (no `iceberg` dependency exists); it is not part of the near-term moat.**

**Key risk.** Low, and now mostly release qualification: package/version the five SDKs, run upstream interop suites, and prove the backlog bridge against a deployed autoscaler. The gateway no longer drops the CloudEvents header shape, and its fleet sampler no longer maps a non-local partition to false zero backlog.

### Chapter B — Blob storage — *deprioritize; a late follow-on, not a headline face*

**Thesis.** Blob storage is the **weakest** face and must not be presented as "nearly free from the object-store crate" — that is precisely the over-claim the honesty mandate forbids. The substrate *consumes* object storage (a bucket handle, multipart PUT, byte-range GET, retention/GC); it does **not** serve an S3 API. An S3-like blob store on Crabka is nearly 100% net-new engineering with no substrate moat today. **The only future wedge — user blobs colocated on the same lakehouse bucket, so a blob can become an Iceberg data file for free — is entirely gated on the unbuilt Ch.2 and Iceberg.**

**What we stand on today.**
- A unified object-store constructor: S3/R2/MinIO, GCS (keyless Workload Identity), local, in-memory, with multipart tuning and credential redaction (`crates/object-store/src/lib.rs`, `crates/object-store/src/config.rs`).
- Blob-lifecycle *primitives* already exercised: streaming multipart PUT, byte-range GET (`GetRange::Bounded`), idempotent delete, retention/GC (`crates/remote-storage/src/s3.rs`).

That is **plumbing** — a bucket handle plus multipart plus range reads — **not a blob service.**

**The net-new (essentially the whole product).**
- An entire **S3-compatible server API**: `PutObject`/`GetObject`/`ListObjectsV2`/`CreateBucket`/multipart/presigned URLs. Grep confirms **none exists in-tree** (the one `PutObject` hit is a test double in observability tests).
- A per-tenant bucket / object-namespace + auth (IAM-style policies, presigned URLs, bucket ACLs). No tenancy or bucket model for user blobs exists.
- A blob metadata/index plane (listing, content-type, ETag, versioning, per-object metadata) distinct from the KIP-405 segment-metadata plane.
- The blob-as-columnar-object / Iceberg-adjacent surface — gated on the unbuilt Ch.2.

**Moat / wedge.** **Essentially none today.** Standalone, an S3-like blob store on Crabka is behind S3/R2 on API surface, durability track record, and per-tenant tooling. The sole substrate angle is *future* colocation with the lakehouse, which depends on Ch.2 and Iceberg landing.

**Key risk.** The framing trap. Averaged in next to messaging, "blobs on the same object-store crate" sounds nearly free; it is not. **Recommendation: deprioritize blob as a headline BaaS face; treat it as a late follow-on that only becomes interesting once the lakehouse exists.**

### Chapter C — Serverless Postgres (Neon-shaped) — *the keystone, and the deepest net-new IP*

**Thesis.** The vision is a disaggregated Postgres: real Postgres-compatible compute streams its WAL into Crabka's quorum WAL (the safekeeper), a pageserver materializes 8 KB relation pages onto the same bucket that holds topics and blobs, and compute reads pages back over an RPC. **But the honest near-term deliverable is NOT a serverless Postgres and must not be pitched as "parity-or-behind Neon" — that implies a working product exists, and it does not.** The pageserver (physical WAL-redo → versioned pages @LSN) plus the `smgr`/`get_page@LSN` compute integration is the hard 80% of Neon, and **essentially none of it exists.** The genuinely differentiated, *landed* deliverable is the **integration story**: Postgres logical replication is already a first-class Crabka topic via `connect-postgres`, so CDC-out and realtime come for free with no Debezium — and (post-Ch.2) an Iceberg table of every DB change for free too.

**What we stand on today** (the storage *ingredients*, not the storage *half*).
- The object bucket a pageserver would write/read pages to (`crates/object-store/src/build.rs:21`).
- A columnar materialization engine on that bucket — Parquet with index pruning, DataFusion row-group pushdown, point-read-by-key — a *pageserver-shaped* read path already running for signals (`crates/blockstore/src/store.rs`, `reader.rs`, `log_blockstore.rs:439`). **It is scan/append-oriented, not versioned-page-@LSN.**
- Real Postgres logical decoding: `pgoutput` parser, replication-slot + publication management, LSN-resumable checkpoints (`crates/connect-postgres/src/pgoutput.rs`, `source.rs`, `offset.rs`). **This DECODES WAL into logical CDC — the opposite direction from applying physical WAL to reconstruct pages.**
- The payload-blind consensus **kernel** is landed (`QuorumStateMachine`, `crates/kraft-core/src/core.rs:30`), so one WAL engine can genuinely serve both Kafka partitions and Postgres WAL groups. **But the `QuorumWalStore` / `WalStore` safekeeper seam is approved-spec-only (slice 6a); do not list it as built.**

**The net-new (the hard 80% of Neon).**
- The **pageserver**: a WAL-redo engine that *applies* Postgres WAL to reconstruct versioned 8 KB pages, with per-page LSN materialization and page GC/compaction. `connect-postgres` decodes *logical* row changes; there is no page redo anywhere. This is Neon's core IP.
- A Postgres **`smgr` shim + `get_page@LSN` RPC** (the path compute calls instead of local disk). No `smgr`, no page-service RPC, no compute integration exists in-tree. Requires a patched Postgres or a protocol shim.
- **Physical** WAL ingest, not logical: the diskless WAL is Kafka-record-shaped (offset-sequenced v2 batches); Postgres WAL is a byte-addressed LSN stream. Either wrap WAL segments as opaque record values (workable — the kernel is payload-blind) or add a byte-stream WAL variant, plus the per-shard `ShardId → WAL engine` registry slice 6a itself flags as net-new.
- A read-replica page service (branch/PITR): copy-on-write branching and page-at-LSN reads — the blockstore is not versioned-page-at-LSN, so MVCC-page-at-LSN and branching are new.
- **The entire storage foundation is spec-only, not landed** (`crates/broker/src/wal/` absent). The safekeeper this stands on is itself unbuilt.

**Moat / wedge.** Shared-substrate co-location, decisively **not** database quality. Pages materialize on the same bucket and same quorum-WAL groups as topics, blobs, and the lakehouse — one durability tier, one query engine spanning DB pages + messages + telemetry. And Postgres logical replication is *already* a Crabka artifact: the same WAL that would feed the pageserver is turned by `connect-postgres` into a topic of change events, so realtime, CDC-out, and (post-Ch.2) a lakehouse table of every DB change come from infrastructure that already exists. Against Neon/Aurora on serverless-Postgres merits: **not parity-or-behind — at the starting line, standing on unbuilt foundations.** This is an integration moat, never a database moat.

**Key risk.** The largest gap in the whole program. Physical WAL-redo → versioned pages, plus `smgr`/`get_page@LSN` compute integration, is Neon's hard core and none of it exists; the ingest direction is backwards (decode vs. apply); the WAL is record-shaped not LSN-addressed; and the storage foundation underneath is approved-spec-only. **Label the pageserver/disaggregated-compute vision explicitly as multi-quarter net-new IP dependent on the unbuilt WAL slices.** The near-term deliverable is the CDC-as-a-topic integration story, not a Neon-competitive database.

### Chapter D — Realtime + CDC

**Thesis.** Because the Neon-shaped Postgres streams its WAL into the quorum WAL, every committed row change is already a logical-decoding event that lands as a keyed record on a topic — so "every DB write is a change stream is a client subscription" is nearly free, and the gateway already exposes a server-push `Subscribe` stream with server-side JSONPath row filtering, the exact shape of Supabase's `postgres_changes`. **The honest wedge is integration only:** the change feed, the topic it fans out from, the subscribe RPC, and per-request bearer-JWT→principal resolution are one substrate with zero glue. It is **not** the realtime engine on its own — Supabase Realtime is a mature WebSocket multiplexer and beats Crabka's one-consumer-group-per-caller gRPC stream on every scale axis.

**What we stand on today.**
- Postgres logical-decoding CDC: `poll()` emits one keyed record per row change (INSERT/UPDATE/DELETE→tombstone) with table/LSN/operation headers (`crates/connect-postgres/src/source.rs:264`, `:303`).
- A single-process at-least-once Connect runtime piping CDC into a topic (`crates/connect/src/runtime.rs`).
- Server-push egress: a bidi `Subscribe` RPC that joins a group on the caller's behalf and streams records with ack-driven commits (`crates/grpc-gateway/src/streaming.rs:245`, `crates/grpc-gateway/src/consume.rs:27`).
- Server-side row filtering: client-supplied JSONPath `FieldPredicate` equality over a decoded structured-JSON view (`gateway.proto:34`, `crates/grpc-gateway/src/streaming.rs:89`).

**The net-new.**
- Realtime-scale fan-out + a client subscription protocol: `Subscribe` joins one consumer group per caller; a real multiplexer needs many-subscribers-per-topic fan-out, a **WebSocket/SSE binding** (only gRPC/Connect exists), presence/broadcast channels, and per-subscription (not per-group) offsets.
- A stable public change-event envelope (old/new row images, op, commit-LSN ordering) and a topic-per-table routing convention the subscription protocol can target.
- **RLS-aware fan-out** — see the top-line security risk below.

**Moat / wedge.** Substrate-derived: the same quorum WAL that holds the Postgres WAL is the log the CDC source decodes, the topic the gateway fans out from, and (via Ch.2) the Iceberg table the feed projects into — so a DB write, a realtime event, an audit log, and an analytics row are one physical object, not four systems glued by connectors. "Live queries over DB changes" is *wiring*, not new infrastructure. On the engine axis alone: parity-or-behind Supabase Realtime.

**Key risk (top-line, security-critical).** The `FieldPredicate` subscription filter is **client-supplied and advisory — it filters, it does not enforce.** Absent RLS-aware server-side fan-out, a naive realtime implementation **leaks every row on a table to every subscriber holding a coarse topic-level Read ACL — a silent authorization bypass, a data-leak-by-default.** Mark `FieldPredicate` as **non-security-enforcing** and require RLS-aware server-side fan-out before any multi-tenant realtime ships. This risk is compounded by, and gated on, the unbuilt auth model in Chapter E.

### Chapter E — Auth / RLS

**Thesis.** The realtime egress primitives and JWT *validation* are real; the authorization *model* that would make this a Supabase-competitive auth face is **0% built**, and it is the load-bearing gap of the whole BaaS program. **State bluntly: the auth *server* is parity-or-behind — GoTrue + Postgres RLS is a complete model Crabka has not started.** The only defensible near-term claim is the identity *seam*: a per-request end-user JWT already resolves to a `Principal`, on one log, reusing one authorizer.

**What we stand on today.**
- Per-request end-user token → identity: an axum middleware resolves `Authorization: Bearer <JWT>` to a `Principal` that overrides the connection-level mTLS identity (`crates/grpc-gateway/src/authz/auth_layer.rs:52`, `:28`).
- JWT/OAuth **validation** library: OAUTHBEARER (RFC 7628), signed-JWS, JWKS, plus SCRAM, mTLS principal extraction, delegation-token HMAC (`crates/security/src/oauthbearer.rs`, `jwks.rs`, `mtls.rs`, `scram/`).
- Resource-level authz on the realtime path: subscribe is gated by Read ACLs on the group and every topic before the stream opens (`crates/grpc-gateway/src/streaming.rs:264`, `crates/metadata/src/acl.rs`). **This is coarse — Topic/Group/Cluster, no per-row/per-user concept.**

**The net-new.**
- **Row-Level Security — entirely absent tree-wide.** RLS is the whole Supabase authz model; it must be enforced *inside* the (unbuilt) Neon-shaped Postgres compute as real RLS policies, because Crabka's only authz today is coarse Kafka ACLs.
- **End-user JWT issuance (a GoTrue equivalent) — absent.** Crabka validates tokens but mints none for end users (the only issuance in-tree is service-scoped delegation tokens, `crates/operator/src/controller/user_delegation_token.rs`). Net-new: signup/login/refresh/reset/OAuth-provider flows minting end-user JWTs whose claims (`sub`, `role`) feed *both* the gateway principal and Postgres RLS (`SET LOCAL request.jwt.claims`/`role`).
- **RLS-aware subscription enforcement** — bind end-user JWT claims to a *server-enforced* row filter (or push the RLS predicate into the feed) so subscriptions leak no unauthorized rows.

**Moat / wedge.** Integration only: change feed + subscribe stream + per-request principal resolution already exist on one log, and **one authorizer** (`crabka-security` + `crabka-authz`, reused verbatim by broker, gateway, and registry) is the seam to build on — versus Supabase's three bolted-together auth systems (Postgres roles/RLS, Storage policies, Realtime channels). But that advantage only materializes *after* end-user issuance and RLS are built. Never claim auth-server superiority in isolation.

**Key risk (the highest-severity gap after the pageserver).** RLS must hold identically across **two enforcement points that today share no policy**: query-time (in the unbuilt Postgres compute) and change-feed-time (the realtime stream). Getting one JWT's claims to flow through both — with no path that leaks a row a subscriber cannot read — is the real net-new engineering, and it **depends on the unbuilt Postgres-compute face.** Until then, multi-tenant realtime is a data-leak-by-default and must not ship.

### Chapter F — Client SDK + Control plane

**Thesis.** This is the **thinnest** face, and the anti-Supabase reality bites hardest here: a developer today provisions a whole Kafka cluster and reasons in ACLs + K8s namespaces, not a `crabka.projects.create()` that mints topics + DB + bucket + keys behind one API key. **The unified-tenancy moat is aspirational, not near-term** — multi-tenancy is implemented *only* in the observability plane. The one real seam is the shared principal/ACL model reused across every face.

**What we stand on today.**
- Native Rust client SDK for messaging: idempotent + transactional producer, consumer, full admin client (`crates/client-producer/`, `crates/client-consumer/`, `crates/client-admin/`, `crates/client-core/`).
- A separate gateway-riding application SDK in Go, TypeScript, Java, Rust, and C++, with messaging, queues, shared error/auth/CloudEvents semantics, contract-v1.1 JSON vectors, mock and live-compatible adapters, and per-language CI workflows (`crates/sdk-conformance/`, `sdks/`, `crates/app-sdk/`, `.github/workflows/sdk-{go,ts,java,rust,cpp}.yml`).
- The polyglot seam — a Connect-RPC/HTTP gateway (`Send`/`SendStream`/`Subscribe`, inbound webhooks, outbound subscriptions) plus server-side structured produce/subscribe with JSON↔schema serialization (`crates/grpc-gateway/`).
- Confluent-compatible Schema Registry over `_schemas` — a client of the broker, not a separate DB (`crates/schema-registry/`).
- A Strimzi-shaped Kubernetes operator over **cluster-scoped** CRDs — Kafka, KafkaNodePool, KafkaTopic, KafkaUser, KafkaGrpcGateway, SchemaRegistry, KafkaRebalance (`crates/operator/src/crd/`).
- Reusable identity primitives and one authorizer (`crates/security/`, `crates/authz/`, gateway `BearerValidator`).
- **Proven tenancy — but only in observability:** Mimir tenant-ID validation, per-tenant limits, tenant-as-physical-object-prefix (`crates/metrics/src/tenant.rs`, `crates/blockstore/src/store.rs`); plus a KIP-73 token-bucket rate limiter (Creusot-verified) and Kafka client-quota plumbing as the enforcement mechanism (`crates/throttle/`, `crates/client-admin/src/quotas.rs`).

**The net-new (almost the entire control plane).**
- A **`Project`/`Tenant` abstraction** that, from one create call, provisions topics + DB + buckets + API keys on the shared substrate. No such CRD exists; every operator CRD is cluster-scoped Kafka machinery, and **a tenant is currently a whole cluster.** (The `X-Tenant` strings in the operator are test fixtures, not a tenancy model.)
- **Messaging/DB/blob multi-tenancy on one substrate** — the messaging, gateway, and (planned) DB/blob paths have no topic namespacing, no per-tenant object-store isolation, and no cross-face tenant identity.
- A unified SDK spanning future service faces behind one project credential, plus a Python binding. The five-language messaging/queue contract is landed; cross-service project semantics are not.
- Usage **metering aggregation and billing** per project (the enforcement mechanisms exist; the rollup and billing do not).
- Dev-loop DX: a `crabka dev` that boots broker + gateway + registry + DB + object-store and seeds a project (the CLI has exactly one subcommand, `format`).

**Moat / wedge.** Unified project identity + single-bootstrap/single-bucket tenancy across faces — one authorizer to reconcile vs. Supabase's three. The proven pattern to generalize is **tenant-as-physical-bucket-prefix**, verified in `crates/blockstore`. Everywhere else — raw client ergonomics, polyglot bindings, the S3 API — parity-or-behind the specialists.

**Key risk (dark side of the whole thesis).** The unified-tenancy advantage is exactly the single point of catastrophic failure. Retrofitting real per-tenant isolation onto the messaging/DB/blob paths is deep, security-critical substrate work, and on a shared WAL/bucket **a cross-tenant read is company-ending and hits all four faces simultaneously.** Present unified tenancy honestly: a proven pattern that *must* be generalized, and flag that generalization as **the highest-severity security work in the whole BaaS program.** The one-substrate moat and the one-substrate blast radius are the same fact.

### Chapter G — Schema-typed columns + complex realtime filtering — *cross-cutting; the sharpest realtime differentiator*

**Thesis.** Crabka already combines a **Schema Registry** (Avro/Protobuf/JSON) with an **Arrow/DataFusion** engine. Those ingredients now power a server-enforced SQL predicate engine over nested schema-typed topic records, evaluated by DataFusion. The remaining product direction is a first-class column type system spanning the other data faces and authorization-policy composition that cannot widen access.

**What we stand on today.**
- Confluent-compatible Schema Registry — Avro/Protobuf/JSON with the full compatibility matrix over the compacted `_schemas` topic (`crates/schema-registry/src/rest/`).
- Confluent wire framing incl. protobuf message-index (`crates/schema-serde/src/wire.rs`).
- A **decode-to-Arrow seam already exists**: `RowBridge`/`RowCodec` decode Kafka `(key, value)` through registry serdes into Arrow/Polars frames with reserved `__key`/`__offset`/`__timestamp`/`__partition` columns (`crates/client-streams/src/columnar/`).
- Arrow/Parquet with **DataFusion** expression evaluation, row-group pushdown, and point-read-by-key (`crates/blockstore/`).
- The topic-side `Subscribe` filter is now a server-enforced SQL boolean expression compiled through DataFusion against registry-decoded Arrow rows. It handles nested/repeated fields and enum symbols across Avro and Protobuf, caches by schema, recompiles across schema evolution, and delivers the original Kafka bytes unchanged (`crates/grpc-gateway/src/{filter.rs,streaming.rs}`, `crates/client-streams/src/columnar/serde/arrow.rs`).

**The net-new.**
- **Authorization-policy composition** — the topic filter now decides delivery rather than hinting; composing that decision with RLS (Chapter E) so a predicate can never widen authorization remains future work.
- **Column-type representation**: protobuf/avro/arrow columns in the Neon-shaped Postgres (composite/domain types, or `bytea` + a registered schema id), and **native nested-struct columns** in the lakehouse/Iceberg view (Arrow is Parquet/Iceberg's native shape).
- **Filter pushdown**: evaluate the predicate as close to the change feed as possible so fan-out only materializes matching changes (avoid decoding+scanning every change for every subscriber).

**Moat / wedge.** One **schema plane** spanning messaging + DB + realtime + lakehouse, and a *complex, server-enforced, DataFusion-powered* subscription filter over nested typed data — the single sharpest realtime differentiator, and the fix for the advisory-filter leak. Supabase's realtime filter is advisory equality on flat columns; this is a query engine over nested schema-typed data on one substrate. **Honest bound:** per raw filter *expressiveness* a full streaming-SQL engine (ksqlDB/Flink/Materialize) is more capable — the wedge is *one schema plane, one engine, one substrate, server-enforced*, integrated with RLS and CDC for free, not a novel query language.

**Key risk.** Filter correctness + **per-change-per-subscriber evaluation cost at fan-out scale** (a complex predicate over a decoded nested row, for every subscriber, on every change) — pushdown and shared evaluation are essential, not optional. The topic-side filter engine is implemented; remaining work is authorization-policy composition, pushdown/shared evaluation, and fan-out scale qualification.

---

## The moat, stated plainly

**Why one-substrate-backs-all beats Supabase's Postgres + S3 + bolt-ons.** Supabase is an *integrated product* — but its integration is at the API layer, over four independent storage systems: Postgres has its own storage, Storage is a metadata service in front of S3, Realtime is a separate Elixir service that opens its own replication connection to *tail* the Postgres WAL, and analytics is a warehouse you export to. Moving data between them means connectors, WAL-tailers, and ETL, and each system has its own durability story, its own copy of the bytes, and its own auth model.

Crabka's integration is at the *storage* layer. There is **one bucket, one durable log, and one query engine** underneath every face:

- **Every table is also a topic and (post-Ch.2) an Iceberg table.** A Postgres row change is not exported to a change stream — it *is* the same log entry the pageserver applied, already sitting on the shared bucket, already a keyed record on a topic, already a candidate columnar row. The realtime feed is not a bolted-on WAL-tailer racing the database; it is the *same physical log*.
- **No ETL between services.** An analytics query can join a Kafka topic against a DB table against a blob manifest through one DataFusion engine on one bucket — no export pipeline, no second copy, no eventual-consistency window between the operational store and the warehouse.
- **One durability tier, one authorizer, one bootstrap.** A single project identity and a single tenant namespace fan out across messaging, DB, blobs, and realtime; there is one ACL/principal model (`crabka-security` + `crabka-authz`) to reconcile, not three.
- **One schema plane across every face (Chapter G).** The Schema Registry + Arrow are the type system for messages, DB columns, and realtime filters alike — so a column can be a protobuf/avro/arrow value and a realtime subscription can filter it with a *server-enforced, DataFusion-evaluated complex predicate over nested types*. Supabase's `postgres_changes` filter is advisory equality on flat columns; this is a query engine over nested schema-typed data on one substrate — a differentiator that is, again, architectural (the same Arrow-decoded record the lakehouse scans is the one the filter evaluates), not a feature bolt-on.

That is the wedge, and it is genuinely defensible because it is architectural, not featural: a competitor cannot replicate "the change feed is the same object as the topic is the same object as the analytics row" without rebuilding their storage layer on a shared log.

**The honest "master of none" counter.** On every *individual* face, Crabka is parity-or-behind a specialist that already ships the end-user-facing halves Crabka lacks: SQS/Pub/Sub on messaging primitives, S3/R2 on blobs (where Crabka has *no server API at all*), Neon/Aurora on serverless Postgres (where Crabka is *at the starting line*, with the pageserver being Neon's unbuilt-here core IP), and — most pointedly — **Supabase itself**, which is *also* a single-platform Postgres + Realtime + Auth + Storage product and already ships GoTrue, RLS, a Storage API, a mature WebSocket multiplexer, first-class TS/Python SDKs, and a project control plane. Against S3 + Neon + Confluent-as-separate-specialists, the "one bucket, one WAL, one engine" story is real. Against Supabase, the comparison *inverts*: Supabase is the integrated incumbent, and Crabka is the one with four *disintegrated* protocol surfaces and no project abstraction. Four faces each parity-or-behind, with an integrated competitor already in market, is the textbook jack-of-all-trades trap.

**The answer.** Do not compete as the Supabase *product*. Compete as the *infrastructure a Supabase-like product is built on* — the shared durable log + columnar object layer that makes cross-service data movement free. Supabase's realtime is bolted onto Postgres via a separate service reading the WAL; Crabka's *is* the same physical log, and that same log is *already* a wire-exact Kafka topic and an analytics-ready columnar table. That is the one true differentiator, and the discipline this chapter demands is to sell exactly that and nothing more: **the write-is-a-stream-is-a-table wedge, with messaging as the single near-term-shippable proof, and every other face labeled honestly by how far it is from the starting line.**

---

## Decomposition & sequencing

The sub-services are buildable projects in dependency order. The ordering principle: **reuse the most-built substrate first, ship the wedge cheaply, and gate the deep net-new IP behind the diskless WAL it depends on.**

**Dependency spine.**

```
   [ Diskless quorum WAL — slices 1-6d ]  ← bounded M1-M3 foundation landed; M4 remains directional
             │
   ┌─────────┴─────────────────────────────────────────────┐
   │                                                        │
 (already landed on the broker log)                   (waits on the WAL)
   │                                                        │
   ▼                                                        ▼
 A. MESSAGING ──► DX/SDK + CloudEvents + KEDA bridge   C. SERVERLESS POSTGRES (keystone)
   │  (thin wrapper; reuses broker + gateway + SR)         │  pageserver + smgr + physical WAL ingest
   │  └► G(topic-side): schema-typed complex filters       │  (multi-quarter net-new IP)
   │     (registry + RowBridge + DataFusion — all landed)  │  └► G(DB-side): protobuf/avro/arrow columns
   │                                                        │      (rides the Postgres compute)
   │                                                        │
   ├──► F₀. per-face TENANCY  (highest-severity security)   │
   │        generalize observability tenant-prefix          │
   │        to messaging/DB/blob                             │
   │                                                        ▼
   │                                                   D. REALTIME + CDC
   │        (CDC-in already landed via connect-postgres) ────┤  change-event envelope + WS/SSE + fan-out
   │                                                        │
   │                                                        ▼
   │                                                   E. AUTH / RLS
   │                                                        │  end-user JWT issuance + RLS
   │                                                        │  + RLS-aware fan-out (needs C's compute)
   │                                                        ▼
   └──► F. PROJECT CONTROL PLANE + UNIFIED SDK ◄────────────┘
            (Project CRD, polyglot bindings, metering/billing)
                        │
     Ch.2 LAKEHOUSE / ICEBERG (0% built) ──► unlocks the 4th leg of the wedge
                        │                      and B. BLOB's only future moat
                        ▼
                 B. BLOB STORAGE  (deprioritized late follow-on)
```

**What reuses the diskless substrate, and what unblocks what:**

1. **Messaging (A)** stands on landed broker code and needs no new WAL work to ship its DX layer — it is the cheapest path to a demonstrable BaaS face and the proof-of-concept for the wedge. It unblocks nothing structurally but *earns the right* to the rest.
2. **Per-face tenancy (F₀)** is a prerequisite for *any* multi-tenant face and is the highest-severity security work in the program. It generalizes the proven observability tenant-prefix pattern (`crates/blockstore`) to the messaging/DB/blob paths. Everything multi-tenant waits on it; it should start early and in parallel with A.
3. **Serverless Postgres (C)** is the **keystone**: Realtime's RLS-aware fan-out (D→E) and the DB face itself both wait on the Postgres compute + pageserver, which in turn waits on the diskless WAL (safekeeper) landing. It is the longest pole — multi-quarter net-new IP — and it gates the most.
4. **Realtime + CDC (D)** has its *ingest* half landed (`connect-postgres` CDC-in) and can prototype the change-event envelope and a WebSocket binding *before* C lands — but its *authorization* half (RLS-aware fan-out) is blocked on both C (the compute hosting RLS) and E (the JWT/RLS model).
5. **Auth/RLS (E)** is blocked on C (RLS lives in the Postgres compute) and is a hard prerequisite for shipping multi-tenant realtime safely. Its identity-validation seam is landed; issuance and RLS are net-new.
6. **Control plane + SDK (F)** composes all faces behind one project identity; it waits on F₀ (tenancy) and benefits from A, C, D, E existing, though the polyglot bindings and dev-loop CLI can start against the messaging face early.
7. **Schema-typed columns + complex filtering (G)** splits cleanly. Its **topic-side** — a complex, server-enforced, DataFusion-backed filter engine over schema-typed *message* records — reuses only landed code (registry + `RowBridge` + DataFusion) and should ship **alongside Messaging (A)** as the concrete demonstration of the schema-plane wedge and the fix for D's advisory-filter leak. Its **DB-column-type side** (protobuf/avro/arrow columns) rides the Postgres compute (C). It also composes with Realtime (D) — G *is* D's filter engine — and with Auth (E) — server-enforcement must respect RLS.
8. **Blob storage (B)** is deprioritized: nearly 100% net-new, no near-term moat, and its only future differentiation (colocation with the lakehouse) is gated on **Ch.2/Iceberg**, which is 0% built.

**Messaging implementation is landed; qualify it next.** The immediate release work is now evidence rather than another design cycle: run every SDK through the shared mock and live matrices, preserve lossless headers and CloudEvents bytes across real clients, qualify the reusable queue session against the broker, and validate the fleet-complete backlog signal with a deployed autoscaler. The schema-typed DataFusion filter engine is part of that implemented surface; its next gates are authorization composition, shared evaluation, and fan-out scale.

---

## Non-goals

- **Serverless functions / compute.** Out of scope for this chapter by decision. Running user code (a Lambda/Edge Functions/Deno-runtime equivalent) is a *later* chapter; the Knative/CloudEvents work (Ch.4) is what sets it up, and this data-plane chapter deliberately stops at data + realtime + auth.
- **Edge / global distribution.** No CDN, no edge-replicated read replicas, no globally-distributed low-latency data plane. The substrate is regional (the quorum WAL is a 2f+1 *AZ* quorum, not cross-region); edge is not something the substrate can honestly back yet.
- **Anything gated on the unbuilt lakehouse (Ch.2 / Iceberg).** The "= an Iceberg table" fourth leg of the wedge, cross-service analytical joins over Iceberg, and blob-as-columnar-data-file are all **aspirational until Ch.2 lands** and must be labeled as such wherever they appear. There is no `iceberg` dependency in-tree today.
- **A Neon-competitive serverless Postgres in the near term.** The pageserver + `smgr`/`get_page@LSN` compute integration is multi-quarter net-new IP dependent on the still-unbuilt diskless WAL. The near-term Postgres deliverable is the *integration story* (CDC-as-a-topic → realtime + lakehouse), explicitly **not** a database that competes with Neon or Aurora on database merits.
- **An S3-competitive blob service in the near term.** No S3 server API, per-tenant bucket plane, or object metadata plane exists; the object-store crate is a *consumer* of object storage, not a *server*. Blob is a late follow-on, not a headline face.
- **End-user auth and multi-tenant realtime before RLS-aware enforcement exists.** Shipping realtime while `FieldPredicate` is only advisory would be a data-leak-by-default across tenants. RLS-consistent enforcement across query-time and change-feed-time — off one JWT — is a hard prerequisite, not a fast-follow, and it is itself gated on the unbuilt Postgres compute.
