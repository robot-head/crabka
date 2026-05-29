# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]
## [0.1.1] — 2026-05-29


### <!-- 0 -->🚀 Features


- S3 backend on Kafka.spec.tieredStorage (KIP-405) ([#228](https://github.com/robot-head/crabka/pull/228))

- Kafka.spec.tieredStorage.metadataManager for topic-backed RLMM ([#230](https://github.com/robot-head/crabka/pull/230))

- Kafka.spec.tieredStorage.persistence — PVC for local-tier dir ([#235](https://github.com/robot-head/crabka/pull/235))

- TieredStorageReady status condition (slice 48j) ([#238](https://github.com/robot-head/crabka/pull/238))

- Kafka.spec.tracing — OTLP env var surface (slice 42b) ([#248](https://github.com/robot-head/crabka/pull/248))

- KafkaUser SCRAM-SHA-256 authentication (slice 36b) ([#269](https://github.com/robot-head/crabka/pull/269))

- Expose SASL/GSSAPI (Kerberos) through broker config + operator CRDs ([#306](https://github.com/robot-head/crabka/pull/306))

- Gate node-pool pod creation on parent KafkaVersionValid ([#311](https://github.com/robot-head/crabka/pull/311))

- Broker runtime metadata.version enforcement ([#307](https://github.com/robot-head/crabka/pull/307))


### <!-- 10 -->💼 Other


- KafkaUser tls-external CRD variant

- Listener OAuth CRD variant

- Sample manifest (CRD regen deferred to T9)

- KafkaUser controller — TlsExternal arm

- Listener reconciler — validate + render OAUTHBEARER

- KafkaUser tls-external integration tests

- Listener OAuth integration tests

- Drop dead metadata.version injection into broker config ([#309](https://github.com/robot-head/crabka/pull/309))


### <!-- 6 -->🧪 Testing


- Implement slice-30 follow-up CA-reconcile tests ([#242](https://github.com/robot-head/crabka/pull/242))

- Hoist duplicated reconcile-test helpers into shared/mod.rs ([#310](https://github.com/robot-head/crabka/pull/310))


### <!-- 7 -->⚙️ Miscellaneous Tasks


- Robust image-tag detection + failure diagnostics ([#110](https://github.com/robot-head/crabka/pull/110))

