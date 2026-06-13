# Changelog

All notable changes to `crabka-protocol` are documented here.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [0.3.6] — 2026-06-13


## [0.3.5] — 2026-06-12


### <!-- 1 -->🐛 Bug Fixes


- Format generated code with prettyplease before rustfmt ([#495](https://github.com/robot-head/crabka/pull/495))


### <!-- 2 -->🚜 Refactor


- Emit all generated code via quote! instead of writeln! ([#493](https://github.com/robot-head/crabka/pull/493))


## [0.1.1] — 2026-05-29


### <!-- 0 -->🚀 Features


- Add crate skeleton

- Add ProtocolError

- Add Encode/Decode traits

- Add fixed-width integer primitives

- Add varint/varlong/uvarint primitives

- Add string/bytes/compact primitives

- Add UUID primitive

- Add KIP-482 tagged-fields support

- Vendor Kafka 4.2.0 message schemas

- Hand-write owned::ApiVersionsRequest

- Hand-write owned::ApiVersionsResponse

- Wire generated ApiVersionsRequest into protocol crate

- Borrowed flavor for ApiVersionsRequest

- Borrowed-flavor codegen for ApiVersionsRequest

- IR-walking owned emitter (primitives only)

- Owned emitter supports primitive arrays

- IR-walking borrowed emitter

- Turn on generation for Metadata request/response

- Turn on generation for Produce request/response

- Turn on generation for OffsetCommit request/response

- Turn on generation for Request/Response headers

- CommonStructs emission + DescribeGroups support

- Central ApiKey enum

- KIP-966 offset-aware unclean leader recovery ([#294](https://github.com/robot-head/crabka/pull/294))


### <!-- 1 -->🐛 Bug Fixes


- Struct Default honors schema-level non-null defaults

- Use ..Default::default() in ApiVersionsResponse literal

- Honor nullableVersions upper bound ([#283](https://github.com/robot-head/crabka/pull/283))


### <!-- 10 -->💼 Other


- Fail build on schema/generated drift

- Introduce RecordsPayload as the wire-field type for `records` ([#214](https://github.com/robot-head/crabka/pull/214))


### <!-- 2 -->🚜 Refactor


- Share is_default helper via crabka-protocol


### <!-- 3 -->📚 Documentation


- Crate-level rustdoc and CONTRIBUTING


### <!-- 6 -->🧪 Testing


- JVM oracle subprocess wrapper

- Byte-equality differential vs JVM oracle for ApiVersions

- Proptest round-trip for ApiVersions request/response

- Corpus replay harness with ApiVersions sample

- Pick oracle wrapper by host OS, not file existence

- Spawn oracle via `sh` to avoid ENOEXEC


### <!-- 7 -->⚙️ Miscellaneous Tasks


- Rust + clippy + fmt matrix and JVM differential job

- Clean up clippy pedantic warnings

- Expand microbenchmark coverage across crates ([#138](https://github.com/robot-head/crabka/pull/138))


## [0.1.0] — 2026-05-11

### Added

- Wire protocol codec for Apache Kafka 4.2.0.
- Owned + borrowed flavors for every active Kafka 4.2 message (189
  message types across 604 supported `(api_key, version)` pairs).
- Typed `RecordBatch` v2 decoder/encoder with `zerocopy` header
  reinterpretation and `crabka-compression` integration.
- Central `ApiKey` enum listing every Kafka 4.2 API.
- Differential testing against `kafka-clients` 4.2.0 for every active
  `(api_key, version)` pair — all byte-equal.

### Supported Kafka versions

- Wire protocol: 4.2.0.

### MSRV

- Rust 1.95.0.
