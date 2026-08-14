# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]
## [0.4.1] — 2026-08-14

## [0.4.0] — 2026-08-12


### <!-- 0 -->🚀 Features


- Expose runtime configuration policy ([#904](https://github.com/robot-head/crabka/pull/904)) (**breaking**)

- Add gateway queue RPCs and backlog autoscaling ([#980](https://github.com/robot-head/crabka/pull/980))


### <!-- 1 -->🐛 Bug Fixes


- Start removing clippy suppressions ([#784](https://github.com/robot-head/crabka/pull/784))


### <!-- 3 -->📚 Documentation


- Rewrite all prose to ASD-STE100 Simplified Technical English ([#982](https://github.com/robot-head/crabka/pull/982))


### <!-- 6 -->🧪 Testing


- Cover log blockstore mutants ([#766](https://github.com/robot-head/crabka/pull/766))

## [0.3.9] — 2026-07-07


### <!-- 0 -->🚀 Features


- Cross-service demo traces + codebase-wide instrumentation ([#706](https://github.com/robot-head/crabka/pull/706))


### <!-- 2 -->🚜 Refactor


- Simplify shared helper logic ([#749](https://github.com/robot-head/crabka/pull/749))


### <!-- 6 -->🧪 Testing


- Harden broker mutant coverage ([#713](https://github.com/robot-head/crabka/pull/713))

## [0.3.8] — 2026-06-23


### <!-- 0 -->🚀 Features


- Geo-replication engine (Slice 1) — MirrorMaker-2 equivalent + data residency ([#593](https://github.com/robot-head/crabka/pull/593))

- Loki-compatible logs observability backend (blockstore + LogQL + querier/distributor/compactor) ([#650](https://github.com/robot-head/crabka/pull/650))


### <!-- 1 -->🐛 Bug Fixes


- Faster broker failover — retry backoff + startupProbe ([#583](https://github.com/robot-head/crabka/pull/583))


### <!-- 2 -->🚜 Refactor


- Migrate whole-function cargo-mutants exclusions to #[mutants::skip] ([#615](https://github.com/robot-head/crabka/pull/615))

## [0.3.7] — 2026-06-17


### <!-- 0 -->🚀 Features


- Emit Cloud Logging-friendly structured JSON across services ([#508](https://github.com/robot-head/crabka/pull/508))

- Scaffold rules_rust client build + lean client_minimal facade ([#570](https://github.com/robot-head/crabka/pull/570))


### <!-- 7 -->⚙️ Miscellaneous Tasks


- De-hardcode release versions + sign/attest published Helm charts ([#530](https://github.com/robot-head/crabka/pull/530))

## [0.3.6] — 2026-06-13

## [0.3.5] — 2026-06-12

## [0.1.1] — 2026-05-29


### <!-- 0 -->🚀 Features


- Enforce min.insync.replicas on Produce acks=-1 ([#251](https://github.com/robot-head/crabka/pull/251))


### <!-- 1 -->🐛 Bug Fixes


- Eliminate cooperative-sticky rebalance metadata races ([#286](https://github.com/robot-head/crabka/pull/286))

- Prompt close() + dedicated coordinator connection ([#293](https://github.com/robot-head/crabka/pull/293))

- Prime fetch offsets before publishing rebalance assignment ([#323](https://github.com/robot-head/crabka/pull/323))


### <!-- 10 -->💼 Other


- Introduce RecordsPayload as the wire-field type for `records` ([#214](https://github.com/robot-head/crabka/pull/214))

- Partitions_total + under_replicated_partitions gauges (slice 8b) ([#254](https://github.com/robot-head/crabka/pull/254))


### <!-- 7 -->⚙️ Miscellaneous Tasks


- Release v0.1.0 ([#49](https://github.com/robot-head/crabka/pull/49))

- Release v0.1.0 ([#52](https://github.com/robot-head/crabka/pull/52))

## [0.1.0] — 2026-05-12


### <!-- 7 -->⚙️ Miscellaneous Tasks


- Release v0.1.0 ([#49](https://github.com/robot-head/crabka/pull/49))

