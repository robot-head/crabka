# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]
## [0.4.0] — 2026-08-12


### <!-- 0 -->🚀 Features


- Finish actionable TODOs ([#783](https://github.com/robot-head/crabka/pull/783))

- Expose runtime configuration policy ([#904](https://github.com/robot-head/crabka/pull/904)) (**breaking**)


### <!-- 1 -->🐛 Bug Fixes


- Update rust crate pollster to v1 ([#787](https://github.com/robot-head/crabka/pull/787))

- Start removing clippy suppressions ([#784](https://github.com/robot-head/crabka/pull/784))

- Update rust crate turso to 0.7 ([#806](https://github.com/robot-head/crabka/pull/806))

- Update rust crate pollster to v1 ([#802](https://github.com/robot-head/crabka/pull/802))


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

- Adopt rs-clock and make sleep-based time/progress tests deterministic ([#725](https://github.com/robot-head/crabka/pull/725))

## [0.3.8] — 2026-06-23


### <!-- 1 -->🐛 Bug Fixes


- Faster broker failover — retry backoff + startupProbe ([#583](https://github.com/robot-head/crabka/pull/583))


### <!-- 2 -->🚜 Refactor


- Migrate whole-function cargo-mutants exclusions to #[mutants::skip] ([#615](https://github.com/robot-head/crabka/pull/615))


### <!-- 4 -->⚡ Performance


- Profiling-driven optimizations (codec decode, broker group-commit, streams changelog) ([#630](https://github.com/robot-head/crabka/pull/630))

## [0.3.7] — 2026-06-17


### <!-- 0 -->🚀 Features


- Emit Cloud Logging-friendly structured JSON across services ([#508](https://github.com/robot-head/crabka/pull/508))

- Columnar/dataframe support (polars, arrow-rs, columnar) ([#540](https://github.com/robot-head/crabka/pull/540))


### <!-- 3 -->📚 Documentation


- Data-formats guide + tested JSON→proto→arrow→polars→proto pipeline ([#544](https://github.com/robot-head/crabka/pull/544))


### <!-- 6 -->🧪 Testing


- De-flake coordinator tests + run only unit tests on macOS/windows matrix ([#519](https://github.com/robot-head/crabka/pull/519))


### <!-- 7 -->⚙️ Miscellaneous Tasks


- De-hardcode release versions + sign/attest published Helm charts ([#530](https://github.com/robot-head/crabka/pull/530))

## [0.3.6] — 2026-06-13


### <!-- 3 -->📚 Documentation


- Drop rustdoc hidden-line syntax from README examples ([#498](https://github.com/robot-head/crabka/pull/498))

## [0.3.5] — 2026-06-12


### <!-- 0 -->🚀 Features


- Sliding windows (KIP-450) ([#474](https://github.com/robot-head/crabka/pull/474))

- Cogroup (KIP-150) — full DSL surface ([#479](https://github.com/robot-head/crabka/pull/479))

- Native emit-final / EmitStrategy.onWindowClose (KIP-825) ([#480](https://github.com/robot-head/crabka/pull/480))

- Versioned KTables slice 1 (KIP-889/914) ([#481](https://github.com/robot-head/crabka/pull/481))

- Versioned KTables slice 2 — KIP-914 join half + KIP-923 grace ([#482](https://github.com/robot-head/crabka/pull/482))

- IQv2 Interactive Queries framework — slice 3a (KIP-796/806) ([#484](https://github.com/robot-head/crabka/pull/484))

- IQv2 versioned queries — slice 3b (KIP-960/968) ([#486](https://github.com/robot-head/crabka/pull/486))

- KTable.groupBy / KGroupedTable table aggregation ([#490](https://github.com/robot-head/crabka/pull/490))

- Record caching (statestore.cache.max.bytes) for KV/Window/Session stores ([#491](https://github.com/robot-head/crabka/pull/491))

- Record-cache parity for cogroup + to_table ([#492](https://github.com/robot-head/crabka/pull/492))


### <!-- 6 -->🧪 Testing


- Sliding-window reduce behavioral golden + aggregate/early-window/DSL-variant coverage (KIP-450) ([#475](https://github.com/robot-head/crabka/pull/475))

- IQv2 coverage — result/request/query accessors, driver failure paths, serve_iq2 gating ([#485](https://github.com/robot-head/crabka/pull/485))

