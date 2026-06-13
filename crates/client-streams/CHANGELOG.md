# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]
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

