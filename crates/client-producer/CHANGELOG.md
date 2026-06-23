# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]
## [0.3.8] — 2026-06-23


### <!-- 1 -->🐛 Bug Fixes


- Faster broker failover — retry backoff + startupProbe ([#583](https://github.com/robot-head/crabka/pull/583))


### <!-- 2 -->🚜 Refactor


- Migrate whole-function cargo-mutants exclusions to #[mutants::skip] ([#615](https://github.com/robot-head/crabka/pull/615))

## [0.3.7] — 2026-06-17


### <!-- 0 -->🚀 Features


- Emit Cloud Logging-friendly structured JSON across services ([#508](https://github.com/robot-head/crabka/pull/508))

- Pipeline produce requests with one in-flight per partition ([#564](https://github.com/robot-head/crabka/pull/564))

- Scaffold rules_rust client build + lean client_minimal facade ([#570](https://github.com/robot-head/crabka/pull/570))


### <!-- 7 -->⚙️ Miscellaneous Tasks


- De-hardcode release versions + sign/attest published Helm charts ([#530](https://github.com/robot-head/crabka/pull/530))

## [0.3.6] — 2026-06-13

## [0.3.5] — 2026-06-12

## [0.1.1] — 2026-05-29


### <!-- 0 -->🚀 Features


- Topic-backed RLMM loopback integration test + produce v13 fix (slice 48f) ([#282](https://github.com/robot-head/crabka/pull/282))


### <!-- 10 -->💼 Other


- Introduce RecordsPayload as the wire-field type for `records` ([#214](https://github.com/robot-head/crabka/pull/214))


### <!-- 7 -->⚙️ Miscellaneous Tasks


- Release v0.1.0 ([#49](https://github.com/robot-head/crabka/pull/49))

- Release v0.1.0 ([#52](https://github.com/robot-head/crabka/pull/52))

## [0.1.0] — 2026-05-12


### <!-- 7 -->⚙️ Miscellaneous Tasks


- Release v0.1.0 ([#49](https://github.com/robot-head/crabka/pull/49))

