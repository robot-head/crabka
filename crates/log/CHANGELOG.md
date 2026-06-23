# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]
## [0.3.8] — 2026-06-23


### <!-- 1 -->🐛 Bug Fixes


- Faster broker failover — retry backoff + startupProbe ([#583](https://github.com/robot-head/crabka/pull/583))

- Seamless KRaft failover — voter hostname re-resolution, submit_change forwarding, SIGTERM controlled shutdown ([#591](https://github.com/robot-head/crabka/pull/591))

## [0.3.7] — 2026-06-17


### <!-- 0 -->🚀 Features


- Emit Cloud Logging-friendly structured JSON across services ([#508](https://github.com/robot-head/crabka/pull/508))

- KIP-534 log-compaction retention + fix control-batch dedup data-loss ([#528](https://github.com/robot-head/crabka/pull/528))


### <!-- 6 -->🧪 Testing


- Data-plane safety models — idempotent-producer + log-truncation (stateright + proptest) ([#524](https://github.com/robot-head/crabka/pull/524))

- Data-path composition model — end-to-end formal verification (HWM↔truncation↔failover↔visibility) ([#539](https://github.com/robot-head/crabka/pull/539))


### <!-- 7 -->⚙️ Miscellaneous Tasks


- De-hardcode release versions + sign/attest published Helm charts ([#530](https://github.com/robot-head/crabka/pull/530))

## [0.3.6] — 2026-06-13

## [0.3.5] — 2026-06-12

## [0.1.1] — 2026-05-29


### <!-- 0 -->🚀 Features


- KRaft metadata snapshots (KIP-630) ([#287](https://github.com/robot-head/crabka/pull/287))


### <!-- 10 -->💼 Other


- Use zerocopy for offset/time/txn index entries ([#202](https://github.com/robot-head/crabka/pull/202))


### <!-- 7 -->⚙️ Miscellaneous Tasks


- Release v0.1.0 ([#49](https://github.com/robot-head/crabka/pull/49))

- Release v0.1.0 ([#52](https://github.com/robot-head/crabka/pull/52))

- Expand microbenchmark coverage across crates ([#138](https://github.com/robot-head/crabka/pull/138))

## [0.1.0] — 2026-05-12


### <!-- 7 -->⚙️ Miscellaneous Tasks


- Release v0.1.0 ([#49](https://github.com/robot-head/crabka/pull/49))

