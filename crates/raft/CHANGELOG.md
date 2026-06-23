# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]
## [0.3.8] — 2026-06-23


### <!-- 1 -->🐛 Bug Fixes


- Faster broker failover — retry backoff + startupProbe ([#583](https://github.com/robot-head/crabka/pull/583))

- Seamless KRaft failover — voter hostname re-resolution, submit_change forwarding, SIGTERM controlled shutdown ([#591](https://github.com/robot-head/crabka/pull/591))


### <!-- 2 -->🚜 Refactor


- Migrate whole-function cargo-mutants exclusions to #[mutants::skip] ([#615](https://github.com/robot-head/crabka/pull/615))


### <!-- 6 -->🧪 Testing


- Kill engine-IO mutants with mockall seams, shrink mutants.toml ([#607](https://github.com/robot-head/crabka/pull/607))

## [0.3.7] — 2026-06-17


### <!-- 0 -->🚀 Features


- Emit Cloud Logging-friendly structured JSON across services ([#508](https://github.com/robot-head/crabka/pull/508))

- Serve DescribeCluster on the controller listener (KIP-919 Phase 2) ([#554](https://github.com/robot-head/crabka/pull/554))

- Interactive in-browser WASM consensus simulator ([#562](https://github.com/robot-head/crabka/pull/562))


### <!-- 1 -->🐛 Bug Fixes


- Clamp high-watermark monotonicity inside recompute_high_watermark ([#512](https://github.com/robot-head/crabka/pull/512))


### <!-- 3 -->📚 Documentation


- Diagrams, simulator-generated failure slideshow, and IA polish ([#556](https://github.com/robot-head/crabka/pull/556))


### <!-- 6 -->🧪 Testing


- Stateright consensus model + deterministic-sync test infra (Phase 1) ([#511](https://github.com/robot-head/crabka/pull/511))


### <!-- 7 -->⚙️ Miscellaneous Tasks


- De-hardcode release versions + sign/attest published Helm charts ([#530](https://github.com/robot-head/crabka/pull/530))

## [0.3.6] — 2026-06-13

## [0.3.5] — 2026-06-12


### <!-- 1 -->🐛 Bug Fixes

- KIP-996 pre-vote now interoperates with a real KIP-996 JVM voter: vote
  responses are matched to their round by the candidate's own role + epoch
  (as Kafka does), replacing a private `VoteResponse` tagged-field echo that a
  JVM peer never sends. A JVM voter's pre-vote grant is now counted, so a
  Crabka-led election no longer stalls in a mixed JVM+Crabka quorum.

- Outgoing `VoteRequest`s now address the recipient voter (`voterId` set to the
  target node id instead of `-1`), built per-recipient in `broadcast_vote`. A
  JVM voter previously rejected Crabka's (pre-)votes as not addressed to it; it
  now grants them. Validated end to end: a JVM voter grants a Crabka candidate's
  pre-vote and real vote in a contested mixed-quorum election.

## [0.1.1] — 2026-05-29


### <!-- 0 -->🚀 Features


- Migrate from bincode to wincode + serde-wincode ([#58](https://github.com/robot-head/crabka/pull/58))

- Expose real raft metrics in DescribeQuorum (KIP-595 follow-up) ([#264](https://github.com/robot-head/crabka/pull/264))

- KIP-853 dynamic KRaft quorum reconfiguration ([#290](https://github.com/robot-head/crabka/pull/290))

- Vertical role separation — process.roles + true observer metadata fetch ([#292](https://github.com/robot-head/crabka/pull/292))

- KRaft metadata snapshots (KIP-630) ([#287](https://github.com/robot-head/crabka/pull/287))

- Broker runtime metadata.version enforcement ([#307](https://github.com/robot-head/crabka/pull/307))


### <!-- 6 -->🧪 Testing


- Fix AddrInUse flake in raft_sasl via pre-bound controller listener ([#284](https://github.com/robot-head/crabka/pull/284))


### <!-- 7 -->⚙️ Miscellaneous Tasks


- Release v0.1.0 ([#52](https://github.com/robot-head/crabka/pull/52))

- Release v0.1.0 ([#59](https://github.com/robot-head/crabka/pull/59))

## [0.1.0] — 2026-05-13


### <!-- 0 -->🚀 Features


- Migrate from bincode to wincode + serde-wincode ([#58](https://github.com/robot-head/crabka/pull/58))


### <!-- 7 -->⚙️ Miscellaneous Tasks


- Release v0.1.0 ([#52](https://github.com/robot-head/crabka/pull/52))

