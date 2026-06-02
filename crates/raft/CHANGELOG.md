# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### <!-- 1 -->🐛 Bug Fixes

- KIP-996 pre-vote now interoperates with a real KIP-996 JVM voter: vote
  responses are matched to their round by the candidate's own role + epoch
  (as Kafka does), replacing a private `VoteResponse` tagged-field echo that a
  JVM peer never sends. A JVM voter's pre-vote grant is now counted, so a
  Crabka-led election no longer stalls in a mixed JVM+Crabka quorum.

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

