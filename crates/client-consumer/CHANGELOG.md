# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]
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

