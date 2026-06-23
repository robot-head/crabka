# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]
## [0.3.8] — 2026-06-23


### <!-- 0 -->🚀 Features


- S3-compatible RemoteStorageManager backend (KIP-405) ([#215](https://github.com/robot-head/crabka/pull/215))

- Authorized-operations bitfield on describe responses (KIP-430) ([#231](https://github.com/robot-head/crabka/pull/231))

- Validate KIP-511 client info and expose Prometheus counter ([#234](https://github.com/robot-head/crabka/pull/234))

- KIP-559 protocol_type/name on group-coordination responses ([#237](https://github.com/robot-head/crabka/pull/237))

- DescribeTopicPartitions admin API (KIP-966) ([#241](https://github.com/robot-head/crabka/pull/241))

- KIP-714 client telemetry handshake (no-op subscription) ([#246](https://github.com/robot-head/crabka/pull/246))

- DescribeProducers admin API (KIP-664) ([#252](https://github.com/robot-head/crabka/pull/252))

- ListTransactions + DescribeTransactions admin APIs (KIP-664) ([#255](https://github.com/robot-head/crabka/pull/255))

- UnregisterBroker admin API (KIP-185) ([#259](https://github.com/robot-head/crabka/pull/259))

- Expose SASL/GSSAPI (Kerberos) through broker config + operator CRDs ([#306](https://github.com/robot-head/crabka/pull/306))

- JVM byte-exact __remote_log_metadata records (resolves mixed-cluster metadata limitation) ([#467](https://github.com/robot-head/crabka/pull/467))

- Surface request-quota throttle in ThrottleTimeMs (KIP-219) ([#553](https://github.com/robot-head/crabka/pull/553))

- Loki-compatible logs observability backend (blockstore + LogQL + querier/distributor/compactor) ([#650](https://github.com/robot-head/crabka/pull/650))


### <!-- 10 -->💼 Other


- Streams Rebalance Protocol (broker-side), JVM-validated ([#366](https://github.com/robot-head/crabka/pull/366))

- Detect & handle log truncation (leader epoch in fetch) ([#365](https://github.com/robot-head/crabka/pull/365))

- Stale-epoch ISR fencing (broker epoch = registration commit offset) ([#372](https://github.com/robot-head/crabka/pull/372))

- Finish format/update features (crabka format --feature, JVM-validated) ([#370](https://github.com/robot-head/crabka/pull/370))

- Handle disk failure for JBOD (controller-side failover + self-shutdown) ([#374](https://github.com/robot-head/crabka/pull/374))

- Bidirectional live classic↔next-gen consumer-group migration (⚠️→✅) ([#376](https://github.com/robot-head/crabka/pull/376))


### <!-- 2 -->🚜 Refactor


- Move broker-dependent integration tests to dedicated workspace crate ([#447](https://github.com/robot-head/crabka/pull/447))


### <!-- 3 -->📚 Documentation


- Product-facing README with Kafka feature matrix ([#93](https://github.com/robot-head/crabka/pull/93))

- Refresh README status + add KIP implementation matrix

- Update README status; annotate plans with incomplete steps ([#223](https://github.com/robot-head/crabka/pull/223))

- Bump KIP-848 status to partial; refresh gaps ([#277](https://github.com/robot-head/crabka/pull/277))

- Refresh status for recently landed features ([#298](https://github.com/robot-head/crabka/pull/298))

- Graduate Crabka to beta, refresh KIP matrix, bump to 0.2.0 ([#361](https://github.com/robot-head/crabka/pull/361))

- Refresh benchmark results for v0.2.0 ([#363](https://github.com/robot-head/crabka/pull/363))

- Update project status documentation ([#457](https://github.com/robot-head/crabka/pull/457))

- Add exhaustive KIP matrix; fix UnregisterBroker KIP-185→919 mislabel ([#549](https://github.com/robot-head/crabka/pull/549))

- Refresh README and website docs ([#608](https://github.com/robot-head/crabka/pull/608))


### <!-- 6 -->🧪 Testing


- Close v0/v1 down-conversion coverage gaps ([#285](https://github.com/robot-head/crabka/pull/285))


### <!-- 7 -->⚙️ Miscellaneous Tasks


- Bootstrap workspace skeleton

