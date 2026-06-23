# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]
## [0.3.8] — 2026-06-23


### <!-- 1 -->🐛 Bug Fixes


- Faster broker failover — retry backoff + startupProbe ([#583](https://github.com/robot-head/crabka/pull/583))

## [0.3.7] — 2026-06-17


### <!-- 0 -->🚀 Features


- Emit Cloud Logging-friendly structured JSON across services ([#508](https://github.com/robot-head/crabka/pull/508))

- Native GCS RemoteStorageManager backend (keyless Workload Identity) ([#574](https://github.com/robot-head/crabka/pull/574))


### <!-- 6 -->🧪 Testing


- Kill cargo-mutants survivors in remote-storage ([#559](https://github.com/robot-head/crabka/pull/559))


### <!-- 7 -->⚙️ Miscellaneous Tasks


- De-hardcode release versions + sign/attest published Helm charts ([#530](https://github.com/robot-head/crabka/pull/530))

## [0.3.6] — 2026-06-13

## [0.3.5] — 2026-06-12

## [0.1.1] — 2026-05-29


### <!-- 0 -->🚀 Features


- S3-compatible RemoteStorageManager backend (KIP-405) ([#215](https://github.com/robot-head/crabka/pull/215))

- S3 multipart upload for large segments ([#224](https://github.com/robot-head/crabka/pull/224))

