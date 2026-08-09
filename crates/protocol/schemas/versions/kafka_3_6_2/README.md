# kafka_3_6_2 schemas

This directory is vendored verbatim from [apache/kafka@3.6.2](https://github.com/apache/kafka/tree/3.6.2/clients/src/main/resources/common/message).

These schemas declare the pre-Kafka-4.0 version ranges for Produce
v0–9 and Fetch v0–15. The crabka codegen emits this directory into
the `kafka_3_6_2` namespace. The broker then decodes the
legacy-exclusive ranges Produce v0–2 and Fetch v0–3. The top-level
4.0 schemas no longer declare those ranges.

Do not hand-edit these schemas. To re-sync against a different
upstream tag, update `VERSION` and re-fetch with the commands in the
plan `2026-05-27-crabka-records-legacy-2bc.md`.
