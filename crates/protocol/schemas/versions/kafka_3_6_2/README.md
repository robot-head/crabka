# kafka_3_6_2 schemas

Vendored verbatim from [apache/kafka@3.6.2](https://github.com/apache/kafka/tree/3.6.2/clients/src/main/resources/common/message).

These schemas declare the pre-Kafka-4.0 version ranges for Produce
(v0–9) and Fetch (v0–15). The crabka codegen emits this directory
into the `kafka_3_6_2` namespace so the broker can decode the
legacy-exclusive ranges (Produce v0–2, Fetch v0–3) that the
top-level 4.0 schemas no longer declare.

Do not hand-edit. To re-sync against a different upstream tag,
update `VERSION` and re-fetch with the commands in the plan
`2026-05-27-crabka-records-legacy-2bc.md`.
