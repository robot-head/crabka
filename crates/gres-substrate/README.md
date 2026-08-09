# crabka-gres-substrate

Substrate-backed durability for Crabka Gres tenant computes: WAL journaling to a per-tenant topic, fencing, and replay.

## Overview

This internal crate owns the G-2 substrate durability core. [`docs/superpowers/specs/2026-07-09-crabka-gres-g2-substrate-wal-design.md`](../../docs/superpowers/specs/2026-07-09-crabka-gres-g2-substrate-wal-design.md) describes that core. The crate supplies `GRW1` framing for engine write batches, merge-rule replay into the disposable read model, Kafka topic ensure, transactional WAL writes, and barrier recovery. The barrier recovery fences stale computes through Kafka transactions.

`crabka-gres --substrate-bootstrap 127.0.0.1:9092 --tenant <name>` uses the live Kafka-backed path. The binary loads the tenant's runtime defaults and SQL SCRAM verifier from `__gres_cfg.<tenant>` before it opens the substrate runtime. The substrate crate stays focused on WAL, fencing, replay, and checkpoint helpers. `memory://` stays available for in-process tests.
