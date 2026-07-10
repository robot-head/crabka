# crabka-gres-substrate

Substrate-backed durability for Crabka Gres tenant computes: WAL journaling to a per-tenant topic, fencing, and replay.

## Overview

This internal crate owns the G-2 substrate durability core described in [`docs/superpowers/specs/2026-07-09-crabka-gres-g2-substrate-wal-design.md`](../../docs/superpowers/specs/2026-07-09-crabka-gres-g2-substrate-wal-design.md): `GRW1` framing for engine write batches, merge-rule replay into the disposable read model, Kafka topic ensure, transactional WAL writes, and barrier recovery that fences stale computes through Kafka transactions.

`crabka-gres --substrate-bootstrap 127.0.0.1:9092 --tenant <name>` uses the live Kafka-backed path. The binary now loads the tenant's runtime defaults and SQL SCRAM verifier from `__gres_cfg.<tenant>` before opening the substrate runtime; the substrate crate remains focused on WAL, fencing, replay, and checkpoint helpers. `memory://` is retained for in-process tests.
