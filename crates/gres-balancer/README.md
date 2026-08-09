# crabka-gres-balancer

Internal dry-run foundation for Chapter Gres range balancing.

The crate models registry snapshots and range metrics. It plans in goal order, and emits split, move, merge, and conversion operations. It does not change the live registry. The first integration batch keeps the executor in dry-run mode only. Later batches will attach these operations to the G-8b range orchestrators and to the CLI and operator surfaces.
