# crabka-gres-balancer

Internal dry-run foundation for Chapter Gres range balancing.

The crate models registry snapshots plus range metrics, runs goal-ordered planning, and emits split, move, merge, and conversion operations without mutating the live registry. The first integration batch keeps executor behavior dry-run only; later batches will attach these operations to the G-8b range orchestrators and CLI/operator surfaces.
