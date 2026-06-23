# crabka-metrics

Prometheus/Grafana-Mimir-equivalent metrics backend for Crabka.

This crate starts with the metrics data layer: Arrow block schemas, native
histogram encoding, float samples, exemplars, and the remote_write v2 symbol
table. It also owns the distributor ingest path, WAL append wiring, and
compactor block/index writes. Query execution lives in `crabka-promql`.
