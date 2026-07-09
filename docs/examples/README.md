# Examples

## Share-group backlog autoscaling

`keda-sharegroup-scaledobject.yaml` is an in-repo KEDA example for KIP-932
share-group consumers. It has no live-cluster dependency: the query uses the
broker `/metrics` endpoint and KEDA's stock Prometheus scaler.

Fleet validation still belongs in an operator environment. Before relying on it
for production scale-to-zero, run a multi-broker smoke test that verifies:

1. Prometheus scrapes every broker.
2. Only the current `__consumer_offsets-0` leader emits
   `crabka_broker_share_group_backlog` for a given share group.
3. `sum(crabka_broker_share_group_backlog{group_id="..."})` stays non-negative
   during coordinator handoff and returns to `0` after the group is deleted or
   fully drained.

The in-process broker tests cover the gauge, the local poller, RF=1
`ListOffsets(LATEST/EARLIEST)` semantics, stale-series removal, coordinator
self-gate clearing, label hygiene, and the remote `ListOffsets` offset-read seam.
A replicated external fleet should additionally confirm that the remote
`ListOffsets(LATEST)` value used by that seam matches the backlog end offset the
autoscaling policy expects for the deployed replication settings, and smoke-test
coordinator handoff plus share-group delete/tombstone cleanup with all broker
metrics endpoints scraped.
