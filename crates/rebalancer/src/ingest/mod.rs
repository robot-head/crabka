//! Periodic cluster-state snapshotter. Spawned by the binary entry;
//! writes the latest snapshot into an `ArcSwap<Option<ClusterState>>`
//! that the RPC handlers read.

pub mod admin_client;

use std::{sync::Arc, time::Duration};

use arc_swap::ArcSwap;
use crabka_client_core::Client;
use tokio_util::sync::CancellationToken;
use tracing::{debug, info, warn};

use crate::{
    metrics::RebalancerMetrics,
    model::{BrokerView, ClusterState, InFlightReassignment, PartitionView},
    time::now_ms,
};

pub type SharedSnapshot = Arc<ArcSwap<Option<ClusterState>>>;

#[must_use]
pub fn new_shared_snapshot() -> SharedSnapshot {
    Arc::new(ArcSwap::new(Arc::new(None)))
}

pub struct Ingester {
    client: Client,
    interval: Duration,
    snapshot: SharedSnapshot,
    shutdown: CancellationToken,
    metrics: RebalancerMetrics,
}

impl Ingester {
    #[must_use]
    pub fn new(
        client: Client,
        interval: Duration,
        snapshot: SharedSnapshot,
        shutdown: CancellationToken,
        metrics: RebalancerMetrics,
    ) -> Self {
        Self {
            client,
            interval,
            snapshot,
            shutdown,
            metrics,
        }
    }

    pub async fn run(self) {
        let mut ticker = tokio::time::interval(self.interval);
        // First tick fires immediately - snapshot once at startup before
        // sleeping.
        loop {
            tokio::select! {
                _ = ticker.tick() => {}
                () = self.shutdown.cancelled() => {
                    info!("ingester shutting down");
                    return;
                }
            }
            match snapshot_once(&self.client).await {
                Ok(state) => {
                    debug!(
                        brokers = state.brokers.len(),
                        partitions = state.partitions.len(),
                        "snapshot ok"
                    );
                    // Record observability before swapping the snapshot
                    // so a /metrics scrape that races a store-and-read
                    // sees the counter for the snapshot it is about to
                    // observe.
                    self.metrics.snapshot_at_ms.set(state.snapshot_at_ms);
                    self.metrics.snapshots_total.inc();
                    // Reflect the in-flight-reassignment backlog observed in
                    // this snapshot (gauge — the current level, not a rate).
                    self.metrics.set_pending_reassignments(
                        i64::try_from(state.in_flight_reassignments.len()).unwrap_or(i64::MAX),
                    );
                    self.snapshot.store(Arc::new(Some(state)));
                }
                Err(e) => {
                    warn!(error = %e, "snapshot tick failed; keeping prior state");
                }
            }
        }
    }
}

#[tracing::instrument(level = "info", skip_all, err)]
pub async fn snapshot_once(client: &Client) -> Result<ClusterState, anyhow::Error> {
    let md = admin_client::fetch_metadata(client).await?;
    let dc = admin_client::fetch_describe_cluster(client).await?;
    let lpr = admin_client::fetch_list_reassignments(client).await?;

    let brokers: Vec<BrokerView> = md
        .brokers
        .iter()
        .map(|b| BrokerView {
            id: b.node_id,
            host: b.host.clone(),
            port: b.port,
            rack: b.rack.clone(),
        })
        .collect();

    let mut partitions: Vec<PartitionView> = Vec::new();
    for t in &md.topics {
        let topic_name = t.name.clone().unwrap_or_default();
        for p in &t.partitions {
            partitions.push(PartitionView {
                topic: topic_name.clone(),
                partition: p.partition_index,
                replicas: p.replica_nodes.clone(),
                leader: p.leader_id,
                isr: p.isr_nodes.clone(),
            });
        }
    }

    let mut in_flight: Vec<InFlightReassignment> = Vec::new();
    for t in &lpr.topics {
        for p in &t.partitions {
            in_flight.push(InFlightReassignment {
                topic: t.name.clone(),
                partition: p.partition_index,
                adding: p.adding_replicas.clone(),
                removing: p.removing_replicas.clone(),
            });
        }
    }

    Ok(ClusterState {
        cluster_id: normalize_cluster_id(&dc.cluster_id),
        snapshot_at_ms: now_ms(),
        brokers,
        partitions,
        in_flight_reassignments: in_flight,
    })
}

fn normalize_cluster_id(cluster_id: &str) -> Option<String> {
    Some(cluster_id.to_string()).filter(|s| !s.is_empty())
}

#[cfg(test)]
mod tests {
    use assert2::assert;

    use super::*;

    #[test]
    fn snapshot_starts_as_none() {
        let s = new_shared_snapshot();
        let g = s.load();
        assert!(g.as_ref().is_none());
    }

    #[test]
    fn swap_replaces_value() {
        let s = new_shared_snapshot();
        let state = ClusterState {
            cluster_id: Some("c".into()),
            snapshot_at_ms: 42,
            brokers: vec![],
            partitions: vec![],
            in_flight_reassignments: vec![],
        };
        s.store(Arc::new(Some(state.clone())));
        let g = s.load();
        let inner: &Option<ClusterState> = &g;
        let v = inner.as_ref().expect("Some after swap");
        assert_eq!((v.snapshot_at_ms, v.cluster_id.as_deref()), (42, Some("c")));
    }

    #[test]
    fn normalize_cluster_id_drops_empty_ids_only() {
        for (name, input, expected) in [
            ("empty", "", None),
            ("named", "cluster-a", Some("cluster-a")),
        ] {
            assert_eq!(
                normalize_cluster_id(input).as_deref(),
                expected,
                "case {name}"
            );
        }
    }
}
