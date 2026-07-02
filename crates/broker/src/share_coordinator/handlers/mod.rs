//! KIP-932 share-state persister RPC handlers (api keys 83–87). Each handler
//! decodes the typed request, gates every `(topic, partition)` on
//! [`crate::share_coordinator::coordinator::ShareCoordinator::is_leader`] for
//! its state partition (returning per-partition `NOT_COORDINATOR` otherwise),
//! delegates to the matching coordinator method, and maps the result to a
//! per-partition `error_code`.
//!
//! These are inter-broker RPCs and carry no per-connection ACL context, so the
//! plain 4-arg [`crate::handlers::HandlerFn`] form fits (see
//! [`crate::txn::handlers::write_txn_markers`]).

pub(crate) mod delete;
pub(crate) mod initialize;
pub(crate) mod read;
pub(crate) mod read_summary;
pub(crate) mod write;

#[cfg(test)]
pub(crate) mod test_support {
    use std::{path::Path, sync::Arc};

    use crabka_log::{Log, LogConfig};

    use crate::{
        broker::{Broker, BrokerHandle},
        config::BrokerConfig,
        partition_registry::PartitionRegistry,
        share_coordinator::{
            bootstrap, config::ShareCoordinatorConfig, coordinator::ShareCoordinator,
            persistence::StateBatch,
        },
    };

    pub(crate) const VERSION: i16 = 0;

    pub(crate) fn batch(first_offset: i64, last_offset: i64) -> StateBatch {
        StateBatch {
            first_offset,
            last_offset,
            delivery_state: 2,
            delivery_count: 3,
        }
    }

    fn open_state_partition(registry: &PartitionRegistry, log_dir: &Path, partition: i32) {
        let part_dir = crate::log_dir::partition_dir(log_dir, bootstrap::TOPIC, partition);
        std::fs::create_dir_all(&part_dir).expect("create state partition dir");
        let log = Log::open(&part_dir, LogConfig::default()).expect("open state partition log");
        let part = crate::broker::spawn_partition(
            bootstrap::TOPIC.to_string(),
            partition,
            log_dir.to_path_buf(),
            log,
            crate::log_dir_status::LogDirRegistry::default(),
            Arc::new(crate::producer_state::ProducerState::new()),
        );
        registry.insert(bootstrap::TOPIC.to_string(), partition, part);
    }

    pub(crate) fn open_all_state_partitions(registry: &PartitionRegistry, log_dir: &Path) {
        for partition in 0..bootstrap::NUM_PARTITIONS {
            open_state_partition(registry, log_dir, partition);
        }
    }

    pub(crate) fn coordinator(log_dir: &Path) -> Arc<ShareCoordinator> {
        let registry = Arc::new(PartitionRegistry::new());
        open_all_state_partitions(&registry, log_dir);
        Arc::new(ShareCoordinator::new(
            1,
            registry,
            ShareCoordinatorConfig::default(),
        ))
    }

    pub(crate) async fn broker(dir: &Path) -> (BrokerHandle, Arc<crate::broker::Broker>) {
        let handle = Broker::start(BrokerConfig::for_tests(dir.to_path_buf()))
            .await
            .expect("start broker");
        let broker = handle.broker_arc_for_test();
        (handle, broker)
    }

    pub(crate) async fn broker_with_led_share_coordinator(
        dir: &Path,
    ) -> (BrokerHandle, Arc<crate::broker::Broker>) {
        let (handle, broker) = broker(dir).await;
        open_all_state_partitions(&broker.partitions, dir);
        broker
            .share_coordinator
            .lead_all_partitions_for_test()
            .await;
        (handle, broker)
    }
}
