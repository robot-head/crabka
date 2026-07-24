use kube::CustomResource;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

/// Crabka cluster spec. The spec carries only the version label;
/// broker pods are described by sibling `KafkaNodePool`s labeled
/// `crabka.io/cluster=<this name>`.
#[derive(CustomResource, Debug, Clone, Deserialize, Serialize, JsonSchema, PartialEq)]
#[kube(
    group = "crabka.io",
    version = "v1alpha1",
    kind = "Kafka",
    plural = "kafkas",
    singular = "kafka",
    shortname = "kk",
    namespaced,
    status = "KafkaStatus",
    derive = "PartialEq"
)]
#[serde(rename_all = "camelCase")]
pub struct KafkaSpec {
    /// Crabka version label, propagated to all pool pods via the
    /// `app.kubernetes.io/version` label.
    pub kafka_version: String,
    /// `KRaft` metadata version (the runtime analog of
    /// `inter.broker.protocol.version`). When unset, tracks
    /// `kafkaVersion`'s `major.minor`; when set, pins the metadata version
    /// for the safe two-step upgrade. Validated against `kafkaVersion` and
    /// the finalized `status.metadataVersion` — an invalid value
    /// surfaces `KafkaVersionValid=False` and blocks the roll.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub metadata_version: Option<String>,
    /// Opaque broker properties (`server.properties`-style key/value
    /// pairs). These are passed through to the broker's
    /// `[server_properties]` TOML table; the broker currently treats
    /// them as inert. Changes propagate through the config
    /// hash.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub config: Option<std::collections::BTreeMap<String, String>>,
    /// Named listeners. Empty (or absent) synthesizes a
    /// single internal `PLAIN` listener on port 9092.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub listeners: Vec<crate::crd::Listener>,
    /// Name of the listener used for inter-broker traffic.
    /// When `None`, the operator picks the first `internal` listener;
    /// when `listeners` is empty, the synthesized default `"PLAIN"`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub inter_broker_listener_name: Option<String>,
    /// Prometheus scrape configuration. When `None`, brokers do
    /// not bind `/metrics` and no `PodMonitor` / `ServiceMonitor` is
    /// rendered. When `Some`, the broker `StatefulSet` gains a `metrics`
    /// container port (TCP 9404) and the resources requested by
    /// `pod_monitor` / `service_monitor` are SSA-applied.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub metrics_config: Option<crate::crd::MetricsConfig>,
    /// Opt-in `NetworkPolicy` generation. When `None`, no
    /// `NetworkPolicy` is generated. When `Some` (even `{}`), the operator
    /// renders a cluster-level `NetworkPolicy` gating ingress to broker /
    /// controller pods.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub network_policy: Option<crate::crd::NetworkPolicySpec>,
    /// Per-cluster CA used for inter-broker mTLS + broker certs.
    /// Absent → fully-defaulted `CertificateAuthority` (operator-generated,
    /// 365/30 days).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub cluster_ca: Option<crate::crd::CertificateAuthority>,
    /// Per-cluster CA used to sign `KafkaUser` TLS certs.
    /// Absent → fully-defaulted `CertificateAuthority`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub clients_ca: Option<crate::crd::CertificateAuthority>,
    /// Broker log configuration. When `None`, brokers use their
    /// built-in default `RUST_LOG` filter. When `Some`, the operator
    /// composes (inline) or reads (external) a `tracing` env-filter string,
    /// renders it into the broker `ConfigMap` (`rust.log` key), wires it
    /// into each broker pod's `RUST_LOG` env, and rolls the cluster on
    /// change via the config hash.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub logging: Option<crate::crd::Logging>,
    /// Delegation-token master HMAC key source. When `None`,
    /// the broker rejects all KIP-48 delegation-token RPCs with err 61
    /// `DELEGATION_TOKEN_AUTH_DISABLED`. When `Some`, the operator
    /// injects `CRABKA_DELEGATION_TOKEN_SECRET_KEY` into each broker
    /// pod via a `valueFrom.secretKeyRef`, baking the key into the
    /// rendered `StatefulSet` so the SSA reconcile doesn't
    /// race with out-of-band `kubectl set env` patches.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub delegation_token: Option<DelegationTokenConfig>,
    /// Cluster-level authorizer selection. When `None`, the
    /// broker uses the default `AllowAll` authorizer (no ACL checks).
    /// When `Some`, the operator renders the `[authorization]` TOML
    /// section so the broker builds the matching `Arc<dyn Authorizer>`
    /// (`SimpleAclAuthorizer` for `type: simple`, `OpaAuthorizer` for
    /// `type: opa`). With `simple` or `opa` selected, the operator's
    /// inter-broker principal MUST appear in `super_users` (no implicit
    /// `ANONYMOUS` allow); operators opt in explicitly.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub authorization: Option<Authorization>,
    /// KIP-405: cluster-wide tiered storage. When `Some`,
    /// every broker pod boots with the local-tier RSM enabled, an
    /// `emptyDir` mounted at `/var/lib/crabka/remote` (the broker's
    /// `remote_log_storage_dir`), and `[remote_storage]` rendered in
    /// the broker TOML. Per-topic enablement is unchanged
    /// (`KafkaTopic.spec.config["remote.storage.enable"] = "true"`).
    ///
    /// The `emptyDir` default with `InmemoryRemoteLogMetadataManager`
    /// as the only RLMM means tier data does not survive pod restarts.
    /// PVC support pairs with the production RLMM.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub tiered_storage: Option<TieredStorage>,
    /// Inter-broker Kerberos initiate config. Required when
    /// `interBrokerListenerName` resolves to a `type: gssapi` listener;
    /// supplies the shared client principal + KDC. The keytab is reused
    /// from that listener's `keytabSecretRef`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub inter_broker_kerberos: Option<InterBrokerKerberos>,
    /// Optional process-wide `krb5.conf`. Mounted into broker pods and
    /// pointed at via `KRB5_CONFIG`; serves both accept and initiate paths.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub krb5_conf_secret_ref: Option<Krb5ConfSecretRef>,
    /// Distributed-tracing wiring for the broker pods. When
    /// `Some`, the operator renders the matching `CRABKA_OTLP_*` env
    /// vars onto every broker pod — the broker's telemetry
    /// pipeline reads them via `TelemetryConfig::from_env` and
    /// installs the OTLP tracer at startup. When `None`, no OTLP env
    /// vars are emitted and the broker leaves tracing off (the
    /// default).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub tracing: Option<Tracing>,
    /// Validated broker operational policy rendered into `[runtime]`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub broker_tuning: Option<BrokerTuning>,
}

macro_rules! validate_tuning_field {
    (refined, $owner:ident, $field:ident, $rule:ty) => {
        if let Some(value) = $owner.$field {
            <$rule>::new(value)
                .map_err(|error| BrokerTuning::invalid(stringify!($field), error))?;
        }
    };
    (plain, $owner:ident, $field:ident, $rule:ty) => {};
    (string, $owner:ident, $field:ident, $rule:ty) => {};
}

macro_rules! render_tuning_field {
    (refined, $owner:ident, $out:ident, $field:ident) => {
        if let Some(value) = $owner.$field {
            use std::fmt::Write as _;
            let _ = writeln!($out, "{} = {value}", stringify!($field));
        }
    };
    (plain, $owner:ident, $out:ident, $field:ident) => {
        if let Some(value) = $owner.$field {
            use std::fmt::Write as _;
            let _ = writeln!($out, "{} = {value}", stringify!($field));
        }
    };
    (string, $owner:ident, $out:ident, $field:ident) => {
        if let Some(value) = &$owner.$field {
            use std::fmt::Write as _;
            let _ = writeln!(
                $out,
                "{} = {}",
                stringify!($field),
                toml::Value::String(value.clone())
            );
        }
    };
}

macro_rules! define_broker_tuning {
    ($(
        $kind:ident
        $(#[$meta:meta])*
        $field:ident: $ty:ty => $rule:ty;
    )*) => {
        /// Typed Kafka CRD surface for broker `[runtime]` policy.
        #[derive(Debug, Clone, Default, Deserialize, Serialize, JsonSchema, PartialEq, Eq)]
        #[serde(rename_all = "camelCase", deny_unknown_fields)]
        pub struct BrokerTuning {
            $(
                $(#[$meta])*
                #[serde(default, skip_serializing_if = "Option::is_none")]
                pub $field: Option<$ty>,
            )*
        }

        impl BrokerTuning {
            /// Validate scalar and relational runtime constraints.
            ///
            /// # Errors
            ///
            /// Returns the invalid camel-case CRD path.
            pub fn validate(&self) -> Result<(), String> {
                $(validate_tuning_field!($kind, self, $field, $rule);)*
                self.validate_strings()?;
                self.validate_relations()
            }

            pub(crate) fn render_runtime_toml(&self) -> String {
                let mut values = String::new();
                $(render_tuning_field!($kind, self, values, $field);)*
                if values.is_empty() {
                    String::new()
                } else {
                    format!("[runtime]\n{values}\n")
                }
            }
        }
    };
}

define_broker_tuning! {
    refined #[schemars(range(min = 1))] startup_leader_wait_timeout_ms: u64 => refined_type::rule::GreaterU64<0>;
    refined #[schemars(range(min = 1))] self_registration_backoff_min_ms: u64 => refined_type::rule::GreaterU64<0>;
    refined #[schemars(range(min = 1))] self_registration_backoff_max_ms: u64 => refined_type::rule::GreaterU64<0>;
    refined #[schemars(range(min = 1))] observer_poll_interval_ms: u64 => refined_type::rule::GreaterU64<0>;
    refined #[schemars(range(min = 1))] audit_spool_replay_interval_ms: u64 => refined_type::rule::GreaterU64<0>;
    refined #[schemars(range(min = 1))] audit_stats_poll_interval_ms: u64 => refined_type::rule::GreaterU64<0>;
    refined #[schemars(range(min = 1))] audit_partition_wait_timeout_ms: u64 => refined_type::rule::GreaterU64<0>;
    refined #[schemars(range(min = 1))] liveness_tick_interval_ms: u64 => refined_type::rule::GreaterU64<0>;
    refined #[schemars(range(min = 1))] gauge_poll_interval_ms: u64 => refined_type::rule::GreaterU64<0>;
    refined #[schemars(range(min = 1))] cleaner_interval_ms: u64 => refined_type::rule::GreaterU64<0>;
    refined #[schemars(range(min = 1))] isr_scan_interval_ms: u64 => refined_type::rule::GreaterU64<0>;
    refined #[schemars(range(min = 1))] future_log_move_retry_backoff_ms: u64 => refined_type::rule::GreaterU64<0>;
    refined #[schemars(range(min = 1))] client_metrics_eviction_tick_ms: u64 => refined_type::rule::GreaterU64<0>;
    refined #[schemars(range(min = 1))] client_metrics_stale_floor_ms: u64 => refined_type::rule::GreaterU64<0>;
    refined #[schemars(range(min = 1))] client_metrics_default_interval_ms: i32 => refined_type::rule::GreaterI32<0>;
    refined #[schemars(range(min = 1))] client_metrics_telemetry_max_bytes: i32 => refined_type::rule::GreaterI32<0>;
    refined #[schemars(range(min = 1))] client_metrics_prom_snapshot_ttl_ms: u64 => refined_type::rule::GreaterU64<0>;
    refined #[schemars(range(min = 1))] rlmm_reconcile_tick_ms: u64 => refined_type::rule::GreaterU64<0>;
    refined #[schemars(range(min = 1))] rlmm_bootstrap_backoff_initial_ms: u64 => refined_type::rule::GreaterU64<0>;
    refined #[schemars(range(min = 1))] rlmm_bootstrap_backoff_max_ms: u64 => refined_type::rule::GreaterU64<0>;
    refined #[schemars(range(min = 1))] connection_creation_throttle_max_ms: u64 => refined_type::rule::GreaterU64<0>;
    refined #[schemars(range(min = 1))] opa_http_timeout_ms: u64 => refined_type::rule::GreaterU64<0>;
    refined #[schemars(range(min = 1))] oauth_jwks_http_timeout_ms: u64 => refined_type::rule::GreaterU64<0>;
    refined #[schemars(range(min = 1))] auto_join_retry_backoff_ms: u64 => refined_type::rule::GreaterU64<0>;
    refined #[schemars(range(min = 1, max = 2_147_483_647))] auto_join_voter_request_timeout_ms: u64 => refined_type::rule::MinMaxU64<1, 2_147_483_647>;
    refined #[schemars(range(min = 1))] replication_fetch_max_bytes: i32 => refined_type::rule::GreaterI32<0>;
    refined #[schemars(range(min = 1))] replication_fetch_max_wait_ms: i32 => refined_type::rule::GreaterI32<0>;
    refined #[schemars(range(min = 1))] replication_fetch_min_bytes: i32 => refined_type::rule::GreaterI32<0>;
    refined #[schemars(range(min = 1))] replication_throttle_exhausted_backoff_ms: u64 => refined_type::rule::GreaterU64<0>;
    refined #[schemars(range(min = 1))] replication_send_error_backoff_ms: u64 => refined_type::rule::GreaterU64<0>;
    refined #[schemars(range(min = 1))] replication_unknown_topic_retry_delay_ms: u64 => refined_type::rule::GreaterU64<0>;
    refined #[schemars(range(min = 1))] replication_epoch_fence_backoff_ms: u64 => refined_type::rule::GreaterU64<0>;
    refined #[schemars(range(min = 1))] replication_unexpected_error_backoff_ms: u64 => refined_type::rule::GreaterU64<0>;
    refined #[schemars(range(min = 1))] replication_reconnect_initial_delay_ms: u64 => refined_type::rule::GreaterU64<0>;
    refined #[schemars(range(min = 1))] replication_reconnect_delay_cap_ms: u64 => refined_type::rule::GreaterU64<0>;
    refined #[schemars(range(min = 1))] coordinator_session_expiry_tick_ms: u64 => refined_type::rule::GreaterU64<0>;
    refined #[schemars(range(min = 1))] coordinator_shutdown_ack_timeout_ms: u64 => refined_type::rule::GreaterU64<0>;
    refined #[schemars(range(min = 1))] consumer_group_session_timeout_ms: u64 => refined_type::rule::GreaterU64<0>;
    refined #[schemars(range(min = 1))] consumer_group_heartbeat_interval_ms: u64 => refined_type::rule::GreaterU64<0>;
    refined #[schemars(range(min = 1))] consumer_group_min_session_timeout_ms: u64 => refined_type::rule::GreaterU64<0>;
    refined #[schemars(range(min = 1))] consumer_group_max_session_timeout_ms: u64 => refined_type::rule::GreaterU64<0>;
    refined #[schemars(range(min = 1))] consumer_group_min_heartbeat_interval_ms: u64 => refined_type::rule::GreaterU64<0>;
    refined #[schemars(range(min = 1))] consumer_group_max_heartbeat_interval_ms: u64 => refined_type::rule::GreaterU64<0>;
    refined #[schemars(range(min = 1))] consumer_group_max_size: usize => refined_type::rule::GreaterUsize<0>;
    refined #[schemars(range(min = 1))] classic_group_initial_rebalance_delay_ms: u64 => refined_type::rule::GreaterU64<0>;
    refined #[schemars(range(min = 1))] sync_group_follower_wait_ms: u64 => refined_type::rule::GreaterU64<0>;
    refined #[schemars(range(min = 1))] unclean_recovery_aggressive_deadline_ms: u64 => refined_type::rule::GreaterU64<0>;
    refined #[schemars(range(min = 1))] unclean_recovery_balanced_deadline_ms: u64 => refined_type::rule::GreaterU64<0>;
    refined #[schemars(range(min = 1))] operator_recovery_deadline_ms: u64 => refined_type::rule::GreaterU64<0>;
    refined #[schemars(range(min = 1))] quota_throttle_max_ms: u64 => refined_type::rule::GreaterU64<0>;
    refined #[schemars(range(min = 1))] self_registration_max_attempts: u32 => refined_type::rule::GreaterU32<0>;
    refined #[schemars(range(min = 1))] observer_fetch_max_bytes: u32 => refined_type::rule::GreaterU32<0>;
    refined #[schemars(range(min = 1))] audit_event_queue_capacity: usize => refined_type::rule::GreaterUsize<0>;
    refined #[schemars(range(min = 1))] audit_tail_window_offsets: i64 => refined_type::rule::GreaterI64<0>;
    refined #[schemars(range(min = 1))] audit_tail_read_max_bytes: usize => refined_type::rule::GreaterUsize<0>;
    refined #[schemars(range(min = 1))] offsets_topic_metadata_wait_timeout_ms: u64 => refined_type::rule::GreaterU64<0>;
    refined #[schemars(range(min = 1))] client_metrics_stale_push_intervals: u32 => refined_type::rule::GreaterU32<0>;
    refined #[schemars(range(min = 1))] client_metrics_otlp_queue_capacity: usize => refined_type::rule::GreaterUsize<0>;
    refined #[schemars(range(min = 1))] coordinator_actor_mailbox_capacity: usize => refined_type::rule::GreaterUsize<0>;
    refined #[schemars(range(min = 1))] unclean_recovery_queue_capacity: usize => refined_type::rule::GreaterUsize<0>;
    refined #[schemars(range(min = 1))] share_recovery_read_max_bytes: usize => refined_type::rule::GreaterUsize<0>;
    refined #[schemars(range(min = 1))] share_session_cache_max_when_unlimited: usize => refined_type::rule::GreaterUsize<0>;
    refined #[schemars(range(min = 1, max = 4_294_967_295_u64))] socket_request_max_bytes: usize => refined_type::rule::MinMaxUsize<1, 4_294_967_295>;
    refined #[schemars(range(min = 1))] sendfile_min_bytes: usize => refined_type::rule::GreaterUsize<0>;
    refined #[schemars(range(min = 1))] socket_send_buffer_bytes: usize => refined_type::rule::GreaterUsize<0>;
    refined #[schemars(range(min = 1))] socket_receive_buffer_bytes: usize => refined_type::rule::GreaterUsize<0>;
    refined #[schemars(range(min = 1))] acl_max_principal_bytes: usize => refined_type::rule::GreaterUsize<0>;
    refined #[schemars(range(min = 1))] acl_max_resource_name_bytes: usize => refined_type::rule::GreaterUsize<0>;
    refined #[schemars(range(min = 1))] telemetry_max_decompression_ratio: usize => refined_type::rule::GreaterUsize<0>;
    refined #[schemars(range(min = 1))] telemetry_decompressed_output_floor_bytes: usize => refined_type::rule::GreaterUsize<0>;
    refined #[schemars(range(min = 1))] telemetry_decompressed_output_ceiling_bytes: usize => refined_type::rule::GreaterUsize<0>;
    string #[schemars(length(min = 1))] inter_broker_server_name: String => ();
    refined #[schemars(range(min = 1))] producer_id_expiration_ms: i64 => refined_type::rule::GreaterI64<0>;
    refined #[schemars(range(min = 1))] producer_id_expiration_scan_interval_ms: u64 => refined_type::rule::GreaterU64<0>;
    refined #[schemars(range(min = 1))] max_produce_group: usize => refined_type::rule::GreaterUsize<0>;
    refined #[schemars(range(min = 1))] partition_writer_queue_depth: usize => refined_type::rule::GreaterUsize<0>;
    refined #[schemars(range(min = 1))] default_min_insync_replicas: i32 => refined_type::rule::GreaterI32<0>;
    refined #[schemars(range(min = 1))] future_log_move_read_chunk_bytes: usize => refined_type::rule::GreaterUsize<0>;
    refined #[schemars(range(min = 1))] share_state_num_partitions: i32 => refined_type::rule::GreaterI32<0>;
    refined #[schemars(range(min = 1))] share_state_replication_factor: i16 => refined_type::rule::GreaterI16<0>;
    refined #[schemars(range(min = 1))] transaction_state_num_partitions: i32 => refined_type::rule::GreaterI32<0>;
    refined #[schemars(range(min = 1))] transaction_state_replication_factor: i16 => refined_type::rule::GreaterI16<0>;
    refined #[schemars(range(min = 1))] transaction_min_timeout_ms: i32 => refined_type::rule::GreaterI32<0>;
    refined #[schemars(range(min = 1, max = 2_147_483_646))] transaction_max_timeout_ms: i32 => refined_type::rule::MinMaxI32<1, 2_147_483_646>;
    plain partition_disk_scan_interval_secs: u64 => ();
    plain observer_lag_bound: u64 => ();
    refined #[schemars(range(min = 1))] heartbeat_interval_ms: u64 => refined_type::rule::GreaterU64<0>;
    refined #[schemars(range(min = 1))] heartbeat_timeout_ms: u64 => refined_type::rule::GreaterU64<0>;
    refined #[schemars(range(min = 1))] replica_lag_time_max_ms: u64 => refined_type::rule::GreaterU64<0>;
    refined #[schemars(range(min = 1))] controller_election_timeout_ms: u64 => refined_type::rule::GreaterU64<0>;
    refined #[schemars(range(min = 1))] controller_heartbeat_interval_ms: u64 => refined_type::rule::GreaterU64<0>;
    refined #[schemars(range(min = 1))] controlled_shutdown_drain_timeout_ms: u64 => refined_type::rule::GreaterU64<0>;
    refined #[schemars(range(min = 1))] metadata_max_bytes_between_snapshots: u64 => refined_type::rule::GreaterU64<0>;
    plain metadata_max_snapshot_interval_ms: u64 => ();
    refined #[schemars(range(min = 1))] metadata_snapshot_interval_records: u64 => refined_type::rule::GreaterU64<0>;
    plain txn_abort_cleanup_interval_ms: u64 => ();
    refined #[schemars(range(min = 1))] leader_imbalance_check_interval_secs: u64 => refined_type::rule::GreaterU64<0>;
    refined #[schemars(range(min = 0, max = 100))] leader_imbalance_per_broker_percentage: u32 => refined_type::rule::MinMaxU32<0, 100>;
    plain tls_reload_interval_ms: u64 => ();
    plain max_incremental_fetch_session_cache_slots: usize => ();
    plain max_connections: usize => ();
    plain max_connections_per_ip: usize => ();
    refined #[schemars(range(min = 1))] delegation_token_max_lifetime_ms: i64 => refined_type::rule::GreaterI64<0>;
    refined #[schemars(range(min = 1))] delegation_token_expiry_check_interval_ms: i64 => refined_type::rule::GreaterI64<0>;
    refined #[schemars(range(min = 1))] delegation_token_default_renew_period_ms: i64 => refined_type::rule::GreaterI64<0>;
    refined #[schemars(range(min = 1))] remote_log_manager_interval_ms: u64 => refined_type::rule::GreaterU64<0>;
    plain share_group_enable: bool => ();
    refined #[schemars(range(min = 1))] share_group_session_timeout_ms: u64 => refined_type::rule::GreaterU64<0>;
    refined #[schemars(range(min = 1))] share_group_heartbeat_interval_ms: u64 => refined_type::rule::GreaterU64<0>;
    refined #[schemars(range(min = 1))] share_group_record_lock_duration_ms: u64 => refined_type::rule::GreaterU64<0>;
    refined #[schemars(range(min = 1))] share_group_max_delivery_attempts: i16 => refined_type::rule::GreaterI16<0>;
    refined #[schemars(range(min = 1))] share_group_max_inflight_records: i32 => refined_type::rule::GreaterI32<0>;
    string share_group_isolation_level: String => ();
    refined #[schemars(range(min = 1))] streams_group_session_timeout_ms: u64 => refined_type::rule::GreaterU64<0>;
    refined #[schemars(range(min = 1))] streams_group_heartbeat_interval_ms: u64 => refined_type::rule::GreaterU64<0>;
    refined #[schemars(range(min = 1))] streams_internal_topic_replication_factor: i16 => refined_type::rule::GreaterI16<0>;
    refined #[schemars(range(min = 0))] streams_group_num_standby_replicas: i32 => refined_type::rule::GreaterEqualI32<0>;
    refined #[schemars(range(min = 0))] streams_group_num_warmup_replicas: i32 => refined_type::rule::GreaterEqualI32<0>;
    refined #[schemars(range(min = 0))] streams_group_acceptable_recovery_lag: i64 => refined_type::rule::GreaterEqualI64<0>;
    refined #[schemars(range(min = 1))] streams_group_task_offset_interval_ms: u64 => refined_type::rule::GreaterU64<0>;
    string streams_group_assignor: String => ();
}

impl BrokerTuning {
    fn camel_case(field: &str) -> String {
        let mut parts = field.split('_');
        let mut result = parts.next().unwrap_or_default().to_owned();
        for part in parts {
            let mut chars = part.chars();
            if let Some(first) = chars.next() {
                result.push(first.to_ascii_uppercase());
                result.extend(chars);
            }
        }
        result
    }

    fn path(field: &str) -> String {
        format!("spec.brokerTuning.{}", Self::camel_case(field))
    }

    fn invalid(field: &str, error: impl std::fmt::Display) -> String {
        format!("{}: {error}", Self::path(field))
    }

    fn invalid_relation(left: &str, right: &str, message: &str) -> String {
        format!("{} and {}: {message}", Self::path(left), Self::path(right))
    }

    fn validate_strings(&self) -> Result<(), String> {
        if let Some(value) = &self.inter_broker_server_name {
            refined_type::rule::NonEmptyString::new(value.clone())
                .map_err(|error| Self::invalid("inter_broker_server_name", error))?;
        }
        if let Some(value) = &self.share_group_isolation_level
            && !matches!(value.as_str(), "read-uncommitted" | "read-committed")
        {
            return Err(Self::invalid(
                "share_group_isolation_level",
                "expected `read-uncommitted` or `read-committed`",
            ));
        }
        if let Some(value) = &self.streams_group_assignor
            && !matches!(value.as_str(), "auto" | "sticky" | "highly-available")
        {
            return Err(Self::invalid(
                "streams_group_assignor",
                "expected `auto`, `sticky`, or `highly-available`",
            ));
        }
        Ok(())
    }

    fn validate_relations(&self) -> Result<(), String> {
        macro_rules! ordered {
            ($left:ident, $left_default:expr, <=, $right:ident, $right_default:expr) => {
                if self.$left.unwrap_or($left_default) > self.$right.unwrap_or($right_default) {
                    return Err(Self::invalid_relation(
                        stringify!($left),
                        stringify!($right),
                        "minimum or initial value exceeds maximum",
                    ));
                }
            };
            ($left:ident, $left_default:expr, <, $right:ident, $right_default:expr) => {
                if self.$left.unwrap_or($left_default) >= self.$right.unwrap_or($right_default) {
                    return Err(Self::invalid_relation(
                        stringify!($left),
                        stringify!($right),
                        "left value must be below right value",
                    ));
                }
            };
        }
        macro_rules! bounded {
            (
                $value:ident, $value_default:expr,
                $min:ident, $min_default:expr,
                $max:ident, $max_default:expr
            ) => {{
                let value = self.$value.unwrap_or($value_default);
                let min = self.$min.unwrap_or($min_default);
                let max = self.$max.unwrap_or($max_default);
                if !(min..=max).contains(&value) {
                    return Err(format!(
                        "{} must be within {} and {}",
                        Self::path(stringify!($value)),
                        Self::path(stringify!($min)),
                        Self::path(stringify!($max))
                    ));
                }
            }};
        }

        ordered!(
            self_registration_backoff_min_ms,
            100,
            <=,
            self_registration_backoff_max_ms,
            5_000
        );
        ordered!(
            rlmm_bootstrap_backoff_initial_ms,
            250,
            <=,
            rlmm_bootstrap_backoff_max_ms,
            10_000
        );
        ordered!(
            replication_fetch_min_bytes,
            1,
            <=,
            replication_fetch_max_bytes,
            1_048_576
        );
        ordered!(
            replication_reconnect_initial_delay_ms,
            100,
            <=,
            replication_reconnect_delay_cap_ms,
            5_000
        );
        ordered!(
            heartbeat_interval_ms,
            3_000,
            <,
            heartbeat_timeout_ms,
            9_000
        );
        ordered!(
            controller_heartbeat_interval_ms,
            500,
            <,
            controller_election_timeout_ms,
            5_000
        );
        ordered!(
            delegation_token_default_renew_period_ms,
            86_400_000,
            <=,
            delegation_token_max_lifetime_ms,
            604_800_000
        );
        ordered!(
            client_metrics_eviction_tick_ms,
            60_000,
            <=,
            client_metrics_stale_floor_ms,
            600_000
        );
        ordered!(
            unclean_recovery_aggressive_deadline_ms,
            2_000,
            <=,
            unclean_recovery_balanced_deadline_ms,
            30_000
        );
        ordered!(
            telemetry_decompressed_output_floor_bytes,
            16_777_216,
            <=,
            telemetry_decompressed_output_ceiling_bytes,
            1_073_741_824
        );
        ordered!(
            transaction_min_timeout_ms,
            1_000,
            <,
            transaction_max_timeout_ms,
            900_000
        );

        ordered!(
            consumer_group_min_session_timeout_ms,
            45_000,
            <=,
            consumer_group_max_session_timeout_ms,
            60_000
        );
        bounded!(
            consumer_group_session_timeout_ms,
            45_000,
            consumer_group_min_session_timeout_ms,
            45_000,
            consumer_group_max_session_timeout_ms,
            60_000
        );
        ordered!(
            consumer_group_min_heartbeat_interval_ms,
            5_000,
            <=,
            consumer_group_max_heartbeat_interval_ms,
            15_000
        );
        bounded!(
            consumer_group_heartbeat_interval_ms,
            5_000,
            consumer_group_min_heartbeat_interval_ms,
            5_000,
            consumer_group_max_heartbeat_interval_ms,
            15_000
        );

        if !(45_000..=60_000).contains(&self.share_group_session_timeout_ms.unwrap_or(45_000)) {
            return Err(Self::invalid(
                "share_group_session_timeout_ms",
                "must be within 45000..=60000",
            ));
        }
        if !(5_000..=15_000).contains(&self.share_group_heartbeat_interval_ms.unwrap_or(5_000)) {
            return Err(Self::invalid(
                "share_group_heartbeat_interval_ms",
                "must be within 5000..=15000",
            ));
        }
        if !(45_000..=60_000).contains(&self.streams_group_session_timeout_ms.unwrap_or(45_000)) {
            return Err(Self::invalid(
                "streams_group_session_timeout_ms",
                "must be within 45000..=60000",
            ));
        }
        if !(5_000..=15_000).contains(&self.streams_group_heartbeat_interval_ms.unwrap_or(5_000)) {
            return Err(Self::invalid(
                "streams_group_heartbeat_interval_ms",
                "must be within 5000..=15000",
            ));
        }
        Ok(())
    }
}

/// Inter-broker GSSAPI initiate config. Single shared client principal
/// cluster-wide (no per-broker host-templated SPNs).
#[derive(Debug, Clone, Deserialize, Serialize, JsonSchema, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct InterBrokerKerberos {
    /// Principal every broker authenticates as when dialing peers, e.g.
    /// `kafka@EXAMPLE.COM`. Must exist in the shared keytab.
    pub client_principal: String,
    /// Target SPN primary. Defaults to `kafka`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub service_name: Option<String>,
    /// KDC endpoint, e.g. `tcp://kdc:88`.
    pub kdc_url: String,
}

/// Reference to a Secret holding a `krb5.conf`.
#[derive(Debug, Clone, Deserialize, Serialize, JsonSchema, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct Krb5ConfSecretRef {
    /// Name of the Secret holding the krb5.conf.
    pub secret_name: String,
    /// Key within the Secret whose value is the krb5.conf contents.
    pub key: String,
}

/// KIP-405: cluster-wide tiered-storage configuration.
///
/// The `type` discriminator picks the backend; per-backend tuning lives
/// in the matching sibling field (`s3` for `Type = S3`, `gcs` for
/// `Type = Gcs`, no extra field for `Local`). Mis-pairings — `type = "S3"`
/// without `spec.s3`, `type = "Gcs"` without `spec.gcs`, or
/// `type = "Local"` with `spec.s3` / `spec.gcs` set — are rejected by the
/// operator reconciler with a `TieredStorageInvalid` status condition.
#[derive(Debug, Clone, Default, Deserialize, Serialize, JsonSchema, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct TieredStorage {
    /// Backend kind selector.
    #[serde(rename = "type")]
    pub kind: TieredStorageType,
    /// S3-backend tuning. Required when `kind == S3`, must be absent
    /// otherwise. The struct mirrors `crabka_remote_storage::S3Config`
    /// — non-credential fields are rendered verbatim into the broker
    /// TOML's `[remote_storage.s3]` block; credentials are sourced
    /// from Kubernetes Secrets and injected as broker-pod env
    /// (`AWS_ACCESS_KEY_ID` / `AWS_SECRET_ACCESS_KEY`).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub s3: Option<S3StorageSpec>,
    /// GCS-backend tuning. Required when `kind == Gcs`, must be absent
    /// otherwise. The struct mirrors `crabka_remote_storage::GcsConfig`
    /// — non-credential fields are rendered verbatim into the broker
    /// TOML's `[remote_storage.gcs]` block. Unlike S3 (env-var
    /// credentials), an explicit service-account JSON key is mounted as a
    /// FILE on the broker pod and surfaced to the broker via
    /// `service_account_path` in the TOML; leaving credentials unset
    /// selects keyless Workload Identity / ADC.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub gcs: Option<GcsStorageSpec>,
    /// KIP-405: pick the
    /// `RemoteLogMetadataManager` the broker pods run. When absent (or set
    /// to `type: Topic`),
    /// the broker activates the durable
    /// `crabka_remote_storage_topic::TopicBasedRemoteLogMetadataManager`
    /// against the internal `__remote_log_metadata` topic, so
    /// tier-segment metadata survives pod restarts and is consistent
    /// across brokers in the cluster. The in-memory fixture is
    /// selected only by an explicit `type: InMemory` (test/dev only).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub metadata_manager: Option<MetadataManagerSpec>,
    /// KIP-405: durable storage for the local-tier
    /// directory. Only valid with `type=Local`. When absent (default),
    /// the operator renders an `emptyDir` for `tier-storage`.
    /// When `Some`, the operator renders a `volumeClaimTemplate`
    /// of the configured size / class so tier data survives pod
    /// restarts — pairing with the topic-backed RLMM, this closes
    /// the "tier data is lost on pod restart" caveat.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub persistence: Option<TieredStoragePersistence>,
}

/// KIP-405: PVC-backed local-tier directory.
///
/// Mirrors [`crate::crd::kafka_node_pool::PersistentClaimSpec`] field
/// shapes so operators learn one schema for both the data dir and the
/// tier-cache dir. PVC retention follows the parent
/// `KafkaNodePool.spec.storage.deleteClaim` setting — the `StatefulSet`'s
/// `persistentVolumeClaimRetentionPolicy` is set-wide and there is no
/// per-template override in Kubernetes.
#[derive(Debug, Clone, Default, Deserialize, Serialize, JsonSchema, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct TieredStoragePersistence {
    /// K8s `Quantity` (e.g., `"50Gi"`, `"500Mi"`). Non-empty;
    /// resource-quantity well-formedness is validated by the
    /// Kubernetes API server at SSA time.
    pub size: String,
    /// Storage class name. `None` = cluster default.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub class: Option<String>,
    /// `true` → `persistentVolumeClaimRetentionPolicy.whenDeleted: Delete`.
    /// Must match the parent `KafkaNodePool.spec.storage.deleteClaim`
    /// when both PVCs are present (K8s `StatefulSets` have a single
    /// set-wide retention policy with no per-template override).
    /// Validated at reconcile time; mismatch surfaces as
    /// `TieredStorageInvalid`.
    #[serde(default)]
    pub delete_claim: bool,
}

/// KIP-405: the set of RSM backends the operator knows how
/// to render. Adding a backend means extending this enum AND the
/// matching render path in
/// `crate::controller::listeners::render_broker_toml`.
#[derive(Debug, Clone, Copy, Default, Deserialize, Serialize, JsonSchema, PartialEq, Eq)]
pub enum TieredStorageType {
    /// On-pod filesystem store via `LocalTieredStorage` (the
    /// reference RSM). Data lives at `/var/lib/crabka/remote` on the
    /// broker pod.
    #[default]
    Local,
    /// S3-compatible object store via `S3RemoteStorage` (the
    /// production RSM). Pair with a populated
    /// [`TieredStorage::s3`] for bucket / region / credentials.
    S3,
    /// Native Google Cloud Storage via `S3RemoteStorage`'s GCS backend.
    /// Pair with a populated [`TieredStorage::gcs`] for bucket / prefix /
    /// credentials. Leaving `gcs.credentials` unset selects GKE Workload
    /// Identity / Application Default Credentials (the keyless production
    /// path); an explicit service-account JSON key is mounted as a file.
    Gcs,
}

/// KIP-405: cluster-wide S3 backend configuration.
///
/// Non-credential fields are rendered into the broker config TOML's
/// `[remote_storage.s3]` block verbatim and parsed back into
/// `crabka_remote_storage::S3Config`. Credentials are NEVER rendered
/// into TOML — when [`Self::credentials`] is set, the operator wires
/// `AWS_ACCESS_KEY_ID` / `AWS_SECRET_ACCESS_KEY` env vars onto the
/// broker pod via `valueFrom.secretKeyRef`, and `object_store`'s
/// `AmazonS3Builder` picks them up through the standard AWS credential
/// chain. When credentials are absent, the broker pod inherits whatever
/// IAM / IRSA / instance-profile auth is wired into the cluster.
#[derive(Debug, Clone, Default, Deserialize, Serialize, JsonSchema, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct S3StorageSpec {
    /// S3 bucket name. Required.
    pub bucket: String,
    /// AWS region. Required even for non-AWS endpoints (`MinIO`, R2) —
    /// `object_store`'s `AmazonS3Builder` rejects an empty region.
    pub region: String,
    /// Optional key prefix inside the bucket. Lets multiple Crabka
    /// clusters share a bucket without colliding.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub prefix: Option<String>,
    /// Optional custom endpoint URL (e.g. `http://minio:9000` for
    /// `MinIO`, `https://<account>.r2.cloudflarestorage.com` for
    /// Cloudflare R2). When `None`, the AWS S3 endpoint for the
    /// configured region is used.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub endpoint: Option<String>,
    /// Optional explicit credentials. When `None`, the broker falls
    /// back to the AWS credential chain (IRSA on EKS, instance profile
    /// on EC2, etc.).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub credentials: Option<S3Credentials>,
    /// Allow plaintext HTTP. Off by default; flip on for `MinIO`
    /// running without TLS. AWS S3 itself never needs this.
    #[serde(default, skip_serializing_if = "std::ops::Not::not")]
    pub allow_http: bool,
    /// Override the single-PUT / multipart cutoff (bytes). When unset,
    /// the broker uses `crabka_remote_storage::DEFAULT_MULTIPART_THRESHOLD`
    /// (100 MiB). Lower in tests to exercise the multipart path on
    /// small fixtures.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub multipart_threshold: Option<u64>,
    /// Override the per-part size for multipart uploads (bytes). When
    /// unset, the broker uses
    /// `crabka_remote_storage::DEFAULT_MULTIPART_CHUNK_SIZE` (16 MiB).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub multipart_chunk_size: Option<u64>,
}

/// KIP-405: cluster-wide native GCS backend configuration.
///
/// Mirrors `crabka_remote_storage::GcsConfig`. Non-credential fields are
/// rendered verbatim into the broker config TOML's `[remote_storage.gcs]`
/// block and parsed back into `crabka_remote_storage::GcsConfig`.
///
/// Credentials differ from S3: GCS credentials are a JSON key FILE, and
/// `object_store`'s GCS builder reads the file path directly (it does NOT
/// consult `GOOGLE_APPLICATION_CREDENTIALS`). So when [`Self::credentials`]
/// is set, the operator mounts the referenced Secret as a file on the
/// broker pod and renders its path into the TOML as `service_account_path`.
/// When credentials are absent, the broker uses Workload Identity / ADC —
/// the keyless GKE path — and no credential file or env is wired.
#[derive(Debug, Clone, Default, Deserialize, Serialize, JsonSchema, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct GcsStorageSpec {
    /// GCS bucket name. Required.
    pub bucket: String,
    /// Optional key prefix inside the bucket. Lets multiple Crabka
    /// clusters share a bucket without colliding.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub prefix: Option<String>,
    /// Optional custom GCS API base URL (e.g. for emulators / fakes).
    /// When `None`, the standard Google Cloud Storage endpoint is used.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub endpoint: Option<String>,
    /// Optional explicit service-account credentials. When None, the
    /// broker uses Workload Identity / ADC (the keyless GKE path).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub credentials: Option<GcsCredentials>,
    /// Allow plaintext HTTP. Off by default; flip on for GCS emulators
    /// running without TLS. Real GCS never needs this.
    #[serde(default, skip_serializing_if = "std::ops::Not::not")]
    pub allow_http: bool,
    /// Override the single-PUT / multipart cutoff (bytes). When unset,
    /// the broker uses `crabka_remote_storage::DEFAULT_MULTIPART_THRESHOLD`
    /// (100 MiB). Lower in tests to exercise the multipart path on
    /// small fixtures.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub multipart_threshold: Option<u64>,
    /// Override the per-part size for multipart uploads (bytes). When
    /// unset, the broker uses
    /// `crabka_remote_storage::DEFAULT_MULTIPART_CHUNK_SIZE` (16 MiB).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub multipart_chunk_size: Option<u64>,
}

/// KIP-405: GCS service-account credential.
///
/// A single [`SecretKeyRef`] to the Secret holding the service-account
/// JSON key. When set, the operator mounts the Secret as a file on the
/// broker pod and renders `service_account_path` into the broker TOML.
/// Omit to use keyless Workload Identity / ADC.
#[derive(Debug, Clone, Default, Deserialize, Serialize, JsonSchema, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct GcsCredentials {
    /// Reference to the Secret holding the service-account JSON key.
    pub service_account_key: SecretKeyRef,
}

impl TieredStorage {
    /// KIP-405: shape-validate the tagged union.
    /// Returns the offending field's description on failure; the
    /// reconciler wraps it in [`crate::controller::common::ReconcileError::TieredStorageInvalid`].
    /// Pure (no I/O) so it can be unit-tested without a cluster.
    ///
    /// # Errors
    ///
    /// Fails when the discriminator and the sibling fields disagree
    /// (e.g. `type=S3` without `s3`, `type=Gcs` without `gcs`, or a
    /// backend set alongside the wrong discriminator), or when the
    /// selected spec is missing a required field (S3: `bucket`,
    /// `region`; GCS: `bucket`).
    pub fn validate(&self) -> Result<(), String> {
        match self.kind {
            TieredStorageType::Local => {
                if self.s3.is_some() {
                    return Err("type=Local must not set `s3`".into());
                }
                if self.gcs.is_some() {
                    return Err("type=Local must not set `gcs`".into());
                }
            }
            TieredStorageType::S3 => {
                if self.gcs.is_some() {
                    return Err("type=S3 must not set `gcs`".into());
                }
                let s3 = self
                    .s3
                    .as_ref()
                    .ok_or("type=S3 requires `s3` (bucket + region at minimum)")?;
                if s3.bucket.trim().is_empty() {
                    return Err("s3.bucket is required and must be non-empty".into());
                }
                if s3.region.trim().is_empty() {
                    return Err("s3.region is required and must be non-empty".into());
                }
            }
            TieredStorageType::Gcs => {
                if self.s3.is_some() {
                    return Err("type=Gcs must not set `s3`".into());
                }
                let gcs = self
                    .gcs
                    .as_ref()
                    .ok_or("type=Gcs requires `gcs` (bucket at minimum)")?;
                if gcs.bucket.trim().is_empty() {
                    return Err("gcs.bucket is required and must be non-empty".into());
                }
            }
        }
        if let Some(mm) = self.metadata_manager.as_ref() {
            mm.validate()?;
        }
        if let Some(p) = self.persistence.as_ref() {
            if self.kind != TieredStorageType::Local {
                return Err("persistence is only valid with type=Local".into());
            }
            if p.size.trim().is_empty() {
                return Err("persistence.size is required and must be non-empty".into());
            }
        }
        Ok(())
    }
}

/// KIP-405: which
/// `RemoteLogMetadataManager` the broker pods use. Defaults to topic-backed
/// (`type: Topic`)
/// when this field is omitted.
#[derive(Debug, Clone, Default, Deserialize, Serialize, JsonSchema, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct MetadataManagerSpec {
    /// Implementation selector.
    #[serde(rename = "type")]
    pub kind: MetadataManagerType,
    /// Topic-backed tuning. Optional when `kind == Topic` (broker
    /// fills defaults for bootstrap and topic parameters), must be
    /// absent otherwise.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub topic: Option<TopicMetadataManagerSpec>,
}

impl MetadataManagerSpec {
    /// Shape-validate. Pure; called by [`TieredStorage::validate`].
    ///
    /// # Errors
    ///
    /// Fails when `type=InMemory` is paired with a `topic` sub-block,
    /// or when a topic-backed configuration supplies a `topic` block
    /// with invalid fields (e.g. empty `bootstrap`, non-positive
    /// `numPartitions`). A bare `type=Topic` with no `topic` block is
    /// valid — the broker fills all defaults.
    pub fn validate(&self) -> Result<(), String> {
        match (self.kind, &self.topic) {
            (MetadataManagerType::InMemory, Some(_)) => {
                Err("metadataManager.type=InMemory must not set `topic`".into())
            }
            (MetadataManagerType::Topic | MetadataManagerType::InMemory, None) => Ok(()),
            (MetadataManagerType::Topic, Some(topic)) => topic.validate(),
        }
    }
}

/// KIP-405: the RLMM implementations the operator knows
/// how to render.
#[derive(Debug, Clone, Copy, Default, Deserialize, Serialize, JsonSchema, PartialEq, Eq)]
pub enum MetadataManagerType {
    /// In-memory fixture from `crabka_remote_storage`.
    /// Tier-segment metadata does not survive pod restarts.
    /// Selected only by an explicit `type: InMemory` (test/dev).
    InMemory,
    /// Production topic-backed manager from
    /// `crabka_remote_storage_topic`. Default. An optional
    /// [`MetadataManagerSpec::topic`] sub-block tunes bootstrap
    /// address and topic-creation parameters; the broker fills
    /// defaults when it is omitted.
    #[default]
    Topic,
}

/// KIP-405: topic-backed RLMM tuning. Renders into the
/// broker TOML's `[remote_storage.kafka_metadata]` block.
#[derive(Debug, Clone, Default, Deserialize, Serialize, JsonSchema, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct TopicMetadataManagerSpec {
    /// `host:port` the broker pod dials to reach its own listener for
    /// publishing / consuming `__remote_log_metadata`. Typically the
    /// pod's loopback inter-broker listener (e.g. `127.0.0.1:9094`).
    pub bootstrap: String,
    /// Partition count for `__remote_log_metadata` on first creation.
    /// Defaults to 50 (Kafka's
    /// `remote.log.metadata.topic.num.partitions`).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub num_partitions: Option<i32>,
    /// Replication factor for `__remote_log_metadata` on first
    /// creation. Defaults to 3 (Kafka's
    /// `remote.log.metadata.topic.replication.factor`).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub replication: Option<i32>,
}

impl TopicMetadataManagerSpec {
    /// Shape-validate. Pure; called by [`MetadataManagerSpec::validate`].
    ///
    /// # Errors
    ///
    /// Fails when `bootstrap` is empty or `num_partitions` /
    /// `replication` are non-positive.
    pub fn validate(&self) -> Result<(), String> {
        if self.bootstrap.trim().is_empty() {
            return Err("metadataManager.topic.bootstrap is required and must be non-empty".into());
        }
        if let Some(p) = self.num_partitions
            && p <= 0
        {
            return Err(format!(
                "metadataManager.topic.numPartitions must be > 0 (got {p})"
            ));
        }
        if let Some(r) = self.replication
            && r <= 0
        {
            return Err(format!(
                "metadataManager.topic.replication must be > 0 (got {r})"
            ));
        }
        Ok(())
    }
}

/// Cluster-wide distributed-tracing configuration. Maps to
/// the broker's `CRABKA_OTLP_*` env-var contract: the operator
/// renders one env entry per populated field on every broker pod, and
/// the broker's `TelemetryConfig::from_env` picks them up at startup.
///
/// The `type` discriminator is reserved for future tracing backends; for
/// now only `Otlp` is meaningful, and the matching `otlp` block is
/// required when `type = Otlp`.
#[derive(Debug, Clone, Default, Deserialize, Serialize, JsonSchema, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct Tracing {
    /// Tracing backend selector.
    #[serde(rename = "type")]
    pub kind: TracingType,
    /// OTLP-backend tuning. Required when `kind == Otlp`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub otlp: Option<OtlpTracing>,
}

/// The tracing backends the operator knows how to render.
#[derive(Debug, Clone, Copy, Default, Deserialize, Serialize, JsonSchema, PartialEq, Eq)]
pub enum TracingType {
    /// OpenTelemetry OTLP exporter. Pair with [`Tracing::otlp`] for the
    /// endpoint / protocol / sampling.
    #[default]
    Otlp,
}

/// OTLP-specific tracing parameters. Each populated field is
/// rendered as a separate env var on every broker pod.
#[derive(Debug, Clone, Deserialize, Serialize, JsonSchema, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct OtlpTracing {
    /// Required. OTLP collector endpoint (`scheme://host:port`).
    /// Rendered as `CRABKA_OTLP_ENDPOINT`; turning the field on
    /// implicitly sets `CRABKA_OTLP_ENABLED=true` as well.
    pub endpoint: String,
    /// Optional protocol. Defaults to `Grpc` (matches Kafka /
    /// OpenTelemetry SDK convention). Rendered as
    /// `CRABKA_OTLP_PROTOCOL`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub protocol: Option<OtlpProtocol>,
    /// Optional sampling ratio in `[0.0, 1.0]`. Rendered as
    /// `CRABKA_OTLP_SAMPLE_RATIO`. Defaults to the broker's `1.0`
    /// (sample every trace).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub sample_ratio: Option<f64>,
    /// Optional `service.name` resource attribute. Rendered as
    /// `OTEL_SERVICE_NAME`. Defaults to the broker's
    /// `"crabka-broker"`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub service_name: Option<String>,
    /// Optional export timeout in seconds. Rendered as
    /// `CRABKA_OTLP_TIMEOUT_SECS`. Defaults to the broker's `10`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub timeout_secs: Option<u64>,
}

/// OTLP wire protocol selector. Mirrors the broker's
/// internal `OtlpProtocol` enum and the `OTEL_EXPORTER_OTLP_PROTOCOL`
/// spec values.
#[derive(Debug, Clone, Copy, Deserialize, Serialize, JsonSchema, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum OtlpProtocol {
    /// gRPC over HTTP/2 (default; `:4317`).
    Grpc,
    /// HTTP/1 + protobuf payload (`:4318`).
    HttpProtobuf,
}

impl OtlpProtocol {
    /// Render the env-var value the broker's `OtlpProtocol::parse`
    /// expects.
    #[must_use]
    pub fn as_env_value(self) -> &'static str {
        match self {
            Self::Grpc => "grpc",
            Self::HttpProtobuf => "http/protobuf",
        }
    }
}

impl Tracing {
    /// Shape-validate the tagged union.
    ///
    /// # Errors
    ///
    /// Fails when `type=Otlp` is missing the `otlp` block, when
    /// `otlp.endpoint` is empty, when `sampleRatio` is outside
    /// `[0.0, 1.0]`, or when `timeoutSecs == 0`.
    pub fn validate(&self) -> Result<(), String> {
        match (self.kind, &self.otlp) {
            (TracingType::Otlp, None) => {
                Err("type=Otlp requires `otlp` (endpoint at minimum)".into())
            }
            (TracingType::Otlp, Some(otlp)) => {
                if otlp.endpoint.trim().is_empty() {
                    return Err("otlp.endpoint is required and must be non-empty".into());
                }
                if let Some(r) = otlp.sample_ratio
                    && !(0.0..=1.0).contains(&r)
                {
                    return Err(format!("otlp.sampleRatio must be in [0.0, 1.0] (got {r})"));
                }
                if let Some(s) = otlp.service_name.as_deref()
                    && s.trim().is_empty()
                {
                    return Err("otlp.serviceName, when set, must be non-empty".into());
                }
                if otlp.timeout_secs == Some(0) {
                    return Err("otlp.timeoutSecs, when set, must be > 0".into());
                }
                Ok(())
            }
        }
    }
}

/// KIP-405: S3 access-key credential pair.
///
/// Two [`SecretKeyRef`]s — one per AWS credential half — so an operator
/// can hold the secret-access-key in a separate, more tightly
/// permissioned Secret than the access-key-id if they want, while still
/// supporting the common case of both keys in one Secret (different
/// `key` values on the same `name`).
#[derive(Debug, Clone, Default, Deserialize, Serialize, JsonSchema, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct S3Credentials {
    /// Reference to the Secret holding the `AWS_ACCESS_KEY_ID` value.
    pub access_key_id: SecretKeyRef,
    /// Reference to the Secret holding the `AWS_SECRET_ACCESS_KEY` value.
    pub secret_access_key: SecretKeyRef,
}

/// Master-HMAC-key source for KIP-48 delegation tokens.
///
/// The operator wires the referenced Secret key as the broker pod's
/// `CRABKA_DELEGATION_TOKEN_SECRET_KEY` env var (env wins over TOML in
/// the broker config layer). Required for delegation-token
/// `KafkaUser` support. If unset on the parent `Kafka`,
/// the broker rejects all delegation-token RPCs with err 61
/// `DELEGATION_TOKEN_AUTH_DISABLED`.
#[derive(Debug, Clone, Default, Deserialize, Serialize, JsonSchema, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct DelegationTokenConfig {
    /// Reference to a Kubernetes `Secret` (same namespace as the
    /// `Kafka` CR) whose `data.<key>` value is the broker's master HMAC
    /// key for KIP-48 delegation tokens.
    pub secret_key_ref: SecretKeyRef,
}

/// Minimal namespaced Secret-key reference (name + optional
/// data-map key, defaulting to `secret-key`).
#[derive(Debug, Clone, Default, Deserialize, Serialize, JsonSchema, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct SecretKeyRef {
    /// Secret name in the same namespace as the `Kafka` CR.
    pub name: String,
    /// Key within the Secret's `data`. Defaults to `secret-key`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub key: Option<String>,
}

/// Cluster-level authorizer selection on `Kafka.spec.authorization`.
///
/// Tagged on `type` to pick the broker-side `Arc<dyn Authorizer>` impl.
/// `None` on the parent spec means `AllowAll` (no `[authorization]` TOML
/// section is rendered, the broker uses `AllowAllAuthorizer`). When set,
/// the operator's inter-broker principal MUST be in `super_users` — there
/// is no implicit ANONYMOUS allow.
///
/// The `schema_with` workaround avoids a kube-rs 3.x `StructuralSchemaRewriter`
/// panic when `oneOf` branches share a `type` discriminator with differing
/// `enum` values — same pattern as `Authentication` in `user.rs` and
/// `ListenerAuthentication` in `listener.rs`.
#[derive(Debug, Clone, Deserialize, Serialize, JsonSchema, PartialEq)]
#[serde(tag = "type", rename_all = "kebab-case")]
#[schemars(schema_with = "authorization_schema")]
pub enum Authorization {
    #[serde(rename = "simple")]
    Simple(SimpleAuthorization),
    #[serde(rename = "opa")]
    Opa(OpaAuthorization),
}

/// `type: simple` config for `Kafka.spec.authorization`. Drives the
/// broker's `SimpleAclAuthorizer`. Distinct from the per-user
/// `crate::crd::user::SimpleAuthorization` (which carries ACL rules for one
/// `KafkaUser`): this one is cluster-wide and only carries the super-user
/// bypass list. ACLs themselves are owned by `KafkaUser` CRs / `CreateAcls`.
#[derive(Debug, Clone, Default, Deserialize, Serialize, JsonSchema, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct SimpleAuthorization {
    /// Principal strings (e.g. `"User:admin"`, `"ANONYMOUS"`) that
    /// bypass ACL checks. Empty = no super-users.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub super_users: Vec<String>,
}

/// `type: opa` config for `Kafka.spec.authorization`. Drives the
/// broker's `OpaAuthorizer` — an HTTP-backed authorizer with an LRU+TTL
/// decision cache. No `derive(Default)` because `url` has no sensible default.
#[derive(Debug, Clone, Deserialize, Serialize, JsonSchema, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct OpaAuthorization {
    /// OPA decision endpoint URL — must include the data-API path, e.g.
    /// `http://opa:8181/v1/data/kafka/authz/allow`.
    pub url: String,
    /// Permit the operation on any OPA error (timeout, 5xx, parse).
    /// Default false (fail-closed).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub allow_on_error: Option<bool>,
    /// Initial capacity of the broker's LRU decision cache. Broker
    /// default applies when unset.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[schemars(range(min = 0))]
    pub initial_cache_capacity: Option<u32>,
    /// Hard upper bound on the LRU decision cache. Broker default
    /// applies when unset.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[schemars(range(min = 1))]
    pub maximum_cache_size: Option<u32>,
    /// Per-entry TTL (ms). Broker default applies when unset.
    /// Minimum 1000 ms — sub-second TTLs defeat the cache.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[schemars(range(min = 1000))]
    pub expire_after_ms: Option<i64>,
    /// Principal strings that bypass OPA entirely. The broker's
    /// internal calls (replication etc.) use `ANONYMOUS` by default,
    /// which MUST be a super-user for inter-broker traffic to work
    /// when `type: opa` is selected. Empty = no super-users (OPA
    /// decides every request).
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub super_users: Vec<String>,
}

fn authorization_schema(_: &mut schemars::SchemaGenerator) -> schemars::Schema {
    schemars::json_schema!({
        "type": "object",
        "required": ["type"],
        "properties": {
            "type": {
                "type": "string",
                "enum": ["simple", "opa"],
            },
            "superUsers": {
                "type": "array",
                "items": { "type": "string" },
            },
            // OPA-only sibling properties.
            "url": { "type": "string" },
            "allowOnError": { "type": "boolean" },
            "initialCacheCapacity": { "type": "integer", "minimum": 0 },
            "maximumCacheSize": { "type": "integer", "minimum": 1 },
            "expireAfterMs": { "type": "integer", "minimum": 1000 },
        },
    })
}

#[derive(Debug, Clone, Default, Deserialize, Serialize, JsonSchema, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct KafkaStatus {
    /// Standard Kubernetes-style condition list. Surfaces
    /// `Ready`, `ListenersValid`, `ListenersReady`.
    #[serde(default)]
    pub conditions: Vec<KafkaCondition>,
    /// Mirrors `StatefulSet.status.replicas`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub replicas: Option<i32>,
    /// Mirrors `StatefulSet.status.readyReplicas`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub ready_replicas: Option<i32>,
    /// Per-listener resolved addresses. Populated once
    /// `ListenersReady=True`.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub listeners: Vec<crate::crd::ListenerStatus>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub cluster_ca: Option<crate::crd::CertificateAuthorityStatus>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub clients_ca: Option<crate::crd::CertificateAuthorityStatus>,
    /// Echo of `spec.kafkaVersion`, for observability.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub kafka_version: Option<String>,
    /// The operator-finalized metadata version. Advances only
    /// when version validation passes; drives the downgrade-window check on
    /// the next reconcile.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub metadata_version: Option<String>,
}

#[derive(Debug, Clone, Deserialize, Serialize, JsonSchema, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct KafkaCondition {
    /// e.g. `Ready`.
    #[serde(rename = "type")]
    pub type_: String,
    /// `True`, `False`, or `Unknown`.
    pub status: String,
    /// CamelCase machine reason.
    pub reason: String,
    /// Human-readable message.
    pub message: String,
    /// RFC3339 timestamp.
    pub last_transition_time: String,
}

#[cfg(test)]
mod tests {
    use assert2::{assert, check};
    use kube::CustomResourceExt as _;

    use super::*;

    #[test]
    fn crd_metadata_is_correct() {
        let crd = Kafka::crd();
        check!(crd.spec.group == "crabka.io");
        check!(crd.spec.names.kind == "Kafka");
        check!(crd.spec.names.plural == "kafkas");
        check!(crd.spec.versions.len() == 1);
        check!(crd.spec.versions[0].name == "v1alpha1");
    }

    #[test]
    fn round_trips_through_json() {
        let k = Kafka::new(
            "demo",
            KafkaSpec {
                kafka_version: "0.1.1".into(),
                metadata_version: None,
                config: None,
                listeners: vec![],
                inter_broker_listener_name: None,
                metrics_config: None,
                network_policy: None,
                cluster_ca: None,
                clients_ca: None,
                logging: None,
                delegation_token: None,
                authorization: None,
                tiered_storage: None,
                inter_broker_kerberos: None,
                krb5_conf_secret_ref: None,
                tracing: None,
                broker_tuning: None,
            },
        );
        let json = serde_json::to_string(&k).unwrap();
        assert!(
            json.contains("\"kafkaVersion\""),
            "expected camelCase wire shape, got: {json}"
        );
        let back: Kafka = serde_json::from_str(&json).unwrap();
        assert!(back.spec == k.spec);
    }

    #[test]
    fn spec_omits_metrics_config_when_none() {
        let k = Kafka::new(
            "demo",
            KafkaSpec {
                kafka_version: "0.1.1".into(),
                metadata_version: None,
                config: None,
                listeners: vec![],
                inter_broker_listener_name: None,
                metrics_config: None,
                network_policy: None,
                cluster_ca: None,
                clients_ca: None,
                logging: None,
                delegation_token: None,
                authorization: None,
                tiered_storage: None,
                inter_broker_kerberos: None,
                krb5_conf_secret_ref: None,
                tracing: None,
                broker_tuning: None,
            },
        );
        let j = serde_json::to_string(&k.spec).unwrap();
        assert!(!j.contains("metricsConfig"), "got: {j}");
    }

    #[test]
    fn spec_carries_metrics_config_pod_monitor() {
        use crate::crd::{MetricsConfig, PodMonitorSpec};
        let json = r#"{"kafkaVersion":"0.1.1","metricsConfig":{"podMonitor":{"interval":"30s"}}}"#;
        let spec: KafkaSpec = serde_json::from_str(json).unwrap();
        let cfg: MetricsConfig = spec.metrics_config.expect("metricsConfig present");
        let pm: PodMonitorSpec = cfg.pod_monitor.expect("podMonitor present");
        assert!(pm.interval.as_deref() == Some("30s"));
    }

    #[test]
    fn spec_only_carries_kafka_version() {
        let json = r#"{"kafkaVersion":"0.1.1"}"#;
        let spec: KafkaSpec = serde_json::from_str(json).unwrap();
        assert!(spec.kafka_version == "0.1.1");
        assert!(spec.config.is_none());
    }

    #[test]
    fn spec_carries_config() {
        let json = r#"{"kafkaVersion":"0.1.1","config":{"log.retention.hours":"24"}}"#;
        let spec: KafkaSpec = serde_json::from_str(json).unwrap();
        let cfg = spec.config.expect("config present");
        assert!(cfg.get("log.retention.hours").map(String::as_str) == Some("24"));
    }

    #[test]
    fn spec_carries_listeners() {
        use crate::crd::{Listener, ListenerType};

        let json = r#"{
            "kafkaVersion":"0.1.1",
            "listeners":[{"name":"PLAIN","port":9092,"type":"internal","tls":false}],
            "interBrokerListenerName":"PLAIN"
        }"#;
        let spec: KafkaSpec = serde_json::from_str(json).unwrap();
        assert!(
            spec.listeners
                == vec![Listener {
                    name: "PLAIN".to_string(),
                    port: 9092,
                    type_: ListenerType::Internal,
                    tls: false,
                    authentication: None,
                    configuration: None,
                    network_policy_peers: None,
                }]
        );
        assert!(spec.inter_broker_listener_name.as_deref() == Some("PLAIN"));
    }

    #[test]
    fn spec_defaults_listeners_to_empty() {
        let json = r#"{"kafkaVersion":"0.1.1"}"#;
        let spec: KafkaSpec = serde_json::from_str(json).unwrap();
        assert!(spec.listeners.is_empty());
        assert!(spec.inter_broker_listener_name.is_none());
    }

    #[test]
    fn status_carries_listener_status() {
        use crate::crd::{ListenerAddress, ListenerStatus, ListenerType};

        let status = KafkaStatus {
            conditions: vec![],
            replicas: Some(1),
            ready_replicas: Some(1),
            listeners: vec![ListenerStatus {
                name: "PLAIN".into(),
                type_: ListenerType::Internal,
                bootstrap_servers: "demo-broker-headless.default.svc.cluster.local:9092".into(),
                addresses: vec![ListenerAddress {
                    host: "demo-broker-headless.default.svc.cluster.local".into(),
                    port: 9092,
                }],
            }],
            cluster_ca: None,
            clients_ca: None,
            kafka_version: None,
            metadata_version: None,
        };
        let json = serde_json::to_string(&status).unwrap();
        assert!(json.contains("\"bootstrapServers\""), "got: {json}");
        let back: KafkaStatus = serde_json::from_str(&json).unwrap();
        assert!(back == status);
    }

    #[test]
    fn spec_carries_metadata_version() {
        let json = r#"{"kafkaVersion":"3.7.0","metadataVersion":"3.6"}"#;
        let spec: KafkaSpec = serde_json::from_str(json).unwrap();
        assert!(spec.metadata_version.as_deref() == Some("3.6"));
    }

    #[test]
    fn spec_omits_metadata_version_when_none() {
        let k = Kafka::new(
            "demo",
            KafkaSpec {
                kafka_version: "3.7.0".into(),
                metadata_version: None,
                config: None,
                listeners: vec![],
                inter_broker_listener_name: None,
                metrics_config: None,
                network_policy: None,
                cluster_ca: None,
                clients_ca: None,
                logging: None,
                delegation_token: None,
                authorization: None,
                tiered_storage: None,
                inter_broker_kerberos: None,
                krb5_conf_secret_ref: None,
                tracing: None,
                broker_tuning: None,
            },
        );
        let j = serde_json::to_string(&k.spec).unwrap();
        assert!(!j.contains("metadataVersion"), "got: {j}");
    }

    #[test]
    fn status_carries_version_fields() {
        let status = KafkaStatus {
            kafka_version: Some("3.7.0".into()),
            metadata_version: Some("3.7".into()),
            ..Default::default()
        };
        let json = serde_json::to_string(&status).unwrap();
        assert!(json.contains("\"metadataVersion\":\"3.7\""), "got: {json}");
        let back: KafkaStatus = serde_json::from_str(&json).unwrap();
        assert!(back == status);
    }

    #[test]
    fn spec_omits_network_policy_when_none() {
        let k = Kafka::new(
            "demo",
            KafkaSpec {
                kafka_version: "0.1.1".into(),
                metadata_version: None,
                config: None,
                listeners: vec![],
                inter_broker_listener_name: None,
                metrics_config: None,
                network_policy: None,
                cluster_ca: None,
                clients_ca: None,
                logging: None,
                delegation_token: None,
                authorization: None,
                tiered_storage: None,
                inter_broker_kerberos: None,
                krb5_conf_secret_ref: None,
                tracing: None,
                broker_tuning: None,
            },
        );
        let j = serde_json::to_string(&k.spec).unwrap();
        assert!(!j.contains("networkPolicy"), "got: {j}");
    }

    #[test]
    fn spec_carries_network_policy_when_set() {
        let json = r#"{"kafkaVersion":"0.1.1","networkPolicy":{}}"#;
        let spec: KafkaSpec = serde_json::from_str(json).unwrap();
        assert!(spec.network_policy.is_some(), "networkPolicy parsed");
    }

    #[test]
    fn spec_omits_logging_when_none() {
        let json = r#"{"kafkaVersion":"0.1.1"}"#;
        let spec: KafkaSpec = serde_json::from_str(json).unwrap();
        assert!(spec.logging.is_none());
        let j = serde_json::to_string(&spec).unwrap();
        assert!(!j.contains("logging"), "got: {j}");
    }

    #[test]
    fn spec_carries_inline_logging() {
        use crate::crd::LoggingType;
        let json = r#"{"kafkaVersion":"0.1.1","logging":{"loggers":{"root":"info","crabka_broker":"debug"}}}"#;
        let spec: KafkaSpec = serde_json::from_str(json).unwrap();
        let lg = spec.logging.expect("logging present");
        assert!(lg.r#type == LoggingType::Inline);
        assert!(lg.loggers.get("crabka_broker").map(String::as_str) == Some("debug"));
    }

    #[test]
    fn kafka_spec_parses_without_ca_fields() {
        let v: KafkaSpec = serde_json::from_value(serde_json::json!({
            "kafkaVersion": "3.7.0",
        }))
        .expect("parse minimal spec");
        assert!(v.cluster_ca.is_none());
        assert!(v.clients_ca.is_none());
    }

    #[test]
    fn spec_omits_delegation_token_when_none() {
        let json = r#"{"kafkaVersion":"0.1.1"}"#;
        let spec: KafkaSpec = serde_json::from_str(json).unwrap();
        assert!(spec.delegation_token.is_none());
        let j = serde_json::to_string(&spec).unwrap();
        assert!(!j.contains("delegationToken"), "got: {j}");
    }

    #[test]
    fn spec_carries_delegation_token_with_default_key() {
        let json = r#"{
            "kafkaVersion":"0.1.1",
            "delegationToken":{"secretKeyRef":{"name":"dt-master"}}
        }"#;
        let spec: KafkaSpec = serde_json::from_str(json).unwrap();
        let dt = spec.delegation_token.expect("delegationToken present");
        assert!(dt.secret_key_ref.name == "dt-master");
        assert!(dt.secret_key_ref.key.is_none());
    }

    #[test]
    fn spec_carries_delegation_token_with_explicit_key() {
        let json = r#"{
            "kafkaVersion":"0.1.1",
            "delegationToken":{"secretKeyRef":{"name":"dt-master","key":"hmac"}}
        }"#;
        let spec: KafkaSpec = serde_json::from_str(json).unwrap();
        let dt = spec.delegation_token.expect("delegationToken present");
        assert!(dt.secret_key_ref.name == "dt-master");
        assert!(dt.secret_key_ref.key.as_deref() == Some("hmac"));
    }

    #[test]
    fn kafka_spec_parses_with_ca_fields() {
        let v: KafkaSpec = serde_json::from_value(serde_json::json!({
            "kafkaVersion": "3.7.0",
            "clusterCa": { "validityDays": 30 },
            "clientsCa": { "generateCertificateAuthority": false },
        }))
        .expect("parse with CAs");
        assert!(v.cluster_ca.as_ref().unwrap().validity_days == 30);
        assert!(
            !v.clients_ca
                .as_ref()
                .unwrap()
                .generate_certificate_authority
        );
    }

    // `Kafka.spec.authorization` round-trip tests.
    //
    // Pin the wire shape of the authorizer-selection CRD
    // alongside its sibling enums on `KafkaSpec`. Mirrors the
    // `delegationToken` round-trip pattern: deserialize Strimzi-shape
    // YAML, assert the typed Rust value, then re-serialize and assert
    // optional fields are omitted (so the rendered TOML stays minimal
    // and the broker's `[authorization]` parser doesn't trip on
    // explicit-null vs absent).

    #[test]
    fn simple_authorization_round_trip() {
        let yaml = r"
kafkaVersion: 0.1.1
authorization:
  type: simple
  superUsers:
    - User:admin
    - ANONYMOUS
";
        let spec: KafkaSpec = serde_yaml::from_str(yaml).expect("yaml must parse");
        let Some(Authorization::Simple(simple)) = spec.authorization.clone() else {
            panic!("expected Simple variant, got {:?}", spec.authorization);
        };
        assert!(simple.super_users == vec!["User:admin".to_string(), "ANONYMOUS".to_string()]);

        // JSON round-trip pins the camelCase wire shape (`superUsers`,
        // `type: "simple"`).
        let json = serde_json::to_string(&spec).unwrap();
        assert!(json.contains("\"type\":\"simple\""), "got: {json}");
        assert!(
            json.contains("\"superUsers\":[\"User:admin\",\"ANONYMOUS\"]"),
            "got: {json}"
        );
        let back: KafkaSpec = serde_json::from_str(&json).unwrap();
        assert!(back == spec);
    }

    #[test]
    fn opa_authorization_round_trip_full_fields() {
        let yaml = r"
kafkaVersion: 0.1.1
authorization:
  type: opa
  url: http://opa.opa.svc:8181/v1/data/kafka/authz/allow
  allowOnError: true
  initialCacheCapacity: 1000
  maximumCacheSize: 50000
  expireAfterMs: 60000
  superUsers:
    - User:admin
    - ANONYMOUS
";
        let spec: KafkaSpec = serde_yaml::from_str(yaml).expect("yaml must parse");
        let Some(Authorization::Opa(opa)) = spec.authorization.clone() else {
            panic!("expected Opa variant, got {:?}", spec.authorization);
        };
        assert!(
            opa == OpaAuthorization {
                url: "http://opa.opa.svc:8181/v1/data/kafka/authz/allow".to_string(),
                allow_on_error: Some(true),
                initial_cache_capacity: Some(1000),
                maximum_cache_size: Some(50_000),
                expire_after_ms: Some(60_000),
                super_users: vec!["User:admin".to_string(), "ANONYMOUS".to_string()],
            }
        );

        let json = serde_json::to_string(&spec).unwrap();
        // Every numeric knob must round-trip in camelCase form.
        for want in [
            "\"type\":\"opa\"",
            "\"allowOnError\":true",
            "\"initialCacheCapacity\":1000",
            "\"maximumCacheSize\":50000",
            "\"expireAfterMs\":60000",
        ] {
            assert!(json.contains(want), "case {want:?}; got: {json}");
        }
        let back: KafkaSpec = serde_json::from_str(&json).unwrap();
        assert!(back == spec);
    }

    #[test]
    fn opa_authorization_minimal_omits_optional_fields() {
        // Only `url` is required on the `opa` variant; every other
        // field is `Option<...>` / `Vec<...>` and must be skipped on
        // serialize when `None`/empty so the rendered TOML and the
        // resulting hash are minimal.
        let yaml = r"
kafkaVersion: 0.1.1
authorization:
  type: opa
  url: http://opa.opa.svc:8181/v1/data/kafka/authz/allow
";
        let spec: KafkaSpec = serde_yaml::from_str(yaml).expect("yaml must parse");
        let Some(Authorization::Opa(opa)) = spec.authorization.clone() else {
            panic!("expected Opa variant, got {:?}", spec.authorization);
        };
        assert!(
            opa == OpaAuthorization {
                url: "http://opa.opa.svc:8181/v1/data/kafka/authz/allow".to_string(),
                allow_on_error: None,
                initial_cache_capacity: None,
                maximum_cache_size: None,
                expire_after_ms: None,
                super_users: vec![],
            }
        );

        let json = serde_json::to_string(&spec).unwrap();
        for absent in [
            "allowOnError",
            "initialCacheCapacity",
            "maximumCacheSize",
            "expireAfterMs",
            "superUsers",
        ] {
            assert!(
                !json.contains(absent),
                "{absent} must be omitted when None/empty; got: {json}"
            );
        }
        let back: KafkaSpec = serde_json::from_str(&json).unwrap();
        assert!(back == spec);
    }

    // ── tieredStorage round-trip tests ─────────────────────

    #[test]
    fn tiered_storage_round_trips_through_json() {
        let json = r#"{"kafkaVersion":"0.1.1","tieredStorage":{"type":"Local"}}"#;
        let spec: KafkaSpec = serde_json::from_str(json).unwrap();
        let ts = spec.tiered_storage.as_ref().expect("tieredStorage parsed");
        assert!(ts.kind == TieredStorageType::Local);

        let serialized = serde_json::to_string(&spec).unwrap();
        assert!(
            serialized.contains("\"tieredStorage\":{\"type\":\"Local\"}"),
            "round-trip JSON: {serialized}"
        );
    }

    #[test]
    fn tiered_storage_omitted_when_none() {
        let json = r#"{"kafkaVersion":"0.1.1"}"#;
        let spec: KafkaSpec = serde_json::from_str(json).unwrap();
        assert!(spec.tiered_storage.is_none());
        let j = serde_json::to_string(&spec).unwrap();
        assert!(!j.contains("tieredStorage"), "got: {j}");
    }

    #[test]
    fn tiered_storage_rejects_unknown_type() {
        let json = r#"{"kafkaVersion":"0.1.1","tieredStorage":{"type":"Bogus"}}"#;
        let res: Result<KafkaSpec, _> = serde_json::from_str(json);
        assert!(res.is_err(), "unknown TieredStorageType must fail");
    }

    // ── S3 tiered storage CRD + validation ──────────

    /// Full S3 wire shape (camelCase, nested `s3.credentials`) round-trips
    /// through serde without losing fields.
    #[test]
    fn tiered_storage_s3_round_trips_through_json() {
        let ts = TieredStorage {
            kind: TieredStorageType::S3,
            s3: Some(S3StorageSpec {
                bucket: "b".into(),
                region: "r".into(),
                prefix: Some("p".into()),
                endpoint: Some("http://m:9000".into()),
                credentials: Some(S3Credentials {
                    access_key_id: SecretKeyRef {
                        name: "creds".into(),
                        key: Some("ak".into()),
                    },
                    secret_access_key: SecretKeyRef {
                        name: "creds".into(),
                        key: Some("sk".into()),
                    },
                }),
                allow_http: true,
                multipart_threshold: Some(1024),
                multipart_chunk_size: Some(512),
            }),
            gcs: None,
            metadata_manager: None,
            persistence: None,
        };
        let j = serde_json::to_string(&ts).unwrap();
        for want in [
            "\"type\":\"S3\"",
            "\"s3\"",
            "\"accessKeyId\"",
            "\"secretAccessKey\"",
            "\"allowHttp\":true",
            "\"multipartThreshold\":1024",
        ] {
            assert!(j.contains(want), "case {want:?}; got: {j}");
        }
        let back: TieredStorage = serde_json::from_str(&j).unwrap();
        assert!(back == ts);
    }

    /// `validate` enforces the four wire-shape rules: kind/s3 pairing,
    /// non-empty bucket, non-empty region. Local + no s3 is the only
    /// happy Local case; S3 + populated s3 with non-empty bucket/region
    /// is the only happy S3 case.
    #[test]
    fn tiered_storage_validate_local_ok_only_without_s3() {
        let ok = TieredStorage {
            kind: TieredStorageType::Local,
            s3: None,
            gcs: None,
            metadata_manager: None,
            persistence: None,
        };
        assert!(ok.validate().is_ok());

        let bad = TieredStorage {
            kind: TieredStorageType::Local,
            s3: Some(S3StorageSpec::default()),
            gcs: None,
            metadata_manager: None,
            persistence: None,
        };
        assert!(
            bad.validate().is_err(),
            "type=Local with s3 must be rejected",
        );
    }

    #[test]
    fn tiered_storage_validate_s3_requires_s3_and_non_empty_bucket_region() {
        let missing_s3 = TieredStorage {
            kind: TieredStorageType::S3,
            s3: None,
            gcs: None,
            metadata_manager: None,
            persistence: None,
        };
        assert!(missing_s3.validate().is_err());

        let missing_bucket = TieredStorage {
            kind: TieredStorageType::S3,
            s3: Some(S3StorageSpec {
                bucket: String::new(),
                region: "r".into(),
                ..Default::default()
            }),
            gcs: None,
            metadata_manager: None,
            persistence: None,
        };
        assert!(missing_bucket.validate().is_err());

        let missing_region = TieredStorage {
            kind: TieredStorageType::S3,
            s3: Some(S3StorageSpec {
                bucket: "b".into(),
                region: "  ".into(),
                ..Default::default()
            }),
            gcs: None,
            metadata_manager: None,
            persistence: None,
        };
        assert!(missing_region.validate().is_err());

        let ok = TieredStorage {
            kind: TieredStorageType::S3,
            s3: Some(S3StorageSpec {
                bucket: "b".into(),
                region: "r".into(),
                ..Default::default()
            }),
            gcs: None,
            metadata_manager: None,
            persistence: None,
        };
        assert!(ok.validate().is_ok());
    }

    // ── GCS tiered storage CRD + validation ─────────

    /// Full GCS wire shape (camelCase, nested `gcs.credentials`)
    /// round-trips through serde and serializes with `type=Gcs` + `gcs`.
    #[test]
    fn tiered_storage_gcs_round_trips_through_json() {
        let ts = TieredStorage {
            kind: TieredStorageType::Gcs,
            s3: None,
            gcs: Some(GcsStorageSpec {
                bucket: "b".into(),
                prefix: Some("p".into()),
                endpoint: Some("http://fake-gcs:4443".into()),
                credentials: Some(GcsCredentials {
                    service_account_key: SecretKeyRef {
                        name: "gcs-creds".into(),
                        key: Some("key.json".into()),
                    },
                }),
                allow_http: true,
                multipart_threshold: Some(1024),
                multipart_chunk_size: Some(512),
            }),
            metadata_manager: None,
            persistence: None,
        };
        let j = serde_json::to_string(&ts).unwrap();
        for want in [
            "\"type\":\"Gcs\"",
            "\"gcs\"",
            "\"serviceAccountKey\"",
            "\"allowHttp\":true",
            "\"multipartThreshold\":1024",
        ] {
            assert!(j.contains(want), "case {want:?}; got: {j}");
        }
        let back: TieredStorage = serde_json::from_str(&j).unwrap();
        assert!(back == ts);
    }

    #[test]
    fn tiered_storage_validate_gcs_requires_gcs_and_non_empty_bucket() {
        let missing_gcs = TieredStorage {
            kind: TieredStorageType::Gcs,
            s3: None,
            gcs: None,
            metadata_manager: None,
            persistence: None,
        };
        let err = missing_gcs.validate().unwrap_err();
        assert!(err.contains("type=Gcs requires `gcs`"), "got: {err}");

        let missing_bucket = TieredStorage {
            kind: TieredStorageType::Gcs,
            s3: None,
            gcs: Some(GcsStorageSpec {
                bucket: "  ".into(),
                ..Default::default()
            }),
            metadata_manager: None,
            persistence: None,
        };
        let err = missing_bucket.validate().unwrap_err();
        assert!(err.contains("gcs.bucket is required"), "got: {err}");

        let ok = TieredStorage {
            kind: TieredStorageType::Gcs,
            s3: None,
            gcs: Some(GcsStorageSpec {
                bucket: "b".into(),
                ..Default::default()
            }),
            metadata_manager: None,
            persistence: None,
        };
        assert!(ok.validate().is_ok());
    }

    #[test]
    fn tiered_storage_validate_gcs_must_not_set_s3() {
        let bad = TieredStorage {
            kind: TieredStorageType::Gcs,
            s3: Some(S3StorageSpec::default()),
            gcs: Some(GcsStorageSpec {
                bucket: "b".into(),
                ..Default::default()
            }),
            metadata_manager: None,
            persistence: None,
        };
        let err = bad.validate().unwrap_err();
        assert!(err.contains("type=Gcs must not set `s3`"), "got: {err}");
    }

    #[test]
    fn tiered_storage_validate_local_and_s3_must_not_set_gcs() {
        let local_with_gcs = TieredStorage {
            kind: TieredStorageType::Local,
            s3: None,
            gcs: Some(GcsStorageSpec {
                bucket: "b".into(),
                ..Default::default()
            }),
            metadata_manager: None,
            persistence: None,
        };
        let err = local_with_gcs.validate().unwrap_err();
        assert!(err.contains("type=Local must not set `gcs`"), "got: {err}");

        let s3_with_gcs = TieredStorage {
            kind: TieredStorageType::S3,
            s3: Some(S3StorageSpec {
                bucket: "b".into(),
                region: "r".into(),
                ..Default::default()
            }),
            gcs: Some(GcsStorageSpec {
                bucket: "b".into(),
                ..Default::default()
            }),
            metadata_manager: None,
            persistence: None,
        };
        let err = s3_with_gcs.validate().unwrap_err();
        assert!(err.contains("type=S3 must not set `gcs`"), "got: {err}");
    }

    #[test]
    fn metadata_manager_inmemory_with_topic_is_rejected() {
        let ts = TieredStorage {
            kind: TieredStorageType::Local,
            s3: None,
            gcs: None,
            metadata_manager: Some(MetadataManagerSpec {
                kind: MetadataManagerType::InMemory,
                topic: Some(TopicMetadataManagerSpec {
                    bootstrap: "127.0.0.1:9092".into(),
                    num_partitions: None,
                    replication: None,
                }),
            }),
            persistence: None,
        };
        let err = ts.validate().unwrap_err();
        assert!(err.contains("must not set `topic`"), "got: {err}");
    }

    #[test]
    fn metadata_manager_topic_without_topic_is_valid() {
        // A bare type=Topic with no topic sub-block is valid; the broker
        // fills default bootstrap/partitions from its own config.
        let ts = TieredStorage {
            kind: TieredStorageType::Local,
            s3: None,
            gcs: None,
            metadata_manager: Some(MetadataManagerSpec {
                kind: MetadataManagerType::Topic,
                topic: None,
            }),
            persistence: None,
        };
        assert!(ts.validate().is_ok());
    }

    #[test]
    fn metadata_manager_topic_requires_non_empty_bootstrap() {
        let ts = TieredStorage {
            kind: TieredStorageType::Local,
            s3: None,
            gcs: None,
            metadata_manager: Some(MetadataManagerSpec {
                kind: MetadataManagerType::Topic,
                topic: Some(TopicMetadataManagerSpec {
                    bootstrap: "  ".into(),
                    num_partitions: None,
                    replication: None,
                }),
            }),
            persistence: None,
        };
        let err = ts.validate().unwrap_err();
        assert!(err.contains("bootstrap is required"), "got: {err}");
    }

    #[test]
    fn metadata_manager_topic_rejects_non_positive_partition_count() {
        let ts = TieredStorage {
            kind: TieredStorageType::Local,
            s3: None,
            gcs: None,
            metadata_manager: Some(MetadataManagerSpec {
                kind: MetadataManagerType::Topic,
                topic: Some(TopicMetadataManagerSpec {
                    bootstrap: "127.0.0.1:9094".into(),
                    num_partitions: Some(0),
                    replication: None,
                }),
            }),
            persistence: None,
        };
        let err = ts.validate().unwrap_err();
        assert!(err.contains("numPartitions"), "got: {err}");
    }

    #[test]
    fn metadata_manager_topic_with_defaults_validates() {
        let ts = TieredStorage {
            kind: TieredStorageType::Local,
            s3: None,
            gcs: None,
            metadata_manager: Some(MetadataManagerSpec {
                kind: MetadataManagerType::Topic,
                topic: Some(TopicMetadataManagerSpec {
                    bootstrap: "127.0.0.1:9094".into(),
                    num_partitions: None,
                    replication: None,
                }),
            }),
            persistence: None,
        };
        assert!(ts.validate().is_ok());
    }

    #[test]
    fn persistence_requires_local_kind() {
        let ts = TieredStorage {
            kind: TieredStorageType::S3,
            s3: Some(S3StorageSpec {
                bucket: "b".into(),
                region: "r".into(),
                ..Default::default()
            }),
            gcs: None,
            metadata_manager: None,
            persistence: Some(TieredStoragePersistence {
                size: "50Gi".into(),
                class: None,
                delete_claim: false,
            }),
        };
        let err = ts.validate().unwrap_err();
        assert!(err.contains("persistence is only valid with type=Local"));
    }

    #[test]
    fn persistence_size_must_be_non_empty() {
        let ts = TieredStorage {
            kind: TieredStorageType::Local,
            s3: None,
            gcs: None,
            metadata_manager: None,
            persistence: Some(TieredStoragePersistence {
                size: "  ".into(),
                class: None,
                delete_claim: false,
            }),
        };
        let err = ts.validate().unwrap_err();
        assert!(err.contains("persistence.size is required"));
    }

    #[test]
    fn persistence_with_local_validates() {
        let ts = TieredStorage {
            kind: TieredStorageType::Local,
            s3: None,
            gcs: None,
            metadata_manager: None,
            persistence: Some(TieredStoragePersistence {
                size: "100Gi".into(),
                class: Some("fast-ssd".into()),
                delete_claim: false,
            }),
        };
        assert!(ts.validate().is_ok());
    }

    #[test]
    fn persistence_delete_claim_round_trips() {
        let p = TieredStoragePersistence {
            size: "10Gi".into(),
            class: None,
            delete_claim: true,
        };
        let yaml = serde_yaml::to_string(&p).unwrap();
        assert!(yaml.contains("deleteClaim: true"));
        let back: TieredStoragePersistence = serde_yaml::from_str(&yaml).unwrap();
        assert!(back == p);
    }

    #[test]
    fn persistence_delete_claim_defaults_false() {
        let yaml = "size: 5Gi\n";
        let p: TieredStoragePersistence = serde_yaml::from_str(yaml).unwrap();
        assert!(!p.delete_claim);
    }

    // ── tracing validation ────────────────────────────────

    #[test]
    fn tracing_otlp_without_otlp_block_is_rejected() {
        let t = Tracing {
            kind: TracingType::Otlp,
            otlp: None,
        };
        let err = t.validate().unwrap_err();
        assert!(err.contains("type=Otlp requires `otlp`"), "got: {err}");
    }

    #[test]
    fn tracing_otlp_requires_non_empty_endpoint() {
        let t = Tracing {
            kind: TracingType::Otlp,
            otlp: Some(OtlpTracing {
                endpoint: "   ".into(),
                protocol: None,
                sample_ratio: None,
                service_name: None,
                timeout_secs: None,
            }),
        };
        let err = t.validate().unwrap_err();
        assert!(err.contains("otlp.endpoint is required"), "got: {err}");
    }

    #[test]
    fn tracing_otlp_rejects_out_of_range_sample_ratio() {
        let t = Tracing {
            kind: TracingType::Otlp,
            otlp: Some(OtlpTracing {
                endpoint: "http://otel:4317".into(),
                protocol: None,
                sample_ratio: Some(1.5),
                service_name: None,
                timeout_secs: None,
            }),
        };
        let err = t.validate().unwrap_err();
        assert!(err.contains("otlp.sampleRatio"), "got: {err}");
    }

    #[test]
    fn tracing_otlp_rejects_zero_timeout() {
        let t = Tracing {
            kind: TracingType::Otlp,
            otlp: Some(OtlpTracing {
                endpoint: "http://otel:4317".into(),
                protocol: None,
                sample_ratio: None,
                service_name: None,
                timeout_secs: Some(0),
            }),
        };
        let err = t.validate().unwrap_err();
        assert!(err.contains("otlp.timeoutSecs"), "got: {err}");
    }

    #[test]
    fn tracing_otlp_with_full_spec_validates() {
        let t = Tracing {
            kind: TracingType::Otlp,
            otlp: Some(OtlpTracing {
                endpoint: "http://otel-collector.observability:4317".into(),
                protocol: Some(OtlpProtocol::Grpc),
                sample_ratio: Some(0.1),
                service_name: Some("prod-cluster".into()),
                timeout_secs: Some(5),
            }),
        };
        assert!(t.validate().is_ok());
    }

    #[test]
    fn otlp_protocol_env_value_matches_broker_parse() {
        // The broker's `OtlpProtocol::parse` accepts "grpc" and
        // "http/protobuf" (spec values). Lock both ends.
        assert!(OtlpProtocol::Grpc.as_env_value() == "grpc");
        assert!(OtlpProtocol::HttpProtobuf.as_env_value() == "http/protobuf");
    }
}
