//! `Producer::builder()` — `bon`-generated builder for `Producer::start`.

use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicU8, AtomicUsize};
use std::time::Duration;

use dashmap::DashMap;
use tokio::sync::{Mutex, Notify, mpsc};
use tokio_util::sync::CancellationToken;

use crabka_client_core::Client;
use crabka_protocol::owned::init_producer_id_request::InitProducerIdRequest;

use crate::compression::Compression;
use crate::error::ProducerError;
use crate::partitioner::UniformStickyPartitioner;
use crate::producer::{Acks, Producer};
use crate::sender;
use crate::transactional::TxnState;

#[bon::bon]
impl Producer {
    /// Build a [`Producer`] pointed at the given bootstrap address.
    ///
    /// `enable_idempotence` defaults to `true`, which forces `acks=All`.
    /// Setting `acks=Zero` together with idempotence is rejected with
    /// [`ProducerError::InvalidConfig`].
    #[builder(start_fn = builder, finish_fn = build)]
    #[allow(clippy::too_many_arguments)] // bon builder; each arg is an independent knob
    pub async fn start(
        #[builder(into)] bootstrap: String,
        #[builder(into, default = "crabka-producer".to_string())] client_id: String,
        #[builder(default = Compression::None)] compression: Compression,
        #[builder(default = true)] enable_idempotence: bool,
        #[builder(default = Acks::One)] acks: Acks,
        #[builder(default = Duration::from_millis(0))] linger: Duration,
        #[builder(default = 16 * 1024)] batch_size: usize,
        #[builder(default = Duration::from_secs(30))] request_timeout: Duration,
        #[builder(default = i32::MAX)] retries: i32,
        #[builder(default = Duration::from_millis(100))] retry_backoff: Duration,
        #[builder(default = 5)] max_in_flight_per_connection: usize,
        #[builder(into)] transactional_id: Option<String>,
        #[builder(default = std::time::Duration::new(60, 0))] transaction_timeout: Duration,
    ) -> Result<Self, ProducerError> {
        // Validate config: idempotence forces acks=All, and acks=Zero is
        // incompatible with idempotence.
        if enable_idempotence && acks == Acks::Zero {
            return Err(ProducerError::InvalidConfig(
                "enable_idempotence=true requires acks=all (not Zero)",
            ));
        }
        let acks = if enable_idempotence { Acks::All } else { acks };

        // 1. Build inner client.
        let client = Client::builder()
            .bootstrap(bootstrap)
            .client_id(client_id.clone())
            .request_timeout(request_timeout)
            .build()
            .await?;

        // 2. InitProducerId if idempotence on.
        let (producer_id, producer_epoch) = if enable_idempotence {
            let init = client
                .send(InitProducerIdRequest {
                    transactional_id: None,
                    transaction_timeout_ms: 0,
                    ..Default::default()
                })
                .await?;
            if init.error_code != 0 {
                return Err(ProducerError::Server(init.error_code));
            }
            (init.producer_id, init.producer_epoch)
        } else {
            (-1, -1)
        };

        // 3. Spawn the sender.
        let (wake_tx, wake_rx) = mpsc::channel(16);
        let shutdown = CancellationToken::new();
        let state = Arc::new(AtomicU8::new(0));
        let metadata_cache = Arc::new(Mutex::new(HashMap::new()));
        let accumulators = Arc::new(DashMap::new());
        let next_seq = Arc::new(DashMap::new());
        let partitioner = Arc::new(UniformStickyPartitioner::new());
        let flush_notify = Arc::new(Notify::new());
        let in_flight = Arc::new(AtomicUsize::new(0));

        let txn_state = Arc::new(Mutex::new(TxnState::Uninitialized));
        let txn_pid_epoch = Arc::new(Mutex::new((-1i64, -1i16)));

        let sender_handle = tokio::spawn(sender::run(sender::SenderConfig {
            client: client.clone(),
            producer_id,
            producer_epoch,
            acks,
            compression,
            linger,
            request_timeout,
            retries,
            retry_backoff,
            metadata_cache: metadata_cache.clone(),
            accumulators: accumulators.clone(),
            next_seq: next_seq.clone(),
            state: state.clone(),
            wake_rx,
            flush_notify: flush_notify.clone(),
            in_flight: in_flight.clone(),
            shutdown: shutdown.clone(),
            transactional_id: transactional_id.clone(),
            txn_state: txn_state.clone(),
            txn_pid_epoch: txn_pid_epoch.clone(),
        }));

        Ok(Producer {
            client,
            client_id,
            producer_id,
            producer_epoch,
            acks,
            compression,
            batch_size,
            linger,
            request_timeout,
            retries,
            retry_backoff,
            max_in_flight: max_in_flight_per_connection,
            metadata_cache,
            accumulators,
            next_seq,
            partitioner,
            state,
            wake_tx,
            flush_notify,
            in_flight,
            sender_shutdown: shutdown,
            sender_handle: Some(sender_handle),
            transactional_id,
            transaction_timeout,
            txn_state,
            txn_coord_client: Mutex::new(None),
            txn_pid_epoch,
        })
    }
}
