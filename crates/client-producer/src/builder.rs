//! `Producer::builder()` — `bon`-generated builder for `Producer::start`.

use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicU8, AtomicUsize};
use std::time::Duration;

use dashmap::DashMap;
use tokio::sync::{Mutex, Notify, mpsc};
use tokio_util::sync::CancellationToken;

use crabka_client_core::{Client, ClientError};
use crabka_protocol::owned::init_producer_id_request::InitProducerIdRequest;
use crabka_protocol::owned::init_producer_id_response::InitProducerIdResponse;

use crate::compression::Compression;
use crate::error::ProducerError;
use crate::partitioner::UniformStickyPartitioner;
use crate::producer::{Acks, Producer};
use crate::sender;
use crate::transactional::TxnState;

/// Retriable cold-coordinator error codes for `InitProducerId`. The broker is
/// loading its coordinator state (`14`), the coordinator is not yet available
/// (`15`), or it has moved to another broker (`16`). At cluster startup a
/// conformant client retries these with backoff rather than failing the build.
const COORDINATOR_LOAD_IN_PROGRESS: i16 = 14;
const COORDINATOR_NOT_AVAILABLE: i16 = 15;
const NOT_COORDINATOR: i16 = 16;

/// How long [`init_producer_id`] keeps retrying a cold coordinator before
/// surfacing the last response. Mirrors a typical client `request.timeout.ms`
/// and the consumer crate's `COORDINATOR_RETRY_TIMEOUT`.
const INIT_PRODUCER_ID_RETRY_TIMEOUT: Duration = Duration::from_secs(30);

fn is_retriable_coordinator_code(code: i16) -> bool {
    matches!(
        code,
        COORDINATOR_LOAD_IN_PROGRESS | COORDINATOR_NOT_AVAILABLE | NOT_COORDINATOR
    )
}

/// Send `InitProducerId` for an idempotent-only producer, retrying on the
/// cold-coordinator codes (14/15/16) and transient `Disconnected` transport
/// errors with capped exponential backoff until the deadline elapses.
///
/// Mirrors the consumer crate's `with_coordinator_retry` shape: on deadline it
/// returns the last response (so the caller's `error_code != 0` handling runs)
/// or surfaces the transport error if the final attempt disconnected. Idempotent
/// producers carry no `transactional_id`, so the id is allocated from any broker
/// — no `FindCoordinator` routing is needed here.
async fn init_producer_id(client: &Client) -> Result<InitProducerIdResponse, ProducerError> {
    const MAX_BACKOFF: Duration = Duration::from_secs(1);
    let start = tokio::time::Instant::now();
    let mut backoff = Duration::from_millis(100);
    loop {
        match client
            .send(InitProducerIdRequest {
                transactional_id: None,
                transaction_timeout_ms: 0,
                ..Default::default()
            })
            .await
        {
            Ok(resp) if !is_retriable_coordinator_code(resp.error_code) => return Ok(resp),
            Ok(resp) => {
                // Cold coordinator: retry until the deadline, then surface the
                // last response so the caller maps it to ProducerError::Server.
                if start.elapsed() >= INIT_PRODUCER_ID_RETRY_TIMEOUT {
                    return Ok(resp);
                }
            }
            Err(ClientError::Disconnected) => {
                // Transient transport failure (e.g. the broker dropped the
                // connection while still loading): retry until the deadline,
                // then surface the disconnect.
                if start.elapsed() >= INIT_PRODUCER_ID_RETRY_TIMEOUT {
                    return Err(ProducerError::Client(ClientError::Disconnected));
                }
            }
            Err(e) => return Err(ProducerError::Client(e)),
        }
        tokio::time::sleep(backoff).await;
        backoff = (backoff * 2).min(MAX_BACKOFF);
    }
}

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
        security: Option<crabka_client_core::security::ClientSecurity>,
    ) -> Result<Self, ProducerError> {
        // Validate config: idempotence forces acks=All, and acks=Zero is
        // incompatible with idempotence.
        if enable_idempotence && acks == Acks::Zero {
            return Err(ProducerError::InvalidConfig(
                "enable_idempotence=true requires acks=all (not Zero)",
            ));
        }
        let acks = if enable_idempotence { Acks::All } else { acks };

        // 1. Build inner client. `security` is cloned (not moved) so it can be
        //    retained on the `Producer` and reused for the secondary
        //    coordinator connections opened by the transactional path.
        let client = Client::builder()
            .bootstrap(bootstrap)
            .client_id(client_id.clone())
            .request_timeout(request_timeout)
            .maybe_security(security.clone())
            .build()
            .await?;

        // 2. InitProducerId if idempotence on.
        //
        // Idempotent-only producers (no transactional_id) allocate a producer
        // id from *any* broker — no FindCoordinator routing to a transaction
        // coordinator is required. But at cluster startup the broker can still
        // transiently return COORDINATOR_LOAD_IN_PROGRESS (14) /
        // COORDINATOR_NOT_AVAILABLE (15) / NOT_COORDINATOR (16) while its
        // internal state loads, and a conformant client retries these with
        // backoff rather than surfacing the first error. `init_producer_id`
        // does that retry and returns the final response.
        let (producer_id, producer_epoch) = if enable_idempotence {
            let init = init_producer_id(&client).await?;
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
        let partition_leaders = Arc::new(DashMap::new());
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
            partition_leaders: partition_leaders.clone(),
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
            security,
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
            partition_leaders,
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

#[cfg(test)]
mod security_arg_tests {
    use super::*;
    use assert2::assert;
    use crabka_client_core::security::{ClientSecurity, SaslCredentials};
    use crabka_security::ListenerProtocol;

    #[tokio::test]
    async fn producer_builder_accepts_security() {
        let security = ClientSecurity {
            protocol: ListenerProtocol::SaslPlaintext,
            tls: None,
            sasl: Some(SaslCredentials::Plain {
                username: "u".into(),
                password: "p".into(),
            }),
            sasl_host: None,
        };
        // 127.0.0.1:1 is unroutable for a listener; with idempotence on the
        // build issues an InitProducerId, which must fail at connect —
        // proving the security arg is threaded (not a type error). A lazy
        // (non-idempotent) build would not connect, so keep idempotence on.
        let res = Producer::builder()
            .bootstrap("127.0.0.1:1")
            .request_timeout(std::time::Duration::from_millis(500))
            .security(security)
            .build()
            .await;
        assert!(res.is_err(), "connect to closed port must fail");
    }
}
