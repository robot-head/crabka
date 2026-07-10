//! `Producer::builder()` — `bon`-generated builder for `Producer::start`.

use std::{
    collections::HashMap,
    sync::{
        Arc,
        atomic::{AtomicU8, AtomicUsize},
    },
    time::Duration,
};

use crabka_client_core::{Client, ClientError};
use crabka_protocol::owned::{
    init_producer_id_request::InitProducerIdRequest,
    init_producer_id_response::InitProducerIdResponse,
};
use dashmap::DashMap;
use tokio::sync::{Mutex, Notify, mpsc};
use tokio_util::sync::CancellationToken;

use crate::{
    compression::Compression,
    error::ProducerError,
    partitioner::UniformStickyPartitioner,
    producer::{Acks, Producer},
    sender,
    transactional::TxnState,
    transport::ClientTransport,
};

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

#[allow(clippy::field_reassign_with_default)]
// cargo-mutants: protocol-default InitProducerId shape, so `-> Default` is equivalent.
#[cfg_attr(test, mutants::skip)]
fn build_init_producer_id_request() -> InitProducerIdRequest {
    let mut req = InitProducerIdRequest::default();
    req.transactional_id = None;
    req.transaction_timeout_ms = 0;
    req
}

fn retry_deadline_elapsed(start: tokio::time::Instant, timeout: Duration) -> bool {
    start.elapsed() >= timeout
}

fn next_backoff(backoff: Duration) -> Duration {
    const MAX_BACKOFF: Duration = Duration::from_secs(1);
    (backoff * 2).min(MAX_BACKOFF)
}

fn validated_acks(enable_idempotence: bool, acks: Acks) -> Result<Acks, ProducerError> {
    if enable_idempotence && acks == Acks::Zero {
        return Err(ProducerError::InvalidConfig(
            "enable_idempotence=true requires acks=all (not Zero)",
        ));
    }
    Ok(if enable_idempotence { Acks::All } else { acks })
}

fn disabled_idempotence_identity() -> (i64, i16) {
    (-1, -1)
}

fn initial_txn_pid_epoch() -> (i64, i16) {
    (-1, -1)
}

fn producer_identity_from_init(init: &InitProducerIdResponse) -> Result<(i64, i16), ProducerError> {
    if init.error_code != 0 {
        return Err(ProducerError::Server(init.error_code));
    }
    Ok((init.producer_id, init.producer_epoch))
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
#[tracing::instrument(level = "info", skip_all, err)]
async fn init_producer_id(client: &Client) -> Result<InitProducerIdResponse, ProducerError> {
    let start = tokio::time::Instant::now();
    let mut backoff = Duration::from_millis(100);
    loop {
        match client.send(build_init_producer_id_request()).await {
            Ok(resp) if !is_retriable_coordinator_code(resp.error_code) => return Ok(resp),
            Ok(resp) => {
                // Cold coordinator: retry until the deadline, then surface the
                // last response so the caller maps it to ProducerError::Server.
                if retry_deadline_elapsed(start, INIT_PRODUCER_ID_RETRY_TIMEOUT) {
                    return Ok(resp);
                }
            }
            Err(ClientError::Disconnected) => {
                // Transient transport failure (e.g. the broker dropped the
                // connection while still loading): retry until the deadline,
                // then surface the disconnect.
                if retry_deadline_elapsed(start, INIT_PRODUCER_ID_RETRY_TIMEOUT) {
                    return Err(ProducerError::Client(ClientError::Disconnected));
                }
            }
            Err(e) => return Err(ProducerError::Client(e)),
        }
        tokio::time::sleep(backoff).await;
        backoff = next_backoff(backoff);
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
    #[tracing::instrument(
        level = "info",
        skip_all,
        fields(
            bootstrap = %bootstrap,
            client_id = %client_id,
            acks = ?acks,
            enable_idempotence,
            transactional_id = transactional_id.as_deref(),
        ),
        err,
    )]
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
        let acks = validated_acks(enable_idempotence, acks)?;

        // 1. Build inner client. `security` is cloned (not moved) so it can be
        //    retained on the `Producer` and reused for the secondary
        //    coordinator connections opened by the transactional path.
        let client = Client::builder()
            .bootstrap(bootstrap)
            .client_id(client_id.clone())
            .connect_timeout(request_timeout)
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
            producer_identity_from_init(&init)?
        } else {
            disabled_idempotence_identity()
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
        let txn_pid_epoch = Arc::new(Mutex::new(initial_txn_pid_epoch()));

        let sender_handle = tokio::spawn(sender::run(sender::SenderConfig {
            transport: Box::new(ClientTransport::new(client.clone())),
            producer_id,
            producer_epoch,
            acks,
            compression,
            linger,
            request_timeout,
            retry_backoff,
            max_in_flight: max_in_flight_per_connection,
            metadata_cache: Arc::clone(&metadata_cache),
            partition_leaders: Arc::clone(&partition_leaders),
            partitioner: Arc::clone(&partitioner),
            accumulators: Arc::clone(&accumulators),
            next_seq: Arc::clone(&next_seq),
            state: Arc::clone(&state),
            wake_rx,
            flush_notify: Arc::clone(&flush_notify),
            in_flight: Arc::clone(&in_flight),
            shutdown: shutdown.clone(),
            transactional_id: transactional_id.clone(),
            txn_state: Arc::clone(&txn_state),
            txn_pid_epoch: Arc::clone(&txn_pid_epoch),
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
    use assert2::assert;
    use crabka_client_core::{
        MockBroker,
        security::{ClientSecurity, SaslCredentials},
    };
    use crabka_security::ListenerProtocol;

    use super::*;

    #[test]
    fn coordinator_retry_classifier_matches_cold_start_codes_only() {
        for (name, code, want) in [
            ("loading", COORDINATOR_LOAD_IN_PROGRESS, true),
            ("unavailable", COORDINATOR_NOT_AVAILABLE, true),
            ("not coordinator", NOT_COORDINATOR, true),
            ("success", 0, false),
            ("invalid request", 42, false),
        ] {
            assert!(is_retriable_coordinator_code(code) == want, "case {name}");
        }
    }

    #[test]
    fn init_producer_id_request_is_idempotent_only_shape() {
        let req = build_init_producer_id_request();

        assert!((req.transactional_id.as_ref(), req.transaction_timeout_ms) == (None, 0));
    }

    #[tokio::test(start_paused = true)]
    async fn retry_helpers_preserve_deadline_and_backoff_boundaries() {
        let start = tokio::time::Instant::now();
        assert!(!retry_deadline_elapsed(start, Duration::from_secs(30)));
        tokio::time::advance(Duration::from_secs(30)).await;
        assert!(retry_deadline_elapsed(start, Duration::from_secs(30)));
        for (name, current, want) in [
            (
                "double below cap",
                Duration::from_millis(100),
                Duration::from_millis(200),
            ),
            (
                "double to cap",
                Duration::from_millis(800),
                Duration::from_secs(1),
            ),
            (
                "remain capped",
                Duration::from_secs(1),
                Duration::from_secs(1),
            ),
        ] {
            assert!(next_backoff(current) == want, "case {name}");
        }
    }

    #[test]
    fn validated_acks_forces_idempotence_to_all_and_rejects_zero() {
        for (name, idempotent, acks, want) in [
            ("idempotent forces all", true, Acks::One, Acks::All),
            ("non-idempotent keeps one", false, Acks::One, Acks::One),
            ("non-idempotent keeps zero", false, Acks::Zero, Acks::Zero),
        ] {
            assert!(
                validated_acks(idempotent, acks).unwrap() == want,
                "case {name}"
            );
        }
        assert!(validated_acks(true, Acks::Zero).is_err());
    }

    #[test]
    fn disabled_idempotence_identity_uses_kafka_sentinel_values() {
        assert!(disabled_idempotence_identity() == (-1, -1));
        assert!(initial_txn_pid_epoch() == (-1, -1));
    }

    #[test]
    fn producer_identity_from_init_maps_error_and_success() {
        let success = InitProducerIdResponse {
            error_code: 0,
            producer_id: 42,
            producer_epoch: 7,
            ..Default::default()
        };
        assert!(producer_identity_from_init(&success).unwrap() == (42, 7));

        let error = InitProducerIdResponse {
            error_code: 51,
            ..Default::default()
        };
        assert!(matches!(
            producer_identity_from_init(&error),
            Err(ProducerError::Server(51))
        ));
    }

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

    #[tokio::test]
    async fn producer_builder_uses_request_timeout_for_initial_connect_handshake() {
        let mock = MockBroker::start(|_api_key, _version, _corr_id, _body| None).await;

        let build = Producer::builder()
            .bootstrap(mock.addr.to_string())
            .request_timeout(Duration::from_millis(100))
            .build();
        let res = tokio::time::timeout(Duration::from_millis(500), build).await;

        mock.stop();

        let err = res
            .expect("producer build should not retain the 30s connect timeout")
            .expect_err("silent broker must time out during build");
        assert!(
            matches!(
                err,
                ProducerError::Client(ClientError::Timeout(d))
                    if d == Duration::from_millis(100)
            ),
            "expected 100ms timeout, got {err:?}"
        );
    }
}
