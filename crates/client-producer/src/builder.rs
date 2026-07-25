//! `Producer::builder()` — `bon`-generated builder for `Producer::start`.

use std::{
    collections::HashMap,
    sync::{
        Arc,
        atomic::{AtomicBool, AtomicU8, AtomicU64, AtomicUsize},
    },
    time::Duration,
};

use crabka_client_core::{Client, ClientError};
use crabka_protocol::owned::{
    init_producer_id_request::InitProducerIdRequest,
    init_producer_id_response::InitProducerIdResponse,
};
use dashmap::DashMap;
use refined_type::rule::{GreaterI32, MinMaxU128};
use tokio::sync::{Mutex, Notify, mpsc};
use tokio_util::sync::CancellationToken;

use crate::{
    compression::Compression,
    error::ProducerError,
    partitioner::UniformStickyPartitioner,
    producer::{Acks, Producer, ProducerIdentity},
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

/// Default producer request timeout.
pub const DEFAULT_PRODUCER_REQUEST_TIMEOUT: Duration = Duration::from_secs(30);
/// Default retries after a batch's initial send.
pub const DEFAULT_PRODUCER_RETRIES: i32 = i32::MAX;
/// Default producer retry backoff.
pub const DEFAULT_PRODUCER_RETRY_BACKOFF: Duration = Duration::from_millis(100);
/// Default wall-clock routing retry budget per batch.
pub const DEFAULT_PRODUCER_ROUTING_RETRY_BUDGET: Duration = Duration::from_secs(30);
/// Default producer-ID initialization retry timeout.
pub const DEFAULT_PRODUCER_INIT_RETRY_TIMEOUT: Duration = Duration::from_secs(30);
/// Default producer-ID initialization backoff cap.
pub const DEFAULT_PRODUCER_INIT_MAX_BACKOFF: Duration = Duration::from_secs(1);
/// Default transaction timeout.
pub const DEFAULT_PRODUCER_TRANSACTION_TIMEOUT: Duration = Duration::from_mins(1);

/// Validated producer retry and transaction timing.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ProducerRetryPolicy {
    request_timeout: Duration,
    retries: i32,
    retry_backoff: Duration,
    routing_retry_budget: Duration,
    init_retry_timeout: Duration,
    init_max_backoff: Duration,
    transaction_timeout: Duration,
}

impl ProducerRetryPolicy {
    /// Validate producer retry and transaction timing.
    ///
    /// # Errors
    ///
    /// Returns an error for non-positive durations, retry durations above
    /// `i32::MAX` milliseconds, negative retries, protocol timeouts that are
    /// not whole milliseconds, or an initial retry backoff above its cap.
    pub fn new(
        request_timeout: Duration,
        retries: i32,
        retry_backoff: Duration,
        routing_retry_budget: Duration,
        init_retry_timeout: Duration,
        init_max_backoff: Duration,
        transaction_timeout: Duration,
    ) -> Result<Self, String> {
        let request_timeout = validated_protocol_duration(request_timeout, "request timeout")?;
        let retries = GreaterI32::<-1>::new(retries)
            .map_err(|error| error.to_string())?
            .into_value();
        let retry_backoff = validated_duration(retry_backoff, "producer retry backoff")?;
        let routing_retry_budget =
            validated_duration(routing_retry_budget, "routing retry budget")?;
        let init_retry_timeout = validated_duration(
            init_retry_timeout,
            "producer-ID initialization retry timeout",
        )?;
        let init_max_backoff = validated_duration(
            init_max_backoff,
            "producer-ID initialization maximum backoff",
        )?;
        let transaction_timeout =
            validated_protocol_duration(transaction_timeout, "transaction timeout")?;
        if retry_backoff > init_max_backoff {
            return Err("producer retry backoff exceeds producer-ID backoff cap".to_owned());
        }
        Ok(Self {
            request_timeout,
            retries,
            retry_backoff,
            routing_retry_budget,
            init_retry_timeout,
            init_max_backoff,
            transaction_timeout,
        })
    }

    #[must_use]
    pub const fn request_timeout(self) -> Duration {
        self.request_timeout
    }

    #[must_use]
    pub const fn retries(self) -> i32 {
        self.retries
    }

    #[must_use]
    pub const fn retry_backoff(self) -> Duration {
        self.retry_backoff
    }

    #[must_use]
    pub const fn routing_retry_budget(self) -> Duration {
        self.routing_retry_budget
    }

    #[must_use]
    pub const fn init_retry_timeout(self) -> Duration {
        self.init_retry_timeout
    }

    #[must_use]
    pub const fn init_max_backoff(self) -> Duration {
        self.init_max_backoff
    }

    #[must_use]
    pub const fn transaction_timeout(self) -> Duration {
        self.transaction_timeout
    }

    #[must_use]
    pub fn request_timeout_ms(self) -> i32 {
        protocol_milliseconds(self.request_timeout)
    }

    #[must_use]
    pub fn transaction_timeout_ms(self) -> i32 {
        protocol_milliseconds(self.transaction_timeout)
    }
}

impl Default for ProducerRetryPolicy {
    fn default() -> Self {
        Self::new(
            DEFAULT_PRODUCER_REQUEST_TIMEOUT,
            DEFAULT_PRODUCER_RETRIES,
            DEFAULT_PRODUCER_RETRY_BACKOFF,
            DEFAULT_PRODUCER_ROUTING_RETRY_BUDGET,
            DEFAULT_PRODUCER_INIT_RETRY_TIMEOUT,
            DEFAULT_PRODUCER_INIT_MAX_BACKOFF,
            DEFAULT_PRODUCER_TRANSACTION_TIMEOUT,
        )
        .expect("default producer retry policy is valid")
    }
}

fn validated_duration(value: Duration, name: &str) -> Result<Duration, String> {
    MinMaxU128::<1, { i32::MAX as u128 * 1_000_000 }>::new(value.as_nanos())
        .map(|_| value)
        .map_err(|error| format!("{name}: {error}"))
}

fn validated_protocol_duration(value: Duration, name: &str) -> Result<Duration, String> {
    let milliseconds = MinMaxU128::<1, { i32::MAX as u128 }>::new(value.as_millis())
        .map_err(|error| format!("{name}: {error}"))?
        .into_value();
    let milliseconds = u64::try_from(milliseconds).map_err(|error| format!("{name}: {error}"))?;
    if Duration::from_millis(milliseconds) != value {
        return Err(format!("{name} must be a whole number of milliseconds"));
    }
    Ok(value)
}

fn protocol_milliseconds(value: Duration) -> i32 {
    i32::try_from(value.as_millis()).expect("validated protocol duration")
}

fn is_retriable_coordinator_code(code: i16) -> bool {
    matches!(
        code,
        COORDINATOR_LOAD_IN_PROGRESS | COORDINATOR_NOT_AVAILABLE | NOT_COORDINATOR
    )
}

// cargo-mutants: protocol-default InitProducerId shape, so `-> Default` is equivalent.
#[cfg_attr(test, mutants::skip)]
fn build_init_producer_id_request() -> InitProducerIdRequest {
    InitProducerIdRequest {
        transactional_id: None,
        transaction_timeout_ms: 0,
        ..Default::default()
    }
}

fn next_backoff(backoff: Duration, max_backoff: Duration) -> Duration {
    backoff.saturating_mul(2).min(max_backoff)
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

/// Send `InitProducerId`, retrying on the
/// cold-coordinator codes (14/15/16) and transient `Disconnected` transport
/// errors with capped exponential backoff until the deadline elapses.
///
/// Mirrors the consumer crate's `with_coordinator_retry` shape: on deadline it
/// returns the last response (so the caller's `error_code != 0` handling runs)
/// or surfaces the transport error if the final attempt disconnected.
#[tracing::instrument(level = "info", skip_all, err)]
pub(crate) async fn init_producer_id_with_retry(
    client: &Client,
    request: InitProducerIdRequest,
    retry_timeout: Duration,
    initial_backoff: Duration,
    max_backoff: Duration,
) -> Result<InitProducerIdResponse, ProducerError> {
    let deadline = tokio::time::Instant::now()
        .checked_add(retry_timeout)
        .ok_or(ProducerError::InvalidConfig(
            "producer-ID retry timeout is too large",
        ))?;
    let mut backoff = initial_backoff;
    loop {
        let remaining = deadline.saturating_duration_since(tokio::time::Instant::now());
        let response = tokio::time::timeout(remaining, client.send(request.clone()))
            .await
            .map_err(|_| ProducerError::Client(ClientError::Timeout(retry_timeout)))?;
        let last_outcome = match response {
            Ok(resp) if !is_retriable_coordinator_code(resp.error_code) => return Ok(resp),
            Ok(resp) => Ok(resp),
            Err(error @ ClientError::Disconnected) => Err(error),
            Err(e) => return Err(ProducerError::Client(e)),
        };
        let remaining = deadline.saturating_duration_since(tokio::time::Instant::now());
        if remaining.is_zero() {
            return last_outcome.map_err(ProducerError::Client);
        }
        let sleep_for = backoff.min(remaining);
        tokio::time::sleep(sleep_for).await;
        if tokio::time::Instant::now() >= deadline {
            return last_outcome.map_err(ProducerError::Client);
        }
        backoff = next_backoff(backoff, max_backoff);
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
    // bon builder; each arg is an independent knob
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
    /// # Errors
    /// Returns an error when configuration is invalid, protocol encoding fails, the broker rejects the request, or transport I/O fails.
    pub async fn start(
        #[builder(into)] bootstrap: String,
        #[builder(into, default = "crabka-producer".to_string())] client_id: String,
        #[builder(default = Compression::None)] compression: Compression,
        #[builder(default = true)] enable_idempotence: bool,
        #[builder(default = Acks::One)] acks: Acks,
        #[builder(default = Duration::from_millis(0))] linger: Duration,
        #[builder(default = 16 * 1024)] batch_size: usize,
        #[builder(default = DEFAULT_PRODUCER_REQUEST_TIMEOUT)] request_timeout: Duration,
        #[builder(default = DEFAULT_PRODUCER_RETRIES)] retries: i32,
        #[builder(default = DEFAULT_PRODUCER_RETRY_BACKOFF)] retry_backoff: Duration,
        #[builder(default = DEFAULT_PRODUCER_ROUTING_RETRY_BUDGET)] routing_retry_budget: Duration,
        #[builder(default = DEFAULT_PRODUCER_INIT_RETRY_TIMEOUT)] init_retry_timeout: Duration,
        #[builder(default = DEFAULT_PRODUCER_INIT_MAX_BACKOFF)] init_max_backoff: Duration,
        #[builder(default = 5)] max_in_flight_per_connection: usize,
        #[builder(into)] transactional_id: Option<String>,
        #[builder(default = DEFAULT_PRODUCER_TRANSACTION_TIMEOUT)] transaction_timeout: Duration,
        security: Option<crabka_client_core::security::ClientSecurity>,
    ) -> Result<Self, ProducerError> {
        let retry_policy = ProducerRetryPolicy::new(
            request_timeout,
            retries,
            retry_backoff,
            routing_retry_budget,
            init_retry_timeout,
            init_max_backoff,
            transaction_timeout,
        )
        .map_err(|_| ProducerError::InvalidConfig("invalid producer retry policy"))?;
        let request_timeout = retry_policy.request_timeout();
        let retries = retry_policy.retries();
        let retry_backoff = retry_policy.retry_backoff();
        let routing_retry_budget = retry_policy.routing_retry_budget();

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
            let init = init_producer_id_with_retry(
                &client,
                build_init_producer_id_request(),
                retry_policy.init_retry_timeout(),
                retry_backoff,
                retry_policy.init_max_backoff(),
            )
            .await?;
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
        let txn_recovery_required = Arc::new(AtomicBool::new(false));
        let txn_recovery_generation = Arc::new(AtomicU64::new(0));
        let txn_pid_epoch = Arc::new(Mutex::new(initial_txn_pid_epoch()));

        let sender_handle = tokio::spawn(sender::run(sender::SenderConfig {
            transport: Box::new(ClientTransport::new(client.clone())),
            producer_id,
            producer_epoch,
            acks,
            compression,
            linger,
            request_timeout_ms: retry_policy.request_timeout_ms(),
            retries,
            retry_backoff,
            routing_retry_budget,
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
            txn_recovery_required: Arc::clone(&txn_recovery_required),
            txn_recovery_generation: Arc::clone(&txn_recovery_generation),
        }));

        Ok(Producer {
            client,
            client_id,
            security,
            identity: ProducerIdentity {
                id: producer_id,
                epoch: producer_epoch,
            },
            acks,
            compression,
            batch_size,
            linger,
            request_timeout,
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
            transaction_timeout_ms: retry_policy.transaction_timeout_ms(),
            init_retry_timeout: retry_policy.init_retry_timeout(),
            init_retry_backoff: retry_policy.retry_backoff(),
            init_max_backoff: retry_policy.init_max_backoff(),
            txn_state,
            txn_recovery_required,
            txn_recovery_generation,
            txn_coord_client: Mutex::new(None),
            txn_pid_epoch,
        })
    }
}

#[cfg(test)]
mod security_arg_tests {
    use std::sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    };

    use bytes::BytesMut;
    use crabka_client_core::{
        MockBroker,
        security::{ClientSecurity, SaslCredentials},
    };
    use crabka_protocol::{
        Encode,
        owned::{
            api_versions_request, api_versions_response::ApiVersionsResponse,
            init_producer_id_request,
        },
    };
    use crabka_security::ListenerProtocol;

    use super::*;

    fn encode_v0(response: &impl Encode) -> Vec<u8> {
        let mut bytes = BytesMut::new();
        response.encode(&mut bytes, 0).expect("encode response");
        bytes.to_vec()
    }

    #[test]
    fn coordinator_retry_classifier_matches_cold_start_codes_only() {
        for (_name, code, want) in [
            ("loading", COORDINATOR_LOAD_IN_PROGRESS, true),
            ("unavailable", COORDINATOR_NOT_AVAILABLE, true),
            ("not coordinator", NOT_COORDINATOR, true),
            ("success", 0, false),
            ("invalid request", 42, false),
        ] {
            assert2::assert!(is_retriable_coordinator_code(code) == want);
        }
    }

    #[test]
    fn producer_retry_policy_defaults_and_distinct_values_are_exact() {
        let defaults = ProducerRetryPolicy::default();
        assert2::assert!(
            (
                defaults.request_timeout(),
                defaults.retries(),
                defaults.retry_backoff(),
                defaults.routing_retry_budget(),
                defaults.init_retry_timeout(),
                defaults.init_max_backoff(),
                defaults.transaction_timeout(),
                defaults.request_timeout_ms(),
                defaults.transaction_timeout_ms(),
            ) == (
                Duration::from_secs(30),
                i32::MAX,
                Duration::from_millis(100),
                Duration::from_secs(30),
                Duration::from_secs(30),
                Duration::from_secs(1),
                Duration::from_mins(1),
                30_000,
                60_000,
            )
        );

        let policy = ProducerRetryPolicy::new(
            Duration::from_millis(11),
            12,
            Duration::from_millis(13),
            Duration::from_millis(14),
            Duration::from_millis(15),
            Duration::from_millis(16),
            Duration::from_millis(17),
        )
        .expect("distinct policy");
        assert2::assert!(
            (
                policy.request_timeout(),
                policy.retries(),
                policy.retry_backoff(),
                policy.routing_retry_budget(),
                policy.init_retry_timeout(),
                policy.init_max_backoff(),
                policy.transaction_timeout(),
                policy.request_timeout_ms(),
                policy.transaction_timeout_ms(),
            ) == (
                Duration::from_millis(11),
                12,
                Duration::from_millis(13),
                Duration::from_millis(14),
                Duration::from_millis(15),
                Duration::from_millis(16),
                Duration::from_millis(17),
                11,
                17,
            )
        );
    }

    #[test]
    fn producer_retry_policy_rejects_invalid_bounds() {
        let valid = [
            Duration::from_millis(1),
            Duration::from_millis(1),
            Duration::from_millis(1),
            Duration::from_millis(1),
            Duration::from_millis(1),
            Duration::from_millis(1),
        ];
        for (_name, request, retries, backoff, routing, init, max, transaction) in [
            (
                "zero request",
                Duration::ZERO,
                0,
                valid[0],
                valid[1],
                valid[2],
                valid[3],
                valid[4],
            ),
            (
                "negative retries",
                valid[0],
                -1,
                valid[1],
                valid[2],
                valid[3],
                valid[4],
                valid[5],
            ),
            (
                "zero backoff",
                valid[0],
                0,
                Duration::ZERO,
                valid[2],
                valid[3],
                valid[4],
                valid[5],
            ),
            (
                "zero routing",
                valid[0],
                0,
                valid[1],
                Duration::ZERO,
                valid[3],
                valid[4],
                valid[5],
            ),
            (
                "zero init",
                valid[0],
                0,
                valid[1],
                valid[2],
                Duration::ZERO,
                valid[4],
                valid[5],
            ),
            (
                "zero max",
                valid[0],
                0,
                valid[1],
                valid[2],
                valid[3],
                Duration::ZERO,
                valid[5],
            ),
            (
                "zero transaction",
                valid[0],
                0,
                valid[1],
                valid[2],
                valid[3],
                valid[4],
                Duration::ZERO,
            ),
            (
                "request protocol overflow",
                Duration::from_millis(i32::MAX as u64 + 1),
                0,
                valid[1],
                valid[2],
                valid[3],
                valid[4],
                valid[5],
            ),
            (
                "transaction protocol overflow",
                valid[0],
                0,
                valid[1],
                valid[2],
                valid[3],
                valid[4],
                Duration::from_millis(i32::MAX as u64 + 1),
            ),
            (
                "initial exceeds max",
                valid[0],
                0,
                Duration::from_millis(2),
                valid[2],
                valid[3],
                Duration::from_millis(1),
                valid[5],
            ),
        ] {
            assert2::assert!(
                ProducerRetryPolicy::new(
                    request,
                    retries,
                    backoff,
                    routing,
                    init,
                    max,
                    transaction,
                )
                .is_err()
            );
        }
        assert2::assert!(
            ProducerRetryPolicy::new(
                valid[0],
                0,
                valid[1],
                Duration::MAX,
                valid[3],
                valid[4],
                valid[5],
            )
            .is_err()
        );
    }

    #[test]
    fn producer_retry_policy_names_invalid_retry_duration() {
        let valid = Duration::from_millis(1);
        let oversized = Duration::from_millis(i32::MAX as u64 + 1);
        let error = |backoff, routing, init, max| {
            ProducerRetryPolicy::new(valid, 0, backoff, routing, init, max, valid)
                .expect_err("invalid retry duration")
        };

        assert!(error(oversized, valid, valid, valid).contains("producer retry backoff"));
        assert!(error(valid, oversized, valid, valid).contains("routing retry budget"));
        assert!(
            error(valid, valid, oversized, valid)
                .contains("producer-ID initialization retry timeout")
        );
        assert!(
            error(valid, valid, valid, oversized)
                .contains("producer-ID initialization maximum backoff")
        );
    }

    #[test]
    fn init_producer_id_request_is_idempotent_only_shape() {
        let req = build_init_producer_id_request();

        assert2::assert!((req.transactional_id.as_ref(), req.transaction_timeout_ms) == (None, 0));
    }

    #[test]
    fn retry_backoff_doubles_and_caps() {
        for (_name, current, want) in [
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
            assert2::assert!(next_backoff(current, Duration::from_secs(1)) == want);
        }
    }

    #[test]
    fn validated_acks_forces_idempotence_to_all_and_rejects_zero() {
        for (_name, idempotent, acks, want) in [
            ("idempotent forces all", true, Acks::One, Acks::All),
            ("non-idempotent keeps one", false, Acks::One, Acks::One),
            ("non-idempotent keeps zero", false, Acks::Zero, Acks::Zero),
        ] {
            assert2::assert!(validated_acks(idempotent, acks).unwrap() == want);
        }
        assert2::assert!(validated_acks(true, Acks::Zero).is_err());
    }

    #[test]
    fn disabled_idempotence_identity_uses_kafka_sentinel_values() {
        assert2::assert!(disabled_idempotence_identity() == (-1, -1));
        assert2::assert!(initial_txn_pid_epoch() == (-1, -1));
    }

    #[test]
    fn producer_identity_from_init_maps_error_and_success() {
        let success = InitProducerIdResponse {
            error_code: 0,
            producer_id: 42,
            producer_epoch: 7,
            ..Default::default()
        };
        assert2::assert!(producer_identity_from_init(&success).unwrap() == (42, 7));

        let error = InitProducerIdResponse {
            error_code: 51,
            ..Default::default()
        };
        assert2::assert!(matches!(
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
        assert2::assert!(res.is_err());
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
        assert2::assert!(matches!(
            err,
            ProducerError::Client(ClientError::Timeout(d))
                if d == Duration::from_millis(100)
        ));
    }

    #[tokio::test]
    async fn producer_builder_uses_configured_init_retry_timeout() {
        let attempts = Arc::new(AtomicUsize::new(0));
        let observed = Arc::clone(&attempts);
        let mock = MockBroker::start(move |api_key, _version, _corr_id, _body| {
            if api_key == api_versions_request::API_KEY {
                return Some(encode_v0(&ApiVersionsResponse::default()));
            }
            if api_key == init_producer_id_request::API_KEY {
                observed.fetch_add(1, Ordering::Relaxed);
                return Some(encode_v0(&InitProducerIdResponse {
                    error_code: COORDINATOR_LOAD_IN_PROGRESS,
                    ..Default::default()
                }));
            }
            None
        })
        .await;

        let build = Producer::builder()
            .bootstrap(mock.addr.to_string())
            .request_timeout(Duration::from_millis(100))
            .retry_backoff(Duration::from_millis(10))
            .init_max_backoff(Duration::from_millis(10))
            .init_retry_timeout(Duration::from_millis(1))
            .build();
        let error = tokio::time::timeout(Duration::from_millis(200), build)
            .await
            .expect("configured init retry timeout must bound the build")
            .expect_err("cold coordinator must remain an error");

        mock.stop();
        assert2::assert!(matches!(
            error,
            ProducerError::Server(COORDINATOR_LOAD_IN_PROGRESS)
        ));
        assert2::assert!(attempts.load(Ordering::Relaxed) >= 1);
    }

    #[tokio::test]
    async fn init_retry_timeout_bounds_an_unresponsive_request() {
        let mock = MockBroker::start(|api_key, _version, _corr_id, _body| {
            (api_key == api_versions_request::API_KEY)
                .then(|| encode_v0(&ApiVersionsResponse::default()))
        })
        .await;

        let build = Producer::builder()
            .bootstrap(mock.addr.to_string())
            .request_timeout(Duration::from_secs(5))
            .retry_backoff(Duration::from_millis(1))
            .init_max_backoff(Duration::from_millis(1))
            .init_retry_timeout(Duration::from_millis(10))
            .build();
        let error = tokio::time::timeout(Duration::from_millis(200), build)
            .await
            .expect("init retry timeout must bound an in-flight request")
            .expect_err("unresponsive InitProducerId must time out");

        mock.stop();
        assert2::assert!(matches!(
            error,
            ProducerError::Client(ClientError::Timeout(timeout))
                if timeout == Duration::from_millis(10)
        ));
    }

    #[tokio::test]
    async fn producer_builder_rejects_retry_policy_before_connection_io() {
        let error = Producer::builder()
            .bootstrap("127.0.0.1:1")
            .request_timeout(Duration::ZERO)
            .build()
            .await
            .expect_err("zero request timeout must be invalid config");

        assert2::assert!(matches!(error, ProducerError::InvalidConfig(_)));
    }
}
