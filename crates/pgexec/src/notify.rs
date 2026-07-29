//! The in-process `LISTEN`/`NOTIFY` bus.
//!
//! One [`NotifyBus`] lives on the engine (shared by every handle produced by
//! `SqlEngine::clone_handle`); every connection registers once and gets a
//! [`NotifySessionHandle`] plus the receiving end of its own bounded queue. The
//! wire loop owns the receiver and pushes `NotificationResponse` messages when
//! the connection is idle.
//!
//! Publishing is deliberately **two-phase and all-or-nothing**:
//! [`NotifyBus::prepare_publish`] reserves one queue permit per (notification,
//! listener) pair and hands back a [`PreparedPublish`]; [`PreparedPublish::send`]
//! then pushes through the reserved permits and cannot fail. The session layer
//! reserves *before* the transaction's durable commit and sends *after*, so a
//! listener whose queue is full fails the **notifying** transaction (54000)
//! instead of silently dropping a notification or disconnecting the listener.
//! This is PostgreSQL's rule: the notifier pays.
//!
//! Every publication funnels through the single private `address` seam, which
//! resolves already-built [`Notification`]s to the queues of their channel's
//! current listeners — plus, for a committing transaction, the subscriptions it
//! has staged but not yet published
//! ([`NotifySessionHandle::prepare_publish_with_pending`]), so its own `LISTEN`
//! reaches its own `NOTIFY` without any other publisher being able to address a
//! subscription that has not committed.
//!
//! Local publication reserves a permit on each addressed queue; the cross-node
//! transport re-injects remote notifications through
//! [`NotifyBus::deliver_remote`], which shares the addressing but deliberately
//! not the reservation discipline — see its documentation.

use std::{
    collections::{HashMap, HashSet},
    sync::{Arc, Mutex},
};

use crabka_pgwire::engine::Notification;
use tokio::sync::mpsc;

/// Per-session queue capacity. A listener that falls this far behind makes the
/// next notifying transaction fail rather than losing a notification.
pub const NOTIFY_QUEUE_CAPACITY: usize = 16_384;

/// Maximum `NOTIFY` payload length in bytes (PostgreSQL's
/// `NOTIFY_PAYLOAD_MAX_LENGTH - 1`). PostgreSQL 18 accepts a 7999-byte payload
/// and rejects an 8000-byte one with 22023.
pub const MAX_PAYLOAD_BYTES: usize = 7999;

/// Maximum channel-name length in bytes (PostgreSQL's `NAMEDATALEN - 1`).
/// PostgreSQL 18 accepts a 63-byte channel name and rejects a 64-byte one.
pub const MAX_CHANNEL_BYTES: usize = 63;

/// A `NOTIFY` that cannot be queued.
///
/// The session layer maps these onto wire errors with [`NotifyError::sqlstate`];
/// all of them are raised by the *notifying* backend, never delivered to a
/// listener.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum NotifyError {
    /// `NOTIFY ""` — PostgreSQL rejects an empty channel name (22023).
    #[error("channel name cannot be empty")]
    EmptyChannel,
    /// A channel name at or beyond `NAMEDATALEN` (22023).
    #[error("channel name too long")]
    ChannelNameTooLong,
    /// A payload at or beyond `NOTIFY_PAYLOAD_MAX_LENGTH` (22023).
    #[error("payload string too long")]
    PayloadTooLong,
    /// A listener's queue is full; the notifying transaction fails (54000).
    #[error("too many notifications in the NOTIFY queue")]
    QueueFull,
}

impl NotifyError {
    /// The five-character SQLSTATE this error reports on the wire.
    #[must_use]
    pub fn sqlstate(&self) -> &'static str {
        match self {
            NotifyError::EmptyChannel
            | NotifyError::ChannelNameTooLong
            | NotifyError::PayloadTooLong => "22023",
            NotifyError::QueueFull => "54000",
        }
    }
}

/// Validate one `NOTIFY` channel/payload pair.
///
/// Callers validate at queue time (when the statement runs) so the error is
/// reported against the statement that wrote it, exactly as PostgreSQL does.
///
/// # Errors
///
/// [`NotifyError::EmptyChannel`], [`NotifyError::ChannelNameTooLong`] or
/// [`NotifyError::PayloadTooLong`] when the pair is not queueable.
pub fn validate(channel: &str, payload: &str) -> Result<(), NotifyError> {
    if channel.is_empty() {
        return Err(NotifyError::EmptyChannel);
    }
    if channel.len() > MAX_CHANNEL_BYTES {
        return Err(NotifyError::ChannelNameTooLong);
    }
    if payload.len() > MAX_PAYLOAD_BYTES {
        return Err(NotifyError::PayloadTooLong);
    }
    Ok(())
}

/// What became of a batch handed to [`NotifyBus::deliver_remote`].
///
/// Both counts are per (notification, listener) pair, so one notification on a
/// channel with three listeners contributes three. Listeners whose receiver was
/// already dropped are in neither count.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct RemoteDelivery {
    /// Deliveries that reached a listener's queue.
    pub delivered: usize,
    /// Deliveries lost because a listener's queue was full.
    pub dropped: usize,
}

/// One registered connection: the sending end of its notification queue. The
/// backend pid lives on the session's handle, since it identifies the *sender*
/// of a notification, not its recipient.
struct SessionSlot {
    tx: mpsc::Sender<Notification>,
}

/// The bus state: registered sessions and the listener set of each channel.
#[derive(Default)]
struct BusInner {
    next_id: u64,
    sessions: HashMap<u64, SessionSlot>,
    channels: HashMap<String, HashSet<u64>>,
}

/// The engine-wide `LISTEN`/`NOTIFY` bus.
pub struct NotifyBus {
    capacity: usize,
    inner: Mutex<BusInner>,
}

impl Default for NotifyBus {
    fn default() -> Self {
        Self::new()
    }
}

impl NotifyBus {
    /// A bus whose per-session queues hold [`NOTIFY_QUEUE_CAPACITY`] entries.
    #[must_use]
    pub fn new() -> Self {
        Self::with_capacity(NOTIFY_QUEUE_CAPACITY)
    }

    /// A bus with a non-default per-session queue capacity (tests drive the
    /// queue-full path with a tiny capacity).
    ///
    /// # Panics
    ///
    /// Panics when `capacity` is zero — a zero-capacity bounded channel cannot
    /// hold a reservation.
    #[must_use]
    pub fn with_capacity(capacity: usize) -> Self {
        assert!(capacity > 0, "notify queue capacity must be positive");
        NotifyBus {
            capacity,
            inner: Mutex::new(BusInner::default()),
        }
    }

    /// Register a connection with backend pid `pid`.
    ///
    /// Returns the session's handle (dropping it deregisters the session and
    /// removes it from every channel) and the receiving end of its queue, which
    /// the wire loop drains. An associated function rather than a method because
    /// the handle keeps the bus alive and `&Arc<Self>` is not a valid receiver.
    ///
    /// # Panics
    ///
    /// Panics if the bus mutex was poisoned by a panicking publisher.
    pub fn register(
        bus: &Arc<Self>,
        pid: i32,
    ) -> (NotifySessionHandle, mpsc::Receiver<Notification>) {
        let (tx, rx) = mpsc::channel(bus.capacity);
        let id = {
            let mut inner = bus.lock();
            inner.next_id += 1;
            let id = inner.next_id;
            inner.sessions.insert(id, SessionSlot { tx });
            id
        };
        (
            NotifySessionHandle {
                bus: Arc::clone(bus),
                id,
                pid,
            },
            rx,
        )
    }

    /// Reserve queue space for a whole batch of notifications sent by `pid`.
    ///
    /// Every `(channel, payload)` is validated, then fanned out to the channel's
    /// current listeners and a permit reserved on each. Either every permit is
    /// held by the returned [`PreparedPublish`] or nothing is reserved at all.
    ///
    /// # Errors
    ///
    /// The validation errors of [`validate`], or [`NotifyError::QueueFull`] when
    /// any listener's queue has no room.
    pub fn prepare_publish(
        &self,
        pid: i32,
        batch: &[(String, String)],
    ) -> Result<PreparedPublish, NotifyError> {
        self.prepare_publish_as(pid, batch, None)
    }

    /// [`Self::prepare_publish`], but the copies of the `session` given as
    /// `(id, subscriptions)` are addressed by that set — the channels it *will*
    /// listen on — instead of by the set the bus currently publishes to it.
    ///
    /// This is how a committing transaction's own `LISTEN` reaches its own
    /// `NOTIFY` (PostgreSQL applies pending listens before queueing the
    /// transaction's notifications) **without** staging that subscription on the
    /// bus: a concurrent publisher still sees only committed subscriptions, so a
    /// `LISTEN` that later rolls back can never have been delivered to.
    fn prepare_publish_as(
        &self,
        pid: i32,
        batch: &[(String, String)],
        session: Option<(u64, &HashSet<String>)>,
    ) -> Result<PreparedPublish, NotifyError> {
        for (channel, payload) in batch {
            validate(channel, payload)?;
        }
        let notifications: Vec<Notification> = batch
            .iter()
            .map(|(channel, payload)| Notification {
                process_id: pid,
                channel: channel.clone(),
                payload: payload.clone(),
            })
            .collect();
        self.fan_out(&notifications, session)
    }

    /// Deliver notifications that originated on another node.
    ///
    /// This is the re-injection point for the cross-node transport: a
    /// [`Notification`] decoded from the range-0 log arrives here already
    /// addressed and already validated by the node that published it, carrying
    /// the *originating* backend pid, which PostgreSQL clients see unchanged.
    ///
    /// **Delivery is best-effort, unlike the local path.** Local publication is
    /// two-phase precisely so a full listener queue fails the notifying
    /// transaction with 54000 — the notifier pays, as in PostgreSQL. A remote
    /// publisher cannot hold permits on this node's queues, and by the time the
    /// record is read off the log its transaction has long since committed on
    /// another node; there is nothing left to fail. So each listener is treated
    /// on its own: whoever has room receives, whoever is full loses that
    /// notification, and the count comes back in the returned
    /// [`RemoteDelivery`] (and a `warn!`) for the caller to meter. This never
    /// blocks and never fails.
    ///
    /// A listener whose receiver has been dropped is skipped without counting
    /// as a drop, exactly as on the local path: a closed queue belongs to a
    /// connection that is already gone.
    ///
    /// Self-delivery needs no special case — a session listening on this node
    /// is an ordinary listener whatever node published to it.
    ///
    /// # Panics
    ///
    /// Panics if the bus mutex was poisoned by a panicking publisher.
    pub fn deliver_remote(&self, notifications: &[Notification]) -> RemoteDelivery {
        let mut delivery = RemoteDelivery::default();
        for (tx, notification) in self.address(notifications, None) {
            match tx.try_send(notification) {
                Ok(()) => delivery.delivered += 1,
                Err(mpsc::error::TrySendError::Full(_)) => delivery.dropped += 1,
                Err(mpsc::error::TrySendError::Closed(_)) => {}
            }
        }
        if delivery.dropped > 0 {
            tracing::warn!(
                dropped = delivery.dropped,
                delivered = delivery.delivered,
                "dropped remote notifications for listeners with a full queue"
            );
        }
        delivery
    }

    /// The single publication funnel: address each notification to the current
    /// listeners of its channel and reserve one queue permit per listener.
    ///
    /// A listener whose receiver has been dropped (its connection is gone but
    /// its handle has not been dropped yet) is skipped: a closed queue is not
    /// the notifier's problem. A *full* queue is, and aborts the whole batch —
    /// permits reserved so far are released when the partial vector drops.
    fn fan_out(
        &self,
        notifications: &[Notification],
        session: Option<(u64, &HashSet<String>)>,
    ) -> Result<PreparedPublish, NotifyError> {
        let targets = self.address(notifications, session);
        let mut permits = Vec::with_capacity(targets.len());
        for (tx, notification) in targets {
            match tx.try_reserve_owned() {
                Ok(permit) => permits.push((permit, notification)),
                Err(mpsc::error::TrySendError::Full(_)) => return Err(NotifyError::QueueFull),
                Err(mpsc::error::TrySendError::Closed(_)) => {}
            }
        }
        Ok(PreparedPublish { permits })
    }

    /// Pair each notification with the queue of every session listening on its
    /// channel — the one place channel membership is resolved, shared by the
    /// local and remote paths.
    ///
    /// `session` overrides one session's membership with the subscription set it
    /// will have after its open transaction commits (see
    /// [`Self::prepare_publish_as`]); that session is then addressed by the
    /// override alone, never by what the bus currently holds for it.
    ///
    /// Senders are collected under the lock and used outside it: `try_reserve_owned`
    /// is non-blocking but takes an owned sender, and holding the bus mutex
    /// across the fan-out would serialize every publisher against every LISTEN.
    fn address(
        &self,
        notifications: &[Notification],
        session: Option<(u64, &HashSet<String>)>,
    ) -> Vec<(mpsc::Sender<Notification>, Notification)> {
        let inner = self.lock();
        let mut targets = Vec::new();
        for notification in notifications {
            if let Some(listeners) = inner.channels.get(&notification.channel) {
                for id in listeners {
                    if session.is_some_and(|(overridden, _)| overridden == *id) {
                        continue;
                    }
                    if let Some(slot) = inner.sessions.get(id) {
                        targets.push((slot.tx.clone(), notification.clone()));
                    }
                }
            }
            if let Some((id, channels)) = session
                && channels.contains(&notification.channel)
                && let Some(slot) = inner.sessions.get(&id)
            {
                targets.push((slot.tx.clone(), notification.clone()));
            }
        }
        targets
    }

    /// Number of sessions currently listening on `channel`.
    ///
    /// # Panics
    ///
    /// Panics if the bus mutex was poisoned by a panicking publisher.
    #[must_use]
    pub fn listener_count(&self, channel: &str) -> usize {
        self.lock().channels.get(channel).map_or(0, HashSet::len)
    }

    /// Number of channels with at least one listener.
    ///
    /// # Panics
    ///
    /// Panics if the bus mutex was poisoned by a panicking publisher.
    #[must_use]
    pub fn channel_count(&self) -> usize {
        self.lock().channels.len()
    }

    /// Number of registered sessions.
    ///
    /// # Panics
    ///
    /// Panics if the bus mutex was poisoned by a panicking publisher.
    #[must_use]
    pub fn session_count(&self) -> usize {
        self.lock().sessions.len()
    }

    fn lock(&self) -> std::sync::MutexGuard<'_, BusInner> {
        self.inner.lock().expect("notify bus mutex")
    }

    fn listen(&self, id: u64, channel: &str) {
        let mut inner = self.lock();
        if !inner.sessions.contains_key(&id) {
            return;
        }
        inner
            .channels
            .entry(channel.to_string())
            .or_default()
            .insert(id);
    }

    fn unlisten(&self, id: u64, channel: &str) {
        let mut inner = self.lock();
        if let Some(listeners) = inner.channels.get_mut(channel) {
            listeners.remove(&id);
            if listeners.is_empty() {
                inner.channels.remove(channel);
            }
        }
    }

    fn unlisten_all(&self, id: u64) {
        let mut inner = self.lock();
        inner.channels.retain(|_, listeners| {
            listeners.remove(&id);
            !listeners.is_empty()
        });
    }

    fn is_listening(&self, id: u64, channel: &str) -> bool {
        self.lock()
            .channels
            .get(channel)
            .is_some_and(|listeners| listeners.contains(&id))
    }

    fn unregister(&self, id: u64) {
        let mut inner = self.lock();
        inner.sessions.remove(&id);
        inner.channels.retain(|_, listeners| {
            listeners.remove(&id);
            !listeners.is_empty()
        });
    }

    fn subscriptions(&self, id: u64) -> HashSet<String> {
        self.lock()
            .channels
            .iter()
            .filter(|(_, listeners)| listeners.contains(&id))
            .map(|(channel, _)| channel.clone())
            .collect()
    }
}

/// One connection's registration on the bus.
///
/// Dropping the handle deregisters the session and removes it from every
/// channel, so closing a connection cleans up without an explicit `UNLISTEN`.
pub struct NotifySessionHandle {
    bus: Arc<NotifyBus>,
    id: u64,
    pid: i32,
}

impl NotifySessionHandle {
    /// The backend pid reported as `process_id` on this session's own
    /// notifications.
    #[must_use]
    pub fn pid(&self) -> i32 {
        self.pid
    }

    /// The bus this session is registered on.
    #[must_use]
    pub fn bus(&self) -> &Arc<NotifyBus> {
        &self.bus
    }

    /// Start listening on `channel`. Idempotent — `LISTEN a; LISTEN a;` leaves
    /// exactly one registration, as in PostgreSQL.
    pub fn listen(&self, channel: &str) {
        self.bus.listen(self.id, channel);
    }

    /// Stop listening on `channel`. A no-op when not listening.
    pub fn unlisten(&self, channel: &str) {
        self.bus.unlisten(self.id, channel);
    }

    /// Stop listening on every channel (`UNLISTEN *`).
    pub fn unlisten_all(&self) {
        self.bus.unlisten_all(self.id);
    }

    /// Whether this session currently listens on `channel`.
    #[must_use]
    pub fn is_listening(&self, channel: &str) -> bool {
        self.bus.is_listening(self.id, channel)
    }

    /// The channels this session listens on right now.
    ///
    /// A committing transaction folds its queued `LISTEN`/`UNLISTEN` into this
    /// set and hands the result to [`Self::prepare_publish_with_pending`].
    #[must_use]
    pub fn subscriptions(&self) -> HashSet<String> {
        self.bus.subscriptions(self.id)
    }

    /// Reserve queue space for a batch of notifications from this session.
    /// Self-delivery needs no special case: this session is an ordinary
    /// listener, and receives its own notification with its own pid.
    ///
    /// # Errors
    ///
    /// The errors of [`NotifyBus::prepare_publish`].
    pub fn prepare_publish(
        &self,
        batch: &[(String, String)],
    ) -> Result<PreparedPublish, NotifyError> {
        self.bus.prepare_publish(self.pid, batch)
    }

    /// [`Self::prepare_publish`] for a transaction that is committing: this
    /// session's own copies are addressed by `subscriptions`, the set it will
    /// listen on once its queued `LISTEN`/`UNLISTEN` are applied, while every
    /// other session is addressed by its committed subscriptions.
    ///
    /// The pending subscriptions are therefore honoured for this transaction's
    /// own notifications without being published to the bus, so a concurrent
    /// publisher cannot address a `LISTEN` that has not committed — and a
    /// `LISTEN` that never commits cannot have received anything.
    ///
    /// # Errors
    ///
    /// The errors of [`NotifyBus::prepare_publish`].
    pub fn prepare_publish_with_pending(
        &self,
        batch: &[(String, String)],
        subscriptions: &HashSet<String>,
    ) -> Result<PreparedPublish, NotifyError> {
        self.bus
            .prepare_publish_as(self.pid, batch, Some((self.id, subscriptions)))
    }
}

impl Drop for NotifySessionHandle {
    fn drop(&mut self) {
        self.bus.unregister(self.id);
    }
}

/// A batch of notifications with queue space already reserved.
///
/// Holding this keeps one slot reserved in each target queue; [`Self::send`]
/// consumes the reservations and cannot fail, and dropping it releases them
/// without sending anything.
#[must_use = "a prepared publish holds queue reservations until it is sent or dropped"]
pub struct PreparedPublish {
    permits: Vec<(mpsc::OwnedPermit<Notification>, Notification)>,
}

impl PreparedPublish {
    /// Number of reserved deliveries (one per listener per notification).
    #[must_use]
    pub fn len(&self) -> usize {
        self.permits.len()
    }

    /// Whether nothing is reserved (no listener, or an empty batch).
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.permits.is_empty()
    }

    /// Deliver every reserved notification. Infallible by construction.
    pub fn send(self) {
        for (permit, notification) in self.permits {
            let _sender = permit.send(notification);
        }
    }
}

#[cfg(test)]
mod tests {
    use assert2::assert;

    use super::*;

    fn bus() -> Arc<NotifyBus> {
        Arc::new(NotifyBus::new())
    }

    /// The error of a failed publish. `PreparedPublish` deliberately implements
    /// neither `Debug` nor `PartialEq` (it holds live queue reservations), so the
    /// success side is discarded here rather than unwrapped.
    fn publish_error(result: Result<PreparedPublish, NotifyError>) -> NotifyError {
        match result {
            Ok(_) => panic!("expected the publish to fail"),
            Err(e) => e,
        }
    }

    fn publish(handle: &NotifySessionHandle, channel: &str, payload: &str) {
        handle
            .prepare_publish(&[(channel.to_string(), payload.to_string())])
            .expect("prepare")
            .send();
    }

    #[test]
    fn listen_is_idempotent_and_unlisten_removes_the_channel() {
        let bus = bus();
        let (handle, _rx) = NotifyBus::register(&bus, 42);
        handle.listen("news");
        handle.listen("news");
        assert!(handle.is_listening("news"));
        assert!(bus.listener_count("news") == 1);

        handle.unlisten("news");
        assert!(!handle.is_listening("news"));
        // The empty listener set is removed, not left behind.
        assert!(bus.channel_count() == 0);
        // UNLISTEN on a channel we never listened to is a no-op.
        handle.unlisten("other");
        assert!(bus.channel_count() == 0);
    }

    #[test]
    fn unlisten_all_clears_every_channel_of_this_session_only() {
        let bus = bus();
        let (a, _rx_a) = NotifyBus::register(&bus, 1);
        let (b, _rx_b) = NotifyBus::register(&bus, 2);
        a.listen("x");
        a.listen("y");
        b.listen("x");

        a.unlisten_all();
        assert!(!a.is_listening("x"));
        assert!(!a.is_listening("y"));
        assert!(b.is_listening("x"));
        assert!(bus.listener_count("x") == 1);
        assert!(bus.channel_count() == 1);
    }

    #[test]
    fn dropping_the_handle_unregisters_and_clears_channel_sets() {
        let bus = bus();
        let (a, _rx_a) = NotifyBus::register(&bus, 1);
        {
            let (b, _rx_b) = NotifyBus::register(&bus, 2);
            b.listen("x");
            b.listen("y");
            assert!(bus.session_count() == 2);
            assert!(bus.channel_count() == 2);
        }
        assert!(bus.session_count() == 1);
        assert!(bus.channel_count() == 0);
        // The surviving session still works.
        a.listen("x");
        assert!(bus.listener_count("x") == 1);
    }

    #[test]
    fn self_delivery_carries_the_publisher_pid() {
        let bus = bus();
        let (handle, mut rx) = NotifyBus::register(&bus, 77);
        handle.listen("self");
        publish(&handle, "self", "hello");
        assert!(
            rx.try_recv()
                == Ok(Notification {
                    process_id: 77,
                    channel: "self".to_string(),
                    payload: "hello".to_string(),
                })
        );
    }

    #[test]
    fn a_batch_reaches_every_listener_of_every_channel() {
        let bus = bus();
        let (publisher, mut prx) = NotifyBus::register(&bus, 1);
        let (listener, mut lrx) = NotifyBus::register(&bus, 2);
        publisher.listen("a");
        listener.listen("a");
        listener.listen("b");

        let prepared = publisher
            .prepare_publish(&[
                ("a".to_string(), "1".to_string()),
                ("b".to_string(), "2".to_string()),
                ("unheard".to_string(), "3".to_string()),
            ])
            .expect("prepare");
        // 2 listeners on "a" + 1 on "b" + 0 on "unheard".
        assert!(prepared.len() == 3);
        // Nothing is delivered until `send`.
        assert!(prx.try_recv() == Err(mpsc::error::TryRecvError::Empty));
        prepared.send();

        assert!(prx.try_recv().expect("a").payload == "1");
        assert!(prx.try_recv() == Err(mpsc::error::TryRecvError::Empty));
        let first = lrx.try_recv().expect("a");
        let second = lrx.try_recv().expect("b");
        assert!(first.channel == "a" && first.payload == "1" && first.process_id == 1);
        assert!(second.channel == "b" && second.payload == "2");
    }

    #[test]
    fn a_full_queue_fails_the_batch_and_sends_nothing() {
        let bus = Arc::new(NotifyBus::with_capacity(1));
        let (publisher, mut prx) = NotifyBus::register(&bus, 1);
        let (slow, mut slow_rx) = NotifyBus::register(&bus, 2);
        publisher.listen("c");
        slow.listen("c");

        // Fill the slow listener's single queue slot.
        publish(&publisher, "c", "first");
        assert!(prx.try_recv().expect("publisher gets its own").payload == "first");

        // The next batch cannot reserve a second slot for the slow listener.
        let err =
            publish_error(publisher.prepare_publish(&[("c".to_string(), "second".to_string())]));
        assert!(err == NotifyError::QueueFull);
        assert!(err.sqlstate() == "54000");
        // Nothing was delivered — not even to the listener whose queue had room.
        assert!(prx.try_recv() == Err(mpsc::error::TryRecvError::Empty));
        assert!(slow_rx.try_recv().expect("the first one").payload == "first");
        assert!(slow_rx.try_recv() == Err(mpsc::error::TryRecvError::Empty));

        // Once the slow listener drains, publishing succeeds again.
        publish(&publisher, "c", "third");
        assert!(slow_rx.try_recv().expect("third").payload == "third");
    }

    #[test]
    fn dropping_a_prepared_publish_releases_the_reservations() {
        let bus = Arc::new(NotifyBus::with_capacity(1));
        let (publisher, mut prx) = NotifyBus::register(&bus, 1);
        publisher.listen("c");

        let prepared = publisher
            .prepare_publish(&[("c".to_string(), "dropped".to_string())])
            .expect("prepare");
        assert!(prepared.len() == 1);
        drop(prepared);

        // The reservation is gone, so the single slot is free again.
        publish(&publisher, "c", "kept");
        assert!(prx.try_recv().expect("kept").payload == "kept");
    }

    #[test]
    fn a_listener_whose_receiver_is_gone_is_skipped_not_fatal() {
        let bus = bus();
        let (publisher, mut prx) = NotifyBus::register(&bus, 1);
        let (gone, gone_rx) = NotifyBus::register(&bus, 2);
        publisher.listen("c");
        gone.listen("c");
        drop(gone_rx);

        publish(&publisher, "c", "payload");
        assert!(prx.try_recv().expect("delivered").payload == "payload");
    }

    #[test]
    fn validation_rejects_empty_channels_and_oversized_payloads() {
        let bus = bus();
        let (handle, _rx) = NotifyBus::register(&bus, 1);
        assert!(
            publish_error(handle.prepare_publish(&[(String::new(), String::new())]))
                == NotifyError::EmptyChannel
        );
        assert!(
            publish_error(
                handle.prepare_publish(&[("c".to_string(), "x".repeat(MAX_PAYLOAD_BYTES + 1))])
            ) == NotifyError::PayloadTooLong
        );
        assert!(
            publish_error(
                handle.prepare_publish(&[("c".repeat(MAX_CHANNEL_BYTES + 1), String::new())])
            ) == NotifyError::ChannelNameTooLong
        );
        // At the limits it is accepted.
        assert!(
            validate(
                &"c".repeat(MAX_CHANNEL_BYTES),
                &"x".repeat(MAX_PAYLOAD_BYTES)
            )
            .is_ok()
        );
        assert!(NotifyError::PayloadTooLong.sqlstate() == "22023");
    }

    /// PostgreSQL 18's boundaries in absolute terms, so a constant that drifts
    /// off by one is caught: it accepts a 7999-byte payload and a 63-byte
    /// channel name, and rejects 8000 and 64 with 22023.
    #[test]
    fn the_length_limits_are_postgresqls_to_the_byte() {
        assert!(validate("c", &"x".repeat(7999)).is_ok());
        assert!(validate("c", &"x".repeat(8000)) == Err(NotifyError::PayloadTooLong));
        assert!(validate(&"c".repeat(63), "").is_ok());
        assert!(validate(&"c".repeat(64), "") == Err(NotifyError::ChannelNameTooLong));
    }

    /// A pending subscription addresses only the session that staged it: it
    /// receives its own notification, and the bus keeps publishing to everyone
    /// else exactly as before.
    #[test]
    fn a_pending_subscription_addresses_only_its_own_session() {
        let bus = bus();
        let (committing, mut own_rx) = NotifyBus::register(&bus, 1);
        let (other, mut other_rx) = NotifyBus::register(&bus, 2);
        other.listen("c");
        let pending: HashSet<String> = ["c".to_string()].into_iter().collect();

        committing
            .prepare_publish_with_pending(&[("c".to_string(), "p".to_string())], &pending)
            .expect("prepare")
            .send();

        assert!(
            own_rx.try_recv()
                == Ok(Notification {
                    process_id: 1,
                    channel: "c".to_string(),
                    payload: "p".to_string(),
                })
        );
        assert!(other_rx.try_recv().expect("the committed listener").payload == "p");
        // Nothing was published to the bus: the pending LISTEN is still invisible.
        assert!(!committing.is_listening("c"));
        assert!(bus.listener_count("c") == 1);
    }

    /// A pending `UNLISTEN` is honoured the same way: the staging session is
    /// addressed by its post-commit set, not by the bus's live membership.
    #[test]
    fn a_pending_unlisten_suppresses_the_stagers_own_copy() {
        let bus = bus();
        let (committing, mut own_rx) = NotifyBus::register(&bus, 1);
        committing.listen("c");

        committing
            .prepare_publish_with_pending(&[("c".to_string(), "p".to_string())], &HashSet::new())
            .expect("prepare")
            .send();

        assert!(own_rx.try_recv() == Err(mpsc::error::TryRecvError::Empty));
        // The live subscription is untouched until the transaction commits.
        assert!(committing.is_listening("c"));
    }

    #[test]
    fn a_batch_that_fails_validation_reserves_nothing() {
        let bus = Arc::new(NotifyBus::with_capacity(1));
        let (handle, mut rx) = NotifyBus::register(&bus, 1);
        handle.listen("c");
        // The valid first entry must not be queued when a later one is invalid.
        let err = publish_error(handle.prepare_publish(&[
            ("c".to_string(), "ok".to_string()),
            (String::new(), "bad".to_string()),
        ]));
        assert!(err == NotifyError::EmptyChannel);
        assert!(rx.try_recv() == Err(mpsc::error::TryRecvError::Empty));
    }

    fn remote(channel: &str, payload: &str, pid: i32) -> Notification {
        Notification {
            process_id: pid,
            channel: channel.to_string(),
            payload: payload.to_string(),
        }
    }

    #[test]
    fn deliver_remote_reaches_every_listener_and_keeps_the_origin_pid() {
        let bus = bus();
        let (a, mut a_rx) = NotifyBus::register(&bus, 1);
        let (b, mut b_rx) = NotifyBus::register(&bus, 2);
        let (_quiet, mut quiet_rx) = NotifyBus::register(&bus, 3);
        a.listen("c");
        b.listen("c");

        // pid 9001 belongs to a backend on another node; it must survive the hop.
        let batch = [remote("c", "from-afar", 9001)];
        assert!(
            bus.deliver_remote(&batch)
                == RemoteDelivery {
                    delivered: 2,
                    dropped: 0
                }
        );
        assert!(a_rx.try_recv() == Ok(remote("c", "from-afar", 9001)));
        assert!(b_rx.try_recv() == Ok(remote("c", "from-afar", 9001)));
        assert!(quiet_rx.try_recv() == Err(mpsc::error::TryRecvError::Empty));
    }

    #[test]
    fn deliver_remote_drops_a_full_listener_and_still_serves_the_others() {
        let bus = Arc::new(NotifyBus::with_capacity(1));
        let (slow, mut slow_rx) = NotifyBus::register(&bus, 1);
        let (fast, mut fast_rx) = NotifyBus::register(&bus, 2);
        slow.listen("c");
        fast.listen("c");

        // Fill the single slot of both queues, then drain only the fast one.
        assert!(bus.deliver_remote(&[remote("c", "first", 7)]).delivered == 2);
        assert!(fast_rx.try_recv().expect("first").payload == "first");

        // Unlike the local path — where one full queue sends nothing to anyone —
        // the reachable listener is still served.
        assert!(
            bus.deliver_remote(&[remote("c", "second", 7)])
                == RemoteDelivery {
                    delivered: 1,
                    dropped: 1
                }
        );
        assert!(fast_rx.try_recv().expect("second").payload == "second");
        assert!(slow_rx.try_recv().expect("first").payload == "first");
        assert!(slow_rx.try_recv() == Err(mpsc::error::TryRecvError::Empty));
    }

    #[test]
    fn deliver_remote_to_a_channel_with_no_listener_is_a_silent_no_op() {
        let bus = bus();
        let (handle, mut rx) = NotifyBus::register(&bus, 1);
        handle.listen("heard");

        assert!(bus.deliver_remote(&[]) == RemoteDelivery::default());
        assert!(bus.deliver_remote(&[remote("unheard", "x", 5)]) == RemoteDelivery::default());
        assert!(rx.try_recv() == Err(mpsc::error::TryRecvError::Empty));
    }

    #[test]
    fn deliver_remote_skips_a_listener_whose_receiver_is_gone_without_counting_it() {
        let bus = bus();
        let (live, mut live_rx) = NotifyBus::register(&bus, 1);
        let (gone, gone_rx) = NotifyBus::register(&bus, 2);
        live.listen("c");
        gone.listen("c");
        drop(gone_rx);

        assert!(
            bus.deliver_remote(&[remote("c", "payload", 5)])
                == RemoteDelivery {
                    delivered: 1,
                    dropped: 0
                }
        );
        assert!(live_rx.try_recv().expect("delivered").payload == "payload");
    }

    #[test]
    fn deliver_remote_delivers_a_multi_channel_batch_in_order() {
        let bus = bus();
        let (handle, mut rx) = NotifyBus::register(&bus, 1);
        handle.listen("a");
        handle.listen("b");

        let batch = [remote("a", "1", 11), remote("b", "2", 12)];
        assert!(bus.deliver_remote(&batch).delivered == 2);
        assert!(rx.try_recv() == Ok(remote("a", "1", 11)));
        assert!(rx.try_recv() == Ok(remote("b", "2", 12)));
    }

    #[test]
    fn listening_after_a_publish_does_not_receive_it() {
        let bus = bus();
        let (publisher, _prx) = NotifyBus::register(&bus, 1);
        let (late, mut late_rx) = NotifyBus::register(&bus, 2);
        publish(&publisher, "c", "missed");
        late.listen("c");
        assert!(late_rx.try_recv() == Err(mpsc::error::TryRecvError::Empty));
        publish(&publisher, "c", "seen");
        assert!(late_rx.try_recv().expect("seen").payload == "seen");
    }
}
