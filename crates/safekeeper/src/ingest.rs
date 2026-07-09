//! Bounded, sans-IO safekeeper WAL ingest orchestration.

use bytes::BytesMut;
use thiserror::Error;

use crate::{
    Lsn,
    conn::{ReplicationSession, ReplicationSessionError, ReplicationTransport},
    frame::{WalFrame, WalFrameError},
    protocol::{CopyBothMessage, PrimaryKeepalive, StandbyStatusUpdate, XLogData},
};

/// Default target size for a produced WAL topic record.
pub const DEFAULT_TARGET_FRAME_BYTES: usize = 512 * 1024;

/// Result type for bounded ingest operations.
pub type Result<T> = std::result::Result<T, IngestError>;

/// Topic name that stores WAL frames for `cluster`.
#[must_use]
pub fn wal_topic_name(cluster: &str) -> String {
    format!("__pg_wal.{cluster}")
}

/// One acked append to the safekeeper WAL topic.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct AppendAck {
    /// End LSN durably acknowledged by the topic append.
    pub end_lsn: Lsn,
}

/// Abstract topic append/read seam used by the sans-IO ingest loop.
pub trait WalTopic {
    /// Ensures the WAL topic exists before replication starts.
    fn ensure_topic(&mut self, topic: &str) -> Result<()>;

    /// Appends one encoded [`WalFrame`] and returns only after the append is acked.
    fn append_frame(&mut self, topic: &str, frame: &WalFrame) -> Result<AppendAck>;

    /// Reads the last encoded frame in the topic, if any.
    fn last_frame(&self, topic: &str) -> Result<Option<Vec<u8>>>;
}

/// Optional gate that validates the stored stream before a frame is appended.
pub trait DecodeGate {
    /// Accepts the next contiguous frame or returns a decode failure.
    fn accept_frame(&mut self, frame: &WalFrame) -> Result<()>;
}

/// Decode gate used when the caller has no WAL decoder wired into this slice.
#[derive(Debug, Default, Clone, Copy)]
pub struct NoopDecodeGate;

impl DecodeGate for NoopDecodeGate {
    fn accept_frame(&mut self, _frame: &WalFrame) -> Result<()> {
        Ok(())
    }
}

/// Configuration for a bounded ingest run.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct IngestConfig {
    /// Cluster suffix used to derive the internal WAL topic.
    pub cluster: String,
    /// Target payload bytes per produced WAL frame.
    pub target_frame_bytes: usize,
    /// Maximum `CopyBoth` messages to consume before returning.
    pub max_messages: usize,
}

impl IngestConfig {
    /// Creates a config with the default frame target.
    #[must_use]
    pub fn bounded(cluster: impl Into<String>, max_messages: usize) -> Self {
        Self {
            cluster: cluster.into(),
            target_frame_bytes: DEFAULT_TARGET_FRAME_BYTES,
            max_messages,
        }
    }
}

/// Summary returned from a bounded ingest run.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct IngestReport {
    /// LSN from which replication was started.
    pub resume_lsn: Lsn,
    /// Last WAL byte written into the topic append pipeline.
    pub written_lsn: Lsn,
    /// Highest frame end LSN acknowledged by the topic.
    pub flushed_lsn: Lsn,
    /// Number of `CopyBoth` messages consumed.
    pub messages_consumed: usize,
    /// Number of frames acked by the topic.
    pub frames_appended: usize,
}

/// Bounded safekeeper ingest loop over replication and topic seams.
pub struct SafekeeperIngest<T, W, G = NoopDecodeGate> {
    session: ReplicationSession<T>,
    topic: W,
    decode_gate: G,
    config: IngestConfig,
}

impl<T, W> SafekeeperIngest<T, W, NoopDecodeGate> {
    /// Creates an ingest loop with no decode gate.
    pub fn new(session: ReplicationSession<T>, topic: W, config: IngestConfig) -> Self {
        Self::with_decode_gate(session, topic, NoopDecodeGate, config)
    }
}

impl<T, W, G> SafekeeperIngest<T, W, G> {
    /// Creates an ingest loop with an explicit decode gate.
    pub fn with_decode_gate(
        session: ReplicationSession<T>,
        topic: W,
        decode_gate: G,
        config: IngestConfig,
    ) -> Self {
        Self {
            session,
            topic,
            decode_gate,
            config,
        }
    }

    /// Consumes the ingest loop and returns its seams.
    #[must_use]
    pub fn into_parts(self) -> (ReplicationSession<T>, W, G) {
        (self.session, self.topic, self.decode_gate)
    }
}

impl<T, W, G> SafekeeperIngest<T, W, G>
where
    T: ReplicationTransport,
    W: WalTopic,
    G: DecodeGate,
{
    /// Runs bounded ingest until the source ends or `max_messages` is reached.
    pub fn run_bounded(&mut self) -> Result<IngestReport> {
        if self.config.target_frame_bytes == 0 {
            return Err(IngestError::InvalidTargetFrameBytes);
        }

        let topic = wal_topic_name(&self.config.cluster);
        self.topic.ensure_topic(&topic)?;
        let stored_resume_lsn = stored_resume_lsn(&self.topic, &topic)?;

        let identified_system = self.session.identify()?;
        self.session.ensure_slot()?;
        let resume_lsn = stored_resume_lsn.unwrap_or(identified_system.flush_lsn);
        self.session.start(resume_lsn)?;

        let mut state = IngestState::new(resume_lsn, self.config.target_frame_bytes);
        while state.messages_consumed < self.config.max_messages {
            let Some(message) = self.session.receive()? else {
                break;
            };

            state.messages_consumed += 1;
            match message {
                CopyBothMessage::XLogData(xlog_data) => {
                    state.accept_xlog_data(&xlog_data)?;
                    self.append_ready_frames(&topic, &mut state)?;
                }
                CopyBothMessage::PrimaryKeepalive(keepalive) => {
                    self.reply_to_keepalive(keepalive, &state)?;
                }
                CopyBothMessage::StandbyStatusUpdate(_) => {
                    return Err(IngestError::UnexpectedStandbyStatusUpdate);
                }
            }
        }

        self.append_tail_frame(&topic, &mut state)?;

        Ok(IngestReport {
            resume_lsn,
            written_lsn: state.written_lsn,
            flushed_lsn: state.flushed_lsn,
            messages_consumed: state.messages_consumed,
            frames_appended: state.frames_appended,
        })
    }

    fn append_ready_frames(&mut self, topic: &str, state: &mut IngestState) -> Result<()> {
        while state.pending_len() >= state.target_frame_bytes {
            let frame = state.drain_pending_frame()?;
            self.append_frame(topic, state, &frame)?;
        }
        Ok(())
    }

    fn append_tail_frame(&mut self, topic: &str, state: &mut IngestState) -> Result<()> {
        if state.pending_len() == 0 {
            return Ok(());
        }

        let frame = state.drain_pending_frame()?;
        self.append_frame(topic, state, &frame)
    }

    fn append_frame(
        &mut self,
        topic: &str,
        state: &mut IngestState,
        frame: &WalFrame,
    ) -> Result<()> {
        self.decode_gate.accept_frame(frame)?;
        let expected_end_lsn = frame_end_lsn(frame)?;
        let ack = self.topic.append_frame(topic, frame)?;

        if ack.end_lsn != expected_end_lsn {
            return Err(IngestError::AppendAckMismatch {
                expected: expected_end_lsn,
                got: ack.end_lsn,
            });
        }

        state.flushed_lsn = ack.end_lsn;
        state.frames_appended += 1;
        Ok(())
    }

    fn reply_to_keepalive(
        &mut self,
        keepalive: PrimaryKeepalive,
        state: &IngestState,
    ) -> Result<()> {
        if !keepalive.reply_requested {
            return Ok(());
        }

        self.session.send_status(StandbyStatusUpdate {
            write_lsn: state.written_lsn,
            flush_lsn: state.flushed_lsn,
            apply_lsn: state.flushed_lsn,
            client_time: keepalive.send_time,
            reply_requested: false,
        })?;
        Ok(())
    }
}

#[derive(Debug, Clone)]
struct IngestState {
    next_lsn: Lsn,
    pending_lsn: Lsn,
    pending: BytesMut,
    target_frame_bytes: usize,
    written_lsn: Lsn,
    flushed_lsn: Lsn,
    messages_consumed: usize,
    frames_appended: usize,
}

impl IngestState {
    fn new(resume_lsn: Lsn, target_frame_bytes: usize) -> Self {
        Self {
            next_lsn: resume_lsn,
            pending_lsn: resume_lsn,
            pending: BytesMut::new(),
            target_frame_bytes,
            written_lsn: resume_lsn,
            flushed_lsn: resume_lsn,
            messages_consumed: 0,
            frames_appended: 0,
        }
    }

    fn pending_len(&self) -> usize {
        self.pending.len()
    }

    fn accept_xlog_data(&mut self, xlog_data: &XLogData) -> Result<()> {
        if xlog_data.data.is_empty() {
            return Ok(());
        }

        let xlog_end = advance_lsn(xlog_data.wal_start, xlog_data.data.len())?;
        if xlog_end <= self.next_lsn {
            return Ok(());
        }

        if xlog_data.wal_start > self.next_lsn {
            return Err(IngestError::LsnGap {
                expected: self.next_lsn,
                got: xlog_data.wal_start,
            });
        }

        let overlap_len = usize::try_from(self.next_lsn.value() - xlog_data.wal_start.value())
            .map_err(|_| IngestError::LsnDistanceTooLarge {
                start: xlog_data.wal_start,
                end: self.next_lsn,
            })?;
        let contiguous_bytes = xlog_data.data.slice(overlap_len..);
        self.pending.extend_from_slice(&contiguous_bytes);
        self.next_lsn = xlog_end;
        self.written_lsn = xlog_end;
        Ok(())
    }

    fn drain_pending_frame(&mut self) -> Result<WalFrame> {
        if self.pending.is_empty() {
            return Err(IngestError::EmptyPendingFrame);
        }

        let payload = self.pending.split().freeze();
        let frame = WalFrame::new(self.pending_lsn, payload)?;
        self.pending_lsn = frame_end_lsn(&frame)?;
        Ok(frame)
    }
}

/// Reads the stored topic tail and returns the LSN to resume from.
pub fn resume_lsn(topic: &impl WalTopic, topic_name: &str) -> Result<Lsn> {
    Ok(stored_resume_lsn(topic, topic_name)?.unwrap_or(Lsn(0)))
}

fn stored_resume_lsn(topic: &impl WalTopic, topic_name: &str) -> Result<Option<Lsn>> {
    let Some(encoded_frame) = topic.last_frame(topic_name)? else {
        return Ok(None);
    };

    let frame = WalFrame::decode(&encoded_frame)?;
    frame_end_lsn(&frame).map(Some)
}

/// Computes the exclusive end LSN of a frame.
pub fn frame_end_lsn(frame: &WalFrame) -> Result<Lsn> {
    advance_lsn(frame.lsn, frame.payload.len())
}

fn advance_lsn(lsn: Lsn, len: usize) -> Result<Lsn> {
    let len = u64::try_from(len).map_err(|_| IngestError::LsnOverflow { lsn, len })?;
    let value = lsn
        .value()
        .checked_add(len)
        .ok_or(IngestError::LsnOverflow {
            lsn,
            len: usize::MAX,
        })?;
    Ok(Lsn(value))
}

/// Errors returned by bounded WAL ingest.
#[derive(Debug, Error, PartialEq, Eq)]
#[non_exhaustive]
pub enum IngestError {
    /// Target frame bytes must be positive.
    #[error("target frame bytes must be greater than zero")]
    InvalidTargetFrameBytes,

    /// WAL source skipped bytes.
    #[error("WAL stream gap: expected {expected}, got {got}")]
    LsnGap {
        /// Next expected LSN.
        expected: Lsn,
        /// Actual message LSN.
        got: Lsn,
    },

    /// Distance between two LSNs does not fit in memory on this platform.
    #[error("LSN distance from {start} to {end} is too large")]
    LsnDistanceTooLarge {
        /// Start LSN.
        start: Lsn,
        /// End LSN.
        end: Lsn,
    },

    /// Advancing an LSN overflowed.
    #[error("advancing LSN {lsn} by {len} bytes overflows")]
    LsnOverflow {
        /// Starting LSN.
        lsn: Lsn,
        /// Byte count.
        len: usize,
    },

    /// The append acknowledgement did not match the frame end.
    #[error("append ack mismatch: expected {expected}, got {got}")]
    AppendAckMismatch {
        /// Expected ack LSN.
        expected: Lsn,
        /// Actual ack LSN.
        got: Lsn,
    },

    /// A standby status update appeared on the primary-to-standby stream.
    #[error("unexpected standby status update on primary stream")]
    UnexpectedStandbyStatusUpdate,

    /// Internal invariant: a frame drain was requested with no pending bytes.
    #[error("cannot drain an empty pending WAL frame")]
    EmptyPendingFrame,

    /// Topic seam failed.
    #[error("WAL topic operation failed: {message}")]
    Topic {
        /// Error text.
        message: String,
    },

    /// Decode gate rejected the stream.
    #[error("WAL decode gate failed: {message}")]
    DecodeGate {
        /// Error text.
        message: String,
    },

    /// Replication session failed.
    #[error(transparent)]
    Replication(#[from] ReplicationSessionError),

    /// WAL frame failed to encode/decode.
    #[error(transparent)]
    WalFrame(#[from] WalFrameError),
}
