//! Per-(topic, partition) accumulator. Each `try_append` enqueues a
//! record + a oneshot tx; the sender drains in-flight batches and
//! resolves the oneshots from the `ProduceResponse`.

use std::{collections::VecDeque, sync::Arc, time::Instant};

use bytes::Bytes;
use dashmap::DashMap;
use tokio::sync::{Mutex, oneshot};

use crate::{
    error::ProducerError,
    record::{Header, RecordMetadata},
};

pub(crate) type AccumulatorMap = Arc<DashMap<(String, i32), Arc<Mutex<Accumulator>>>>;

/// A record waiting inside an in-progress batch.
#[derive(Debug)]
pub(crate) struct PendingRecord {
    pub offset_delta: i32,
    pub timestamp_ms: i64,
    pub key: Option<Bytes>,
    pub value: Option<Bytes>,
    pub headers: Vec<Header>,
    pub ack: oneshot::Sender<Result<RecordMetadata, ProducerError>>,
}

/// One in-progress `RecordBatch`. The sender wraps this into a Kafka
/// `RecordBatch` at flush time and assigns `base_sequence`.
#[derive(Debug)]
pub(crate) struct InProgressBatch {
    /// Wall-clock time when this batch's first record was appended.
    /// Used by the sender to decide `linger.ms` expiry (the sender
    /// currently relies on the linger ticker rather than reading this
    /// field, but it's still useful for diagnostics).
    #[allow(dead_code)]
    pub first_append_at: Instant,
    /// Approximate uncompressed body size.
    pub size_bytes: usize,
    pub records: Vec<PendingRecord>,
}

impl InProgressBatch {
    fn new() -> Self {
        Self {
            first_append_at: Instant::now(),
            size_bytes: 0,
            records: Vec::new(),
        }
    }

    pub fn is_empty(&self) -> bool {
        self.records.is_empty()
    }
}

/// One accumulator per (topic, partition).
#[derive(Debug)]
pub(crate) struct Accumulator {
    /// `None` until the first append. Sender pops the in-progress batch
    /// into `ready` when it flushes (rotating the partitioner sticky if
    /// applicable).
    pub current: Option<InProgressBatch>,
    /// FIFO of batches the sender hasn't flushed yet.
    pub ready: VecDeque<InProgressBatch>,
    /// `batch.size` cap. If a single record would push us past this, we
    /// seal `current` first and start fresh.
    pub batch_size: usize,
}

/// Result of [`Accumulator::try_append`].
#[allow(dead_code)] // `BatchFull` is reserved for future backpressure paths.
pub(crate) enum AppendResult {
    Appended(oneshot::Receiver<Result<RecordMetadata, ProducerError>>),
    /// The accumulator's `batch.size` is full but a new batch could be
    /// started. The caller (sender wakeup) needs to seal and rotate.
    BatchFull,
}

impl Accumulator {
    pub fn new(batch_size: usize) -> Self {
        Self {
            current: None,
            ready: VecDeque::new(),
            batch_size,
        }
    }

    #[tracing::instrument(
        level = "trace",
        skip_all,
        fields(
            key_len = key.as_ref().map(bytes::Bytes::len),
            value_len = value.as_ref().map(bytes::Bytes::len),
            headers = headers.len(),
        ),
    )]
    pub fn try_append(
        &mut self,
        key: Option<Bytes>,
        value: Option<Bytes>,
        headers: Vec<Header>,
        timestamp_ms: i64,
    ) -> AppendResult {
        // Approximate the per-record size: 8 bytes overhead + key + value + headers.
        let record_size = approx_record_size(key.as_deref(), value.as_deref(), &headers);

        let need_new_batch = match &self.current {
            None => true,
            Some(b) => b.size_bytes + record_size > self.batch_size && !b.is_empty(),
        };

        if need_new_batch {
            if let Some(prev) = self.current.take() {
                self.ready.push_back(prev);
            }
            self.current = Some(InProgressBatch::new());
        }

        let batch = self
            .current
            .as_mut()
            .expect("current set above when need_new_batch was true");

        let (tx, rx) = oneshot::channel();
        let offset_delta = i32::try_from(batch.records.len()).unwrap_or(i32::MAX);
        batch.records.push(PendingRecord {
            offset_delta,
            timestamp_ms,
            key,
            value,
            headers,
            ack: tx,
        });
        batch.size_bytes += record_size;
        AppendResult::Appended(rx)
    }

    /// Move the current in-progress batch into `ready`. Called by the
    /// sender at flush time (linger expiry, explicit flush, batch full).
    pub fn seal_current(&mut self) {
        if let Some(b) = self.current.take()
            && !b.is_empty()
        {
            self.ready.push_back(b);
        }
    }
}

fn approx_record_size(key: Option<&[u8]>, value: Option<&[u8]>, headers: &[Header]) -> usize {
    let mut n = 8usize; // varint overhead estimate
    n += key.map_or(0, <[u8]>::len) + 4;
    n += value.map_or(0, <[u8]>::len) + 4;
    for h in headers {
        n += h.key.len() + h.value.as_ref().map_or(0, bytes::Bytes::len) + 8;
    }
    n
}

#[cfg(test)]
mod tests {
    use assert2::check;

    use super::*;

    #[test]
    fn first_append_creates_batch() {
        let mut a = Accumulator::new(1024);
        let _ = a.try_append(None, Some(Bytes::from_static(b"hi")), vec![], 0);
        let current = a.current.as_ref().expect("append creates current batch");
        check!(
            (
                current.records.len(),
                current.is_empty(),
                current.size_bytes
            ) == (1, false, approx_record_size(None, Some(b"hi"), &[]))
        );
    }

    #[test]
    fn record_past_batch_size_rolls_over() {
        let record_size = approx_record_size(None, Some(&[0u8; 32]), &[]);
        let mut a = Accumulator::new(record_size * 2 - 1);
        let _ = a.try_append(None, Some(Bytes::from(vec![0u8; 32])), vec![], 0);
        let _ = a.try_append(None, Some(Bytes::from(vec![0u8; 32])), vec![], 0);
        let current = a
            .current
            .as_ref()
            .expect("second record starts a new batch");
        check!(
            (
                a.ready.len(),
                current.records.len(),
                current.records[0].offset_delta
            ) == (1, 1, 0)
        );
    }

    #[test]
    fn exact_batch_size_boundary_does_not_roll_over() {
        let record_size = approx_record_size(None, Some(&[0u8; 32]), &[]);
        let mut a = Accumulator::new(record_size * 2);

        let _ = a.try_append(None, Some(Bytes::from(vec![0u8; 32])), vec![], 0);
        let _ = a.try_append(None, Some(Bytes::from(vec![0u8; 32])), vec![], 0);

        let current = a.current.as_ref().unwrap();
        check!(
            (
                a.ready.is_empty(),
                current.records.len(),
                current.records[0].offset_delta,
                current.records[1].offset_delta,
                current.size_bytes,
            ) == (true, 2, 0, 1, record_size * 2)
        );
    }

    #[test]
    fn seal_moves_current_to_ready() {
        let mut a = Accumulator::new(1024);
        let _ = a.try_append(None, Some(Bytes::from_static(b"x")), vec![], 0);
        a.seal_current();
        assert2::assert!((a.current.is_none(), a.ready.len()) == (true, 1));
    }

    #[test]
    fn seal_drops_empty_current_batch() {
        let mut a = Accumulator::new(1024);
        a.current = Some(InProgressBatch::new());

        assert2::assert!(a.current.as_ref().unwrap().is_empty());
        a.seal_current();

        assert2::assert!((a.current.is_none(), a.ready.is_empty()) == (true, true));
    }

    #[test]
    fn approx_record_size_counts_overhead_key_value_and_headers() {
        let headers = vec![
            Header {
                key: "h1".into(),
                value: Some(Bytes::from_static(b"abc")),
            },
            Header {
                key: "empty".into(),
                value: None,
            },
        ];

        let populated_size: usize = [8, 3, 4, 5, 4, 2, 3, 8, 5, 0, 8].into_iter().sum();
        for (_name, key, value, case_headers, expected) in [
            (
                "populated",
                Some(&b"key"[..]),
                Some(&b"value"[..]),
                headers.as_slice(),
                populated_size,
            ),
            ("empty", None, None, &[][..], 8 + 4 + 4),
        ] {
            assert2::assert!(approx_record_size(key, value, case_headers) == expected);
        }
    }
}
