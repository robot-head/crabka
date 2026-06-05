//! Time windows + the `Windowed<K>` output key + a windowed output serde.
use bytes::{BufMut, Bytes, BytesMut};

use crate::processor::serde::{Serde, SerdeError};

/// A half-open time window `[start, end)` (epoch millis).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct Window {
    pub start: i64,
    pub end: i64,
}

/// An aggregation key tagged with its window — the output key of a windowed
/// aggregation (`KTable<Windowed<K>, V>`).
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct Windowed<K> {
    pub key: K,
    pub window: Window,
}

/// Tumbling / hopping time windows (epoch-aligned). `advance_ms == size_ms` is
/// tumbling; `advance_ms < size_ms` is hopping. `grace_ms` is recorded for the
/// changelog retention computation (window closing itself is deferred to a later slice).
#[derive(Debug, Clone, Copy)]
pub struct TimeWindows {
    pub size_ms: i64,
    pub advance_ms: i64,
    pub grace_ms: i64,
}

impl TimeWindows {
    /// Tumbling window of `size_ms` (advance == size, grace 0).
    #[must_use]
    pub fn of_size(size_ms: i64) -> Self {
        assert!(size_ms > 0, "window size must be > 0");
        Self {
            size_ms,
            advance_ms: size_ms,
            grace_ms: 0,
        }
    }
    /// Hopping: advance by `advance_ms` (`0 < advance_ms <= size_ms`).
    #[must_use]
    pub fn advance_by(mut self, advance_ms: i64) -> Self {
        assert!(
            advance_ms > 0 && advance_ms <= self.size_ms,
            "0 < advance <= size"
        );
        self.advance_ms = advance_ms;
        self
    }
    /// Set the grace period (only affects changelog retention here).
    #[must_use]
    pub fn grace(mut self, grace_ms: i64) -> Self {
        assert!(grace_ms >= 0, "grace must be >= 0");
        self.grace_ms = grace_ms;
        self
    }
    /// The window starts a timestamp `t` falls into (JVM `TimeWindows.windowsFor`).
    #[must_use]
    pub fn windows_for(&self, t: i64) -> Vec<i64> {
        let mut start = (std::cmp::max(0, t - self.size_ms + self.advance_ms) / self.advance_ms)
            * self.advance_ms;
        let mut out = Vec::new();
        while start <= t {
            out.push(start);
            start += self.advance_ms;
        }
        out
    }
}

/// `Serde<Windowed<K>>` producing the JVM **output-topic** format:
/// `inner_key_bytes ‖ windowStart : 8-byte BE` (no end, no seqnum). Carries the
/// window `size` so `deserialize` can reconstruct `end = start + size`.
#[derive(Debug, Clone, Copy)]
pub struct TimeWindowedSerde<KS> {
    inner: KS,
    size_ms: i64,
}

impl<KS> TimeWindowedSerde<KS> {
    #[must_use]
    pub fn new(inner: KS, size_ms: i64) -> Self {
        Self { inner, size_ms }
    }
}

impl<K, KS> Serde<Windowed<K>> for TimeWindowedSerde<KS>
where
    K: Send + Sync + 'static,
    KS: Serde<K>,
{
    fn serialize(&self, value: &Windowed<K>) -> Bytes {
        let kb = self.inner.serialize(&value.key);
        let mut b = BytesMut::with_capacity(kb.len() + 8);
        b.extend_from_slice(&kb);
        b.put_i64(value.window.start);
        b.freeze()
    }
    fn deserialize(&self, bytes: &[u8]) -> Result<Windowed<K>, SerdeError> {
        if bytes.len() < 8 {
            return Err(SerdeError(format!(
                "windowed key too short: {}",
                bytes.len()
            )));
        }
        let split = bytes.len() - 8;
        let key = self.inner.deserialize(&bytes[..split])?;
        let start = i64::from_be_bytes(bytes[split..].try_into().expect("8 bytes"));
        Ok(Windowed {
            key,
            window: Window {
                start,
                end: start + self.size_ms,
            },
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn windows_for_tumbling_one_window() {
        let w = TimeWindows::of_size(10);
        assert_eq!(w.windows_for(0), vec![0]);
        assert_eq!(w.windows_for(9), vec![0]);
        assert_eq!(w.windows_for(10), vec![10]);
        assert_eq!(w.windows_for(25), vec![20]);
    }

    #[test]
    fn windows_for_hopping_overlaps() {
        let w = TimeWindows::of_size(10).advance_by(5);
        assert_eq!(w.windows_for(12), vec![5, 10]); // start0 = max(0,12-10+5)/5*5 = 5
        assert_eq!(w.windows_for(0), vec![0]);
    }

    #[test]
    fn time_windowed_serde_round_trips_output_format() {
        use crate::processor::serde::{Serde, StringSerde};
        let s = TimeWindowedSerde::new(StringSerde, 10);
        let wk = Windowed {
            key: "k".to_string(),
            window: Window { start: 20, end: 30 },
        };
        let b = s.serialize(&wk);
        assert_eq!(b.len(), 9); // "k"(1) ‖ 20i64 BE(8)
        assert_eq!(&b[1..9], &20i64.to_be_bytes());
        let back = s.deserialize(&b).unwrap();
        assert_eq!(back.key, "k");
        assert_eq!(back.window, Window { start: 20, end: 30 }); // end = start + size
    }
}
