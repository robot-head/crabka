//! Time windows + the `Windowed<K>` output key + a windowed output serde.
use bytes::{BufMut, Bytes, BytesMut};

use crate::processor::serde::{Serde, SerdeAssociate, SerdeError};

/// A time window (epoch millis). Time windows ([`TimeWindows`]) are half-open
/// `[start, end)`; session windows ([`SessionWindows`]) are inclusive `[start,
/// end]` (both bounds are observed record timestamps). The interpretation is
/// carried by the producing operator, not encoded in this struct.
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
/// tumbling; `advance_ms < size_ms` is hopping. `grace_ms` contributes to
/// changelog retention and to [`Suppressed::until_window_closes`] timing when the
/// resulting table is suppressed.
///
/// [`Suppressed::until_window_closes`]: crate::dsl::Suppressed::until_window_closes
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

/// Symmetric-or-asymmetric join window: a record at `t` matches the other side's
/// records with timestamp in `[t - before_ms, t + after_ms]`. `JoinWindows::of`
/// is symmetric (before == after); `.before`/`.after` make it asymmetric.
#[derive(Debug, Clone, Copy)]
pub struct JoinWindows {
    pub before_ms: i64,
    pub after_ms: i64,
    pub grace_ms: i64,
}

impl JoinWindows {
    /// Symmetric window of `time_difference_ms` before and after (grace 0).
    #[must_use]
    pub fn of(time_difference_ms: i64) -> Self {
        assert!(time_difference_ms >= 0, "time difference must be >= 0");
        Self {
            before_ms: time_difference_ms,
            after_ms: time_difference_ms,
            grace_ms: 0,
        }
    }
    #[must_use]
    pub fn before(mut self, before_ms: i64) -> Self {
        assert!(before_ms >= 0, "before must be >= 0");
        self.before_ms = before_ms;
        self
    }
    #[must_use]
    pub fn after(mut self, after_ms: i64) -> Self {
        assert!(after_ms >= 0, "after must be >= 0");
        self.after_ms = after_ms;
        self
    }
    #[must_use]
    pub fn grace(mut self, grace_ms: i64) -> Self {
        assert!(grace_ms >= 0, "grace must be >= 0");
        self.grace_ms = grace_ms;
        self
    }
    /// Window size (= `before + after`) — the store retention basis.
    #[must_use]
    pub fn size(&self) -> i64 {
        self.before_ms + self.after_ms
    }
}

/// Session windows: records for a key form one session while they stay within
/// `gap_ms` of each other (inactivity gap). A session window `[start, end]` is
/// defined by data, not epoch-aligned. `grace_ms` contributes to changelog
/// retention and to [`Suppressed::until_window_closes`] timing when the resulting
/// table is suppressed.
///
/// [`Suppressed::until_window_closes`]: crate::dsl::Suppressed::until_window_closes
#[derive(Debug, Clone, Copy)]
pub struct SessionWindows {
    pub gap_ms: i64,
    pub grace_ms: i64,
}

impl SessionWindows {
    /// Inactivity gap of `gap_ms` (grace 0). `gap_ms > 0`.
    #[must_use]
    pub fn of_inactivity_gap(gap_ms: i64) -> Self {
        assert!(gap_ms > 0, "session gap must be > 0");
        Self {
            gap_ms,
            grace_ms: 0,
        }
    }
    /// Set the grace period (only affects changelog retention here).
    #[must_use]
    pub fn grace(mut self, grace_ms: i64) -> Self {
        assert!(grace_ms >= 0, "grace must be >= 0");
        self.grace_ms = grace_ms;
        self
    }
}

/// Sliding windows (KIP-450). A record at time `t` belongs to every window of
/// fixed size `time_difference_ms` (`W`) that contains it — i.e. windows
/// `[ws, ws + W]` with `ws ∈ [t - W, t]`. Windows are **inclusive on both ends**
/// and **data-defined** (not epoch-aligned), so there is no `windows_for`: the
/// affected windows are discovered by scanning the window store. `grace_ms`
/// allows out-of-order records up to `W + grace_ms` behind stream time and feeds
/// changelog retention.
#[derive(Debug, Clone, Copy)]
pub struct SlidingWindows {
    /// Window size `W`; window `[start, start + time_difference_ms]` (inclusive).
    pub time_difference_ms: i64,
    pub grace_ms: i64,
}

impl SlidingWindows {
    /// Time difference of `time_difference_ms` with no grace.
    #[must_use]
    pub fn of_time_difference_with_no_grace(time_difference_ms: i64) -> Self {
        assert!(time_difference_ms >= 0, "time difference must be >= 0");
        Self {
            time_difference_ms,
            grace_ms: 0,
        }
    }
    /// Time difference + grace period.
    #[must_use]
    pub fn of_time_difference_and_grace(time_difference_ms: i64, grace_ms: i64) -> Self {
        assert!(time_difference_ms >= 0, "time difference must be >= 0");
        assert!(grace_ms >= 0, "grace must be >= 0");
        Self {
            time_difference_ms,
            grace_ms,
        }
    }
}

/// `Serde<Windowed<K>>` producing the JVM session **output-topic** format:
/// `inner_key_bytes ‖ end:8B BE ‖ start:8B BE` (both bounds in the bytes; distinct
/// from `TimeWindowedSerde`, which encodes only the start and derives `end`).
#[derive(Debug, Clone, Copy)]
pub struct SessionWindowedSerde<KS> {
    inner: KS,
}

impl<KS> SessionWindowedSerde<KS> {
    #[must_use]
    pub fn new(inner: KS) -> Self {
        Self { inner }
    }
}

impl<K, KS> Serde<Windowed<K>> for SessionWindowedSerde<KS>
where
    K: Send + Sync + 'static,
    KS: Serde<K>,
{
    fn serialize(&self, topic: &str, value: &Windowed<K>) -> Bytes {
        let kb = self.inner.serialize(topic, &value.key);
        let mut b = BytesMut::with_capacity(kb.len() + 16);
        b.extend_from_slice(&kb);
        b.put_i64(value.window.end);
        b.put_i64(value.window.start);
        b.freeze()
    }
    fn deserialize(&self, topic: &str, bytes: &[u8]) -> Result<Windowed<K>, SerdeError> {
        if bytes.len() < 16 {
            return Err(SerdeError(format!(
                "session key too short: {}",
                bytes.len()
            )));
        }
        let split = bytes.len() - 16;
        let key = self.inner.deserialize(topic, &bytes[..split])?;
        let end = i64::from_be_bytes(bytes[split..split + 8].try_into().expect("8 bytes"));
        let start = i64::from_be_bytes(bytes[split + 8..].try_into().expect("8 bytes"));
        Ok(Windowed {
            key,
            window: Window { start, end },
        })
    }
}
impl<KS: SerdeAssociate> SerdeAssociate for SessionWindowedSerde<KS> {
    type Target = Windowed<KS::Target>;
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
    fn serialize(&self, topic: &str, value: &Windowed<K>) -> Bytes {
        let kb = self.inner.serialize(topic, &value.key);
        let mut b = BytesMut::with_capacity(kb.len() + 8);
        b.extend_from_slice(&kb);
        b.put_i64(value.window.start);
        b.freeze()
    }
    fn deserialize(&self, topic: &str, bytes: &[u8]) -> Result<Windowed<K>, SerdeError> {
        if bytes.len() < 8 {
            return Err(SerdeError(format!(
                "windowed key too short: {}",
                bytes.len()
            )));
        }
        let split = bytes.len() - 8;
        let key = self.inner.deserialize(topic, &bytes[..split])?;
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

impl<KS: SerdeAssociate> SerdeAssociate for TimeWindowedSerde<KS> {
    type Target = Windowed<KS::Target>;
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn windows_for_tumbling_one_window() {
        let w = TimeWindows::of_size(10);
        for (name, timestamp, expected) in [
            ("window start", 0, vec![0]),
            ("window end minus one", 9, vec![0]),
            ("next window start", 10, vec![10]),
            ("later window", 25, vec![20]),
        ] {
            assert_eq!(w.windows_for(timestamp), expected, "case {name}");
        }
    }

    #[test]
    fn windows_for_hopping_overlaps() {
        let w = TimeWindows::of_size(10).advance_by(5);
        for (name, timestamp, expected) in [
            ("overlapping windows", 12, vec![5, 10]),
            ("initial window", 0, vec![0]),
        ] {
            assert_eq!(w.windows_for(timestamp), expected, "case {name}");
        }
    }

    #[test]
    fn join_windows_before_after_size() {
        let w = JoinWindows::of(10);
        let a = JoinWindows::of(10).before(3).after(7).grace(5);
        assert_eq!(
            (
                (w.before_ms, w.after_ms, w.grace_ms, w.size()),
                (a.before_ms, a.after_ms, a.grace_ms, a.size())
            ),
            ((10, 10, 0, 20), (3, 7, 5, 10))
        );
    }

    #[test]
    fn time_windowed_serde_round_trips_output_format() {
        use crate::processor::serde::{Serde, StringSerde};
        let s = TimeWindowedSerde::new(StringSerde, 10);
        let wk = Windowed {
            key: "k".to_string(),
            window: Window { start: 20, end: 30 },
        };
        let b = s.serialize("t", &wk);
        assert_eq!(b.len(), 9); // "k"(1) ‖ 20i64 BE(8)
        assert_eq!(&b[1..9], &20i64.to_be_bytes());
        let back = s.deserialize("t", &b).unwrap();
        assert_eq!(back, wk);
    }

    #[test]
    fn session_windows_gap_and_grace() {
        let w = SessionWindows::of_inactivity_gap(60_000);
        assert_eq!((w.gap_ms, w.grace_ms), (60_000, 0));
        let g = SessionWindows::of_inactivity_gap(60_000).grace(5);
        assert_eq!((g.gap_ms, g.grace_ms), (60_000, 5));
    }

    #[test]
    fn session_windowed_serde_round_trips_end_then_start() {
        use crate::processor::serde::{Serde, StringSerde};
        let s = SessionWindowedSerde::new(StringSerde);
        let wk = Windowed {
            key: "k".to_string(),
            window: Window { start: 5, end: 9 },
        };
        let b = s.serialize("t", &wk);
        assert_eq!(b.len(), 17); // "k"(1) ‖ end:8 ‖ start:8
        assert_eq!(&b[1..9], &9i64.to_be_bytes()); // end first
        assert_eq!(&b[9..17], &5i64.to_be_bytes()); // start second
        let back = s.deserialize("t", &b).unwrap();
        assert_eq!(back, wk);
    }

    #[test]
    fn sliding_windows_constructors() {
        let w = SlidingWindows::of_time_difference_with_no_grace(100);
        assert_eq!((w.time_difference_ms, w.grace_ms), (100, 0));
        let g = SlidingWindows::of_time_difference_and_grace(100, 50);
        assert_eq!((g.time_difference_ms, g.grace_ms), (100, 50));
    }

    #[test]
    #[should_panic(expected = "time difference must be >= 0")]
    fn sliding_windows_rejects_negative_difference() {
        let _ = SlidingWindows::of_time_difference_with_no_grace(-1);
    }
}
