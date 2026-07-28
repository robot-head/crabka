//! Time windows + the `Windowed<K>` output key + a windowed output serde.
use bytes::{BufMut, Bytes, BytesMut};
use crabka_units::prelude::*;

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

/// Tumbling / hopping time windows (epoch-aligned). `advance == size` is
/// tumbling; `advance < size` is hopping. `grace` contributes to changelog
/// retention and to [`Suppressed::until_window_closes`] timing when the resulting
/// table is suppressed.
///
/// The window bounds these produce are epoch-millisecond *instants* and stay
/// `i64`; the size, hop, and grace are *extents* and are [`Time`] quantities.
///
/// [`Suppressed::until_window_closes`]: crate::dsl::Suppressed::until_window_closes
#[derive(Debug, Clone, Copy)]
pub struct TimeWindows {
    pub size: Time,
    pub advance: Time,
    pub grace: Time,
}

impl TimeWindows {
    /// Tumbling window of `size` (advance == size, grace 0).
    #[must_use]
    /// # Panics
    /// Panics if synchronized client state is poisoned or a response violates an invariant established by protocol validation.
    pub fn of_size(size: Time) -> Self {
        assert!(size > Time::ZERO, "window size must be > 0");
        Self {
            size,
            advance: size,
            grace: Time::ZERO,
        }
    }
    /// Hopping: advance by `advance` (`0 < advance <= size`).
    #[must_use]
    /// # Panics
    /// Panics if synchronized client state is poisoned or a response violates an invariant established by protocol validation.
    pub fn advance_by(mut self, advance: Time) -> Self {
        assert!(
            advance > Time::ZERO && advance <= self.size,
            "0 < advance <= size"
        );
        self.advance = advance;
        self
    }
    /// Set the grace period (only affects changelog retention here).
    #[must_use]
    /// # Panics
    /// Panics if synchronized client state is poisoned or a response violates an invariant established by protocol validation.
    pub fn grace(mut self, grace: Time) -> Self {
        assert!(grace >= Time::ZERO, "grace must be >= 0");
        self.grace = grace;
        self
    }
    /// The window starts a timestamp `t` falls into (JVM `TimeWindows.windowsFor`).
    ///
    /// `t` and the returned starts are epoch-millisecond instants; the size and
    /// hop cross into that coordinate space here.
    #[must_use]
    pub fn windows_for(&self, t: i64) -> Vec<i64> {
        let size_ms = self.size.millis_i64();
        let advance_ms = self.advance.millis_i64();
        let mut start = (std::cmp::max(0, t - size_ms + advance_ms) / advance_ms) * advance_ms;
        let mut out = Vec::new();
        while start <= t {
            out.push(start);
            start += advance_ms;
        }
        out
    }
}

/// Symmetric-or-asymmetric join window: a record at `t` matches the other side's
/// records with timestamp in `[t - before, t + after]`. `JoinWindows::of` is
/// symmetric (before == after); `.before`/`.after` make it asymmetric.
///
/// `before` and `after` are extents measured from the joining record, not
/// absolute bounds, so both are [`Time`].
#[derive(Debug, Clone, Copy)]
pub struct JoinWindows {
    pub before: Time,
    pub after: Time,
    pub grace: Time,
}

impl JoinWindows {
    /// Symmetric window of `time_difference` before and after (grace 0).
    #[must_use]
    /// # Panics
    /// Panics if synchronized client state is poisoned or a response violates an invariant established by protocol validation.
    pub fn of(time_difference: Time) -> Self {
        assert!(
            time_difference >= Time::ZERO,
            "time difference must be >= 0"
        );
        Self {
            before: time_difference,
            after: time_difference,
            grace: Time::ZERO,
        }
    }
    #[must_use]
    /// # Panics
    /// Panics if synchronized client state is poisoned or a response violates an invariant established by protocol validation.
    pub fn before(mut self, before: Time) -> Self {
        assert!(before >= Time::ZERO, "before must be >= 0");
        self.before = before;
        self
    }
    #[must_use]
    /// # Panics
    /// Panics if synchronized client state is poisoned or a response violates an invariant established by protocol validation.
    pub fn after(mut self, after: Time) -> Self {
        assert!(after >= Time::ZERO, "after must be >= 0");
        self.after = after;
        self
    }
    #[must_use]
    /// # Panics
    /// Panics if synchronized client state is poisoned or a response violates an invariant established by protocol validation.
    pub fn grace(mut self, grace: Time) -> Self {
        assert!(grace >= Time::ZERO, "grace must be >= 0");
        self.grace = grace;
        self
    }
    /// Window size (= `before + after`) — the store retention basis.
    #[must_use]
    pub fn size(&self) -> Time {
        self.before + self.after
    }
}

/// Session windows: records for a key form one session while they stay within
/// `gap` of each other (inactivity gap). A session window `[start, end]` is
/// defined by data, not epoch-aligned. `grace` contributes to changelog retention
/// and to [`Suppressed::until_window_closes`] timing when the resulting table is
/// suppressed.
///
/// [`Suppressed::until_window_closes`]: crate::dsl::Suppressed::until_window_closes
#[derive(Debug, Clone, Copy)]
pub struct SessionWindows {
    pub gap: Time,
    pub grace: Time,
}

impl SessionWindows {
    /// Inactivity gap of `gap` (grace 0). `gap > 0`.
    #[must_use]
    /// # Panics
    /// Panics if synchronized client state is poisoned or a response violates an invariant established by protocol validation.
    pub fn of_inactivity_gap(gap: Time) -> Self {
        assert!(gap > Time::ZERO, "session gap must be > 0");
        Self {
            gap,
            grace: Time::ZERO,
        }
    }
    /// Set the grace period (only affects changelog retention here).
    #[must_use]
    /// # Panics
    /// Panics if synchronized client state is poisoned or a response violates an invariant established by protocol validation.
    pub fn grace(mut self, grace: Time) -> Self {
        assert!(grace >= Time::ZERO, "grace must be >= 0");
        self.grace = grace;
        self
    }
}

/// Sliding windows (KIP-450). A record at time `t` belongs to every window of
/// fixed size `time_difference` (`W`) that contains it — i.e. windows
/// `[ws, ws + W]` with `ws ∈ [t - W, t]`. Windows are **inclusive on both ends**
/// and **data-defined** (not epoch-aligned), so there is no `windows_for`: the
/// affected windows are discovered by scanning the window store. `grace` allows
/// out-of-order records up to `W + grace` behind stream time and feeds changelog
/// retention.
#[derive(Debug, Clone, Copy)]
pub struct SlidingWindows {
    /// Window size `W`; window `[start, start + time_difference]` (inclusive).
    pub time_difference: Time,
    pub grace: Time,
}

impl SlidingWindows {
    /// Time difference of `time_difference` with no grace.
    #[must_use]
    /// # Panics
    /// Panics if synchronized client state is poisoned or a response violates an invariant established by protocol validation.
    pub fn of_time_difference_with_no_grace(time_difference: Time) -> Self {
        assert!(
            time_difference >= Time::ZERO,
            "time difference must be >= 0"
        );
        Self {
            time_difference,
            grace: Time::ZERO,
        }
    }
    /// Time difference + grace period.
    #[must_use]
    /// # Panics
    /// Panics if synchronized client state is poisoned or a response violates an invariant established by protocol validation.
    pub fn of_time_difference_and_grace(time_difference: Time, grace: Time) -> Self {
        assert!(
            time_difference >= Time::ZERO,
            "time difference must be >= 0"
        );
        assert!(grace >= Time::ZERO, "grace must be >= 0");
        Self {
            time_difference,
            grace,
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
    size: Time,
}

impl<KS> TimeWindowedSerde<KS> {
    #[must_use]
    pub fn new(inner: KS, size: Time) -> Self {
        Self { inner, size }
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
                end: start + self.size.millis_i64(),
            },
        })
    }
}

impl<KS: SerdeAssociate> SerdeAssociate for TimeWindowedSerde<KS> {
    type Target = Windowed<KS::Target>;
}

#[cfg(test)]
mod tests {
    use assert2::check;

    use super::*;

    #[test]
    fn windows_for_tumbling_one_window() {
        let w = TimeWindows::of_size(millis(10));
        check!(w.windows_for(0) == vec![0]);
        check!(w.windows_for(9) == vec![0]);
        check!(w.windows_for(10) == vec![10]);
        check!(w.windows_for(25) == vec![20]);
    }

    #[test]
    fn windows_for_hopping_overlaps() {
        let w = TimeWindows::of_size(millis(10)).advance_by(millis(5));
        check!(w.windows_for(12) == vec![5, 10]); // start0 = max(0,12-10+5)/5*5 = 5
        check!(w.windows_for(0) == vec![0]);
    }

    #[test]
    fn join_windows_before_after_size() {
        let w = JoinWindows::of(millis(10));
        check!((w.before, w.after, w.grace) == (millis(10), millis(10), Time::ZERO));
        check!(w.size() == millis(20));
        let a = JoinWindows::of(millis(10))
            .before(millis(3))
            .after(millis(7))
            .grace(millis(5));
        check!((a.before, a.after, a.grace) == (millis(3), millis(7), millis(5)));
        check!(a.size() == millis(10));
    }

    #[test]
    fn time_windowed_serde_round_trips_output_format() {
        use crate::processor::serde::{Serde, StringSerde};
        let s = TimeWindowedSerde::new(StringSerde, millis(10));
        let wk = Windowed {
            key: "k".to_string(),
            window: Window { start: 20, end: 30 },
        };
        let b = s.serialize("t", &wk);
        check!(b.len() == 9); // "k"(1) ‖ 20i64 BE(8)
        check!(b[1..9] == 20i64.to_be_bytes());
        let back = s.deserialize("t", &b).unwrap();
        check!(back.key == "k");
        check!(back.window == Window { start: 20, end: 30 }); // end = start + size
    }

    #[test]
    fn session_windows_gap_and_grace() {
        let w = SessionWindows::of_inactivity_gap(secs(60));
        check!((w.gap, w.grace) == (secs(60), Time::ZERO));
        let g = SessionWindows::of_inactivity_gap(secs(60)).grace(millis(5));
        check!((g.gap, g.grace) == (secs(60), millis(5)));
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
        check!(b.len() == 17); // "k"(1) ‖ end:8 ‖ start:8
        check!(b[1..9] == 9i64.to_be_bytes()); // end first
        check!(b[9..17] == 5i64.to_be_bytes()); // start second
        let back = s.deserialize("t", &b).unwrap();
        check!(back.key == "k");
        check!(back.window == Window { start: 5, end: 9 });
    }

    #[test]
    fn sliding_windows_constructors() {
        let w = SlidingWindows::of_time_difference_with_no_grace(millis(100));
        check!((w.time_difference, w.grace) == (millis(100), Time::ZERO));
        let g = SlidingWindows::of_time_difference_and_grace(millis(100), millis(50));
        check!((g.time_difference, g.grace) == (millis(100), millis(50)));
    }

    #[test]
    #[should_panic(expected = "time difference must be >= 0")]
    fn sliding_windows_rejects_negative_difference() {
        let _ = SlidingWindows::of_time_difference_with_no_grace(Time::from_millis(-1));
    }
}
