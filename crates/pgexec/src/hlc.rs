//! Packed-`u64` Hybrid Logical Clock for distributed-mode timestamp allocation.
//!
//! A Hybrid Logical Clock timestamp is one `u64`: the physical component
//! (milliseconds since a Crabka epoch) occupies the high [`PHYSICAL_BITS`] and a
//! logical counter the low [`LOGICAL_BITS`]. This single packing makes numeric
//! order, big-endian byte order, and HLC happens-before order coincide, so the
//! existing MVCC key/tuple encodings and the `commit_ts <= read_ts` visibility
//! check are untouched by mode (see the timestamp-source seam design). A stamp
//! whose physical component is zero — a purely logical timestamp — sorts below
//! every stamp with a real physical component, which is what lets a solo tenant
//! be promoted onto the HLC monotonically.
//!
//! Wall-clock time is *injected* into [`HybridLogicalClock::now`] and
//! [`HybridLogicalClock::observe`] rather than read from `SystemTime`, so the
//! clock is deterministic under test.

use std::sync::atomic::{AtomicU64, Ordering};

/// Low bits of a packed stamp holding the logical counter.
pub const LOGICAL_BITS: u32 = 22;

/// High bits of a packed stamp holding the physical millisecond component.
pub const PHYSICAL_BITS: u32 = 42;

/// Largest representable logical counter (`2^LOGICAL_BITS - 1`), about 4M
/// causally related events per physical millisecond.
pub const MAX_LOGICAL: u32 = (1 << LOGICAL_BITS) - 1;

/// Largest representable physical millisecond component (`2^PHYSICAL_BITS - 1`),
/// over a century from a Crabka epoch.
pub const MAX_PHYSICAL_MS: u64 = (1 << PHYSICAL_BITS) - 1;

const LOGICAL_MASK: u64 = MAX_LOGICAL as u64;

/// The unpacked components of a packed HLC stamp.
///
/// Field order is deliberate: the derived [`Ord`] compares `physical_ms` before
/// `logical`, matching the numeric order of the packed `u64` exactly.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub struct Hlc {
    /// Physical component in milliseconds (zero for a purely logical stamp).
    pub physical_ms: u64,
    /// Logical counter distinguishing events within one physical millisecond.
    pub logical: u32,
}

/// Pack a physical millisecond value and a logical counter into one stamp.
///
/// Each component is masked to its field width ([`MAX_PHYSICAL_MS`],
/// [`MAX_LOGICAL`]); a sane millisecond clock stays inside [`PHYSICAL_BITS`] for
/// over a century, and the clock rules below never let the logical counter
/// exceed [`MAX_LOGICAL`].
#[must_use]
pub fn pack(physical_ms: u64, logical: u32) -> u64 {
    ((physical_ms & MAX_PHYSICAL_MS) << LOGICAL_BITS) | (u64::from(logical) & LOGICAL_MASK)
}

/// Split a packed stamp back into its physical and logical components.
#[must_use]
pub fn unpack(packed: u64) -> Hlc {
    Hlc {
        physical_ms: packed >> LOGICAL_BITS,
        // The mask keeps at most `LOGICAL_BITS`, which always fits `u32`; the
        // fallback is unreachable and only spares an infallible `unwrap`.
        logical: u32::try_from(packed & LOGICAL_MASK).unwrap_or(MAX_LOGICAL),
    }
}

/// Increment the logical component of `packed`, rolling into the physical
/// component when the counter is already saturated.
///
/// Rolling into physical is chosen over failing: the clock never blocks or
/// returns an error, and a stamp that borrows a millisecond from the future
/// stays within the skew budget the mode already tolerates. `(physical + 1, 0)`
/// strictly dominates `(physical, MAX_LOGICAL)`, so monotonicity is preserved.
fn bump_logical(packed: u64) -> u64 {
    let hlc = unpack(packed);
    if hlc.logical < MAX_LOGICAL {
        pack(hlc.physical_ms, hlc.logical + 1)
    } else {
        pack(hlc.physical_ms + 1, 0)
    }
}

/// The HLC local/send rule: advance `last` against injected wall time.
fn advance_local(last: u64, wall_ms: u64) -> u64 {
    let last_physical = last >> LOGICAL_BITS;
    if wall_ms > last_physical {
        pack(wall_ms, 0)
    } else {
        bump_logical(last)
    }
}

/// The HLC receive rule: fold a remote stamp and injected wall time into `last`.
///
/// The result strictly dominates both `last` and `remote`: the new physical
/// component is the max of all three inputs, and the logical component is bumped
/// past whichever input(s) supplied that max.
fn receive(last: u64, remote: u64, wall_ms: u64) -> u64 {
    let last = unpack(last);
    let remote = unpack(remote);
    let physical = last.physical_ms.max(remote.physical_ms).max(wall_ms);
    let logical = if physical == last.physical_ms && physical == remote.physical_ms {
        last.logical.max(remote.logical) + 1
    } else if physical == last.physical_ms {
        last.logical + 1
    } else if physical == remote.physical_ms {
        remote.logical + 1
    } else {
        0
    };
    if logical > MAX_LOGICAL {
        pack(physical + 1, 0)
    } else {
        pack(physical, logical)
    }
}

/// A node-local Hybrid Logical Clock over a single packed-`u64` stamp.
///
/// The last-issued stamp lives in an [`AtomicU64`] updated with a
/// compare-exchange loop rather than behind a [`std::sync::Mutex`]: every
/// timestamp allocation in HLC mode hits this clock, the critical section is a
/// handful of branch-free integer operations with no allocation and no `.await`,
/// and a lock-free retry avoids both lock contention on that hot path and the
/// poisoning ceremony a `Mutex` would add. It mirrors the atomic `fetch_max`
/// discipline the durable-horizon cache already uses.
#[derive(Debug)]
pub struct HybridLogicalClock {
    last: AtomicU64,
}

impl Default for HybridLogicalClock {
    fn default() -> Self {
        Self::seeded_at(0)
    }
}

impl HybridLogicalClock {
    /// Create a clock whose last-issued stamp is zero (a purely logical stamp
    /// below every real-physical stamp).
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Create a clock seeded so no stamp it issues can fall at or below `packed`.
    ///
    /// Node startup folds the local durable horizon in this way before serving
    /// its first stamp, discharging the seam's durable-horizon obligation.
    #[must_use]
    pub fn seeded_at(packed: u64) -> Self {
        Self {
            last: AtomicU64::new(packed),
        }
    }

    /// Return the last-issued stamp without advancing the clock.
    #[must_use]
    pub fn peek(&self) -> u64 {
        self.last.load(Ordering::Acquire)
    }

    /// Issue a stamp under the HLC local/send rule against injected `wall_ms`.
    ///
    /// When `wall_ms` exceeds the last physical component the stamp jumps to
    /// `(wall_ms, 0)`; otherwise the physical component is held and the logical
    /// counter increments. The result never regresses.
    pub fn now(&self, wall_ms: u64) -> u64 {
        let wall_ms = wall_ms.min(MAX_PHYSICAL_MS);
        self.update(|last| advance_local(last, wall_ms))
    }

    /// Fold `remote_packed` and injected `wall_ms` into the clock under the HLC
    /// receive rule and issue the resulting stamp.
    ///
    /// The result never regresses and strictly dominates both the prior state
    /// and `remote_packed`.
    pub fn observe(&self, remote_packed: u64, wall_ms: u64) -> u64 {
        let wall_ms = wall_ms.min(MAX_PHYSICAL_MS);
        self.update(|last| receive(last, remote_packed, wall_ms))
    }

    fn update(&self, transition: impl Fn(u64) -> u64) -> u64 {
        let mut last = self.last.load(Ordering::Acquire);
        loop {
            let candidate = transition(last);
            match self.last.compare_exchange_weak(
                last,
                candidate,
                Ordering::AcqRel,
                Ordering::Acquire,
            ) {
                Ok(_) => return candidate,
                Err(current) => last = current,
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use assert2::assert;

    use super::*;

    #[test]
    fn pack_unpack_round_trips() {
        let cases = [
            (0_u64, 0_u32),
            (0, MAX_LOGICAL),
            (1, 0),
            (1, 1),
            (1_700_000_000_000, 7),
            (MAX_PHYSICAL_MS, MAX_LOGICAL),
        ];
        for (physical_ms, logical) in cases {
            let expected = Hlc {
                physical_ms,
                logical,
            };
            assert!(unpack(pack(physical_ms, logical)) == expected);
        }
    }

    #[test]
    fn pack_masks_out_of_range_components() {
        // A logical value above the field width wraps into the field, never into
        // the physical component.
        assert!(
            unpack(pack(5, MAX_LOGICAL + 1))
                == Hlc {
                    physical_ms: 5,
                    logical: 0
                }
        );
    }

    #[test]
    fn packed_order_matches_lexicographic_physical_logical() {
        let stamps = [
            (0_u64, 0_u32),
            (0, 1),
            (0, MAX_LOGICAL),
            (1, 0),
            (1, 5),
            (2, 0),
            (1_700_000_000_000, 0),
            (1_700_000_000_000, 9),
            (MAX_PHYSICAL_MS, MAX_LOGICAL),
        ];
        for &a in &stamps {
            for &b in &stamps {
                let numeric = pack(a.0, a.1).cmp(&pack(b.0, b.1));
                let lexicographic = a.cmp(&b);
                assert!(numeric == lexicographic);
            }
        }
    }

    #[test]
    fn logical_only_stamp_sorts_below_real_physical_stamp() {
        assert!(pack(0, MAX_LOGICAL) < pack(1, 0));
    }

    #[test]
    fn now_advances_physical_when_wall_moves() {
        let clock = HybridLogicalClock::new();
        assert!(
            unpack(clock.now(5))
                == Hlc {
                    physical_ms: 5,
                    logical: 0
                }
        );
        assert!(
            unpack(clock.now(9))
                == Hlc {
                    physical_ms: 9,
                    logical: 0
                }
        );
    }

    #[test]
    fn now_increments_logical_when_wall_stalls() {
        let clock = HybridLogicalClock::new();
        let first = unpack(clock.now(5));
        let second = unpack(clock.now(5));
        let third = unpack(clock.now(5));
        assert!(
            first
                == Hlc {
                    physical_ms: 5,
                    logical: 0
                }
        );
        assert!(
            second
                == Hlc {
                    physical_ms: 5,
                    logical: 1
                }
        );
        assert!(
            third
                == Hlc {
                    physical_ms: 5,
                    logical: 2
                }
        );
    }

    #[test]
    fn stalled_wall_forces_physical_progress_only_when_wall_advances() {
        let clock = HybridLogicalClock::new();
        clock.now(5);
        clock.now(5);
        // Wall stuck at or below the physical component keeps the physical
        // component pinned while the logical counter climbs.
        assert!(
            unpack(clock.now(3))
                == Hlc {
                    physical_ms: 5,
                    logical: 2
                }
        );
        // Only a wall value above the physical component moves it forward, and
        // it resets the logical counter.
        assert!(
            unpack(clock.now(6))
                == Hlc {
                    physical_ms: 6,
                    logical: 0
                }
        );
    }

    #[test]
    fn now_never_regresses_when_wall_goes_backwards() {
        let clock = HybridLogicalClock::new();
        let mut previous = clock.now(100);
        for wall_ms in [50, 100, 100, 10, 101] {
            let next = clock.now(wall_ms);
            assert!(next > previous);
            previous = next;
        }
    }

    #[test]
    fn logical_overflow_rolls_into_physical() {
        let clock = HybridLogicalClock::seeded_at(pack(7, MAX_LOGICAL));
        // Wall stalled at the physical component: the saturated logical counter
        // rolls into the next millisecond rather than failing or wrapping.
        assert!(
            unpack(clock.now(7))
                == Hlc {
                    physical_ms: 8,
                    logical: 0
                }
        );
    }

    #[test]
    fn observe_result_dominates_prior_state_and_remote() {
        let cases = [
            (pack(10, 5), pack(3, 0), 2_u64),
            (pack(10, 5), pack(10, 9), 4),
            (pack(10, 5), pack(20, 0), 1),
            (pack(0, 0), pack(0, 0), 0),
            (pack(4, MAX_LOGICAL), pack(4, MAX_LOGICAL), 4),
        ];
        for (last, remote, wall_ms) in cases {
            let clock = HybridLogicalClock::seeded_at(last);
            let result = clock.observe(remote, wall_ms);
            assert!(result > last);
            assert!(result > remote);
        }
    }

    #[test]
    fn observe_folds_remote_so_next_now_exceeds_it() {
        let clock = HybridLogicalClock::new();
        clock.now(5);
        let remote = pack(1_000, 3);
        let observed = clock.observe(remote, 6);
        assert!(observed > remote);
        // The remote physical component is now baked into local state, so a
        // subsequent read with stale wall time still exceeds it.
        assert!(clock.now(6) > remote);
    }

    #[test]
    fn interleaved_now_and_observe_is_monotonic() {
        let clock = HybridLogicalClock::new();
        let mut previous = 0;
        let steps: [(bool, u64, u64); 8] = [
            (false, 0, 10),
            (false, 0, 10),
            (true, pack(50, 2), 5),
            (false, 0, 20),
            (true, pack(20, 0), 20),
            (false, 0, 3),
            (true, pack(1_000, 9), 1),
            (false, 0, 1_000),
        ];
        for (is_observe, remote, wall_ms) in steps {
            let next = if is_observe {
                clock.observe(remote, wall_ms)
            } else {
                clock.now(wall_ms)
            };
            assert!(next > previous);
            if is_observe {
                assert!(next > remote);
            }
            previous = next;
        }
    }
}
