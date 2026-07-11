//! Per-broker audit hash-chain primitives.
//!
//! The chain binds each record to all prior records: `new_head =
//! SHA256(prev_head ‖ seq_be ‖ value)`. The writer stamps `(seq, prev_head)`
//! on each record; the verifier recomputes the chain and checks continuity.
//! Writer and verifier MUST use these functions — never reimplement the formula.

use std::fmt::Write as _;

use sha2::{Digest, Sha256};

/// Chain head before any record has been written.
pub const GENESIS_HEAD: [u8; 32] = [0u8; 32];

/// Canonical chain hash. `new_head = SHA256(prev ‖ seq.to_be_bytes() ‖ value)`.
#[must_use]
pub fn chain_hash(prev: &[u8; 32], seq: u64, value: &[u8]) -> [u8; 32] {
    let mut h = Sha256::new();
    h.update(prev);
    h.update(seq.to_be_bytes());
    h.update(value);
    h.finalize().into()
}

/// Running per-broker chain state.
#[derive(Debug, Clone)]
pub struct ChainState {
    next_seq: u64,
    head: [u8; 32],
}

impl Default for ChainState {
    fn default() -> Self {
        Self {
            next_seq: 0,
            head: GENESIS_HEAD,
        }
    }
}

impl ChainState {
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Resume a chain from a recovered position (after restart / spool replay).
    #[must_use]
    pub fn resume(next_seq: u64, head: [u8; 32]) -> Self {
        Self { next_seq, head }
    }

    /// Advance the chain for a record carrying `value`. Returns the `(seq,
    /// prev_head)` to stamp on that record (the head as it was *before* this
    /// record), then folds the record into the head.
    #[tracing::instrument(level = "debug", skip_all, fields(seq = self.next_seq, bytes = value.len()))]
    pub fn extend(&mut self, value: &[u8]) -> (u64, [u8; 32]) {
        let seq = self.next_seq;
        let prev = self.head;
        self.head = chain_hash(&prev, seq, value);
        self.next_seq += 1;
        (seq, prev)
    }

    #[must_use]
    pub fn head(&self) -> [u8; 32] {
        self.head
    }

    #[must_use]
    pub fn next_seq(&self) -> u64 {
        self.next_seq
    }
}

/// Lowercase hex encoding.
#[must_use]
pub fn to_hex(bytes: &[u8]) -> String {
    let mut s = String::with_capacity(bytes.len() * 2);
    for b in bytes {
        let _ = write!(s, "{b:02x}");
    }
    s
}

/// Parse exactly 32 bytes of lowercase/uppercase hex.
#[must_use]
pub fn from_hex32(s: &str) -> Option<[u8; 32]> {
    if s.len() != 64 {
        return None;
    }
    let mut out = [0u8; 32];
    let bytes = s.as_bytes();
    let mut i = 0;
    while i < 32 {
        let hi = (bytes[i * 2] as char).to_digit(16)?;
        let lo = (bytes[i * 2 + 1] as char).to_digit(16)?;
        out[i] = u8::try_from(hi * 16 + lo).ok()?;
        i += 1;
    }
    Some(out)
}

#[cfg(test)]
mod tests {
    use assert2::check;

    use super::*;

    #[test]
    fn genesis_is_zero() {
        check!(GENESIS_HEAD == [0u8; 32]);
    }

    #[test]
    fn chain_hash_is_deterministic_and_order_sensitive() {
        let a = chain_hash(&GENESIS_HEAD, 0, b"alpha");
        let b = chain_hash(&GENESIS_HEAD, 0, b"alpha");
        check!(a == b);
        // different seq => different hash
        check!(chain_hash(&GENESIS_HEAD, 1, b"alpha") != a);
        // different value => different hash
        check!(chain_hash(&GENESIS_HEAD, 0, b"beta") != a);
        // different prev => different hash
        check!(chain_hash(&a, 0, b"alpha") != a);
    }

    #[test]
    fn chain_state_advances_and_reports_prev() {
        let mut c = ChainState::new();
        check!(c.next_seq() == 0);
        let (s0, p0) = c.extend(b"r0");
        check!(
            (s0, p0, c.next_seq(), c.head())
                == (0, GENESIS_HEAD, 1, chain_hash(&GENESIS_HEAD, 0, b"r0"))
        );

        let (s1, p1) = c.extend(b"r1");
        check!(
            (s1, p1, c.head())
                == (
                    1,
                    chain_hash(&GENESIS_HEAD, 0, b"r0"),
                    chain_hash(&p1, 1, b"r1"),
                )
        );
    }

    #[test]
    fn resume_sets_next_seq_and_head_and_continues() {
        let head = chain_hash(&GENESIS_HEAD, 4, b"r4");
        let mut c = ChainState::resume(5, head);
        check!((c.next_seq(), c.head()) == (5, head));
        let (seq, prev) = c.extend(b"r5");
        check!((seq, prev, c.head()) == (5, head, chain_hash(&head, 5, b"r5")));
    }

    #[test]
    fn hex_round_trips() {
        let h = chain_hash(&GENESIS_HEAD, 7, b"x");
        let s = to_hex(&h);
        check!(s.len() == 64);
        for (name, input, expected) in [
            ("round trip", s.as_str(), Some(h)),
            ("non-hex input", "zz", None),
            ("odd-length input", "abc", None),
        ] {
            check!(from_hex32(input) == expected, "case {name}");
        }
    }
}
