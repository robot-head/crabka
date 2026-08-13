//! `pg_snapshot` — a transaction snapshot in the form `PostgreSQL` exports it.
//!
//! A snapshot is the triple `(xmin, xmax, xip)`. `xmin` is the lowest
//! transaction id that was still running, `xmax` is one past the highest id
//! that had been assigned, and `xip` lists the ids that were running between
//! them. Everything below `xmin` is settled, everything at or above `xmax`
//! started later, and an id inside the window is invisible exactly when `xip`
//! holds it. That is the same triple `crabka_pgmvcc::visibility::Snapshot`
//! carries, so the type exports engine state rather than describing it.
//!
//! The text form is `xmin:xmax:xip_list`, with the list comma-separated and
//! empty when nothing was running. `12:20:13,15,18` reads as "13, 15 and 18
//! were running; 12 through 19 is the undecided window".
//!
//! # Why an id with zero low bits is rejected
//!
//! `PostgreSQL` stores a transaction id as a 32-bit counter plus a 32-bit
//! epoch, and `FullTransactionIdIsValid` tests only the counter half. An id
//! whose low 32 bits are zero is therefore the invalid id of some epoch, and
//! `pg_snapshot_in` rejects it. `1:9223372036854775807:3` parses and
//! `1:9223372036854775808:3` does not, because 2^63 has a zero low half.
//!
//! Gres numbers transactions with a flat 64-bit counter and has no epoch, so
//! that rule describes no limit of its own. It is kept because it is part of
//! the *grammar* of the exported format, not part of the engine: a value that
//! one server accepts and another rejects would make the text form
//! unportable, which is the one thing the format exists for.

use std::{fmt, str::FromStr};

use crate::error::TypeError;

/// A transaction snapshot as `pg_current_snapshot()` reports it.
///
/// The invariants below hold for every value that exists, because the two
/// constructors are the only ways to build one:
///
/// * `xmin` and `xmax` are both valid ids, and `xmin <= xmax`;
/// * `xip` is sorted ascending and holds no duplicate;
/// * every member of `xip` lies in `xmin..xmax`.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct PgSnapshot {
    xmin: u64,
    xmax: u64,
    xip: Vec<u64>,
}

impl PgSnapshot {
    /// The type name that `PostgreSQL` puts in the 22P02 for a bad input, and
    /// that its own `txid_snapshot` errors also carry.
    const TYPE_NAME: &'static str = "pg_snapshot";

    /// Build a snapshot from the running set an engine reports.
    ///
    /// `running` is sorted and de-duplicated, which is what `sort_snapshot`
    /// does to the array `pg_current_snapshot()` copies out of the procarray.
    /// Ids outside `xmin..xmax` are dropped: such an id says nothing, because
    /// the window alone already decides it.
    #[must_use]
    pub fn from_running(xmin: u64, xmax: u64, running: &[u64]) -> Self {
        let mut xip: Vec<u64> = running
            .iter()
            .copied()
            .filter(|id| (xmin..xmax).contains(id))
            .collect();
        xip.sort_unstable();
        xip.dedup();
        Self { xmin, xmax, xip }
    }

    /// The lowest id that was still running — `pg_snapshot_xmin`.
    #[must_use]
    pub fn xmin(&self) -> u64 {
        self.xmin
    }

    /// One past the highest id that had been assigned — `pg_snapshot_xmax`.
    #[must_use]
    pub fn xmax(&self) -> u64 {
        self.xmax
    }

    /// The running ids, ascending — `pg_snapshot_xip`.
    #[must_use]
    pub fn xip(&self) -> &[u64] {
        &self.xip
    }

    /// Would this snapshot see the effects of transaction `xid`?
    ///
    /// This is `pg_visible_in_snapshot`. It reads the triple and nothing else,
    /// so it gives the same answer on any server for any snapshot, whether the
    /// snapshot was taken here or parsed from text.
    #[must_use]
    pub fn is_visible(&self, xid: u64) -> bool {
        if xid < self.xmin {
            return true;
        }
        if xid >= self.xmax {
            return false;
        }
        self.xip.binary_search(&xid).is_err()
    }
}

impl fmt::Display for PgSnapshot {
    /// `pg_snapshot_out` — `xmin:xmax:` then the ids, comma-separated.
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}:{}:", self.xmin, self.xmax)?;
        for (index, id) in self.xip.iter().enumerate() {
            if index > 0 {
                f.write_str(",")?;
            }
            write!(f, "{id}")?;
        }
        Ok(())
    }
}

impl FromStr for PgSnapshot {
    type Err = TypeError;

    /// `pg_snapshot_in` — 22P02 for anything the grammar rejects.
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        parse(s).ok_or_else(|| TypeError::InvalidText {
            type_name: Self::TYPE_NAME,
            value: s.to_string(),
        })
    }
}

/// Is this a transaction id the format admits?
///
/// `FullTransactionIdIsValid` tests the 32-bit counter half alone, so an id is
/// admissible exactly when its low 32 bits are not all zero. See the module
/// documentation for why Gres keeps a rule its own numbering does not need.
fn is_valid_id(id: u64) -> bool {
    id & 0xFFFF_FFFF != 0
}

/// The whole grammar, returning `None` for every rejection so that one caller
/// attaches the message and the offending text.
fn parse(s: &str) -> Option<PgSnapshot> {
    let (xmin, rest) = field(s)?;
    let (xmax, rest) = field(rest)?;
    if !is_valid_id(xmin) || !is_valid_id(xmax) || xmax < xmin {
        return None;
    }

    let mut xip: Vec<u64> = Vec::new();
    let mut rest = rest;
    let mut last: Option<u64> = None;
    while !rest.is_empty() {
        let (id, tail) = number(rest);
        // The list must arrive inside the window and in ascending order.
        // `PostgreSQL` checks the order rather than sorting, so `12:16:14,13`
        // is an error and not a re-ordering.
        if id < xmin || id >= xmax || last.is_some_and(|previous| id < previous) {
            return None;
        }
        if last != Some(id) {
            xip.push(id);
        }
        last = Some(id);
        rest = match tail.strip_prefix(',') {
            Some(after) => after,
            None if tail.is_empty() => tail,
            None => return None,
        };
    }

    Some(PgSnapshot { xmin, xmax, xip })
}

/// One colon-terminated field, and what follows the colon.
fn field(s: &str) -> Option<(u64, &str)> {
    let (value, rest) = number(s);
    rest.strip_prefix(':').map(|tail| (value, tail))
}

/// `strtou64` — the C conversion `parse_snapshot` reads each number with.
///
/// Leading white space and one sign are skipped, the digits are read as
/// decimal, and an overflow saturates the way `strtoull` reports `ERANGE`.
/// When there is no digit at all the value is zero and nothing is consumed,
/// which is what makes a missing field fail on the *validity* test rather than
/// on a separate "not a number" one.
fn number(s: &str) -> (u64, &str) {
    let digits_start = s
        .trim_start_matches([' ', '\t', '\n', '\u{b}', '\u{c}', '\r'])
        .strip_prefix(['+', '-'])
        .unwrap_or_else(|| s.trim_start_matches([' ', '\t', '\n', '\u{b}', '\u{c}', '\r']));
    let digits: &str = digits_start
        .split_once(|c: char| !c.is_ascii_digit())
        .map_or(digits_start, |(head, _)| head);
    if digits.is_empty() {
        return (0, s);
    }
    let magnitude = digits.parse::<u64>().unwrap_or(u64::MAX);
    let negated = s
        .trim_start_matches([' ', '\t', '\n', '\u{b}', '\u{c}', '\r'])
        .starts_with('-');
    let value = if negated {
        magnitude.wrapping_neg()
    } else {
        magnitude
    };
    (value, &digits_start[digits.len()..])
}

#[cfg(test)]
mod tests {
    use assert2::assert;

    use super::PgSnapshot;
    use crate::error::TypeError;

    /// The 31-element snapshot `xid.sql` uses to reach the binary-search path.
    const WIDE: &str = "100:150:101,102,103,104,105,106,107,108,109,110,111,112,113,114,115,116,\
                        117,118,119,120,121,122,123,124,125,126,127,128,129,130,131";

    fn parsed(text: &str) -> PgSnapshot {
        text.parse::<PgSnapshot>().expect("input is accepted")
    }

    #[test]
    fn accepted_input_prints_back_in_canonical_form() {
        let cases = [
            ("12:13:", "12:13:"),
            ("12:18:14,16", "12:18:14,16"),
            // A repeated id is one running transaction, so it is folded.
            ("12:16:14,14", "12:16:14"),
            (
                "100001:100009:100005,100007,100008",
                "100001:100009:100005,100007,100008",
            ),
            (
                "1000100010001000:1000100010001100:1000100010001012,1000100010001013",
                "1000100010001000:1000100010001100:1000100010001012,1000100010001013",
            ),
            ("1:9223372036854775807:3", "1:9223372036854775807:3"),
            (WIDE, WIDE),
        ];
        for (input, printed) in cases {
            assert!(parsed(input).to_string() == printed);
        }
    }

    #[test]
    fn parsing_yields_the_whole_triple() {
        assert!(
            parsed("12:20:13,15,18")
                == PgSnapshot {
                    xmin: 12,
                    xmax: 20,
                    xip: vec![13, 15, 18],
                }
        );
        assert!(
            parsed("12:13:")
                == PgSnapshot {
                    xmin: 12,
                    xmax: 13,
                    xip: vec![],
                }
        );
    }

    #[test]
    fn rejected_input_is_22p02_naming_the_whole_text() {
        let cases = [
            // xmax below xmin.
            "31:12:",
            // xmin is the invalid id.
            "0:1:",
            // An in-progress id below xmin.
            "12:13:0",
            // The list is out of order.
            "12:16:14,13",
            // 2^63 has a zero low half, so it is no id.
            "1:9223372036854775808:3",
            // An in-progress id at xmax.
            "12:16:16",
            // Trailing junk after the list.
            "12:16:14x",
            // Only one colon.
            "12:16",
        ];
        for input in cases {
            let error = input.parse::<PgSnapshot>().expect_err("input is rejected");
            assert!(
                error
                    == TypeError::InvalidText {
                        type_name: "pg_snapshot",
                        value: input.to_string(),
                    }
            );
            assert!(error.sqlstate() == "22P02");
        }
    }

    #[test]
    fn visibility_follows_the_window_then_the_running_set() {
        let snapshot = parsed("12:20:13,15,18");
        // `xid.sql` reads ids 11 through 21 against this snapshot.
        let expected = [
            (11, true),
            (12, true),
            (13, false),
            (14, true),
            (15, false),
            (16, true),
            (17, true),
            (18, false),
            (19, true),
            (20, false),
            (21, false),
        ];
        for (xid, visible) in expected {
            assert!(snapshot.is_visible(xid) == visible, "xid {xid}");
        }
    }

    #[test]
    fn visibility_over_a_wide_running_set_matches_the_window() {
        let snapshot = parsed(WIDE);
        for xid in 90..=160_u64 {
            let expected = xid < 100 || (xid < 150 && !(101..=131).contains(&xid));
            assert!(snapshot.is_visible(xid) == expected, "xid {xid}");
        }
    }

    #[test]
    fn a_parsed_snapshot_answers_its_own_components() {
        let snapshot = parsed("12:20:13,15,18");
        assert!(snapshot.xmin() == 12);
        assert!(snapshot.xmax() == 20);
        assert!(snapshot.xip() == [13, 15, 18]);
    }

    #[test]
    fn a_running_set_is_sorted_deduplicated_and_clipped_to_the_window() {
        let snapshot = PgSnapshot::from_running(12, 20, &[18, 13, 15, 13, 7, 20, 25]);
        assert!(
            snapshot
                == PgSnapshot {
                    xmin: 12,
                    xmax: 20,
                    xip: vec![13, 15, 18],
                }
        );
        assert!(snapshot.to_string() == "12:20:13,15,18");
    }

    #[test]
    fn an_empty_window_sees_everything_below_it() {
        let snapshot = PgSnapshot::from_running(13, 13, &[]);
        assert!(snapshot.to_string() == "13:13:");
        assert!(snapshot.is_visible(12));
        assert!(!snapshot.is_visible(13));
    }
}
