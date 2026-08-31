//! S2: SQL cursor position algebra.
//!
//! A Gres cursor materializes its whole result at its first `FETCH`/`MOVE`.
//! Beyond its rows, a cursor therefore carries only a position. This module owns that position and
//! `PostgreSQL`'s `FETCH`/`MOVE` direction semantics, independent of how a row
//! is represented.
//!
//! The position model is `PostgreSQL`'s: `0` means "before the first row",
//! `1..=count` means "on that row", and `count + 1` means "after the last row".
//! A `FETCH` moves first and reports the rows it moved over. A `MOVE` is the
//! same walk, but it discards the rows.

use crabka_pgparser::ast::{FetchCount, FetchDirection};

/// A cursor's current position within a materialized result.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct CursorPosition {
    /// `0` before the first row, `1..=count` on a row, `count + 1` past the end.
    position: usize,
    count: usize,
}

/// The rows a `FETCH`/`MOVE` walks over, in the order `PostgreSQL` returns them.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct FetchPlan {
    /// One-based row numbers, in output order. A backward walk is descending.
    pub(crate) rows: Vec<usize>,
    /// Whether `PostgreSQL` rejects this operation for a `NO SCROLL` cursor.
    pub(crate) backward: bool,
}

impl CursorPosition {
    /// A freshly declared cursor: before the first of `count` rows.
    pub(crate) const fn new(count: usize) -> Self {
        Self { position: 0, count }
    }

    /// The current one-based row, or `None` while positioned before or after
    /// the materialized result.
    pub(crate) const fn current_row(self) -> Option<usize> {
        if self.position >= 1 && self.position <= self.count {
            Some(self.position)
        } else {
            None
        }
    }

    /// Walk `direction` from the current position, return the rows crossed, and
    /// leave the position where `PostgreSQL` leaves it.
    pub(crate) fn walk(&mut self, direction: FetchDirection) -> FetchPlan {
        match direction {
            FetchDirection::Relative(FetchCount::AllForward) => {
                // One step past the remaining rows: `FETCH ALL` leaves the
                // cursor after the last row, not on it.
                let remaining =
                    i64::try_from(self.count.saturating_sub(self.position)).unwrap_or(i64::MAX);
                self.forward(remaining.saturating_add(1))
            }
            FetchDirection::Relative(FetchCount::AllBackward) => {
                self.backward(i64::try_from(self.position).unwrap_or(i64::MAX))
            }
            FetchDirection::Relative(FetchCount::Rows(rows)) => {
                if rows >= 0 {
                    self.forward(rows)
                } else {
                    self.backward(rows.saturating_neg())
                }
            }
            FetchDirection::RelativeOne(0) => {
                // `RELATIVE 0` re-fetches the current row without moving.
                let on_row = self.position >= 1 && self.position <= self.count;
                FetchPlan {
                    rows: if on_row {
                        vec![self.position]
                    } else {
                        Vec::new()
                    },
                    backward: false,
                }
            }
            FetchDirection::RelativeOne(offset) => {
                let target = i64::try_from(self.position).unwrap_or(i64::MAX) + offset;
                self.jump(target, offset < 0)
            }
            FetchDirection::Absolute(0) => {
                let backward = self.position > 0;
                self.position = 0;
                FetchPlan {
                    rows: Vec::new(),
                    backward,
                }
            }
            FetchDirection::Absolute(target) if target > 0 => {
                let backward = target < i64::try_from(self.position).unwrap_or(i64::MAX);
                self.jump(target, backward)
            }
            FetchDirection::Absolute(target) => {
                // A negative absolute counts back from the end: `-1` is the last row.
                let resolved = i64::try_from(self.count).unwrap_or(i64::MAX) + 1 + target;
                self.jump(resolved, true)
            }
        }
    }

    /// Move forward over at most `rows` rows, and stop one past the last.
    fn forward(&mut self, rows: i64) -> FetchPlan {
        let mut crossed = Vec::new();
        for _ in 0..rows.max(0) {
            if self.position > self.count {
                break;
            }
            self.position += 1;
            if self.position <= self.count {
                crossed.push(self.position);
            } else {
                break;
            }
        }
        FetchPlan {
            rows: crossed,
            backward: false,
        }
    }

    /// Move backward over at most `rows` rows, and stop before the first.
    fn backward(&mut self, rows: i64) -> FetchPlan {
        let mut crossed = Vec::new();
        let moved = rows.max(0) > 0 && self.position > 0;
        for _ in 0..rows.max(0) {
            if self.position == 0 {
                break;
            }
            self.position -= 1;
            if self.position >= 1 {
                crossed.push(self.position);
            } else {
                break;
            }
        }
        FetchPlan {
            rows: crossed,
            backward: moved,
        }
    }

    /// Land exactly on `target`, and return that one row when it exists.
    fn jump(&mut self, target: i64, backward: bool) -> FetchPlan {
        let count = i64::try_from(self.count).unwrap_or(i64::MAX);
        if target < 1 {
            self.position = 0;
            return FetchPlan {
                rows: Vec::new(),
                backward,
            };
        }
        if target > count {
            self.position = self.count + 1;
            return FetchPlan {
                rows: Vec::new(),
                backward,
            };
        }
        self.position = usize::try_from(target).unwrap_or(self.count);
        FetchPlan {
            rows: vec![self.position],
            backward,
        }
    }
}

#[cfg(test)]
mod tests {
    use assert2::assert;

    use super::*;

    fn direction(spelling: &str) -> FetchDirection {
        let statement = format!("FETCH {spelling} FROM c");
        let parsed = crabka_pgparser::parse(&statement).expect("fetch direction parses");
        match parsed.as_slice() {
            [crabka_pgparser::ast::Statement::FetchCursor { direction, .. }] => *direction,
            other => panic!("expected one FETCH statement, got {other:?}"),
        }
    }

    /// The exact walk PostgreSQL 18.4 performs over a five-row cursor, captured
    /// from the oracle. Each step is `(spelling, rows returned, position after)`.
    #[test]
    fn the_five_row_walk_matches_the_postgres_oracle() {
        let mut position = CursorPosition::new(5);
        let script: &[(&str, &[usize], usize)] = &[
            ("NEXT", &[1], 1),
            ("NEXT", &[2], 2),
            ("2", &[3, 4], 4),
            ("ALL", &[5], 6),
            ("ALL", &[], 6),
            ("NEXT", &[], 6),
            ("PRIOR", &[5], 5),
            ("FIRST", &[1], 1),
            ("LAST", &[5], 5),
            ("ABSOLUTE 2", &[2], 2),
            ("RELATIVE 2", &[4], 4),
            ("BACKWARD 2", &[3, 2], 2),
            ("BACKWARD ALL", &[1], 0),
            ("FORWARD 3", &[1, 2, 3], 3),
            ("-1", &[2], 2),
            ("ABSOLUTE 0", &[], 0),
            ("ABSOLUTE -1", &[5], 5),
            ("RELATIVE 0", &[5], 5),
            ("ABSOLUTE 99", &[], 6),
        ];
        for (spelling, expected_rows, expected_position) in script {
            let plan = position.walk(direction(spelling));
            assert!(
                plan.rows == *expected_rows,
                "{spelling}: rows {:?}",
                plan.rows
            );
            assert!(
                position.position == *expected_position,
                "{spelling}: position {}",
                position.position
            );
        }
    }

    #[test]
    fn no_scroll_restrictions_follow_cursor_direction() {
        let cases: &[(&str, usize, bool)] = &[
            ("NEXT", 2, false),
            ("FORWARD ALL", 2, false),
            ("PRIOR", 2, true),
            ("BACKWARD 1", 2, true),
            ("BACKWARD ALL", 2, true),
            ("BACKWARD ALL", 0, false),
            ("FIRST", 3, true),
            ("FIRST", 0, false),
            ("LAST", 1, true),
            ("ABSOLUTE 2", 4, true),
            ("ABSOLUTE 4", 2, false),
            ("RELATIVE -1", 3, true),
            ("RELATIVE 0", 3, false),
        ];
        for (spelling, start, expected) in cases {
            let mut position = CursorPosition::new(5);
            position.walk(FetchDirection::Absolute(
                i64::try_from(*start).expect("small start"),
            ));
            let plan = position.walk(direction(spelling));
            assert!(plan.backward == *expected, "{spelling} from {start}");
        }
    }

    #[test]
    fn an_empty_cursor_never_leaves_the_two_boundary_positions() {
        let mut position = CursorPosition::new(0);
        for spelling in ["NEXT", "ALL", "LAST", "ABSOLUTE 1", "FORWARD 10"] {
            let plan = position.walk(direction(spelling));
            assert!(plan.rows.is_empty(), "{spelling}");
        }
        assert!(position.position == 1);
        let plan = position.walk(direction("BACKWARD ALL"));
        assert!(plan.rows.is_empty());
        assert!(position.position == 0);
    }

    #[test]
    fn where_current_of_current_row_is_present_only_while_on_a_row() {
        let mut position = CursorPosition::new(2);
        assert!(position.current_row().is_none());

        position.walk(direction("NEXT"));
        assert!(position.current_row() == Some(1));

        position.walk(direction("NEXT"));
        assert!(position.current_row() == Some(2));

        position.walk(direction("NEXT"));
        assert!(position.current_row().is_none());
    }
}
