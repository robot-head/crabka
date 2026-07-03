//! Domain newtypes for the Postgres logical-decoding connector.
//!
//! Logical decoding threads several same-typed primitives that are trivially
//! transposable at a call site: the pgoutput `Commit` message carries two
//! adjacent [`PgLsn`] values (`commit_lsn` and `end_lsn`), transaction ids and
//! commit timestamps are both `i64` and sit next to each other on a row, and
//! `relation_id` is a bare `u32` shared by the relation cache and every row
//! event. Wrapping each in a distinct newtype turns a swapped argument into a
//! compile error instead of silent offset/transaction corruption. See the
//! [newtype guidance] in the style guide.
//!
//! [newtype guidance]: ../../../docs/style_guides/code_style_guide.md

use derive_more::{Display, From, Into};

use crate::PgLsn;

/// LSN at which a transaction was committed (pgoutput `Commit.commit_lsn`).
///
/// Distinct from [`EndLsn`] so the two adjacent LSNs in a commit message cannot
/// be transposed when the message is constructed.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord, Display, From, Into)]
pub struct CommitLsn(pub PgLsn);

/// End-of-transaction LSN, the resume point after a commit (pgoutput
/// `Commit.end_lsn`).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord, Display, From, Into)]
pub struct EndLsn(pub PgLsn);

/// Postgres transaction id (`xid`) carried by begin/commit and stamped onto
/// each decoded row.
///
/// A bare `i64` sits next to `commit_timestamp_ms` on a row event; the newtype
/// keeps the two from being swapped.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord, Display, From, Into)]
pub struct TransactionId(pub i64);

/// pgoutput relation identifier, keying the relation cache and every row event.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord, Display, From, Into)]
pub struct RelationId(pub u32);
