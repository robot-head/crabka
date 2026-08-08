//! Domain newtypes for the Postgres logical-decoding connector.
//!
//! Logical decoding threads several primitives of the same type, and a call
//! site can transpose them easily. The pgoutput `Commit` message carries two
//! adjacent [`PgLsn`] values, `commit_lsn` and `end_lsn`. Transaction ids and
//! commit timestamps are both `i64`, and they sit next to each other on a row.
//! `relation_id` is a bare `u32` that the relation cache and every row event
//! share. A distinct newtype around each one turns a swapped argument into a
//! compile error, instead of silent offset or transaction corruption. See the
//! [newtype guidance] in the style guide.
//!
//! [newtype guidance]: ../../../docs/style_guides/code_style_guide.md

use derive_more::{Display, From, Into};

use crate::PgLsn;

/// LSN at which a transaction was committed. This is the pgoutput
/// `Commit.commit_lsn`.
///
/// It is distinct from [`EndLsn`], so the two adjacent LSNs in a commit message
/// cannot be transposed when the code constructs the message.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord, Display, From, Into)]
pub struct CommitLsn(pub PgLsn);

/// End-of-transaction LSN, the resume point after a commit. This is the
/// pgoutput `Commit.end_lsn`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord, Display, From, Into)]
pub struct EndLsn(pub PgLsn);

/// Postgres transaction id (`xid`) carried by begin/commit and stamped onto
/// each decoded row.
///
/// A bare `i64` sits next to `commit_timestamp_ms` on a row event. The newtype
/// prevents a swap of the two.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord, Display, From, Into)]
pub struct TransactionId(pub i64);

/// pgoutput relation identifier. It keys the relation cache and every row
/// event.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord, Display, From, Into)]
pub struct RelationId(pub u32);
