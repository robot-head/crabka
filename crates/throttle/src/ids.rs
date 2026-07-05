//! Newtypes for the token-bucket domain values.
//!
//! The consume arithmetic threads four same-typed `u64` token counts —
//! `available`, `refill`, `burst`, and `requested` — plus returns a
//! `(grant, new_available)` pair of the same type. Bare `u64`s let a caller
//! transpose them and still compile (the textbook swap bug). These wrappers
//! make the compiler reject a mixed-up call site.
//!
//! The values are pure in-memory counts (never serialised), so no
//! `#[serde(transparent)]` is needed. Arithmetic is done on the inner `u64`
//! (`.0`) inside [`crate::plan_consume`], since the operations cross newtype
//! boundaries (`available + refill`, `capped - grant`) — deriving `Add`/`Sub`
//! would not type-check across distinct wrappers, so they are omitted.

#[cfg(creusot)]
use creusot_std::prelude::*;
use derive_more::{Display, From, Into};

/// Tokens currently sitting in the bucket, available to grant.
#[cfg_attr(creusot, derive(DeepModel))]
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Display, From, Into)]
pub struct AvailableTokens(pub u64);

/// Tokens accrued since the last refill, to be added to `available` this call.
#[cfg_attr(creusot, derive(DeepModel))]
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Display, From, Into)]
pub struct RefillTokens(pub u64);

/// The burst cap: the maximum the bucket may hold after a refill.
#[cfg_attr(creusot, derive(DeepModel))]
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Display, From, Into)]
pub struct BurstCapacity(pub u64);

/// Tokens the caller is asking to consume.
#[cfg_attr(creusot, derive(DeepModel))]
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Display, From, Into)]
pub struct RequestedTokens(pub u64);

/// Tokens actually granted by a consume call (`<= requested`).
#[cfg_attr(creusot, derive(DeepModel))]
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Display, From, Into)]
pub struct GrantedTokens(pub u64);

/// The bucket's new `available` count after a consume call commits.
#[cfg_attr(creusot, derive(DeepModel))]
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Display, From, Into)]
pub struct NewAvailable(pub u64);
