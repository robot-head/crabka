//! Newtypes for the token-bucket domain values.
//!
//! The consume arithmetic threads four `u64` token counts of the same type:
//! `available`, `refill`, `burst`, and `requested`. It also returns a
//! `(grant, new_available)` pair of the same type. With bare `u64`s, a caller
//! can transpose them and the code still compiles. This is the textbook swap
//! bug. These wrappers make the compiler reject a mixed-up call site.
//!
//! The values are pure in-memory counts that are never serialised, so they do
//! not need `#[serde(transparent)]`. [`crate::plan_consume`] does the
//! arithmetic on the inner `u64` (`.0`), because the operations cross newtype
//! boundaries: `available + refill` and `capped - grant`. A derived `Add` or
//! `Sub` would not type-check across distinct wrappers, so this module omits
//! them.

#[cfg(creusot)]
use creusot_std::prelude::DeepModel;
#[cfg(not(creusot))]
use derive_more::{Display, From, Into};

/// Tokens currently sitting in the bucket, available to grant.
#[cfg_attr(creusot, derive(DeepModel))]
#[cfg_attr(
    not(creusot),
    derive(
        Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Display, From, Into
    )
)]
pub struct AvailableTokens(pub u64);

/// Tokens accrued since the last refill, to be added to `available` this call.
#[cfg_attr(creusot, derive(DeepModel))]
#[cfg_attr(
    not(creusot),
    derive(
        Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Display, From, Into
    )
)]
pub struct RefillTokens(pub u64);

/// The burst cap: the maximum the bucket may hold after a refill.
#[cfg_attr(creusot, derive(DeepModel))]
#[cfg_attr(
    not(creusot),
    derive(
        Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Display, From, Into
    )
)]
pub struct BurstCapacity(pub u64);

/// Tokens the caller is asking to consume.
#[cfg_attr(creusot, derive(DeepModel))]
#[cfg_attr(
    not(creusot),
    derive(
        Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Display, From, Into
    )
)]
pub struct RequestedTokens(pub u64);

/// Tokens actually granted by a consume call (`<= requested`).
#[cfg_attr(creusot, derive(DeepModel))]
#[cfg_attr(
    not(creusot),
    derive(
        Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Display, From, Into
    )
)]
pub struct GrantedTokens(pub u64);

/// The bucket's new `available` count after a consume call commits.
#[cfg_attr(creusot, derive(DeepModel))]
#[cfg_attr(
    not(creusot),
    derive(
        Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Display, From, Into
    )
)]
pub struct NewAvailable(pub u64);
