//! The registry of user-declared casts.
//!
//! [`crate::cast::cast_allowed`] is the plan-time legality oracle, and it is a
//! pure function over a [`ColumnType`] pair — reached from expression type
//! inference, which carries no catalog handle. A cast the user declared with
//! `CREATE CAST` therefore has to be visible the same way a user *type* is: as
//! a process-wide snapshot the executor republishes whenever the durable
//! catalog changes.
//!
//! The registry holds identity only, keyed on the `(castsource, casttarget)`
//! oid pair that is also `pg_cast`'s unique index. What the cast *does* is the
//! executor's business.
//!
//! Like [`crate::usertype`], process-wide is not the same as per-catalog, and
//! two catalogs in one process would alias. That is the same known defect,
//! recorded in the same place.

use std::{
    collections::HashSet,
    sync::{OnceLock, RwLock},
};

/// One declared cast's identity.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct DeclaredCast {
    /// `pg_cast.castsource`.
    pub source: u32,
    /// `pg_cast.casttarget`.
    pub target: u32,
}

fn registry() -> &'static RwLock<HashSet<DeclaredCast>> {
    static REGISTRY: OnceLock<RwLock<HashSet<DeclaredCast>>> = OnceLock::new();
    REGISTRY.get_or_init(|| RwLock::new(HashSet::new()))
}

/// Replace the registry with `casts`, the durable catalog's current contents.
///
/// # Panics
///
/// If the registry lock is poisoned, which can only happen if another thread
/// panicked while holding it.
pub fn publish(casts: impl IntoIterator<Item = DeclaredCast>) {
    let mut guard = registry().write().expect("cast registry is healthy");
    *guard = casts.into_iter().collect();
}

/// Whether the user declared a cast from `source` to `target`.
///
/// # Panics
///
/// If the registry lock is poisoned.
#[must_use]
pub fn is_declared(source: u32, target: u32) -> bool {
    let guard = registry().read().expect("cast registry is healthy");
    guard.contains(&DeclaredCast { source, target })
}

/// Whether any cast is declared at all.
///
/// The cast path checks this first so a server that has never run `CREATE CAST`
/// — which is nearly all of them — pays one atomic read rather than a type
/// inference per cast expression per row.
///
/// # Panics
///
/// If the registry lock is poisoned.
#[must_use]
pub fn any_declared() -> bool {
    !registry()
        .read()
        .expect("cast registry is healthy")
        .is_empty()
}

#[cfg(test)]
mod tests {
    use assert2::assert;

    use super::*;

    #[test]
    fn publish_replaces_the_whole_snapshot() {
        publish([DeclaredCast {
            source: 23,
            target: 700,
        }]);
        assert!(any_declared());
        assert!(is_declared(23, 700));
        assert!(!is_declared(700, 23));
        // A second publish is the new catalog state, not an addition to it.
        publish([DeclaredCast {
            source: 700,
            target: 23,
        }]);
        assert!(!is_declared(23, 700));
        assert!(is_declared(700, 23));
        publish([]);
        assert!(!any_declared());
    }
}
