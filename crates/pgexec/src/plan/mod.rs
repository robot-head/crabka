//! The planner/executor boundary.
//!
//! P0a will move the read path behind these types.  Keep this module small:
//! the existing executor remains the implementation until that move lands.

pub(crate) mod exec;
pub(crate) mod query;
