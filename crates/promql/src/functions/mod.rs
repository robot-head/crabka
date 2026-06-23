//! `PromQL` functions implemented as `DataFusion` `ScalarUDF`s over the
//! windowed columns that [`crate::extension::range_manipulate`] emits.
//!
//! The rate family (`rate`, `increase`, `delta`, `irate`, `idelta`) lives in
//! [`rate`]; the shared, interpreter-faithful extrapolation math lives in
//! [`extrapolate`]. The `*_over_time` family (`sum`, `avg`, `count`, `min`,
//! `max`, `stddev`, `stdvar`, `last`, `present`, plus `quantile_over_time`)
//! lives in [`over_time`], porting the engine's per-window reductions. A
//! top-level `f(selector[range])` instant query over float-only series is
//! lowered onto these UDFs by [`crate::planner::rate_range`] (registered on the
//! planner's [`crate::extension::planner::prom_session_context`]); every nested
//! or histogram-bearing form stays on the tree-walking interpreter.

pub mod aggregate_udaf;
pub mod extrapolate;
pub mod over_time;
pub mod rate;
pub mod scalar_math;

pub use aggregate_udaf::{prom_max_udaf, prom_min_udaf, register_aggregate_udafs};
pub use over_time::{OverTimeFamily, register_over_time_udfs};
pub use rate::{
    delta_udf, idelta_udf, increase_udf, irate_udf, rate_family_udfs, rate_udf, register_rate_udfs,
};
pub use scalar_math::{ScalarMathOp, register_scalar_math_udfs};
