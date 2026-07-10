use serde::{Deserialize, Serialize};

use crate::{MapEpoch, RangeId};

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub struct BarrierOffset(i64);

impl BarrierOffset {
    pub const ZERO: Self = Self(0);

    #[must_use]
    pub const fn new(value: i64) -> Self {
        Self(value)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RangeLifecyclePhase {
    Fencing,
    BarrierProduced,
    Replaying,
    Reseeding,
    Settling,
    Serving,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RangeTransition {
    pub range_id: RangeId,
    pub map_epoch: MapEpoch,
    pub phase: RangeLifecyclePhase,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RangePrologue {
    pub range_id: RangeId,
    pub map_epoch: MapEpoch,
    pub fenced_epoch: i16,
    pub barrier_offset: BarrierOffset,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct FenceFirstRecovery {
    pub prologue: RangePrologue,
    pub transitions: Vec<RangeTransition>,
}

impl FenceFirstRecovery {
    #[must_use]
    pub fn is_ready_to_serve(&self) -> bool {
        self.transitions
            .last()
            .is_some_and(|transition| transition.phase == RangeLifecyclePhase::Serving)
    }
}
