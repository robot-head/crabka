//! Stub — replaced by T5.

use crate::detector::AnomalyKind;
use super::{Rule, RuleCtx, RuleHit};

pub struct UnderReplicatedPartitions;

impl Rule for UnderReplicatedPartitions {
    fn kind(&self) -> AnomalyKind { AnomalyKind::UnderReplicatedPartitions }
    fn evaluate(&self, _ctx: &RuleCtx<'_>) -> Vec<RuleHit> { Vec::new() }
}
