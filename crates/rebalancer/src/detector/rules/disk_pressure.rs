//! Stub — replaced by T6.

use crate::detector::AnomalyKind;
use super::{Rule, RuleCtx, RuleHit};

pub struct DiskPressure;

impl Rule for DiskPressure {
    fn kind(&self) -> AnomalyKind { AnomalyKind::DiskPressure }
    fn evaluate(&self, _ctx: &RuleCtx<'_>) -> Vec<RuleHit> { Vec::new() }
}
