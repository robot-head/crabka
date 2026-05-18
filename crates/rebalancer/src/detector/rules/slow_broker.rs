//! Stub — replaced by T7.

use crate::detector::AnomalyKind;
use super::{Rule, RuleCtx, RuleHit};

pub struct SlowBroker;

impl Rule for SlowBroker {
    fn kind(&self) -> AnomalyKind { AnomalyKind::SlowBroker }
    fn evaluate(&self, _ctx: &RuleCtx<'_>) -> Vec<RuleHit> { Vec::new() }
}
