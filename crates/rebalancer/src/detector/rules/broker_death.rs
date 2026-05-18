//! Stub — replaced by T4.

use crate::detector::AnomalyKind;
use super::{Rule, RuleCtx, RuleHit};

pub struct BrokerDeath;

impl Rule for BrokerDeath {
    fn kind(&self) -> AnomalyKind { AnomalyKind::BrokerDeath }
    fn evaluate(&self, _ctx: &RuleCtx<'_>) -> Vec<RuleHit> { Vec::new() }
}
