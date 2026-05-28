//! `RangeAssignor` — stub, real implementation in Task 4.

use super::{Assignment, Assignor, MemberSubscription, TopicMetadata};

pub struct RangeAssignor;

impl Assignor for RangeAssignor {
    fn name(&self) -> &'static str {
        "range"
    }
    fn assign(
        &self,
        _members: &[MemberSubscription],
        _topics: &TopicMetadata,
    ) -> Assignment {
        Default::default()
    }
}
