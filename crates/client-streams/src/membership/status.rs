//! Map response `Status` codes to the typed [`StreamsStatus`].

use crabka_protocol::owned::common::streams_group_heartbeat_response::status::Status;

use super::types::StreamsStatus;

/// KIP-1071 `StreamsGroupHeartbeatResponse.Status` codes.
pub(crate) fn map_status(s: &Status) -> StreamsStatus {
    let detail = s.status_detail.clone();
    match s.status_code {
        0 => StreamsStatus::StaleTopology(detail),
        1 => StreamsStatus::MissingSourceTopics(detail),
        2 => StreamsStatus::IncorrectlyPartitionedTopics(detail),
        3 => StreamsStatus::MissingInternalTopics(detail),
        4 => StreamsStatus::ShutdownApplication,
        5 => StreamsStatus::AssignmentDelayed(detail),
        other => StreamsStatus::Unknown(other, detail),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::membership::types::StreamsStatus;
    use assert2::check;
    use crabka_protocol::owned::common::streams_group_heartbeat_response::status::Status;

    #[test]
    fn maps_known_codes() {
        let s = Status {
            status_code: 1,
            status_detail: "in".into(),
            ..Default::default()
        };
        check!(matches!(map_status(&s), StreamsStatus::MissingSourceTopics(d) if d == "in"));
        let s = Status {
            status_code: 4,
            status_detail: String::new(),
            ..Default::default()
        };
        check!(matches!(map_status(&s), StreamsStatus::ShutdownApplication));
    }

    #[test]
    fn maps_unknown_code_to_unknown() {
        let s = Status {
            status_code: 99,
            status_detail: "x".into(),
            ..Default::default()
        };
        check!(matches!(map_status(&s), StreamsStatus::Unknown(99, _)));
    }
}
