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
    use assert2::check;
    use crabka_protocol::owned::common::streams_group_heartbeat_response::status::Status;

    use super::*;
    use crate::membership::types::StreamsStatus;

    fn s(code: i8, detail: &str) -> Status {
        Status {
            status_code: code,
            status_detail: detail.into(),
            ..Default::default()
        }
    }

    #[test]
    fn maps_known_codes() {
        check!(
            matches!(map_status(&s(1, "in")), StreamsStatus::MissingSourceTopics(d) if d == "in")
        );
        check!(matches!(
            map_status(&s(4, "")),
            StreamsStatus::ShutdownApplication
        ));
    }

    #[test]
    fn maps_all_remaining_known_codes() {
        check!(matches!(map_status(&s(0, "t")), StreamsStatus::StaleTopology(d) if d == "t"));
        check!(matches!(
            map_status(&s(2, "p")),
            StreamsStatus::IncorrectlyPartitionedTopics(d) if d == "p"
        ));
        check!(matches!(
            map_status(&s(3, "m")),
            StreamsStatus::MissingInternalTopics(d) if d == "m"
        ));
        check!(matches!(
            map_status(&s(5, "a")),
            StreamsStatus::AssignmentDelayed(d) if d == "a"
        ));
    }

    #[test]
    fn maps_unknown_code_to_unknown() {
        check!(matches!(
            map_status(&s(99, "x")),
            StreamsStatus::Unknown(99, _)
        ));
    }
}
