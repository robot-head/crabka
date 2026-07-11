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
        for (name, code, detail, expected) in [
            (
                "stale topology",
                0,
                "t",
                StreamsStatus::StaleTopology("t".into()),
            ),
            (
                "missing source",
                1,
                "in",
                StreamsStatus::MissingSourceTopics("in".into()),
            ),
            (
                "incorrect partitioning",
                2,
                "p",
                StreamsStatus::IncorrectlyPartitionedTopics("p".into()),
            ),
            (
                "missing internal",
                3,
                "m",
                StreamsStatus::MissingInternalTopics("m".into()),
            ),
            ("shutdown", 4, "", StreamsStatus::ShutdownApplication),
            (
                "assignment delayed",
                5,
                "a",
                StreamsStatus::AssignmentDelayed("a".into()),
            ),
        ] {
            check!(map_status(&s(code, detail)) == expected, "case {name}");
        }
    }

    #[test]
    fn maps_unknown_code_to_unknown() {
        check!(matches!(
            map_status(&s(99, "x")),
            StreamsStatus::Unknown(99, _)
        ));
    }
}
