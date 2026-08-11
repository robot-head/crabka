//! Voter placement helpers for diskless WAL quorums.

use crabka_metadata::{BrokerRegistrationRecord, NodeId};

/// Selects the WAL voters from distinct configured racks. An incomplete
/// result makes the caller fail closed instead of weakening the AZ-loss
/// durability guarantee.
pub(crate) fn select_voters(
    brokers: impl IntoIterator<Item = BrokerRegistrationRecord>,
    local_node: NodeId,
    voters: usize,
) -> Vec<NodeId> {
    if voters == 0 {
        return Vec::new();
    }
    let mut brokers = brokers.into_iter().collect::<Vec<_>>();
    brokers.sort_by_key(|broker| broker.node_id.0);

    let mut selected = Vec::with_capacity(voters);
    let Some(local) = brokers
        .iter()
        .find(|broker| broker.node_id == local_node && broker.rack.is_some())
    else {
        return selected;
    };
    selected.push(local.node_id);

    let rack_distinct = rack_distinct_candidates(&brokers, &selected)
        .map(|broker| broker.node_id)
        .collect::<Vec<_>>();
    for node_id in rack_distinct {
        if selected.len() == voters {
            return selected;
        }
        selected.push(node_id);
    }

    selected
}

fn rack_distinct_candidates<'a>(
    brokers: &'a [BrokerRegistrationRecord],
    selected: &'a [NodeId],
) -> impl Iterator<Item = &'a BrokerRegistrationRecord> {
    let mut used_racks = selected
        .iter()
        .filter_map(|node_id| {
            brokers
                .iter()
                .find(|broker| broker.node_id == *node_id)
                .and_then(|broker| broker.rack.as_deref())
        })
        .collect::<Vec<_>>();
    brokers.iter().filter(move |broker| {
        if selected.contains(&broker.node_id) {
            return false;
        }
        let Some(rack) = broker.rack.as_deref() else {
            return false;
        };
        if used_racks.contains(&rack) {
            return false;
        }
        used_racks.push(rack);
        true
    })
}

#[cfg(test)]
mod tests {
    use crabka_metadata::BrokerEndpoint;
    use crabka_security::ListenerProtocol;

    use super::*;

    #[test]
    fn placement_prefers_rack_distinct_voters_with_local_first() {
        let selected = select_voters(
            [
                broker(3, Some("c")),
                broker(1, Some("a")),
                broker(2, Some("b")),
                broker(4, Some("a")),
            ],
            NodeId(1),
            3,
        );

        assert_eq!(selected, vec![NodeId(1), NodeId(2), NodeId(3)]);
    }

    #[test]
    fn placement_refuses_to_weaken_the_rack_failure_budget() {
        let selected = select_voters(
            [broker(1, Some("a")), broker(2, Some("a")), broker(3, None)],
            NodeId(1),
            3,
        );

        assert_eq!(selected, vec![NodeId(1)]);
    }

    #[test]
    fn placement_does_not_invent_an_unregistered_local_voter() {
        let selected = select_voters([broker(2, Some("a")), broker(3, Some("b"))], NodeId(1), 2);

        assert!(selected.is_empty());
    }

    #[test]
    fn placement_requires_a_rack_for_the_leader() {
        let selected = select_voters(
            [broker(1, None), broker(2, Some("a")), broker(3, Some("b"))],
            NodeId(1),
            3,
        );

        assert!(selected.is_empty());
    }

    fn broker(id: u64, rack: Option<&str>) -> BrokerRegistrationRecord {
        BrokerRegistrationRecord {
            node_id: NodeId(id),
            broker_epoch: 0,
            incarnation_id: uuid::Uuid::nil(),
            host: format!("broker-{id}"),
            port: 9092,
            rack: rack.map(str::to_string),
            endpoints: vec![BrokerEndpoint {
                name: "INTERNAL".into(),
                host: format!("broker-{id}"),
                port: 19092,
                protocol: ListenerProtocol::Plaintext,
            }],
        }
    }
}
