//! Range assignor (Kafka's classic default).
//!
//! Given a set of members and a per-topic partition count, hands each
//! member a contiguous range of partitions per topic. The trailing
//! members get one less partition when the partition count doesn't
//! divide evenly.

use std::collections::HashMap;

/// Returns `member_id → Vec<(topic, partition)>` assignments.
#[must_use]
#[tracing::instrument(
    name = "consumer.assignor.range",
    level = "info",
    skip_all,
    fields(members = members.len(), topics = topic_partitions.len())
)]
pub fn assign(
    mut members: Vec<(String, Vec<String>)>,
    topic_partitions: &HashMap<String, i32>,
) -> HashMap<String, Vec<(String, i32)>> {
    members.sort_by(|(a, _), (b, _)| a.cmp(b));
    let mut out: HashMap<String, Vec<(String, i32)>> = members
        .iter()
        .map(|(m, _)| (m.clone(), Vec::new()))
        .collect();

    // For determinism, iterate topics in sorted order.
    let mut topics: Vec<(&String, &i32)> = topic_partitions.iter().collect();
    topics.sort_by_key(|(a, _)| *a);

    for (topic, &partition_count) in topics {
        let subscribed: Vec<&String> = members
            .iter()
            .filter(|(_, subs)| subs.iter().any(|t| t == topic))
            .map(|(m, _)| m)
            .collect();
        if subscribed.is_empty() || partition_count <= 0 {
            continue;
        }
        let n = i32::try_from(subscribed.len()).unwrap_or(i32::MAX);
        let per = partition_count / n;
        let extras = partition_count % n;
        let mut next: i32 = 0;
        for (i, m) in subscribed.iter().enumerate() {
            let extra = i32::from(i32::try_from(i).unwrap_or(i32::MAX) < extras);
            let take = per + extra;
            for p in next..(next + take) {
                if let Some(slot) = out.get_mut(*m) {
                    slot.push((topic.clone(), p));
                }
            }
            next += take;
        }
    }
    out
}

#[cfg(test)]
mod tests {

    use super::*;

    #[test]
    fn one_member_takes_everything() {
        let mut tp = HashMap::new();
        tp.insert("t".into(), 4);
        let a = assign(vec![("m1".into(), vec!["t".into()])], &tp);
        assert2::assert!(
            a["m1"]
                == vec![
                    ("t".into(), 0),
                    ("t".into(), 1),
                    ("t".into(), 2),
                    ("t".into(), 3),
                ]
        );
    }

    #[test]
    fn two_members_split_evenly() {
        let mut tp = HashMap::new();
        tp.insert("t".into(), 4);
        let a = assign(
            vec![
                ("m1".into(), vec!["t".into()]),
                ("m2".into(), vec!["t".into()]),
            ],
            &tp,
        );
        assert2::assert!(
            a == HashMap::from([
                ("m1".into(), vec![("t".into(), 0), ("t".into(), 1)]),
                ("m2".into(), vec![("t".into(), 2), ("t".into(), 3)]),
            ])
        );
    }

    #[test]
    fn extras_go_to_lower_member_ids() {
        let mut tp = HashMap::new();
        tp.insert("t".into(), 5);
        let a = assign(
            vec![
                ("m1".into(), vec!["t".into()]),
                ("m2".into(), vec!["t".into()]),
            ],
            &tp,
        );
        assert2::assert!((a["m1"].len(), a["m2"].len()) == (3, 2));
    }

    #[test]
    fn member_with_no_subscriptions_gets_empty() {
        let mut tp = HashMap::new();
        tp.insert("t".into(), 2);
        let a = assign(
            vec![("m1".into(), vec!["t".into()]), ("m2".into(), vec![])],
            &tp,
        );
        assert2::assert!((a["m1"].len(), a["m2"].len()) == (2, 0));
    }

    #[test]
    fn non_positive_partition_count_assigns_nothing() {
        let mut tp = HashMap::new();
        tp.insert("t".into(), 0);
        let a = assign(vec![("m1".into(), vec!["t".into()])], &tp);
        assert2::assert!(a["m1"].is_empty());
    }

    #[test]
    fn topic_with_no_subscribers_assigns_nothing() {
        let mut tp = HashMap::new();
        tp.insert("t".into(), 2);
        let a = assign(vec![("m1".into(), vec!["other".into()])], &tp);
        assert2::assert!(a["m1"].is_empty());
    }
}
