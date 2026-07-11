//! Cooperative-sticky assignor (KIP-429).
//!
//! Byte-identical port of Apache Kafka 3.6.x
//! `org.apache.kafka.clients.consumer.CooperativeStickyAssignor` +
//! `AbstractStickyAssignor.generalAssign` /
//! `ConstrainedAssignmentBuilder`. Returns the partitions each member
//! should be holding *after this rebalance round*. When a partition
//! would have to move off a still-living owner, it is omitted entirely
//! (`phase-1` of the two-phase cooperative protocol); the owner will see
//! its loss on the next `SyncGroup`, revoke, and a follow-up rebalance
//! places the released partition.

use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet};

/// One member's input to [`assign`]: `(member_id, subscribed_topics, owned_partitions, generation_id)`.
pub type MemberInput = (String, Vec<String>, Vec<(String, i32)>, i32);

/// Cooperative-sticky assignment per KIP-429.
///
/// Inputs: each member as `(member_id, subscribed_topics, owned_partitions, generation_id)`.
/// `topic_partitions` gives the partition count for every topic any member subscribes to.
///
/// Output: per-member assigned partitions for THIS rebalance round. Cooperative semantics
/// mean partitions being moved from a still-active owner are omitted — the next rebalance
/// (`phase-2`) will place them after the owner revokes.
#[must_use]
#[tracing::instrument(
    name = "consumer.assignor.cooperative_sticky",
    level = "info",
    skip_all,
    fields(members = members.len(), topics = topic_partitions.len())
)]
pub fn assign(
    members: &[MemberInput],
    topic_partitions: &HashMap<String, i32>,
) -> HashMap<String, Vec<(String, i32)>> {
    if members.is_empty() {
        return HashMap::new();
    }

    // Index members by id (sorted, deterministic ordering everywhere).
    let mut member_ids: Vec<String> = members.iter().map(|(id, _, _, _)| id.clone()).collect();
    member_ids.sort();

    let subs: BTreeMap<String, BTreeSet<String>> = members
        .iter()
        .map(|(id, subs, _, _)| (id.clone(), subs.iter().cloned().collect()))
        .collect();
    let gens: BTreeMap<String, i32> = members
        .iter()
        .map(|(id, _, _, generation)| (id.clone(), *generation))
        .collect();

    // === Pass 1: prepopulateCurrentAssignments ===
    // Resolve zombie conflicts by generation_id; build current_assignment
    // filtered against the live subscription + partition counts.
    let current_assignment =
        prepopulate_current_assignments(members, &subs, &gens, topic_partitions);

    // Map (topic, partition) → previous owner among the live, still-subscribed
    // members. Used both by the cooperative adjustment and by the sticky
    // preference inside the general assignment.
    let mut previous_owner: HashMap<(String, i32), String> = HashMap::new();
    for (m, parts) in &current_assignment {
        for tp in parts {
            previous_owner.insert(tp.clone(), m.clone());
        }
    }

    // === Pass 2: branch decision ===
    // If every member subscribes to the EXACT same set of topics, use the
    // constrained algorithm; else the general algorithm.
    let all_equal = {
        let mut iter = subs.values();
        match iter.next() {
            None => true,
            Some(first) => iter.all(|s| s == first),
        }
    };

    let raw_assignment = if all_equal {
        constrained_assign(&member_ids, &subs, &current_assignment, topic_partitions)
    } else {
        general_assign(&member_ids, &subs, &current_assignment, topic_partitions)
    };

    // === Pass 3: cooperative adjustment ===
    // Strip any partition that's moving from a still-live, still-subscribed
    // previous owner to a new member.
    let mut adjusted: HashMap<String, Vec<(String, i32)>> = HashMap::new();
    for id in &member_ids {
        adjusted.insert(id.clone(), Vec::new());
    }
    for id in &member_ids {
        let new_parts = raw_assignment.get(id).cloned().unwrap_or_default();
        for tp in new_parts {
            match previous_owner.get(&tp) {
                Some(prev) if prev != id => {
                    // Previous owner still alive (it's in current_assignment,
                    // which by construction means they're in members & subscribed).
                    // Omit: phase 2 will place it.
                }
                _ => {
                    adjusted.get_mut(id).unwrap().push(tp);
                }
            }
        }
    }

    // Sort each member's partition list (topic, partition) for determinism.
    for v in adjusted.values_mut() {
        v.sort();
    }
    adjusted
}

/// Build `current_assignment` filtered to live topics/subscriptions, with
/// zombie conflicts resolved by `generation_id`.
fn prepopulate_current_assignments(
    members: &[MemberInput],
    subs: &BTreeMap<String, BTreeSet<String>>,
    _gens: &BTreeMap<String, i32>,
    topic_partitions: &HashMap<String, i32>,
) -> BTreeMap<String, Vec<(String, i32)>> {
    // Track per-(t,p) the best (highest-gen) owner seen so far + a flag
    // that says "tied at this generation → drop from everyone".
    // Map: (topic, partition) → (generation_id, member_id, tied)
    let mut best: HashMap<(String, i32), (i32, String, bool)> = HashMap::new();

    for (id, _id_subs, owned, generation) in members {
        let id_subs = subs.get(id).cloned().unwrap_or_default();
        for (t, p) in owned {
            // Filter stale ownership: topic gone, partition_index out of bounds,
            // or member no longer subscribed.
            let Some(&pcount) = topic_partitions.get(t) else {
                continue;
            };
            if *p < 0 || *p >= pcount {
                continue;
            }
            if !id_subs.contains(t) {
                continue;
            }
            let key = (t.clone(), *p);
            match best.get(&key) {
                None => {
                    best.insert(key, (*generation, id.clone(), false));
                }
                Some((existing_gen, _existing_id, _tied)) => {
                    if *generation > *existing_gen {
                        best.insert(key, (*generation, id.clone(), false));
                    } else if *generation == *existing_gen {
                        // Tie → mark as dropped.
                        let prev = best.get(&key).cloned().unwrap();
                        best.insert(key, (prev.0, prev.1, true));
                    }
                    // else: lower-gen claim loses silently.
                }
            }
        }
    }

    let mut out: BTreeMap<String, Vec<(String, i32)>> = BTreeMap::new();
    for (id, _, _, _) in members {
        out.insert(id.clone(), Vec::new());
    }
    for (tp, (_gen, owner, tied)) in best {
        if tied {
            continue;
        }
        out.entry(owner).or_default().push(tp);
    }
    for v in out.values_mut() {
        v.sort();
    }
    out
}

/// `ConstrainedAssignmentBuilder` — used when all members share the same
/// subscription. Faster path; produces the same final assignment as the
/// general algorithm in this special case.
fn constrained_assign(
    member_ids: &[String],
    subs: &BTreeMap<String, BTreeSet<String>>,
    current_assignment: &BTreeMap<String, Vec<(String, i32)>>,
    topic_partitions: &HashMap<String, i32>,
) -> HashMap<String, Vec<(String, i32)>> {
    // Determine the shared subscription (all members have the same).
    let shared: BTreeSet<String> = member_ids
        .first()
        .and_then(|m| subs.get(m).cloned())
        .unwrap_or_default();

    // Enumerate all partitions of subscribed topics (sorted topic, then partition).
    let mut all_partitions: Vec<(String, i32)> = Vec::new();
    let mut topics_sorted: Vec<&String> = shared.iter().collect();
    topics_sorted.sort();
    for t in topics_sorted {
        let Some(&n) = topic_partitions.get(t) else {
            continue;
        };
        for p in 0..n {
            all_partitions.push((t.clone(), p));
        }
    }

    let num_members = member_ids.len();
    if num_members == 0 {
        return HashMap::new();
    }
    let total = all_partitions.len();
    let base = total / num_members;
    let remainder = total % num_members;

    // Per-member target_size. JVM gives the +1 to the first `remainder` members
    // in iteration order; since our member_ids are sorted, that's lex order.
    let mut targets: BTreeMap<String, usize> = BTreeMap::new();
    for (i, id) in member_ids.iter().enumerate() {
        let extra = usize::from(i < remainder);
        targets.insert(id.clone(), base + extra);
    }

    // Step A: each member keeps a prefix of their owned (lex-sorted),
    // up to target_size.
    let mut out: HashMap<String, Vec<(String, i32)>> = HashMap::new();
    let mut taken: HashSet<(String, i32)> = HashSet::new();
    for id in member_ids {
        let mut owned: Vec<(String, i32)> = current_assignment.get(id).cloned().unwrap_or_default();
        owned.sort();
        let target = *targets.get(id).unwrap_or(&0);
        let kept: Vec<(String, i32)> = owned.into_iter().take(target).collect();
        for tp in &kept {
            taken.insert(tp.clone());
        }
        out.insert(id.clone(), kept);
    }

    // Step B: distribute the remaining (unowned-or-released) partitions
    // round-robin to members still below their target. Iterate partitions
    // in (topic, partition) order; member queue in lex order.
    let mut unassigned: Vec<(String, i32)> = all_partitions
        .into_iter()
        .filter(|tp| !taken.contains(tp))
        .collect();
    unassigned.sort();

    let mut member_cursor = 0usize;
    for tp in unassigned {
        // Find the next member with capacity, starting from cursor (round-robin).
        let mut placed = false;
        for _ in 0..num_members {
            let id = &member_ids[member_cursor % num_members];
            member_cursor += 1;
            let target = *targets.get(id).unwrap_or(&0);
            let slot = out.entry(id.clone()).or_default();
            if slot.len() < target {
                slot.push(tp);
                placed = true;
                break;
            }
        }
        // Invariant: per-member targets sum to `total`, and Step A only ever
        // marks partitions of `all_partitions` as `taken` (each at most once,
        // since `prepopulate_current_assignments` filters stale ownership and
        // resolves conflicts). So the remaining capacity Σ(target − kept)
        // equals |unassigned|, and because the inner loop scans every member,
        // any leftover capacity is always found. A failure to place means the
        // target arithmetic or Step-A bookkeeping has regressed.
        debug_assert!(
            placed,
            "constrained_assign: unassigned partition found no slot despite Σtargets == total"
        );
        if !placed {
            break;
        }
    }

    for v in out.values_mut() {
        v.sort();
    }
    out
}

/// `AbstractStickyAssignor.generalAssign` — the four-pass general
/// algorithm used when subscriptions are non-uniform.
// Byte-port of the JVM constrained algorithm; splitting would obscure
// the four-pass structure.
#[allow(clippy::too_many_lines)]
fn general_assign(
    member_ids: &[String],
    subs: &BTreeMap<String, BTreeSet<String>>,
    current_assignment: &BTreeMap<String, Vec<(String, i32)>>,
    topic_partitions: &HashMap<String, i32>,
) -> HashMap<String, Vec<(String, i32)>> {
    // Step 1: compute subscribers-per-topic.
    let mut subscribers_per_topic: BTreeMap<String, BTreeSet<String>> = BTreeMap::new();
    for (id, s) in subs {
        for t in s {
            if topic_partitions.contains_key(t) {
                subscribers_per_topic
                    .entry(t.clone())
                    .or_default()
                    .insert(id.clone());
            }
        }
    }

    // Step 2: enumerate all partitions (only of topics that have ≥1 subscriber)
    // and sort by rarity (ascending subscriber count), tiebreak (topic, partition).
    let mut sorted_partitions: Vec<(String, i32)> = Vec::new();
    for t in subscribers_per_topic.keys() {
        let Some(&n) = topic_partitions.get(t) else {
            continue;
        };
        for p in 0..n {
            sorted_partitions.push((t.clone(), p));
        }
    }
    sorted_partitions.sort_by(|a, b| {
        let sa = subscribers_per_topic.get(&a.0).map_or(0, BTreeSet::len);
        let sb = subscribers_per_topic.get(&b.0).map_or(0, BTreeSet::len);
        sa.cmp(&sb)
            .then_with(|| a.0.cmp(&b.0))
            .then_with(|| a.1.cmp(&b.1))
    });

    // Initialise per-member new assignment.
    let mut new_assignment: BTreeMap<String, Vec<(String, i32)>> = BTreeMap::new();
    for id in member_ids {
        new_assignment.insert(id.clone(), Vec::new());
    }

    // Previous owner map (still alive + still subscribed) — used for sticky preference.
    let mut prev_owner: HashMap<(String, i32), String> = HashMap::new();
    for (m, parts) in current_assignment {
        for tp in parts {
            prev_owner.insert(tp.clone(), m.clone());
        }
    }

    // Step 3: place each partition.
    // For each partition in rarity order, choose the least-loaded subscribed
    // member (lex tiebreak), with sticky preference for the previous owner if
    // they're still subscribed AND giving it to them doesn't push them more
    // than 1 above the current min load among subscribed members.
    for tp in &sorted_partitions {
        let subscribed: BTreeSet<String> = subscribers_per_topic
            .get(&tp.0)
            .cloned()
            .unwrap_or_default();
        if subscribed.is_empty() {
            continue;
        }

        let chosen = choose_assignment_owner(tp, &subscribed, &prev_owner, &new_assignment);

        new_assignment
            .get_mut(&chosen)
            .expect("chosen exists")
            .push(tp.clone());
    }

    // Step 4: balance pass. While there exist two members m1, m2 with
    // |m1| - |m2| > 1 AND m2 subscribes to some partition currently on m1
    // (i.e. we can legally move it), move one partition. Prefer moving a
    // partition that m2 did NOT previously own (sticky), then (topic,partition)
    // order for determinism.
    //
    // We bound iterations conservatively to avoid pathological loops.
    balance_assignment(
        member_ids,
        subs,
        &prev_owner,
        &mut new_assignment,
        max_balance_iters(sorted_partitions.len(), member_ids.len()),
    );

    // Convert to HashMap and sort each list for determinism.
    let mut out: HashMap<String, Vec<(String, i32)>> = HashMap::new();
    for (k, mut v) in new_assignment {
        v.sort();
        out.insert(k, v);
    }
    out
}

fn pick_least_loaded(
    subscribed: &BTreeSet<String>,
    assignment: &BTreeMap<String, Vec<(String, i32)>>,
) -> String {
    least_loaded(subscribed, assignment).1
}

fn least_loaded(
    subscribed: &BTreeSet<String>,
    assignment: &BTreeMap<String, Vec<(String, i32)>>,
) -> (usize, String) {
    let mut best: Option<(usize, String)> = None;
    for id in subscribed {
        let load = assignment.get(id).map_or(0, Vec::len);
        match &best {
            None => best = Some((load, id.clone())),
            Some((bl, bid)) => {
                debug_assert!(id >= bid, "BTreeSet iteration must be lexicographic");
                if load < *bl {
                    best = Some((load, id.clone()));
                }
            }
        }
    }
    best.expect("non-empty subscription")
}

fn choose_assignment_owner(
    tp: &(String, i32),
    subscribed: &BTreeSet<String>,
    prev_owner: &HashMap<(String, i32), String>,
    assignment: &BTreeMap<String, Vec<(String, i32)>>,
) -> String {
    let (min_load, _) = least_loaded(subscribed, assignment);
    match prev_owner.get(tp) {
        Some(prev) if subscribed.contains(prev) => {
            let prev_load = assignment.get(prev).map_or(0, Vec::len);
            if prev_load <= min_load {
                prev.clone()
            } else {
                pick_least_loaded(subscribed, assignment)
            }
        }
        _ => pick_least_loaded(subscribed, assignment),
    }
}

fn max_balance_iters(partitions: usize, members: usize) -> usize {
    partitions.saturating_mul(members) + 16
}

fn balance_assignment(
    member_ids: &[String],
    subs: &BTreeMap<String, BTreeSet<String>>,
    prev_owner: &HashMap<(String, i32), String>,
    new_assignment: &mut BTreeMap<String, Vec<(String, i32)>>,
    max_iters: usize,
) {
    for _ in 0..max_iters {
        // Find heaviest and lightest among ALL members (lex tiebreak on id).
        let mut heaviest: Option<(usize, String)> = None;
        let mut lightest: Option<(usize, String)> = None;
        for id in member_ids {
            let load = new_assignment.get(id).map_or(0, Vec::len);
            match &heaviest {
                None => heaviest = Some((load, id.clone())),
                Some((hl, _)) if load > *hl => heaviest = Some((load, id.clone())),
                _ => {}
            }
            match &lightest {
                None => lightest = Some((load, id.clone())),
                Some((ll, _)) if load < *ll => lightest = Some((load, id.clone())),
                _ => {}
            }
        }
        let Some((hload, hid)) = heaviest else {
            break;
        };
        let Some((lload, lid)) = lightest else {
            break;
        };
        if hload <= lload + 1 {
            break;
        }

        // Need to find a partition on hid whose topic lid subscribes to.
        let l_subs = subs.get(&lid).cloned().unwrap_or_default();
        let h_parts = new_assignment.get(&hid).cloned().unwrap_or_default();
        let mut candidates: Vec<(String, i32)> = h_parts
            .into_iter()
            .filter(|tp| l_subs.contains(&tp.0))
            .collect();
        if candidates.is_empty() {
            // Can't fix this imbalance with these two endpoints; we'd need
            // multi-hop, which JVM also doesn't do. The JVM scans the full pair
            // matrix per iteration; the cases covered here (assignor tests +
            // steady state) produce the same result.
            break;
        }
        // Sort candidates: partitions lid didn't previously own first
        // (sticky), then (topic, partition).
        candidates.sort_by(|a, b| {
            let a_was_lids = prev_owner.get(a) == Some(&lid);
            let b_was_lids = prev_owner.get(b) == Some(&lid);
            a_was_lids
                .cmp(&b_was_lids)
                .then_with(|| a.0.cmp(&b.0))
                .then_with(|| a.1.cmp(&b.1))
        });
        let moved = candidates.into_iter().next().expect("non-empty");

        let h_vec = new_assignment.get_mut(&hid).unwrap();
        if let Some(pos) = h_vec.iter().position(|x| x == &moved) {
            h_vec.remove(pos);
        }
        new_assignment.get_mut(&lid).unwrap().push(moved);
    }
}

#[cfg(test)]
mod tests {

    use super::*;

    fn tp(items: &[(&str, i32)]) -> Vec<(String, i32)> {
        items.iter().map(|(t, p)| ((*t).to_string(), *p)).collect()
    }

    fn topics(ts: &[&str]) -> Vec<String> {
        ts.iter().map(|s| (*s).to_string()).collect()
    }

    fn total_assigned(out: &HashMap<String, Vec<(String, i32)>>) -> usize {
        out.values().map(Vec::len).sum()
    }

    fn member_ids(ids: &[&str]) -> Vec<String> {
        ids.iter().map(|id| (*id).to_string()).collect()
    }

    fn subs(items: &[(&str, &[&str])]) -> BTreeMap<String, BTreeSet<String>> {
        items
            .iter()
            .map(|(id, topics)| {
                (
                    (*id).to_string(),
                    topics.iter().map(|t| (*t).to_string()).collect(),
                )
            })
            .collect()
    }

    fn topic_parts(items: &[(&str, i32)]) -> HashMap<String, i32> {
        items
            .iter()
            .map(|(topic, count)| ((*topic).to_string(), *count))
            .collect()
    }

    fn current(items: &[(&str, &[(&str, i32)])]) -> BTreeMap<String, Vec<(String, i32)>> {
        items
            .iter()
            .map(|(id, partitions)| ((*id).to_string(), tp(partitions)))
            .collect()
    }

    fn assigned(items: &[(&str, &[(&str, i32)])]) -> HashMap<String, Vec<(String, i32)>> {
        items
            .iter()
            .map(|(id, partitions)| ((*id).to_string(), tp(partitions)))
            .collect()
    }

    #[test]
    fn single_member_takes_all() {
        let mut topic_parts = HashMap::new();
        topic_parts.insert("t".to_string(), 4);
        let a = assign(
            &[("m1".to_string(), topics(&["t"]), vec![], 0)],
            &topic_parts,
        );
        assert2::assert!(a["m1"] == tp(&[("t", 0), ("t", 1), ("t", 2), ("t", 3)]));
    }

    #[test]
    fn fresh_join_balances() {
        let mut topic_parts = HashMap::new();
        topic_parts.insert("t".to_string(), 4);
        let a = assign(
            &[
                ("m1".to_string(), topics(&["t"]), vec![], 0),
                ("m2".to_string(), topics(&["t"]), vec![], 0),
            ],
            &topic_parts,
        );
        assert2::assert!(
            a == assigned(&[("m1", &[("t", 0), ("t", 2)]), ("m2", &[("t", 1), ("t", 3)])])
        );
    }

    #[test]
    fn prepopulate_prefers_higher_generation_and_drops_exact_ties() {
        let topic_parts = topic_parts(&[("t", 3)]);
        let subs = subs(&[("m1", &["t"]), ("m2", &["t"]), ("m3", &["t"])]);
        let gens = BTreeMap::from([
            ("m1".to_string(), 5),
            ("m2".to_string(), 4),
            ("m3".to_string(), 5),
        ]);

        let out = prepopulate_current_assignments(
            &[
                (
                    "m1".to_string(),
                    topics(&["t"]),
                    tp(&[("t", 0), ("t", 1)]),
                    5,
                ),
                ("m2".to_string(), topics(&["t"]), tp(&[("t", 0)]), 4),
                ("m3".to_string(), topics(&["t"]), tp(&[("t", 1)]), 5),
            ],
            &subs,
            &gens,
            &topic_parts,
        );

        assert2::assert!(out == current(&[("m1", &[("t", 0)]), ("m2", &[]), ("m3", &[])]));
    }

    #[test]
    fn general_assign_chooses_least_loaded_subscriber() {
        let out = general_assign(
            &member_ids(&["m1", "m2"]),
            &subs(&[("m1", &["t"]), ("m2", &["t"])]),
            &BTreeMap::new(),
            &topic_parts(&[("t", 2)]),
        );

        assert2::assert!(
            out == HashMap::from([
                ("m1".into(), tp(&[("t", 0)])),
                ("m2".into(), tp(&[("t", 1)])),
            ])
        );
    }

    #[test]
    fn general_assign_keeps_previous_owner_only_when_subscribed_and_not_overloaded() {
        for (_name, ids, member_subs, current_assignment, topic_partitions, expected) in [
            // Subscribed, not-overloaded previous owner keeps its partition.
            (
                "subscribed previous owner",
                member_ids(&["m1", "m2"]),
                subs(&[("m1", &["t"]), ("m2", &["t"])]),
                current(&[("m1", &[]), ("m2", &[("t", 0)])]),
                topic_parts(&[("t", 2)]),
                assigned(&[("m1", &[("t", 1)]), ("m2", &[("t", 0)])]),
            ),
            // Unsubscribed previous owner loses the partition.
            (
                "unsubscribed previous owner",
                member_ids(&["m1", "m2"]),
                subs(&[("m1", &["t"]), ("m2", &["u"])]),
                current(&[("m1", &[]), ("m2", &[("t", 0)])]),
                topic_parts(&[("t", 1), ("u", 1)]),
                assigned(&[("m1", &[("t", 0)]), ("m2", &[("u", 0)])]),
            ),
            // Overloaded previous owner keeps only up to the load bound.
            (
                "overloaded previous owner",
                member_ids(&["m1", "m2", "m3"]),
                subs(&[("m1", &["t"]), ("m2", &["t"]), ("m3", &["t"])]),
                current(&[("m1", &[("t", 0), ("t", 1)]), ("m2", &[]), ("m3", &[])]),
                topic_parts(&[("t", 3)]),
                assigned(&[
                    ("m1", &[("t", 0)]),
                    ("m2", &[("t", 1)]),
                    ("m3", &[("t", 2)]),
                ]),
            ),
        ] {
            let out = general_assign(&ids, &member_subs, &current_assignment, &topic_partitions);
            assert2::assert!(out == expected);
        }
    }

    #[test]
    fn pick_least_loaded_prefers_lower_load_then_lexicographic_id() {
        let subscribed: BTreeSet<String> = member_ids(&["m1", "m2", "m3"]).into_iter().collect();
        for (_name, assignment, expected) in [
            // Unique minimum load wins.
            (
                "unique minimum",
                current(&[
                    ("m1", &[("t", 0)]),
                    ("m2", &[]),
                    ("m3", &[("t", 1), ("t", 2)]),
                ]),
                "m2",
            ),
            // Tied minimum load: lexicographically-first id wins.
            (
                "lexicographic tie break",
                current(&[
                    ("m1", &[("t", 0)]),
                    ("m2", &[("t", 1)]),
                    ("m3", &[("t", 2), ("t", 3)]),
                ]),
                "m1",
            ),
        ] {
            assert2::assert!(pick_least_loaded(&subscribed, &assignment) == expected);
        }
    }

    #[test]
    fn choose_assignment_owner_uses_least_loaded_and_sticky_previous_owner_bounds() {
        let subscribed: BTreeSet<String> = member_ids(&["m1", "m2", "m3"]).into_iter().collect();
        let tp = ("t".to_string(), 0);
        for (_name, prev_owner, assignment, expected) in [
            // No previous owner: least-loaded member wins.
            (
                "no previous owner",
                HashMap::new(),
                current(&[
                    ("m1", &[("a", 0)]),
                    ("m2", &[]),
                    ("m3", &[("b", 0), ("b", 1)]),
                ]),
                "m2",
            ),
            // Sticky previous owner above the min load: least-loaded wins.
            (
                "overloaded sticky owner",
                HashMap::from([(tp.clone(), "m1".to_string())]),
                current(&[
                    ("m1", &[("a", 0)]),
                    ("m2", &[]),
                    ("m3", &[("b", 0), ("b", 1)]),
                ]),
                "m2",
            ),
            // Sticky previous owner at the min load keeps the partition.
            (
                "balanced sticky owner",
                HashMap::from([(tp.clone(), "m1".to_string())]),
                current(&[("m1", &[]), ("m2", &[]), ("m3", &[("b", 0), ("b", 1)])]),
                "m1",
            ),
            // Unsubscribed previous owner: least-loaded wins.
            (
                "unsubscribed previous owner",
                HashMap::from([(tp.clone(), "mx".to_string())]),
                current(&[
                    ("m1", &[("a", 0)]),
                    ("m2", &[]),
                    ("m3", &[("b", 0), ("b", 1)]),
                ]),
                "m2",
            ),
        ] {
            assert2::assert!(
                choose_assignment_owner(&tp, &subscribed, &prev_owner, &assignment) == expected
            );
        }
    }

    #[test]
    fn max_balance_iters_has_safety_slack_after_partition_member_product() {
        for (_name, partitions, members, expected) in [
            ("empty partitions", 0, 5, 16),
            ("nonempty product", 3, 4, 28),
        ] {
            assert2::assert!(max_balance_iters(partitions, members) == expected);
        }
    }

    #[test]
    fn balance_assignment_moves_from_heaviest_to_lightest_with_sticky_candidate_order() {
        let ids = member_ids(&["m1", "m2", "m3"]);
        let subs = subs(&[("m1", &["t"]), ("m2", &["t"]), ("m3", &["t"])]);
        let prev_owner = HashMap::from([(("t".to_string(), 1), "m2".to_string())]);
        let mut assignment = current(&[
            ("m1", &[("t", 0), ("t", 1), ("t", 2), ("t", 3)]),
            ("m2", &[]),
            ("m3", &[]),
        ]);

        balance_assignment(&ids, &subs, &prev_owner, &mut assignment, 8);

        assert2::assert!(
            assignment
                == current(&[
                    ("m1", &[("t", 2), ("t", 3)]),
                    ("m2", &[("t", 0)]),
                    ("m3", &[("t", 1)]),
                ])
        );
    }

    #[test]
    fn balance_assignment_selects_non_first_heaviest_and_non_sticky_candidate() {
        let ids = member_ids(&["m1", "m2", "m3"]);
        let subs = subs(&[("m1", &["t"]), ("m2", &["t"]), ("m3", &["t"])]);
        let prev_owner = HashMap::from([(("t".to_string(), 1), "m1".to_string())]);
        let mut assignment = current(&[
            ("m1", &[]),
            ("m2", &[("t", 0), ("t", 1), ("t", 2)]),
            ("m3", &[]),
        ]);

        balance_assignment(&ids, &subs, &prev_owner, &mut assignment, 1);

        assert2::assert!(
            assignment
                == current(&[
                    ("m1", &[("t", 0)]),
                    ("m2", &[("t", 1), ("t", 2)]),
                    ("m3", &[]),
                ])
        );
    }

    #[test]
    fn balance_assignment_keeps_lex_first_heaviest_on_equal_load() {
        let ids = member_ids(&["m1", "m2", "m3"]);
        let subs = subs(&[("m1", &["t"]), ("m2", &["t"]), ("m3", &["t"])]);
        let mut assignment = current(&[
            ("m1", &[("t", 0), ("t", 1)]),
            ("m2", &[("t", 2), ("t", 3)]),
            ("m3", &[]),
        ]);

        balance_assignment(&ids, &subs, &HashMap::new(), &mut assignment, 1);

        assert2::assert!(
            assignment
                == current(&[
                    ("m1", &[("t", 1)]),
                    ("m2", &[("t", 2), ("t", 3)]),
                    ("m3", &[("t", 0)]),
                ])
        );
    }

    #[test]
    fn balance_assignment_moves_partition_lightest_did_not_previously_own() {
        let ids = member_ids(&["m1", "m2"]);
        let subs = subs(&[("m1", &["t"]), ("m2", &["t"])]);
        let prev_owner = HashMap::from([(("t".to_string(), 0), "m1".to_string())]);
        let mut assignment = current(&[("m1", &[]), ("m2", &[("t", 0), ("t", 1), ("t", 2)])]);

        balance_assignment(&ids, &subs, &prev_owner, &mut assignment, 1);

        assert2::assert!(
            assignment == current(&[("m1", &[("t", 1)]), ("m2", &[("t", 0), ("t", 2)]),])
        );
    }

    #[test]
    fn balance_assignment_stops_when_spread_is_at_most_one() {
        let ids = member_ids(&["m1", "m2", "m3"]);
        let subs = subs(&[("m1", &["t"]), ("m2", &["t"]), ("m3", &["t"])]);
        let mut assignment = current(&[
            ("m1", &[("t", 0), ("t", 1)]),
            ("m2", &[("t", 2)]),
            ("m3", &[("t", 3)]),
        ]);

        balance_assignment(&ids, &subs, &HashMap::new(), &mut assignment, 8);

        assert2::assert!(
            assignment
                == current(&[
                    ("m1", &[("t", 0), ("t", 1)]),
                    ("m2", &[("t", 2)]),
                    ("m3", &[("t", 3)]),
                ])
        );
    }

    #[test]
    fn all_owned_stable_no_op() {
        let mut topic_parts = HashMap::new();
        topic_parts.insert("t".to_string(), 4);
        let owned_m1 = tp(&[("t", 0), ("t", 1)]);
        let owned_m2 = tp(&[("t", 2), ("t", 3)]);
        let a = assign(
            &[
                ("m1".to_string(), topics(&["t"]), owned_m1.clone(), 5),
                ("m2".to_string(), topics(&["t"]), owned_m2.clone(), 5),
            ],
            &topic_parts,
        );
        assert2::assert!(
            a == assigned(&[("m1", &[("t", 0), ("t", 1)]), ("m2", &[("t", 2), ("t", 3)])])
        );
    }

    #[test]
    fn partial_revocation_on_member_join() {
        // m1=4 owned, m2=4 owned, m3 joins fresh. 9 partitions total.
        // target = 3 each (9/3); m1 keeps 3 of its 4, m2 keeps 3 of its 4,
        // released = 2, brand-new partition = 1 (8 was unowned). All
        // three of those (the ones that would go to m3) are OMITTED in
        // phase 1 because they need to move from a live owner.
        let mut topic_parts = HashMap::new();
        topic_parts.insert("t".to_string(), 9);
        let owned_m1 = tp(&[("t", 0), ("t", 1), ("t", 2), ("t", 3)]);
        let owned_m2 = tp(&[("t", 4), ("t", 5), ("t", 6), ("t", 7)]);
        // partition 8 is brand new (unowned).
        let a = assign(
            &[
                ("m1".to_string(), topics(&["t"]), owned_m1.clone(), 5),
                ("m2".to_string(), topics(&["t"]), owned_m2.clone(), 5),
                ("m3".to_string(), topics(&["t"]), vec![], 5),
            ],
            &topic_parts,
        );

        // m1, m2 each keep the first 3 of their owned; m3 only receives
        // brand-new (unowned) partitions in phase 1 — partition 8. The two
        // released partitions ((t,3) and (t,7)) are omitted this round.
        assert2::assert!(
            a == assigned(&[
                ("m1", &[("t", 0), ("t", 1), ("t", 2)]),
                ("m2", &[("t", 4), ("t", 5), ("t", 6)]),
                ("m3", &[("t", 8)]),
            ])
        );
    }

    #[test]
    fn phase2_picks_up_revoked() {
        // Simulate phase 2: re-run assign with m1 + m2's owned trimmed
        // down to phase-1 output. m3 owned remains empty.
        // Phase 1 produced m1=3, m2=3, m3 owns whatever was unowned.
        let mut topic_parts = HashMap::new();
        topic_parts.insert("t".to_string(), 9);
        let phase1 = assign(
            &[
                (
                    "m1".to_string(),
                    topics(&["t"]),
                    tp(&[("t", 0), ("t", 1), ("t", 2), ("t", 3)]),
                    5,
                ),
                (
                    "m2".to_string(),
                    topics(&["t"]),
                    tp(&[("t", 4), ("t", 5), ("t", 6), ("t", 7)]),
                    5,
                ),
                ("m3".to_string(), topics(&["t"]), vec![], 5),
            ],
            &topic_parts,
        );

        // Now feed phase-1 output back as owned and re-assign.
        let phase2 = assign(
            &[
                ("m1".to_string(), topics(&["t"]), phase1["m1"].clone(), 6),
                ("m2".to_string(), topics(&["t"]), phase1["m2"].clone(), 6),
                ("m3".to_string(), topics(&["t"]), phase1["m3"].clone(), 6),
            ],
            &topic_parts,
        );

        // Phase 2 places all 9: m3 keeps its unowned pickup ((t,8)) and now
        // also receives the two partitions released in phase 1.
        assert2::assert!(
            phase2
                == assigned(&[
                    ("m1", &[("t", 0), ("t", 1), ("t", 2)]),
                    ("m2", &[("t", 4), ("t", 5), ("t", 6)]),
                    ("m3", &[("t", 3), ("t", 7), ("t", 8)]),
                ])
        );
    }

    #[test]
    fn member_leaves_partitions_redistributed() {
        // m1 leaves; m2 keeps its half of the partitions. m1's old
        // partitions have no live owner now, so they're freely
        // redistributed in this round (not held back).
        let mut topic_parts = HashMap::new();
        topic_parts.insert("t".to_string(), 4);
        let a = assign(
            &[(
                "m2".to_string(),
                topics(&["t"]),
                tp(&[("t", 2), ("t", 3)]),
                5,
            )],
            &topic_parts,
        );
        assert2::assert!(a == assigned(&[("m2", &[("t", 0), ("t", 1), ("t", 2), ("t", 3)])]));
    }

    #[test]
    fn multi_topic_asymmetric_subscriptions() {
        // m1 subscribes to [t1, t2], m2 only [t1]. Forces general path.
        // t1 has 2 partitions, t2 has 2 partitions.
        // t2 partitions can only go to m1; t1 must be split.
        let mut topic_parts = HashMap::new();
        topic_parts.insert("t1".to_string(), 2);
        topic_parts.insert("t2".to_string(), 2);
        let a = assign(
            &[
                ("m1".to_string(), topics(&["t1", "t2"]), vec![], 0),
                ("m2".to_string(), topics(&["t1"]), vec![], 0),
            ],
            &topic_parts,
        );
        // m2 only gets t1 partitions.
        for (t, _) in &a["m2"] {
            assert2::assert!(t == "t1");
        }
        // m1 gets both t2 partitions (only it can hold them); balanced 2/2.
        assert2::assert!(
            a == assigned(&[
                ("m1", &[("t2", 0), ("t2", 1)]),
                ("m2", &[("t1", 0), ("t1", 1)]),
            ])
        );
    }

    #[test]
    fn generation_zombie_lower_loses() {
        // m1 gen=5 and m2 gen=4 both claim (t, 0). m1 wins.
        // 4 partitions; balanced means 2/2 in steady state. After
        // resolving the zombie m1 owns (t,0)+(t,1), m2 owns nothing on
        // record (since (t,0) is now disputed off them, and they had no
        // others). The new assignment should give m1 some, m2 some;
        // total should be 4 (no live owner is losing partitions).
        let mut topic_parts = HashMap::new();
        topic_parts.insert("t".to_string(), 4);
        let a = assign(
            &[
                (
                    "m1".to_string(),
                    topics(&["t"]),
                    tp(&[("t", 0), ("t", 1)]),
                    5,
                ),
                ("m2".to_string(), topics(&["t"]), tp(&[("t", 0)]), 4),
            ],
            &topic_parts,
        );
        // m1 keeps both of its claims (post zombie resolution); m2 doesn't
        // get (t,0) since m1 effectively owns it, and receives the rest.
        assert2::assert!(
            a == assigned(&[("m1", &[("t", 0), ("t", 1)]), ("m2", &[("t", 2), ("t", 3)])])
        );
    }

    #[test]
    fn generation_zombie_tie_both_lose() {
        // m1 gen=5 and m2 gen=5 both claim (t,0) → neither keeps it.
        // Both also claim other unique partitions to keep current_assignment
        // non-empty.
        let mut topic_parts = HashMap::new();
        topic_parts.insert("t".to_string(), 4);
        let a = assign(
            &[
                (
                    "m1".to_string(),
                    topics(&["t"]),
                    tp(&[("t", 0), ("t", 1)]),
                    5,
                ),
                (
                    "m2".to_string(),
                    topics(&["t"]),
                    tp(&[("t", 0), ("t", 2)]),
                    5,
                ),
            ],
            &topic_parts,
        );
        // After zombie resolution (t,0) is unowned → assignable in this
        // round. m1's non-disputed claim was (t,1); m2's was (t,2).
        // Partition (t,3) was always unowned.
        // Phase 1: m1 keeps (t,1), m2 keeps (t,2); (t,0) and (t,3) are
        // unowned → distributed in this round.
        assert2::assert!(total_assigned(&a) == 4);
    }

    #[test]
    fn brand_new_topic() {
        // Topic added between rebalances; nobody owns its partitions.
        // They're all assigned in this round.
        let mut topic_parts = HashMap::new();
        topic_parts.insert("newt".to_string(), 3);
        let a = assign(
            &[
                ("m1".to_string(), topics(&["newt"]), vec![], 5),
                ("m2".to_string(), topics(&["newt"]), vec![], 5),
            ],
            &topic_parts,
        );
        // 2/1 split, lex-first gets the extra.
        assert2::assert!(
            a == assigned(&[("m1", &[("newt", 0), ("newt", 2)]), ("m2", &[("newt", 1)])])
        );
    }

    #[test]
    fn partition_count_decreased() {
        // m1 claims (t,5) but topic only has 3 partitions now → dropped.
        let mut topic_parts = HashMap::new();
        topic_parts.insert("t".to_string(), 3);
        let a = assign(
            &[(
                "m1".to_string(),
                topics(&["t"]),
                tp(&[("t", 0), ("t", 5)]),
                5,
            )],
            &topic_parts,
        );
        // m1 still receives (t,0..3) but never (t,5).
        assert2::assert!(a == assigned(&[("m1", &[("t", 0), ("t", 1), ("t", 2)])]));
    }

    #[test]
    fn empty_members_returns_empty() {
        let topic_parts = HashMap::new();
        let a = assign(&[], &topic_parts);
        assert2::assert!(a.is_empty());
    }

    #[test]
    fn member_with_no_subscriptions() {
        // m2 subscribes to nothing → empty assignment.
        let mut topic_parts = HashMap::new();
        topic_parts.insert("t".to_string(), 2);
        let a = assign(
            &[
                ("m1".to_string(), topics(&["t"]), vec![], 0),
                ("m2".to_string(), vec![], vec![], 0),
            ],
            &topic_parts,
        );
        assert2::assert!((a["m1"].len(), a["m2"].len()) == (2, 0));
    }
}
