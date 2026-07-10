//! KIP-1071 server-side task assignor (sticky / highly-available; active,
//! standby, warmup placement + catch-up promotion).
//!
//! This is a *pure* module: no async, no I/O, no metadata access. It takes
//! already-resolved inputs ([`AssignorInput`] + a slice of [`AssignorMember`])
//! and returns a target [`StreamsAssignment`]. The coordinator actor (a later
//! slice) builds the inputs from member state + topology and applies the
//! output as the next target assignment.
//!
//! A *task* is `(subtopology_id, partition)`. Tasks come in three roles:
//! **active** (exactly one instance per task), **standby** (replicas of
//! stateful tasks for failover), and **warmup** (transient: a member building
//! up a stateful task's local state before it can safely take it over as
//! active).
//!
//! Determinism is mandatory: members are processed in lexicographic
//! `member_id` order and tasks in `(subtopology_id, partition)` order, so the
//! same inputs always yield the same assignment.

use std::collections::{BTreeMap, BTreeSet, HashMap};

use super::config::StreamsAssignorKind;

/// A single group member as seen by the assignor, with its current ownership
/// (used for stickiness) and reported per-task changelog lag (used to decide
/// warmup catch-up).
#[derive(Debug, Clone)]
pub struct AssignorMember {
    pub member_id: String,
    /// Process the member runs in. Standby placement spreads copies across
    /// distinct processes for fault tolerance.
    pub process_id: String,
    pub rack_id: Option<String>,
    /// Active tasks the member currently owns (for stickiness):
    /// `subtopology_id -> partitions`.
    pub current_active: BTreeMap<String, Vec<i32>>,
    /// Standby tasks the member currently owns.
    pub current_standby: BTreeMap<String, Vec<i32>>,
    /// Warmup tasks the member currently owns.
    pub current_warmup: BTreeMap<String, Vec<i32>>,
    /// Reported changelog lag per task: `(subtopology, partition) -> lag`
    /// (`end - position`). An absent entry means unknown / not caught up.
    pub task_lag: BTreeMap<(String, i32), i64>,
}

/// Inputs to a single assignment computation. The task universe, which
/// subtopologies are stateful, and the placement knobs (standby/warmup counts,
/// acceptable recovery lag, and the assignor kind).
#[derive(Debug, Clone)]
pub struct AssignorInput {
    /// The full task universe: `subtopology_id -> ALL partitions`.
    pub tasks: BTreeMap<String, Vec<i32>>,
    /// Subtopology ids that have a changelog (i.e. are stateful).
    pub stateful: BTreeSet<String>,
    /// `num.standby.replicas`: standby copies per stateful task.
    pub num_standby_replicas: i32,
    /// `max.warmup.replicas`: global cap on concurrent warmup tasks created in
    /// this assignment.
    pub num_warmup_replicas: i32,
    /// `acceptable.recovery.lag`: max changelog lag at which a warmup target is
    /// considered caught up and the active move is allowed immediately.
    pub acceptable_recovery_lag: i64,
    /// Server-side assignor selection.
    pub kind: StreamsAssignorKind,
}

/// The computed target assignment: per-member task maps for each role.
///
/// A member with no tasks in a role simply has no entry in that role's map.
#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub struct StreamsAssignment {
    /// `member_id -> active tasks`.
    pub active: HashMap<String, BTreeMap<String, Vec<i32>>>,
    /// `member_id -> standby tasks`.
    pub standby: HashMap<String, BTreeMap<String, Vec<i32>>>,
    /// `member_id -> warmup tasks`.
    pub warmup: HashMap<String, BTreeMap<String, Vec<i32>>>,
}

/// A `(subtopology_id, partition)` task in its canonical ordered form.
type Task = (String, i32);

/// Compute the target [`StreamsAssignment`] for a streams group.
///
/// See the module docs for the algorithm. With no members, returns an empty
/// assignment.
#[must_use]
pub fn assign(members: &[AssignorMember], input: &AssignorInput) -> StreamsAssignment {
    if members.is_empty() {
        return StreamsAssignment::default();
    }

    // Stable member ordering for every downstream decision.
    let mut ordered: Vec<&AssignorMember> = members.iter().collect();
    ordered.sort_by(|a, b| a.member_id.cmp(&b.member_id));

    let kind = resolve_kind(input);
    let highly_available = matches!(kind, StreamsAssignorKind::HighlyAvailable);

    // Flatten the task universe in (subtopology, partition) order.
    let tasks = flatten_tasks(&input.tasks);

    // 1. Balanced active target (both modes).
    let mut active = assign_active(&ordered, &tasks);

    // 2. Warmup deferral (HighlyAvailable only). May rewrite active back to the
    //    current owner and stage a warmup on the intended target.
    let mut warmup: HashMap<String, Vec<Task>> = HashMap::new();
    if highly_available {
        defer_warmups(&ordered, input, &tasks, &mut active, &mut warmup);
    }

    // 3. Standby placement (HighlyAvailable only).
    let mut standby: HashMap<String, Vec<Task>> = HashMap::new();
    if highly_available {
        assign_standby(&ordered, input, &tasks, &active, &warmup, &mut standby);
    }

    // 4. Assemble + normalize.
    StreamsAssignment {
        active: to_role_maps(&active),
        standby: to_role_maps(&standby),
        warmup: to_role_maps(&warmup),
    }
}

/// Resolve `Auto` to a concrete kind: `HighlyAvailable` when any stateful
/// subtopology exists, `Sticky` otherwise. `Sticky` / `HighlyAvailable` pass
/// through unchanged.
fn resolve_kind(input: &AssignorInput) -> StreamsAssignorKind {
    match input.kind {
        StreamsAssignorKind::Auto => {
            if input.stateful.is_empty() {
                StreamsAssignorKind::Sticky
            } else {
                StreamsAssignorKind::HighlyAvailable
            }
        }
        other => other,
    }
}

/// Flatten the `subtopology -> partitions` universe into an ordered, de-duped
/// list of `(subtopology, partition)` tasks.
fn flatten_tasks(tasks: &BTreeMap<String, Vec<i32>>) -> Vec<Task> {
    let mut out: Vec<Task> = Vec::new();
    for (sub, parts) in tasks {
        for &p in parts {
            out.push((sub.clone(), p));
        }
    }
    // BTreeMap already orders by subtopology; sort/dedup guards against
    // unsorted or duplicate partition lists in the input.
    out.sort();
    out.dedup();
    out
}

/// Look up which member currently owns `task` as active, if any still present.
fn current_active_owner<'a>(members: &[&'a AssignorMember], task: &Task) -> Option<&'a str> {
    members
        .iter()
        .find(|m| owns(&m.current_active, task))
        .map(|m| m.member_id.as_str())
}

/// Whether a role map contains `task`.
fn owns(role: &BTreeMap<String, Vec<i32>>, task: &Task) -> bool {
    role.get(&task.0)
        .is_some_and(|parts| parts.contains(&task.1))
}

/// Step 1: compute the balanced active target. Each task gets exactly one
/// owner. Sticky-keep tasks whose current owner still exists, place the rest on
/// the least-loaded member, then balance until max-min load `<= 1`.
fn assign_active(members: &[&AssignorMember], tasks: &[Task]) -> HashMap<String, Vec<Task>> {
    let mut active: HashMap<String, Vec<Task>> = HashMap::new();
    for m in members {
        active.entry(m.member_id.clone()).or_default();
    }

    // Sticky placement first; collect orphans for least-loaded placement.
    let mut orphans: Vec<Task> = Vec::new();
    for task in tasks {
        if let Some(owner) = current_active_owner(members, task) {
            active
                .get_mut(owner)
                .expect("owner present")
                .push(task.clone());
        } else {
            orphans.push(task.clone());
        }
    }

    // Place orphans on the least-loaded member (tie-break: lexicographic id).
    for task in orphans {
        let target = least_loaded(members, &active);
        active.get_mut(&target).expect("member present").push(task);
    }

    // Balancing pass: while the spread exceeds 1, move one task from the
    // most-loaded member to the least-loaded. We move the lexicographically
    // largest task off the most-loaded member — simple and deterministic, and
    // since orphan placement already balanced perfectly, this only fires when
    // sticky placement skewed the load.
    while let Some((max_id, min_id)) = load_extremes(members, &active) {
        let max_load = active[&max_id].len();
        let min_load = active[&min_id].len();
        if max_load <= min_load + 1 {
            break;
        }
        let moved = {
            let from = active.get_mut(&max_id).expect("member present");
            // Largest task by (subtopology, partition) order.
            let idx = from
                .iter()
                .enumerate()
                .max_by(|(_, a), (_, b)| a.cmp(b))
                .map(|(i, _)| i)
                .expect("most-loaded member is non-empty");
            from.swap_remove(idx)
        };
        active.get_mut(&min_id).expect("member present").push(moved);
    }

    active
}

/// Member id with the fewest active tasks (tie-break: lexicographic id).
fn least_loaded(members: &[&AssignorMember], active: &HashMap<String, Vec<Task>>) -> String {
    members
        .iter()
        .min_by(|a, b| {
            let la = active[&a.member_id].len();
            let lb = active[&b.member_id].len();
            la.cmp(&lb).then_with(|| a.member_id.cmp(&b.member_id))
        })
        .map(|m| m.member_id.clone())
        .expect("members non-empty")
}

/// `(most_loaded_id, least_loaded_id)` by active load, each with deterministic
/// tie-breaks. `None` when there are no members.
fn load_extremes(
    members: &[&AssignorMember],
    active: &HashMap<String, Vec<Task>>,
) -> Option<(String, String)> {
    let max = members
        .iter()
        .max_by(|a, b| {
            let la = active[&a.member_id].len();
            let lb = active[&b.member_id].len();
            // On a load tie, prefer the lexicographically *larger* id as the
            // donor so the pairing with `least_loaded` (which prefers smaller)
            // is stable.
            la.cmp(&lb).then_with(|| a.member_id.cmp(&b.member_id))
        })
        .map(|m| m.member_id.clone())?;
    let min = least_loaded(members, active);
    Some((max, min))
}

/// Step 2: warmup deferral (`HighlyAvailable` only). For each *stateful* task
/// whose balanced-target owner differs from its current active owner (and a
/// current owner exists), keep the move only if the target is caught up;
/// otherwise leave it active on the current owner and stage a warmup on the
/// intended target — subject to the global warmup cap.
fn defer_warmups(
    members: &[&AssignorMember],
    input: &AssignorInput,
    tasks: &[Task],
    active: &mut HashMap<String, Vec<Task>>,
    warmup: &mut HashMap<String, Vec<Task>>,
) {
    // Snapshot the balanced-target owner for every task before mutating.
    let target_owner: HashMap<Task, String> = active
        .iter()
        .flat_map(|(member, ts)| ts.iter().map(move |t| (t.clone(), member.clone())))
        .collect();

    let mut warmups_created: i32 = 0;

    for task in tasks {
        if !input.stateful.contains(&task.0) {
            continue; // stateless: move applies directly, no warmup.
        }
        let Some(current) = current_active_owner(members, task) else {
            continue; // no prior owner: applied directly.
        };
        let Some(target) = target_owner.get(task) else {
            continue; // not in the active target (shouldn't happen).
        };
        if target == current {
            continue; // not a move.
        }

        // Is the intended target caught up on this task's changelog?
        let caught_up = members
            .iter()
            .find(|m| m.member_id == *target)
            .and_then(|m| m.task_lag.get(&(task.0.clone(), task.1)))
            .is_some_and(|&lag| lag <= input.acceptable_recovery_lag);

        if caught_up {
            continue; // keep the move; active already on `target`.
        }

        // Defer the move: active stays on `current`. Move the task back.
        move_active(active, target, current, task);

        // Stage a warmup on the intended target while under the global cap.
        if warmups_created < input.num_warmup_replicas {
            warmup.entry(target.clone()).or_default().push(task.clone());
            warmups_created += 1;
        }
    }
}

/// Move `task` from `from`'s active set to `to`'s active set.
fn move_active(active: &mut HashMap<String, Vec<Task>>, from: &str, to: &str, task: &Task) {
    if let Some(list) = active.get_mut(from)
        && let Some(idx) = list.iter().position(|t| t == task)
    {
        list.swap_remove(idx);
    }
    active.entry(to.to_owned()).or_default().push(task.clone());
}

/// Step 3: standby placement (`HighlyAvailable` only). For each stateful task,
/// place up to `num_standby_replicas` copies on members in processes distinct
/// from the active owner, from each other, and from any warmup holder of the
/// task. Prefer a different rack, then fewest standby load, then lexicographic
/// id.
fn assign_standby(
    members: &[&AssignorMember],
    input: &AssignorInput,
    tasks: &[Task],
    active: &HashMap<String, Vec<Task>>,
    warmup: &HashMap<String, Vec<Task>>,
    standby: &mut HashMap<String, Vec<Task>>,
) {
    if input.num_standby_replicas <= 0 {
        return;
    }

    // Reverse index: which member holds each task as active.
    let active_owner: HashMap<&Task, &str> = active
        .iter()
        .flat_map(|(member, ts)| ts.iter().map(move |t| (t, member.as_str())))
        .collect();

    let by_id: HashMap<&str, &AssignorMember> =
        members.iter().map(|m| (m.member_id.as_str(), *m)).collect();

    for task in tasks {
        if !input.stateful.contains(&task.0) {
            continue; // stateless tasks get no standby.
        }
        let Some(&owner_id) = active_owner.get(task) else {
            continue;
        };
        let Some(owner) = by_id.get(owner_id) else {
            continue;
        };
        let active_rack = owner.rack_id.as_deref();

        // Processes already excluded for this task: the active owner's.
        let mut used_processes: BTreeSet<&str> = BTreeSet::new();
        used_processes.insert(owner.process_id.as_str());

        for _ in 0..input.num_standby_replicas {
            let chosen = members
                .iter()
                .filter(|m| !used_processes.contains(m.process_id.as_str()))
                .filter(|m| !owns_role(warmup, &m.member_id, task))
                .min_by(|a, b| {
                    standby_rank(a, active_rack, standby).cmp(&standby_rank(
                        b,
                        active_rack,
                        standby,
                    ))
                });

            let Some(chosen) = chosen else {
                break; // no more distinct processes available.
            };
            used_processes.insert(chosen.process_id.as_str());
            standby
                .entry(chosen.member_id.clone())
                .or_default()
                .push(task.clone());
        }
    }
}

/// Ranking key for a standby candidate: prefer a *different* rack from the
/// active owner, then fewest standby tasks so far, then lexicographic id.
/// Smaller is better.
fn standby_rank(
    m: &AssignorMember,
    active_rack: Option<&str>,
    standby: &HashMap<String, Vec<Task>>,
) -> (u8, usize, String) {
    // 0 = preferred (different rack, or no rack info to compare on); 1 = same
    // rack as the active owner.
    let rack_penalty = match (active_rack, m.rack_id.as_deref()) {
        (Some(a), Some(b)) if a == b => 1,
        _ => 0,
    };
    let load = standby.get(&m.member_id).map_or(0, Vec::len);
    (rack_penalty, load, m.member_id.clone())
}

/// Whether `member` holds `task` in the given role map.
fn owns_role(role: &HashMap<String, Vec<Task>>, member: &str, task: &Task) -> bool {
    role.get(member).is_some_and(|ts| ts.contains(task))
}

/// Convert a `member -> Vec<Task>` working map into the public
/// `member -> (subtopology -> partitions)` form, normalising every task map
/// (sort + dedup partitions, drop empty subtopology entries) and dropping
/// members with no tasks in the role.
fn to_role_maps(
    by_member: &HashMap<String, Vec<Task>>,
) -> HashMap<String, BTreeMap<String, Vec<i32>>> {
    let mut out: HashMap<String, BTreeMap<String, Vec<i32>>> = HashMap::new();
    for (member, ts) in by_member {
        if ts.is_empty() {
            continue;
        }
        let mut role: BTreeMap<String, Vec<i32>> = BTreeMap::new();
        for (sub, part) in ts {
            role.entry(sub.clone()).or_default().push(*part);
        }
        for parts in role.values_mut() {
            parts.sort_unstable();
            parts.dedup();
        }
        role.retain(|_, parts| !parts.is_empty());
        if !role.is_empty() {
            out.insert(member.clone(), role);
        }
    }
    out
}

#[cfg(test)]
mod tests {
    use assert2::{assert, check};

    use super::*;

    fn member(id: &str, process: &str) -> AssignorMember {
        AssignorMember {
            member_id: id.to_owned(),
            process_id: process.to_owned(),
            rack_id: None,
            current_active: BTreeMap::new(),
            current_standby: BTreeMap::new(),
            current_warmup: BTreeMap::new(),
            task_lag: BTreeMap::new(),
        }
    }

    fn input(
        tasks: &[(&str, &[i32])],
        stateful: &[&str],
        kind: StreamsAssignorKind,
    ) -> AssignorInput {
        AssignorInput {
            tasks: tasks
                .iter()
                .map(|(s, p)| ((*s).to_owned(), p.to_vec()))
                .collect(),
            stateful: stateful.iter().map(|s| (*s).to_owned()).collect(),
            num_standby_replicas: 0,
            num_warmup_replicas: 0,
            acceptable_recovery_lag: 10_000,
            kind,
        }
    }

    /// Total active tasks across all members in an assignment.
    fn count(role: &HashMap<String, BTreeMap<String, Vec<i32>>>) -> usize {
        role.values().flat_map(BTreeMap::values).map(Vec::len).sum()
    }

    #[test]
    fn empty_members_empty_assignment() {
        let inp = input(&[("a", &[0, 1])], &[], StreamsAssignorKind::Sticky);
        let out = assign(&[], &inp);
        assert!(
            out == StreamsAssignment {
                active: HashMap::new(),
                standby: HashMap::new(),
                warmup: HashMap::new(),
            }
        );
    }

    #[test]
    fn single_member_single_stateless_subtopology() {
        let members = [member("A", "p1")];
        let inp = input(&[("sub-0", &[0, 1, 2])], &[], StreamsAssignorKind::Sticky);
        let out = assign(&members, &inp);
        assert!(
            out == StreamsAssignment {
                active: HashMap::from([(
                    "A".to_string(),
                    BTreeMap::from([("sub-0".to_string(), vec![0, 1, 2])]),
                )]),
                standby: HashMap::new(),
                warmup: HashMap::new(),
            }
        );
    }

    #[test]
    fn two_members_four_stateless_tasks_balanced() {
        let members = [member("A", "p1"), member("B", "p2")];
        let inp = input(
            &[("sub-0", &[0, 1, 2, 3])],
            &[],
            StreamsAssignorKind::Sticky,
        );
        let out = assign(&members, &inp);
        // 2/2 balanced; deterministic: least-loaded fills A first, then B,
        // alternating.
        assert!(
            out.active
                == HashMap::from([
                    (
                        "A".to_string(),
                        BTreeMap::from([("sub-0".to_string(), vec![0, 2])]),
                    ),
                    (
                        "B".to_string(),
                        BTreeMap::from([("sub-0".to_string(), vec![1, 3])]),
                    ),
                ])
        );
        // Re-running yields identical output.
        let out2 = assign(&members, &inp);
        assert!(out.active == out2.active);
    }

    #[test]
    fn stickiness_keeps_owned_tasks() {
        let mut a = member("A", "p1");
        a.current_active = BTreeMap::from([("sub-0".to_owned(), vec![0, 1])]);
        let b = member("B", "p2");
        let members = [a, b];
        // Universe grew to 4 partitions; A owns 0,1 already.
        let inp = input(
            &[("sub-0", &[0, 1, 2, 3])],
            &[],
            StreamsAssignorKind::Sticky,
        );
        let out = assign(&members, &inp);
        // A keeps 0,1 (sticky); B fills the rest to balance 2/2.
        assert!(
            out.active
                == HashMap::from([
                    (
                        "A".to_string(),
                        BTreeMap::from([("sub-0".to_string(), vec![0, 1])]),
                    ),
                    (
                        "B".to_string(),
                        BTreeMap::from([("sub-0".to_string(), vec![2, 3])]),
                    ),
                ])
        );
    }

    #[test]
    fn stickiness_rebalances_when_skewed() {
        // A owns all 4; adding B must move some over to balance.
        let mut a = member("A", "p1");
        a.current_active = BTreeMap::from([("sub-0".to_owned(), vec![0, 1, 2, 3])]);
        let b = member("B", "p2");
        let members = [a, b];
        let inp = input(
            &[("sub-0", &[0, 1, 2, 3])],
            &[],
            StreamsAssignorKind::Sticky,
        );
        let out = assign(&members, &inp);
        assert!(out.active["A"]["sub-0"].len() == 2);
        assert!(out.active["B"]["sub-0"].len() == 2);
    }

    #[test]
    fn highly_available_standby_on_other_process() {
        let members = [member("A", "p1"), member("B", "p2")];
        let mut inp = input(
            &[("sub-0", &[0, 1])],
            &["sub-0"],
            StreamsAssignorKind::HighlyAvailable,
        );
        inp.num_standby_replicas = 1;
        let out = assign(&members, &inp);
        // Each of the 2 active tasks gets exactly one standby.
        assert!(count(&out.active) == 2);
        assert!(count(&out.standby) == 2);
        // The standby for a task sits on the *other* process than its active.
        for (sub, parts) in [("sub-0", vec![0, 1])]
            .iter()
            .flat_map(|(s, ps)| ps.iter().map(move |p| ((*s).to_owned(), *p)))
        {
            let active_owner = out
                .active
                .iter()
                .find(|(_, m)| m.get(&sub).is_some_and(|v| v.contains(&parts)))
                .map(|(id, _)| id.clone())
                .expect("active owner exists");
            let standby_owner = out
                .standby
                .iter()
                .find(|(_, m)| m.get(&sub).is_some_and(|v| v.contains(&parts)))
                .map(|(id, _)| id.clone())
                .expect("standby owner exists");
            assert!(active_owner != standby_owner);
        }
    }

    #[test]
    fn highly_available_same_process_no_standby() {
        // Both members in the same process: no fault-tolerant standby possible.
        let members = [member("A", "p1"), member("B", "p1")];
        let mut inp = input(
            &[("sub-0", &[0, 1])],
            &["sub-0"],
            StreamsAssignorKind::HighlyAvailable,
        );
        inp.num_standby_replicas = 1;
        let out = assign(&members, &inp);
        assert!(count(&out.active) == 2);
        assert!(count(&out.standby) == 0);
    }

    #[test]
    fn warmup_deferral_when_target_not_caught_up() {
        // A currently owns both partitions; balanced target wants to move one
        // to B. B has no lag info -> not caught up -> defer + warmup.
        let mut a = member("A", "p1");
        a.current_active = BTreeMap::from([("sub-0".to_owned(), vec![0, 1])]);
        let b = member("B", "p2");
        let members = [a, b];
        let mut inp = input(
            &[("sub-0", &[0, 1])],
            &["sub-0"],
            StreamsAssignorKind::HighlyAvailable,
        );
        inp.num_warmup_replicas = 2;
        inp.num_standby_replicas = 0;
        let out = assign(&members, &inp);
        // Active stays on A (move deferred); B holds a warmup.
        check!(count(&out.active) == 2);
        check!(out.active["A"]["sub-0"].len() == 2);
        check!(count(&out.warmup) == 1);
        check!(out.warmup.contains_key("B"));
    }

    #[test]
    fn warmup_promotes_when_caught_up() {
        // Same skew, but B reports lag within acceptable bounds for the task it
        // would receive -> active move applied immediately, no warmup.
        let mut a = member("A", "p1");
        a.current_active = BTreeMap::from([("sub-0".to_owned(), vec![0, 1])]);
        let mut b = member("B", "p2");
        // The balanced target moves the lexicographically-largest task (1) off
        // A onto B; report B caught up on it.
        b.task_lag = BTreeMap::from([(("sub-0".to_owned(), 1), 5_i64)]);
        let members = [a, b];
        let mut inp = input(
            &[("sub-0", &[0, 1])],
            &["sub-0"],
            StreamsAssignorKind::HighlyAvailable,
        );
        inp.num_warmup_replicas = 2;
        inp.acceptable_recovery_lag = 10;
        let out = assign(&members, &inp);
        // Move applied: A keeps 0, B takes 1; no warmup.
        assert!(
            (out.active, out.warmup)
                == (
                    HashMap::from([
                        (
                            "A".to_string(),
                            BTreeMap::from([("sub-0".to_string(), vec![0])]),
                        ),
                        (
                            "B".to_string(),
                            BTreeMap::from([("sub-0".to_string(), vec![1])]),
                        ),
                    ]),
                    HashMap::new(),
                )
        );
    }

    #[test]
    fn warmup_cap_respected() {
        // Two tasks both need to move to B, but the warmup cap is 1.
        let mut a = member("A", "p1");
        a.current_active = BTreeMap::from([("sub-0".to_owned(), vec![0, 1, 2, 3])]);
        let b = member("B", "p2");
        let members = [a, b];
        let mut inp = input(
            &[("sub-0", &[0, 1, 2, 3])],
            &["sub-0"],
            StreamsAssignorKind::HighlyAvailable,
        );
        inp.num_warmup_replicas = 1;
        inp.num_standby_replicas = 0;
        let out = assign(&members, &inp);
        // Balance wants 2 tasks on B but neither is caught up -> both deferred;
        // only one warmup created (cap = 1); both stay active on A.
        assert!(out.active["A"]["sub-0"].len() == 4);
        assert!(count(&out.warmup) == 1);
    }

    #[test]
    fn auto_resolves_to_sticky_when_stateless() {
        let members = [member("A", "p1"), member("B", "p2")];
        let mut inp = input(&[("sub-0", &[0, 1])], &[], StreamsAssignorKind::Auto);
        inp.num_standby_replicas = 1;
        inp.num_warmup_replicas = 2;
        let out = assign(&members, &inp);
        // Sticky: active-only, no standby/warmup even with non-zero knobs.
        check!(count(&out.active) == 2);
        check!(out.standby.is_empty());
        check!(out.warmup.is_empty());
    }

    #[test]
    fn auto_resolves_to_highly_available_when_stateful() {
        let members = [member("A", "p1"), member("B", "p2")];
        let mut inp = input(&[("sub-0", &[0, 1])], &["sub-0"], StreamsAssignorKind::Auto);
        inp.num_standby_replicas = 1;
        let out = assign(&members, &inp);
        // HighlyAvailable: stateful tasks get standby copies.
        assert!(count(&out.active) == 2);
        assert!(count(&out.standby) == 2);
    }
}
