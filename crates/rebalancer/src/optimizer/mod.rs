//! Optimizer: runs an ordered list of `Goal`s over a `ClusterState`,
//! coalesces their movements, and emits a `Proposal`.

use std::collections::HashMap;

use uuid::Uuid;

use crate::goals::{Goal, GoalContext, GoalPriority};
use crate::model::{
    ClusterState, Movement, PartitionView, Proposal, ProposalStatus, ProposalSummary,
    validate_movement,
};

#[derive(Debug, thiserror::Error)]
pub enum OptimizeError {
    #[error("hard goal `{goal}` produced {extra} movements past the {cap} cap")]
    HardGoalUnsatisfied {
        goal: String,
        extra: usize,
        cap: usize,
    },
}

#[derive(Debug)]
pub struct OptimizeOutput {
    pub proposal: Proposal,
    pub state_after: ClusterState,
}

/// Run the goals over `state` and produce a `Proposal`. Goals are
/// applied in priority order (Hard before Soft). The cluster state
/// passed to each goal reflects the cumulative effect of prior goals'
/// movements — soft goals see post-hard-goal counts.
pub fn optimize(
    state: &ClusterState,
    goals: &[&dyn Goal],
    ctx: &GoalContext,
) -> Result<OptimizeOutput, OptimizeError> {
    // 1. Order: Hard first, ties broken by registration order.
    let mut ordered: Vec<(usize, &&dyn Goal)> = goals.iter().enumerate().collect();
    ordered.sort_by_key(|(idx, g)| {
        (
            match g.priority() {
                GoalPriority::Hard => 0,
                GoalPriority::Soft => 1,
            },
            *idx,
        )
    });

    // 2. Working clone of the state — each Movement updates it before
    //    the next goal sees it.
    let mut working = state.clone();

    // (topic, partition) → (Movement, priority of the goal that wrote it).
    // Last writer wins on coalesce — and the priority tag follows the
    // last writer, so a soft goal overwriting a hard goal's slot
    // demotes that slot to soft for truncation purposes.
    let mut accum: HashMap<(String, i32), (Movement, GoalPriority)> = HashMap::new();
    let mut goals_applied: Vec<String> = Vec::new();
    let mut hard_overflow: Option<(String, usize)> = None;

    for (_idx, g) in &ordered {
        goals_applied.push(g.name().to_string());
        let movements = g.propose(&working, ctx);
        let prio = g.priority();
        for m in movements {
            if validate_movement(&working, &m).is_err() {
                // Silently drop — the goal will see the unchanged state next iter.
                continue;
            }
            // Apply to working state immediately.
            apply_movement(&mut working, &m);
            let key = (m.topic.clone(), m.partition);
            accum.insert(key, (m, prio));
        }
        if accum.len() > ctx.max_movements_per_proposal && matches!(prio, GoalPriority::Hard) {
            let extra = accum.len() - ctx.max_movements_per_proposal;
            hard_overflow = Some((g.name().to_string(), extra));
        }
    }

    if let Some((goal, extra)) = hard_overflow {
        return Err(OptimizeError::HardGoalUnsatisfied {
            goal,
            extra,
            cap: ctx.max_movements_per_proposal,
        });
    }

    // 3. Partition accumulated movements by priority so truncation can
    //    drop soft-goal movements first (spec invariant).
    let cap = ctx.max_movements_per_proposal;
    let (mut hard_mvs, mut soft_mvs): (Vec<Movement>, Vec<Movement>) =
        accum
            .into_values()
            .fold((Vec::new(), Vec::new()), |(mut h, mut s), (m, p)| {
                match p {
                    GoalPriority::Hard => h.push(m),
                    GoalPriority::Soft => s.push(m),
                }
                (h, s)
            });

    // Belt-and-suspenders: the per-iteration check above should have
    // already caught this, but reassert here in case future
    // refactorings change accumulation order.
    if hard_mvs.len() > cap {
        let extra = hard_mvs.len() - cap;
        return Err(OptimizeError::HardGoalUnsatisfied {
            goal: "<post-loop>".to_string(),
            extra,
            cap,
        });
    }

    // 4. Keep all hard movements + as many soft as fit under the cap.
    let soft_room = cap - hard_mvs.len();
    // Sort soft deterministically before slicing so truncation is stable.
    soft_mvs.sort_by(|a, b| (a.topic.as_str(), a.partition).cmp(&(b.topic.as_str(), b.partition)));
    soft_mvs.truncate(soft_room);

    hard_mvs.append(&mut soft_mvs);
    let mut movements = hard_mvs;
    // 5. Final deterministic order: by (topic, partition).
    movements.sort_by(|a, b| (a.topic.as_str(), a.partition).cmp(&(b.topic.as_str(), b.partition)));

    // 6. Compute summary.
    let summary = compute_summary(state, &working, &movements);

    Ok(OptimizeOutput {
        proposal: Proposal {
            id: Uuid::new_v4().to_string(),
            status: ProposalStatus::Computed,
            created_at_ms: now_ms(),
            goals_applied,
            summary,
            movements,
            started_at_ms: 0,
            terminated_at_ms: 0,
            failure_reason: None,
            throttle_bytes_per_sec: 0,
        },
        state_after: working,
    })
}

fn apply_movement(state: &mut ClusterState, m: &Movement) {
    if let Some(p) = state
        .partitions
        .iter_mut()
        .find(|p| p.topic == m.topic && p.partition == m.partition)
    {
        p.replicas.clone_from(&m.new_replicas);
        p.leader = m.new_leader;
        // ISR: drop replicas that left the set; otherwise leave.
        p.isr.retain(|r| p.replicas.contains(r));
        // If the new leader isn't in ISR, add it (we assume the
        // executor has caught up the replica; slice 43b's executor
        // will gate on real ISR catch-up).
        if !p.isr.contains(&p.leader) {
            p.isr.push(p.leader);
        }
    }
}

fn compute_summary(
    before: &ClusterState,
    after: &ClusterState,
    movements: &[Movement],
) -> ProposalSummary {
    let replica_movements = i32::try_from(
        movements
            .iter()
            .filter(|m| m.old_replicas != m.new_replicas)
            .count(),
    )
    .unwrap_or(i32::MAX);
    let leader_movements = i32::try_from(
        movements
            .iter()
            .filter(|m| m.old_leader != m.new_leader)
            .count(),
    )
    .unwrap_or(i32::MAX);

    ProposalSummary {
        replica_movements,
        leader_movements,
        max_replicas_before: i32::try_from(max_replicas_per_broker(&before.partitions))
            .unwrap_or(i32::MAX),
        max_replicas_after: i32::try_from(max_replicas_per_broker(&after.partitions))
            .unwrap_or(i32::MAX),
        max_leaders_before: i32::try_from(max_leaders_per_broker(&before.partitions))
            .unwrap_or(i32::MAX),
        max_leaders_after: i32::try_from(max_leaders_per_broker(&after.partitions))
            .unwrap_or(i32::MAX),
    }
}

fn max_replicas_per_broker(parts: &[PartitionView]) -> usize {
    let mut counts: HashMap<i32, usize> = HashMap::new();
    for p in parts {
        for b in &p.replicas {
            *counts.entry(*b).or_insert(0) += 1;
        }
    }
    counts.values().copied().max().unwrap_or(0)
}

fn max_leaders_per_broker(parts: &[PartitionView]) -> usize {
    let mut counts: HashMap<i32, usize> = HashMap::new();
    for p in parts {
        *counts.entry(p.leader).or_insert(0) += 1;
    }
    counts.values().copied().max().unwrap_or(0)
}

fn now_ms() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_or(0, |d| i64::try_from(d.as_millis()).unwrap_or(i64::MAX))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::goals::tests::FixedGoal;
    use crate::model::{BrokerView, PartitionView};

    fn ctx() -> GoalContext {
        GoalContext {
            imbalance_threshold_pct: 10,
            max_movements_per_proposal: 256,
        }
    }

    fn state() -> ClusterState {
        ClusterState {
            cluster_id: None,
            snapshot_at_ms: 0,
            brokers: vec![
                BrokerView {
                    id: 1,
                    host: "h1".into(),
                    port: 9092,
                    rack: None,
                },
                BrokerView {
                    id: 2,
                    host: "h2".into(),
                    port: 9092,
                    rack: None,
                },
            ],
            partitions: vec![PartitionView {
                topic: "t".into(),
                partition: 0,
                replicas: vec![1, 2],
                leader: 2,
                isr: vec![1, 2],
            }],
            in_flight_reassignments: vec![],
        }
    }

    fn mv() -> Movement {
        Movement {
            topic: "t".into(),
            partition: 0,
            old_replicas: vec![1, 2],
            new_replicas: vec![1, 2],
            old_leader: 2,
            new_leader: 1,
        }
    }

    #[test]
    fn hard_runs_before_soft() {
        let soft = FixedGoal {
            name: "soft",
            priority: GoalPriority::Soft,
            movements: vec![mv()],
        };
        let hard = FixedGoal {
            name: "hard",
            priority: GoalPriority::Hard,
            movements: vec![],
        };
        // Soft first in `goals` list — but optimizer must call hard first.
        let goals: Vec<&dyn Goal> = vec![&soft, &hard];
        let out = optimize(&state(), &goals, &ctx()).unwrap();
        assert_eq!(out.proposal.goals_applied[0], "hard");
        assert_eq!(out.proposal.goals_applied[1], "soft");
    }

    #[test]
    fn empty_goals_returns_no_movements() {
        let goals: Vec<&dyn Goal> = vec![];
        let out = optimize(&state(), &goals, &ctx()).unwrap();
        assert!(out.proposal.movements.is_empty());
        assert_eq!(out.proposal.status, ProposalStatus::Computed);
    }

    #[test]
    fn duplicate_movements_coalesce_last_writer_wins() {
        let g1 = FixedGoal {
            name: "g1",
            priority: GoalPriority::Soft,
            movements: vec![Movement {
                topic: "t".into(),
                partition: 0,
                old_replicas: vec![1, 2],
                new_replicas: vec![1, 2],
                old_leader: 2,
                new_leader: 1,
            }],
        };
        // g2 emits a movement with the SAME (topic, partition) but a
        // different new_leader — this would normally be rejected by
        // validate_movement against the post-g1 state because new_leader=2
        // and current leader=1 are both in the replica set, so it's valid.
        // After coalesce, g2's wins.
        let g2 = FixedGoal {
            name: "g2",
            priority: GoalPriority::Soft,
            movements: vec![Movement {
                topic: "t".into(),
                partition: 0,
                old_replicas: vec![1, 2],
                new_replicas: vec![1, 2],
                old_leader: 1,
                new_leader: 2,
            }],
        };
        let goals: Vec<&dyn Goal> = vec![&g1, &g2];
        let out = optimize(&state(), &goals, &ctx()).unwrap();
        assert_eq!(out.proposal.movements.len(), 1);
        assert_eq!(out.proposal.movements[0].new_leader, 2);
    }

    #[test]
    fn invalid_movement_silently_dropped() {
        let bad = FixedGoal {
            name: "bad",
            priority: GoalPriority::Soft,
            movements: vec![Movement {
                topic: "ghost".into(),
                partition: 0,
                old_replicas: vec![1, 2],
                new_replicas: vec![1, 2],
                old_leader: 1,
                new_leader: 1,
            }],
        };
        let goals: Vec<&dyn Goal> = vec![&bad];
        let out = optimize(&state(), &goals, &ctx()).unwrap();
        assert!(out.proposal.movements.is_empty());
    }

    #[test]
    fn truncation_protects_hard_goal_movements() {
        // Regression test for slice 43a review finding: prior to the
        // priority-aware truncation, sort-then-truncate could drop
        // hard-goal movements whose (topic, partition) sorted later
        // than soft-goal movements.
        //
        // Scenario: cap = 3, 2 hard movements with keys ("z", 0..1),
        // 3 soft movements with keys ("a", 0..2). Pre-fix output kept
        // ("a", 0..2) and dropped the hard movements. Post-fix output
        // keeps both ("z", _) movements + exactly one ("a", _).
        let brokers = vec![
            BrokerView {
                id: 1,
                host: "h1".into(),
                port: 9092,
                rack: None,
            },
            BrokerView {
                id: 2,
                host: "h2".into(),
                port: 9092,
                rack: None,
            },
        ];
        let mut partitions = Vec::new();
        for p in 0..3 {
            partitions.push(PartitionView {
                topic: "a".into(),
                partition: p,
                replicas: vec![1, 2],
                leader: 2,
                isr: vec![1, 2],
            });
        }
        for p in 0..2 {
            partitions.push(PartitionView {
                topic: "z".into(),
                partition: p,
                replicas: vec![1, 2],
                leader: 2,
                isr: vec![1, 2],
            });
        }
        let s = ClusterState {
            cluster_id: None,
            snapshot_at_ms: 0,
            brokers,
            partitions,
            in_flight_reassignments: vec![],
        };

        let hard_movements = (0..2)
            .map(|p| Movement {
                topic: "z".into(),
                partition: p,
                old_replicas: vec![1, 2],
                new_replicas: vec![1, 2],
                old_leader: 2,
                new_leader: 1,
            })
            .collect();
        let soft_movements = (0..3)
            .map(|p| Movement {
                topic: "a".into(),
                partition: p,
                old_replicas: vec![1, 2],
                new_replicas: vec![1, 2],
                old_leader: 2,
                new_leader: 1,
            })
            .collect();

        let hard = FixedGoal {
            name: "hard",
            priority: GoalPriority::Hard,
            movements: hard_movements,
        };
        let soft = FixedGoal {
            name: "soft",
            priority: GoalPriority::Soft,
            movements: soft_movements,
        };
        let ctx = GoalContext {
            imbalance_threshold_pct: 10,
            max_movements_per_proposal: 3,
        };
        let goals: Vec<&dyn Goal> = vec![&hard, &soft];
        let out = optimize(&s, &goals, &ctx).unwrap();

        assert_eq!(out.proposal.movements.len(), 3);
        let z_count = out
            .proposal
            .movements
            .iter()
            .filter(|m| m.topic == "z")
            .count();
        let a_count = out
            .proposal
            .movements
            .iter()
            .filter(|m| m.topic == "a")
            .count();
        // Both hard movements must survive.
        assert_eq!(z_count, 2, "both hard ('z', _) movements must be kept");
        // Exactly one soft movement fits in the remaining slot.
        assert_eq!(a_count, 1, "exactly one soft ('a', _) movement should fit");
    }

    #[test]
    fn hard_goal_overflow_returns_error() {
        let mut movements = Vec::new();
        // 5 valid leader-flip movements would fit, but cap is 3.
        for i in 0..5 {
            movements.push(Movement {
                topic: "t".into(),
                partition: i,
                old_replicas: vec![1, 2],
                new_replicas: vec![1, 2],
                old_leader: 2,
                new_leader: 1,
            });
        }
        let mut s = state();
        // Multi-partition state.
        s.partitions = (0..5)
            .map(|i| PartitionView {
                topic: "t".into(),
                partition: i,
                replicas: vec![1, 2],
                leader: 2,
                isr: vec![1, 2],
            })
            .collect();
        let bulk = FixedGoal {
            name: "bulk",
            priority: GoalPriority::Hard,
            movements,
        };
        let ctx = GoalContext {
            imbalance_threshold_pct: 10,
            max_movements_per_proposal: 3,
        };
        let goals: Vec<&dyn Goal> = vec![&bulk];
        let err = optimize(&s, &goals, &ctx).unwrap_err();
        assert!(matches!(
            err,
            OptimizeError::HardGoalUnsatisfied {
                extra: 2,
                cap: 3,
                ..
            }
        ));
    }
}
