//! Executes a scenario's fault timeline against a live cluster.
//!
//! [`run_schedule`] expands the scenario's [`FaultEvent`]s into a flat,
//! time-sorted list of atomic actions — apply/heal pairs for timed faults,
//! kill/restart pairs for node crashes, alternating half-cycles for flaps —
//! and executes them sequentially on the calling task, so the `&mut Cluster`
//! borrow is never shared across concurrent timers. Event durations may
//! overlap freely: each event contributes independent atoms and the stable
//! time sort interleaves them. The returned log records what was actually
//! applied, in application order, for inclusion in the report.

use std::time::Duration;

use anyhow::Context as _;
use tokio::time::Instant;

use crate::{
    cluster::Cluster,
    proxy::{ChaosProxy, LatencySpec},
    report::AppliedFault,
    scenario::{FaultAction, FaultEvent, FaultTarget, PartitionStyle},
};

/// Applies `events` to the cluster, anchored at `window_start` (the start of
/// the measurement window). `ranges` is the topology's range count, used to
/// fan [`FaultTarget::AllRanges`] out to every range proxy (the cluster does
/// not expose its range count). Returns once every event has been applied
/// and every timed heal/restart has completed.
///
/// Atoms execute sequentially on the calling task: a slow atom — most
/// plausibly a node restart replaying WAL — delays every subsequent atom
/// past its scheduled offset.
///
/// # Errors
///
/// Returns an error if a kill or restart fails at the process level,
/// abandoning the rest of the schedule; proxy reconfiguration is infallible.
pub async fn run_schedule(
    events: &[FaultEvent],
    ranges: u16,
    cluster: &mut Cluster,
    window_start: Instant,
) -> anyhow::Result<Vec<AppliedFault>> {
    let atoms = expand(events);
    let mut applied = Vec::with_capacity(atoms.len());
    for atom in atoms {
        tokio::time::sleep_until(window_start + Duration::from_secs(atom.at_s)).await;
        apply(atom, ranges, cluster).await?;
        let description = describe(atom.action);
        tracing::info!(at_s = atom.at_s, %description, "fault applied");
        applied.push(AppliedFault {
            at_s: atom.at_s,
            description,
        });
    }
    Ok(applied)
}

/// One atomic state change in the expanded fault timeline.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct Atom {
    /// Seconds after the measurement window starts.
    at_s: u64,
    /// The state change to apply.
    action: AtomAction,
}

/// The concrete state change one atom applies.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum AtomAction {
    /// Cut the target's link.
    Partition {
        /// Whose proxies to cut.
        target: FaultTarget,
        /// How the cut manifests.
        style: PartitionStyle,
    },
    /// Heal the target's link.
    Heal {
        /// Whose proxies to heal.
        target: FaultTarget,
    },
    /// Apply one-way delay to the target's link.
    SetLatency {
        /// Whose proxies to delay.
        target: FaultTarget,
        /// Base one-way delay in milliseconds.
        ms: u64,
        /// Uniform jitter added on top.
        jitter_ms: u64,
    },
    /// Remove the target's delay.
    ClearLatency {
        /// Whose proxies to restore.
        target: FaultTarget,
    },
    /// Cap the target's bandwidth.
    SetThrottle {
        /// Whose proxies to cap.
        target: FaultTarget,
        /// Cap in bytes per second, per direction.
        bytes_per_sec: u64,
    },
    /// Remove the target's bandwidth cap.
    ClearThrottle {
        /// Whose proxies to restore.
        target: FaultTarget,
    },
    /// SIGKILL a node's process.
    Kill {
        /// Node index to kill.
        node: u16,
    },
    /// Restart a killed node.
    Restart {
        /// Node index to restart.
        node: u16,
    },
}

/// Expands events into a flat, time-sorted atom list. The sort is stable, so
/// atoms scheduled for the same second execute in event order.
fn expand(events: &[FaultEvent]) -> Vec<Atom> {
    let mut atoms = Vec::new();
    for event in events {
        expand_event(event, &mut atoms);
    }
    atoms.sort_by_key(|atom| atom.at_s);
    atoms
}

/// Appends one event's atoms: the applying atom at `at_s` and, for timed
/// faults, the corresponding heal/restore/restart atom when the duration
/// elapses.
fn expand_event(event: &FaultEvent, atoms: &mut Vec<Atom>) {
    let at_s = event.at_s;
    let mut push = |at_s: u64, action: AtomAction| atoms.push(Atom { at_s, action });
    match &event.action {
        FaultAction::Partition {
            target,
            duration_s,
            style,
        } => {
            push(
                at_s,
                AtomAction::Partition {
                    target: *target,
                    style: *style,
                },
            );
            push(
                at_s.saturating_add(*duration_s),
                AtomAction::Heal { target: *target },
            );
        }
        FaultAction::Latency {
            target,
            ms,
            jitter_ms,
            duration_s,
        } => {
            push(
                at_s,
                AtomAction::SetLatency {
                    target: *target,
                    ms: *ms,
                    jitter_ms: *jitter_ms,
                },
            );
            push(
                at_s.saturating_add(*duration_s),
                AtomAction::ClearLatency { target: *target },
            );
        }
        FaultAction::Throttle {
            target,
            bytes_per_sec,
            duration_s,
        } => {
            push(
                at_s,
                AtomAction::SetThrottle {
                    target: *target,
                    bytes_per_sec: *bytes_per_sec,
                },
            );
            push(
                at_s.saturating_add(*duration_s),
                AtomAction::ClearThrottle { target: *target },
            );
        }
        FaultAction::KillNode {
            node,
            restart_after_s,
        } => {
            push(at_s, AtomAction::Kill { node: *node });
            if let Some(delay_s) = restart_after_s {
                push(
                    at_s.saturating_add(*delay_s),
                    AtomAction::Restart { node: *node },
                );
            }
        }
        FaultAction::Flap {
            target,
            period_s,
            duration_s,
        } => expand_flap(*target, at_s, *period_s, *duration_s, atoms),
    }
}

/// Emits alternating blackhole/heal atoms every `period_s` starting at
/// `at_s`, guaranteed to end healed at or before `at_s + duration_s`. A zero
/// period is clamped to one second; a zero duration emits nothing.
fn expand_flap(
    target: FaultTarget,
    at_s: u64,
    period_s: u64,
    duration_s: u64,
    atoms: &mut Vec<Atom>,
) {
    let period_s = period_s.max(1);
    let end_s = at_s.saturating_add(duration_s);
    let mut t_s = at_s;
    let mut cut = false;
    while t_s < end_s {
        let action = if cut {
            AtomAction::Heal { target }
        } else {
            AtomAction::Partition {
                target,
                style: PartitionStyle::Blackhole,
            }
        };
        atoms.push(Atom { at_s: t_s, action });
        cut = !cut;
        t_s = t_s.saturating_add(period_s);
    }
    if cut {
        atoms.push(Atom {
            at_s: end_s,
            action: AtomAction::Heal { target },
        });
    }
}

/// Applies one atom to the live cluster.
async fn apply(atom: Atom, ranges: u16, cluster: &mut Cluster) -> anyhow::Result<()> {
    match atom.action {
        AtomAction::Partition { target, style } => {
            for proxy in target_proxies(cluster, ranges, target) {
                proxy.set_partitioned(Some(style)).await;
            }
        }
        AtomAction::Heal { target } => {
            for proxy in target_proxies(cluster, ranges, target) {
                proxy.set_partitioned(None).await;
            }
        }
        AtomAction::SetLatency {
            target,
            ms,
            jitter_ms,
        } => {
            for proxy in target_proxies(cluster, ranges, target) {
                proxy.set_latency(Some(LatencySpec { ms, jitter_ms }));
            }
        }
        AtomAction::ClearLatency { target } => {
            for proxy in target_proxies(cluster, ranges, target) {
                proxy.set_latency(None);
            }
        }
        AtomAction::SetThrottle {
            target,
            bytes_per_sec,
        } => {
            for proxy in target_proxies(cluster, ranges, target) {
                proxy.set_throttle_bytes_per_sec(Some(bytes_per_sec));
            }
        }
        AtomAction::ClearThrottle { target } => {
            for proxy in target_proxies(cluster, ranges, target) {
                proxy.set_throttle_bytes_per_sec(None);
            }
        }
        AtomAction::Kill { node } => cluster
            .kill_node(node)
            .await
            .with_context(|| format!("kill node {node} at t={}s", atom.at_s))?,
        AtomAction::Restart { node } => cluster
            .restart_node(node)
            .await
            .with_context(|| format!("restart node {node} at t={}s", atom.at_s))?,
    }
    Ok(())
}

/// The proxies a target fans out to.
fn target_proxies(cluster: &Cluster, ranges: u16, target: FaultTarget) -> Vec<&ChaosProxy> {
    match target {
        FaultTarget::Range(range) => vec![cluster.range_proxy(range)],
        FaultTarget::AllRanges => (0..ranges)
            .map(|range| cluster.range_proxy(range))
            .collect(),
        FaultTarget::Sql(node) => vec![cluster.sql_proxy(node)],
        FaultTarget::AllSql => (0..cluster.node_count())
            .map(|node| cluster.sql_proxy(node))
            .collect(),
    }
}

/// Compact human description of one atom for the report's fault log.
fn describe(action: AtomAction) -> String {
    match action {
        AtomAction::Partition { target, style } => {
            format!("partition {target} {}", style_name(style))
        }
        AtomAction::Heal { target } => format!("heal {target}"),
        AtomAction::SetLatency {
            target,
            ms,
            jitter_ms,
        } => format!("latency {target} {ms}ms±{jitter_ms}ms"),
        AtomAction::ClearLatency { target } => format!("clear latency {target}"),
        AtomAction::SetThrottle {
            target,
            bytes_per_sec,
        } => format!("throttle {target} {bytes_per_sec}B/s"),
        AtomAction::ClearThrottle { target } => format!("clear throttle {target}"),
        AtomAction::Kill { node } => format!("kill node{node}"),
        AtomAction::Restart { node } => format!("restart node{node}"),
    }
}

/// The kebab-case name of a partition style.
fn style_name(style: PartitionStyle) -> &'static str {
    match style {
        PartitionStyle::Blackhole => "blackhole",
        PartitionStyle::Reset => "reset",
    }
}

#[cfg(test)]
mod tests {
    use assert2::assert;

    use super::*;

    fn event(at_s: u64, action: FaultAction) -> FaultEvent {
        FaultEvent { at_s, action }
    }

    #[test]
    fn expansion_pairs_each_timed_fault_with_its_heal() {
        let range0 = FaultTarget::Range(0);
        let cases: Vec<(&str, Vec<FaultEvent>, Vec<Atom>)> = vec![
            (
                "partition applies then heals",
                vec![event(
                    20,
                    FaultAction::Partition {
                        target: range0,
                        duration_s: 15,
                        style: PartitionStyle::Reset,
                    },
                )],
                vec![
                    Atom {
                        at_s: 20,
                        action: AtomAction::Partition {
                            target: range0,
                            style: PartitionStyle::Reset,
                        },
                    },
                    Atom {
                        at_s: 35,
                        action: AtomAction::Heal { target: range0 },
                    },
                ],
            ),
            (
                "latency clears after its duration",
                vec![event(
                    5,
                    FaultAction::Latency {
                        target: FaultTarget::AllRanges,
                        ms: 100,
                        jitter_ms: 20,
                        duration_s: 10,
                    },
                )],
                vec![
                    Atom {
                        at_s: 5,
                        action: AtomAction::SetLatency {
                            target: FaultTarget::AllRanges,
                            ms: 100,
                            jitter_ms: 20,
                        },
                    },
                    Atom {
                        at_s: 15,
                        action: AtomAction::ClearLatency {
                            target: FaultTarget::AllRanges,
                        },
                    },
                ],
            ),
            (
                "throttle clears after its duration",
                vec![event(
                    40,
                    FaultAction::Throttle {
                        target: FaultTarget::Sql(1),
                        bytes_per_sec: 65536,
                        duration_s: 5,
                    },
                )],
                vec![
                    Atom {
                        at_s: 40,
                        action: AtomAction::SetThrottle {
                            target: FaultTarget::Sql(1),
                            bytes_per_sec: 65536,
                        },
                    },
                    Atom {
                        at_s: 45,
                        action: AtomAction::ClearThrottle {
                            target: FaultTarget::Sql(1),
                        },
                    },
                ],
            ),
            (
                "kill without restart leaves the node down",
                vec![event(
                    10,
                    FaultAction::KillNode {
                        node: 2,
                        restart_after_s: None,
                    },
                )],
                vec![Atom {
                    at_s: 10,
                    action: AtomAction::Kill { node: 2 },
                }],
            ),
            (
                "kill with restart schedules the restart",
                vec![event(
                    10,
                    FaultAction::KillNode {
                        node: 2,
                        restart_after_s: Some(10),
                    },
                )],
                vec![
                    Atom {
                        at_s: 10,
                        action: AtomAction::Kill { node: 2 },
                    },
                    Atom {
                        at_s: 20,
                        action: AtomAction::Restart { node: 2 },
                    },
                ],
            ),
        ];
        for (name, events, expected) in cases {
            assert!(expand(&events) == expected, "{name}");
        }
    }

    #[test]
    fn flap_alternates_blackhole_and_heal_and_ends_healed() {
        let target = FaultTarget::Range(1);
        let partition = |at_s: u64| Atom {
            at_s,
            action: AtomAction::Partition {
                target,
                style: PartitionStyle::Blackhole,
            },
        };
        let heal = |at_s: u64| Atom {
            at_s,
            action: AtomAction::Heal { target },
        };
        let cases: Vec<(&str, u64, u64, u64, Vec<Atom>)> = vec![
            (
                "even half-cycle count ends healed inside the window",
                0,
                2,
                8,
                vec![partition(0), heal(2), partition(4), heal(6)],
            ),
            (
                "odd half-cycle count heals exactly at the window end",
                0,
                2,
                5,
                vec![partition(0), heal(2), partition(4), heal(5)],
            ),
            (
                "period longer than duration degenerates to one partition",
                10,
                60,
                5,
                vec![partition(10), heal(15)],
            ),
            ("zero duration emits nothing", 10, 2, 0, vec![]),
            (
                "zero period clamps to one second",
                0,
                0,
                2,
                vec![partition(0), heal(1)],
            ),
        ];
        for (name, at_s, period_s, duration_s, expected) in cases {
            let events = vec![event(
                at_s,
                FaultAction::Flap {
                    target,
                    period_s,
                    duration_s,
                },
            )];
            assert!(expand(&events) == expected, "{name}");
        }
    }

    #[test]
    fn overlapping_events_interleave_in_time_order() {
        let range0 = FaultTarget::Range(0);
        let events = vec![
            event(
                10,
                FaultAction::Partition {
                    target: range0,
                    duration_s: 20,
                    style: PartitionStyle::Blackhole,
                },
            ),
            event(
                15,
                FaultAction::Latency {
                    target: FaultTarget::AllSql,
                    ms: 50,
                    jitter_ms: 0,
                    duration_s: 5,
                },
            ),
            event(
                20,
                FaultAction::KillNode {
                    node: 1,
                    restart_after_s: None,
                },
            ),
        ];
        let expected = vec![
            Atom {
                at_s: 10,
                action: AtomAction::Partition {
                    target: range0,
                    style: PartitionStyle::Blackhole,
                },
            },
            Atom {
                at_s: 15,
                action: AtomAction::SetLatency {
                    target: FaultTarget::AllSql,
                    ms: 50,
                    jitter_ms: 0,
                },
            },
            // Same second: the stable sort keeps event order (the latency
            // clear comes from an earlier event than the kill).
            Atom {
                at_s: 20,
                action: AtomAction::ClearLatency {
                    target: FaultTarget::AllSql,
                },
            },
            Atom {
                at_s: 20,
                action: AtomAction::Kill { node: 1 },
            },
            Atom {
                at_s: 30,
                action: AtomAction::Heal { target: range0 },
            },
        ];
        assert!(expand(&events) == expected);
    }

    #[test]
    fn descriptions_are_compact_and_name_the_target() {
        let cases = [
            (
                AtomAction::Partition {
                    target: FaultTarget::Range(0),
                    style: PartitionStyle::Blackhole,
                },
                "partition range:0 blackhole",
            ),
            (
                AtomAction::Partition {
                    target: FaultTarget::Range(3),
                    style: PartitionStyle::Reset,
                },
                "partition range:3 reset",
            ),
            (
                AtomAction::Heal {
                    target: FaultTarget::Range(0),
                },
                "heal range:0",
            ),
            (
                AtomAction::SetLatency {
                    target: FaultTarget::AllRanges,
                    ms: 100,
                    jitter_ms: 20,
                },
                "latency all-ranges 100ms±20ms",
            ),
            (
                AtomAction::ClearLatency {
                    target: FaultTarget::AllRanges,
                },
                "clear latency all-ranges",
            ),
            (
                AtomAction::SetThrottle {
                    target: FaultTarget::Sql(1),
                    bytes_per_sec: 65536,
                },
                "throttle sql:1 65536B/s",
            ),
            (
                AtomAction::ClearThrottle {
                    target: FaultTarget::AllSql,
                },
                "clear throttle all-sql",
            ),
            (AtomAction::Kill { node: 2 }, "kill node2"),
            (AtomAction::Restart { node: 2 }, "restart node2"),
        ];
        for (action, expected) in cases {
            assert!(describe(action) == expected, "{action:?}");
        }
    }
}
