//! Executes a scenario's fault timeline against a live cluster.
//!
//! [`run_schedule`] expands the scenario's [`FaultEvent`]s into a flat,
//! time-sorted list of atomic actions — apply/heal pairs for timed faults,
//! kill/restart pairs for node crashes, alternating half-cycles for flaps —
//! and executes them sequentially on the calling task, so the `&mut Cluster`
//! borrow is never shared across concurrent timers.
//!
//! Event durations may overlap freely, including on the same endpoint: the
//! executor tracks active faults per individual proxy and per fault kind
//! (partition, latency, throttle). While several overlap, the most recently
//! applied value wins; a heal or clear removes only its own event's
//! contribution and re-applies the most recent still-active value, so a
//! proxy's state is cleared only when the last overlapping fault ends.
//! `all-ranges`/`all-sql` targets are fanned out to concrete proxies before
//! this bookkeeping, so a broad heal never clears a still-active
//! single-endpoint fault (and vice versa). Kill/restart are process-level
//! and bypass the proxy bookkeeping. The returned log records what was
//! actually applied, in application order, for inclusion in the report.

use std::{collections::BTreeMap, fmt, time::Duration};

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
    let nodes = cluster.node_count();
    let mut state = ScheduleState::default();
    let mut applied = Vec::with_capacity(atoms.len());
    for atom in atoms {
        tokio::time::sleep_until(window_start + Duration::from_secs(atom.at_s)).await;
        let step = state.step(atom, ranges, nodes);
        for command in step.commands {
            issue(cluster, command).await;
        }
        match atom.action {
            AtomAction::Kill { node } => cluster
                .kill_node(node)
                .await
                .with_context(|| format!("kill node {node} at t={}s", atom.at_s))?,
            AtomAction::Restart { node } => cluster
                .restart_node(node)
                .await
                .with_context(|| format!("restart node {node} at t={}s", atom.at_s))?,
            AtomAction::Partition { .. }
            | AtomAction::Heal { .. }
            | AtomAction::SetLatency { .. }
            | AtomAction::ClearLatency { .. }
            | AtomAction::SetThrottle { .. }
            | AtomAction::ClearThrottle { .. } => {}
        }
        tracing::info!(at_s = atom.at_s, description = %step.description, "fault applied");
        applied.push(AppliedFault {
            at_s: atom.at_s,
            description: step.description,
        });
    }
    Ok(applied)
}

/// One atomic state change in the expanded fault timeline.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct Atom {
    /// Seconds after the measurement window starts.
    at_s: u64,
    /// Index of the source [`FaultEvent`], so overlapping events release
    /// only their own contribution.
    event: usize,
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
    /// End this event's partition of the target.
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
    /// End this event's delay on the target.
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
    /// End this event's bandwidth cap on the target.
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
    for (event, entry) in events.iter().enumerate() {
        expand_event(event, entry, &mut atoms);
    }
    atoms.sort_by_key(|atom| atom.at_s);
    atoms
}

/// Appends one event's atoms: the applying atom at `at_s` and, for timed
/// faults, the corresponding heal/restore/restart atom when the duration
/// elapses.
fn expand_event(event: usize, entry: &FaultEvent, atoms: &mut Vec<Atom>) {
    let at_s = entry.at_s;
    let mut push = |at_s: u64, action: AtomAction| {
        atoms.push(Atom {
            at_s,
            event,
            action,
        });
    };
    match &entry.action {
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
        } => expand_flap(event, *target, at_s, *period_s, *duration_s, atoms),
    }
}

/// Emits alternating blackhole/heal atoms every `period_s` starting at
/// `at_s`, guaranteed to end healed at or before `at_s + duration_s`. A zero
/// period is clamped to one second; a zero duration emits nothing.
fn expand_flap(
    event: usize,
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
        atoms.push(Atom {
            at_s: t_s,
            event,
            action,
        });
        cut = !cut;
        t_s = t_s.saturating_add(period_s);
    }
    if cut {
        atoms.push(Atom {
            at_s: end_s,
            event,
            action: AtomAction::Heal { target },
        });
    }
}

/// One concrete proxied endpoint, the unit the active-fault bookkeeping
/// tracks (broad targets are fanned out to these before bookkeeping).
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
enum ProxyId {
    /// The RPC endpoint of one range.
    Range(u16),
    /// The SQL front door of one node.
    Sql(u16),
}

impl fmt::Display for ProxyId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Range(range) => write!(f, "range:{range}"),
            Self::Sql(node) => write!(f, "sql:{node}"),
        }
    }
}

/// The three proxy-state fault kinds, tracked independently per proxy.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
enum FaultKind {
    /// Link cut.
    Partition,
    /// One-way delay.
    Latency,
    /// Bandwidth cap.
    Throttle,
}

/// The value one proxy-state fault applies while active.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum FaultValue {
    /// Cut with this style.
    Partition(PartitionStyle),
    /// Delay by `ms` with `jitter_ms` of jitter.
    Latency {
        /// Base one-way delay in milliseconds.
        ms: u64,
        /// Uniform jitter added on top.
        jitter_ms: u64,
    },
    /// Cap at this many bytes per second.
    Throttle {
        /// Cap in bytes per second, per direction.
        bytes_per_sec: u64,
    },
}

/// The kind a value belongs to.
fn kind_of(value: FaultValue) -> FaultKind {
    match value {
        FaultValue::Partition(_) => FaultKind::Partition,
        FaultValue::Latency { .. } => FaultKind::Latency,
        FaultValue::Throttle { .. } => FaultKind::Throttle,
    }
}

/// The fault-log word for a kind.
fn kind_word(kind: FaultKind) -> &'static str {
    match kind {
        FaultKind::Partition => "partition",
        FaultKind::Latency => "latency",
        FaultKind::Throttle => "throttle",
    }
}

/// One reconfiguration command for a concrete proxy; a `None` payload clears
/// the corresponding state.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ProxyCommand {
    /// Set or clear the partition state.
    Partition(ProxyId, Option<PartitionStyle>),
    /// Set or clear the latency state.
    Latency(ProxyId, Option<LatencySpec>),
    /// Set or clear the throttle state.
    Throttle(ProxyId, Option<u64>),
}

/// The command applying `value` to `proxy`.
fn set_command(proxy: ProxyId, value: FaultValue) -> ProxyCommand {
    match value {
        FaultValue::Partition(style) => ProxyCommand::Partition(proxy, Some(style)),
        FaultValue::Latency { ms, jitter_ms } => {
            ProxyCommand::Latency(proxy, Some(LatencySpec { ms, jitter_ms }))
        }
        FaultValue::Throttle { bytes_per_sec } => {
            ProxyCommand::Throttle(proxy, Some(bytes_per_sec))
        }
    }
}

/// The command clearing `kind` on `proxy`.
fn clear_command(kind: FaultKind, proxy: ProxyId) -> ProxyCommand {
    match kind {
        FaultKind::Partition => ProxyCommand::Partition(proxy, None),
        FaultKind::Latency => ProxyCommand::Latency(proxy, None),
        FaultKind::Throttle => ProxyCommand::Throttle(proxy, None),
    }
}

/// The concrete proxies a target fans out to.
fn fan_out(target: FaultTarget, ranges: u16, nodes: u16) -> Vec<ProxyId> {
    match target {
        FaultTarget::Range(range) => vec![ProxyId::Range(range)],
        FaultTarget::AllRanges => (0..ranges).map(ProxyId::Range).collect(),
        FaultTarget::Sql(node) => vec![ProxyId::Sql(node)],
        FaultTarget::AllSql => (0..nodes).map(ProxyId::Sql).collect(),
    }
}

/// One active fault: which event applied it, when, and its value.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct ActiveEntry {
    event: usize,
    at_s: u64,
    value: FaultValue,
}

/// Result of resolving one atom against the bookkeeping: the proxy commands
/// to issue (in fan-out order) and the fault-log description.
#[derive(Debug, Clone, PartialEq, Eq)]
struct Step {
    commands: Vec<ProxyCommand>,
    description: String,
}

/// Pure executor bookkeeping: the active overlapping faults per
/// (proxy, kind), in application order, so releasing one event's fault
/// re-applies the most recent still-active value instead of clearing state
/// another event still owns. Drivable without a live cluster: feed it
/// expanded atoms, collect the emitted [`ProxyCommand`]s.
#[derive(Debug, Default)]
struct ScheduleState {
    active: BTreeMap<(ProxyId, FaultKind), Vec<ActiveEntry>>,
}

impl ScheduleState {
    /// Resolves one atom into proxy commands and its log description.
    /// Kill/restart atoms are process-level: no commands, base description.
    fn step(&mut self, atom: Atom, ranges: u16, nodes: u16) -> Step {
        let base = describe(atom.action);
        match atom.action {
            AtomAction::Partition { target, style } => self.apply_step(
                atom,
                target,
                FaultValue::Partition(style),
                ranges,
                nodes,
                base,
            ),
            AtomAction::SetLatency {
                target,
                ms,
                jitter_ms,
            } => self.apply_step(
                atom,
                target,
                FaultValue::Latency { ms, jitter_ms },
                ranges,
                nodes,
                base,
            ),
            AtomAction::SetThrottle {
                target,
                bytes_per_sec,
            } => self.apply_step(
                atom,
                target,
                FaultValue::Throttle { bytes_per_sec },
                ranges,
                nodes,
                base,
            ),
            AtomAction::Heal { target } => self.release_step(
                atom.event,
                target,
                FaultKind::Partition,
                ranges,
                nodes,
                base,
            ),
            AtomAction::ClearLatency { target } => {
                self.release_step(atom.event, target, FaultKind::Latency, ranges, nodes, base)
            }
            AtomAction::ClearThrottle { target } => {
                self.release_step(atom.event, target, FaultKind::Throttle, ranges, nodes, base)
            }
            AtomAction::Kill { .. } | AtomAction::Restart { .. } => Step {
                commands: Vec::new(),
                description: base,
            },
        }
    }

    /// Records `value` as active for the atom's event on every fanned-out
    /// proxy and issues it (last-applied-wins while overlapping).
    fn apply_step(
        &mut self,
        atom: Atom,
        target: FaultTarget,
        value: FaultValue,
        ranges: u16,
        nodes: u16,
        base: String,
    ) -> Step {
        let commands = fan_out(target, ranges, nodes)
            .into_iter()
            .map(|proxy| {
                self.insert(atom.event, atom.at_s, proxy, value);
                set_command(proxy, value)
            })
            .collect();
        Step {
            commands,
            description: base,
        }
    }

    /// Removes the event's entry on every fanned-out proxy: re-applies the
    /// most recent still-active value where one remains, clears the proxy
    /// state where none does, and names the survivors in the description.
    fn release_step(
        &mut self,
        event: usize,
        target: FaultTarget,
        kind: FaultKind,
        ranges: u16,
        nodes: u16,
        base: String,
    ) -> Step {
        let proxies = fan_out(target, ranges, nodes);
        let fanned = proxies.len();
        let mut commands = Vec::with_capacity(fanned);
        let mut remnants = Vec::new();
        for proxy in proxies {
            match self.release(event, proxy, kind) {
                Some(entry) => {
                    commands.push(set_command(proxy, entry.value));
                    remnants.push((proxy, entry.at_s));
                }
                None => commands.push(clear_command(kind, proxy)),
            }
        }
        let mut description = base;
        description.push_str(&describe_remnants(kind, &remnants, fanned));
        Step {
            commands,
            description,
        }
    }

    /// Marks `value` active for `event` on `proxy`, replacing the event's
    /// previous entry (as a flap's next cycle does).
    fn insert(&mut self, event: usize, at_s: u64, proxy: ProxyId, value: FaultValue) {
        let entries = self.active.entry((proxy, kind_of(value))).or_default();
        entries.retain(|entry| entry.event != event);
        entries.push(ActiveEntry { event, at_s, value });
    }

    /// Removes `event`'s entry for `(proxy, kind)` and returns the most
    /// recently applied entry still active, if any.
    fn release(&mut self, event: usize, proxy: ProxyId, kind: FaultKind) -> Option<ActiveEntry> {
        let key = (proxy, kind);
        let entries = self.active.get_mut(&key)?;
        entries.retain(|entry| entry.event != event);
        let last = entries.last().copied();
        if last.is_none() {
            self.active.remove(&key);
        }
        last
    }
}

/// Issues one command to the corresponding live proxy.
async fn issue(cluster: &Cluster, command: ProxyCommand) {
    match command {
        ProxyCommand::Partition(proxy, style) => {
            proxy_ref(cluster, proxy).set_partitioned(style).await;
        }
        ProxyCommand::Latency(proxy, latency) => {
            proxy_ref(cluster, proxy).set_latency(latency);
        }
        ProxyCommand::Throttle(proxy, limit) => {
            proxy_ref(cluster, proxy).set_throttle_bytes_per_sec(limit);
        }
    }
}

/// The live proxy behind a concrete proxy id.
fn proxy_ref(cluster: &Cluster, proxy: ProxyId) -> &ChaosProxy {
    match proxy {
        ProxyId::Range(range) => cluster.range_proxy(range),
        ProxyId::Sql(node) => cluster.sql_proxy(node),
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

/// Description suffix for a heal/clear that left other events' faults
/// active: empty when everything cleared, otherwise the surviving faults
/// and when they were applied.
fn describe_remnants(kind: FaultKind, remnants: &[(ProxyId, u64)], fanned: usize) -> String {
    match remnants {
        [] => String::new(),
        [(_, at_s)] if fanned == 1 => {
            format!(" ({} from t={at_s}s still active)", kind_word(kind))
        }
        _ => {
            let survivors: Vec<String> = remnants
                .iter()
                .map(|(proxy, at_s)| format!("{proxy} from t={at_s}s"))
                .collect();
            format!(
                " ({} still active on {})",
                kind_word(kind),
                survivors.join(", ")
            )
        }
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

    /// Expands `events` and drives every atom through a fresh
    /// [`ScheduleState`], returning what the executor would do at each
    /// step: `(at_s, commands, description)`.
    fn drive(
        events: &[FaultEvent],
        ranges: u16,
        nodes: u16,
    ) -> Vec<(u64, Vec<ProxyCommand>, String)> {
        let mut state = ScheduleState::default();
        expand(events)
            .into_iter()
            .map(|atom| {
                let step = state.step(atom, ranges, nodes);
                (atom.at_s, step.commands, step.description)
            })
            .collect()
    }

    #[test]
    fn expansion_pairs_each_timed_fault_with_its_heal() {
        let range0 = FaultTarget::Range(0);
        let atom = |at_s: u64, action: AtomAction| Atom {
            at_s,
            event: 0,
            action,
        };
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
                    atom(
                        20,
                        AtomAction::Partition {
                            target: range0,
                            style: PartitionStyle::Reset,
                        },
                    ),
                    atom(35, AtomAction::Heal { target: range0 }),
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
                    atom(
                        5,
                        AtomAction::SetLatency {
                            target: FaultTarget::AllRanges,
                            ms: 100,
                            jitter_ms: 20,
                        },
                    ),
                    atom(
                        15,
                        AtomAction::ClearLatency {
                            target: FaultTarget::AllRanges,
                        },
                    ),
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
                    atom(
                        40,
                        AtomAction::SetThrottle {
                            target: FaultTarget::Sql(1),
                            bytes_per_sec: 65536,
                        },
                    ),
                    atom(
                        45,
                        AtomAction::ClearThrottle {
                            target: FaultTarget::Sql(1),
                        },
                    ),
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
                vec![atom(10, AtomAction::Kill { node: 2 })],
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
                    atom(10, AtomAction::Kill { node: 2 }),
                    atom(20, AtomAction::Restart { node: 2 }),
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
            event: 0,
            action: AtomAction::Partition {
                target,
                style: PartitionStyle::Blackhole,
            },
        };
        let heal = |at_s: u64| Atom {
            at_s,
            event: 0,
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
                event: 0,
                action: AtomAction::Partition {
                    target: range0,
                    style: PartitionStyle::Blackhole,
                },
            },
            Atom {
                at_s: 15,
                event: 1,
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
                event: 1,
                action: AtomAction::ClearLatency {
                    target: FaultTarget::AllSql,
                },
            },
            Atom {
                at_s: 20,
                event: 2,
                action: AtomAction::Kill { node: 1 },
            },
            Atom {
                at_s: 30,
                event: 0,
                action: AtomAction::Heal { target: range0 },
            },
        ];
        assert!(expand(&events) == expected);
    }

    #[test]
    fn overlapping_partitions_keep_the_link_cut_until_the_last_heal() {
        // The review example: partitions at t=0 (10s) and t=5 (10s) on
        // range:0 — the t=10 heal must not clear the t=5 partition; the
        // link clears only at t=15.
        let range0 = FaultTarget::Range(0);
        let partition = |at_s: u64| {
            event(
                at_s,
                FaultAction::Partition {
                    target: range0,
                    duration_s: 10,
                    style: PartitionStyle::Blackhole,
                },
            )
        };
        let cut = ProxyCommand::Partition(ProxyId::Range(0), Some(PartitionStyle::Blackhole));
        let expected = vec![
            (0, vec![cut], "partition range:0 blackhole".to_owned()),
            (5, vec![cut], "partition range:0 blackhole".to_owned()),
            (
                10,
                vec![cut],
                "heal range:0 (partition from t=5s still active)".to_owned(),
            ),
            (
                15,
                vec![ProxyCommand::Partition(ProxyId::Range(0), None)],
                "heal range:0".to_owned(),
            ),
        ];
        assert!(drive(&[partition(0), partition(5)], 1, 1) == expected);
    }

    #[test]
    fn overlapping_latency_restores_the_underlying_value_then_clears() {
        let range0 = FaultTarget::Range(0);
        let events = vec![
            event(
                0,
                FaultAction::Latency {
                    target: range0,
                    ms: 100,
                    jitter_ms: 0,
                    duration_s: 20,
                },
            ),
            event(
                5,
                FaultAction::Latency {
                    target: range0,
                    ms: 200,
                    jitter_ms: 50,
                    duration_s: 5,
                },
            ),
        ];
        let latency = |ms: u64, jitter_ms: u64| {
            ProxyCommand::Latency(ProxyId::Range(0), Some(LatencySpec { ms, jitter_ms }))
        };
        let expected = vec![
            (
                0,
                vec![latency(100, 0)],
                "latency range:0 100ms±0ms".to_owned(),
            ),
            (
                5,
                vec![latency(200, 50)],
                "latency range:0 200ms±50ms".to_owned(),
            ),
            // The second event's clear restores the first event's value.
            (
                10,
                vec![latency(100, 0)],
                "clear latency range:0 (latency from t=0s still active)".to_owned(),
            ),
            // The last clear removes the state.
            (
                20,
                vec![ProxyCommand::Latency(ProxyId::Range(0), None)],
                "clear latency range:0".to_owned(),
            ),
        ];
        assert!(drive(&events, 1, 1) == expected);
    }

    #[test]
    fn healing_all_ranges_keeps_a_single_range_partition_cut() {
        let events = vec![
            event(
                0,
                FaultAction::Partition {
                    target: FaultTarget::Range(0),
                    duration_s: 20,
                    style: PartitionStyle::Blackhole,
                },
            ),
            event(
                5,
                FaultAction::Partition {
                    target: FaultTarget::AllRanges,
                    duration_s: 5,
                    style: PartitionStyle::Blackhole,
                },
            ),
        ];
        let cut = |range: u16| {
            ProxyCommand::Partition(ProxyId::Range(range), Some(PartitionStyle::Blackhole))
        };
        let clear = |range: u16| ProxyCommand::Partition(ProxyId::Range(range), None);
        let expected = vec![
            (0, vec![cut(0)], "partition range:0 blackhole".to_owned()),
            (
                5,
                vec![cut(0), cut(1)],
                "partition all-ranges blackhole".to_owned(),
            ),
            // The all-ranges heal clears range 1 but re-applies range 0's
            // still-active single-range partition.
            (
                10,
                vec![cut(0), clear(1)],
                "heal all-ranges (partition still active on range:0 from t=0s)".to_owned(),
            ),
            (20, vec![clear(0)], "heal range:0".to_owned()),
        ];
        assert!(drive(&events, 2, 1) == expected);
    }

    #[test]
    fn flap_heals_never_clear_a_standing_partition_on_the_same_target() {
        let range1 = FaultTarget::Range(1);
        let events = vec![
            event(
                0,
                FaultAction::Partition {
                    target: range1,
                    duration_s: 12,
                    style: PartitionStyle::Blackhole,
                },
            ),
            event(
                2,
                FaultAction::Flap {
                    target: range1,
                    period_s: 2,
                    duration_s: 6,
                },
            ),
        ];
        let cut = ProxyCommand::Partition(ProxyId::Range(1), Some(PartitionStyle::Blackhole));
        let survived = "heal range:1 (partition from t=0s still active)".to_owned();
        let expected = vec![
            (0, vec![cut], "partition range:1 blackhole".to_owned()),
            (2, vec![cut], "partition range:1 blackhole".to_owned()),
            // The flap's own heals release only the flap's entry; the
            // standing partition stays applied.
            (4, vec![cut], survived.clone()),
            (6, vec![cut], "partition range:1 blackhole".to_owned()),
            (8, vec![cut], survived),
            // Only the standing partition's own heal clears the proxy.
            (
                12,
                vec![ProxyCommand::Partition(ProxyId::Range(1), None)],
                "heal range:1".to_owned(),
            ),
        ];
        assert!(drive(&events, 2, 1) == expected);
    }

    #[test]
    fn kill_and_restart_atoms_issue_no_proxy_commands() {
        let events = vec![event(
            3,
            FaultAction::KillNode {
                node: 1,
                restart_after_s: Some(4),
            },
        )];
        let expected = vec![
            (3, Vec::new(), "kill node1".to_owned()),
            (7, Vec::new(), "restart node1".to_owned()),
        ];
        assert!(drive(&events, 2, 2) == expected);
    }

    #[test]
    fn fan_out_resolves_broad_targets_to_concrete_proxies() {
        let cases = [
            (FaultTarget::Range(2), vec![ProxyId::Range(2)]),
            (
                FaultTarget::AllRanges,
                vec![ProxyId::Range(0), ProxyId::Range(1), ProxyId::Range(2)],
            ),
            (FaultTarget::Sql(1), vec![ProxyId::Sql(1)]),
            (FaultTarget::AllSql, vec![ProxyId::Sql(0), ProxyId::Sql(1)]),
        ];
        for (target, expected) in cases {
            assert!(fan_out(target, 3, 2) == expected, "target {target}");
        }
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
