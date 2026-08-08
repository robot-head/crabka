//! Executes a scenario's fault timeline against a live cluster.
//!
//! [`run_schedule`] expands the scenario's [`FaultEvent`]s into a flat,
//! time-sorted list of atomic actions. Those actions are apply and heal pairs
//! for timed faults, kill and restart pairs for node crashes, and alternating
//! half-cycles for flaps. It runs them one after another on the calling task,
//! so no two concurrent timers ever share the `&mut Cluster` borrow.
//!
//! Event durations may overlap freely, including on the same endpoint. The
//! executor tracks the active faults for each individual proxy and each fault
//! kind: partition, latency, and throttle. While several overlap, the value
//! applied most recently wins. A heal or a clear removes only its own event's
//! contribution and applies the most recent still-active value again. A
//! proxy's state therefore clears only when the last overlapping fault ends.
//!
//! The executor fans `all-ranges` and `all-sql` targets out to concrete
//! proxies before this bookkeeping. A broad heal therefore never clears a
//! still-active single-endpoint fault, and a single-endpoint heal never clears
//! a broad one. Kill and restart are process-level and skip the proxy
//! bookkeeping. The returned log records what the executor really applied, in
//! application order, for the report.

use std::{cmp::Ordering, collections::BTreeMap, fmt};

use anyhow::Context as _;
use crabka_units::{fmt::Human as _, prelude::*};
use tokio::time::Instant;

use crate::{
    cluster::Cluster,
    config::LoadtestRuntimePolicy,
    proxy::{ChaosProxy, LatencySpec},
    report::AppliedFault,
    scenario::{FaultAction, FaultEvent, FaultTarget, PartitionStyle},
};

/// Applies `events` to the cluster, anchored at `window_start`, which is the
/// start of the measurement window.
///
/// `ranges` is the topology's range count. This function uses it to fan
/// [`FaultTarget::AllRanges`] out to every range proxy, because the cluster
/// does not expose its range count. The function returns once it has applied
/// every event and every timed heal and restart has finished.
///
/// Atoms run one after another on the calling task. A slow atom, most likely a
/// node restart that replays WAL, therefore delays every later atom past its
/// scheduled offset.
///
/// # Errors
///
/// Returns an error if a kill or a restart fails at the process level, and
/// then abandons the rest of the schedule. Proxy reconfiguration cannot
/// fail.
pub async fn run_schedule(
    events: &[FaultEvent],
    ranges: u16,
    cluster: &mut Cluster,
    window_start: Instant,
) -> anyhow::Result<Vec<AppliedFault>> {
    run_schedule_with_policy(
        events,
        ranges,
        cluster,
        window_start,
        LoadtestRuntimePolicy::default(),
    )
    .await
}

/// Runs a fault schedule with explicit harness policy.
///
/// # Errors
/// Returns an error when a process-level kill or restart fails.
pub async fn run_schedule_with_policy(
    events: &[FaultEvent],
    ranges: u16,
    cluster: &mut Cluster,
    window_start: Instant,
    policy: LoadtestRuntimePolicy,
) -> anyhow::Result<Vec<AppliedFault>> {
    let atoms = expand_with_policy(events, policy);
    let nodes = cluster.node_count();
    let mut state = ScheduleState::default();
    let mut applied = Vec::with_capacity(atoms.len());
    for atom in atoms {
        tokio::time::sleep_until(window_start + atom.at.to_std()).await;
        let step = state.step(atom, ranges, nodes);
        for command in step.commands {
            issue(cluster, command).await;
        }
        match atom.action {
            AtomAction::Kill { node } => cluster
                .kill_node(node)
                .await
                .with_context(|| format!("kill node {node} at t={}", atom.at.human()))?,
            AtomAction::Restart { node } => cluster
                .restart_node(node)
                .await
                .with_context(|| format!("restart node {node} at t={}", atom.at.human()))?,
            AtomAction::Partition { .. }
            | AtomAction::Heal { .. }
            | AtomAction::SetLatency { .. }
            | AtomAction::ClearLatency { .. }
            | AtomAction::SetThrottle { .. }
            | AtomAction::ClearThrottle { .. } => {}
        }
        tracing::info!(at = %atom.at.human(), description = %step.description, "fault applied");
        applied.push(AppliedFault {
            at: atom.at,
            description: step.description,
        });
    }
    Ok(applied)
}

/// One atomic state change in the expanded fault timeline.
#[derive(Debug, Clone, Copy, PartialEq)]
struct Atom {
    /// Offset from the start of the measurement window.
    at: Time,
    /// Index of the source [`FaultEvent`], so overlapping events release
    /// only their own contribution.
    event: usize,
    /// The state change to apply.
    action: AtomAction,
}

/// The concrete state change one atom applies.
#[derive(Debug, Clone, Copy, PartialEq)]
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
        /// Base one-way delay.
        delay: Time,
        /// Uniform jitter added on top.
        jitter: Time,
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
        /// Cap per direction.
        rate: ByteRate,
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
/// atoms scheduled for the same second run in event order.
#[cfg(test)]
fn expand(events: &[FaultEvent]) -> Vec<Atom> {
    expand_with_policy(events, LoadtestRuntimePolicy::default())
}

fn expand_with_policy(events: &[FaultEvent], policy: LoadtestRuntimePolicy) -> Vec<Atom> {
    let mut atoms = Vec::new();
    for (event, entry) in events.iter().enumerate() {
        expand_event(event, entry, &mut atoms, policy);
    }
    // `Time` is float-backed, so the ordering comes from a total float
    // comparison; the sort stays stable, keeping same-offset atoms in event
    // order.
    atoms.sort_by(|left, right| total_order(left.at, right.at));
    atoms
}

/// Total ordering of two offsets, for sorting the atom timeline.
fn total_order(left: Time, right: Time) -> Ordering {
    left.secs_f64().total_cmp(&right.secs_f64())
}

/// Appends one event's atoms. Those are the atom that applies the fault at
/// `at` and, for a timed fault, the matching heal, restore, or restart atom
/// for the moment the duration elapses.
fn expand_event(
    event: usize,
    entry: &FaultEvent,
    atoms: &mut Vec<Atom>,
    policy: LoadtestRuntimePolicy,
) {
    let at = entry.at;
    let mut push = |at: Time, action: AtomAction| {
        atoms.push(Atom { at, event, action });
    };
    match &entry.action {
        FaultAction::Partition {
            target,
            duration,
            style,
        } => {
            push(
                at,
                AtomAction::Partition {
                    target: *target,
                    style: *style,
                },
            );
            push(at + *duration, AtomAction::Heal { target: *target });
        }
        FaultAction::Latency {
            target,
            delay,
            jitter,
            duration,
        } => {
            push(
                at,
                AtomAction::SetLatency {
                    target: *target,
                    delay: *delay,
                    jitter: *jitter,
                },
            );
            push(at + *duration, AtomAction::ClearLatency { target: *target });
        }
        FaultAction::Throttle {
            target,
            rate,
            duration,
        } => {
            push(
                at,
                AtomAction::SetThrottle {
                    target: *target,
                    rate: *rate,
                },
            );
            push(
                at + *duration,
                AtomAction::ClearThrottle { target: *target },
            );
        }
        FaultAction::KillNode {
            node,
            restart_after,
        } => {
            push(at, AtomAction::Kill { node: *node });
            if let Some(delay) = restart_after {
                push(at + *delay, AtomAction::Restart { node: *node });
            }
        }
        FaultAction::Flap {
            target,
            period,
            duration,
        } => expand_flap(
            event,
            *target,
            at,
            *period,
            *duration,
            atoms,
            policy.min_flap_period,
        ),
    }
}

/// Emits alternating blackhole and heal atoms every `period`, from `at`
/// onward. The sequence always ends healed at or before `at + duration`. A
/// non-positive period is clamped to the configured minimum, and a zero
/// duration emits nothing.
fn expand_flap(
    event: usize,
    target: FaultTarget,
    at: Time,
    period: Time,
    duration: Time,
    atoms: &mut Vec<Atom>,
    min_period: Time,
) {
    let period = period.max(min_period);
    let end = at + duration;
    let mut t = at;
    let mut cut = false;
    while t < end {
        let action = if cut {
            AtomAction::Heal { target }
        } else {
            AtomAction::Partition {
                target,
                style: PartitionStyle::Blackhole,
            }
        };
        atoms.push(Atom {
            at: t,
            event,
            action,
        });
        cut = !cut;
        t += period;
    }
    if cut {
        atoms.push(Atom {
            at: end,
            event,
            action: AtomAction::Heal { target },
        });
    }
}

/// One concrete proxied endpoint. It is the unit that the active-fault
/// bookkeeping tracks, and broad targets fan out to these before the
/// bookkeeping runs.
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
#[derive(Debug, Clone, Copy, PartialEq)]
enum FaultValue {
    /// Cut with this style.
    Partition(PartitionStyle),
    /// Delay by `delay` with `jitter` of jitter.
    Latency {
        /// Base one-way delay.
        delay: Time,
        /// Uniform jitter added on top.
        jitter: Time,
    },
    /// Cap at this throughput.
    Throttle {
        /// Cap per direction.
        rate: ByteRate,
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

/// One reconfiguration command for a concrete proxy. A `None` payload clears
/// the corresponding state.
#[derive(Debug, Clone, Copy, PartialEq)]
enum ProxyCommand {
    /// Set or clear the partition state.
    Partition(ProxyId, Option<PartitionStyle>),
    /// Set or clear the latency state.
    Latency(ProxyId, Option<LatencySpec>),
    /// Set or clear the throttle state.
    Throttle(ProxyId, Option<ByteRate>),
}

/// The command that applies `value` to `proxy`.
fn set_command(proxy: ProxyId, value: FaultValue) -> ProxyCommand {
    match value {
        FaultValue::Partition(style) => ProxyCommand::Partition(proxy, Some(style)),
        FaultValue::Latency { delay, jitter } => {
            ProxyCommand::Latency(proxy, Some(LatencySpec { delay, jitter }))
        }
        FaultValue::Throttle { rate } => ProxyCommand::Throttle(proxy, Some(rate)),
    }
}

/// The command that clears `kind` on `proxy`.
fn clear_command(kind: FaultKind, proxy: ProxyId) -> ProxyCommand {
    match kind {
        FaultKind::Partition => ProxyCommand::Partition(proxy, None),
        FaultKind::Latency => ProxyCommand::Latency(proxy, None),
        FaultKind::Throttle => ProxyCommand::Throttle(proxy, None),
    }
}

/// The concrete proxies that a target fans out to.
fn fan_out(target: FaultTarget, ranges: u16, nodes: u16) -> Vec<ProxyId> {
    match target {
        FaultTarget::Range(range) => vec![ProxyId::Range(range)],
        FaultTarget::AllRanges => (0..ranges).map(ProxyId::Range).collect(),
        FaultTarget::Sql(node) => vec![ProxyId::Sql(node)],
        FaultTarget::AllSql => (0..nodes).map(ProxyId::Sql).collect(),
    }
}

/// One active fault: which event applied it, when, and its value.
#[derive(Debug, Clone, Copy, PartialEq)]
struct ActiveEntry {
    event: usize,
    at: Time,
    value: FaultValue,
}

/// Result of one atom resolved against the bookkeeping: the proxy commands to
/// issue, in fan-out order, and the fault-log description.
#[derive(Debug, Clone, PartialEq)]
struct Step {
    commands: Vec<ProxyCommand>,
    description: String,
}

/// Pure executor bookkeeping. It holds the active overlapping faults for each
/// (proxy, kind) pair, in application order. A release of one event's fault
/// therefore applies the most recent still-active value again, instead of a
/// clear of state that another event still owns.
///
/// This type runs without a live cluster. Feed it expanded atoms and collect
/// the emitted [`ProxyCommand`]s.
#[derive(Debug, Default)]
struct ScheduleState {
    active: BTreeMap<(ProxyId, FaultKind), Vec<ActiveEntry>>,
}

impl ScheduleState {
    /// Resolves one atom into proxy commands and its log description. Kill
    /// and restart atoms are process-level, so they give no commands and the
    /// base description.
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
                delay,
                jitter,
            } => self.apply_step(
                atom,
                target,
                FaultValue::Latency { delay, jitter },
                ranges,
                nodes,
                base,
            ),
            AtomAction::SetThrottle { target, rate } => self.apply_step(
                atom,
                target,
                FaultValue::Throttle { rate },
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
    /// proxy, then issues it. While faults overlap, the value applied last
    /// wins.
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
                self.insert(atom.event, atom.at, proxy, value);
                set_command(proxy, value)
            })
            .collect();
        Step {
            commands,
            description: base,
        }
    }

    /// Removes the event's entry on every fanned-out proxy. Where a
    /// still-active value remains, this method applies the most recent one
    /// again. Where none remains, it clears the proxy state. It names the
    /// survivors in the description.
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
                    remnants.push((proxy, entry.at));
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

    /// Marks `value` active for `event` on `proxy`. It replaces the event's
    /// previous entry, as the next cycle of a flap does.
    fn insert(&mut self, event: usize, at: Time, proxy: ProxyId, value: FaultValue) {
        let entries = self.active.entry((proxy, kind_of(value))).or_default();
        entries.retain(|entry| entry.event != event);
        entries.push(ActiveEntry { event, at, value });
    }

    /// Removes the entry of `event` for `(proxy, kind)`. It returns the entry
    /// applied most recently that is still active, if there is one.
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
            proxy_ref(cluster, proxy).set_throttle(limit);
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
            delay,
            jitter,
        } => format!("latency {target} {}±{}", delay.human(), jitter.human()),
        AtomAction::ClearLatency { target } => format!("clear latency {target}"),
        AtomAction::SetThrottle { target, rate } => {
            format!("throttle {target} {}", rate.human())
        }
        AtomAction::ClearThrottle { target } => format!("clear throttle {target}"),
        AtomAction::Kill { node } => format!("kill node{node}"),
        AtomAction::Restart { node } => format!("restart node{node}"),
    }
}

/// Description suffix for a heal or clear that left the faults of other events
/// active. It is empty when everything cleared. Otherwise it names the
/// surviving faults and the time when the executor applied them.
fn describe_remnants(kind: FaultKind, remnants: &[(ProxyId, Time)], fanned: usize) -> String {
    match remnants {
        [] => String::new(),
        [(_, at)] if fanned == 1 => {
            format!(" ({} from t={} still active)", kind_word(kind), at.human())
        }
        _ => {
            let survivors: Vec<String> = remnants
                .iter()
                .map(|(proxy, at)| format!("{proxy} from t={}", at.human()))
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

    #[test]
    fn explicit_policy_controls_minimum_flap_period() {
        let events = vec![FaultEvent {
            at: Time::ZERO,
            action: FaultAction::Flap {
                target: FaultTarget::AllRanges,
                period: Time::ZERO,
                duration: secs(1),
            },
        }];
        let policy = LoadtestRuntimePolicy {
            min_flap_period: millis(200),
            ..Default::default()
        };
        let atoms = expand_with_policy(&events, policy);
        assert2::assert!(atoms.iter().any(|atom| atom.at == millis(200)));
    }

    fn event(at: Time, action: FaultAction) -> FaultEvent {
        FaultEvent { at, action }
    }

    /// Expands `events` and drives every atom through a fresh
    /// [`ScheduleState`]. It returns what the executor would do at each step,
    /// as `(at, commands, description)`.
    fn drive(
        events: &[FaultEvent],
        ranges: u16,
        nodes: u16,
    ) -> Vec<(Time, Vec<ProxyCommand>, String)> {
        let mut state = ScheduleState::default();
        expand(events)
            .into_iter()
            .map(|atom| {
                let step = state.step(atom, ranges, nodes);
                (atom.at, step.commands, step.description)
            })
            .collect()
    }

    #[test]
    fn expansion_pairs_each_timed_fault_with_its_heal() {
        let range0 = FaultTarget::Range(0);
        let atom = |at: Time, action: AtomAction| Atom {
            at,
            event: 0,
            action,
        };
        let cases: Vec<(&str, Vec<FaultEvent>, Vec<Atom>)> = vec![
            (
                "partition applies then heals",
                vec![event(
                    secs(20),
                    FaultAction::Partition {
                        target: range0,
                        duration: secs(15),
                        style: PartitionStyle::Reset,
                    },
                )],
                vec![
                    atom(
                        secs(20),
                        AtomAction::Partition {
                            target: range0,
                            style: PartitionStyle::Reset,
                        },
                    ),
                    atom(secs(35), AtomAction::Heal { target: range0 }),
                ],
            ),
            (
                "latency clears after its duration",
                vec![event(
                    secs(5),
                    FaultAction::Latency {
                        target: FaultTarget::AllRanges,
                        delay: millis(100),
                        jitter: millis(20),
                        duration: secs(10),
                    },
                )],
                vec![
                    atom(
                        secs(5),
                        AtomAction::SetLatency {
                            target: FaultTarget::AllRanges,
                            delay: millis(100),
                            jitter: millis(20),
                        },
                    ),
                    atom(
                        secs(15),
                        AtomAction::ClearLatency {
                            target: FaultTarget::AllRanges,
                        },
                    ),
                ],
            ),
            (
                "throttle clears after its duration",
                vec![event(
                    secs(40),
                    FaultAction::Throttle {
                        target: FaultTarget::Sql(1),
                        rate: kibibytes_per_sec(64),
                        duration: secs(5),
                    },
                )],
                vec![
                    atom(
                        secs(40),
                        AtomAction::SetThrottle {
                            target: FaultTarget::Sql(1),
                            rate: kibibytes_per_sec(64),
                        },
                    ),
                    atom(
                        secs(45),
                        AtomAction::ClearThrottle {
                            target: FaultTarget::Sql(1),
                        },
                    ),
                ],
            ),
            (
                "kill without restart leaves the node down",
                vec![event(
                    secs(10),
                    FaultAction::KillNode {
                        node: 2,
                        restart_after: None,
                    },
                )],
                vec![atom(secs(10), AtomAction::Kill { node: 2 })],
            ),
            (
                "kill with restart schedules the restart",
                vec![event(
                    secs(10),
                    FaultAction::KillNode {
                        node: 2,
                        restart_after: Some(secs(10)),
                    },
                )],
                vec![
                    atom(secs(10), AtomAction::Kill { node: 2 }),
                    atom(secs(20), AtomAction::Restart { node: 2 }),
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
        let partition = |at: Time| Atom {
            at,
            event: 0,
            action: AtomAction::Partition {
                target,
                style: PartitionStyle::Blackhole,
            },
        };
        let heal = |at: Time| Atom {
            at,
            event: 0,
            action: AtomAction::Heal { target },
        };
        let cases: Vec<(&str, Time, Time, Time, Vec<Atom>)> = vec![
            (
                "even half-cycle count ends healed inside the window",
                Time::ZERO,
                secs(2),
                secs(8),
                vec![
                    partition(Time::ZERO),
                    heal(secs(2)),
                    partition(secs(4)),
                    heal(secs(6)),
                ],
            ),
            (
                "odd half-cycle count heals exactly at the window end",
                Time::ZERO,
                secs(2),
                secs(5),
                vec![
                    partition(Time::ZERO),
                    heal(secs(2)),
                    partition(secs(4)),
                    heal(secs(5)),
                ],
            ),
            (
                "period longer than duration degenerates to one partition",
                secs(10),
                secs(60),
                secs(5),
                vec![partition(secs(10)), heal(secs(15))],
            ),
            (
                "zero duration emits nothing",
                secs(10),
                secs(2),
                Time::ZERO,
                vec![],
            ),
            (
                "zero period clamps to one second",
                Time::ZERO,
                Time::ZERO,
                secs(2),
                vec![partition(Time::ZERO), heal(secs(1))],
            ),
        ];
        for (name, at, period, duration, expected) in cases {
            let events = vec![event(
                at,
                FaultAction::Flap {
                    target,
                    period,
                    duration,
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
                secs(10),
                FaultAction::Partition {
                    target: range0,
                    duration: secs(20),
                    style: PartitionStyle::Blackhole,
                },
            ),
            event(
                secs(15),
                FaultAction::Latency {
                    target: FaultTarget::AllSql,
                    delay: millis(50),
                    jitter: Time::ZERO,
                    duration: secs(5),
                },
            ),
            event(
                secs(20),
                FaultAction::KillNode {
                    node: 1,
                    restart_after: None,
                },
            ),
        ];
        let expected = vec![
            Atom {
                at: secs(10),
                event: 0,
                action: AtomAction::Partition {
                    target: range0,
                    style: PartitionStyle::Blackhole,
                },
            },
            Atom {
                at: secs(15),
                event: 1,
                action: AtomAction::SetLatency {
                    target: FaultTarget::AllSql,
                    delay: millis(50),
                    jitter: Time::ZERO,
                },
            },
            // Same second: the stable sort keeps event order (the latency
            // clear comes from an earlier event than the kill).
            Atom {
                at: secs(20),
                event: 1,
                action: AtomAction::ClearLatency {
                    target: FaultTarget::AllSql,
                },
            },
            Atom {
                at: secs(20),
                event: 2,
                action: AtomAction::Kill { node: 1 },
            },
            Atom {
                at: secs(30),
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
        let partition = |at: Time| {
            event(
                at,
                FaultAction::Partition {
                    target: range0,
                    duration: secs(10),
                    style: PartitionStyle::Blackhole,
                },
            )
        };
        let cut = ProxyCommand::Partition(ProxyId::Range(0), Some(PartitionStyle::Blackhole));
        let expected = vec![
            (
                Time::ZERO,
                vec![cut],
                "partition range:0 blackhole".to_owned(),
            ),
            (secs(5), vec![cut], "partition range:0 blackhole".to_owned()),
            (
                secs(10),
                vec![cut],
                "heal range:0 (partition from t=5s still active)".to_owned(),
            ),
            (
                secs(15),
                vec![ProxyCommand::Partition(ProxyId::Range(0), None)],
                "heal range:0".to_owned(),
            ),
        ];
        assert!(drive(&[partition(Time::ZERO), partition(secs(5))], 1, 1) == expected);
    }

    #[test]
    fn overlapping_latency_restores_the_underlying_value_then_clears() {
        let range0 = FaultTarget::Range(0);
        let events = vec![
            event(
                Time::ZERO,
                FaultAction::Latency {
                    target: range0,
                    delay: millis(100),
                    jitter: Time::ZERO,
                    duration: secs(20),
                },
            ),
            event(
                secs(5),
                FaultAction::Latency {
                    target: range0,
                    delay: millis(200),
                    jitter: millis(50),
                    duration: secs(5),
                },
            ),
        ];
        let latency = |delay: Time, jitter: Time| {
            ProxyCommand::Latency(ProxyId::Range(0), Some(LatencySpec { delay, jitter }))
        };
        let expected = vec![
            (
                Time::ZERO,
                vec![latency(millis(100), Time::ZERO)],
                "latency range:0 100ms±0s".to_owned(),
            ),
            (
                secs(5),
                vec![latency(millis(200), millis(50))],
                "latency range:0 200ms±50ms".to_owned(),
            ),
            // The second event's clear restores the first event's value.
            (
                secs(10),
                vec![latency(millis(100), Time::ZERO)],
                "clear latency range:0 (latency from t=0s still active)".to_owned(),
            ),
            // The last clear removes the state.
            (
                secs(20),
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
                Time::ZERO,
                FaultAction::Partition {
                    target: FaultTarget::Range(0),
                    duration: secs(20),
                    style: PartitionStyle::Blackhole,
                },
            ),
            event(
                secs(5),
                FaultAction::Partition {
                    target: FaultTarget::AllRanges,
                    duration: secs(5),
                    style: PartitionStyle::Blackhole,
                },
            ),
        ];
        let cut = |range: u16| {
            ProxyCommand::Partition(ProxyId::Range(range), Some(PartitionStyle::Blackhole))
        };
        let clear = |range: u16| ProxyCommand::Partition(ProxyId::Range(range), None);
        let expected = vec![
            (
                Time::ZERO,
                vec![cut(0)],
                "partition range:0 blackhole".to_owned(),
            ),
            (
                secs(5),
                vec![cut(0), cut(1)],
                "partition all-ranges blackhole".to_owned(),
            ),
            // The all-ranges heal clears range 1 but re-applies range 0's
            // still-active single-range partition.
            (
                secs(10),
                vec![cut(0), clear(1)],
                "heal all-ranges (partition still active on range:0 from t=0s)".to_owned(),
            ),
            (secs(20), vec![clear(0)], "heal range:0".to_owned()),
        ];
        assert!(drive(&events, 2, 1) == expected);
    }

    #[test]
    fn flap_heals_never_clear_a_standing_partition_on_the_same_target() {
        let range1 = FaultTarget::Range(1);
        let events = vec![
            event(
                Time::ZERO,
                FaultAction::Partition {
                    target: range1,
                    duration: secs(12),
                    style: PartitionStyle::Blackhole,
                },
            ),
            event(
                secs(2),
                FaultAction::Flap {
                    target: range1,
                    period: secs(2),
                    duration: secs(6),
                },
            ),
        ];
        let cut = ProxyCommand::Partition(ProxyId::Range(1), Some(PartitionStyle::Blackhole));
        let survived = "heal range:1 (partition from t=0s still active)".to_owned();
        let expected = vec![
            (
                Time::ZERO,
                vec![cut],
                "partition range:1 blackhole".to_owned(),
            ),
            (secs(2), vec![cut], "partition range:1 blackhole".to_owned()),
            // The flap's own heals release only the flap's entry; the
            // standing partition stays applied.
            (secs(4), vec![cut], survived.clone()),
            (secs(6), vec![cut], "partition range:1 blackhole".to_owned()),
            (secs(8), vec![cut], survived),
            // Only the standing partition's own heal clears the proxy.
            (
                secs(12),
                vec![ProxyCommand::Partition(ProxyId::Range(1), None)],
                "heal range:1".to_owned(),
            ),
        ];
        assert!(drive(&events, 2, 1) == expected);
    }

    #[test]
    fn kill_and_restart_atoms_issue_no_proxy_commands() {
        let events = vec![event(
            secs(3),
            FaultAction::KillNode {
                node: 1,
                restart_after: Some(secs(4)),
            },
        )];
        let expected = vec![
            (secs(3), Vec::new(), "kill node1".to_owned()),
            (secs(7), Vec::new(), "restart node1".to_owned()),
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
                    delay: millis(100),
                    jitter: millis(20),
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
                    rate: kibibytes_per_sec(64),
                },
                "throttle sql:1 64KiB/s",
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
