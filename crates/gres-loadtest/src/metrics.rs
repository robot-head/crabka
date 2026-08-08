//! Per-process resource sampling via `/proc`.
//!
//! The sampler polls `/proc/<pid>/stat` and `/proc/<pid>/status` on a fixed
//! interval, for every process in a live [`ProcessRoster`]. From `stat` it
//! takes utime and stime and converts them to core-seconds with
//! `rustix::param::clock_ticks_per_second`. From `status` it takes `VmRSS`.
//! This works on Linux only. On other platforms the `/proc` reads fail and
//! every process reports zeros.
//!
//! The sampler snapshots the roster again on every tick. An entry appended
//! mid-run, such as a node that a fault restarted, with a fresh pid under a
//! `label#N` entry, therefore attaches at the next tick. Its CPU window starts
//! at that attach, which is the correct attribution for a process born
//! mid-run. A process that disappears, because a fault killed it, keeps its
//! last observed totals under its own entry.

use std::fs;

use crabka_units::prelude::*;
use tokio::{
    sync::oneshot,
    task::JoinHandle,
    time::{Instant, MissedTickBehavior, interval_at},
};

use crate::{
    cluster::{ProcessInfo, ProcessRoster},
    report::ProcessResources,
};

/// Handle to a background resource sampler.
///
/// A drop of the handle without a call to [`ProcSampler::stop`] cancels the
/// background task, and the collected totals are then discarded.
#[derive(Debug)]
pub struct ProcSampler {
    stop_tx: oneshot::Sender<()>,
    task: JoinHandle<Vec<Tracked>>,
}

impl ProcSampler {
    /// Starts sampling the roster's processes every `interval`.
    ///
    /// This method takes one sample synchronously before the background task
    /// starts, so a window shorter than `interval` still observes every
    /// process. The reported CPU therefore covers only the sampled window, as
    /// the last observed total minus the first observed total, and not the
    /// whole process lifetime.
    ///
    /// The sampler snapshots the roster again on every tick. Entries appended
    /// after the spawn, such as restarted nodes, attach at that point, and
    /// their windows start at the attach. A pid whose `/proc` entries are
    /// never readable reports zeros.
    ///
    /// # Panics
    ///
    /// Panics if called outside a Tokio runtime, or if `interval` is zero.
    #[must_use]
    pub fn spawn(roster: ProcessRoster, interval: Time) -> Self {
        let mut tracked: Vec<Tracked> = Vec::new();
        attach_and_sample(&roster, &mut tracked);
        let (stop_tx, mut stop_rx) = oneshot::channel();
        let task = tokio::spawn(async move {
            let interval = interval.to_std();
            let mut ticker = interval_at(Instant::now() + interval, interval);
            ticker.set_missed_tick_behavior(MissedTickBehavior::Delay);
            loop {
                tokio::select! {
                    _ = &mut stop_rx => break,
                    _ = ticker.tick() => attach_and_sample(&roster, &mut tracked),
                }
            }
            // Final pass so the window ends at `stop`, not at the last tick
            // (and a process born after the last tick still gets a row).
            attach_and_sample(&roster, &mut tracked);
            tracked
        });
        Self { stop_tx, task }
    }

    /// Stops the sampling and returns the totals over the sampled window, one
    /// entry for each roster entry, in roster order, which is launch order.
    ///
    /// This method takes a final sample of the still-alive pids before it
    /// computes the totals. A process that vanished mid-run keeps its last
    /// observed totals.
    ///
    /// # Panics
    ///
    /// Panics if the background sampling task itself panicked.
    pub async fn stop(self) -> Vec<ProcessResources> {
        let Self { stop_tx, task } = self;
        // A send failure means the task already exited (it saw the handle
        // drop or the runtime shut down); joining below is still correct.
        let _: Result<(), ()> = stop_tx.send(());
        let tracked = task.await.expect("resource sampler task panicked");
        tracked.into_iter().map(Tracked::into_resources).collect()
    }
}

/// Attaches the roster entries that are not yet tracked, then samples every
/// tracked process. The roster is append-only, so the untracked entries are
/// exactly the tail past `tracked.len()`, and the result order stays roster
/// order. A process attached mid-run starts its CPU window at the attach.
fn attach_and_sample(roster: &ProcessRoster, tracked: &mut Vec<Tracked>) {
    for info in roster.snapshot().into_iter().skip(tracked.len()) {
        tracked.push(Tracked::new(info));
    }
    for process in &mut *tracked {
        process.sample();
    }
}

/// Sampling state for one tracked process.
#[derive(Debug)]
struct Tracked {
    info: ProcessInfo,
    tick_rate: Frequency,
    cpu: Option<CpuWindow>,
    max_rss: ByteSize,
}

/// First and last observed cumulative CPU totals, in clock ticks.
#[derive(Debug, Clone, Copy)]
struct CpuWindow {
    first_ticks: u64,
    last_ticks: u64,
}

impl Tracked {
    fn new(info: ProcessInfo) -> Self {
        Self {
            info,
            tick_rate: clock_tick_rate(),
            cpu: None,
            max_rss: ByteSize::ZERO,
        }
    }

    /// Takes one sample. This method quietly skips a pid that it cannot read,
    /// because the process is gone or never existed, and it keeps the previous
    /// totals.
    fn sample(&mut self) {
        let pid = self.info.pid;
        if let Ok(stat) = fs::read_to_string(format!("/proc/{pid}/stat"))
            && let Some(parsed) = parse_proc_stat(&stat)
        {
            let total = parsed.utime_ticks.saturating_add(parsed.stime_ticks);
            match &mut self.cpu {
                Some(window) => window.last_ticks = total,
                None => {
                    self.cpu = Some(CpuWindow {
                        first_ticks: total,
                        last_ticks: total,
                    });
                }
            }
        }
        if let Ok(status) = fs::read_to_string(format!("/proc/{pid}/status"))
            && let Some(rss) = parse_vm_rss(&status)
        {
            self.max_rss = self.max_rss.max(rss);
        }
    }

    fn into_resources(self) -> ProcessResources {
        let cpu_time = self.cpu.map_or(Time::ZERO, |window| {
            ticks_to_time(
                window.last_ticks.saturating_sub(window.first_ticks),
                self.tick_rate,
            )
        });
        ProcessResources {
            label: self.info.label,
            pid: self.info.pid,
            cpu_time,
            max_rss: self.max_rss,
        }
    }
}

/// CPU fields parsed from one `/proc/<pid>/stat` line.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct ProcStat {
    utime_ticks: u64,
    stime_ticks: u64,
}

/// Parses `utime` (field 14) and `stime` (field 15) out of a
/// `/proc/<pid>/stat` line.
///
/// Field 2, `comm`, is parenthesised and can itself hold spaces and
/// parentheses. The kernel does not escape it, so the only safe parse splits
/// on the substring after the *last* `)` and counts the fields from there. The
/// field after `comm` is field 3, the process state.
fn parse_proc_stat(line: &str) -> Option<ProcStat> {
    let (_, after_comm) = line.rsplit_once(')')?;
    let mut fields = after_comm.split_ascii_whitespace();
    // `after_comm` starts at field 3 (index 0 here); utime is field 14.
    let utime_ticks = fields.nth(11)?.parse().ok()?;
    let stime_ticks = fields.next()?.parse().ok()?;
    Some(ProcStat {
        utime_ticks,
        stime_ticks,
    })
}

/// Parses the `VmRSS:` line out of `/proc/<pid>/status` content. The kernel
/// labels it `kB` but counts kibibytes. A kernel thread has no `VmRSS`
/// line.
fn parse_vm_rss(status: &str) -> Option<ByteSize> {
    let line = status.lines().find(|line| line.starts_with("VmRSS:"))?;
    let count: u64 = line
        .strip_prefix("VmRSS:")?
        .split_ascii_whitespace()
        .next()?
        .parse()
        .ok()?;
    Some(kibibytes(1) * u64_as_f64(count))
}

/// The kernel's clock-tick rate for `/proc` CPU fields.
#[cfg(target_os = "linux")]
fn clock_tick_rate() -> Frequency {
    Frequency::from_per_sec(u64_as_f64(rustix::param::clock_ticks_per_second()))
}

/// Off Linux there is no `/proc` to sample. The reads fail and every process
/// reports zeros, so the rate is never observable. Any nonzero constant keeps
/// the arithmetic well-defined.
#[cfg(not(target_os = "linux"))]
fn clock_tick_rate() -> Frequency {
    per_sec(100)
}

/// The CPU time that `ticks` of a clock running at `tick_rate` represent. A
/// non-positive rate is impossible on Linux, but the guard is cheap. Such a
/// rate gives no elapsed time rather than an infinity, because
/// [`FrequencyExt::period`] reports a zero period for it.
fn ticks_to_time(ticks: u64, tick_rate: Frequency) -> Time {
    tick_rate.period() * u64_as_f64(ticks)
}

/// A `u64` → `f64` conversion that is lossless for practical values. It is
/// built from exact `u32` → `f64` conversions, so it needs no `as` cast, which
/// would lose precision.
fn u64_as_f64(value: u64) -> f64 {
    const TWO_POW_32: f64 = 4_294_967_296.0;
    let high = u32::try_from(value >> 32).expect("u64 >> 32 fits in u32");
    let low = u32::try_from(value & 0xFFFF_FFFF).expect("masked to 32 bits");
    f64::from(high) * TWO_POW_32 + f64::from(low)
}

#[cfg(test)]
mod tests {
    use assert2::{assert, check};
    use crabka_units::fmt::Human as _;

    use super::*;

    #[test]
    fn parse_proc_stat_handles_parenthesised_comm() {
        let realistic = "1234567 (tokio-runtime-w) S 1 1234 1234 0 -1 4194304 1523 0 2 0 \
                         1234 567 0 0 20 0 8 0 4980775 1146880000 4283 18446744073709551615";
        let pathological = "42 (a) b) R 0 0 0 0 -1 0 0 0 0 0 99 101 0 0 20 0 1 0 0 0 0";
        let truncated = "7 (short) S 0 0 0 0 -1 0 0 0 0 0";
        let non_numeric = "7 (bad) S 0 0 0 0 -1 0 0 0 0 0 x 5 0 0";
        let cases = [
            (
                realistic,
                Some(ProcStat {
                    utime_ticks: 1234,
                    stime_ticks: 567,
                }),
            ),
            (
                pathological,
                Some(ProcStat {
                    utime_ticks: 99,
                    stime_ticks: 101,
                }),
            ),
            (truncated, None),
            (non_numeric, None),
            ("no parenthesis at all", None),
            ("", None),
        ];
        for (line, expected) in cases {
            assert!(parse_proc_stat(line) == expected, "line: {line:?}");
        }
    }

    #[test]
    fn parse_vm_rss_reads_kibibytes() {
        let realistic = "Name:\tcrabka-gres\nUmask:\t0022\nState:\tS (sleeping)\n\
                         VmPeak:\t  204800 kB\nVmHWM:\t   12345 kB\nVmRSS:\t    5348 kB\n\
                         RssAnon:\t    4000 kB\n";
        let cases = [
            (realistic, Some(kibibytes(5348))),
            ("Name:\tkthreadd\nState:\tS (sleeping)\n", None),
            ("VmRSS:\tlots kB\n", None),
            ("", None),
        ];
        for (status, expected) in cases {
            check!(parse_vm_rss(status) == expected, "status: {status:?}");
        }
    }

    #[test]
    fn ticks_scale_by_the_clock_period() {
        // (ticks, tick rate, expected CPU time); a non-positive rate has no
        // period, so it reports no elapsed time rather than an infinity.
        let cases = [
            (0, per_sec(100), Time::ZERO),
            (150, per_sec(100), millis(1500)),
            (1, per_sec(1000), millis(1)),
            (100, Frequency::ZERO, Time::ZERO),
        ];
        for (ticks, rate, expected) in cases {
            let got = ticks_to_time(ticks, rate);
            check!(
                (got - expected).abs() < nanos(1),
                "ticks: {ticks}, rate: {}",
                rate.human()
            );
        }
    }

    /// A roster pre-populated with the given entries.
    fn roster_of(entries: Vec<ProcessInfo>) -> ProcessRoster {
        let roster = ProcessRoster::default();
        for entry in entries {
            roster.push(entry);
        }
        roster
    }

    /// Burns CPU on the current thread for about `budget`. Only the
    /// Linux-gated self-sampling tests need it.
    #[cfg(target_os = "linux")]
    fn burn_cpu(budget: Time) {
        let start = std::time::Instant::now();
        let budget = budget.to_std();
        let mut spin: u64 = 0;
        while start.elapsed() < budget {
            spin = spin.wrapping_add(1);
        }
        std::hint::black_box(spin);
    }

    // Sampling the test's own process requires `/proc` — Linux-only; other
    // platforms report zeros by design.
    #[cfg(target_os = "linux")]
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn samples_own_process_cpu_and_rss() {
        let sampler = ProcSampler::spawn(
            roster_of(vec![ProcessInfo {
                label: "self".to_string(),
                pid: std::process::id(),
            }]),
            millis(50),
        );
        // Burn CPU so the window's utime delta is at least a few clock ticks.
        burn_cpu(millis(200));
        let resources = sampler.stop().await;
        assert!(resources.len() == 1);
        assert!(resources[0].label == "self");
        assert!(resources[0].pid == std::process::id());
        check!(resources[0].cpu_time > Time::ZERO);
        check!(resources[0].max_rss > ByteSize::ZERO);
    }

    // Asserts nonzero readings from the live-attached entry, so `/proc` is
    // required — Linux-only.
    #[cfg(target_os = "linux")]
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn roster_entries_appended_mid_run_are_attached_and_reported() {
        let roster = roster_of(vec![ProcessInfo {
            label: "self".to_string(),
            pid: std::process::id(),
        }]);
        let sampler = ProcSampler::spawn(roster.clone(), millis(20));
        // Let at least one tick pass before the roster grows, as it would
        // when a fault restarts a node mid-window.
        tokio::time::sleep(millis(50).to_std()).await;
        roster.push(ProcessInfo {
            label: "self#2".to_string(),
            pid: std::process::id(),
        });
        burn_cpu(millis(100));
        // Leave the sampler a few ticks to attach and sample the new entry.
        tokio::time::sleep(millis(60).to_std()).await;
        let resources = sampler.stop().await;
        assert!(resources.len() == 2);
        assert!(resources[0].label == "self");
        assert!(resources[1].label == "self#2");
        assert!(resources[1].pid == std::process::id());
        // The late entry's window starts at attach: sampled, non-negative,
        // and its RSS was observed.
        check!(resources[1].cpu_time >= Time::ZERO);
        check!(resources[1].max_rss > ByteSize::ZERO);
    }

    #[tokio::test]
    async fn never_readable_pids_report_zeros_in_spawn_order() {
        let sampler = ProcSampler::spawn(
            roster_of(vec![
                ProcessInfo {
                    label: "ghost-b".to_string(),
                    pid: 999_999_999,
                },
                ProcessInfo {
                    label: "ghost-a".to_string(),
                    pid: 999_999_998,
                },
            ]),
            millis(10),
        );
        tokio::time::sleep(millis(30).to_std()).await;
        let resources = sampler.stop().await;
        let expected = vec![
            ProcessResources {
                label: "ghost-b".to_string(),
                pid: 999_999_999,
                cpu_time: Time::ZERO,
                max_rss: ByteSize::ZERO,
            },
            ProcessResources {
                label: "ghost-a".to_string(),
                pid: 999_999_998,
                cpu_time: Time::ZERO,
                max_rss: ByteSize::ZERO,
            },
        ];
        assert!(resources == expected);
    }
}
