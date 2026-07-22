//! Per-process resource sampling via `/proc`.
//!
//! The sampler polls `/proc/<pid>/stat` (utime + stime, converted to
//! core-seconds via `rustix::param::clock_ticks_per_second`) and
//! `/proc/<pid>/status` (`VmRSS`) on a fixed interval for every process in a
//! live [`ProcessRoster`]. The roster is re-snapshotted on every tick, so an
//! entry appended mid-run — a node restarted by a fault, with a fresh pid
//! under a `label#N` entry — is attached at the next tick, its CPU window
//! starting at attach (the correct attribution for a process born mid-run).
//! A process that disappears (killed by a fault) keeps its last observed
//! totals under its own entry.

use std::{fs, time::Duration};

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
/// Dropping the handle without calling [`ProcSampler::stop`] cancels the
/// background task; the collected totals are then discarded.
#[derive(Debug)]
pub struct ProcSampler {
    stop_tx: oneshot::Sender<()>,
    task: JoinHandle<Vec<Tracked>>,
}

impl ProcSampler {
    /// Starts sampling the roster's processes every `interval`.
    ///
    /// One sample is taken synchronously before the background task starts,
    /// so a window shorter than `interval` still observes every process, and
    /// the reported CPU covers only the sampled window (last observed total
    /// minus first observed total), not the whole process lifetime. The
    /// roster is re-snapshotted on every tick: entries appended after spawn
    /// (restarted nodes) are attached then, their windows starting at
    /// attach. A pid whose `/proc` entries are never readable reports zeros.
    ///
    /// # Panics
    ///
    /// Panics if called outside a Tokio runtime, or if `interval` is zero.
    #[must_use]
    pub fn spawn(roster: ProcessRoster, interval: Duration) -> Self {
        let mut tracked: Vec<Tracked> = Vec::new();
        attach_and_sample(&roster, &mut tracked);
        let (stop_tx, mut stop_rx) = oneshot::channel();
        let task = tokio::spawn(async move {
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

    /// Stops sampling and returns totals over the sampled window, one entry
    /// per roster entry in roster (launch) order.
    ///
    /// A final sample of still-alive pids is taken before totals are
    /// computed; processes that vanished mid-run keep their last observed
    /// totals.
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

/// Attaches roster entries not yet tracked, then samples every tracked
/// process. The roster is append-only, so the untracked entries are exactly
/// the tail past `tracked.len()` and result ordering stays roster order. A
/// process attached mid-run starts its CPU window at attach.
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
    ticks_per_second: u64,
    cpu: Option<CpuWindow>,
    max_rss_bytes: u64,
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
            ticks_per_second: rustix::param::clock_ticks_per_second(),
            cpu: None,
            max_rss_bytes: 0,
        }
    }

    /// Takes one sample. A pid that cannot be read (process gone, or never
    /// existed) is silently skipped, keeping the previous totals.
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
            && let Some(rss_bytes) = parse_vm_rss_bytes(&status)
        {
            self.max_rss_bytes = self.max_rss_bytes.max(rss_bytes);
        }
    }

    fn into_resources(self) -> ProcessResources {
        let cpu_core_seconds = self.cpu.map_or(0.0, |window| {
            ticks_to_seconds(
                window.last_ticks.saturating_sub(window.first_ticks),
                self.ticks_per_second,
            )
        });
        ProcessResources {
            label: self.info.label,
            pid: self.info.pid,
            cpu_core_seconds,
            max_rss_bytes: self.max_rss_bytes,
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
/// Field 2 (`comm`) is parenthesised and may itself contain spaces and
/// parentheses; the kernel does not escape it, so the only safe parse is to
/// split on the substring after the *last* `)` and count fields from there
/// (the field after `comm` is field 3, the process state).
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

/// Parses the `VmRSS:` line (kB) out of `/proc/<pid>/status` content,
/// returning bytes. Kernel threads have no `VmRSS` line.
fn parse_vm_rss_bytes(status: &str) -> Option<u64> {
    let line = status.lines().find(|line| line.starts_with("VmRSS:"))?;
    let kilobytes: u64 = line
        .strip_prefix("VmRSS:")?
        .split_ascii_whitespace()
        .next()?
        .parse()
        .ok()?;
    kilobytes.checked_mul(1024)
}

/// Converts a clock-tick count to seconds. A zero `ticks_per_second`
/// (impossible on Linux, but cheap to guard) is treated as 1.
fn ticks_to_seconds(ticks: u64, ticks_per_second: u64) -> f64 {
    u64_as_f64(ticks) / u64_as_f64(ticks_per_second.max(1))
}

/// Lossless-for-practical-values `u64` → `f64` conversion built from exact
/// `u32` → `f64` conversions, avoiding a precision-losing `as` cast.
fn u64_as_f64(value: u64) -> f64 {
    const TWO_POW_32: f64 = 4_294_967_296.0;
    let high = u32::try_from(value >> 32).expect("u64 >> 32 fits in u32");
    let low = u32::try_from(value & 0xFFFF_FFFF).expect("masked to 32 bits");
    f64::from(high) * TWO_POW_32 + f64::from(low)
}

#[cfg(test)]
mod tests {
    use assert2::assert;

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
    fn parse_vm_rss_bytes_reads_kilobytes() {
        let realistic = "Name:\tcrabka-gres\nUmask:\t0022\nState:\tS (sleeping)\n\
                         VmPeak:\t  204800 kB\nVmHWM:\t   12345 kB\nVmRSS:\t    5348 kB\n\
                         RssAnon:\t    4000 kB\n";
        let cases = [
            (realistic, Some(5348 * 1024)),
            ("Name:\tkthreadd\nState:\tS (sleeping)\n", None),
            ("VmRSS:\tlots kB\n", None),
            ("", None),
        ];
        for (status, expected) in cases {
            assert!(parse_vm_rss_bytes(status) == expected, "status: {status:?}");
        }
    }

    #[test]
    fn ticks_to_seconds_divides_by_clock_rate() {
        // (ticks, ticks per second, expected seconds); a zero rate clamps to 1.
        let cases = [
            (0, 100, 0.0),
            (150, 100, 1.5),
            (1, 1000, 0.001),
            (100, 0, 100.0),
        ];
        for (ticks, rate, expected) in cases {
            let got = ticks_to_seconds(ticks, rate);
            assert!(
                (got - expected).abs() < 1e-9,
                "ticks: {ticks}, rate: {rate}"
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

    /// Burns CPU on the current thread for roughly `duration`.
    fn burn_cpu(duration: Duration) {
        let start = std::time::Instant::now();
        let mut spin: u64 = 0;
        while start.elapsed() < duration {
            spin = spin.wrapping_add(1);
        }
        std::hint::black_box(spin);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn samples_own_process_cpu_and_rss() {
        let sampler = ProcSampler::spawn(
            roster_of(vec![ProcessInfo {
                label: "self".to_string(),
                pid: std::process::id(),
            }]),
            Duration::from_millis(50),
        );
        // Burn CPU so the window's utime delta is at least a few clock ticks.
        burn_cpu(Duration::from_millis(200));
        let resources = sampler.stop().await;
        assert!(resources.len() == 1);
        assert!(resources[0].label == "self");
        assert!(resources[0].pid == std::process::id());
        assert!(resources[0].cpu_core_seconds > 0.0);
        assert!(resources[0].max_rss_bytes > 0);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn roster_entries_appended_mid_run_are_attached_and_reported() {
        let roster = roster_of(vec![ProcessInfo {
            label: "self".to_string(),
            pid: std::process::id(),
        }]);
        let sampler = ProcSampler::spawn(roster.clone(), Duration::from_millis(20));
        // Let at least one tick pass before the roster grows, as it would
        // when a fault restarts a node mid-window.
        tokio::time::sleep(Duration::from_millis(50)).await;
        roster.push(ProcessInfo {
            label: "self#2".to_string(),
            pid: std::process::id(),
        });
        burn_cpu(Duration::from_millis(100));
        // Leave the sampler a few ticks to attach and sample the new entry.
        tokio::time::sleep(Duration::from_millis(60)).await;
        let resources = sampler.stop().await;
        assert!(resources.len() == 2);
        assert!(resources[0].label == "self");
        assert!(resources[1].label == "self#2");
        assert!(resources[1].pid == std::process::id());
        // The late entry's window starts at attach: sampled, non-negative,
        // and its RSS was observed.
        assert!(resources[1].cpu_core_seconds >= 0.0);
        assert!(resources[1].max_rss_bytes > 0);
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
            Duration::from_millis(10),
        );
        tokio::time::sleep(Duration::from_millis(30)).await;
        let resources = sampler.stop().await;
        let expected = vec![
            ProcessResources {
                label: "ghost-b".to_string(),
                pid: 999_999_999,
                cpu_core_seconds: 0.0,
                max_rss_bytes: 0,
            },
            ProcessResources {
                label: "ghost-a".to_string(),
                pid: 999_999_998,
                cpu_core_seconds: 0.0,
                max_rss_bytes: 0,
            },
        ];
        assert!(resources == expected);
    }
}
