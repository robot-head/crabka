//! Execution-level tests for stream-time punctuation through the
//! [`TopologyTestDriver`]. After each `pipe_input`, the driver fires due
//! `STREAM_TIME` punctuators at the current stream-time (at most once each).
//!
//! Ground truth is the captured JVM firing model in
//! `tests/testdata/punctuation/behavior.json`: piping records at stream-times
//! `{0, 5, 9, 10, 11, 100}` with a 10ms interval fires the punctuator at
//! `{0, 10, 100}` — the *current* stream-time at each fire, with no per-boundary
//! catch-up (sub-interval steps 5/9/11 do not fire; the 100ms jump fires once).
//!
//! Each test shares a fired-timestamp log between a processor and its punctuator
//! via `Arc<Mutex<_>>` (the same pattern as `dsl/processors/stream_join.rs`), so
//! assertions can inspect the firing sequence directly — and the punctuator also
//! forwards each fired timestamp downstream so it is independently observable via
//! `read_output`.

use std::{
    sync::{Arc, Mutex},
    time::Duration,
};

use async_trait::async_trait;
use crabka_client_streams::{
    Cancellable, Consumed, I64Serde, NodeHandle, Processor, ProcessorContext, Produced,
    PunctuationType, Punctuator, Record, StringSerde, Topology, TopologyTestDriver,
};

/// A stream-time punctuator that, on each fire, records the fired timestamp into
/// a shared log and forwards a record carrying that timestamp downstream (so the
/// fire is observable both via the shared `Arc<Mutex<…>>` and `read_output`).
struct LoggingPunctuator {
    fired: Arc<Mutex<Vec<i64>>>,
}

#[async_trait]
impl Punctuator<String, i64> for LoggingPunctuator {
    async fn punctuate(&mut self, ctx: &mut ProcessorContext<'_, '_, String, i64>, ts: i64) {
        self.fired.lock().unwrap().push(ts);
        ctx.forward(Record::new(None, ts, ts));
    }
}

/// A processor that, in `init`, schedules `LoggingPunctuator` on `STREAM_TIME` with
/// the given interval and stashes the returned [`Cancellable`] into a shared slot
/// the test can reach. Records pass straight through (forwarded unchanged) so the
/// stream-time clock advances per record without affecting the punctuator output.
struct SchedulingProc {
    fired: Arc<Mutex<Vec<i64>>>,
    handle: Arc<Mutex<Option<Cancellable>>>,
    interval_ms: u64,
}

#[async_trait]
impl Processor<String, i64, String, i64> for SchedulingProc {
    async fn init(&mut self, ctx: &mut ProcessorContext<'_, '_, String, i64>) {
        let c = ctx.schedule(
            Duration::from_millis(self.interval_ms),
            PunctuationType::StreamTime,
            LoggingPunctuator {
                fired: self.fired.clone(),
            },
        );
        *self.handle.lock().unwrap() = Some(c);
    }

    async fn process(
        &mut self,
        ctx: &mut ProcessorContext<'_, '_, String, i64>,
        r: Record<String, i64>,
    ) {
        // Pass the record through unchanged; it only serves to advance stream-time.
        ctx.forward(r);
    }
}

/// What [`build_driver`] returns: the driver, the shared fired-timestamp log, and
/// the shared slot holding the schedule's [`Cancellable`].
type DriverRig = (
    TopologyTestDriver,
    Arc<Mutex<Vec<i64>>>,
    Arc<Mutex<Option<Cancellable>>>,
);

/// Build `source("in") -> proc -> sink("out")` with `proc` scheduling a
/// stream-time punctuator. Returns the driver plus the shared fired-log and the
/// shared cancellable slot. K/V = `String`/`i64`; the sink uses `I64Serde` so
/// fired timestamps surface through `read_output`.
fn build_driver(interval_ms: u64) -> DriverRig {
    let fired = Arc::new(Mutex::new(Vec::new()));
    let handle: Arc<Mutex<Option<Cancellable>>> = Arc::new(Mutex::new(None));

    let mut t = Topology::new();
    let src: NodeHandle<String, i64> = t.add_source("in", ["in"]);
    let proc_fired = fired.clone();
    let proc_handle = handle.clone();
    let proc = t.add_processor(
        "proc",
        move || SchedulingProc {
            fired: proc_fired.clone(),
            handle: proc_handle.clone(),
            interval_ms,
        },
        [&src],
    );
    t.add_sink("out", "out", [&proc]);

    let driver = TopologyTestDriver::new(&t.build("app").unwrap()).unwrap();
    (driver, fired, handle)
}

/// Sentinel value carried by every piped (pass-through) record, distinct from
/// any fired stream-time so the two are separable in the output stream. All test
/// stream-times are `>= 0`, so `-1` can only be a pass-through.
const PASS_THROUGH: i64 = -1;

/// Pipe one `String`/`i64` record on `"in"` at the given stream-time. The value
/// is the [`PASS_THROUGH`] sentinel so it is distinguishable from fired timestamps.
fn pipe_at(driver: &mut TopologyTestDriver, ts: i64) {
    driver.pipe_input(
        "in",
        Consumed::with(StringSerde, I64Serde),
        Some("k".to_string()),
        PASS_THROUGH,
        ts,
    );
}

/// Drain `"out"` and return only the forwarded *fired* timestamps (i.e. the
/// non-[`PASS_THROUGH`] values).
fn drain_fires(driver: &mut TopologyTestDriver) -> Vec<i64> {
    let mut out = Vec::new();
    while let Some((_, v)) = driver.read_output("out", Produced::with(StringSerde, I64Serde)) {
        if v != PASS_THROUGH {
            out.push(v);
        }
    }
    out
}

#[test]
fn fires_at_current_stream_time_on_boundaries() {
    // Mirrors behavior.json: pipe at {0,5,9,10,11,100}; fires at {0,10,100}.
    let (mut driver, fired, _handle) = build_driver(10);
    for ts in [0, 5, 9, 10, 11, 100] {
        pipe_at(&mut driver, ts);
    }
    assert2::assert!(
        *fired.lock().unwrap() == vec![0, 10, 100],
        "fired-log must equal the captured JVM sequence (current stream-time per fire)"
    );

    // The punctuator forwards each fired ts downstream; confirm the same {0,10,100}
    // surface via read_output (pass-through records carry PASS_THROUGH and are filtered).
    assert2::assert!(drain_fires(&mut driver) == vec![0, 10, 100]);
}

#[test]
fn catch_up_jump_fires_once() {
    // A 100ms jump fires ONCE more at the current stream-time (100), not at every
    // 10ms boundary in between.
    let (mut driver, fired, _handle) = build_driver(10);
    pipe_at(&mut driver, 0); // fire 0
    pipe_at(&mut driver, 100); // single catch-up fire at 100
    assert2::assert!(*fired.lock().unwrap() == vec![0, 100]);
}

#[test]
fn cancel_stops_firing() {
    // First record fires at 0; cancel the schedule via the test-held handle; a
    // later record at ts=50 must NOT fire.
    let (mut driver, fired, handle) = build_driver(10);
    pipe_at(&mut driver, 0); // fire 0
    assert2::assert!(*fired.lock().unwrap() == vec![0]);

    handle
        .lock()
        .unwrap()
        .as_ref()
        .expect("schedule() ran in init")
        .cancel();

    pipe_at(&mut driver, 50); // would fire at 50 if not cancelled
    assert2::assert!(*fired.lock().unwrap() == vec![0], "no fires after cancel()");
}

#[test]
fn punctuator_reads_and_writes_store() {
    // A stream-time punctuator that increments a counter in a connected KV store
    // on each fire, proving `ctx.get_state_store` works from a punctuator.
    struct CountingPunctuator;
    #[async_trait]
    impl Punctuator<String, i64> for CountingPunctuator {
        async fn punctuate(&mut self, ctx: &mut ProcessorContext<'_, '_, String, i64>, _ts: i64) {
            let store = ctx.get_state_store::<String, i64>("fires").unwrap();
            let n = store.get(&"n".to_string()).await.unwrap_or(0) + 1;
            store.put("n".to_string(), n).await;
        }
    }
    struct StoreScheduler;
    #[async_trait]
    impl Processor<String, i64, String, i64> for StoreScheduler {
        async fn init(&mut self, ctx: &mut ProcessorContext<'_, '_, String, i64>) {
            ctx.schedule(
                Duration::from_millis(10),
                PunctuationType::StreamTime,
                CountingPunctuator,
            );
        }
        async fn process(
            &mut self,
            _ctx: &mut ProcessorContext<'_, '_, String, i64>,
            _r: Record<String, i64>,
        ) {
        }
    }

    let mut t = Topology::new();
    let src: NodeHandle<String, i64> = t.add_source("in", ["in"]);
    let proc = t.add_processor("proc", || StoreScheduler, [&src]);
    t.add_state_store("fires", StringSerde, I64Serde, [proc.name()]);
    t.add_sink("out", "out", [&proc]);
    let mut driver = TopologyTestDriver::new(&t.build("app").unwrap()).unwrap();

    // pipe at {0, 5, 10, 100} → boundaries crossed at {0, 10, 100} = 3 fires.
    for ts in [0, 5, 10, 100] {
        pipe_at(&mut driver, ts);
    }
    assert2::assert!(
        driver.store_get::<String, i64>("fires", &"n".to_string()) == Some(3),
        "store counter equals the number of stream-time fires"
    );
}

// ---------------------------------------------------------------------------
// Wall-clock punctuation (Task 6).
//
// Ground truth is the `=== wall ===` section of `behavior.json`: a wall-clock
// schedule (interval 10) with the TTD mock clock starting at 0. The steps
// `advanceWallClockTime(+3, +3, +4, +100)` bring the clock to `{3, 6, 10, 110}`
// and fire the punctuator at `{10, 110}` — first-fire at `0 (clock in init) +
// interval = 10`, no sub-interval fires (3, 6), and the +100 jump fires ONCE at
// 110 (no catch-up to 20/30/…). The wall clock is independent of stream-time:
// piping records advances stream-time only, and `advance_wall_clock_time`
// advances wall-time only.
// ---------------------------------------------------------------------------

/// A processor that, in `init`, schedules `LoggingPunctuator` on `WALL_CLOCK_TIME`
/// with the given interval, logging fired timestamps into a shared slot. Records
/// pass through unchanged (they advance stream-time, never the wall clock).
struct WallSchedulingProc {
    fired: Arc<Mutex<Vec<i64>>>,
    interval_ms: u64,
}

#[async_trait]
impl Processor<String, i64, String, i64> for WallSchedulingProc {
    async fn init(&mut self, ctx: &mut ProcessorContext<'_, '_, String, i64>) {
        ctx.schedule(
            Duration::from_millis(self.interval_ms),
            PunctuationType::WallClockTime,
            LoggingPunctuator {
                fired: self.fired.clone(),
            },
        );
    }

    async fn process(
        &mut self,
        ctx: &mut ProcessorContext<'_, '_, String, i64>,
        r: Record<String, i64>,
    ) {
        ctx.forward(r);
    }
}

/// Build `source("in") -> proc -> sink("out")` with `proc` scheduling a
/// wall-clock punctuator (interval `interval_ms`). Returns the driver plus the
/// shared fired-log. The wall fired-log is populated by `advance_wall_clock_time`,
/// not by `pipe_input`.
fn build_wall_driver(interval_ms: u64) -> (TopologyTestDriver, Arc<Mutex<Vec<i64>>>) {
    let fired = Arc::new(Mutex::new(Vec::new()));

    let mut t = Topology::new();
    let src: NodeHandle<String, i64> = t.add_source("in", ["in"]);
    let proc_fired = fired.clone();
    let proc = t.add_processor(
        "proc",
        move || WallSchedulingProc {
            fired: proc_fired.clone(),
            interval_ms,
        },
        [&src],
    );
    t.add_sink("out", "out", [&proc]);

    let driver = TopologyTestDriver::new(&t.build("app").unwrap()).unwrap();
    (driver, fired)
}

#[test]
fn wall_clock_fires_on_boundary_at_clock_value() {
    // Mirrors the `=== wall ===` section of behavior.json. The mock clock starts
    // at 0; init stamps first-fire at 0 + interval = 10. The advance steps bring
    // the clock to {3, 6, 10, 110}; the punctuator fires at {10, 110} — value is
    // the current clock, NOT the first advance value, and the +100 jump fires ONCE.
    let (mut driver, fired) = build_wall_driver(10);

    driver.advance_wall_clock_time(Duration::from_millis(3)); // clock 3 — no fire
    driver.advance_wall_clock_time(Duration::from_millis(3)); // clock 6 — no fire
    driver.advance_wall_clock_time(Duration::from_millis(4)); // clock 10 — fire 10
    driver.advance_wall_clock_time(Duration::from_millis(100)); // clock 110 — fire 110

    assert2::assert!(
        *fired.lock().unwrap() == vec![10, 110],
        "wall fired-log must equal the captured JVM sequence (first-fire at interval; +100 fires once)"
    );

    // The punctuator forwards each fired ts downstream; confirm {10, 110} surface
    // via read_output (no pass-through records were piped here).
    assert2::assert!(drain_fires(&mut driver) == vec![10, 110]);
}

#[test]
fn wall_clock_catch_up_fires_once() {
    // advance(10) fires at clock 10; advance(55) brings the clock to 65 and fires
    // ONCE at 65 (the current clock), not at every 10ms boundary in between.
    let (mut driver, fired) = build_wall_driver(10);

    driver.advance_wall_clock_time(Duration::from_millis(10)); // clock 10 — fire 10
    driver.advance_wall_clock_time(Duration::from_millis(55)); // clock 65 — single catch-up fire at 65

    assert2::assert!(*fired.lock().unwrap() == vec![10, 65]);
}

#[test]
fn stream_and_wall_fire_independently() {
    // ONE processor scheduling BOTH a stream-time and a wall-clock punctuator
    // (interval 10 each), logging into two separate shared logs. Piping records
    // advances stream-time only (STREAM log fires, WALL stays empty); advancing
    // the wall clock advances wall-time only (WALL log fires, STREAM unchanged).
    struct BothScheduler {
        stream_log: Arc<Mutex<Vec<i64>>>,
        wall_log: Arc<Mutex<Vec<i64>>>,
    }
    #[async_trait]
    impl Processor<String, i64, String, i64> for BothScheduler {
        async fn init(&mut self, ctx: &mut ProcessorContext<'_, '_, String, i64>) {
            ctx.schedule(
                Duration::from_millis(10),
                PunctuationType::StreamTime,
                LoggingPunctuator {
                    fired: self.stream_log.clone(),
                },
            );
            ctx.schedule(
                Duration::from_millis(10),
                PunctuationType::WallClockTime,
                LoggingPunctuator {
                    fired: self.wall_log.clone(),
                },
            );
        }
        async fn process(
            &mut self,
            ctx: &mut ProcessorContext<'_, '_, String, i64>,
            r: Record<String, i64>,
        ) {
            ctx.forward(r);
        }
    }

    let stream_log = Arc::new(Mutex::new(Vec::new()));
    let wall_log = Arc::new(Mutex::new(Vec::new()));

    let mut t = Topology::new();
    let src: NodeHandle<String, i64> = t.add_source("in", ["in"]);
    let s_log = stream_log.clone();
    let w_log = wall_log.clone();
    let proc = t.add_processor(
        "proc",
        move || BothScheduler {
            stream_log: s_log.clone(),
            wall_log: w_log.clone(),
        },
        [&src],
    );
    t.add_sink("out", "out", [&proc]);
    let mut driver = TopologyTestDriver::new(&t.build("app").unwrap()).unwrap();

    // Pipe at stream-times {0, 10}: STREAM fires {0, 10}; WALL stays empty
    // (piping doesn't advance the wall clock).
    pipe_at(&mut driver, 0);
    pipe_at(&mut driver, 10);
    assert2::assert!(*stream_log.lock().unwrap() == vec![0, 10]);
    assert2::assert!(
        *wall_log.lock().unwrap() == Vec::<i64>::new(),
        "piping advances stream-time only — wall clock untouched"
    );

    // Advance the wall clock by 10: WALL fires {10}; STREAM unchanged.
    driver.advance_wall_clock_time(Duration::from_millis(10));
    assert2::assert!(*wall_log.lock().unwrap() == vec![10]);
    assert2::assert!(
        *stream_log.lock().unwrap() == vec![0, 10],
        "advancing the wall clock doesn't fire stream-time punctuators"
    );
}
