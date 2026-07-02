//! Plotly HTML graph rendering for the benchmark report.
//!
//! Turns the cross-run aggregates from [`crate::aggregate`] into one
//! self-contained HTML page:
//!
//! - a grouped **bar chart per headline metric** — crabka vs kafka across
//!   every scenario cell, with run-to-run sample-stddev error bars; and
//! - per `(scenario, metric)`, an **across-run-averaged time-series line
//!   chart** showing how the metric moved over the run for each stack.
//!
//! The page loads `plotly.js` from the CDN (matching the version the `plotly`
//! crate itself targets) and embeds each figure inline, so the whole report is
//! a single `.html` file.

use std::collections::BTreeMap;

use plotly::{
    Bar, Layout, Plot, Scatter,
    common::{ErrorData, ErrorType, Line, Mode, Title},
    layout::{Axis, BarMode},
};

use crate::{
    aggregate::{
        CellAgg, ScalarMetric, TsSeries, aggregate_cells, averaged_timeseries, scalar_metrics,
    },
    scenario::{RunOutput, Stack},
};

/// plotly.js version the `plotly` crate (0.14) renders against; pin the same
/// one in the page `<head>` so the inline figures find a compatible global.
const PLOTLY_CDN: &str = "https://cdn.plot.ly/plotly-3.0.1.min.js";

/// Render every run into one self-contained HTML report titled `title`.
#[must_use]
pub fn render_html(runs: &[RunOutput], title: &str) -> String {
    let cells = aggregate_cells(runs);
    let ts = averaged_timeseries(runs);

    let mut body = String::new();
    if cells.is_empty() {
        body.push_str("<p>No runs to report.</p>\n");
        return wrap_page(title, &body);
    }

    body.push_str("<h2>Summary — mean ± stddev across runs</h2>\n");
    for m in scalar_metrics() {
        let plot = bar_chart(&cells, &m);
        body.push_str("<div class=\"plot\">");
        body.push_str(&plot.to_inline_html(Some(&format!("bar-{}", m.key))));
        body.push_str("</div>\n");
    }

    if !ts.is_empty() {
        body.push_str("<h2>Over the run — averaged across runs</h2>\n");
        body.push_str(&timeseries_charts(&ts));
    }

    wrap_page(title, &body)
}

/// Render an HTML *fragment* (no `<html>` wrapper) for embedding in the Zola
/// website: the averaged scalar summary, then — per scenario — throughput,
/// CPU and memory over the run window with every individual run drawn faintly
/// plus a bold across-run mean line. `tagged` pairs each run with its
/// `runNN` tag (parsed from the result filename) so per-run traces are named.
#[must_use]
pub fn render_web_fragment(tagged: &[(String, RunOutput)]) -> String {
    let runs: Vec<RunOutput> = tagged.iter().map(|(_, r)| r.clone()).collect();
    let cells = aggregate_cells(&runs);
    if cells.is_empty() {
        return "<p>No benchmark runs available yet.</p>\n".to_string();
    }
    let ts = averaged_timeseries(&runs);

    let mut out = String::new();
    out.push_str(&format!(
        "<script src=\"{PLOTLY_CDN}\" charset=\"utf-8\"></script>\n"
    ));

    out.push_str(
        "<h2 id=\"summary\">Summary — mean across runs (error bars = run-to-run stddev)</h2>\n",
    );
    for m in scalar_metrics() {
        let plot = bar_chart(&cells, &m);
        out.push_str("<div class=\"bench-plot\">");
        out.push_str(&plot.to_inline_html(Some(&format!("bench-bar-{}", m.key))));
        out.push_str("</div>\n");
    }

    out.push_str(
        "<h2 id=\"per-run\">Per run &amp; averaged — throughput, CPU and memory over the run</h2>\n",
    );
    // Group runs into cells, preserving each run's tag for the faint per-run lines.
    let mut by_cell: BTreeMap<(&str, u32), Vec<(&str, &RunOutput)>> = BTreeMap::new();
    for (tag, r) in tagged {
        by_cell
            .entry((r.scenario.name.as_str(), r.topology.broker_count))
            .or_default()
            .push((tag.as_str(), r));
    }
    for (&(scenario, brokers), cell_runs) in &by_cell {
        out.push_str(&format!(
            "<h3>{} @ {brokers} brokers</h3>\n",
            escape(scenario)
        ));
        for wm in &web_metrics() {
            let plot = per_run_chart(scenario, brokers, cell_runs, wm, &ts);
            let id = sanitize_id(&format!("bench-ts-{scenario}-{brokers}-{}", wm.avg_key));
            out.push_str("<div class=\"bench-plot\">");
            out.push_str(&plot.to_inline_html(Some(&id)));
            out.push_str("</div>\n");
        }
    }
    out
}

/// A website time-series metric: the matching [`averaged_timeseries`] key (for
/// the bold mean line), a label, and how to pull a run's `(t_offset_ms, value)`
/// points (for the faint per-run lines).
struct WebMetric {
    avg_key: &'static str,
    label: &'static str,
    per_run: fn(&RunOutput) -> Vec<(u64, f64)>,
}

fn web_metrics() -> Vec<WebMetric> {
    vec![
        WebMetric {
            avg_key: "producer_msgs_per_sec",
            label: "Throughput (producer msgs/s)",
            per_run: |r| {
                r.samples
                    .iter()
                    .map(|s| (s.t_offset_ms, s.producer_msgs_per_sec))
                    .collect()
            },
        },
        WebMetric {
            avg_key: "broker_cpu_cores",
            label: "Broker CPU (cores)",
            per_run: |r| {
                r.broker_samples
                    .iter()
                    .map(|b| (b.t_offset_ms, b.cpu_cores))
                    .collect()
            },
        },
        WebMetric {
            avg_key: "broker_mem_working_set_mb",
            label: "Broker memory (MiB)",
            per_run: |r| {
                r.broker_samples
                    .iter()
                    .map(|b| (b.t_offset_ms, b.mem_working_set_bytes as f64 / 1_048_576.0))
                    .collect()
            },
        },
    ]
}

/// `(bold mean line colour, faint per-run line colour)` per stack.
fn stack_colors(s: Stack) -> (&'static str, &'static str) {
    match s {
        Stack::Crabka => ("rgb(234,88,12)", "rgba(234,88,12,0.20)"),
        Stack::Kafka => ("rgb(37,99,235)", "rgba(37,99,235,0.20)"),
    }
}

/// One chart for `wm` in one cell: every run drawn as a faint line (hidden from
/// the legend) plus a bold across-run mean line per stack.
fn per_run_chart(
    scenario: &str,
    brokers: u32,
    cell_runs: &[(&str, &RunOutput)],
    wm: &WebMetric,
    ts: &[TsSeries],
) -> Plot {
    let mut plot = Plot::new();
    for stack in [Stack::Crabka, Stack::Kafka] {
        let (mean_color, faint_color) = stack_colors(stack);

        for (tag, run) in cell_runs.iter().filter(|(_, r)| r.stack == stack) {
            let pts = (wm.per_run)(run);
            if pts.is_empty() {
                continue;
            }
            let x: Vec<f64> = pts.iter().map(|(t, _)| *t as f64 / 1000.0).collect();
            let y: Vec<f64> = pts.iter().map(|(_, v)| *v).collect();
            plot.add_trace(
                Scatter::new(x, y)
                    .mode(Mode::Lines)
                    .name(format!("{} {tag}", stack_name(stack)))
                    .legend_group(stack_name(stack))
                    .show_legend(false)
                    .line(Line::new().color(faint_color).width(1.0)),
            );
        }

        if let Some(series) = ts.iter().find(|s| {
            s.scenario == scenario
                && s.broker_count == brokers
                && s.stack == stack
                && s.metric == wm.avg_key
        }) {
            let x: Vec<f64> = series
                .points
                .iter()
                .map(|p| p.t_offset_ms as f64 / 1000.0)
                .collect();
            let y: Vec<f64> = series.points.iter().map(|p| p.mean).collect();
            let n = series.points.iter().map(|p| p.n).max().unwrap_or(0);
            plot.add_trace(
                Scatter::new(x, y)
                    .mode(Mode::Lines)
                    .name(format!("{} (mean of {n})", stack_name(stack)))
                    .legend_group(stack_name(stack))
                    .line(Line::new().color(mean_color).width(3.0)),
            );
        }
    }
    plot.set_layout(
        Layout::new()
            .title(Title::with_text(format!("{scenario} — {}", wm.label)))
            .height(360)
            .x_axis(Axis::new().title(Title::with_text("seconds into run")))
            .y_axis(Axis::new().title(Title::with_text(wm.label))),
    );
    plot
}

/// One grouped bar chart for metric `m`: a crabka and a kafka bar per cell,
/// each carrying the across-run sample-stddev as a symmetric error bar.
fn bar_chart(cells: &[CellAgg], m: &ScalarMetric) -> Plot {
    let labels: Vec<String> = cells.iter().map(|c| c.scenario.clone()).collect();

    let stack_series = |pick: fn(&CellAgg) -> &crate::aggregate::StackAgg| {
        let mut means = Vec::with_capacity(cells.len());
        let mut errs = Vec::with_capacity(cells.len());
        for c in cells {
            let s = pick(c).metrics.get(m.key).copied().unwrap_or_default();
            means.push(s.mean);
            errs.push(s.stddev);
        }
        (means, errs)
    };
    let (cy, ce) = stack_series(|c| &c.crabka);
    let (ky, ke) = stack_series(|c| &c.kafka);

    let mut plot = Plot::new();
    plot.add_trace(
        Bar::new(labels.clone(), cy)
            .name("crabka")
            .error_y(ErrorData::new(ErrorType::Data).array(ce)),
    );
    plot.add_trace(
        Bar::new(labels, ky)
            .name("kafka")
            .error_y(ErrorData::new(ErrorType::Data).array(ke)),
    );
    plot.set_layout(
        Layout::new()
            .title(Title::with_text(format!("{} ({})", m.label, m.unit)))
            .bar_mode(BarMode::Group)
            .height(420)
            .y_axis(Axis::new().title(Title::with_text(m.unit))),
    );
    plot
}

/// One line chart per `(scenario, metric)`: a crabka and a kafka line of the
/// across-run-averaged value over the run.
fn timeseries_charts(ts: &[TsSeries]) -> String {
    let mut groups: BTreeMap<(&str, u32, &'static str), Vec<&TsSeries>> = BTreeMap::new();
    for s in ts {
        groups
            .entry((s.scenario.as_str(), s.broker_count, s.metric))
            .or_default()
            .push(s);
    }

    let mut out = String::new();
    for (&(scenario, brokers, metric), series) in &groups {
        let label = ts_metric_label(metric);
        let mut plot = Plot::new();
        for s in series {
            let x: Vec<f64> = s
                .points
                .iter()
                .map(|p| p.t_offset_ms as f64 / 1000.0)
                .collect();
            let y: Vec<f64> = s.points.iter().map(|p| p.mean).collect();
            plot.add_trace(
                Scatter::new(x, y)
                    .name(stack_name(s.stack))
                    .mode(Mode::Lines),
            );
        }
        plot.set_layout(
            Layout::new()
                .title(Title::with_text(format!("{scenario} — {label}")))
                .height(340)
                .x_axis(Axis::new().title(Title::with_text("seconds into run")))
                .y_axis(Axis::new().title(Title::with_text(label))),
        );
        let id = sanitize_id(&format!("ts-{scenario}-{brokers}-{metric}"));
        out.push_str("<div class=\"plot\">");
        out.push_str(&plot.to_inline_html(Some(&id)));
        out.push_str("</div>\n");
    }
    out
}

fn stack_name(s: Stack) -> &'static str {
    match s {
        Stack::Crabka => "crabka",
        Stack::Kafka => "kafka",
    }
}

/// Human label for a time-series metric key. The keys are a closed set
/// (see [`crate::aggregate`]); the fallback is unreachable in practice.
fn ts_metric_label(key: &str) -> &'static str {
    match key {
        "producer_msgs_per_sec" => "Producer throughput (msgs/s)",
        "consumer_msgs_per_sec" => "Consumer throughput (msgs/s)",
        "producer_p99_ms" => "Producer ack p99 (ms)",
        "consumer_e2e_p99_ms" => "Consumer e2e p99 (ms)",
        "broker_cpu_cores" => "Broker CPU (cores)",
        "broker_mem_working_set_mb" => "Broker working set (MiB)",
        _ => "value",
    }
}

/// Reduce an arbitrary string to a safe HTML id (alnum + dashes).
fn sanitize_id(s: &str) -> String {
    s.chars()
        .map(|c| if c.is_ascii_alphanumeric() { c } else { '-' })
        .collect()
}

fn wrap_page(title: &str, body: &str) -> String {
    let t = escape(title);
    format!(
        "<!doctype html>\n<html lang=\"en\">\n<head>\n<meta charset=\"utf-8\">\n\
<title>{t}</title>\n<script src=\"{PLOTLY_CDN}\" charset=\"utf-8\"></script>\n\
<style>body{{font-family:system-ui,sans-serif;margin:24px;max-width:1100px}}\
h1{{font-size:1.6rem}}h2{{margin-top:2rem;border-bottom:1px solid #ddd;padding-bottom:4px}}\
.plot{{margin:10px 0}}</style>\n</head>\n<body>\n<h1>{t}</h1>\n{body}\n</body>\n</html>\n"
    )
}

/// Minimal HTML-text escaping for trusted scenario/metric labels.
fn escape(s: &str) -> String {
    s.replace('&', "&amp;")
        .replace('<', "&lt;")
        .replace('>', "&gt;")
}

#[cfg(test)]
mod tests {
    use assert2::assert;

    use super::*;
    use crate::scenario::{
        Acks, Compression, LatencyPercentiles, LoadMode, ModeTag, Resource, Sample, Scenario,
        Throughput, Topology,
    };

    fn run(
        stack: Stack,
        scenario: &str,
        prod_mps: f64,
        p99: f64,
        samples: Vec<Sample>,
    ) -> RunOutput {
        RunOutput {
            scenario: Scenario {
                name: scenario.into(),
                mode_tag: ModeTag::Cluster,
                msg_size_bytes: 100,
                key_size_bytes: 0,
                partitions: 100,
                replication_factor: 3,
                producers: 1,
                consumers: 1,
                mode: LoadMode::Saturate,
                acks: Acks::Leader,
                compression: Compression::None,
                linger_ms: 5,
                batch_size: 16384,
                duration_s: 60,
                warmup_s: 10,
                failover: None,
            },
            stack,
            topology: Topology {
                partitions: 100,
                replication_factor: 3,
                broker_count: 6,
            },
            wallclock_start_unix_ms: 0,
            wallclock_end_unix_ms: 60_000,
            throughput: Throughput {
                producer_msgs_per_sec: prod_mps,
                ..Throughput::default()
            },
            producer_latency_ms: LatencyPercentiles {
                p99_ms: p99,
                ..LatencyPercentiles::default()
            },
            consumer_e2e_latency_ms: LatencyPercentiles::default(),
            resource: Resource::default(),
            disturbance: None,
            startup_ms: None,
            first_ack_ms: 0,
            errors: vec![],
            notes: vec![],
            samples,
            broker_samples: vec![],
        }
    }

    fn sample(t: u64, prod_mps: f64) -> Sample {
        Sample {
            t_offset_ms: t,
            producer_msgs_per_sec: prod_mps,
            consumer_msgs_per_sec: prod_mps * 0.9,
            producer_p50_ms: 1.0,
            producer_p99_ms: 4.0,
            consumer_e2e_p99_ms: 7.0,
        }
    }

    #[test]
    fn render_html_embeds_library_bars_and_timeseries() {
        let runs = vec![
            run(
                Stack::Crabka,
                "small-msg",
                1000.0,
                5.0,
                vec![sample(0, 1000.0), sample(2000, 1100.0)],
            ),
            run(
                Stack::Kafka,
                "small-msg",
                800.0,
                7.0,
                vec![sample(0, 800.0), sample(2000, 820.0)],
            ),
            run(Stack::Crabka, "fan-out", 500.0, 9.0, vec![]),
            run(Stack::Kafka, "fan-out", 400.0, 11.0, vec![]),
        ];
        let html = render_html(&runs, "Crabka vs Strimzi");

        // Valid page that loads the matching plotly.js and embeds real figures,
        // represents both scenarios and both stacks, and carries a headline bar
        // metric plus a time-series chart.
        for needle in [
            "<html",
            "cdn.plot.ly/plotly-3.0.1",
            "Plotly.newPlot",
            "Crabka vs Strimzi",
            "small-msg",
            "fan-out",
            "crabka",
            "kafka",
            "Producer throughput",
            "seconds into run",
        ] {
            assert!(html.contains(needle), "missing {needle:?}");
        }
    }

    #[test]
    fn render_html_empty_runs_is_a_valid_page() {
        let html = render_html(&[], "Empty");
        assert!(html.contains("<html") && html.contains("Empty"));
        assert!(html.contains("No runs"));
    }

    #[test]
    fn web_fragment_has_per_run_and_mean_for_cpu_mem_throughput() {
        use crate::scenario::BrokerSample;
        let mk = |stack, prod: f64, cpu: f64, mem: u64| {
            let mut r = run(
                stack,
                "small-msg",
                prod,
                5.0,
                vec![sample(0, prod), sample(2000, prod * 1.1)],
            );
            r.broker_samples = vec![
                BrokerSample {
                    t_offset_ms: 0,
                    cpu_cores: cpu,
                    mem_working_set_bytes: mem,
                },
                BrokerSample {
                    t_offset_ms: 2000,
                    cpu_cores: cpu,
                    mem_working_set_bytes: mem,
                },
            ];
            r
        };
        let tagged = vec![
            (
                "run01".to_string(),
                mk(Stack::Crabka, 1000.0, 2.0, 300 * 1_048_576),
            ),
            (
                "run02".to_string(),
                mk(Stack::Crabka, 1200.0, 2.2, 320 * 1_048_576),
            ),
            (
                "run01".to_string(),
                mk(Stack::Kafka, 800.0, 3.5, 2000 * 1_048_576),
            ),
            (
                "run02".to_string(),
                mk(Stack::Kafka, 820.0, 3.6, 2100 * 1_048_576),
            ),
        ];
        let html = render_web_fragment(&tagged);
        // Loads plotly, charts throughput/CPU/memory per run for both stacks,
        // and labels the bold mean line with the run count ("mean of 2").
        for needle in [
            "cdn.plot.ly/plotly-3.0.1",
            "Plotly.newPlot",
            "Per run",
            "small-msg",
            "Throughput",
            "Broker CPU",
            "Broker memory",
            "mean of 2",
            "crabka",
            "kafka",
        ] {
            assert!(html.contains(needle), "missing {needle:?}");
        }
    }

    #[test]
    fn web_fragment_empty_is_graceful() {
        assert!(render_web_fragment(&[]).contains("No benchmark runs"));
    }
}
