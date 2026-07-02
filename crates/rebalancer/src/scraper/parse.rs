//! Scoped `OpenMetrics` text parser. Recognizes only three families:
//! `crabka_broker_partition_bytes_in_total`,
//! `crabka_broker_partition_bytes_out_total`, and
//! `crabka_broker_partition_disk_bytes`. Everything else is silently
//! skipped — no allocation, no panic.

#[derive(Debug, Clone, Copy, Hash, PartialEq, Eq)]
pub enum MetricKind {
    BytesIn,
    BytesOut,
    DiskBytes,
    CpuMicros,
}

#[derive(Debug, Clone, PartialEq)]
pub struct ParsedSample {
    pub metric: MetricKind,
    pub topic: String,
    pub partition: i32,
    pub value: f64,
}

#[must_use]
pub fn parse(text: &str) -> Vec<ParsedSample> {
    let mut out = Vec::new();
    for line in text.lines() {
        let trimmed = line.trim();
        if trimmed.is_empty() {
            continue;
        }
        if trimmed.starts_with('#') {
            continue;
        }
        let Some(sample) = parse_line(trimmed) else {
            continue;
        };
        out.push(sample);
    }
    out
}

fn parse_line(line: &str) -> Option<ParsedSample> {
    // Expected shape: <name>{<labels>} <value> [<timestamp>]
    let (name_and_labels, value_str) = line.rsplit_once(char::is_whitespace)?;
    // The "value" portion may have a trailing timestamp; split again on whitespace.
    let value_str = value_str.split_whitespace().next()?;
    let value: f64 = value_str.parse().ok()?;

    let (name, labels) = name_and_labels.split_once('{')?;
    let labels = labels.strip_suffix('}')?;

    let metric = match name {
        "crabka_broker_partition_bytes_in_total" => MetricKind::BytesIn,
        "crabka_broker_partition_bytes_out_total" => MetricKind::BytesOut,
        "crabka_broker_partition_disk_bytes" => MetricKind::DiskBytes,
        "crabka_broker_partition_cpu_micros_total" => MetricKind::CpuMicros,
        _ => return None,
    };

    let mut topic: Option<String> = None;
    let mut partition: Option<i32> = None;
    for pair in labels.split(',') {
        let (k, v) = pair.split_once('=')?;
        let v = v
            .trim()
            .strip_prefix('"')
            .and_then(|s| s.strip_suffix('"'))?;
        match k.trim() {
            "topic" => topic = Some(v.to_string()),
            "partition" => partition = v.parse().ok(),
            _ => {} // ignore unknown label
        }
    }

    Some(ParsedSample {
        metric,
        topic: topic?,
        partition: partition?,
        value,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use assert2::{assert, check};

    #[test]
    fn empty_input_returns_empty() {
        assert!(parse("").is_empty());
    }

    #[test]
    fn skips_blank_and_comment_lines_before_parsing() {
        let txt = r#"

  # crabka_broker_partition_bytes_in_total{topic="ignored",partition="0"} 999
crabka_broker_partition_bytes_in_total{topic="kept",partition="1"} 7
"#;
        let out = parse(txt);
        assert!(out.len() == 1);
        check!(out[0].topic == "kept");
        check!(out[0].partition == 1);
        check!((out[0].value - 7.0).abs() < 1e-9);
    }

    #[test]
    fn parses_well_formed_counters() {
        for (txt, want_metric) in [
            (
                "crabka_broker_partition_bytes_in_total{topic=\"t\",partition=\"0\"} 1024\n",
                MetricKind::BytesIn,
            ),
            (
                "crabka_broker_partition_cpu_micros_total{topic=\"t\",partition=\"0\"} 1024\n",
                MetricKind::CpuMicros,
            ),
        ] {
            let out = parse(txt);
            assert!(out.len() == 1);
            check!(out[0].metric == want_metric);
            check!(out[0].topic == "t");
            check!(out[0].partition == 0);
            check!((out[0].value - 1024.0).abs() < 1e-9);
        }
    }

    #[test]
    fn parses_a_gauge() {
        let txt = r#"crabka_broker_partition_disk_bytes{topic="t",partition="5"} 1234567
"#;
        let out = parse(txt);
        assert!(out.len() == 1);
        check!(out[0].metric == MetricKind::DiskBytes);
        check!(out[0].partition == 5);
        check!((out[0].value - 1_234_567.0).abs() < 1e-3);
    }

    #[test]
    fn mixed_metrics_only_known_families_surface() {
        let txt = r#"# HELP foo
# TYPE crabka_broker_partition_bytes_in_total counter
crabka_broker_partition_bytes_in_total{topic="t",partition="0"} 1
crabka_broker_topic_bytes_in_total{topic="t"} 999
some_other_metric 7
crabka_broker_partition_bytes_out_total{topic="t",partition="0"} 2
crabka_broker_partition_cpu_micros_total{topic="t",partition="0"} 42
"#;
        let out = parse(txt);
        assert!(out.len() == 3);
        check!(out[0].metric == MetricKind::BytesIn);
        check!(out[1].metric == MetricKind::BytesOut);
        check!(out[2].metric == MetricKind::CpuMicros);
    }

    #[test]
    fn malformed_line_is_skipped() {
        let txt = "crabka_broker_partition_bytes_in_total{nope this is broken\n";
        assert!(parse(txt).is_empty());
    }

    #[test]
    fn missing_partition_label_is_skipped() {
        let txt = "crabka_broker_partition_bytes_in_total{topic=\"t\"} 1024\n";
        assert!(parse(txt).is_empty(), "missing partition label must skip");
    }
}
