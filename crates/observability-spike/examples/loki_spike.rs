//! Run the throwaway Loki storage/query spike.
//!
//! This writes one tiny Parquet log block, registers it with `DataFusion`, runs a
//! LogQL-shaped SQL filter, and emits Loki's `streams` response shape.

use std::{fs::File, sync::Arc};

use crabka_logql::{LineFilterOp, MatchOp, PipelineStage, parse_query};
use crabka_observability_spike::{
    Labels, LogEntry, LogSelector, labels, loki_streams_response, series_fingerprint,
};
use datafusion::{
    arrow::{
        array::{
            Array, Int64Array, LargeStringArray, RecordBatch, StringArray, StringViewArray,
            UInt64Array,
        },
        datatypes::{DataType, Field, Schema},
    },
    parquet::arrow::ArrowWriter,
    prelude::{ParquetReadOptions, SessionContext},
};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let dir = tempfile::tempdir()?;
    let path = dir.path().join("logs.parquet");

    let api_labels = labels([("app", "api"), ("env", "prod")]);
    let worker_labels = labels([("app", "worker"), ("env", "prod")]);
    let api_fp = series_fingerprint(&api_labels);
    let worker_fp = series_fingerprint(&worker_labels);

    let schema = Arc::new(Schema::new(vec![
        Field::new("series_fingerprint", DataType::UInt64, false),
        Field::new("timestamp_ns", DataType::Int64, false),
        Field::new("line", DataType::Utf8, false),
        Field::new("app", DataType::Utf8, false),
        Field::new("env", DataType::Utf8, false),
    ]));
    let batch = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![
            Arc::new(UInt64Array::from(vec![api_fp, api_fp, worker_fp])),
            Arc::new(Int64Array::from(vec![10_i64, 20, 15])),
            Arc::new(StringArray::from(vec![
                "ok",
                "error: boom",
                "error: hidden",
            ])),
            Arc::new(StringArray::from(vec!["api", "api", "worker"])),
            Arc::new(StringArray::from(vec!["prod", "prod", "prod"])),
        ],
    )?;

    let mut writer = ArrowWriter::try_new(File::create(&path)?, schema, None)?;
    writer.write(&batch)?;
    writer.close()?;

    let ctx = SessionContext::new();
    ctx.register_parquet(
        "logs",
        path.to_str().ok_or("non-utf8 parquet path")?,
        ParquetReadOptions::default(),
    )
    .await?;
    let batches = ctx
        .sql(
            "select timestamp_ns, line \
             from logs \
             where app = 'api' and line like '%error%' \
             order by timestamp_ns",
        )
        .await?
        .collect()
        .await?;

    let mut entries = Vec::new();
    for batch in &batches {
        let timestamps = batch
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .ok_or("timestamp_ns column was not Int64")?;
        for row in 0..batch.num_rows() {
            entries.push(LogEntry::new(
                timestamps.value(row),
                api_labels.clone(),
                string_value(batch.column(1).as_ref(), row)?,
            ));
        }
    }

    let query = r#"{app="api", env!="dev"} |= "error""#;
    let response = loki_streams_response(&entries, &spike_selector(query)?, 0, 30);
    println!("{}", serde_json::to_string_pretty(&response)?);
    println!(
        "GO: parsed LogQL -> Parquet block -> DataFusion filter -> Loki streams JSON returned {} row(s)",
        entries.len()
    );

    Ok(())
}

fn spike_selector(query: &str) -> Result<LogSelector, Box<dyn std::error::Error>> {
    let parsed = parse_query(query)?;
    let mut labels = Vec::new();
    for matcher in parsed.matchers {
        match matcher.op {
            MatchOp::Equal => labels.push((matcher.name, matcher.value)),
            MatchOp::NotEqual => {}
            MatchOp::RegexEqual | MatchOp::RegexNotEqual => {
                return Err("spike example only lowers exact label matchers".into());
            }
        }
    }
    let mut selector = LogSelector::new(labels.into_iter().collect::<Labels>());
    for stage in parsed.pipeline {
        match stage {
            PipelineStage::LineFilter(filter) if filter.op == LineFilterOp::Contains => {
                selector = selector.contains(filter.pattern);
            }
            PipelineStage::LineFilter(_) => {
                return Err("spike example only lowers contains line filters".into());
            }
            PipelineStage::Parser(_) | PipelineStage::FieldFilter(_) => {
                return Err("spike example does not lower parser or field-filter stages".into());
            }
            _ => return Err("spike example only lowers contains line filters".into()),
        }
    }
    Ok(selector)
}

fn string_value(array: &dyn Array, row: usize) -> Result<&str, Box<dyn std::error::Error>> {
    if let Some(strings) = array.as_any().downcast_ref::<StringArray>() {
        return Ok(strings.value(row));
    }
    if let Some(strings) = array.as_any().downcast_ref::<LargeStringArray>() {
        return Ok(strings.value(row));
    }
    if let Some(strings) = array.as_any().downcast_ref::<StringViewArray>() {
        return Ok(strings.value(row));
    }
    Err(format!(
        "line column used unsupported Arrow type {:?}",
        array.data_type()
    )
    .into())
}
