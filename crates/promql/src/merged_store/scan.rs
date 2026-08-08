//! Scan-table merging for [`super::MergedMetricStore`].

use std::sync::Arc;

use crabka_metrics::{COL_FINGERPRINT, COL_TIMESTAMP};
use datafusion::{catalog::MemTable, prelude::SessionContext};

use crate::PromqlError;

pub(super) const FLOAT_TABLE: &str = "merged_float_samples";
pub(super) const HISTOGRAM_TABLE: &str = "merged_native_histograms";

pub(super) async fn merge_scan_table<const N: usize>(
    ctx: &SessionContext,
    table_name: &str,
    schema: arrow::datatypes::SchemaRef,
    scans: [(SessionContext, Option<String>); N],
) -> Result<Option<String>, PromqlError> {
    // Register each non-empty source's batches under a private alias in the
    // output context, tagged with a `__src` priority literal. The `scans` array
    // is ordered `[cold, hot]`, so the array index doubles as the priority:
    // hot (higher index) is authoritative when both stores hold the same
    // `(fingerprint, timestamp)` sample. Without this dedup, any sample present
    // in both stores - the steady state, since hot retention is time-based and
    // independent of compaction - is double-counted by range/rate/aggregate
    // queries.
    let mut sources = Vec::new();
    for (priority, (scan_ctx, table)) in scans.into_iter().enumerate() {
        let Some(table) = table else {
            continue;
        };
        let dataframe = scan_ctx.sql(&format!("SELECT * FROM {table}")).await?;
        let batches = dataframe.collect().await?;
        if batches.is_empty() {
            continue;
        }
        let source_table = MemTable::try_new(schema.clone(), vec![batches])?;
        let source_name = format!("{table_name}__src{priority}");
        ctx.register_table(source_name.as_str(), Arc::new(source_table))?;
        sources.push((priority, source_name));
    }
    if sources.is_empty() {
        return Ok(None);
    }

    // Project the real schema columns explicitly so the `__src` helper column
    // never escapes into the output (which must equal the passed-in schema).
    let projection = schema
        .fields()
        .iter()
        .map(|field| quote_ident(field.name()))
        .collect::<Vec<_>>()
        .join(", ");
    let fp_col = quote_ident(COL_FINGERPRINT);
    let ts_col = quote_ident(COL_TIMESTAMP);

    // UNION ALL the tagged sources, then keep exactly one row per
    // `(fingerprint, timestamp)`, preferring the highest-priority source (hot).
    let union = sources
        .iter()
        .map(|(priority, name)| {
            format!(
                "SELECT *, {priority} AS __src FROM {name}",
                name = quote_ident(name)
            )
        })
        .collect::<Vec<_>>()
        .join(" UNION ALL ");
    let deduped = format!(
        "SELECT {projection} FROM (\
            SELECT *, ROW_NUMBER() OVER (\
                PARTITION BY {fp_col}, {ts_col} ORDER BY __src DESC\
            ) AS __rn FROM ({union}) AS __tagged\
        ) AS __ranked WHERE __rn = 1"
    );
    let dataframe = ctx.sql(&deduped).await?;
    let batches = dataframe.collect().await?;

    // Drop the private source aliases and register the deduped result under the
    // public table name so the output schema exactly equals the input schema.
    for (_, source_name) in &sources {
        ctx.deregister_table(source_name.as_str())?;
    }
    let table = MemTable::try_new(schema, vec![batches])?;
    ctx.register_table(table_name, Arc::new(table))?;
    Ok(Some(table_name.to_string()))
}

/// Quotes a SQL identifier for safe interpolation into a `DataFusion` query.
fn quote_ident(ident: &str) -> String {
    format!("\"{}\"", ident.replace('"', "\"\""))
}
