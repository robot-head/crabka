//! Flamegraph merge engine.

use std::collections::BTreeMap;
use std::sync::Arc;

use arrow::array::AsArray;
use arrow::datatypes::{Int64Type, UInt64Type};
use crabka_blockstore::{LabelMatcher, SeriesMatchOp as MatchOp};

use crate::{
    FlameGraph, FlameGraphDiff, Frame, Heatmap, LabeledHeatmap, ProfileError, ProfileStore,
    ProfileType, Series, SeriesAgg, Tree, bin_heatmap, diff_trees,
    samples::{
        COL_FINGERPRINT, COL_TIMESTAMP, PCOL_SPAN_ID, PCOL_STACKTRACE_ID,
        PCOL_STACKTRACE_PARTITION, PCOL_TOTAL_VALUE, PCOL_VALUE,
    },
    series::{fold_bucket, step_bucket_ms, step_ms_from_secs},
    tree_to_pprof, tree_to_pprof_with_max_nodes,
};

/// Engine configuration.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct EngineOpts {
    pub default_max_nodes: i64,
}

impl Default for EngineOpts {
    fn default() -> Self {
        Self {
            default_max_nodes: 2048,
        }
    }
}

/// Profiles flamegraph engine.
pub struct FlameEngine<S: ProfileStore> {
    store: Arc<S>,
    opts: EngineOpts,
}

impl<S: ProfileStore> FlameEngine<S> {
    #[must_use]
    pub fn new(store: Arc<S>, opts: EngineOpts) -> Self {
        Self { store, opts }
    }

    pub async fn select_merge_stacktraces(
        &self,
        tenant: &str,
        profile_type: &str,
        label_selector: &str,
        start_ms: i64,
        end_ms: i64,
        max_nodes: i64,
    ) -> Result<FlameGraph, ProfileError> {
        let tree = self
            .merge_to_tree(
                tenant,
                profile_type,
                label_selector,
                start_ms,
                end_ms,
                None,
                &[],
            )
            .await?;
        let max_nodes = if max_nodes > 0 {
            max_nodes
        } else {
            self.opts.default_max_nodes
        };
        Ok(tree.to_flamegraph(max_nodes))
    }

    #[allow(clippy::too_many_arguments)]
    pub async fn select_merge_stacktraces_grouped(
        &self,
        tenant: &str,
        profile_type: &str,
        label_selector: &str,
        start_ms: i64,
        end_ms: i64,
        max_nodes: i64,
        group_by: &[String],
    ) -> Result<FlameGraph, ProfileError> {
        if group_by.is_empty() {
            return self
                .select_merge_stacktraces(
                    tenant,
                    profile_type,
                    label_selector,
                    start_ms,
                    end_ms,
                    max_nodes,
                )
                .await;
        }
        let base_matchers = crate::matcher::parse_label_selector(label_selector)?;
        let groups = self
            .store
            .series(tenant, &base_matchers, group_by, start_ms, end_ms)
            .await?;
        let mut tree = Tree::new();
        for labels in groups {
            let mut matchers = base_matchers.clone();
            matchers.extend(
                labels.iter().map(|(name, value)| {
                    LabelMatcher::new(name.clone(), MatchOp::Eq, value.clone())
                }),
            );
            let scan = self
                .store
                .select(tenant, profile_type, &matchers, start_ms, end_ms)
                .await?;
            let prefix = vec![Frame {
                function: group_frame_name(&labels),
                file: String::new(),
                line: 0,
            }];
            merge_scan_to_tree(&scan, &mut tree, &prefix, None, &[]).await?;
        }
        let max_nodes = if max_nodes > 0 {
            max_nodes
        } else {
            self.opts.default_max_nodes
        };
        Ok(tree.to_flamegraph(max_nodes))
    }

    #[allow(clippy::too_many_arguments)]
    pub async fn select_merge_stacktraces_with_stack_trace_selector(
        &self,
        tenant: &str,
        profile_type: &str,
        label_selector: &str,
        start_ms: i64,
        end_ms: i64,
        max_nodes: i64,
        call_sites: &[String],
    ) -> Result<FlameGraph, ProfileError> {
        let tree = self
            .merge_to_tree(
                tenant,
                profile_type,
                label_selector,
                start_ms,
                end_ms,
                None,
                call_sites,
            )
            .await?;
        let max_nodes = if max_nodes > 0 {
            max_nodes
        } else {
            self.opts.default_max_nodes
        };
        Ok(tree.to_flamegraph(max_nodes))
    }

    #[allow(clippy::too_many_arguments)]
    pub async fn select_merge_stacktraces_tree_with_stack_trace_selector(
        &self,
        tenant: &str,
        profile_type: &str,
        label_selector: &str,
        start_ms: i64,
        end_ms: i64,
        max_nodes: i64,
        call_sites: &[String],
    ) -> Result<Vec<u8>, ProfileError> {
        let tree = self
            .merge_to_tree(
                tenant,
                profile_type,
                label_selector,
                start_ms,
                end_ms,
                None,
                call_sites,
            )
            .await?;
        let max_nodes = if max_nodes > 0 {
            max_nodes
        } else {
            self.opts.default_max_nodes
        };
        Ok(tree.to_pyroscope_tree_bytes(max_nodes))
    }

    pub async fn select_merge_stacktraces_sharded(
        &self,
        tenant: &str,
        profile_type: &str,
        label_selector: &str,
        ranges: &[(i64, i64)],
        max_nodes: i64,
    ) -> Result<FlameGraph, ProfileError> {
        if ranges.is_empty() {
            return Err(ProfileError::Plan(
                "sharded stacktrace query requires at least one time range".to_string(),
            ));
        }
        let mut merged = Tree::new();
        for (start_ms, end_ms) in ranges {
            validate_range(*start_ms, *end_ms)?;
            let tree = self
                .merge_to_tree(
                    tenant,
                    profile_type,
                    label_selector,
                    *start_ms,
                    *end_ms,
                    None,
                    &[],
                )
                .await?;
            merged.merge(tree);
        }
        let max_nodes = if max_nodes > 0 {
            max_nodes
        } else {
            self.opts.default_max_nodes
        };
        Ok(merged.to_flamegraph(max_nodes))
    }

    pub async fn select_merge_stacktraces_with_stack_trace_selector_sharded(
        &self,
        tenant: &str,
        profile_type: &str,
        label_selector: &str,
        ranges: &[(i64, i64)],
        max_nodes: i64,
        call_sites: &[String],
    ) -> Result<FlameGraph, ProfileError> {
        if ranges.is_empty() {
            return Err(ProfileError::Plan(
                "sharded stacktrace query requires at least one time range".to_string(),
            ));
        }
        let mut merged = Tree::new();
        for (start_ms, end_ms) in ranges {
            validate_range(*start_ms, *end_ms)?;
            let tree = self
                .merge_to_tree(
                    tenant,
                    profile_type,
                    label_selector,
                    *start_ms,
                    *end_ms,
                    None,
                    call_sites,
                )
                .await?;
            merged.merge(tree);
        }
        let max_nodes = if max_nodes > 0 {
            max_nodes
        } else {
            self.opts.default_max_nodes
        };
        Ok(merged.to_flamegraph(max_nodes))
    }

    pub async fn select_merge_stacktraces_tree_with_stack_trace_selector_sharded(
        &self,
        tenant: &str,
        profile_type: &str,
        label_selector: &str,
        ranges: &[(i64, i64)],
        max_nodes: i64,
        call_sites: &[String],
    ) -> Result<Vec<u8>, ProfileError> {
        if ranges.is_empty() {
            return Err(ProfileError::Plan(
                "sharded stacktrace query requires at least one time range".to_string(),
            ));
        }
        let mut merged = Tree::new();
        for (start_ms, end_ms) in ranges {
            validate_range(*start_ms, *end_ms)?;
            let tree = self
                .merge_to_tree(
                    tenant,
                    profile_type,
                    label_selector,
                    *start_ms,
                    *end_ms,
                    None,
                    call_sites,
                )
                .await?;
            merged.merge(tree);
        }
        let max_nodes = if max_nodes > 0 {
            max_nodes
        } else {
            self.opts.default_max_nodes
        };
        Ok(merged.to_pyroscope_tree_bytes(max_nodes))
    }

    #[allow(clippy::too_many_arguments)]
    pub(crate) async fn merge_to_tree(
        &self,
        tenant: &str,
        profile_type: &str,
        label_selector: &str,
        start_ms: i64,
        end_ms: i64,
        span_ids: Option<&[u64]>,
        call_sites: &[String],
    ) -> Result<Tree, ProfileError> {
        if matches!(span_ids, Some(ids) if ids.is_empty()) {
            return Err(ProfileError::Plan(
                "span selector must contain at least one span id".to_string(),
            ));
        }
        let matchers = crate::matcher::parse_label_selector(label_selector)?;
        let scan = self
            .store
            .select(tenant, profile_type, &matchers, start_ms, end_ms)
            .await?;
        let span_where = span_ids.map_or_else(String::new, |ids| {
            format!(
                " WHERE {span} IN ({ids})",
                span = PCOL_SPAN_ID,
                ids = ids.iter().map(u64::to_string).collect::<Vec<_>>().join(",")
            )
        });
        let sql = format!(
            "SELECT {partition}, {stacktrace}, SUM({value}) AS v \
             FROM {table}{span_where} GROUP BY {partition}, {stacktrace} \
             ORDER BY {partition}, {stacktrace}",
            partition = PCOL_STACKTRACE_PARTITION,
            stacktrace = PCOL_STACKTRACE_ID,
            value = PCOL_VALUE,
            table = scan.samples_table,
            span_where = span_where,
        );
        let mut tree = Tree::new();
        merge_sql_to_tree(&scan, &sql, &mut tree, &[], call_sites).await?;
        Ok(tree)
    }

    #[allow(clippy::too_many_arguments)]
    pub async fn select_series(
        &self,
        tenant: &str,
        profile_type: &str,
        label_selector: &str,
        group_by: &[String],
        step_secs: f64,
        agg: SeriesAgg,
        start_ms: i64,
        end_ms: i64,
    ) -> Result<Vec<Series>, ProfileError> {
        self.select_series_with_stack_trace_selector(
            tenant,
            profile_type,
            label_selector,
            group_by,
            step_secs,
            agg,
            start_ms,
            end_ms,
            &[],
        )
        .await
    }

    #[allow(clippy::too_many_arguments)]
    pub async fn select_series_with_stack_trace_selector(
        &self,
        tenant: &str,
        profile_type: &str,
        label_selector: &str,
        group_by: &[String],
        step_secs: f64,
        agg: SeriesAgg,
        start_ms: i64,
        end_ms: i64,
        call_sites: &[String],
    ) -> Result<Vec<Series>, ProfileError> {
        let step_ms = step_ms_from_secs(step_secs)?;
        let base_matchers = crate::matcher::parse_label_selector(label_selector)?;
        let groups = if group_by.is_empty() {
            vec![Vec::new()]
        } else {
            self.store
                .series(tenant, &base_matchers, group_by, start_ms, end_ms)
                .await?
        };

        let mut out = Vec::new();
        for labels in groups {
            let mut matchers = base_matchers.clone();
            matchers.extend(
                labels.iter().map(|(name, value)| {
                    LabelMatcher::new(name.clone(), MatchOp::Eq, value.clone())
                }),
            );
            let scan = self
                .store
                .select(tenant, profile_type, &matchers, start_ms, end_ms)
                .await?;
            let buckets = if call_sites.is_empty() {
                series_buckets_from_totals(&scan, step_ms).await?
            } else {
                series_buckets_from_stacktrace_selector(&scan, step_ms, call_sites).await?
            };
            if buckets.is_empty() {
                continue;
            }
            out.push(Series {
                labels,
                points: buckets
                    .into_iter()
                    .map(|(bucket, values)| (bucket, fold_bucket(agg, &values)))
                    .collect(),
            });
        }
        Ok(out)
    }

    #[allow(clippy::too_many_arguments)]
    pub async fn select_series_sharded(
        &self,
        tenant: &str,
        profile_type: &str,
        label_selector: &str,
        group_by: &[String],
        step_secs: f64,
        agg: SeriesAgg,
        ranges: &[(i64, i64)],
    ) -> Result<Vec<Series>, ProfileError> {
        self.select_series_with_stack_trace_selector_sharded(
            tenant,
            profile_type,
            label_selector,
            group_by,
            step_secs,
            agg,
            ranges,
            &[],
        )
        .await
    }

    #[allow(clippy::too_many_arguments)]
    pub async fn select_series_with_stack_trace_selector_sharded(
        &self,
        tenant: &str,
        profile_type: &str,
        label_selector: &str,
        group_by: &[String],
        step_secs: f64,
        agg: SeriesAgg,
        ranges: &[(i64, i64)],
        call_sites: &[String],
    ) -> Result<Vec<Series>, ProfileError> {
        if ranges.is_empty() {
            return Err(ProfileError::Plan(
                "sharded series query requires at least one time range".to_string(),
            ));
        }
        let (start_ms, end_ms) = covering_range(ranges)?;
        if agg == SeriesAgg::Average {
            return self
                .select_series_with_stack_trace_selector(
                    tenant,
                    profile_type,
                    label_selector,
                    group_by,
                    step_secs,
                    agg,
                    start_ms,
                    end_ms,
                    call_sites,
                )
                .await;
        }

        let mut merged: BTreeMap<Vec<(String, String)>, BTreeMap<i64, f64>> = BTreeMap::new();
        for (start_ms, end_ms) in ranges {
            let series = self
                .select_series_with_stack_trace_selector(
                    tenant,
                    profile_type,
                    label_selector,
                    group_by,
                    step_secs,
                    agg,
                    *start_ms,
                    *end_ms,
                    call_sites,
                )
                .await?;
            for item in series {
                let points = merged.entry(item.labels).or_default();
                for (timestamp, value) in item.points {
                    *points.entry(timestamp).or_default() += value;
                }
            }
        }

        Ok(merged
            .into_iter()
            .map(|(labels, points)| Series {
                labels,
                points: points.into_iter().collect(),
            })
            .collect())
    }

    pub async fn diff(
        &self,
        tenant: &str,
        left: (&str, &str, i64, i64),
        right: (&str, &str, i64, i64),
        max_nodes: i64,
    ) -> Result<FlameGraphDiff, ProfileError> {
        self.diff_with_stack_trace_selector(tenant, left, right, max_nodes, &[], &[])
            .await
    }

    #[allow(clippy::too_many_arguments)]
    pub async fn diff_with_stack_trace_selector(
        &self,
        tenant: &str,
        left: (&str, &str, i64, i64),
        right: (&str, &str, i64, i64),
        max_nodes: i64,
        left_call_sites: &[String],
        right_call_sites: &[String],
    ) -> Result<FlameGraphDiff, ProfileError> {
        let left_tree = self
            .merge_to_tree(
                tenant,
                left.0,
                left.1,
                left.2,
                left.3,
                None,
                left_call_sites,
            )
            .await?;
        let right_tree = self
            .merge_to_tree(
                tenant,
                right.0,
                right.1,
                right.2,
                right.3,
                None,
                right_call_sites,
            )
            .await?;
        let max_nodes = if max_nodes > 0 {
            max_nodes
        } else {
            self.opts.default_max_nodes
        };
        Ok(diff_trees(left_tree, right_tree, max_nodes))
    }

    pub async fn select_merge_profile(
        &self,
        tenant: &str,
        profile_type: &str,
        label_selector: &str,
        start_ms: i64,
        end_ms: i64,
    ) -> Result<Vec<u8>, ProfileError> {
        self.select_merge_profile_with_stack_trace_selector(
            tenant,
            profile_type,
            label_selector,
            start_ms,
            end_ms,
            &[],
        )
        .await
    }

    pub async fn select_merge_profile_with_stack_trace_selector(
        &self,
        tenant: &str,
        profile_type: &str,
        label_selector: &str,
        start_ms: i64,
        end_ms: i64,
        call_sites: &[String],
    ) -> Result<Vec<u8>, ProfileError> {
        let profile_type = ProfileType::parse(profile_type)?;
        let tree = self
            .merge_to_tree(
                tenant,
                &profile_type.to_string(),
                label_selector,
                start_ms,
                end_ms,
                None,
                call_sites,
            )
            .await?;
        Ok(tree_to_pprof(&tree, &profile_type).encode())
    }

    #[allow(clippy::too_many_arguments)]
    pub async fn select_merge_profile_with_max_nodes_and_stack_trace_selector(
        &self,
        tenant: &str,
        profile_type: &str,
        label_selector: &str,
        start_ms: i64,
        end_ms: i64,
        max_nodes: i64,
        call_sites: &[String],
    ) -> Result<Vec<u8>, ProfileError> {
        let profile_type = ProfileType::parse(profile_type)?;
        let tree = self
            .merge_to_tree(
                tenant,
                &profile_type.to_string(),
                label_selector,
                start_ms,
                end_ms,
                None,
                call_sites,
            )
            .await?;
        let max_nodes = if max_nodes > 0 {
            max_nodes
        } else {
            self.opts.default_max_nodes
        };
        Ok(tree_to_pprof_with_max_nodes(&tree, &profile_type, max_nodes).encode())
    }

    #[allow(clippy::too_many_arguments)]
    pub async fn select_merge_span_profile(
        &self,
        tenant: &str,
        profile_type: &str,
        label_selector: &str,
        span_selector: &[u64],
        start_ms: i64,
        end_ms: i64,
        max_nodes: i64,
    ) -> Result<FlameGraph, ProfileError> {
        let tree = self
            .merge_to_tree(
                tenant,
                profile_type,
                label_selector,
                start_ms,
                end_ms,
                Some(span_selector),
                &[],
            )
            .await?;
        let max_nodes = if max_nodes > 0 {
            max_nodes
        } else {
            self.opts.default_max_nodes
        };
        Ok(tree.to_flamegraph(max_nodes))
    }

    #[allow(clippy::too_many_arguments)]
    pub async fn select_merge_span_profile_tree(
        &self,
        tenant: &str,
        profile_type: &str,
        label_selector: &str,
        span_selector: &[u64],
        start_ms: i64,
        end_ms: i64,
        max_nodes: i64,
    ) -> Result<Vec<u8>, ProfileError> {
        let tree = self
            .merge_to_tree(
                tenant,
                profile_type,
                label_selector,
                start_ms,
                end_ms,
                Some(span_selector),
                &[],
            )
            .await?;
        let max_nodes = if max_nodes > 0 {
            max_nodes
        } else {
            self.opts.default_max_nodes
        };
        Ok(tree.to_pyroscope_tree_bytes(max_nodes))
    }

    #[allow(clippy::too_many_arguments)]
    pub async fn select_merge_span_profile_sharded(
        &self,
        tenant: &str,
        profile_type: &str,
        label_selector: &str,
        span_selector: &[u64],
        ranges: &[(i64, i64)],
        max_nodes: i64,
    ) -> Result<FlameGraph, ProfileError> {
        if matches!(span_selector, []) {
            return Err(ProfileError::Plan(
                "span selector must contain at least one span id".to_string(),
            ));
        }
        if ranges.is_empty() {
            return Err(ProfileError::Plan(
                "sharded span profile query requires at least one time range".to_string(),
            ));
        }
        let mut merged = Tree::new();
        for (start_ms, end_ms) in ranges {
            validate_range(*start_ms, *end_ms)?;
            let tree = self
                .merge_to_tree(
                    tenant,
                    profile_type,
                    label_selector,
                    *start_ms,
                    *end_ms,
                    Some(span_selector),
                    &[],
                )
                .await?;
            merged.merge(tree);
        }
        let max_nodes = if max_nodes > 0 {
            max_nodes
        } else {
            self.opts.default_max_nodes
        };
        Ok(merged.to_flamegraph(max_nodes))
    }

    #[allow(clippy::too_many_arguments)]
    pub async fn select_merge_span_profile_tree_sharded(
        &self,
        tenant: &str,
        profile_type: &str,
        label_selector: &str,
        span_selector: &[u64],
        ranges: &[(i64, i64)],
        max_nodes: i64,
    ) -> Result<Vec<u8>, ProfileError> {
        if matches!(span_selector, []) {
            return Err(ProfileError::Plan(
                "span selector must contain at least one span id".to_string(),
            ));
        }
        if ranges.is_empty() {
            return Err(ProfileError::Plan(
                "sharded span profile query requires at least one time range".to_string(),
            ));
        }
        let mut merged = Tree::new();
        for (start_ms, end_ms) in ranges {
            validate_range(*start_ms, *end_ms)?;
            let tree = self
                .merge_to_tree(
                    tenant,
                    profile_type,
                    label_selector,
                    *start_ms,
                    *end_ms,
                    Some(span_selector),
                    &[],
                )
                .await?;
            merged.merge(tree);
        }
        let max_nodes = if max_nodes > 0 {
            max_nodes
        } else {
            self.opts.default_max_nodes
        };
        Ok(merged.to_pyroscope_tree_bytes(max_nodes))
    }

    #[allow(clippy::too_many_arguments)]
    pub async fn select_heatmap(
        &self,
        tenant: &str,
        profile_type: &str,
        label_selector: &str,
        start_ms: i64,
        end_ms: i64,
        time_buckets: usize,
        value_buckets: usize,
    ) -> Result<Heatmap, ProfileError> {
        Ok(self
            .select_heatmaps(
                tenant,
                profile_type,
                label_selector,
                &[],
                start_ms,
                end_ms,
                time_buckets,
                value_buckets,
            )
            .await?
            .into_iter()
            .next()
            .map_or_else(
                || bin_heatmap(&[], start_ms, end_ms, time_buckets, value_buckets),
                |item| item.heatmap,
            ))
    }

    #[allow(clippy::too_many_arguments)]
    pub async fn select_heatmaps(
        &self,
        tenant: &str,
        profile_type: &str,
        label_selector: &str,
        group_by: &[String],
        start_ms: i64,
        end_ms: i64,
        time_buckets: usize,
        value_buckets: usize,
    ) -> Result<Vec<LabeledHeatmap>, ProfileError> {
        let base_matchers = crate::matcher::parse_label_selector(label_selector)?;
        let groups = if group_by.is_empty() {
            vec![Vec::new()]
        } else {
            self.store
                .series(tenant, &base_matchers, group_by, start_ms, end_ms)
                .await?
        };

        let mut out = Vec::new();
        for labels in groups {
            let mut matchers = base_matchers.clone();
            matchers.extend(
                labels.iter().map(|(name, value)| {
                    LabelMatcher::new(name.clone(), MatchOp::Eq, value.clone())
                }),
            );
            let scan = self
                .store
                .select(tenant, profile_type, &matchers, start_ms, end_ms)
                .await?;
            let points = heatmap_points_from_totals(&scan).await?;
            if points.is_empty() && !group_by.is_empty() {
                continue;
            }
            out.push(LabeledHeatmap {
                labels,
                heatmap: bin_heatmap(&points, start_ms, end_ms, time_buckets, value_buckets),
            });
        }
        Ok(out)
    }
}

async fn heatmap_points_from_totals(
    scan: &crate::ProfileScan,
) -> Result<Vec<(i64, i64)>, ProfileError> {
    let sql = format!(
        "SELECT {timestamp}, MAX({total}) AS total \
         FROM {table} GROUP BY {timestamp}, {fingerprint}",
        timestamp = COL_TIMESTAMP,
        total = PCOL_TOTAL_VALUE,
        table = scan.samples_table,
        fingerprint = COL_FINGERPRINT,
    );
    let batches = scan
        .ctx
        .sql(&sql)
        .await
        .map_err(|err| ProfileError::Plan(err.to_string()))?
        .collect()
        .await
        .map_err(|err| ProfileError::Exec(err.to_string()))?;
    let mut points = Vec::new();
    for batch in batches {
        let timestamps = batch.column(0).as_primitive::<Int64Type>();
        let totals = batch.column(1).as_primitive::<Int64Type>();
        for row in 0..batch.num_rows() {
            points.push((timestamps.value(row), totals.value(row)));
        }
    }
    Ok(points)
}

async fn merge_scan_to_tree(
    scan: &crate::ProfileScan,
    tree: &mut Tree,
    prefix_frames: &[Frame],
    span_ids: Option<&[u64]>,
    call_sites: &[String],
) -> Result<(), ProfileError> {
    let span_where = span_ids.map_or_else(String::new, |ids| {
        format!(
            " WHERE {span} IN ({ids})",
            span = PCOL_SPAN_ID,
            ids = ids.iter().map(u64::to_string).collect::<Vec<_>>().join(",")
        )
    });
    let sql = format!(
        "SELECT {partition}, {stacktrace}, SUM({value}) AS v \
         FROM {table}{span_where} GROUP BY {partition}, {stacktrace} \
         ORDER BY {partition}, {stacktrace}",
        partition = PCOL_STACKTRACE_PARTITION,
        stacktrace = PCOL_STACKTRACE_ID,
        value = PCOL_VALUE,
        table = scan.samples_table,
        span_where = span_where,
    );
    merge_sql_to_tree(scan, &sql, tree, prefix_frames, call_sites).await
}

async fn merge_sql_to_tree(
    scan: &crate::ProfileScan,
    sql: &str,
    tree: &mut Tree,
    prefix_frames: &[Frame],
    call_sites: &[String],
) -> Result<(), ProfileError> {
    let batches = scan
        .ctx
        .sql(sql)
        .await
        .map_err(|err| ProfileError::Plan(err.to_string()))?
        .collect()
        .await
        .map_err(|err| ProfileError::Exec(err.to_string()))?;
    for batch in batches {
        let partitions = batch.column(0).as_primitive::<UInt64Type>();
        let stacktrace_ids = batch.column(1).as_primitive::<UInt64Type>();
        let values = batch.column(2).as_primitive::<Int64Type>();
        for row in 0..batch.num_rows() {
            let partition = partitions.value(row);
            let stacktrace_id = u32::try_from(stacktrace_ids.value(row)).map_err(|err| {
                ProfileError::Symbolize(format!("stacktrace id does not fit u32: {err}"))
            })?;
            let mut frames = scan.symbols.resolve(partition, stacktrace_id);
            if call_sites.is_empty() || stack_matches_call_sites(&frames, call_sites) {
                frames.extend_from_slice(prefix_frames);
                tree.add_stack(&frames, values.value(row));
            }
        }
    }
    Ok(())
}

fn group_frame_name(labels: &[(String, String)]) -> String {
    if labels.len() == 1 {
        labels[0].1.clone()
    } else {
        labels
            .iter()
            .map(|(name, value)| format!("{name}={value}"))
            .collect::<Vec<_>>()
            .join(", ")
    }
}

async fn series_buckets_from_totals(
    scan: &crate::ProfileScan,
    step_ms: i64,
) -> Result<BTreeMap<i64, Vec<i64>>, ProfileError> {
    let sql = format!(
        "SELECT {timestamp}, MAX({total}) AS total \
         FROM {table} GROUP BY {timestamp}, {fingerprint}",
        timestamp = COL_TIMESTAMP,
        total = PCOL_TOTAL_VALUE,
        table = scan.samples_table,
        fingerprint = COL_FINGERPRINT,
    );
    let batches = scan
        .ctx
        .sql(&sql)
        .await
        .map_err(|err| ProfileError::Plan(err.to_string()))?
        .collect()
        .await
        .map_err(|err| ProfileError::Exec(err.to_string()))?;
    let mut buckets: BTreeMap<i64, Vec<i64>> = BTreeMap::new();
    for batch in batches {
        let timestamps = batch.column(0).as_primitive::<Int64Type>();
        let totals = batch.column(1).as_primitive::<Int64Type>();
        for row in 0..batch.num_rows() {
            buckets
                .entry(step_bucket_ms(timestamps.value(row), step_ms))
                .or_default()
                .push(totals.value(row));
        }
    }
    Ok(buckets)
}

async fn series_buckets_from_stacktrace_selector(
    scan: &crate::ProfileScan,
    step_ms: i64,
    call_sites: &[String],
) -> Result<BTreeMap<i64, Vec<i64>>, ProfileError> {
    let sql = format!(
        "SELECT {timestamp}, {fingerprint}, {partition}, {stacktrace}, SUM({value}) AS v \
         FROM {table} GROUP BY {timestamp}, {fingerprint}, {partition}, {stacktrace} \
         ORDER BY {timestamp}, {fingerprint}, {partition}, {stacktrace}",
        timestamp = COL_TIMESTAMP,
        fingerprint = COL_FINGERPRINT,
        partition = PCOL_STACKTRACE_PARTITION,
        stacktrace = PCOL_STACKTRACE_ID,
        value = PCOL_VALUE,
        table = scan.samples_table,
    );
    let batches = scan
        .ctx
        .sql(&sql)
        .await
        .map_err(|err| ProfileError::Plan(err.to_string()))?
        .collect()
        .await
        .map_err(|err| ProfileError::Exec(err.to_string()))?;

    let mut per_profile: BTreeMap<(i64, u64), i64> = BTreeMap::new();
    for batch in batches {
        let timestamps = batch.column(0).as_primitive::<Int64Type>();
        let fingerprints = batch.column(1).as_primitive::<UInt64Type>();
        let partitions = batch.column(2).as_primitive::<UInt64Type>();
        let stacktrace_ids = batch.column(3).as_primitive::<UInt64Type>();
        let values = batch.column(4).as_primitive::<Int64Type>();
        for row in 0..batch.num_rows() {
            let partition = partitions.value(row);
            let stacktrace_id = u32::try_from(stacktrace_ids.value(row)).map_err(|err| {
                ProfileError::Symbolize(format!("stacktrace id does not fit u32: {err}"))
            })?;
            let frames = scan.symbols.resolve(partition, stacktrace_id);
            if stack_matches_call_sites(&frames, call_sites) {
                *per_profile
                    .entry((timestamps.value(row), fingerprints.value(row)))
                    .or_default() += values.value(row);
            }
        }
    }

    let mut buckets: BTreeMap<i64, Vec<i64>> = BTreeMap::new();
    for ((timestamp, _fingerprint), value) in per_profile {
        buckets
            .entry(step_bucket_ms(timestamp, step_ms))
            .or_default()
            .push(value);
    }
    Ok(buckets)
}

fn stack_matches_call_sites(frames: &[Frame], call_sites: &[String]) -> bool {
    call_sites.iter().all(|site| {
        frames
            .iter()
            .any(|frame| frame.function == *site || frame.file == *site)
    })
}

fn covering_range(ranges: &[(i64, i64)]) -> Result<(i64, i64), ProfileError> {
    let mut start = i64::MAX;
    let mut end = i64::MIN;
    for (range_start, range_end) in ranges {
        validate_range(*range_start, *range_end)?;
        start = start.min(*range_start);
        end = end.max(*range_end);
    }
    Ok((start, end))
}

fn validate_range(start_ms: i64, end_ms: i64) -> Result<(), ProfileError> {
    if start_ms > end_ms {
        return Err(ProfileError::Plan(format!(
            "invalid time range: start {start_ms} is after end {end_ms}"
        )));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use assert2::assert;

    use super::*;
    use crate::{FunctionRec, InMemoryProfileStore, LineRec, LocationRec, SeriesAgg};

    const PT: &str = "process_cpu:cpu:nanoseconds:cpu:nanoseconds";

    fn merge_fixture() -> FlameEngine<InMemoryProfileStore> {
        let mut store = InMemoryProfileStore::new();
        let (work_stack, other_stack) = {
            let db = store.symbols_mut();
            let main = intern_location(db, "main");
            let work = intern_location(db, "work");
            let other = intern_location(db, "other");
            (
                db.intern_stacktrace(0, &[work, main]),
                db.intern_stacktrace(0, &[other, main]),
            )
        };
        store.push_sample(
            "tenant-a",
            PT,
            vec![("service".to_string(), "api".to_string())],
            0,
            work_stack,
            10,
            100,
        );
        store.push_sample(
            "tenant-a",
            PT,
            vec![("service".to_string(), "api".to_string())],
            0,
            work_stack,
            5,
            110,
        );
        store.push_sample(
            "tenant-a",
            PT,
            vec![("service".to_string(), "worker".to_string())],
            0,
            other_stack,
            3,
            120,
        );
        FlameEngine::new(Arc::new(store), EngineOpts::default())
    }

    fn intern_location(db: &mut crate::SymbolDb, name: &str) -> u32 {
        let name_ref = db.intern_string(name);
        let filename_ref = db.intern_string(&format!("{name}.go"));
        let function_id = db.intern_function(FunctionRec {
            name: name_ref,
            system_name: name_ref,
            filename: filename_ref,
            start_line: 1,
        });
        db.intern_location(LocationRec {
            address: 0,
            mapping_id: 0,
            lines: vec![LineRec {
                function_id,
                line: 1,
            }],
        })
    }

    fn self_value_for(fg: &FlameGraph, name: &str) -> i64 {
        let name_index = fg
            .names
            .iter()
            .position(|value| value == name)
            .expect("name exists");
        fg.levels
            .iter()
            .flat_map(|level| level.values.chunks_exact(4))
            .find(|chunk| chunk[3] == i64::try_from(name_index).expect("index fits i64"))
            .expect("bar exists")[2]
    }

    #[test]
    fn default_max_nodes_is_2048() {
        assert!(EngineOpts::default().default_max_nodes == 2048);
    }

    #[tokio::test]
    async fn engine_diff_two_windows() {
        let mut store = InMemoryProfileStore::new();
        let (stack_a, stack_b) = {
            let db = store.symbols_mut();
            let a = intern_location(db, "a");
            let b = intern_location(db, "b");
            (db.intern_stacktrace(0, &[a]), db.intern_stacktrace(0, &[b]))
        };
        store.push_sample_with_total(
            "tenant-a",
            PT,
            vec![("svc".to_string(), "x".to_string())],
            0,
            stack_a,
            10,
            10,
            0,
        );
        store.push_sample_with_total(
            "tenant-a",
            PT,
            vec![("svc".to_string(), "x".to_string())],
            0,
            stack_a,
            10,
            15,
            30_000,
        );
        store.push_sample_with_total(
            "tenant-a",
            PT,
            vec![("svc".to_string(), "x".to_string())],
            0,
            stack_b,
            5,
            15,
            30_000,
        );
        let engine = FlameEngine::new(
            Arc::new(store),
            EngineOpts {
                default_max_nodes: 2048,
            },
        );

        let diff = engine
            .diff("tenant-a", (PT, "{}", 0, 1), (PT, "{}", 29_000, 60_000), 0)
            .await
            .unwrap();

        assert!(diff.left_ticks == 10);
        assert!(diff.right_ticks == 15);
        assert!(diff.names.iter().any(|name| name == "b"));
    }

    #[tokio::test]
    async fn select_merge_profile_returns_merged_pprof_bytes() {
        let bytes = merge_fixture()
            .select_merge_profile("tenant-a", PT, r#"{service="api"}"#, 0, 200)
            .await
            .unwrap();
        let profile = crate::PprofProfile::decode(&bytes).unwrap();
        let total: i64 = profile
            .inner()
            .sample
            .iter()
            .map(|sample| sample.value.iter().sum::<i64>())
            .sum();

        assert!(total == 15);
    }

    #[tokio::test]
    async fn span_profile_filters_by_span_id() {
        let mut store = InMemoryProfileStore::new();
        let (stack_a, stack_b) = {
            let db = store.symbols_mut();
            let a = intern_location(db, "a");
            let b = intern_location(db, "b");
            (db.intern_stacktrace(0, &[a]), db.intern_stacktrace(0, &[b]))
        };
        store.push_sample_with_total_and_span(
            "tenant-a",
            PT,
            vec![("svc".to_string(), "x".to_string())],
            0,
            stack_a,
            6,
            10,
            0,
            111,
        );
        store.push_sample_with_total_and_span(
            "tenant-a",
            PT,
            vec![("svc".to_string(), "x".to_string())],
            0,
            stack_b,
            4,
            10,
            0,
            222,
        );
        let engine = FlameEngine::new(Arc::new(store), EngineOpts::default());

        let fg = engine
            .select_merge_span_profile("tenant-a", PT, "{}", &[111], 0, 60_000, 0)
            .await
            .unwrap();

        assert!(fg.total == 6);
        assert!(
            engine
                .select_merge_span_profile("tenant-a", PT, "{}", &[], 0, 60_000, 0)
                .await
                .is_err()
        );
    }

    #[tokio::test]
    async fn sharded_span_profile_matches_whole_range() {
        let mut store = InMemoryProfileStore::new();
        let (stack_a, stack_b) = {
            let db = store.symbols_mut();
            let a = intern_location(db, "a");
            let b = intern_location(db, "b");
            (db.intern_stacktrace(0, &[a]), db.intern_stacktrace(0, &[b]))
        };
        store.push_sample_with_total_and_span(
            "tenant-a",
            PT,
            vec![("svc".to_string(), "x".to_string())],
            0,
            stack_a,
            6,
            10,
            0,
            111,
        );
        store.push_sample_with_total_and_span(
            "tenant-a",
            PT,
            vec![("svc".to_string(), "x".to_string())],
            0,
            stack_b,
            4,
            10,
            30_000,
            111,
        );
        let engine = FlameEngine::new(Arc::new(store), EngineOpts::default());
        let whole = engine
            .select_merge_span_profile("tenant-a", PT, "{}", &[111], 0, 60_000, 0)
            .await
            .unwrap();
        let sharded = engine
            .select_merge_span_profile_sharded(
                "tenant-a",
                PT,
                "{}",
                &[111],
                &[(0, 10_000), (10_001, 60_000)],
                0,
            )
            .await
            .unwrap();

        assert!(sharded == whole);
    }

    #[tokio::test]
    async fn select_heatmap_bins_profile_totals() {
        let mut store = InMemoryProfileStore::new();
        store.push_sample_with_total(
            "tenant-a",
            PT,
            vec![("svc".to_string(), "x".to_string())],
            0,
            1,
            2,
            5,
            0,
        );
        store.push_sample_with_total(
            "tenant-a",
            PT,
            vec![("svc".to_string(), "x".to_string())],
            0,
            2,
            3,
            5,
            0,
        );
        store.push_sample_with_total(
            "tenant-a",
            PT,
            vec![("svc".to_string(), "x".to_string())],
            0,
            1,
            30,
            30,
            60,
        );
        let engine = FlameEngine::new(Arc::new(store), EngineOpts::default());

        let heatmap = engine
            .select_heatmap("tenant-a", PT, "{}", 0, 100, 2, 2)
            .await
            .unwrap();

        assert!(heatmap.counts[0][0] == 1);
        assert!(heatmap.counts[1][1] == 1);
    }

    #[tokio::test]
    async fn raw_ids_never_cross_a_partition_boundary() {
        let mut store = InMemoryProfileStore::new();
        let (alpha_stack, beta_stack) = {
            let db = store.symbols_mut();
            let alpha = intern_location(db, "alpha");
            let beta = intern_location(db, "beta");
            (
                db.intern_stacktrace(0, &[alpha]),
                db.intern_stacktrace(1, &[beta]),
            )
        };
        assert!(alpha_stack == beta_stack);
        store.push_sample("tenant-a", PT, Vec::new(), 0, alpha_stack, 5, 0);
        store.push_sample("tenant-a", PT, Vec::new(), 1, beta_stack, 7, 0);
        let engine = FlameEngine::new(Arc::new(store), EngineOpts::default());

        let fg = engine
            .select_merge_stacktraces("tenant-a", PT, "{}", 0, 60_000, 0)
            .await
            .unwrap();

        assert!(fg.names.iter().any(|name| name == "alpha"));
        assert!(fg.names.iter().any(|name| name == "beta"));
        assert!(fg.total == 12);
    }

    #[tokio::test]
    async fn merge_folds_duplicate_ids_before_symbolize() {
        let fg = merge_fixture()
            .select_merge_stacktraces("tenant-a", PT, "{}", 0, 200, 2048)
            .await
            .unwrap();

        assert!(fg.total == 18);
        assert!(fg.levels[0].values == vec![0, 18, 0, 0]);
        assert!(self_value_for(&fg, "work") == 15);
    }

    #[tokio::test]
    async fn merge_applies_label_selector_and_max_nodes_fallback() {
        let fg = merge_fixture()
            .select_merge_stacktraces("tenant-a", PT, r#"{service="api"}"#, 0, 200, 0)
            .await
            .unwrap();

        assert!(fg.total == 15);
        assert!(fg.names[0] == "total");
        assert!(self_value_for(&fg, "work") == 15);
        assert!(!fg.names.iter().any(|name| name == "other"));
    }

    #[tokio::test]
    async fn merge_stack_trace_selector_filters_call_sites() {
        let fg = merge_fixture()
            .select_merge_stacktraces_with_stack_trace_selector(
                "tenant-a",
                PT,
                "{}",
                0,
                200,
                0,
                &["work".to_string()],
            )
            .await
            .unwrap();

        assert!(fg.total == 15);
        assert!(fg.names.iter().any(|name| name == "work"));
        assert!(!fg.names.iter().any(|name| name == "other"));
    }

    #[tokio::test]
    async fn sharded_merge_matches_whole_range_merge() {
        let engine = merge_fixture();
        let whole = engine
            .select_merge_stacktraces("tenant-a", PT, "{}", 0, 200, 2048)
            .await
            .unwrap();
        let sharded = engine
            .select_merge_stacktraces_sharded("tenant-a", PT, "{}", &[(0, 105), (105, 200)], 2048)
            .await
            .unwrap();

        assert!(sharded == whole);
    }

    fn series_fixture() -> FlameEngine<InMemoryProfileStore> {
        let mut store = InMemoryProfileStore::new();
        let stack_a = 1;
        let stack_b = 2;
        store.push_sample_with_total(
            "tenant-a",
            PT,
            vec![("service".to_string(), "api".to_string())],
            0,
            stack_a,
            60,
            100,
            0,
        );
        store.push_sample_with_total(
            "tenant-a",
            PT,
            vec![("service".to_string(), "api".to_string())],
            0,
            stack_b,
            40,
            100,
            0,
        );
        store.push_sample_with_total(
            "tenant-a",
            PT,
            vec![("service".to_string(), "api".to_string())],
            0,
            stack_a,
            50,
            50,
            16_000,
        );
        store.push_sample_with_total(
            "tenant-a",
            PT,
            vec![("service".to_string(), "web".to_string())],
            0,
            stack_a,
            7,
            7,
            0,
        );
        store.push_sample_with_total(
            "tenant-a",
            "memory:alloc_space:bytes:space:bytes",
            vec![("service".to_string(), "api".to_string())],
            0,
            stack_a,
            999,
            999,
            0,
        );
        FlameEngine::new(Arc::new(store), EngineOpts::default())
    }

    #[tokio::test]
    async fn select_series_sum_buckets_by_step_and_counts_total_once_per_profile() {
        let mut got = series_fixture()
            .select_series(
                "tenant-a",
                PT,
                "{}",
                &["service".to_string()],
                15.0,
                SeriesAgg::Sum,
                0,
                60_000,
            )
            .await
            .unwrap();
        got.sort_by(|left, right| left.labels.cmp(&right.labels));

        assert!(got[0].labels == vec![("service".to_string(), "api".to_string())]);
        assert!(got[0].points == vec![(0, 100.0), (15_000, 50.0)]);
        assert!(got[1].labels == vec![("service".to_string(), "web".to_string())]);
        assert!(got[1].points == vec![(0, 7.0)]);
    }

    #[tokio::test]
    async fn select_series_floors_timestamps_to_step_buckets() {
        let mut store = InMemoryProfileStore::new();
        store.push_sample_with_total(
            "tenant-a",
            PT,
            vec![("service".to_string(), "api".to_string())],
            0,
            1,
            1,
            10,
            0,
        );
        store.push_sample_with_total(
            "tenant-a",
            PT,
            vec![("service".to_string(), "api".to_string())],
            0,
            1,
            1,
            20,
            10_000,
        );
        store.push_sample_with_total(
            "tenant-a",
            PT,
            vec![("service".to_string(), "api".to_string())],
            0,
            1,
            1,
            5,
            16_000,
        );
        let engine = FlameEngine::new(Arc::new(store), EngineOpts::default());

        let got = engine
            .select_series(
                "tenant-a",
                PT,
                "{}",
                &["service".to_string()],
                15.0,
                SeriesAgg::Sum,
                0,
                60_000,
            )
            .await
            .unwrap();

        assert!(
            got == vec![Series {
                labels: vec![("service".to_string(), "api".to_string())],
                points: vec![(0, 30.0), (15_000, 5.0)],
            }]
        );
    }

    #[tokio::test]
    async fn select_series_average_and_label_selector_bucket_by_step() {
        let got = series_fixture()
            .select_series(
                "tenant-a",
                PT,
                r#"{service="api"}"#,
                &[],
                60.0,
                SeriesAgg::Average,
                0,
                60_000,
            )
            .await
            .unwrap();

        assert!(
            got == vec![Series {
                labels: Vec::new(),
                points: vec![(0, 75.0)],
            }]
        );
    }

    #[tokio::test]
    async fn sharded_select_series_merges_points_for_same_label_set() {
        let mut got = series_fixture()
            .select_series_sharded(
                "tenant-a",
                PT,
                "{}",
                &["service".to_string()],
                15.0,
                SeriesAgg::Sum,
                &[(0, 10_000), (10_000, 60_000)],
            )
            .await
            .unwrap();
        got.sort_by(|left, right| left.labels.cmp(&right.labels));

        assert!(got[0].labels == vec![("service".to_string(), "api".to_string())]);
        assert!(got[0].points == vec![(0, 100.0), (15_000, 50.0)]);
        assert!(got[1].labels == vec![("service".to_string(), "web".to_string())]);
        assert!(got[1].points == vec![(0, 7.0)]);
    }
}
