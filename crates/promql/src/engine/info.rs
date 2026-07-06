use std::collections::{BTreeMap, BTreeSet};

use crabka_blockstore::{LabelMatcher, Labels};
use promql_parser::parser::{Call, Expr, VectorSelector};

use super::selector::{compile_label_matchers, info_data_label_matchers, labels_match};
use crate::{
    PromqlError,
    error::Result,
    result::{InstantSample, SampleValue},
};

fn info_identifying_key(labels: &Labels) -> Option<String> {
    Some(format!(
        "job={}\ninstance={}\n",
        labels.get("job")?,
        labels.get("instance")?
    ))
}

/// Parsed, store-independent context for `info(v [, data_label_selector])`.
pub(super) struct InfoContext<'a> {
    pub(super) data_label_selector: Option<&'a VectorSelector>,
    pub(super) data_label_matchers: Vec<LabelMatcher>,
    required_data_label_matchers_match_empty: bool,
    selected_data_labels: BTreeSet<String>,
    restrict_data_labels: bool,
}

/// Parse and validate an `info(v [, data_label_selector])` call.
pub(super) fn parse_info_call(call: &Call) -> Result<InfoContext<'_>> {
    let [_arg, data_label_selector @ ..] = call.args.args.as_slice() else {
        return Err(PromqlError::Plan(format!(
            "info expects one or two arguments for default target_info enrichment, got {}",
            call.args.args.len()
        )));
    };
    if data_label_selector.len() > 1 {
        return Err(PromqlError::Plan(format!(
            "info expects one or two arguments for default target_info enrichment, got {}",
            call.args.args.len()
        )));
    }
    let data_label_selector = match data_label_selector {
        [] => None,
        [selector] => match selector.as_ref() {
            Expr::VectorSelector(selector) => Some(selector),
            _ => {
                return Err(PromqlError::Plan(
                    "info data label selector must be a vector selector".to_string(),
                ));
            }
        },
        [_, _, ..] => unreachable!("data label selector arity checked above"),
    };
    let data_label_matchers = data_label_selector
        .map(info_data_label_matchers)
        .transpose()?
        .unwrap_or_default();
    let required_data_label_matchers = data_label_matchers
        .iter()
        .filter(|matcher| !matches!(matcher.name.as_str(), "__name__" | "job" | "instance"))
        .cloned()
        .collect::<Vec<_>>();
    let required_data_label_matchers_match_empty =
        labels_match(&Labels::new(), &required_data_label_matchers)?;
    let selected_data_labels = data_label_matchers
        .iter()
        .filter(|matcher| !matches!(matcher.name.as_str(), "__name__" | "job" | "instance"))
        .map(|matcher| matcher.name.clone())
        .collect::<BTreeSet<_>>();
    let restrict_data_labels = data_label_selector.is_some() && !selected_data_labels.is_empty();
    Ok(InfoContext {
        data_label_selector,
        data_label_matchers,
        required_data_label_matchers_match_empty,
        selected_data_labels,
        restrict_data_labels,
    })
}

/// Join input series with overlapping `target_info` series.
pub(super) fn apply_info(
    samples: Vec<InstantSample>,
    info_by_key: &BTreeMap<String, InstantSample>,
    context: &InfoContext<'_>,
) -> Vec<InstantSample> {
    samples
        .into_iter()
        .filter_map(|mut sample| {
            if sample.labels.get("__name__") == Some("target_info") {
                return Some(sample);
            }
            let key = info_identifying_key(&sample.labels)?;
            let Some(info) = info_by_key.get(&key) else {
                return if context.data_label_selector.is_some()
                    && !context.required_data_label_matchers_match_empty
                {
                    None
                } else {
                    Some(sample)
                };
            };
            for (name, value) in info.labels.iter() {
                if matches!(name.as_str(), "__name__" | "job" | "instance") {
                    continue;
                }
                if context.restrict_data_labels && !context.selected_data_labels.contains(name) {
                    continue;
                }
                if sample.labels.get(name).is_none() {
                    sample.labels.insert(name, value);
                }
            }
            Some(sample)
        })
        .collect()
}

pub(super) fn info_samples_by_identifying_key(
    info_samples: Vec<InstantSample>,
    data_label_matchers: &[LabelMatcher],
) -> Result<BTreeMap<String, InstantSample>> {
    // Precompile the regex matchers once before the per-sample loop.
    let compiled = compile_label_matchers(data_label_matchers)?;
    let mut info_by_key = BTreeMap::<String, InstantSample>::new();
    for sample in info_samples {
        if matches!(sample.value, SampleValue::Histogram(_)) {
            return Err(PromqlError::Plan(
                "info series selector must match float samples".to_string(),
            ));
        }
        if !compiled.matches(&sample.labels) {
            continue;
        }
        let Some(key) = info_identifying_key(&sample.labels) else {
            continue;
        };
        info_by_key
            .entry(key)
            .and_modify(|existing| {
                if sample.ts_ms > existing.ts_ms {
                    *existing = sample.clone();
                } else if sample.ts_ms == existing.ts_ms {
                    for (name, value) in sample.labels.iter() {
                        if existing.labels.get(name).is_none() {
                            existing.labels.insert(name, value);
                        }
                    }
                }
            })
            .or_insert(sample);
    }
    Ok(info_by_key)
}
