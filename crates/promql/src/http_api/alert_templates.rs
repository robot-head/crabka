use std::collections::BTreeMap;

use crabka_blockstore::Labels;
use serde_json::{Map, Value};

use super::format_sample_value;

/// Expands the minimal Prometheus alert-template subset for annotation and
/// label values.
///
/// This function ignores whitespace inside the braces. It supports these
/// actions:
/// - `{{ $value }}` -> the firing sample value through [`format_sample_value`].
/// - `{{ $labels.NAME }}` / `{{ $labels."NAME" }}` -> the series label `NAME`,
///   or "" when the label is absent.
///
/// This function passes through any other `{{ ... }}` action unchanged.
/// Prometheus's `humanize` and the related functions are out of scope.
pub(crate) fn expand_alert_template(tmpl: &str, value: f64, labels: &Labels) -> String {
    let mut out = String::with_capacity(tmpl.len());
    let mut rest = tmpl;
    while let Some(open) = rest.find("{{") {
        out.push_str(&rest[..open]);
        let after_open = &rest[open + 2..];
        let Some(close) = after_open.find("}}") else {
            // No closing braces: emit the remainder verbatim.
            out.push_str(&rest[open..]);
            rest = "";
            break;
        };
        let action = after_open[..close].trim();
        let full = &rest[open..open + 2 + close + 2];
        match expand_alert_action(action, value, labels) {
            Some(expanded) => out.push_str(&expanded),
            None => out.push_str(full),
        }
        rest = &after_open[close + 2..];
    }
    out.push_str(rest);
    out
}

/// Builds a [`Labels`] set from an alert label map for template `$labels.NAME`
/// lookups.
pub(super) fn labels_from_map(map: &BTreeMap<String, String>) -> Labels {
    let mut labels = Labels::new();
    for (name, value) in map {
        labels.insert(name, value);
    }
    labels
}

/// Applies [`expand_alert_template`] to every string value of a JSON object.
///
/// Keys and non-string values stay unchanged. Alert annotation maps use this
/// function.
pub(super) fn expand_alert_mapping_json(mapping: &Value, value: f64, labels: &Labels) -> Value {
    let Value::Object(object) = mapping else {
        return mapping.clone();
    };
    let expanded = object
        .iter()
        .map(|(key, entry)| {
            let expanded = entry.as_str().map_or_else(
                || entry.clone(),
                |text| Value::String(expand_alert_template(text, value, labels)),
            );
            (key.clone(), expanded)
        })
        .collect::<Map<_, _>>();
    Value::Object(expanded)
}

fn expand_alert_action(action: &str, value: f64, labels: &Labels) -> Option<String> {
    if action == "$value" {
        return Some(format_sample_value(value));
    }
    if let Some(label_ref) = action.strip_prefix("$labels.") {
        let name = label_ref.trim();
        let name = name
            .strip_prefix('"')
            .and_then(|inner| inner.strip_suffix('"'))
            .unwrap_or(name);
        let resolved = labels
            .iter()
            .find(|(label, _)| label.as_str() == name)
            .map(|(_, label_value)| label_value.clone())
            .unwrap_or_default();
        return Some(resolved);
    }
    None
}
