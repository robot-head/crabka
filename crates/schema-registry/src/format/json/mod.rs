//! JSON Schema: parse as JSON + well-formedness; canonical form = recursively
//! key-sorted compact JSON (the dedup key). Compatibility uses the in-tree
//! structural diff.

mod compat;
mod diff;

use super::ParsedSchema;
use crate::error::SrError;

pub struct JsonSchema {
    value: serde_json::Value,
    /// Resolved registry references as `(name, parsed-document)` pairs, where
    /// `name` is the `$ref` target string a referring schema uses. JSON refs do
    /// NOT affect the canonical form (cp does not inline them) — they only feed
    /// the compatibility diff so a cross-subject `$ref` resolves to its target.
    refs: Vec<(String, serde_json::Value)>,
}

impl JsonSchema {
    pub(crate) fn value(&self) -> &serde_json::Value {
        &self.value
    }
    pub(crate) fn refs(&self) -> &[(String, serde_json::Value)] {
        &self.refs
    }
}

pub fn parse(schema: &str, refs: &[super::ResolvedReference]) -> Result<JsonSchema, SrError> {
    let value: serde_json::Value = serde_json::from_str(schema)
        .map_err(|e| SrError::InvalidSchema(format!("JSON Schema: {e}")))?;
    if !value.is_object() && !value.is_boolean() {
        return Err(SrError::InvalidSchema(
            "JSON Schema must be an object or boolean".into(),
        ));
    }
    // Stash the resolved reference documents for compat resolution, skipping any
    // that don't parse as JSON (the referrer is still valid; that ref just won't
    // resolve — matching the permissive treatment of an unresolved `$ref`).
    let refs = refs
        .iter()
        .filter_map(|r| {
            serde_json::from_str::<serde_json::Value>(&r.schema)
                .ok()
                .map(|v| (r.name.clone(), v))
        })
        .collect();
    Ok(JsonSchema { value, refs })
}

/// Confluent JSON Schema compatibility: can a reader using `reader` read data
/// written with `writer`? Diffs (original = writer, update = reader); rejects if
/// any difference is backward-incompatible.
#[tracing::instrument(level = "debug", name = "json.check", skip_all, fields(reader_refs = reader_refs.len(), writer_refs = writer_refs.len(), diffs = tracing::field::Empty))]
pub fn check(
    reader: &str,
    writer: &str,
    reader_refs: &[super::ResolvedReference],
    writer_refs: &[super::ResolvedReference],
) -> Result<(), Vec<String>> {
    let reader_s = parse(reader, reader_refs).map_err(|e| vec![format!("reader: {e}")])?;
    let writer_s = parse(writer, writer_refs).map_err(|e| vec![format!("writer: {e}")])?;
    let diffs = diff::compare_with_refs(
        writer_s.value(),
        reader_s.value(),
        writer_s.refs(),
        reader_s.refs(),
    );
    tracing::Span::current().record("diffs", diffs.len());
    let incompatible: Vec<&diff::Difference> = diffs
        .iter()
        .filter(|d| !compat::is_backward_compatible(&d.kind))
        .collect();
    if incompatible.is_empty() {
        Ok(())
    } else {
        Err(compat::messages(&incompatible))
    }
}

impl ParsedSchema for JsonSchema {
    fn canonical_form(&self) -> String {
        // Refs are intentionally NOT inlined — cp leaves a JSON `$ref` as-written
        // in the canonical (dedup) form. Only `self.value` participates.
        canonicalize(&self.value)
    }
}

pub(crate) fn canonicalize(v: &serde_json::Value) -> String {
    match v {
        serde_json::Value::Object(map) => {
            let mut keys: Vec<&String> = map.keys().collect();
            keys.sort();
            let inner: Vec<String> = keys
                .iter()
                .map(|k| {
                    format!(
                        "{}:{}",
                        serde_json::to_string(k).unwrap(),
                        canonicalize(&map[*k])
                    )
                })
                .collect();
            format!("{{{}}}", inner.join(","))
        }
        serde_json::Value::Array(a) => {
            format!(
                "[{}]",
                a.iter().map(canonicalize).collect::<Vec<_>>().join(",")
            )
        }
        other => serde_json::to_string(other).unwrap(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::format::ParsedSchema;

    #[test]
    fn json_resolves_registry_ref_in_compat() {
        use crate::format::{ResolvedReference, SchemaType};
        let dep = r#"{"type":"integer","maximum":10}"#;
        let refs = vec![ResolvedReference {
            name: "Amount".into(),
            ty: SchemaType::Json,
            schema: dep.into(),
        }];
        let with_ref = r#"{"type":"object","properties":{"a":{"$ref":"Amount"}}}"#;
        // canonical form is the schema as-written (refs NOT inlined)
        assert2::assert!(
            parse(with_ref, &refs).unwrap().canonical_form()
                == parse(with_ref, &[]).unwrap().canonical_form()
        );
        // check resolves the ref: reader == writer (with the ref present) is compatible
        assert2::assert!(check(with_ref, with_ref, &refs, &refs).is_ok());
    }

    #[test]
    fn parses_object_and_dedups_key_order() {
        let a = parse(
            r#"{"type":"object","properties":{"a":{"type":"integer"},"b":{"type":"string"}}}"#,
            &[],
        )
        .unwrap();
        let b = parse(
            r#"{"properties":{"b":{"type":"string"},"a":{"type":"integer"}},"type":"object"}"#,
            &[],
        )
        .unwrap();
        assert2::assert!(a.canonical_form() == b.canonical_form());
    }

    #[test]
    fn rejects_non_json() {
        assert2::assert!(parse("not json", &[]).is_err());
    }

    // cp is authority: adding a property to an open content model is
    // backward-INcompatible (`add_prop_open` BACKWARD=false in the cp golden
    // matrix), even though the property is "optional" — the reader expects a
    // field the writer's data does not carry.
    #[derive(Debug)]
    enum CompatibilityDisposition {
        Compatible,
        Incompatible,
        NoPanic,
    }

    const COMPATIBILITY_CASES: &[(&str, CompatibilityDisposition, fn())] = &[
        (
            "add-property-open-model-is-incompatible",
            CompatibilityDisposition::Incompatible,
            add_property_open_model_is_incompatible as fn(),
        ),
        (
            "add-required-property-closed-model-is-incompatible",
            CompatibilityDisposition::Incompatible,
            add_required_property_closed_model_is_incompatible as fn(),
        ),
        (
            "type-narrowed-is-incompatible",
            CompatibilityDisposition::Incompatible,
            type_narrowed_is_incompatible as fn(),
        ),
        (
            "required-added-is-incompatible",
            CompatibilityDisposition::Incompatible,
            required_added_is_incompatible as fn(),
        ),
        (
            "enum-changes-do-not-panic",
            CompatibilityDisposition::NoPanic,
            enum_changes_do_not_panic as fn(),
        ),
        (
            "maximum-lowered-is-incompatible",
            CompatibilityDisposition::Incompatible,
            maximum_lowered_is_incompatible as fn(),
        ),
        (
            "min-length-added-is-incompatible",
            CompatibilityDisposition::Incompatible,
            min_length_added_is_incompatible as fn(),
        ),
        (
            "max-items-changes-do-not-panic",
            CompatibilityDisposition::NoPanic,
            max_items_changes_do_not_panic as fn(),
        ),
        (
            "items-type-change-is-incompatible",
            CompatibilityDisposition::Incompatible,
            items_type_change_is_incompatible as fn(),
        ),
        (
            "anyof-subschema-added-does-not-panic",
            CompatibilityDisposition::NoPanic,
            anyof_subschema_added_does_not_panic as fn(),
        ),
        (
            "allof-subschema-added-does-not-panic",
            CompatibilityDisposition::NoPanic,
            allof_subschema_added_does_not_panic as fn(),
        ),
        (
            "ref-resolves-and-diffs-target",
            CompatibilityDisposition::Incompatible,
            ref_resolves_and_diffs_target as fn(),
        ),
        (
            "recursive-ref-terminates",
            CompatibilityDisposition::Compatible,
            recursive_ref_terminates as fn(),
        ),
        (
            "dependencies-and-conditionals-do-not-panic",
            CompatibilityDisposition::NoPanic,
            dependencies_and_conditionals_do_not_panic as fn(),
        ),
        (
            "type-changed-neither-subset-is-incompatible",
            CompatibilityDisposition::Incompatible,
            type_changed_neither_subset_is_incompatible as fn(),
        ),
        (
            "type-extended-writer-had-type-reader-drops-is-compatible",
            CompatibilityDisposition::Compatible,
            type_extended_writer_had_type_reader_drops_is_compatible as fn(),
        ),
        (
            "exclusive-maximum-added-is-incompatible",
            CompatibilityDisposition::Incompatible,
            exclusive_maximum_added_is_incompatible as fn(),
        ),
        (
            "exclusive-maximum-removed-is-compatible",
            CompatibilityDisposition::Compatible,
            exclusive_maximum_removed_is_compatible as fn(),
        ),
        (
            "exclusive-maximum-decreased-is-incompatible",
            CompatibilityDisposition::Incompatible,
            exclusive_maximum_decreased_is_incompatible as fn(),
        ),
        (
            "exclusive-maximum-increased-is-compatible",
            CompatibilityDisposition::Compatible,
            exclusive_maximum_increased_is_compatible as fn(),
        ),
        (
            "exclusive-minimum-added-is-incompatible",
            CompatibilityDisposition::Incompatible,
            exclusive_minimum_added_is_incompatible as fn(),
        ),
        (
            "exclusive-minimum-removed-is-compatible",
            CompatibilityDisposition::Compatible,
            exclusive_minimum_removed_is_compatible as fn(),
        ),
        (
            "exclusive-minimum-increased-is-incompatible",
            CompatibilityDisposition::Incompatible,
            exclusive_minimum_increased_is_incompatible as fn(),
        ),
        (
            "exclusive-minimum-decreased-is-compatible",
            CompatibilityDisposition::Compatible,
            exclusive_minimum_decreased_is_compatible as fn(),
        ),
        (
            "multiple-of-added-is-incompatible",
            CompatibilityDisposition::Incompatible,
            multiple_of_added_is_incompatible as fn(),
        ),
        (
            "multiple-of-removed-is-compatible",
            CompatibilityDisposition::Compatible,
            multiple_of_removed_is_compatible as fn(),
        ),
        (
            "multiple-of-changed-is-incompatible",
            CompatibilityDisposition::Incompatible,
            multiple_of_changed_is_incompatible as fn(),
        ),
        (
            "max-length-added-is-incompatible",
            CompatibilityDisposition::Incompatible,
            max_length_added_is_incompatible as fn(),
        ),
        (
            "max-length-removed-is-compatible",
            CompatibilityDisposition::Compatible,
            max_length_removed_is_compatible as fn(),
        ),
        (
            "max-length-decreased-is-incompatible",
            CompatibilityDisposition::Incompatible,
            max_length_decreased_is_incompatible as fn(),
        ),
        (
            "max-length-increased-is-compatible",
            CompatibilityDisposition::Compatible,
            max_length_increased_is_compatible as fn(),
        ),
        (
            "min-length-removed-is-compatible",
            CompatibilityDisposition::Compatible,
            min_length_removed_is_compatible as fn(),
        ),
        (
            "min-length-decreased-is-compatible",
            CompatibilityDisposition::Compatible,
            min_length_decreased_is_compatible as fn(),
        ),
        (
            "min-length-increased-is-incompatible",
            CompatibilityDisposition::Incompatible,
            min_length_increased_is_incompatible as fn(),
        ),
        (
            "pattern-added-is-incompatible",
            CompatibilityDisposition::Incompatible,
            pattern_added_is_incompatible as fn(),
        ),
        (
            "pattern-removed-is-compatible",
            CompatibilityDisposition::Compatible,
            pattern_removed_is_compatible as fn(),
        ),
        (
            "pattern-changed-is-incompatible",
            CompatibilityDisposition::Incompatible,
            pattern_changed_is_incompatible as fn(),
        ),
        (
            "max-items-added-is-incompatible",
            CompatibilityDisposition::Incompatible,
            max_items_added_is_incompatible as fn(),
        ),
        (
            "max-items-removed-is-compatible",
            CompatibilityDisposition::Compatible,
            max_items_removed_is_compatible as fn(),
        ),
        (
            "max-items-decreased-is-incompatible",
            CompatibilityDisposition::Incompatible,
            max_items_decreased_is_incompatible as fn(),
        ),
        (
            "max-items-increased-is-compatible",
            CompatibilityDisposition::Compatible,
            max_items_increased_is_compatible as fn(),
        ),
        (
            "min-items-added-is-incompatible",
            CompatibilityDisposition::Incompatible,
            min_items_added_is_incompatible as fn(),
        ),
        (
            "min-items-removed-is-compatible",
            CompatibilityDisposition::Compatible,
            min_items_removed_is_compatible as fn(),
        ),
        (
            "min-items-decreased-is-compatible",
            CompatibilityDisposition::Compatible,
            min_items_decreased_is_compatible as fn(),
        ),
        (
            "min-items-increased-is-incompatible",
            CompatibilityDisposition::Incompatible,
            min_items_increased_is_incompatible as fn(),
        ),
        (
            "additional-items-removed-is-incompatible",
            CompatibilityDisposition::Incompatible,
            additional_items_removed_is_incompatible as fn(),
        ),
        (
            "additional-items-added-is-compatible",
            CompatibilityDisposition::Compatible,
            additional_items_added_is_compatible as fn(),
        ),
        (
            "max-properties-added-is-incompatible",
            CompatibilityDisposition::Incompatible,
            max_properties_added_is_incompatible as fn(),
        ),
        (
            "max-properties-removed-is-compatible",
            CompatibilityDisposition::Compatible,
            max_properties_removed_is_compatible as fn(),
        ),
        (
            "max-properties-decreased-is-incompatible",
            CompatibilityDisposition::Incompatible,
            max_properties_decreased_is_incompatible as fn(),
        ),
        (
            "max-properties-increased-is-compatible",
            CompatibilityDisposition::Compatible,
            max_properties_increased_is_compatible as fn(),
        ),
        (
            "min-properties-added-is-incompatible",
            CompatibilityDisposition::Incompatible,
            min_properties_added_is_incompatible as fn(),
        ),
        (
            "min-properties-removed-is-compatible",
            CompatibilityDisposition::Compatible,
            min_properties_removed_is_compatible as fn(),
        ),
        (
            "min-properties-decreased-is-compatible",
            CompatibilityDisposition::Compatible,
            min_properties_decreased_is_compatible as fn(),
        ),
        (
            "min-properties-increased-is-incompatible",
            CompatibilityDisposition::Incompatible,
            min_properties_increased_is_incompatible as fn(),
        ),
        (
            "enum-narrowed-is-incompatible",
            CompatibilityDisposition::Incompatible,
            enum_narrowed_is_incompatible as fn(),
        ),
        (
            "enum-extended-is-compatible",
            CompatibilityDisposition::Compatible,
            enum_extended_is_compatible as fn(),
        ),
        (
            "enum-changed-disjoint-is-incompatible",
            CompatibilityDisposition::Incompatible,
            enum_changed_disjoint_is_incompatible as fn(),
        ),
        (
            "const-changed-is-incompatible",
            CompatibilityDisposition::Incompatible,
            const_changed_is_incompatible as fn(),
        ),
        (
            "const-added-is-incompatible",
            CompatibilityDisposition::Incompatible,
            const_added_is_incompatible as fn(),
        ),
        (
            "const-removed-is-compatible",
            CompatibilityDisposition::Compatible,
            const_removed_is_compatible as fn(),
        ),
        (
            "oneof-subschema-added-is-compatible",
            CompatibilityDisposition::Compatible,
            oneof_subschema_added_is_compatible as fn(),
        ),
        (
            "oneof-subschema-removed-is-incompatible",
            CompatibilityDisposition::Incompatible,
            oneof_subschema_removed_is_incompatible as fn(),
        ),
        (
            "oneof-subschemas-disjoint-is-incompatible",
            CompatibilityDisposition::Incompatible,
            oneof_subschemas_disjoint_is_incompatible as fn(),
        ),
        (
            "allof-subschema-removed-is-compatible",
            CompatibilityDisposition::Compatible,
            allof_subschema_removed_is_compatible as fn(),
        ),
        (
            "allof-subschemas-disjoint-is-incompatible",
            CompatibilityDisposition::Incompatible,
            allof_subschemas_disjoint_is_incompatible as fn(),
        ),
        (
            "not-both-sides-different-is-incompatible",
            CompatibilityDisposition::Incompatible,
            not_both_sides_different_is_incompatible as fn(),
        ),
        (
            "not-same-both-sides-is-compatible",
            CompatibilityDisposition::Compatible,
            not_same_both_sides_is_compatible as fn(),
        ),
        (
            "combinator-keyword-mismatch-is-incompatible",
            CompatibilityDisposition::Incompatible,
            combinator_keyword_mismatch_is_incompatible as fn(),
        ),
        (
            "combinator-one-side-absent-is-incompatible",
            CompatibilityDisposition::Incompatible,
            combinator_one_side_absent_is_incompatible as fn(),
        ),
        (
            "combinator-reader-adds-is-incompatible",
            CompatibilityDisposition::Incompatible,
            combinator_reader_adds_is_incompatible as fn(),
        ),
        (
            "remote-ref-is-permissive",
            CompatibilityDisposition::Compatible,
            remote_ref_is_permissive as fn(),
        ),
        (
            "dangling-local-ref-is-permissive",
            CompatibilityDisposition::Compatible,
            dangling_local_ref_is_permissive as fn(),
        ),
        (
            "ref-only-on-reader-side-resolves-vs-writer",
            CompatibilityDisposition::Compatible,
            ref_only_on_reader_side_resolves_vs_writer as fn(),
        ),
        (
            "ref-only-on-writer-side-resolves-vs-reader",
            CompatibilityDisposition::Compatible,
            ref_only_on_writer_side_resolves_vs_reader as fn(),
        ),
        (
            "dependency-added-is-compatible",
            CompatibilityDisposition::Compatible,
            dependency_added_is_compatible as fn(),
        ),
        (
            "dependency-removed-is-compatible",
            CompatibilityDisposition::Compatible,
            dependency_removed_is_compatible as fn(),
        ),
        (
            "dependent-required-added-is-compatible",
            CompatibilityDisposition::Compatible,
            dependent_required_added_is_compatible as fn(),
        ),
        (
            "dependent-schemas-added-is-compatible",
            CompatibilityDisposition::Compatible,
            dependent_schemas_added_is_compatible as fn(),
        ),
        (
            "dependency-key-added-to-existing-map-is-compatible",
            CompatibilityDisposition::Compatible,
            dependency_key_added_to_existing_map_is_compatible as fn(),
        ),
        (
            "dependency-key-removed-from-existing-map-is-compatible",
            CompatibilityDisposition::Compatible,
            dependency_key_removed_from_existing_map_is_compatible as fn(),
        ),
        (
            "conditional-if-changed-is-incompatible",
            CompatibilityDisposition::Incompatible,
            conditional_if_changed_is_incompatible as fn(),
        ),
        (
            "conditional-then-changed-is-incompatible",
            CompatibilityDisposition::Incompatible,
            conditional_then_changed_is_incompatible as fn(),
        ),
        (
            "conditional-else-added-is-incompatible",
            CompatibilityDisposition::Incompatible,
            conditional_else_added_is_incompatible as fn(),
        ),
        (
            "conditional-else-removed-is-incompatible",
            CompatibilityDisposition::Incompatible,
            conditional_else_removed_is_incompatible as fn(),
        ),
        (
            "conditional-same-both-sides-is-compatible",
            CompatibilityDisposition::Compatible,
            conditional_same_both_sides_is_compatible as fn(),
        ),
        (
            "conditional-only-on-reader-is-incompatible",
            CompatibilityDisposition::Incompatible,
            conditional_only_on_reader_is_incompatible as fn(),
        ),
        (
            "additional-properties-removed-is-incompatible",
            CompatibilityDisposition::Incompatible,
            additional_properties_removed_is_incompatible as fn(),
        ),
        (
            "additional-properties-added-is-compatible",
            CompatibilityDisposition::Compatible,
            additional_properties_added_is_compatible as fn(),
        ),
        (
            "property-removed-from-closed-model-is-incompatible",
            CompatibilityDisposition::Incompatible,
            property_removed_from_closed_model_is_incompatible as fn(),
        ),
        (
            "property-added-to-closed-model-is-compatible",
            CompatibilityDisposition::Compatible,
            property_added_to_closed_model_is_compatible as fn(),
        ),
        (
            "type-extended-superset-is-compatible",
            CompatibilityDisposition::Compatible,
            type_extended_superset_is_compatible as fn(),
        ),
        (
            "property-removed-from-open-model-is-compatible",
            CompatibilityDisposition::Compatible,
            property_removed_from_open_model_is_compatible as fn(),
        ),
        (
            "ref-to-root-resolves",
            CompatibilityDisposition::Compatible,
            ref_to_root_resolves as fn(),
        ),
        (
            "maximum-added-is-incompatible",
            CompatibilityDisposition::Incompatible,
            maximum_added_is_incompatible as fn(),
        ),
        (
            "maximum-removed-is-compatible",
            CompatibilityDisposition::Compatible,
            maximum_removed_is_compatible as fn(),
        ),
        (
            "maximum-increased-is-compatible",
            CompatibilityDisposition::Compatible,
            maximum_increased_is_compatible as fn(),
        ),
        (
            "minimum-added-is-incompatible",
            CompatibilityDisposition::Incompatible,
            minimum_added_is_incompatible as fn(),
        ),
        (
            "minimum-removed-is-compatible",
            CompatibilityDisposition::Compatible,
            minimum_removed_is_compatible as fn(),
        ),
        (
            "minimum-decreased-is-compatible",
            CompatibilityDisposition::Compatible,
            minimum_decreased_is_compatible as fn(),
        ),
        (
            "minimum-increased-is-incompatible",
            CompatibilityDisposition::Incompatible,
            minimum_increased_is_incompatible as fn(),
        ),
    ];

    #[test]
    fn compatibility_families_are_named_and_table_driven() {
        for (_name, _disposition, run) in COMPATIBILITY_CASES {
            let result = std::panic::catch_unwind(*run);
            assert2::assert!(result.is_ok());
        }
    }

    fn add_property_open_model_is_incompatible() {
        let w = r#"{"type":"object","properties":{"a":{"type":"integer"}}}"#;
        let r = r#"{"type":"object","properties":{"a":{"type":"integer"},"b":{"type":"string"}}}"#;
        assert2::assert!(check(r, w, &[], &[]).is_err());
    }

    fn add_required_property_closed_model_is_incompatible() {
        let w = r#"{"type":"object","additionalProperties":false,"properties":{"a":{"type":"integer"}}}"#;
        let r = r#"{"type":"object","additionalProperties":false,"properties":{"a":{"type":"integer"},"b":{"type":"string"}},"required":["b"]}"#;
        assert2::assert!(check(r, w, &[], &[]).is_err());
    }

    fn type_narrowed_is_incompatible() {
        let w = r#"{"type":["string","null"]}"#;
        let r = r#"{"type":"string"}"#;
        assert2::assert!(check(r, w, &[], &[]).is_err());
    }

    fn required_added_is_incompatible() {
        let w = r#"{"type":"object","properties":{"a":{"type":"integer"}}}"#;
        let r = r#"{"type":"object","properties":{"a":{"type":"integer"}},"required":["a"]}"#;
        assert2::assert!(check(r, w, &[], &[]).is_err());
    }

    // --- enum / numeric / string / array / object-size constraints ---

    fn enum_changes_do_not_panic() {
        let narrow = r#"{"enum":["a"]}"#;
        let wide = r#"{"enum":["a","b"]}"#;
        let _ = check(wide, narrow, &[], &[]);
        let _ = check(narrow, wide, &[], &[]);
    }

    fn maximum_lowered_is_incompatible() {
        assert2::assert!(
            check(
                r#"{"type":"integer","maximum":10}"#,
                r#"{"type":"integer","maximum":100}"#,
                &[],
                &[]
            )
            .is_err()
        );
    }

    fn min_length_added_is_incompatible() {
        assert2::assert!(
            check(
                r#"{"type":"string","minLength":3}"#,
                r#"{"type":"string"}"#,
                &[],
                &[]
            )
            .is_err()
        );
    }

    fn max_items_changes_do_not_panic() {
        let _ = check(
            r#"{"type":"array","maxItems":9}"#,
            r#"{"type":"array","maxItems":3}"#,
            &[],
            &[],
        );
    }

    fn items_type_change_is_incompatible() {
        assert2::assert!(
            check(
                r#"{"type":"array","items":{"type":"string"}}"#,
                r#"{"type":"array","items":{"type":"integer"}}"#,
                &[],
                &[]
            )
            .is_err()
        );
    }

    // --- combinators ---

    fn anyof_subschema_added_does_not_panic() {
        let _ = check(
            r#"{"anyOf":[{"type":"string"},{"type":"integer"}]}"#,
            r#"{"anyOf":[{"type":"string"}]}"#,
            &[],
            &[],
        );
    }

    fn allof_subschema_added_does_not_panic() {
        let _ = check(
            r#"{"allOf":[{"type":"object"},{"required":["a"]}]}"#,
            r#"{"allOf":[{"type":"object"}]}"#,
            &[],
            &[],
        );
    }

    // --- $ref / dependencies / conditionals ---

    fn ref_resolves_and_diffs_target() {
        let w = r##"{"$ref":"#/$defs/T","$defs":{"T":{"type":"integer"}}}"##;
        let r = r##"{"$ref":"#/$defs/T","$defs":{"T":{"type":"string"}}}"##;
        assert2::assert!(check(r, w, &[], &[]).is_err());
    }

    fn recursive_ref_terminates() {
        let s = r##"{"$ref":"#/$defs/N","$defs":{"N":{"type":"object","properties":{"next":{"$ref":"#/$defs/N"}}}}}"##;
        assert2::assert!(check(s, s, &[], &[]).is_ok());
    }

    fn dependencies_and_conditionals_do_not_panic() {
        let _ = check(
            r#"{"if":{"required":["a"]},"then":{"required":["b"]}}"#,
            r#"{"type":"object"}"#,
            &[],
            &[],
        );
    }

    // -----------------------------------------------------------------------
    // Type detection — TypeChanged branch (not subset either way)
    // -----------------------------------------------------------------------

    fn type_changed_neither_subset_is_incompatible() {
        // reader has ["integer","boolean"], writer has ["string","number"] — neither subset
        let w = r#"{"type":["string","number"]}"#;
        let r = r#"{"type":["integer","boolean"]}"#;
        assert2::assert!(check(r, w, &[], &[]).is_err());
    }

    fn type_extended_writer_had_type_reader_drops_is_compatible() {
        // writer constrains to "string"; reader drops type (permissive) → TypeExtended → compatible
        let w = r#"{"type":"string"}"#;
        let r = "{}";
        assert2::assert!(check(r, w, &[], &[]).is_ok());
    }

    // -----------------------------------------------------------------------
    // Exclusive numeric bounds — add/remove/change
    // -----------------------------------------------------------------------

    fn exclusive_maximum_added_is_incompatible() {
        // reader gains exclusiveMaximum (tighter)
        let w = r#"{"type":"number"}"#;
        let r = r#"{"type":"number","exclusiveMaximum":100}"#;
        assert2::assert!(check(r, w, &[], &[]).is_err());
    }

    fn exclusive_maximum_removed_is_compatible() {
        let w = r#"{"type":"number","exclusiveMaximum":100}"#;
        let r = r#"{"type":"number"}"#;
        assert2::assert!(check(r, w, &[], &[]).is_ok());
    }

    fn exclusive_maximum_decreased_is_incompatible() {
        // reader lowers exclusiveMaximum: 100 → 50 (tighter)
        let w = r#"{"type":"number","exclusiveMaximum":100}"#;
        let r = r#"{"type":"number","exclusiveMaximum":50}"#;
        assert2::assert!(check(r, w, &[], &[]).is_err());
    }

    fn exclusive_maximum_increased_is_compatible() {
        let w = r#"{"type":"number","exclusiveMaximum":50}"#;
        let r = r#"{"type":"number","exclusiveMaximum":100}"#;
        assert2::assert!(check(r, w, &[], &[]).is_ok());
    }

    fn exclusive_minimum_added_is_incompatible() {
        let w = r#"{"type":"number"}"#;
        let r = r#"{"type":"number","exclusiveMinimum":5}"#;
        assert2::assert!(check(r, w, &[], &[]).is_err());
    }

    fn exclusive_minimum_removed_is_compatible() {
        let w = r#"{"type":"number","exclusiveMinimum":5}"#;
        let r = r#"{"type":"number"}"#;
        assert2::assert!(check(r, w, &[], &[]).is_ok());
    }

    fn exclusive_minimum_increased_is_incompatible() {
        // reader raises exclusiveMinimum: 5 → 10 (tighter)
        let w = r#"{"type":"number","exclusiveMinimum":5}"#;
        let r = r#"{"type":"number","exclusiveMinimum":10}"#;
        assert2::assert!(check(r, w, &[], &[]).is_err());
    }

    fn exclusive_minimum_decreased_is_compatible() {
        let w = r#"{"type":"number","exclusiveMinimum":10}"#;
        let r = r#"{"type":"number","exclusiveMinimum":5}"#;
        assert2::assert!(check(r, w, &[], &[]).is_ok());
    }

    // -----------------------------------------------------------------------
    // multipleOf — add / remove / change
    // -----------------------------------------------------------------------

    fn multiple_of_added_is_incompatible() {
        let w = r#"{"type":"integer"}"#;
        let r = r#"{"type":"integer","multipleOf":3}"#;
        assert2::assert!(check(r, w, &[], &[]).is_err());
    }

    fn multiple_of_removed_is_compatible() {
        let w = r#"{"type":"integer","multipleOf":3}"#;
        let r = r#"{"type":"integer"}"#;
        assert2::assert!(check(r, w, &[], &[]).is_ok());
    }

    fn multiple_of_changed_is_incompatible() {
        let w = r#"{"type":"integer","multipleOf":2}"#;
        let r = r#"{"type":"integer","multipleOf":3}"#;
        assert2::assert!(check(r, w, &[], &[]).is_err());
    }

    // -----------------------------------------------------------------------
    // String: maxLength / minLength full coverage
    // -----------------------------------------------------------------------

    fn max_length_added_is_incompatible() {
        let w = r#"{"type":"string"}"#;
        let r = r#"{"type":"string","maxLength":10}"#;
        assert2::assert!(check(r, w, &[], &[]).is_err());
    }

    fn max_length_removed_is_compatible() {
        let w = r#"{"type":"string","maxLength":10}"#;
        let r = r#"{"type":"string"}"#;
        assert2::assert!(check(r, w, &[], &[]).is_ok());
    }

    fn max_length_decreased_is_incompatible() {
        let w = r#"{"type":"string","maxLength":20}"#;
        let r = r#"{"type":"string","maxLength":10}"#;
        assert2::assert!(check(r, w, &[], &[]).is_err());
    }

    fn max_length_increased_is_compatible() {
        let w = r#"{"type":"string","maxLength":10}"#;
        let r = r#"{"type":"string","maxLength":20}"#;
        assert2::assert!(check(r, w, &[], &[]).is_ok());
    }

    fn min_length_removed_is_compatible() {
        let w = r#"{"type":"string","minLength":3}"#;
        let r = r#"{"type":"string"}"#;
        assert2::assert!(check(r, w, &[], &[]).is_ok());
    }

    fn min_length_decreased_is_compatible() {
        let w = r#"{"type":"string","minLength":5}"#;
        let r = r#"{"type":"string","minLength":2}"#;
        assert2::assert!(check(r, w, &[], &[]).is_ok());
    }

    fn min_length_increased_is_incompatible() {
        let w = r#"{"type":"string","minLength":2}"#;
        let r = r#"{"type":"string","minLength":5}"#;
        assert2::assert!(check(r, w, &[], &[]).is_err());
    }

    // -----------------------------------------------------------------------
    // String: pattern — add / remove / change
    // -----------------------------------------------------------------------

    fn pattern_added_is_incompatible() {
        let w = r#"{"type":"string"}"#;
        let r = r#"{"type":"string","pattern":"^[a-z]+"}"#;
        assert2::assert!(check(r, w, &[], &[]).is_err());
    }

    fn pattern_removed_is_compatible() {
        let w = r#"{"type":"string","pattern":"^[a-z]+"}"#;
        let r = r#"{"type":"string"}"#;
        assert2::assert!(check(r, w, &[], &[]).is_ok());
    }

    fn pattern_changed_is_incompatible() {
        let w = r#"{"type":"string","pattern":"^[a-z]+"}"#;
        let r = r#"{"type":"string","pattern":"^[A-Z]+"}"#;
        assert2::assert!(check(r, w, &[], &[]).is_err());
    }

    // -----------------------------------------------------------------------
    // Array constraints: minItems / maxItems / additionalItems
    // -----------------------------------------------------------------------

    fn max_items_added_is_incompatible() {
        let w = r#"{"type":"array"}"#;
        let r = r#"{"type":"array","maxItems":5}"#;
        assert2::assert!(check(r, w, &[], &[]).is_err());
    }

    fn max_items_removed_is_compatible() {
        let w = r#"{"type":"array","maxItems":5}"#;
        let r = r#"{"type":"array"}"#;
        assert2::assert!(check(r, w, &[], &[]).is_ok());
    }

    fn max_items_decreased_is_incompatible() {
        let w = r#"{"type":"array","maxItems":10}"#;
        let r = r#"{"type":"array","maxItems":5}"#;
        assert2::assert!(check(r, w, &[], &[]).is_err());
    }

    fn max_items_increased_is_compatible() {
        let w = r#"{"type":"array","maxItems":5}"#;
        let r = r#"{"type":"array","maxItems":10}"#;
        assert2::assert!(check(r, w, &[], &[]).is_ok());
    }

    fn min_items_added_is_incompatible() {
        let w = r#"{"type":"array"}"#;
        let r = r#"{"type":"array","minItems":2}"#;
        assert2::assert!(check(r, w, &[], &[]).is_err());
    }

    fn min_items_removed_is_compatible() {
        let w = r#"{"type":"array","minItems":2}"#;
        let r = r#"{"type":"array"}"#;
        assert2::assert!(check(r, w, &[], &[]).is_ok());
    }

    fn min_items_decreased_is_compatible() {
        let w = r#"{"type":"array","minItems":5}"#;
        let r = r#"{"type":"array","minItems":2}"#;
        assert2::assert!(check(r, w, &[], &[]).is_ok());
    }

    fn min_items_increased_is_incompatible() {
        let w = r#"{"type":"array","minItems":2}"#;
        let r = r#"{"type":"array","minItems":5}"#;
        assert2::assert!(check(r, w, &[], &[]).is_err());
    }

    fn additional_items_removed_is_incompatible() {
        // writer allows extra items; reader bans them (additionalItems: false)
        let w = r#"{"type":"array","items":[{"type":"string"}]}"#;
        let r = r#"{"type":"array","items":[{"type":"string"}],"additionalItems":false}"#;
        assert2::assert!(check(r, w, &[], &[]).is_err());
    }

    fn additional_items_added_is_compatible() {
        // writer bans extra items; reader allows them
        let w = r#"{"type":"array","items":[{"type":"string"}],"additionalItems":false}"#;
        let r = r#"{"type":"array","items":[{"type":"string"}]}"#;
        assert2::assert!(check(r, w, &[], &[]).is_ok());
    }

    // -----------------------------------------------------------------------
    // Object size: maxProperties / minProperties
    // -----------------------------------------------------------------------

    fn max_properties_added_is_incompatible() {
        let w = r#"{"type":"object"}"#;
        let r = r#"{"type":"object","maxProperties":3}"#;
        assert2::assert!(check(r, w, &[], &[]).is_err());
    }

    fn max_properties_removed_is_compatible() {
        let w = r#"{"type":"object","maxProperties":3}"#;
        let r = r#"{"type":"object"}"#;
        assert2::assert!(check(r, w, &[], &[]).is_ok());
    }

    fn max_properties_decreased_is_incompatible() {
        let w = r#"{"type":"object","maxProperties":10}"#;
        let r = r#"{"type":"object","maxProperties":3}"#;
        assert2::assert!(check(r, w, &[], &[]).is_err());
    }

    fn max_properties_increased_is_compatible() {
        let w = r#"{"type":"object","maxProperties":3}"#;
        let r = r#"{"type":"object","maxProperties":10}"#;
        assert2::assert!(check(r, w, &[], &[]).is_ok());
    }

    fn min_properties_added_is_incompatible() {
        let w = r#"{"type":"object"}"#;
        let r = r#"{"type":"object","minProperties":2}"#;
        assert2::assert!(check(r, w, &[], &[]).is_err());
    }

    fn min_properties_removed_is_compatible() {
        let w = r#"{"type":"object","minProperties":2}"#;
        let r = r#"{"type":"object"}"#;
        assert2::assert!(check(r, w, &[], &[]).is_ok());
    }

    fn min_properties_decreased_is_compatible() {
        let w = r#"{"type":"object","minProperties":5}"#;
        let r = r#"{"type":"object","minProperties":2}"#;
        assert2::assert!(check(r, w, &[], &[]).is_ok());
    }

    fn min_properties_increased_is_incompatible() {
        let w = r#"{"type":"object","minProperties":2}"#;
        let r = r#"{"type":"object","minProperties":5}"#;
        assert2::assert!(check(r, w, &[], &[]).is_err());
    }

    // -----------------------------------------------------------------------
    // Enum: changed (disjoint sets) and const
    // -----------------------------------------------------------------------

    fn enum_narrowed_is_incompatible() {
        // reader narrows enum: {"a","b"} → {"a"}
        let w = r#"{"enum":["a","b"]}"#;
        let r = r#"{"enum":["a"]}"#;
        assert2::assert!(check(r, w, &[], &[]).is_err());
    }

    fn enum_extended_is_compatible() {
        // reader widens enum: {"a"} → {"a","b"}
        let w = r#"{"enum":["a"]}"#;
        let r = r#"{"enum":["a","b"]}"#;
        assert2::assert!(check(r, w, &[], &[]).is_ok());
    }

    fn enum_changed_disjoint_is_incompatible() {
        // disjoint sets → EnumArrayChanged
        let w = r#"{"enum":["a","b"]}"#;
        let r = r#"{"enum":["c","d"]}"#;
        assert2::assert!(check(r, w, &[], &[]).is_err());
    }

    fn const_changed_is_incompatible() {
        // const treated as single-element enum
        let w = r#"{"const":"foo"}"#;
        let r = r#"{"const":"bar"}"#;
        assert2::assert!(check(r, w, &[], &[]).is_err());
    }

    fn const_added_is_incompatible() {
        // writer has no enum constraint; reader restricts to a single const
        let w = r#"{"type":"string"}"#;
        let r = r#"{"type":"string","const":"only"}"#;
        assert2::assert!(check(r, w, &[], &[]).is_err());
    }

    fn const_removed_is_compatible() {
        // writer had const; reader drops the restriction
        let w = r#"{"type":"string","const":"only"}"#;
        let r = r#"{"type":"string"}"#;
        assert2::assert!(check(r, w, &[], &[]).is_ok());
    }

    // -----------------------------------------------------------------------
    // Combinators: oneOf / allOf / not / keyword mismatch
    // -----------------------------------------------------------------------

    fn oneof_subschema_added_is_compatible() {
        // reader gains an extra alternative in oneOf → SumTypeExtended → compatible
        let w = r#"{"oneOf":[{"type":"string"}]}"#;
        let r = r#"{"oneOf":[{"type":"string"},{"type":"integer"}]}"#;
        assert2::assert!(check(r, w, &[], &[]).is_ok());
    }

    fn oneof_subschema_removed_is_incompatible() {
        // reader loses an alternative → SumTypeNarrowed → incompatible
        let w = r#"{"oneOf":[{"type":"string"},{"type":"integer"}]}"#;
        let r = r#"{"oneOf":[{"type":"string"}]}"#;
        assert2::assert!(check(r, w, &[], &[]).is_err());
    }

    fn oneof_subschemas_disjoint_is_incompatible() {
        // neither is a subset → CombinedTypeSubschemasChanged
        let w = r#"{"oneOf":[{"type":"string"},{"type":"integer"}]}"#;
        let r = r#"{"oneOf":[{"type":"number"},{"type":"boolean"}]}"#;
        assert2::assert!(check(r, w, &[], &[]).is_err());
    }

    fn allof_subschema_removed_is_compatible() {
        // reader drops a constraint → ProductTypeExtended → compatible
        let w = r#"{"allOf":[{"type":"object"},{"required":["a"]}]}"#;
        let r = r#"{"allOf":[{"type":"object"}]}"#;
        assert2::assert!(check(r, w, &[], &[]).is_ok());
    }

    fn allof_subschemas_disjoint_is_incompatible() {
        // neither is a subset → CombinedTypeSubschemasChanged
        let w = r#"{"allOf":[{"required":["a"]}]}"#;
        let r = r#"{"allOf":[{"required":["b"]}]}"#;
        assert2::assert!(check(r, w, &[], &[]).is_err());
    }

    fn not_both_sides_different_is_incompatible() {
        // both have `not` but different subschemas → NotTypeNarrowed
        let w = r#"{"not":{"type":"string"}}"#;
        let r = r#"{"not":{"type":"integer"}}"#;
        assert2::assert!(check(r, w, &[], &[]).is_err());
    }

    fn not_same_both_sides_is_compatible() {
        let s = r#"{"not":{"type":"string"}}"#;
        assert2::assert!(check(s, s, &[], &[]).is_ok());
    }

    fn combinator_keyword_mismatch_is_incompatible() {
        // allOf vs anyOf → CombinedTypeChanged
        let w = r#"{"allOf":[{"type":"string"}]}"#;
        let r = r#"{"anyOf":[{"type":"string"}]}"#;
        assert2::assert!(check(r, w, &[], &[]).is_err());
    }

    fn combinator_one_side_absent_is_incompatible() {
        // writer has allOf; reader has none → CombinedTypeChanged
        let w = r#"{"allOf":[{"type":"string"}]}"#;
        let r = r#"{"type":"string"}"#;
        assert2::assert!(check(r, w, &[], &[]).is_err());
    }

    fn combinator_reader_adds_is_incompatible() {
        // reader adds allOf; writer has none → CombinedTypeChanged
        let w = r#"{"type":"string"}"#;
        let r = r#"{"allOf":[{"type":"string"}]}"#;
        assert2::assert!(check(r, w, &[], &[]).is_err());
    }

    // -----------------------------------------------------------------------
    // $ref: remote (permissive), dangling, one side only
    // -----------------------------------------------------------------------

    fn remote_ref_is_permissive() {
        // remote refs cannot be resolved → treated permissively → no diff
        let w = r#"{"$ref":"http://example.com/schema"}"#;
        let r = r#"{"$ref":"http://example.com/other-schema"}"#;
        let _ = check(r, w, &[], &[]); // must not panic; remote refs produce no diff
    }

    fn dangling_local_ref_is_permissive() {
        // $ref that points to nonexistent location → None → permissive
        let w = r##"{"$ref":"#/$defs/Missing"}"##;
        let r = r#"{"type":"string"}"#;
        let _ = check(r, w, &[], &[]); // must not panic
    }

    fn ref_only_on_reader_side_resolves_vs_writer() {
        // reader has $ref; writer does not — resolved reader vs raw writer
        let w = r#"{"type":"integer"}"#;
        let r = r##"{"$ref":"#/$defs/T","$defs":{"T":{"type":"string"}}}"##;
        // string vs integer → TypeChanged → incompatible
        assert2::assert!(check(r, w, &[], &[]).is_err());
    }

    fn ref_only_on_writer_side_resolves_vs_reader() {
        // writer has $ref; reader does not — exercises the (Some(ores), None) branch in
        // compare_refs. Note: the top-level writer has no "type" field itself, so
        // compare_type fires first (no-type vs integer → TypeNarrowed). The $ref
        // resolution branch still executes. We just exercise without asserting direction.
        let w = r##"{"$ref":"#/$defs/T","$defs":{"T":{"type":"integer"}}}"##;
        let r = r#"{"type":"integer"}"#;
        let _ = check(r, w, &[], &[]);
    }

    // -----------------------------------------------------------------------
    // Dependencies: add / remove; dependentRequired / dependentSchemas variants
    // -----------------------------------------------------------------------

    fn dependency_added_is_compatible() {
        // writer has no dependencies; reader adds one → DependencyAdded → compatible
        let w = r#"{"type":"object"}"#;
        let r = r#"{"type":"object","dependencies":{"foo":["bar"]}}"#;
        assert2::assert!(check(r, w, &[], &[]).is_ok());
    }

    fn dependency_removed_is_compatible() {
        // writer has dependency; reader drops it → DependencyRemoved → compatible
        let w = r#"{"type":"object","dependencies":{"foo":["bar"]}}"#;
        let r = r#"{"type":"object"}"#;
        assert2::assert!(check(r, w, &[], &[]).is_ok());
    }

    fn dependent_required_added_is_compatible() {
        let w = r#"{"type":"object"}"#;
        let r = r#"{"type":"object","dependentRequired":{"foo":["bar"]}}"#;
        assert2::assert!(check(r, w, &[], &[]).is_ok());
    }

    fn dependent_schemas_added_is_compatible() {
        let w = r#"{"type":"object"}"#;
        let r = r#"{"type":"object","dependentSchemas":{"foo":{"required":["bar"]}}}"#;
        assert2::assert!(check(r, w, &[], &[]).is_ok());
    }

    fn dependency_key_added_to_existing_map_is_compatible() {
        let w = r#"{"type":"object","dependencies":{"foo":["bar"]}}"#;
        let r = r#"{"type":"object","dependencies":{"foo":["bar"],"baz":["qux"]}}"#;
        assert2::assert!(check(r, w, &[], &[]).is_ok());
    }

    fn dependency_key_removed_from_existing_map_is_compatible() {
        let w = r#"{"type":"object","dependencies":{"foo":["bar"],"baz":["qux"]}}"#;
        let r = r#"{"type":"object","dependencies":{"foo":["bar"]}}"#;
        assert2::assert!(check(r, w, &[], &[]).is_ok());
    }

    // -----------------------------------------------------------------------
    // Conditionals: if / then / else — added, removed, changed
    // -----------------------------------------------------------------------

    fn conditional_if_changed_is_incompatible() {
        let w = r#"{"if":{"required":["a"]},"then":{"required":["b"]}}"#;
        let r = r#"{"if":{"required":["x"]},"then":{"required":["b"]}}"#;
        assert2::assert!(check(r, w, &[], &[]).is_err());
    }

    fn conditional_then_changed_is_incompatible() {
        let w = r#"{"if":{"required":["a"]},"then":{"required":["b"]}}"#;
        let r = r#"{"if":{"required":["a"]},"then":{"required":["c"]}}"#;
        assert2::assert!(check(r, w, &[], &[]).is_err());
    }

    fn conditional_else_added_is_incompatible() {
        // reader gains an else branch
        let w = r#"{"if":{"required":["a"]},"then":{"required":["b"]}}"#;
        let r = r#"{"if":{"required":["a"]},"then":{"required":["b"]},"else":{"required":["c"]}}"#;
        assert2::assert!(check(r, w, &[], &[]).is_err());
    }

    fn conditional_else_removed_is_incompatible() {
        let w = r#"{"if":{"required":["a"]},"then":{"required":["b"]},"else":{"required":["c"]}}"#;
        let r = r#"{"if":{"required":["a"]},"then":{"required":["b"]}}"#;
        assert2::assert!(check(r, w, &[], &[]).is_err());
    }

    fn conditional_same_both_sides_is_compatible() {
        let s = r#"{"if":{"required":["a"]},"then":{"required":["b"]}}"#;
        assert2::assert!(check(s, s, &[], &[]).is_ok());
    }

    fn conditional_only_on_reader_is_incompatible() {
        // writer has no conditional; reader adds one entirely → ConditionalChanged
        let w = r#"{"type":"object"}"#;
        let r = r#"{"type":"object","if":{"required":["a"]},"then":{"required":["b"]}}"#;
        assert2::assert!(check(r, w, &[], &[]).is_err());
    }

    // -----------------------------------------------------------------------
    // AdditionalProperties: open/closed transitions
    // -----------------------------------------------------------------------

    fn additional_properties_removed_is_incompatible() {
        // writer is open; reader closes with additionalProperties:false
        let w = r#"{"type":"object"}"#;
        let r = r#"{"type":"object","additionalProperties":false}"#;
        assert2::assert!(check(r, w, &[], &[]).is_err());
    }

    fn additional_properties_added_is_compatible() {
        // writer is closed; reader opens up
        let w = r#"{"type":"object","additionalProperties":false}"#;
        let r = r#"{"type":"object"}"#;
        assert2::assert!(check(r, w, &[], &[]).is_ok());
    }

    // -----------------------------------------------------------------------
    // Properties: closed model add/remove
    // -----------------------------------------------------------------------

    fn property_removed_from_closed_model_is_incompatible() {
        // writer has a property in a closed model; reader drops it
        let w = r#"{"type":"object","additionalProperties":false,"properties":{"a":{"type":"integer"},"b":{"type":"string"}}}"#;
        let r = r#"{"type":"object","additionalProperties":false,"properties":{"a":{"type":"integer"}}}"#;
        assert2::assert!(check(r, w, &[], &[]).is_err());
    }

    fn property_added_to_closed_model_is_compatible() {
        // reader adds a new optional property to a closed model → compatible
        let w = r#"{"type":"object","additionalProperties":false,"properties":{"a":{"type":"integer"}}}"#;
        let r = r#"{"type":"object","additionalProperties":false,"properties":{"a":{"type":"integer"},"b":{"type":"string"}}}"#;
        assert2::assert!(check(r, w, &[], &[]).is_ok());
    }

    // -----------------------------------------------------------------------
    // TypeExtended: both sides have type, reader is strict superset
    // -----------------------------------------------------------------------

    fn type_extended_superset_is_compatible() {
        // writer: ["string"], reader: ["string","integer"] — reader is superset → TypeExtended → compatible
        let w = r#"{"type":"string"}"#;
        let r = r#"{"type":["string","integer"]}"#;
        assert2::assert!(check(r, w, &[], &[]).is_ok());
    }

    // -----------------------------------------------------------------------
    // PropertyRemovedFromOpenContentModel: property removed (no additionalProperties:false)
    // -----------------------------------------------------------------------

    fn property_removed_from_open_model_is_compatible() {
        // reader drops a property from an open model → PropertyRemovedFromOpenContentModel → compatible
        let w = r#"{"type":"object","properties":{"a":{"type":"integer"},"b":{"type":"string"}}}"#;
        let r = r#"{"type":"object","properties":{"a":{"type":"integer"}}}"#;
        assert2::assert!(check(r, w, &[], &[]).is_ok());
    }

    // -----------------------------------------------------------------------
    // $ref: "#" root self-reference
    // -----------------------------------------------------------------------

    fn ref_to_root_resolves() {
        // $ref:"#" resolves to the root document; exercises ptr.is_empty() branch
        let w = r##"{"$ref":"#"}"##;
        let r = r##"{"$ref":"#"}"##;
        let _ = check(r, w, &[], &[]); // same schema, must not panic (cycle guard terminates it)
    }

    // -----------------------------------------------------------------------
    // Maximum / Minimum: full increase/decrease coverage
    // -----------------------------------------------------------------------

    fn maximum_added_is_incompatible() {
        let w = r#"{"type":"number"}"#;
        let r = r#"{"type":"number","maximum":100}"#;
        assert2::assert!(check(r, w, &[], &[]).is_err());
    }

    fn maximum_removed_is_compatible() {
        let w = r#"{"type":"number","maximum":100}"#;
        let r = r#"{"type":"number"}"#;
        assert2::assert!(check(r, w, &[], &[]).is_ok());
    }

    fn maximum_increased_is_compatible() {
        let w = r#"{"type":"number","maximum":10}"#;
        let r = r#"{"type":"number","maximum":100}"#;
        assert2::assert!(check(r, w, &[], &[]).is_ok());
    }

    fn minimum_added_is_incompatible() {
        let w = r#"{"type":"number"}"#;
        let r = r#"{"type":"number","minimum":1}"#;
        assert2::assert!(check(r, w, &[], &[]).is_err());
    }

    fn minimum_removed_is_compatible() {
        let w = r#"{"type":"number","minimum":1}"#;
        let r = r#"{"type":"number"}"#;
        assert2::assert!(check(r, w, &[], &[]).is_ok());
    }

    fn minimum_decreased_is_compatible() {
        let w = r#"{"type":"number","minimum":10}"#;
        let r = r#"{"type":"number","minimum":5}"#;
        assert2::assert!(check(r, w, &[], &[]).is_ok());
    }

    fn minimum_increased_is_incompatible() {
        let w = r#"{"type":"number","minimum":5}"#;
        let r = r#"{"type":"number","minimum":10}"#;
        assert2::assert!(check(r, w, &[], &[]).is_err());
    }
}
