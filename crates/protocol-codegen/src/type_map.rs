//! Map a schema field type string to a Rust type expression.

/// Owned-flavor Rust type for a schema type. `nullable` and `is_struct_ref`
/// shape the wrapping. Struct references must be resolved by the caller and
/// passed in as `is_struct_ref = true` along with the resolved Rust path
/// (e.g., `"super::common::ProduceTopic"`).
#[must_use]
pub fn owned_type(schema_type: &str, nullable: bool, struct_path: Option<&str>) -> String {
    let inner = inner_owned(schema_type, struct_path);
    if nullable {
        format!("Option<{inner}>")
    } else {
        inner
    }
}

/// Borrowed-flavor Rust type. Strings/bytes become `&'a str`/`&'a [u8]`,
/// arrays own their outer `Vec`, struct references take the `<'a>` form.
#[must_use]
pub fn borrowed_type(schema_type: &str, nullable: bool, struct_path: Option<&str>) -> String {
    let inner = inner_borrowed(schema_type, struct_path);
    if nullable {
        format!("Option<{inner}>")
    } else {
        inner
    }
}

fn inner_owned(t: &str, struct_path: Option<&str>) -> String {
    if let Some(elem) = t.strip_prefix("[]") {
        let elem_path = struct_path; // struct_path applies to the element
        return format!("Vec<{}>", inner_owned(elem, elem_path));
    }
    match t {
        "bool" => "bool".into(),
        "int8" => "i8".into(),
        "int16" => "i16".into(),
        "int32" => "i32".into(),
        "int64" => "i64".into(),
        "uint16" => "u16".into(),
        "uint32" => "u32".into(),
        "float64" => "f64".into(),
        "string" => "String".into(),
        "bytes" => "::bytes::Bytes".into(),
        "records" => "crate::records::RecordsPayload".into(),
        "uuid" => "crate::primitives::uuid::Uuid".into(),
        other => struct_path.map_or_else(|| panic!("unmapped owned type: {other}"), str::to_owned),
    }
}

fn inner_borrowed(t: &str, struct_path: Option<&str>) -> String {
    if let Some(elem) = t.strip_prefix("[]") {
        return format!("Vec<{}>", inner_borrowed(elem, struct_path));
    }
    match t {
        "bool" => "bool".into(),
        "int8" => "i8".into(),
        "int16" => "i16".into(),
        "int32" => "i32".into(),
        "int64" => "i64".into(),
        "uint16" => "u16".into(),
        "uint32" => "u32".into(),
        "float64" => "f64".into(),
        "string" => "&'a str".into(),
        "bytes" => "&'a [u8]".into(),
        "records" => "crate::records::RecordsPayloadBorrowed<'a>".into(),
        "uuid" => "crate::primitives::uuid::Uuid".into(),
        other => struct_path.map_or_else(
            || panic!("unmapped borrowed type: {other}"),
            // The caller is responsible for including `<'a>` in the path when needed.
            // Pass the path verbatim.
            str::to_owned,
        ),
    }
}

#[cfg(test)]
mod tests {
    use assert2::check;

    use super::*;

    #[test]
    fn primitives_owned() {
        for (t, nullable, want) in [
            ("int16", false, "i16"),
            ("int32", true, "Option<i32>"),
            ("string", false, "String"),
            ("string", true, "Option<String>"),
            ("bytes", false, "::bytes::Bytes"),
            ("uuid", false, "crate::primitives::uuid::Uuid"),
            ("records", false, "crate::records::RecordsPayload"),
        ] {
            assert2::assert!(owned_type(t, nullable, None) == want);
        }
    }

    #[test]
    fn primitives_borrowed() {
        for (t, nullable, want) in [
            ("string", false, "&'a str"),
            ("bytes", true, "Option<&'a [u8]>"),
            (
                "records",
                false,
                "crate::records::RecordsPayloadBorrowed<'a>",
            ),
        ] {
            assert2::assert!(borrowed_type(t, nullable, None) == want);
        }
    }

    #[test]
    fn arrays() {
        for (case, got, want) in [
            (
                "owned primitive",
                owned_type("[]int32", false, None),
                "Vec<i32>",
            ),
            (
                "owned nullable string",
                owned_type("[]string", true, None),
                "Option<Vec<String>>",
            ),
            (
                "borrowed string",
                borrowed_type("[]string", false, None),
                "Vec<&'a str>",
            ),
        ] {
            check!(got == want, "case {case}");
        }
    }

    #[test]
    fn struct_refs() {
        for (case, got, want) in [
            (
                "owned",
                owned_type("ProduceTopic", false, Some("ProduceTopic")),
                "ProduceTopic",
            ),
            // Caller is responsible for including <'a> in struct_path when needed.
            (
                "borrowed",
                borrowed_type("ProduceTopic", false, Some("ProduceTopic<'a>")),
                "ProduceTopic<'a>",
            ),
        ] {
            check!(got == want, "case {case}");
        }
        check!(borrowed_type("ProduceTopic", false, Some("ProduceTopic")) == "ProduceTopic");
        check!(owned_type("[]ProduceTopic", false, Some("ProduceTopic")) == "Vec<ProduceTopic>");
        check!(
            borrowed_type("[]ProduceTopic", false, Some("ProduceTopic<'a>"))
                == "Vec<ProduceTopic<'a>>"
        );
        check!(borrowed_type("[]ProduceTopic", false, Some("ProduceTopic")) == "Vec<ProduceTopic>");
    }
}
