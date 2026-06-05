//! Structural diff between two `FileDescriptorProto`, mirroring Confluent's
//! `SchemaDiff`. Each `Difference` is classified by `compat.rs`. No direction logic
//! here — the engine calls `check` with (reader, writer) swapped per level.

use std::collections::{BTreeMap, BTreeSet};

use prost_reflect::prost_types::descriptor_proto::ReservedRange;
use prost_reflect::prost_types::field_descriptor_proto::Type as FieldType;
use prost_reflect::prost_types::{
    DescriptorProto, FieldDescriptorProto, FileDescriptorProto, OneofDescriptorProto,
};

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Kind {
    FieldAdded,
    FieldRemoved,
    FieldScalarKindChanged { compatible_group: bool },
    FieldKindChanged,
    FieldNamedTypeChanged,
    FieldLabelChanged,
    MessageRemoved,
    MessageAdded,
    // Oneof rules (Task 2)
    OneofFieldMovedIn,
    OneofFieldMovedOut,
    OneofAdded,
    OneofRemoved,
    // Reserved rules (Task 3)
    ReservedNumberAdded,
    ReservedNameAdded,
}

#[derive(Debug, Clone)]
pub struct Difference {
    pub kind: Kind,
    pub path: String,
}

#[must_use]
pub fn compare(original: &FileDescriptorProto, update: &FileDescriptorProto) -> Vec<Difference> {
    let mut out = Vec::new();
    compare_messages("", &original.message_type, &update.message_type, &mut out);
    out
}

fn compare_messages(
    prefix: &str,
    orig: &[DescriptorProto],
    upd: &[DescriptorProto],
    out: &mut Vec<Difference>,
) {
    let orig_by: BTreeMap<&str, &DescriptorProto> = orig.iter().map(|m| (m.name(), m)).collect();
    let upd_by: BTreeMap<&str, &DescriptorProto> = upd.iter().map(|m| (m.name(), m)).collect();
    for (name, om) in &orig_by {
        let path = join(prefix, name);
        match upd_by.get(name) {
            None => out.push(Difference {
                kind: Kind::MessageRemoved,
                path,
            }),
            Some(um) => compare_message(&path, om, um, out),
        }
    }
    for name in upd_by.keys() {
        if !orig_by.contains_key(name) {
            out.push(Difference {
                kind: Kind::MessageAdded,
                path: join(prefix, name),
            });
        }
    }
}

fn compare_message(
    path: &str,
    orig: &DescriptorProto,
    upd: &DescriptorProto,
    out: &mut Vec<Difference>,
) {
    let orig_f: BTreeMap<i32, &FieldDescriptorProto> =
        orig.field.iter().map(|f| (f.number(), f)).collect();
    let upd_f: BTreeMap<i32, &FieldDescriptorProto> =
        upd.field.iter().map(|f| (f.number(), f)).collect();
    for (num, of) in &orig_f {
        let fpath = format!("{path}.#{num}");
        match upd_f.get(num) {
            None => out.push(Difference {
                kind: Kind::FieldRemoved,
                path: fpath,
            }),
            Some(uf) => {
                // Map fields: both sides are maps → compare their entry key/value fields
                // directly; skip normal label/type checks (the outer field is always
                // `repeated Message` for both sides, so those checks are noise).
                let orig_entry = find_map_entry(orig, of);
                let upd_entry = find_map_entry(upd, uf);
                match (orig_entry, upd_entry) {
                    (Some(oe), Some(ue)) => compare_map_entries(&fpath, oe, ue, out),
                    _ => compare_field(&fpath, of, uf, out),
                }
            }
        }
    }
    for num in upd_f.keys() {
        if !orig_f.contains_key(num) {
            out.push(Difference {
                kind: Kind::FieldAdded,
                path: format!("{path}.#{num}"),
            });
        }
    }
    // Compare real oneof declarations (synthetic proto3_optional oneofs are named
    // with a leading underscore by protoc; filter them out by name).
    let orig_real: Vec<OneofDescriptorProto> = orig
        .oneof_decl
        .iter()
        .filter(|o| !o.name().starts_with('_'))
        .cloned()
        .collect();
    let upd_real: Vec<OneofDescriptorProto> = upd
        .oneof_decl
        .iter()
        .filter(|o| !o.name().starts_with('_'))
        .cloned()
        .collect();
    compare_oneofs(path, &orig_real, &upd_real, out);
    // Reserved ranges and names (Task 3)
    compare_reserved(path, &orig.reserved_range, &upd.reserved_range, out);
    compare_reserved_names(path, &orig.reserved_name, &upd.reserved_name, out);
}

fn compare_field(
    path: &str,
    of: &FieldDescriptorProto,
    uf: &FieldDescriptorProto,
    out: &mut Vec<Difference>,
) {
    // Oneof membership change (proto3 `optional` is a synthetic oneof — not a real one).
    let orig_in_oneof = real_oneof(of);
    let upd_in_oneof = real_oneof(uf);
    if !orig_in_oneof && upd_in_oneof {
        out.push(Difference {
            kind: Kind::OneofFieldMovedIn,
            path: path.to_string(),
        });
        // Still check type/label changes below — they can co-occur.
    } else if orig_in_oneof && !upd_in_oneof {
        out.push(Difference {
            kind: Kind::OneofFieldMovedOut,
            path: path.to_string(),
        });
    }

    if of.label() != uf.label() {
        out.push(Difference {
            kind: Kind::FieldLabelChanged,
            path: path.to_string(),
        });
    }
    let (ot, ut) = (of.r#type(), uf.r#type());
    if ot != ut {
        if is_scalar(ot) && is_scalar(ut) {
            out.push(Difference {
                kind: Kind::FieldScalarKindChanged {
                    compatible_group: same_group(ot, ut),
                },
                path: path.to_string(),
            });
        } else {
            out.push(Difference {
                kind: Kind::FieldKindChanged,
                path: path.to_string(),
            });
        }
    } else if matches!(ot, FieldType::Message | FieldType::Enum) && of.type_name != uf.type_name {
        out.push(Difference {
            kind: Kind::FieldNamedTypeChanged,
            path: path.to_string(),
        });
    }
}

/// True iff this field is a real (user-declared) oneof member.
/// proto3 `optional` generates a SYNTHETIC oneof (`proto3_optional == Some(true)`),
/// which must NOT be treated as a real oneof membership change.
fn real_oneof(f: &FieldDescriptorProto) -> bool {
    f.oneof_index.is_some() && f.proto3_optional != Some(true)
}

fn compare_oneofs(
    prefix: &str,
    orig: &[OneofDescriptorProto],
    upd: &[OneofDescriptorProto],
    out: &mut Vec<Difference>,
) {
    let orig_names: BTreeSet<&str> = orig.iter().map(OneofDescriptorProto::name).collect();
    let upd_names: BTreeSet<&str> = upd.iter().map(OneofDescriptorProto::name).collect();
    for name in &orig_names {
        if !upd_names.contains(name) {
            out.push(Difference {
                kind: Kind::OneofRemoved,
                path: join(prefix, name),
            });
        }
    }
    for name in &upd_names {
        if !orig_names.contains(name) {
            out.push(Difference {
                kind: Kind::OneofAdded,
                path: join(prefix, name),
            });
        }
    }
}

/// Returns the synthetic map-entry `DescriptorProto` for `field` if it is a
/// map field inside `containing`.  A map field has `type == Message` and its
/// `type_name` leaf matches a nested type whose `options.map_entry == Some(true)`.
fn find_map_entry<'a>(
    containing: &'a DescriptorProto,
    field: &FieldDescriptorProto,
) -> Option<&'a DescriptorProto> {
    if field.r#type() != FieldType::Message {
        return None;
    }
    let type_name = field.type_name.as_deref()?;
    // The type_name is a fully-qualified path like ".pkg.Outer.MEntry"; we only
    // need the leaf (last component) to match against `nested_type` names.
    let leaf = type_name.rsplit('.').next()?;
    containing.nested_type.iter().find(|nt| {
        nt.name.as_deref() == Some(leaf)
            && nt.options.as_ref().and_then(|o| o.map_entry) == Some(true)
    })
}

/// Compare two map entry descriptors (key=#1, value=#2) field-by-field.
fn compare_map_entries(
    path: &str,
    orig_entry: &DescriptorProto,
    upd_entry: &DescriptorProto,
    out: &mut Vec<Difference>,
) {
    let orig_f: BTreeMap<i32, &FieldDescriptorProto> =
        orig_entry.field.iter().map(|f| (f.number(), f)).collect();
    let upd_f: BTreeMap<i32, &FieldDescriptorProto> =
        upd_entry.field.iter().map(|f| (f.number(), f)).collect();
    // Compare key (#1) and value (#2) types using normal scalar/kind checks.
    for num in [1i32, 2i32] {
        if let (Some(of), Some(uf)) = (orig_f.get(&num), upd_f.get(&num)) {
            let sub = if num == 1 { "key" } else { "value" };
            let fpath = format!("{path}.{sub}");
            // Only compare types — map entries have no meaningful label difference.
            let (ot, ut) = (of.r#type(), uf.r#type());
            if ot != ut {
                if is_scalar(ot) && is_scalar(ut) {
                    out.push(Difference {
                        kind: Kind::FieldScalarKindChanged {
                            compatible_group: same_group(ot, ut),
                        },
                        path: fpath,
                    });
                } else {
                    out.push(Difference {
                        kind: Kind::FieldKindChanged,
                        path: fpath,
                    });
                }
            } else if matches!(ot, FieldType::Message | FieldType::Enum)
                && of.type_name != uf.type_name
            {
                out.push(Difference {
                    kind: Kind::FieldNamedTypeChanged,
                    path: fpath,
                });
            }
        }
    }
}

fn compare_reserved(
    path: &str,
    orig: &[ReservedRange],
    upd: &[ReservedRange],
    out: &mut Vec<Difference>,
) {
    // Collect all numbers covered by orig ranges
    let orig_nums: BTreeSet<i32> = orig
        .iter()
        .flat_map(|r| {
            let start = r.start.unwrap_or(0);
            let end = r.end.unwrap_or(0);
            start..end
        })
        .collect();
    let upd_nums: BTreeSet<i32> = upd
        .iter()
        .flat_map(|r| {
            let start = r.start.unwrap_or(0);
            let end = r.end.unwrap_or(0);
            start..end
        })
        .collect();
    // Numbers newly reserved in upd but not in orig
    for num in upd_nums.difference(&orig_nums) {
        out.push(Difference {
            kind: Kind::ReservedNumberAdded,
            path: format!("{path}.reserved#{num}"),
        });
    }
}

fn compare_reserved_names(path: &str, orig: &[String], upd: &[String], out: &mut Vec<Difference>) {
    let orig_set: BTreeSet<&str> = orig.iter().map(String::as_str).collect();
    let upd_set: BTreeSet<&str> = upd.iter().map(String::as_str).collect();
    for name in upd_set.difference(&orig_set) {
        out.push(Difference {
            kind: Kind::ReservedNameAdded,
            path: format!("{path}.reserved_name#{name}"),
        });
    }
}

fn is_scalar(t: FieldType) -> bool {
    !matches!(t, FieldType::Message | FieldType::Enum | FieldType::Group)
}

fn same_group(a: FieldType, b: FieldType) -> bool {
    fn g(t: FieldType) -> u8 {
        use FieldType::{
            Bool, Bytes, Fixed32, Fixed64, Int32, Int64, Sfixed32, Sfixed64, Sint32, Sint64,
            String as Str, Uint32, Uint64,
        };
        match t {
            Int32 | Int64 | Uint32 | Uint64 | Bool => 1,
            Sint32 | Sint64 => 2,
            Str | Bytes => 3,
            Fixed32 | Sfixed32 => 4,
            Fixed64 | Sfixed64 => 5,
            FieldType::Float => 6,
            FieldType::Double => 7,
            _ => 0,
        }
    }
    let (ga, gb) = (g(a), g(b));
    ga != 0 && ga == gb
}

fn join(prefix: &str, name: &str) -> String {
    if prefix.is_empty() {
        name.to_string()
    } else {
        format!("{prefix}.{name}")
    }
}
