//! Structural diff between two `FileDescriptorProto`, which mirrors Confluent's
//! `SchemaDiff`. `compat.rs` classifies each `Difference`. This module has no
//! direction logic. The engine calls `check` with (reader, writer) swapped per
//! level.

use std::collections::{BTreeMap, BTreeSet};

use prost_reflect::prost_types::{
    DescriptorProto, EnumDescriptorProto, FieldDescriptorProto, FileDescriptorProto,
    OneofDescriptorProto, descriptor_proto::ReservedRange,
    field_descriptor_proto::Type as FieldType,
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
    // Oneof rules
    OneofFieldMovedIn,
    OneofFieldMovedOut,
    OneofAdded,
    OneofRemoved,
    // Reserved rules
    ReservedNumberAdded,
    ReservedNameAdded,
    // Enum rules
    EnumConstAdded,
    EnumConstRemoved,
    EnumAdded,
    EnumRemoved,
    // Package rule
    PackageChanged,
}

#[derive(Debug, Clone)]
pub struct Difference {
    pub kind: Kind,
    pub path: String,
}

/// The wire-level kind a field encodes.
///
/// `protox_parse` does NOT resolve named type references. For a
/// `message`-typed or `enum`-typed field it leaves `type` unset, so `r#type()`
/// reports the zero value `Double`, and it only sets `type_name` to the short
/// leaf name. We therefore cannot trust `r#type()` for named types. We resolve
/// `type_name` against the set of enum names declared in the file.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum FieldKind {
    /// A primitive scalar with its concrete wire type.
    Scalar(FieldType),
    /// A named enum. It is a varint on the wire, in the same group as
    /// int32/int64/bool.
    Enum,
    /// A named message or group. It is length-delimited and never
    /// group-compatible with a scalar.
    Message,
}

/// Resolution context. It is the set of enum leaf-names declared anywhere in
/// each file, and it tells an `enum`-typed field from a `message`-typed one.
/// Both arrive from `protox_parse` with an unresolved `type` and only a
/// `type_name`.
struct Resolver<'a> {
    orig_enums: BTreeSet<&'a str>,
    upd_enums: BTreeSet<&'a str>,
}

impl Resolver<'_> {
    fn kind(enum_names: &BTreeSet<&str>, f: &FieldDescriptorProto) -> FieldKind {
        match f.type_name.as_deref() {
            // A named-type reference (message or enum). protox gives the short
            // leaf name; classify by membership in the file's enum-name set.
            // Known edge: an enum and a message sharing a leaf name in
            // different scopes could be misclassified here; not exercised by the
            // cp matrix. A fully-qualified-name resolver is the proper fix when
            // full Confluent canonical form lands.
            Some(tn) => {
                let leaf = tn.rsplit('.').next().unwrap_or(tn);
                if enum_names.contains(leaf) {
                    FieldKind::Enum
                } else {
                    FieldKind::Message
                }
            }
            // No type_name → a primitive scalar; trust the concrete wire type.
            None => FieldKind::Scalar(f.r#type()),
        }
    }

    fn orig_kind(&self, f: &FieldDescriptorProto) -> FieldKind {
        Self::kind(&self.orig_enums, f)
    }

    fn upd_kind(&self, f: &FieldDescriptorProto) -> FieldKind {
        Self::kind(&self.upd_enums, f)
    }
}

/// Collect the leaf-names of every enum declared in the file, both top-level
/// and nested at any depth, so that the code can classify a named-type field as
/// an enum or a message.
fn collect_enum_names(file: &FileDescriptorProto) -> BTreeSet<&str> {
    let mut set = BTreeSet::new();
    for e in &file.enum_type {
        set.insert(e.name());
    }
    for m in &file.message_type {
        collect_enum_names_in_message(m, &mut set);
    }
    set
}

/// Recurse a message's own + nested enum declarations into `set`.
fn collect_enum_names_in_message<'a>(m: &'a DescriptorProto, set: &mut BTreeSet<&'a str>) {
    for e in &m.enum_type {
        set.insert(e.name());
    }
    for n in &m.nested_type {
        collect_enum_names_in_message(n, set);
    }
}

#[must_use]
pub fn compare(original: &FileDescriptorProto, update: &FileDescriptorProto) -> Vec<Difference> {
    let mut out = Vec::new();
    let r = Resolver {
        orig_enums: collect_enum_names(original),
        upd_enums: collect_enum_names(update),
    };
    // Package change
    if original.package != update.package {
        out.push(Difference {
            kind: Kind::PackageChanged,
            path: "package".to_string(),
        });
    }
    compare_messages(
        "",
        &original.message_type,
        &update.message_type,
        &r,
        &mut out,
    );
    compare_enums("", &original.enum_type, &update.enum_type, &mut out);
    out
}

fn compare_messages(
    prefix: &str,
    orig: &[DescriptorProto],
    upd: &[DescriptorProto],
    r: &Resolver,
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
            Some(um) => compare_message(&path, om, um, r, out),
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
    r: &Resolver,
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
                    (Some(oe), Some(ue)) => compare_map_entries(&fpath, oe, ue, r, out),
                    _ => compare_field(&fpath, of, uf, upd, r, out),
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
    // Reserved ranges and names
    compare_reserved(path, &orig.reserved_range, &upd.reserved_range, out);
    compare_reserved_names(path, &orig.reserved_name, &upd.reserved_name, out);
    // Recurse into non-map nested messages
    // Keep ordinary nested messages; synthetic `map<>` entry messages are
    // compared via the map path (`compare_map_entries`), not as nested types.
    let keep_nested =
        |nt: &&DescriptorProto| nt.options.as_ref().and_then(|o| o.map_entry) != Some(true);
    let orig_nested: Vec<&DescriptorProto> = orig.nested_type.iter().filter(keep_nested).collect();
    let upd_nested: Vec<&DescriptorProto> = upd.nested_type.iter().filter(keep_nested).collect();
    let orig_nested_owned: Vec<DescriptorProto> = orig_nested.into_iter().cloned().collect();
    let upd_nested_owned: Vec<DescriptorProto> = upd_nested.into_iter().cloned().collect();
    compare_messages(path, &orig_nested_owned, &upd_nested_owned, r, out);
    // Compare nested enums
    compare_enums(path, &orig.enum_type, &upd.enum_type, out);
}

fn compare_field(
    path: &str,
    of: &FieldDescriptorProto,
    uf: &FieldDescriptorProto,
    upd_msg: &DescriptorProto,
    r: &Resolver,
    out: &mut Vec<Difference>,
) {
    // Oneof membership change (proto3 `optional` is a synthetic oneof — not a real one).
    let orig_in_oneof = real_oneof(of);
    let upd_in_oneof = real_oneof(uf);
    if !orig_in_oneof && upd_in_oneof {
        // A field that moves INTO a oneof is only a meaningful (incompatible)
        // change when that oneof groups it with OTHER fields — a single-field
        // oneof is wire-identical to a plain field. cp treats "move one field
        // into its own oneof" (oneof_added) as compatible, but moving ≥2 formerly
        // independent fields into one oneof (oneof_move_in) as incompatible.
        if oneof_member_count(upd_msg, uf) >= 2 {
            out.push(Difference {
                kind: Kind::OneofFieldMovedIn,
                path: path.to_string(),
            });
        }
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
    compare_field_types(
        path,
        r.orig_kind(of),
        r.upd_kind(uf),
        of.type_name.as_ref(),
        uf.type_name.as_ref(),
        out,
    );
}

/// Classify a type change between two resolved field kinds.
///
/// Group-comparable kinds are scalars and enums, which all encode as a single
/// wire value. Two such kinds that differ give a `FieldScalarKindChanged`,
/// which is compatible only when the wire group is the same. Any change that
/// touches a message gives a `FieldKindChanged`. An unchanged named type with a
/// different referent gives a `FieldNamedTypeChanged`.
fn compare_field_types(
    path: &str,
    ok: FieldKind,
    uk: FieldKind,
    o_type_name: Option<&String>,
    u_type_name: Option<&String>,
    out: &mut Vec<Difference>,
) {
    if ok == uk {
        // Same kind. For named types, a differing referent is a named-type change.
        if matches!(ok, FieldKind::Message | FieldKind::Enum) && o_type_name != u_type_name {
            out.push(Difference {
                kind: Kind::FieldNamedTypeChanged,
                path: path.to_string(),
            });
        }
        return;
    }
    match (kind_group(ok), kind_group(uk)) {
        // Both encode as a single wire value → scalar-kind change (enum counts).
        (Some(go), Some(gu)) => out.push(Difference {
            kind: Kind::FieldScalarKindChanged {
                compatible_group: go == gu,
            },
            path: path.to_string(),
        }),
        // At least one side is a message → a structural kind change.
        _ => out.push(Difference {
            kind: Kind::FieldKindChanged,
            path: path.to_string(),
        }),
    }
}

/// How many fields belong to the same real oneof as `field` within `msg`.
fn oneof_member_count(msg: &DescriptorProto, field: &FieldDescriptorProto) -> usize {
    let Some(idx) = field.oneof_index else {
        return 0;
    };
    msg.field
        .iter()
        .filter(|f| f.oneof_index == Some(idx) && f.proto3_optional != Some(true))
        .count()
}

/// True only when this field is a real, user-declared oneof member.
///
/// proto3 `optional` generates a SYNTHETIC oneof, where
/// `proto3_optional == Some(true)`. The code must NOT treat that as a real
/// oneof membership change.
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
/// map field inside `containing`. A map field has `type == Message`, and its
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
    r: &Resolver,
    out: &mut Vec<Difference>,
) {
    let orig_f: BTreeMap<i32, &FieldDescriptorProto> =
        orig_entry.field.iter().map(|f| (f.number(), f)).collect();
    let upd_f: BTreeMap<i32, &FieldDescriptorProto> =
        upd_entry.field.iter().map(|f| (f.number(), f)).collect();
    // Compare key (#1) and value (#2) types using the same kind logic as fields.
    for num in [1i32, 2i32] {
        if let (Some(of), Some(uf)) = (orig_f.get(&num), upd_f.get(&num)) {
            let sub = if num == 1 { "key" } else { "value" };
            let fpath = format!("{path}.{sub}");
            compare_field_types(
                &fpath,
                r.orig_kind(of),
                r.upd_kind(uf),
                of.type_name.as_ref(),
                uf.type_name.as_ref(),
                out,
            );
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

fn compare_enums(
    prefix: &str,
    orig: &[EnumDescriptorProto],
    upd: &[EnumDescriptorProto],
    out: &mut Vec<Difference>,
) {
    let orig_by: BTreeMap<&str, &EnumDescriptorProto> =
        orig.iter().map(|e| (e.name(), e)).collect();
    let upd_by: BTreeMap<&str, &EnumDescriptorProto> = upd.iter().map(|e| (e.name(), e)).collect();
    for (name, oe) in &orig_by {
        let epath = join(prefix, name);
        match upd_by.get(name) {
            None => out.push(Difference {
                kind: Kind::EnumRemoved,
                path: epath,
            }),
            Some(ue) => compare_enum_values(&epath, oe, ue, out),
        }
    }
    for name in upd_by.keys() {
        if !orig_by.contains_key(name) {
            out.push(Difference {
                kind: Kind::EnumAdded,
                path: join(prefix, name),
            });
        }
    }
}

fn compare_enum_values(
    path: &str,
    orig: &EnumDescriptorProto,
    upd: &EnumDescriptorProto,
    out: &mut Vec<Difference>,
) {
    // Match values by NUMBER (not name), mirroring how fields are matched.
    let orig_vals: BTreeMap<i32, &str> =
        orig.value.iter().map(|v| (v.number(), v.name())).collect();
    let upd_vals: BTreeMap<i32, &str> = upd.value.iter().map(|v| (v.number(), v.name())).collect();
    for num in orig_vals.keys() {
        if !upd_vals.contains_key(num) {
            out.push(Difference {
                kind: Kind::EnumConstRemoved,
                path: format!("{path}.#{num}"),
            });
        }
    }
    for num in upd_vals.keys() {
        if !orig_vals.contains_key(num) {
            out.push(Difference {
                kind: Kind::EnumConstAdded,
                path: format!("{path}.#{num}"),
            });
        }
    }
}

/// The wire-compatibility group of a single-value field kind, or `None` for a
/// message.
///
/// A message is length-delimited and never group-compatible with a single
/// value. Two fields whose kinds share a non-`None` group are interchangeable
/// on the wire. Differing groups are an across-group scalar change, which is
/// incompatible.
///
/// Enums map to the varint group (1), so `int32 ↔ enum` is in-group and
/// compatible. This matches cp-schema-registry's golden verdicts.
fn kind_group(k: FieldKind) -> Option<u8> {
    use FieldType::{
        Bool, Bytes, Fixed32, Fixed64, Int32, Int64, Sfixed32, Sfixed64, Sint32, Sint64,
        String as Str, Uint32, Uint64,
    };
    match k {
        // Enum is a varint on the wire → same group as int32/int64/uint/bool.
        FieldKind::Enum => Some(1),
        FieldKind::Message => None,
        FieldKind::Scalar(t) => match t {
            Int32 | Int64 | Uint32 | Uint64 | Bool => Some(1),
            Sint32 | Sint64 => Some(2),
            Str | Bytes => Some(3),
            Fixed32 | Sfixed32 => Some(4),
            Fixed64 | Sfixed64 => Some(5),
            FieldType::Float => Some(6),
            FieldType::Double => Some(7),
            // Group/Message/Enum are handled by FieldKind above; this arm is
            // unreachable for a Scalar but keeps the match total.
            FieldType::Group | FieldType::Message | FieldType::Enum => None,
        },
    }
}

fn join(prefix: &str, name: &str) -> String {
    if prefix.is_empty() {
        name.to_string()
    } else {
        format!("{prefix}.{name}")
    }
}
