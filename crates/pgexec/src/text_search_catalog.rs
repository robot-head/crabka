//! Durable SQL-managed text-search configuration/dictionary metadata.

use std::{collections::BTreeMap, sync::LazyLock, sync::RwLock};

use crabka_pgkv::Kv;
use crabka_pgparser::ast::{TextSearchDdl, TextSearchObjectKind};

use crate::error::ExecError;

const PREFIX: &[u8] = b"\0\0\0\0catalog/text-search/";

#[derive(Debug, Clone, PartialEq, Eq)]
struct Object {
    kind: TextSearchObjectKind,
    base: String,
}

static OBJECTS: LazyLock<RwLock<BTreeMap<String, Object>>> =
    LazyLock::new(|| RwLock::new(BTreeMap::new()));

fn canonical(name: &str) -> String {
    name.rsplit('.').next().unwrap_or(name).to_ascii_lowercase()
}

fn key(kind: TextSearchObjectKind, name: &str) -> Vec<u8> {
    let mut key = PREFIX.to_vec();
    key.push(match kind {
        TextSearchObjectKind::Configuration => b'c',
        TextSearchObjectKind::Dictionary => b'd',
    });
    key.push(b'/');
    key.extend_from_slice(canonical(name).as_bytes());
    key
}

fn kind_name(kind: TextSearchObjectKind) -> &'static str {
    match kind {
        TextSearchObjectKind::Configuration => "text search configuration",
        TextSearchObjectKind::Dictionary => "text search dictionary",
    }
}

fn builtin(kind: TextSearchObjectKind, name: &str) -> Option<Object> {
    let name = canonical(name);
    match (kind, name.as_str()) {
        (TextSearchObjectKind::Configuration, "simple") => Some(Object {
            kind,
            base: "simple".into(),
        }),
        (TextSearchObjectKind::Configuration, "english") => Some(Object {
            kind,
            base: "english".into(),
        }),
        (TextSearchObjectKind::Dictionary, "simple" | "english_stem") => {
            Some(Object { kind, base: name })
        }
        _ => None,
    }
}

fn find(kind: TextSearchObjectKind, name: &str) -> Option<Object> {
    builtin(kind, name).or_else(|| {
        OBJECTS
            .read()
            .expect("text-search registry")
            .get(&canonical(name))
            .filter(|object| object.kind == kind)
            .cloned()
    })
}

pub(crate) fn config_is_simple(name: &str) -> Result<bool, ExecError> {
    let Some(object) = find(TextSearchObjectKind::Configuration, name) else {
        return Err(ExecError::UndefinedObject(format!(
            "text search configuration \"{name}\" does not exist"
        )));
    };
    Ok(canonical(&object.base) == "simple")
}

pub(crate) fn hydrate(kv: &dyn Kv) -> Result<(), ExecError> {
    let mut objects = OBJECTS.write().expect("text-search registry");
    for (raw_key, value) in kv.scan_prefix(PREFIX)? {
        let Some(suffix) = raw_key.strip_prefix(PREFIX) else {
            continue;
        };
        let (kind, name) = match suffix.split_first() {
            Some((b'c', rest)) if rest.starts_with(b"/") => {
                (TextSearchObjectKind::Configuration, &rest[1..])
            }
            Some((b'd', rest)) if rest.starts_with(b"/") => {
                (TextSearchObjectKind::Dictionary, &rest[1..])
            }
            _ => continue,
        };
        let name = String::from_utf8(name.to_vec())
            .map_err(|_| ExecError::Unsupported("corrupt text-search catalog key".into()))?;
        let base = String::from_utf8(value)
            .map_err(|_| ExecError::Unsupported("corrupt text-search catalog value".into()))?;
        objects.insert(name, Object { kind, base });
    }
    Ok(())
}

pub(crate) fn execute(kv: &dyn Kv, ddl: &TextSearchDdl) -> Result<&'static str, ExecError> {
    match ddl {
        TextSearchDdl::Create { kind, name, base } => {
            let name = canonical(name);
            if find(*kind, &name).is_some() {
                return Err(ExecError::DuplicateObject(format!(
                    "{} \"{name}\" already exists",
                    kind_name(*kind)
                )));
            }
            match kind {
                TextSearchObjectKind::Configuration => {
                    config_is_simple(base)?;
                }
                TextSearchObjectKind::Dictionary => {
                    if find(TextSearchObjectKind::Dictionary, base).is_none()
                        && !matches!(canonical(base).as_str(), "simple" | "snowball")
                    {
                        return Err(ExecError::UndefinedObject(format!(
                            "text search template \"{base}\" does not exist"
                        )));
                    }
                }
            }
            kv.put(key(*kind, &name), base.as_bytes().to_vec())?;
            OBJECTS.write().expect("text-search registry").insert(
                name,
                Object {
                    kind: *kind,
                    base: canonical(base),
                },
            );
            Ok(match kind {
                TextSearchObjectKind::Configuration => "CREATE TEXT SEARCH CONFIGURATION",
                TextSearchObjectKind::Dictionary => "CREATE TEXT SEARCH DICTIONARY",
            })
        }
        TextSearchDdl::Alter {
            kind,
            name,
            rename_to,
        } => {
            let old = canonical(name);
            let Some(object) = find(*kind, &old) else {
                return Err(ExecError::UndefinedObject(format!(
                    "{} \"{old}\" does not exist",
                    kind_name(*kind)
                )));
            };
            if builtin(*kind, &old).is_some() && rename_to.is_some() {
                return Err(ExecError::Unsupported(format!(
                    "cannot alter built-in {} \"{old}\"",
                    kind_name(*kind)
                )));
            }
            if let Some(new_name) = rename_to {
                let new_name = canonical(new_name);
                if find(*kind, &new_name).is_some() {
                    return Err(ExecError::DuplicateObject(format!(
                        "{} \"{new_name}\" already exists",
                        kind_name(*kind)
                    )));
                }
                kv.put(key(*kind, &new_name), object.base.as_bytes().to_vec())?;
                kv.delete(&key(*kind, &old))?;
                let mut objects = OBJECTS.write().expect("text-search registry");
                objects.remove(&old);
                objects.insert(new_name, object);
            }
            Ok(match kind {
                TextSearchObjectKind::Configuration => "ALTER TEXT SEARCH CONFIGURATION",
                TextSearchObjectKind::Dictionary => "ALTER TEXT SEARCH DICTIONARY",
            })
        }
        TextSearchDdl::Drop {
            kind,
            name,
            if_exists,
        } => {
            let name = canonical(name);
            if builtin(*kind, &name).is_some() {
                return Err(ExecError::Unsupported(format!(
                    "cannot drop built-in {} \"{name}\"",
                    kind_name(*kind)
                )));
            }
            if find(*kind, &name).is_none() {
                if *if_exists {
                    return Ok(match kind {
                        TextSearchObjectKind::Configuration => "DROP TEXT SEARCH CONFIGURATION",
                        TextSearchObjectKind::Dictionary => "DROP TEXT SEARCH DICTIONARY",
                    });
                }
                return Err(ExecError::UndefinedObject(format!(
                    "{} \"{name}\" does not exist",
                    kind_name(*kind)
                )));
            }
            kv.delete(&key(*kind, &name))?;
            OBJECTS.write().expect("text-search registry").remove(&name);
            Ok(match kind {
                TextSearchObjectKind::Configuration => "DROP TEXT SEARCH CONFIGURATION",
                TextSearchObjectKind::Dictionary => "DROP TEXT SEARCH DICTIONARY",
            })
        }
    }
}

pub(crate) fn catalog_rows(kind: TextSearchObjectKind) -> Vec<(String, String)> {
    let builtins: &[(&str, &str)] = match kind {
        TextSearchObjectKind::Configuration => &[("simple", "simple"), ("english", "english")],
        TextSearchObjectKind::Dictionary => {
            &[("simple", "simple"), ("english_stem", "english_stem")]
        }
    };
    let mut rows = builtins
        .iter()
        .map(|(name, base)| ((*name).into(), (*base).into()))
        .collect::<Vec<_>>();
    rows.extend(
        OBJECTS
            .read()
            .expect("text-search registry")
            .iter()
            .filter(|(_, object)| object.kind == kind)
            .map(|(name, object)| (name.clone(), object.base.clone())),
    );
    rows.sort();
    rows.dedup_by(|left, right| left.0 == right.0);
    rows
}
