//! Durable SQL-managed text-search configuration/dictionary metadata.

use std::collections::BTreeSet;

use crabka_pgkv::{Kv, WriteOp};
use crabka_pgparser::ast::{TextSearchDdl, TextSearchObjectKind};

use crate::error::ExecError;

const PREFIX: &[u8] = b"\0\0\0\0catalog/text-search/";

#[derive(Debug, Clone, PartialEq, Eq)]
struct Object {
    base: String,
}

fn canonical(name: &str) -> String {
    name.to_ascii_lowercase()
}

const fn kind_tag(kind: TextSearchObjectKind) -> u8 {
    match kind {
        TextSearchObjectKind::Configuration => b'c',
        TextSearchObjectKind::Dictionary => b'd',
    }
}

fn key(kind: TextSearchObjectKind, name: &str) -> Vec<u8> {
    let mut key = PREFIX.to_vec();
    key.push(kind_tag(kind));
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
    let name = name.strip_prefix("pg_catalog.").unwrap_or(&name);
    match (kind, name) {
        (TextSearchObjectKind::Configuration, "simple") => Some(Object {
            base: "simple".into(),
        }),
        (TextSearchObjectKind::Configuration, "english") => Some(Object {
            base: "english".into(),
        }),
        (TextSearchObjectKind::Dictionary, "simple" | "english_stem") => {
            Some(Object { base: name.into() })
        }
        _ => None,
    }
}

fn find(kv: &dyn Kv, kind: TextSearchObjectKind, name: &str) -> Result<Option<Object>, ExecError> {
    if let Some(object) = builtin(kind, name) {
        return Ok(Some(object));
    }
    kv.get(&key(kind, name))?
        .map(|value| {
            String::from_utf8(value)
                .map(|base| Object { base })
                .map_err(|_| ExecError::Unsupported("corrupt text-search catalog value".into()))
        })
        .transpose()
}

pub(crate) fn config_is_simple(kv: Option<&dyn Kv>, name: &str) -> Result<bool, ExecError> {
    let mut current = canonical(name);
    let mut seen = BTreeSet::new();
    loop {
        if !seen.insert(current.clone()) {
            return Err(ExecError::Unsupported(
                "text search configuration COPY cycle".into(),
            ));
        }
        if let Some(object) = builtin(TextSearchObjectKind::Configuration, &current) {
            let base = canonical(&object.base);
            return Ok(base.strip_prefix("pg_catalog.").unwrap_or(&base) == "simple");
        }
        let Some(kv) = kv else {
            return Err(ExecError::UndefinedObject(format!(
                "text search configuration \"{current}\" does not exist"
            )));
        };
        let Some(object) = find(kv, TextSearchObjectKind::Configuration, &current)? else {
            return Err(ExecError::UndefinedObject(format!(
                "text search configuration \"{current}\" does not exist"
            )));
        };
        let base = canonical(&object.base);
        current = base;
    }
}

pub(crate) fn execute(
    kv: &dyn Kv,
    ddl: &TextSearchDdl,
) -> Result<(&'static str, Vec<WriteOp>), ExecError> {
    match ddl {
        TextSearchDdl::Create { kind, name, base } => {
            let name = canonical(name);
            if find(kv, *kind, &name)?.is_some() {
                return Err(ExecError::DuplicateObject(format!(
                    "{} \"{name}\" already exists",
                    kind_name(*kind)
                )));
            }
            match kind {
                TextSearchObjectKind::Configuration => {
                    config_is_simple(Some(kv), base)?;
                }
                TextSearchObjectKind::Dictionary => {
                    if find(kv, TextSearchObjectKind::Dictionary, base)?.is_none()
                        && !matches!(canonical(base).as_str(), "simple" | "snowball")
                    {
                        return Err(ExecError::UndefinedObject(format!(
                            "text search template \"{base}\" does not exist"
                        )));
                    }
                }
            }
            Ok((
                match kind {
                    TextSearchObjectKind::Configuration => "CREATE TEXT SEARCH CONFIGURATION",
                    TextSearchObjectKind::Dictionary => "CREATE TEXT SEARCH DICTIONARY",
                },
                vec![WriteOp::Put {
                    key: key(*kind, &name),
                    value: canonical(base).into_bytes(),
                }],
            ))
        }
        TextSearchDdl::Alter {
            kind,
            name,
            rename_to,
        } => {
            let old = canonical(name);
            let Some(object) = find(kv, *kind, &old)? else {
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
                if find(kv, *kind, &new_name)?.is_some() {
                    return Err(ExecError::DuplicateObject(format!(
                        "{} \"{new_name}\" already exists",
                        kind_name(*kind)
                    )));
                }
                return Ok((
                    match kind {
                        TextSearchObjectKind::Configuration => "ALTER TEXT SEARCH CONFIGURATION",
                        TextSearchObjectKind::Dictionary => "ALTER TEXT SEARCH DICTIONARY",
                    },
                    vec![
                        WriteOp::Put {
                            key: key(*kind, &new_name),
                            value: object.base.into_bytes(),
                        },
                        WriteOp::Delete {
                            key: key(*kind, &old),
                        },
                    ],
                ));
            }
            Ok((
                match kind {
                    TextSearchObjectKind::Configuration => "ALTER TEXT SEARCH CONFIGURATION",
                    TextSearchObjectKind::Dictionary => "ALTER TEXT SEARCH DICTIONARY",
                },
                Vec::new(),
            ))
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
            if find(kv, *kind, &name)?.is_none() {
                if *if_exists {
                    return Ok((
                        match kind {
                            TextSearchObjectKind::Configuration => "DROP TEXT SEARCH CONFIGURATION",
                            TextSearchObjectKind::Dictionary => "DROP TEXT SEARCH DICTIONARY",
                        },
                        Vec::new(),
                    ));
                }
                return Err(ExecError::UndefinedObject(format!(
                    "{} \"{name}\" does not exist",
                    kind_name(*kind)
                )));
            }
            Ok((
                match kind {
                    TextSearchObjectKind::Configuration => "DROP TEXT SEARCH CONFIGURATION",
                    TextSearchObjectKind::Dictionary => "DROP TEXT SEARCH DICTIONARY",
                },
                vec![WriteOp::Delete {
                    key: key(*kind, &name),
                }],
            ))
        }
    }
}

pub(crate) fn catalog_rows(
    kv: &dyn Kv,
    kind: TextSearchObjectKind,
) -> Result<Vec<(String, String)>, ExecError> {
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
    let prefix = key(kind, "");
    for (raw_key, value) in kv.scan_prefix(&prefix)? {
        let name = String::from_utf8(raw_key[prefix.len()..].to_vec())
            .map_err(|_| ExecError::Unsupported("corrupt text-search catalog key".into()))?;
        let base = String::from_utf8(value)
            .map_err(|_| ExecError::Unsupported("corrupt text-search catalog value".into()))?;
        rows.push((name, base));
    }
    rows.sort();
    rows.dedup_by(|left, right| left.0 == right.0);
    Ok(rows)
}
