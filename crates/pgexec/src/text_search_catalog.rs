//! Durable SQL-managed text-search configuration/dictionary metadata.

use std::collections::BTreeSet;

use crabka_pgkv::{Kv, WriteOp};
use crabka_pgparser::ast::{OptionList, TextSearchDdl, TextSearchObjectKind};

use crate::error::ExecError;

const PREFIX: &[u8] = b"\0\0\0\0catalog/text-search/";

#[derive(Debug, Clone, PartialEq, Eq)]
struct Object {
    base: String,
    options: OptionList,
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
            options: Vec::new(),
        }),
        (TextSearchObjectKind::Configuration, "english") => Some(Object {
            base: "english".into(),
            options: Vec::new(),
        }),
        (TextSearchObjectKind::Dictionary, "simple" | "english_stem") => Some(Object {
            base: name.into(),
            options: Vec::new(),
        }),
        _ => None,
    }
}

fn encode(object: &Object) -> Vec<u8> {
    let mut value = object.base.clone().into_bytes();
    for (name, option_value) in &object.options {
        value.push(b'\0');
        value.extend_from_slice(name.as_bytes());
        value.push(b'\0');
        value.extend_from_slice(option_value.as_bytes());
    }
    value
}

fn decode(value: Vec<u8>) -> Result<Object, ExecError> {
    let value = String::from_utf8(value)
        .map_err(|_| ExecError::Unsupported("corrupt text-search catalog value".into()))?;
    let mut fields = value.split('\0');
    let base = fields.next().unwrap_or_default().to_owned();
    let mut options = Vec::new();
    while let Some(name) = fields.next() {
        let Some(value) = fields.next() else {
            return Err(ExecError::Unsupported(
                "corrupt text-search catalog value".into(),
            ));
        };
        options.push((name.to_owned(), value.to_owned()));
    }
    Ok(Object { base, options })
}

fn options_text(options: &OptionList) -> String {
    options
        .iter()
        .map(|(name, value)| {
            if value.parse::<i64>().is_ok() {
                format!("{name} = {value}")
            } else {
                format!("{name} = '{value}'")
            }
        })
        .collect::<Vec<_>>()
        .join(", ")
}

fn validate_dictionary_options(base: &str, options: &OptionList) -> Result<(), ExecError> {
    if canonical(base) == "ispell" {
        if let Some((name, _)) = options
            .iter()
            .find(|(name, _)| !matches!(name.as_str(), "template" | "dictfile" | "afffile"))
        {
            return Err(ExecError::InvalidParameterValueMessage(format!(
                "unrecognized Ispell parameter: \"{name}\""
            )));
        }
        let option = |name| {
            options
                .iter()
                .find(|(key, _)| key == name)
                .map(|(_, value)| value.as_str())
        };
        crate::text_search_ispell::validate_files(
            option("dictfile").unwrap_or("ispell_sample"),
            option("afffile").unwrap_or("ispell_sample"),
        )?;
    }
    if canonical(base) == "synonym"
        && let Some((_, value)) = options.iter().find(|(name, _)| name == "casesensitive")
        && !matches!(value.as_str(), "0" | "1" | "off" | "on" | "false" | "true")
    {
        return Err(ExecError::InvalidParameterValueMessage(
            "casesensitive requires a Boolean value".into(),
        ));
    }
    Ok(())
}

fn validate_mapping_options(options: &OptionList) -> Result<(), ExecError> {
    const TOKEN_TYPES: &[&str] = &[
        "asciiword",
        "word",
        "numword",
        "email",
        "url",
        "host",
        "sfloat",
        "version",
        "hword_numpart",
        "hword_part",
        "hword_asciipart",
        "blank",
        "tag",
        "protocol",
        "numhword",
        "asciihword",
        "hword",
        "url_path",
        "file",
        "float",
        "int",
        "uint",
        "entity",
    ];
    for (name, value) in options {
        let token_types: Vec<_> = if name == "__mapping_replace" {
            Vec::new()
        } else if name == "__mapping_drop" {
            value
                .split_once('\u{1e}')
                .map(|(_, token_types)| token_types.split('\u{1f}').collect())
                .unwrap_or_default()
        } else {
            name.strip_prefix("__mapping_add_")
                .or_else(|| name.strip_prefix("__mapping_"))
                .into_iter()
                .collect()
        };
        if let Some(token_type) = token_types
            .into_iter()
            .find(|token_type| !TOKEN_TYPES.contains(token_type))
        {
            return Err(ExecError::UndefinedObject(format!(
                "token type \"{token_type}\" does not exist"
            )));
        }
    }
    Ok(())
}

fn find(kv: &dyn Kv, kind: TextSearchObjectKind, name: &str) -> Result<Option<Object>, ExecError> {
    if let Some(object) = builtin(kind, name) {
        return Ok(Some(object));
    }
    kv.get(&key(kind, name))?.map(decode).transpose()
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

/// Dictionaries assigned to ordinary word tokens by a configuration mapping.
pub(crate) fn config_dictionaries(
    kv: Option<&dyn Kv>,
    name: &str,
) -> Result<Vec<String>, ExecError> {
    config_token_dictionaries(kv, name, "word")
}

/// Dictionaries assigned to one parser token type by a configuration mapping.
pub(crate) fn config_token_dictionaries(
    kv: Option<&dyn Kv>,
    name: &str,
    token_type: &str,
) -> Result<Vec<String>, ExecError> {
    let mut current = canonical(name);
    let mut seen = BTreeSet::new();
    loop {
        if !seen.insert(current.clone()) {
            return Err(ExecError::Unsupported(
                "text search configuration COPY cycle".into(),
            ));
        }
        if let Some(object) = builtin(TextSearchObjectKind::Configuration, &current) {
            return Ok(builtin_token_dictionaries(&object.base, token_type));
        }
        let kv = kv.ok_or_else(|| {
            ExecError::UndefinedObject(format!(
                "text search configuration \"{current}\" does not exist"
            ))
        })?;
        let object = find(kv, TextSearchObjectKind::Configuration, &current)?.ok_or_else(|| {
            ExecError::UndefinedObject(format!(
                "text search configuration \"{current}\" does not exist"
            ))
        })?;
        let key = format!("__mapping_{token_type}");
        if let Some((_, dictionaries)) = object.options.iter().find(|(name, _)| name == &key) {
            return Ok(dictionaries.split('\u{1f}').map(str::to_owned).collect());
        }
        current = canonical(&object.base);
    }
}

fn builtin_token_dictionaries(config: &str, token_type: &str) -> Vec<String> {
    const SIMPLE: &[&str] = &[
        "email",
        "url",
        "url_path",
        "host",
        "file",
        "version",
        "sfloat",
        "float",
        "int",
        "uint",
        "numword",
        "hword_numpart",
        "numhword",
    ];
    const STEM: &[&str] = &[
        "asciiword",
        "hword_asciipart",
        "asciihword",
        "word",
        "hword_part",
        "hword",
    ];
    match canonical(config).as_str() {
        "simple" if SIMPLE.contains(&token_type) || STEM.contains(&token_type) => {
            vec!["simple".into()]
        }
        "english" if SIMPLE.contains(&token_type) => vec!["simple".into()],
        "english" if STEM.contains(&token_type) => vec!["english_stem".into()],
        _ => Vec::new(),
    }
}

pub(crate) fn skipped_mapping_notice(
    kv: &dyn Kv,
    ddl: &TextSearchDdl,
) -> Result<Option<String>, ExecError> {
    let TextSearchDdl::Alter {
        kind: TextSearchObjectKind::Configuration,
        name,
        options,
        ..
    } = ddl
    else {
        return Ok(None);
    };
    let Some((_, value)) = options.iter().find(|(name, _)| name == "__mapping_drop") else {
        return Ok(None);
    };
    let Some((if_exists, token_types)) = value.split_once('\u{1e}') else {
        return Ok(None);
    };
    if if_exists != "1" {
        return Ok(None);
    }
    let Some(object) = find(kv, TextSearchObjectKind::Configuration, name)? else {
        return Ok(None);
    };
    let missing = token_types
        .split('\u{1f}')
        .collect::<BTreeSet<_>>()
        .into_iter()
        .find(|token_type| {
            !object
                .options
                .iter()
                .any(|(name, _)| name == &format!("__mapping_{token_type}"))
        });
    Ok(missing.map(|token_type| {
        format!("mapping for token type \"{token_type}\" does not exist, skipping")
    }))
}

pub(crate) fn execute(
    kv: &dyn Kv,
    ddl: &TextSearchDdl,
) -> Result<(&'static str, Vec<WriteOp>), ExecError> {
    match ddl {
        TextSearchDdl::Create {
            kind,
            name,
            base,
            options,
        } => {
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
                        && !matches!(
                            canonical(base).as_str(),
                            "simple" | "snowball" | "ispell" | "synonym" | "thesaurus"
                        )
                    {
                        return Err(ExecError::UndefinedObject(format!(
                            "text search template \"{base}\" does not exist"
                        )));
                    }
                    validate_dictionary_options(base, options)?;
                }
            }
            Ok((
                match kind {
                    TextSearchObjectKind::Configuration => "CREATE TEXT SEARCH CONFIGURATION",
                    TextSearchObjectKind::Dictionary => "CREATE TEXT SEARCH DICTIONARY",
                },
                vec![WriteOp::Put {
                    key: key(*kind, &name),
                    value: encode(&Object {
                        base: canonical(base),
                        options: options
                            .iter()
                            .filter(|(name, _)| name != "template")
                            .cloned()
                            .collect(),
                    }),
                }],
            ))
        }
        TextSearchDdl::Alter {
            kind,
            name,
            rename_to,
            options,
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
                            value: encode(&object),
                        },
                        WriteOp::Delete {
                            key: key(*kind, &old),
                        },
                    ],
                ));
            }
            let mut object = object;
            if *kind == TextSearchObjectKind::Configuration {
                validate_mapping_options(options)?;
            }
            for (name, value) in options {
                let name = if *kind == TextSearchObjectKind::Configuration {
                    if let Some(token_type) = name.strip_prefix("__mapping_add_") {
                        format!("__mapping_{token_type}")
                    } else {
                        name.clone()
                    }
                } else {
                    name.clone()
                };
                if *kind == TextSearchObjectKind::Configuration && name == "__mapping_drop" {
                    let Some((if_exists, token_types)) = value.split_once('\u{1e}') else {
                        return Err(ExecError::Unsupported(
                            "invalid text-search mapping removal".into(),
                        ));
                    };
                    let token_types = token_types.split('\u{1f}').collect::<BTreeSet<_>>();
                    for token_type in token_types {
                        let key = format!("__mapping_{token_type}");
                        let Some(index) = object.options.iter().position(|(name, _)| name == &key)
                        else {
                            if if_exists == "1" {
                                continue;
                            }
                            return Err(ExecError::UndefinedObject(format!(
                                "mapping for token type \"{token_type}\" does not exist"
                            )));
                        };
                        object.options.remove(index);
                    }
                    continue;
                }
                if *kind == TextSearchObjectKind::Configuration && name == "__mapping_replace" {
                    let Some((from, to)) = value.split_once('\u{1e}') else {
                        return Err(ExecError::Unsupported(
                            "invalid text-search mapping replacement".into(),
                        ));
                    };
                    if !object
                        .options
                        .iter()
                        .any(|(token_type, _)| token_type.starts_with("__mapping_"))
                    {
                        object.options.push((
                            "__mapping_word".into(),
                            config_dictionaries(Some(kv), &object.base)?.join("\u{1f}"),
                        ));
                    }
                    for (_, dictionaries) in object
                        .options
                        .iter_mut()
                        .filter(|(token_type, _)| token_type.starts_with("__mapping_"))
                    {
                        *dictionaries = dictionaries
                            .split('\u{1f}')
                            .map(|dictionary| if dictionary == from { to } else { dictionary })
                            .collect::<Vec<_>>()
                            .join("\u{1f}");
                    }
                    continue;
                }
                if let Some((_, current)) = object.options.iter_mut().find(|(key, _)| *key == name)
                {
                    *current = value.clone();
                } else {
                    object.options.push((name.clone(), value.clone()));
                }
            }
            if *kind == TextSearchObjectKind::Dictionary {
                validate_dictionary_options(&object.base, &object.options)?;
            }
            Ok((
                match kind {
                    TextSearchObjectKind::Configuration => "ALTER TEXT SEARCH CONFIGURATION",
                    TextSearchObjectKind::Dictionary => "ALTER TEXT SEARCH DICTIONARY",
                },
                if options.is_empty() {
                    Vec::new()
                } else {
                    vec![WriteOp::Put {
                        key: key(*kind, &old),
                        value: encode(&object),
                    }]
                },
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

/// The template a text-search dictionary is built on.
///
/// crabka implements the `simple`, `snowball`, and `ispell` templates.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum DictionaryTemplate {
    /// `simple`: fold to lower case, and reject a stop word.
    Simple,
    /// `snowball`: fold, reject a stop word, then stem.
    Snowball,
    /// `ispell`: derive lexemes from a selected dictionary and affix file.
    Ispell { dict_file: String, aff_file: String },
    /// `synonym`: replace a token from a selected synonym file.
    Synonym {
        synonyms: String,
        case_sensitive: bool,
    },
    /// `thesaurus`: replace terms from a selected thesaurus file.
    Thesaurus {
        dict_file: String,
        dictionary: String,
    },
}

/// The template `name` bottoms out at, following `TEMPLATE` through the
/// user-defined dictionaries in between.
///
/// The 42704 this raises is PostgreSQL's own wording for a `regdictionary`
/// that resolves to nothing, which is what a dictionary built on a template
/// crabka does not have leaves behind: the `CREATE` failed, so the name is
/// genuinely absent from the catalog rather than present and unserviceable.
pub(crate) fn dictionary_template(
    kv: Option<&dyn Kv>,
    name: &str,
) -> Result<DictionaryTemplate, ExecError> {
    let undefined =
        || ExecError::UndefinedObject(format!("text search dictionary \"{name}\" does not exist"));
    let strip = |value: &str| {
        let value = canonical(value);
        value
            .strip_prefix("pg_catalog.")
            .unwrap_or(&value)
            .to_string()
    };
    let mut current = strip(name);
    let mut seen = BTreeSet::new();
    loop {
        if !seen.insert(current.clone()) {
            return Err(ExecError::Unsupported(
                "text search dictionary TEMPLATE cycle".into(),
            ));
        }
        // The two built-in dictionaries come first: `simple` names both a
        // dictionary and the template it is built on, and `english_stem` is a
        // `snowball` dictionary whose own name is neither.
        match current.as_str() {
            "simple" => return Ok(DictionaryTemplate::Simple),
            "english_stem" => return Ok(DictionaryTemplate::Snowball),
            _ => {}
        }
        let kv = kv.ok_or_else(undefined)?;
        let object = find(kv, TextSearchObjectKind::Dictionary, &current)?.ok_or_else(undefined)?;
        let base = strip(&object.base);
        // `snowball` is a template only, so it terminates the walk; `simple`
        // rejoins the arm above on the next turn.
        if base == "snowball" {
            return Ok(DictionaryTemplate::Snowball);
        }
        if base == "ispell" {
            let option = |name| {
                object
                    .options
                    .iter()
                    .find(|(key, _)| key == name)
                    .map(|(_, value)| value.clone())
            };
            return Ok(DictionaryTemplate::Ispell {
                dict_file: option("dictfile").unwrap_or_else(|| "ispell_sample".into()),
                aff_file: option("afffile").unwrap_or_else(|| "ispell_sample".into()),
            });
        }
        if base == "synonym" {
            let option = |name| {
                object
                    .options
                    .iter()
                    .find(|(key, _)| key == name)
                    .map(|(_, value)| value.clone())
            };
            let case_sensitive = match option("casesensitive").as_deref() {
                None | Some("0" | "off" | "false") => false,
                Some("1" | "on" | "true") => true,
                Some(_) => {
                    return Err(ExecError::InvalidParameterValueMessage(
                        "casesensitive requires a Boolean value".into(),
                    ));
                }
            };
            return Ok(DictionaryTemplate::Synonym {
                synonyms: option("synonyms").unwrap_or_default(),
                case_sensitive,
            });
        }
        if base == "thesaurus" {
            return Ok(DictionaryTemplate::Thesaurus {
                dict_file: object
                    .options
                    .iter()
                    .find(|(key, _)| key == "dictfile")
                    .map(|(_, value)| value.clone())
                    .unwrap_or_default(),
                dictionary: object
                    .options
                    .iter()
                    .find(|(key, _)| key == "dictionary")
                    .map(|(_, value)| value.clone())
                    .unwrap_or_else(|| "english_stem".into()),
            });
        }
        current = base;
    }
}

/// The oid `pg_ts_config`/`pg_ts_dict` reports for a text-search object, and
/// the one `regconfig`/`regdictionary` resolve to.
///
/// crabka has no oid counter for these — they are named, not numbered, in the
/// catalog — so the oid is derived from the name by FNV-1a within a reserved
/// band. Both readers call this so a `regconfig` value and the `pg_ts_config`
/// row for the same name can never disagree.
pub(crate) fn object_oid(name: &str) -> i32 {
    let mut hash = 2_166_136_261u32;
    for byte in name.bytes() {
        hash = (hash ^ u32::from(byte)).wrapping_mul(16_777_619);
    }
    i32::try_from(60_000 + hash % 1_000_000).expect("bounded oid")
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
        let object = decode(value)?;
        rows.push((
            name,
            if kind == TextSearchObjectKind::Dictionary {
                options_text(&object.options)
            } else {
                object.base
            },
        ));
    }
    rows.sort();
    rows.dedup_by(|left, right| left.0 == right.0);
    Ok(rows)
}

#[cfg(test)]
mod tests {
    use assert2::assert;
    use crabka_pgkv::{Kv, MemKv};

    use super::*;

    #[test]
    fn dictionary_options_survive_alter() {
        let kv = MemKv::new();
        let create = TextSearchDdl::Create {
            kind: TextSearchObjectKind::Dictionary,
            name: "custom".into(),
            base: "simple".into(),
            options: vec![
                ("template".into(), "simple".into()),
                ("stopwords".into(), "none".into()),
            ],
        };
        let (_, writes) = execute(&kv, &create).expect("create dictionary");
        kv.write_batch(&writes).expect("write dictionary");

        let alter = TextSearchDdl::Alter {
            kind: TextSearchObjectKind::Dictionary,
            name: "custom".into(),
            rename_to: None,
            options: vec![("stopwords".into(), "english".into())],
        };
        let (_, writes) = execute(&kv, &alter).expect("alter dictionary");
        kv.write_batch(&writes).expect("write altered dictionary");

        assert!(
            find(&kv, TextSearchObjectKind::Dictionary, "custom").expect("read dictionary")
                == Some(Object {
                    base: "simple".into(),
                    options: vec![("stopwords".into(), "english".into())],
                })
        );
    }

    #[test]
    fn added_non_word_mapping_overrides_only_its_token_type() {
        let kv = MemKv::new();
        let create = TextSearchDdl::Create {
            kind: TextSearchObjectKind::Configuration,
            name: "custom".into(),
            base: "english".into(),
            options: Vec::new(),
        };
        let (_, writes) = execute(&kv, &create).expect("create configuration");
        kv.write_batch(&writes).expect("write configuration");
        let alter = TextSearchDdl::Alter {
            kind: TextSearchObjectKind::Configuration,
            name: "custom".into(),
            rename_to: None,
            options: vec![("__mapping_add_asciiword".into(), "simple".into())],
        };
        let (_, writes) = execute(&kv, &alter).expect("alter mapping");
        kv.write_batch(&writes).expect("write mapping");

        assert!(
            config_dictionaries(Some(&kv), "custom").expect("word dictionaries")
                == ["english_stem"]
        );
        assert!(
            config_token_dictionaries(Some(&kv), "custom", "asciiword")
                .expect("asciiword dictionaries")
                == ["simple"]
        );
    }
}
