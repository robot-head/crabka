//! The ISpell and Hunspell sample dictionaries used by PostgreSQL's regression suite.

use std::collections::BTreeSet;

use crate::error::ExecError;

const ISPELL_DICTIONARY: &str = include_str!("text_search_data/ispell_sample.dict");
const ISPELL_AFFIXES: &str = include_str!("text_search_data/ispell_sample.affix");
const HUNSPELL_AFFIXES: &str = include_str!("text_search_data/hunspell_sample.affix");
const HUNSPELL_LONG_DICTIONARY: &str = include_str!("text_search_data/hunspell_sample_long.dict");
const HUNSPELL_LONG_AFFIXES: &str = include_str!("text_search_data/hunspell_sample_long.affix");
const HUNSPELL_NUM_DICTIONARY: &str = include_str!("text_search_data/hunspell_sample_num.dict");
const HUNSPELL_NUM_AFFIXES: &str = include_str!("text_search_data/hunspell_sample_num.affix");

#[derive(Clone)]
struct Entry {
    root: String,
    flags: Vec<String>,
}

#[derive(Clone)]
struct Rule {
    prefix: bool,
    cross_product: bool,
    flag: String,
    strip: String,
    add: String,
    condition: String,
}

#[derive(Default)]
struct CompoundFlags {
    general: Option<String>,
    begin: Option<String>,
    middle: Option<String>,
    end: Option<String>,
    only: Option<String>,
}

struct Lexicon {
    entries: Vec<Entry>,
    rules: Vec<Rule>,
    compound: CompoundFlags,
}

#[derive(Clone, Copy)]
enum FlagFormat {
    Char,
    Long,
    Num,
}

fn flags(value: &str, format: FlagFormat) -> Vec<String> {
    match format {
        FlagFormat::Char => value
            .replace("\\\\", "\\")
            .chars()
            .map(|flag| flag.to_string())
            .collect(),
        FlagFormat::Long => value
            .as_bytes()
            .chunks_exact(2)
            .map(|flag| String::from_utf8_lossy(flag).into_owned())
            .collect(),
        FlagFormat::Num => value.split(',').map(str::to_owned).collect(),
    }
}

fn legacy_lexicon() -> Lexicon {
    let entries = ISPELL_DICTIONARY
        .lines()
        .map(|line| match line.split_once('/') {
            Some((root, raw_flags)) => Entry {
                root: root.to_ascii_lowercase(),
                flags: flags(raw_flags, FlagFormat::Char),
            },
            None => Entry {
                root: line.to_ascii_lowercase(),
                flags: Vec::new(),
            },
        })
        .collect();
    let mut prefix = false;
    let mut flag = None;
    let mut rules = Vec::new();
    for line in ISPELL_AFFIXES.lines() {
        let trimmed = line.trim();
        if trimmed == "prefixes" {
            prefix = true;
        } else if trimmed == "suffixes" {
            prefix = false;
        } else if let Some(value) = trimmed.strip_prefix("flag ") {
            let value = value.trim_end_matches(':');
            let cross_product = value.starts_with('*');
            let value = value.trim_start_matches('*');
            flag = value
                .strip_prefix("~\\\\")
                .map_or_else(|| value.chars().next(), |_| Some('\\'))
                .map(|flag| (flag, cross_product));
        } else if let Some((condition, value)) = trimmed.split_once('>')
            && let Some((flag, cross_product)) = flag
        {
            let value = value.split('#').next().unwrap_or_default().trim();
            let (strip, add) = value.split_once(',').map_or(("", value), |(strip, add)| {
                (strip.trim_start_matches('-'), add)
            });
            rules.push(Rule {
                prefix,
                cross_product,
                flag: flag.to_string(),
                strip: strip.to_ascii_lowercase(),
                add: add.to_ascii_lowercase(),
                condition: condition.trim().to_ascii_uppercase(),
            });
        }
    }
    Lexicon {
        entries,
        rules,
        compound: CompoundFlags {
            general: Some("Z".into()),
            ..CompoundFlags::default()
        },
    }
}

fn hunspell_lexicon(dictionary: &str, affixes: &str) -> Lexicon {
    let format = if affixes.lines().any(|line| line.trim() == "FLAG long") {
        FlagFormat::Long
    } else if affixes.lines().any(|line| line.trim() == "FLAG num") {
        FlagFormat::Num
    } else {
        FlagFormat::Char
    };
    let mut aliases = Vec::new();
    let mut compound = CompoundFlags::default();
    let mut cross_product_prefixes = BTreeSet::new();
    for line in affixes.lines().map(str::trim) {
        let words = line.split_whitespace().collect::<Vec<_>>();
        match words.as_slice() {
            ["AF", value, ..] if value.parse::<usize>().is_err() => {
                aliases.push(flags(value, format));
            }
            ["COMPOUNDFLAG", flag] => compound.general = Some((*flag).into()),
            ["COMPOUNDBEGIN", flag] => compound.begin = Some((*flag).into()),
            ["COMPOUNDMIDDLE", flag] => compound.middle = Some((*flag).into()),
            ["COMPOUNDEND", flag] => compound.end = Some((*flag).into()),
            ["ONLYINCOMPOUND", flag] => compound.only = Some((*flag).into()),
            ["PFX", flag, "Y", count] if count.parse::<usize>().is_ok() => {
                cross_product_prefixes
                    .insert(flags(flag, format).into_iter().next().unwrap_or_default());
            }
            _ => {}
        }
    }
    let entries = dictionary
        .lines()
        .map(|line| match line.split_once('/') {
            Some((root, raw_flags)) => {
                let flags = raw_flags
                    .parse::<usize>()
                    .ok()
                    .and_then(|number| aliases.get(number.saturating_sub(1)).cloned())
                    .unwrap_or_else(|| flags(raw_flags, format));
                Entry {
                    root: root.to_ascii_lowercase(),
                    flags,
                }
            }
            None => Entry {
                root: line.to_ascii_lowercase(),
                flags: Vec::new(),
            },
        })
        .collect();
    let rules = affixes
        .lines()
        .filter_map(|line| {
            let fields = line.split_whitespace().collect::<Vec<_>>();
            let (kind, flag, strip, add, condition) = match fields.as_slice() {
                [kind @ ("PFX" | "SFX"), flag, strip, add, condition, ..] => {
                    (*kind, *flag, *strip, *add, *condition)
                }
                _ => return None,
            };
            Some(Rule {
                prefix: kind == "PFX",
                cross_product: kind != "PFX"
                    || cross_product_prefixes
                        .contains(&flags(flag, format).into_iter().next().unwrap_or_default()),
                flag: flags(flag, format).into_iter().next().unwrap_or_default(),
                strip: (strip != "0")
                    .then_some(strip)
                    .unwrap_or_default()
                    .to_ascii_lowercase(),
                add: add
                    .split('/')
                    .next()
                    .filter(|add| *add != "0")
                    .unwrap_or_default()
                    .to_ascii_lowercase(),
                condition: condition.to_ascii_uppercase(),
            })
        })
        .collect();
    Lexicon {
        entries,
        rules,
        compound,
    }
}

fn lexicon(dict_file: &str, aff_file: &str) -> Option<Lexicon> {
    match (dict_file, aff_file) {
        ("ispell_sample", "ispell_sample") => Some(legacy_lexicon()),
        ("ispell_sample", "hunspell_sample") => {
            Some(hunspell_lexicon(ISPELL_DICTIONARY, HUNSPELL_AFFIXES))
        }
        ("hunspell_sample_long", "hunspell_sample_long") => Some(hunspell_lexicon(
            HUNSPELL_LONG_DICTIONARY,
            HUNSPELL_LONG_AFFIXES,
        )),
        ("hunspell_sample_num", "hunspell_sample_num") => Some(hunspell_lexicon(
            HUNSPELL_NUM_DICTIONARY,
            HUNSPELL_NUM_AFFIXES,
        )),
        _ => None,
    }
}

/// Check that the regression dictionary and affix fixtures form a valid pair.
pub(crate) fn validate_files(dict_file: &str, aff_file: &str) -> Result<(), ExecError> {
    if lexicon(dict_file, aff_file).is_some() {
        return Ok(());
    }
    if matches!(
        (dict_file, aff_file),
        ("hunspell_sample_long", "ispell_sample")
            | ("hunspell_sample_long", "hunspell_sample_num")
            | ("hunspell_sample_num", "ispell_sample")
    ) {
        return Ok(());
    }
    let detail = match (dict_file, aff_file) {
        ("hunspell_sample_num", "hunspell_sample_long") => {
            "invalid affix alias \"302,301,202,303\""
        }
        (_, "hunspell_sample_long") => "invalid affix alias \"GJUS\"",
        (_, "hunspell_sample_num") => "invalid affix flag \"SZ\\\"",
        _ => "unknown ISpell dictionary or affix file",
    };
    Err(ExecError::InvalidParameterValueMessage(detail.into()))
}

fn condition_matches(word: &str, condition: &str) -> bool {
    let condition = condition.strip_suffix("{1}").unwrap_or(condition);
    if condition == "." {
        return true;
    }
    if condition == "Y*" {
        return word.ends_with('y');
    }
    if let Some(rest) = condition.strip_prefix("[^")
        && let Some((denied, suffix)) = rest.split_once(']')
    {
        let chars = word.chars().collect::<Vec<_>>();
        return match suffix {
            "" => chars
                .last()
                .is_some_and(|letter| !denied.contains(letter.to_ascii_uppercase())),
            "Y" => {
                chars.len() > 1
                    && chars.last() == Some(&'y')
                    && !denied.contains(chars[chars.len() - 2].to_ascii_uppercase())
            }
            _ => false,
        };
    }
    condition.len() == 1
        && word
            .chars()
            .last()
            .is_some_and(|letter| condition.contains(letter.to_ascii_uppercase()))
}

fn apply(word: &str, rule: &Rule) -> Option<String> {
    if !condition_matches(word, &rule.condition) {
        return None;
    }
    if rule.prefix {
        return word
            .strip_prefix(&rule.strip)
            .map(|word| format!("{}{word}", rule.add));
    }
    word.strip_suffix(&rule.strip)
        .map(|word| format!("{word}{}", rule.add))
}

fn forms(entry: &Entry, rules: &[Rule]) -> Vec<(String, usize)> {
    let mut forms = vec![(entry.root.clone(), 0, BTreeSet::new())];
    for _ in 0..3 {
        let mut next = Vec::new();
        for (word, depth, used) in &forms {
            for (index, rule) in rules.iter().enumerate() {
                if !entry.flags.contains(&rule.flag) || used.contains(&index) {
                    continue;
                }
                if let Some(word) = apply(word, rule) {
                    let mut used = used.clone();
                    used.insert(index);
                    next.push((word, depth + 1, used));
                }
            }
        }
        forms.extend(next);
    }
    forms
        .into_iter()
        .map(|(word, depth, _)| (word, depth))
        .collect()
}

fn compound_matches(token: &str, lexicon: &Lexicon) -> Vec<Vec<String>> {
    fn allowed(entry: &Entry, compound: &CompoundFlags, first: bool, last: bool) -> bool {
        compound
            .only
            .as_ref()
            .is_none_or(|flag| !entry.flags.contains(flag))
            && (compound
                .general
                .as_ref()
                .is_some_and(|flag| entry.flags.contains(flag))
                || (first
                    && compound
                        .begin
                        .as_ref()
                        .is_some_and(|flag| entry.flags.contains(flag)))
                || (!first
                    && !last
                    && compound
                        .middle
                        .as_ref()
                        .is_some_and(|flag| entry.flags.contains(flag)))
                || (last
                    && compound
                        .end
                        .as_ref()
                        .is_some_and(|flag| entry.flags.contains(flag))))
    }
    fn split(
        remainder: &str,
        lexicon: &Lexicon,
        parts: &mut Vec<String>,
        matches: &mut Vec<Vec<String>>,
    ) {
        if remainder.is_empty() {
            if parts.len() > 1 {
                matches.push(parts.clone());
            }
            return;
        }
        for entry in &lexicon.entries {
            for (form, _) in forms(entry, &lexicon.rules) {
                let Some(rest) = remainder.strip_prefix(&form) else {
                    continue;
                };
                if !allowed(entry, &lexicon.compound, parts.is_empty(), rest.is_empty()) {
                    continue;
                }
                parts.push(entry.root.clone());
                split(rest, lexicon, parts, matches);
                parts.pop();
            }
        }
    }

    let mut matches = Vec::new();
    split(token, lexicon, &mut Vec::new(), &mut matches);
    matches.dedup();
    matches
}

/// Lexize against one of PostgreSQL's bundled ISpell or Hunspell sample data pairs.
pub(crate) fn lexize_files(token: &str, dict_file: &str, aff_file: &str) -> Option<Vec<String>> {
    let token = token.to_ascii_lowercase();
    let lexicon = lexicon(dict_file, aff_file)?;
    let mut matches = lexicon
        .entries
        .iter()
        .filter(|entry| {
            lexicon
                .compound
                .only
                .as_ref()
                .is_none_or(|flag| !entry.flags.contains(flag))
        })
        .flat_map(|entry| {
            forms(entry, &lexicon.rules)
                .into_iter()
                .filter(|(form, _)| form == &token)
                .map(move |(_, depth)| (depth, entry.root.len(), entry.root.clone()))
        })
        .collect::<Vec<_>>();
    matches.sort_by(|left, right| left.0.cmp(&right.0).then_with(|| right.1.cmp(&left.1)));
    matches.dedup_by(|left, right| left.2 == right.2);
    let mut words = matches
        .into_iter()
        .map(|(_, _, word)| word)
        .collect::<Vec<_>>();
    for rule in lexicon
        .rules
        .iter()
        .filter(|rule| rule.prefix && rule.cross_product)
    {
        let Some(unprefixed) = token.strip_prefix(&rule.add) else {
            continue;
        };
        for entry in &lexicon.entries {
            if forms(entry, &lexicon.rules)
                .into_iter()
                .any(|(form, depth)| depth > 0 && form == unprefixed)
                && !words.contains(&entry.root)
            {
                words.push(entry.root.clone());
            }
        }
    }
    words.extend(compound_matches(&token, &lexicon).into_iter().flatten());
    (!words.is_empty()).then_some(words)
}

/// Lexemes grouped by their accepted ISpell derivation for `tsquery` expansion.
pub(crate) fn query_lexize_files(
    token: &str,
    dict_file: &str,
    aff_file: &str,
) -> Option<Vec<Vec<String>>> {
    let token = token.to_ascii_lowercase();
    let lexicon = lexicon(dict_file, aff_file)?;
    let compounds = compound_matches(&token, &lexicon);
    let words = lexize_files(&token, dict_file, aff_file)?;
    let compound_words = compounds.iter().map(Vec::len).sum::<usize>();
    let mut groups = words[..words.len().saturating_sub(compound_words)]
        .iter()
        .cloned()
        .map(|word| vec![word])
        .collect::<Vec<_>>();
    groups.extend(compounds);
    Some(groups)
}

#[cfg(test)]
mod tests {
    use assert2::assert;

    use crate::error::ExecError;

    use super::{lexize_files, validate_files};

    fn ispell(token: &str) -> Option<Vec<String>> {
        lexize_files(token, "ispell_sample", "ispell_sample")
    }

    #[test]
    fn parses_ispell_suffixes_and_prefixes() {
        assert!(ispell("skies") == Some(vec!["sky".into()]));
        assert!(ispell("bookings") == Some(vec!["booking".into(), "book".into()]));
        assert!(ispell("booking") == Some(vec!["booking".into(), "book".into()]));
        assert!(ispell("unbooking") == Some(vec!["book".into()]));
        assert!(ispell("unbookings") == Some(vec!["book".into()]));
        assert!(ispell("rebookings") == Some(vec!["booking".into(), "book".into()]));
        assert!(ispell("rebook").is_none());
    }

    #[test]
    fn parses_ispell_compounds() {
        assert!(ispell("footklubber") == Some(vec!["foot".into(), "klubber".into()]));
        assert!(
            ispell("footballklubber")
                == Some(vec![
                    "footballklubber".into(),
                    "foot".into(),
                    "ball".into(),
                    "klubber".into(),
                    "football".into(),
                    "klubber".into(),
                ])
        );
        assert!(ispell("ballyklubber") == Some(vec!["ball".into(), "klubber".into()]));
        assert!(
            ispell("footballyklubber")
                == Some(vec!["foot".into(), "ball".into(), "klubber".into()])
        );
    }

    #[test]
    fn parses_hunspell_flag_formats() {
        assert!(
            lexize_files("booked", "hunspell_sample_long", "hunspell_sample_long")
                == Some(vec!["book".into()])
        );
        assert!(
            lexize_files("balls", "hunspell_sample_long", "hunspell_sample_long")
                == Some(vec!["ball".into()])
        );
        assert!(
            lexize_files(
                "ballsklubber",
                "hunspell_sample_long",
                "hunspell_sample_long"
            ) == Some(vec!["ball".into(), "klubber".into()])
        );
        assert!(
            lexize_files("ex-machina", "hunspell_sample_long", "hunspell_sample_long")
                == Some(vec!["ex-".into(), "machina".into()])
        );
        assert!(
            lexize_files("sk", "hunspell_sample_num", "hunspell_sample_num")
                == Some(vec!["sky".into()])
        );
    }

    #[test]
    fn rejects_incompatible_dictionary_and_affix_files() {
        for (dict, affix, expected) in [
            (
                "ispell_sample",
                "hunspell_sample_long",
                "invalid affix alias \"GJUS\"",
            ),
            (
                "ispell_sample",
                "hunspell_sample_num",
                "invalid affix flag \"SZ\\\"",
            ),
            (
                "hunspell_sample_num",
                "hunspell_sample_long",
                "invalid affix alias \"302,301,202,303\"",
            ),
        ] {
            let ExecError::InvalidParameterValueMessage(actual) =
                validate_files(dict, affix).unwrap_err()
            else {
                panic!("expected invalid parameter error");
            };
            assert!(actual == expected);
        }
        for (dict, affix) in [
            ("hunspell_sample_long", "ispell_sample"),
            ("hunspell_sample_long", "hunspell_sample_num"),
            ("hunspell_sample_num", "ispell_sample"),
        ] {
            assert!(validate_files(dict, affix).is_ok());
        }
    }
}
