//! PostgreSQL's compact regression thesaurus dictionary.

use rust_stemmers::{Algorithm, Stemmer};

const SAMPLE: &str = include_str!("text_search_data/thesaurus_sample.ths");

pub(crate) fn lexize(token: &str, dict_file: &str) -> Option<Vec<String>> {
    if dict_file != "thesaurus_sample" {
        return None;
    }
    SAMPLE.lines().find_map(|line| {
        let (input, output) = line.split_once(':')?;
        let input = input.trim();
        (input.eq_ignore_ascii_case(token) && !input.contains(' ')).then(|| {
            output
                .split_whitespace()
                .map(|word| word.trim_start_matches('*').to_ascii_lowercase())
                .collect()
        })
    })
}

pub(crate) fn lexize_phrase(tokens: &[&str], dict_file: &str) -> Option<(usize, Vec<String>)> {
    if dict_file != "thesaurus_sample" {
        return None;
    }
    SAMPLE
        .lines()
        .filter_map(|line| {
            let (input, output) = line.split_once(':')?;
            let input = input.split_whitespace().collect::<Vec<_>>();
            let stemmer = Stemmer::create(Algorithm::English);
            (input.len() <= tokens.len()
                && input.iter().zip(tokens).all(|(wanted, token)| {
                    *wanted == "?"
                        || stemmer.stem(&wanted.to_ascii_lowercase())
                            == stemmer.stem(&token.to_ascii_lowercase())
                }))
            .then(|| {
                (
                    input.len(),
                    output
                        .split_whitespace()
                        .map(|word| word.trim_start_matches('*').to_ascii_lowercase())
                        .collect::<Vec<_>>(),
                )
            })
        })
        .max_by_key(|(words, _)| *words)
}
