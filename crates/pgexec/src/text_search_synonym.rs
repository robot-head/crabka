//! PostgreSQL's compact regression synonym dictionary.

const SAMPLE: &str = include_str!("text_search_data/synonym_sample.syn");

pub(crate) fn lexize(token: &str, synonyms: &str, case_sensitive: bool) -> Option<Vec<String>> {
    if synonyms != "synonym_sample" {
        return None;
    }
    let token = if case_sensitive {
        token.to_owned()
    } else {
        token.to_ascii_lowercase()
    };
    SAMPLE.lines().find_map(|line| {
        let (word, replacement) = line.split_once('\t')?;
        let word = if case_sensitive {
            word.to_owned()
        } else {
            word.to_ascii_lowercase()
        };
        (word == token).then(|| vec![replacement.into()])
    })
}

#[cfg(test)]
mod tests {
    use assert2::assert;

    use super::lexize;

    #[test]
    fn respects_case_sensitive_option() {
        assert!(lexize("PoStGrEs", "synonym_sample", false) == Some(vec!["pgsql".into()]));
        assert!(lexize("PoStGrEs", "synonym_sample", true).is_none());
        assert!(lexize("indices", "synonym_sample", false) == Some(vec!["index*".into()]));
    }
}
