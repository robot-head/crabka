//! The `search_path` GUC: the list of schemas an unqualified relation name is
//! looked for in, and the schema a new one is created in.
//!
//! The value is stored the way `PostgreSQL` stores it — one string, each entry
//! re-quoted only where a bare identifier would not round-trip — because that
//! is what `SHOW search_path` has to reproduce. Verified against
//! `postgres:18.4`:
//!
//! ```text
//! SET search_path = "MySchema", public;   SHOW -> "MySchema", public
//! SET search_path = MySchema;             SHOW -> myschema
//! SET search_path = 'a,b', public;        SHOW -> "a,b", public
//! SET search_path = '"unbalanced';        SHOW -> """unbalanced"
//! SET search_path = "$user", public;      SHOW -> "$user", public
//! ```
//!
//! Nothing here validates. A schema that does not exist is silently skipped
//! rather than refused, and even `'"unbalanced'` is accepted — `PostgreSQL`'s
//! list parsing is far more permissive than it looks, and inventing a `22023`
//! would manufacture a divergence.

/// The entry that stands for the session user's own schema.
const USER_ENTRY: &str = "$user";

/// The schema every session can see without naming it.
pub const PG_CATALOG: &str = "pg_catalog";

/// The `search_path` value: its entries in written order, unexpanded.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SearchPath {
    entries: Vec<String>,
}

impl Default for SearchPath {
    /// `PostgreSQL`'s own default, `"$user", public`.
    fn default() -> Self {
        Self {
            entries: vec![
                USER_ENTRY.to_string(),
                crabka_pgcatalog::PUBLIC_SCHEMA.to_string(),
            ],
        }
    }
}

impl SearchPath {
    /// The path a `SET search_path = …` list sets, from the items the parser
    /// kept apart.
    #[must_use]
    pub fn from_items(items: &[String]) -> Self {
        Self {
            entries: items.iter().map(|item| item.trim().to_string()).collect(),
        }
    }

    /// The path a stored GUC string denotes, splitting on commas outside a
    /// quoted entry — `PostgreSQL`'s `SplitIdentifierString`.
    #[must_use]
    pub fn parse(stored: &str) -> Self {
        let mut entries = Vec::new();
        let mut current = String::new();
        let mut quoted = false;
        let mut chars = stored.chars().peekable();
        while let Some(ch) = chars.next() {
            match ch {
                '"' if quoted && chars.peek() == Some(&'"') => {
                    chars.next();
                    current.push('"');
                }
                '"' => quoted = !quoted,
                ',' if !quoted => entries.push(std::mem::take(&mut current).trim().to_string()),
                _ => current.push(ch),
            }
        }
        entries.push(current.trim().to_string());
        Self { entries }
    }

    /// The value `SHOW search_path` reports: every entry re-quoted where a bare
    /// identifier would not read back as itself.
    #[must_use]
    pub fn render(&self) -> String {
        self.entries
            .iter()
            .map(|entry| quote_list_entry(entry))
            .collect::<Vec<_>>()
            .join(", ")
    }

    /// The schemas named, in order, with `"$user"` expanded to `user`,
    /// `pg_temp` expanded to `temp`, and repeats dropped. Entries are not
    /// checked against the catalog here — `current_schemas` and the resolver
    /// each filter against it themselves.
    #[must_use]
    pub fn expanded(&self, user: &str, temp: &str) -> Vec<String> {
        let mut out: Vec<String> = Vec::with_capacity(self.entries.len());
        for entry in &self.entries {
            let name = match entry.as_str() {
                USER_ENTRY => user,
                crabka_pgcatalog::PG_TEMP_ALIAS => temp,
                other => other,
            };
            if name.is_empty() || out.iter().any(|seen| seen == name) {
                continue;
            }
            out.push(name.to_string());
        }
        out
    }

    /// True when the path names the temporary namespace itself, in which case
    /// it sits where it was written rather than implicitly first. Verified
    /// against `postgres:18.4`, where `SET search_path = public, pg_temp` makes
    /// `current_schemas(true)` report `{pg_catalog,public,pg_temp_1}`.
    #[must_use]
    pub fn names_temp_schema(&self, temp: &str) -> bool {
        self.entries
            .iter()
            .any(|entry| entry == crabka_pgcatalog::PG_TEMP_ALIAS || entry == temp)
    }
}

/// Spell one list entry the way `PostgreSQL`'s `quote_identifier` does: bare
/// when it reads back as itself, and double-quoted (with embedded quotes
/// doubled) otherwise.
fn quote_list_entry(entry: &str) -> String {
    let bare = !entry.is_empty()
        && !entry.starts_with(|ch: char| ch.is_ascii_digit())
        && entry
            .chars()
            .all(|ch| ch.is_ascii_lowercase() || ch.is_ascii_digit() || ch == '_');
    if bare {
        return entry.to_string();
    }
    format!("\"{}\"", entry.replace('"', "\"\""))
}

#[cfg(test)]
mod tests {
    use assert2::assert;

    use super::SearchPath;

    /// Each case is `(the items a SET wrote, what SHOW must report)`, taken
    /// from `postgres:18.4`.
    #[test]
    fn a_set_list_renders_the_way_show_reports_it() {
        let cases = [
            (vec!["MySchema", "public"], "\"MySchema\", public"),
            (vec!["myschema"], "myschema"),
            (vec!["a,b", "public"], "\"a,b\", public"),
            (vec!["\"unbalanced"], "\"\"\"unbalanced\""),
            (vec!["$user", "public"], "\"$user\", public"),
            (vec!["x y", "q,r"], "\"x y\", \"q,r\""),
            (vec![""], "\"\""),
            (vec!["pg_catalog", "public"], "pg_catalog, public"),
        ];
        for (items, rendered) in cases {
            let items: Vec<String> = items.into_iter().map(str::to_string).collect();
            assert!(SearchPath::from_items(&items).render() == rendered);
        }
    }

    /// A rendered path has to read back as the entries it was built from,
    /// including the ones a comma or a quote makes unrepresentable in a plain
    /// join.
    #[test]
    fn rendering_round_trips_through_parsing() {
        for items in [
            vec!["MySchema", "public"],
            vec!["a,b", "public"],
            vec!["\"unbalanced"],
            vec!["$user", "public"],
            vec!["x y", "q,r"],
        ] {
            let items: Vec<String> = items.into_iter().map(str::to_string).collect();
            let path = SearchPath::from_items(&items);
            assert!(SearchPath::parse(&path.render()) == path);
        }
    }

    #[test]
    fn the_user_entry_expands_and_repeats_collapse() {
        let path = SearchPath::default();
        assert!(path.expanded("alice", "pg_temp_3") == vec!["alice".to_string(), "public".into()]);
        let doubled = SearchPath::from_items(&["public".into(), "public".into()]);
        assert!(doubled.expanded("alice", "pg_temp_3") == vec!["public".to_string()]);
        assert!(doubled.render() == "public, public");
    }

    /// A written `pg_temp` names the session's own temporary namespace, and is
    /// what puts it somewhere other than implicitly first.
    #[test]
    fn the_temp_entry_expands_to_the_sessions_own_namespace() {
        let path = SearchPath::from_items(&["public".into(), "pg_temp".into()]);
        assert!(
            path.expanded("alice", "pg_temp_3") == vec!["public".to_string(), "pg_temp_3".into()]
        );
        assert!(path.names_temp_schema("pg_temp_3"));
        assert!(path.render() == "public, pg_temp");
        assert!(!SearchPath::default().names_temp_schema("pg_temp_3"));
    }

    #[test]
    fn an_empty_path_names_no_schema() {
        let path = SearchPath::from_items(&[String::new()]);
        assert!(path.expanded("alice", "pg_temp_3").is_empty());
        assert!(path.render() == "\"\"");
    }
}
