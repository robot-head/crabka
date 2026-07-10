//! `remote_write` v2 string interning. `symbols[0]` is always the empty string;
//! all label names/values and metadata strings are `u32` indices into `symbols`.

use std::collections::{HashMap, HashSet, hash_map::Entry};

/// Errors from symbol-table operations.
#[derive(Debug, thiserror::Error)]
pub enum SymbolError {
    #[error("symbols[0] must be the empty string")]
    FirstNotEmpty,

    #[error("duplicate symbol `{0}`")]
    DuplicateSymbol(String),

    #[error("label_refs length {0} is not even")]
    OddRefs(usize),

    #[error("symbol ref {0} out of range (len {1})")]
    OutOfRange(u32, usize),

    #[error("duplicate label `{0}`")]
    DuplicateLabel(String),

    #[error("symbol table length {0} exceeds u32 refs")]
    TooManySymbols(usize),
}

/// A string-interning table matching `remote_write` v2 semantics.
#[derive(Debug)]
pub struct SymbolTable {
    symbols: Vec<String>,
    index: HashMap<String, u32>,
}

impl Default for SymbolTable {
    fn default() -> Self {
        let mut index = HashMap::new();
        index.insert(String::new(), 0);
        Self {
            symbols: vec![String::new()],
            index,
        }
    }
}

impl SymbolTable {
    /// Create a symbol table containing the required empty zero symbol.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Build from an existing symbol list, such as a received v2 request.
    ///
    /// The first symbol must be the empty string.
    pub fn from_symbols(symbols: Vec<String>) -> Result<Self, SymbolError> {
        if symbols.first().map(String::as_str) != Some("") {
            return Err(SymbolError::FirstNotEmpty);
        }

        let mut index = HashMap::with_capacity(symbols.len());
        for (i, symbol) in symbols.iter().enumerate() {
            let ref_ = u32::try_from(i).map_err(|_| SymbolError::TooManySymbols(symbols.len()))?;
            match index.entry(symbol.clone()) {
                Entry::Vacant(entry) => {
                    entry.insert(ref_);
                }
                Entry::Occupied(_) => return Err(SymbolError::DuplicateSymbol(symbol.clone())),
            }
        }

        Ok(Self { symbols, index })
    }

    /// Intern `s`, returning its stable ref.
    pub fn intern(&mut self, s: &str) -> u32 {
        if let Some(&ref_) = self.index.get(s) {
            return ref_;
        }

        let ref_ = u32::try_from(self.symbols.len()).expect("symbol table overflow");
        let symbol = s.to_string();
        self.symbols.push(symbol.clone());
        self.index.insert(symbol, ref_);
        ref_
    }

    /// Resolve a symbol ref to a string.
    #[must_use]
    pub fn resolve(&self, ref_: u32) -> Option<&str> {
        self.symbols.get(ref_ as usize).map(String::as_str)
    }

    /// Return all symbols in ref order.
    #[must_use]
    pub fn symbols(&self) -> &[String] {
        &self.symbols
    }

    /// Resolve even-length `(name_ref, value_ref)` pairs into label pairs.
    pub fn resolve_label_refs(&self, refs: &[u32]) -> Result<Vec<(String, String)>, SymbolError> {
        if !refs.len().is_multiple_of(2) {
            return Err(SymbolError::OddRefs(refs.len()));
        }

        let mut labels = Vec::with_capacity(refs.len() / 2);
        let mut names = HashSet::with_capacity(refs.len() / 2);
        for pair in refs.as_chunks::<2>().0 {
            let name = self
                .resolve(pair[0])
                .ok_or(SymbolError::OutOfRange(pair[0], self.symbols.len()))?;
            let value = self
                .resolve(pair[1])
                .ok_or(SymbolError::OutOfRange(pair[1], self.symbols.len()))?;
            if !names.insert(name) {
                return Err(SymbolError::DuplicateLabel(name.to_string()));
            }
            labels.push((name.to_string(), value.to_string()));
        }

        Ok(labels)
    }
}

#[cfg(test)]
mod tests {
    use assert2::check;

    use super::*;

    #[test]
    fn intern_is_stable_and_zero_is_empty() {
        let mut t = SymbolTable::new();
        assert2::assert!(t.resolve(0) == Some(""));
        let a = t.intern("app");
        let b = t.intern("api");
        check!(t.intern("app") == a);
        check!(t.resolve(a) == Some("app"));
        check!(t.resolve(b) == Some("api"));
    }

    #[test]
    fn resolve_label_refs_pairs_names_and_values() {
        let mut t = SymbolTable::new();
        let app = t.intern("app");
        let api = t.intern("api");
        let env = t.intern("env");
        let prod = t.intern("prod");
        let labels = t.resolve_label_refs(&[app, api, env, prod]).unwrap();
        assert2::assert!(
            labels == vec![("app".into(), "api".into()), ("env".into(), "prod".into())]
        );
    }

    #[test]
    fn odd_length_refs_rejected() {
        let t = SymbolTable::new();
        assert2::assert!(t.resolve_label_refs(&[1]).is_err());
    }

    #[test]
    fn from_symbols_requires_empty_first() {
        assert2::assert!(SymbolTable::from_symbols(vec!["x".into()]).is_err());
        assert2::assert!(SymbolTable::from_symbols(vec![String::new(), "x".into()]).is_ok());
    }

    #[test]
    fn from_symbols_rejects_duplicates() {
        assert2::assert!(
            SymbolTable::from_symbols(vec![String::new(), "x".into(), "x".into()]).is_err()
        );
    }

    #[test]
    fn resolve_label_refs_rejects_out_of_range_refs() {
        let t = SymbolTable::new();
        assert2::assert!(t.resolve_label_refs(&[0, 7]).is_err());
    }

    #[test]
    fn resolve_label_refs_rejects_duplicate_label_names() {
        let mut t = SymbolTable::new();
        let job = t.intern("job");
        let api = t.intern("api");
        let worker = t.intern("worker");

        let err = t.resolve_label_refs(&[job, api, job, worker]).unwrap_err();

        assert2::assert!(matches!(err, SymbolError::DuplicateLabel(name) if name == "job"));
    }
}
