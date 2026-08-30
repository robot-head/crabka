//! SP40: the dependency-inversion seam between the executor and a foreign-data
//! wrapper, that is the `kafka_fdw` crate. The executor knows only this trait.
//! The FDW crate implements it, and
//! [`crate::SqlEngine::set_foreign_scanner`] injects it into the engine. With no
//! scanner registered, a `SELECT` from a foreign table returns `0A000`, with the
//! message "foreign tables require the `kafka` feature".

use crabka_pgcatalog::{Column, ForeignServer, Table, UserMapping};
use crabka_pgtypes::Datum;

use crate::{clock::EvalCtx, error::ExecError};

/// The slice of the scan a [`ForeignScanner`] should materialize. Phase 1 always
/// passes `ScanBounds::default()`, that is a full snapshot to the topic's
/// high-water mark. Predicate and offset pushdown lands in a later task, which
/// will fill these fields from the query's `WHERE` clause.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct ScanBounds {
    /// Inclusive lower partition-offset bound per partition, if pushed down.
    /// Empty means scan from the beginning of every partition.
    pub start_offsets: Vec<(i32, i64)>,
    /// Exclusive upper partition-offset bound per partition, if pushed down.
    /// Empty means scan to each partition's high-water mark.
    pub end_offsets: Vec<(i32, i64)>,
}

/// A filter on the tables an `IMPORT FOREIGN SCHEMA` materializes. The executor
/// translates the parsed `ImportSelector` into this neutral shape, so the FDW
/// does not depend on the parser's AST.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ImportFilter {
    /// Import every table the server exposes.
    All,
    /// Import only the named tables, from `LIMIT TO (...)`.
    Only(Vec<String>),
    /// Import every table except the named ones, from `EXCEPT (...)`.
    Except(Vec<String>),
}

impl ImportFilter {
    /// Translate the parser's [`crabka_pgparser::ast::ImportSelector`] into this neutral
    /// filter so the FDW seam never depends on the parser AST.
    #[must_use]
    pub fn from_selector(selector: &crabka_pgparser::ast::ImportSelector) -> Self {
        use crabka_pgparser::ast::ImportSelector;
        match selector {
            ImportSelector::All => Self::All,
            ImportSelector::LimitTo(names) => Self::Only(names.clone()),
            ImportSelector::Except(names) => Self::Except(names.clone()),
        }
    }

    /// Whether a table or topic of the given `name` survives this filter. `All`
    /// keeps everything, `Only` keeps just the listed names, and `Except` drops
    /// the listed names. The match is on the bare table or topic name.
    #[must_use]
    pub fn retains(&self, name: &str) -> bool {
        match self {
            Self::All => true,
            Self::Only(names) => names.iter().any(|n| n == name),
            Self::Except(names) => !names.iter().any(|n| n == name),
        }
    }
}

/// The executor↔FDW seam. An implementor turns a foreign table's catalog
/// metadata, that is the schema, the server connection options and an optional
/// user mapping, into rows aligned to the table's column order. The envelope
/// columns `_partition`, `_offset`, `_timestamp`, `_key` and `_headers` come
/// first, then the decoded value columns, exactly as
/// [`crabka_pgcatalog::create_foreign_table`] lays them out.
pub trait ForeignScanner: Send + Sync {
    /// Materialize the foreign table's rows for one scan.
    ///
    /// - `table` carries the column schema and the `table.foreign` metadata,
    ///   that is the server name and the table OPTIONS such as `topic` and
    ///   `value_format`.
    /// - `server` carries the server-level OPTIONS, such as `bootstrap` and
    ///   `registry_url`.
    /// - `mapping` is the resolved user mapping, that is the credentials, if one
    ///   exists.
    /// - `bounds` is the requested slice, a full snapshot in phase 1.
    /// - `ctx` is the per-statement evaluation context, that is the zone and the
    ///   clock, for any temporal decoding.
    ///
    /// Each returned row MUST have exactly `table.columns.len()` datums in
    /// column order.
    ///
    /// # Errors
    ///
    /// Returns an error when the requested operation cannot be completed.
    fn scan(
        &self,
        table: &Table,
        server: &ForeignServer,
        mapping: Option<&UserMapping>,
        bounds: &ScanBounds,
        ctx: &EvalCtx,
    ) -> Result<Vec<Vec<Datum>>, ExecError>;

    /// Enumerate the importable tables for `IMPORT FOREIGN SCHEMA`.
    ///
    /// Returns one [`ImportedTable`] per table the server exposes that survives
    /// `filter`. Each one carries its complete table schema. Each one also carries the table OPTIONS the executor
    /// must persist, so a later `scan` decodes consistently. The `value_format`
    /// is the crucial one, and it must match the schema the columns were derived
    /// from. The Kafka FDW enumerates every topic and derives `value_columns`
    /// from the topic's Schema Registry `"<topic>-value"` subject. When no
    /// subject is registered, it falls back to a single raw `value bytea` column
    /// and `value_format=raw`.
    ///
    /// # Errors
    ///
    /// Returns an error when the requested operation cannot be completed.
    fn import_schema(
        &self,
        server: &ForeignServer,
        mapping: Option<&UserMapping>,
        filter: &ImportFilter,
    ) -> Result<Vec<ImportedTable>, ExecError>;

    /// Enumerate importable tables with SQL `IMPORT FOREIGN SCHEMA` options.
    ///
    /// Existing scanners that do not interpret import options retain their
    /// `import_schema` implementation. An FDW that does can override this
    /// method without making the executor depend on its option vocabulary.
    fn import_schema_with_options(
        &self,
        server: &ForeignServer,
        mapping: Option<&UserMapping>,
        filter: &ImportFilter,
        options: &[(String, String)],
    ) -> Result<Vec<ImportedTable>, ExecError> {
        let _ = options;
        self.import_schema(server, mapping, filter)
    }
}

/// One table `IMPORT FOREIGN SCHEMA` materializes. It carries the table's name,
/// complete column schema, and table OPTIONS to persist, such as `topic` and
/// `value_format`. The persisted OPTIONS let a later `scan` decode the value
/// bytes the same way the import derived the columns.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ImportedTable {
    /// The foreign table name, which is also the Kafka topic name.
    pub name: String,
    /// The complete schema the foreign-data wrapper exposes.
    pub columns: Vec<Column>,
    /// Table OPTIONS to store on the created foreign table.
    pub options: Vec<(String, String)>,
}

#[cfg(test)]
mod tests {
    use crabka_pgparser::ast::ImportSelector;

    use super::ImportFilter;

    #[test]
    fn from_selector_translates_each_variant() {
        assert_eq!(
            ImportFilter::from_selector(&ImportSelector::All),
            ImportFilter::All
        );
        assert_eq!(
            ImportFilter::from_selector(&ImportSelector::LimitTo(vec![
                "orders".into(),
                "payments".into()
            ])),
            ImportFilter::Only(vec!["orders".into(), "payments".into()])
        );
        assert_eq!(
            ImportFilter::from_selector(&ImportSelector::Except(vec!["foo".into()])),
            ImportFilter::Except(vec!["foo".into()])
        );
    }

    #[test]
    fn all_retains_everything() {
        let f = ImportFilter::All;
        assert!(f.retains("orders"));
        assert!(f.retains("anything"));
    }

    #[test]
    fn only_keeps_listed_drops_others() {
        let f = ImportFilter::Only(vec!["orders".into(), "payments".into()]);
        assert!(f.retains("orders"));
        assert!(f.retains("payments"));
        assert!(!f.retains("shipments"));
    }

    #[test]
    fn except_drops_listed_keeps_others() {
        let f = ImportFilter::Except(vec!["foo".into(), "bar".into()]);
        assert!(!f.retains("foo"));
        assert!(!f.retains("bar"));
        assert!(f.retains("orders"));
    }
}
