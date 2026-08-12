//! `PostgreSQL`'s relation-option catalog: every `WITH (…)` and `SET (…)` name,
//! the relation families it belongs to, and the values it accepts.
//!
//! `PostgreSQL` keeps this in one table — the `boolRelOpts`/`intRelOpts`/
//! `realRelOpts`/`enumRelOpts` arrays of `src/backend/access/common/reloptions.c`
//! — and every caller validates through it: `heap_reloptions` for a table,
//! `index_reloptions` for an index of any access method,
//! `partitioned_table_reloptions`, `view_reloptions`. [`RELOPTIONS`] is that
//! table, and [`validate`] is `transformRelOptions` plus `parseRelOptions`
//! plus `parse_one_reloption` folded into one pass. Naming the option's
//! *family* rather than its statement is what makes one table serve them all:
//! `fillfactor` is refused on a GIN index because the catalog says it belongs
//! to the heap and to four of the six index methods, not because a GIN-shaped
//! branch says so.
//!
//! What this module does not do is *store* anything. crabka has no page
//! layout to pack, no autovacuum to schedule and no buffering build, so an
//! option's default and its accepted value are equally inert once the
//! statement is past. The catalog therefore carries each option's range but
//! not its default: a default nothing reads is a claim nothing checks.

use std::fmt::Write as _;

/// The relation families an option can belong to — `RELOPT_KIND_*`.
///
/// One option may belong to several: `fillfactor` is a heap option *and* a
/// btree, hash, gist and spgist option, with the same range in all five.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RelOptKind {
    /// An ordinary table, and a materialized view, which is stored like one.
    Heap,
    /// A table's TOAST relation, written under the `toast.` namespace.
    Toast,
    Btree,
    Hash,
    Gin,
    Gist,
    Spgist,
    Brin,
    /// A view. crabka's view options are read by the typed parser in
    /// `Parser::view_option_name`, not through [`validate`]; the rows are here
    /// so the catalog is the whole of `reloptions.c` and so the unit test below
    /// can hold the two readers to the same three names.
    View,
    /// A single column, set by `ALTER TABLE … ALTER COLUMN … SET (…)`.
    Attribute,
    /// A tablespace, set by `ALTER TABLESPACE … SET (…)`.
    Tablespace,
}

/// The six `pg_am` rows with `amtype = 'i'`, in the order `\dA` lists them.
///
/// A partitioned index takes the same options as an unpartitioned one of the
/// same method, so nothing here distinguishes them.
pub static INDEX_KINDS: [RelOptKind; 6] = [
    RelOptKind::Brin,
    RelOptKind::Btree,
    RelOptKind::Gin,
    RelOptKind::Gist,
    RelOptKind::Hash,
    RelOptKind::Spgist,
];

/// The value an option accepts, and the range it accepts it in.
#[derive(Debug, Clone, Copy)]
enum OptionType {
    /// `RELOPT_TYPE_BOOL`. Read with `parse_bool`, which takes any unambiguous
    /// prefix of `true`/`false`/`yes`/`no`, `on`/`off`, and `1`/`0`.
    Bool,
    /// `RELOPT_TYPE_INT`. The bounds are inclusive and are quoted with `%d` in
    /// the DETAIL line.
    Int { min: i32, max: i32 },
    /// `RELOPT_TYPE_REAL`. The bounds are inclusive and are quoted with `%f` —
    /// six decimal places — in the DETAIL line.
    Real { min: f64, max: f64 },
    /// `RELOPT_TYPE_ENUM`. `members` is matched case-insensitively; `detail` is
    /// the option's own `detailmsg`, which names the values a *user* may write
    /// and so is shorter than `members` where the two disagree.
    Enum {
        members: &'static [&'static str],
        detail: &'static str,
    },
}

/// One row of the catalog.
#[derive(Debug, Clone, Copy)]
pub struct RelOption {
    /// The lowercase spelling. `PostgreSQL`'s grammar downcases an unquoted
    /// option name, so `WITH (FiLLFaCToR=30)` reaches here as `fillfactor` and
    /// `WITH ("Fillfactor" = 10)` does not reach it at all.
    pub name: &'static str,
    kinds: &'static [RelOptKind],
    ty: OptionType,
}

impl RelOption {
    /// Does this option belong to `kind`?
    #[must_use]
    pub fn applies_to(&self, kind: RelOptKind) -> bool {
        self.kinds.contains(&kind)
    }
}

const INT_MAX: i32 = i32::MAX;
/// `MAX_KILOBYTES` — `INT_MAX` on a 64-bit build.
const MAX_KILOBYTES: i32 = i32::MAX;
/// `MAX_IO_CONCURRENCY`, from `storage/bufmgr.h`.
const MAX_IO_CONCURRENCY: i32 = 1000;
/// `TOAST_TUPLE_TARGET_MAIN` — `MaximumBytesPerTuple(TOAST_TUPLES_PER_PAGE_MAIN)`
/// on the default 8 kB page.
const TOAST_TUPLE_TARGET_MAIN: i32 = 8160;

/// `Valid values are "on", "off", and "auto".` — shared by `vacuum_index_cleanup`
/// and by `GiST`'s `buffering`.
const ON_OFF_AUTO: &str = "Valid values are \"on\", \"off\", and \"auto\".";

const HEAP: &[RelOptKind] = &[RelOptKind::Heap];
const HEAP_TOAST: &[RelOptKind] = &[RelOptKind::Heap, RelOptKind::Toast];
const VIEW: &[RelOptKind] = &[RelOptKind::View];
const ATTRIBUTE: &[RelOptKind] = &[RelOptKind::Attribute];
const TABLESPACE: &[RelOptKind] = &[RelOptKind::Tablespace];

/// Every option `reloptions.c` defines, in that file's order.
///
/// Options with the same name in several families collapse to one row when the
/// families agree on the range, which is what `fillfactor`'s five rows do. They
/// disagree only on the default, and this catalog keeps no defaults.
pub const RELOPTIONS: &[RelOption] = &[
    // boolRelOpts
    RelOption {
        name: "autosummarize",
        kinds: &[RelOptKind::Brin],
        ty: OptionType::Bool,
    },
    RelOption {
        name: "autovacuum_enabled",
        kinds: HEAP_TOAST,
        ty: OptionType::Bool,
    },
    RelOption {
        name: "user_catalog_table",
        kinds: HEAP,
        ty: OptionType::Bool,
    },
    RelOption {
        name: "fastupdate",
        kinds: &[RelOptKind::Gin],
        ty: OptionType::Bool,
    },
    RelOption {
        name: "security_barrier",
        kinds: VIEW,
        ty: OptionType::Bool,
    },
    RelOption {
        name: "security_invoker",
        kinds: VIEW,
        ty: OptionType::Bool,
    },
    RelOption {
        name: "vacuum_truncate",
        kinds: HEAP_TOAST,
        ty: OptionType::Bool,
    },
    RelOption {
        name: "deduplicate_items",
        kinds: &[RelOptKind::Btree],
        ty: OptionType::Bool,
    },
    // intRelOpts. `fillfactor` is five rows upstream — heap, btree, hash, gist,
    // spgist — that differ only in their default. GIN and BRIN have none.
    RelOption {
        name: "fillfactor",
        kinds: &[
            RelOptKind::Heap,
            RelOptKind::Btree,
            RelOptKind::Hash,
            RelOptKind::Gist,
            RelOptKind::Spgist,
        ],
        ty: OptionType::Int { min: 10, max: 100 },
    },
    RelOption {
        name: "autovacuum_vacuum_threshold",
        kinds: HEAP_TOAST,
        ty: OptionType::Int {
            min: 0,
            max: INT_MAX,
        },
    },
    RelOption {
        name: "autovacuum_vacuum_max_threshold",
        kinds: HEAP_TOAST,
        ty: OptionType::Int {
            min: -1,
            max: INT_MAX,
        },
    },
    RelOption {
        name: "autovacuum_vacuum_insert_threshold",
        kinds: HEAP_TOAST,
        ty: OptionType::Int {
            min: -1,
            max: INT_MAX,
        },
    },
    RelOption {
        name: "autovacuum_analyze_threshold",
        kinds: HEAP,
        ty: OptionType::Int {
            min: 0,
            max: INT_MAX,
        },
    },
    RelOption {
        name: "autovacuum_vacuum_cost_limit",
        kinds: HEAP_TOAST,
        ty: OptionType::Int {
            min: 1,
            max: 10_000,
        },
    },
    RelOption {
        name: "autovacuum_freeze_min_age",
        kinds: HEAP_TOAST,
        ty: OptionType::Int {
            min: 0,
            max: 1_000_000_000,
        },
    },
    RelOption {
        name: "autovacuum_multixact_freeze_min_age",
        kinds: HEAP_TOAST,
        ty: OptionType::Int {
            min: 0,
            max: 1_000_000_000,
        },
    },
    RelOption {
        name: "autovacuum_freeze_max_age",
        kinds: HEAP_TOAST,
        ty: OptionType::Int {
            min: 100_000,
            max: 2_000_000_000,
        },
    },
    RelOption {
        name: "autovacuum_multixact_freeze_max_age",
        kinds: HEAP_TOAST,
        ty: OptionType::Int {
            min: 10_000,
            max: 2_000_000_000,
        },
    },
    RelOption {
        name: "autovacuum_freeze_table_age",
        kinds: HEAP_TOAST,
        ty: OptionType::Int {
            min: 0,
            max: 2_000_000_000,
        },
    },
    RelOption {
        name: "autovacuum_multixact_freeze_table_age",
        kinds: HEAP_TOAST,
        ty: OptionType::Int {
            min: 0,
            max: 2_000_000_000,
        },
    },
    RelOption {
        name: "log_autovacuum_min_duration",
        kinds: HEAP_TOAST,
        ty: OptionType::Int {
            min: -1,
            max: INT_MAX,
        },
    },
    RelOption {
        name: "toast_tuple_target",
        kinds: HEAP,
        ty: OptionType::Int {
            min: 128,
            max: TOAST_TUPLE_TARGET_MAIN,
        },
    },
    RelOption {
        name: "pages_per_range",
        kinds: &[RelOptKind::Brin],
        ty: OptionType::Int {
            min: 1,
            max: 131_072,
        },
    },
    RelOption {
        name: "gin_pending_list_limit",
        kinds: &[RelOptKind::Gin],
        ty: OptionType::Int {
            min: 64,
            max: MAX_KILOBYTES,
        },
    },
    RelOption {
        name: "effective_io_concurrency",
        kinds: TABLESPACE,
        ty: OptionType::Int {
            min: 0,
            max: MAX_IO_CONCURRENCY,
        },
    },
    RelOption {
        name: "maintenance_io_concurrency",
        kinds: TABLESPACE,
        ty: OptionType::Int {
            min: 0,
            max: MAX_IO_CONCURRENCY,
        },
    },
    RelOption {
        name: "parallel_workers",
        kinds: HEAP,
        ty: OptionType::Int { min: 0, max: 1024 },
    },
    // realRelOpts
    RelOption {
        name: "autovacuum_vacuum_cost_delay",
        kinds: HEAP_TOAST,
        ty: OptionType::Real {
            min: 0.0,
            max: 100.0,
        },
    },
    RelOption {
        name: "autovacuum_vacuum_scale_factor",
        kinds: HEAP_TOAST,
        ty: OptionType::Real {
            min: 0.0,
            max: 100.0,
        },
    },
    RelOption {
        name: "autovacuum_vacuum_insert_scale_factor",
        kinds: HEAP_TOAST,
        ty: OptionType::Real {
            min: 0.0,
            max: 100.0,
        },
    },
    RelOption {
        name: "autovacuum_analyze_scale_factor",
        kinds: HEAP,
        ty: OptionType::Real {
            min: 0.0,
            max: 100.0,
        },
    },
    RelOption {
        name: "vacuum_max_eager_freeze_failure_rate",
        kinds: HEAP_TOAST,
        ty: OptionType::Real { min: 0.0, max: 1.0 },
    },
    RelOption {
        name: "seq_page_cost",
        kinds: TABLESPACE,
        ty: OptionType::Real {
            min: 0.0,
            max: f64::MAX,
        },
    },
    RelOption {
        name: "random_page_cost",
        kinds: TABLESPACE,
        ty: OptionType::Real {
            min: 0.0,
            max: f64::MAX,
        },
    },
    RelOption {
        name: "n_distinct",
        kinds: ATTRIBUTE,
        ty: OptionType::Real {
            min: -1.0,
            max: f64::MAX,
        },
    },
    RelOption {
        name: "n_distinct_inherited",
        kinds: ATTRIBUTE,
        ty: OptionType::Real {
            min: -1.0,
            max: f64::MAX,
        },
    },
    RelOption {
        name: "vacuum_cleanup_index_scale_factor",
        kinds: &[RelOptKind::Btree],
        ty: OptionType::Real {
            min: 0.0,
            max: 1e10,
        },
    },
    // enumRelOpts
    RelOption {
        name: "vacuum_index_cleanup",
        kinds: HEAP_TOAST,
        ty: OptionType::Enum {
            members: &["auto", "on", "off", "true", "false", "yes", "no", "1", "0"],
            detail: ON_OFF_AUTO,
        },
    },
    RelOption {
        name: "buffering",
        kinds: &[RelOptKind::Gist],
        ty: OptionType::Enum {
            members: &["auto", "on", "off"],
            detail: ON_OFF_AUTO,
        },
    },
    RelOption {
        name: "check_option",
        kinds: VIEW,
        ty: OptionType::Enum {
            members: &["local", "cascaded"],
            detail: "Valid values are \"local\" and \"cascaded\".",
        },
    },
];

/// A rejected option list, in `PostgreSQL`'s wording.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RelOptionError {
    pub sqlstate: &'static str,
    pub message: String,
    pub detail: Option<String>,
    pub hint: Option<&'static str>,
}

impl RelOptionError {
    /// The SQLSTATE all but two of these carry: `invalid_parameter_value`.
    fn invalid(message: String) -> Self {
        Self {
            sqlstate: "22023",
            message,
            detail: None,
            hint: None,
        }
    }

    fn with_detail(mut self, detail: String) -> Self {
        self.detail = Some(detail);
        self
    }
}

/// What a `WITH (…)` or `SET (…)` list is being written on.
///
/// The three shapes differ in more than which families they admit, which is why
/// this is a statement-facing enum rather than a bare `&[RelOptKind]`: a table
/// also strips `oids`, and a partitioned table refuses every option with a
/// message of its own instead of naming the option.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RelOptionTarget {
    /// `CREATE TABLE`, `CREATE MATERIALIZED VIEW`, `ALTER TABLE … SET (…)`:
    /// heap options, plus toast options under the `toast.` namespace, plus the
    /// historical `oids` spelling.
    Table,
    /// `CREATE TABLE … PARTITION BY …`: no options at all. A partitioned table
    /// has no storage to parameterise, and `partitioned_table_reloptions`
    /// refuses the whole list rather than reporting a name.
    PartitionedTable,
    /// `CREATE INDEX … USING <am>`: the one access method's options.
    Index(RelOptKind),
    /// `ALTER INDEX … SET (…)`, where the statement does not name the access
    /// method. Any index option is admitted, so a btree option on a GIN index
    /// gets through here where `CREATE INDEX` would have caught it.
    AnyIndex,
}

impl RelOptionTarget {
    /// The access method's family, for a `CREATE INDEX` that wrote `USING`.
    /// `None` for a method `pg_am` has no row for; the caller has already
    /// refused that.
    #[must_use]
    pub fn for_index_method(method: &str) -> Option<Self> {
        let kind = match method {
            "brin" => RelOptKind::Brin,
            "btree" => RelOptKind::Btree,
            "gin" => RelOptKind::Gin,
            "gist" => RelOptKind::Gist,
            "hash" => RelOptKind::Hash,
            "spgist" => RelOptKind::Spgist,
            _ => return None,
        };
        Some(Self::Index(kind))
    }
}

/// The namespaces a target admits. Everything else is
/// `unrecognized parameter namespace`.
fn valid_namespaces(target: RelOptionTarget) -> &'static [(&'static str, RelOptKind)] {
    match target {
        // `validnsps = {"toast", NULL}` in `DefineRelation` and
        // `ATExecSetRelOptions`, decided before the relation kind is — which is
        // why a partitioned table admits the namespace even though it has no
        // TOAST relation to give the options to.
        RelOptionTarget::Table | RelOptionTarget::PartitionedTable => {
            &[("toast", RelOptKind::Toast)]
        }
        RelOptionTarget::Index(_) | RelOptionTarget::AnyIndex => &[],
    }
}

/// The families an unqualified name is looked up in.
fn bare_kinds(target: RelOptionTarget) -> &'static [RelOptKind] {
    match target {
        RelOptionTarget::Table => HEAP,
        RelOptionTarget::PartitionedTable => &[],
        RelOptionTarget::AnyIndex => &INDEX_KINDS,
        RelOptionTarget::Index(kind) => one_index_kind(kind),
    }
}

/// The one-element slice holding `kind`, borrowed out of [`INDEX_KINDS`].
///
/// Empty for a family that is not an index method. Only
/// [`RelOptionTarget::for_index_method`] builds an `Index`, so that cannot
/// happen — and admitting nothing is the safer reading of it if it ever does.
fn one_index_kind(kind: RelOptKind) -> &'static [RelOptKind] {
    match INDEX_KINDS.iter().position(|entry| *entry == kind) {
        Some(at) => &INDEX_KINDS[at..=at],
        None => &[],
    }
}

/// Validate a written `WITH (…)`/`SET (…)` list against the catalog.
///
/// `options` is the list as written, each entry a name — possibly
/// `namespace.name` — and the value beside it, or `None` where only the name
/// was written. This is `transformRelOptions` followed by the target's
/// `*_reloptions` parser, and it reports the first fault in the order those two
/// find them: every namespace is checked before any value is read, then the
/// unqualified names, then each namespace's names in turn.
///
/// # Errors
///
/// Returns the option list's first fault, worded as `PostgreSQL` words it.
pub fn validate(
    target: RelOptionTarget,
    options: &[(String, Option<String>)],
) -> Result<(), RelOptionError> {
    let namespaces = valid_namespaces(target);

    // `transformRelOptions` walks the whole list once per namespace, and the
    // namespace check sits above the filter that skips the other namespaces'
    // entries — so a bad namespace is reported before any value is parsed, no
    // matter which namespace holds the first bad value.
    for (key, _) in options {
        if let Some((namespace, _)) = split_namespace(key)
            && !namespaces.iter().any(|(name, _)| *name == namespace)
        {
            return Err(RelOptionError::invalid(format!(
                "unrecognized parameter namespace \"{namespace}\""
            )));
        }
    }

    // `oids` never reaches the catalog: `transformRelOptions` filters it out of
    // the unqualified list, refusing the affirmative spelling outright. It is
    // filtered before the emptiness test below, so `PARTITION BY … WITH (oids =
    // false)` is a partitioned table with no options rather than one with a
    // rejected option.
    let mut unqualified: Vec<(&str, Option<&str>)> = Vec::new();
    for (key, value) in options {
        let Some(name) = bare_name(key) else { continue };
        if name.contains('=') {
            return Err(RelOptionError::invalid(format!(
                "invalid option name \"{name}\": must not contain \"=\""
            )));
        }
        if matches!(
            target,
            RelOptionTarget::Table | RelOptionTarget::PartitionedTable
        ) && name == "oids"
        {
            match parse_bool(value.as_deref().unwrap_or("true")) {
                // `WITH (oids = false)` is the one spelling still accepted, and
                // it is dropped rather than stored.
                Some(false) => continue,
                Some(true) => {
                    return Err(RelOptionError {
                        sqlstate: "0A000",
                        message: "tables declared WITH OIDS are not supported".to_string(),
                        detail: None,
                        hint: None,
                    });
                }
                None => {
                    return Err(RelOptionError {
                        sqlstate: "42601",
                        message: "oids requires a Boolean value".to_string(),
                        detail: None,
                        hint: None,
                    });
                }
            }
        }
        unqualified.push((name, value.as_deref()));
    }

    if target == RelOptionTarget::PartitionedTable {
        // The namespaced entries are not checked at all: nothing ever creates
        // the TOAST relation they were meant for, so `PostgreSQL` never reaches
        // a parser for them either.
        if unqualified.is_empty() {
            return Ok(());
        }
        return Err(RelOptionError {
            sqlstate: "42809",
            message: "cannot specify storage parameters for a partitioned table".to_string(),
            detail: None,
            hint: Some("Specify storage parameters for its leaf partitions instead."),
        });
    }

    parse_one_namespace(bare_kinds(target), &unqualified)?;

    for (namespace, kind) in namespaces {
        let entries: Vec<(&str, Option<&str>)> = options
            .iter()
            .filter_map(|(key, value)| {
                let (written, name) = split_namespace(key)?;
                (written == *namespace).then_some((name, value.as_deref()))
            })
            .collect();
        parse_one_namespace(std::slice::from_ref(kind), &entries)?;
    }
    Ok(())
}

/// `RESET (…)` names no values, and `PostgreSQL` refuses one at 42601 rather
/// than ignoring it.
///
/// The names themselves are *not* checked: `ATExecSetRelOptions` removes them
/// from the stored list and validates what is left, so resetting a name the
/// catalog has never heard of succeeds — which is the only way to clear an
/// option that a direct `pg_class` update put there.
///
/// # Errors
///
/// Returns 42601 when any entry carries a value.
pub fn validate_reset(options: &[(String, Option<String>)]) -> Result<(), RelOptionError> {
    if options.iter().any(|(_, value)| value.is_some()) {
        return Err(RelOptionError {
            sqlstate: "42601",
            message: "RESET must not include values for parameters".to_string(),
            detail: None,
            hint: None,
        });
    }
    Ok(())
}

/// Split `namespace.name`. `None` when the key names no namespace.
fn split_namespace(key: &str) -> Option<(&str, &str)> {
    key.split_once('.')
}

/// The key when it names no namespace.
fn bare_name(key: &str) -> Option<&str> {
    key.split_once('.').is_none().then_some(key)
}

/// `parseRelOptions` over one namespace's entries: find each name among the
/// families in scope, refuse a repeat, then read the value.
fn parse_one_namespace(
    kinds: &[RelOptKind],
    entries: &[(&str, Option<&str>)],
) -> Result<(), RelOptionError> {
    let mut seen: Vec<&str> = Vec::new();
    for (name, value) in entries {
        let Some(option) = RELOPTIONS.iter().find(|option| {
            option.name == *name && kinds.iter().any(|kind| option.applies_to(*kind))
        }) else {
            return Err(RelOptionError::invalid(format!(
                "unrecognized parameter \"{name}\""
            )));
        };
        if seen.contains(name) {
            return Err(RelOptionError::invalid(format!(
                "parameter \"{name}\" specified more than once"
            )));
        }
        seen.push(name);
        // A bare name is `name=true`, which is how `WITH (autovacuum_enabled)`
        // reads — and why `WITH (fillfactor)` is an *invalid integer*, not a
        // missing value.
        parse_one_reloption(option, value.unwrap_or("true"))?;
    }
    Ok(())
}

/// `parse_one_reloption`: read one value at the option's type, then range-check
/// it. Every message quotes the value as written, not as parsed.
fn parse_one_reloption(option: &RelOption, value: &str) -> Result<(), RelOptionError> {
    let name = option.name;
    match option.ty {
        OptionType::Bool => {
            if parse_bool(value).is_none() {
                return Err(RelOptionError::invalid(format!(
                    "invalid value for boolean option \"{name}\": {value}"
                )));
            }
        }
        OptionType::Int { min, max } => {
            let Some(parsed) = parse_int(value) else {
                return Err(RelOptionError::invalid(format!(
                    "invalid value for integer option \"{name}\": {value}"
                )));
            };
            if parsed < f64::from(min) || parsed > f64::from(max) {
                return Err(RelOptionError::invalid(format!(
                    "value {value} out of bounds for option \"{name}\""
                ))
                .with_detail(format!("Valid values are between \"{min}\" and \"{max}\".")));
            }
        }
        OptionType::Real { min, max } => {
            let Some(parsed) = parse_real(value) else {
                return Err(RelOptionError::invalid(format!(
                    "invalid value for floating point option \"{name}\": {value}"
                )));
            };
            if parsed < min || parsed > max {
                let mut detail = String::from("Valid values are between \"");
                // `%f`, which is six decimal places and no exponent.
                let _ = write!(detail, "{min:.6}\" and \"{max:.6}\".");
                return Err(RelOptionError::invalid(format!(
                    "value {value} out of bounds for option \"{name}\""
                ))
                .with_detail(detail));
            }
        }
        OptionType::Enum { members, detail } => {
            if !members
                .iter()
                .any(|member| member.eq_ignore_ascii_case(value))
            {
                return Err(RelOptionError::invalid(format!(
                    "invalid value for enum option \"{name}\": {value}"
                ))
                .with_detail(detail.to_string()));
            }
        }
    }
    Ok(())
}

/// `parse_bool`: any case-insensitive prefix of `true`, `false`, `yes` or `no`;
/// `on` or `off` written out to at least two letters, because one `o` does not
/// choose between them; and `1` or `0` alone.
fn parse_bool(value: &str) -> Option<bool> {
    let lowered = value.to_ascii_lowercase();
    if lowered.is_empty() {
        return None;
    }
    for (word, result) in [
        ("true", true),
        ("false", false),
        ("yes", true),
        ("no", false),
    ] {
        if word.starts_with(&lowered) {
            return Some(result);
        }
    }
    if lowered.len() >= 2 {
        if "on".starts_with(&lowered) {
            return Some(true);
        }
        if "off".starts_with(&lowered) {
            return Some(false);
        }
    }
    match lowered.as_str() {
        "1" => Some(true),
        "0" => Some(false),
        _ => None,
    }
}

/// `parse_int` with no unit flags: `strtol` with C's base detection, re-read as
/// a float when the text runs on into a fraction or an exponent or overflows,
/// then rounded to nearest — ties to even, which is what `rint` does under the
/// default rounding mode — and range-checked against `int`.
///
/// The rounded value comes back as `f64` because that is all the caller needs:
/// the bounds are `int`s, and every message quotes the text rather than the
/// number.
fn parse_int(value: &str) -> Option<f64> {
    let text = value.trim();
    if let Some(exact) = parse_c_int(text) {
        return Some(f64::from(exact));
    }
    let parsed: f64 = text.parse().ok()?;
    if parsed.is_nan() {
        return None;
    }
    let rounded = parsed.round_ties_even();
    (rounded >= f64::from(i32::MIN) && rounded <= f64::from(i32::MAX)).then_some(rounded)
}

/// A C integer literal that fills the whole of `text`: optional sign, then
/// decimal, or `0x…` hexadecimal, or a leading `0` for octal.
///
/// `None` covers both "stopped before the end" and "does not fit an `int`". The
/// caller re-reads those as floats, which is what `parse_int` does with a
/// fraction and with `strtol`'s `ERANGE` alike, and a value too large for an
/// `int` fails the same range check either way.
fn parse_c_int(text: &str) -> Option<i32> {
    let (negative, rest) = match text.strip_prefix('-') {
        Some(rest) => (true, rest),
        None => (false, text.strip_prefix('+').unwrap_or(text)),
    };
    let (radix, digits) =
        if let Some(hex) = rest.strip_prefix("0x").or_else(|| rest.strip_prefix("0X")) {
            (16, hex)
        } else if rest.len() > 1 && rest.starts_with('0') {
            (8, &rest[1..])
        } else {
            (10, rest)
        };
    if digits.is_empty() || !digits.bytes().all(|byte| byte.is_ascii_alphanumeric()) {
        return None;
    }
    let magnitude = i64::from_str_radix(digits, radix).ok()?;
    let signed = if negative { -magnitude } else { magnitude };
    i32::try_from(signed).ok()
}

/// `parse_real` with no unit flags: `strtod` over the whole text, with `NaN`
/// refused outright and an infinity left to fail the range check.
fn parse_real(value: &str) -> Option<f64> {
    let parsed: f64 = value.trim().parse().ok()?;
    (!parsed.is_nan()).then_some(parsed)
}

#[cfg(test)]
mod tests {
    use assert2::assert;

    use super::{RELOPTIONS, RelOptKind, RelOptionTarget, validate, validate_reset};

    fn options(pairs: &[(&str, Option<&str>)]) -> Vec<(String, Option<String>)> {
        pairs
            .iter()
            .map(|(key, value)| ((*key).to_string(), value.map(ToString::to_string)))
            .collect()
    }

    fn refuse(target: RelOptionTarget, pairs: &[(&str, Option<&str>)]) -> (String, Option<String>) {
        let error = validate(target, &options(pairs)).expect_err("expected a refusal");
        (error.message, error.detail)
    }

    #[test]
    fn fillfactor_bounds_are_the_same_on_every_access_method_that_has_one() {
        for method in ["btree", "hash", "gist", "spgist"] {
            let target = RelOptionTarget::for_index_method(method).expect("an index method");
            assert!(validate(target, &options(&[("fillfactor", Some("10"))])) == Ok(()));
            assert!(validate(target, &options(&[("fillfactor", Some("100"))])) == Ok(()));
            assert!(
                refuse(target, &[("fillfactor", Some("9"))])
                    == (
                        "value 9 out of bounds for option \"fillfactor\"".into(),
                        Some("Valid values are between \"10\" and \"100\".".into())
                    ),
                "{method}"
            );
            assert!(
                refuse(target, &[("fillfactor", Some("101"))])
                    == (
                        "value 101 out of bounds for option \"fillfactor\"".into(),
                        Some("Valid values are between \"10\" and \"100\".".into())
                    ),
                "{method}"
            );
        }
    }

    #[test]
    fn the_two_access_methods_without_a_fillfactor_do_not_take_one() {
        for method in ["gin", "brin"] {
            let target = RelOptionTarget::for_index_method(method).expect("an index method");
            assert!(
                refuse(target, &[("fillfactor", Some("50"))])
                    == ("unrecognized parameter \"fillfactor\"".into(), None),
                "{method}"
            );
        }
    }

    #[test]
    fn an_access_method_takes_only_its_own_options() {
        let gin = RelOptionTarget::for_index_method("gin").expect("gin");
        let btree = RelOptionTarget::for_index_method("btree").expect("btree");
        let gist = RelOptionTarget::for_index_method("gist").expect("gist");
        let brin = RelOptionTarget::for_index_method("brin").expect("brin");

        assert!(validate(gin, &options(&[("fastupdate", Some("off"))])) == Ok(()));
        assert!(
            refuse(btree, &[("fastupdate", Some("off"))])
                == ("unrecognized parameter \"fastupdate\"".into(), None)
        );
        assert!(validate(btree, &options(&[("deduplicate_items", Some("on"))])) == Ok(()));
        assert!(
            refuse(gist, &[("deduplicate_items", Some("on"))])
                == ("unrecognized parameter \"deduplicate_items\"".into(), None)
        );
        assert!(validate(gist, &options(&[("buffering", Some("auto"))])) == Ok(()));
        assert!(
            refuse(gist, &[("buffering", Some("invalid_value"))])
                == (
                    "invalid value for enum option \"buffering\": invalid_value".into(),
                    Some("Valid values are \"on\", \"off\", and \"auto\".".into())
                )
        );
        assert!(validate(brin, &options(&[("pages_per_range", Some("2"))])) == Ok(()));
        assert!(
            refuse(brin, &[("pages_per_range", Some("0"))])
                == (
                    "value 0 out of bounds for option \"pages_per_range\"".into(),
                    Some("Valid values are between \"1\" and \"131072\".".into())
                )
        );
    }

    #[test]
    fn alter_index_admits_every_access_methods_options_because_it_names_none() {
        for name in ["fillfactor", "fastupdate", "deduplicate_items", "buffering"] {
            let value = match name {
                "fillfactor" => "90",
                "buffering" => "auto",
                _ => "on",
            };
            assert!(
                validate(RelOptionTarget::AnyIndex, &options(&[(name, Some(value))])) == Ok(()),
                "{name}"
            );
        }
        assert!(
            refuse(
                RelOptionTarget::AnyIndex,
                &[("autovacuum_enabled", Some("off"))]
            ) == ("unrecognized parameter \"autovacuum_enabled\"".into(), None)
        );
    }

    #[test]
    fn a_table_reads_heap_options_and_the_toast_namespace() {
        let table = RelOptionTarget::Table;
        assert!(
            validate(
                table,
                &options(&[
                    ("fillfactor", Some("30")),
                    ("autovacuum_enabled", Some("false")),
                    ("autovacuum_analyze_scale_factor", Some("0.2")),
                    ("toast.autovacuum_vacuum_cost_delay", Some("23")),
                ])
            ) == Ok(())
        );
        assert!(
            refuse(table, &[("toast.not_existing_option", Some("42"))])
                == (
                    "unrecognized parameter \"not_existing_option\"".into(),
                    None
                )
        );
        assert!(
            refuse(table, &[("not_existing_namespace.fillfactor", Some("2"))])
                == (
                    "unrecognized parameter namespace \"not_existing_namespace\"".into(),
                    None
                )
        );
        // A heap option is not a toast option: `autovacuum_analyze_scale_factor`
        // is `RELOPT_KIND_HEAP` alone.
        assert!(
            refuse(
                table,
                &[("toast.autovacuum_analyze_scale_factor", Some("0.2"))]
            ) == (
                "unrecognized parameter \"autovacuum_analyze_scale_factor\"".into(),
                None
            )
        );
    }

    #[test]
    fn a_bad_namespace_is_reported_before_any_value_is_read() {
        assert!(
            refuse(
                RelOptionTarget::Table,
                &[("fillfactor", Some("2")), ("nope.fillfactor", Some("30"))]
            ) == ("unrecognized parameter namespace \"nope\"".into(), None)
        );
    }

    /// One written option list, the message it draws, and the DETAIL below it.
    type Rejection = (
        &'static [(&'static str, Option<&'static str>)],
        &'static str,
        Option<&'static str>,
    );

    #[test]
    fn every_value_shape_reloptions_rejects() {
        let table = RelOptionTarget::Table;
        let cases: &[Rejection] = &[
            (
                &[("fillfactor", Some("2"))],
                "value 2 out of bounds for option \"fillfactor\"",
                Some("Valid values are between \"10\" and \"100\"."),
            ),
            (
                &[("autovacuum_analyze_scale_factor", Some("-10.0"))],
                "value -10.0 out of bounds for option \"autovacuum_analyze_scale_factor\"",
                Some("Valid values are between \"0.000000\" and \"100.000000\"."),
            ),
            (
                &[("not_existing_option", Some("2"))],
                "unrecognized parameter \"not_existing_option\"",
                None,
            ),
            (
                // `strtol` stops at the point, so `parse_int` re-reads the text
                // as a float and rounds it — to -30, which is out of bounds
                // rather than unreadable.
                &[("fillfactor", Some("-30.1"))],
                "value -30.1 out of bounds for option \"fillfactor\"",
                Some("Valid values are between \"10\" and \"100\"."),
            ),
            (
                &[("fillfactor", Some("string"))],
                "invalid value for integer option \"fillfactor\": string",
                None,
            ),
            (
                &[("fillfactor", Some("true"))],
                "invalid value for integer option \"fillfactor\": true",
                None,
            ),
            (
                // A bare non-boolean name means `= true`, so it fails as a
                // value rather than as a missing one.
                &[("fillfactor", None)],
                "invalid value for integer option \"fillfactor\": true",
                None,
            ),
            (
                &[("autovacuum_enabled", Some("12"))],
                "invalid value for boolean option \"autovacuum_enabled\": 12",
                None,
            ),
            (
                &[("autovacuum_enabled", Some("30.5"))],
                "invalid value for boolean option \"autovacuum_enabled\": 30.5",
                None,
            ),
            (
                &[("autovacuum_analyze_scale_factor", Some("true"))],
                "invalid value for floating point option \"autovacuum_analyze_scale_factor\": true",
                None,
            ),
            (
                &[("fillfactor", Some("30")), ("fillfactor", Some("40"))],
                "parameter \"fillfactor\" specified more than once",
                None,
            ),
        ];
        for (written, message, detail) in cases {
            assert!(
                refuse(table, written) == ((*message).to_string(), detail.map(ToString::to_string)),
                "{written:?}"
            );
        }
    }

    #[test]
    fn a_bare_boolean_name_is_true_and_a_prefix_spelling_is_accepted() {
        for value in [
            None,
            Some("t"),
            Some("TRU"),
            Some("yes"),
            Some("n"),
            Some("on"),
            Some("off"),
            Some("1"),
            Some("0"),
        ] {
            assert!(
                validate(
                    RelOptionTarget::Table,
                    &options(&[("autovacuum_enabled", value)])
                ) == Ok(()),
                "{value:?}"
            );
        }
        // One `o` does not choose between `on` and `off`.
        assert!(
            refuse(RelOptionTarget::Table, &[("autovacuum_enabled", Some("o"))])
                == (
                    "invalid value for boolean option \"autovacuum_enabled\": o".into(),
                    None
                )
        );
    }

    #[test]
    fn the_historical_oids_spelling_is_refused_only_in_the_affirmative() {
        assert!(validate(RelOptionTarget::Table, &options(&[("oids", Some("false"))])) == Ok(()));
        for value in [None, Some("true")] {
            let error = validate(RelOptionTarget::Table, &options(&[("oids", value)]))
                .expect_err("refused");
            assert!(error.sqlstate == "0A000", "{value:?}");
            assert!(
                error.message == "tables declared WITH OIDS are not supported",
                "{value:?}"
            );
        }
    }

    #[test]
    fn a_partitioned_table_refuses_the_whole_list_without_naming_an_option() {
        let error = validate(
            RelOptionTarget::PartitionedTable,
            &options(&[("fillfactor", Some("100"))]),
        )
        .expect_err("refused");
        assert!(error.sqlstate == "42809");
        assert!(error.message == "cannot specify storage parameters for a partitioned table");
        assert!(error.hint == Some("Specify storage parameters for its leaf partitions instead."));
        // `oids = false` is stripped before the emptiness test, so it leaves no
        // list to refuse.
        assert!(
            validate(
                RelOptionTarget::PartitionedTable,
                &options(&[("oids", Some("false"))])
            ) == Ok(())
        );
    }

    #[test]
    fn reset_takes_any_name_but_no_value() {
        assert!(validate_reset(&options(&[("illegal_option", None)])) == Ok(()));
        let error = validate_reset(&options(&[("fillfactor", Some("12"))])).expect_err("refused");
        assert!(error.sqlstate == "42601");
        assert!(error.message == "RESET must not include values for parameters");
    }

    #[test]
    fn the_catalogs_view_options_are_exactly_the_three_the_view_parser_takes() {
        let mut from_catalog: Vec<&str> = RELOPTIONS
            .iter()
            .filter(|option| option.applies_to(RelOptKind::View))
            .map(|option| option.name)
            .collect();
        from_catalog.sort_unstable();

        // The typed reader in the parser, exercised through a statement rather
        // than by reading its source.
        let accepted = |name: &str| {
            crate::parse(&format!(
                "CREATE VIEW v WITH ({name} = {}) AS SELECT 1",
                if name == "check_option" {
                    "local"
                } else {
                    "true"
                }
            ))
            .is_ok()
        };
        assert!(from_catalog == vec!["check_option", "security_barrier", "security_invoker"]);
        for name in &from_catalog {
            assert!(accepted(name), "{name}");
        }
        assert!(!accepted("fillfactor"));
    }
}
