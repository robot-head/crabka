//! SP29: scalar (row) functions + the `||` concatenation operator.
//!
//! Like SP27's aggregates and SP28's predicates, every function here is a pure,
//! deterministic transform over a *single row's* already-MVCC-resolved Datums.
//! A whole table lives on one range (`RangeMap::range_for_table`), so a scalar
//! function executes entirely inside one `execute_read`/`eval` on one engine.
//! There is no cross-range scatter, no new lock/visibility rule, and no new
//! interleaving. This is exactly CLAUDE.md's "pure-data / single-node refactor"
//! carve-out, so SP29 ships NO Stateright model. A model of a scalar fold would
//! have an interleaving-free state space and would only restate these unit
//! tests.
//!
//! The dispatch mirrors SP28: scalar `eval` and the grouped evaluator
//! (`agg::eval_grouped`) share the pure combinators, and only the child-eval
//! closure differs (`eval_scalar` takes it as `FnMut(&Expr) -> Result<Datum>`).
//!
//! Supported: string `length`/`char_length`/`character_length`, `upper`,
//! `lower`, `btrim`/`ltrim`/`rtrim`, `substr`/`substring` (the comma form),
//! `replace`, `concat`; math `abs`, `mod`; null/conditional `coalesce`,
//! `nullif`, `greatest`, `least`. `||` is a binary operator handled in `eval`.

use std::{
    cmp::Ordering,
    sync::atomic::{AtomicU64, Ordering as AtomicOrdering},
    time::{Duration, Instant},
};

use crabka_pgparser::ast::{Expr, FuncArgs, FuncCall};
use crabka_pgtypes::{
    ColumnType, Datum, ElemType, ops,
    usertype::{MultirangeRef, RangeRef},
};

use crate::{clock::EvalCtx, error::ExecError, scope::Scope};

/// The scalar functions SP29 supports.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ScalarFunc {
    Length,
    Upper,
    Lower,
    Btrim,
    Ltrim,
    Rtrim,
    Substr,
    Overlay,
    Replace,
    Concat,
    NumNonNulls,
    NumNulls,
    Abs,
    Mod,
    TypedAdd(ColumnType),
    TypedSub(ColumnType),
    Int8Inc,
    Int4Sum,
    Int4Larger,
    Int4Smaller,
    ArrayLarger,
    EnumFirst,
    EnumLast,
    EnumRange,
    Int4AvgAccum,
    Int8Avg,
    Float8Accum,
    Float8Avg,
    BoolState {
        and: bool,
    },
    BoolCompare {
        equal: bool,
    },
    Coalesce,
    NullIf,
    Greatest,
    Least,
    // SP33: rounding family (type-preserving).
    Floor,
    Ceil,
    Round,
    Trunc,
    Sign,
    // SP33: transcendental family (always float8).
    Sqrt,
    Power,
    Exp,
    Ln,
    Log,
    Pi,
    Float4Send,
    // SP33: string family.
    Lpad,
    Rpad,
    Left,
    Right,
    Repeat,
    Reverse,
    Strpos,
    Initcap,
    Ascii,
    Chr,
    CurrentSetting,
    SetConfig,
    /// S3: one member of the advisory-lock family. The flags carry the spelling
    /// so the eight `pg_[try_]advisory[_xact]_lock[_shared]` names share one
    /// implementation.
    AdvisoryLock {
        /// `pg_try_advisory_*` returns a boolean and does not wait.
        try_only: bool,
        /// The `_xact` spellings release at transaction end.
        transactional: bool,
        /// The `_shared` spellings take a share lock.
        shared: bool,
    },
    /// S3: `pg_advisory_unlock` / `pg_advisory_unlock_shared`.
    AdvisoryUnlock {
        shared: bool,
    },
    /// S3: `pg_advisory_unlock_all`.
    AdvisoryUnlockAll,
    CurrentDatabase,
    GetDatabaseEncoding,
    /// `unicode_version()`: the Unicode release the engine's character tables
    /// come from.
    UnicodeVersion,
    /// `unicode_assigned(text)`: has every code point in the string been
    /// assigned?
    UnicodeAssigned,
    CurrentSchema,
    CurrentUser,
    SessionUser,
    Version,
    FormatType,
    /// `pg_typeof(any)`: the argument's resolved type name.
    PgTypeof,
    /// `pg_input_is_valid(text, text)`: would the type's input function accept
    /// this string?
    PgInputIsValid,
    /// Regression helper exposing PostgreSQL's `IsBinaryCoercible`.
    BinaryCoercible,
    PgNumaAvailable,
    /// PostgreSQL's temporal hash support functions.
    TemporalHash {
        ty: ColumnType,
        extended: bool,
    },
    /// PostgreSQL's integer hash support functions, including seeded variants.
    IntegerHash {
        ty: ColumnType,
        extended: bool,
    },
    /// PostgreSQL's cross-type-compatible floating-point hash functions.
    FloatHash {
        ty: ColumnType,
        extended: bool,
    },
    /// PostgreSQL's byte-string hash support functions.
    TextHash {
        ty: ColumnType,
        extended: bool,
    },
    /// PostgreSQL's `oidvector` hash support functions.
    OidVectorHash {
        extended: bool,
    },
    /// PostgreSQL's polymorphic array hash support functions.
    ArrayHash {
        extended: bool,
    },
    BpcharHash {
        extended: bool,
    },
    UuidHash {
        extended: bool,
    },
    PgLsnHash {
        extended: bool,
    },
    EnumHash {
        extended: bool,
    },
    RangeHash {
        extended: bool,
    },
    MultirangeHash {
        extended: bool,
    },
    /// `pg_sleep(float8)`: cancellable wait, modeled as the engine's void text.
    PgSleep,
    UuidV4,
    UuidV7,
    UuidExtractVersion,
    UuidExtractTimestamp,
    PgGetFunctionArgDefault,
    RangeConstructor(RangeRef),
    MultirangeConstructor(MultirangeRef),
    GenericMultirangeConstructor,
    IsEmpty,
    LowerInc,
    LowerInf,
    UpperInc,
    UpperInf,
    RangeContains,
    RangeContainedBy,
    RangeOverlaps,
    RangeAdjacent,
    RangeMinus,
    RangeMerge,
    MultirangePredicate,
    PgTableIsVisible,
    LoCreate,
    LoOpen,
    LoClose,
    LoRead,
    LoWrite,
    LoSeek,
    LoTell,
    LoFromBytea,
    LoImport,
    LoExport,
    LoGet,
    LoPut,
    LoTruncate,
    LoUnlink,
    NextVal,
    CurrVal,
    SetVal,
    PgNotify,
    RestoreRelationStats,
    ClearRelationStats,
    RestoreAttributeStats,
    ClearAttributeStats,
}

static UUID_SEQUENCE: AtomicU64 = AtomicU64::new(0);

fn uuid_v4() -> crabka_pgtypes::uuid::UuidBytes {
    let mut bytes = UUID_SEQUENCE
        .fetch_add(1, AtomicOrdering::Relaxed)
        .to_be_bytes()
        .repeat(2);
    bytes[6] = (bytes[6] & 0x0f) | 0x40;
    bytes[8] = (bytes[8] & 0x3f) | 0x80;
    crabka_pgtypes::uuid::UuidBytes(bytes.try_into().expect("16 UUID bytes"))
}

fn uuid_v7(timestamp: jiff::Timestamp) -> crabka_pgtypes::uuid::UuidBytes {
    let millis = timestamp.as_millisecond();
    let mut bytes = [0; 16];
    bytes[..6].copy_from_slice(&(millis as u64).to_be_bytes()[2..]);
    bytes[8..].copy_from_slice(
        &UUID_SEQUENCE
            .fetch_add(1, AtomicOrdering::Relaxed)
            .to_be_bytes(),
    );
    bytes[6] = 0x70;
    bytes[8] = (bytes[8] & 0x3f) | 0x80;
    crabka_pgtypes::uuid::UuidBytes(bytes)
}

fn uuid_bytes(value: &Datum) -> Result<crabka_pgtypes::uuid::UuidBytes, ExecError> {
    match value {
        Datum::Text(value) => Ok(crabka_pgtypes::uuid::UuidBytes::parse(value)?),
        other => Err(type_error("uuid", other)),
    }
}

fn uuid_timestamp(
    value: crabka_pgtypes::uuid::UuidBytes,
) -> Result<Option<jiff::Timestamp>, ExecError> {
    let bytes = value.0;
    if bytes[8] & 0xc0 != 0x80 {
        return Ok(None);
    }
    match bytes[6] >> 4 {
        1 => {
            let ticks = (u64::from(bytes[6] & 0x0f) << 56)
                | (u64::from(bytes[7]) << 48)
                | (u64::from(u16::from_be_bytes([bytes[4], bytes[5]])) << 32)
                | u64::from(u32::from_be_bytes([bytes[0], bytes[1], bytes[2], bytes[3]]));
            const UUID_EPOCH_TICKS: u64 = 122_192_928_000_000_000;
            let Some(unix_ticks) = ticks.checked_sub(UUID_EPOCH_TICKS) else {
                return Ok(None);
            };
            let micros = i64::try_from(unix_ticks / 10).map_err(|_| ExecError::FunctionError {
                sqlstate: "22008",
                message: "timestamp out of range".into(),
            })?;
            Ok(Some(jiff::Timestamp::from_microsecond(micros).map_err(
                |_| ExecError::FunctionError {
                    sqlstate: "22008",
                    message: "timestamp out of range".into(),
                },
            )?))
        }
        7 => {
            let mut raw = [0; 8];
            raw[2..].copy_from_slice(&bytes[..6]);
            Ok(Some(
                jiff::Timestamp::from_millisecond(i64::try_from(u64::from_be_bytes(raw)).map_err(
                    |_| ExecError::FunctionError {
                        sqlstate: "22008",
                        message: "timestamp out of range".into(),
                    },
                )?)
                .map_err(|_| ExecError::FunctionError {
                    sqlstate: "22008",
                    message: "timestamp out of range".into(),
                })?,
            ))
        }
        _ => Ok(None),
    }
}

/// Classify a lowercased function name. The lexer lowercases unquoted idents.
/// `None` means "not a known scalar function". The caller then tries the
/// aggregate path or reports an undefined function.
fn scalar_func(name: &str) -> Option<ScalarFunc> {
    let name = name.strip_prefix("pg_catalog.").unwrap_or(name);
    Some(match name {
        "length" | "char_length" | "character_length" => ScalarFunc::Length,
        "upper" => ScalarFunc::Upper,
        "lower" => ScalarFunc::Lower,
        "btrim" => ScalarFunc::Btrim,
        "ltrim" => ScalarFunc::Ltrim,
        "rtrim" => ScalarFunc::Rtrim,
        "substr" | "substring" => ScalarFunc::Substr,
        "overlay" => ScalarFunc::Overlay,
        "replace" => ScalarFunc::Replace,
        "concat" => ScalarFunc::Concat,
        "num_nonnulls" => ScalarFunc::NumNonNulls,
        "num_nulls" => ScalarFunc::NumNulls,
        "abs" => ScalarFunc::Abs,
        "mod" => ScalarFunc::Mod,
        "int2pl" => ScalarFunc::TypedAdd(ColumnType::Int2),
        "int4pl" => ScalarFunc::TypedAdd(ColumnType::Int4),
        "int8pl" => ScalarFunc::TypedAdd(ColumnType::Int8),
        "float4pl" => ScalarFunc::TypedAdd(ColumnType::Float4),
        "float8pl" => ScalarFunc::TypedAdd(ColumnType::Float8),
        "float8mi" => ScalarFunc::TypedSub(ColumnType::Float8),
        "int8inc" => ScalarFunc::Int8Inc,
        "int4_sum" => ScalarFunc::Int4Sum,
        "int4larger" => ScalarFunc::Int4Larger,
        "int4smaller" => ScalarFunc::Int4Smaller,
        "array_larger" => ScalarFunc::ArrayLarger,
        "enum_first" => ScalarFunc::EnumFirst,
        "enum_last" => ScalarFunc::EnumLast,
        "enum_range" => ScalarFunc::EnumRange,
        "int4_avg_accum" => ScalarFunc::Int4AvgAccum,
        "int8_avg" => ScalarFunc::Int8Avg,
        "float8_accum" => ScalarFunc::Float8Accum,
        "float8_avg" => ScalarFunc::Float8Avg,
        "booland_statefunc" => ScalarFunc::BoolState { and: true },
        "boolor_statefunc" => ScalarFunc::BoolState { and: false },
        "booleq" => ScalarFunc::BoolCompare { equal: true },
        "boolne" => ScalarFunc::BoolCompare { equal: false },
        "coalesce" => ScalarFunc::Coalesce,
        "nullif" => ScalarFunc::NullIf,
        "greatest" => ScalarFunc::Greatest,
        "least" => ScalarFunc::Least,
        "floor" => ScalarFunc::Floor,
        "ceil" | "ceiling" => ScalarFunc::Ceil,
        "round" => ScalarFunc::Round,
        "trunc" => ScalarFunc::Trunc,
        "sign" => ScalarFunc::Sign,
        "sqrt" => ScalarFunc::Sqrt,
        "power" | "pow" => ScalarFunc::Power,
        "exp" => ScalarFunc::Exp,
        "ln" => ScalarFunc::Ln,
        "log" => ScalarFunc::Log,
        "pi" => ScalarFunc::Pi,
        "float4send" => ScalarFunc::Float4Send,
        "lpad" => ScalarFunc::Lpad,
        "rpad" => ScalarFunc::Rpad,
        "left" => ScalarFunc::Left,
        "right" => ScalarFunc::Right,
        "repeat" => ScalarFunc::Repeat,
        "reverse" => ScalarFunc::Reverse,
        "strpos" | "position" => ScalarFunc::Strpos,
        "initcap" => ScalarFunc::Initcap,
        "ascii" => ScalarFunc::Ascii,
        "chr" => ScalarFunc::Chr,
        "current_setting" => ScalarFunc::CurrentSetting,
        "set_config" => ScalarFunc::SetConfig,
        "pg_advisory_lock" => ScalarFunc::AdvisoryLock {
            try_only: false,
            transactional: false,
            shared: false,
        },
        "pg_advisory_lock_shared" => ScalarFunc::AdvisoryLock {
            try_only: false,
            transactional: false,
            shared: true,
        },
        "pg_advisory_xact_lock" => ScalarFunc::AdvisoryLock {
            try_only: false,
            transactional: true,
            shared: false,
        },
        "pg_advisory_xact_lock_shared" => ScalarFunc::AdvisoryLock {
            try_only: false,
            transactional: true,
            shared: true,
        },
        "pg_try_advisory_lock" => ScalarFunc::AdvisoryLock {
            try_only: true,
            transactional: false,
            shared: false,
        },
        "pg_try_advisory_lock_shared" => ScalarFunc::AdvisoryLock {
            try_only: true,
            transactional: false,
            shared: true,
        },
        "pg_try_advisory_xact_lock" => ScalarFunc::AdvisoryLock {
            try_only: true,
            transactional: true,
            shared: false,
        },
        "pg_try_advisory_xact_lock_shared" => ScalarFunc::AdvisoryLock {
            try_only: true,
            transactional: true,
            shared: true,
        },
        "pg_advisory_unlock" => ScalarFunc::AdvisoryUnlock { shared: false },
        "pg_advisory_unlock_shared" => ScalarFunc::AdvisoryUnlock { shared: true },
        "pg_advisory_unlock_all" => ScalarFunc::AdvisoryUnlockAll,
        "current_database" => ScalarFunc::CurrentDatabase,
        "getdatabaseencoding" => ScalarFunc::GetDatabaseEncoding,
        "unicode_version" => ScalarFunc::UnicodeVersion,
        "unicode_assigned" => ScalarFunc::UnicodeAssigned,
        "current_schema" => ScalarFunc::CurrentSchema,
        "current_user" => ScalarFunc::CurrentUser,
        "session_user" => ScalarFunc::SessionUser,
        "version" => ScalarFunc::Version,
        "format_type" => ScalarFunc::FormatType,
        "pg_typeof" => ScalarFunc::PgTypeof,
        "pg_input_is_valid" => ScalarFunc::PgInputIsValid,
        "binary_coercible" => ScalarFunc::BinaryCoercible,
        "pg_numa_available" => ScalarFunc::PgNumaAvailable,
        "interval_hash" => ScalarFunc::TemporalHash {
            ty: ColumnType::Interval,
            extended: false,
        },
        "interval_hash_extended" => ScalarFunc::TemporalHash {
            ty: ColumnType::Interval,
            extended: true,
        },
        "time_hash" => ScalarFunc::TemporalHash {
            ty: ColumnType::Time,
            extended: false,
        },
        "time_hash_extended" => ScalarFunc::TemporalHash {
            ty: ColumnType::Time,
            extended: true,
        },
        "timetz_hash" => ScalarFunc::TemporalHash {
            ty: ColumnType::Timetz,
            extended: false,
        },
        "timetz_hash_extended" => ScalarFunc::TemporalHash {
            ty: ColumnType::Timetz,
            extended: true,
        },
        "timestamp_hash" => ScalarFunc::TemporalHash {
            ty: ColumnType::Timestamp,
            extended: false,
        },
        "timestamp_hash_extended" => ScalarFunc::TemporalHash {
            ty: ColumnType::Timestamp,
            extended: true,
        },
        "timestamptz_hash" => ScalarFunc::TemporalHash {
            ty: ColumnType::Timestamptz,
            extended: false,
        },
        "timestamptz_hash_extended" => ScalarFunc::TemporalHash {
            ty: ColumnType::Timestamptz,
            extended: true,
        },
        "hashchar" => ScalarFunc::IntegerHash {
            ty: ColumnType::InternalChar,
            extended: false,
        },
        "hashcharextended" => ScalarFunc::IntegerHash {
            ty: ColumnType::InternalChar,
            extended: true,
        },
        "hashint2" => ScalarFunc::IntegerHash {
            ty: ColumnType::Int2,
            extended: false,
        },
        "hashint2extended" => ScalarFunc::IntegerHash {
            ty: ColumnType::Int2,
            extended: true,
        },
        "hashint4" => ScalarFunc::IntegerHash {
            ty: ColumnType::Int4,
            extended: false,
        },
        "hashint4extended" => ScalarFunc::IntegerHash {
            ty: ColumnType::Int4,
            extended: true,
        },
        "hashint8" => ScalarFunc::IntegerHash {
            ty: ColumnType::Int8,
            extended: false,
        },
        "hashint8extended" => ScalarFunc::IntegerHash {
            ty: ColumnType::Int8,
            extended: true,
        },
        "hashoid" => ScalarFunc::IntegerHash {
            ty: ColumnType::Oid,
            extended: false,
        },
        "hashoidextended" => ScalarFunc::IntegerHash {
            ty: ColumnType::Oid,
            extended: true,
        },
        "hashfloat4" => ScalarFunc::FloatHash {
            ty: ColumnType::Float4,
            extended: false,
        },
        "hashfloat4extended" => ScalarFunc::FloatHash {
            ty: ColumnType::Float4,
            extended: true,
        },
        "hashfloat8" => ScalarFunc::FloatHash {
            ty: ColumnType::Float8,
            extended: false,
        },
        "hashfloat8extended" => ScalarFunc::FloatHash {
            ty: ColumnType::Float8,
            extended: true,
        },
        "hashname" => ScalarFunc::TextHash {
            ty: ColumnType::Name,
            extended: false,
        },
        "hashnameextended" => ScalarFunc::TextHash {
            ty: ColumnType::Name,
            extended: true,
        },
        "hashtext" => ScalarFunc::TextHash {
            ty: ColumnType::Text,
            extended: false,
        },
        "hashtextextended" => ScalarFunc::TextHash {
            ty: ColumnType::Text,
            extended: true,
        },
        "hashoidvector" => ScalarFunc::OidVectorHash { extended: false },
        "hashoidvectorextended" => ScalarFunc::OidVectorHash { extended: true },
        "hash_array" => ScalarFunc::ArrayHash { extended: false },
        "hash_array_extended" => ScalarFunc::ArrayHash { extended: true },
        "hashbpchar" => ScalarFunc::BpcharHash { extended: false },
        "hashbpcharextended" => ScalarFunc::BpcharHash { extended: true },
        "uuid_hash" => ScalarFunc::UuidHash { extended: false },
        "uuid_hash_extended" => ScalarFunc::UuidHash { extended: true },
        "pg_lsn_hash" => ScalarFunc::PgLsnHash { extended: false },
        "pg_lsn_hash_extended" => ScalarFunc::PgLsnHash { extended: true },
        "hashenum" => ScalarFunc::EnumHash { extended: false },
        "hashenumextended" => ScalarFunc::EnumHash { extended: true },
        "hash_range" => ScalarFunc::RangeHash { extended: false },
        "hash_range_extended" => ScalarFunc::RangeHash { extended: true },
        "hash_multirange" => ScalarFunc::MultirangeHash { extended: false },
        "hash_multirange_extended" => ScalarFunc::MultirangeHash { extended: true },
        "pg_sleep" => ScalarFunc::PgSleep,
        "gen_random_uuid" | "uuid_generate_v4" | "uuidv4" => ScalarFunc::UuidV4,
        "uuidv7" => ScalarFunc::UuidV7,
        "uuid_extract_version" => ScalarFunc::UuidExtractVersion,
        "uuid_extract_timestamp" => ScalarFunc::UuidExtractTimestamp,
        "pg_get_function_arg_default" => ScalarFunc::PgGetFunctionArgDefault,
        "isempty" => ScalarFunc::IsEmpty,
        "lower_inc" => ScalarFunc::LowerInc,
        "lower_inf" => ScalarFunc::LowerInf,
        "upper_inc" => ScalarFunc::UpperInc,
        "upper_inf" => ScalarFunc::UpperInf,
        "range_contains" => ScalarFunc::RangeContains,
        "range_contained_by" => ScalarFunc::RangeContainedBy,
        "range_overlaps" => ScalarFunc::RangeOverlaps,
        "range_adjacent" => ScalarFunc::RangeAdjacent,
        "range_minus" => ScalarFunc::RangeMinus,
        "range_merge" => ScalarFunc::RangeMerge,
        "range_overlaps_multirange"
        | "multirange_overlaps_range"
        | "multirange_overlaps_multirange"
        | "multirange_contains_elem"
        | "multirange_contains_range"
        | "multirange_contains_multirange"
        | "elem_contained_by_multirange"
        | "range_contained_by_multirange"
        | "multirange_contained_by_multirange" => ScalarFunc::MultirangePredicate,
        "pg_table_is_visible" => ScalarFunc::PgTableIsVisible,
        "lo_create" | "lo_creat" => ScalarFunc::LoCreate,
        "lo_open" => ScalarFunc::LoOpen,
        "lo_close" => ScalarFunc::LoClose,
        "loread" => ScalarFunc::LoRead,
        "lowrite" => ScalarFunc::LoWrite,
        "lo_lseek" | "lo_lseek64" => ScalarFunc::LoSeek,
        "lo_tell" | "lo_tell64" => ScalarFunc::LoTell,
        "lo_from_bytea" => ScalarFunc::LoFromBytea,
        "lo_import" => ScalarFunc::LoImport,
        "lo_export" => ScalarFunc::LoExport,
        "lo_get" => ScalarFunc::LoGet,
        "lo_put" => ScalarFunc::LoPut,
        "lo_truncate" | "lo_truncate64" => ScalarFunc::LoTruncate,
        "lo_unlink" => ScalarFunc::LoUnlink,
        "nextval" => ScalarFunc::NextVal,
        "currval" => ScalarFunc::CurrVal,
        "setval" => ScalarFunc::SetVal,
        "pg_notify" => ScalarFunc::PgNotify,
        "pg_restore_relation_stats" => ScalarFunc::RestoreRelationStats,
        "pg_clear_relation_stats" => ScalarFunc::ClearRelationStats,
        "pg_restore_attribute_stats" => ScalarFunc::RestoreAttributeStats,
        "pg_clear_attribute_stats" => ScalarFunc::ClearAttributeStats,
        "multirange" => ScalarFunc::GenericMultirangeConstructor,
        _ => match ColumnType::from_sql_name(name) {
            Some(ColumnType::Range(range)) => ScalarFunc::RangeConstructor(range),
            Some(ColumnType::Multirange(multirange)) => {
                ScalarFunc::MultirangeConstructor(multirange)
            }
            _ => return None,
        },
    })
}

/// Is `name` a known scalar function? (The dispatch point in `eval`/`infer_type`.)
///
/// The scalar surface spans four modules: this one plus `math_fn`, `string_fn`
/// and `regexp_fn`. No single file then owns hundreds of functions. They share
/// one dispatch point, so `eval`, `infer_type` and `agg::is_wrapping_scalar_func`
/// each need only ask this question once.
pub(crate) fn is_scalar(name: &str) -> bool {
    scalar_func(name).is_some()
        || crate::math_fn::is_math_func(name)
        || crate::string_fn::is_string_func(name)
        || crate::regexp_fn::is_regexp_func(name)
        || crate::text_search_fn::is_text_search_func(name)
        || crate::network_fn::is_network_func(name)
        || crate::xml_fn::is_xml_func(name)
        || crate::bit_fn::is_bit_func(name)
        || crate::money_fn::is_money_func(name)
        || crate::sysid_fn::is_sysid_func(name)
        || crate::snapshot_fn::is_snapshot_func(name)
        || crate::geometry_fn::is_geometry_func(name)
        || constructor_cast_type(name).is_some()
}

/// Every one-argument function `PostgreSQL` names after a type, with the
/// argument types it declares — `("float8", ["int4", "int8", …])` for the
/// `float8(bigint)` that `SELECT q1, float8(q1) FROM int8_tbl` calls.
///
/// A cast function's `pg_proc.proname` is its *target type's* `pg_type.typname`,
/// so the call means exactly `q1::float8`. `PostgreSQL` documents the spelling
/// as obsolescent, but its own regression suite writes it, so the engine has to
/// resolve it.
///
/// Enumerated from the pinned donor `REL_18_4`, not from what a test happened to
/// name: every `src/include/catalog/pg_proc.dat` row whose `proname` is a
/// `pg_type.dat` `typname` and whose `proargtypes` has exactly one non-array
/// entry. A donor bump re-runs that filter over the new files.
///
/// Taking the arity from `pg_proc` and not from `pg_cast` is what keeps the
/// length-coercion casts out. `pg_cast` has a `numeric` → `numeric` row, but its
/// function is `numeric(numeric, int4)`; a one-argument `numeric(x::numeric)` is
/// 42883 on `PostgreSQL` and has to stay 42883 here.
///
/// Sorted by name, so the lookup below can binary-search and a duplicate is
/// visible on sight.
const CONSTRUCTOR_CASTS: &[(&str, &[&str])] = &[
    ("bool", &["int4", "jsonb"]),
    ("box", &["circle", "point", "polygon"]),
    ("bpchar", &["char", "name"]),
    ("bytea", &["int2", "int4", "int8"]),
    ("char", &["int4", "text"]),
    ("cidr", &["inet"]),
    ("circle", &["box", "polygon"]),
    ("date", &["timestamp", "timestamptz"]),
    ("datemultirange", &["daterange"]),
    (
        "float4",
        &["float8", "int2", "int4", "int8", "jsonb", "numeric"],
    ),
    (
        "float8",
        &["float4", "int2", "int4", "int8", "jsonb", "numeric"],
    ),
    (
        "int2",
        &[
            "bytea", "float4", "float8", "int4", "int8", "jsonb", "numeric",
        ],
    ),
    (
        "int4",
        &[
            "bit", "bool", "bytea", "char", "float4", "float8", "int2", "int8", "jsonb", "numeric",
        ],
    ),
    ("int4multirange", &["int4range"]),
    (
        "int8",
        &[
            "bit", "bytea", "float4", "float8", "int2", "int4", "jsonb", "numeric", "oid",
        ],
    ),
    ("int8multirange", &["int8range"]),
    ("interval", &["time"]),
    ("lseg", &["box"]),
    ("macaddr", &["macaddr8"]),
    ("macaddr8", &["macaddr"]),
    ("money", &["int4", "int8", "numeric"]),
    ("name", &["bpchar", "text", "varchar"]),
    (
        "numeric",
        &["float4", "float8", "int2", "int4", "int8", "jsonb", "money"],
    ),
    ("nummultirange", &["numrange"]),
    ("oid", &["int8"]),
    ("path", &["polygon"]),
    ("pg_lsn", &["numeric"]),
    ("point", &["box", "circle", "lseg", "polygon"]),
    ("polygon", &["box", "circle", "path"]),
    ("regclass", &["text"]),
    ("text", &["bool", "bpchar", "char", "inet", "name", "xml"]),
    ("time", &["interval", "timestamp", "timestamptz", "timetz"]),
    ("timestamp", &["date", "timestamptz"]),
    ("timestamptz", &["date", "timestamp"]),
    ("timetz", &["time", "timestamptz"]),
    ("tsmultirange", &["tsrange"]),
    ("tstzmultirange", &["tstzrange"]),
    ("varchar", &["name"]),
    ("xid", &["xid8"]),
    ("xml", &["text"]),
];

/// The Unicode release `unicode_version()` reports, as (major, minor).
///
/// `PostgreSQL` generates every Unicode table it has in one run, so it has one
/// version to report. Crabka's come from two crates on separate release
/// schedules — `unicode-normalization` for UAX #15 and
/// `unicode-general-category` for `unicode_assigned` — so the *lower* of the
/// two is the release at which every Unicode answer this engine gives is
/// current. Reporting the higher would claim coverage the other table has not
/// caught up to.
fn unicode_version() -> (u64, u64) {
    let (norm_major, norm_minor, _) = unicode_normalization::UNICODE_VERSION;
    let normalization = (u64::from(norm_major), u64::from(norm_minor));
    let (cat_major, cat_minor, _) = unicode_general_category::UNICODE_VERSION;
    normalization.min((cat_major, cat_minor))
}

/// Resolve one of the enumeration's `typname`s.
///
/// A `pg_proc` name is a `typname`, which is what a **quoted** type name
/// resolves through: `PostgreSQL` spells the one-byte type's cast function
/// `char`, the same identifier `"char"` names, and never `character(1)`. So the
/// quoted table is consulted first, exactly as the parser does for `'a'::"char"`.
fn type_of_typname(name: &str) -> Option<ColumnType> {
    ColumnType::from_quoted_builtin_sql_name(name)
        .or_else(|| ColumnType::from_builtin_sql_name(name))
}

/// The type a constructor-style cast call converts to, or `None` when `name` is
/// no such function.
fn constructor_cast_type(name: &str) -> Option<ColumnType> {
    CONSTRUCTOR_CASTS
        .binary_search_by_key(&name, |(target, _)| *target)
        .ok()?;
    type_of_typname(name)
}

/// The `Expr::Cast` a one-argument constructor-style call is, or `None` when the
/// call is not one.
///
/// Rewriting to the cast node rather than converting here is what keeps the two
/// spellings from drifting: `float8(x)` then goes through the same operand
/// evaluation, the same `CREATE CAST` lookup and the same conversion table as
/// `x::float8`, and gains whatever either of those gains.
///
/// The operand's type must be one the function declares. Without that check
/// `xid('1'::xid)` would answer a value where `PostgreSQL` has only
/// `xid(xid8)` and reports 42883, and `text(row('Jim','Beam'))` would render the
/// composite the explicit cast renders. An operand still typed `unknown` is
/// allowed through, because `PostgreSQL` resolves those against the candidate
/// list rather than rejecting them.
///
/// Crabka holds `name` values in `text`, so the two collapse into one candidate
/// here and `varchar(x::text)` resolves where `PostgreSQL` wants a `name`. That
/// is the existing type mapping showing through, not a rule of this table.
fn constructor_cast(fc: &FuncCall, scope: Option<&Scope>) -> Option<Expr> {
    let index = CONSTRUCTOR_CASTS
        .binary_search_by_key(&fc.name.as_str(), |(target, _)| *target)
        .ok()?;
    let ty = type_of_typname(fc.name.as_str())?;
    let FuncArgs::Exprs(args) = &fc.args else {
        return None;
    };
    let [arg] = args.as_slice() else {
        return None;
    };
    let declared = CONSTRUCTOR_CASTS[index].1;
    let given = scope.and_then(|scope| crate::eval::infer_type(arg, scope).ok());
    if let Some(given) = given
        && !declared
            .iter()
            .filter_map(|name| type_of_typname(name))
            .any(|declared| declared.oid() == given.oid())
    {
        return None;
    }
    Some(Expr::Cast {
        expr: Box::new(arg.clone()),
        ty,
    })
}

/// The call a bare, unparenthesised `name` denotes, when `PostgreSQL` reserves
/// that name for a niladic function and does not leave it available as a
/// column reference.
///
/// `SELECT current_schema` is a function call on 18.4, because `CURRENT_SCHEMA`
/// is a keyword in its grammar and the name can never reach an identifier. The
/// lexer here hands it over as an ordinary identifier instead. The other
/// no-paren spellings (`current_date`, `session_user`, `localtimestamp`, …)
/// are already calls when they arrive, so this covers only the ones that are
/// not.
pub(crate) fn niladic_keyword_call(name: &str) -> Option<FuncCall> {
    matches!(name, "current_schema").then(|| FuncCall {
        sql_syntax: false,
        name: name.to_string(),
        distinct: false,
        args: FuncArgs::Exprs(Vec::new()),
        order_by: Vec::new(),
        within_group: false,
        filter: None,
    })
}

pub(crate) fn undefined_function(name: &str) -> ExecError {
    // `merge_action()` exists, but only inside a MERGE's RETURNING list — the
    // executor rewrites it to a binding there and never reaches this point.
    // Everywhere else PostgreSQL reports the misuse, not a missing function.
    if name.eq_ignore_ascii_case("merge_action") {
        return ExecError::Syntax(
            "MERGE_ACTION() can only be used in the RETURNING list of a MERGE command".into(),
        );
    }
    ExecError::UndefinedFunction(format!("function {name}(...) does not exist"))
}

/// `DISTINCT`/`ALL` has a meaning only for aggregates (PostgreSQL 42809). The
/// parser discards `ALL`, so only an explicit `DISTINCT` reaches here.
fn distinct_not_aggregate(name: &str) -> ExecError {
    ExecError::WrongObjectType(format!(
        "DISTINCT specified, but {name} is not an aggregate function"
    ))
}

/// A sort inside the parentheses orders the rows an aggregate accumulates. A
/// scalar function sees one row at a time and has nothing to order, so
/// `PostgreSQL` reports 42809 for it — the same class as a misplaced `DISTINCT`.
fn order_by_not_aggregate(name: &str) -> ExecError {
    ExecError::WrongObjectType(format!(
        "ORDER BY specified, but {name} is not an aggregate function"
    ))
}

/// The positional argument list of a scalar call. `f(*)` is never valid for a
/// scalar function (only `count(*)` is), so it is an undefined-function error.
fn exprs_of(fc: &FuncCall) -> Result<&[Expr], ExecError> {
    match &fc.args {
        FuncArgs::Exprs(v) => Ok(v),
        FuncArgs::Star | FuncArgs::Named { .. } | FuncArgs::Variadic { .. } => {
            Err(undefined_function(&fc.name))
        }
    }
}

/// Reject the aggregate-only modifiers (42809) and return the call's argument
/// list. Shared front-door check for both `scalar_result_type` and
/// `eval_scalar`.
pub(crate) fn checked_args(fc: &FuncCall) -> Result<&[Expr], ExecError> {
    check_scalar_modifiers(fc)?;
    exprs_of(fc)
}

pub(crate) fn check_scalar_modifiers(fc: &FuncCall) -> Result<(), ExecError> {
    if fc.distinct {
        return Err(distinct_not_aggregate(&fc.name));
    }
    if !fc.order_by.is_empty() {
        return Err(order_by_not_aggregate(&fc.name));
    }
    Ok(())
}

/// The values passed through SQL's `VARIADIC array_expression` syntax.
pub(crate) enum ExpandedVariadicArgs {
    Values(Vec<Datum>),
    NullArray(Vec<Datum>),
}

pub(crate) fn expand_variadic_args(
    positional: &[Expr],
    array: &Expr,
    mut eval_child: impl FnMut(&Expr) -> Result<Datum, ExecError>,
) -> Result<ExpandedVariadicArgs, ExecError> {
    let mut values = positional
        .iter()
        .map(&mut eval_child)
        .collect::<Result<Vec<_>, _>>()?;
    match eval_child(array)? {
        Datum::Array(array) => {
            values.extend(array.elems);
            Ok(ExpandedVariadicArgs::Values(values))
        }
        Datum::Null => Ok(ExpandedVariadicArgs::NullArray(values)),
        _ => Err(ExecError::TypeMismatch(
            "VARIADIC argument must be an array".into(),
        )),
    }
}

/// Statically infer a scalar call's result type, for RowDescription.
///
/// This validates the name and the arity. It also validates the argument types
/// where the result type depends on them, or where the function is strictly
/// typed. A bad name, arity, or argument type is 42883.
///
/// NB: this runs for PROJECTED expressions, through `resolve_projection`. A
/// scalar function that appears only in `WHERE`/`HAVING`/`ORDER BY` runs with no
/// separate type-resolution pass, which is a pre-existing trait of the engine
/// and is also true of arithmetic. So an argument-type misuse THERE surfaces at
/// runtime as 42804, not here as 42883. This per-clause difference is
/// documented.
pub(crate) fn scalar_result_type(fc: &FuncCall, scope: &Scope) -> Result<ColumnType, ExecError> {
    match builtin_scalar_result_type(fc, scope) {
        Err(ExecError::UndefinedFunction(message)) => constructor_cast(fc, Some(scope))
            .map_or(Err(ExecError::UndefinedFunction(message)), |cast| {
                crate::eval::infer_type(&cast, scope)
            }),
        resolved => resolved,
    }
}

/// [`scalar_result_type`] without the constructor-cast last resort, so that
/// `text(inet)` keeps `network_fn`'s meaning and only a 42883 there falls
/// through to the cast.
fn builtin_scalar_result_type(fc: &FuncCall, scope: &Scope) -> Result<ColumnType, ExecError> {
    if crate::text_search_fn::is_text_search_func(&fc.name) {
        return crate::text_search_fn::text_search_result_type(fc, scope);
    }
    if crate::network_fn::is_network_func(&fc.name) {
        return crate::network_fn::network_func_result_type(fc, scope);
    }
    if crate::xml_fn::is_xml_func(&fc.name) {
        return crate::xml_fn::xml_func_result_type(fc, scope);
    }
    if crate::bit_fn::is_bit_func(&fc.name) {
        return crate::bit_fn::bit_func_result_type(fc, scope);
    }
    if crate::money_fn::is_money_func(&fc.name) {
        return crate::money_fn::money_func_result_type(fc, scope);
    }
    if crate::sysid_fn::is_sysid_func(&fc.name) {
        return crate::sysid_fn::sysid_func_result_type(fc, scope);
    }
    if crate::snapshot_fn::is_snapshot_func(&fc.name) {
        return crate::snapshot_fn::snapshot_func_result_type(fc, scope);
    }
    if crate::math_fn::is_math_func(&fc.name) {
        return crate::math_fn::math_func_result_type(fc, scope);
    }
    if crate::string_fn::is_string_func(&fc.name) {
        return crate::string_fn::string_func_result_type(fc, scope);
    }
    if crate::regexp_fn::is_regexp_func(&fc.name) {
        return crate::regexp_fn::regexp_func_result_type(fc, scope);
    }
    // Last of the family modules, so every name the older families already own
    // keeps its meaning; the geometric surface adds only names of its own.
    if crate::geometry_fn::is_geometry_func(&fc.name) {
        return crate::geometry_fn::geometry_func_result_type(fc, scope);
    }
    let f = scalar_func(&fc.name).ok_or_else(|| undefined_function(&fc.name))?;
    if matches!(
        f,
        ScalarFunc::Concat | ScalarFunc::NumNonNulls | ScalarFunc::NumNulls
    ) && let FuncArgs::Variadic { .. } = &fc.args
    {
        check_scalar_modifiers(fc)?;
        return Ok(match f {
            ScalarFunc::Concat => ColumnType::Text,
            ScalarFunc::NumNonNulls | ScalarFunc::NumNulls => ColumnType::Int4,
            _ => unreachable!(),
        });
    }
    let args = checked_args(fc)?;
    let n = args.len();
    match f {
        ScalarFunc::Length => {
            require_arity(fc, n == 1)?;
            let ty = crate::eval::infer_type(&args[0], scope)?;
            // `length(lseg)` and `length(path)` are the geometric overloads and
            // return `float8`. Only the `length` spelling has them —
            // `char_length`/`character_length` are `text`-only in PostgreSQL.
            if fc.name == "length"
                && let Some(result) = crate::geometry_fn::geometric_length_type(ty)
            {
                return Ok(result);
            }
            match ty {
                ColumnType::TsVector => {}
                ColumnType::Bytea if fc.name == "length" => {}
                // `length(bit)` is `bitlength` — the bit count, not a
                // character count.
                ty if crate::bit_fn::is_bit_type(ty) => {}
                _ => require_text(&args[0], scope).map_err(|error| match error {
                    ExecError::UndefinedFunction(_) => {
                        undefined_function_spelled(&fc.name, args, scope)
                    }
                    other => other,
                })?,
            }
            Ok(ColumnType::Int4)
        }
        ScalarFunc::Upper | ScalarFunc::Lower => {
            require_arity(fc, n == 1)?;
            let ty = crate::eval::infer_type(&args[0], scope)?;
            match ty {
                ColumnType::Range(range) => Ok(*range.subtype),
                ColumnType::Multirange(multirange) => Ok(*multirange.range.subtype),
                _ => {
                    require_text(&args[0], scope)?;
                    Ok(ColumnType::Text)
                }
            }
        }
        ScalarFunc::Btrim | ScalarFunc::Ltrim | ScalarFunc::Rtrim => {
            require_arity(fc, n == 1 || n == 2)?;
            // The `bytea` trims are two-argument only: PostgreSQL declares no
            // default byte set, because "whitespace" is a property of text.
            if is_bytea_subject(&args[0], scope)? {
                if n != 2 {
                    return Err(undefined_function_spelled(&fc.name, args, scope));
                }
                require_bytea(&args[1], scope)?;
                return Ok(ColumnType::Bytea);
            }
            for a in args {
                require_text(a, scope)?;
            }
            Ok(ColumnType::Text)
        }
        ScalarFunc::Substr => {
            require_arity(fc, n == 2 || n == 3)?;
            // `substring(bytea, int [, int])` is byte-indexed and has no
            // pattern form, so its count arguments are always integers.
            if is_bytea_subject(&args[0], scope)? {
                for a in &args[1..] {
                    require_int(a, scope)?;
                }
                return Ok(ColumnType::Bytea);
            }
            // `substring(bit, int [, int])` is a distinct function returning
            // `bit`; it has no pattern form, so the count arguments are always
            // integers.
            if crate::bit_fn::is_bit_type(crate::eval::infer_type(&args[0], scope)?) {
                for a in &args[1..] {
                    require_int(a, scope)?;
                }
                return Ok(ColumnType::Bit(None));
            }
            require_text(&args[0], scope)?;
            // Two shapes share the name, and PostgreSQL tells them apart by the
            // second argument's type: `substring(text, int [, int])` takes a
            // position, `substring(text, text [, text])` takes a pattern.
            if crate::eval::infer_type(&args[1], scope)?.is_string() {
                for a in &args[1..] {
                    require_text(a, scope)?;
                }
            } else {
                for a in &args[1..] {
                    require_int(a, scope)?;
                }
            }
            Ok(ColumnType::Text)
        }
        // `overlay(string, replacement, start [, count])` — the count defaults to
        // the replacement's own length, so the default replaces exactly as many
        // characters as it inserts.
        ScalarFunc::Overlay => {
            require_arity(fc, n == 3 || n == 4)?;
            if crate::bit_fn::is_bit_type(crate::eval::infer_type(&args[0], scope)?) {
                for a in &args[2..] {
                    require_int(a, scope)?;
                }
                return Ok(ColumnType::Bit(None));
            }
            if is_bytea_subject(&args[0], scope)? {
                require_bytea(&args[1], scope)?;
                for a in &args[2..] {
                    require_int(a, scope)?;
                }
                return Ok(ColumnType::Bytea);
            }
            require_text(&args[0], scope)?;
            require_text(&args[1], scope)?;
            for a in &args[2..] {
                require_int(a, scope)?;
            }
            Ok(ColumnType::Text)
        }
        ScalarFunc::Replace => {
            require_arity(fc, n == 3)?;
            for a in args {
                require_text(a, scope)?;
            }
            Ok(ColumnType::Text)
        }
        // `concat` is VARIADIC "any": any number of arguments of any type, but
        // at least one — PostgreSQL has no zero-argument candidate, so a bare
        // `concat()` is 42883.
        ScalarFunc::Concat => {
            if args.is_empty() {
                return Err(undefined_function_spelled(&fc.name, args, scope));
            }
            Ok(ColumnType::Text)
        }
        ScalarFunc::Abs => {
            require_arity(fc, n == 1)?;
            // `abs` is declared over the numeric family only; naming the
            // argument's type is what PostgreSQL reports for `abs(money)`.
            if matches!(
                crate::eval::infer_type(&args[0], scope)?,
                ColumnType::Money | ColumnType::Bit(_) | ColumnType::VarBit(_)
            ) {
                return Err(undefined_function_spelled(&fc.name, args, scope));
            }
            // abs preserves the numeric type (int width, or SP30's float8).
            require_numeric(&args[0], scope)
        }
        ScalarFunc::Mod => {
            require_arity(fc, n == 2)?;
            // `mod` has no float8 candidate, so nothing prefers one overload.
            if args.iter().all(is_unknown_literal) {
                return Err(ambiguous_function(&fc.name, n));
            }
            // SP32: mod takes int OR numeric operands (PostgreSQL has no float8 mod);
            // a numeric operand makes the result numeric, else the int promotion.
            let lt = require_int_or_numeric(&args[0], scope)?;
            let rt = require_int_or_numeric(&args[1], scope)?;
            if lt.is_numeric() || rt.is_numeric() {
                Ok(ColumnType::Numeric(None))
            } else {
                Ok(promote(lt, rt))
            }
        }
        ScalarFunc::TypedAdd(ty) => {
            require_arity(fc, n == 2)?;
            for arg in args {
                if crate::eval::infer_type(arg, scope)? != ty {
                    return Err(undefined_function_spelled(&fc.name, args, scope));
                }
            }
            Ok(ty)
        }
        ScalarFunc::TypedSub(ty) => {
            require_arity(fc, n == 2)?;
            for arg in args {
                if crate::eval::infer_type(arg, scope)? != ty {
                    return Err(undefined_function_spelled(&fc.name, args, scope));
                }
            }
            Ok(ty)
        }
        ScalarFunc::Int8Inc => {
            require_arity(fc, n == 1)?;
            if crate::eval::infer_type(&args[0], scope)? == ColumnType::Int8 {
                Ok(ColumnType::Int8)
            } else {
                Err(undefined_function_spelled(&fc.name, args, scope))
            }
        }
        ScalarFunc::Int4Sum => {
            require_arity(fc, n == 2)?;
            if crate::eval::infer_type(&args[0], scope)? == ColumnType::Int8
                && crate::eval::infer_type(&args[1], scope)? == ColumnType::Int4
            {
                Ok(ColumnType::Int8)
            } else {
                Err(undefined_function_spelled(&fc.name, args, scope))
            }
        }
        ScalarFunc::Int4Larger | ScalarFunc::Int4Smaller => {
            require_arity(fc, n == 2)?;
            if args
                .iter()
                .all(|arg| crate::eval::infer_type(arg, scope) == Ok(ColumnType::Int4))
            {
                Ok(ColumnType::Int4)
            } else {
                Err(undefined_function_spelled(&fc.name, args, scope))
            }
        }
        ScalarFunc::ArrayLarger => {
            require_arity(fc, n == 2)?;
            let left = crate::eval::infer_type(&args[0], scope)?;
            let right = crate::eval::infer_type(&args[1], scope)?;
            if left == right && matches!(left, ColumnType::Array(_)) {
                Ok(left)
            } else {
                Err(undefined_function_spelled(&fc.name, args, scope))
            }
        }
        ScalarFunc::EnumFirst | ScalarFunc::EnumLast => {
            require_arity(fc, n == 1)?;
            Ok(ColumnType::Enum(enum_arg_type(fc, args, scope)?))
        }
        ScalarFunc::EnumRange => {
            require_arity(fc, n == 1 || n == 2)?;
            Ok(ColumnType::Array(ElemType::User(enum_arg_type(
                fc, args, scope,
            )?)))
        }
        ScalarFunc::Int4AvgAccum => {
            require_arity(fc, n == 2)?;
            if crate::eval::infer_type(&args[0], scope)? == ColumnType::Array(ElemType::Int8)
                && crate::eval::infer_type(&args[1], scope)? == ColumnType::Int4
            {
                Ok(ColumnType::Array(ElemType::Int8))
            } else {
                Err(undefined_function_spelled(&fc.name, args, scope))
            }
        }
        ScalarFunc::Int8Avg => {
            require_arity(fc, n == 1)?;
            if crate::eval::infer_type(&args[0], scope)? == ColumnType::Array(ElemType::Int8) {
                Ok(ColumnType::Numeric(None))
            } else {
                Err(undefined_function_spelled(&fc.name, args, scope))
            }
        }
        ScalarFunc::Float8Accum => {
            require_arity(fc, n == 2)?;
            if crate::eval::infer_type(&args[0], scope)? == ColumnType::Array(ElemType::Float8)
                && crate::eval::infer_type(&args[1], scope)? == ColumnType::Float8
            {
                Ok(ColumnType::Array(ElemType::Float8))
            } else {
                Err(undefined_function_spelled(&fc.name, args, scope))
            }
        }
        ScalarFunc::Float8Avg => {
            require_arity(fc, n == 1)?;
            if crate::eval::infer_type(&args[0], scope)? == ColumnType::Array(ElemType::Float8) {
                Ok(ColumnType::Float8)
            } else {
                Err(undefined_function_spelled(&fc.name, args, scope))
            }
        }
        ScalarFunc::BoolState { .. } => {
            require_arity(fc, n == 2)?;
            for arg in args {
                if !is_unknown_literal(arg) {
                    require_bool(arg, scope)?;
                }
            }
            Ok(ColumnType::Bool)
        }
        ScalarFunc::BoolCompare { .. } => {
            require_arity(fc, n == 2)?;
            for arg in args {
                if !is_unknown_literal(arg) {
                    require_bool(arg, scope)?;
                }
            }
            Ok(ColumnType::Bool)
        }
        ScalarFunc::Coalesce | ScalarFunc::Greatest | ScalarFunc::Least | ScalarFunc::NullIf => {
            require_arity(
                fc,
                if f == ScalarFunc::NullIf {
                    n == 2
                } else {
                    n >= 1
                },
            )?;
            let ty = unify_args(f, args, scope)?;
            match f {
                ScalarFunc::NullIf if crate::eval::is_scalar_jsonpath(ty) => {
                    return Err(ExecError::UndefinedFunction(
                        "operator does not exist: jsonpath = jsonpath".into(),
                    ));
                }
                // `NULLIF` is `=` in disguise, and neither `json` nor `xml`
                // has an `=`. The wording is the operator's, not the
                // function's, which is why this cannot fall through to the
                // generic arity check.
                ScalarFunc::NullIf if crate::eval::is_uncomparable_scalar(ty) => {
                    let name = ty.name();
                    return Err(ExecError::UndefinedFunction(format!(
                        "operator does not exist: {name} = {name}"
                    )));
                }
                ScalarFunc::Greatest | ScalarFunc::Least => {
                    if matches!(
                        ty.storage_type(),
                        ColumnType::JsonPath
                            | ColumnType::Json
                            | ColumnType::Xml
                            | ColumnType::Array(ElemType::JsonPath)
                            | ColumnType::Array(ElemType::Json)
                            | ColumnType::Array(ElemType::Xml)
                    ) {
                        return Err(ExecError::UndefinedFunction(format!(
                            "could not identify a comparison function for type {}",
                            ty.name()
                        )));
                    }
                }
                ScalarFunc::NullIf => {}
                ScalarFunc::Coalesce => {}
                _ => unreachable!(),
            }
            // PostgreSQL resolves the common type ignoring `unknown` literals,
            // then coerces each literal to it — at PLAN time, which is why
            // `coalesce(1, 'x')` is 22P02 even though the literal is never the
            // value returned.
            for a in args {
                if let Expr::StringLiteral(s) = a {
                    crate::eval::cast_value(&Datum::Text(s.clone()), ty, &jiff::tz::TimeZone::UTC)?;
                }
            }
            Ok(ty)
        }
        ScalarFunc::NumNonNulls | ScalarFunc::NumNulls => {
            require_arity(fc, n >= 1)?;
            Ok(ColumnType::Int4)
        }
        ScalarFunc::Floor | ScalarFunc::Ceil | ScalarFunc::Sign => {
            require_arity(fc, n == 1)?;
            // preserves the input numeric type (int2/int4/int8/numeric); `real`
            // has no overload of its own, so it resolves to the `float8` one.
            let t = require_numeric(&args[0], scope)?;
            Ok(float4_widens(t))
        }
        ScalarFunc::Round | ScalarFunc::Trunc => {
            require_arity(fc, n == 1 || n == 2)?;
            // `trunc` also covers `macaddr`, so — unlike `round` — it has no
            // preferred candidate to settle an all-`unknown` call.
            if f == ScalarFunc::Trunc && args.iter().all(is_unknown_literal) {
                return Err(ambiguous_function(&fc.name, n));
            }
            if n == 1 {
                // `trunc(macaddr)` / `trunc(macaddr8)` zero the device part of
                // a hardware address, keeping the manufacturer prefix.
                if f == ScalarFunc::Trunc
                    && let ty @ (ColumnType::MacAddr | ColumnType::MacAddr8) =
                        crate::eval::infer_type(&args[0], scope)?
                {
                    return Ok(ty);
                }
                let t = require_numeric(&args[0], scope)?;
                Ok(float4_widens(t))
            } else {
                // two-arg: numeric (or int promoted to numeric) first arg, int
                // second arg, → numeric. Neither float width has a 2-arg form,
                // so the scale argument is what makes an `unknown` value
                // resolve to `numeric` rather than the usual `float8`.
                if !is_unknown_literal(&args[0]) {
                    let t0 = float4_widens(require_numeric(&args[0], scope)?);
                    if t0 == ColumnType::Float8 {
                        return Err(no_matching_function());
                    }
                }
                require_int_or_null(&args[1], scope)?;
                Ok(ColumnType::Numeric(None))
            }
        }
        ScalarFunc::Sqrt | ScalarFunc::Exp | ScalarFunc::Ln | ScalarFunc::Log => {
            require_arity(fc, n == 1 || (f == ScalarFunc::Log && n == 2))?;
            if n == 2 {
                // `log(base, num)` is declared over `numeric` alone — there is
                // no two-argument `float8` candidate — so a `float8` operand
                // has nothing to resolve to and both `unknown` literals land on
                // `numeric` rather than the usual `float8`.
                for arg in args {
                    if !is_unknown_literal(arg)
                        && float4_widens(require_numeric(arg, scope)?) == ColumnType::Float8
                    {
                        return Err(no_matching_function());
                    }
                }
                return Ok(ColumnType::Numeric(None));
            }
            let at = require_numeric(&args[0], scope)?;
            Ok(if at.is_numeric() {
                ColumnType::Numeric(None)
            } else {
                ColumnType::Float8
            })
        }
        ScalarFunc::Power => {
            require_arity(fc, n == 2)?;
            let a = require_numeric(&args[0], scope)?;
            let b = require_numeric(&args[1], scope)?;
            Ok(power_overload(args, a, b))
        }
        ScalarFunc::Pi => {
            require_arity(fc, n == 0)?;
            Ok(ColumnType::Float8)
        }
        ScalarFunc::PgSleep => {
            require_arity(fc, n == 1)?;
            require_numeric(&args[0], scope)?;
            Ok(crate::routine::VOID_RESULT_TYPE)
        }
        ScalarFunc::UuidV4 => {
            require_arity(fc, n == 0)?;
            Ok(ColumnType::Uuid)
        }
        ScalarFunc::UuidV7 => {
            require_arity(fc, n <= 1)?;
            if let Some(arg) = args.first()
                && crate::eval::infer_type(arg, scope)?
                    .temporal_base()
                    .is_none_or(|(base, _)| base != ColumnType::Interval)
            {
                return Err(no_matching_function());
            }
            Ok(ColumnType::Uuid)
        }
        ScalarFunc::UuidExtractVersion => {
            require_arity(fc, n == 1)?;
            require_uuid(&args[0], scope)?;
            Ok(ColumnType::Int4)
        }
        ScalarFunc::UuidExtractTimestamp => {
            require_arity(fc, n == 1)?;
            require_uuid(&args[0], scope)?;
            Ok(ColumnType::Timestamptz)
        }
        ScalarFunc::PgGetFunctionArgDefault => {
            require_arity(fc, n == 2)?;
            Ok(ColumnType::Text)
        }
        // `float4send(real)` is `real`'s binary output function, and the wire
        // format is what it returns: four big-endian IEEE 754 bytes. The suite
        // reads it to pin how a decimal literal rounds into `real`, which is a
        // question the printed value cannot answer on its own.
        ScalarFunc::Float4Send => {
            require_arity(fc, n == 1)?;
            if !is_unknown_literal(&args[0]) {
                let t = crate::eval::infer_type(&args[0], scope)?;
                // `real` is the only declared parameter; the integer widths
                // reach it through PostgreSQL's implicit widening casts, while
                // `float8` and `numeric` do not (their casts to `real` are
                // assignment-only) and leave the call unresolved.
                if !matches!(
                    t,
                    ColumnType::Float4 | ColumnType::Int2 | ColumnType::Int4 | ColumnType::Int8
                ) {
                    return Err(undefined_function_spelled(&fc.name, args, scope));
                }
            }
            Ok(ColumnType::Bytea)
        }
        ScalarFunc::Lpad | ScalarFunc::Rpad => {
            require_arity(fc, n == 2 || n == 3)?;
            require_text(&args[0], scope)?;
            require_int(&args[1], scope)?;
            if n == 3 {
                require_text(&args[2], scope)?;
            }
            Ok(ColumnType::Text)
        }
        ScalarFunc::Left | ScalarFunc::Right | ScalarFunc::Repeat => {
            require_arity(fc, n == 2)?;
            require_text(&args[0], scope)?;
            require_int(&args[1], scope)?;
            Ok(ColumnType::Text)
        }
        ScalarFunc::Reverse | ScalarFunc::Initcap => {
            // The arity check has to come first: a match guard reading
            // `args[0]` would index an empty slice for a bare `reverse()`.
            require_arity(fc, n == 1)?;
            // `reverse` has a `bytea` overload and `initcap` does not — case is
            // meaningless over bytes.
            if f == ScalarFunc::Reverse && is_bytea_subject(&args[0], scope)? {
                return Ok(ColumnType::Bytea);
            }
            require_text(&args[0], scope)?;
            Ok(ColumnType::Text)
        }
        ScalarFunc::Strpos => {
            require_arity(fc, n == 2)?;
            // `position(bit, bit)` is `bitposition`; it shares the name and the
            // `int4` result with the text form.
            if crate::bit_fn::is_bit_type(crate::eval::infer_type(&args[0], scope)?)
                || crate::bit_fn::is_bit_type(crate::eval::infer_type(&args[1], scope)?)
            {
                return Ok(ColumnType::Int4);
            }
            // `position(bytea, bytea)` counts bytes, and either argument being
            // `bytea` selects it — the other side's `unknown` literal then
            // coerces to `bytea` rather than to `text`.
            if is_bytea_subject(&args[0], scope)? || is_bytea_subject(&args[1], scope)? {
                require_bytea(&args[0], scope)?;
                require_bytea(&args[1], scope)?;
                return Ok(ColumnType::Int4);
            }
            require_text(&args[0], scope)?;
            require_text(&args[1], scope)?;
            Ok(ColumnType::Int4)
        }
        ScalarFunc::Ascii => {
            require_arity(fc, n == 1)?;
            require_text(&args[0], scope)?;
            Ok(ColumnType::Int4)
        }
        ScalarFunc::Chr => {
            require_arity(fc, n == 1)?;
            require_int(&args[0], scope)?;
            Ok(ColumnType::Text)
        }
        ScalarFunc::CurrentSetting => {
            require_arity(fc, n == 1 || n == 2)?;
            require_text(&args[0], scope)?;
            if n == 2 {
                require_bool(&args[1], scope)?;
            }
            Ok(ColumnType::Text)
        }
        ScalarFunc::SetConfig => {
            require_arity(fc, n == 3)?;
            require_text(&args[0], scope)?;
            require_text(&args[1], scope)?;
            require_bool(&args[2], scope)?;
            Ok(ColumnType::Text)
        }
        // The waiting spellings return `void`; Gres reports it as text (the
        // same mapping `pg_notify` already documents), the `try_`/unlock
        // spellings return boolean exactly like PostgreSQL.
        ScalarFunc::AdvisoryLock { try_only, .. } => {
            require_arity(fc, n == 1 || n == 2)?;
            for arg in args.iter().take(n) {
                // An untyped NULL resolves against the `int8`/`(int4, int4)`
                // overloads exactly as any other unknown literal does, so it is
                // a strict-NULL call rather than a no-such-function error.
                require_int_or_null(arg, scope)?;
            }
            Ok(if try_only {
                ColumnType::Bool
            } else {
                ColumnType::Text
            })
        }
        ScalarFunc::AdvisoryUnlock { .. } => {
            require_arity(fc, n == 1 || n == 2)?;
            for arg in args.iter().take(n) {
                require_int_or_null(arg, scope)?;
            }
            Ok(ColumnType::Bool)
        }
        ScalarFunc::AdvisoryUnlockAll => {
            require_arity(fc, n == 0)?;
            Ok(ColumnType::Text)
        }
        ScalarFunc::UnicodeAssigned => {
            require_arity(fc, n == 1)?;
            require_text(&args[0], scope)?;
            Ok(ColumnType::Bool)
        }
        ScalarFunc::CurrentDatabase
        | ScalarFunc::GetDatabaseEncoding
        | ScalarFunc::UnicodeVersion
        | ScalarFunc::CurrentSchema
        | ScalarFunc::CurrentUser
        | ScalarFunc::SessionUser
        | ScalarFunc::Version => {
            require_arity(fc, n == 0)?;
            Ok(ColumnType::Text)
        }
        ScalarFunc::FormatType => {
            require_arity(fc, n == 2)?;
            require_oid_or_null(&args[0], scope)?;
            require_int_or_null(&args[1], scope)?;
            Ok(ColumnType::Text)
        }
        // `pg_typeof` reports `regtype` in PostgreSQL; crabka has no regtype
        // column type, so it reports `text`. The value is identical — a regtype
        // renders as the type's name — but the RowDescription OID is 25, not
        // 2206. Documented divergence, shared with `pg_notify`'s void.
        ScalarFunc::PgTypeof => {
            require_arity(fc, n == 1)?;
            Ok(ColumnType::Text)
        }
        ScalarFunc::PgInputIsValid => {
            require_arity(fc, n == 2)?;
            require_text(&args[0], scope)?;
            require_text(&args[1], scope)?;
            Ok(ColumnType::Bool)
        }
        ScalarFunc::BinaryCoercible => {
            require_arity(fc, n == 2)?;
            require_oid_or_null(&args[0], scope)?;
            require_oid_or_null(&args[1], scope)?;
            Ok(ColumnType::Bool)
        }
        ScalarFunc::PgNumaAvailable => {
            require_arity(fc, n == 0)?;
            Ok(ColumnType::Bool)
        }
        ScalarFunc::TemporalHash { ty, extended } => {
            require_arity(fc, n == if extended { 2 } else { 1 })?;
            if is_unknown_literal(&args[0])
                || crate::eval::infer_type(&args[0], scope)?
                    .temporal_base()
                    .is_some_and(|(base, _)| base == ty)
            {
                if extended {
                    require_int(&args[1], scope)?;
                    Ok(ColumnType::Int8)
                } else {
                    Ok(ColumnType::Int4)
                }
            } else {
                Err(no_matching_function())
            }
        }
        ScalarFunc::IntegerHash { ty, extended } => {
            require_arity(fc, n == if extended { 2 } else { 1 })?;
            let widens_to = |from: ColumnType, to: ColumnType| {
                matches!(
                    (from, to),
                    (
                        ColumnType::Int2,
                        ColumnType::Int2 | ColumnType::Int4 | ColumnType::Int8 | ColumnType::Oid
                    ) | (
                        ColumnType::Int4,
                        ColumnType::Int4 | ColumnType::Int8 | ColumnType::Oid
                    ) | (ColumnType::Int8, ColumnType::Int8 | ColumnType::Oid)
                        | (ColumnType::Oid, ColumnType::Oid)
                        | (ColumnType::InternalChar, ColumnType::InternalChar)
                )
            };
            if !widens_to(crate::eval::infer_type(&args[0], scope)?, ty)
                || (extended
                    && !widens_to(crate::eval::infer_type(&args[1], scope)?, ColumnType::Int8))
            {
                return Err(undefined_function_spelled(&fc.name, args, scope));
            }
            Ok(if extended {
                ColumnType::Int8
            } else {
                ColumnType::Int4
            })
        }
        ScalarFunc::TextHash { ty, extended } => {
            require_arity(fc, n == if extended { 2 } else { 1 })?;
            let arg_ty = crate::eval::infer_type(&args[0], scope)?;
            if !is_unknown_literal(&args[0])
                && arg_ty != ty
                && !(ty == ColumnType::Name && arg_ty == ColumnType::Text)
            {
                return Err(no_matching_function());
            }
            if extended {
                require_int(&args[1], scope)?;
            }
            Ok(if extended {
                ColumnType::Int8
            } else {
                ColumnType::Int4
            })
        }
        ScalarFunc::OidVectorHash { extended } => {
            require_arity(fc, n == if extended { 2 } else { 1 })?;
            if crate::eval::infer_type(&args[0], scope)? != ColumnType::OidVector {
                return Err(no_matching_function());
            }
            if extended {
                require_int(&args[1], scope)?;
            }
            Ok(if extended {
                ColumnType::Int8
            } else {
                ColumnType::Int4
            })
        }
        ScalarFunc::ArrayHash { extended } => {
            require_arity(fc, n == if extended { 2 } else { 1 })?;
            if !matches!(
                crate::eval::infer_type(&args[0], scope)?,
                ColumnType::Array(_)
            ) {
                return Err(no_matching_function());
            }
            if extended {
                require_int(&args[1], scope)?;
            }
            Ok(if extended {
                ColumnType::Int8
            } else {
                ColumnType::Int4
            })
        }
        ScalarFunc::BpcharHash { extended } => {
            require_arity(fc, n == if extended { 2 } else { 1 })?;
            require_text(&args[0], scope)?;
            if extended {
                require_int(&args[1], scope)?;
            }
            Ok(if extended {
                ColumnType::Int8
            } else {
                ColumnType::Int4
            })
        }
        ScalarFunc::UuidHash { extended } => {
            require_arity(fc, n == if extended { 2 } else { 1 })?;
            if !is_unknown_literal(&args[0])
                && crate::eval::infer_type(&args[0], scope)? != ColumnType::Uuid
            {
                return Err(no_matching_function());
            }
            if extended {
                require_int(&args[1], scope)?;
            }
            Ok(if extended {
                ColumnType::Int8
            } else {
                ColumnType::Int4
            })
        }
        ScalarFunc::PgLsnHash { extended } => {
            require_arity(fc, n == if extended { 2 } else { 1 })?;
            if !is_unknown_literal(&args[0])
                && crate::eval::infer_type(&args[0], scope)? != ColumnType::PgLsn
            {
                return Err(no_matching_function());
            }
            if extended {
                require_int(&args[1], scope)?;
            }
            Ok(if extended {
                ColumnType::Int8
            } else {
                ColumnType::Int4
            })
        }
        ScalarFunc::EnumHash { extended } => {
            require_arity(fc, n == if extended { 2 } else { 1 })?;
            if !matches!(
                crate::eval::infer_type(&args[0], scope)?,
                ColumnType::Enum(_)
            ) {
                return Err(no_matching_function());
            }
            if extended {
                require_int(&args[1], scope)?;
            }
            Ok(if extended {
                ColumnType::Int8
            } else {
                ColumnType::Int4
            })
        }
        ScalarFunc::RangeHash { extended } => {
            require_arity(fc, n == if extended { 2 } else { 1 })?;
            if !matches!(
                crate::eval::infer_type(&args[0], scope)?,
                ColumnType::Range(_)
            ) {
                return Err(no_matching_function());
            }
            if extended {
                require_int(&args[1], scope)?;
            }
            Ok(if extended {
                ColumnType::Int8
            } else {
                ColumnType::Int4
            })
        }
        ScalarFunc::MultirangeHash { extended } => {
            require_arity(fc, n == if extended { 2 } else { 1 })?;
            if !matches!(
                crate::eval::infer_type(&args[0], scope)?,
                ColumnType::Multirange(_)
            ) {
                return Err(no_matching_function());
            }
            if extended {
                require_int(&args[1], scope)?;
            }
            Ok(if extended {
                ColumnType::Int8
            } else {
                ColumnType::Int4
            })
        }
        ScalarFunc::FloatHash { ty, extended } => {
            require_arity(fc, n == if extended { 2 } else { 1 })?;
            let accepts = |input: ColumnType| {
                matches!(
                    (input, ty),
                    (
                        ColumnType::Int2 | ColumnType::Int4 | ColumnType::Int8 | ColumnType::Float4,
                        ColumnType::Float4
                    ) | (
                        ColumnType::Int2
                            | ColumnType::Int4
                            | ColumnType::Int8
                            | ColumnType::Float4
                            | ColumnType::Float8,
                        ColumnType::Float8
                    )
                )
            };
            if !accepts(crate::eval::infer_type(&args[0], scope)?)
                || (extended
                    && crate::eval::infer_type(&args[1], scope)? != ColumnType::Int8
                    && !matches!(
                        crate::eval::infer_type(&args[1], scope)?,
                        ColumnType::Int2 | ColumnType::Int4
                    ))
            {
                return Err(undefined_function_spelled(&fc.name, args, scope));
            }
            Ok(if extended {
                ColumnType::Int8
            } else {
                ColumnType::Int4
            })
        }
        ScalarFunc::RangeConstructor(range) => {
            require_arity(fc, (1..=3).contains(&n))?;
            Ok(ColumnType::Range(range))
        }
        ScalarFunc::MultirangeConstructor(multirange) => {
            for arg in args {
                if crate::eval::infer_type(arg, scope)? != ColumnType::Range(multirange.range) {
                    return Err(no_matching_function());
                }
            }
            Ok(ColumnType::Multirange(multirange))
        }
        ScalarFunc::GenericMultirangeConstructor => {
            require_arity(fc, n == 1)?;
            let ColumnType::Range(range) = crate::eval::infer_type(&args[0], scope)? else {
                return Err(no_matching_function());
            };
            ColumnType::multirange_for_range(range).ok_or_else(no_matching_function)
        }
        ScalarFunc::IsEmpty
        | ScalarFunc::LowerInc
        | ScalarFunc::LowerInf
        | ScalarFunc::UpperInc
        | ScalarFunc::UpperInf => {
            require_arity(fc, n == 1)?;
            if !matches!(
                crate::eval::infer_type(&args[0], scope)?,
                ColumnType::Range(_) | ColumnType::Multirange(_)
            ) {
                return Err(no_matching_function());
            }
            Ok(ColumnType::Bool)
        }
        ScalarFunc::RangeContains
        | ScalarFunc::RangeContainedBy
        | ScalarFunc::RangeOverlaps
        | ScalarFunc::RangeAdjacent => {
            require_arity(fc, n == 2)?;
            Ok(ColumnType::Bool)
        }
        ScalarFunc::MultirangePredicate => {
            require_arity(fc, n == 2)?;
            Ok(ColumnType::Bool)
        }
        ScalarFunc::RangeMinus | ScalarFunc::RangeMerge => {
            if matches!(scalar_func(&fc.name), Some(ScalarFunc::RangeMerge))
                && n == 1
                && matches!(
                    crate::eval::infer_type(&args[0], scope)?,
                    ColumnType::Multirange(_)
                )
            {
                let ColumnType::Multirange(multirange) = crate::eval::infer_type(&args[0], scope)?
                else {
                    unreachable!()
                };
                return Ok(ColumnType::Range(multirange.range));
            }
            require_arity(fc, n == 2)?;
            let left = crate::eval::infer_type(&args[0], scope)?;
            let right = crate::eval::infer_type(&args[1], scope)?;
            if matches!((left, right), (ColumnType::Range(a), ColumnType::Range(b)) if a == b) {
                Ok(left)
            } else {
                Err(no_matching_function())
            }
        }
        ScalarFunc::PgTableIsVisible => {
            require_arity(fc, n == 1)?;
            require_int(&args[0], scope)?;
            Ok(ColumnType::Bool)
        }
        ScalarFunc::LoCreate => {
            require_arity(fc, n == 1)?;
            require_oid_or_null(&args[0], scope)?;
            Ok(ColumnType::Oid)
        }
        ScalarFunc::LoOpen => {
            require_arity(fc, n == 2)?;
            require_oid_or_null(&args[0], scope)?;
            require_int(&args[1], scope)?;
            Ok(ColumnType::Int4)
        }
        ScalarFunc::LoClose => {
            require_arity(fc, n == 1)?;
            require_int(&args[0], scope)?;
            Ok(ColumnType::Int4)
        }
        ScalarFunc::LoRead => {
            require_arity(fc, n == 2)?;
            require_int(&args[0], scope)?;
            require_int(&args[1], scope)?;
            Ok(ColumnType::Bytea)
        }
        ScalarFunc::LoWrite => {
            require_arity(fc, n == 2)?;
            require_int(&args[0], scope)?;
            require_bytea(&args[1], scope)?;
            Ok(ColumnType::Int4)
        }
        ScalarFunc::LoSeek => {
            require_arity(fc, n == 3)?;
            require_int(&args[0], scope)?;
            require_int(&args[1], scope)?;
            require_int(&args[2], scope)?;
            Ok(if fc.name == "lo_lseek" {
                ColumnType::Int4
            } else {
                ColumnType::Int8
            })
        }
        ScalarFunc::LoTell => {
            require_arity(fc, n == 1)?;
            require_int(&args[0], scope)?;
            Ok(if fc.name == "lo_tell" {
                ColumnType::Int4
            } else {
                ColumnType::Int8
            })
        }
        ScalarFunc::LoFromBytea => {
            require_arity(fc, n == 2)?;
            require_oid_or_null(&args[0], scope)?;
            require_bytea(&args[1], scope)?;
            Ok(ColumnType::Oid)
        }
        ScalarFunc::LoImport => {
            require_arity(fc, n == 1 || n == 2)?;
            require_text(&args[0], scope)?;
            if n == 2 {
                require_oid_or_null(&args[1], scope)?;
            }
            Ok(ColumnType::Oid)
        }
        ScalarFunc::LoExport => {
            require_arity(fc, n == 2)?;
            require_oid_or_null(&args[0], scope)?;
            require_text(&args[1], scope)?;
            Ok(ColumnType::Int4)
        }
        ScalarFunc::LoGet => {
            require_arity(fc, n == 1 || n == 3)?;
            require_oid_or_null(&args[0], scope)?;
            if n == 3 {
                require_int(&args[1], scope)?;
                require_int(&args[2], scope)?;
            }
            Ok(ColumnType::Bytea)
        }
        ScalarFunc::LoPut => {
            require_arity(fc, n == 3)?;
            require_oid_or_null(&args[0], scope)?;
            require_int(&args[1], scope)?;
            require_bytea(&args[2], scope)?;
            Ok(crate::routine::VOID_RESULT_TYPE)
        }
        ScalarFunc::LoTruncate => {
            require_arity(fc, n == 2)?;
            require_int(&args[0], scope)?;
            require_int(&args[1], scope)?;
            Ok(ColumnType::Int4)
        }
        ScalarFunc::LoUnlink => {
            require_arity(fc, n == 1)?;
            require_oid_or_null(&args[0], scope)?;
            Ok(ColumnType::Int4)
        }
        ScalarFunc::NextVal | ScalarFunc::CurrVal => {
            require_arity(fc, n == 1)?;
            require_text(&args[0], scope)?;
            Ok(ColumnType::Int8)
        }
        ScalarFunc::SetVal => {
            require_arity(fc, n == 2 || n == 3)?;
            require_text(&args[0], scope)?;
            require_int(&args[1], scope)?;
            if n == 3 {
                require_bool(&args[2], scope)?;
            }
            Ok(ColumnType::Int8)
        }
        // PostgreSQL's `pg_notify` returns `void`, which crabka has no column
        // type for; it reports `text` and evaluates to the empty string, which
        // renders identically to `void` on the wire. Documented divergence: the
        // RowDescription type OID is 25, not 2278.
        ScalarFunc::PgNotify => {
            require_arity(fc, n == 2)?;
            require_text(&args[0], scope)?;
            require_text(&args[1], scope)?;
            Ok(ColumnType::Text)
        }
        ScalarFunc::RestoreRelationStats => Ok(ColumnType::Bool),
        ScalarFunc::ClearRelationStats => {
            require_arity(fc, n == 2)?;
            Ok(ColumnType::Text)
        }
        ScalarFunc::RestoreAttributeStats => Ok(ColumnType::Bool),
        ScalarFunc::ClearAttributeStats => {
            require_arity(fc, n == 4)?;
            Ok(ColumnType::Text)
        }
    }
}

/// Evaluate a scalar call. `eval_child` evaluates each argument expression
/// against the current row. It is the SAME `eval` for a scalar context and
/// `agg::eval_grouped` for a grouped context, so the two share the combinators
/// and only the closure differs. Short-circuiting functions (`coalesce`) and
/// the lazy ones evaluate arguments only as far as they need to.
pub(crate) fn eval_scalar(
    fc: &FuncCall,
    scope: Option<&Scope>,
    ctx: &EvalCtx,
    mut eval_child: impl FnMut(&Expr) -> Result<Datum, ExecError>,
) -> Result<Datum, ExecError> {
    match builtin_eval_scalar(fc, scope, ctx, &mut eval_child) {
        // A rewritten operand is evaluated a second time here. Every function
        // that can reach this arm is pure — the side-effecting ones (`nextval`,
        // the advisory locks) are named nothing like a type and resolve long
        // before it.
        Err(ExecError::UndefinedFunction(message)) => match constructor_cast(fc, scope) {
            Some(cast) => eval_child(&cast),
            None => Err(ExecError::UndefinedFunction(message)),
        },
        resolved => resolved,
    }
}

/// [`eval_scalar`] without the constructor-cast last resort. See
/// [`builtin_scalar_result_type`] for why the fallback is not inline.
fn builtin_eval_scalar(
    fc: &FuncCall,
    scope: Option<&Scope>,
    ctx: &EvalCtx,
    eval_child: impl FnMut(&Expr) -> Result<Datum, ExecError>,
) -> Result<Datum, ExecError> {
    let mut eval_child = bpchar_text_arguments(fc, scope, eval_child);
    if crate::text_search_fn::is_text_search_func(&fc.name) {
        return crate::text_search_fn::eval_text_search(fc, ctx, eval_child);
    }
    if crate::network_fn::is_network_func(&fc.name) {
        return crate::network_fn::eval_network(fc, ctx, eval_child);
    }
    if crate::xml_fn::is_xml_func(&fc.name) {
        return crate::xml_fn::eval_xml(fc, ctx, eval_child);
    }
    if crate::bit_fn::is_bit_func(&fc.name) {
        return crate::bit_fn::eval_bit(fc, ctx, eval_child);
    }
    if crate::money_fn::is_money_func(&fc.name) {
        return crate::money_fn::eval_money(fc, ctx, eval_child);
    }
    if crate::sysid_fn::is_sysid_func(&fc.name) {
        return crate::sysid_fn::eval_sysid(fc, ctx, eval_child);
    }
    if crate::snapshot_fn::is_snapshot_func(&fc.name) {
        return crate::snapshot_fn::eval_snapshot(fc, ctx, eval_child);
    }
    if crate::math_fn::is_math_func(&fc.name) {
        return crate::math_fn::eval_math(fc, ctx, eval_child);
    }
    if crate::string_fn::is_string_func(&fc.name) {
        return crate::string_fn::eval_string(fc, ctx, eval_child);
    }
    if crate::regexp_fn::is_regexp_func(&fc.name) {
        return crate::regexp_fn::eval_regexp(fc, ctx, eval_child);
    }
    if crate::geometry_fn::is_geometry_func(&fc.name) {
        return crate::geometry_fn::eval_geometry(fc, ctx, eval_child);
    }
    let f = scalar_func(&fc.name).ok_or_else(|| undefined_function(&fc.name))?;
    if matches!(
        f,
        ScalarFunc::Concat | ScalarFunc::NumNonNulls | ScalarFunc::NumNulls
    ) && let FuncArgs::Variadic { positional, array } = &fc.args
    {
        check_scalar_modifiers(fc)?;
        return match expand_variadic_args(positional, array, eval_child)? {
            ExpandedVariadicArgs::Values(values) => eval_eager(f, fc, &values, ctx),
            ExpandedVariadicArgs::NullArray(_) => Ok(Datum::Null),
        };
    }
    let args = checked_args(fc)?;
    if matches!(
        f,
        ScalarFunc::BoolState { .. } | ScalarFunc::BoolCompare { .. }
    ) {
        require_arity(fc, args.len() == 2)?;
        if let Some(scope) = scope {
            for arg in args {
                if !is_unknown_literal(arg) {
                    require_bool(arg, scope)?;
                }
            }
        }
    }
    match f {
        // coalesce returns the first non-NULL argument, NOT evaluating the rest
        // (so `coalesce(x, 1/0)` with x non-null never divides by zero).
        ScalarFunc::Coalesce => {
            require_arity(fc, !args.is_empty())?;
            let target = scope.map(|scope| unify_args(f, args, scope)).transpose()?;
            for a in args {
                let v = eval_child(a)?;
                if !v.is_null() {
                    return match target {
                        Some(target) => crate::eval::cast_value(&v, target, &ctx.time_zone),
                        None => Ok(v),
                    };
                }
            }
            Ok(Datum::Null)
        }
        // `pg_typeof` reports its argument's resolved type. Use the caller's
        // scope when available because text-backed distinct types (and typed
        // NULL columns) cannot be recovered from their Datum alone.
        ScalarFunc::PgTypeof => {
            require_arity(fc, args.len() == 1)?;
            let value = eval_child(&args[0])?;
            Ok(Datum::Text(typeof_name(&args[0], &value, scope)))
        }
        ScalarFunc::Greatest | ScalarFunc::Least => {
            require_arity(fc, !args.is_empty())?;
            if let Some(scope) = scope {
                crate::eval::require_comparison_function(unify_args(f, args, scope)?)?;
            }
            let want_greater = matches!(f, ScalarFunc::Greatest);
            let vals = resolved_args(f, args, scope, ctx, &mut eval_child)?;
            let mut best: Option<Datum> = None;
            for v in vals {
                if v.is_null() {
                    continue; // greatest/least ignore NULL arguments
                }
                best = Some(match best {
                    None => v,
                    Some(cur) => {
                        let replace = match ops::compare(&v, &cur)? {
                            Some(Ordering::Greater) => want_greater,
                            Some(Ordering::Less) => !want_greater,
                            _ => false, // Equal (both non-null, so never None)
                        };
                        if replace { v } else { cur }
                    }
                });
            }
            Ok(best.unwrap_or(Datum::Null))
        }
        ScalarFunc::NullIf => {
            require_arity(fc, args.len() == 2)?;
            if let Some(scope) = scope {
                let ty = unify_args(f, args, scope)?;
                if crate::eval::is_scalar_jsonpath(ty) {
                    return Err(ExecError::UndefinedFunction(
                        "operator does not exist: jsonpath = jsonpath".into(),
                    ));
                }
                if crate::eval::is_uncomparable_scalar(ty) {
                    let name = ty.name();
                    return Err(ExecError::UndefinedFunction(format!(
                        "operator does not exist: {name} = {name}"
                    )));
                }
            }
            let vals = resolved_args(f, args, scope, ctx, &mut eval_child)?;
            let [a, b] = vals.as_slice() else {
                return Err(undefined_function(&fc.name));
            };
            let (a, b) = (a.clone(), b.clone());
            if crate::eval::runtime_equality_short_circuit(&a, &b) == Some(false) {
                return Ok(a);
            }
            crate::eval::require_runtime_equality(&a, &b)?;
            // NULLIF(a, b) = NULL when a = b, else a. `compare` is None if either
            // is NULL (so a NULL `a` falls through to `Ok(a)` = NULL).
            match ops::compare(&a, &b)? {
                Some(Ordering::Equal) => Ok(Datum::Null),
                _ => Ok(a),
            }
        }
        // `format_type` is NOT strict in its typmod: a NULL there means "no
        // modifier", so only a NULL OID yields NULL.
        ScalarFunc::FormatType => {
            require_arity(fc, args.len() == 2)?;
            let oid = eval_child(&args[0])?;
            let typmod = eval_child(&args[1])?;
            if oid.is_null() {
                return Ok(Datum::Null);
            }
            // A NULL second argument is not `-1`: it withholds the modifier
            // rather than stating there is none, which is a different name for
            // `bit` and `bpchar`. `FORMAT_TYPE_TYPEMOD_GIVEN` is exactly this
            // distinction.
            Ok(Datum::Text(match &typmod {
                Datum::Null => format_type(int_arg(&oid)?, -1),
                other => format_type_given(int_arg(&oid)?, int_arg(other)?),
            }))
        }
        // `pg_notify` is NOT strict: PostgreSQL substitutes the empty string for
        // a NULL channel or payload, so `pg_notify(NULL, 'x')` raises the same
        // empty-channel error as `pg_notify('', 'x')` rather than returning NULL.
        ScalarFunc::PgNotify => {
            require_arity(fc, args.len() == 2)?;
            let channel = eval_child(&args[0])?;
            let payload = eval_child(&args[1])?;
            eval_pg_notify(&channel, &payload, ctx)
        }
        ScalarFunc::RestoreRelationStats => {
            let values = args
                .iter()
                .map(&mut eval_child)
                .collect::<Result<Vec<_>, _>>()?;
            crate::routine::request_statistics(crate::stats_fn::StatisticsRequest::RestoreRelation(
                values,
            ))
        }
        ScalarFunc::ClearRelationStats => {
            require_arity(fc, args.len() == 2)?;
            let values = args
                .iter()
                .map(&mut eval_child)
                .collect::<Result<Vec<_>, _>>()?;
            crate::routine::request_statistics(crate::stats_fn::StatisticsRequest::ClearRelation(
                values,
            ))
        }
        ScalarFunc::RestoreAttributeStats => {
            let values = args
                .iter()
                .map(&mut eval_child)
                .collect::<Result<Vec<_>, _>>()?;
            crate::routine::request_statistics(
                crate::stats_fn::StatisticsRequest::RestoreAttribute(values),
            )
        }
        ScalarFunc::ClearAttributeStats => {
            require_arity(fc, args.len() == 4)?;
            let values = args
                .iter()
                .map(&mut eval_child)
                .collect::<Result<Vec<_>, _>>()?;
            crate::routine::request_statistics(crate::stats_fn::StatisticsRequest::ClearAttribute(
                values,
            ))
        }
        ScalarFunc::EnumFirst | ScalarFunc::EnumLast | ScalarFunc::EnumRange => {
            let scope = scope.ok_or_else(|| undefined_function(&fc.name))?;
            let ty = enum_arg_type(fc, args, scope)?;
            let vals = args
                .iter()
                .map(&mut eval_child)
                .collect::<Result<Vec<_>, _>>()?;
            eval_enum_support(f, fc, ty, &vals, ctx)
        }
        // Eager, strict-or-concat functions: evaluate every argument first.
        _ => {
            let mut vals = args
                .iter()
                .map(&mut eval_child)
                .collect::<Result<Vec<_>, _>>()?;
            coerce_unknown_args(f, args, &mut vals, ctx)?;
            eval_eager(f, fc, &vals, ctx)
        }
    }
}

/// Wrap an argument evaluator so a `character(n)` argument arrives with its
/// blank padding removed.
///
/// This is one place, not one per function, because it is one cast:
/// `PostgreSQL` declares almost every string function over `text` and reaches it
/// from `character` through the implicit `text(bpchar)` coercion described on
/// [`crabka_pgtypes::string::bpchar_to_text`]. `lower`, `replace`, `lpad`,
/// `substr`, `quote_literal`, `to_tsvector` and `length` (whose `bpcharlen`
/// overload measures with `bcTruelen`) therefore all agree with the trimmed
/// value, and each one that did not was a divergence.
///
/// [`preserves_bpchar_padding`] carries the exceptions, and every scalar family
/// this evaluator dispatches to is covered — the `json`, `array` and date/time
/// families are separate arms of `crate::eval` and never reach here, which is
/// what keeps `to_jsonb(c)` and `ARRAY[c]` padded as `PostgreSQL` leaves them.
///
/// Without a scope there is no static type to consult, so the wrapper is inert:
/// a padded `text` value is never mistaken for a `character` one.
fn bpchar_text_arguments<'a>(
    fc: &FuncCall,
    scope: Option<&'a Scope>,
    mut eval_child: impl FnMut(&Expr) -> Result<Datum, ExecError>,
) -> impl FnMut(&Expr) -> Result<Datum, ExecError> {
    let scope = scope.filter(|_| !preserves_bpchar_padding(&fc.name));
    move |expr: &Expr| {
        let value = eval_child(expr)?;
        let Some(scope) = scope else { return Ok(value) };
        Ok(crate::eval::bpchar_to_text_value(expr, scope, &value)?.unwrap_or(value))
    }
}

/// The scalar functions that read a `character` argument's padding rather than
/// casting it to `text`, measured on 18.4.
///
/// Two reasons put a name here. `concat`, `concat_ws` and `format` are declared
/// `VARIADIC "any"` and render each argument with its own output function, and
/// `bpcharout` returns the padded datum — so `concat('x'::char(8), 'Z')` is
/// `'x       Z'` while `'x'::char(8) || 'Z'` is `'xZ'`. `coalesce`, `nullif`,
/// `greatest`, `least` and `pg_typeof` are polymorphic and hand the argument
/// back (or describe it) as the `character` it still is.
///
/// `octet_length` and `bit_length` are the third case: `bpcharoctetlen` is a
/// real `character` overload that measures the stored, padded datum, so
/// `octet_length('x'::char(8))` is 8 where `length('x'::char(8))` is 1.
fn preserves_bpchar_padding(name: &str) -> bool {
    matches!(
        name,
        "bit_length"
            | "coalesce"
            | "concat"
            | "concat_ws"
            | "format"
            | "greatest"
            | "least"
            | "nullif"
            | "octet_length"
            | "pg_typeof"
    ) || crate::xml_fn::is_xml_func(name)
}

/// Coerce `unknown` literal arguments into the fixed type the scalar function
/// resolves them to. PostgreSQL performs this at plan time; the scalar evaluator
/// has no scope, so it re-derives the target from the function and typed inputs.
fn coerce_unknown_args(
    f: ScalarFunc,
    args: &[Expr],
    vals: &mut [Datum],
    ctx: &EvalCtx,
) -> Result<(), ExecError> {
    if !args.iter().any(is_unknown_literal) {
        return Ok(());
    }
    let target = match f {
        ScalarFunc::BoolCompare { .. } => ColumnType::Bool,
        ScalarFunc::TemporalHash { ty, .. } => ty,
        ScalarFunc::IntegerHash { ty, .. } => ty,
        ScalarFunc::FloatHash { ty, .. } => ty,
        ScalarFunc::TextHash { ty, .. } => ty,
        ScalarFunc::OidVectorHash { .. } => ColumnType::OidVector,
        ScalarFunc::ArrayHash { .. } => return Ok(()),
        ScalarFunc::BpcharHash { .. } => ColumnType::Text,
        ScalarFunc::UuidHash { .. } => ColumnType::Uuid,
        ScalarFunc::PgLsnHash { .. } => ColumnType::PgLsn,
        ScalarFunc::EnumHash { .. } => return Ok(()),
        ScalarFunc::RangeHash { .. } | ScalarFunc::MultirangeHash { .. } => return Ok(()),
        // The rounding pair's two-argument form is `numeric(value, int)`; its
        // one-argument form has a preferred `float8` candidate like the rest.
        ScalarFunc::Round | ScalarFunc::Trunc if args.len() == 2 => ColumnType::Numeric(None),
        // `log(base, num)` likewise has no two-argument `float8` candidate.
        ScalarFunc::Log if args.len() == 2 => ColumnType::Numeric(None),
        ScalarFunc::Abs
        | ScalarFunc::Floor
        | ScalarFunc::Ceil
        | ScalarFunc::Round
        | ScalarFunc::Trunc
        | ScalarFunc::Sign
        | ScalarFunc::Sqrt
        | ScalarFunc::Exp
        | ScalarFunc::Ln
        | ScalarFunc::Log => ColumnType::Float8,
        // `power` has both a `float8` and a `numeric` candidate, so a typed
        // operand picks the overload the same way `mod`'s does.
        ScalarFunc::Power if args.len() == 2 => {
            let typed = |i: usize| {
                if is_unknown_literal(&args[i]) {
                    None
                } else {
                    vals[i].column_type()
                }
            };
            match (typed(0), typed(1)) {
                (Some(a), Some(b)) => power_result_type(a, b),
                (Some(t), None) | (None, Some(t)) => power_result_type(t, t),
                (None, None) => ColumnType::Float8,
            }
        }
        // `mod` has no float8 candidate, so a typed operand picks the overload.
        ScalarFunc::Mod => args
            .iter()
            .zip(vals.iter())
            .filter(|(a, _)| !is_unknown_literal(a))
            .find_map(|(_, v)| v.column_type())
            .unwrap_or(ColumnType::Numeric(None)),
        _ => return Ok(()),
    };
    for (i, (a, v)) in args.iter().zip(vals.iter_mut()).enumerate() {
        if !is_unknown_literal(a) || v.is_null() {
            continue;
        }
        // The rounding pair's second argument is always the int scale.
        let to = if i == 1 && matches!(f, ScalarFunc::Round | ScalarFunc::Trunc) {
            ColumnType::Int4
        } else {
            target
        };
        *v = crabka_pgtypes::cast::cast(v, to, &ctx.time_zone)?;
    }
    Ok(())
}

/// Apply an eager scalar function to its already-evaluated arguments. Every
/// function here except `concat`, `num_nonnulls`, `num_nulls`, and `int4_sum`
/// is strict and returns NULL for any NULL argument. The count functions
/// inspect NULLs, `concat` skips them, and `int4_sum` initializes or preserves
/// its aggregate state around one.
fn eval_eager(
    f: ScalarFunc,
    fc: &FuncCall,
    vals: &[Datum],
    ctx: &EvalCtx,
) -> Result<Datum, ExecError> {
    if let ScalarFunc::Concat = f {
        // `concat` runs each argument through its output function in the
        // session's styles, so DateStyle and IntervalStyle match DataRow.
        let mut s = String::new();
        for v in vals {
            if !v.is_null() {
                s.push_str(&text_render_in(v, ctx.output_style()));
            }
        }
        return Ok(Datum::Text(s));
    }
    if matches!(f, ScalarFunc::NumNonNulls | ScalarFunc::NumNulls) {
        require_arity(
            fc,
            !vals.is_empty() || matches!(&fc.args, FuncArgs::Variadic { .. }),
        )?;
        let want_null = f == ScalarFunc::NumNulls;
        return i32::try_from(
            vals.iter()
                .filter(|value| value.is_null() == want_null)
                .count(),
        )
        .map(Datum::Int4)
        .map_err(|_| ExecError::Type(crabka_pgtypes::TypeError::Overflow));
    }
    if let ScalarFunc::RangeConstructor(range) = f {
        return eval_range_constructor(range, fc, vals, ctx);
    }
    if let ScalarFunc::MultirangeConstructor(multirange) = f {
        let ranges = vals
            .iter()
            .map(|value| {
                let Datum::Range(range) = value else {
                    return Err(crabka_pgtypes::TypeError::TypeMismatch {
                        message: "multirange constructor requires ranges".into(),
                    });
                };
                if range.ty != multirange.range {
                    return Err(crabka_pgtypes::TypeError::TypeMismatch {
                        message: "multirange component type does not match".into(),
                    });
                }
                Ok(range.clone())
            })
            .collect::<Result<Vec<_>, _>>()?;
        return crabka_pgtypes::multirange::from_ranges(multirange, ranges)
            .map(Datum::Multirange)
            .map_err(ExecError::from);
    }
    if let ScalarFunc::GenericMultirangeConstructor = f {
        let [Datum::Range(range)] = vals else {
            return Err(undefined_function(&fc.name));
        };
        let ColumnType::Multirange(ty) = ColumnType::multirange_for_range(range.ty)
            .ok_or_else(|| undefined_function(&fc.name))?
        else {
            unreachable!()
        };
        return crabka_pgtypes::multirange::from_ranges(ty, vec![range.clone()])
            .map(Datum::Multirange)
            .map_err(ExecError::from);
    }
    // Strict: a NULL argument short-circuits to NULL. `int4_sum` is the
    // aggregate transition exception: PostgreSQL calls it with a NULL state.
    if f != ScalarFunc::Int4Sum && vals.iter().any(Datum::is_null) {
        return Ok(Datum::Null);
    }
    if let (ScalarFunc::RangeMerge, [Datum::Multirange(multirange)]) = (f, vals) {
        return Ok(Datum::Range(
            match (multirange.ranges.first(), multirange.ranges.last()) {
                (Some(first), Some(last)) => crabka_pgtypes::range::merge(first, last)?,
                _ => crabka_pgtypes::RangeValue {
                    ty: multirange.ty.range,
                    lower: None,
                    upper: None,
                    lower_inclusive: false,
                    upper_inclusive: false,
                    empty: true,
                },
            },
        ));
    }
    match f {
        ScalarFunc::BoolCompare { equal } => {
            require_arity(fc, vals.len() == 2)?;
            let same = bool_arg(&vals[0])? == bool_arg(&vals[1])?;
            Ok(Datum::Bool(same == equal))
        }
        ScalarFunc::Length => {
            require_arity(fc, vals.len() == 1)?;
            if let Datum::BitString(bits) = &vals[0] {
                return Ok(Datum::Int4(crate::bit_fn::length(bits)));
            }
            // The two geometric overloads are `float8`; the rest count units of
            // the argument's own storage.
            if fc.name == "length"
                && let Some(length) = crate::geometry_fn::length_of(&vals[0])
            {
                return Ok(Datum::Float8(length));
            }
            let n = match &vals[0] {
                Datum::TsVector(vector) => vector.len(),
                Datum::Bytea(bytes) if fc.name == "length" => bytes.len(),
                value => text_arg(value)?.chars().count(),
            };
            i32::try_from(n)
                .map(Datum::Int4)
                .map_err(|_| ExecError::Type(crabka_pgtypes::TypeError::Overflow))
        }
        ScalarFunc::Upper => {
            require_arity(fc, vals.len() == 1)?;
            match &vals[0] {
                Datum::Range(range) => Ok(range
                    .upper
                    .as_deref()
                    .filter(|_| !range.empty)
                    .cloned()
                    .unwrap_or(Datum::Null)),
                Datum::Multirange(multirange) => Ok(multirange
                    .ranges
                    .last()
                    .and_then(|range| range.upper.as_deref())
                    .cloned()
                    .unwrap_or(Datum::Null)),
                value => Ok(Datum::Text(text_arg(value)?.to_uppercase())),
            }
        }
        ScalarFunc::Lower => {
            require_arity(fc, vals.len() == 1)?;
            match &vals[0] {
                Datum::Range(range) => Ok(range
                    .lower
                    .as_deref()
                    .filter(|_| !range.empty)
                    .cloned()
                    .unwrap_or(Datum::Null)),
                Datum::Multirange(multirange) => Ok(multirange
                    .ranges
                    .first()
                    .and_then(|range| range.lower.as_deref())
                    .cloned()
                    .unwrap_or(Datum::Null)),
                value => Ok(Datum::Text(text_arg(value)?.to_lowercase())),
            }
        }
        ScalarFunc::IsEmpty
        | ScalarFunc::LowerInc
        | ScalarFunc::LowerInf
        | ScalarFunc::UpperInc
        | ScalarFunc::UpperInf => {
            require_arity(fc, vals.len() == 1)?;
            let value = match &vals[0] {
                Datum::Range(range) => match f {
                    ScalarFunc::IsEmpty => range.empty,
                    ScalarFunc::LowerInc => !range.empty && range.lower_inclusive,
                    ScalarFunc::LowerInf => !range.empty && range.lower.is_none(),
                    ScalarFunc::UpperInc => !range.empty && range.upper_inclusive,
                    ScalarFunc::UpperInf => !range.empty && range.upper.is_none(),
                    _ => unreachable!(),
                },
                Datum::Multirange(multirange) => match f {
                    ScalarFunc::IsEmpty => multirange.ranges.is_empty(),
                    ScalarFunc::LowerInc => multirange
                        .ranges
                        .first()
                        .is_some_and(|range| range.lower_inclusive),
                    ScalarFunc::LowerInf => multirange
                        .ranges
                        .first()
                        .is_some_and(|range| range.lower.is_none()),
                    ScalarFunc::UpperInc => multirange
                        .ranges
                        .last()
                        .is_some_and(|range| range.upper_inclusive),
                    ScalarFunc::UpperInf => multirange
                        .ranges
                        .last()
                        .is_some_and(|range| range.upper.is_none()),
                    _ => unreachable!(),
                },
                value => return Err(type_error(&fc.name, value)),
            };
            Ok(Datum::Bool(value))
        }
        ScalarFunc::RangeContains
        | ScalarFunc::RangeContainedBy
        | ScalarFunc::RangeOverlaps
        | ScalarFunc::RangeAdjacent
        | ScalarFunc::RangeMinus
        | ScalarFunc::RangeMerge => {
            require_arity(fc, vals.len() == 2)?;
            let (Datum::Range(left), Datum::Range(right)) = (&vals[0], &vals[1]) else {
                return Err(type_error(&fc.name, &vals[0]));
            };
            Ok(match f {
                ScalarFunc::RangeContains => {
                    Datum::Bool(crabka_pgtypes::range::contains_range(left, right)?)
                }
                ScalarFunc::RangeContainedBy => {
                    Datum::Bool(crabka_pgtypes::range::contains_range(right, left)?)
                }
                ScalarFunc::RangeOverlaps => {
                    Datum::Bool(crabka_pgtypes::range::overlaps(left, right)?)
                }
                ScalarFunc::RangeAdjacent => {
                    Datum::Bool(crabka_pgtypes::range::adjacent(left, right)?)
                }
                ScalarFunc::RangeMinus => {
                    Datum::Range(crabka_pgtypes::range::difference(left, right)?)
                }
                ScalarFunc::RangeMerge => Datum::Range(crabka_pgtypes::range::merge(left, right)?),
                _ => unreachable!(),
            })
        }
        ScalarFunc::MultirangePredicate => eval_multirange_predicate(&fc.name, vals),
        ScalarFunc::Btrim | ScalarFunc::Ltrim | ScalarFunc::Rtrim => {
            require_arity(fc, vals.len() == 1 || vals.len() == 2)?;
            if let Datum::Bytea(bytes) = &vals[0] {
                let ends = match f {
                    ScalarFunc::Ltrim => crate::bytea_fn::TrimEnds::Leading,
                    ScalarFunc::Rtrim => crate::bytea_fn::TrimEnds::Trailing,
                    _ => crate::bytea_fn::TrimEnds::Both,
                };
                let set = bytea_arg(&vals[1], ctx)?;
                return Ok(crate::bytea_fn::trim(bytes, &set, ends));
            }
            let s = text_arg(&vals[0])?;
            // The optional second argument is the set of characters to strip
            // (default: ASCII/Unicode whitespace).
            let trimmed = match vals.get(1) {
                None => trim_ws(f, s),
                Some(chars) => {
                    let set: Vec<char> = text_arg(chars)?.chars().collect();
                    trim_set(f, s, &set)
                }
            };
            Ok(Datum::Text(trimmed))
        }
        ScalarFunc::Substr => {
            require_arity(fc, vals.len() == 2 || vals.len() == 3)?;
            if let Datum::BitString(bits) = &vals[0] {
                let start = bit_index(&vals[1])?;
                let count = match vals.get(2) {
                    None => None,
                    Some(c) => Some(bit_index(c)?),
                };
                return crate::bit_fn::substring(bits, start, count);
            }
            // Byte-indexed, and reached only when the plan gate saw a known
            // `bytea` — so no multi-byte character can be split by accident.
            if let Datum::Bytea(bytes) = &vals[0] {
                let count = match vals.get(2) {
                    None => None,
                    Some(c) => Some(int_arg(c)?),
                };
                return crate::bytea_fn::substring(bytes, int_arg(&vals[1])?, count);
            }
            let s = text_arg(&vals[0])?;
            // The pattern forms, distinguished at runtime the same way the plan
            // gate distinguishes them: a text second argument is a regexp.
            if let Datum::Text(pattern) = &vals[1] {
                return match vals.get(2) {
                    // `SUBSTRING(s SIMILAR pattern ESCAPE esc)`.
                    Some(escape) => {
                        let escape = text_arg(escape)?.chars().next();
                        Ok(crate::pattern::similar_substring(s, pattern, escape)?
                            .map_or(Datum::Null, Datum::Text))
                    }
                    // `SUBSTRING(s FROM posix_pattern)`.
                    None => posix_substring(s, pattern),
                };
            }
            let start = int_arg(&vals[1])?;
            let count = match vals.get(2) {
                None => None,
                Some(c) => Some(int_arg(c)?),
            };
            substr(s, start, count)
        }
        ScalarFunc::Overlay => {
            require_arity(fc, vals.len() == 3 || vals.len() == 4)?;
            if let Datum::BitString(bits) = &vals[0] {
                let start = bit_index(&vals[2])?;
                let count = match vals.get(3) {
                    None => None,
                    Some(c) => Some(bit_index(c)?),
                };
                return crate::bit_fn::overlay(bits, &vals[1], start, count);
            }
            if let Datum::Bytea(bytes) = &vals[0] {
                let replacement = bytea_arg(&vals[1], ctx)?;
                let start = int_arg(&vals[2])?;
                let count = match vals.get(3) {
                    Some(c) => int_arg(c)?,
                    // The default replaces exactly as many bytes as it inserts.
                    None => i64::try_from(replacement.len()).unwrap_or(i64::MAX),
                };
                return crate::bytea_fn::overlay(bytes, &replacement, start, count);
            }
            let s = text_arg(&vals[0])?;
            let replacement = text_arg(&vals[1])?;
            let start = int_arg(&vals[2])?;
            let count = match vals.get(3) {
                Some(c) => int_arg(c)?,
                None => replacement.chars().count() as i64,
            };
            overlay(s, replacement, start, count)
        }
        ScalarFunc::Replace => {
            require_arity(fc, vals.len() == 3)?;
            let (s, from, to) = (
                text_arg(&vals[0])?,
                text_arg(&vals[1])?,
                text_arg(&vals[2])?,
            );
            // PostgreSQL `replace` leaves the string unchanged when `from` is empty.
            let out = if from.is_empty() {
                s.to_string()
            } else {
                s.replace(from, to)
            };
            Ok(Datum::Text(out))
        }
        ScalarFunc::Abs => {
            require_arity(fc, vals.len() == 1)?;
            match &vals[0] {
                // `abs((-32768)::int2)` has no int2 result — 22003, like PostgreSQL.
                Datum::Int2(n) => n.checked_abs().map(Datum::Int2).ok_or_else(|| {
                    ExecError::Type(crabka_pgtypes::TypeError::out_of_range_for("smallint"))
                }),
                Datum::Int4(n) => n
                    .checked_abs()
                    .map(Datum::Int4)
                    .ok_or(ExecError::Type(crabka_pgtypes::TypeError::Overflow)),
                // `TypeError::Overflow` says "integer out of range", which is
                // `int4`'s wording and not this overload's: `int8abs` reports
                // the type it could not represent the result in.
                Datum::Int8(n) => n.checked_abs().map(Datum::Int8).ok_or_else(|| {
                    ExecError::Type(crabka_pgtypes::TypeError::out_of_range_for("bigint"))
                }),
                // SP30: abs over float8 (always representable, no overflow trap).
                Datum::Float4(f) => Ok(Datum::Float4(f.abs())),
                Datum::Float8(f) => Ok(Datum::Float8(f.abs())),
                // SP32: abs over numeric.
                Datum::Numeric(d) => Ok(Datum::Numeric(crabka_pgtypes::numeric::abs(d))),
                other => Err(type_error("abs", other)),
            }
        }
        ScalarFunc::Mod => {
            require_arity(fc, vals.len() == 2)?;
            Ok(ops::rem(&vals[0], &vals[1])?)
        }
        ScalarFunc::TypedAdd(_) => {
            require_arity(fc, vals.len() == 2)?;
            Ok(ops::add(&vals[0], &vals[1])?)
        }
        ScalarFunc::TypedSub(_) => {
            require_arity(fc, vals.len() == 2)?;
            Ok(ops::sub(&vals[0], &vals[1])?)
        }
        ScalarFunc::Int8Inc => {
            require_arity(fc, vals.len() == 1)?;
            Ok(ops::add(&vals[0], &Datum::Int8(1))?)
        }
        ScalarFunc::Int4Sum => {
            require_arity(fc, vals.len() == 2)?;
            let state = match &vals[0] {
                Datum::Null => None,
                Datum::Int8(value) => Some(*value),
                other => return Err(type_error(&fc.name, other)),
            };
            let value = match &vals[1] {
                Datum::Null => None,
                Datum::Int4(value) => Some(i64::from(*value)),
                other => return Err(type_error(&fc.name, other)),
            };
            match (state, value) {
                (None, None) => Ok(Datum::Null),
                (None, Some(value)) => Ok(Datum::Int8(value)),
                (Some(state), None) => Ok(Datum::Int8(state)),
                (Some(state), Some(value)) => Ok(Datum::Int8(state.wrapping_add(value))),
            }
        }
        ScalarFunc::Int4Larger => {
            require_arity(fc, vals.len() == 2)?;
            let (Datum::Int4(left), Datum::Int4(right)) = (&vals[0], &vals[1]) else {
                return Err(undefined_function(&fc.name));
            };
            Ok(Datum::Int4((*left).max(*right)))
        }
        ScalarFunc::Int4Smaller => {
            require_arity(fc, vals.len() == 2)?;
            let (Datum::Int4(left), Datum::Int4(right)) = (&vals[0], &vals[1]) else {
                return Err(undefined_function(&fc.name));
            };
            Ok(Datum::Int4((*left).min(*right)))
        }
        ScalarFunc::ArrayLarger => {
            require_arity(fc, vals.len() == 2)?;
            let (Datum::Array(left), Datum::Array(right)) = (&vals[0], &vals[1]) else {
                return Err(undefined_function(&fc.name));
            };
            if left.elem != right.elem {
                return Err(undefined_function(&fc.name));
            }
            match ops::compare(&vals[0], &vals[1])? {
                Some(std::cmp::Ordering::Less) => Ok(vals[1].clone()),
                Some(_) => Ok(vals[0].clone()),
                None => unreachable!("strict functions receive non-null values"),
            }
        }
        ScalarFunc::BoolState { and } => {
            require_arity(fc, vals.len() == 2)?;
            let (Datum::Bool(left), Datum::Bool(right)) = (&vals[0], &vals[1]) else {
                return Err(undefined_function(&fc.name));
            };
            Ok(Datum::Bool(if and {
                *left && *right
            } else {
                *left || *right
            }))
        }
        ScalarFunc::Int4AvgAccum => {
            require_arity(fc, vals.len() == 2)?;
            if vals.iter().any(Datum::is_null) {
                return Ok(Datum::Null);
            }
            let Datum::Array(state) = &vals[0] else {
                return Err(type_error(&fc.name, &vals[0]));
            };
            if state.elem != ElemType::Int8 || state.elems.len() != 2 {
                return Err(ExecError::FunctionError {
                    sqlstate: "22023",
                    message: "int4_avg_accum: expected 2-element int8 array".into(),
                });
            }
            let Datum::Int4(value) = vals[1] else {
                return Err(type_error(&fc.name, &vals[1]));
            };
            let [Datum::Int8(count), Datum::Int8(sum)] = state.elems.as_slice() else {
                unreachable!("validated int4 average state");
            };
            Ok(Datum::Array(crabka_pgtypes::ArrayValue::new(
                ElemType::Int8,
                vec![Datum::Int8(count + 1), Datum::Int8(sum + i64::from(value))],
            )))
        }
        ScalarFunc::Int8Avg => {
            require_arity(fc, vals.len() == 1)?;
            if vals[0].is_null() {
                return Ok(Datum::Null);
            }
            let Datum::Array(state) = &vals[0] else {
                return Err(type_error(&fc.name, &vals[0]));
            };
            if state.elem != ElemType::Int8 || state.elems.len() != 2 {
                return Err(ExecError::FunctionError {
                    sqlstate: "22023",
                    message: "int8_avg: expected 2-element int8 array".into(),
                });
            }
            let [Datum::Int8(count), Datum::Int8(sum)] = state.elems.as_slice() else {
                unreachable!("validated int8 average state");
            };
            if *count == 0 {
                Ok(Datum::Null)
            } else {
                Ok(ops::div(
                    &Datum::Numeric(crabka_pgtypes::numeric::NumericValue::from(*sum)),
                    &Datum::Numeric(crabka_pgtypes::numeric::NumericValue::from(*count)),
                )?)
            }
        }
        ScalarFunc::Float8Accum => {
            require_arity(fc, vals.len() == 2)?;
            if vals.iter().any(Datum::is_null) {
                return Ok(Datum::Null);
            }
            let Datum::Array(state) = &vals[0] else {
                return Err(type_error(&fc.name, &vals[0]));
            };
            if state.elem != ElemType::Float8 || state.elems.len() != 3 {
                return Err(ExecError::FunctionError {
                    sqlstate: "22023",
                    message: "float8_accum: expected 3-element float8 array".into(),
                });
            }
            let Datum::Float8(value) = vals[1] else {
                return Err(type_error(&fc.name, &vals[1]));
            };
            let [
                Datum::Float8(count),
                Datum::Float8(sum),
                Datum::Float8(sum2),
            ] = state.elems.as_slice()
            else {
                unreachable!("validated float8 accumulator state");
            };
            Ok(Datum::Array(crabka_pgtypes::ArrayValue::new(
                ElemType::Float8,
                vec![
                    Datum::Float8(count + 1.0),
                    Datum::Float8(sum + value),
                    Datum::Float8(sum2 + value * value),
                ],
            )))
        }
        ScalarFunc::Float8Avg => {
            require_arity(fc, vals.len() == 1)?;
            if vals[0].is_null() {
                return Ok(Datum::Null);
            }
            let Datum::Array(state) = &vals[0] else {
                return Err(type_error(&fc.name, &vals[0]));
            };
            if state.elem != ElemType::Float8 || state.elems.len() != 3 {
                return Err(ExecError::FunctionError {
                    sqlstate: "22023",
                    message: "float8_avg: expected 3-element float8 array".into(),
                });
            }
            let [Datum::Float8(count), Datum::Float8(sum), Datum::Float8(_)] =
                state.elems.as_slice()
            else {
                unreachable!("validated float8 accumulator state");
            };
            if *count == 0.0 {
                Ok(Datum::Null)
            } else {
                Ok(Datum::Float8(sum / count))
            }
        }
        ScalarFunc::Floor | ScalarFunc::Ceil | ScalarFunc::Sign => {
            require_arity(fc, vals.len() == 1)?;
            round_family(f, &vals[0], None)
        }
        ScalarFunc::Round | ScalarFunc::Trunc => {
            require_arity(fc, vals.len() == 1 || vals.len() == 2)?;
            if f == ScalarFunc::Trunc && vals.len() == 1 {
                match &vals[0] {
                    Datum::MacAddr(value) => return Ok(Datum::MacAddr(value.trunc())),
                    Datum::MacAddr8(value) => return Ok(Datum::MacAddr8(value.trunc())),
                    _ => {}
                }
            }
            let scale = match vals.get(1) {
                None => None,
                Some(s) => Some(int_arg(s)?),
            };
            round_family(f, &vals[0], scale)
        }
        ScalarFunc::Sqrt => {
            require_arity(fc, vals.len() == 1)?;
            if let Datum::Numeric(d) = &vals[0] {
                return crabka_pgtypes::numeric::num_sqrt(d)
                    .map(Datum::Numeric)
                    .map_err(ExecError::Type);
            }
            let x = as_f64(&vals[0])?;
            if x < 0.0 {
                return Err(domain(
                    "2201F",
                    "cannot take square root of a negative number",
                ));
            }
            Ok(Datum::Float8(x.sqrt()))
        }
        ScalarFunc::Exp => {
            require_arity(fc, vals.len() == 1)?;
            if let Datum::Numeric(d) = &vals[0] {
                return crabka_pgtypes::numeric::num_exp(d)
                    .map(Datum::Numeric)
                    .map_err(ExecError::Type);
            }
            finite_or_overflow(as_f64(&vals[0])?.exp())
        }
        ScalarFunc::Ln => {
            require_arity(fc, vals.len() == 1)?;
            if let Datum::Numeric(d) = &vals[0] {
                return crabka_pgtypes::numeric::num_ln(d)
                    .map(Datum::Numeric)
                    .map_err(ExecError::Type);
            }
            let x = as_f64(&vals[0])?;
            if x <= 0.0 {
                return Err(domain(
                    "2201E",
                    "cannot take logarithm of a non-positive number",
                ));
            }
            Ok(Datum::Float8(x.ln()))
        }
        ScalarFunc::Log => {
            require_arity(fc, vals.len() == 1 || vals.len() == 2)?;
            if let [base, num] = vals {
                return crabka_pgtypes::numeric::num_log(&to_numeric(base)?, &to_numeric(num)?)
                    .map(Datum::Numeric)
                    .map_err(ExecError::Type);
            }
            if let Datum::Numeric(d) = &vals[0] {
                return crabka_pgtypes::numeric::num_log10(d)
                    .map(Datum::Numeric)
                    .map_err(ExecError::Type);
            }
            let x = as_f64(&vals[0])?;
            if x <= 0.0 {
                return Err(domain(
                    "2201E",
                    "cannot take logarithm of a non-positive number",
                ));
            }
            Ok(Datum::Float8(x.log10()))
        }
        ScalarFunc::Power => {
            require_arity(fc, vals.len() == 2)?;
            let any_num =
                matches!(&vals[0], Datum::Numeric(_)) || matches!(&vals[1], Datum::Numeric(_));
            let any_f64 =
                matches!(&vals[0], Datum::Float8(_)) || matches!(&vals[1], Datum::Float8(_));
            if any_num && !any_f64 {
                let b = to_numeric(&vals[0])?;
                let e = to_numeric(&vals[1])?;
                return crabka_pgtypes::numeric::num_power(&b, &e)
                    .map(Datum::Numeric)
                    .map_err(ExecError::Type);
            }
            power(as_f64(&vals[0])?, as_f64(&vals[1])?)
        }
        ScalarFunc::Pi => {
            require_arity(fc, vals.is_empty())?;
            Ok(Datum::Float8(std::f64::consts::PI))
        }
        ScalarFunc::PgSleep => {
            require_arity(fc, vals.len() == 1)?;
            sleep_for(as_f64(&vals[0])?)?;
            Ok(crate::routine::void_result_value())
        }
        ScalarFunc::UuidV4 => {
            require_arity(fc, vals.is_empty())?;
            Ok(Datum::Text(uuid_v4().to_canonical_text()))
        }
        ScalarFunc::UuidV7 => {
            require_arity(fc, vals.len() <= 1)?;
            let timestamp = match vals {
                [] => ctx.stmt_now,
                [Datum::Interval(interval)] => crabka_pgtypes::datetime::timestamptz_plus_interval(
                    ctx.stmt_now,
                    *interval,
                    &ctx.time_zone,
                )?,
                _ => return Err(no_matching_function()),
            };
            Ok(Datum::Text(uuid_v7(timestamp).to_canonical_text()))
        }
        ScalarFunc::UuidExtractVersion => {
            require_arity(fc, vals.len() == 1)?;
            let bytes = uuid_bytes(&vals[0])?.0;
            let version = bytes[6] >> 4;
            Ok(if bytes[8] & 0xc0 == 0x80 && matches!(version, 1..=8) {
                Datum::Int4(i32::from(version))
            } else {
                Datum::Null
            })
        }
        ScalarFunc::UuidExtractTimestamp => {
            require_arity(fc, vals.len() == 1)?;
            Ok(uuid_timestamp(uuid_bytes(&vals[0])?)?
                .map(Datum::Timestamptz)
                .unwrap_or(Datum::Null))
        }
        ScalarFunc::PgGetFunctionArgDefault => {
            require_arity(fc, vals.len() == 2)?;
            let Some(kv) = ctx.catalog() else {
                return Err(ExecError::Unsupported(
                    "pg_get_function_arg_default requires a SQL session".into(),
                ));
            };
            let oid = i32::try_from(int_arg(&vals[0])?).unwrap_or(0);
            let default = crate::routine::function_arg_default(kv, oid, int_arg(&vals[1])?)?;
            Ok(default.map(Datum::Text).unwrap_or(Datum::Null))
        }
        ScalarFunc::Float4Send => {
            require_arity(fc, vals.len() == 1)?;
            let Datum::Float4(value) =
                crabka_pgtypes::cast::cast(&vals[0], ColumnType::Float4, &ctx.time_zone)?
            else {
                return Err(type_error("float4send", &vals[0]));
            };
            Ok(Datum::Bytea(crabka_pgtypes::encoding::encode_binary(
                &Datum::Float4(value),
            )))
        }
        ScalarFunc::Lpad | ScalarFunc::Rpad => {
            require_arity(fc, vals.len() == 2 || vals.len() == 3)?;
            let s = text_arg(&vals[0])?;
            let width = int_arg(&vals[1])?;
            let fill = match vals.get(2) {
                None => " ",
                Some(d) => text_arg(d)?,
            };
            Ok(Datum::Text(pad(f, s, width, fill)?))
        }
        ScalarFunc::Left | ScalarFunc::Right => {
            require_arity(fc, vals.len() == 2)?;
            let s = text_arg(&vals[0])?;
            let n = int_arg(&vals[1])?;
            Ok(Datum::Text(left_right(f, s, n)))
        }
        ScalarFunc::Repeat => {
            require_arity(fc, vals.len() == 2)?;
            let s = text_arg(&vals[0])?;
            let n = int_arg(&vals[1])?;
            repeat_str(s, n)
        }
        ScalarFunc::Reverse => {
            require_arity(fc, vals.len() == 1)?;
            if let Datum::Bytea(bytes) = &vals[0] {
                return Ok(Datum::Bytea(bytes.iter().rev().copied().collect()));
            }
            Ok(Datum::Text(text_arg(&vals[0])?.chars().rev().collect()))
        }
        ScalarFunc::Initcap => {
            require_arity(fc, vals.len() == 1)?;
            Ok(Datum::Text(initcap(text_arg(&vals[0])?)))
        }
        ScalarFunc::Strpos => {
            require_arity(fc, vals.len() == 2)?;
            if (matches!(&vals[0], Datum::BitString(_)) || matches!(&vals[1], Datum::BitString(_)))
                && let Some(found) = crate::bit_fn::position(&vals[0], &vals[1])?
            {
                return Ok(Datum::Int4(found));
            }
            if matches!(&vals[0], Datum::Bytea(_)) || matches!(&vals[1], Datum::Bytea(_)) {
                let (haystack, needle) = (bytea_arg(&vals[0], ctx)?, bytea_arg(&vals[1], ctx)?);
                return Ok(Datum::Int4(crate::bytea_fn::position(&haystack, &needle)));
            }
            Ok(Datum::Int4(strpos(
                text_arg(&vals[0])?,
                text_arg(&vals[1])?,
            )))
        }
        ScalarFunc::Ascii => {
            require_arity(fc, vals.len() == 1)?;
            let code = text_arg(&vals[0])?.chars().next().map_or(0, |c| c as i32);
            Ok(Datum::Int4(code))
        }
        ScalarFunc::Chr => {
            require_arity(fc, vals.len() == 1)?;
            chr(int_arg(&vals[0])?)
        }
        ScalarFunc::CurrentSetting => {
            require_arity(fc, vals.len() == 1 || vals.len() == 2)?;
            let missing_ok = vals.get(1).map(bool_arg).transpose()?.unwrap_or(false);
            match crate::session::current_setting_runtime(text_arg(&vals[0])?, missing_ok)? {
                Some(value) => Ok(Datum::Text(value)),
                None => Ok(Datum::Null),
            }
        }
        ScalarFunc::SetConfig => {
            require_arity(fc, vals.len() == 3)?;
            let name = text_arg(&vals[0])?;
            let value = text_arg(&vals[1])?;
            let local = bool_arg(&vals[2])?;
            crate::session::set_config_runtime(name, value, local).map(Datum::Text)
        }
        ScalarFunc::AdvisoryLock {
            try_only,
            transactional,
            shared,
        } => {
            require_arity(fc, vals.len() == 1 || vals.len() == 2)?;
            let key = advisory_key(vals)?;
            let granted =
                crate::session::advisory_lock_runtime(key, shared, transactional, !try_only)?;
            Ok(if try_only {
                Datum::Bool(granted)
            } else {
                Datum::Text(String::new())
            })
        }
        ScalarFunc::AdvisoryUnlock { shared } => {
            require_arity(fc, vals.len() == 1 || vals.len() == 2)?;
            let key = advisory_key(vals)?;
            crate::session::advisory_unlock_runtime(key, shared).map(Datum::Bool)
        }
        ScalarFunc::AdvisoryUnlockAll => {
            require_arity(fc, vals.is_empty())?;
            crate::session::advisory_unlock_all_runtime()?;
            Ok(Datum::Text(String::new()))
        }
        ScalarFunc::LoCreate => {
            require_arity(fc, vals.len() == 1)?;
            let requested = lo_oid(&vals[0])?;
            let runtime = lo_runtime(ctx)?;
            crate::largeobject::require_writable(runtime, &format!("{}()", fc.name))?;
            let oid = runtime
                .pending
                .lock()
                .expect("pending large objects")
                .create(
                    runtime.kv.as_ref(),
                    requested,
                    &ctx.current_user,
                    runtime.compat_privileges,
                )?;
            Ok(Datum::Int4(
                i32::try_from(oid).expect("PostgreSQL OID fits int4 datum"),
            ))
        }
        ScalarFunc::LoOpen => {
            require_arity(fc, vals.len() == 2)?;
            let runtime = lo_runtime(ctx)?;
            let descriptor = crate::largeobject::open(
                runtime,
                &ctx.current_user,
                lo_oid(&vals[0])?,
                i32::try_from(int_arg(&vals[1])?).map_err(|_| lo_offset_error())?,
            )?;
            Ok(Datum::Int4(descriptor))
        }
        ScalarFunc::LoClose => {
            require_arity(fc, vals.len() == 1)?;
            crate::largeobject::close(
                lo_runtime(ctx)?,
                i32::try_from(int_arg(&vals[0])?).map_err(|_| lo_offset_error())?,
            )?;
            Ok(Datum::Int4(0))
        }
        ScalarFunc::LoRead => {
            require_arity(fc, vals.len() == 2)?;
            let descriptor = i32::try_from(int_arg(&vals[0])?).map_err(|_| lo_offset_error())?;
            let len = usize::try_from(int_arg(&vals[1])?).map_err(|_| lo_offset_error())?;
            Ok(Datum::Bytea(crate::largeobject::read_descriptor(
                lo_runtime(ctx)?,
                descriptor,
                len,
            )?))
        }
        ScalarFunc::LoWrite => {
            require_arity(fc, vals.len() == 2)?;
            let descriptor = i32::try_from(int_arg(&vals[0])?).map_err(|_| lo_offset_error())?;
            let bytes = bytea_arg(&vals[1], ctx)?;
            crate::largeobject::write_descriptor(lo_runtime(ctx)?, descriptor, &bytes)?;
            Ok(Datum::Int4(
                i32::try_from(bytes.len()).map_err(|_| lo_offset_error())?,
            ))
        }
        ScalarFunc::LoSeek => {
            require_arity(fc, vals.len() == 3)?;
            let position = crate::largeobject::seek_descriptor(
                lo_runtime(ctx)?,
                i32::try_from(int_arg(&vals[0])?).map_err(|_| lo_offset_error())?,
                int_arg(&vals[1])?,
                i32::try_from(int_arg(&vals[2])?).map_err(|_| lo_offset_error())?,
            )?;
            let position = i64::try_from(position).map_err(|_| lo_offset_error())?;
            Ok(if fc.name == "lo_lseek" {
                Datum::Int4(i32::try_from(position).map_err(|_| lo_offset_error())?)
            } else {
                Datum::Int8(position)
            })
        }
        ScalarFunc::LoTell => {
            require_arity(fc, vals.len() == 1)?;
            let position = i64::try_from(crate::largeobject::tell_descriptor(
                lo_runtime(ctx)?,
                i32::try_from(int_arg(&vals[0])?).map_err(|_| lo_offset_error())?,
            )?)
            .map_err(|_| lo_offset_error())?;
            Ok(if fc.name == "lo_tell" {
                Datum::Int4(i32::try_from(position).map_err(|_| lo_offset_error())?)
            } else {
                Datum::Int8(position)
            })
        }
        ScalarFunc::LoFromBytea => {
            require_arity(fc, vals.len() == 2)?;
            let requested = lo_oid(&vals[0])?;
            let runtime = lo_runtime(ctx)?;
            crate::largeobject::require_writable(runtime, &format!("{}()", fc.name))?;
            let mut pending = runtime.pending.lock().expect("pending large objects");
            let oid = pending.create(
                runtime.kv.as_ref(),
                requested,
                &ctx.current_user,
                runtime.compat_privileges,
            )?;
            pending.replace(
                runtime.kv.as_ref(),
                oid,
                bytea_arg(&vals[1], ctx)?.into_owned(),
            )?;
            Ok(Datum::Int4(
                i32::try_from(oid).expect("PostgreSQL OID fits int4 datum"),
            ))
        }
        ScalarFunc::LoImport => {
            require_arity(fc, vals.len() == 1 || vals.len() == 2)?;
            let runtime = lo_file_runtime(ctx, "lo_import", true)?;
            let path = text_arg(&vals[0])?;
            let bytes = std::fs::read(path).map_err(|error| ExecError::FunctionError {
                sqlstate: "58P01",
                message: format!("could not open server file \"{path}\": {error}"),
            })?;
            let requested = vals.get(1).map_or(Ok(0), lo_oid)?;
            let mut pending = runtime.pending.lock().expect("pending large objects");
            let oid = pending.create(
                runtime.kv.as_ref(),
                requested,
                &ctx.current_user,
                runtime.compat_privileges,
            )?;
            pending.replace(runtime.kv.as_ref(), oid, bytes)?;
            Ok(Datum::Int4(
                i32::try_from(oid).expect("PostgreSQL OID fits int4 datum"),
            ))
        }
        ScalarFunc::LoExport => {
            require_arity(fc, vals.len() == 2)?;
            let runtime = lo_file_runtime(ctx, "lo_export", false)?;
            let oid = lo_oid(&vals[0])?;
            crate::largeobject::require_privilege(
                runtime,
                &ctx.current_user,
                oid,
                crate::largeobject::LoPrivilege::Select,
            )?;
            let path = text_arg(&vals[1])?;
            let bytes = runtime
                .pending
                .lock()
                .expect("pending large objects")
                .read(runtime.kv.as_ref(), oid)?;
            std::fs::write(path, bytes).map_err(|error| ExecError::FunctionError {
                sqlstate: "58P01",
                message: format!("could not write server file \"{path}\": {error}"),
            })?;
            Ok(Datum::Int4(1))
        }
        ScalarFunc::LoGet => {
            require_arity(fc, vals.len() == 1 || vals.len() == 3)?;
            let oid = lo_oid(&vals[0])?;
            let runtime = lo_runtime(ctx)?;
            crate::largeobject::require_privilege(
                runtime,
                &ctx.current_user,
                oid,
                crate::largeobject::LoPrivilege::Select,
            )?;
            let result = if vals.len() == 1 {
                runtime
                    .pending
                    .lock()
                    .expect("pending large objects")
                    .read(runtime.kv.as_ref(), oid)?
            } else {
                let offset = usize::try_from(int_arg(&vals[1])?).map_err(|_| lo_offset_error())?;
                let length = usize::try_from(int_arg(&vals[2])?).map_err(|_| lo_offset_error())?;
                runtime
                    .pending
                    .lock()
                    .expect("pending large objects")
                    .read_range(runtime.kv.as_ref(), oid, offset, length)?
            };
            Ok(Datum::Bytea(result))
        }
        ScalarFunc::LoPut => {
            require_arity(fc, vals.len() == 3)?;
            let oid = lo_oid(&vals[0])?;
            let offset = usize::try_from(int_arg(&vals[1])?).map_err(|_| lo_offset_error())?;
            let runtime = lo_runtime(ctx)?;
            crate::largeobject::require_writable(runtime, &format!("{}()", fc.name))?;
            crate::largeobject::require_privilege(
                runtime,
                &ctx.current_user,
                oid,
                crate::largeobject::LoPrivilege::Update,
            )?;
            runtime
                .pending
                .lock()
                .expect("pending large objects")
                .write_at(runtime.kv.as_ref(), oid, offset, &bytea_arg(&vals[2], ctx)?)?;
            Ok(crate::routine::void_result_value())
        }
        ScalarFunc::LoTruncate => {
            require_arity(fc, vals.len() == 2)?;
            let descriptor = i32::try_from(int_arg(&vals[0])?).map_err(|_| lo_offset_error())?;
            let len = usize::try_from(int_arg(&vals[1])?).map_err(|_| lo_offset_error())?;
            let runtime = lo_runtime(ctx)?;
            crate::largeobject::truncate_descriptor(
                runtime,
                descriptor,
                len,
                &format!("{}()", fc.name),
            )?;
            Ok(Datum::Int4(0))
        }
        ScalarFunc::LoUnlink => {
            require_arity(fc, vals.len() == 1)?;
            let oid = lo_oid(&vals[0])?;
            let runtime = lo_runtime(ctx)?;
            crate::largeobject::require_writable(runtime, &format!("{}()", fc.name))?;
            crate::largeobject::require_privilege(
                runtime,
                &ctx.current_user,
                oid,
                crate::largeobject::LoPrivilege::Update,
            )?;
            runtime
                .pending
                .lock()
                .expect("pending large objects")
                .unlink(runtime.kv.as_ref(), oid)?;
            Ok(Datum::Int4(1))
        }
        ScalarFunc::NextVal => {
            require_arity(fc, vals.len() == 1)?;
            let name = text_arg(&vals[0])?.to_string();
            let runtime = ctx.sequence.as_ref().ok_or_else(|| {
                ExecError::Unsupported("sequence functions require a SQL session".into())
            })?;
            let (value, staged) =
                runtime
                    .manager
                    .nextval_written(&*runtime.kv, ctx.resolution(), &name)?;
            if let Some(staged) = staged {
                runtime
                    .pending
                    .lock()
                    .expect("pending sequences")
                    .stage(staged);
            }
            runtime
                .currvals
                .lock()
                .expect("sequence currvals")
                .insert(name, value);
            Ok(Datum::Int8(value))
        }
        ScalarFunc::CurrVal => {
            require_arity(fc, vals.len() == 1)?;
            let name = text_arg(&vals[0])?;
            let runtime = ctx.sequence.as_ref().ok_or_else(|| {
                ExecError::Unsupported("sequence functions require a SQL session".into())
            })?;
            let value = runtime
                .currvals
                .lock()
                .expect("sequence currvals")
                .get(name)
                .copied()
                .ok_or_else(|| {
                    ExecError::ObjectNotInPrerequisiteState(format!(
                        "currval of sequence \"{name}\" is not yet defined in this session"
                    ))
                })?;
            Ok(Datum::Int8(value))
        }
        ScalarFunc::SetVal => {
            require_arity(fc, vals.len() == 2 || vals.len() == 3)?;
            let name = text_arg(&vals[0])?.to_string();
            let value = int_arg(&vals[1])?;
            let is_called = vals.get(2).map(bool_arg).transpose()?.unwrap_or(true);
            let runtime = ctx.sequence.as_ref().ok_or_else(|| {
                ExecError::Unsupported("sequence functions require a SQL session".into())
            })?;
            let (value, staged) = runtime.manager.setval_written(
                &*runtime.kv,
                ctx.resolution(),
                &name,
                value,
                is_called,
            )?;
            if let Some(staged) = staged {
                runtime
                    .pending
                    .lock()
                    .expect("pending sequences")
                    .stage(staged);
            }
            runtime
                .currvals
                .lock()
                .expect("sequence currvals")
                .insert(name, value);
            Ok(Datum::Int8(value))
        }
        // The database the session connected to, which is also the one
        // `pg_database` row's `datname`. The two read the same field so they
        // cannot disagree; they used to be two separate literals.
        ScalarFunc::CurrentDatabase => {
            require_arity(fc, vals.is_empty())?;
            Ok(Datum::Text(ctx.database().to_string()))
        }
        ScalarFunc::GetDatabaseEncoding => {
            require_arity(fc, vals.is_empty())?;
            Ok(Datum::Text("UTF8".into()))
        }
        ScalarFunc::UnicodeVersion => {
            require_arity(fc, vals.is_empty())?;
            let (major, minor) = unicode_version();
            Ok(Datum::Text(format!("{major}.{minor}")))
        }
        // `pg_u_prop_assigned`: a code point is assigned when its
        // General_Category is anything but `Cn`. Rust's `char` cannot hold a
        // surrogate, so the halves PostgreSQL also rejects here can never reach
        // this point — they are refused when the literal is lexed.
        ScalarFunc::UnicodeAssigned => {
            require_arity(fc, vals.len() == 1)?;
            let assigned = text_arg(&vals[0])?.chars().all(|c| {
                unicode_general_category::get_general_category(c)
                    != unicode_general_category::GeneralCategory::Unassigned
            });
            Ok(Datum::Bool(assigned))
        }
        // The schema a `CREATE` with no qualifier lands in — the first
        // `search_path` entry that names an existing schema, and NULL when the
        // path names none. Verified against `postgres:18.4`, where
        // `SET search_path = notme; SELECT current_schema` is NULL rather than
        // `public`.
        ScalarFunc::CurrentSchema => {
            require_arity(fc, vals.is_empty())?;
            let kv = ctx.catalog().ok_or_else(|| {
                ExecError::Unsupported("current_schema requires a SQL session".into())
            })?;
            Ok(ctx
                .resolution()
                .creation_schema(kv)?
                .map_or(Datum::Null, Datum::Text))
        }
        ScalarFunc::CurrentUser => {
            require_arity(fc, vals.is_empty())?;
            Ok(Datum::Text(ctx.current_user.clone()))
        }
        ScalarFunc::SessionUser => {
            require_arity(fc, vals.is_empty())?;
            Ok(Datum::Text(ctx.session_user.clone()))
        }
        ScalarFunc::Version => {
            require_arity(fc, vals.is_empty())?;
            Ok(Datum::Text(crabka_pgcatalog::server_version_string()))
        }
        ScalarFunc::PgInputIsValid => {
            require_arity(fc, vals.len() == 2)?;
            let input = text_arg(&vals[0])?;
            let type_name = text_arg(&vals[1])?;
            Ok(Datum::Bool(input_error(input, type_name, ctx)?.is_none()))
        }
        ScalarFunc::BinaryCoercible => {
            require_arity(fc, vals.len() == 2)?;
            let source = u32::try_from(int_arg(&vals[0])?).unwrap_or(0);
            let target = u32::try_from(int_arg(&vals[1])?).unwrap_or(0);
            Ok(Datum::Bool(crate::catalog_rel::is_binary_coercible(
                source, target,
            )))
        }
        ScalarFunc::PgNumaAvailable => {
            require_arity(fc, vals.is_empty())?;
            Ok(Datum::Bool(false))
        }
        ScalarFunc::TemporalHash { ty, extended } => {
            require_arity(fc, vals.len() == if extended { 2 } else { 1 })?;
            let seed = if extended {
                int_arg(&vals[1])?.cast_unsigned()
            } else {
                0
            };
            let hash = match (ty, &vals[0]) {
                (ColumnType::Interval, Datum::Interval(value)) => {
                    crate::partition::hash::hash_int64_extended(
                        value.canonical_micros() as i64,
                        seed,
                    )
                }
                (ColumnType::Time, Datum::Time(value)) => {
                    crate::partition::hash::hash_int64_extended(
                        i64::from_be_bytes(crabka_pgtypes::datetime::time_to_binary(*value)),
                        seed,
                    )
                }
                (ColumnType::Timestamp, Datum::Timestamp(value)) => {
                    crate::partition::hash::hash_int64_extended(
                        i64::from_be_bytes(crabka_pgtypes::datetime::timestamp_to_binary(*value)),
                        seed,
                    )
                }
                (ColumnType::Timestamptz, Datum::Timestamptz(value)) => {
                    crate::partition::hash::hash_int64_extended(
                        i64::from_be_bytes(crabka_pgtypes::datetime::timestamptz_to_binary(*value)),
                        seed,
                    )
                }
                (ColumnType::Timetz, Datum::Timetz(value)) => {
                    let binary = crabka_pgtypes::datetime::timetz_to_binary(*value);
                    crate::partition::hash::hash_int64_extended(
                        i64::from_be_bytes(binary[..8].try_into().expect("eight bytes")),
                        seed,
                    ) ^ crate::partition::hash::hash_int32_extended(
                        i32::from_be_bytes(binary[8..].try_into().expect("four bytes")),
                        seed,
                    )
                    .cast_unsigned()
                }
                _ => return Err(type_error(&fc.name, &vals[0])),
            };
            Ok(if extended {
                Datum::Int8(i64::from_ne_bytes(hash.to_ne_bytes()))
            } else {
                Datum::Int4(i32::from_ne_bytes(
                    hash.to_ne_bytes()[..4].try_into().expect("u64"),
                ))
            })
        }
        ScalarFunc::IntegerHash { ty, extended } => {
            require_arity(fc, vals.len() == if extended { 2 } else { 1 })?;
            let seed = if extended {
                int_arg(&vals[1])?.cast_unsigned()
            } else {
                0
            };
            let value = match vals[0] {
                Datum::InternalChar(value) => i64::from(value.cast_signed()),
                _ => int_arg(&vals[0])?,
            };
            match (ty, extended) {
                (ColumnType::Int2 | ColumnType::Int4 | ColumnType::InternalChar, false) => {
                    Ok(Datum::Int4(crate::partition::hash::hash_int32(
                        i32::try_from(value).map_err(|_| type_error(&fc.name, &vals[0]))?,
                    )))
                }
                (ColumnType::Oid, false) => Ok(Datum::Int4(crate::partition::hash::hash_int32(
                    i32::from_ne_bytes(
                        u32::try_from(value)
                            .map_err(|_| type_error(&fc.name, &vals[0]))?
                            .to_ne_bytes(),
                    ),
                ))),
                (ColumnType::Int8, false) => {
                    Ok(Datum::Int4(crate::partition::hash::hash_int64(value)))
                }
                (ColumnType::Int2 | ColumnType::Int4 | ColumnType::InternalChar, true) => {
                    Ok(Datum::Int8(crate::partition::hash::hash_int32_extended(
                        i32::try_from(value).map_err(|_| type_error(&fc.name, &vals[0]))?,
                        seed,
                    )))
                }
                (ColumnType::Oid, true) => {
                    Ok(Datum::Int8(crate::partition::hash::hash_int32_extended(
                        i32::from_ne_bytes(
                            u32::try_from(value)
                                .map_err(|_| type_error(&fc.name, &vals[0]))?
                                .to_ne_bytes(),
                        ),
                        seed,
                    )))
                }
                (ColumnType::Int8, true) => Ok(Datum::Int8(i64::from_ne_bytes(
                    crate::partition::hash::hash_int64_extended(value, seed).to_ne_bytes(),
                ))),
                _ => Err(type_error(&fc.name, &vals[0])),
            }
        }
        ScalarFunc::FloatHash { ty, extended } => {
            require_arity(fc, vals.len() == if extended { 2 } else { 1 })?;
            let seed = if extended {
                int_arg(&vals[1])?.cast_unsigned()
            } else {
                0
            };
            let value = match crabka_pgtypes::cast::cast(&vals[0], ty, &ctx.time_zone)? {
                Datum::Float4(value) => f64::from(value),
                Datum::Float8(value) => value,
                other => return Err(type_error(&fc.name, &other)),
            };
            let hash = crate::partition::hash::hash_float64_extended(value, seed);
            Ok(if extended {
                Datum::Int8(i64::from_ne_bytes(hash.to_ne_bytes()))
            } else {
                Datum::Int4(crate::partition::hash::hash_float64(value))
            })
        }
        ScalarFunc::TextHash { extended, .. } => {
            require_arity(fc, vals.len() == if extended { 2 } else { 1 })?;
            let seed = if extended {
                int_arg(&vals[1])?.cast_unsigned()
            } else {
                0
            };
            let value = text_arg(&vals[0])?;
            let hash = crate::partition::hash::hash_bytes_extended(value.as_bytes(), seed)?;
            Ok(if extended {
                Datum::Int8(i64::from_ne_bytes(hash.to_ne_bytes()))
            } else {
                Datum::Int4(crate::partition::hash::hash_bytes(value.as_bytes())?)
            })
        }
        ScalarFunc::OidVectorHash { extended } => {
            require_arity(fc, vals.len() == if extended { 2 } else { 1 })?;
            let seed = if extended {
                int_arg(&vals[1])?.cast_unsigned()
            } else {
                0
            };
            let Datum::OidVector(vector) = &vals[0] else {
                return Err(type_error(&fc.name, &vals[0]));
            };
            let mut bytes = Vec::with_capacity(vector.elems.len() * size_of::<i32>());
            for value in &vector.elems {
                let Datum::Int4(value) = value else {
                    return Err(type_error(&fc.name, value));
                };
                bytes.extend(value.to_ne_bytes());
            }
            let hash = crate::partition::hash::hash_bytes_extended(&bytes, seed)?;
            Ok(if extended {
                Datum::Int8(i64::from_ne_bytes(hash.to_ne_bytes()))
            } else {
                Datum::Int4(crate::partition::hash::hash_bytes(&bytes)?)
            })
        }
        ScalarFunc::ArrayHash { extended } => {
            require_arity(fc, vals.len() == if extended { 2 } else { 1 })?;
            let seed = if extended {
                int_arg(&vals[1])?.cast_unsigned()
            } else {
                0
            };
            let Datum::Array(array) = &vals[0] else {
                return Err(type_error(&fc.name, &vals[0]));
            };
            let hash = crate::partition::hash::hash_array_extended(array, seed)?;
            Ok(if extended {
                Datum::Int8(i64::from_ne_bytes(hash.to_ne_bytes()))
            } else {
                Datum::Int4(i32::from_ne_bytes(
                    hash.to_ne_bytes()[..4].try_into().expect("u64"),
                ))
            })
        }
        ScalarFunc::BpcharHash { extended } => {
            require_arity(fc, vals.len() == if extended { 2 } else { 1 })?;
            let seed = if extended {
                int_arg(&vals[1])?.cast_unsigned()
            } else {
                0
            };
            let value = crabka_pgtypes::string::bpchar_to_text(text_arg(&vals[0])?);
            let hash = crate::partition::hash::hash_bytes_extended(value.as_bytes(), seed)?;
            Ok(if extended {
                Datum::Int8(i64::from_ne_bytes(hash.to_ne_bytes()))
            } else {
                Datum::Int4(crate::partition::hash::hash_bytes(value.as_bytes())?)
            })
        }
        ScalarFunc::UuidHash { extended } => {
            require_arity(fc, vals.len() == if extended { 2 } else { 1 })?;
            let seed = if extended {
                int_arg(&vals[1])?.cast_unsigned()
            } else {
                0
            };
            let uuid = crabka_pgtypes::uuid::UuidBytes::parse(text_arg(&vals[0])?)?;
            let hash = crate::partition::hash::hash_bytes_extended(&uuid.0, seed)?;
            Ok(if extended {
                Datum::Int8(i64::from_ne_bytes(hash.to_ne_bytes()))
            } else {
                Datum::Int4(crate::partition::hash::hash_bytes(&uuid.0)?)
            })
        }
        ScalarFunc::PgLsnHash { extended } => {
            require_arity(fc, vals.len() == if extended { 2 } else { 1 })?;
            let Datum::PgLsn(value) = vals[0] else {
                return Err(type_error(&fc.name, &vals[0]));
            };
            let seed = if extended {
                int_arg(&vals[1])?.cast_unsigned()
            } else {
                0
            };
            let hash = crate::partition::hash::hash_int64_extended(value.cast_signed(), seed);
            Ok(if extended {
                Datum::Int8(i64::from_ne_bytes(hash.to_ne_bytes()))
            } else {
                Datum::Int4(crate::partition::hash::hash_int64(value.cast_signed()))
            })
        }
        ScalarFunc::EnumHash { extended } => {
            require_arity(fc, vals.len() == if extended { 2 } else { 1 })?;
            let Datum::Enum(value) = &vals[0] else {
                return Err(type_error(&fc.name, &vals[0]));
            };
            let label = value
                .sort_order()
                .ok_or_else(|| type_error(&fc.name, &vals[0]))?;
            let oid = value
                .ty
                .oid
                .checked_mul(128)
                .and_then(|base| base.checked_add(u32::try_from(label + 1).ok()?))
                .unwrap_or(u32::MAX);
            let seed = if extended {
                int_arg(&vals[1])?.cast_unsigned()
            } else {
                0
            };
            Ok(if extended {
                Datum::Int8(crate::partition::hash::hash_int32_extended(
                    i32::from_ne_bytes(oid.to_ne_bytes()),
                    seed,
                ))
            } else {
                Datum::Int4(crate::partition::hash::hash_int32(i32::from_ne_bytes(
                    oid.to_ne_bytes(),
                )))
            })
        }
        ScalarFunc::RangeHash { extended } => {
            require_arity(fc, vals.len() == if extended { 2 } else { 1 })?;
            let Datum::Range(range) = &vals[0] else {
                return Err(type_error(&fc.name, &vals[0]));
            };
            let seed = if extended {
                int_arg(&vals[1])?.cast_unsigned()
            } else {
                0
            };
            hash_range(range, seed, extended)
        }
        ScalarFunc::MultirangeHash { extended } => {
            require_arity(fc, vals.len() == if extended { 2 } else { 1 })?;
            let Datum::Multirange(multirange) = &vals[0] else {
                return Err(type_error(&fc.name, &vals[0]));
            };
            let seed = if extended {
                int_arg(&vals[1])?.cast_unsigned()
            } else {
                0
            };
            let mut hash = 1_u64;
            for range in &multirange.ranges {
                let value = hash_range_value(range, seed, extended)?;
                hash = hash.wrapping_mul(31).wrapping_add(value);
            }
            Ok(hash_result(hash, extended))
        }
        ScalarFunc::PgTableIsVisible => {
            require_arity(fc, vals.len() == 1)?;
            crate::catalog_fn::relation_is_visible(&vals[0], ctx)
        }
        // concat / coalesce / nullif / greatest / least are handled before here.
        _ => unreachable!("non-eager scalar function reached eval_eager"),
    }
}

fn enum_arg_type(
    fc: &FuncCall,
    args: &[Expr],
    scope: &Scope,
) -> Result<crabka_pgtypes::usertype::UserTypeRef, ExecError> {
    let mut ty = None;
    for arg in args {
        if crate::eval::is_unknown_literal(arg) {
            continue;
        }
        let ColumnType::Enum(candidate) = crate::eval::infer_type(arg, scope)? else {
            return Err(undefined_function_spelled(&fc.name, args, scope));
        };
        if ty
            .replace(candidate)
            .is_some_and(|existing| existing != candidate)
        {
            return Err(undefined_function_spelled(&fc.name, args, scope));
        }
    }
    ty.ok_or_else(|| undefined_function_spelled(&fc.name, args, scope))
}

fn hash_range(
    range: &crabka_pgtypes::RangeValue,
    seed: u64,
    extended: bool,
) -> Result<Datum, ExecError> {
    Ok(hash_result(
        hash_range_value(range, seed, extended)?,
        extended,
    ))
}

fn hash_range_value(
    range: &crabka_pgtypes::RangeValue,
    seed: u64,
    extended: bool,
) -> Result<u64, ExecError> {
    const EMPTY: u8 = 0x01;
    const LB_INC: u8 = 0x02;
    const UB_INC: u8 = 0x04;
    const LB_INF: u8 = 0x08;
    const UB_INF: u8 = 0x10;

    let mut flags = if range.empty { EMPTY } else { 0 };
    flags |= match (&range.lower, range.lower_inclusive) {
        (None, _) => LB_INF,
        (Some(_), true) => LB_INC,
        (Some(_), false) => 0,
    };
    flags |= match (&range.upper, range.upper_inclusive) {
        (None, _) => UB_INF,
        (Some(_), true) => UB_INC,
        (Some(_), false) => 0,
    };
    let lower = range.lower.as_deref().map_or(Ok(0), |value| {
        crate::partition::hash::column_hash(value, seed)?
            .ok_or_else(|| type_error("hash_range", value))
    })?;
    let upper = range.upper.as_deref().map_or(Ok(0), |value| {
        crate::partition::hash::column_hash(value, seed)?
            .ok_or_else(|| type_error("hash_range", value))
    })?;
    let mut hash =
        crate::partition::hash::hash_int32_extended(i32::from(flags), seed).cast_unsigned() ^ lower;
    hash = if extended {
        ((hash << 1) & 0xffff_fffe_ffff_fffe) | ((hash >> 31) & 0x0000_0001_0000_0001)
    } else {
        u64::from((hash as u32).rotate_left(1))
    };
    Ok(hash ^ upper)
}

fn hash_result(hash: u64, extended: bool) -> Datum {
    if extended {
        Datum::Int8(i64::from_ne_bytes(hash.to_ne_bytes()))
    } else {
        Datum::Int4(i32::from_ne_bytes(
            hash.to_ne_bytes()[..4].try_into().expect("u64"),
        ))
    }
}

fn eval_enum_support(
    f: ScalarFunc,
    fc: &FuncCall,
    ty: crabka_pgtypes::usertype::UserTypeRef,
    vals: &[Datum],
    ctx: &EvalCtx,
) -> Result<Datum, ExecError> {
    let labels = crabka_pgtypes::usertype::lookup_oid(ty.oid)
        .and_then(|user| user.labels().map(ToOwned::to_owned))
        .ok_or_else(|| undefined_function(&fc.name))?;
    let value = |label: &str| {
        Datum::Enum(crabka_pgtypes::datum::EnumValue {
            ty,
            label: label.to_string(),
        })
    };
    let bound = |datum: &Datum| -> Result<Option<usize>, ExecError> {
        if datum.is_null() {
            return Ok(None);
        }
        let datum = crate::eval::cast_operand(datum, ColumnType::Enum(ty), ctx)?;
        let Datum::Enum(value) = datum else {
            unreachable!("enum coercion returned a non-enum value")
        };
        Ok(labels.iter().position(|label| label == &value.label))
    };
    match f {
        ScalarFunc::EnumFirst => {
            require_arity(fc, vals.len() == 1)?;
            let result = labels
                .first()
                .map(|label| value(label))
                .ok_or_else(|| undefined_function(&fc.name))?;
            crate::eval::ensure_enum_datum_safe(ctx, &result)?;
            Ok(result)
        }
        ScalarFunc::EnumLast => {
            require_arity(fc, vals.len() == 1)?;
            let result = labels
                .last()
                .map(|label| value(label))
                .ok_or_else(|| undefined_function(&fc.name))?;
            crate::eval::ensure_enum_datum_safe(ctx, &result)?;
            Ok(result)
        }
        ScalarFunc::EnumRange => {
            require_arity(fc, vals.len() == 1 || vals.len() == 2)?;
            let first = bound(&vals[0])?;
            let last = vals.get(1).map(|value| bound(value)).transpose()?.flatten();
            let lower = first.unwrap_or(0);
            let upper = last.unwrap_or_else(|| labels.len().saturating_sub(1));
            let elems = (lower <= upper)
                .then(|| {
                    labels[lower..=upper]
                        .iter()
                        .map(|label| value(label))
                        .collect()
                })
                .unwrap_or_default();
            let result = Datum::Array(crabka_pgtypes::ArrayValue::new(ElemType::User(ty), elems));
            crate::eval::ensure_enum_datum_safe(ctx, &result)?;
            Ok(result)
        }
        _ => unreachable!("non-enum support function reached eval_enum_support"),
    }
}

fn eval_multirange_predicate(name: &str, vals: &[Datum]) -> Result<Datum, ExecError> {
    if vals.len() != 2 {
        return Err(undefined_function(name));
    }
    let result = match (name, &vals[0], &vals[1]) {
        ("range_overlaps_multirange", Datum::Range(range), Datum::Multirange(multirange))
        | ("multirange_overlaps_range", Datum::Multirange(multirange), Datum::Range(range)) => {
            crabka_pgtypes::multirange::overlaps_range(multirange, range)?
        }
        ("multirange_overlaps_multirange", Datum::Multirange(left), Datum::Multirange(right)) => {
            crabka_pgtypes::multirange::overlaps(left, right)?
        }
        ("multirange_contains_elem", Datum::Multirange(multirange), element)
        | ("elem_contained_by_multirange", element, Datum::Multirange(multirange)) => {
            crabka_pgtypes::multirange::contains_element(multirange, element)?
        }
        ("multirange_contains_range", Datum::Multirange(multirange), Datum::Range(range))
        | ("range_contained_by_multirange", Datum::Range(range), Datum::Multirange(multirange)) => {
            crabka_pgtypes::multirange::contains_range(multirange, range)?
        }
        ("multirange_contains_multirange", Datum::Multirange(left), Datum::Multirange(right)) => {
            crabka_pgtypes::multirange::contains(left, right)?
        }
        (
            "multirange_contained_by_multirange",
            Datum::Multirange(left),
            Datum::Multirange(right),
        ) => crabka_pgtypes::multirange::contains(right, left)?,
        _ => return Err(type_error(name, &vals[0])),
    };
    Ok(Datum::Bool(result))
}

pub(crate) fn input_error(
    input: &str,
    type_name: &str,
    ctx: &EvalCtx,
) -> Result<Option<crabka_pgwire::error::PgError>, ExecError> {
    let time_zone = &ctx.time_zone;
    let ty = input_type(type_name).ok_or_else(|| ExecError::FunctionError {
        sqlstate: "42704",
        message: format!("type \"{type_name}\" does not exist"),
    })?;
    // A `reg*` input function resolves against the catalog, so the pure cast
    // this otherwise drives would report `invalid input syntax` for every name
    // rather than the missing-object error PostgreSQL reports. Route it through
    // the same resolver the cast operator uses.
    // A hard error still propagates: PostgreSQL's own `regproc.sql` records
    // `pg_input_error_info('way.too.many.names', 'regtype')` as an ERROR rather
    // than a row, and `crate::reg_fn::soft` is the same test that decides
    // whether `to_reg*` swallows it.
    if let Some(kind) = crate::reg_fn::RegKind::of(ty) {
        let value = Datum::Text(input.to_string());
        return match crate::reg_fn::reg_cast(kind, &value, ctx) {
            Ok(_) => Ok(None),
            Err(error) if crate::reg_fn::soft(&error) => Ok(Some(error.into_pg())),
            Err(error) => Err(error),
        };
    }
    // `bit_in` / `varbit_in` take the length modifier themselves and reject a
    // mismatch, where the *cast* to `bit(n)` would silently pad or truncate.
    // `pg_input_error_info('01010001', 'bit(10)')` is the input function, so it
    // must report `bit string length 8 does not match type bit(10)`.
    if let ColumnType::Bit(len) | ColumnType::VarBit(len) = ty {
        let varying = matches!(ty, ColumnType::VarBit(_));
        return Ok(
            crabka_pgtypes::BitString::parse_with_typmod(input, len, varying)
                .err()
                .map(|error| ExecError::from(error).into_pg()),
        );
    }
    let value = Datum::Text(input.to_string());
    let result = if matches!(ty, ColumnType::Varchar(Some(_)) | ColumnType::Char(Some(_))) {
        crabka_pgtypes::cast::cast_assign(&value, ty, time_zone).map_err(ExecError::from)
    } else {
        crate::eval::cast_value(&value, ty, time_zone)
    };
    Ok(result.err().map(ExecError::into_pg))
}

/// Resolve the typmod spelling accepted by `regtype` arguments to PostgreSQL's
/// soft-input functions. The ordinary expression parser already applies these
/// modifiers; this text argument reaches the type layer directly.
fn input_type(type_name: &str) -> Option<ColumnType> {
    let normalized = type_name.trim().to_ascii_lowercase();
    let Some(body) = normalized.strip_suffix(')') else {
        // The grammar's `bit` production defaults to `bit(1)`, while a bare
        // `bit varying` stays unconstrained — the same asymmetry the
        // expression parser applies.
        if normalized == "bit" {
            return Some(ColumnType::Bit(Some(1)));
        }
        return ColumnType::from_sql_name(&normalized);
    };
    let (base, modifier) = body.split_once('(')?;
    let parts = modifier.split(',').map(str::trim).collect::<Vec<_>>();
    match (base.trim(), parts.as_slice()) {
        ("varchar" | "character varying", [limit]) => {
            Some(ColumnType::Varchar(Some(limit.parse().ok()?)))
        }
        ("char" | "character" | "bpchar", [limit]) => {
            Some(ColumnType::Char(Some(limit.parse().ok()?)))
        }
        ("bit", [len]) => Some(ColumnType::Bit(Some(len.parse().ok()?))),
        ("varbit" | "bit varying", [len]) => Some(ColumnType::VarBit(Some(len.parse().ok()?))),
        ("numeric" | "decimal", [precision]) => {
            Some(ColumnType::Numeric(Some(crabka_pgtypes::numeric::Typmod {
                precision: precision.parse().ok()?,
                scale: 0,
            })))
        }
        ("numeric" | "decimal", [precision, scale]) => {
            Some(ColumnType::Numeric(Some(crabka_pgtypes::numeric::Typmod {
                precision: precision.parse().ok()?,
                scale: scale.parse().ok()?,
            })))
        }
        _ => None,
    }
}

// ---- argument-type helpers ----

/// 42883 for an argument whose static type a function does not accept (PG's
/// "no function matches the given name and argument types").
pub(crate) fn no_matching_function() -> ExecError {
    ExecError::UndefinedFunction("no function matches the given name and argument types".into())
}

/// Require the argument to statically type as a string. A bare `NULL` qualifies,
/// because it types as text. Otherwise the function does not exist for it
/// (42883).
///
/// `varchar(n)` and `char(n)` count. PostgreSQL declares its string functions on
/// `text` alone, but `varchar` and `bpchar` are binary-coercible to it, so a call
/// like `length(a_varchar_column)` resolves through that implicit cast. A match
/// on `Text` alone would reject every string function on a `varchar`/`char`
/// column.
fn require_text(arg: &Expr, scope: &Scope) -> Result<(), ExecError> {
    if crate::eval::infer_type(arg, scope)?.is_string() {
        Ok(())
    } else {
        Err(no_matching_function())
    }
}

fn require_uuid(arg: &Expr, scope: &Scope) -> Result<(), ExecError> {
    if is_unknown_literal(arg) || crate::eval::infer_type(arg, scope)? == ColumnType::Uuid {
        Ok(())
    } else {
        Err(no_matching_function())
    }
}

/// Does this argument select the `bytea` overload of a name `text` also owns?
///
/// Only a *known* `bytea` does. An `unknown` literal must not: `substr('abc',
/// 2)` has to stay the text call, and PostgreSQL resolves it that way because
/// `text` is the preferred type of the string category.
fn is_bytea_subject(arg: &Expr, scope: &Scope) -> Result<bool, ExecError> {
    Ok(crate::eval::infer_type(arg, scope)? == ColumnType::Bytea)
}

/// Require an argument the `bytea` overload accepts: `bytea` itself, or an
/// `unknown` literal that the coercion will run `byteain` over.
fn require_bytea(arg: &Expr, scope: &Scope) -> Result<(), ExecError> {
    if is_unknown_literal(arg) || crate::eval::infer_type(arg, scope)? == ColumnType::Bytea {
        Ok(())
    } else {
        Err(no_matching_function())
    }
}

/// Require the argument to statically type as an integer. Returns that width.
fn require_int(arg: &Expr, scope: &Scope) -> Result<ColumnType, ExecError> {
    match crate::eval::infer_type(arg, scope)? {
        t @ (ColumnType::Int2 | ColumnType::Int4 | ColumnType::Int8) => Ok(t),
        _ => Err(no_matching_function()),
    }
}

fn require_bool(arg: &Expr, scope: &Scope) -> Result<(), ExecError> {
    if crate::eval::infer_type(arg, scope)?.storage_type() == ColumnType::Bool {
        Ok(())
    } else {
        Err(no_matching_function())
    }
}

/// SP32: require an int OR numeric argument. These are the `mod` operand types,
/// because PostgreSQL has no `float8` modulo.
fn require_int_or_numeric(arg: &Expr, scope: &Scope) -> Result<ColumnType, ExecError> {
    // An `unknown` operand constrains nothing; the caller's other operand picks
    // the overload, and `mod`'s widest candidate settles an all-unknown pair —
    // which `scalar_result_type` has already rejected as ambiguous.
    if is_unknown_literal(arg) {
        return Ok(ColumnType::Int4);
    }
    let t = crate::eval::infer_type(arg, scope)?;
    if matches!(t, ColumnType::Int2 | ColumnType::Int4 | ColumnType::Int8) || t.is_numeric() {
        Ok(t)
    } else {
        Err(no_matching_function())
    }
}

/// SP30/SP32: require a numeric argument (int4/int8/float8/numeric). Returns
/// that type so the caller (`abs`) can preserve it.
/// This folds `real` onto `double precision` for the functions PostgreSQL
/// overloads on `float8` but not `float4` (`floor`, `ceil`, `round`, `trunc`,
/// `sign`, `sqrt`, …). Those calls resolve through the implicit widening cast,
/// so their result is `double precision`, not `real`.
fn float4_widens(t: ColumnType) -> ColumnType {
    if t == ColumnType::Float4 {
        ColumnType::Float8
    } else {
        t
    }
}

fn require_numeric(arg: &Expr, scope: &Scope) -> Result<ColumnType, ExecError> {
    // An `unknown` argument constrains nothing, and every candidate set that
    // reaches here prefers `float8` — which is why `sqrt(NULL)` is a `double
    // precision` NULL rather than an unresolvable call.
    if is_unknown_literal(arg) {
        return Ok(ColumnType::Float8);
    }
    let t = crate::eval::infer_type(arg, scope)?;
    if matches!(
        t,
        ColumnType::Int2
            | ColumnType::Int4
            | ColumnType::Int8
            | ColumnType::Float4
            | ColumnType::Float8
    ) || t.is_numeric()
    {
        Ok(t)
    } else {
        Err(no_matching_function())
    }
}

/// Unify every argument's type into one. This is PostgreSQL's
/// `select_common_type` for `COALESCE`/`GREATEST`/`LEAST`/`NULLIF`. The choice
/// ignores `unknown` inputs, which are a bare `NULL` and an unadorned string
/// literal, and an argument list that is entirely `unknown` resolves to `text`.
/// Incompatible known types are 42804, spelled the way PostgreSQL spells them,
/// with the construct's name.
fn unify_args(f: ScalarFunc, args: &[Expr], scope: &Scope) -> Result<ColumnType, ExecError> {
    let mut acc: Option<ColumnType> = None;
    for a in args {
        if is_unknown_literal(a) {
            continue;
        }
        acc = crate::eval::unify_branch(acc, a, scope).map_err(|e| name_mismatch(f, e))?;
    }
    Ok(acc.unwrap_or(ColumnType::Text))
}

/// Is `e` an argument PostgreSQL still calls `unknown` at this point?
fn is_unknown_literal(e: &Expr) -> bool {
    matches!(e, Expr::StringLiteral(_) | Expr::NullLiteral)
}

/// Evaluate `args` and apply PostgreSQL's common-type resolution to the result.
/// This coerces every `unknown` string literal to the type the other arguments
/// settled on. `greatest`/`least`/`nullif` all compare their arguments against
/// one another, so they need one common type before `ops::compare` runs.
/// Without it, `greatest(1, '2')` would try to order an integer against text.
fn resolved_args(
    f: ScalarFunc,
    args: &[Expr],
    scope: Option<&Scope>,
    ctx: &EvalCtx,
    mut eval_child: impl FnMut(&Expr) -> Result<Datum, ExecError>,
) -> Result<Vec<Datum>, ExecError> {
    let mut vals = args
        .iter()
        .map(&mut eval_child)
        .collect::<Result<Vec<_>, _>>()?;
    let common = match scope {
        Some(scope) => Some(unify_args(f, args, scope)?),
        None => args
            .iter()
            .zip(&vals)
            .filter(|(a, _)| !is_unknown_literal(a))
            .filter_map(|(_, v)| v.column_type())
            .try_fold(None, |acc: Option<ColumnType>, t| match acc {
                None => Ok(Some(t)),
                Some(a) => crate::eval::unify_types(a, t).map(Some),
            })?,
    };
    let Some(common) = common else {
        return Ok(vals);
    };
    for (a, v) in args.iter().zip(&mut vals) {
        if is_unknown_literal(a) && !v.is_null() {
            *v = crate::eval::cast_value(v, common, &ctx.time_zone)?;
        }
    }
    Ok(vals)
}

/// The type name `pg_typeof` reports: the evaluated value's own type. It falls
/// back to an explicit cast's target when the value is NULL, and to PostgreSQL's
/// `unknown` for a literal that never acquired one.
fn typeof_name(arg: &Expr, value: &Datum, scope: Option<&Scope>) -> String {
    if is_unknown_literal(arg) {
        return "unknown".into();
    }
    if let Some(scope) = scope
        && let Ok(ty) = crate::eval::infer_type(arg, scope)
    {
        return type_display_name(ty);
    }
    // A domain's *value* is a base-type value; only the expression records that
    // it went through the domain, so an explicit cast to one is read off the
    // node ahead of the value.
    if let Expr::Cast {
        ty: ty @ (ColumnType::Domain(_) | ColumnType::JsonPath),
        ..
    } = arg
    {
        return type_display_name(*ty);
    }
    if let Some(t) = value.column_type() {
        return type_display_name(t);
    }
    match arg {
        Expr::Cast { ty, .. } => type_display_name(*ty),
        _ => "unknown".into(),
    }
}

/// The name `pg_typeof`/`format_type` print for a type: an array renders as
/// `element[]`, everything else as its bare SQL name.
fn type_display_name(t: ColumnType) -> String {
    match t {
        ColumnType::Array(elem) => elem.array_name().to_string(),
        other => other.name().to_string(),
    }
}

/// `format_type`'s first argument is declared `oid`, and every `reg*` reaches
/// `oid` by a binary coercion — so `format_type('int4'::regtype, -1)` and
/// `format_type(to_regtype('varchar(32)'), 36)` both type-check, which is the
/// spelling PostgreSQL's own `regproc.sql` uses.
fn require_oid_or_null(arg: &Expr, scope: &Scope) -> Result<(), ExecError> {
    if matches!(arg, Expr::NullLiteral) {
        return Ok(());
    }
    let ty = crate::eval::infer_type(arg, scope)?;
    if ty.is_reg() || ty == ColumnType::Oid {
        return Ok(());
    }
    require_int(arg, scope).map(|_| ())
}

/// Require an integer argument, or a bare `NULL` (which PostgreSQL resolves to
/// the parameter's own type rather than rejecting).
fn require_int_or_null(arg: &Expr, scope: &Scope) -> Result<(), ExecError> {
    if matches!(arg, Expr::NullLiteral) {
        return Ok(());
    }
    require_int(arg, scope).map(|_| ())
}

/// PostgreSQL `format_type_be(oid)`: the SQL-standard spelling of a type, named
/// with **no** modifier in hand. An unrecognized OID is `-`, matching
/// PostgreSQL's placeholder for a type that no longer exists.
///
/// A negative `typmod` means "this type carries no modifier", not "a modifier
/// of -1 was supplied". The two readings differ for exactly the two types whose
/// bare grammar keyword implies a length; [`format_type_given`] is the other
/// reading.
pub(crate) fn format_type(oid: i64, typmod: i64) -> String {
    format_type_extended(oid, typmod, typmod >= 0)
}

/// PostgreSQL `format_type_extended(oid, typmod, FORMAT_TYPE_TYPEMOD_GIVEN)`:
/// the caller states a modifier, and `-1` states that the value has none.
///
/// `bit` and `bpchar` are the two types where that is not the same as saying
/// nothing. Their bare grammar keywords mean `bit(1)` and `character(1)`, so a
/// value that really carries no length has to be named in a spelling the parser
/// will not re-decorate: `"bit"` in quotes, and `bpchar` under PostgreSQL's
/// internal name. Naming the same type with no modifier in hand instead prints
/// the SQL keyword, `bit` and `character`.
pub(crate) fn format_type_given(oid: i64, typmod: i64) -> String {
    format_type_extended(oid, typmod, true)
}

fn format_type_extended(oid: i64, typmod: i64, given: bool) -> String {
    let Ok(oid) = u32::try_from(oid) else {
        return "-".to_string();
    };
    let Some((base, kind)) = builtin_format_type(oid) else {
        return "-".to_string();
    };
    let modifier = if typmod < 0 || !given {
        String::new()
    } else {
        type_modifier(kind, typmod)
    };
    let (element, suffix) = match base.strip_suffix("[]") {
        Some(element) => (element, "[]"),
        None => (base, ""),
    };
    if modifier.is_empty() {
        // With a modifier stated and none to state, the name has to survive
        // being read back: `character` would come back as `character(1)`.
        let element = match (element, given) {
            ("bpchar", false) => "character",
            ("bit", true) => "\"bit\"",
            (element, _) => element,
        };
        return format!("{element}{suffix}");
    }
    // `bpchar` is PostgreSQL's internal name for an unmodified blank-padded
    // char; once a length is attached it prints as the SQL spelling.
    let element = if element == "bpchar" {
        "character"
    } else {
        element
    };
    // A fractional-seconds precision goes right after the type word, before the
    // `with`/`without time zone` tail: `timestamp(3) with time zone`.
    match (kind, element.split_once(' ')) {
        (TypmodKind::Seconds, Some((head, tail))) => {
            format!("{head}{modifier} {tail}{suffix}")
        }
        _ => format!("{element}{modifier}{suffix}"),
    }
}

/// How a type spells its `typmod`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum TypmodKind {
    /// Nothing ever prints a modifier (`integer`, `text`, …).
    None,
    /// A character length, stored with the 4-byte varlena header included.
    Length,
    /// `numeric(precision, scale)`, packed into one int32.
    PrecisionScale,
    /// A fractional-seconds precision (`timestamp(3)`), printed before the
    /// `with/without time zone` tail.
    Seconds,
    /// `interval(p)`: precision is stored in the low word beneath the field mask.
    Interval,
    /// A `bit(n)` / `bit varying(n)` length, stored raw. Distinct from
    /// [`TypmodKind::Seconds`] even though both print the stored word, because
    /// `bit varying` has a space in its name and the modifier goes at the END:
    /// `bit varying(5)`, never `bit(5) varying`.
    Bits,
    /// `printTypmod`'s fallback for a type with no `typmodout`: the stored word
    /// is printed verbatim. The geometric types are the case — none of them
    /// accepts a modifier (`point(4)` as a declaration is `42601 type modifier
    /// is not allowed for type "point"`), so this is only ever reached by
    /// calling `format_type` with a typmod the type could not have carried,
    /// where `PostgreSQL` still prints `point(4)` rather than dropping it.
    Verbatim,
}

fn type_modifier(kind: TypmodKind, typmod: i64) -> String {
    match kind {
        TypmodKind::None => String::new(),
        TypmodKind::Length => format!("({})", typmod - 4),
        TypmodKind::PrecisionScale => {
            let packed = typmod - 4;
            format!("({},{})", (packed >> 16) & 0xffff, packed & 0xffff)
        }
        TypmodKind::Seconds | TypmodKind::Bits | TypmodKind::Verbatim => format!("({typmod})"),
        TypmodKind::Interval => crabka_pgtypes::IntervalTypmod::from_typmod(typmod as i32)
            .map_or_else(
                || format!("({})", typmod & 0xffff),
                |typmod| typmod.suffix(),
            ),
    }
}

/// The built-in types `format_type` knows, as `(printed name, typmod spelling)`.
/// The name carries a `[]` suffix for an array type, and the modifier goes in
/// before that suffix (`character varying(6)[]`).
fn builtin_format_type(oid: u32) -> Option<(&'static str, TypmodKind)> {
    use TypmodKind::{Bits, Interval, Length, None as NoMod, PrecisionScale, Seconds, Verbatim};
    Some(match oid {
        // The seven geometric types. None accepts a modifier, so `Verbatim` is
        // only reached through a direct `format_type(600, 4)` call; the reason
        // they are here at all is that psql's `\d` renders every column through
        // `format_type`, and without these arms it printed `-` for the type.
        crabka_pgtypes::oids::POINT => ("point", Verbatim),
        crabka_pgtypes::oids::LSEG => ("lseg", Verbatim),
        crabka_pgtypes::oids::PATH => ("path", Verbatim),
        crabka_pgtypes::oids::BOX => ("box", Verbatim),
        crabka_pgtypes::oids::POLYGON => ("polygon", Verbatim),
        crabka_pgtypes::oids::LINE => ("line", Verbatim),
        crabka_pgtypes::oids::CIRCLE => ("circle", Verbatim),
        790 => ("money", NoMod),
        791 => ("money[]", NoMod),
        1560 => ("bit", Bits),
        1561 => ("bit[]", Bits),
        1562 => ("bit varying", Bits),
        1563 => ("bit varying[]", Bits),
        16 => ("boolean", NoMod),
        17 => ("bytea", NoMod),
        18 => ("\"char\"", NoMod),
        19 => ("name", NoMod),
        20 => ("bigint", NoMod),
        21 => ("smallint", NoMod),
        23 => ("integer", NoMod),
        25 => ("text", NoMod),
        26 => ("oid", NoMod),
        27 => ("tid", NoMod),
        28 => ("xid", NoMod),
        29 => ("cid", NoMod),
        30 => ("oidvector", NoMod),
        22 => ("int2vector", NoMod),
        271 => ("xid8[]", NoMod),
        1010 => ("tid[]", NoMod),
        1011 => ("xid[]", NoMod),
        1012 => ("cid[]", NoMod),
        1028 => ("oid[]", NoMod),
        3220 => ("pg_lsn", NoMod),
        3221 => ("pg_lsn[]", NoMod),
        5069 => ("xid8", NoMod),
        2949 => ("txid_snapshot[]", NoMod),
        2970 => ("txid_snapshot", NoMod),
        5038 => ("pg_snapshot", NoMod),
        5039 => ("pg_snapshot[]", NoMod),
        114 => ("json", NoMod),
        142 => ("xml", NoMod),
        199 => ("json[]", NoMod),
        700 => ("real", NoMod),
        701 => ("double precision", NoMod),
        705 => ("unknown", NoMod),
        1000 => ("boolean[]", NoMod),
        1001 => ("bytea[]", NoMod),
        1005 => ("smallint[]", NoMod),
        1007 => ("integer[]", NoMod),
        1009 => ("text[]", NoMod),
        1014 => ("bpchar[]", Length),
        1015 => ("character varying[]", Length),
        1016 => ("bigint[]", NoMod),
        1021 => ("real[]", NoMod),
        1022 => ("double precision[]", NoMod),
        1042 => ("bpchar", Length),
        1043 => ("character varying", Length),
        1082 => ("date", NoMod),
        1083 => ("time without time zone", Seconds),
        1114 => ("timestamp without time zone", Seconds),
        1115 => ("timestamp without time zone[]", Seconds),
        1182 => ("date[]", NoMod),
        1183 => ("time without time zone[]", Seconds),
        1184 => ("timestamp with time zone", Seconds),
        1185 => ("timestamp with time zone[]", Seconds),
        1186 => ("interval", Interval),
        1187 => ("interval[]", Interval),
        1231 => ("numeric[]", PrecisionScale),
        1266 => ("time with time zone", Seconds),
        1700 => ("numeric", PrecisionScale),
        // The `reg*` family. `format_type` is what `psql`'s `\d` prints in the
        // Type column and what `pg_attribute` reports, so a missing entry here
        // is a blank column rather than a wrong one.
        24 => ("regproc", NoMod),
        2202 => ("regprocedure", NoMod),
        2203 => ("regoper", NoMod),
        2204 => ("regoperator", NoMod),
        2205 => ("regclass", NoMod),
        2206 => ("regtype", NoMod),
        3734 => ("regconfig", NoMod),
        3769 => ("regdictionary", NoMod),
        4089 => ("regnamespace", NoMod),
        4096 => ("regrole", NoMod),
        4191 => ("regcollation", NoMod),
        2278 => ("void", NoMod),
        2950 => ("uuid", NoMod),
        2951 => ("uuid[]", NoMod),
        143 => ("xml[]", NoMod),
        3802 => ("jsonb", NoMod),
        3807 => ("jsonb[]", NoMod),
        3614 => ("tsvector", NoMod),
        3615 => ("tsquery", NoMod),
        3643 => ("tsvector[]", NoMod),
        3645 => ("tsquery[]", NoMod),
        _ => return Option::None,
    })
}

/// PostgreSQL prefixes the "types … cannot be matched" message with the
/// construct's SQL name (`COALESCE types integer and text cannot be matched`).
fn name_mismatch(f: ScalarFunc, e: ExecError) -> ExecError {
    let name = match f {
        ScalarFunc::Coalesce => "COALESCE",
        ScalarFunc::Greatest => "GREATEST",
        ScalarFunc::Least => "LEAST",
        _ => "NULLIF",
    };
    match e {
        ExecError::TypeMismatch(message) => ExecError::TypeMismatch(format!("{name} {message}")),
        other => other,
    }
}

/// Is `e` an argument PostgreSQL still calls `unknown`? This is public so the
/// sibling function modules can apply the same overload-resolution rules.
pub(crate) fn is_unknown_arg(e: &Expr) -> bool {
    is_unknown_literal(e)
}

/// 42725: PostgreSQL cannot choose between a function's overloads, because
/// every argument is still `unknown` and no candidate is preferred. The
/// families that have two or more equally-good numeric overloads raise it
/// (`gcd`, `lcm`, `to_hex`, the two-argument `random`).
pub(crate) fn ambiguous_function(name: &str, arity: usize) -> ExecError {
    let spelled = vec!["unknown"; arity].join(", ");
    ExecError::FunctionError {
        sqlstate: "42725",
        message: format!("function {name}({spelled}) is not unique"),
    }
}

/// 42883 that spells out the argument types PostgreSQL could not match, so a
/// bad call reads `function concat() does not exist` and not a generic `(...)`.
pub(crate) fn undefined_function_spelled(name: &str, args: &[Expr], scope: &Scope) -> ExecError {
    let spelled: Vec<&str> = args
        .iter()
        .map(|a| {
            if is_unknown_literal(a) {
                "unknown"
            } else {
                crate::eval::infer_type(a, scope).map_or("unknown", ColumnType::name)
            }
        })
        .collect();
    ExecError::UndefinedFunction(format!(
        "function {name}({}) does not exist",
        spelled.join(", ")
    ))
}

fn promote(a: ColumnType, b: ColumnType) -> ColumnType {
    if a == ColumnType::Int2 && b == ColumnType::Int2 {
        ColumnType::Int2
    } else if matches!(a, ColumnType::Int2 | ColumnType::Int4)
        && matches!(b, ColumnType::Int2 | ColumnType::Int4)
    {
        ColumnType::Int4
    } else {
        ColumnType::Int8
    }
}

pub(crate) fn require_arity(fc: &FuncCall, ok: bool) -> Result<(), ExecError> {
    if ok {
        Ok(())
    } else {
        Err(undefined_function(&fc.name))
    }
}

fn lo_runtime(
    ctx: &EvalCtx,
) -> Result<&std::sync::Arc<crate::clock::LargeObjectRuntime>, ExecError> {
    ctx.largeobject.as_ref().ok_or_else(|| {
        ExecError::Unsupported("large-object functions require a SQL session".into())
    })
}

fn lo_file_runtime<'a>(
    ctx: &'a EvalCtx,
    function: &str,
    changes_database: bool,
) -> Result<&'a std::sync::Arc<crate::clock::LargeObjectRuntime>, ExecError> {
    let runtime = lo_runtime(ctx)?;
    let role = crate::catalog_fn::effective_privilege_role(&ctx.current_user);
    if !crate::rls::role_is_superuser(runtime.kv.as_ref(), &role)? {
        return Err(ExecError::FunctionError {
            sqlstate: "42501",
            message: format!("permission denied for function {function}"),
        });
    }
    if changes_database && runtime.read_only {
        return Err(ExecError::FunctionError {
            sqlstate: "25006",
            message: format!("cannot execute {function}() in a read-only transaction"),
        });
    }
    Ok(runtime)
}

fn lo_oid(value: &Datum) -> Result<u32, ExecError> {
    u32::try_from(int_arg(value)?).map_err(|_| ExecError::FunctionError {
        sqlstate: "22003",
        message: "OID out of range".into(),
    })
}

fn lo_offset_error() -> ExecError {
    ExecError::FunctionError {
        sqlstate: "22003",
        message: "large object seek target out of range".into(),
    }
}

/// A text argument at runtime. A non-text Datum here means the call sat in a
/// non-projected position, so `scalar_result_type` never type-checked it.
/// PostgreSQL rejects it at plan time (42883); Gres surfaces it at runtime
/// (42804).
/// Queue one `pg_notify(channel, payload)` on the session's pending
/// notification list. This is the same list a `NOTIFY` statement writes to, so
/// both deliver at the same point (commit, or the end of an autocommit
/// statement) and dedup against each other.
fn eval_pg_notify(channel: &Datum, payload: &Datum, ctx: &EvalCtx) -> Result<Datum, ExecError> {
    let channel = nullable_text_arg(channel)?;
    let payload = nullable_text_arg(payload)?;
    let pending = ctx
        .notify
        .as_ref()
        .ok_or_else(|| ExecError::Unsupported("pg_notify requires a SQL session".into()))?;
    pending
        .lock()
        .expect("notify pending mutex")
        .queue_notify(channel, payload)
        .map_err(crate::session::notify_queue_error)?;
    Ok(Datum::Text(String::new()))
}

/// A text argument of a non-strict function, with PostgreSQL's NULL-as-empty
/// -string conversion.
fn nullable_text_arg(d: &Datum) -> Result<&str, ExecError> {
    match d {
        Datum::Null => Ok(""),
        other => text_arg(other),
    }
}

fn text_arg(d: &Datum) -> Result<&str, ExecError> {
    match d {
        Datum::Text(s) => Ok(s),
        other => Err(type_error("function", other)),
    }
}

/// A `bytea` argument's bytes.
///
/// The plan gate lets an `unknown` literal stand where a `bytea` is wanted,
/// exactly as PostgreSQL's coercion does, and such a literal reaches evaluation
/// still spelled as text. Running `byteain` over it here is what makes
/// `btrim(b, '\x00')` agree with `btrim(b, '\x00'::bytea)` — reading the
/// literal's own UTF-8 bytes instead would silently trim four ASCII characters
/// rather than one NUL.
fn bytea_arg<'a>(d: &'a Datum, ctx: &EvalCtx) -> Result<std::borrow::Cow<'a, [u8]>, ExecError> {
    match d {
        Datum::Bytea(bytes) => Ok(std::borrow::Cow::Borrowed(bytes)),
        Datum::Text(_) => match crabka_pgtypes::cast::cast(d, ColumnType::Bytea, &ctx.time_zone)? {
            Datum::Bytea(bytes) => Ok(std::borrow::Cow::Owned(bytes)),
            _ => unreachable!("a cast to bytea yields bytea"),
        },
        other => Err(type_error("function", other)),
    }
}

/// An integer argument at runtime, promoted to i64.
/// The advisory-lock key: the single `int8` spelling, or `PostgreSQL`'s packing
/// of the two-`int4` spelling into one `int8`.
fn advisory_key(vals: &[Datum]) -> Result<i64, ExecError> {
    match vals {
        [key] => int_arg(key),
        [high, low] => {
            let high = i32::try_from(int_arg(high)?)
                .map_err(|_| ExecError::InvalidParameterValue("advisory lock key".into()))?;
            let low = i32::try_from(int_arg(low)?)
                .map_err(|_| ExecError::InvalidParameterValue("advisory lock key".into()))?;
            Ok(crate::lockmgr::AdvisoryLockManager::pack_key(high, low))
        }
        _ => Err(ExecError::InvalidParameterValue("advisory lock key".into())),
    }
}

/// A bit-string function's `int4` position argument. The bit routines take
/// `int32` throughout — `SUBSTRING(b FROM 2 FOR 2147483646)` relies on the
/// overflow of `start + count` in 32 bits meaning "to the end of the string" —
/// so a wider value is out of range rather than silently clamped.
fn bit_index(d: &Datum) -> Result<i32, ExecError> {
    match d {
        Datum::Int2(n) => Ok(i32::from(*n)),
        Datum::Int4(n) => Ok(*n),
        Datum::Int8(n) => {
            i32::try_from(*n).map_err(|_| ExecError::Type(crabka_pgtypes::TypeError::Overflow))
        }
        other => Err(type_error("function", other)),
    }
}

pub(crate) fn int_arg(d: &Datum) -> Result<i64, ExecError> {
    match d {
        Datum::Int2(n) => Ok(i64::from(*n)),
        Datum::Int4(n) => Ok(i64::from(*n)),
        Datum::Int8(n) => Ok(*n),
        // Every catalog function that takes an object identifier reads it
        // through here, and psql writes those arguments as an explicit
        // `'123'::pg_catalog.oid` rather than passing a catalog column. Without
        // this arm `\d+` on a view fails: `pg_get_viewdef('123'::oid, true)`
        // reports that the function does not accept an argument of type oid.
        // PostgreSQL declares int2/int4/int8 -> oid implicit, so widening the
        // other direction here is the same coercion read backwards.
        Datum::Oid(n) => Ok(i64::from(*n)),
        // A `reg*` is binary-coercible to `oid` in both directions, so the same
        // arguments read as `'v'::regclass`. Without this arm
        // `pg_get_viewdef('v'::regclass)` — the spelling PostgreSQL's own
        // `pg_get_viewdef(regclass)` overload takes — reports that the function
        // does not accept an argument of type regclass.
        Datum::Regclass(value) => Ok(i64::from(value.oid)),
        other => Err(type_error("function", other)),
    }
}

fn bool_arg(d: &Datum) -> Result<bool, ExecError> {
    match d {
        Datum::Bool(value) => Ok(*value),
        other => Err(type_error("function", other)),
    }
}

pub(crate) fn type_error(what: &str, got: &Datum) -> ExecError {
    ExecError::TypeMismatch(format!(
        "{what} does not accept an argument of type {}",
        got.column_type().map(|t| t.name()).unwrap_or("unknown")
    ))
}

/// The canonical text rendering of a non-NULL Datum, which is the wire text
/// encoding. So `concat` agrees with the DataRow output and with the `||`
/// operator.
pub(crate) fn text_render(d: &Datum, tz: &jiff::tz::TimeZone) -> String {
    String::from_utf8(crabka_pgtypes::encoding::encode_text(d, tz))
        .expect("a Datum's text encoding is always valid UTF-8")
}

/// [`text_render`] in the session's output styles rather than the canonical
/// ones. This is what a *user-visible* rendering wants: the deparser prints a
/// stored constant through its type's output function exactly as `ruleutils.c`
/// does, so `DateStyle` and `IntervalStyle` reach it.
pub(crate) fn text_render_in(
    d: &Datum,
    style: crabka_pgtypes::encoding::OutputStyle<'_>,
) -> String {
    String::from_utf8(crabka_pgtypes::encoding::encode_text_in(d, style))
        .expect("a Datum's text encoding is always valid UTF-8")
}

// ---- rounding helpers (SP33) ----

/// Rounding-family value transform. `scale` is `Some` only for the two-arg
/// `round`/`trunc` form, which always yields numeric and promotes an int first
/// argument to numeric. The one-arg form preserves the input numeric type.
fn round_family(f: ScalarFunc, v: &Datum, scale: Option<i64>) -> Result<Datum, ExecError> {
    use crabka_pgtypes::numeric as num;
    if let Some(n) = scale {
        let bd = match v {
            Datum::Int2(i) => num::from_i64(i64::from(*i)),
            Datum::Int4(i) => num::from_i64(i64::from(*i)),
            Datum::Int8(i) => num::from_i64(*i),
            Datum::Numeric(d) => d.clone(),
            other => return Err(type_error("function", other)),
        };
        return Ok(Datum::Numeric(match f {
            ScalarFunc::Round => num::round(&bd, n),
            ScalarFunc::Trunc => num::trunc(&bd, n),
            _ => unreachable!("scale is only set for round/trunc"),
        }));
    }
    match v {
        Datum::Int2(_) | Datum::Int4(_) | Datum::Int8(_) => match f {
            ScalarFunc::Sign => sign_int(v),
            _ => Ok(v.clone()), // floor/ceil/round/trunc of an integer is itself
        },
        // `real` has no overload of its own here, so PostgreSQL widens it to
        // `double precision` — the result type `float4_widens` already reports.
        Datum::Float4(x) => round_family(f, &Datum::Float8(f64::from(*x)), None),
        Datum::Float8(x) => Ok(Datum::Float8(match f {
            ScalarFunc::Floor => x.floor(),
            ScalarFunc::Ceil => x.ceil(),
            ScalarFunc::Round => x.round_ties_even(), // PG float8 round = half-to-even
            ScalarFunc::Trunc => x.trunc(),
            ScalarFunc::Sign => float_sign(*x),
            _ => unreachable!(),
        })),
        Datum::Numeric(d) => Ok(Datum::Numeric(match f {
            ScalarFunc::Floor => num::floor(d),
            ScalarFunc::Ceil => num::ceil(d),
            ScalarFunc::Round => num::round(d, 0),
            ScalarFunc::Trunc => num::trunc(d, 0),
            ScalarFunc::Sign => num::sign(d),
            _ => unreachable!(),
        })),
        other => Err(type_error("function", other)),
    }
}

/// `sign` of an integer, with its width preserved.
fn sign_int(v: &Datum) -> Result<Datum, ExecError> {
    Ok(match v {
        Datum::Int2(n) => Datum::Int2(n.signum()),
        Datum::Int4(n) => Datum::Int4(n.signum()),
        Datum::Int8(n) => Datum::Int8(n.signum()),
        other => return Err(type_error("sign", other)),
    })
}

// ---- transcendental helpers (SP33) ----

/// Coerce any numeric Datum (int4/int8/float8/numeric) to f64 for the
/// transcendental functions, which always compute in float8.
fn as_f64(d: &Datum) -> Result<f64, ExecError> {
    Ok(match d {
        Datum::Int2(n) => f64::from(*n),
        Datum::Int4(n) => f64::from(*n),
        Datum::Int8(n) => *n as f64,
        Datum::Float4(x) => f64::from(*x),
        Datum::Float8(x) => *x,
        Datum::Numeric(d) => crabka_pgtypes::numeric::to_f64(d),
        other => return Err(type_error("function", other)),
    })
}

/// Sleep in short increments so PostgreSQL's query-cancel flag interrupts a
/// long `pg_sleep`, just as its latch-based implementation does.
fn sleep_for(seconds: f64) -> Result<(), ExecError> {
    let started = Instant::now();
    loop {
        crate::session::check_query_canceled()?;
        let remaining = seconds - started.elapsed().as_secs_f64();
        if remaining.is_nan() || remaining <= 0.0 {
            return Ok(());
        }
        std::thread::sleep(Duration::from_secs_f64(remaining.min(0.01)));
    }
}

/// Build a domain error carrying its PostgreSQL SQLSTATE.
pub(crate) fn domain(sqlstate: &'static str, message: &'static str) -> ExecError {
    ExecError::Type(crabka_pgtypes::TypeError::Domain { sqlstate, message })
}

/// Wrap an f64 result and map an overflow to infinity onto 22003. This matches
/// the engine's float8 arithmetic, which treats a finite-to-infinite overflow as
/// out of range.
fn finite_or_overflow(x: f64) -> Result<Datum, ExecError> {
    if x.is_infinite() {
        Err(ExecError::Type(crabka_pgtypes::TypeError::Overflow))
    } else {
        Ok(Datum::Float8(x))
    }
}

/// PostgreSQL power result type. It is float8 if any operand is float8. If not,
/// it is numeric if any operand is numeric. Otherwise it is float8, the all-int
/// case, which is PG's preferred type.
/// Which `power` overload a call resolves to. PostgreSQL has both
/// `power(double precision, double precision)` and `power(numeric, numeric)`,
/// and an `unknown` literal constrains neither: `func_select_candidate` keeps
/// the candidate that needs no coercion of the argument that IS typed. So
/// `power('-2'::numeric, 'inf')` is `numeric` — and answers `Infinity` — where
/// forcing the untyped `'inf'` to `float8` would drag the whole call onto the
/// float path and raise instead.
fn power_overload(args: &[Expr], a: ColumnType, b: ColumnType) -> ColumnType {
    match (is_unknown_literal(&args[0]), is_unknown_literal(&args[1])) {
        (false, false) => power_result_type(a, b),
        (true, false) => power_result_type(b, b),
        (false, true) => power_result_type(a, a),
        (true, true) => ColumnType::Float8,
    }
}

fn power_result_type(a: ColumnType, b: ColumnType) -> ColumnType {
    let (a, b) = (float4_widens(a), float4_widens(b));
    if a == ColumnType::Float8 || b == ColumnType::Float8 {
        ColumnType::Float8
    } else if a.is_numeric() || b.is_numeric() {
        ColumnType::Numeric(None)
    } else {
        ColumnType::Float8
    }
}

/// Promote an int4/int8/numeric Datum to a [`NumericValue`] (for the numeric
/// power path, where one operand may be an integer).
pub(crate) fn to_numeric(d: &Datum) -> Result<crabka_pgtypes::numeric::NumericValue, ExecError> {
    match d {
        Datum::Int2(n) => Ok(crabka_pgtypes::numeric::from_i64(i64::from(*n))),
        Datum::Int4(n) => Ok(crabka_pgtypes::numeric::from_i64(i64::from(*n))),
        Datum::Int8(n) => Ok(crabka_pgtypes::numeric::from_i64(*n)),
        Datum::Numeric(d) => Ok(d.clone()),
        other => Err(type_error("power", other)),
    }
}

/// `power(base, exp)` with PostgreSQL's domain checks (2201F).
fn power(base: f64, exp: f64) -> Result<Datum, ExecError> {
    if base == 0.0 && exp < 0.0 {
        return Err(domain(
            "2201F",
            "zero raised to a negative power is undefined",
        ));
    }
    if base < 0.0 && exp.fract() != 0.0 {
        return Err(domain(
            "2201F",
            "a negative number raised to a non-integer power yields a complex result",
        ));
    }
    finite_or_overflow(base.powf(exp))
}

/// `sign` of a float8: −1 / 0 / 1, and `NaN` for `NaN` (PostgreSQL `dsign`).
fn float_sign(x: f64) -> f64 {
    if x.is_nan() {
        f64::NAN
    } else if x > 0.0 {
        1.0
    } else if x < 0.0 {
        -1.0
    } else {
        0.0
    }
}

fn eval_range_constructor(
    ty: RangeRef,
    fc: &FuncCall,
    vals: &[Datum],
    ctx: &EvalCtx,
) -> Result<Datum, ExecError> {
    require_arity(fc, (1..=3).contains(&vals.len()))?;
    if vals.len() == 1 {
        return crabka_pgtypes::cast::cast(&vals[0], ColumnType::Range(ty), &ctx.time_zone)
            .map_err(ExecError::from);
    }
    if vals.get(2).is_some_and(Datum::is_null) {
        return Ok(Datum::Null);
    }
    let bounds = vals.get(2).map_or(Ok("[)"), text_arg)?;
    let [left, right] = bounds.as_bytes() else {
        return Err(ExecError::Type(crabka_pgtypes::TypeError::Coded {
            sqlstate: "22023",
            message: "range constructor flags argument must be two characters".into(),
        }));
    };
    if !matches!(left, b'[' | b'(') || !matches!(right, b']' | b')') {
        return Err(ExecError::Type(crabka_pgtypes::TypeError::Coded {
            sqlstate: "22023",
            message: "range constructor flags argument must contain one of '(', '[' followed by one of ')', ']'".into(),
        }));
    }
    let cast_bound = |value: &Datum| -> Result<Option<Box<Datum>>, ExecError> {
        if value.is_null() {
            Ok(None)
        } else {
            Ok(Some(Box::new(crabka_pgtypes::cast::cast(
                value,
                *ty.subtype,
                &ctx.time_zone,
            )?)))
        }
    };
    let value = crabka_pgtypes::RangeValue {
        ty,
        lower: cast_bound(&vals[0])?,
        upper: cast_bound(&vals[1])?,
        lower_inclusive: *left == b'[',
        upper_inclusive: *right == b']',
        empty: false,
    };
    let literal = crabka_pgtypes::range::to_text(&value, |bound| {
        String::from_utf8(crabka_pgtypes::encoding::encode_text_in(
            bound,
            ctx.output_style(),
        ))
        .expect("a Datum's text encoding is always valid UTF-8")
    });
    Ok(Datum::Range(crabka_pgtypes::range::parse(
        &literal,
        ty,
        &ctx.time_zone,
    )?))
}

// ---- string helpers ----

fn trim_ws(f: ScalarFunc, s: &str) -> String {
    match f {
        ScalarFunc::Ltrim => s.trim_start().to_string(),
        ScalarFunc::Rtrim => s.trim_end().to_string(),
        _ => s.trim().to_string(), // btrim
    }
}

fn trim_set(f: ScalarFunc, s: &str, set: &[char]) -> String {
    let in_set = |c: char| set.contains(&c);
    match f {
        ScalarFunc::Ltrim => s.trim_start_matches(in_set).to_string(),
        ScalarFunc::Rtrim => s.trim_end_matches(in_set).to_string(),
        _ => s.trim_matches(in_set).to_string(), // btrim
    }
}

/// PostgreSQL `substr(string, start [, count])`. `start` is 1-based, and
/// characters before position 1 count against `count`. A negative `count` is an
/// error (22011). A NULL argument already short-circuited to NULL in
/// `eval_eager`.
/// `substring(string, posix_pattern)` returns the first match, or the first
/// parenthesized subexpression when the pattern has one, and NULL when the
/// pattern does not match at all.
fn posix_substring(s: &str, pattern: &str) -> Result<Datum, ExecError> {
    let regex = regex::Regex::new(pattern).map_err(|_| {
        ExecError::Type(crabka_pgtypes::TypeError::Domain {
            sqlstate: "2201B",
            message: "invalid regular expression",
        })
    })?;
    let Some(captures) = regex.captures(s) else {
        return Ok(Datum::Null);
    };
    // PostgreSQL reserves group 1 for exactly this: with parentheses the group
    // is the result, without them the whole match is.
    Ok(captures
        .get(1)
        .or_else(|| captures.get(0))
        .map_or(Datum::Null, |m| Datum::Text(m.as_str().to_string())))
}

/// `overlay(string, replacement, start, count)`: replace `count` characters
/// that start at `start` with `replacement`.
///
/// PostgreSQL defines it as `substring(s, 1, start - 1) || replacement ||
/// substring(s, start + count)`. That is why a `start` of 0 with a positive
/// `count` is a negative-length substring error and not an insertion at the
/// front.
fn overlay(s: &str, replacement: &str, start: i64, count: i64) -> Result<Datum, ExecError> {
    let prefix = substr(s, 1, Some(start - 1))?;
    let suffix = substr(s, start.saturating_add(count), None)?;
    let (Datum::Text(prefix), Datum::Text(suffix)) = (prefix, suffix) else {
        unreachable!("substr of text is text");
    };
    Ok(Datum::Text(format!("{prefix}{replacement}{suffix}")))
}

fn substr(s: &str, start: i64, count: Option<i64>) -> Result<Datum, ExecError> {
    let chars: Vec<char> = s.chars().collect();
    let len = chars.len() as i64;
    // The window is [start, end) in 1-based positions, clamped to [1, len+1).
    let end = match count {
        None => len + 1,
        Some(c) => {
            if c < 0 {
                return Err(ExecError::FunctionError {
                    sqlstate: "22011",
                    message: "negative substring length not allowed".into(),
                });
            }
            start.saturating_add(c)
        }
    };
    let lo = start.max(1);
    let hi = end.min(len + 1);
    if lo >= hi {
        return Ok(Datum::Text(String::new()));
    }
    let out: String = chars[(lo - 1) as usize..(hi - 1) as usize].iter().collect();
    Ok(Datum::Text(out))
}

// ---- string-family helpers (SP33) ----

/// PostgreSQL's ~1 GB field-size limit. It guards `repeat`/`lpad`/`rpad` against
/// an adversarially huge string. The engine raises 54000, "requested length too
/// large", and does not abort the process on an out-of-memory allocation.
const MAX_FIELD_SIZE: usize = 1 << 30;

/// 54000 (`program_limit_exceeded`): a call asked a string function to produce
/// a field larger than the engine permits.
fn length_too_large() -> ExecError {
    domain("54000", "requested length too large")
}

/// `lpad`/`rpad`: pad `s` to `width` chars with `fill`. When `s` is longer than
/// `width`, both forms truncate it to its first `width` chars. A `width <= 0`
/// yields the empty string. An empty `fill` that cannot pad leaves `s`
/// unchanged. A `width` beyond [`MAX_FIELD_SIZE`] is 54000, and not an OOM
/// allocation.
fn pad(f: ScalarFunc, s: &str, width: i64, fill: &str) -> Result<String, ExecError> {
    let chars: Vec<char> = s.chars().collect();
    let len = chars.len() as i64;
    if width <= 0 {
        return Ok(String::new());
    }
    if width as usize > MAX_FIELD_SIZE {
        return Err(length_too_large());
    }
    if len >= width {
        return Ok(chars[..width as usize].iter().collect());
    }
    let fill_chars: Vec<char> = fill.chars().collect();
    if fill_chars.is_empty() {
        return Ok(s.to_string());
    }
    let pad_len = (width - len) as usize;
    let padding: String = fill_chars.iter().cycle().take(pad_len).collect();
    Ok(match f {
        ScalarFunc::Lpad => format!("{padding}{s}"),
        _ => format!("{s}{padding}"), // Rpad
    })
}

/// `left`/`right`: the first or last `n` chars. A negative `n` drops `|n|` chars
/// from the far end, as PostgreSQL does.
fn left_right(f: ScalarFunc, s: &str, n: i64) -> String {
    let chars: Vec<char> = s.chars().collect();
    let len = chars.len() as i64;
    let take = if n < 0 { (len + n).max(0) } else { n.min(len) };
    match f {
        ScalarFunc::Left => chars[..take as usize].iter().collect(),
        _ => chars[(len - take) as usize..].iter().collect(), // Right
    }
}

/// `repeat(s, n)`: an `n <= 0` yields the empty string. A guard keeps the result
/// within the field-size limit.
fn repeat_str(s: &str, n: i64) -> Result<Datum, ExecError> {
    if n <= 0 {
        return Ok(Datum::Text(String::new()));
    }
    let n = n as usize;
    match s.len().checked_mul(n) {
        Some(total) if total <= MAX_FIELD_SIZE => Ok(Datum::Text(s.repeat(n))),
        _ => Err(length_too_large()),
    }
}

/// `initcap(s)`: uppercase the first alphanumeric of each word, lowercase the rest.
fn initcap(s: &str) -> String {
    let mut out = String::with_capacity(s.len());
    let mut prev_alnum = false;
    for c in s.chars() {
        if c.is_alphanumeric() {
            if prev_alnum {
                out.extend(c.to_lowercase());
            } else {
                out.extend(c.to_uppercase());
            }
            prev_alnum = true;
        } else {
            out.push(c);
            prev_alnum = false;
        }
    }
    out
}

/// `strpos(s, sub)`: the 1-based char index of the first `sub` in `s`, or `0`
/// when `sub` is absent. An empty `sub` matches at position `1`, as PostgreSQL
/// does.
fn strpos(s: &str, sub: &str) -> i32 {
    if sub.is_empty() {
        return 1;
    }
    match s.find(sub) {
        None => 0,
        Some(byte_idx) => (s[..byte_idx].chars().count() + 1) as i32,
    }
}

/// `chr(n)`: the one-character string for Unicode code point `n`. `0` or an
/// out-of-range / surrogate code point is 54000.
fn chr(n: i64) -> Result<Datum, ExecError> {
    if n == 0 {
        return Err(domain("54000", "null character not permitted"));
    }
    match u32::try_from(n).ok().and_then(char::from_u32) {
        Some(c) => Ok(Datum::Text(c.to_string())),
        None => Err(domain(
            "54000",
            "requested character too large for encoding",
        )),
    }
}

#[cfg(test)]
mod tests {
    use assert2::assert;
    use crabka_pgcatalog::{Column, RelationName, Table};
    use crabka_pgparser::parser::parse_expr_for_test as pexpr;

    use super::*;

    fn table() -> Table {
        Table {
            id: 1,
            owner: crabka_pgcatalog::BOOTSTRAP_ROLE.into(),
            name: RelationName::public("t"),
            columns: vec![
                Column::new("s", ColumnType::Text),
                Column::new("n", ColumnType::Int4),
            ],
            sharded: false,
            row_security: false,
            force_row_security: false,
            sharding: None,
            foreign: None,
            materialized: None,
            checks: Vec::new(),
        }
    }

    fn table_n() -> Table {
        Table {
            id: 1,
            owner: crabka_pgcatalog::BOOTSTRAP_ROLE.into(),
            name: RelationName::public("t"),
            columns: vec![Column::new("qn", ColumnType::Numeric(None))],
            sharded: false,
            row_security: false,
            force_row_security: false,
            sharding: None,
            foreign: None,
            materialized: None,
            checks: Vec::new(),
        }
    }

    /// The table's single-relation scope, or the empty (FROM-less) scope.
    fn scope_of(t: Option<&Table>) -> Scope {
        match t {
            Some(t) => Scope::single(t, &t.name.name),
            None => Scope::empty(),
        }
    }

    /// Evaluate a scalar-function expression with no row context.
    fn ev(sql: &str) -> Datum {
        let ctx = crate::clock::EvalCtx::test_default();
        crate::eval::eval(&pexpr(sql).expect("parse"), &Scope::empty(), &[], &ctx).expect("eval")
    }

    /// Render an expression the way the wire would, so a case's expectation is
    /// exactly the text PostgreSQL prints — including a numeric's display
    /// scale, which is the whole point of `log_var`'s scale selection.
    fn rendered(sql: &str) -> String {
        let ctx = crate::clock::EvalCtx::test_default();
        text_render(&ev(sql), &ctx.time_zone)
    }

    #[test]
    fn null_count_functions_are_variadic_and_non_strict() {
        for (sql, expected) in [
            ("num_nonnulls(NULL, 1, NULL, 2)", Datum::Int4(2)),
            ("num_nulls(NULL, 1, NULL, 2)", Datum::Int4(2)),
            ("num_nonnulls(NULL::int, NULL::text)", Datum::Int4(0)),
            ("num_nulls(NULL::int, NULL::text)", Datum::Int4(2)),
            ("num_nonnulls(VARIADIC ARRAY[1, NULL, 2])", Datum::Int4(2)),
            ("num_nulls(VARIADIC ARRAY[1, NULL, 2])", Datum::Int4(1)),
            ("num_nonnulls(VARIADIC ARRAY[]::int[])", Datum::Int4(0)),
            ("num_nulls(VARIADIC ARRAY[]::int[])", Datum::Int4(0)),
            ("num_nonnulls(VARIADIC NULL::int[])", Datum::Null),
            ("num_nulls(VARIADIC NULL::int[])", Datum::Null),
        ] {
            assert!(ev(sql) == expected, "{sql}");
        }
        assert!(
            crate::eval::infer_type(
                &pexpr("num_nonnulls(NULL)").expect("parse"),
                &Scope::empty()
            ) == Ok(ColumnType::Int4)
        );
        assert!(matches!(
            crate::eval::infer_type(&pexpr("num_nulls()").expect("parse"), &Scope::empty()),
            Err(ExecError::UndefinedFunction(_))
        ));
    }

    /// `log(base, num)` is declared over `numeric` alone, and its result scale
    /// comes from `log_var`, not from either input on its own.
    #[test]
    fn two_argument_log_divides_the_logarithms_at_postgres_display_scale() {
        for (sql, expected) in [
            ("log(2, 2)", "1.0000000000000000"),
            ("log(2, 4.2)", "2.0703893278913979"),
            ("log(4.2, 2)", "0.4830009440873890"),
            ("log(numeric '2', 'Infinity')", "Infinity"),
            ("log(numeric 'Infinity', 2)", "0"),
            ("log(numeric 'Infinity', 'Infinity')", "NaN"),
            ("log(numeric 'NaN', 2)", "NaN"),
            ("log(0.99923, 4.58934e34)", "-103611.55579544132"),
            ("log(1.000016, 8.452010e18)", "2723830.2877097365"),
            // The one-argument form is still base 10.
            ("log(100)", "2"),
        ] {
            assert!(rendered(sql) == expected, "{sql} gave {}", rendered(sql));
        }
        assert!(
            crate::eval::infer_type(&pexpr("log(2, 8)").expect("parse"), &Scope::empty())
                .expect("type")
                == ColumnType::Numeric(None)
        );
    }

    #[test]
    fn two_argument_log_reports_the_domain_error_of_whichever_operand_fails() {
        for (sql, code) in [
            ("log(numeric '0', 10)", "2201E"),
            ("log(numeric '10', 0)", "2201E"),
            ("log(numeric '-Infinity', 10)", "2201E"),
            ("log(numeric 'Infinity', 0)", "2201E"),
            ("log(numeric 'Infinity', '-Infinity')", "2201E"),
            // ln(1) is zero, so the division has no divisor.
            ("log(1.0, 12.34)", "22012"),
            // There is no two-argument float8 candidate.
            ("log(2::float8, 8::float8)", "42883"),
        ] {
            assert!(err_code(sql, None) == code, "{sql}");
        }
    }

    /// `float4send` is `real`'s binary output function: four big-endian IEEE
    /// 754 bytes, which is how the suite pins the rounding of a decimal literal
    /// that prints the same either way.
    #[test]
    fn float4send_reports_the_four_wire_bytes_of_a_real() {
        for (sql, expected) in [
            ("float4send('5e-20'::float4)", r"\x1f6c1e4a"),
            ("float4send('67e14'::float4)", r"\x59be6cea"),
            // Two literals that print alike and round to the same real.
            ("float4send('1.17549435e-38'::float4)", r"\x00800000"),
            ("float4send('1.1754944e-38'::float4)", r"\x00800000"),
            ("float4send(0::float4)", r"\x00000000"),
            ("float4send('-0'::float4)", r"\x80000000"),
        ] {
            assert!(rendered(sql) == expected, "{sql} gave {}", rendered(sql));
        }
        assert!(
            crate::eval::infer_type(
                &pexpr("float4send(1::float4)").expect("parse"),
                &Scope::empty()
            )
            .expect("type")
                == ColumnType::Bytea
        );
        assert!(ev("float4send(null)") == Datum::Null);
        // `float8` reaches `real` only by an assignment cast, so it resolves to
        // no candidate at all.
        assert!(err_code("float4send(1::float8)", None) == "42883");
        assert!(err_code("float4send('a'::text)", None) == "42883");
    }

    #[test]
    fn uuid_generation_and_extraction_follow_rfc_bits() {
        let v4 = ev("uuidv4()");
        let v7 = ev("uuidv7()");
        let bytes = |value: &Datum| match value {
            Datum::Text(text) => crabka_pgtypes::uuid::UuidBytes::parse(text).expect("uuid"),
            other => panic!("expected UUID text, got {other:?}"),
        };
        assert_eq!(bytes(&v4).0[6] >> 4, 4);
        assert_eq!(bytes(&v4).0[8] >> 6, 2);
        assert_eq!(bytes(&v7).0[6] >> 4, 7);
        assert_eq!(bytes(&v7).0[8] >> 6, 2);
        assert_eq!(ev("uuid_extract_version(uuidv4())"), Datum::Int4(4));
        assert_eq!(ev("uuid_extract_version(uuidv7())"), Datum::Int4(7));
        assert_ne!(v4, ev("uuidv4()"));
        assert!(
            text_render(&v7, &crate::clock::EvalCtx::test_default().time_zone)
                < text_render(
                    &ev("uuidv7()"),
                    &crate::clock::EvalCtx::test_default().time_zone
                )
        );
        assert_eq!(
            ev("uuid_extract_version('11111111-1111-1111-1111-111111111111')"),
            Datum::Null
        );
        assert_eq!(
            ev("uuid_extract_timestamp('C232AB00-9414-11EC-B3C8-9F6BDECED846')"),
            ev("timestamptz '2022-02-22 19:22:22+00'")
        );
        assert_eq!(
            ev("uuid_extract_timestamp('11111111-1111-1111-1111-111111111111')"),
            Datum::Null
        );
    }

    #[test]
    fn binary_coercible_matches_builtin_identity_and_relabel_casts() {
        assert!(ev("binary_coercible(23, 23)") == Datum::Bool(true));
        assert!(ev("binary_coercible(23::oid, 23::oid)") == Datum::Bool(true));
        assert!(ev("binary_coercible(23, 26)") == Datum::Bool(true));
        assert!(ev("binary_coercible(25, 1043)") == Datum::Bool(true));
        assert!(ev("binary_coercible(23, 25)") == Datum::Bool(false));
    }

    #[test]
    fn boolean_comparison_builtins_are_strict_and_resolve_unknown_literals() {
        for (sql, expected) in [
            ("booleq(true, true)", true),
            ("booleq(true, false)", false),
            ("booleq(bool 'false', false)", true),
            ("boolne(true, false)", true),
            ("boolne(false, false)", false),
            ("booleq('true', 'true')", true),
            ("boolne('false', 'true')", true),
        ] {
            assert_eq!(ev(sql), Datum::Bool(expected), "{sql}");
        }
        assert_eq!(ev("booleq(null, true)"), Datum::Null);
        assert_eq!(ev("boolne(false, null)"), Datum::Null);

        for sql in ["booleq(true, false)", "boolne('true', 'false')"] {
            assert_eq!(
                crate::eval::infer_type(&pexpr(sql).expect("parse"), &Scope::empty())
                    .expect("type"),
                ColumnType::Bool,
                "{sql}"
            );
        }
        assert_eq!(err_code("booleq(1, true)", None), "42883");
        assert_eq!(err_code("boolne(true)", None), "42883");
        assert_eq!(ec_eval("booleq('not-a-bool', true)"), "22P02");
    }

    /// The SQL-standard call forms that spell their arguments with keywords.
    /// Every expectation here comes from PostgreSQL 18.4, including the
    /// clipping and error cases, because the keyword spellings and the comma
    /// spellings must agree and both must agree with the oracle.
    #[test]
    fn sql_standard_keyword_argument_call_forms_match_postgresql() {
        let cases: &[(&str, &str)] = &[
            // SUBSTRING: FROM/FOR, either alone, and the comma spelling.
            ("substring('abcdef' FROM 2 FOR 3)", "bcd"),
            ("substring('abcdef' FROM 2)", "bcdef"),
            ("substring('abcdef' FOR 3)", "abc"),
            ("substring('abcdef', 2, 3)", "bcd"),
            // A start before position 1 spends the count getting there.
            ("substring('abcdef' FROM 0 FOR 3)", "ab"),
            ("substring('abcdef' FROM -1 FOR 3)", "a"),
            // The pattern forms: the first capture group if there is one, else
            // the whole match.
            ("substring('abcdef' FROM 'b.d')", "bcd"),
            ("substring('abcdef' FROM '(b)(.d)')", "b"),
            (
                "substring('abcdef' SIMILAR '%#\"b_d#\"%' ESCAPE '#')",
                "bcd",
            ),
            // TRIM: the side chooses btrim/ltrim/rtrim, and omitted characters
            // mean spaces.
            ("trim(' x ')", "x"),
            ("trim(both from ' x ')", "x"),
            ("trim(leading 'x' from 'xxa')", "a"),
            ("trim(trailing 'x' from 'axx')", "a"),
            ("trim(both 'x' from 'xxaxx')", "a"),
            ("trim('x' from 'xxaxx')", "a"),
            ("trim(leading from '  xxa')", "xxa"),
            // OVERLAY's count defaults to the replacement's own length, so the
            // default replaces exactly as much as it inserts.
            ("overlay('abcdef' placing 'ZZ' from 2 for 3)", "aZZef"),
            ("overlay('abcdef' placing 'ZZ' from 2)", "aZZdef"),
            ("overlay('abcdef' placing 'ZZ' from 2 for 0)", "aZZbcdef"),
            ("overlay('abcdef' placing '' from 2 for 2)", "adef"),
        ];
        for (expr, expected) in cases {
            let got = ev(expr);
            assert!(
                got == Datum::Text((*expected).into()),
                "{expr}: {got:?} != {expected}"
            );
        }

        // POSITION reverses its arguments relative to `strpos`, and returns an
        // integer rather than text.
        for (expr, expected) in [
            ("position('b' in 'abc')", 2),
            ("position('z' in 'abc')", 0),
            ("position('' in 'abc')", 1),
        ] {
            assert!(ev(expr) == Datum::Int4(expected), "{expr}");
        }

        // A pattern that does not match is NULL, not the empty string.
        assert!(ev("substring('abcdef' FROM 'x')") == Datum::Null);

        // PostgreSQL raises 22011 (substring_error), not a type error, and
        // `overlay` inherits it through the substring it is defined as.
        assert!(ec_eval("substring('abcdef' FROM 2 FOR -1)") == "22011");
        assert!(ec_eval("overlay('abcdef' placing 'ZZ' from 0)") == "22011");
    }

    /// SQLSTATE of a runtime eval error (no row context).
    fn ec_eval(sql: &str) -> String {
        let ctx = crate::clock::EvalCtx::test_default();
        crate::eval::eval(&pexpr(sql).expect("parse"), &Scope::empty(), &[], &ctx)
            .expect_err("expected error")
            .into_pg()
            .code
    }

    fn err_code(sql: &str, t: Option<&Table>) -> String {
        // Drive both the static (projection) and runtime path: infer first (this
        // is what a projected expression hits), falling back to eval.
        let ctx = crate::clock::EvalCtx::test_default();
        let e = pexpr(sql).expect("parse");
        let scope = scope_of(t);
        crate::eval::infer_type(&e, &scope)
            .err()
            .or_else(|| crate::eval::eval(&e, &scope, &[Datum::Null, Datum::Null], &ctx).err())
            .expect("expected error")
            .into_pg()
            .code
    }

    #[test]
    fn string_length_upper_lower() {
        assert_eq!(ev("length('hello')"), Datum::Int4(5));
        assert_eq!(ev(r"length('\x00ff'::bytea)"), Datum::Int4(2));
        assert_eq!(ev("char_length('abc')"), Datum::Int4(3));
        assert_eq!(ev("character_length('')"), Datum::Int4(0));
        let error = crate::eval::infer_type(&pexpr("length(42)").expect("parse"), &Scope::empty())
            .expect_err("integer length must not resolve")
            .into_pg();
        assert_eq!(error.message, "function length(integer) does not exist");
        assert_eq!(err_code(r"char_length('\x00ff'::bytea)", None), "42883");
        assert_eq!(ev("upper('aBc')"), Datum::Text("ABC".into()));
        assert_eq!(ev("lower('aBc')"), Datum::Text("abc".into()));
        // strict: NULL argument → NULL.
        assert_eq!(ev("length(null)"), Datum::Null);
        assert_eq!(ev("upper(null)"), Datum::Null);
    }

    #[test]
    fn range_constructors_and_accessors_keep_typed_bounds() {
        let text = |sql: &str| {
            String::from_utf8(crabka_pgtypes::encoding::encode_text(
                &ev(sql),
                &jiff::tz::TimeZone::UTC,
            ))
            .expect("range output is UTF-8")
        };
        assert_eq!(text("int4range(1, 4, '(]')"), "[2,5)");
        assert_eq!(text("int4range(int4range(1, 4))"), "[1,4)");
        assert_eq!(text("int4multirange()"), "{}");
        assert_eq!(text("multirange(int4range(1, 4))"), "{[1,4)}");
        assert_eq!(
            text("lower(int4multirange(int4range(1, 4), int4range(8, 10)))"),
            "1"
        );
        assert_eq!(
            text("upper(int4multirange(int4range(1, 4), int4range(8, 10)))"),
            "10"
        );
        assert_eq!(text("int4range(1, 4)::int4multirange"), "{[1,4)}");
        assert_eq!(
            text("int4multirange(int4range(5, 8), int4range(1, 5))"),
            "{[1,8)}"
        );
        assert_eq!(
            ev("int4range(1, 10) @> int4multirange(int4range(2, 4), int4range(6, 8))"),
            Datum::Bool(true)
        );
        assert_eq!(
            ev("int4range(1, 3) && int4multirange(int4range(2, 4), int4range(6, 8))"),
            Datum::Bool(true)
        );
        assert_eq!(
            ev("int4multirange(int4range(2, 4), int4range(6, 8)) @> 3"),
            Datum::Bool(true)
        );
        assert_eq!(
            ec_eval("int4multirange(int4range(2, 4), int4range(6, 8)) @> '3'"),
            "22P02"
        );
        assert_eq!(
            ec_eval("'3' <@ int4multirange(int4range(2, 4), int4range(6, 8))"),
            "22P02"
        );
        assert_eq!(
            ev("int4multirange(int4range(2, 4), int4range(6, 8)) << int4range(9, 10)"),
            Datum::Bool(true)
        );
        assert_eq!(
            ev(
                "int4multirange(int4range(2, 4), int4range(6, 8)) &< int4multirange(int4range(7, 10))"
            ),
            Datum::Bool(true)
        );
        assert_eq!(
            text(
                "int4multirange(int4range(1, 5), int4range(10, 15)) * int4multirange(int4range(3, 12))"
            ),
            "{[3,5),[10,12)}"
        );
        assert_eq!(
            text(
                "int4multirange(int4range(1, 5), int4range(10, 15)) - int4multirange(int4range(3, 12))"
            ),
            "{[1,3),[12,15)}"
        );
        assert_eq!(
            ev("multirange_contains_range(int4multirange(int4range(1, 5)), int4range(2, 4))"),
            Datum::Bool(true)
        );
        assert_eq!(
            text("range_merge(int4multirange(int4range(1, 5), int4range(10, 15)))"),
            "[1,15)"
        );
        assert_eq!(ev("lower(int4range(1, 4))"), Datum::Int4(1));
        assert_eq!(ev("upper(int4range(1, 4))"), Datum::Int4(4));
        assert_eq!(ev("isempty('empty'::int4range)"), Datum::Bool(true));
        assert_eq!(ev("lower_inf(int4range(null, 4))"), Datum::Bool(true));
        assert_eq!(ev("upper_inc(int4range(1, 4, '[]'))"), Datum::Bool(false));
        assert_eq!(ev("numrange(1.0, 3.0) @> 2.0"), Datum::Bool(true));
        assert_eq!(
            ev("numrange(1.0, 3.0) && numrange(2.0, 4.0)"),
            Datum::Bool(true)
        );
        assert_eq!(
            ev("numrange(1.0, 2.0) -|- numrange(2.0, 3.0)"),
            Datum::Bool(true)
        );
        assert_eq!(
            ev("numrange(1.0, 2.0) << numrange(3.0, 4.0)"),
            Datum::Bool(true)
        );
        assert_eq!(
            ev("numrange(1.0, 2.0) &< numrange(1.5, 3.0)"),
            Datum::Bool(true)
        );
        assert_eq!(text("numrange(1.0, 2.0) + numrange(2.0, 3.0)"), "[1.0,3.0)");
        assert_eq!(text("numrange(1.0, 2.0) + '[2.0,3.0)'"), "[1.0,3.0)");
        assert_eq!(text("numrange(1.0, 2.0) * numrange(1.5, 3.0)"), "[1.5,2.0)");
        assert_eq!(text("numrange(1.0, 2.0) - numrange(1.5, 3.0)"), "[1.0,1.5)");
        assert_eq!(
            text("numrange(1.0, 3.0) - numrange(1.0, 3.0, '()')"),
            "[1.0,1.0]"
        );
        assert_eq!(
            ev("numrange(1.0, 2.0, '[]') &< numrange(1.0, 2.0)"),
            Datum::Bool(false)
        );
        assert_eq!(
            text("range_merge(numrange(1.0, 2.0), numrange(2.5, 3.0))"),
            "[1.0,3.0)"
        );
        // A user-defined range type is usable as a constructor function. The
        // oid is chosen here rather than allocated: the catalog that persists a
        // type owns its oid, and this test never builds one.
        crabka_pgtypes::usertype::replace(&crabka_pgtypes::usertype::UserType {
            oid: 300_700,
            array_oid: crabka_pgtypes::usertype::user_array_oid(300_700),
            schema: crabka_pgtypes::usertype::USER_TYPE_DEFAULT_SCHEMA.to_string(),
            name: "range_constructor_test".to_string(),
            body: crabka_pgtypes::usertype::UserTypeBody::Range(
                crabka_pgtypes::usertype::RangeBody {
                    subtype: ColumnType::Text,
                    collation: None,
                    multirange_schema: None,
                    multirange_name: None,
                },
            ),
        });
        assert!(text("range_constructor_test('a', 'z')") == "[a,z)");
        crabka_pgtypes::usertype::unregister("range_constructor_test");
    }

    #[test]
    fn trims_default_and_with_set() {
        assert_eq!(ev("btrim('  hi  ')"), Datum::Text("hi".into()));
        assert_eq!(ev("ltrim('  hi  ')"), Datum::Text("hi  ".into()));
        assert_eq!(ev("rtrim('  hi  ')"), Datum::Text("  hi".into()));
        assert_eq!(ev("btrim('xxhixx', 'x')"), Datum::Text("hi".into()));
        assert_eq!(ev("ltrim('xyhi', 'xy')"), Datum::Text("hi".into()));
        // a NULL character-set argument → NULL (strict).
        assert_eq!(ev("btrim('hi', null)"), Datum::Null);
    }

    #[test]
    fn substr_and_replace() {
        assert_eq!(ev("substr('abcdef', 2, 3)"), Datum::Text("bcd".into()));
        assert_eq!(ev("substring('abcdef', 4)"), Datum::Text("def".into()));
        // start before position 1: the count is consumed from position 1.
        assert_eq!(ev("substr('abcdef', 0, 2)"), Datum::Text("a".into()));
        assert_eq!(ev("substr('abc', 5)"), Datum::Text("".into()));
        assert_eq!(
            ev("replace('a.b.c', '.', '-')"),
            Datum::Text("a-b-c".into())
        );
        // A negative substring length is PostgreSQL's 22011 (substring_error).
        let ctx = crate::clock::EvalCtx::test_default();
        let err = crate::eval::eval(
            &pexpr("substr('abc', 1, -1)").expect("p"),
            &Scope::empty(),
            &[],
            &ctx,
        )
        .expect_err("neg len");
        assert_eq!(err.into_pg().code, "22011");
    }

    #[test]
    fn concat_skips_nulls_and_renders_each() {
        assert_eq!(ev("concat('a', 'b', 'c')"), Datum::Text("abc".into()));
        assert_eq!(ev("concat('x', null, 'y')"), Datum::Text("xy".into()));
        assert_eq!(ev("concat(1, '+', 2)"), Datum::Text("1+2".into()));
        // all-NULL (and zero-arg) concat is the empty string, never NULL.
        assert_eq!(ev("concat(null, null)"), Datum::Text("".into()));
        assert_eq!(ev("concat()"), Datum::Text("".into()));
    }

    #[test]
    fn abs_and_mod() {
        let ctx = crate::clock::EvalCtx::test_default();
        let num = |s: &str| Datum::Numeric(crabka_pgtypes::numeric::parse(s).expect("n"));
        assert_eq!(ev("abs(-5)"), Datum::Int4(5));
        assert_eq!(ev("abs(7)"), Datum::Int4(7));
        // SP32: a bare decimal is numeric, so abs over it is numeric (float8 abs
        // is reached via an explicit cast).
        assert_eq!(ev("abs(-2.5)"), num("2.5"));
        assert_eq!(ev("abs(2.5)"), num("2.5"));
        assert_eq!(ev("abs(-2.5::float8)"), Datum::Float8(2.5));
        assert_eq!(ev("mod(11, 3)"), Datum::Int4(2));
        assert_eq!(ev("mod(-11, 3)"), Datum::Int4(-2));
        // SP32: numeric mod (the remainder takes the dividend's sign).
        assert_eq!(ev("mod(7.5, 2)"), num("1.5"));
        assert_eq!(ev("abs(null)"), Datum::Null);
        // abs overflow at i32::MIN is 22003. A bare `-2147483648` literal is a
        // negated int8, so the overflow only arises on an actual int4 column
        // value; evaluate `abs(n)` against a row holding Int4(i32::MIN).
        let t = table();
        let err = crate::eval::eval(
            &pexpr("abs(n)").expect("p"),
            &scope_of(Some(&t)),
            &[Datum::Null, Datum::Int4(i32::MIN)],
            &ctx,
        )
        .expect_err("overflow");
        assert_eq!(err.into_pg().code, "22003");
        // mod by zero is 22012.
        let err = crate::eval::eval(&pexpr("mod(1, 0)").expect("p"), &Scope::empty(), &[], &ctx)
            .expect_err("div0");
        assert_eq!(err.into_pg().code, "22012");
    }

    #[test]
    fn float8_accumulator_keeps_count_sum_and_sum_of_squares() {
        assert!(
            ev("float8_accum(ARRAY[0::float8, 0::float8, 0::float8], 2::float8)")
                == Datum::Array(crabka_pgtypes::ArrayValue::new(
                    ElemType::Float8,
                    vec![Datum::Float8(1.0), Datum::Float8(2.0), Datum::Float8(4.0)],
                ))
        );
        assert!(ev("float8_avg(ARRAY[3::float8, 6::float8, 14::float8])") == Datum::Float8(2.0));
        assert!(ev("float8_avg(ARRAY[0::float8, 0::float8, 0::float8])") == Datum::Null);
        assert!(ev("float8_accum(NULL::float8[], 2::float8)") == Datum::Null);
        assert!(ev("float8_avg(NULL::float8[])") == Datum::Null);
        assert!(
            ev("int4_avg_accum(ARRAY[0::int8, 0::int8], 2)")
                == Datum::Array(crabka_pgtypes::ArrayValue::new(
                    ElemType::Int8,
                    vec![Datum::Int8(1), Datum::Int8(2)],
                ))
        );
        assert!(
            ev("int8_avg(ARRAY[3::int8, 6::int8])")
                == Datum::Numeric(crabka_pgtypes::numeric::parse("2").expect("numeric"))
        );
        assert!(ev("int4_avg_accum(NULL::int8[], 2)") == Datum::Null);
        assert!(ev("int8_avg(NULL::int8[])") == Datum::Null);
        assert_eq!(ev("int4_sum(NULL::int8, 2)"), Datum::Int8(2));
        assert_eq!(ev("int4_sum(7::int8, NULL::int4)"), Datum::Int8(7));
        assert_eq!(ev("int4_sum(NULL::int8, NULL::int4)"), Datum::Null);
        assert_eq!(
            ev("int4_sum(9223372036854775807::int8, 1)"),
            Datum::Int8(i64::MIN)
        );
        assert_eq!(
            crate::eval::infer_type(
                &pexpr("int4_sum(7::int8, 2)").expect("parse"),
                &Scope::empty(),
            )
            .expect("int4_sum signature"),
            ColumnType::Int8
        );
        assert_eq!(err_code("int4_sum(7::int8)", None), "42883");
        assert_eq!(err_code("int4_sum(7, 2)", None), "42883");
        assert_eq!(err_code("int4_sum(7::int8, 2::int8)", None), "42883");
        assert_eq!(ev("int4larger(2, 7)"), Datum::Int4(7));
        assert_eq!(ev("int4larger(7, 2)"), Datum::Int4(7));
        assert_eq!(ev("int4larger(NULL::int4, 2)"), Datum::Null);
        assert_eq!(ev("int4smaller(2, 7)"), Datum::Int4(2));
        assert_eq!(ev("int4smaller(7, 2)"), Datum::Int4(2));
        assert_eq!(ev("int4smaller(NULL::int4, 2)"), Datum::Null);
        assert_eq!(
            crate::eval::infer_type(&pexpr("int4larger(2, 7)").expect("parse"), &Scope::empty(),)
                .expect("int4larger signature"),
            ColumnType::Int4
        );
        assert_eq!(err_code("int4larger(2)", None), "42883");
        assert_eq!(ev("booland_statefunc(true, false)"), Datum::Bool(false));
        assert_eq!(ev("boolor_statefunc(false, true)"), Datum::Bool(true));
        assert_eq!(ev("booland_statefunc(NULL::bool, true)"), Datum::Null);
        assert_eq!(ev("boolor_statefunc(false, NULL::bool)"), Datum::Null);
        assert!(
            crate::eval::infer_type(
                &pexpr("booland_statefunc(true)").expect("parse"),
                &Scope::empty(),
            )
            .is_err()
        );
        assert!(
            crate::eval::infer_type(
                &pexpr("booland_statefunc(1, true)").expect("parse"),
                &Scope::empty(),
            )
            .is_err()
        );
        assert_eq!(ev("float8mi(5::float8, 2::float8)"), Datum::Float8(3.0));
        assert_eq!(
            ev("float8mi(-0.0::float8, 0.0::float8)"),
            Datum::Float8(-0.0)
        );
        assert_eq!(ev("float8mi(NULL::float8, 2::float8)"), Datum::Null);
        assert_eq!(
            crate::eval::infer_type(
                &pexpr("float8mi(5::float8, 2::float8)").expect("parse"),
                &Scope::empty(),
            )
            .expect("float8mi signature"),
            ColumnType::Float8
        );
        assert!(
            crate::eval::infer_type(
                &pexpr("float8mi(5::float8)").expect("parse"),
                &Scope::empty(),
            )
            .is_err()
        );
        assert!(
            ev("array_larger(ARRAY[1, 2], ARRAY[1, 3])")
                == Datum::Array(crabka_pgtypes::ArrayValue::new(
                    ElemType::Int4,
                    vec![Datum::Int4(1), Datum::Int4(3)],
                ))
        );
        assert!(
            ev("array_larger(ARRAY[1, 2], ARRAY[1, 2])")
                == Datum::Array(crabka_pgtypes::ArrayValue::new(
                    ElemType::Int4,
                    vec![Datum::Int4(1), Datum::Int4(2)],
                ))
        );
        assert_eq!(ev("array_larger(NULL::int4[], ARRAY[1])"), Datum::Null);
        assert_eq!(
            crate::eval::infer_type(
                &pexpr("array_larger(ARRAY[1], ARRAY[2])").expect("parse"),
                &Scope::empty(),
            )
            .expect("array_larger signature"),
            ColumnType::Array(ElemType::Int4)
        );
        assert!(
            crate::eval::infer_type(
                &pexpr("array_larger(ARRAY[1])").expect("parse"),
                &Scope::empty(),
            )
            .is_err()
        );
        assert!(
            crate::eval::infer_type(
                &pexpr("array_larger(ARRAY[1], ARRAY['a'])").expect("parse"),
                &Scope::empty(),
            )
            .is_err()
        );
        assert_eq!(
            err_code("int4_avg_accum(ARRAY[0::int8, 0::int8])", None),
            "42883"
        );
        assert_eq!(err_code("int4_avg_accum(ARRAY[0, 0], 2)", None), "42883");
        assert_eq!(
            err_code("int4_avg_accum(ARRAY[0::int8, 0::int8], 2::int8)", None),
            "42883"
        );
        assert_eq!(err_code("int4_avg_accum(ARRAY[0::int8], 2)", None), "22023");
        assert_eq!(err_code("int8_avg(ARRAY[0::int8])", None), "22023");
        assert_eq!(
            err_code("float8_accum(ARRAY[0::float8, 0::float8, 0::float8])", None),
            "42883"
        );
        assert_eq!(
            err_code("float8_accum(ARRAY[0, 0, 0], 2::float8)", None),
            "42883"
        );
        assert_eq!(
            err_code(
                "float8_accum(ARRAY[0::float8, 0::float8, 0::float8], 2)",
                None
            ),
            "42883"
        );
        assert_eq!(
            err_code("float8_accum(ARRAY[0::float8, 0::float8], 2::float8)", None),
            "22023"
        );
        assert_eq!(
            err_code("float8_avg(ARRAY[0::float8, 0::float8])", None),
            "22023"
        );
    }

    #[test]
    fn coalesce_short_circuits_and_nullif() {
        assert_eq!(ev("coalesce(null, null, 3)"), Datum::Int4(3));
        assert_eq!(ev("coalesce(null, null)"), Datum::Null);
        // short-circuit: the un-taken `1/0` branch is never evaluated.
        assert_eq!(ev("coalesce(7, 1/0)"), Datum::Int4(7));
        assert_eq!(ev("nullif(5, 5)"), Datum::Null);
        assert_eq!(ev("nullif(5, 6)"), Datum::Int4(5));
        assert_eq!(ev("nullif(null, 1)"), Datum::Null);
    }

    #[test]
    fn greatest_least_ignore_nulls() {
        assert_eq!(ev("greatest(3, 7, 2)"), Datum::Int4(7));
        assert_eq!(ev("least(3, 7, 2)"), Datum::Int4(2));
        assert_eq!(ev("greatest(null, 4, null)"), Datum::Int4(4));
        assert_eq!(ev("least('b', 'a', 'c')"), Datum::Text("a".into()));
        assert_eq!(ev("greatest(null, null)"), Datum::Null);
    }

    #[test]
    fn result_types_for_row_description() {
        let t = table();
        let scope = scope_of(Some(&t));
        let ty = |sql: &str| crate::eval::infer_type(&pexpr(sql).expect("p"), &scope).expect("ty");
        assert_eq!(ty("length(s)"), ColumnType::Int4);
        assert_eq!(ty("upper(s)"), ColumnType::Text);
        assert_eq!(ty("substr(s, 1, 2)"), ColumnType::Text);
        assert_eq!(ty("concat(s, n)"), ColumnType::Text);
        assert_eq!(ty("abs(n)"), ColumnType::Int4);
        assert_eq!(ty("mod(n, 2)"), ColumnType::Int4);
        // coalesce(int4, int8) unifies to int8.
        assert_eq!(ty("coalesce(n, 3000000000)"), ColumnType::Int8);
        assert_eq!(ty("nullif(s, 'x')"), ColumnType::Text);
        // `||` is text; one operand text is enough.
        assert_eq!(ty("'id=' || n"), ColumnType::Text);
    }

    #[test]
    fn database_encoding_is_utf8() {
        let expr = pexpr("getdatabaseencoding()").expect("parse");
        assert_eq!(
            crate::eval::infer_type(&expr, &Scope::empty()).expect("type"),
            ColumnType::Text
        );
        assert_eq!(ev("getdatabaseencoding()"), Datum::Text("UTF8".into()));
        assert_eq!(err_code("getdatabaseencoding(1)", None), "42883");
    }

    #[test]
    fn numa_availability_is_false() {
        let expr = pexpr("pg_numa_available()").expect("parse");
        assert_eq!(
            crate::eval::infer_type(&expr, &Scope::empty()).expect("type"),
            ColumnType::Bool
        );
        assert_eq!(ev("pg_numa_available()"), Datum::Bool(false));
        assert_eq!(err_code("pg_numa_available(1)", None), "42883");
    }

    #[test]
    fn error_surface() {
        let t = table();
        // unknown function → 42883.
        assert_eq!(err_code("frobnicate(s)", Some(&t)), "42883");
        // wrong arity → 42883.
        assert_eq!(err_code("length(s, s)", Some(&t)), "42883");
        // bad argument type in a projected position → 42883.
        assert_eq!(err_code("upper(n)", Some(&t)), "42883");
        assert_eq!(err_code("abs(s)", Some(&t)), "42883");
        // `int || int` (neither operand text) → 42883.
        assert_eq!(err_code("n || n", Some(&t)), "42883");
        // incompatible coalesce/greatest types → 42804.
        assert_eq!(err_code("coalesce(n, s)", Some(&t)), "42804");
        // DISTINCT on a scalar function → 42809.
        assert_eq!(err_code("upper(distinct s)", Some(&t)), "42809");
    }

    #[test]
    fn concat_operator_evaluates_and_propagates_null() {
        assert_eq!(ev("'a' || 'b' || 'c'"), Datum::Text("abc".into()));
        assert_eq!(ev("'id=' || 5"), Datum::Text("id=5".into()));
        assert_eq!(ev("'x' || null"), Datum::Null);
    }

    #[test]
    fn rounding_family_preserves_type() {
        let num = |s: &str| Datum::Numeric(crabka_pgtypes::numeric::parse(s).expect("n"));
        // int in → int out (unchanged)
        assert_eq!(ev("floor(5)"), Datum::Int4(5));
        assert_eq!(ev("ceil(5)"), Datum::Int4(5));
        assert_eq!(ev("trunc(5)"), Datum::Int4(5));
        assert_eq!(ev("round(5)"), Datum::Int4(5));
        assert_eq!(ev("sign(-7)"), Datum::Int4(-1));
        // numeric in → numeric out
        assert_eq!(ev("floor(2.9)"), num("2"));
        assert_eq!(ev("ceiling(2.1)"), num("3"));
        assert_eq!(ev("round(2.5)"), num("3"));
        assert_eq!(ev("round(2.567, 2)"), num("2.57"));
        assert_eq!(ev("trunc(2.99)"), num("2"));
        assert_eq!(ev("trunc(2.567, 1)"), num("2.5"));
        assert_eq!(ev("sign(-0.3)"), num("-1"));
        // float8 in → float8 out (round half-to-even)
        assert_eq!(ev("floor(2.9::float8)"), Datum::Float8(2.0));
        assert_eq!(ev("round(2.5::float8)"), Datum::Float8(2.0)); // half-to-even
        assert_eq!(ev("round(3.5::float8)"), Datum::Float8(4.0));
        assert_eq!(ev("sign(-3.0::float8)"), Datum::Float8(-1.0));
        // two-arg round/trunc on an int → numeric
        assert_eq!(ev("round(1234, -2)"), num("1200"));
        // strict NULL
        assert_eq!(ev("floor(null)"), Datum::Null);
    }

    #[test]
    fn rounding_family_types_and_errors() {
        let t = table();
        let ty = |sql: &str| {
            crate::eval::infer_type(&pexpr(sql).expect("p"), &scope_of(Some(&t))).expect("ty")
        };
        assert_eq!(ty("floor(n)"), ColumnType::Int4);
        assert_eq!(ty("round(2.5)"), ColumnType::Numeric(None));
        assert_eq!(ty("floor(2.5::float8)"), ColumnType::Float8);
        assert_eq!(ty("round(2.5, 1)"), ColumnType::Numeric(None));
        // two-arg round on a float8 first arg → 42883 (PG has no round(float8,int)).
        assert_eq!(err_code("round(2.5::float8, 1)", Some(&t)), "42883");
        // non-numeric arg → 42883.
        assert_eq!(err_code("floor(s)", Some(&t)), "42883");
    }

    #[test]
    fn transcendental_family_returns_float8() {
        assert_eq!(ev("sqrt(4)"), Datum::Float8(2.0));
        assert_eq!(ev("sqrt(2.25::float8)"), Datum::Float8(1.5));
        assert_eq!(ev("power(2, 10)"), Datum::Float8(1024.0));
        assert_eq!(ev("pow(2, 0.5::float8)"), Datum::Float8(2.0_f64.sqrt()));
        assert_eq!(ev("exp(0)"), Datum::Float8(1.0));
        assert_eq!(ev("ln(1)"), Datum::Float8(0.0));
        assert_eq!(ev("log(1000)"), Datum::Float8(3.0));
        assert_eq!(ev("pi()"), Datum::Float8(std::f64::consts::PI));
        // strict NULL
        assert_eq!(ev("sqrt(null)"), Datum::Null);
    }

    #[test]
    fn string_family_values() {
        assert_eq!(ev("lpad('hi', 5)"), Datum::Text("   hi".into()));
        assert_eq!(ev("lpad('hi', 5, '*')"), Datum::Text("***hi".into()));
        assert_eq!(ev("lpad('hello', 3)"), Datum::Text("hel".into()));
        assert_eq!(ev("rpad('hi', 5, 'ab')"), Datum::Text("hiaba".into()));
        assert_eq!(ev("rpad('hello', 3)"), Datum::Text("hel".into()));
        assert_eq!(ev("left('abcdef', 2)"), Datum::Text("ab".into()));
        assert_eq!(ev("left('abcdef', -2)"), Datum::Text("abcd".into()));
        assert_eq!(ev("right('abcdef', 2)"), Datum::Text("ef".into()));
        assert_eq!(ev("right('abcdef', -2)"), Datum::Text("cdef".into()));
        assert_eq!(ev("repeat('ab', 3)"), Datum::Text("ababab".into()));
        assert_eq!(ev("repeat('ab', 0)"), Datum::Text("".into()));
        assert_eq!(ev("reverse('abc')"), Datum::Text("cba".into()));
        assert_eq!(
            ev("initcap('hello WORLD')"),
            Datum::Text("Hello World".into())
        );
        assert_eq!(ev("strpos('abcde', 'cd')"), Datum::Int4(3));
        assert_eq!(ev("strpos('abcde', 'xy')"), Datum::Int4(0));
        assert_eq!(ev("strpos('abc', '')"), Datum::Int4(1));
        assert_eq!(ev("ascii('A')"), Datum::Int4(65));
        assert_eq!(ev("ascii('')"), Datum::Int4(0));
        assert_eq!(ev("chr(65)"), Datum::Text("A".into()));
        // strict NULL
        assert_eq!(ev("lpad(null, 5)"), Datum::Null);
        assert_eq!(ev("reverse(null)"), Datum::Null);
    }

    #[test]
    fn string_family_types_and_errors() {
        let t = table();
        let ty = |sql: &str| {
            crate::eval::infer_type(&pexpr(sql).expect("p"), &scope_of(Some(&t))).expect("ty")
        };
        assert_eq!(ty("lpad(s, 5)"), ColumnType::Text);
        assert_eq!(ty("strpos(s, 'x')"), ColumnType::Int4);
        assert_eq!(ty("ascii(s)"), ColumnType::Int4);
        assert_eq!(ty("chr(n)"), ColumnType::Text);
        // chr(0) and an out-of-range code point → 54000.
        assert_eq!(ec_eval("chr(0)"), "54000");
        assert_eq!(ec_eval("chr(99999999999)"), "54000");
        // wrong arg type → 42883.
        assert_eq!(err_code("left(n, 2)", Some(&t)), "42883");
        assert_eq!(err_code("ascii(n)", Some(&t)), "42883");
        // an adversarially huge lpad/rpad width or repeat count is 54000
        // ("requested length too large"), guarded against OOM — not a process abort.
        assert_eq!(ec_eval("lpad('x', 9999999999)"), "54000");
        assert_eq!(ec_eval("rpad('x', 9999999999)"), "54000");
        assert_eq!(ec_eval("repeat('x', 9999999999)"), "54000");
    }

    #[test]
    fn transcendental_domain_errors() {
        let t = table();
        let ty = |sql: &str| {
            crate::eval::infer_type(&pexpr(sql).expect("p"), &scope_of(Some(&t))).expect("ty")
        };
        assert_eq!(ty("sqrt(n)"), ColumnType::Float8);
        assert_eq!(ty("pi()"), ColumnType::Float8);
        // sqrt(negative) → 2201F
        assert_eq!(ec_eval("sqrt(-1)"), "2201F");
        // ln/log of a non-positive number → 2201E
        assert_eq!(ec_eval("ln(0)"), "2201E");
        assert_eq!(ec_eval("ln(-1)"), "2201E");
        assert_eq!(ec_eval("log(0)"), "2201E");
        // zero to a negative power → 2201F
        assert_eq!(ec_eval("power(0, -1)"), "2201F");
        // wrong arity → 42883
        assert_eq!(err_code("pi(1)", Some(&t)), "42883");
        assert_eq!(err_code("power(2)", Some(&t)), "42883");
    }

    #[test]
    fn transcendentals_are_numeric_for_numeric_input() {
        let num = |s: &str| Datum::Numeric(crabka_pgtypes::numeric::parse(s).expect("n"));
        // numeric in -> numeric out (oracle-validated exact values from pgtypes unit tests)
        assert_eq!(ev("sqrt(2.0)"), num("1.414213562373095"));
        assert_eq!(ev("exp(1.0)"), num("2.7182818284590452"));
        assert_eq!(ev("ln(2.0)"), num("0.6931471805599453"));
        assert_eq!(ev("power(2.0, 3.0)"), num("8.0000000000000000"));
        // int in -> float8 out (unchanged)
        assert_eq!(ev("sqrt(4)"), Datum::Float8(2.0));
        assert_eq!(ev("exp(0)"), Datum::Float8(1.0));
        // float8 in -> float8 out (unchanged)
        assert_eq!(ev("sqrt(4.0::float8)"), Datum::Float8(2.0));
        // strict NULL
        assert_eq!(ev("sqrt(null)"), Datum::Null);
        // numeric-path domain errors (2201E/2201F) and overflow (22003) surface
        // end-to-end from SQL — never a panic, hang, or silently-wrong value.
        assert_eq!(ec_eval("sqrt(-1::numeric)"), "2201F");
        assert_eq!(ec_eval("ln(0::numeric)"), "2201E");
        assert_eq!(ec_eval("power(0::numeric, -1::numeric)"), "2201F");
        assert_eq!(ec_eval("exp(6000::numeric)"), "22003");
        assert_eq!(ec_eval("power(10::numeric, 200000::numeric)"), "22003");
    }

    #[test]
    fn transcendental_result_types() {
        let t = table_n();
        let ty = |sql: &str| {
            crate::eval::infer_type(&pexpr(sql).expect("p"), &scope_of(Some(&t))).expect("ty")
        };
        assert_eq!(ty("sqrt(qn)"), ColumnType::Numeric(None)); // qn is numeric
        assert_eq!(ty("ln(qn)"), ColumnType::Numeric(None));
        assert_eq!(ty("sqrt(4)"), ColumnType::Float8); // int literal
        assert_eq!(ty("sqrt(4.0::float8)"), ColumnType::Float8);
        assert_eq!(ty("power(qn, 2)"), ColumnType::Numeric(None)); // numeric base
        assert_eq!(ty("power(2, 3)"), ColumnType::Float8); // all-int
    }

    /// A table whose columns are named after the geometric functions. Every one
    /// of these is an ordinary identifier in `PostgreSQL` — none of the seven
    /// type names is even a keyword there — so a column keeps the name whatever
    /// `is_scalar` says about it.
    fn geometric_name_table() -> Table {
        Table {
            id: 1,
            owner: crabka_pgcatalog::BOOTSTRAP_ROLE.into(),
            name: RelationName::public("t"),
            columns: vec![
                Column::new("area", ColumnType::Int4),
                Column::new("center", ColumnType::Text),
                Column::new("path", ColumnType::Text),
                Column::new("slope", ColumnType::Int4),
                Column::new("npoints", ColumnType::Int4),
                Column::new("radius", ColumnType::Int4),
                Column::new("width", ColumnType::Int4),
                Column::new("height", ColumnType::Int4),
                Column::new("length", ColumnType::Int4),
            ],
            sharded: false,
            row_security: false,
            force_row_security: false,
            sharding: None,
            foreign: None,
            materialized: None,
            checks: Vec::new(),
        }
    }

    /// Adding the geometric family to `is_scalar` must not take any of its
    /// names away from a column that happens to use one. A bare `area` is a
    /// column reference, not a niladic call.
    #[test]
    fn a_column_named_after_a_geometric_function_is_still_a_column() {
        let t = geometric_name_table();
        let scope = scope_of(Some(&t));
        let ty =
            |sql: &str| crate::eval::infer_type(&pexpr(sql).expect("parse"), &scope).expect("type");
        for (sql, expected) in [
            ("area", ColumnType::Int4),
            ("center", ColumnType::Text),
            ("path", ColumnType::Text),
            ("slope", ColumnType::Int4),
            ("npoints", ColumnType::Int4),
            ("radius", ColumnType::Int4),
            ("width", ColumnType::Int4),
            ("height", ColumnType::Int4),
            ("length", ColumnType::Int4),
            ("area + 1", ColumnType::Int4),
            ("t.area", ColumnType::Int4),
            ("upper(center)", ColumnType::Text),
        ] {
            assert!(ty(sql) == expected, "{sql}");
        }
        // And the values come through, not a function's result.
        let ctx = crate::clock::EvalCtx::test_default();
        let row = vec![
            Datum::Int4(7),
            Datum::Text("c".into()),
            Datum::Text("p".into()),
            Datum::Int4(1),
            Datum::Int4(2),
            Datum::Int4(3),
            Datum::Int4(4),
            Datum::Int4(5),
            Datum::Int4(6),
        ];
        let value = crate::eval::eval(&pexpr("area + 1").expect("parse"), &scope, &row, &ctx)
            .expect("eval");
        assert!(value == Datum::Int4(8));
    }

    /// `length` keeps every non-geometric overload it already had; only `lseg`
    /// and `path` arguments take the `float8` reading, and only under the
    /// `length` spelling.
    #[test]
    fn length_keeps_its_non_geometric_overloads() {
        let t = table();
        let ty = |sql: &str| {
            crate::eval::infer_type(&pexpr(sql).expect("parse"), &scope_of(Some(&t))).expect("type")
        };
        for sql in [
            "length('abc')",
            "length(s)",
            "char_length(s)",
            "character_length('abc')",
            "length(B'1011')",
            "length('abc'::bytea)",
            "length('a b'::tsvector)",
        ] {
            assert!(ty(sql) == ColumnType::Int4, "{sql}");
        }
        assert!(ev("length('abc')") == Datum::Int4(3));
        assert!(ev("length(B'1011')") == Datum::Int4(4));
        assert!(ev("length('abc'::bytea)") == Datum::Int4(3));
        // The two geometric overloads, which return float8 rather than int4.
        assert!(ty("length(lseg '[(0,0),(3,4)]')") == ColumnType::Float8);
        assert!(ty("length(path '((0,0),(3,0),(3,4))')") == ColumnType::Float8);
        assert!(ev("length(lseg '[(0,0),(3,4)]')") == Datum::Float8(5.0));
        assert!(ev("length(path '((0,0),(3,0),(3,4))')") == Datum::Float8(12.0));
        // `char_length`/`character_length` have no geometric overload.
        assert!(err_code("char_length(lseg '[(0,0),(3,4)]')", Some(&t)) == "42883");
    }

    /// The geometric family answers through the ordinary scalar dispatch, so
    /// its result types reach `RowDescription` and its errors are 42883.
    #[test]
    fn the_geometric_family_resolves_through_the_scalar_dispatch() {
        let t = table();
        let ty = |sql: &str| {
            crate::eval::infer_type(&pexpr(sql).expect("parse"), &scope_of(Some(&t))).expect("type")
        };
        for (sql, expected) in [
            ("area(box '(0,0),(2,3)')", ColumnType::Float8),
            ("area(circle '<(0,0),2>')", ColumnType::Float8),
            ("area(path '((0,0),(2,0),(2,2),(0,2))')", ColumnType::Float8),
            ("center(box '(0,0),(2,4)')", ColumnType::Point),
            ("point(1, 2)", ColumnType::Point),
            ("box(point '(1,2)', point '(3,4)')", ColumnType::Box),
            ("lseg(box '(0,0),(2,3)')", ColumnType::Lseg),
            ("line(point '(0,0)', point '(1,1)')", ColumnType::Line),
            ("polygon(box '(0,0),(1,1)')", ColumnType::Polygon),
            ("path(polygon '((0,0),(1,1))')", ColumnType::Path),
            ("circle(box '(0,0),(2,2)')", ColumnType::Circle),
            ("npoints(polygon '((0,0),(1,1))')", ColumnType::Int4),
            ("isclosed(path '((0,0),(1,1))')", ColumnType::Bool),
            ("poly_center(polygon '((0,0),(1,1))')", ColumnType::Point),
        ] {
            assert!(ty(sql) == expected, "{sql}");
        }
        assert!(ev("area(box '(0,0),(2,3)')") == Datum::Float8(6.0));
        assert!(ev("width(box '(0,0),(2,3)')") == Datum::Float8(2.0));
        // An argument type with no overload is 42883, both at plan time and at
        // value time.
        assert!(err_code("area(lseg '[(0,0),(1,1)]')", Some(&t)) == "42883");
        assert!(ec_eval("area(lseg '[(0,0),(1,1)]')") == "42883");
        assert!(err_code("area(n)", Some(&t)) == "42883");
    }

    /// The enumeration is only trustworthy if it stays sorted (the lookup binary
    /// searches it), holds no name twice, and names types this engine has.
    #[test]
    fn the_constructor_cast_enumeration_is_sorted_and_resolvable() {
        assert!(CONSTRUCTOR_CASTS.windows(2).all(|w| w[0].0 < w[1].0));
        for (target, sources) in CONSTRUCTOR_CASTS {
            assert!(sources.windows(2).all(|w| w[0] < w[1]), "{target}");
            assert!(type_of_typname(target).is_some(), "{target}");
            for source in *sources {
                assert!(type_of_typname(source).is_some(), "{target}({source})");
            }
        }
    }

    /// `T(x)` is the cast function `pg_proc` names after its target type, so it
    /// converts exactly as `x::T` does — in the value and in the reported type.
    #[test]
    fn a_constructor_call_casts_to_the_type_it_names() {
        for (call, cast) in [
            (
                "float8(4567890123456789::int8)",
                "4567890123456789::int8::float8",
            ),
            ("float4(2::int8)", "2::int8::float4"),
            ("int4('123'::int8)", "'123'::int8::int4"),
            ("int2('123'::int8)", "'123'::int8::int2"),
            ("int8(2.5::float8)", "2.5::float8::int8"),
            ("numeric(2.5::float8)", "2.5::float8::numeric"),
            ("oid(4::int8)", "4::int8::oid"),
            ("bool(1::int4)", "1::int4::bool"),
            ("text(true::bool)", "true::bool::text"),
            (
                "date(timestamp '2026-08-13 12:00:00')",
                "timestamp '2026-08-13 12:00:00'::date",
            ),
            (
                "timestamptz(date '2026-08-13')",
                "date '2026-08-13'::timestamptz",
            ),
            ("cidr(inet '10.0.0.0/8')", "inet '10.0.0.0/8'::cidr"),
        ] {
            assert!(ev(call) == ev(cast), "{call}");
            let scope = Scope::empty();
            let ty = |sql: &str| {
                crate::eval::infer_type(&pexpr(sql).expect("parse"), &scope).expect("type")
            };
            assert!(ty(call) == ty(cast), "{call}");
        }
    }

    /// The name a *quoted* type name resolves through is the one `pg_proc` gives
    /// the cast function, so `"char"(x)` is the one-byte type and never
    /// `character(1)`.
    #[test]
    fn the_char_constructor_names_the_one_byte_type() {
        assert!(constructor_cast_type("char") == Some(ColumnType::InternalChar));
        assert!(constructor_cast_type("bpchar") == Some(ColumnType::Char(None)));
    }

    /// A name outside the enumeration stays 42883, and so do the arities the
    /// enumeration does not carry: it holds only the one-argument functions.
    #[test]
    fn a_constructor_call_is_still_undefined_off_the_enumeration() {
        for sql in [
            "jsonb('1')",
            "uuid('00000000-0000-0000-0000-000000000000')",
            "float8()",
            "float8(1::int8, 2)",
        ] {
            assert!(err_code(sql, None) == "42883", "{sql}");
        }
    }

    /// An operand type the function does not declare stays 42883 — the
    /// enumeration is a candidate list, not a licence to cast anything.
    /// `xid(xid8)` is the only `xid` overload, `numeric(numeric)` is the
    /// length coercion and takes a modifier, and no candidate takes a
    /// composite.
    #[test]
    fn an_undeclared_operand_type_has_no_constructor_cast() {
        for sql in [
            "xid('1'::xid)",
            "numeric(1.5::numeric)",
            "text(1::int4)",
            "text(row('Jim', 'Beam'))",
            "int4('123'::text)",
        ] {
            assert!(err_code(sql, None) == "42883", "{sql}");
        }
    }

    /// `int8`'s overflow says which type could not hold the result. The wording
    /// is `int4`'s in `TypeError::Overflow`, which is not this overload's.
    #[test]
    fn abs_reports_the_overflowing_type_by_name() {
        for (sql, message) in [
            ("abs('-9223372036854775808'::int8)", "bigint out of range"),
            ("abs((-2147483648)::int4)", "integer out of range"),
            ("abs((-32768)::int2)", "smallint out of range"),
        ] {
            let ctx = crate::clock::EvalCtx::test_default();
            let pg = crate::eval::eval(&pexpr(sql).expect("parse"), &Scope::empty(), &[], &ctx)
                .expect_err("expected overflow")
                .into_pg();
            assert!(pg.code == "22003", "{sql}");
            assert!(pg.message == message, "{sql}");
        }
    }

    /// `unicode_version()` answers the release the engine's own tables come
    /// from, in PostgreSQL's two-component form, and never claims a release
    /// past the older of the two tables.
    #[test]
    fn unicode_version_reports_the_older_of_the_two_tables() {
        let (major, minor) = unicode_version();
        assert!(ev("unicode_version()") == Datum::Text(format!("{major}.{minor}")));
        let (norm_major, norm_minor, _) = unicode_normalization::UNICODE_VERSION;
        let (cat_major, cat_minor, _) = unicode_general_category::UNICODE_VERSION;
        assert!((major, minor) <= (u64::from(norm_major), u64::from(norm_minor)));
        assert!((major, minor) <= (cat_major, cat_minor));
    }

    /// `unicode_assigned` is false as soon as one code point is unassigned, and
    /// vacuously true for the empty string. `U+10FFFF` is the last code point
    /// and a permanent noncharacter, so it is `Cn` in every Unicode release.
    #[test]
    fn unicode_assigned_rejects_a_single_unassigned_code_point() {
        for (sql, expected) in [
            ("unicode_assigned('')", Datum::Bool(true)),
            ("unicode_assigned('abc')", Datum::Bool(true)),
            ("unicode_assigned(U&'abc')", Datum::Bool(true)),
            ("unicode_assigned(U&'abc\\+10FFFF')", Datum::Bool(false)),
            ("unicode_assigned(U&'\\00E4\\24D1c')", Datum::Bool(true)),
            ("unicode_assigned(NULL)", Datum::Null),
        ] {
            assert!(ev(sql) == expected, "{sql}");
        }
        assert!(err_code("unicode_assigned(1)", None) == "42883");
        assert!(err_code("unicode_assigned('a', 'b')", None) == "42883");
    }

    /// `interval_hash` follows interval comparison rather than storage fields:
    /// one month and thirty days are equal and must therefore hash equally.
    #[test]
    fn interval_hash_uses_the_canonical_interval_span() {
        let scope = Scope::empty();
        let ty =
            |sql: &str| crate::eval::infer_type(&pexpr(sql).expect("parse"), &scope).expect("type");
        assert!(ty("interval_hash('30 days')") == ColumnType::Int4);
        assert!(ev("interval_hash('30 days') = interval_hash('1 month')") == Datum::Bool(true));
        assert!(ev("interval_hash('30 days') = interval_hash('1 day')") == Datum::Bool(false));
        // PostgreSQL 18.4 values cover the fold's positive and negative halves.
        for (sql, expected) in [
            ("interval_hash('30 days')", 1_574_789_525),
            ("interval_hash('1 day')", -2_053_980_660),
            ("interval_hash('-1 day')", -1_092_701_610),
            ("interval_hash('100000 years')", -1_374_199_132),
            ("interval_hash('-100000 years')", 1_889_647_881),
        ] {
            assert!(ev(sql) == Datum::Int4(expected), "{sql}");
        }
        assert!(err_code("interval_hash(1)", None) == "42883");
    }

    #[test]
    fn temporal_hashes_share_postgres_integer_hashes() {
        let scope = Scope::empty();
        let ty =
            |sql: &str| crate::eval::infer_type(&pexpr(sql).expect("parse"), &scope).expect("type");

        for sql in [
            "time_hash('00:00')",
            "timetz_hash('00:00+00')",
            "timestamp_hash('2000-01-01 00:00')",
            "timestamptz_hash('2000-01-01 00:00+00')",
        ] {
            assert!(ty(sql) == ColumnType::Int4, "{sql}");
        }
        for sql in [
            "time_hash_extended('00:00', 1::int8)",
            "timetz_hash_extended('00:00+00', 1::int8)",
            "interval_hash_extended('1 month', 1::int8)",
            "timestamp_hash_extended('2000-01-01 00:00', 1::int8)",
            "timestamptz_hash_extended('2000-01-01 00:00+00', 1::int8)",
        ] {
            assert!(ty(sql) == ColumnType::Int8, "{sql}");
        }
        assert!(ev("time_hash('00:00')") == ev("hashint8(0)"));
        assert!(ev("timestamp_hash('2000-01-01 00:00')") == ev("hashint8(0)"));
        assert!(ev("timestamptz_hash('2000-01-01 00:00+00')") == ev("hashint8(0)"));
        assert!(
            ev("interval_hash_extended('1 month', 1::int8)")
                == ev("hashint8extended(2592000000000::int8, 1::int8)")
        );
        assert!(err_code("time_hash(1)", None) == "42883");
        assert!(err_code("timetz_hash_extended('00:00+00', 'x')", None) == "42883");
    }

    #[test]
    fn hash_functions_share_the_partition_hash_primitives() {
        let scope = Scope::empty();
        let ty =
            |sql: &str| crate::eval::infer_type(&pexpr(sql).expect("parse"), &scope).expect("type");

        assert!(ty("hashint2(42::int2)") == ColumnType::Int4);
        assert!(ty("hashchar('x'::\"char\")") == ColumnType::Int4);
        assert!(ty("hashint4extended(42, 1::int8)") == ColumnType::Int8);
        assert!(ty("hashoid(42)") == ColumnType::Int4);
        assert!(ty("hashname('gres')") == ColumnType::Int4);
        assert!(ty("hashname('gres'::text)") == ColumnType::Int4);
        assert!(ty("hashtextextended('gres', 1::int8)") == ColumnType::Int8);
        assert!(ty("hashoidvector('1 2'::oidvector)") == ColumnType::Int4);
        assert!(ty("hash_array('{1,2}'::int4[])") == ColumnType::Int4);
        assert!(ty("hashbpchar('gres'::char(8))") == ColumnType::Int4);
        assert!(ty("hashfloat4(42)") == ColumnType::Int4);
        assert!(ty("hashfloat8extended(42, 1::int8)") == ColumnType::Int8);
        assert!(ty("uuid_hash('a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11')") == ColumnType::Int4);
        assert!(ty("pg_lsn_hash('16/B374D84'::pg_lsn)") == ColumnType::Int4);
        assert!(ty("hash_range(int4range(1, 2))") == ColumnType::Int4);
        assert!(ty("hash_multirange('{[1,2)}'::int4multirange)") == ColumnType::Int4);
        assert!(ev("hashint2(42::int2)") == Datum::Int4(crate::partition::hash::hash_int32(42)));
        assert!(
            ev("hashchar('x'::\"char\")")
                == Datum::Int4(crate::partition::hash::hash_int32(i32::from(b'x')))
        );
        assert!(
            ev("hashcharextended('x'::\"char\", 1::int8)")
                == Datum::Int8(crate::partition::hash::hash_int32_extended(
                    i32::from(b'x'),
                    1
                ))
        );
        assert!(
            ev("hashint4extended(42, 1::int8)")
                == Datum::Int8(crate::partition::hash::hash_int32_extended(42, 1))
        );
        assert!(ev("hashint8(-42::int8)") == Datum::Int4(crate::partition::hash::hash_int64(-42)));
        assert!(ev("hashoid(42)") == Datum::Int4(crate::partition::hash::hash_int32(42)));
        assert!(
            ev("hashname('gres')")
                == Datum::Int4(crate::partition::hash::hash_bytes(b"gres").expect("hash"))
        );
        assert!(
            ev("hashtextextended('gres', 1::int8)")
                == Datum::Int8(i64::from_ne_bytes(
                    crate::partition::hash::hash_bytes_extended(b"gres", 1)
                        .expect("hash")
                        .to_ne_bytes(),
                ))
        );
        assert!(
            ev("hashoidvectorextended('1 2'::oidvector, 1::int8)")
                == Datum::Int8(i64::from_ne_bytes(
                    crate::partition::hash::hash_bytes_extended(
                        &[1_i32.to_ne_bytes(), 2_i32.to_ne_bytes()].concat(),
                        1,
                    )
                    .expect("hash")
                    .to_ne_bytes(),
                ))
        );
        assert!(ev("hashbpchar('gres'::char(8))") == ev("hashbpchar('gres')"));
        assert!(ev("hashfloat4(42)") == ev("hashfloat8(42)"));
        assert!(ev("hashfloat8(-0::float8)") == Datum::Int4(0));
        assert!(ev("hashfloat4extended(0::float4, 1::int8)") == Datum::Int8(1));
        assert!(
            ev("hashint8extended(-42::int8, 1::int8)")
                == Datum::Int8(i64::from_ne_bytes(
                    crate::partition::hash::hash_int64_extended(-42, 1).to_ne_bytes(),
                ))
        );
        let uuid = crabka_pgtypes::uuid::UuidBytes::parse("a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11")
            .expect("uuid");
        assert!(
            ev("uuid_hash_extended('a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11', 1::int8)")
                == Datum::Int8(i64::from_ne_bytes(
                    crate::partition::hash::hash_bytes_extended(&uuid.0, 1)
                        .expect("hash")
                        .to_ne_bytes(),
                ))
        );
        let lsn = 0x16_0b37_4d84_u64;
        assert!(
            ev("pg_lsn_hash_extended('16/B374D84'::pg_lsn, 1::int8)")
                == Datum::Int8(i64::from_ne_bytes(
                    crate::partition::hash::hash_int64_extended(lsn.cast_signed(), 1).to_ne_bytes(),
                ))
        );
        assert!(matches!(ev("hash_range(int4range(1, 2))"), Datum::Int4(_)));
        assert!(matches!(
            ev("hash_range_extended(int4range(1, 2), 0)"),
            Datum::Int8(_)
        ));
        assert!(matches!(
            ev("hash_multirange('{[1,2)}'::int4multirange)"),
            Datum::Int4(_)
        ));
        assert!(matches!(
            ev("hash_multirange_extended('{[1,2)}'::int4multirange, 0)"),
            Datum::Int8(_)
        ));
        assert!(ev("hashint4(null::int4)") == Datum::Null);
        assert!(err_code("hashint4(42::int8)", None) == "42883");
        assert!(err_code("hashint2(42)", None) == "42883");
    }

    #[test]
    fn pg_sleep_waits_and_observes_query_cancellation() {
        let started = Instant::now();
        assert_eq!(ev("pg_sleep(0.02)"), Datum::Text(String::new()));
        assert!(started.elapsed() >= Duration::from_millis(10));
        assert_eq!(ev("pg_sleep('NaN'::float8)"), Datum::Text(String::new()));
        assert_eq!(
            crate::eval::infer_type(&pexpr("pg_sleep(0.02)").expect("parse"), &Scope::empty())
                .expect("type"),
            crate::routine::VOID_RESULT_TYPE
        );

        let canceled = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(true));
        let error = crate::session::with_query_cancel_runtime(Some(canceled), || {
            let ctx = crate::clock::EvalCtx::test_default();
            crate::eval::eval(
                &pexpr("pg_sleep(1)").expect("parse"),
                &Scope::empty(),
                &[],
                &ctx,
            )
            .expect_err("cancelled sleep")
        });
        assert_eq!(error.into_pg().code, "57014");
    }
}
