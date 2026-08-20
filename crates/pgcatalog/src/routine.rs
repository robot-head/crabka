//! P2: the SQL-routine catalog — `CREATE FUNCTION`, `CREATE PROCEDURE` and
//! `CREATE AGGREGATE`, and the `pg_proc` rows they produce.
//!
//! The catalog stores a routine as its *source text* plus the resolved
//! signature, the same shape the view catalog uses. It parses the body again
//! when a caller calls the routine, so a routine never carries a stale plan.
//! Overloads coexist because the storage key is the routine's
//! `name(argtype, …)` identity. That is exactly the identity `PostgreSQL` uses
//! for `DROP FUNCTION f(int)`.

use crabka_pgkv::{Kv, KvError, WriteOp};
use crabka_pgtypes::ColumnType;
use zerocopy::{FromBytes, IntoBytes, byteorder::big_endian::U32};

use crate::{
    CatalogError,
    serde::{read_string, read_type, take_n, take_u8, write_str, write_type},
};

/// The first OID handed to a user routine.
///
/// It sits above every other reserved catalog band, so a routine's
/// `pg_proc.oid` never collides with a relation's.
pub const ROUTINE_OID_BASE: u32 = 140_000;

/// A routine's kind, which `PostgreSQL` reports as `pg_proc.prokind`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RoutineKind {
    Function,
    Procedure,
    /// A user-defined aggregate, whose definition rides along in
    /// [`Routine::aggregate`].
    Aggregate,
}

impl RoutineKind {
    /// The `pg_proc.prokind` letter.
    #[must_use]
    pub const fn catalog_code(self) -> &'static str {
        match self {
            Self::Function => "f",
            Self::Procedure => "p",
            Self::Aggregate => "a",
        }
    }

    /// The word `PostgreSQL` uses for this kind in error messages and tags.
    #[must_use]
    pub const fn word(self) -> &'static str {
        match self {
            Self::Function => "function",
            Self::Procedure => "procedure",
            Self::Aggregate => "aggregate",
        }
    }

    fn code(self) -> u8 {
        match self {
            Self::Function => 0,
            Self::Procedure => 1,
            Self::Aggregate => 2,
        }
    }

    fn from_code(code: u8) -> Option<Self> {
        match code {
            0 => Some(Self::Function),
            1 => Some(Self::Procedure),
            2 => Some(Self::Aggregate),
            _ => None,
        }
    }
}

/// A parameter mode, which `PostgreSQL` reports as `pg_proc.proargmodes`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum ParamMode {
    #[default]
    In,
    Out,
    InOut,
    Variadic,
}

impl ParamMode {
    /// True when the mode contributes to the routine's input signature.
    #[must_use]
    pub const fn is_input(self) -> bool {
        matches!(self, Self::In | Self::InOut | Self::Variadic)
    }

    /// True when the mode contributes to the routine's output row.
    #[must_use]
    pub const fn is_output(self) -> bool {
        matches!(self, Self::Out | Self::InOut)
    }

    /// The `pg_proc.proargmodes` letter.
    #[must_use]
    pub const fn catalog_code(self) -> &'static str {
        match self {
            Self::In => "i",
            Self::Out => "o",
            Self::InOut => "b",
            Self::Variadic => "v",
        }
    }

    /// The prefix `pg_get_function_arguments` writes before the parameter.
    #[must_use]
    pub const fn spelled_prefix(self) -> &'static str {
        match self {
            Self::In => "",
            Self::Out => "OUT ",
            Self::InOut => "INOUT ",
            Self::Variadic => "VARIADIC ",
        }
    }

    fn code(self) -> u8 {
        match self {
            Self::In => 0,
            Self::Out => 1,
            Self::InOut => 2,
            Self::Variadic => 3,
        }
    }

    fn from_code(code: u8) -> Option<Self> {
        match code {
            0 => Some(Self::In),
            1 => Some(Self::Out),
            2 => Some(Self::InOut),
            3 => Some(Self::Variadic),
            _ => None,
        }
    }
}

/// A type named in a routine signature.
///
/// `column` is `None` for a composite type named by its relation, which the
/// catalog resolves through the relation's own schema when the routine runs.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RoutineType {
    pub column: Option<ColumnType>,
    pub name: String,
}

impl RoutineType {
    /// A built-in scalar type.
    #[must_use]
    pub fn builtin(ty: ColumnType) -> Self {
        Self {
            column: Some(ty),
            name: ty.name().to_string(),
        }
    }

    /// A type that is named but not resolved to a built-in, such as a
    /// composite or `void`.
    #[must_use]
    pub const fn named(name: String) -> Self {
        Self { column: None, name }
    }

    /// True for `RETURNS void`, which produces one NULL-valued row.
    #[must_use]
    pub fn is_void(&self) -> bool {
        self.column.is_none() && self.name == "void"
    }

    /// True for `RETURNS record`, whose shape comes from the call site.
    #[must_use]
    pub fn is_record(&self) -> bool {
        self.column.is_none() && self.name == "record"
    }
}

/// One declared parameter of a routine.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RoutineParam {
    pub name: Option<String>,
    pub mode: ParamMode,
    pub ty: RoutineType,
    /// SQL source of the parameter's `DEFAULT`, if written.
    pub default: Option<String>,
}

/// What a routine returns.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RoutineResult {
    /// No `RETURNS` clause: a procedure, or a function whose whole result comes
    /// from its `OUT`/`INOUT` parameters.
    Unspecified,
    Type {
        ty: RoutineType,
        setof: bool,
    },
    Table(Vec<(String, RoutineType)>),
}

/// How the author wrote a routine's body.
///
/// This decides how `pg_get_functiondef` renders the body, and whether
/// `pg_proc.prosrc` carries it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BodyForm {
    /// `AS 'text'` / `AS $$ … $$`. The body is `pg_proc.prosrc`.
    Source,
    /// `BEGIN ATOMIC … END`. The body is `pg_proc.prosqlbody`, and `prosrc` is
    /// empty, exactly as `PostgreSQL` stores it.
    Atomic,
    /// `RETURN <expr>`, the `PostgreSQL` 14 single-expression SQL body. The
    /// catalog stores it like `Atomic` but renders it as `RETURN`.
    Return,
}

/// The definition of a user-defined aggregate, carried by the [`Routine`] whose
/// [`RoutineKind`] is [`RoutineKind::Aggregate`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AggregateDefinition {
    /// The transition function's name, resolved against the routine catalog
    /// when the aggregate runs.
    pub transfn: String,
    /// The transition (state) type, `pg_aggregate.aggtranstype`.
    pub transtype: RoutineType,
    /// The final function's name, if the definition named one.
    pub finalfn: Option<String>,
    /// The optional partial-aggregation combine function.
    pub combinefn: Option<String>,
    /// The optional internal-state serialization function.
    pub serialfn: Option<String>,
    /// The optional internal-state deserialization function.
    pub deserialfn: Option<String>,
    /// `pg_aggregate.aggfinalmodify` (`r`, `s`, or `w`).
    pub finalfunc_modify: char,
    /// `pg_aggregate.agginitval`: the initial state's text representation.
    pub initcond: Option<String>,
    /// Parameters before `ORDER BY` in an ordered-set signature. Zero for an
    /// ordinary aggregate.
    pub direct_args: usize,
    /// Parameters after `ORDER BY` in an ordered-set signature. Zero for an
    /// ordinary aggregate.
    pub ordered_args: usize,
    /// Whether the final function receives NULL placeholders for every input.
    pub finalfunc_extra: bool,
    /// Whether an ordered-set aggregate is hypothetical-set.
    pub hypothetical: bool,
    /// Options this engine records for catalog fidelity but does not execute,
    /// each already spelled `name=value`.
    pub unimplemented: Vec<String>,
}

/// A stored SQL routine.
///
/// `cost`/`rows` are `f64`, so the type is `PartialEq` but not `Eq`.
#[derive(Debug, Clone, PartialEq)]
pub struct Routine {
    pub oid: u32,
    pub name: String,
    pub kind: RoutineKind,
    pub params: Vec<RoutineParam>,
    pub result: RoutineResult,
    pub language: String,
    /// The body's source text. For [`BodyForm::Return`] this is the expression
    /// alone; for C-language routines this is the `pg_proc.prosrc` link symbol;
    /// for the other forms it is the whole body.
    pub body: String,
    /// The C-language object file stored in `pg_proc.probin`; `None` for
    /// routines in languages that do not load an external object.
    pub object_file: Option<String>,
    pub body_form: BodyForm,
    /// `pg_proc.provolatile` (`i`/`s`/`v`).
    pub volatility: char,
    /// `pg_proc.proparallel` (`s`/`r`/`u`).
    pub parallel: char,
    pub strict: bool,
    pub security_definer: bool,
    pub leakproof: bool,
    pub cost: f64,
    pub rows: f64,
    /// `pg_proc.proconfig` entries, each already spelled `name=value`.
    pub config: Vec<String>,
    pub owner: String,
    /// The aggregate definition, present exactly when `kind` is
    /// [`RoutineKind::Aggregate`].
    pub aggregate: Option<AggregateDefinition>,
}

impl Routine {
    /// The routine's input parameters, in declaration order.
    pub fn input_params(&self) -> impl Iterator<Item = &RoutineParam> {
        self.params.iter().filter(|p| p.mode.is_input())
    }

    /// The routine's output parameters, in declaration order.
    pub fn output_params(&self) -> impl Iterator<Item = &RoutineParam> {
        self.params.iter().filter(|p| p.mode.is_output())
    }

    /// The input type names that identify this routine among its overloads.
    #[must_use]
    pub fn input_type_names(&self) -> Vec<String> {
        self.input_params().map(|p| p.ty.name.clone()).collect()
    }

    /// How many trailing input parameters carry a `DEFAULT`.
    #[must_use]
    pub fn default_count(&self) -> usize {
        self.input_params().filter(|p| p.default.is_some()).count()
    }

    /// The routine's `name(argtype, …)` identity. It is the same identity
    /// `PostgreSQL` prints in `42883` and accepts in `DROP FUNCTION`.
    #[must_use]
    pub fn identity(&self) -> String {
        signature_identity(&self.name, &self.input_type_names())
    }

    /// True when this routine is a user-defined aggregate.
    #[must_use]
    pub fn is_aggregate(&self) -> bool {
        self.kind == RoutineKind::Aggregate
    }

    /// True when the routine returns a set (`SETOF`, or `RETURNS TABLE`).
    #[must_use]
    pub fn returns_set(&self) -> bool {
        match &self.result {
            RoutineResult::Type { setof, .. } => *setof,
            RoutineResult::Table(_) => true,
            RoutineResult::Unspecified => false,
        }
    }
}

/// Spell a routine identity the way `PostgreSQL` does.
///
/// The form is `name(t1,t2)`, with no space after the comma. A zero-argument
/// routine is `name()`.
#[must_use]
pub fn signature_identity(name: &str, arg_types: &[String]) -> String {
    format!("{name}({})", arg_types.join(","))
}

fn routine_prefix() -> Vec<u8> {
    b"\0\0\0\0catalog_routine/".to_vec()
}

fn routine_key(identity: &str) -> Vec<u8> {
    let mut k = routine_prefix();
    k.extend_from_slice(identity.as_bytes());
    k
}

/// The routine OID counter's key.
///
/// It sits in the `meta` family rather than beside the routine records, so the
/// routine scan sees only routines.
fn next_routine_oid_key() -> Vec<u8> {
    b"\0\0\0\0meta/next_routine_oid".to_vec()
}

fn read_next_routine_oid(kv: &dyn Kv) -> Result<u32, CatalogError> {
    match kv.get(&next_routine_oid_key())? {
        Some(bytes) => {
            let (value, _) = U32::read_from_prefix(bytes.as_slice())
                .map_err(|_| KvError::CorruptRow("next routine oid is not u32".into()))?;
            Ok(value.get())
        }
        None => Ok(ROUTINE_OID_BASE),
    }
}

/// Look up a routine by its `name(argtype, …)` identity.
///
/// # Errors
///
/// Returns storage/corruption errors from the catalog KV seam.
pub fn get_routine(kv: &dyn Kv, identity: &str) -> Result<Option<Routine>, CatalogError> {
    match kv.get(&routine_key(identity))? {
        Some(bytes) => Ok(Some(deserialize_routine(&bytes)?)),
        None => Ok(None),
    }
}

/// Every stored routine, in catalog-identity order.
///
/// # Errors
///
/// Returns storage/corruption errors from the catalog KV seam.
pub fn list_routines(kv: &dyn Kv) -> Result<Vec<Routine>, CatalogError> {
    kv.scan_prefix(&routine_prefix())?
        .into_iter()
        .map(|(_, bytes)| deserialize_routine(&bytes).map_err(CatalogError::from))
        .collect()
}

/// Every stored routine carrying `name`, in catalog-identity order.
///
/// # Errors
///
/// Returns storage/corruption errors from the catalog KV seam.
pub fn routines_named(kv: &dyn Kv, name: &str) -> Result<Vec<Routine>, CatalogError> {
    let mut prefix = routine_prefix();
    prefix.extend_from_slice(name.as_bytes());
    prefix.push(b'(');
    kv.scan_prefix(&prefix)?
        .into_iter()
        .map(|(_, bytes)| deserialize_routine(&bytes).map_err(CatalogError::from))
        .collect()
}

/// The write batch that stores `routine`, and allocates its OID when it is new.
///
/// A replacement keeps the existing OID, which matches
/// `CREATE OR REPLACE FUNCTION`.
///
/// # Errors
///
/// Returns storage/corruption errors from the catalog KV seam.
pub fn put_routine_ops(kv: &dyn Kv, routine: &Routine) -> Result<Vec<WriteOp>, CatalogError> {
    let identity = routine.identity();
    let mut stored = routine.clone();
    let mut ops = Vec::new();
    if stored.oid == 0 {
        if let Some(existing) = get_routine(kv, &identity)? {
            stored.oid = existing.oid;
        } else {
            let oid = read_next_routine_oid(kv)?;
            stored.oid = oid;
            ops.push(WriteOp::Put {
                key: next_routine_oid_key(),
                value: U32::new(oid + 1).as_bytes().to_vec(),
            });
        }
    }
    ops.push(WriteOp::Put {
        key: routine_key(&identity),
        value: serialize_routine(&stored),
    });
    Ok(ops)
}

/// The write batch that removes the routine with `identity`.
#[must_use]
pub fn drop_routine_ops(identity: &str) -> Vec<WriteOp> {
    vec![WriteOp::Delete {
        key: routine_key(identity),
    }]
}

const ROUTINE_VERSION: u8 = 5;

fn write_routine_type(out: &mut Vec<u8>, ty: &RoutineType) {
    match ty.column {
        Some(column) => {
            out.push(1);
            write_type(out, column);
        }
        None => out.push(0),
    }
    write_str(out, &ty.name);
}

fn read_routine_type(cur: &mut &[u8]) -> Result<RoutineType, KvError> {
    let column = match take_u8(cur)? {
        0 => None,
        1 => Some(read_type(cur)?),
        other => {
            return Err(KvError::CorruptRow(format!(
                "unknown routine type tag {other}"
            )));
        }
    };
    Ok(RoutineType {
        column,
        name: read_string(cur)?,
    })
}

fn write_count(out: &mut Vec<u8>, count: usize) {
    let value = u32::try_from(count).expect("routine list length must fit in u32");
    out.extend_from_slice(&value.to_be_bytes());
}

fn read_count(cur: &mut &[u8]) -> Result<usize, KvError> {
    let bytes: [u8; 4] = take_n(cur, 4)?
        .try_into()
        .expect("take_n returned exactly four bytes");
    Ok(usize::try_from(u32::from_be_bytes(bytes)).expect("u32 fits in usize on supported targets"))
}

fn write_opt_str(out: &mut Vec<u8>, value: Option<&str>) {
    match value {
        Some(text) => {
            out.push(1);
            write_str(out, text);
        }
        None => out.push(0),
    }
}

fn read_opt_str(cur: &mut &[u8]) -> Result<Option<String>, KvError> {
    match take_u8(cur)? {
        0 => Ok(None),
        1 => Ok(Some(read_string(cur)?)),
        other => Err(KvError::CorruptRow(format!(
            "unknown optional-string tag {other}"
        ))),
    }
}

/// An aggregate definition: a presence byte, and for a present definition the
/// transition function name, the transition type, support functions, initial
/// condition, and then the recorded-but-unexecuted option list.
fn write_aggregate(out: &mut Vec<u8>, aggregate: Option<&AggregateDefinition>) {
    let Some(aggregate) = aggregate else {
        out.push(0);
        return;
    };
    out.push(1);
    write_str(out, &aggregate.transfn);
    write_routine_type(out, &aggregate.transtype);
    write_opt_str(out, aggregate.finalfn.as_deref());
    write_opt_str(out, aggregate.combinefn.as_deref());
    write_opt_str(out, aggregate.serialfn.as_deref());
    write_opt_str(out, aggregate.deserialfn.as_deref());
    out.push(aggregate.finalfunc_modify as u8);
    write_opt_str(out, aggregate.initcond.as_deref());
    write_count(out, aggregate.direct_args);
    write_count(out, aggregate.ordered_args);
    out.push(u8::from(aggregate.finalfunc_extra));
    out.push(u8::from(aggregate.hypothetical));
    write_count(out, aggregate.unimplemented.len());
    for option in &aggregate.unimplemented {
        write_str(out, option);
    }
}

/// Reads back what [`write_aggregate`] wrote, refusing any presence byte
/// outside the two it writes.
fn read_aggregate(cur: &mut &[u8]) -> Result<Option<AggregateDefinition>, KvError> {
    match take_u8(cur)? {
        0 => Ok(None),
        1 => {
            let transfn = read_string(cur)?;
            let transtype = read_routine_type(cur)?;
            let finalfn = read_opt_str(cur)?;
            let combinefn = read_opt_str(cur)?;
            let serialfn = read_opt_str(cur)?;
            let deserialfn = read_opt_str(cur)?;
            let finalfunc_modify = char::from(take_u8(cur)?);
            let initcond = read_opt_str(cur)?;
            let direct_args = read_count(cur)?;
            let ordered_args = read_count(cur)?;
            let finalfunc_extra = take_u8(cur)? != 0;
            let hypothetical = take_u8(cur)? != 0;
            let count = read_count(cur)?;
            let mut unimplemented = Vec::with_capacity(count.min(1024));
            for _ in 0..count {
                unimplemented.push(read_string(cur)?);
            }
            Ok(Some(AggregateDefinition {
                transfn,
                transtype,
                finalfn,
                combinefn,
                serialfn,
                deserialfn,
                finalfunc_modify,
                initcond,
                direct_args,
                ordered_args,
                finalfunc_extra,
                hypothetical,
                unimplemented,
            }))
        }
        other => Err(KvError::CorruptRow(format!(
            "unknown aggregate definition tag {other}"
        ))),
    }
}

/// Serialize a routine for the catalog KV.
///
/// # Panics
///
/// Panics when a list length or string exceeds its `u32` wire limit.
#[must_use]
pub fn serialize_routine(routine: &Routine) -> Vec<u8> {
    let mut out = vec![ROUTINE_VERSION];
    out.extend_from_slice(&routine.oid.to_be_bytes());
    write_str(&mut out, &routine.name);
    out.push(routine.kind.code());
    write_count(&mut out, routine.params.len());
    for param in &routine.params {
        write_opt_str(&mut out, param.name.as_deref());
        out.push(param.mode.code());
        write_routine_type(&mut out, &param.ty);
        write_opt_str(&mut out, param.default.as_deref());
    }
    match &routine.result {
        RoutineResult::Unspecified => out.push(0),
        RoutineResult::Type { ty, setof } => {
            out.push(1);
            write_routine_type(&mut out, ty);
            out.push(u8::from(*setof));
        }
        RoutineResult::Table(columns) => {
            out.push(2);
            write_count(&mut out, columns.len());
            for (name, ty) in columns {
                write_str(&mut out, name);
                write_routine_type(&mut out, ty);
            }
        }
    }
    write_str(&mut out, &routine.language);
    write_str(&mut out, &routine.body);
    write_opt_str(&mut out, routine.object_file.as_deref());
    out.push(match routine.body_form {
        BodyForm::Source => 0,
        BodyForm::Atomic => 1,
        BodyForm::Return => 2,
    });
    out.push(routine.volatility as u8);
    out.push(routine.parallel as u8);
    out.push(u8::from(routine.strict));
    out.push(u8::from(routine.security_definer));
    out.push(u8::from(routine.leakproof));
    out.extend_from_slice(&routine.cost.to_be_bytes());
    out.extend_from_slice(&routine.rows.to_be_bytes());
    write_count(&mut out, routine.config.len());
    for entry in &routine.config {
        write_str(&mut out, entry);
    }
    write_str(&mut out, &routine.owner);
    write_aggregate(&mut out, routine.aggregate.as_ref());
    out
}

/// Deserialize a routine record.
///
/// # Errors
///
/// Returns catalog corruption errors for truncated or unsupported bytes.
///
/// # Panics
///
/// Panics only if a fixed-width slice validated by the decoder cannot be
/// converted to its corresponding array.
pub fn deserialize_routine(bytes: &[u8]) -> Result<Routine, KvError> {
    let mut cur = bytes;
    let version = take_u8(&mut cur)?;
    if version != ROUTINE_VERSION {
        return Err(KvError::CorruptRow(format!(
            "unknown routine version {version}"
        )));
    }
    let oid = u32::from_be_bytes(
        take_n(&mut cur, 4)?
            .try_into()
            .expect("take_n returned exactly four bytes"),
    );
    let name = read_string(&mut cur)?;
    let kind = RoutineKind::from_code(take_u8(&mut cur)?)
        .ok_or_else(|| KvError::CorruptRow("unknown routine kind".into()))?;
    let param_count = read_count(&mut cur)?;
    let mut params = Vec::with_capacity(param_count.min(1024));
    for _ in 0..param_count {
        let param_name = read_opt_str(&mut cur)?;
        let mode = ParamMode::from_code(take_u8(&mut cur)?)
            .ok_or_else(|| KvError::CorruptRow("unknown parameter mode".into()))?;
        params.push(RoutineParam {
            name: param_name,
            mode,
            ty: read_routine_type(&mut cur)?,
            default: read_opt_str(&mut cur)?,
        });
    }
    let result = match take_u8(&mut cur)? {
        0 => RoutineResult::Unspecified,
        1 => {
            let ty = read_routine_type(&mut cur)?;
            RoutineResult::Type {
                ty,
                setof: take_u8(&mut cur)? != 0,
            }
        }
        2 => {
            let count = read_count(&mut cur)?;
            let mut columns = Vec::with_capacity(count.min(1024));
            for _ in 0..count {
                let column_name = read_string(&mut cur)?;
                columns.push((column_name, read_routine_type(&mut cur)?));
            }
            RoutineResult::Table(columns)
        }
        other => {
            return Err(KvError::CorruptRow(format!(
                "unknown routine result tag {other}"
            )));
        }
    };
    let language = read_string(&mut cur)?;
    let body = read_string(&mut cur)?;
    let object_file = read_opt_str(&mut cur)?;
    let body_form = match take_u8(&mut cur)? {
        0 => BodyForm::Source,
        1 => BodyForm::Atomic,
        2 => BodyForm::Return,
        other => {
            return Err(KvError::CorruptRow(format!(
                "unknown routine body form {other}"
            )));
        }
    };
    let volatility = char::from(take_u8(&mut cur)?);
    let parallel = char::from(take_u8(&mut cur)?);
    let strict = take_u8(&mut cur)? != 0;
    let security_definer = take_u8(&mut cur)? != 0;
    let leakproof = take_u8(&mut cur)? != 0;
    let cost = f64::from_be_bytes(
        take_n(&mut cur, 8)?
            .try_into()
            .expect("take_n returned exactly eight bytes"),
    );
    let rows = f64::from_be_bytes(
        take_n(&mut cur, 8)?
            .try_into()
            .expect("take_n returned exactly eight bytes"),
    );
    let config_count = read_count(&mut cur)?;
    let mut config = Vec::with_capacity(config_count.min(1024));
    for _ in 0..config_count {
        config.push(read_string(&mut cur)?);
    }
    let owner = read_string(&mut cur)?;
    let aggregate = read_aggregate(&mut cur)?;
    if !cur.is_empty() {
        return Err(KvError::CorruptRow(
            "trailing bytes in routine record".into(),
        ));
    }
    Ok(Routine {
        oid,
        name,
        kind,
        params,
        result,
        language,
        body,
        object_file,
        body_form,
        volatility,
        parallel,
        strict,
        security_definer,
        leakproof,
        cost,
        rows,
        config,
        owner,
        aggregate,
    })
}

#[cfg(test)]
mod tests {
    use assert2::assert;
    use crabka_pgkv::MemKv;

    use super::*;

    fn sample() -> Routine {
        Routine {
            oid: 0,
            name: "add2".into(),
            kind: RoutineKind::Function,
            params: vec![
                RoutineParam {
                    name: Some("a".into()),
                    mode: ParamMode::In,
                    ty: RoutineType::builtin(ColumnType::Int4),
                    default: None,
                },
                RoutineParam {
                    name: Some("b".into()),
                    mode: ParamMode::In,
                    ty: RoutineType::builtin(ColumnType::Int4),
                    default: Some("10".into()),
                },
            ],
            result: RoutineResult::Type {
                ty: RoutineType::builtin(ColumnType::Int4),
                setof: false,
            },
            language: "sql".into(),
            body: "SELECT $1 + $2".into(),
            object_file: None,
            body_form: BodyForm::Source,
            volatility: 'v',
            parallel: 'u',
            strict: false,
            security_definer: false,
            leakproof: false,
            cost: 100.0,
            rows: 0.0,
            config: vec!["search_path=public".into()],
            owner: "crab".into(),
            aggregate: None,
        }
    }

    /// `CREATE AGGREGATE newavg (int4)`, the fully-populated shape.
    fn sample_aggregate() -> Routine {
        Routine {
            name: "newavg".into(),
            kind: RoutineKind::Aggregate,
            params: vec![RoutineParam {
                name: None,
                mode: ParamMode::In,
                ty: RoutineType::builtin(ColumnType::Int4),
                default: None,
            }],
            result: RoutineResult::Type {
                ty: RoutineType::builtin(ColumnType::Numeric(None)),
                setof: false,
            },
            language: "internal".into(),
            body: String::new(),
            body_form: BodyForm::Source,
            aggregate: Some(AggregateDefinition {
                transfn: "int4_avg_accum".into(),
                transtype: RoutineType::named("_int8".into()),
                finalfn: Some("int8_avg".into()),
                combinefn: None,
                serialfn: None,
                deserialfn: None,
                finalfunc_modify: 'r',
                initcond: Some("{0,0}".into()),
                direct_args: 0,
                ordered_args: 0,
                finalfunc_extra: false,
                hypothetical: false,
                unimplemented: vec!["parallel=safe".into(), "sortop=<".into()],
            }),
            ..sample()
        }
    }

    #[test]
    fn identity_spells_the_postgresql_signature() {
        assert!(sample().identity() == "add2(integer,integer)");
        assert!(signature_identity("f", &[]) == "f()");
    }

    #[test]
    fn round_trips_through_the_catalog_encoding() {
        let mut routine = sample();
        routine.oid = 140_007;
        let bytes = serialize_routine(&routine);
        assert!(bytes[0] == ROUTINE_VERSION);
        assert!(deserialize_routine(&bytes).expect("decodes") == routine);
    }

    #[test]
    fn round_trips_c_object_file_separately_from_its_link_symbol() {
        let mut routine = sample();
        routine.language = "c".into();
        routine.body = "binary_coercible".into();
        routine.object_file = Some("$libdir/regress".into());

        let bytes = serialize_routine(&routine);
        assert!(deserialize_routine(&bytes).expect("decodes") == routine);
    }

    #[test]
    fn round_trips_every_result_shape() {
        for result in [
            RoutineResult::Unspecified,
            RoutineResult::Type {
                ty: RoutineType::named("void".into()),
                setof: false,
            },
            RoutineResult::Type {
                ty: RoutineType::builtin(ColumnType::Int4),
                setof: true,
            },
            RoutineResult::Table(vec![
                ("a".into(), RoutineType::builtin(ColumnType::Int4)),
                ("b".into(), RoutineType::builtin(ColumnType::Text)),
            ]),
        ] {
            let mut routine = sample();
            routine.oid = 140_001;
            routine.result = result;
            let bytes = serialize_routine(&routine);
            assert!(deserialize_routine(&bytes).expect("decodes") == routine);
        }
    }

    #[test]
    fn allocates_oids_from_the_routine_band_and_keeps_them_on_replace() {
        let kv = MemKv::default();
        let ops = put_routine_ops(&kv, &sample()).expect("ops");
        kv.write_batch(&ops).expect("write");
        let stored = get_routine(&kv, "add2(integer,integer)")
            .expect("read")
            .expect("present");
        assert!(stored.oid == ROUTINE_OID_BASE);

        let mut replacement = sample();
        replacement.body = "SELECT $1 * $2".into();
        let ops = put_routine_ops(&kv, &replacement).expect("ops");
        kv.write_batch(&ops).expect("write");
        let stored = get_routine(&kv, "add2(integer,integer)")
            .expect("read")
            .expect("present");
        assert!(stored.oid == ROUTINE_OID_BASE);
        assert!(stored.body == "SELECT $1 * $2");

        let mut other = sample();
        other.name = "add3".into();
        let ops = put_routine_ops(&kv, &other).expect("ops");
        kv.write_batch(&ops).expect("write");
        let stored = get_routine(&kv, "add3(integer,integer)")
            .expect("read")
            .expect("present");
        assert!(stored.oid == ROUTINE_OID_BASE + 1);
    }

    #[test]
    fn the_oid_counter_is_not_mistaken_for_a_table_schema() {
        // The table catalog scans every slash-free key under the catalog
        // prefix and decodes it as a relation schema, so the counter's key must
        // carry a slash or `list_tables` fails to decode it.
        let kv = MemKv::default();
        let ops = put_routine_ops(&kv, &sample()).expect("ops");
        kv.write_batch(&ops).expect("write");
        assert!(crate::list_tables(&kv).expect("tables scan") == Vec::new());
        assert!(crate::list_views(&kv).expect("views scan") == Vec::new());
        assert!(list_routines(&kv).expect("routine scan").len() == 1);
    }

    #[test]
    fn lists_routines_by_name() {
        let kv = MemKv::default();
        for name in ["f", "fg", "g"] {
            let mut routine = sample();
            routine.name = name.into();
            let ops = put_routine_ops(&kv, &routine).expect("ops");
            kv.write_batch(&ops).expect("write");
        }
        let named: Vec<String> = routines_named(&kv, "f")
            .expect("scan")
            .into_iter()
            .map(|r| r.identity())
            .collect();
        assert!(named == vec!["f(integer,integer)".to_string()]);
        assert!(list_routines(&kv).expect("scan").len() == 3);
    }

    #[test]
    fn drop_removes_the_stored_routine() {
        let kv = MemKv::default();
        let ops = put_routine_ops(&kv, &sample()).expect("ops");
        kv.write_batch(&ops).expect("write");
        kv.write_batch(&drop_routine_ops("add2(integer,integer)"))
            .expect("write");
        assert!(get_routine(&kv, "add2(integer,integer)").expect("read") == None);
    }

    #[test]
    fn round_trips_aggregates_alongside_plain_routines() {
        let minimal = Routine {
            aggregate: Some(AggregateDefinition {
                transfn: "int4larger".into(),
                transtype: RoutineType::builtin(ColumnType::Int4),
                finalfn: None,
                combinefn: None,
                serialfn: None,
                deserialfn: None,
                finalfunc_modify: 'r',
                initcond: None,
                direct_args: 0,
                ordered_args: 0,
                finalfunc_extra: false,
                hypothetical: false,
                unimplemented: Vec::new(),
            }),
            ..sample_aggregate()
        };
        for routine in [sample(), sample_aggregate(), minimal] {
            let bytes = serialize_routine(&routine);
            assert!(bytes[0] == ROUTINE_VERSION);
            assert!(deserialize_routine(&bytes).expect("decodes") == routine);
        }
    }

    #[test]
    fn round_trips_ordered_set_aggregate_metadata() {
        let mut routine = sample_aggregate();
        let aggregate = routine.aggregate.as_mut().expect("sample is an aggregate");
        aggregate.direct_args = 1;
        aggregate.ordered_args = 1;
        aggregate.finalfunc_extra = true;
        aggregate.hypothetical = true;

        let bytes = serialize_routine(&routine);
        assert!(deserialize_routine(&bytes).expect("decodes") == routine);
    }

    #[test]
    fn refuses_the_superseded_routine_version() {
        let mut bytes = serialize_routine(&sample_aggregate());
        bytes[0] = 2;
        assert!(let Err(KvError::CorruptRow(_)) = deserialize_routine(&bytes));
    }

    #[test]
    fn refuses_trailing_bytes_after_an_aggregate() {
        let mut bytes = serialize_routine(&sample_aggregate());
        bytes.push(0);
        assert!(let Err(KvError::CorruptRow(_)) = deserialize_routine(&bytes));
    }

    #[test]
    fn aggregate_is_a_kind_of_its_own() {
        let aggregate = sample_aggregate();
        assert!(aggregate.is_aggregate());
        assert!(!sample().is_aggregate());
        assert!(
            RoutineKind::from_code(RoutineKind::Aggregate.code()) == Some(RoutineKind::Aggregate)
        );
        assert!(RoutineKind::Aggregate.catalog_code() == "a");
        assert!(RoutineKind::Aggregate.word() == "aggregate");
    }

    #[test]
    fn allocates_and_preserves_an_aggregate_oid_like_a_function() {
        let kv = MemKv::default();
        let ops = put_routine_ops(&kv, &sample_aggregate()).expect("ops");
        kv.write_batch(&ops).expect("write");
        let stored = get_routine(&kv, "newavg(integer)")
            .expect("read")
            .expect("present");
        assert!(stored.oid == ROUTINE_OID_BASE);
        assert!(
            stored
                == Routine {
                    oid: ROUTINE_OID_BASE,
                    ..sample_aggregate()
                }
        );

        let mut replacement = sample_aggregate();
        replacement.aggregate.as_mut().expect("aggregate").initcond = Some("{1,1}".into());
        let ops = put_routine_ops(&kv, &replacement).expect("ops");
        kv.write_batch(&ops).expect("write");
        let stored = get_routine(&kv, "newavg(integer)")
            .expect("read")
            .expect("present");
        assert!(stored.oid == ROUTINE_OID_BASE);
        assert!(stored.aggregate == replacement.aggregate);
    }

    #[test]
    fn a_zero_argument_aggregate_is_identified_by_its_bare_name() {
        // `CREATE AGGREGATE newcnt (*)` declares no parameters at all.
        let routine = Routine {
            name: "newcnt".into(),
            params: Vec::new(),
            ..sample_aggregate()
        };
        assert!(routine.identity() == "newcnt()");

        let kv = MemKv::default();
        let ops = put_routine_ops(&kv, &routine).expect("ops");
        kv.write_batch(&ops).expect("write");
        let stored = get_routine(&kv, "newcnt()")
            .expect("read")
            .expect("present");
        assert!(stored.oid == ROUTINE_OID_BASE);
        assert!(routines_named(&kv, "newcnt").expect("scan").len() == 1);
        assert!(list_routines(&kv).expect("scan").len() == 1);
    }

    #[test]
    fn default_count_counts_only_input_parameters() {
        let mut routine = sample();
        routine.params.push(RoutineParam {
            name: Some("c".into()),
            mode: ParamMode::Out,
            ty: RoutineType::builtin(ColumnType::Int4),
            default: None,
        });
        assert!(routine.default_count() == 1);
        assert!(routine.input_params().count() == 2);
        assert!(routine.output_params().count() == 1);
    }
}
