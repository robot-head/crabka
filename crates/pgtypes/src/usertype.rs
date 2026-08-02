//! The registry of user-defined types: `CREATE TYPE` composites and enums, and
//! `CREATE DOMAIN` domains.
//!
//! [`ColumnType`] is `Copy`, and the engine passes it by value through every
//! expression, catalog row and wire description. A user type therefore cannot
//! hold an owned `String` or a boxed definition inside it. The type carries a
//! [`UserTypeRef`] instead: an oid plus a name interned for the process
//! lifetime. The definition itself lives here, keyed by oid.
//!
//! The registry is process-wide rather than per-session because
//! [`ColumnType::from_sql_name`] is a pure function reached from the parser,
//! from `CHECK`-constraint re-parsing, and from view expansion, none of which
//! hold a catalog handle. Gres already applies DDL outside transaction control
//! (a `CREATE TABLE` in a rolled-back block survives), so a type that survives
//! its rolled-back `CREATE TYPE` is the same documented divergence, not a new
//! one. The durable definition still lives in the catalog; [`register`] is how
//! the catalog's contents reach the parser.

use std::{
    collections::HashMap,
    sync::{OnceLock, RwLock},
};

use crate::datum::ColumnType;

/// The identity of a user-defined type: its `pg_type` oid and its name.
///
/// `Copy`, and therefore usable inside [`ColumnType`], because [`intern`]
/// interns the name and the name lives for the process lifetime.
#[derive(Debug, Clone, Copy, Eq)]
pub struct UserTypeRef {
    /// The type's `pg_type.oid`.
    pub oid: u32,
    /// The type's `pg_type.typname`, interned.
    pub name: &'static str,
}

impl PartialEq for UserTypeRef {
    /// Identity is the oid alone: two refs to the same type always agree on the
    /// name, and a renamed type keeps its oid exactly as `PostgreSQL` does.
    fn eq(&self, other: &Self) -> bool {
        self.oid == other.oid
    }
}

impl std::hash::Hash for UserTypeRef {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.oid.hash(state);
    }
}

/// A domain: a [`UserTypeRef`] plus the base type its values are stored and
/// encoded as. The base type is fixed at `CREATE DOMAIN` (`ALTER DOMAIN` can
/// change the default, the nullability and the constraints, never the base), so
/// it is safe to carry inside the `Copy` reference.
#[derive(Debug, Clone, Copy, Eq)]
pub struct DomainRef {
    /// The domain's `pg_type.oid`.
    pub oid: u32,
    /// The domain's `pg_type.typname`, interned.
    pub name: &'static str,
    /// `pg_type.typbasetype`: what the value actually is.
    pub base: &'static ColumnType,
}

/// A range type's identity and bound type.
#[derive(Debug, Clone, Copy, Eq)]
pub struct RangeRef {
    pub oid: u32,
    pub name: &'static str,
    pub subtype: &'static ColumnType,
}

/// A multirange type's identity and component range type.
#[derive(Debug, Clone, Copy, Eq)]
pub struct MultirangeRef {
    pub oid: u32,
    pub name: &'static str,
    pub range: RangeRef,
}

impl PartialEq for MultirangeRef {
    fn eq(&self, other: &Self) -> bool {
        self.oid == other.oid
    }
}

impl std::hash::Hash for MultirangeRef {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.oid.hash(state);
    }
}

impl PartialEq for RangeRef {
    fn eq(&self, other: &Self) -> bool {
        self.oid == other.oid
    }
}

impl std::hash::Hash for RangeRef {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.oid.hash(state);
    }
}

impl PartialEq for DomainRef {
    fn eq(&self, other: &Self) -> bool {
        self.oid == other.oid
    }
}

impl std::hash::Hash for DomainRef {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.oid.hash(state);
    }
}

impl DomainRef {
    /// The domain as a plain [`UserTypeRef`].
    #[must_use]
    pub fn as_ref(self) -> UserTypeRef {
        UserTypeRef {
            oid: self.oid,
            name: self.name,
        }
    }
}

/// One attribute of a composite type (`pg_attribute`).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CompositeField {
    /// `pg_attribute.attname`.
    pub name: String,
    /// `pg_attribute.atttypid`, as a column type.
    pub ty: ColumnType,
}

/// One `CHECK` constraint on a domain.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DomainCheck {
    /// `pg_constraint.conname`.
    pub name: String,
    /// The constraint's source text, with `VALUE` naming the tested value.
    pub expr: String,
}

/// What a user-defined type *is*.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum UserTypeBody {
    /// `CREATE TYPE … AS (field type, …)`: `pg_type.typtype = 'c'`.
    Composite(Vec<CompositeField>),
    /// `CREATE TYPE … AS ENUM (…)`: `pg_type.typtype = 'e'`. Labels are held in
    /// `pg_enum.enumsortorder` order, which is the order `<` uses.
    Enum(Vec<String>),
    /// `CREATE TYPE … AS RANGE` — `pg_type.typtype = 'r'`.
    Range(RangeBody),
    /// `CREATE DOMAIN … AS base …` — `pg_type.typtype = 'd'`.
    Domain(DomainBody),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RangeBody {
    pub subtype: ColumnType,
    pub collation: Option<String>,
}

/// A domain's constraints and default.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DomainBody {
    /// `pg_type.typbasetype`.
    pub base: ColumnType,
    /// `pg_type.typnotnull`: a `NOT NULL` domain constraint.
    pub not_null: bool,
    /// `pg_type.typdefault` source text, applied where a column of the domain
    /// has no default of its own.
    pub default: Option<String>,
    /// `CHECK (VALUE …)` constraints, in creation order.
    pub checks: Vec<DomainCheck>,
}

/// A registered user-defined type.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct UserType {
    /// `pg_type.oid`.
    pub oid: u32,
    /// `pg_type.typname`.
    pub name: String,
    /// The definition.
    pub body: UserTypeBody,
}

impl UserType {
    /// The `Copy` reference that a [`ColumnType`] carries for this type.
    #[must_use]
    pub fn type_ref(&self) -> UserTypeRef {
        UserTypeRef {
            oid: self.oid,
            name: intern(&self.name),
        }
    }

    /// This type as a [`ColumnType`].
    #[must_use]
    pub fn column_type(&self) -> ColumnType {
        match &self.body {
            UserTypeBody::Composite(_) => ColumnType::Record(Some(self.type_ref())),
            UserTypeBody::Enum(_) => ColumnType::Enum(self.type_ref()),
            UserTypeBody::Range(range) => ColumnType::Range(RangeRef {
                oid: self.oid,
                name: intern(&self.name),
                subtype: leak_column_type(range.subtype),
            }),
            UserTypeBody::Domain(domain) => ColumnType::Domain(DomainRef {
                oid: self.oid,
                name: intern(&self.name),
                base: leak_column_type(domain.base),
            }),
        }
    }

    /// The automatically-created multirange companion of a range type.
    #[must_use]
    pub fn multirange_type(&self) -> Option<ColumnType> {
        let ColumnType::Range(range) = self.column_type() else {
            return None;
        };
        Some(ColumnType::Multirange(MultirangeRef {
            oid: self.oid + 3,
            name: intern(&default_multirange_name(&self.name)),
            range,
        }))
    }

    /// `pg_type.typtype`.
    #[must_use]
    pub fn typtype(&self) -> &'static str {
        match &self.body {
            UserTypeBody::Composite(_) => "c",
            UserTypeBody::Enum(_) => "e",
            UserTypeBody::Range(_) => "r",
            UserTypeBody::Domain(_) => "d",
        }
    }

    /// The composite's fields, or `None` when this is not a composite.
    #[must_use]
    pub fn fields(&self) -> Option<&[CompositeField]> {
        match &self.body {
            UserTypeBody::Composite(fields) => Some(fields),
            _ => None,
        }
    }

    /// The enum's labels in sort order, or `None` when this is not an enum.
    #[must_use]
    pub fn labels(&self) -> Option<&[String]> {
        match &self.body {
            UserTypeBody::Enum(labels) => Some(labels),
            _ => None,
        }
    }

    /// The domain body, or `None` when this is not a domain.
    #[must_use]
    pub fn domain(&self) -> Option<&DomainBody> {
        match &self.body {
            UserTypeBody::Domain(domain) => Some(domain),
            _ => None,
        }
    }

    #[must_use]
    pub fn range(&self) -> Option<&RangeBody> {
        match &self.body {
            UserTypeBody::Range(range) => Some(range),
            _ => None,
        }
    }
}

/// The first oid handed out to a user-defined type.
///
/// Above every oid the engine reports for a built-in type, a catalog relation,
/// an index (`50_000 +`) or a system view (`120_0xx`), and above `FirstNormalObjectId`
/// (16384) so that `oid >= 16384` "is a user object" tests behave.
const FIRST_USER_TYPE_OID: u32 = 300_000;

/// The registry hands out oids in this stride so that a composite's type oid,
/// its array type oid and its backing `pg_class` relation oid never collide.
const OID_STRIDE: u32 = 4;

#[derive(Default)]
struct Registry {
    by_lower_name: HashMap<String, u32>,
    multirange_by_lower_name: HashMap<String, u32>,
    by_oid: HashMap<u32, UserType>,
    next_oid: u32,
}

fn registry() -> &'static RwLock<Registry> {
    static REGISTRY: OnceLock<RwLock<Registry>> = OnceLock::new();
    REGISTRY.get_or_init(|| {
        RwLock::new(Registry {
            next_oid: FIRST_USER_TYPE_OID,
            ..Registry::default()
        })
    })
}

fn interner() -> &'static RwLock<HashMap<String, &'static str>> {
    static INTERNER: OnceLock<RwLock<HashMap<String, &'static str>>> = OnceLock::new();
    INTERNER.get_or_init(|| RwLock::new(HashMap::new()))
}

/// Intern `name` so it can live inside a `Copy` [`ColumnType`].
///
/// [`intern`] is what makes `ColumnType::name() -> &'static str` work for a type
/// whose name is only known at run time. The leak is bounded by the number of
/// distinct type names a process ever sees, i.e. by DDL, not by traffic.
///
/// # Panics
///
/// If the process-wide interner lock is poisoned, which can only happen if
/// another thread panicked while holding it.
#[must_use]
pub fn intern(name: &str) -> &'static str {
    if let Some(interned) = interner()
        .read()
        .expect("user type interner is not poisoned")
        .get(name)
    {
        return interned;
    }
    let mut guard = interner()
        .write()
        .expect("user type interner is not poisoned");
    // Another thread may have interned the same name between the two locks.
    if let Some(interned) = guard.get(name) {
        return interned;
    }
    let leaked: &'static str = Box::leak(name.to_string().into_boxed_str());
    guard.insert(name.to_string(), leaked);
    leaked
}

/// Intern a [`ColumnType`] so a [`DomainRef`] can point at it.
fn leak_column_type(ty: ColumnType) -> &'static ColumnType {
    static LEAKED: OnceLock<RwLock<Vec<&'static ColumnType>>> = OnceLock::new();
    let cache = LEAKED.get_or_init(|| RwLock::new(Vec::new()));
    if let Some(found) = cache
        .read()
        .expect("leaked column types are not poisoned")
        .iter()
        .find(|candidate| ***candidate == ty)
    {
        return found;
    }
    let mut guard = cache.write().expect("leaked column types are not poisoned");
    if let Some(found) = guard.iter().find(|candidate| ***candidate == ty) {
        return found;
    }
    let leaked: &'static ColumnType = Box::leak(Box::new(ty));
    guard.push(leaked);
    leaked
}

/// Register `body` under `name` with a fresh oid, and return the registered
/// type. This function replaces an existing registration under the same name.
/// Callers enforce `PostgreSQL`'s duplicate-name rule (42710) before they get
/// here, and DDL that legitimately replaces a definition (`ALTER TYPE`) goes
/// through [`replace`].
///
/// # Panics
///
/// If the process-wide user-type registry lock is poisoned, which can only
/// happen if another thread panicked while holding it.
#[must_use]
pub fn register(name: &str, body: UserTypeBody) -> UserType {
    let mut guard = registry().write().expect("user type registry is healthy");
    let oid = guard.next_oid;
    guard.next_oid += OID_STRIDE;
    let ty = UserType {
        oid,
        name: name.to_string(),
        body,
    };
    guard.by_lower_name.insert(name.to_ascii_lowercase(), oid);
    if matches!(ty.body, UserTypeBody::Range(_)) {
        guard
            .multirange_by_lower_name
            .insert(default_multirange_name(name).to_ascii_lowercase(), oid);
    }
    guard.by_oid.insert(oid, ty.clone());
    // Intern eagerly so `column_type()` never has to take the interner lock
    // while the registry lock is held.
    drop(guard);
    let _ = intern(name);
    ty
}

/// Re-register a type that already has an oid: the catalog-hydration path and
/// the `ALTER TYPE` / `ALTER DOMAIN` path, both of which must preserve the oid.
///
/// # Panics
///
/// If the process-wide user-type registry lock is poisoned, which can only
/// happen if another thread panicked while holding it.
pub fn replace(ty: &UserType) {
    let _ = intern(&ty.name);
    if matches!(ty.body, UserTypeBody::Range(_)) {
        let _ = intern(&default_multirange_name(&ty.name));
    }
    let mut guard = registry().write().expect("user type registry is healthy");
    guard.by_lower_name.retain(|_, oid| *oid != ty.oid);
    guard
        .multirange_by_lower_name
        .retain(|_, oid| *oid != ty.oid);
    guard
        .by_lower_name
        .insert(ty.name.to_ascii_lowercase(), ty.oid);
    if matches!(ty.body, UserTypeBody::Range(_)) {
        guard.multirange_by_lower_name.insert(
            default_multirange_name(&ty.name).to_ascii_lowercase(),
            ty.oid,
        );
    }
    guard.by_oid.insert(ty.oid, ty.clone());
    guard.next_oid = guard.next_oid.max(ty.oid + OID_STRIDE);
}

/// Forget the type named `name` (`DROP TYPE` / `DROP DOMAIN`).
///
/// # Panics
///
/// If the process-wide user-type registry lock is poisoned, which can only
/// happen if another thread panicked while holding it.
pub fn unregister(name: &str) {
    // Only the NAME is forgotten. The oid keeps resolving, because a stored row
    // encodes its column's type oid: rows written before the drop still have to
    // decode afterwards, and the background vacuum reads exactly those rows when
    // it prunes a dropped table. Removing the oid too made every later read of
    // that data a `corrupt row encoding: column type oid N is not a registered
    // type`, and the vacuum then failed on every pass, forever.
    //
    // Dropping the name is what makes the type unreachable from SQL — a new
    // reference is 42704 and `CREATE TYPE` may reuse the name with a fresh oid.
    let mut guard = registry().write().expect("user type registry is healthy");
    if let Some(oid) = guard.by_lower_name.remove(&name.to_ascii_lowercase()) {
        guard
            .multirange_by_lower_name
            .retain(|_, found| *found != oid);
    }
}

/// The type registered under `name`, matched case-insensitively on the already
/// case-folded name the lexer produces.
///
/// # Panics
///
/// If the process-wide user-type registry lock is poisoned, which can only
/// happen if another thread panicked while holding it.
#[must_use]
pub fn lookup(name: &str) -> Option<UserType> {
    let guard = registry().read().expect("user type registry is healthy");
    let oid = *guard.by_lower_name.get(&name.to_ascii_lowercase())?;
    guard.by_oid.get(&oid).cloned()
}

/// The type with this oid.
///
/// # Panics
///
/// If the process-wide user-type registry lock is poisoned, which can only
/// happen if another thread panicked while holding it.
#[must_use]
pub fn lookup_oid(oid: u32) -> Option<UserType> {
    registry()
        .read()
        .expect("user type registry is healthy")
        .by_oid
        .get(&oid)
        .cloned()
}

/// Every registered type, ordered by oid so catalog scans are deterministic.
///
/// # Panics
///
/// If the process-wide user-type registry lock is poisoned, which can only
/// happen if another thread panicked while holding it.
#[must_use]
pub fn all() -> Vec<UserType> {
    let guard = registry().read().expect("user type registry is healthy");
    let mut types: Vec<UserType> = guard.by_oid.values().cloned().collect();
    types.sort_by_key(|ty| ty.oid);
    types
}

/// The `ColumnType` a SQL type name resolves to when it is not built in.
/// [`ColumnType::from_sql_name`] falls through to this.
#[must_use]
pub fn column_type_for_name(name: &str) -> Option<ColumnType> {
    if let Some(ty) = lookup(name) {
        return Some(ty.column_type());
    }
    let guard = registry().read().expect("user type registry is healthy");
    let oid = *guard
        .multirange_by_lower_name
        .get(&name.to_ascii_lowercase())?;
    guard.by_oid.get(&oid)?.multirange_type()
}

/// Resolve either a user type oid or its derived multirange oid.
#[must_use]
pub fn column_type_for_oid(oid: u32) -> Option<ColumnType> {
    if let Some(ty) = lookup_oid(oid) {
        return Some(ty.column_type());
    }
    lookup_oid(oid.checked_sub(3)?)?.multirange_type()
}

fn default_multirange_name(range_name: &str) -> String {
    range_name.strip_suffix("range").map_or_else(
        || format!("{range_name}_multirange"),
        |stem| format!("{stem}multirange"),
    )
}

/// The `pg_class` oid of the relation backing a composite type
/// (`pg_type.typrelid`), derived from the type oid so nothing has to store it.
#[must_use]
pub fn composite_relation_oid(type_oid: u32) -> u32 {
    type_oid + 1
}

/// The oid of the array type over a user-defined type (`pg_type.typarray`).
#[must_use]
pub fn user_array_oid(type_oid: u32) -> u32 {
    type_oid + 2
}

#[cfg(test)]
mod tests {

    #[test]
    fn range_registers_derived_multirange_name_and_oid() {
        use crate::{ColumnType, usertype::RangeBody};

        let range = register(
            "companion_textrange",
            UserTypeBody::Range(RangeBody {
                subtype: ColumnType::Text,
                collation: Some("C".into()),
            }),
        );
        let Some(ColumnType::Multirange(multirange)) =
            column_type_for_name("companion_textmultirange")
        else {
            panic!("derived multirange is registered");
        };
        assert_eq!(multirange.oid, range.oid + 3);
        assert_eq!(multirange.range.oid, range.oid);
        assert_eq!(
            column_type_for_oid(multirange.oid),
            Some(ColumnType::Multirange(multirange))
        );
    }

    /// `DROP TYPE` must make the name unresolvable without making stored rows
    /// undecodable: a row encodes its column's type oid, so the oid has to keep
    /// resolving after the drop. Losing it made every later read of that data a
    /// corrupt-row error and wedged the background vacuum on every pass.
    #[test]
    fn dropping_a_type_frees_the_name_but_keeps_the_oid_decodable() {
        use assert2::assert;

        let ty = register(
            "drop_tombstone_t",
            UserTypeBody::Enum(vec!["a".to_string()]),
        );

        unregister("drop_tombstone_t");

        assert!(lookup("drop_tombstone_t").is_none(), "name must be free");
        assert!(
            lookup_oid(ty.oid).is_some(),
            "oid {} must still decode stored rows",
            ty.oid
        );

        // The freed name may be reused, and takes a fresh oid.
        let reused = register(
            "drop_tombstone_t",
            UserTypeBody::Enum(vec!["b".to_string()]),
        );
        assert!(reused.oid != ty.oid);
        assert!(lookup("drop_tombstone_t").map(|t| t.oid) == Some(reused.oid));
    }
    use assert2::assert;

    use super::*;

    #[test]
    fn interning_returns_one_pointer_per_name() {
        let a = intern("ut_interned_name");
        let b = intern("ut_interned_name");
        assert!(std::ptr::eq(a, b));
        assert!(a == "ut_interned_name");
    }

    #[test]
    fn a_registered_composite_resolves_by_name_and_reports_its_fields() {
        let registered = register(
            "ut_reg_composite",
            UserTypeBody::Composite(vec![CompositeField {
                name: "x".into(),
                ty: ColumnType::Int4,
            }]),
        );
        let found = lookup("UT_REG_COMPOSITE").expect("case-insensitive lookup");
        assert!(found == registered);
        assert!(found.typtype() == "c");
        assert!(found.fields().expect("composite").len() == 1);
        assert!(found.labels().is_none());
        assert!(found.domain().is_none());
        assert!(column_type_for_name("ut_reg_composite") == Some(found.column_type()));
        assert!(matches!(found.column_type(), ColumnType::Record(Some(_))));
        unregister("ut_reg_composite");
        assert!(lookup("ut_reg_composite").is_none());
    }

    #[test]
    fn a_registered_domain_carries_its_base_type() {
        let registered = register(
            "ut_reg_domain",
            UserTypeBody::Domain(DomainBody {
                base: ColumnType::Numeric(None),
                not_null: true,
                default: Some("0".into()),
                checks: vec![DomainCheck {
                    name: "ut_reg_domain_check".into(),
                    expr: "VALUE > 0".into(),
                }],
            }),
        );
        let ColumnType::Domain(domain) = registered.column_type() else {
            panic!("a domain resolves to ColumnType::Domain");
        };
        assert!(*domain.base == ColumnType::Numeric(None));
        assert!(domain.name == "ut_reg_domain");
        assert!(domain.as_ref().oid == registered.oid);
        assert!(registered.typtype() == "d");
        assert!(registered.domain().expect("domain").not_null);
        unregister("ut_reg_domain");
    }

    #[test]
    fn replace_preserves_the_oid_so_alter_type_does_not_orphan_columns() {
        let created = register(
            "ut_replace_enum",
            UserTypeBody::Enum(vec!["a".into(), "b".into()]),
        );
        let mut altered = created.clone();
        altered.body = UserTypeBody::Enum(vec!["a".into(), "b".into(), "c".into()]);
        replace(&altered);
        let found = lookup("ut_replace_enum").expect("still registered");
        assert!(found.oid == created.oid);
        assert!(found.labels().expect("enum") == ["a", "b", "c"]);
        assert!(lookup_oid(created.oid) == Some(found));
        unregister("ut_replace_enum");
    }

    #[test]
    fn distinct_types_get_distinct_non_overlapping_oids() {
        let a = register("ut_oid_a", UserTypeBody::Composite(Vec::new()));
        let b = register("ut_oid_b", UserTypeBody::Composite(Vec::new()));
        assert!(a.oid != b.oid);
        assert!(a.oid >= FIRST_USER_TYPE_OID);
        // The derived relation and array oids of one type never reach the next.
        assert!(composite_relation_oid(a.oid) != b.oid);
        assert!(user_array_oid(a.oid) != b.oid);
        assert!(user_array_oid(a.oid) != composite_relation_oid(b.oid));
        assert!(all().iter().any(|ty| ty.oid == a.oid));
        unregister("ut_oid_a");
        unregister("ut_oid_b");
    }
}
