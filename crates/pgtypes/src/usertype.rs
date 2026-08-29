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
//! hold a catalog handle. The durable definition still lives in the catalog;
//! accepted DDL publishes an atomic catalog-snapshot delta here, and catalog
//! rollback publishes the inverse delta.
//!
//! Process-wide is *not* the same as per-catalog, and conflating the two is a
//! known defect — see [`CatalogTypes`], which owns all of the mutable state in
//! this module so that giving each catalog its own instance is a local change
//! rather than a rewrite.

use std::{
    collections::HashMap,
    sync::{Arc, OnceLock, RwLock},
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
    /// `pg_type.typarray`, which relation row types allocate independently of
    /// their composite type OID.
    pub array_oid: u32,
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
            array_oid: user_array_oid(self.oid),
            name: self.name,
        }
    }
}

/// A user-defined base type: a [`UserTypeRef`] plus the type whose physical
/// representation it borrows.
///
/// `CREATE TYPE … (LIKE = float4)` copies `float4`'s `typlen`, `typbyval` and
/// `typalign`, which is `PostgreSQL`'s way of saying "the same bytes". gres holds
/// that literally: a value of the base type is a `Datum` of `representation`.
/// The base type is still a *distinct* type — it inherits none of the
/// representation type's casts or operators, and the only way between the two
/// is a `CREATE CAST … WITHOUT FUNCTION` declared by hand.
#[derive(Debug, Clone, Copy, Eq)]
pub struct BaseRef {
    /// The base type's `pg_type.oid`.
    pub oid: u32,
    /// The base type's `pg_type.typname`, interned.
    pub name: &'static str,
    /// The type supplying `typlen`/`typbyval`/`typalign`, and so the `Datum`
    /// shape a value of this type is carried in.
    pub representation: &'static ColumnType,
}

impl PartialEq for BaseRef {
    fn eq(&self, other: &Self) -> bool {
        self.oid == other.oid
    }
}

impl std::hash::Hash for BaseRef {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.oid.hash(state);
    }
}

impl BaseRef {
    /// The base type as a plain [`UserTypeRef`].
    #[must_use]
    pub fn as_ref(self) -> UserTypeRef {
        UserTypeRef {
            oid: self.oid,
            array_oid: user_array_oid(self.oid),
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
    /// Whether existing stored values have been checked against this constraint.
    pub validated: bool,
}

/// What a user-defined type *is*.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum UserTypeBody {
    /// `CREATE TYPE … AS (field type, …)`: `pg_type.typtype = 'c'`.
    Composite(Vec<CompositeField>),
    /// `CREATE TYPE … AS ENUM (…)`: `pg_type.typtype = 'e'`. Labels are held in
    /// `pg_enum.enumsortorder` order, which is the order `<` uses.
    Enum(Vec<String>),
    /// An enum whose labels were repositioned by `ALTER TYPE ... ADD VALUE`.
    /// The bit patterns preserve PostgreSQL's `float4` sort keys across catalog
    /// writes without making the user-type definition non-`Eq`.
    EnumOrdered {
        labels: Vec<String>,
        sort_orders: Vec<u32>,
    },
    /// `CREATE TYPE … AS RANGE` — `pg_type.typtype = 'r'`.
    Range(RangeBody),
    /// `CREATE DOMAIN … AS base …` — `pg_type.typtype = 'd'`.
    Domain(DomainBody),
    /// `CREATE TYPE name;` with nothing after the name — a *shell*.
    ///
    /// A shell is a placeholder that carries a name and an oid and nothing
    /// else: `pg_type.typtype = 'p'`, `typisdefined = false`. It exists to
    /// break the cycle in a base type's definition, where the type's I/O
    /// functions must name the type and the type must name its I/O functions.
    /// A shell has no values and no [`ColumnType`], so it can be named in a
    /// routine signature and nowhere else.
    Shell,
    /// `CREATE TYPE name (INPUT = …, OUTPUT = …, …)` — a user-defined base
    /// type, `pg_type.typtype = 'b'`.
    Base(BaseBody),
}

/// A user-defined base type's definition.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BaseBody {
    /// The type whose `typlen`/`typbyval`/`typalign` this one copies, which is
    /// what `LIKE = T` selects. A value of the base type is held in this type's
    /// `Datum`, so the two are the same bytes — which is the whole content of
    /// binary coercibility between them.
    pub representation: ColumnType,
    /// `pg_type.typinput`: the routine that parses the external text form.
    pub input: String,
    /// `pg_type.typoutput`.
    pub output: String,
    /// `pg_type.typmodin`, when the type accepts a modifier list.
    pub typmod_in: Option<String>,
    /// `pg_type.typmodout`, when the type renders a modifier list.
    pub typmod_out: Option<String>,
    /// `pg_type.typcategory`, the one-character class `format_type` and the
    /// preference rules read.
    pub category: String,
    /// `pg_type.typispreferred`.
    pub preferred: bool,
    /// `pg_type.typdelim`, the array element separator.
    pub delimiter: String,
    /// `pg_type.typstorage`: `p`, `e`, `m`, or `x`.
    pub storage: char,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RangeBody {
    pub subtype: ColumnType,
    pub collation: Option<String>,
    /// Schema of an explicitly named multirange companion. `None` means the
    /// default companion in the range type's own schema.
    pub multirange_schema: Option<String>,
    /// Unqualified name of an explicitly named multirange companion.
    pub multirange_name: Option<String>,
}

/// A domain's constraints and default.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DomainBody {
    /// `pg_type.typbasetype`.
    pub base: ColumnType,
    /// `pg_type.typnotnull`: a `NOT NULL` domain constraint.
    pub not_null: bool,
    /// Explicit name of the `NOT NULL` constraint, when it was added as one.
    pub not_null_name: Option<String>,
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
    /// `pg_type.typarray`.
    pub array_oid: u32,
    /// `pg_namespace.nspname` for the type.
    pub schema: String,
    /// `pg_type.typname`, always unqualified.
    pub name: String,
    /// The definition.
    pub body: UserTypeBody,
}

impl UserType {
    /// The name used by SQL lookup: bare in `public`, schema-qualified
    /// everywhere else.
    #[must_use]
    pub fn qualified_name(&self) -> String {
        if self.schema == USER_TYPE_DEFAULT_SCHEMA {
            self.name.clone()
        } else {
            format!("{}.{}", self.schema, self.name)
        }
    }

    /// The `Copy` reference that a [`ColumnType`] carries for this type.
    #[must_use]
    pub fn type_ref(&self) -> UserTypeRef {
        UserTypeRef {
            oid: self.oid,
            array_oid: self.array_oid,
            name: intern(&self.qualified_name()),
        }
    }

    /// This type as a [`ColumnType`], or `None` for a shell.
    ///
    /// A shell is the one user type with no value representation at all, so it
    /// has no `ColumnType`. Returning `None` is what keeps `CREATE TABLE t (c
    /// shell)` and `'x'::shell` from finding a type to use.
    #[must_use]
    pub fn column_type(&self) -> Option<ColumnType> {
        let qualified_name = self.qualified_name();
        Some(match &self.body {
            UserTypeBody::Composite(_) => ColumnType::Record(Some(self.type_ref())),
            UserTypeBody::Enum(_) | UserTypeBody::EnumOrdered { .. } => {
                ColumnType::Enum(self.type_ref())
            }
            UserTypeBody::Range(range) => ColumnType::Range(RangeRef {
                oid: self.oid,
                name: intern(&qualified_name),
                subtype: leak_column_type(range.subtype),
            }),
            UserTypeBody::Domain(domain) => ColumnType::Domain(DomainRef {
                oid: self.oid,
                name: intern(&qualified_name),
                base: leak_column_type(domain.base),
            }),
            UserTypeBody::Base(base) => ColumnType::Base(BaseRef {
                oid: self.oid,
                name: intern(&qualified_name),
                representation: leak_column_type(base.representation),
            }),
            UserTypeBody::Shell => return None,
        })
    }

    /// The automatically-created multirange companion of a range type.
    #[must_use]
    pub fn multirange_type(&self) -> Option<ColumnType> {
        let Some(ColumnType::Range(range)) = self.column_type() else {
            return None;
        };
        Some(ColumnType::Multirange(MultirangeRef {
            oid: self.oid + 3,
            name: intern(&self.multirange_name()?),
            range,
        }))
    }

    /// The explicit or automatically-derived companion name.
    #[must_use]
    pub fn multirange_name(&self) -> Option<String> {
        let (schema, name) = self.multirange_identity()?;
        Some(if schema == USER_TYPE_DEFAULT_SCHEMA {
            name
        } else {
            format!("{schema}.{name}")
        })
    }

    /// Exact schema and unqualified name of a range's companion type.
    #[must_use]
    pub fn multirange_identity(&self) -> Option<(String, String)> {
        let UserTypeBody::Range(range) = &self.body else {
            return None;
        };
        match (&range.multirange_schema, &range.multirange_name) {
            (Some(schema), Some(name)) => Some((schema.clone(), name.clone())),
            (None, None) => Some((self.schema.clone(), default_multirange_name(&self.name))),
            _ => None,
        }
    }

    /// `pg_type.typtype`.
    #[must_use]
    pub fn typtype(&self) -> &'static str {
        match &self.body {
            UserTypeBody::Composite(_) => "c",
            UserTypeBody::Enum(_) | UserTypeBody::EnumOrdered { .. } => "e",
            UserTypeBody::Range(_) => "r",
            UserTypeBody::Domain(_) => "d",
            // A shell is `TYPTYPE_PSEUDO` until the base type completes it,
            // exactly as `TypeShellMake` leaves it.
            UserTypeBody::Shell => "p",
            UserTypeBody::Base(_) => "b",
        }
    }

    /// Whether this type is a shell: named, but with no definition yet.
    #[must_use]
    pub const fn is_shell(&self) -> bool {
        matches!(self.body, UserTypeBody::Shell)
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
            UserTypeBody::Enum(labels) | UserTypeBody::EnumOrdered { labels, .. } => Some(labels),
            _ => None,
        }
    }

    /// The `pg_enum.enumsortorder` bit patterns when this enum has explicit
    /// ordering metadata. Creation-order enums use consecutive integers.
    #[must_use]
    pub fn enum_sort_orders(&self) -> Option<&[u32]> {
        match &self.body {
            UserTypeBody::EnumOrdered { sort_orders, .. } => Some(sort_orders),
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

/// The default schema used by the process-wide parser registry.
pub const USER_TYPE_DEFAULT_SCHEMA: &str = "public";

/// The name and oid indexes of one [`CatalogTypes`].
///
/// Every map here is keyed by a SQL name or an oid, both of which are only
/// meaningful within a single catalog — which is why this lives inside
/// [`CatalogTypes`] rather than in a `static` of its own.
///
/// There is deliberately no oid counter here. Oids are allocated by the
/// catalog that will persist them (`crabka_pgcatalog::next_user_type_oid`,
/// from a durable per-catalog KV counter) and only ever *published* here. A
/// second counter in this process would be unreconcilable with that one rather
/// than merely redundant: oids are written into rows and onto the wire, so two
/// catalogs built in separate processes both start at 300000 and collide the
/// moment they are loaded together, however either of them counts.
#[derive(Default)]
struct TypeIndex {
    by_identity: HashMap<(String, String), u32>,
    by_lower_name: HashMap<String, u32>,
    multirange_by_identity: HashMap<(String, String), u32>,
    multirange_by_lower_name: HashMap<String, u32>,
    /// Shared rather than owned because [`CatalogTypes::lookup_oid`] is on the
    /// row-decode path — once per user-typed field of every row read — and a
    /// `UserType` clone is three heap allocations plus one per composite field
    /// or enum label. Handing back the `Arc` makes the lookup a refcount bump.
    by_oid: HashMap<u32, Arc<UserType>>,
}

impl TypeIndex {
    /// Forget every SQL name that resolves to `oid`, keeping the oid itself
    /// resolvable so stored rows still decode.
    fn remove_name_mappings(&mut self, oid: u32) {
        self.by_identity.retain(|_, found| *found != oid);
        self.by_lower_name.retain(|_, found| *found != oid);
        self.multirange_by_identity.retain(|_, found| *found != oid);
        self.multirange_by_lower_name
            .retain(|_, found| *found != oid);
    }

    /// Index `ty` under every name it answers to.
    fn insert(&mut self, ty: &UserType) {
        let qualified_name = ty.qualified_name();
        self.by_identity
            .insert((ty.schema.clone(), ty.name.clone()), ty.oid);
        self.by_lower_name
            .insert(qualified_name.to_ascii_lowercase(), ty.oid);
        if let Some(companion) = ty.multirange_name() {
            self.multirange_by_lower_name
                .insert(companion.to_ascii_lowercase(), ty.oid);
        }
        if let Some(identity) = ty.multirange_identity() {
            self.multirange_by_identity.insert(identity, ty.oid);
        }
        self.by_oid.insert(ty.oid, Arc::new(ty.clone()));
    }
}

/// The user-defined types of **one catalog**: the name and oid indexes, and the
/// cache of leaked `&'static ColumnType`s that a [`DomainRef`] base or a
/// [`RangeRef`] subtype points at.
///
/// # Why this is a struct and must stay one
///
/// Nothing here is process-global data. Oids are handed out from a per-catalog
/// KV counter, and names resolve against one catalog's `pg_type`. Yet every
/// operation below is reached today through a single process-wide singleton
/// (`catalog_types()`), so two `SqlEngine`s in one process assign the same oid
/// to different types and then resolve each other's type names — the last
/// engine to hydrate its catalog on session open wins, repeatedly.
///
/// The leaked-`ColumnType` cache aliases the same way and more quietly:
/// [`UserTypeRef`], [`DomainRef`], [`RangeRef`] and [`MultirangeRef`] all
/// compare on the oid alone, on the assumption that two references to the same
/// type agree on everything else. Two catalogs break that assumption, so one
/// catalog's leaked value is handed to another under the wrong name and over
/// the wrong base type, permanently, behind a `&'static`. Adding the name to
/// the cache key does not fix it — two catalogs whose `public.zdom` is oid
/// 300000 over different bases still collide.
///
/// Gathering the state behind one owner is the first stage of the fix; the
/// remaining stage keys these instances by catalog and resolves through the
/// caller's catalog instead of the singleton. **Do not flatten this back into
/// free functions over `static`s.** The free functions further down are
/// deliberately thin delegates to the singleton, and exist only so that stage
/// one changed no call sites.
pub struct CatalogTypes {
    /// Guarded separately from [`Self::leaked_column_types`] because
    /// [`Self::column_type_for_name`] materialises a `ColumnType` — and so
    /// reaches the leak cache — while still holding this read guard.
    index: RwLock<TypeIndex>,
    /// Deduplicated `&'static ColumnType`s, so a `Copy` [`DomainRef`] can point
    /// at its base type. Bounded by the number of distinct base and subtype
    /// shapes the catalog ever sees, i.e. by DDL, not by traffic.
    leaked_column_types: RwLock<Vec<&'static ColumnType>>,
}

/// The process-wide [`CatalogTypes`]. Every free function in this module goes
/// through here; see [`CatalogTypes`] for why that is a defect and not a design.
fn catalog_types() -> &'static CatalogTypes {
    static CATALOG_TYPES: OnceLock<CatalogTypes> = OnceLock::new();
    CATALOG_TYPES.get_or_init(CatalogTypes::new)
}

impl CatalogTypes {
    /// A catalog with no user-defined types.
    fn new() -> Self {
        Self {
            index: RwLock::new(TypeIndex::default()),
            leaked_column_types: RwLock::new(Vec::new()),
        }
    }

    /// Publish a type that already has an oid. See [`replace`].
    ///
    /// # Panics
    ///
    /// If the type-index lock is poisoned, which can only happen if another
    /// thread panicked while holding it.
    pub fn replace(&self, ty: &UserType) {
        let qualified_name = ty.qualified_name();
        let _ = intern(&qualified_name);
        if let Some(companion) = ty.multirange_name() {
            let _ = intern(&companion);
        }
        let mut guard = self.index.write().expect("user type registry is healthy");
        guard.remove_name_mappings(ty.oid);
        guard.insert(ty);
    }

    /// Atomically publish the user-type changes between two durable catalog
    /// snapshots. See [`publish_catalog_delta`].
    ///
    /// The whole delta is applied under a single write guard, so no reader can
    /// observe it half-applied.
    ///
    /// # Panics
    ///
    /// If the type-index lock is poisoned, which can only happen if another
    /// thread panicked while holding it.
    pub fn publish_catalog_delta(&self, before: &[UserType], after: &[UserType]) {
        let before_by_oid = before
            .iter()
            .map(|ty| (ty.oid, ty))
            .collect::<HashMap<_, _>>();
        let after_by_oid = after
            .iter()
            .map(|ty| (ty.oid, ty))
            .collect::<HashMap<_, _>>();
        let changed_after = after
            .iter()
            .filter(|ty| before_by_oid.get(&ty.oid).copied() != Some(*ty))
            .collect::<Vec<_>>();

        // Intern before taking the registry lock: constructing a ColumnType from
        // the newly published definition may take the interner lock in the other
        // order.
        for ty in &changed_after {
            let _ = intern(&ty.qualified_name());
            if let Some(companion) = ty.multirange_name() {
                let _ = intern(&companion);
            }
        }

        let mut guard = self.index.write().expect("user type registry is healthy");
        for ty in before {
            if after_by_oid.get(&ty.oid).copied() != Some(ty) {
                guard.remove_name_mappings(ty.oid);
            }
        }
        for ty in changed_after {
            guard.insert(ty);
        }
    }

    /// Forget the type named `name`. See [`unregister`].
    ///
    /// # Panics
    ///
    /// If the type-index lock is poisoned, which can only happen if another
    /// thread panicked while holding it.
    pub fn unregister(&self, name: &str) {
        // Only the NAME is forgotten. The oid keeps resolving, because a stored row
        // encodes its column's type oid: rows written before the drop still have to
        // decode afterwards, and the background vacuum reads exactly those rows when
        // it prunes a dropped table. Removing the oid too made every later read of
        // that data a `corrupt row encoding: column type oid N is not a registered
        // type`, and the vacuum then failed on every pass, forever.
        //
        // Dropping the name is what makes the type unreachable from SQL — a new
        // reference is 42704 and `CREATE TYPE` may reuse the name with a fresh oid.
        let mut guard = self.index.write().expect("user type registry is healthy");
        if let Some(oid) = guard.by_lower_name.remove(&name.to_ascii_lowercase()) {
            guard.by_identity.retain(|_, found| *found != oid);
            guard
                .multirange_by_identity
                .retain(|_, found| *found != oid);
            guard
                .multirange_by_lower_name
                .retain(|_, found| *found != oid);
        }
    }

    /// Forget one exact `(schema, name)` identity. See [`unregister_in`].
    ///
    /// # Panics
    ///
    /// If the type-index lock is poisoned, which can only happen if another
    /// thread panicked while holding it.
    pub fn unregister_in(&self, schema: &str, name: &str) {
        let mut guard = self.index.write().expect("user type registry is healthy");
        let identity = (schema.to_string(), name.to_string());
        if let Some(oid) = guard.by_identity.remove(&identity) {
            guard.by_lower_name.retain(|_, found| *found != oid);
            guard
                .multirange_by_identity
                .retain(|_, found| *found != oid);
            guard
                .multirange_by_lower_name
                .retain(|_, found| *found != oid);
        }
    }

    /// The type registered under `name`, case-insensitively. See [`lookup`].
    ///
    /// # Panics
    ///
    /// If the type-index lock is poisoned, which can only happen if another
    /// thread panicked while holding it.
    #[must_use]
    pub fn lookup(&self, name: &str) -> Option<Arc<UserType>> {
        let guard = self.index.read().expect("user type registry is healthy");
        let lower_name = name.to_ascii_lowercase();
        let oid = *guard
            .by_identity
            .get(&(USER_TYPE_DEFAULT_SCHEMA.to_string(), name.to_string()))
            .or_else(|| guard.by_lower_name.get(&lower_name))?;
        guard.by_oid.get(&oid).cloned()
    }

    /// The type with this exact structured identity. See [`lookup_in`].
    ///
    /// # Panics
    ///
    /// If the type-index lock is poisoned, which can only happen if another
    /// thread panicked while holding it.
    #[must_use]
    pub fn lookup_in(&self, schema: &str, name: &str) -> Option<Arc<UserType>> {
        let guard = self.index.read().expect("user type registry is healthy");
        let oid = *guard
            .by_identity
            .get(&(schema.to_string(), name.to_string()))?;
        guard.by_oid.get(&oid).cloned()
    }

    /// The type with this oid. See [`lookup_oid`].
    ///
    /// The `Arc` is the point: this runs once per user-typed field of every row
    /// the storage layer decodes, and cloning the definition out of the map made
    /// the copy 85–97% of the call.
    ///
    /// # Panics
    ///
    /// If the type-index lock is poisoned, which can only happen if another
    /// thread panicked while holding it.
    #[must_use]
    pub fn lookup_oid(&self, oid: u32) -> Option<Arc<UserType>> {
        self.index
            .read()
            .expect("user type registry is healthy")
            .by_oid
            .get(&oid)
            .cloned()
    }

    /// Every registered type, ordered by oid. See [`all`].
    ///
    /// # Panics
    ///
    /// If the type-index lock is poisoned, which can only happen if another
    /// thread panicked while holding it.
    #[must_use]
    pub fn all(&self) -> Vec<Arc<UserType>> {
        let guard = self.index.read().expect("user type registry is healthy");
        let mut types: Vec<Arc<UserType>> = guard.by_oid.values().cloned().collect();
        types.sort_by_key(|ty| ty.oid);
        types
    }

    /// The `ColumnType` a SQL type name resolves to. See
    /// [`column_type_for_name`].
    ///
    /// # Panics
    ///
    /// If the type-index lock is poisoned, which can only happen if another
    /// thread panicked while holding it.
    #[must_use]
    pub fn column_type_for_name(&self, name: &str) -> Option<ColumnType> {
        if let Some(ty) = self.lookup(name) {
            return ty.column_type();
        }
        let guard = self.index.read().expect("user type registry is healthy");
        let oid = *guard
            .multirange_by_lower_name
            .get(&name.to_ascii_lowercase())?;
        guard.by_oid.get(&oid)?.multirange_type()
    }

    /// Resolve an exact schema and unqualified user-type name. See
    /// [`column_type_for_name_in`].
    ///
    /// # Panics
    ///
    /// If the type-index lock is poisoned, which can only happen if another
    /// thread panicked while holding it.
    #[must_use]
    pub fn column_type_for_name_in(&self, schema: &str, name: &str) -> Option<ColumnType> {
        if let Some(ty) = self.lookup_in(schema, name) {
            return ty.column_type();
        }
        let guard = self.index.read().expect("user type registry is healthy");
        let oid = *guard
            .multirange_by_identity
            .get(&(schema.to_string(), name.to_string()))?;
        guard.by_oid.get(&oid)?.multirange_type()
    }

    /// Resolve either a user type oid or its derived multirange oid. See
    /// [`column_type_for_oid`].
    #[must_use]
    pub fn column_type_for_oid(&self, oid: u32) -> Option<ColumnType> {
        if let Some(ty) = self.lookup_oid(oid) {
            return ty.column_type();
        }
        self.lookup_oid(oid.checked_sub(3)?)?.multirange_type()
    }

    /// Leak `ty` — or return the equal value already leaked — so a `Copy`
    /// [`DomainRef`] or [`RangeRef`] can point at it.
    ///
    /// "Equal" is [`ColumnType`]'s `PartialEq`, which for a user-type base or
    /// subtype is the oid alone. That is why this cache belongs to a catalog:
    /// shared between two of them it returns the wrong type entirely.
    ///
    /// # Panics
    ///
    /// If the leak-cache lock is poisoned, which can only happen if another
    /// thread panicked while holding it.
    fn leak_column_type(&self, ty: ColumnType) -> &'static ColumnType {
        if let Some(found) = self
            .leaked_column_types
            .read()
            .expect("leaked column types are not poisoned")
            .iter()
            .find(|candidate| ***candidate == ty)
        {
            return found;
        }
        let mut guard = self
            .leaked_column_types
            .write()
            .expect("leaked column types are not poisoned");
        if let Some(found) = guard.iter().find(|candidate| ***candidate == ty) {
            return found;
        }
        let leaked: &'static ColumnType = Box::leak(Box::new(ty));
        guard.push(leaked);
        leaked
    }
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
/// Unlike [`CatalogTypes`], this map is legitimately process-wide: it is keyed
/// by the name and its value *is* the same name, so two catalogs that both have
/// a `public.zdom` share one `&'static str` spelling `"public.zdom"` and neither
/// can learn anything about the other from it. Nothing about a type's identity
/// travels through here.
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

/// Intern a [`ColumnType`] so a [`DomainRef`] can point at it, through the
/// process-wide [`CatalogTypes`].
fn leak_column_type(ty: ColumnType) -> &'static ColumnType {
    catalog_types().leak_column_type(ty)
}

// Everything below delegates to the process-wide `CatalogTypes`. The delegates
// are the compatibility layer for callers that have no catalog handle; when a
// caller acquires one it should call the `CatalogTypes` method directly rather
// than growing another free function here.

/// Re-register a type that already has an oid — the catalog-hydration path and
/// the `ALTER TYPE` / `ALTER DOMAIN` path, both of which must preserve the oid.
///
/// # Panics
///
/// If the process-wide user-type registry lock is poisoned, which can only
/// happen if another thread panicked while holding it.
pub fn replace(ty: &UserType) {
    catalog_types().replace(ty);
}

/// Atomically publish the user-type changes between two durable catalog
/// snapshots. Callers take the snapshots on either side of one committed DDL
/// batch and invoke this only after every post-DDL acceptance hook succeeds.
/// Dropped definitions remain addressable by oid for decoding old rows, while
/// their SQL names disappear.
///
/// # Panics
///
/// If the process-wide user-type registry lock is poisoned, which can only
/// happen if another thread panicked while holding it.
pub fn publish_catalog_delta(before: &[UserType], after: &[UserType]) {
    catalog_types().publish_catalog_delta(before, after);
}

/// Forget the type named `name` (`DROP TYPE` / `DROP DOMAIN`).
///
/// # Panics
///
/// If the process-wide user-type registry lock is poisoned, which can only
/// happen if another thread panicked while holding it.
pub fn unregister(name: &str) {
    catalog_types().unregister(name);
}

/// Forget one exact `(schema, name)` identity while retaining the legacy raw-
/// string entrypoint for callers that do not have structured parser metadata.
///
/// # Panics
///
/// If the process-wide user-type registry lock is poisoned, which can only
/// happen if another thread panicked while holding it.
pub fn unregister_in(schema: &str, name: &str) {
    catalog_types().unregister_in(schema, name);
}

/// The type registered under `name`, matched case-insensitively on the already
/// case-folded name the lexer produces.
///
/// # Panics
///
/// If the process-wide user-type registry lock is poisoned, which can only
/// happen if another thread panicked while holding it.
#[must_use]
pub fn lookup(name: &str) -> Option<Arc<UserType>> {
    catalog_types().lookup(name)
}

/// The type with this exact structured identity.
///
/// # Panics
///
/// If the process-wide user-type registry lock is poisoned, which can only
/// happen if another thread panicked while holding it.
#[must_use]
pub fn lookup_in(schema: &str, name: &str) -> Option<Arc<UserType>> {
    catalog_types().lookup_in(schema, name)
}

/// The type with this oid, shared rather than copied — see
/// [`CatalogTypes::lookup_oid`].
///
/// `None` means *no such type in this catalog*, and callers treat it as an
/// error, not as a fallback: the row decoder turns it into `corrupt row
/// encoding`. Nothing here can answer with a different catalog's type.
///
/// # Panics
///
/// If the process-wide user-type registry lock is poisoned, which can only
/// happen if another thread panicked while holding it.
#[must_use]
pub fn lookup_oid(oid: u32) -> Option<Arc<UserType>> {
    catalog_types().lookup_oid(oid)
}

/// Every registered type, ordered by oid so catalog scans are deterministic.
///
/// # Panics
///
/// If the process-wide user-type registry lock is poisoned, which can only
/// happen if another thread panicked while holding it.
#[must_use]
pub fn all() -> Vec<Arc<UserType>> {
    catalog_types().all()
}

/// The `ColumnType` a SQL type name resolves to when it is not built in.
/// [`ColumnType::from_sql_name`] falls through to this.
///
/// # Panics
///
/// If the process-wide user-type registry lock is poisoned, which can only
/// happen if another thread panicked while holding it.
#[must_use]
pub fn column_type_for_name(name: &str) -> Option<ColumnType> {
    catalog_types().column_type_for_name(name)
}

/// Resolve an exact schema and unqualified user-type name.
///
/// # Panics
///
/// If the process-wide user-type registry lock is poisoned, which can only
/// happen if another thread panicked while holding it.
#[must_use]
pub fn column_type_for_name_in(schema: &str, name: &str) -> Option<ColumnType> {
    catalog_types().column_type_for_name_in(schema, name)
}

/// Resolve either a user type oid or its derived multirange oid.
#[must_use]
pub fn column_type_for_oid(oid: u32) -> Option<ColumnType> {
    catalog_types().column_type_for_oid(oid)
}

/// Derives the default multirange companion name for a range type.
#[must_use]
pub fn default_multirange_name(range_name: &str) -> String {
    let mut name = range_name.find("range").map_or_else(
        || format!("{range_name}_multirange"),
        |start| {
            let end = start + "range".len();
            format!("{}multirange{}", &range_name[..start], &range_name[end..])
        },
    );
    if name.len() > 63 {
        let mut end = 63;
        while !name.is_char_boundary(end) {
            end -= 1;
        }
        name.truncate(end);
    }
    name
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

/// The array oid of a user-defined range's multirange companion. Range types
/// do not have a composite relation, so their reserved `+1` oid is available.
#[must_use]
pub fn user_multirange_array_oid(multirange_oid: u32) -> u32 {
    multirange_oid - 2
}

#[cfg(test)]
mod tests {
    use assert2::assert;

    use super::*;

    /// The stride the catalog allocates user-type oids at
    /// (`crabka_pgcatalog`'s `USER_TYPE_OID_STRIDE`). Restated here because
    /// this module derives oids *inside* that stride and nothing in it
    /// allocates; see [`derived_oids_stay_inside_the_catalog_oid_stride`].
    const CATALOG_OID_STRIDE: u32 = 4;

    /// Publish a type in the default schema under an oid the test names.
    ///
    /// Every oid a real type carries was allocated by the catalog that
    /// persists it, so tests choose theirs explicitly rather than asking the
    /// registry to invent one.
    fn publish(oid: u32, name: &str, body: UserTypeBody) -> UserType {
        let ty = UserType {
            oid,
            array_oid: user_array_oid(oid),
            schema: USER_TYPE_DEFAULT_SCHEMA.to_string(),
            name: name.to_string(),
            body,
        };
        replace(&ty);
        ty
    }

    /// [`publish`], into one named catalog instead of the process-wide one.
    fn publish_in(catalog: &CatalogTypes, oid: u32, name: &str, body: UserTypeBody) -> UserType {
        let ty = UserType {
            oid,
            array_oid: user_array_oid(oid),
            schema: USER_TYPE_DEFAULT_SCHEMA.to_string(),
            name: name.to_string(),
            body,
        };
        catalog.replace(&ty);
        ty
    }

    #[test]
    fn default_multirange_names_replace_first_range_and_fit_name_limit() {
        assert!(default_multirange_name("price") == "price_multirange");
        assert!(default_multirange_name("range_range") == "multirange_range");
        let clipped = default_multirange_name(&"x".repeat(70));
        assert!(clipped.len() == 63);
        assert!(clipped.is_char_boundary(clipped.len()));
    }

    #[test]
    fn range_registers_derived_multirange_name_and_oid() {
        use crate::{ColumnType, usertype::RangeBody};

        publish(
            300_100,
            "companion_textrange",
            UserTypeBody::Range(RangeBody {
                subtype: ColumnType::Text,
                collation: Some("C".into()),
                multirange_schema: None,
                multirange_name: None,
            }),
        );
        let Some(ColumnType::Multirange(multirange)) =
            column_type_for_name("companion_textmultirange")
        else {
            panic!("derived multirange is registered");
        };
        // The companion sits at the range's oid + 3, inside the catalog stride.
        assert!(multirange.oid == 300_103);
        assert!(multirange.range.oid == 300_100);
        assert!(column_type_for_oid(300_103) == Some(ColumnType::Multirange(multirange)));
    }

    /// `DROP TYPE` must make the name unresolvable without making stored rows
    /// undecodable: a row encodes its column's type oid, so the oid has to keep
    /// resolving after the drop. Losing it made every later read of that data a
    /// corrupt-row error and wedged the background vacuum on every pass.
    #[test]
    fn dropping_a_type_frees_the_name_but_keeps_the_oid_decodable() {
        publish(
            300_200,
            "drop_tombstone_t",
            UserTypeBody::Enum(vec!["a".to_string()]),
        );

        unregister("drop_tombstone_t");

        assert!(lookup("drop_tombstone_t").is_none(), "name must be free");
        assert!(
            lookup_oid(300_200).is_some(),
            "oid 300200 must still decode stored rows"
        );

        // The freed name may be reused by a type the catalog gave a fresh oid.
        publish(
            300_204,
            "drop_tombstone_t",
            UserTypeBody::Enum(vec!["b".to_string()]),
        );
        assert!(lookup("drop_tombstone_t").map(|ty| ty.oid) == Some(300_204));
        // The tombstoned oid still resolves to its *own* definition, not the
        // one that took its name over.
        assert!(
            lookup_oid(300_200).and_then(|ty| ty.labels().map(<[String]>::to_vec))
                == Some(vec!["a".to_string()])
        );
    }

    #[test]
    fn interning_returns_one_pointer_per_name() {
        let a = intern("ut_interned_name");
        let b = intern("ut_interned_name");
        assert!(std::ptr::eq(a, b));
        assert!(a == "ut_interned_name");
    }

    #[test]
    fn a_registered_composite_resolves_by_name_and_reports_its_fields() {
        let registered = publish(
            300_300,
            "ut_reg_composite",
            UserTypeBody::Composite(vec![CompositeField {
                name: "x".into(),
                ty: ColumnType::Int4,
            }]),
        );
        let found = lookup("UT_REG_COMPOSITE").expect("case-insensitive lookup");
        assert!(*found == registered);
        assert!(found.oid == 300_300);
        assert!(found.typtype() == "c");
        assert!(found.fields().expect("composite").len() == 1);
        assert!(found.labels().is_none());
        assert!(found.domain().is_none());
        assert!(column_type_for_name("ut_reg_composite") == found.column_type());
        assert!(matches!(
            found.column_type(),
            Some(ColumnType::Record(Some(_)))
        ));
        unregister("ut_reg_composite");
        assert!(lookup("ut_reg_composite").is_none());
    }

    #[test]
    fn a_registered_domain_carries_its_base_type() {
        let registered = publish(
            300_400,
            "ut_reg_domain",
            UserTypeBody::Domain(DomainBody {
                base: ColumnType::Numeric(None),
                not_null: true,
                not_null_name: None,
                default: Some("0".into()),
                checks: vec![DomainCheck {
                    name: "ut_reg_domain_check".into(),
                    expr: "VALUE > 0".into(),
                    validated: true,
                }],
            }),
        );
        let Some(ColumnType::Domain(domain)) = registered.column_type() else {
            panic!("a domain resolves to ColumnType::Domain");
        };
        assert!(*domain.base == ColumnType::Numeric(None));
        assert!(domain.name == "ut_reg_domain");
        assert!(domain.as_ref().oid == 300_400);
        assert!(registered.typtype() == "d");
        assert!(registered.domain().expect("domain").not_null);
        unregister("ut_reg_domain");
    }

    #[test]
    fn replace_preserves_the_oid_so_alter_type_does_not_orphan_columns() {
        let created = publish(
            300_500,
            "ut_replace_enum",
            UserTypeBody::Enum(vec!["a".into(), "b".into()]),
        );
        let mut altered = created;
        altered.body = UserTypeBody::Enum(vec!["a".into(), "b".into(), "c".into()]);
        replace(&altered);
        let found = lookup("ut_replace_enum").expect("still registered");
        assert!(found.oid == 300_500);
        assert!(found.labels().expect("enum") == ["a", "b", "c"]);
        assert!(lookup_oid(300_500) == Some(found));
        unregister("ut_replace_enum");
    }

    /// A resolved definition is a snapshot, not a window onto the registry.
    ///
    /// [`lookup_oid`] hands out a shared `Arc` rather than a private copy, so
    /// this is worth stating: `replace` publishes a *new* `Arc` under the oid
    /// and never mutates the one a caller already holds. A row decoder that
    /// resolved a type just before an `ALTER TYPE` committed therefore keeps
    /// decoding against a consistent definition instead of seeing labels
    /// appear underneath it.
    #[test]
    fn a_resolved_definition_is_a_snapshot_not_a_live_view() {
        let created = publish(
            300_900,
            "ut_snapshot_enum",
            UserTypeBody::Enum(vec!["a".into()]),
        );
        let held = lookup_oid(300_900).expect("registered");

        let mut altered = created;
        altered.body = UserTypeBody::Enum(vec!["a".into(), "b".into()]);
        replace(&altered);

        assert!(held.labels().expect("enum") == ["a"]);
        assert!(
            lookup_oid(300_900)
                .expect("still registered")
                .labels()
                .expect("enum")
                == ["a", "b"]
        );
        unregister("ut_snapshot_enum");
    }

    /// The catalog allocates user-type oids [`CATALOG_OID_STRIDE`] apart
    /// precisely so that the oids this module *derives* from a type oid — the
    /// `pg_class` row type at `+1`, the array type at `+2`, the multirange
    /// companion at `+3` — never land on the next type. Nothing here
    /// allocates; the arithmetic is the whole contract.
    #[test]
    fn derived_oids_stay_inside_the_catalog_oid_stride() {
        let a = publish(300_600, "ut_oid_a", UserTypeBody::Composite(Vec::new()));
        let b = publish(300_604, "ut_oid_b", UserTypeBody::Composite(Vec::new()));
        assert!(b.oid == a.oid + CATALOG_OID_STRIDE);
        assert!(composite_relation_oid(a.oid) != b.oid);
        assert!(user_array_oid(a.oid) != b.oid);
        assert!(user_array_oid(a.oid) != composite_relation_oid(b.oid));
        assert!(all().iter().any(|ty| ty.oid == a.oid));
        unregister("ut_oid_a");
        unregister("ut_oid_b");
    }

    /// Every relation's rows reach this registry whether or not the catalog
    /// has any user-defined type: the row decoder calls [`lookup_oid`] once
    /// per user-typed field, and `ElemType::from_array_oid` scans [`all`] on
    /// every array decode, built-in element types included. A catalog with no
    /// user types must answer both — with `None` and with nothing — rather
    /// than inventing a type or falling back to somewhere else.
    #[test]
    fn an_empty_catalog_still_answers_the_row_decode_path() {
        let empty = CatalogTypes::new();
        assert!(empty.all().is_empty());
        assert!(empty.lookup_oid(300_000).is_none());
        assert!(empty.lookup("int4").is_none());
        assert!(empty.lookup_in(USER_TYPE_DEFAULT_SCHEMA, "int4").is_none());
        assert!(empty.column_type_for_name("int4").is_none());
        // Not a user type oid and not a multirange derived from one.
        assert!(empty.column_type_for_oid(23).is_none());

        // And through the real array path. `from_array_oid` scans the whole
        // registry before falling back to the built-in element types, so a
        // built-in array must resolve identically whether the catalog holds no
        // user types or some — a user type must never shadow it.
        let int4_array = crate::datum::oids::INT4ARRAY;
        let with_none = crate::datum::ElemType::from_array_oid(int4_array)
            .expect("int4[] resolves with no user type registered");
        publish(
            301_000,
            "ut_empty_path_probe",
            UserTypeBody::Enum(vec!["a".into()]),
        );
        let with_some = crate::datum::ElemType::from_array_oid(int4_array)
            .expect("int4[] still resolves once a user type exists");
        assert!(with_none == with_some);
        assert!(with_none.array_oid() == int4_array);
        unregister("ut_empty_path_probe");
    }

    /// The premise behind [`CatalogTypes`] owning its leak cache: every
    /// user-type reference compares on the oid alone, so a cache keyed on
    /// `ColumnType` equality treats two catalogs' distinct types at the same
    /// oid as one value — with the wrong name *and* the wrong base or subtype.
    /// Adding the name to the key would not help, as the range/multirange rows
    /// here show.
    #[test]
    fn user_type_refs_compare_on_the_oid_alone() {
        let subrange = |oid, name, subtype| RangeRef { oid, name, subtype };
        let cases: [(&str, ColumnType, ColumnType); 5] = [
            (
                "record",
                ColumnType::Record(Some(UserTypeRef {
                    oid: 300_000,
                    array_oid: user_array_oid(300_000),
                    name: "public.zcomp",
                })),
                ColumnType::Record(Some(UserTypeRef {
                    oid: 300_000,
                    array_oid: user_array_oid(300_000),
                    name: "public.zother",
                })),
            ),
            (
                "enum",
                ColumnType::Enum(UserTypeRef {
                    oid: 300_000,
                    array_oid: user_array_oid(300_000),
                    name: "public.zenum",
                }),
                ColumnType::Enum(UserTypeRef {
                    oid: 300_000,
                    array_oid: user_array_oid(300_000),
                    name: "public.zother",
                }),
            ),
            (
                "domain over a different base",
                ColumnType::Domain(DomainRef {
                    oid: 300_000,
                    name: "public.zdom",
                    base: &ColumnType::Int4,
                }),
                ColumnType::Domain(DomainRef {
                    oid: 300_000,
                    name: "public.zdom",
                    base: &ColumnType::Text,
                }),
            ),
            (
                "range over a different subtype",
                ColumnType::Range(subrange(300_000, "public.zrange", &ColumnType::Int4)),
                ColumnType::Range(subrange(300_000, "public.zrange", &ColumnType::Text)),
            ),
            (
                "multirange over a different range",
                ColumnType::Multirange(MultirangeRef {
                    oid: 300_003,
                    name: "public.zmultirange",
                    range: subrange(300_000, "public.zrange", &ColumnType::Int4),
                }),
                ColumnType::Multirange(MultirangeRef {
                    oid: 300_003,
                    name: "public.zmultirange",
                    range: subrange(300_004, "public.zother", &ColumnType::Text),
                }),
            ),
        ];
        for (label, left, right) in cases {
            assert!(
                left == right,
                "{label}: equality must ignore everything but the oid"
            );
        }
    }

    /// Two catalogs allocate from independent durable counters that both start
    /// at 300000, so both hand that oid to a *different* type — and no
    /// allocation scheme fixes that, because the oids are already written into
    /// rows and onto the wire before the two catalogs ever meet. Because those
    /// types then compare equal (see above), a shared leak cache would return
    /// one catalog's `&'static ColumnType` to the other, permanently. Owning
    /// the cache per [`CatalogTypes`] is what keeps them apart.
    #[test]
    fn each_catalog_leaks_its_own_column_types() {
        let first = CatalogTypes::new();
        let second = CatalogTypes::new();

        let first_enum = publish_in(
            &first,
            300_000,
            "zleak_first",
            UserTypeBody::Enum(vec!["a".into()]),
        );
        let second_enum = publish_in(
            &second,
            300_000,
            "zleak_second",
            UserTypeBody::Enum(vec!["b".into()]),
        );
        assert!(first_enum.oid == second_enum.oid);

        let enum_type = |ty: &UserType| ty.column_type().expect("an enum has a column type");
        let first_leaked = first.leak_column_type(enum_type(&first_enum));
        let second_leaked = second.leak_column_type(enum_type(&second_enum));

        // Equal by the oid-only comparison, which is exactly the trap.
        assert!(first_leaked == second_leaked);
        assert!(!std::ptr::eq(first_leaked, second_leaked));

        let (ColumnType::Enum(first_ref), ColumnType::Enum(second_ref)) =
            (*first_leaked, *second_leaked)
        else {
            panic!("an enum type leaks as ColumnType::Enum");
        };
        assert!(first_ref.name == "zleak_first");
        assert!(second_ref.name == "zleak_second");

        // Within one catalog the cache still dedups to a single pointer.
        assert!(std::ptr::eq(
            first_leaked,
            first.leak_column_type(enum_type(&first_enum))
        ));
    }

    /// A catalog's own state is reachable only through its own methods: names
    /// registered in one instance do not resolve in another, and neither
    /// reaches the process-wide singleton the free functions use.
    #[test]
    fn catalog_instances_do_not_share_their_indexes() {
        let first = CatalogTypes::new();
        let second = CatalogTypes::new();

        let registered = publish_in(
            &first,
            300_800,
            "zindex_isolated",
            UserTypeBody::Composite(Vec::new()),
        );

        assert!(first.lookup("zindex_isolated").as_deref() == Some(&registered));
        assert!(
            first
                .lookup_in(USER_TYPE_DEFAULT_SCHEMA, "zindex_isolated")
                .is_some()
        );
        assert!(second.lookup("zindex_isolated").is_none());
        assert!(second.lookup_oid(300_800).is_none());
        assert!(lookup("zindex_isolated").is_none());
        assert!(second.all().is_empty());
    }
}
