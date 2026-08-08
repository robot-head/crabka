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
            name: intern(&self.qualified_name()),
        }
    }

    /// This type as a [`ColumnType`].
    #[must_use]
    pub fn column_type(&self) -> ColumnType {
        let qualified_name = self.qualified_name();
        match &self.body {
            UserTypeBody::Composite(_) => ColumnType::Record(Some(self.type_ref())),
            UserTypeBody::Enum(_) => ColumnType::Enum(self.type_ref()),
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

/// The default schema used by the process-wide parser registry.
pub const USER_TYPE_DEFAULT_SCHEMA: &str = "public";

/// Oids are handed out in this stride so that a composite's type oid, its array
/// type oid and its backing `pg_class` relation oid never collide.
const OID_STRIDE: u32 = 4;

/// The name and oid indexes of one [`CatalogTypes`], plus its oid counter.
///
/// Every map here is keyed by a SQL name or an oid, both of which are only
/// meaningful within a single catalog — which is why this lives inside
/// [`CatalogTypes`] rather than in a `static` of its own.
#[derive(Default)]
struct TypeIndex {
    by_identity: HashMap<(String, String), u32>,
    by_lower_name: HashMap<String, u32>,
    multirange_by_identity: HashMap<(String, String), u32>,
    multirange_by_lower_name: HashMap<String, u32>,
    by_oid: HashMap<u32, UserType>,
    next_oid: u32,
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

    /// Index `ty` under every name it answers to, and keep the oid counter
    /// ahead of it.
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
        self.by_oid.insert(ty.oid, ty.clone());
        self.next_oid = self.next_oid.max(ty.oid + OID_STRIDE);
    }
}

/// The user-defined types of **one catalog**: the name and oid indexes, the oid
/// counter, and the cache of leaked `&'static ColumnType`s that a [`DomainRef`]
/// base or a [`RangeRef`] subtype points at.
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
    /// A catalog with no user-defined types, ready to hand out oids from
    /// [`FIRST_USER_TYPE_OID`].
    fn new() -> Self {
        Self {
            index: RwLock::new(TypeIndex {
                next_oid: FIRST_USER_TYPE_OID,
                ..TypeIndex::default()
            }),
            leaked_column_types: RwLock::new(Vec::new()),
        }
    }

    /// Register `body` under `name`, allocating a fresh oid, and return the
    /// registered type. See [`register`].
    ///
    /// # Panics
    ///
    /// If the type-index lock is poisoned, which can only happen if another
    /// thread panicked while holding it.
    #[must_use]
    pub fn register(&self, name: &str, body: UserTypeBody) -> UserType {
        let mut guard = self.index.write().expect("user type registry is healthy");
        let oid = guard.next_oid;
        guard.next_oid += OID_STRIDE;
        let ty = UserType {
            oid,
            schema: USER_TYPE_DEFAULT_SCHEMA.to_string(),
            name: name.to_string(),
            body,
        };
        let qualified_name = ty.qualified_name();
        guard
            .by_identity
            .insert((ty.schema.clone(), ty.name.clone()), oid);
        guard
            .by_lower_name
            .insert(qualified_name.to_ascii_lowercase(), oid);
        if let Some(companion) = ty.multirange_name() {
            guard
                .multirange_by_lower_name
                .insert(companion.to_ascii_lowercase(), oid);
        }
        if let Some(identity) = ty.multirange_identity() {
            guard.multirange_by_identity.insert(identity, oid);
        }
        guard.by_oid.insert(oid, ty.clone());
        // Intern eagerly so `column_type()` never has to take the interner lock
        // while the registry lock is held.
        drop(guard);
        let _ = intern(&qualified_name);
        ty
    }

    /// Re-register a type that already has an oid. See [`replace`].
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
    pub fn lookup(&self, name: &str) -> Option<UserType> {
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
    pub fn lookup_in(&self, schema: &str, name: &str) -> Option<UserType> {
        let guard = self.index.read().expect("user type registry is healthy");
        let oid = *guard
            .by_identity
            .get(&(schema.to_string(), name.to_string()))?;
        guard.by_oid.get(&oid).cloned()
    }

    /// The type with this oid. See [`lookup_oid`].
    ///
    /// # Panics
    ///
    /// If the type-index lock is poisoned, which can only happen if another
    /// thread panicked while holding it.
    #[must_use]
    pub fn lookup_oid(&self, oid: u32) -> Option<UserType> {
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
    pub fn all(&self) -> Vec<UserType> {
        let guard = self.index.read().expect("user type registry is healthy");
        let mut types: Vec<UserType> = guard.by_oid.values().cloned().collect();
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
            return Some(ty.column_type());
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
            return Some(ty.column_type());
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
            return Some(ty.column_type());
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

/// Register `body` under `name`, allocating a fresh oid, and return the
/// registered type. An existing registration under the same name is replaced —
/// callers enforce `PostgreSQL`'s duplicate-name rule (42710) before getting
/// here, and DDL that legitimately replaces a definition (`ALTER TYPE`) goes
/// through [`replace`].
///
/// # Panics
///
/// If the process-wide user-type registry lock is poisoned, which can only
/// happen if another thread panicked while holding it.
#[must_use]
pub fn register(name: &str, body: UserTypeBody) -> UserType {
    catalog_types().register(name, body)
}

/// Re-register a type that already has an oid: the catalog-hydration path and
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
pub fn lookup(name: &str) -> Option<UserType> {
    catalog_types().lookup(name)
}

/// The type with this exact structured identity.
///
/// # Panics
///
/// If the process-wide user-type registry lock is poisoned, which can only
/// happen if another thread panicked while holding it.
#[must_use]
pub fn lookup_in(schema: &str, name: &str) -> Option<UserType> {
    catalog_types().lookup_in(schema, name)
}

/// The type with this oid.
///
/// # Panics
///
/// If the process-wide user-type registry lock is poisoned, which can only
/// happen if another thread panicked while holding it.
#[must_use]
pub fn lookup_oid(oid: u32) -> Option<UserType> {
    catalog_types().lookup_oid(oid)
}

/// Every registered type, ordered by oid so catalog scans are deterministic.
///
/// # Panics
///
/// If the process-wide user-type registry lock is poisoned, which can only
/// happen if another thread panicked while holding it.
#[must_use]
pub fn all() -> Vec<UserType> {
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

    #[test]
    fn default_multirange_names_replace_first_range_and_fit_name_limit() {
        assert_eq!(default_multirange_name("price"), "price_multirange");
        assert_eq!(default_multirange_name("range_range"), "multirange_range");
        let clipped = default_multirange_name(&"x".repeat(70));
        assert_eq!(clipped.len(), 63);
        assert!(clipped.is_char_boundary(clipped.len()));
    }

    #[test]
    fn range_registers_derived_multirange_name_and_oid() {
        use crate::{ColumnType, usertype::RangeBody};

        let range = register(
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
                    name: "public.zcomp",
                })),
                ColumnType::Record(Some(UserTypeRef {
                    oid: 300_000,
                    name: "public.zother",
                })),
            ),
            (
                "enum",
                ColumnType::Enum(UserTypeRef {
                    oid: 300_000,
                    name: "public.zenum",
                }),
                ColumnType::Enum(UserTypeRef {
                    oid: 300_000,
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

    /// Two catalogs allocate from independent counters, so both hand out
    /// oid [`FIRST_USER_TYPE_OID`] to *different* types. Because those types
    /// then compare equal (see above), a shared leak cache would return one
    /// catalog's `&'static ColumnType` to the other, permanently. Owning the
    /// cache per [`CatalogTypes`] is what keeps them apart.
    #[test]
    fn each_catalog_leaks_its_own_column_types() {
        let first = CatalogTypes::new();
        let second = CatalogTypes::new();

        let first_enum = first.register("zleak_first", UserTypeBody::Enum(vec!["a".into()]));
        let second_enum = second.register("zleak_second", UserTypeBody::Enum(vec!["b".into()]));
        assert!(first_enum.oid == second_enum.oid);

        let first_leaked = first.leak_column_type(first_enum.column_type());
        let second_leaked = second.leak_column_type(second_enum.column_type());

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
            first.leak_column_type(first_enum.column_type())
        ));
    }

    /// A catalog's own state is reachable only through its own methods: names
    /// registered in one instance do not resolve in another, and neither
    /// reaches the process-wide singleton the free functions use.
    #[test]
    fn catalog_instances_do_not_share_their_indexes() {
        let first = CatalogTypes::new();
        let second = CatalogTypes::new();

        let registered = first.register("zindex_isolated", UserTypeBody::Composite(Vec::new()));

        assert!(first.lookup("zindex_isolated").as_ref() == Some(&registered));
        assert!(
            first
                .lookup_in(USER_TYPE_DEFAULT_SCHEMA, "zindex_isolated")
                .is_some()
        );
        assert!(second.lookup("zindex_isolated").is_none());
        assert!(second.lookup_oid(registered.oid).is_none());
        assert!(lookup("zindex_isolated").is_none());
        assert!(second.all().is_empty());
    }
}
