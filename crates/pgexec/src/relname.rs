//! The one place a parsed [`RelationRef`] becomes a [`RelationName`].
//!
//! The AST carries a relation name *as written*, which is an optional schema
//! and a name. The catalog stores it *as resolved*, as a schema and a name that
//! together key the record. [`resolve_relation`] is the only crossing of that
//! boundary, and the catalog offers no lookup that takes a bare string. An
//! operation therefore cannot accidentally skip the search path, because it
//! would not compile.
//!
//! [`parse_written_relation`] is the step *before* that. It handles the names
//! that arrive as a runtime string and not through the grammar: a `regclass`
//! input and the `nextval`/`setval` argument, which is the same thing.

use std::sync::LazyLock;

use crabka_pgcatalog::{CatalogError, RelationName};
use crabka_pgkv::Kv;
use crabka_pgparser::ast::RelationRef;

use crate::{error::ExecError, search_path::SearchPath};

/// What a statement is about to do with a relation name, which decides both
/// where an unqualified name lands and how a qualifier naming a missing schema
/// is reported.
///
/// `PostgreSQL` draws the reporting distinction inside
/// `RangeVarGetRelidExtended`. A utility statement looks the schema up
/// strictly, so a missing one is `3F000`. Parse analysis of a `SELECT`/DML
/// target looks it up permissively, and then reports the whole dotted name as
/// a missing relation. Verified against `postgres:18.4`:
///
/// ```text
/// SELECT * FROM nope.t;         42P01  relation "nope.t" does not exist
/// INSERT INTO nope.t VALUES(1); 42P01  relation "nope.t" does not exist
/// DROP TABLE nope.t;            3F000  schema "nope" does not exist
/// CREATE TABLE nope.t (x int);  3F000  schema "nope" does not exist
/// TRUNCATE nope.t;              3F000  schema "nope" does not exist
/// ```
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SchemaDisposition {
    /// A `SELECT`, `INSERT`, `UPDATE`, `DELETE` or `MERGE` target: a missing
    /// schema is never reported as such, only as a missing relation.
    Reference,
    /// A utility statement that names an existing relation: every
    /// `ALTER`/`DROP`, plus `TRUNCATE`, `COPY`, `GRANT`, `REVOKE` and
    /// `COMMENT`. The schema is resolved first, so a missing one is reported
    /// before the relation is ever looked for.
    Utility,
    /// A statement that creates a relation. The schema is resolved strictly,
    /// as for [`SchemaDisposition::Utility`]. But an unqualified name lands in
    /// the first *existing* explicit search-path entry, and not where it
    /// already exists. `SET search_path = nosuch, s1, s2` creates in `s1`.
    /// When the path names no existing entry, the result is
    /// `3F000 no schema has been selected to create in`.
    Creation,
    /// A `CREATE TEMPORARY`. The search path does not decide where this lands.
    /// An unqualified name goes to the session's own temporary namespace, and a
    /// qualifier is only accepted when it names that same namespace:
    ///
    /// ```text
    /// CREATE TEMP TABLE s.t (x int);  42P16  cannot create temporary relation
    ///                                        in non-temporary schema
    /// ```
    TemporaryCreation,
}

/// Everything a written relation name needs in order to name a schema.
///
/// It is one value and not four parameters, because it has exactly the
/// lifetime of a session and because every resolution needs all of it. The
/// path says which schemas to look in. The user is what `"$user"` expands to.
/// The backend id names the session's own temporary namespace. The database is
/// what a three-part name's leading part is measured against.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ResolutionScope {
    /// The session's `search_path`, unexpanded.
    pub search_path: SearchPath,
    /// `current_user`, which the `"$user"` entry expands to.
    pub user: String,
    /// The backend id the wire layer announced for this session, which names
    /// its temporary schema. 0 outside a session.
    pub backend_id: i32,
    /// The database the session connected to, which a three-part name's
    /// catalog part has to equal to be a local reference.
    ///
    /// This is session state and not a constant: a constant made
    /// `postgres.public.t` resolve locally from every database and made the
    /// connected database's own name read as another database's.
    /// [`crate::exec::DEFAULT_DATABASE`] outside a session.
    pub database: String,
}

impl Default for ResolutionScope {
    fn default() -> Self {
        Self {
            search_path: SearchPath::default(),
            user: crate::catalog_fn::OBJECT_OWNER.to_string(),
            backend_id: 0,
            database: crate::exec::DEFAULT_DATABASE.to_string(),
        }
    }
}

static DEFAULT_SCOPE: LazyLock<ResolutionScope> = LazyLock::new(ResolutionScope::default);

impl ResolutionScope {
    /// The scope a context with no session resolves against: `PostgreSQL`'s
    /// own default `search_path`. Planning contexts and unit tests genuinely
    /// have no session, and a relation named in one still must resolve.
    #[must_use]
    pub fn default_scope() -> &'static Self {
        &DEFAULT_SCOPE
    }

    /// The scope a relation's *stored body* resolves its unqualified names in:
    /// this one, with the relation's own schema searched first.
    ///
    /// A view keeps its body as SQL text, so every read, every write rewritten
    /// through it, and every catalog predicate asked about it re-resolves the
    /// names that body writes. Doing that in the reader's scope makes a view
    /// mean different relations to different sessions — and, for a view outside
    /// the reader's `search_path`, makes it mean nothing at all. Every one of
    /// those callers therefore resolves in this scope instead, so they agree
    /// with each other and the answer belongs to the view.
    #[must_use]
    pub fn for_stored_body(&self, schema: &str) -> Self {
        Self {
            search_path: self.search_path.searching_first(schema),
            user: self.user.clone(),
            backend_id: self.backend_id,
            database: self.database.clone(),
        }
    }

    /// This session's temporary namespace, whether or not it exists yet.
    #[must_use]
    pub fn temp_schema(&self) -> String {
        crabka_pgcatalog::temp_schema_name(self.backend_id)
    }

    /// The schemas an unqualified name is looked for in, in order, filtered to
    /// the ones that exist.
    ///
    /// Two entries are implicit, and you suppress each one when you write it.
    /// The session's temporary namespace comes first, then `pg_catalog`. A path
    /// that names either one puts it where it was written instead. Verified
    /// against `postgres:18.4`:
    ///
    /// ```text
    /// SET search_path = "$user", public;   {pg_temp_1,pg_catalog,public}
    /// SET search_path = public, pg_catalog; {pg_temp_1,public,pg_catalog}
    /// SET search_path = public, pg_temp;    {pg_catalog,public,pg_temp_1}
    /// ```
    ///
    /// # Errors
    ///
    /// Returns storage/corruption errors from the catalog KV seam.
    pub fn visible_schemas(&self, kv: &dyn Kv) -> Result<Vec<String>, ExecError> {
        let temp = self.temp_schema();
        let written = self.search_path.expanded(&self.user, &temp);
        let mut schemas = Vec::with_capacity(written.len() + 2);
        // A session that has created no temporary relation has no temporary
        // namespace, and nothing shadows.
        if !self.search_path.names_temp_schema(&temp) && crabka_pgcatalog::schema_exists(kv, &temp)?
        {
            schemas.push(temp);
        }
        if !written
            .iter()
            .any(|name| name == crate::search_path::PG_CATALOG)
        {
            schemas.push(crate::search_path::PG_CATALOG.to_string());
        }
        for name in written {
            if crabka_pgcatalog::schema_exists(kv, &name)? {
                schemas.push(name);
            }
        }
        Ok(schemas)
    }

    /// The schemas `current_schemas(false)` reports: the explicit entries that
    /// exist, without either implicit entry.
    ///
    /// # Errors
    ///
    /// Returns storage/corruption errors from the catalog KV seam.
    pub fn explicit_schemas(&self, kv: &dyn Kv) -> Result<Vec<String>, ExecError> {
        let mut schemas = Vec::new();
        for name in self.search_path.expanded(&self.user, &self.temp_schema()) {
            if crabka_pgcatalog::schema_exists(kv, &name)? {
                schemas.push(name);
            }
        }
        Ok(schemas)
    }

    /// The schema a `CREATE` with no qualifier lands in: the first explicit
    /// entry that exists. `None` when the path names none, which is
    /// `PostgreSQL`'s `no schema has been selected to create in`.
    ///
    /// # Errors
    ///
    /// Returns storage/corruption errors from the catalog KV seam.
    pub fn creation_schema(&self, kv: &dyn Kv) -> Result<Option<String>, ExecError> {
        Ok(self.explicit_schemas(kv)?.into_iter().next())
    }
}

/// The catalog name `reference` denotes, under `scope`.
///
/// This is the whole namespace model, and it is deliberately the only copy of
/// it.
///
/// # Errors
///
/// [`ExecError::Catalog`] with [`CatalogError::UndefinedSchema`] (`3F000`)
/// when a written qualifier names a schema that does not exist and
/// `disposition` is not [`SchemaDisposition::Reference`], or when a
/// [`SchemaDisposition::Creation`] finds no schema to create in. A
/// [`SchemaDisposition::Reference`] never fails here. Its missing schema
/// appears as the `42P01` the relation lookup raises against the dotted name.
///
/// [`ExecError::InvalidTableDefinition`] (`42P16`) for the two ways a creation
/// can name the wrong kind of namespace: `cannot create temporary relation in
/// non-temporary schema`, and `cannot create relations in temporary schemas of
/// other sessions`.
pub fn resolve_relation(
    kv: &dyn Kv,
    scope: &ResolutionScope,
    reference: &RelationRef,
    disposition: SchemaDisposition,
) -> Result<RelationName, ExecError> {
    let temp = scope.temp_schema();
    let creating = matches!(
        disposition,
        SchemaDisposition::Creation | SchemaDisposition::TemporaryCreation
    );
    if let Some(written) = &reference.schema {
        // `pg_temp` is an alias for whichever namespace is this session's, not
        // a schema in its own right.
        let schema = if written == crabka_pgcatalog::PG_TEMP_ALIAS {
            temp.clone()
        } else {
            written.clone()
        };
        if creating && crabka_pgcatalog::is_temp_schema(&schema) && schema != temp {
            return Err(ExecError::InvalidTableDefinition(
                "cannot create relations in temporary schemas of other sessions".into(),
            ));
        }
        if disposition == SchemaDisposition::TemporaryCreation && schema != temp {
            return Err(ExecError::InvalidTableDefinition(
                "cannot create temporary relation in non-temporary schema".into(),
            ));
        }
        // The session's own temporary namespace is brought into being by the
        // first statement that puts something in it, so a `CREATE TEMPORARY`
        // must not be refused for naming a namespace that does not exist yet.
        let creates_the_namespace =
            disposition == SchemaDisposition::TemporaryCreation && schema == temp;
        if disposition != SchemaDisposition::Reference
            && !creates_the_namespace
            && !crabka_pgcatalog::schema_exists(kv, &schema)?
        {
            // The report names the qualifier as written, so `pg_temp` is
            // reported as `pg_temp`.
            return Err(CatalogError::UndefinedSchema(written.clone()).into());
        }
        let resolved = RelationName::new(schema, reference.name.clone());
        // A `42P01` names the qualifier as it was written, so an alias that
        // resolved to nothing keeps its written spelling for the report:
        // `SELECT * FROM pg_temp.nothere` is `relation "pg_temp.nothere" does
        // not exist`, never the expanded namespace's name.
        if !creating
            && written == crabka_pgcatalog::PG_TEMP_ALIAS
            && !crabka_pgcatalog::relation_exists(kv, &resolved)?
        {
            return Ok(RelationName::new(written.clone(), reference.name.clone()));
        }
        return Ok(resolved);
    }
    if disposition == SchemaDisposition::TemporaryCreation {
        return Ok(RelationName::new(temp, reference.name.clone()));
    }
    if disposition == SchemaDisposition::Creation {
        let schema = scope
            .creation_schema(kv)?
            .ok_or(ExecError::NoSchemaSelected)?;
        return Ok(RelationName::new(schema, reference.name.clone()));
    }
    for schema in scope.visible_schemas(kv)? {
        let candidate = RelationName::new(schema, reference.name.clone());
        // A synthesised catalog relation counts as present, so the implicit
        // `pg_catalog` entry finds `pg_class` before a user relation of that
        // name in `public` does — which the oracle confirms is what
        // `PostgreSQL` does.
        if crate::exec::is_virtual_relation(&candidate)
            || crabka_pgcatalog::relation_exists(kv, &candidate)?
        {
            return Ok(candidate);
        }
    }
    // Nothing on the path holds it. The name still has to resolve to
    // *something* so the lookup can report the `42P01` PostgreSQL reports —
    // which names the relation as written, so the fallback schema must be the
    // one whose rendering is bare.
    Ok(RelationName::public(reference.name.clone()))
}

/// [`resolve_relation`] over a statement's whole name list: `DROP TABLE a, b,
/// c` and its relatives.
///
/// # Errors
///
/// As [`resolve_relation`], for the first entry that fails.
pub fn resolve_relations(
    kv: &dyn Kv,
    scope: &ResolutionScope,
    references: &[RelationRef],
    disposition: SchemaDisposition,
) -> Result<Vec<RelationName>, ExecError> {
    references
        .iter()
        .map(|reference| resolve_relation(kv, scope, reference, disposition))
        .collect()
}

// ------------------------------------------------- reading a written name

/// A relation name that arrived as a runtime string, parsed into its parts.
///
/// [`parse_written_relation`] produces this value, and it is the only reader of
/// such a string. The parts then resolve through [`resolve_relation`] like any
/// name the grammar produced.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WrittenRelation {
    /// The schema and relation the parts denote, each already unquoted and
    /// case-folded. This is the same shape the lexer hands the grammar.
    pub reference: RelationRef,
    /// Every parsed part joined by dots and left unquoted, which is
    /// `PostgreSQL`'s `NameListToString` and what its diagnostics from this
    /// path name: `'"A b".c'::regclass` is `relation "A b.c" does not exist`.
    /// It keeps the catalog part a three-part name wrote, which
    /// [`WrittenRelation::reference`] drops.
    pub dotted: String,
}

impl WrittenRelation {
    /// The `42P01` for a name nothing answers to, spelled as
    /// `NameListToString` renders it.
    #[must_use]
    pub fn undefined_table(&self) -> ExecError {
        CatalogError::UndefinedTable(self.dotted.clone()).into()
    }
}

/// Read a `regclass` input the way `PostgreSQL`'s `regclassin` reads one.
///
/// The text is *not* one identifier. `stringToQualifiedNameList` splits it on
/// the dots that fall outside double quotes, downcases each unquoted part, and
/// unwraps each quoted one. Inside a quoted part, `""` is one literal quote and
/// a dot is an ordinary character. The function also tolerates whitespace
/// around every part. `makeRangeVarFromNameList` then reads one part as a
/// relation, two as `schema.relation` and three as `catalog.schema.relation`.
///
/// This is the input half of the `regclass` round trip whose output half is
/// `crate::catalog_fn::relation_name_by_oid`, so every spelling that one quotes
/// must read back here.
///
/// Verified against `postgres:18.4`:
///
/// ```text
/// '"a.b"'::regclass           relation named a.b, not schema "a / relation b"
/// '"MyTbl"'::regclass         relation named MyTbl
/// 'MYTABLE'::regclass         relation named mytable
/// ' public . t '::regclass    relation public.t
/// '<thisdb>.public.t'         relation public.t — the catalog part is this db
/// 'otherdb.public.t'          0A000 cross-database references are not implem…
/// 'a.b.c.d'                   42601 improper relation name (too many dotted …
/// ''  '   '  '.t'  't.'  'a..b'  '"abc'  '"a"b'  'x y'
///                             42602 invalid name syntax
/// '""'                        42P01 relation "" does not exist
/// ```
///
/// One `PostgreSQL` step is deliberately absent: `truncate_identifier` clips
/// each part to `NAMEDATALEN - 1` bytes. crabka does not truncate an identifier
/// anywhere, not in the lexer and not at `CREATE`. A truncation only here would
/// make an over-long name unreadable back out of the catalog that stored it.
///
/// # Errors
///
/// 42602 `invalid name syntax` for text that is not a qualified name, 42601 for
/// more than three parts, and 0A000 for a catalog part that names another
/// database.
pub fn parse_written_relation(
    scope: &ResolutionScope,
    text: &str,
) -> Result<WrittenRelation, ExecError> {
    let parts = split_identifier_string(text).ok_or_else(invalid_name_syntax)?;
    let dotted = parts.join(".");
    let reference = match parts.as_slice() {
        // An empty list is the empty (or all-whitespace) input, which
        // `stringToQualifiedNameList` refuses after `SplitIdentifierString`
        // accepts it.
        [] => return Err(invalid_name_syntax()),
        [name] => RelationRef::bare(name),
        [schema, name] => RelationRef::qualified(schema, name),
        [catalog, schema, name] => {
            if *catalog != scope.database {
                return Err(ExecError::Unsupported(format!(
                    "cross-database references are not implemented: \"{dotted}\""
                )));
            }
            RelationRef::qualified(schema, name)
        }
        _ => {
            return Err(ExecError::FunctionError {
                sqlstate: "42601",
                message: format!("improper relation name (too many dotted names): {dotted}"),
            });
        }
    };
    Ok(WrittenRelation { reference, dotted })
}

pub(crate) fn invalid_name_syntax() -> ExecError {
    ExecError::FunctionError {
        sqlstate: "42602",
        message: "invalid name syntax".into(),
    }
}

/// `PostgreSQL`'s `SplitIdentifierString(text, '.')`: the parts of a written
/// name, or `None` for the text it rejects. An empty result is the empty input,
/// which that function accepts and its one caller here does not.
pub(crate) fn split_identifier_string(text: &str) -> Option<Vec<String>> {
    let mut rest = text.trim_start_matches(is_scanner_space);
    if rest.is_empty() {
        return Some(Vec::new());
    }
    let mut parts = Vec::new();
    loop {
        let part = if let Some(body) = rest.strip_prefix('"') {
            let (name, tail) = quoted_identifier(body)?;
            rest = tail;
            name
        } else {
            // An unquoted part runs to the next dot or whitespace, and is
            // downcased exactly as the lexer downcases one: ASCII `A`-`Z` only,
            // because crabka is UTF-8 and `downcase_identifier` leaves a
            // multibyte encoding's high bytes alone.
            let end = rest
                .find(|c: char| c == '.' || is_scanner_space(c))
                .unwrap_or(rest.len());
            let (name, tail) = rest.split_at(end);
            if name.is_empty() {
                return None;
            }
            rest = tail;
            name.to_ascii_lowercase()
        };
        parts.push(part);
        rest = rest.trim_start_matches(is_scanner_space);
        match rest.strip_prefix('.') {
            Some(tail) => rest = tail.trim_start_matches(is_scanner_space),
            // Anything but a separator after a part — `"a"b`, `x y` — is not a
            // qualified name at all.
            None if rest.is_empty() => return Some(parts),
            None => return None,
        }
    }
}

/// The body of a double-quoted part and the text after its closing quote, with
/// each doubled quote collapsed to one. `None` when the quote is never closed.
fn quoted_identifier(body: &str) -> Option<(String, &str)> {
    let mut name = String::new();
    let mut rest = body;
    loop {
        let close = rest.find('"')?;
        name.push_str(&rest[..close]);
        rest = &rest[close + 1..];
        match rest.strip_prefix('"') {
            Some(tail) => {
                name.push('"');
                rest = tail;
            }
            None => return Some((name, rest)),
        }
    }
}

/// Whitespace as `PostgreSQL`'s scanner counts it. This is the `{space}` class
/// in `scan.l` that `scanner_isspace` mirrors, and it includes the vertical tab.
fn is_scanner_space(c: char) -> bool {
    matches!(c, ' ' | '\t' | '\n' | '\r' | '\u{b}' | '\u{c}')
}

/// True when `error` is the missing-schema report an `IF EXISTS` form skips.
///
/// `DROP TABLE IF EXISTS nope.t` is a skipped no-op on `PostgreSQL`, not a
/// `3F000`, even though the plain spelling reports the schema.
#[must_use]
pub fn is_missing_schema(error: &ExecError) -> bool {
    matches!(error, ExecError::Catalog(CatalogError::UndefinedSchema(_)))
}

#[cfg(test)]
mod tests {
    use assert2::assert;
    use crabka_pgcatalog::RelationName;
    use crabka_pgkv::{Kv as _, MemKv};
    use crabka_pgparser::ast::RelationRef;

    use super::{
        ResolutionScope, SchemaDisposition, WrittenRelation, is_missing_schema,
        parse_written_relation, resolve_relation,
    };
    use crate::search_path::SearchPath;

    fn written(reference: RelationRef, dotted: &str) -> WrittenRelation {
        WrittenRelation {
            reference,
            dotted: dotted.to_string(),
        }
    }

    /// Every shape `postgres:18.4` accepts, with the parts it reads out of it.
    /// A quoted part keeps its case and may hold a dot. An unquoted one
    /// downcases and ends at the first dot or whitespace. `""` inside quotes is
    /// one literal quote. Whitespace may sit anywhere around a part.
    #[test]
    fn a_written_name_is_read_the_way_regclassin_reads_one() {
        let cases = [
            ("t", written(RelationRef::bare("t"), "t")),
            ("MYTABLE", written(RelationRef::bare("mytable"), "mytable")),
            ("\"MyTbl\"", written(RelationRef::bare("MyTbl"), "MyTbl")),
            // A dot inside quotes is part of the name, not a qualifier.
            ("\"a.b\"", written(RelationRef::bare("a.b"), "a.b")),
            ("\"a\"\"b\"", written(RelationRef::bare("a\"b"), "a\"b")),
            ("\"\"", written(RelationRef::bare(""), "")),
            // A quote inside an *unquoted* part is an ordinary character.
            ("a\"b", written(RelationRef::bare("a\"b"), "a\"b")),
            ("S1.T", written(RelationRef::qualified("s1", "t"), "s1.t")),
            (
                " s1 . t ",
                written(RelationRef::qualified("s1", "t"), "s1.t"),
            ),
            (
                "\t\n\u{b}\u{c}\rs1\r.\u{c}t\u{b}",
                written(RelationRef::qualified("s1", "t"), "s1.t"),
            ),
            (
                "\"A b\" . \"c.d\"",
                written(RelationRef::qualified("A b", "c.d"), "A b.c.d"),
            ),
            // Three parts name a catalog, which only this one database answers
            // to; the reference drops it and the rendering keeps it.
            (
                "postgres.s1.t",
                written(RelationRef::qualified("s1", "t"), "postgres.s1.t"),
            ),
        ];
        for (input, expected) in cases {
            assert!(
                parse_written_relation(ResolutionScope::default_scope(), input).expect("parses")
                    == expected,
                "{input:?}"
            );
        }
    }

    /// Every refusal, with the SQLSTATE and message `postgres:18.4` raises.
    #[test]
    fn a_name_that_is_not_a_qualified_name_is_refused_as_postgres_refuses_it() {
        let syntax = ("42602", "invalid name syntax".to_string());
        let cases = [
            ("", syntax.clone()),
            ("   ", syntax.clone()),
            (".", syntax.clone()),
            (".t", syntax.clone()),
            ("t.", syntax.clone()),
            ("\"t\" . ", syntax.clone()),
            ("a..b", syntax.clone()),
            // A quote that never closes, and text after one that does.
            ("\"abc", syntax.clone()),
            ("\"a\"b", syntax.clone()),
            // Whitespace inside an unquoted part ends it, and what follows is
            // neither a separator nor the end.
            ("x y", syntax.clone()),
            (
                "a.b.c.d",
                (
                    "42601",
                    "improper relation name (too many dotted names): a.b.c.d".to_string(),
                ),
            ),
            (
                "\"a b\".\"c\".d.e",
                (
                    "42601",
                    "improper relation name (too many dotted names): a b.c.d.e".to_string(),
                ),
            ),
            (
                "OtherDB.Public.T",
                (
                    "0A000",
                    "cross-database references are not implemented: \"otherdb.public.t\""
                        .to_string(),
                ),
            ),
        ];
        for (input, (code, message)) in cases {
            let error = parse_written_relation(ResolutionScope::default_scope(), input)
                .expect_err("refused")
                .into_pg();
            assert!(
                (error.code.as_str(), error.message) == (code, message),
                "{input:?}"
            );
        }
    }

    /// The `42P01` a name nothing answers to raises spells the *parsed* parts,
    /// joined by dots and unquoted. This is `PostgreSQL`'s `NameListToString`,
    /// not the text as typed.
    #[test]
    fn a_missing_relation_is_named_by_its_parsed_parts() {
        for (input, named) in [
            (" NoSuch . T ", "nosuch.t"),
            ("\"A b\".c", "A b.c"),
            ("postgres.nosuchschema.t", "postgres.nosuchschema.t"),
        ] {
            let error = parse_written_relation(ResolutionScope::default_scope(), input)
                .expect("parses")
                .undefined_table()
                .into_pg();
            assert!(error.code == "42P01", "{input:?}");
            assert!(
                error.message == format!("relation \"{named}\" does not exist"),
                "{input:?}"
            );
        }
    }

    fn scope_over(entries: &[&str]) -> ResolutionScope {
        ResolutionScope {
            search_path: SearchPath::from_items(
                &entries.iter().map(|e| (*e).to_string()).collect::<Vec<_>>(),
            ),
            ..ResolutionScope::default()
        }
    }

    fn with_schemas(names: &[&str]) -> MemKv {
        let kv = MemKv::default();
        for name in names {
            let ops = crabka_pgcatalog::create_schema_ops(&kv, name, "postgres").expect("schema");
            kv.write_batch(&ops).expect("write");
        }
        kv
    }

    fn create(kv: &MemKv, name: &RelationName) {
        let (_, ops) = crabka_pgcatalog::create_table_ops(
            kv,
            name,
            vec![crabka_pgcatalog::Column::new(
                "x",
                crabka_pgtypes::ColumnType::Int4,
            )],
        )
        .expect("create table");
        kv.write_batch(&ops).expect("write");
    }

    #[test]
    fn a_written_qualifier_is_kept_verbatim() {
        let kv = with_schemas(&["s1"]);
        let scope = ResolutionScope::default();
        for schema in ["s1", "pg_catalog", "information_schema", "public"] {
            let reference = RelationRef::qualified(schema, "t");
            let resolved = resolve_relation(&kv, &scope, &reference, SchemaDisposition::Utility)
                .expect("resolves");
            assert!(resolved == RelationName::new(schema, "t"));
        }
    }

    #[test]
    fn a_missing_schema_is_reported_for_every_disposition_but_a_reference() {
        let kv = MemKv::default();
        let scope = ResolutionScope::default();
        let reference = RelationRef::qualified("nope", "t");
        let referenced = resolve_relation(&kv, &scope, &reference, SchemaDisposition::Reference)
            .expect("no check");
        assert!(referenced == RelationName::new("nope", "t"));
        for disposition in [SchemaDisposition::Utility, SchemaDisposition::Creation] {
            let error = resolve_relation(&kv, &scope, &reference, disposition).expect_err("3F000");
            assert!(is_missing_schema(&error));
            assert!(error.into_pg().code == "3F000");
        }
    }

    #[test]
    fn an_unqualified_name_takes_the_first_search_path_entry_that_holds_it() {
        let kv = with_schemas(&["s1", "s2"]);
        create(&kv, &RelationName::new("s2", "t"));
        let reference = RelationRef::bare("t");
        for (entries, expected) in [
            (vec!["s1", "s2"], RelationName::new("s2", "t")),
            (vec!["s2", "s1"], RelationName::new("s2", "t")),
            // Nothing on the path holds it, so it falls back to the spelling
            // the 42P01 has to report.
            (vec!["s1"], RelationName::public("t")),
        ] {
            let scope = scope_over(&entries);
            let resolved = resolve_relation(&kv, &scope, &reference, SchemaDisposition::Reference)
                .expect("resolves");
            assert!(resolved == expected);
        }
    }

    #[test]
    fn a_creation_lands_in_the_first_existing_explicit_entry() {
        let kv = with_schemas(&["s1", "s2"]);
        let reference = RelationRef::bare("lands");
        let scope = scope_over(&["nosuch", "s2", "s1"]);
        let resolved =
            resolve_relation(&kv, &scope, &reference, SchemaDisposition::Creation).expect("lands");
        assert!(resolved == RelationName::new("s2", "lands"));
    }

    #[test]
    fn a_creation_with_no_existing_entry_is_refused() {
        let kv = MemKv::default();
        let scope = scope_over(&["notme"]);
        let error = resolve_relation(
            &kv,
            &scope,
            &RelationRef::bare("t"),
            SchemaDisposition::Creation,
        )
        .expect_err("3F000");
        assert!(error.into_pg().code == "3F000");
    }

    #[test]
    fn pg_catalog_is_implicit_and_first_unless_written() {
        let kv = MemKv::default();
        assert!(
            scope_over(&["public"])
                .visible_schemas(&kv)
                .expect("visible")
                == vec!["pg_catalog".to_string(), "public".to_string()]
        );
        assert!(
            scope_over(&["public", "pg_catalog"])
                .visible_schemas(&kv)
                .expect("visible")
                == vec!["public".to_string(), "pg_catalog".to_string()]
        );
    }

    /// A session's own backend id, and a namespace belonging to some other
    /// session.
    const OWN_BACKEND: i32 = 7;
    const OTHER_TEMP: &str = "pg_temp_9";

    fn temp_scope(entries: &[&str]) -> ResolutionScope {
        ResolutionScope {
            backend_id: OWN_BACKEND,
            ..scope_over(entries)
        }
    }

    /// Bring a session's temporary namespace into being, as the first statement
    /// that creates something in it does.
    fn with_temp_schema(kv: &MemKv, name: &str) {
        kv.write_batch(&[crabka_pgcatalog::create_temp_schema_op(name)])
            .expect("write");
    }

    /// `pg_temp` names the session's own namespace wherever it is written, and
    /// an unqualified `CREATE TEMPORARY` lands there. The search path has no
    /// say.
    #[test]
    fn a_temporary_creation_lands_in_the_sessions_own_namespace() {
        let kv = with_schemas(&["s1"]);
        let scope = temp_scope(&["s1"]);
        let own = RelationName::new("pg_temp_7", "t");
        let cases = [
            RelationRef::bare("t"),
            RelationRef::qualified("pg_temp", "t"),
            RelationRef::qualified("pg_temp_7", "t"),
        ];
        for reference in cases {
            let resolved = resolve_relation(
                &kv,
                &scope,
                &reference,
                SchemaDisposition::TemporaryCreation,
            )
            .expect("resolves");
            assert!(resolved == own, "{reference:?}");
        }
    }

    /// The two `42P16` refusals, each with the wording `postgres:18.4` uses.
    #[test]
    fn a_creation_naming_the_wrong_kind_of_namespace_is_refused() {
        let kv = with_schemas(&["s1"]);
        with_temp_schema(&kv, OTHER_TEMP);
        let scope = temp_scope(&["s1"]);
        let cases = [
            (
                RelationRef::qualified("s1", "t"),
                SchemaDisposition::TemporaryCreation,
                "cannot create temporary relation in non-temporary schema",
            ),
            (
                RelationRef::qualified("pg_catalog", "t"),
                SchemaDisposition::TemporaryCreation,
                "cannot create temporary relation in non-temporary schema",
            ),
            (
                RelationRef::qualified(OTHER_TEMP, "t"),
                SchemaDisposition::TemporaryCreation,
                "cannot create relations in temporary schemas of other sessions",
            ),
            (
                RelationRef::qualified(OTHER_TEMP, "t"),
                SchemaDisposition::Creation,
                "cannot create relations in temporary schemas of other sessions",
            ),
        ];
        for (reference, disposition, message) in cases {
            let error = resolve_relation(&kv, &scope, &reference, disposition)
                .expect_err("refused")
                .into_pg();
            assert!(error.code == "42P16", "{reference:?}");
            assert!(error.message == message, "{reference:?}");
        }
    }

    /// The temporary namespace is implicit and FIRST, ahead of the implicit
    /// `pg_catalog`, until the path names it. Then it sits where it was
    /// written. It is absent completely until the session has one.
    #[test]
    fn the_temporary_namespace_is_implicit_first_unless_written() {
        let kv = with_schemas(&[]);
        let scope = temp_scope(&["public"]);
        assert!(scope.visible_schemas(&kv).expect("visible") == ["pg_catalog", "public"]);
        with_temp_schema(&kv, "pg_temp_7");
        let cases = [
            (vec!["public"], vec!["pg_temp_7", "pg_catalog", "public"]),
            (
                vec!["public", "pg_catalog"],
                vec!["pg_temp_7", "public", "pg_catalog"],
            ),
            (
                vec!["public", "pg_temp"],
                vec!["pg_catalog", "public", "pg_temp_7"],
            ),
            (vec!["pg_temp"], vec!["pg_catalog", "pg_temp_7"]),
        ];
        for (entries, expected) in cases {
            let scope = temp_scope(&entries);
            assert!(
                scope.visible_schemas(&kv).expect("visible") == expected,
                "{entries:?}"
            );
        }
        // A written `pg_temp` is an ordinary explicit entry, so it is also the
        // creation target: `SET search_path = pg_temp` makes `current_schema`
        // the temporary namespace.
        assert!(
            temp_scope(&["pg_temp"])
                .creation_schema(&kv)
                .expect("creation")
                == Some("pg_temp_7".to_string())
        );
    }

    #[test]
    fn a_nonexistent_entry_is_skipped_rather_than_refused() {
        let kv = with_schemas(&["s1"]);
        let scope = scope_over(&["notme", "s1"]);
        assert!(scope.explicit_schemas(&kv).expect("explicit") == vec!["s1".to_string()]);
        assert!(scope.creation_schema(&kv).expect("creation") == Some("s1".to_string()));
    }
}
