//! The element list of `CREATE SCHEMA name [AUTHORIZATION role] <element> …`.
//!
//! `PostgreSQL` runs the elements as if the new schema were the first entry on
//! `search_path`, and before it runs any of them it does two things to the
//! list. Both live here, because both are decisions about the written text and
//! neither needs the catalog.
//!
//! The first is `setSchemaName`: every element's *own* relation takes the new
//! schema's name. An element that already wrote a qualifier keeps it only when
//! it is the same schema, and is refused otherwise. That refusal is the whole
//! of `create_schema`'s first half in `pg_regress`.
//!
//! The second is the ordering. The list is written in whatever order suits the
//! author, and `transformCreateSchemaStmtElements` reorders it into one with no
//! forward references: sequences, then tables, then views, then indexes, then
//! triggers, then grants. `namespace.sql` depends on it, writing an index and a
//! view over a table it declares last.
//!
//! Upstream's own note is worth keeping: "the logic we use for determining
//! forward references is presently quite incomplete". Six buckets are the whole
//! rule. Two tables that reference each other are still the author's problem.

use crabka_pgparser::ast::{RelationRef, Statement};

use crate::error::ExecError;

/// The buckets `transformCreateSchemaStmtElements` sorts the elements into,
/// declared in the order it concatenates them.
///
/// The derived `Ord` is that order, so the sort is one `sort_by_key`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
enum ElementKind {
    Sequence,
    Table,
    View,
    Index,
    Trigger,
    Grant,
}

/// The elements of a `CREATE SCHEMA`, qualified with `schema` and reordered so
/// that a later one may reference an earlier one.
///
/// The returned statements are ordinary DDL and are meant to be executed in the
/// order given, each against a catalog that already holds the effects of the
/// ones before it.
///
/// # Errors
///
/// Returns 42P15 when an element names a schema other than `schema`, and 0A000
/// for a statement the parser should already have refused as an element.
pub(crate) fn plan(schema: &str, elements: &[Statement]) -> Result<Vec<Statement>, ExecError> {
    let mut planned = Vec::with_capacity(elements.len());
    // In written order, because the refusal below belongs to the first element
    // that contradicts the schema, not to the first of its kind.
    for element in elements {
        let mut element = element.clone();
        let kind = qualify(schema, &mut element)?;
        planned.push((kind, element));
    }
    // Stable, so elements of one kind keep the order they were written in.
    planned.sort_by_key(|(kind, _)| *kind);
    Ok(planned.into_iter().map(|(_, element)| element).collect())
}

/// Give `element`'s own relation the schema being created, and say which bucket
/// the element belongs to.
fn qualify(schema: &str, element: &mut Statement) -> Result<ElementKind, ExecError> {
    match element {
        // `CREATE SEQUENCE` shares its variant with `CREATE INDEX`, and the two
        // qualify different fields: a sequence is the relation being created,
        // an index is a name upstream never qualifies at all — an index lands
        // in its table's schema, so the table is what carries the qualifier.
        Statement::CreateIndex {
            name: Some(name),
            table,
            ..
        } if table.name == crabka_pgparser::ast::SEQUENCE_RELATION => {
            set_schema(schema, name)?;
            Ok(ElementKind::Sequence)
        }
        Statement::CreateIndex { table, .. } => {
            set_schema(schema, table)?;
            Ok(ElementKind::Index)
        }
        Statement::CreateTable { name, .. } => {
            set_schema(schema, name)?;
            Ok(ElementKind::Table)
        }
        Statement::CreateView { name, .. } => {
            set_schema(schema, name)?;
            Ok(ElementKind::View)
        }
        Statement::CreateTrigger(trigger) => {
            set_schema(schema, &mut trigger.table)?;
            Ok(ElementKind::Trigger)
        }
        // A `GrantStmt` names objects that already exist, so upstream leaves it
        // alone and lets the prepended `search_path` find them.
        Statement::GrantTablePrivileges { .. }
        | Statement::GrantSchemaPrivileges { .. }
        | Statement::GrantForeignPrivileges { .. } => Ok(ElementKind::Grant),
        other => Err(ExecError::Unsupported(format!(
            "{} is not a CREATE SCHEMA element",
            crate::telemetry::statement_operation(other)
        ))),
    }
}

/// `setSchemaName`: an unqualified reference takes `schema`, and a qualified
/// one has to already name it.
fn set_schema(schema: &str, reference: &mut RelationRef) -> Result<(), ExecError> {
    match &reference.schema {
        None => {
            reference.schema = Some(schema.to_string());
            Ok(())
        }
        Some(written) if written == schema => Ok(()),
        // Unquoted, both of them, exactly as `parse_utilcmd.c` writes it.
        Some(written) => Err(ExecError::InvalidSchemaDefinition(format!(
            "CREATE specifies a schema ({written}) different from the one being created ({schema})"
        ))),
    }
}

#[cfg(test)]
mod tests {
    use assert2::assert;
    use crabka_pgparser::ast::Statement;

    use super::plan;

    /// Parse one `CREATE SCHEMA` and plan its elements against `schema`.
    fn planned(schema: &str, sql: &str) -> Result<Vec<String>, crate::error::ExecError> {
        let Statement::CreateSchema { elements, .. } = one(sql) else {
            panic!("not a CREATE SCHEMA: {sql}")
        };
        Ok(plan(schema, &elements)?.iter().map(named).collect())
    }

    fn one(sql: &str) -> Statement {
        let mut parsed =
            crabka_pgparser::parse(sql).unwrap_or_else(|error| panic!("{sql}: {error}"));
        assert!(parsed.len() == 1, "{sql}");
        parsed.pop().expect("one statement")
    }

    /// `kind schema.relation` for the relation the element qualifies, which is
    /// what the plan is about.
    fn named(element: &Statement) -> String {
        match element {
            Statement::CreateIndex {
                name: Some(name),
                table,
                ..
            } if table.name == crabka_pgparser::ast::SEQUENCE_RELATION => {
                format!("sequence {name}")
            }
            Statement::CreateIndex { table, .. } => format!("index on {table}"),
            Statement::CreateTable { name, .. } => format!("table {name}"),
            Statement::CreateView { name, .. } => format!("view {name}"),
            Statement::CreateTrigger(trigger) => format!("trigger on {}", trigger.table),
            Statement::GrantTablePrivileges { tables, .. } => format!("grant on {}", tables[0]),
            other => format!("{other:?}"),
        }
    }

    /// Every element's own relation lands in the schema being created, whether
    /// or not it wrote the qualifier itself.
    #[test]
    fn an_element_relation_takes_the_new_schema() {
        let cases: &[(&str, &str)] = &[
            ("CREATE SCHEMA s CREATE TABLE t (a int)", "table s.t"),
            ("CREATE SCHEMA s CREATE TABLE s.t (a int)", "table s.t"),
            ("CREATE SCHEMA s CREATE SEQUENCE q", "sequence s.q"),
            ("CREATE SCHEMA s CREATE VIEW v AS SELECT 1", "view s.v"),
            // An index qualifies the table it indexes, never its own name.
            ("CREATE SCHEMA s CREATE INDEX ON t (a)", "index on s.t"),
            (
                "CREATE SCHEMA s CREATE UNIQUE INDEX i ON t (a)",
                "index on s.t",
            ),
            (
                "CREATE SCHEMA s CREATE TRIGGER g BEFORE INSERT ON t EXECUTE FUNCTION f()",
                "trigger on s.t",
            ),
            // A grant is left as written.
            ("CREATE SCHEMA s GRANT SELECT ON t TO PUBLIC", "grant on t"),
        ];
        for (sql, want) in cases {
            let got = planned("s", sql).unwrap_or_else(|error| panic!("{sql}: {error:?}"));
            assert!(got == vec![(*want).to_string()], "case: {sql}");
        }
    }

    /// A qualifier that names another schema is 42P15, and the message names
    /// the written schema first.
    #[test]
    fn an_element_may_not_name_another_schema() {
        let cases: &[&str] = &[
            "CREATE SCHEMA s CREATE TABLE other.t (a int)",
            "CREATE SCHEMA s CREATE SEQUENCE other.q",
            "CREATE SCHEMA s CREATE VIEW other.v AS SELECT 1",
            "CREATE SCHEMA s CREATE INDEX ON other.t (a)",
            "CREATE SCHEMA s CREATE TRIGGER g BEFORE INSERT ON other.t EXECUTE FUNCTION f()",
        ];
        for sql in cases {
            let error = planned("s", sql).expect_err(sql);
            assert!(
                error.into_pg().message
                    == "CREATE specifies a schema (other) different from the one being created (s)",
                "case: {sql}"
            );
        }
    }

    /// The refusal belongs to the first element that contradicts the schema,
    /// in the order the elements were written rather than the order they run.
    #[test]
    fn the_first_written_contradiction_is_the_one_reported() {
        let sql = "CREATE SCHEMA s CREATE TABLE first_one.t (a int) CREATE SEQUENCE second_one.q";
        let error = planned("s", sql).expect_err(sql);
        assert!(
            error.into_pg().message
                == "CREATE specifies a schema (first_one) different from the one being \
                    created (s)"
        );
    }

    /// Sequences, tables, views, indexes, triggers and grants, in that order,
    /// so a view can read a table the author wrote after it. `namespace.sql`
    /// writes exactly this shape.
    #[test]
    fn elements_run_in_dependency_order_not_written_order() {
        let sql = "CREATE SCHEMA s \
                   CREATE UNIQUE INDEX abc_a_idx ON abc (a) \
                   CREATE VIEW abc_view AS SELECT a FROM abc \
                   CREATE TABLE abc (a int, b int)";
        let got = planned("s", sql).expect(sql);
        assert!(got == vec!["table s.abc", "view s.abc_view", "index on s.abc"]);
    }

    /// Within one kind the written order survives, because a second table may
    /// reference the first.
    #[test]
    fn one_kind_keeps_its_written_order() {
        let sql = "CREATE SCHEMA s \
                   CREATE TABLE a (x int) \
                   CREATE TABLE b (y int) \
                   CREATE TABLE c (z int)";
        let got = planned("s", sql).expect(sql);
        assert!(got == vec!["table s.a", "table s.b", "table s.c"]);
    }
}
