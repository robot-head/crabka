//! Parser-owned registry of accepted `PostgreSQL` command identities.

macro_rules! command_identities {
    ($(($variant:ident, $name:literal)),+ $(,)?) => {
        #[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
        pub enum CommandIdentity { $( $variant, )+ }
        impl CommandIdentity {
            pub const ALL: &[Self] = &[ $( Self::$variant, )+ ];
            #[must_use]
            pub const fn name(self) -> &'static str { match self { $( Self::$variant => $name, )+ } }
        }
    };
}

command_identities!(
    (Abort, "ABORT"),
    (AlterConversion, "ALTER CONVERSION"),
    (AlterDatabase, "ALTER DATABASE"),
    (AlterExtension, "ALTER EXTENSION"),
    (AlterLanguage, "ALTER LANGUAGE"),
    (AlterLargeObject, "ALTER LARGE OBJECT"),
    (AlterOperator, "ALTER OPERATOR"),
    (AlterOperatorClass, "ALTER OPERATOR CLASS"),
    (AlterOperatorFamily, "ALTER OPERATOR FAMILY"),
    (AlterPublication, "ALTER PUBLICATION"),
    (AlterRule, "ALTER RULE"),
    (AlterServer, "ALTER SERVER"),
    (AlterSubscription, "ALTER SUBSCRIPTION"),
    (AlterTable, "ALTER TABLE"),
    (AlterTablespace, "ALTER TABLESPACE"),
    (AlterTextSearchParser, "ALTER TEXT SEARCH PARSER"),
    (AlterTextSearchTemplate, "ALTER TEXT SEARCH TEMPLATE"),
    (AlterUserMapping, "ALTER USER MAPPING"),
    (Begin, "BEGIN"),
    (Commit, "COMMIT"),
    (CommitPrepared, "COMMIT PREPARED"),
    (Copy, "COPY"),
    (CreateAccessMethod, "CREATE ACCESS METHOD"),
    (CreateConversion, "CREATE CONVERSION"),
    (CreateDatabase, "CREATE DATABASE"),
    (CreateForeignDataWrapper, "CREATE FOREIGN DATA WRAPPER"),
    (CreateForeignTable, "CREATE FOREIGN TABLE"),
    (CreateIndex, "CREATE INDEX"),
    (CreateLanguage, "CREATE LANGUAGE"),
    (CreateOperator, "CREATE OPERATOR"),
    (CreateOperatorClass, "CREATE OPERATOR CLASS"),
    (CreateOperatorFamily, "CREATE OPERATOR FAMILY"),
    (CreatePublication, "CREATE PUBLICATION"),
    (CreateRole, "CREATE ROLE"),
    (CreateRule, "CREATE RULE"),
    (CreateSequence, "CREATE SEQUENCE"),
    (CreateServer, "CREATE SERVER"),
    (CreateSubscription, "CREATE SUBSCRIPTION"),
    (CreateTable, "CREATE TABLE"),
    (CreateTablespace, "CREATE TABLESPACE"),
    (CreateTextSearchParser, "CREATE TEXT SEARCH PARSER"),
    (CreateTextSearchTemplate, "CREATE TEXT SEARCH TEMPLATE"),
    (CreateTransform, "CREATE TRANSFORM"),
    (CreateUser, "CREATE USER"),
    (CreateUserMapping, "CREATE USER MAPPING"),
    (CreateView, "CREATE VIEW"),
    (Delete, "DELETE"),
    (Discard, "DISCARD"),
    (DropAccessMethod, "DROP ACCESS METHOD"),
    (DropConversion, "DROP CONVERSION"),
    (DropDatabase, "DROP DATABASE"),
    (DropExtension, "DROP EXTENSION"),
    (DropForeignDataWrapper, "DROP FOREIGN DATA WRAPPER"),
    (DropForeignTable, "DROP FOREIGN TABLE"),
    (DropIndex, "DROP INDEX"),
    (DropLanguage, "DROP LANGUAGE"),
    (DropOperator, "DROP OPERATOR"),
    (DropOperatorClass, "DROP OPERATOR CLASS"),
    (DropOperatorFamily, "DROP OPERATOR FAMILY"),
    (DropPublication, "DROP PUBLICATION"),
    (DropRole, "DROP ROLE"),
    (DropRule, "DROP RULE"),
    (DropSequence, "DROP SEQUENCE"),
    (DropServer, "DROP SERVER"),
    (DropSubscription, "DROP SUBSCRIPTION"),
    (DropTable, "DROP TABLE"),
    (DropTablespace, "DROP TABLESPACE"),
    (DropTextSearchParser, "DROP TEXT SEARCH PARSER"),
    (DropTextSearchTemplate, "DROP TEXT SEARCH TEMPLATE"),
    (DropTransform, "DROP TRANSFORM"),
    (DropUser, "DROP USER"),
    (DropUserMapping, "DROP USER MAPPING"),
    (DropView, "DROP VIEW"),
    (End, "END"),
    (Grant, "GRANT"),
    (ImportForeignSchema, "IMPORT FOREIGN SCHEMA"),
    (Insert, "INSERT"),
    (Load, "LOAD"),
    (PrepareTransaction, "PREPARE TRANSACTION"),
    (Reset, "RESET"),
    (Revoke, "REVOKE"),
    (Rollback, "ROLLBACK"),
    (RollbackPrepared, "ROLLBACK PREPARED"),
    (SecurityLabel, "SECURITY LABEL"),
    (Select, "SELECT"),
    (Set, "SET"),
    (SetRole, "SET ROLE"),
    (SetTransaction, "SET TRANSACTION"),
    (Show, "SHOW"),
    (StartTransaction, "START TRANSACTION"),
    (Update, "UPDATE"),
    (Values, "VALUES"),
);

impl CommandIdentity {
    /// Classify a statement at the parser boundary from its typed AST and
    /// leading command tokens. The parser rejects a statement if this registry
    /// cannot classify it, so accepted identities cannot drift independently.
    #[must_use]
    pub fn classify(statement: &crate::ast::Statement, sql: &str) -> Option<Self> {
        use crate::ast::Statement;

        if let Some(command) = statement.compatibility_refusal() {
            return Self::ALL
                .iter()
                .copied()
                .find(|identity| identity.name() == command.command_name());
        }
        if matches!(statement, Statement::Query(_)) {
            let first = leading_words(sql).into_iter().next();
            return if first.as_deref() == Some("values") {
                Some(Self::Values)
            } else {
                Some(Self::Select)
            };
        }

        let mut words = leading_words(sql);
        if words.first().is_some_and(|word| word == "create")
            && let Some(index_position) = words.iter().position(|word| word == "index")
            && index_position > 1
            && words[1..index_position]
                .iter()
                .all(|modifier| matches!(modifier.as_str(), "unique" | "global" | "local"))
        {
            words.drain(1..index_position);
        }
        Self::ALL
            .iter()
            .copied()
            .filter(|identity| {
                let expected: Vec<_> = identity
                    .name()
                    .split_ascii_whitespace()
                    .map(str::to_ascii_lowercase)
                    .collect();
                words.len() >= expected.len()
                    && words
                        .iter()
                        .zip(expected)
                        .all(|(actual, expected)| actual == &expected)
            })
            .max_by_key(|identity| identity.name().matches(' ').count())
    }
}

fn leading_words(sql: &str) -> Vec<String> {
    sql.split(|character: char| !character.is_ascii_alphanumeric() && character != '_')
        .filter(|word| !word.is_empty())
        .map(str::to_ascii_lowercase)
        .collect()
}
