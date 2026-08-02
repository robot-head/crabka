use crabka_pgparser::{
    ast::{
        AlterEventTriggerAction, AlterTriggerAction, EventTriggerEvent, Statement,
        TriggerEnableMode, TriggerEvent, TriggerLevel, TriggerTiming,
    },
    command::CommandIdentity,
    parse, parse_with_command_identities,
};

fn one(sql: &str) -> Statement {
    let mut statements = parse(sql).unwrap_or_else(|error| panic!("{sql}: {}", error.message));
    assert_eq!(statements.len(), 1);
    statements.remove(0)
}

fn identity(sql: &str) -> CommandIdentity {
    let mut statements = parse_with_command_identities(sql)
        .unwrap_or_else(|error| panic!("{sql}: {}", error.message));
    assert_eq!(statements.len(), 1);
    statements.remove(0).1
}

#[test]
fn parses_complete_create_trigger_surface() {
    let Statement::CreateTrigger(trigger) = one(
        "CREATE OR REPLACE TRIGGER audit BEFORE INSERT OR UPDATE OF a, b OR DELETE OR TRUNCATE \
         ON public.items REFERENCING OLD TABLE AS old_rows NEW TABLE new_rows FOR EACH ROW \
         WHEN (NEW.a > 0) EXECUTE FUNCTION public.audit_row('one', 'two')",
    ) else {
        panic!("expected CREATE TRIGGER");
    };
    assert!(trigger.or_replace);
    assert!(!trigger.constraint);
    assert_eq!(trigger.name, "audit");
    assert_eq!(trigger.timing, TriggerTiming::Before);
    assert_eq!(
        trigger.events,
        vec![
            TriggerEvent::Insert,
            TriggerEvent::Update {
                columns: vec!["a".into(), "b".into()]
            },
            TriggerEvent::Delete,
            TriggerEvent::Truncate,
        ]
    );
    assert_eq!(trigger.table.schema.as_deref(), Some("public"));
    assert_eq!(trigger.level, TriggerLevel::Row);
    assert_eq!(trigger.transitions.len(), 2);
    assert!(trigger.when.is_some());
    assert_eq!(trigger.function, "public.audit_row");
    assert_eq!(trigger.arguments, vec!["one", "two"]);
}

#[test]
fn parses_constraint_trigger_options() {
    let Statement::CreateTrigger(trigger) = one(
        "CREATE CONSTRAINT TRIGGER fk_check AFTER UPDATE ON child FROM parent \
         DEFERRABLE INITIALLY DEFERRED FOR EACH ROW EXECUTE PROCEDURE check_fk()",
    ) else {
        panic!("expected CREATE TRIGGER");
    };
    assert!(trigger.constraint);
    assert_eq!(trigger.timing, TriggerTiming::After);
    assert_eq!(trigger.referenced_table.unwrap().name, "parent");
    assert!(trigger.deferrable);
    assert!(trigger.initially_deferred);
    assert_eq!(trigger.level, TriggerLevel::Row);
}

#[test]
fn parses_alter_and_drop_trigger() {
    assert!(matches!(
        one("ALTER TRIGGER old ON s.t RENAME TO new"),
        Statement::AlterTrigger {
            action: AlterTriggerAction::RenameTo(name),
            ..
        } if name == "new"
    ));
    assert!(matches!(
        one("ALTER TRIGGER tr ON t NO DEPENDS ON EXTENSION ext"),
        Statement::AlterTrigger {
            action: AlterTriggerAction::DependsOnExtension { dependent: false, extension },
            ..
        } if extension == "ext"
    ));
    assert!(matches!(
        one("DROP TRIGGER IF EXISTS tr ON s.t CASCADE"),
        Statement::DropTrigger {
            if_exists: true,
            cascade: true,
            ..
        }
    ));
}

#[test]
fn parses_event_trigger_surface() {
    let Statement::CreateEventTrigger(trigger) =
        one("CREATE EVENT TRIGGER ddl_audit ON ddl_command_end \
         WHEN TAG IN ('CREATE TABLE', 'ALTER TABLE') \
         EXECUTE FUNCTION public.audit_ddl()")
    else {
        panic!("expected CREATE EVENT TRIGGER");
    };
    assert_eq!(trigger.event, EventTriggerEvent::DdlCommandEnd);
    assert_eq!(trigger.filters[0].variable, "tag");
    assert_eq!(
        trigger.filters[0].values,
        vec!["CREATE TABLE", "ALTER TABLE"]
    );
    assert_eq!(trigger.function, "public.audit_ddl");

    assert!(matches!(
        one("ALTER EVENT TRIGGER ddl_audit ENABLE REPLICA"),
        Statement::AlterEventTrigger {
            action: AlterEventTriggerAction::Enable(TriggerEnableMode::Replica),
            ..
        }
    ));
    assert!(matches!(
        one("ALTER EVENT TRIGGER ddl_audit OWNER TO current_user"),
        Statement::AlterEventTrigger {
            action: AlterEventTriggerAction::OwnerTo(owner),
            ..
        } if owner == "current_user"
    ));
    assert!(matches!(
        one("DROP EVENT TRIGGER IF EXISTS ddl_audit RESTRICT"),
        Statement::DropEventTrigger {
            if_exists: true,
            cascade: false,
            ..
        }
    ));
}

#[test]
fn reports_all_trigger_command_identities() {
    let cases = [
        (
            "CREATE TRIGGER t AFTER INSERT ON x EXECUTE FUNCTION f()",
            CommandIdentity::CreateTrigger,
        ),
        (
            "ALTER TRIGGER t ON x RENAME TO u",
            CommandIdentity::AlterTrigger,
        ),
        ("DROP TRIGGER t ON x", CommandIdentity::DropTrigger),
        (
            "CREATE EVENT TRIGGER e ON login EXECUTE FUNCTION f()",
            CommandIdentity::CreateEventTrigger,
        ),
        (
            "ALTER EVENT TRIGGER e DISABLE",
            CommandIdentity::AlterEventTrigger,
        ),
        ("DROP EVENT TRIGGER e", CommandIdentity::DropEventTrigger),
    ];
    for (sql, expected) in cases {
        assert_eq!(identity(sql), expected, "{sql}");
    }
}

#[test]
fn accepts_simple_trigger_arguments_and_rejects_expressions_and_unknown_events() {
    let Statement::CreateTrigger(trigger) =
        parse("CREATE TRIGGER t AFTER INSERT ON x EXECUTE FUNCTION f(1)")
            .unwrap()
            .remove(0)
    else {
        panic!("expected trigger");
    };
    assert_eq!(trigger.arguments, ["1"]);
    assert!(parse("CREATE TRIGGER t AFTER INSERT ON x EXECUTE FUNCTION f(1 + 2)").is_err());
    assert!(parse("CREATE EVENT TRIGGER e ON nope EXECUTE FUNCTION f()").is_err());
}
