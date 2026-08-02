use crabka_pgexec::SqlEngine;
use crabka_pgwire::engine::{Engine, QueryResult, Session};

async fn exec(engine: &SqlEngine, sql: &str) -> QueryResult {
    engine
        .connect()
        .simple_query(sql)
        .await
        .unwrap_or_else(|error| panic!("{sql}: {error:?}"))
        .remove(0)
}

async fn scalar(engine: &SqlEngine, sql: &str) -> String {
    let QueryResult::Rows { rows, .. } = exec(engine, sql).await else {
        panic!("expected rows");
    };
    String::from_utf8(rows[0][0].as_ref().unwrap().text.to_vec()).unwrap()
}

async fn exec_session(session: &mut crabka_pgexec::SqlSession, sql: &str) -> QueryResult {
    session
        .simple_query(sql)
        .await
        .unwrap_or_else(|error| panic!("{sql}: {error:?}"))
        .remove(0)
}

async fn scalar_session(session: &mut crabka_pgexec::SqlSession, sql: &str) -> String {
    let QueryResult::Rows { rows, .. } = exec_session(session, sql).await else {
        panic!("expected rows");
    };
    String::from_utf8(rows[0][0].as_ref().unwrap().text.to_vec()).unwrap()
}

#[tokio::test]
async fn before_row_trigger_can_modify_and_skip_rows() {
    let engine = SqlEngine::new();
    exec(&engine, "CREATE TABLE items (id int, value text)").await;
    exec(
        &engine,
        "CREATE FUNCTION normalize_item() RETURNS trigger LANGUAGE plpgsql AS $$
         BEGIN
           IF NEW.id < 0 THEN RETURN NULL; END IF;
           NEW.value := TG_OP || ':' || NEW.value;
           RETURN NEW;
         END $$",
    )
    .await;
    exec(
        &engine,
        "CREATE TRIGGER normalize BEFORE INSERT ON items FOR EACH ROW
         EXECUTE FUNCTION normalize_item()",
    )
    .await;
    exec(&engine, "INSERT INTO items VALUES (1, 'ok'), (-1, 'skip')").await;
    assert_eq!(
        scalar(&engine, "SELECT value FROM items").await,
        "INSERT:ok"
    );
}

#[tokio::test]
async fn trigger_when_and_update_of_are_honored() {
    let engine = SqlEngine::new();
    exec(
        &engine,
        "CREATE TABLE items (id int, value text, untouched int)",
    )
    .await;
    exec(
        &engine,
        "CREATE FUNCTION mark_update() RETURNS trigger LANGUAGE plpgsql AS $$
         BEGIN NEW.value := TG_NAME || ':' || NEW.value; RETURN NEW; END $$",
    )
    .await;
    exec(
        &engine,
        "CREATE TRIGGER mark BEFORE UPDATE OF value ON items FOR EACH ROW
         WHEN (NEW.id > 0) EXECUTE FUNCTION mark_update()",
    )
    .await;
    exec(&engine, "INSERT INTO items VALUES (1, 'x', 0)").await;
    exec(&engine, "UPDATE items SET untouched = 1").await;
    assert_eq!(scalar(&engine, "SELECT value FROM items").await, "x");
    exec(&engine, "UPDATE items SET value = 'y'").await;
    assert_eq!(scalar(&engine, "SELECT value FROM items").await, "mark:y");
}

#[tokio::test]
async fn after_row_and_zero_row_statement_triggers_execute_sql() {
    let engine = SqlEngine::new();
    exec(&engine, "CREATE TABLE items (id int)").await;
    exec(&engine, "CREATE TABLE audit (message text)").await;
    exec(
        &engine,
        "CREATE FUNCTION audit_row() RETURNS trigger LANGUAGE plpgsql AS $$
         BEGIN INSERT INTO audit VALUES (TG_OP || ':' || NEW.id::text); RETURN NEW; END $$",
    )
    .await;
    exec(
        &engine,
        "CREATE FUNCTION audit_statement() RETURNS trigger LANGUAGE plpgsql AS $$
         BEGIN INSERT INTO audit VALUES (TG_LEVEL || ':' || TG_OP); RETURN NULL; END $$",
    )
    .await;
    exec(
        &engine,
        "CREATE TRIGGER row_audit AFTER INSERT ON items FOR EACH ROW EXECUTE FUNCTION audit_row()",
    )
    .await;
    exec(
        &engine,
        "CREATE TRIGGER statement_audit AFTER UPDATE ON items FOR EACH STATEMENT
         EXECUTE FUNCTION audit_statement()",
    )
    .await;
    exec(&engine, "INSERT INTO items VALUES (7)").await;
    exec(&engine, "UPDATE items SET id = id WHERE false").await;
    assert_eq!(
        scalar(&engine, "SELECT string_agg(message, ',') FROM audit").await,
        "INSERT:7,STATEMENT:UPDATE"
    );
}

#[tokio::test]
async fn event_triggers_fire_in_order_with_tag_filters() {
    let engine = SqlEngine::new();
    exec(&engine, "CREATE TABLE ddl_audit (message text)").await;
    exec(
        &engine,
        "CREATE FUNCTION audit_ddl() RETURNS event_trigger LANGUAGE plpgsql AS $$
         BEGIN INSERT INTO ddl_audit VALUES (TG_EVENT || ':' || TG_TAG); RETURN NULL; END $$",
    )
    .await;
    exec(
        &engine,
        "CREATE EVENT TRIGGER audit_start ON ddl_command_start
         WHEN TAG IN ('CREATE TABLE') EXECUTE FUNCTION audit_ddl()",
    )
    .await;
    exec(
        &engine,
        "CREATE EVENT TRIGGER audit_end ON ddl_command_end
         WHEN TAG IN ('CREATE TABLE') EXECUTE FUNCTION audit_ddl()",
    )
    .await;
    exec(
        &engine,
        "CREATE EVENT TRIGGER audit_drop ON sql_drop
         WHEN TAG IN ('DROP TABLE') EXECUTE FUNCTION audit_ddl()",
    )
    .await;
    exec(
        &engine,
        "CREATE EVENT TRIGGER audit_fdw ON ddl_command_end
         WHEN TAG IN ('CREATE FOREIGN DATA WRAPPER') EXECUTE FUNCTION audit_ddl()",
    )
    .await;

    exec(&engine, "CREATE TABLE event_target (id int)").await;
    exec(&engine, "DROP TABLE event_target").await;
    exec(&engine, "CREATE ROLE event_shared_role").await;
    exec(&engine, "CREATE FOREIGN DATA WRAPPER event_fdw").await;
    assert_eq!(
        scalar(&engine, "SELECT string_agg(message, ',') FROM ddl_audit").await,
        "ddl_command_start:CREATE TABLE,ddl_command_end:CREATE TABLE,sql_drop:DROP TABLE,ddl_command_end:CREATE FOREIGN DATA WRAPPER"
    );
}

#[tokio::test]
async fn table_rewrite_event_triggers_fire_for_type_rewrites() {
    let engine = SqlEngine::new();
    exec(
        &engine,
        "CREATE TABLE rewrite_audit (message text, relation_oid int, reason int)",
    )
    .await;
    exec(&engine, "CREATE TABLE rewrite_target (id int)").await;
    exec(
        &engine,
        "CREATE FUNCTION audit_rewrite() RETURNS event_trigger LANGUAGE plpgsql AS $$
         BEGIN
           INSERT INTO rewrite_audit VALUES (
             TG_EVENT || ':' || TG_TAG,
             pg_event_trigger_table_rewrite_oid(),
             pg_event_trigger_table_rewrite_reason()
           );
           RETURN NULL;
         END $$",
    )
    .await;
    exec(
        &engine,
        "CREATE EVENT TRIGGER audit_rewrite ON table_rewrite
         WHEN TAG IN ('ALTER TABLE') EXECUTE FUNCTION audit_rewrite()",
    )
    .await;
    exec(
        &engine,
        "ALTER TABLE rewrite_target ALTER COLUMN id TYPE bigint",
    )
    .await;
    assert_eq!(
        scalar(&engine, "SELECT message FROM rewrite_audit").await,
        "table_rewrite:ALTER TABLE"
    );
    assert_eq!(
        scalar(&engine, "SELECT reason FROM rewrite_audit").await,
        "4"
    );
    assert_eq!(
        scalar(
            &engine,
            "SELECT (relation_oid = 'rewrite_target'::regclass)::text FROM rewrite_audit",
        )
        .await,
        "true"
    );
}

#[tokio::test]
async fn ddl_command_end_failure_rolls_back_the_ddl_target() {
    let engine = SqlEngine::new();
    exec(
        &engine,
        "CREATE FUNCTION reject_table() RETURNS event_trigger LANGUAGE plpgsql AS $$
         BEGIN RAISE EXCEPTION 'rejected'; END $$",
    )
    .await;
    exec(
        &engine,
        "CREATE EVENT TRIGGER reject_table ON ddl_command_end
         WHEN TAG IN ('CREATE TABLE') EXECUTE FUNCTION reject_table()",
    )
    .await;
    let error = engine
        .connect()
        .simple_query("CREATE TABLE rejected_target (id int)")
        .await
        .unwrap_err();
    assert!(error.message.contains("rejected"));
    assert_eq!(
        scalar(
            &engine,
            "SELECT count(*) FROM pg_class WHERE relname = 'rejected_target'",
        )
        .await,
        "0"
    );
}

#[tokio::test]
async fn event_trigger_helper_srfs_expose_command_and_drop_objects() {
    let engine = SqlEngine::new();
    exec(
        &engine,
        "CREATE TABLE event_command_audit (tag text, identity text)",
    )
    .await;
    exec(
        &engine,
        "CREATE TABLE event_drop_audit (kind text, name text, identity text)",
    )
    .await;
    exec(
        &engine,
        "CREATE FUNCTION audit_event_command() RETURNS event_trigger LANGUAGE plpgsql AS $$
         BEGIN
           INSERT INTO event_command_audit
           SELECT command_tag, object_identity FROM pg_event_trigger_ddl_commands();
           RETURN NULL;
         END $$",
    )
    .await;
    exec(
        &engine,
        "CREATE FUNCTION audit_event_drop() RETURNS event_trigger LANGUAGE plpgsql AS $$
         BEGIN
           INSERT INTO event_drop_audit
           SELECT object_type, object_name, object_identity
           FROM pg_event_trigger_dropped_objects();
           RETURN NULL;
         END $$",
    )
    .await;
    exec(
        &engine,
        "CREATE EVENT TRIGGER audit_event_command ON ddl_command_end
         WHEN TAG IN ('CREATE TABLE') EXECUTE FUNCTION audit_event_command()",
    )
    .await;
    exec(
        &engine,
        "CREATE EVENT TRIGGER audit_event_drop ON sql_drop
         WHEN TAG IN ('DROP TABLE') EXECUTE FUNCTION audit_event_drop()",
    )
    .await;
    exec(&engine, "CREATE TABLE helper_target (id int)").await;
    exec(&engine, "DROP TABLE helper_target").await;
    assert_eq!(
        scalar(
            &engine,
            "SELECT tag || ':' || identity FROM event_command_audit",
        )
        .await,
        "CREATE TABLE:public.helper_target"
    );
    assert_eq!(
        scalar(
            &engine,
            "SELECT kind || ':' || name || ':' || identity FROM event_drop_audit",
        )
        .await,
        "table:helper_target:public.helper_target"
    );
}

#[tokio::test]
async fn sql_drop_reports_cascaded_views_triggers_and_constraints() {
    let engine = SqlEngine::new();
    exec(
        &engine,
        "CREATE TABLE cascade_drop_audit (kind text, name text)",
    )
    .await;
    exec(
        &engine,
        "CREATE FUNCTION audit_cascade_drop() RETURNS event_trigger LANGUAGE plpgsql AS $$
         BEGIN
           INSERT INTO cascade_drop_audit
           SELECT object_type, object_name FROM pg_event_trigger_dropped_objects();
           RETURN NULL;
         END $$",
    )
    .await;
    exec(
        &engine,
        "CREATE EVENT TRIGGER audit_cascade_drop ON sql_drop
         WHEN TAG IN ('DROP TABLE') EXECUTE FUNCTION audit_cascade_drop()",
    )
    .await;
    exec(&engine, "CREATE TABLE cascade_base (id int PRIMARY KEY)").await;
    exec(
        &engine,
        "CREATE TABLE cascade_ref (id int REFERENCES cascade_base(id))",
    )
    .await;
    exec(
        &engine,
        "CREATE VIEW cascade_view AS SELECT id FROM cascade_base",
    )
    .await;
    exec(
        &engine,
        "CREATE FUNCTION cascade_view_insert() RETURNS trigger LANGUAGE plpgsql AS $$
         BEGIN RETURN NEW; END $$",
    )
    .await;
    exec(
        &engine,
        "CREATE TRIGGER cascade_view_trigger INSTEAD OF INSERT ON cascade_view
         FOR EACH ROW EXECUTE FUNCTION cascade_view_insert()",
    )
    .await;
    exec(&engine, "DROP TABLE cascade_base CASCADE").await;
    for (kind, name) in [
        ("table", "cascade_base"),
        ("view", "cascade_view"),
        ("trigger", "cascade_view_trigger"),
    ] {
        assert_eq!(
            scalar(
                &engine,
                &format!(
                    "SELECT count(*) FROM cascade_drop_audit WHERE kind = '{kind}' AND name = '{name}'"
                ),
            )
            .await,
            "1"
        );
    }
    assert_eq!(
        scalar(
            &engine,
            "SELECT count(*) FROM cascade_drop_audit WHERE kind = 'table constraint'",
        )
        .await,
        "1"
    );
}

#[tokio::test]
async fn event_trigger_catalog_reports_transferred_owner() {
    let engine = SqlEngine::new();
    exec(&engine, "CREATE ROLE event_trigger_owner").await;
    exec(
        &engine,
        "CREATE FUNCTION owned_event_trigger_fn() RETURNS event_trigger LANGUAGE plpgsql AS $$
         BEGIN RETURN NULL; END $$",
    )
    .await;
    exec(
        &engine,
        "CREATE EVENT TRIGGER owned_event_trigger ON ddl_command_end
         EXECUTE FUNCTION owned_event_trigger_fn()",
    )
    .await;
    exec(
        &engine,
        "ALTER EVENT TRIGGER owned_event_trigger OWNER TO event_trigger_owner",
    )
    .await;
    assert_eq!(
        scalar(
            &engine,
            "SELECT rolname FROM pg_event_trigger JOIN pg_roles ON pg_roles.oid = evtowner
             WHERE evtname = 'owned_event_trigger'",
        )
        .await,
        "event_trigger_owner"
    );
}

#[tokio::test]
async fn trigger_arguments_rows_and_event_tags_are_validated() {
    let engine = SqlEngine::new();
    exec(&engine, "CREATE TABLE validated_trigger_rows (id int)").await;
    exec(
        &engine,
        "CREATE FUNCTION use_trigger_argument() RETURNS trigger LANGUAGE plpgsql AS $$
         BEGIN NEW.id := TG_ARGV[0]::int; RETURN NEW; END $$",
    )
    .await;
    exec(
        &engine,
        "CREATE TRIGGER use_arg BEFORE INSERT ON validated_trigger_rows
         FOR EACH ROW EXECUTE FUNCTION use_trigger_argument('7')",
    )
    .await;
    exec(&engine, "INSERT INTO validated_trigger_rows VALUES (1)").await;
    assert_eq!(
        scalar(&engine, "SELECT id FROM validated_trigger_rows").await,
        "7"
    );

    exec(
        &engine,
        "CREATE FUNCTION bad_trigger_row() RETURNS trigger LANGUAGE plpgsql AS $$
         BEGIN RETURN ROW('not-an-int'); END $$",
    )
    .await;
    exec(
        &engine,
        "CREATE TRIGGER bad_row BEFORE UPDATE ON validated_trigger_rows
         FOR EACH ROW EXECUTE FUNCTION bad_trigger_row()",
    )
    .await;
    let error = engine
        .connect()
        .simple_query("UPDATE validated_trigger_rows SET id = 8")
        .await
        .unwrap_err();
    assert_eq!(error.code, "42804");
    assert_eq!(
        scalar(&engine, "SELECT id FROM validated_trigger_rows").await,
        "7"
    );

    let error = engine
        .connect()
        .simple_query(
            "CREATE TRIGGER duplicate_alias AFTER UPDATE ON validated_trigger_rows
             REFERENCING OLD TABLE AS changed NEW TABLE AS changed
             FOR EACH STATEMENT EXECUTE FUNCTION use_trigger_argument()",
        )
        .await
        .unwrap_err();
    assert_eq!(error.code, "42601");

    exec(&engine, "CREATE TABLE sequence_tag_audit (tag text)").await;
    exec(
        &engine,
        "CREATE FUNCTION audit_sequence_tag() RETURNS event_trigger LANGUAGE plpgsql AS $$
         BEGIN INSERT INTO sequence_tag_audit VALUES (TG_TAG); RETURN NULL; END $$",
    )
    .await;
    for sql in [
        "CREATE EVENT TRIGGER audit_create_sequence ON ddl_command_end
         WHEN TAG IN ('CREATE SEQUENCE') EXECUTE FUNCTION audit_sequence_tag()",
        "CREATE EVENT TRIGGER audit_drop_sequence ON ddl_command_end
         WHEN TAG IN ('DROP SEQUENCE') EXECUTE FUNCTION audit_sequence_tag()",
    ] {
        exec(&engine, sql).await;
    }
    let error = engine
        .connect()
        .simple_query(
            "CREATE EVENT TRIGGER bad_tag ON ddl_command_end
             WHEN TAG IN ('CREATE TABEL') EXECUTE FUNCTION audit_sequence_tag()",
        )
        .await
        .unwrap_err();
    assert_eq!(error.code, "22023");
    exec(&engine, "CREATE SEQUENCE trigger_sequence").await;
    exec(&engine, "DROP SEQUENCE trigger_sequence").await;
    assert_eq!(
        scalar(
            &engine,
            "SELECT string_agg(tag, ',') FROM sequence_tag_audit"
        )
        .await,
        "CREATE SEQUENCE,DROP SEQUENCE"
    );
}

#[tokio::test]
async fn trigger_catalog_and_definition_are_visible() {
    let engine = SqlEngine::new();
    exec(&engine, "CREATE TABLE documented (id int, value text)").await;
    exec(
        &engine,
        "CREATE FUNCTION documented_fn() RETURNS trigger LANGUAGE plpgsql AS $$
         BEGIN RETURN NEW; END $$",
    )
    .await;
    exec(
        &engine,
        "CREATE TRIGGER documented_trigger BEFORE UPDATE OF value ON documented
         FOR EACH ROW WHEN (NEW.id > 0) EXECUTE FUNCTION documented_fn('argument')",
    )
    .await;
    let definition = scalar(
        &engine,
        "SELECT pg_get_triggerdef(oid) FROM pg_trigger WHERE tgname = 'documented_trigger'",
    )
    .await;
    assert!(definition.contains("CREATE TRIGGER documented_trigger BEFORE UPDATE OF value"));
    assert!(definition.contains("WHEN (NEW.id > 0)"));
    assert!(definition.contains("EXECUTE FUNCTION documented_fn('argument')"));
    exec(
        &engine,
        "ALTER FUNCTION documented_fn() RENAME TO documented_fn_renamed",
    )
    .await;
    assert!(
        scalar(
            &engine,
            "SELECT pg_get_triggerdef(oid) FROM pg_trigger WHERE tgname = 'documented_trigger'",
        )
        .await
        .contains("EXECUTE FUNCTION documented_fn_renamed('argument')")
    );
    assert_eq!(
        scalar(
            &engine,
            "SELECT action_statement FROM information_schema.triggers
             WHERE trigger_name = 'documented_trigger'",
        )
        .await,
        "EXECUTE FUNCTION documented_fn_renamed('argument')"
    );
    assert_eq!(
        scalar(
            &engine,
            "SELECT relhastriggers::text FROM pg_class WHERE relname = 'documented'",
        )
        .await,
        "true"
    );
    assert_eq!(
        scalar(
            &engine,
            "SELECT count(*) FROM pg_depend
             WHERE objid = (SELECT oid FROM pg_trigger WHERE tgname = 'documented_trigger')",
        )
        .await,
        "2"
    );
}

#[tokio::test]
async fn on_conflict_fires_insert_and_update_trigger_classes() {
    let engine = SqlEngine::new();
    exec(
        &engine,
        "CREATE TABLE upserted (id int PRIMARY KEY, value text)",
    )
    .await;
    exec(&engine, "CREATE TABLE upsert_audit (message text)").await;
    exec(
        &engine,
        "CREATE FUNCTION audit_upsert() RETURNS trigger LANGUAGE plpgsql AS $$
         BEGIN
           INSERT INTO upsert_audit VALUES (TG_WHEN || ':' || TG_LEVEL || ':' || TG_OP);
           IF TG_LEVEL = 'ROW' THEN RETURN NEW; END IF;
           RETURN NULL;
         END $$",
    )
    .await;
    for sql in [
        "CREATE TRIGGER a_bsi BEFORE INSERT ON upserted FOR EACH STATEMENT EXECUTE FUNCTION audit_upsert()",
        "CREATE TRIGGER a_bsu BEFORE UPDATE ON upserted FOR EACH STATEMENT EXECUTE FUNCTION audit_upsert()",
        "CREATE TRIGGER a_bri BEFORE INSERT ON upserted FOR EACH ROW EXECUTE FUNCTION audit_upsert()",
        "CREATE TRIGGER a_bru BEFORE UPDATE ON upserted FOR EACH ROW EXECUTE FUNCTION audit_upsert()",
        "CREATE TRIGGER a_aru AFTER UPDATE ON upserted FOR EACH ROW EXECUTE FUNCTION audit_upsert()",
        "CREATE TRIGGER a_asu AFTER UPDATE ON upserted FOR EACH STATEMENT EXECUTE FUNCTION audit_upsert()",
        "CREATE TRIGGER a_asi AFTER INSERT ON upserted FOR EACH STATEMENT EXECUTE FUNCTION audit_upsert()",
    ] {
        exec(&engine, sql).await;
    }
    exec(&engine, "INSERT INTO upserted VALUES (1, 'old')").await;
    exec(&engine, "TRUNCATE upsert_audit").await;
    exec(
        &engine,
        "INSERT INTO upserted VALUES (1, 'new')
         ON CONFLICT (id) DO UPDATE SET value = excluded.value",
    )
    .await;
    assert_eq!(
        scalar(&engine, "SELECT string_agg(message, ',') FROM upsert_audit",).await,
        "BEFORE:STATEMENT:INSERT,BEFORE:STATEMENT:UPDATE,BEFORE:ROW:INSERT,BEFORE:ROW:UPDATE,AFTER:ROW:UPDATE,AFTER:STATEMENT:UPDATE,AFTER:STATEMENT:INSERT"
    );
}

#[tokio::test]
async fn after_trigger_sees_staged_row_and_reports_depth() {
    let engine = SqlEngine::new();
    exec(&engine, "CREATE TABLE visible_rows (id int)").await;
    exec(&engine, "CREATE TABLE visibility_audit (message text)").await;
    exec(
        &engine,
        "CREATE FUNCTION inspect_after_row() RETURNS trigger LANGUAGE plpgsql AS $$
         BEGIN
           INSERT INTO visibility_audit
           SELECT count(*)::text FROM visible_rows;
           INSERT INTO visibility_audit VALUES (pg_trigger_depth()::text);
           RETURN NEW;
         END $$",
    )
    .await;
    exec(
        &engine,
        "CREATE TRIGGER inspect_after AFTER INSERT ON visible_rows
         FOR EACH ROW EXECUTE FUNCTION inspect_after_row()",
    )
    .await;
    exec(&engine, "INSERT INTO visible_rows VALUES (1)").await;
    assert_eq!(
        scalar(
            &engine,
            "SELECT string_agg(message, ',') FROM visibility_audit"
        )
        .await,
        "1,1"
    );
}

#[tokio::test]
async fn login_event_and_information_schema_are_exposed() {
    let engine = SqlEngine::new();
    exec(&engine, "CREATE TABLE login_audit (seen int)").await;
    exec(
        &engine,
        "CREATE FUNCTION audit_login() RETURNS event_trigger LANGUAGE plpgsql AS $$
         BEGIN INSERT INTO login_audit VALUES (1); RETURN NULL; END $$",
    )
    .await;
    exec(
        &engine,
        "CREATE EVENT TRIGGER audit_login_event ON login EXECUTE FUNCTION audit_login()",
    )
    .await;
    assert_eq!(
        scalar(&engine, "SELECT count(*) FROM login_audit").await,
        "1"
    );

    exec(&engine, "CREATE TABLE schema_target (id int, value text)").await;
    exec(
        &engine,
        "CREATE FUNCTION schema_trigger_fn() RETURNS trigger LANGUAGE plpgsql AS $$
         BEGIN RETURN NEW; END $$",
    )
    .await;
    exec(
        &engine,
        "CREATE TRIGGER schema_trigger BEFORE UPDATE OF value ON schema_target
         FOR EACH ROW EXECUTE FUNCTION schema_trigger_fn()",
    )
    .await;
    assert_eq!(
        scalar(
            &engine,
            "SELECT event_manipulation || ':' || action_timing || ':' || action_orientation
             FROM information_schema.triggers WHERE trigger_name = 'schema_trigger'",
        )
        .await,
        "UPDATE:BEFORE:ROW"
    );
    assert_eq!(
        scalar(
            &engine,
            "SELECT event_object_column FROM information_schema.triggered_update_columns
             WHERE trigger_name = 'schema_trigger'",
        )
        .await,
        "value"
    );
}

#[tokio::test]
async fn login_event_can_reject_connection_startup() {
    let engine = SqlEngine::new();
    exec(
        &engine,
        "CREATE FUNCTION reject_login() RETURNS event_trigger LANGUAGE plpgsql AS $$
         BEGIN RAISE EXCEPTION 'login rejected'; END $$",
    )
    .await;
    exec(
        &engine,
        "CREATE EVENT TRIGGER reject_login_event ON login EXECUTE FUNCTION reject_login()",
    )
    .await;
    let mut session = engine.connect();
    let error = session.startup().await.unwrap_err();
    assert!(error.message.contains("login rejected"));
}

#[tokio::test]
async fn merge_actions_fire_their_row_trigger_classes() {
    let engine = SqlEngine::new();
    exec(
        &engine,
        "CREATE TABLE merge_target (id int PRIMARY KEY, value text)",
    )
    .await;
    exec(&engine, "CREATE TABLE merge_source (id int, value text)").await;
    exec(&engine, "CREATE TABLE merge_audit (message text)").await;
    exec(
        &engine,
        "CREATE FUNCTION audit_merge() RETURNS trigger LANGUAGE plpgsql AS $$
         BEGIN
           INSERT INTO merge_audit VALUES (TG_OP);
           IF TG_OP = 'DELETE' THEN RETURN OLD; END IF;
           RETURN NEW;
         END $$",
    )
    .await;
    for sql in [
        "CREATE TRIGGER merge_insert BEFORE INSERT ON merge_target FOR EACH ROW EXECUTE FUNCTION audit_merge()",
        "CREATE TRIGGER merge_update BEFORE UPDATE ON merge_target FOR EACH ROW EXECUTE FUNCTION audit_merge()",
        "CREATE TRIGGER merge_delete BEFORE DELETE ON merge_target FOR EACH ROW EXECUTE FUNCTION audit_merge()",
    ] {
        exec(&engine, sql).await;
    }
    exec(
        &engine,
        "INSERT INTO merge_target VALUES (1, 'old'), (3, 'gone')",
    )
    .await;
    exec(
        &engine,
        "INSERT INTO merge_source VALUES (1, 'new'), (2, 'added')",
    )
    .await;
    exec(&engine, "TRUNCATE merge_audit").await;
    exec(
        &engine,
        "MERGE INTO merge_target AS t USING merge_source AS s ON t.id = s.id
         WHEN MATCHED THEN UPDATE SET value = s.value
         WHEN NOT MATCHED THEN INSERT (id, value) VALUES (s.id, s.value)
         WHEN NOT MATCHED BY SOURCE THEN DELETE",
    )
    .await;
    assert_eq!(
        scalar(&engine, "SELECT string_agg(message, ',') FROM merge_audit").await,
        "UPDATE,INSERT,DELETE"
    );
}

#[tokio::test]
async fn autocommit_trigger_side_effects_are_atomic() {
    let engine = SqlEngine::new();
    exec(&engine, "CREATE TABLE atomic_rows (id int PRIMARY KEY)").await;
    exec(&engine, "CREATE TABLE atomic_audit (message text)").await;
    exec(
        &engine,
        "CREATE FUNCTION atomic_before() RETURNS trigger LANGUAGE plpgsql AS $$
         BEGIN INSERT INTO atomic_audit VALUES ('before:' || NEW.id::text); RETURN NEW; END $$",
    )
    .await;
    exec(
        &engine,
        "CREATE TRIGGER atomic_before BEFORE INSERT ON atomic_rows
         FOR EACH ROW EXECUTE FUNCTION atomic_before()",
    )
    .await;
    assert!(
        engine
            .connect()
            .simple_query("INSERT INTO atomic_rows VALUES (1), (1)")
            .await
            .is_err()
    );
    assert_eq!(
        scalar(&engine, "SELECT count(*) FROM atomic_rows").await,
        "0"
    );
    assert_eq!(
        scalar(&engine, "SELECT count(*) FROM atomic_audit").await,
        "0"
    );

    exec(
        &engine,
        "CREATE FUNCTION atomic_after() RETURNS trigger LANGUAGE plpgsql AS $$
         BEGIN
           INSERT INTO atomic_audit VALUES ('after:' || NEW.id::text);
           RAISE EXCEPTION 'reject after trigger';
         END $$",
    )
    .await;
    exec(
        &engine,
        "CREATE TRIGGER atomic_after AFTER INSERT ON atomic_rows
         FOR EACH ROW EXECUTE FUNCTION atomic_after()",
    )
    .await;
    assert!(
        engine
            .connect()
            .simple_query("INSERT INTO atomic_rows VALUES (2)")
            .await
            .is_err()
    );
    assert_eq!(
        scalar(&engine, "SELECT count(*) FROM atomic_rows").await,
        "0"
    );
    assert_eq!(
        scalar(&engine, "SELECT count(*) FROM atomic_audit").await,
        "0"
    );
}

#[tokio::test]
async fn constraint_trigger_deferral_obeys_set_constraints() {
    let engine = SqlEngine::new();
    exec(&engine, "CREATE TABLE deferred_target (id int)").await;
    exec(&engine, "CREATE TABLE deferred_audit (id int)").await;
    exec(
        &engine,
        "CREATE FUNCTION audit_deferred() RETURNS trigger LANGUAGE plpgsql AS $$
         BEGIN INSERT INTO deferred_audit VALUES (NEW.id); RETURN NEW; END $$",
    )
    .await;
    exec(
        &engine,
        "CREATE CONSTRAINT TRIGGER deferred_check AFTER INSERT ON deferred_target
         DEFERRABLE INITIALLY DEFERRED FOR EACH ROW EXECUTE FUNCTION audit_deferred()",
    )
    .await;

    let mut session = engine.connect();
    exec_session(&mut session, "BEGIN").await;
    exec_session(&mut session, "INSERT INTO deferred_target VALUES (7)").await;
    assert_eq!(
        scalar_session(&mut session, "SELECT count(*) FROM deferred_audit").await,
        "0"
    );
    exec_session(&mut session, "SET CONSTRAINTS deferred_check IMMEDIATE").await;
    assert_eq!(
        scalar_session(&mut session, "SELECT id FROM deferred_audit").await,
        "7"
    );
    exec_session(&mut session, "COMMIT").await;
    exec(&engine, "INSERT INTO deferred_target VALUES (8)").await;
    assert_eq!(
        scalar(&engine, "SELECT count(*) FROM deferred_audit").await,
        "2"
    );
}

#[tokio::test]
async fn transition_tables_contain_the_whole_statement() {
    let engine = SqlEngine::new();
    exec(
        &engine,
        "CREATE TABLE transition_target (id int, value text)",
    )
    .await;
    exec(
        &engine,
        "CREATE TABLE transition_audit (old_count int, new_count int)",
    )
    .await;
    exec(
        &engine,
        "CREATE FUNCTION audit_transition() RETURNS trigger LANGUAGE plpgsql AS $$
         BEGIN
           INSERT INTO transition_audit
           SELECT (SELECT count(*) FROM old_rows), (SELECT count(*) FROM new_rows);
           RETURN NULL;
         END $$",
    )
    .await;
    exec(
        &engine,
        "CREATE TRIGGER transition_update AFTER UPDATE ON transition_target
         REFERENCING OLD TABLE AS old_rows NEW TABLE AS new_rows
         FOR EACH STATEMENT EXECUTE FUNCTION audit_transition()",
    )
    .await;
    exec(
        &engine,
        "INSERT INTO transition_target VALUES (1, 'a'), (2, 'b')",
    )
    .await;
    exec(&engine, "UPDATE transition_target SET value = 'changed'").await;
    assert_eq!(
        scalar(
            &engine,
            "SELECT old_count::text || ':' || new_count::text FROM transition_audit",
        )
        .await,
        "2:2"
    );
}

#[tokio::test]
async fn cte_and_partition_parent_statement_triggers_fire() {
    let engine = SqlEngine::new();
    exec(&engine, "CREATE TABLE cte_trigger_source (id int)").await;
    exec(&engine, "CREATE TABLE cte_trigger_sink (id int)").await;
    exec(
        &engine,
        "CREATE TABLE statement_trigger_audit (message text)",
    )
    .await;
    exec(
        &engine,
        "CREATE FUNCTION audit_statement_target() RETURNS trigger LANGUAGE plpgsql AS $$
         BEGIN INSERT INTO statement_trigger_audit VALUES (TG_TABLE_NAME || ':' || TG_OP); RETURN NULL; END $$",
    )
    .await;
    exec(
        &engine,
        "CREATE TRIGGER cte_update AFTER UPDATE ON cte_trigger_source
         FOR EACH STATEMENT EXECUTE FUNCTION audit_statement_target()",
    )
    .await;
    exec(&engine, "INSERT INTO cte_trigger_source VALUES (1)").await;
    exec(
        &engine,
        "WITH changed AS (UPDATE cte_trigger_source SET id = 2 RETURNING id)
         INSERT INTO cte_trigger_sink SELECT id FROM changed",
    )
    .await;
    assert_eq!(
        scalar(&engine, "SELECT message FROM statement_trigger_audit").await,
        "cte_trigger_source:UPDATE"
    );

    exec(
        &engine,
        "CREATE TABLE transition_parent (id int, value text) PARTITION BY RANGE (id)",
    )
    .await;
    exec(
        &engine,
        "CREATE TABLE transition_leaf PARTITION OF transition_parent FOR VALUES FROM (0) TO (10)",
    )
    .await;
    exec(&engine, "CREATE TABLE parent_transition_audit (rows int)").await;
    exec(
        &engine,
        "CREATE FUNCTION audit_parent_transition() RETURNS trigger LANGUAGE plpgsql AS $$
         BEGIN INSERT INTO parent_transition_audit SELECT count(*) FROM changed; RETURN NULL; END $$",
    )
    .await;
    exec(
        &engine,
        "CREATE TRIGGER parent_transition AFTER UPDATE ON transition_parent
         REFERENCING NEW TABLE AS changed FOR EACH STATEMENT
         EXECUTE FUNCTION audit_parent_transition()",
    )
    .await;
    exec(&engine, "INSERT INTO transition_parent VALUES (1, 'old')").await;
    exec(&engine, "UPDATE transition_parent SET value = 'new'").await;
    assert_eq!(
        scalar(&engine, "SELECT rows FROM parent_transition_audit").await,
        "1"
    );
}

#[tokio::test]
async fn builtin_tsvector_update_trigger_populates_the_target_column() {
    let engine = SqlEngine::new();
    exec(
        &engine,
        "CREATE TABLE trigger_document (search tsvector, title text, body text)",
    )
    .await;
    exec(
        &engine,
        "CREATE TRIGGER document_search BEFORE INSERT OR UPDATE ON trigger_document
         FOR EACH ROW EXECUTE FUNCTION tsvector_update_trigger(search, 'pg_catalog.simple', title, body)",
    )
    .await;
    exec(
        &engine,
        "INSERT INTO trigger_document(title, body) VALUES ('cat', 'dog')",
    )
    .await;
    assert_eq!(
        scalar(&engine, "SELECT search::text FROM trigger_document").await,
        "'cat':1 'dog':2"
    );
}

#[tokio::test]
async fn instead_of_view_triggers_route_row_changes() {
    let engine = SqlEngine::new();
    exec(&engine, "CREATE TABLE view_base (id int, value text)").await;
    exec(
        &engine,
        "CREATE VIEW trigger_view AS SELECT id, value FROM view_base",
    )
    .await;
    exec(
        &engine,
        "CREATE FUNCTION view_insert() RETURNS trigger LANGUAGE plpgsql AS $$
         BEGIN
           INSERT INTO view_base VALUES (NEW.id, NEW.value);
           RETURN NEW;
         END $$",
    )
    .await;
    exec(
        &engine,
        "CREATE TRIGGER view_insert INSTEAD OF INSERT ON trigger_view
         FOR EACH ROW EXECUTE FUNCTION view_insert()",
    )
    .await;
    assert_eq!(
        scalar(
            &engine,
            "INSERT INTO trigger_view VALUES (7, 'routed') RETURNING id::text || ':' || value",
        )
        .await,
        "7:routed"
    );
    assert_eq!(
        scalar(&engine, "SELECT id::text || ':' || value FROM view_base",).await,
        "7:routed"
    );
    exec(
        &engine,
        "CREATE FUNCTION view_update() RETURNS trigger LANGUAGE plpgsql AS $$
         BEGIN
           UPDATE view_base SET id = NEW.id, value = NEW.value WHERE id = OLD.id;
           RETURN NEW;
         END $$",
    )
    .await;
    exec(
        &engine,
        "CREATE FUNCTION view_delete() RETURNS trigger LANGUAGE plpgsql AS $$
         BEGIN
           IF NEW IS NOT NULL THEN RAISE EXCEPTION 'NEW must be null'; END IF;
           DELETE FROM view_base WHERE id = OLD.id;
           RETURN OLD;
         END $$",
    )
    .await;
    exec(
        &engine,
        "CREATE TRIGGER view_update INSTEAD OF UPDATE ON trigger_view
         FOR EACH ROW EXECUTE FUNCTION view_update()",
    )
    .await;
    exec(
        &engine,
        "CREATE TRIGGER view_delete INSTEAD OF DELETE ON trigger_view
         FOR EACH ROW EXECUTE FUNCTION view_delete()",
    )
    .await;
    assert_eq!(
        scalar(
            &engine,
            "SELECT is_trigger_insertable_into || ':' || is_trigger_updatable || ':' || is_trigger_deletable
             FROM information_schema.views WHERE table_name = 'trigger_view'",
        )
        .await,
        "YES:YES:YES"
    );
    assert_eq!(
        scalar(
            &engine,
            "UPDATE trigger_view SET value = 'changed' WHERE id = 7 RETURNING value",
        )
        .await,
        "changed"
    );
    assert_eq!(
        scalar(
            &engine,
            "DELETE FROM trigger_view WHERE id = 7 RETURNING value",
        )
        .await,
        "changed"
    );
    assert_eq!(scalar(&engine, "SELECT count(*) FROM view_base").await, "0");
    exec(&engine, "DROP VIEW trigger_view").await;
    assert_eq!(
        scalar(
            &engine,
            "SELECT count(*) FROM pg_trigger WHERE tgname LIKE 'view_%'",
        )
        .await,
        "0"
    );
}

#[tokio::test]
async fn partition_trigger_clones_are_row_level_only() {
    let engine = SqlEngine::new();
    exec(&engine, "CREATE TABLE partition_audit (name text)").await;
    exec(
        &engine,
        "CREATE TABLE trigger_parent (id int) PARTITION BY RANGE (id)",
    )
    .await;
    exec(
        &engine,
        "CREATE TABLE trigger_leaf PARTITION OF trigger_parent FOR VALUES FROM (0) TO (10)",
    )
    .await;
    exec(
        &engine,
        "CREATE FUNCTION audit_partition_trigger() RETURNS trigger LANGUAGE plpgsql AS $$
         BEGIN INSERT INTO partition_audit VALUES (TG_NAME); RETURN NEW; END $$",
    )
    .await;
    exec(
        &engine,
        "CREATE TRIGGER parent_row AFTER INSERT ON trigger_parent
         FOR EACH ROW EXECUTE FUNCTION audit_partition_trigger()",
    )
    .await;
    exec(
        &engine,
        "CREATE TRIGGER parent_statement AFTER INSERT ON trigger_parent
         FOR EACH STATEMENT EXECUTE FUNCTION audit_partition_trigger()",
    )
    .await;
    assert_eq!(
        scalar(
            &engine,
            "SELECT count(*) FROM pg_trigger WHERE tgparentid <> 0"
        )
        .await,
        "1"
    );
    exec(&engine, "INSERT INTO trigger_parent VALUES (1)").await;
    assert_eq!(
        scalar(&engine, "SELECT string_agg(name, ',') FROM partition_audit").await,
        "parent_row,parent_statement"
    );

    exec(
        &engine,
        "CREATE TABLE nested_parent (id int) PARTITION BY RANGE (id)",
    )
    .await;
    exec(
        &engine,
        "CREATE TABLE nested_middle PARTITION OF nested_parent
         FOR VALUES FROM (0) TO (100) PARTITION BY RANGE (id)",
    )
    .await;
    exec(
        &engine,
        "CREATE TRIGGER nested_row AFTER INSERT ON nested_parent
         FOR EACH ROW EXECUTE FUNCTION audit_partition_trigger()",
    )
    .await;
    exec(
        &engine,
        "CREATE TABLE nested_leaf PARTITION OF nested_middle FOR VALUES FROM (0) TO (10)",
    )
    .await;
    exec(
        &engine,
        "ALTER TRIGGER nested_row ON nested_parent RENAME TO nested_renamed",
    )
    .await;
    assert_eq!(
        scalar(
            &engine,
            "SELECT count(*) FROM pg_trigger WHERE tgname = 'nested_renamed'",
        )
        .await,
        "3"
    );
    exec(
        &engine,
        "ALTER TABLE nested_parent DISABLE TRIGGER nested_renamed",
    )
    .await;
    exec(&engine, "INSERT INTO nested_parent VALUES (1)").await;
    assert_eq!(
        scalar(
            &engine,
            "SELECT count(*) FROM partition_audit WHERE name = 'nested_renamed'",
        )
        .await,
        "0"
    );
    exec(&engine, "DROP TRIGGER nested_renamed ON nested_parent").await;
    assert_eq!(
        scalar(
            &engine,
            "SELECT count(*) FROM pg_trigger WHERE tgname = 'nested_renamed'",
        )
        .await,
        "0"
    );
}

#[tokio::test]
async fn foreign_key_actions_fire_child_statement_triggers() {
    let engine = SqlEngine::new();
    exec(&engine, "CREATE TABLE fk_trigger_audit (message text)").await;
    exec(
        &engine,
        "CREATE TABLE fk_trigger_parent (id int PRIMARY KEY)",
    )
    .await;
    exec(
        &engine,
        "CREATE TABLE fk_trigger_child (
           id int,
           parent_id int REFERENCES fk_trigger_parent(id) ON DELETE CASCADE
         )",
    )
    .await;
    exec(
        &engine,
        "CREATE FUNCTION audit_fk_statement() RETURNS trigger LANGUAGE plpgsql AS $$
         BEGIN
           INSERT INTO fk_trigger_audit VALUES (TG_WHEN || ':' || TG_OP);
           RETURN NULL;
         END $$",
    )
    .await;
    exec(
        &engine,
        "CREATE TRIGGER fk_before BEFORE DELETE ON fk_trigger_child
         FOR EACH STATEMENT EXECUTE FUNCTION audit_fk_statement()",
    )
    .await;
    exec(
        &engine,
        "CREATE TRIGGER fk_after AFTER DELETE ON fk_trigger_child
         FOR EACH STATEMENT EXECUTE FUNCTION audit_fk_statement()",
    )
    .await;
    exec(&engine, "INSERT INTO fk_trigger_parent VALUES (1)").await;
    exec(
        &engine,
        "INSERT INTO fk_trigger_child VALUES (1, 1), (2, 1)",
    )
    .await;
    exec(&engine, "DELETE FROM fk_trigger_parent WHERE id = 1").await;
    assert_eq!(
        scalar(
            &engine,
            "SELECT string_agg(message, ',') FROM fk_trigger_audit",
        )
        .await,
        "BEFORE:DELETE,AFTER:DELETE"
    );
}

#[tokio::test]
async fn disable_trigger_all_rejects_tables_with_foreign_keys() {
    let engine = SqlEngine::new();
    exec(
        &engine,
        "CREATE TABLE disable_fk_parent (id int PRIMARY KEY)",
    )
    .await;
    exec(
        &engine,
        "CREATE TABLE disable_fk_child (parent_id int REFERENCES disable_fk_parent(id))",
    )
    .await;
    let error = engine
        .connect()
        .simple_query("ALTER TABLE disable_fk_child DISABLE TRIGGER ALL")
        .await
        .unwrap_err();
    assert_eq!(error.code, "0A000");
    assert!(
        engine
            .connect()
            .simple_query("INSERT INTO disable_fk_child VALUES (1)")
            .await
            .is_err()
    );
}

#[tokio::test]
async fn sharded_timestamp_writes_fire_row_and_statement_triggers() {
    let mut engine = SqlEngine::new();
    engine.init_gtm_coordinator().expect("gtm");
    let mut session = engine.connect();
    exec_session(
        &mut session,
        "CREATE TABLE sharded_items (id int, value text)
         SHARDED BY HASH (id) BUCKETS 16",
    )
    .await;
    exec_session(&mut session, "CREATE TABLE sharded_audit (message text)").await;
    exec_session(
        &mut session,
        "CREATE FUNCTION prepare_sharded_item() RETURNS trigger LANGUAGE plpgsql AS $$
         BEGIN
           IF NEW.id < 0 THEN RETURN NULL; END IF;
           NEW.value := 'prepared:' || NEW.value;
           RETURN NEW;
         END $$",
    )
    .await;
    exec_session(
        &mut session,
        "CREATE FUNCTION audit_sharded_item() RETURNS trigger LANGUAGE plpgsql AS $$
         BEGIN
           INSERT INTO sharded_audit VALUES (TG_LEVEL || ':' || TG_OP);
           RETURN NEW;
         END $$",
    )
    .await;
    exec_session(
        &mut session,
        "CREATE TRIGGER sharded_prepare BEFORE INSERT ON sharded_items
         FOR EACH ROW EXECUTE FUNCTION prepare_sharded_item()",
    )
    .await;
    exec_session(
        &mut session,
        "CREATE TRIGGER sharded_row_audit AFTER INSERT ON sharded_items
         FOR EACH ROW EXECUTE FUNCTION audit_sharded_item()",
    )
    .await;
    exec_session(
        &mut session,
        "CREATE TRIGGER sharded_statement_audit AFTER INSERT ON sharded_items
         FOR EACH STATEMENT EXECUTE FUNCTION audit_sharded_item()",
    )
    .await;
    exec_session(
        &mut session,
        "CREATE TRIGGER sharded_update_row AFTER UPDATE ON sharded_items
         FOR EACH ROW EXECUTE FUNCTION audit_sharded_item()",
    )
    .await;
    exec_session(
        &mut session,
        "CREATE TRIGGER sharded_update_statement AFTER UPDATE ON sharded_items
         FOR EACH STATEMENT EXECUTE FUNCTION audit_sharded_item()",
    )
    .await;
    exec_session(
        &mut session,
        "CREATE TRIGGER sharded_delete_row AFTER DELETE ON sharded_items
         FOR EACH ROW EXECUTE FUNCTION audit_sharded_item()",
    )
    .await;
    exec_session(
        &mut session,
        "CREATE TRIGGER sharded_delete_statement AFTER DELETE ON sharded_items
         FOR EACH STATEMENT EXECUTE FUNCTION audit_sharded_item()",
    )
    .await;

    let result = exec_session(
        &mut session,
        "INSERT INTO sharded_items VALUES (1, 'kept'), (-1, 'skipped')",
    )
    .await;
    assert_eq!(
        result,
        QueryResult::Command {
            tag: "INSERT 0 1".into()
        }
    );
    assert_eq!(
        scalar_session(&mut session, "SELECT value FROM sharded_items").await,
        "prepared:kept"
    );
    assert_eq!(
        scalar_session(
            &mut session,
            "SELECT string_agg(message, ',') FROM sharded_audit",
        )
        .await,
        "ROW:INSERT,STATEMENT:INSERT"
    );
    exec_session(&mut session, "UPDATE sharded_items SET value = 'changed'").await;
    exec_session(&mut session, "DELETE FROM sharded_items").await;
    assert_eq!(
        scalar_session(
            &mut session,
            "SELECT string_agg(message, ',') FROM sharded_audit",
        )
        .await,
        "ROW:INSERT,STATEMENT:INSERT,ROW:UPDATE,STATEMENT:UPDATE,ROW:DELETE,STATEMENT:DELETE"
    );
}
