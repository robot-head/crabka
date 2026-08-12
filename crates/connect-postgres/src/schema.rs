use bytes::Bytes;
use crabka_schema_serde::{RegistryClient, SchemaKind, wire::encode_protobuf};
use prost::Message as _;
use prost_reflect::{
    DescriptorPool, DynamicMessage, MessageDescriptor, Value,
    prost_types::{
        DescriptorProto, FieldDescriptorProto, FileDescriptorProto, FileDescriptorSet,
        field_descriptor_proto::{Label, Type},
    },
};

use crate::{
    PostgresConnectError,
    model::{ColumnValue, EntityDifference, EntityKey, Operation, ScalarValue},
};

const KEY_MESSAGE_INDEX: &[i32] = &[1];
const VALUE_MESSAGE_INDEX: &[i32] = &[2];
const PACKAGE: &str = "crabka.connect.postgres";
const COLUMN_VALUE: &str = "ColumnValue";
const ENTITY_KEY: &str = "EntityKey";
const ENTITY_DIFFERENCE: &str = "EntityDifference";
const KEY_SUBJECT: &str = "crabka-connect-postgres-key";
const VALUE_SUBJECT: &str = "crabka-connect-postgres-value";
const KEY_MESSAGE_TYPE: &str = "crabka.connect.postgres.EntityKey";
const VALUE_MESSAGE_TYPE: &str = "crabka.connect.postgres.EntityDifference";
const PROTO_SCHEMA: &str = r#"syntax = "proto3";
package crabka.connect.postgres;

message ColumnValue {
  string name = 1;
  string kind = 2;
  string string_value = 3;
  bool bool_value = 4;
  int64 int_value = 5;
  bytes bytes_value = 6;
  bool is_null = 7;
}

message EntityKey {
  string table = 1;
  repeated ColumnValue columns = 2;
}

message EntityDifference {
  string table = 1;
  string operation = 2;
  string lsn = 3;
  repeated ColumnValue before = 4;
  repeated ColumnValue after = 5;
  EntityKey key = 6;
  int64 txid = 7;
  int64 commit_timestamp_ms = 8;
}
"#;

#[derive(Debug, Clone)]
pub struct PostgresProtoEncoder {
    key_schema_id: u32,
    value_schema_id: u32,
    key: MessageDescriptor,
    value: MessageDescriptor,
    column_value: MessageDescriptor,
}

impl PostgresProtoEncoder {
    /// Register the connector schemas and build an encoder with the allocated IDs.
    ///
    /// # Errors
    /// Returns an error when Schema Registry rejects either schema or descriptor construction fails.
    pub async fn from_registry(registry_url: &str) -> Result<Self, PostgresConnectError> {
        let registry = RegistryClient::new(registry_url);
        let key_schema_id = registry
            .register(
                KEY_SUBJECT,
                SchemaKind::Protobuf,
                PROTO_SCHEMA,
                Some(KEY_MESSAGE_TYPE),
            )
            .await
            .map_err(registry_error)?;
        let value_schema_id = registry
            .register(
                VALUE_SUBJECT,
                SchemaKind::Protobuf,
                PROTO_SCHEMA,
                Some(VALUE_MESSAGE_TYPE),
            )
            .await
            .map_err(registry_error)?;
        Self::with_schema_ids(key_schema_id, value_schema_id)
    }

    #[cfg(test)]
    pub(crate) fn for_test() -> Result<Self, PostgresConnectError> {
        Self::with_schema_ids(41, 57)
    }

    /// # Errors
    /// Returns an error when configuration is invalid, protocol encoding fails, the broker rejects the request, or transport I/O fails.
    fn with_schema_ids(
        key_schema_id: u32,
        value_schema_id: u32,
    ) -> Result<Self, PostgresConnectError> {
        let pool = DescriptorPool::from_file_descriptor_set(schema_descriptor_set())
            .map_err(convert_error)?;

        Ok(Self {
            key_schema_id,
            value_schema_id,
            key: message_descriptor(&pool, ENTITY_KEY)?,
            value: message_descriptor(&pool, ENTITY_DIFFERENCE)?,
            column_value: message_descriptor(&pool, COLUMN_VALUE)?,
        })
    }

    /// # Errors
    /// Returns an error when configuration is invalid, protocol encoding fails, the broker rejects the request, or transport I/O fails.
    pub fn encode_key(&self, key: &EntityKey) -> Result<Bytes, PostgresConnectError> {
        let message = self.key_to_message(key)?;
        Ok(encode_protobuf(
            self.key_schema_id,
            KEY_MESSAGE_INDEX,
            &message.encode_to_vec(),
        ))
    }

    /// # Errors
    /// Returns an error when configuration is invalid, protocol encoding fails, the broker rejects the request, or transport I/O fails.
    pub fn encode_value(&self, value: &EntityDifference) -> Result<Bytes, PostgresConnectError> {
        let message = self.difference_to_message(value)?;
        Ok(encode_protobuf(
            self.value_schema_id,
            VALUE_MESSAGE_INDEX,
            &message.encode_to_vec(),
        ))
    }

    fn key_to_message(&self, key: &EntityKey) -> Result<DynamicMessage, PostgresConnectError> {
        let mut message = DynamicMessage::new(self.key.clone());
        set_field(&mut message, "table", Value::String(key.table.clone()))?;
        set_field(
            &mut message,
            "columns",
            Value::List(self.columns_to_values(&key.columns)?),
        )?;
        Ok(message)
    }

    fn difference_to_message(
        &self,
        difference: &EntityDifference,
    ) -> Result<DynamicMessage, PostgresConnectError> {
        let mut message = DynamicMessage::new(self.value.clone());
        set_field(
            &mut message,
            "table",
            Value::String(difference.table.clone()),
        )?;
        set_field(
            &mut message,
            "operation",
            Value::String(operation_name(difference.op).to_owned()),
        )?;
        set_field(
            &mut message,
            "lsn",
            Value::String(difference.lsn.to_string()),
        )?;
        set_field(
            &mut message,
            "before",
            Value::List(self.columns_to_values(&difference.before)?),
        )?;
        set_field(
            &mut message,
            "after",
            Value::List(self.columns_to_values(&difference.after)?),
        )?;
        set_field(
            &mut message,
            "key",
            Value::Message(self.key_to_message(&difference.key)?),
        )?;
        if let Some(txid) = difference.txid {
            set_field(&mut message, "txid", Value::I64(txid.0))?;
        }
        if let Some(commit_timestamp_ms) = difference.commit_timestamp_ms {
            set_field(
                &mut message,
                "commit_timestamp_ms",
                Value::I64(commit_timestamp_ms),
            )?;
        }
        Ok(message)
    }

    fn columns_to_values(
        &self,
        columns: &[ColumnValue],
    ) -> Result<Vec<Value>, PostgresConnectError> {
        columns
            .iter()
            .map(|column| self.column_to_value(column).map(Value::Message))
            .collect()
    }

    fn column_to_value(
        &self,
        column: &ColumnValue,
    ) -> Result<DynamicMessage, PostgresConnectError> {
        let mut message = DynamicMessage::new(self.column_value.clone());
        set_field(&mut message, "name", Value::String(column.name.clone()))?;
        set_scalar_fields(&mut message, &column.value)?;
        Ok(message)
    }
}

fn schema_descriptor_set() -> FileDescriptorSet {
    FileDescriptorSet {
        file: vec![FileDescriptorProto {
            name: Some("crabka/connect/postgres/cdc.proto".to_owned()),
            package: Some(PACKAGE.to_owned()),
            syntax: Some("proto3".to_owned()),
            message_type: vec![
                DescriptorProto {
                    name: Some(COLUMN_VALUE.to_owned()),
                    field: vec![
                        field("name", 1, Type::String),
                        field("kind", 2, Type::String),
                        field("string_value", 3, Type::String),
                        field("bool_value", 4, Type::Bool),
                        field("int_value", 5, Type::Int64),
                        field("bytes_value", 6, Type::Bytes),
                        field("is_null", 7, Type::Bool),
                    ],
                    ..DescriptorProto::default()
                },
                DescriptorProto {
                    name: Some(ENTITY_KEY.to_owned()),
                    field: vec![
                        field("table", 1, Type::String),
                        repeated_message_field("columns", 2, COLUMN_VALUE),
                    ],
                    ..DescriptorProto::default()
                },
                DescriptorProto {
                    name: Some(ENTITY_DIFFERENCE.to_owned()),
                    field: vec![
                        field("table", 1, Type::String),
                        field("operation", 2, Type::String),
                        field("lsn", 3, Type::String),
                        repeated_message_field("before", 4, COLUMN_VALUE),
                        repeated_message_field("after", 5, COLUMN_VALUE),
                        message_field("key", 6, ENTITY_KEY),
                        field("txid", 7, Type::Int64),
                        field("commit_timestamp_ms", 8, Type::Int64),
                    ],
                    ..DescriptorProto::default()
                },
            ],
            ..FileDescriptorProto::default()
        }],
    }
}

fn field(name: &str, number: i32, field_type: Type) -> FieldDescriptorProto {
    FieldDescriptorProto {
        name: Some(name.to_owned()),
        number: Some(number),
        label: Some(Label::Optional as i32),
        r#type: Some(field_type as i32),
        ..FieldDescriptorProto::default()
    }
}

fn message_field(name: &str, number: i32, message_name: &str) -> FieldDescriptorProto {
    FieldDescriptorProto {
        type_name: Some(full_type_name(message_name)),
        ..field(name, number, Type::Message)
    }
}

fn repeated_message_field(name: &str, number: i32, message_name: &str) -> FieldDescriptorProto {
    FieldDescriptorProto {
        label: Some(Label::Repeated as i32),
        ..message_field(name, number, message_name)
    }
}

fn full_type_name(message_name: &str) -> String {
    format!(".{PACKAGE}.{message_name}")
}

fn message_descriptor(
    pool: &DescriptorPool,
    name: &str,
) -> Result<MessageDescriptor, PostgresConnectError> {
    pool.get_message_by_name(&format!("{PACKAGE}.{name}"))
        .ok_or_else(|| PostgresConnectError::Convert(format!("protobuf message {name} not found")))
}

fn set_field(
    message: &mut DynamicMessage,
    name: &str,
    value: Value,
) -> Result<(), PostgresConnectError> {
    message
        .try_set_field_by_name(name, value)
        .map_err(convert_error)
}

fn operation_name(operation: Operation) -> &'static str {
    match operation {
        Operation::Insert => "insert",
        Operation::Update => "update",
        Operation::Delete => "delete",
    }
}

fn set_scalar_fields(
    message: &mut DynamicMessage,
    value: &ScalarValue,
) -> Result<(), PostgresConnectError> {
    match value {
        ScalarValue::Null => {
            set_field(message, "kind", Value::String("null".to_owned()))?;
            set_field(message, "is_null", Value::Bool(true))?;
        }
        ScalarValue::UnchangedToast => {
            set_field(message, "kind", Value::String("unchanged_toast".to_owned()))?;
        }
        ScalarValue::Bool(value) => {
            set_field(message, "kind", Value::String("bool".to_owned()))?;
            set_field(message, "bool_value", Value::Bool(*value))?;
        }
        ScalarValue::Int(value) => {
            set_field(message, "kind", Value::String("int".to_owned()))?;
            set_field(message, "int_value", Value::I64(*value))?;
        }
        ScalarValue::Float(value) => {
            set_field(message, "kind", Value::String("float".to_owned()))?;
            set_field(message, "string_value", Value::String(value.clone()))?;
        }
        ScalarValue::Text(value) => {
            set_field(message, "kind", Value::String("text".to_owned()))?;
            set_field(message, "string_value", Value::String(value.clone()))?;
        }
        ScalarValue::Bytes(value) => {
            set_field(message, "kind", Value::String("bytes".to_owned()))?;
            set_field(
                message,
                "bytes_value",
                Value::Bytes(Bytes::copy_from_slice(value)),
            )?;
        }
    }

    Ok(())
}

fn convert_error(error: impl std::fmt::Display) -> PostgresConnectError {
    PostgresConnectError::Convert(error.to_string())
}

fn registry_error(error: impl std::fmt::Display) -> PostgresConnectError {
    PostgresConnectError::Convert(format!("schema registry: {error}"))
}

#[cfg(test)]
mod tests {
    use bytes::Bytes;
    use crabka_schema_serde::wire::decode_protobuf;
    use prost_reflect::{DescriptorPool, DynamicMessage, Value};
    use wiremock::{
        Mock, MockServer, ResponseTemplate,
        matchers::{body_json, method, path},
    };

    use super::{
        COLUMN_VALUE, ENTITY_DIFFERENCE, ENTITY_KEY, KEY_MESSAGE_TYPE, PROTO_SCHEMA,
        PostgresProtoEncoder, VALUE_MESSAGE_TYPE, message_descriptor, schema_descriptor_set,
    };
    use crate::{
        ColumnValue, EntityDifference, EntityKey, Operation, PgLsn, TableSchema,
        ids::{RelationId, TransactionId},
        model::{ColumnSchema, ScalarValue},
        pgoutput::{RelationCache, RelationEvent, RowEvent, RowEventKind},
    };

    #[test]
    fn encoder_frames_key_and_value_as_protobuf() {
        let encoder =
            PostgresProtoEncoder::with_schema_ids(41, 57).expect("encoder builds descriptors");
        let diff = sample_difference();
        let pool = DescriptorPool::from_file_descriptor_set(schema_descriptor_set())
            .expect("descriptor pool builds");

        let key = encoder.encode_key(&diff.key).expect("key encodes");
        let value = encoder.encode_value(&diff).expect("value encodes");

        let (key_id, key_index, key_body) = decode_protobuf(&key).expect("key frame decodes");
        let key_frame = (key_id, key_index, key_body.is_empty());

        let (value_id, value_index, value_body) =
            decode_protobuf(&value).expect("value frame decodes");
        let value_frame = (value_id, value_index, value_body.is_empty());

        let key_message = DynamicMessage::decode(
            message_descriptor(&pool, ENTITY_KEY).expect("key descriptor"),
            key_body,
        )
        .expect("key body decodes");
        let key_columns = list_field(&key_message, "columns");
        let id_column = message_value(&key_columns[0]);
        let key_projection = (
            string_field(&key_message, "table"),
            string_field(id_column, "name"),
            string_field(id_column, "kind"),
            i64_field(id_column, "int_value"),
        );

        let value_message = DynamicMessage::decode(
            message_descriptor(&pool, ENTITY_DIFFERENCE).expect("value descriptor"),
            value_body,
        )
        .expect("value body decodes");
        let value_projection = (
            string_field(&value_message, "table"),
            string_field(&value_message, "operation"),
            string_field(&value_message, "lsn"),
        );

        let after = list_field(&value_message, "after");
        let name_column = message_value(&after[0]);
        let name_projection = (
            string_field(name_column, "name"),
            string_field(name_column, "kind"),
            string_field(name_column, "string_value"),
        );

        let before = list_field(&value_message, "before");
        let null_column = message_value(&before[1]);
        let null_projection = (
            string_field(null_column, "name"),
            string_field(null_column, "kind"),
            bool_field(null_column, "is_null"),
        );

        let avatar_column = message_value(&after[1]);
        let avatar_projection = (
            string_field(avatar_column, "name"),
            string_field(avatar_column, "kind"),
            bytes_field(avatar_column, "bytes_value"),
        );

        let unchanged_toast_column = message_value(&after[2]);
        let unchanged_projection = (
            string_field(unchanged_toast_column, "name"),
            string_field(unchanged_toast_column, "kind"),
        );

        assert2::assert!(key_frame == (41, vec![1], false));
        assert2::assert!(value_frame == (57, vec![2], false));
        assert2::assert!(
            key_projection
                == (
                    "public.accounts".to_string(),
                    "id".to_string(),
                    "int".to_string(),
                    42,
                )
        );
        assert2::assert!(
            value_projection
                == (
                    "public.accounts".to_string(),
                    "update".to_string(),
                    "0/2A".to_string(),
                )
        );
        assert2::assert!(
            name_projection == ("name".to_string(), "text".to_string(), "new".to_string())
        );
        assert2::assert!(null_projection == ("nickname".to_string(), "null".to_string(), true));
        assert2::assert!(
            avatar_projection
                == (
                    "avatar".to_string(),
                    "bytes".to_string(),
                    Bytes::from_static(b"abc"),
                )
        );
        assert2::assert!(
            unchanged_projection == ("details".to_string(), "unchanged_toast".to_string())
        );
    }

    #[tokio::test]
    async fn registry_allocates_ids_used_by_key_and_value_frames() {
        let server = MockServer::start().await;
        for (subject, message_type, id) in [
            ("crabka-connect-postgres-key", KEY_MESSAGE_TYPE, 73),
            ("crabka-connect-postgres-value", VALUE_MESSAGE_TYPE, 91),
        ] {
            Mock::given(method("POST"))
                .and(path(format!("/subjects/{subject}/versions")))
                .and(body_json(serde_json::json!({
                    "schema": PROTO_SCHEMA,
                    "schemaType": "PROTOBUF",
                    "messageType": message_type,
                })))
                .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                    "id": id,
                })))
                .expect(1)
                .mount(&server)
                .await;
        }

        let encoder = PostgresProtoEncoder::from_registry(&server.uri())
            .await
            .expect("registry allocates both IDs");
        let difference = sample_difference();
        let key = encoder.encode_key(&difference.key).expect("key encodes");
        let value = encoder.encode_value(&difference).expect("value encodes");

        assert2::assert!(decode_protobuf(&key).expect("key frame").0 == 73);
        assert2::assert!(decode_protobuf(&value).expect("value frame").0 == 91);
    }

    #[test]
    fn decoded_int8_key_encodes_as_int_scalar_kind() {
        let mut cache = RelationCache::default();
        cache.apply_relation(RelationEvent {
            relation_id: RelationId(7),
            schema: "public".to_owned(),
            table: "orders".to_owned(),
            columns: vec![ColumnSchema {
                name: "id".to_owned(),
                type_name: "int8".to_owned(),
                key: true,
            }],
        });
        let difference = cache
            .translate(RowEvent {
                relation_id: RelationId(7),
                lsn: PgLsn(0x2a),
                commit_lsn: None,
                txid: None,
                commit_timestamp_ms: None,
                kind: RowEventKind::Insert,
                values: vec![ColumnValue {
                    name: "col0".to_owned(),
                    value: ScalarValue::Text("42".to_owned()),
                }],
            })
            .expect("decoded row should translate");
        assert2::assert!(
            &difference
                == &EntityDifference {
                    table: "public.orders".into(),
                    key: EntityKey {
                        table: "public.orders".into(),
                        columns: vec![ColumnValue {
                            name: "id".into(),
                            value: ScalarValue::Int(42),
                        }],
                    },
                    op: Operation::Insert,
                    before: vec![],
                    after: vec![ColumnValue {
                        name: "id".into(),
                        value: ScalarValue::Int(42),
                    }],
                    lsn: PgLsn(0x2a),
                    txid: None,
                    commit_timestamp_ms: None,
                    schema: TableSchema {
                        schema: "public".into(),
                        table: "orders".into(),
                        columns: vec![ColumnSchema {
                            name: "id".into(),
                            type_name: "int8".into(),
                            key: true,
                        }],
                    },
                }
        );

        let encoder =
            PostgresProtoEncoder::with_schema_ids(41, 57).expect("encoder builds descriptors");
        let key = encoder.encode_key(&difference.key).expect("key encodes");
        let pool = DescriptorPool::from_file_descriptor_set(schema_descriptor_set())
            .expect("descriptor pool builds");
        let (_, _, key_body) = decode_protobuf(&key).expect("key frame decodes");
        let key_message = DynamicMessage::decode(
            message_descriptor(&pool, ENTITY_KEY).expect("key descriptor"),
            key_body,
        )
        .expect("key body decodes");
        let key_columns = list_field(&key_message, "columns");
        let id_column = message_value(&key_columns[0]);

        assert2::assert!(string_field(id_column, "kind") == "int".to_string());
        assert2::assert!(i64_field(id_column, "int_value") == 42);
    }

    #[test]
    fn descriptor_fields_have_expected_proto3_labels() {
        let descriptor_set = schema_descriptor_set();
        let file = descriptor_set.file.first().expect("descriptor file");
        let column_value = file
            .message_type
            .iter()
            .find(|message| message.name.as_deref() == Some(COLUMN_VALUE))
            .expect("column value message");
        let entity_key = file
            .message_type
            .iter()
            .find(|message| message.name.as_deref() == Some(ENTITY_KEY))
            .expect("entity key message");
        let entity_difference = file
            .message_type
            .iter()
            .find(|message| message.name.as_deref() == Some(ENTITY_DIFFERENCE))
            .expect("entity difference message");

        assert2::assert!(
            [
                column_value
                    .field
                    .iter()
                    .find(|field| field.name.as_deref() == Some("name"))
                    .and_then(|field| field.label),
                entity_key
                    .field
                    .iter()
                    .find(|field| field.name.as_deref() == Some("columns"))
                    .and_then(|field| field.label),
                entity_difference
                    .field
                    .iter()
                    .find(|field| field.name.as_deref() == Some("key"))
                    .and_then(|field| field.label),
            ] == [
                Some(prost_reflect::prost_types::field_descriptor_proto::Label::Optional as i32),
                Some(prost_reflect::prost_types::field_descriptor_proto::Label::Repeated as i32),
                Some(prost_reflect::prost_types::field_descriptor_proto::Label::Optional as i32),
            ]
        );
    }

    fn sample_difference() -> EntityDifference {
        let key = EntityKey {
            table: "public.accounts".to_owned(),
            columns: vec![ColumnValue {
                name: "id".to_owned(),
                value: ScalarValue::Int(42),
            }],
        };

        EntityDifference {
            table: "public.accounts".to_owned(),
            key,
            op: Operation::Update,
            before: vec![
                ColumnValue {
                    name: "name".to_owned(),
                    value: ScalarValue::Text("old".to_owned()),
                },
                ColumnValue {
                    name: "nickname".to_owned(),
                    value: ScalarValue::Null,
                },
            ],
            after: vec![
                ColumnValue {
                    name: "name".to_owned(),
                    value: ScalarValue::Text("new".to_owned()),
                },
                ColumnValue {
                    name: "avatar".to_owned(),
                    value: ScalarValue::Bytes(b"abc".to_vec()),
                },
                ColumnValue {
                    name: "details".to_owned(),
                    value: ScalarValue::UnchangedToast,
                },
            ],
            lsn: PgLsn(42),
            txid: Some(TransactionId(7)),
            commit_timestamp_ms: Some(1_700_000_000_000),
            schema: TableSchema {
                schema: "public".to_owned(),
                table: "accounts".to_owned(),
                columns: vec![
                    ColumnSchema {
                        name: "id".to_owned(),
                        type_name: "int8".to_owned(),
                        key: true,
                    },
                    ColumnSchema {
                        name: "name".to_owned(),
                        type_name: "text".to_owned(),
                        key: false,
                    },
                ],
            },
        }
    }

    fn string_field(message: &DynamicMessage, name: &str) -> String {
        match message
            .get_field_by_name(name)
            .expect("field exists")
            .as_ref()
        {
            Value::String(value) => value.clone(),
            other => panic!("field {name} was not a string: {other:?}"),
        }
    }

    fn bool_field(message: &DynamicMessage, name: &str) -> bool {
        match message
            .get_field_by_name(name)
            .expect("field exists")
            .as_ref()
        {
            Value::Bool(value) => *value,
            other => panic!("field {name} was not a bool: {other:?}"),
        }
    }

    fn i64_field(message: &DynamicMessage, name: &str) -> i64 {
        match message
            .get_field_by_name(name)
            .expect("field exists")
            .as_ref()
        {
            Value::I64(value) => *value,
            other => panic!("field {name} was not an int64: {other:?}"),
        }
    }

    fn bytes_field(message: &DynamicMessage, name: &str) -> Bytes {
        match message
            .get_field_by_name(name)
            .expect("field exists")
            .as_ref()
        {
            Value::Bytes(value) => value.clone(),
            other => panic!("field {name} was not bytes: {other:?}"),
        }
    }

    fn list_field(message: &DynamicMessage, name: &str) -> Vec<Value> {
        match message
            .get_field_by_name(name)
            .expect("field exists")
            .as_ref()
        {
            Value::List(value) => value.clone(),
            other => panic!("field {name} was not a list: {other:?}"),
        }
    }

    fn message_value(value: &Value) -> &DynamicMessage {
        match value {
            Value::Message(message) => message,
            other => panic!("value was not a message: {other:?}"),
        }
    }
}
