use bytes::Bytes;
use crabka_schema_serde::wire::encode_protobuf;
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

// Local placeholder IDs until schema registry allocation is wired in.
const KEY_SCHEMA_ID: u32 = 1;
const VALUE_SCHEMA_ID: u32 = 2;

const KEY_MESSAGE_INDEX: &[i32] = &[1];
const VALUE_MESSAGE_INDEX: &[i32] = &[2];
const PACKAGE: &str = "crabka.connect.postgres";
const COLUMN_VALUE: &str = "ColumnValue";
const ENTITY_KEY: &str = "EntityKey";
const ENTITY_DIFFERENCE: &str = "EntityDifference";

#[derive(Debug, Clone)]
pub struct PostgresProtoEncoder {
    key: MessageDescriptor,
    value: MessageDescriptor,
    column_value: MessageDescriptor,
}

impl PostgresProtoEncoder {
    pub fn new() -> Result<Self, PostgresConnectError> {
        let pool = DescriptorPool::from_file_descriptor_set(schema_descriptor_set())
            .map_err(convert_error)?;

        Ok(Self {
            key: message_descriptor(&pool, ENTITY_KEY)?,
            value: message_descriptor(&pool, ENTITY_DIFFERENCE)?,
            column_value: message_descriptor(&pool, COLUMN_VALUE)?,
        })
    }

    pub fn encode_key(&self, key: &EntityKey) -> Result<Bytes, PostgresConnectError> {
        let message = self.key_to_message(key)?;
        Ok(encode_protobuf(
            KEY_SCHEMA_ID,
            KEY_MESSAGE_INDEX,
            &message.encode_to_vec(),
        ))
    }

    pub fn encode_value(&self, value: &EntityDifference) -> Result<Bytes, PostgresConnectError> {
        let message = self.difference_to_message(value)?;
        Ok(encode_protobuf(
            VALUE_SCHEMA_ID,
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
            set_field(&mut message, "txid", Value::I64(txid))?;
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

#[cfg(test)]
mod tests {
    use bytes::Bytes;
    use crabka_schema_serde::wire::decode_protobuf;
    use prost_reflect::{DescriptorPool, DynamicMessage, Value};

    use super::{
        COLUMN_VALUE, ENTITY_DIFFERENCE, ENTITY_KEY, KEY_SCHEMA_ID, PostgresProtoEncoder,
        VALUE_SCHEMA_ID, message_descriptor, schema_descriptor_set,
    };
    use crate::{
        ColumnValue, EntityDifference, EntityKey, Operation, PgLsn, TableSchema,
        model::{ColumnSchema, ScalarValue},
        pgoutput::{RelationCache, RelationEvent, RowEvent, RowEventKind},
    };

    #[test]
    fn encoder_frames_key_and_value_as_protobuf() {
        let encoder = PostgresProtoEncoder::new().expect("encoder builds descriptors");
        let diff = sample_difference();
        let pool = DescriptorPool::from_file_descriptor_set(schema_descriptor_set())
            .expect("descriptor pool builds");

        let key = encoder.encode_key(&diff.key).expect("key encodes");
        let value = encoder.encode_value(&diff).expect("value encodes");

        let (key_id, key_index, key_body) = decode_protobuf(&key).expect("key frame decodes");
        assert_eq!(key_id, KEY_SCHEMA_ID);
        assert_eq!(key_index, vec![1]);
        assert!(!key_body.is_empty());

        let (value_id, value_index, value_body) =
            decode_protobuf(&value).expect("value frame decodes");
        assert_eq!(value_id, VALUE_SCHEMA_ID);
        assert_eq!(value_index, vec![2]);
        assert!(!value_body.is_empty());

        let key_message = DynamicMessage::decode(
            message_descriptor(&pool, ENTITY_KEY).expect("key descriptor"),
            key_body,
        )
        .expect("key body decodes");
        assert_eq!(string_field(&key_message, "table"), "public.accounts");
        let key_columns = list_field(&key_message, "columns");
        let id_column = message_value(&key_columns[0]);
        assert_eq!(string_field(id_column, "name"), "id");
        assert_eq!(string_field(id_column, "kind"), "int");
        assert_eq!(i64_field(id_column, "int_value"), 42);

        let value_message = DynamicMessage::decode(
            message_descriptor(&pool, ENTITY_DIFFERENCE).expect("value descriptor"),
            value_body,
        )
        .expect("value body decodes");
        assert_eq!(string_field(&value_message, "table"), "public.accounts");
        assert_eq!(string_field(&value_message, "operation"), "update");
        assert_eq!(string_field(&value_message, "lsn"), "0/2A");

        let after = list_field(&value_message, "after");
        let name_column = message_value(&after[0]);
        assert_eq!(string_field(name_column, "name"), "name");
        assert_eq!(string_field(name_column, "kind"), "text");
        assert_eq!(string_field(name_column, "string_value"), "new");

        let before = list_field(&value_message, "before");
        let null_column = message_value(&before[1]);
        assert_eq!(string_field(null_column, "name"), "nickname");
        assert_eq!(string_field(null_column, "kind"), "null");
        assert!(bool_field(null_column, "is_null"));

        let avatar_column = message_value(&after[1]);
        assert_eq!(string_field(avatar_column, "name"), "avatar");
        assert_eq!(string_field(avatar_column, "kind"), "bytes");
        assert_eq!(bytes_field(avatar_column, "bytes_value").as_ref(), b"abc");

        let unchanged_toast_column = message_value(&after[2]);
        assert_eq!(string_field(unchanged_toast_column, "name"), "details");
        assert_eq!(
            string_field(unchanged_toast_column, "kind"),
            "unchanged_toast"
        );
    }

    #[test]
    fn decoded_int8_key_encodes_as_int_scalar_kind() {
        let mut cache = RelationCache::default();
        cache.apply_relation(RelationEvent {
            relation_id: 7,
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
                relation_id: 7,
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
        assert_eq!(difference.key.columns[0].value, ScalarValue::Int(42));

        let encoder = PostgresProtoEncoder::new().expect("encoder builds descriptors");
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

        assert_eq!(string_field(id_column, "kind"), "int");
        assert_eq!(i64_field(id_column, "int_value"), 42);
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

        assert_eq!(
            column_value
                .field
                .iter()
                .find(|field| field.name.as_deref() == Some("name"))
                .and_then(|field| field.label),
            Some(prost_reflect::prost_types::field_descriptor_proto::Label::Optional as i32)
        );
        assert_eq!(
            entity_key
                .field
                .iter()
                .find(|field| field.name.as_deref() == Some("columns"))
                .and_then(|field| field.label),
            Some(prost_reflect::prost_types::field_descriptor_proto::Label::Repeated as i32)
        );
        assert_eq!(
            entity_difference
                .field
                .iter()
                .find(|field| field.name.as_deref() == Some("key"))
                .and_then(|field| field.label),
            Some(prost_reflect::prost_types::field_descriptor_proto::Label::Optional as i32)
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
            txid: Some(7),
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
