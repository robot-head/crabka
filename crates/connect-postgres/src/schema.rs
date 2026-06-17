use std::fmt::Write as _;

use bytes::Bytes;
use crabka_schema_serde::wire::encode_protobuf;
use prost::Message as _;
use prost_reflect::prost_types::field_descriptor_proto::{Label, Type};
use prost_reflect::prost_types::{
    DescriptorProto, FieldDescriptorProto, FileDescriptorProto, FileDescriptorSet,
};
use prost_reflect::{DescriptorPool, DynamicMessage, MessageDescriptor, Value};

use crate::PostgresConnectError;
use crate::model::{ColumnValue, EntityDifference, EntityKey, Operation, ScalarValue};

pub const KEY_SCHEMA_ID: u32 = 1;
pub const VALUE_SCHEMA_ID: u32 = 2;

const MESSAGE_INDEX: &[i32] = &[0];
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
            MESSAGE_INDEX,
            &message.encode_to_vec(),
        ))
    }

    pub fn encode_value(&self, value: &EntityDifference) -> Result<Bytes, PostgresConnectError> {
        let message = self.difference_to_message(value)?;
        Ok(encode_protobuf(
            VALUE_SCHEMA_ID,
            MESSAGE_INDEX,
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
        set_field(
            &mut message,
            "value",
            Value::String(scalar_to_string(&column.value)),
        )?;
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
                        field("value", 2, Type::String),
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

fn scalar_to_string(value: &ScalarValue) -> String {
    match value {
        ScalarValue::Null => String::new(),
        ScalarValue::Bool(value) => value.to_string(),
        ScalarValue::Int(value) => value.to_string(),
        ScalarValue::Float(value) | ScalarValue::Text(value) => value.clone(),
        ScalarValue::Bytes(value) => {
            let mut hex = String::with_capacity(value.len() * 2);
            for byte in value {
                let _ = write!(hex, "{byte:02x}");
            }
            hex
        }
    }
}

fn convert_error(error: impl std::fmt::Display) -> PostgresConnectError {
    PostgresConnectError::Convert(error.to_string())
}

#[cfg(test)]
mod tests {
    use crabka_schema_serde::wire::decode_protobuf;

    use crate::model::{ColumnSchema, ScalarValue};
    use crate::{
        ColumnValue, EntityDifference, EntityKey, Operation, PgLsn, TableSchema,
        schema::{KEY_SCHEMA_ID, PostgresProtoEncoder, VALUE_SCHEMA_ID},
    };

    #[test]
    fn encoder_frames_key_and_value_as_protobuf() {
        let encoder = PostgresProtoEncoder::new().expect("encoder builds descriptors");
        let diff = sample_difference();

        let key = encoder.encode_key(&diff.key).expect("key encodes");
        let value = encoder.encode_value(&diff).expect("value encodes");

        let (key_id, key_index, key_body) = decode_protobuf(&key).expect("key frame decodes");
        assert_eq!(key_id, KEY_SCHEMA_ID);
        assert_eq!(key_index, vec![0]);
        assert!(!key_body.is_empty());

        let (value_id, value_index, value_body) =
            decode_protobuf(&value).expect("value frame decodes");
        assert_eq!(value_id, VALUE_SCHEMA_ID);
        assert_eq!(value_index, vec![0]);
        assert!(!value_body.is_empty());
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
            before: vec![ColumnValue {
                name: "name".to_owned(),
                value: ScalarValue::Text("old".to_owned()),
            }],
            after: vec![ColumnValue {
                name: "name".to_owned(),
                value: ScalarValue::Text("new".to_owned()),
            }],
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
}
