# Topic Proto Message Type Binding Design

## Goal

Add optional `messageType` metadata to schema subjects so protobuf clients can bind a schema id to the intended protobuf message descriptor instead of relying on the Confluent message-index path alone.

## Design

`messageType` is an optional extension field in the same JSON envelope that already carries `schemaType`, `references`, `schema`, and `deleted`. It is accepted on schema register/lookup payloads, persisted on `_schemas` `SCHEMA` values, and returned from schema/version read endpoints only when present. If omitted or null, emitted JSON remains Confluent-compatible.

The Rust schema-serde protobuf path derives `messageType` from `ReflectMessage::descriptor().full_name()` when registering or looking up a local schema. The cache stores id-to-message-type metadata returned by the registry. During typed protobuf deserialization, if a writer id has message metadata, the serde validates it against `T`'s descriptor before decoding.

## Testing

Tests cover `_schemas` record round-trip, REST register/get-by-id/get-version echo behavior, request payload serialization, cache metadata storage, and protobuf typed-deserializer mismatch rejection.
