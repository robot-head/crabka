# crabka-schema-serde

Client-agnostic Confluent-compatible schema serializers and deserializers for Crabka.
Frames payloads with the Confluent wire format (`magic(0x00) | schema_id(4 BE) | body`),
resolves schemas against a Confluent-compatible Schema Registry, and supports Avro,
Protobuf, and JSON Schema formats via optional feature flags.
