"""Capture byte-exact Confluent serializer goldens for Order{id,total}.

Registers each format (in order: avro, protobuf, json) against a fresh
cp-schema-registry, serializes a fixed value with the official Confluent
serializers, and writes `<fmt>.hex` to /out plus prints the assigned schema id.
Run via docker-compose (see docker-compose.yml).
"""

import subprocess
import sys

from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroSerializer
from confluent_kafka.schema_registry.json_schema import JSONSerializer
from confluent_kafka.serialization import MessageField, SerializationContext

SR = "http://schema-registry:8081"
OUT = "/out"
VALUE = {"id": "o-1", "total": 9.5}

sr = SchemaRegistryClient({"url": SR})


def emit(fmt, framed):
    framed = bytes(framed)
    schema_id = int.from_bytes(framed[1:5], "big")
    with open(f"{OUT}/{fmt}.hex", "w") as f:
        f.write(framed.hex())
    print(f"{fmt.upper()} id={schema_id} hex={framed.hex()} body={framed[5:]!r}")


# 1) AVRO  -> subject orders-value
avro_schema = (
    '{"type":"record","name":"Order","fields":'
    '[{"name":"id","type":"string"},{"name":"total","type":"double"}]}'
)
aser = AvroSerializer(sr, avro_schema, lambda o, c: o)
emit("avro", aser(VALUE, SerializationContext("orders", MessageField.VALUE)))

# 2) PROTOBUF -> subject orders-pb-value
subprocess.run(
    [sys.executable, "-m", "grpc_tools.protoc", "-I/proto", "--python_out=/tmp", "/proto/order.proto"],
    check=True,
)
sys.path.insert(0, "/tmp")
import order_pb2  # noqa: E402

from confluent_kafka.schema_registry.protobuf import ProtobufSerializer  # noqa: E402

pser = ProtobufSerializer(order_pb2.Order, sr, {"use.deprecated.format": False})
emit("protobuf", pser(order_pb2.Order(id="o-1", total=9.5), SerializationContext("orders-pb", MessageField.VALUE)))

# 3) JSON -> subject orders-json-value
json_schema = (
    '{"$schema":"http://json-schema.org/draft-07/schema#","type":"object",'
    '"properties":{"id":{"type":"string"},"total":{"type":"number"}},'
    '"required":["id","total"]}'
)
jser = JSONSerializer(json_schema, sr, lambda o, c: o)
emit("json", jser(VALUE, SerializationContext("orders-json", MessageField.VALUE)))
