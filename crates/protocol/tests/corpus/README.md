# Wire-format corpus

Each frame is two files with the same stem:

- `*.hex` — the raw bytes of the message body (not including the 4-byte length
  prefix). Whitespace is ignored.
- `*.toml` — metadata sidecar:

```toml
api_key = 18
version = 3
direction = "request"   # "request" or "response"
source_kafka_version = "4.2.0"
synthetic = false       # true if hand-constructed rather than captured
description = "ApiVersions v3 from kafka-console-producer"
```

Every commit, the test harness decodes every frame using the owned codec,
re-encodes, and asserts the bytes match.
