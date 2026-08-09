# Wire-format corpus

Each frame is two files with the same stem:

- `*.hex` holds the raw bytes of the message body. These bytes do not include
  the 4-byte length prefix. The harness ignores whitespace.
- `*.toml` is the metadata sidecar:

```toml
api_key = 18
version = 3
direction = "request"   # "request" or "response"
source_kafka_version = "4.2.0"
synthetic = false       # true if hand-constructed rather than captured
description = "ApiVersions v3 from kafka-console-producer"
```

On every commit, the test harness decodes every frame with the owned codec. The
harness then re-encodes each frame and asserts that the bytes match.
