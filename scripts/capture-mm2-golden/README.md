# capture-mm2-golden

This program captures golden byte vectors from the **real JVM MirrorMaker-2
record codecs** in `mirror.gcr.io/apache/kafka:4.0.0`. The vectors are the proof
of Crabka's MM2 byte exactness. The program writes them to
`crates/replicator/tests/fixtures/mm2_serde_golden.json`.

The committed fixture is the authority, and the Rust test
`crates/replicator/tests/mm2_golden_jvm.rs` reads it. This program records the
provenance and lets anyone reproduce the vectors.

## What it captures

`Capture.java` constructs each record with the FIXED constants below. It calls
the package-private `recordKey()` and `recordValue()` methods on the real classes
`org.apache.kafka.connect.mirror.{Heartbeat,Checkpoint,OffsetSync}`. It prints
one `name=<lowercase-hex>` line per key and per value.

`Capture.java` is declared in package `org.apache.kafka.connect.mirror`, so it
can reach those package-private methods.

FIXED constants. These must match `Capture.java` and `mm2_golden_jvm.rs`:

| field      | value       |
| ---------- | ----------- |
| source     | `us-east`   |
| target     | `eu-west`   |
| timestamp  | `100`       |
| group      | `analytics` |
| topic      | `orders`    |
| partition  | `7`         |
| upstream   | `1000`      |
| downstream | `742`       |
| metadata   | `""` (empty)|

## Reproduce

`mirror.gcr.io/apache/kafka:4.0.0` supplies only a JRE and has no `javac`.
Extract the Kafka jars from that image. Then compile and run against them with a
local JDK 17 or newer. The MM2 classes are in `connect-mirror-4.0.0.jar` under
`/opt/kafka/libs`.

```bash
# 1. extract the Kafka libs out of the image (do NOT commit them)
cid="$(docker create mirror.gcr.io/apache/kafka:4.0.0)"
docker cp "$cid:/opt/kafka/libs" ./scripts/capture-mm2-golden/libs
docker rm "$cid"

# 2. compile + run with local Java 17 (Windows classpath separator is ';')
cd scripts/capture-mm2-golden
javac -proc:none -cp "libs/*" -d out Capture.java
java -cp "libs/*;out" org.apache.kafka.connect.mirror.Capture
#   (on Linux/macOS use ':' instead of ';')

# 3. clean up — the extracted libs are large and must NOT be committed
rm -rf libs out
```

Paste the six printed `name=<hex>` lines into
`crates/replicator/tests/fixtures/mm2_serde_golden.json` as a JSON map.

## Note: not all MM2 records are versioned

`javap -p -c` against the jar confirms this:

- **Heartbeat** and **Checkpoint**: the *key* is versionless, because
  `serializeKey()` writes `KEY_SCHEMA` only. The *value* carries an
  `[int16 version]` header, because `serializeValue(short)` writes
  `HEADER_SCHEMA` first.
- **OffsetSync**: has **no** `HEADER_SCHEMA` or version field. Both the key and
  the value are versionless, and they carry only `KEY_SCHEMA` and
  `VALUE_SCHEMA`.

The Rust codec in `crates/replicator/src/mm2` matches this exactly.
