# capture-mm2-golden

Captures golden byte vectors from the **real JVM MirrorMaker-2 record codecs**
(`mirror.gcr.io/apache/kafka:4.0.0`) for Crabka's MM2 byte-exactness proof, and writes them to
`crates/replicator/tests/fixtures/mm2_serde_golden.json`.

The committed fixture is the source of truth consumed by the Rust test
`crates/replicator/tests/mm2_golden_jvm.rs`. This program documents the
provenance and lets anyone reproduce the vectors.

## What it captures

`Capture.java` constructs each record with the FIXED constants below, calls the
package-private `recordKey()` / `recordValue()` on the real classes
(`org.apache.kafka.connect.mirror.{Heartbeat,Checkpoint,OffsetSync}`), and prints
one `name=<lowercase-hex>` line per key/value.

It is declared in package `org.apache.kafka.connect.mirror` so it can reach those
package-private methods.

FIXED constants (must match `Capture.java` and `mm2_golden_jvm.rs`):

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

`mirror.gcr.io/apache/kafka:4.0.0` ships only a JRE (no `javac`), so extract the Kafka jars
from that image and compile + run against them with a local JDK 17 (or newer).
The MM2 classes live in `connect-mirror-4.0.0.jar` under `/opt/kafka/libs`.

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

Confirmed via `javap -p -c` against the jar:

- **Heartbeat** / **Checkpoint**: the *key* is versionless (`serializeKey()`
  writes `KEY_SCHEMA` only); the *value* carries an `[int16 version]` header
  (`serializeValue(short)` writes `HEADER_SCHEMA` first).
- **OffsetSync**: has **no** `HEADER_SCHEMA` / version field at all — both key
  and value are versionless (only `KEY_SCHEMA` / `VALUE_SCHEMA`).

The Rust codec in `crates/replicator/src/mm2` mirrors this exactly.
