# ACL live gate debug report

## Root cause

The broker authorization decision was correct. The old gate nevertheless crossed Docker and a
cold JVM before it could observe the broker's Metadata response. The reported failing run retained
an empty redirected log and reported a nonzero outer command, but did **not** retain the inner
`docker run` status. Its exact pre-output cause (timeout, Docker failure, or JVM startup failure)
therefore cannot be proven retrospectively and is not claimed here. The demonstrated root cause
of the gate failure is narrower: the assertion depended on an external process boundary whose
failure could leave no Kafka protocol evidence, even though the ACL itself was correct.

The repaired gate removes that boundary. A hidden `crabka gres probe-topic-read` command uses
Crabka's native SCRAM client and sends a named Kafka Metadata request. The assertion passes only
for process status 1 plus the exact named-topic result
`topic __gres_tenants metadata: UNKNOWN (29)`. Authentication failures, omitted/missing topics,
transport errors, timeouts, success, another topic's error, and generic authorization text fail.
The password is read from a mode-0600 file and is never logged.

## RED / GREEN evidence

- RED: `bash scripts/tests/gres-e2e-topic-probe.sh` failed because the script still contained
  `kafka-console-consumer.sh` and did not invoke the native probe.
- RED refinement: the contract rejected an implementation that accepted code 29 for a different
  topic; before tightening, it failed with `classifier accepted authorization denial for the wrong topic`.
- GREEN: `bash scripts/tests/gres-e2e-topic-probe.sh` passes the exact-topic/code-29 contract and
  negative cases for empty output, timeout, wrong topic, generic denial text, and success.
- GREEN: `cargo test -p crabka-cli` passed 46 tests (44 unit, 2 integration).
- GREEN: `bash -n scripts/gres-e2e.sh` and `cargo fmt --all -- --check` passed (stable rustfmt emits
  existing warnings for nightly-only configuration keys).

## Live evidence

### Old probe: successful protocol boundary

With the live E2E running, the former command was executed concurrently against its SASL listener
(the generated properties file was mode 0600 and its password was never printed):

```bash
timeout 20s docker run --rm --network host --user "$(id -u):$(id -g)" \
  -v "${PWD}/target/old-probe.properties:/tmp/client.properties:ro" \
  mirror.gcr.io/apache/kafka:4.0.0 \
  /opt/kafka/bin/kafka-console-consumer.sh \
  --bootstrap-server "127.0.0.1:${sasl_port}" \
  --consumer.config /tmp/client.properties \
  --topic __gres_tenants --partition 0 --offset earliest \
  --max-messages 1 --timeout-ms 5000 \
  >target/old-probe-success.log 2>&1
```

Observed statuses and size were `old_probe_status=0`, `old_probe_bytes=750`, and enclosing
`e2e_status=1` (the latter was the later missing-psycopg gate). Relevant retained excerpt:

```text
The metadata response from the cluster reported a recoverable issue ... {__gres_tenants=TOPIC_AUTHORIZATION_FAILED}
org.apache.kafka.common.errors.TopicAuthorizationException: Not authorized to access topics: [__gres_tenants]
Processed a total of 0 messages
```

This proves the broker-to-JVM boundary returns the required named-topic authorization result when
the external process reaches Kafka. It also explains why the classifier historically had to accept
an authorization signature even when the console consumer exited zero.

### Old boundary: controlled empty pre-output timeout

Because the original empty-log run did not retain its inner status, a controlled diagnostic forced
the same pinned image/JVM launcher to time out before the JVM emitted output:

```bash
timeout --signal=KILL 0.001s docker run --rm \
  mirror.gcr.io/apache/kafka:4.0.0 \
  /opt/kafka/bin/kafka-console-consumer.sh --help \
  >target/old-probe-forced-empty.log 2>&1
status=$?
wc -c -l target/old-probe-forced-empty.log
```

Observed: `status=124`, `0` bytes, `0` lines. This does not identify the original run as a timeout;
it demonstrates the old component boundary's relevant failure mode: a nonzero bounded command can
produce an empty artifact before any Kafka protocol result is available. Such an outcome remains
a hard assertion failure, never authorization proof.

### Repaired native probe

`CRABKA_GRES_SKIP_BUILD=1 CRABKA_GRES_E2E_KEEP_ARTIFACTS=1 ./scripts/gres-e2e.sh`
exited 1 at the later psycopg gate after passing all three ACL assertions. The retained
global-registry artifact contains exactly:

`crabka gres: topic __gres_tenants metadata: UNKNOWN (29)`

The E2E continued beyond the repaired ACL gate and then failed independently because Python
`psycopg` is not installed for the later PgDog driver smoke tests. Artifacts remain under
`target/gres-e2e-artifacts`.
