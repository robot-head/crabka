# CloudEvents HTTP gateway smoke

This smoke demo exercises the implemented gateway CloudEvents path:

1. HTTP binary and structured CloudEvents ingress.
2. Kafka records carrying `ce_*` headers and bare `content-type`.
3. Outbound webhook egress in CloudEvents binary and structured modes.

The fully automated in-process regression is:

```bash
cargo test -p crabka-grpc-gateway --test cloudevents_roundtrip
```

## Files

- `webhooks.toml` defines a named HTTP ingress endpoint at
  `/v1/webhooks/events` that writes to `cloudevents-demo`.
- `outbound-webhooks.toml` defines two outbound webhook subscriptions from the
  same topic: one `cloud_events_binary`, one `cloud_events_structured`.
- `smoke.sh` starts a local webhook capture server on `127.0.0.1:18080`, posts a
  binary event to `/v1/produce/{topic}`, posts a structured event to the named
  ingress endpoint, and waits until both CloudEvents egress shapes arrive.

## Run

After the broker is listening, create the topic `cloudevents-demo` with your
normal Kafka-compatible admin tooling before starting the gateway. This
repository does not ship a standalone topic-admin CLI; the regression test above
creates the topic programmatically.

Start a broker and gateway from the repository root in separate terminals:

```bash
cargo run -p crabka-broker --bin crabka-broker -- \
  --listen-addr 127.0.0.1:9092 \
  --advertised-listener 127.0.0.1:9092

CRABKA_GATEWAY_ADVERTISED_ADDR=127.0.0.1:9500 \
cargo run -p crabka-grpc-gateway --bin crabka-grpc-gateway -- \
  --bootstrap-servers 127.0.0.1:9092 \
  --listen-addr 127.0.0.1:9500 \
  --webhooks-config demo/cloudevents/webhooks.toml \
  --outbound-webhooks-config demo/cloudevents/outbound-webhooks.toml
```

Then run the smoke:

```bash
demo/cloudevents/smoke.sh
```

Override endpoints when needed:

```bash
CRABKA_GATEWAY_URL=http://127.0.0.1:9500 \
CRABKA_CE_TOPIC=cloudevents-demo \
CRABKA_CE_CAPTURE_PORT=18080 \
demo/cloudevents/smoke.sh
```
