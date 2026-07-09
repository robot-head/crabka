# Crabka Go SDK

Reference Go slice for the serverless SDK contract. It includes generated Connect clients for `crabka.gateway.v1`, a small `crabka` facade, mock/unreachable endpoints for conformance, and the JSON-lines conformance adapter for contract v1.1.

The live client talks to the Gateway's Connect-RPC endpoint. Plain `http://` endpoints use an h2c-capable HTTP/2 transport by default so the bidi `Subscribe` stream can run against the plaintext gateway listener.

## Supported surface

- `New(endpoint, httpClient, ...Option)` creates a client for the Gateway endpoint. Pass `nil` for the HTTP client to use the SDK default; `http://` endpoints get an h2c transport and `https://` endpoints use Go's default client.
- `Messaging().Publish(ctx, Record{...})` sends a raw record with ordered headers.
- `Messaging().PublishEvent(ctx, topic, CloudEvent{...})` maps CloudEvents to the Crabka binary-mode headers: `ce_id`, `ce_source`, `ce_type`, `ce_specversion`, and bare `content-type` for `datacontenttype`.
- `Messaging().Subscribe(ctx, topics, group, filter)` opens the Gateway bidi stream with `auto_commit=true`. `Subscription.Next(ctx, timeout)` receives the next message and acknowledges the received offset on the stream.
- Filters are equality-only over structured JSON records: use `&Filter{Path: "$.kind", Op: Equals, Value: "created"}`. Non-equality operators and paths that do not start with `$.` are rejected.
- `Queues()` implements the queue v1.1 acquire / acknowledge / renew subset used by the conformance suite.

Bearer tokens configured with `WithBearerToken` are for dev/test only. The current gateway token path uses unsecured development JWS material and is not a production authentication surface.

## Deferred or intentionally skipped surface

- Manual per-offset subscribe ack is not a public SDK mode yet; subscribe uses `auto_commit=true` until the MSG-3 ack contract lands.
- Share-group consume and topic auto-provision are not exposed because the Gateway does not yet have the required RPCs.
- CloudEvents consume is not exposed until MSG-1 defines the receive-side mapping.
- Database, auth sign-in, and blob APIs are explicit conformance stubs and return typed SDK errors.

Regenerate gateway stubs from the repository root with:

```sh
go run github.com/bufbuild/buf/cmd/buf@latest generate
```

Build and run the adapter used by the Rust conformance runner with:

```sh
go build -o bin/conformance-adapter ./cmd/conformance-adapter
cargo run -p crabka-sdk-conformance --bin conformance -- \
  --adapter sdks/go/bin/conformance-adapter \
  --vectors crates/sdk-conformance/vectors/v1
```

## Live harness

The live gateway harness lives at `testdata/docker-compose.yml`. It launches:

- `broker` from `${CRABKA_BROKER_IMAGE:-ghcr.io/robot-head/crabka-broker:edge}` on `${CRABKA_BROKER_PORT:-9092}`.
- `gateway` from `${CRABKA_GATEWAY_IMAGE:-ghcr.io/robot-head/crabka-gateway:edge}` on `${CRABKA_GATEWAY_PORT:-9500}`, configured with `CRABKA_BOOTSTRAP_SERVERS=broker:9092`, `CRABKA_GATEWAY_LISTEN_ADDR=0.0.0.0:9500`, and `CRABKA_GATEWAY_ADVERTISED_ADDR=gateway:9500`.

The gateway image is assembled from `packaging/apko/crabka-gateway.yaml`, which installs the `crabka-grpc-gateway` APK produced by `packaging/melange/crabka.yaml`. Default CI runs `tools/check-sdk-go-harness-artifacts.sh` to verify the apko config, melange subpackage, publish matrix entry, compose wiring, and `docker compose config` when Docker is available. It does not start containers by default.

Run the live smoke gate from the repository root only when the broker and gateway images are pullable or locally tagged:

```sh
./tools/check-sdk-go-harness-artifacts.sh
docker compose -f sdks/go/testdata/docker-compose.yml up -d
(cd sdks/go && CRABKA_GO_INTEGRATION=1 CRABKA_GATEWAY_ENDPOINT=http://127.0.0.1:${CRABKA_GATEWAY_PORT:-9500} go test -tags integration ./...)
docker compose -f sdks/go/testdata/docker-compose.yml down -v
```

The `integration`-tagged Go smoke is opt-in: without `CRABKA_GO_INTEGRATION=1` it skips, and with that opt-in it fails clearly unless `CRABKA_GATEWAY_ENDPOINT` names the compose-published gateway. The current SDK has no standalone gateway hello RPC, so the smoke creates a live SDK client and performs `GET /healthz` through the SDK's default endpoint transport. For plaintext `http://` endpoints, that is the same h2c-capable transport used by streaming SDK calls, proving the compose-published gateway endpoint is reachable without requiring Kafka topic data.

The GitHub Actions live compose smoke sets `CRABKA_GO_INTEGRATION=1` and `CRABKA_GATEWAY_ENDPOINT=http://127.0.0.1:${CRABKA_GATEWAY_PORT:-9500}` and is intentionally `workflow_dispatch`-only for the same image availability reason.
