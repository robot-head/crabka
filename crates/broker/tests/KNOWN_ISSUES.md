# Known issues

## `console_producer_round_trip` / `kafka_topics_describe_smokes_metadata`

These tests run a Rust `crabka-broker` on the host. The JVM Kafka
command-line tools run inside a `mirror.gcr.io/confluentinc/cp-kafka` testcontainers
container. The JVM client in the container must reach the host broker.
The host broker's `advertised_listener` must also point at an address
that the container can reach. `Metadata` responses return that address.
If the address is not reachable, the JVM client connects to the
bootstrap server, learns the real address from `Metadata`, and then
cannot reconnect.

The network pattern depends on the Docker host:

- Linux CI runners, for example GitHub Actions ubuntu-latest: set
  `CRABKA_HOST_BOOTSTRAP=<docker_bridge_gateway>:<port>`. The value is
  usually `172.17.0.1:9092`. The CI workflow job
  `broker-jvm-acceptance` exports the bridge IP with:

  ```sh
  BRIDGE_IP=$(docker network inspect bridge -f '{{(index .IPAM.Config 0).Gateway}}')
  ```

- Docker Desktop on macOS and Windows: the default is
  `host.docker.internal:9092`. Docker Desktop maps that name to the
  host loopback address. The test uses this case when
  `CRABKA_HOST_BOOTSTRAP` is not set.

The test binds the broker on `0.0.0.0:<port>`. It takes the port from
the last colon-separated segment of `CRABKA_HOST_BOOTSTRAP`. If that
environment variable is not set, the port is `9092`.

## Both tests are `#[ignore = "requires Docker"]`

Run them with `cargo test -p crabka-broker --test jvm_acceptance -- --ignored`.
The CI job `broker-jvm-acceptance` is the only place that runs them by
default. On a contributor's machine, `cargo test` skips them.

## ``

Docker on Windows with the bridge-gateway pattern is not reliable. The
JVM command-line tools also behave inconsistently under Docker Desktop
on Windows. A `cfg` gate removes the test file on Windows. The whole
file compiles to an empty test binary there.
