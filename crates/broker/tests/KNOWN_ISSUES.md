# Known issues

## `console_producer_round_trip` / `kafka_topics_describe_smokes_metadata`

These tests run a Rust `crabka-broker` on the host while the JVM Kafka
command-line tools run inside a `mirror.gcr.io/confluentinc/cp-kafka` testcontainers
container. The in-container JVM client must be able to reach the host
broker, AND the host broker's `advertised_listener` (returned in
`Metadata` responses) must point at an address that's reachable from
inside the container — else the JVM client connects to the bootstrap
server fine, learns the "real" address from Metadata, and then fails
to reconnect.

The networking pattern depends on the Docker host:

- **Linux CI runners** (GitHub Actions ubuntu-latest, etc.): set
  `CRABKA_HOST_BOOTSTRAP=<docker_bridge_gateway>:<port>` (typically
  `172.17.0.1:9092`). The CI workflow `broker-jvm-acceptance` job
  exports the bridge IP via:

  ```sh
  BRIDGE_IP=$(docker network inspect bridge -f '{{(index .IPAM.Config 0).Gateway}}')
  ```

- **Docker Desktop on macOS / Windows**: the default is
  `host.docker.internal:9092`, which Docker Desktop maps to the host's
  loopback. The test reaches this case when `CRABKA_HOST_BOOTSTRAP`
  isn't set.

The test binds the broker on `0.0.0.0:<port>`, taking the port from the
last colon-separated segment of `CRABKA_HOST_BOOTSTRAP` (or `9092`
when the env var is unset).

## Both tests are `#[ignore = "requires Docker"]`

Run with `cargo test -p crabka-broker --test jvm_acceptance -- --ignored`.
The CI job `broker-jvm-acceptance` is the only place these execute by
default; `cargo test` on a contributor's machine skips them.

## ``

Docker on Windows + the bridge-gateway pattern is fragile and the JVM
command-line tools have inconsistent behavior under Docker Desktop on
Windows. The test file is `cfg`-gated out on Windows; the entire file
compiles to an empty test binary there.
