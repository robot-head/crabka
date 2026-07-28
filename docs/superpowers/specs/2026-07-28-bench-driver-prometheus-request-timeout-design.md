# Bench Driver Prometheus Request Timeout Design

## Goal

Replace the fixed 15-second Prometheus HTTP request timeout in
`crabka-bench-driver` with one validated runtime setting while preserving the
existing default and resource-capture behavior.

## Scope

This slice changes only the timeout applied to the `reqwest::Client` used by
`PromClient`.

It does not change Prometheus queries, retry behavior, capture timing, response
parsing, error handling, or other benchmark timeouts. It does not add a CRD:
the checked-in benchmark Job and its launcher script own this binary's runtime
configuration, and no operator-managed resource owns the setting.

## Public Configuration

The compiled default remains exactly 15 seconds.

The binary accepts:

- `--prometheus-request-timeout-seconds`
- `BENCH_PROMETHEUS_REQUEST_TIMEOUT_SECONDS`

The command-line value wins when both sources are present. When neither is
present, the compiled default is used.

Zero, malformed, negative, and values outside the `u64` range fail during
argument parsing, before Prometheus I/O begins.

`bench/scripts/run-scenario.sh` provides an overrideable default of 15 and
exports `BENCH_PROMETHEUS_REQUEST_TIMEOUT_SECONDS`. The checked-in Job template
passes that environment variable to the driver container so rendered benchmark
Jobs always contain a nonempty value.

## Validated Type

Add a public `PrometheusRequestTimeoutSeconds(u64)` newtype. Its constructor
and `FromStr` implementation validate with
`refined_type::rule::GreaterU64<0>`. The type exposes a `Duration` only when
constructing the HTTP client.

The default is a named `DEFAULT_PROMETHEUS_REQUEST_TIMEOUT_SECONDS` constant.
Add the workspace-pinned `refined_type` as a direct dependency of
`crabka-bench-driver`. The only permitted `Cargo.lock` change is adding
`refined_type` to that package's direct dependency list; dependency versions
and transitive packages must remain unchanged.

## Input Resolution

Add the field to the existing private Clap `Cli` parser. Clap's explicit long
name, `env` support, and typed default provide command-line-over-environment
precedence without custom resolution code.

Copy the resolved typed value into `DriverConfig`. Existing driver
configuration behavior remains unchanged.

## Runtime Flow

The exact value flow is:

```text
CLI / environment / typed default
  -> PrometheusRequestTimeoutSeconds
  -> DriverConfig::prometheus_request_timeout_seconds
  -> PromClient::new
  -> reqwest::ClientBuilder::timeout
```

`PromClient::new` must require the typed timeout as an argument rather than
retain a hidden fallback. Its base URL validation and HTTP client construction
errors remain unchanged.

## Deployment Wiring

Add `BENCH_PROMETHEUS_REQUEST_TIMEOUT_SECONDS` to the documented inputs and
container environment in `bench/manifests/driver/job-template.yaml`.

Add the overrideable 15-second default and export to
`bench/scripts/run-scenario.sh`. The existing `envsubst` rendering path remains
unchanged; callers may override the value using the same environment-based
mechanism as the other benchmark settings.

## Tests

Test-first coverage will prove:

1. the configured default remains 15 seconds;
2. one second is accepted;
3. zero, malformed, negative, and `u64` overflow values are rejected;
4. an environment value is accepted and a command-line value overrides it;
5. `PromClient` construction requires and accepts the validated timeout; and
6. the rendered Job contains an explicitly supplied timeout override.

Completion gates are the package's all-target tests, strict Clippy, nightly
formatting, a single-help-entry check, shell syntax validation, rendered
manifest inspection, diff hygiene, and the exact lockfile constraint above.

## Audit Closure

After implementation, update `docs/configuration-audit.md` with exact scanner
and focused-search counts, the complete value flow, verification evidence, and
the next real unresolved owner. Invariants, test inputs, and already-configured
defaults remain fixed rather than becoming additional settings.
