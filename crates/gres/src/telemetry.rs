//! Gres tracing + OTLP distributed-tracing pipeline.
//!
//! `crabka-gres` always installs a structured-JSON `tracing_subscriber` `fmt`
//! layer on stdout, gated by the usual `RUST_LOG` `EnvFilter`, so container log
//! collectors ingest fields rather than ANSI text. When the environment
//! configures OTLP export, gres attaches a second `tracing-opentelemetry`
//! layer. That layer converts `tracing` spans into OpenTelemetry spans and
//! batch-exports them over OTLP to a collector, either gRPC `:4317` or
//! HTTP/protobuf `:4318`. The pipeline itself is [`crabka_telemetry`]. This
//! module only supplies the gres-specific filters and documents the knobs.
//!
//! # Enabling
//!
//! OTLP is **off by default**. A gres with no OTLP environment does stdout
//! logging only. It turns on when any endpoint is set, that is
//! `CRABKA_OTLP_ENDPOINT`, `OTEL_EXPORTER_OTLP_TRACES_ENDPOINT`, or
//! `OTEL_EXPORTER_OTLP_ENDPOINT`, or when `CRABKA_OTLP_ENABLED=true`.
//! `OTEL_SDK_DISABLED=true` force-disables it.
//!
//! # Span targets
//!
//! Query-path spans live under five dedicated targets, each named for the crate
//! that owns it, so an `EnvFilter` directive reads as the subsystem it selects:
//!
//! | Target | Level | Spans |
//! |---|---|---|
//! | `crabka_pgwire::session` | `DEBUG` | `gres.session`, `gres.statement`, `gres.parse`/`bind`/`describe`/`execute` |
//! | `crabka_pgexec::statement` | `DEBUG` | `pg.parse.sql`, `db.statement`, `pg.select`, `pg.write`, `pg.ddl` |
//! | `crabka_pgexec::exec` | `DEBUG`/`TRACE` | `gres.exec_read`, `pg.execute_write`, `pg.commit` at `DEBUG`; `pg.scan`, `pg.read_context`, `pg.lock.row` at `TRACE` |
//! | `crabka_gres_ranges::route` | `DEBUG`/`TRACE` | `pg.timestamp_scatter`, `pg.prewrite`, `pg.resolve`, `gres.range_rpc`, `gres.range_serve`, `tso.grant`, `range.barrier` at `DEBUG`; `pg.route` at `TRACE` |
//! | `crabka_gres_substrate::wal` | `DEBUG`/`TRACE` | `pg.commit`, `gres.wal_append`, `gres.wal_apply` at `DEBUG`; `wal.chunk` at `TRACE` |
//!
//! Only the OTLP layer enables those targets, through [`OTEL_DEFAULT_FILTER`].
//! The stdout `fmt` layer's default, [`FMT_DEFAULT_FILTER`], deliberately names
//! none of them. Otherwise a gres that does not even export would print every
//! per-statement span to stdout.
//!
//! # Operator filter recipes
//!
//! Set `CRABKA_OTLP_FILTER` to override [`OTEL_DEFAULT_FILTER`] wholesale. The
//! pipeline reads it when it builds the OTLP layer.
//!
//! - **Statement level only** — one span per session and per statement, and
//!   nothing about routing or storage. This is the cheapest useful setting for
//!   always-on production tracing:
//!
//!   ```text
//!   CRABKA_OTLP_FILTER=info,crabka_pgwire::session=debug,crabka_pgexec::statement=debug
//!   ```
//!
//! - **Default** — the statement tier plus routing, cross-node RPC hops, the 2PC
//!   rounds and the WAL append. This is the waterfall an operator needs to tell
//!   *which step* was slow. It is [`OTEL_DEFAULT_FILTER`]. Unset
//!   `CRABKA_OTLP_FILTER` to get it.
//!
//! - **Full internal detail** — adds the per-scan, per-read-context and
//!   contended-row-lock spans. A single large scan emits many spans, so use this
//!   setting for a targeted investigation, not for steady state:
//!
//!   ```text
//!   CRABKA_OTLP_FILTER=info,crabka_pgwire::session=debug,crabka_pgexec::statement=debug,crabka_pgexec::exec=trace,crabka_gres_ranges::route=trace,crabka_gres_substrate::wal=trace
//!   ```
//!
//! # SQL text on spans
//!
//! `CRABKA_OTLP_SQL_TEXT=true` attaches the **verbatim** SQL of each statement as
//! the `db.query.text` span attribute. It is **off by default**, and it is the
//! only setting here that can export secrets or personal data. The query text
//! is the statement as the client sent it, literals included, for example
//! `INSERT INTO users VALUES ('123-45-6789', …)` or
//! `ALTER ROLE app PASSWORD 'hunter2'`. Anything that reaches the collector
//! reaches everyone who can read the trace backend.
//!
//! With the flag off, spans still carry `db.query.summary`, for example
//! `"SELECT orders"`, plus `db.operation.name`, `db.collection.name`,
//! `db.namespace` and `pg.table_id`. That is enough to group and attribute
//! latency without any literal.
//!
//! The flag is read where the attribute is recorded. `crabka-pgexec` and
//! `crabka-gres-ranges` each hold a `LazyLock<bool>`, which keeps those crates
//! free of a `crabka-telemetry` dependency. The value is therefore sampled once
//! per process at first use, and a later change to the environment has no
//! effect. This module is the single place that documents the flag.

// Re-export the generic OTLP pipeline from crabka-telemetry.
pub use crabka_telemetry::{OtlpConfig, OtlpProtocol, TelemetryError, TelemetryGuard, init};

/// Default filter for the stdout JSON `fmt` layer, used when `RUST_LOG` is
/// unset.
///
/// This names none of the five span targets on purpose. They sit at `DEBUG`,
/// so a name here would print a span line per statement, or per scan, to
/// stdout on every gres, exporting or not.
pub const FMT_DEFAULT_FILTER: &str = "crabka_gres=info,info";

/// Default filter for the OTLP layer, used when `CRABKA_OTLP_FILTER` is unset.
///
/// This enables the query-path targets at `DEBUG`: session, statement,
/// executor, routing and WAL. The `TRACE`-level spans inside those targets stay
/// off. Those are the per-scan, per-read-context, contended row lock and WAL
/// chunk spans. Widen a single target to `=trace` to get them.
pub const OTEL_DEFAULT_FILTER: &str = "info,crabka_pgwire::session=debug,crabka_pgexec::statement=debug,crabka_pgexec::exec=debug,crabka_gres_ranges::route=debug,crabka_gres_substrate::wal=debug";

/// Environment variable that gates verbatim SQL, `db.query.text`, on statement
/// spans. It is off unless set to a truthy value. The module docs explain why
/// it is off by default.
pub const SQL_TEXT_ENV: &str = "CRABKA_OTLP_SQL_TEXT";

/// `tracing` target for the pgwire session and statement spans.
pub const PGWIRE_SESSION_TARGET: &str = "crabka_pgwire::session";

/// `tracing` target for the pgexec statement tier.
pub const PGEXEC_STATEMENT_TARGET: &str = "crabka_pgexec::statement";

/// `tracing` target for the pgexec executor internals.
pub const PGEXEC_EXEC_TARGET: &str = "crabka_pgexec::exec";

/// `tracing` target for range routing, cross-node RPC and the 2PC rounds.
pub const RANGES_ROUTE_TARGET: &str = "crabka_gres_ranges::route";

/// `tracing` target for the substrate WAL append and apply paths.
pub const SUBSTRATE_WAL_TARGET: &str = "crabka_gres_substrate::wal";

/// Resolve `service.instance.id`, the resource attribute that separates one
/// gres process's spans from another's in the trace backend. `get` is the
/// environment lookup, injected so that this stays a pure, testable function.
///
/// `OTEL_SERVICE_INSTANCE_ID` wins when set, so a deployment can pin the id to
/// whatever it already calls the node, such as a pod name. Otherwise the id
/// comes from the node's own addresses. The first choice is the advertised
/// range endpoint, which is what the cluster identifies a compute node by; see
/// `node_identity`. The fallback is the `PostgreSQL` listen address, for a
/// single-node gres that serves no ranges. Both are stable across a restart,
/// unlike a generated uuid, so a node's traces stay attributable across a roll.
///
/// The function prefixes `HOSTNAME` when the container runtime provides it,
/// because the listen addresses default to loopback. Without that prefix, every
/// pod of a `StatefulSet` left on the defaults would report the same instance
/// id and collapse into one resource.
///
/// A configured port of `0` asks the kernel for an ephemeral port, so the
/// address is not an identity at all. Every such process would report
/// `127.0.0.1:0` and collapse into a single resource, which is exactly the
/// failure the `HOSTNAME` prefix guards against. Those ids get a random suffix.
/// The suffix is a random value rather than the process id, because a
/// containerized gres is pid 1, so pids collide across precisely the processes
/// that need separating. This loses no stability: a node that let the kernel
/// pick its port has no stable address to be identified by.
pub fn service_instance_id(
    serve: &crate::ServeArgs,
    get: impl Fn(&str) -> Option<String>,
) -> String {
    use rand::RngExt as _;

    if let Some(id) = get("OTEL_SERVICE_INSTANCE_ID")
        .map(|id| id.trim().to_owned())
        .filter(|id| !id.is_empty())
    {
        return id;
    }
    let endpoint = serve.range_listen.as_deref().unwrap_or(&serve.listen);
    let id = match get("HOSTNAME").filter(|host| !host.is_empty()) {
        Some(host) => format!("{host}/{endpoint}"),
        None => endpoint.to_owned(),
    };
    if ephemeral_port(endpoint) {
        let nonce: u64 = rand::rng().random();
        format!("{id}#{nonce:016x}")
    } else {
        id
    }
}

/// Whether `endpoint` asks the kernel for an ephemeral port, that is, whether
/// its port component is `0`. It handles the bracketed IPv6 form `[::1]:0`,
/// because it only ever looks after the last colon.
fn ephemeral_port(endpoint: &str) -> bool {
    endpoint
        .rsplit_once(':')
        .and_then(|(_, port)| port.trim().parse::<u16>().ok())
        == Some(0)
}

#[cfg(test)]
mod tests {
    use assert2::{assert, check};
    use tracing_subscriber::EnvFilter;

    use super::*;

    /// Serve arguments as the binary would parse them, so the tests exercise the
    /// same defaults `main` runs with.
    fn serve_args(arguments: &[&str]) -> crate::ServeArgs {
        let mut argv = vec!["crabka-gres"];
        argv.extend_from_slice(arguments);
        crate::Cli::try_parse_from(argv)
            .expect("serve arguments")
            .serve
    }

    /// The same, for a node that advertises a range endpoint. `--range-listen`
    /// is accepted only alongside the multi-range substrate arguments.
    fn range_serve_args(range_listen: &str) -> crate::ServeArgs {
        serve_args(&[
            "--substrate-bootstrap",
            "memory://",
            "--tenant",
            "tenant-a",
            "--ranges",
            "0",
            "--range-listen",
            range_listen,
        ])
    }

    /// Both defaults must parse. A typo would silently degrade to whatever
    /// `EnvFilter::new` salvages, which is how a target quietly stops
    /// exporting.
    #[test]
    fn default_filters_parse() {
        check!(EnvFilter::try_new(FMT_DEFAULT_FILTER).is_ok());
        check!(EnvFilter::try_new(OTEL_DEFAULT_FILTER).is_ok());
    }

    /// The stdout filter must not name a span target. Otherwise a gres with no
    /// OTLP configured at all prints every statement span to stdout.
    #[test]
    fn fmt_filter_names_no_span_target() {
        for target in [
            PGWIRE_SESSION_TARGET,
            PGEXEC_STATEMENT_TARGET,
            PGEXEC_EXEC_TARGET,
            RANGES_ROUTE_TARGET,
            SUBSTRATE_WAL_TARGET,
        ] {
            assert!(!FMT_DEFAULT_FILTER.contains(target), "target {target}");
        }
    }

    /// The OTLP filter must enable every span target. Otherwise a whole tier of
    /// the waterfall is missing by default.
    #[test]
    fn otel_filter_enables_every_span_target() {
        for target in [
            PGWIRE_SESSION_TARGET,
            PGEXEC_STATEMENT_TARGET,
            PGEXEC_EXEC_TARGET,
            RANGES_ROUTE_TARGET,
            SUBSTRATE_WAL_TARGET,
        ] {
            assert!(
                OTEL_DEFAULT_FILTER.contains(&format!("{target}=debug")),
                "target {target}"
            );
        }
    }

    /// `OtlpConfig::from_env` keeps OTLP off by default. With no OTLP
    /// environment at all it yields `None`, and `init` then installs the stdout
    /// layer alone.
    #[test]
    fn otlp_disabled_without_environment() {
        let cfg = OtlpConfig::from_env(|_| None, "gres-1", "0.0.0", "crabka-gres")
            .expect("valid OTLP configuration");
        assert!(cfg.is_none());
    }

    /// `--range-listen …:0` binds an ephemeral port, so the configured address
    /// identifies nothing. Every process would report `127.0.0.1:0`, and its
    /// spans would land in one resource with every other node's.
    #[test]
    fn ephemeral_range_port_yields_a_distinct_id_per_process() {
        let serve = range_serve_args("127.0.0.1:0");

        let first = service_instance_id(&serve, |_| None);
        let second = service_instance_id(&serve, |_| None);

        check!(first != second);
        check!(first.starts_with("127.0.0.1:0#"));
        check!(second.starts_with("127.0.0.1:0#"));
    }

    /// The `PostgreSQL` listen address is the fallback identity for a gres that
    /// serves no ranges, and it takes the same treatment.
    #[test]
    fn ephemeral_sql_port_yields_a_distinct_id_per_process() {
        let serve = serve_args(&["--listen", "127.0.0.1:0"]);

        let first = service_instance_id(&serve, |_| None);
        let second = service_instance_id(&serve, |_| None);

        check!(first != second);
        check!(first.starts_with("127.0.0.1:0#"));
    }

    /// A deployment that pins the id, to a pod name for example, keeps it,
    /// ephemeral port or not.
    #[test]
    fn pinned_instance_id_wins() {
        let serve = range_serve_args("127.0.0.1:0");

        let id = service_instance_id(&serve, |key| match key {
            "OTEL_SERVICE_INSTANCE_ID" => Some("  gres-0  ".to_owned()),
            "HOSTNAME" => Some("gres-0.gres".to_owned()),
            _ => None,
        });

        assert!(id == "gres-0");
    }

    /// A real configured port is a stable identity across a restart, which is
    /// worth more than uniqueness from a nonce. It must not pick up a
    /// suffix.
    #[test]
    fn configured_port_keeps_a_stable_id() {
        let serve = range_serve_args("10.0.0.4:7654");

        let first = service_instance_id(&serve, |_| None);
        let second = service_instance_id(&serve, |key| {
            (key == "HOSTNAME").then(|| "gres-2".to_owned())
        });

        check!(first == "10.0.0.4:7654");
        check!(second == "gres-2/10.0.0.4:7654");
    }

    #[test]
    fn otlp_enabled_by_endpoint() {
        let cfg = OtlpConfig::from_env(
            |key| (key == "CRABKA_OTLP_ENDPOINT").then(|| "http://collector:4317".to_owned()),
            "gres-1",
            "0.0.0",
            "crabka-gres",
        )
        .expect("valid OTLP configuration")
        .expect("OTLP enabled by endpoint");
        check!(cfg.endpoint == "http://collector:4317");
        check!(cfg.service_name == "crabka-gres");
        check!(cfg.service_instance_id == "gres-1");
    }
}
