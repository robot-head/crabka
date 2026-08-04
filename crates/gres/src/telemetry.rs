//! Gres tracing + OTLP distributed-tracing pipeline.
//!
//! `crabka-gres` always installs a structured-JSON `tracing_subscriber` `fmt`
//! layer (stdout, gated by the usual `RUST_LOG` `EnvFilter`) so container log
//! collectors ingest fields rather than ANSI text. When OTLP export is
//! configured via the environment, a second `tracing-opentelemetry` layer is
//! attached that converts `tracing` spans into OpenTelemetry spans and
//! batch-exports them over OTLP to a collector (gRPC `:4317` or HTTP/protobuf
//! `:4318`). The pipeline itself is [`crabka_telemetry`]; this module only
//! supplies the gres-specific filters and documents the knobs.
//!
//! # Enabling
//!
//! OTLP is **off by default** — a gres with no OTLP environment behaves exactly
//! as before, stdout logging only. It turns on when any endpoint is set
//! (`CRABKA_OTLP_ENDPOINT`, `OTEL_EXPORTER_OTLP_TRACES_ENDPOINT`,
//! `OTEL_EXPORTER_OTLP_ENDPOINT`) or `CRABKA_OTLP_ENABLED=true`, and is
//! force-disabled by `OTEL_SDK_DISABLED=true`.
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
//! Only the OTLP layer enables those targets, via [`OTEL_DEFAULT_FILTER`]. The
//! stdout `fmt` layer's default ([`FMT_DEFAULT_FILTER`]) deliberately names none
//! of them: every per-statement span would otherwise be printed to stdout on a
//! gres that is not even exporting.
//!
//! # Operator filter recipes
//!
//! Set `CRABKA_OTLP_FILTER` to override [`OTEL_DEFAULT_FILTER`] wholesale; it is
//! read by the pipeline when the OTLP layer is built.
//!
//! - **Statement level only** — one span per session and per statement, nothing
//!   about routing or storage. Cheapest useful setting for always-on production
//!   tracing:
//!
//!   ```text
//!   CRABKA_OTLP_FILTER=info,crabka_pgwire::session=debug,crabka_pgexec::statement=debug
//!   ```
//!
//! - **Default** — the statement tier plus routing, cross-node RPC hops, the 2PC
//!   rounds and the WAL append; the waterfall an operator needs to tell *which
//!   step* was slow. This is [`OTEL_DEFAULT_FILTER`]; unset `CRABKA_OTLP_FILTER`
//!   to get it.
//!
//! - **Full internal detail** — adds the per-scan, per-read-context and
//!   contended-row-lock spans. A single large scan emits many spans, so use it
//!   for a targeted investigation, not steady state:
//!
//!   ```text
//!   CRABKA_OTLP_FILTER=info,crabka_pgwire::session=debug,crabka_pgexec::statement=debug,crabka_pgexec::exec=trace,crabka_gres_ranges::route=trace,crabka_gres_substrate::wal=trace
//!   ```
//!
//! # SQL text on spans
//!
//! `CRABKA_OTLP_SQL_TEXT=true` attaches the **verbatim** SQL of each statement as
//! the `db.query.text` span attribute. It is **off by default** and is the only
//! setting here that can export secrets or personal data: the query text is the
//! statement as the client sent it, literals included — `INSERT INTO users
//! VALUES ('123-45-6789', …)`, `ALTER ROLE app PASSWORD 'hunter2'`. Anything
//! that reaches the collector reaches everyone who can read the trace backend.
//!
//! With it off, spans still carry `db.query.summary` (for example
//! `"SELECT orders"`), plus `db.operation.name`, `db.collection.name`,
//! `db.namespace` and `pg.table_id` — enough to group and attribute latency
//! without reproducing any literal.
//!
//! The flag is read where the attribute is recorded (`crabka-pgexec` and
//! `crabka-gres-ranges` each hold a `LazyLock<bool>`, which keeps those crates
//! free of a `crabka-telemetry` dependency), so the value is sampled once per
//! process at first use and changing the environment afterwards has no effect.
//! This module is the single place it is documented.

// Re-export the generic OTLP pipeline from crabka-telemetry.
pub use crabka_telemetry::{OtlpConfig, OtlpProtocol, TelemetryError, TelemetryGuard, init};

/// Default filter for the stdout JSON `fmt` layer, used when `RUST_LOG` is
/// unset.
///
/// Names none of the five span targets on purpose: they sit at `DEBUG`, so
/// naming one here would print a span line per statement — or per scan — to
/// stdout on every gres, exporting or not.
pub const FMT_DEFAULT_FILTER: &str = "crabka_gres=info,info";

/// Default filter for the OTLP layer, used when `CRABKA_OTLP_FILTER` is unset.
///
/// Enables the query-path targets at `DEBUG`: session, statement, executor,
/// routing and WAL. The `TRACE`-level spans within those targets (per-scan,
/// per-read-context, contended row locks, WAL chunks) stay off; widen a single
/// target to `=trace` to get them.
pub const OTEL_DEFAULT_FILTER: &str = "info,crabka_pgwire::session=debug,crabka_pgexec::statement=debug,crabka_pgexec::exec=debug,crabka_gres_ranges::route=debug,crabka_gres_substrate::wal=debug";

/// Environment variable gating verbatim SQL (`db.query.text`) on statement
/// spans. Off unless set to a truthy value; see the module docs for why it is
/// off by default.
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

/// Resolve `service.instance.id` — the resource attribute that separates one
/// gres process's spans from another's in the trace backend. `get` is the
/// environment lookup, injected so this is a pure, testable function.
///
/// `OTEL_SERVICE_INSTANCE_ID` wins when set, so a deployment can pin the id to
/// whatever it already calls the node (a pod name, say). Otherwise the id is
/// derived from the node's own addresses: the advertised range endpoint, which
/// is what the cluster identifies a compute node by (see `node_identity`),
/// falling back to the `PostgreSQL` listen address for a single-node gres that
/// serves no ranges. Both are stable across a restart, unlike a generated uuid,
/// so a node's traces stay attributable across a roll.
///
/// `HOSTNAME` is prefixed when the container runtime provides it, because the
/// listen addresses default to loopback: without it, every pod of a `StatefulSet`
/// left on the defaults would report the same instance id and collapse into one
/// resource.
///
/// A configured port of `0` asks the kernel for an ephemeral port, so the
/// address is not an identity at all — every such process would report
/// `127.0.0.1:0` and collapse into a single resource, exactly the failure the
/// `HOSTNAME` prefix guards against. Those ids get a random suffix. A random
/// value rather than the process id because a containerized gres is pid 1, so
/// pids collide across precisely the processes that need separating; and no
/// stability is lost, since a node that let the kernel pick its port has no
/// stable address to be identified by in the first place.
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
/// its port component is `0`. Handles the bracketed IPv6 form (`[::1]:0`) by
/// only ever looking after the last colon.
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

    /// The same, for a node that advertises a range endpoint: `--range-listen`
    /// is only accepted alongside the multi-range substrate arguments.
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

    /// Both defaults must parse — a typo would silently degrade to whatever
    /// `EnvFilter::new` salvages, which is how a target quietly stops exporting.
    #[test]
    fn default_filters_parse() {
        check!(EnvFilter::try_new(FMT_DEFAULT_FILTER).is_ok());
        check!(EnvFilter::try_new(OTEL_DEFAULT_FILTER).is_ok());
    }

    /// The stdout filter must not name a span target, or every statement span
    /// is printed to stdout on a gres with no OTLP configured at all.
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

    /// The OTLP filter must enable every span target, else a whole tier of the
    /// waterfall is missing by default.
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

    /// `OtlpConfig::from_env` is what keeps OTLP off by default: with no OTLP
    /// environment at all it yields `None`, and `init` then installs the stdout
    /// layer alone.
    #[test]
    fn otlp_disabled_without_environment() {
        let cfg = OtlpConfig::from_env(|_| None, "gres-1", "0.0.0", "crabka-gres")
            .expect("valid OTLP configuration");
        assert!(cfg.is_none());
    }

    /// `--range-listen …:0` binds an ephemeral port, so the configured address
    /// identifies nothing: every process would report `127.0.0.1:0` and its
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

    /// A deployment that pins the id — to a pod name, say — keeps it, ephemeral
    /// port or not.
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
    /// worth more than uniqueness-by-nonce: it must not pick up a suffix.
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
