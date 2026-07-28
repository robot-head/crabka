//! Typed `PgDog` configuration renderers for Gres fleets.
//!
//! The renderers intentionally serialize typed structs through `toml` instead of
//! assembling text by hand. When the pinned `PgDog` image changes, re-run the G-4
//! front-door e2e leg against these goldens before accepting any output change.

use std::{collections::HashSet, str::FromStr};

use crabka_units::{Time, convert::TimeExt as _, days, minutes, secs};
use refined_type::rule::GreaterU16;
use serde::Serialize;

use crate::{ControlError, TenantState};

const DEFAULT_LISTEN_PORT: u16 = 6_432;
const DEFAULT_COLD_START_CEILING: Time = secs(30);
const DEFAULT_IDLE_TIMEOUT: Time = minutes(1);
const DEFAULT_SERVER_LIFETIME: Time = minutes(5);
/// The shortest timeout `PgDog` is ever configured with, so a derived value
/// cannot round down to something it would reject.
const MINIMUM_TIMEOUT: Time = secs(1);
// PgDog's documented way to disable proactive database healthchecks. This is
// required for scale-to-zero routes: an ephemeral idle healthcheck must not
// become the event that wakes a suspended tenant. A century, which is how PgDog
// documents "never".
const DISABLED_IDLE_HEALTHCHECK_DELAY: Time = days(36_525);

/// One tenant route available to `PgDog`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TenantEndpoint {
    /// Database name exposed at the `PgDog` front door.
    pub name: String,
    /// Backend compute host for active tenants.
    pub backend_host: String,
    /// Backend compute port for active tenants.
    pub backend_port: u16,
    /// Tenant lifecycle state from the registry.
    pub state: TenantState,
    /// Optional per-tenant pooler mode override.
    pub pooler_mode: Option<PgdogPoolerMode>,
}

/// `PgDog` pooler mode rendered in `PgDog`'s lower-case spelling.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum PgdogPoolerMode {
    /// Reuse server connections only across transaction boundaries.
    #[default]
    Transaction,
    /// Keep one server connection bound to one client session.
    Session,
}

/// A positive number of backend connection attempts.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PgdogConnectAttempts(u16);

impl PgdogConnectAttempts {
    /// Validate a backend connection attempt count.
    ///
    /// # Errors
    ///
    /// Returns an error when `value` is zero.
    pub fn new(value: u16) -> Result<Self, String> {
        GreaterU16::<0>::new(value)
            .map(|value| Self(value.into_value()))
            .map_err(|error| error.to_string())
    }

    /// Return the validated value.
    #[must_use]
    pub const fn into_value(self) -> u16 {
        self.0
    }
}

impl FromStr for PgdogConnectAttempts {
    type Err = String;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        value
            .parse()
            .map_err(|error: std::num::ParseIntError| error.to_string())
            .and_then(Self::new)
    }
}

/// Explicit timeout overrides for a `PgDog` render.
///
/// Not `Eq`: a [`Time`] is `f64`-backed, so it is only `PartialEq`.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct PgdogTimeouts {
    /// Backend connection attempt timeout.
    pub connect_timeout: Time,
    /// Time a client may wait for a pooled server connection.
    pub checkout_timeout: Time,
}

impl PgdogTimeouts {
    /// Derive the total cold-start ceiling from one connection-attempt timeout.
    #[must_use]
    pub fn cold_start_ceiling_for_attempt_timeout(
        attempt_timeout: Time,
        connect_attempts: PgdogConnectAttempts,
    ) -> Time {
        attempt_timeout * f64::from(connect_attempts.into_value())
    }

    /// Build timeout values that can cover the supplied cold-start ceiling.
    #[must_use]
    pub fn for_cold_start_ceiling(
        cold_start_ceiling: Time,
        connect_attempts: PgdogConnectAttempts,
    ) -> Self {
        let attempt_count = f64::from(connect_attempts.into_value());

        Self {
            connect_timeout: at_least(cold_start_ceiling / attempt_count, MINIMUM_TIMEOUT),
            checkout_timeout: at_least(cold_start_ceiling, MINIMUM_TIMEOUT),
        }
    }

    fn ensure_covers_cold_start(
        self,
        cold_start_ceiling: Time,
        connect_attempts: PgdogConnectAttempts,
    ) -> Result<Self, ControlError> {
        let attempt_count = f64::from(connect_attempts.into_value());
        let total_budget = self.connect_timeout * attempt_count + self.checkout_timeout;

        if total_budget < cold_start_ceiling {
            return Err(ControlError::invalid_field(
                "cold_start_ceiling",
                "connect and checkout timeout budget must cover cold-start ceiling",
            ));
        }

        Ok(self)
    }
}

/// `PgDog` user entry rendered to `users.toml` for local/dev deployments.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PgdogUser {
    /// `PostgreSQL` role name.
    pub name: String,
    /// `PgDog` database route this user may access.
    pub database: String,
    /// Optional PgDog-local password. Omit it for passthrough skeleton entries.
    pub password: Option<String>,
}

/// General `PgDog` settings shared by the rendered fleet.
///
/// Not `Eq`: the timeout fields are `f64`-backed quantities.
#[derive(Debug, Clone, PartialEq)]
pub struct PgdogGeneral {
    /// `PgDog` frontend listen port.
    pub listen_port: u16,
    /// Optional TLS certificate path for client connections.
    pub tls_cert_path: Option<String>,
    /// Optional TLS private-key path for client connections.
    pub tls_key_path: Option<String>,
    /// Optional CA bundle used to authenticate required client certificates.
    pub tls_client_ca_path: Option<String>,
    /// Enable `PgDog` passthrough authentication.
    pub passthrough_auth: bool,
    /// Fleet-wide pooler mode.
    pub pooler_mode: PgdogPoolerMode,
    /// Maximum acceptable tenant wake latency.
    pub cold_start_ceiling: Time,
    /// Number of backend connection attempts.
    pub connect_attempts: PgdogConnectAttempts,
    /// Optional explicit timeout knobs. Omitted values are derived from the ceiling.
    pub timeouts: Option<PgdogTimeouts>,
    /// Idle pooled-server disconnect window.
    pub idle_timeout: Time,
    /// Maximum lifetime for pooled backend connections.
    pub server_lifetime: Time,
    /// Optional local/dev users rendered to `users.toml`.
    pub users: Vec<PgdogUser>,
}

impl Default for PgdogGeneral {
    fn default() -> Self {
        Self {
            listen_port: DEFAULT_LISTEN_PORT,
            tls_cert_path: None,
            tls_key_path: None,
            tls_client_ca_path: None,
            passthrough_auth: true,
            pooler_mode: PgdogPoolerMode::Transaction,
            cold_start_ceiling: DEFAULT_COLD_START_CEILING,
            connect_attempts: PgdogConnectAttempts::new(3)
                .expect("the default PgDog connect attempt count is positive"),
            timeouts: None,
            idle_timeout: DEFAULT_IDLE_TIMEOUT,
            server_lifetime: DEFAULT_SERVER_LIFETIME,
            users: Vec::new(),
        }
    }
}

/// Inputs needed to render `PgDog` configuration files.
#[derive(Debug, Clone)]
pub struct PgdogRenderInput<'a> {
    /// Tenant routes in registry order.
    pub tenants: &'a [TenantEndpoint],
    /// Activator route used for suspended tenants when present.
    pub activator: Option<(String, u16)>,
    /// General `PgDog` settings.
    pub general: PgdogGeneral,
}

/// Render `pgdog.toml` for `PgDog`.
///
/// # Errors
///
/// Returns an error when tenants are duplicated, timeout budgets cannot cover the
/// cold-start ceiling, route fields are empty, or TOML serialization fails.
pub fn render_pgdog_toml(input: &PgdogRenderInput<'_>) -> Result<String, ControlError> {
    let config = PgdogConfig::try_from_input(input)?;
    toml::to_string_pretty(&config).map_err(ControlError::from)
}

/// Render `users.toml` for `PgDog`.
///
/// # Errors
///
/// Returns an error when user entries are duplicated or TOML serialization fails.
pub fn render_users_toml(input: &PgdogRenderInput<'_>) -> Result<String, ControlError> {
    let config = UsersConfig::try_from_general(&input.general)?;
    toml::to_string_pretty(&config).map_err(ControlError::from)
}

#[derive(Debug, Serialize)]
struct PgdogConfig<'a> {
    general: RenderGeneral<'a>,
    databases: Vec<RenderDatabase<'a>>,
}

impl<'a> PgdogConfig<'a> {
    fn try_from_input(input: &'a PgdogRenderInput<'a>) -> Result<Self, ControlError> {
        if input.general.tls_cert_path.is_some() != input.general.tls_key_path.is_some() {
            return Err(ControlError::invalid_field(
                "frontend_tls",
                "TLS certificate and private key must be configured together",
            ));
        }
        if input.general.tls_client_ca_path.is_some() && input.general.tls_cert_path.is_none() {
            return Err(ControlError::invalid_field(
                "frontend_tls",
                "client CA requires a TLS certificate and private key",
            ));
        }
        let timeouts = input
            .general
            .timeouts
            .unwrap_or_else(|| {
                PgdogTimeouts::for_cold_start_ceiling(
                    input.general.cold_start_ceiling,
                    input.general.connect_attempts,
                )
            })
            .ensure_covers_cold_start(
                input.general.cold_start_ceiling,
                input.general.connect_attempts,
            )?;

        let general = RenderGeneral::from_settings(&input.general, timeouts);
        let databases = render_databases(input)?;

        Ok(Self { general, databases })
    }
}

/// The `[general]` table exactly as `PgDog` parses it.
///
/// This is a wire boundary: every field holds the raw form `PgDog` reads —
/// milliseconds for the timeouts — and the surrounding logic converts once, in
/// [`RenderGeneral::from_settings`].
#[derive(Debug, Serialize)]
struct RenderGeneral<'a> {
    port: u16,
    min_pool_size: u32,
    pooler_mode: PgdogPoolerMode,
    passthrough_auth: &'static str,
    connect_timeout: u64,
    connect_attempts: u16,
    checkout_timeout: u64,
    idle_timeout: u64,
    server_lifetime: u64,
    idle_healthcheck_delay: u64,
    #[serde(skip_serializing_if = "Option::is_none")]
    tls_certificate: Option<&'a str>,
    #[serde(skip_serializing_if = "Option::is_none")]
    tls_private_key: Option<&'a str>,
    #[serde(skip_serializing_if = "Option::is_none")]
    tls_client_ca_certificate: Option<&'a str>,
    tls_client_required: bool,
}

impl<'a> RenderGeneral<'a> {
    fn from_settings(general: &'a PgdogGeneral, timeouts: PgdogTimeouts) -> Self {
        Self {
            port: general.listen_port,
            // Scale-to-zero tenants cannot suspend while PgDog holds an eager
            // backend session open. Connections are created on demand instead.
            min_pool_size: 0,
            pooler_mode: general.pooler_mode,
            passthrough_auth: if general.passthrough_auth {
                "enabled"
            } else {
                "disabled"
            },
            connect_timeout: milliseconds_rounded_up(timeouts.connect_timeout),
            connect_attempts: general.connect_attempts.into_value(),
            checkout_timeout: milliseconds_rounded_up(timeouts.checkout_timeout),
            idle_timeout: milliseconds_rounded_up(general.idle_timeout),
            server_lifetime: milliseconds_rounded_up(general.server_lifetime),
            idle_healthcheck_delay: milliseconds_rounded_up(DISABLED_IDLE_HEALTHCHECK_DELAY),
            tls_certificate: general.tls_cert_path.as_deref(),
            tls_private_key: general.tls_key_path.as_deref(),
            tls_client_ca_certificate: general.tls_client_ca_path.as_deref(),
            tls_client_required: general.tls_cert_path.is_some(),
        }
    }
}

#[derive(Debug, Serialize)]
struct RenderDatabase<'a> {
    name: &'a str,
    host: &'a str,
    port: u16,
    pooler_mode: PgdogPoolerMode,
}

#[derive(Debug, Serialize)]
struct UsersConfig<'a> {
    #[serde(skip_serializing_if = "Vec::is_empty")]
    users: Vec<RenderUser<'a>>,
}

impl<'a> UsersConfig<'a> {
    fn try_from_general(general: &'a PgdogGeneral) -> Result<Self, ControlError> {
        let mut seen_users = HashSet::with_capacity(general.users.len());
        let mut users = Vec::with_capacity(general.users.len());

        for user in &general.users {
            if user.name.is_empty() {
                return Err(ControlError::invalid_field(
                    "user name",
                    "must not be empty",
                ));
            }
            if user.database.is_empty() {
                return Err(ControlError::invalid_field(
                    "user database",
                    "must not be empty",
                ));
            }
            if user.password.as_ref().is_some_and(String::is_empty) {
                return Err(ControlError::invalid_field(
                    "user password",
                    "must not be empty",
                ));
            }
            if !seen_users.insert(user.name.as_str()) {
                return Err(ControlError::invalid_field("user name", "must be unique"));
            }

            users.push(RenderUser {
                name: user.name.as_str(),
                database: user.database.as_str(),
                password: user.password.as_deref(),
            });
        }

        Ok(Self { users })
    }
}

#[derive(Debug, Serialize)]
struct RenderUser<'a> {
    name: &'a str,
    database: &'a str,
    #[serde(skip_serializing_if = "Option::is_none")]
    password: Option<&'a str>,
}

fn render_databases<'a>(
    input: &'a PgdogRenderInput<'a>,
) -> Result<Vec<RenderDatabase<'a>>, ControlError> {
    let mut seen_tenants = HashSet::with_capacity(input.tenants.len());
    let mut databases = Vec::with_capacity(input.tenants.len());

    for tenant in input.tenants {
        if tenant.name.is_empty() {
            return Err(ControlError::invalid_field(
                "tenant name",
                "must not be empty",
            ));
        }
        if !seen_tenants.insert(tenant.name.as_str()) {
            return Err(ControlError::invalid_field("tenant name", "must be unique"));
        }

        match tenant.state {
            TenantState::Active => push_database(
                &mut databases,
                tenant,
                &tenant.backend_host,
                tenant.backend_port,
                input.general.pooler_mode,
            )?,
            TenantState::Parking | TenantState::Suspended | TenantState::ResumeRequested => {
                if let Some((activator_host, activator_port)) = &input.activator {
                    push_database(
                        &mut databases,
                        tenant,
                        activator_host,
                        *activator_port,
                        input.general.pooler_mode,
                    )?;
                }
            }
        }
    }

    Ok(databases)
}

fn push_database<'a>(
    databases: &mut Vec<RenderDatabase<'a>>,
    tenant: &'a TenantEndpoint,
    host: &'a str,
    port: u16,
    fleet_pooler_mode: PgdogPoolerMode,
) -> Result<(), ControlError> {
    if host.is_empty() {
        return Err(ControlError::invalid_field(
            "backend host",
            "must not be empty",
        ));
    }
    if port == 0 {
        return Err(ControlError::invalid_field(
            "backend port",
            "must be greater than zero",
        ));
    }

    databases.push(RenderDatabase {
        name: tenant.name.as_str(),
        host,
        port,
        pooler_mode: tenant.pooler_mode.unwrap_or(fleet_pooler_mode),
    });

    Ok(())
}

/// The larger of two extents.
///
/// A [`Time`] is `f64`-backed and so only `PartialOrd`, which rules out
/// [`Ord::max`].
fn at_least(extent: Time, floor: Time) -> Time {
    if extent < floor { floor } else { extent }
}

/// One extent as the whole milliseconds `PgDog` reads, never rounding a timeout
/// down to a shorter one than was configured.
///
/// Truncating and then correcting is deliberate: [`TimeExt::millis_i64`] rounds
/// to nearest, which would report a 1.6 ms timeout as 2 ms and a 2.4 ms timeout
/// as 2 ms. [`TimeExt::millis_i64_trunc`] divides the exact nanosecond count, so
/// the round-trip comparison detects a sub-millisecond remainder without ever
/// tripping on float error in a whole-millisecond value.
fn milliseconds_rounded_up(extent: Time) -> u64 {
    let whole = extent.millis_i64_trunc();
    let millis = if Time::from_millis(whole) < extent {
        whole.saturating_add(1)
    } else {
        whole
    };
    u64::try_from(millis).unwrap_or(0)
}

#[cfg(test)]
mod tests {
    use assert2::{assert, check};
    use crabka_units::millis;

    use super::*;

    const EXPECTED_DEFAULT_PGDOG: &str = include_str!("../tests/golden/pgdog-default.toml");
    const EXPECTED_PGDOG: &str = include_str!("../tests/golden/pgdog.toml");
    const EXPECTED_USERS: &str = include_str!("../tests/golden/users.toml");

    #[test]
    fn connect_attempts_accept_positive_u16_boundaries() {
        assert!("1".parse::<PgdogConnectAttempts>().is_ok());
        assert!("65535".parse::<PgdogConnectAttempts>().is_ok());
        assert!("0".parse::<PgdogConnectAttempts>().is_err());
    }

    #[test]
    fn renders_exact_default_pgdog_toml() {
        let tenants = vec![active_tenant("blue", "blue-0.gres.svc", 5432)];
        let input = PgdogRenderInput {
            tenants: &tenants,
            activator: None,
            general: PgdogGeneral::default(),
        };

        let rendered = render_pgdog_toml(&input).expect("default PgDog render succeeds");

        assert!(rendered == EXPECTED_DEFAULT_PGDOG);
    }

    #[test]
    fn renders_pgdog_toml_from_typed_config() {
        let general = test_general();
        let tenants = test_tenants();
        let input = PgdogRenderInput {
            tenants: &tenants,
            activator: None,
            general,
        };

        let rendered = render_pgdog_toml(&input).expect("pgdog render succeeds");

        assert!(rendered == EXPECTED_PGDOG);
        check!(rendered.contains("tls_certificate = \"/etc/pgdog/tls/tls.crt\""));
        check!(rendered.contains("tls_private_key = \"/etc/pgdog/tls/tls.key\""));
        check!(rendered.contains("tls_client_required = true"));
        check!(rendered.contains("port = 6432"));
        check!(!rendered.contains("listen_port"));
        check!(!rendered.contains("tls_cert_path"));
        check!(!rendered.contains("tls_key_path"));
    }

    #[test]
    fn rejects_partial_frontend_tls_configuration() {
        let general = PgdogGeneral {
            tls_cert_path: Some("/etc/pgdog/tls/tls.crt".to_owned()),
            tls_key_path: None,
            ..PgdogGeneral::default()
        };
        let tenants = vec![active_tenant("blue", "blue-0.gres.svc", 5432)];
        let input = PgdogRenderInput {
            tenants: &tenants,
            activator: None,
            general,
        };

        let error = render_pgdog_toml(&input).expect_err("partial TLS must fail");

        assert!(
            error
                .to_string()
                .contains("TLS certificate and private key")
        );
    }

    #[test]
    fn requires_tls_clients_whenever_frontend_tls_is_configured() {
        struct Case {
            name: &'static str,
            certificate: Option<&'static str>,
            private_key: Option<&'static str>,
            client_ca: Option<&'static str>,
        }

        let cases = [
            Case {
                name: "plaintext frontend",
                certificate: None,
                private_key: None,
                client_ca: None,
            },
            Case {
                name: "server-authenticated TLS",
                certificate: Some("/tls/server.crt"),
                private_key: Some("/tls/server.key"),
                client_ca: None,
            },
            Case {
                name: "mutual TLS",
                certificate: Some("/tls/server.crt"),
                private_key: Some("/tls/server.key"),
                client_ca: Some("/tls/client-ca.crt"),
            },
        ];
        let tenants = vec![active_tenant("blue", "blue-0.gres.svc", 5432)];

        let actual = cases
            .into_iter()
            .map(|case| {
                let input = PgdogRenderInput {
                    tenants: &tenants,
                    activator: None,
                    general: PgdogGeneral {
                        tls_cert_path: case.certificate.map(str::to_owned),
                        tls_key_path: case.private_key.map(str::to_owned),
                        tls_client_ca_path: case.client_ca.map(str::to_owned),
                        ..PgdogGeneral::default()
                    },
                };
                let rendered = render_pgdog_toml(&input).expect("valid frontend TLS");
                let required = rendered.contains("tls_client_required = true");
                (case.name, required)
            })
            .collect::<Vec<_>>();

        assert_eq!(
            actual,
            vec![
                ("plaintext frontend", false),
                ("server-authenticated TLS", true),
                ("mutual TLS", true),
            ]
        );
    }

    #[test]
    fn renders_users_toml_from_typed_config() {
        let general = test_general();
        let tenants = test_tenants();
        let input = PgdogRenderInput {
            tenants: &tenants,
            activator: None,
            general,
        };

        let rendered = render_users_toml(&input).expect("users render succeeds");

        assert!(rendered == EXPECTED_USERS);
    }

    #[test]
    fn rejects_duplicate_tenant_names() {
        let general = PgdogGeneral::default();
        let tenants = vec![
            active_tenant("blue", "blue-0.gres.svc", 5432),
            active_tenant("blue", "blue-1.gres.svc", 5432),
        ];
        let input = PgdogRenderInput {
            tenants: &tenants,
            activator: None,
            general,
        };

        let error = render_pgdog_toml(&input).expect_err("duplicate tenants fail");

        assert!(error.to_string().contains("tenant name"));
        assert!(error.to_string().contains("unique"));
    }

    #[test]
    fn rejects_timeout_budget_below_cold_start_ceiling() {
        let general = PgdogGeneral {
            cold_start_ceiling: secs(30),
            connect_attempts: PgdogConnectAttempts::new(2).expect("positive attempts"),
            timeouts: Some(PgdogTimeouts {
                connect_timeout: secs(3),
                checkout_timeout: secs(5),
            }),
            ..PgdogGeneral::default()
        };
        let tenants = vec![active_tenant("blue", "blue-0.gres.svc", 5432)];
        let input = PgdogRenderInput {
            tenants: &tenants,
            activator: None,
            general,
        };

        let error = render_pgdog_toml(&input).expect_err("insufficient budget fails");

        assert!(error.to_string().contains("cold-start ceiling"));
    }

    #[test]
    fn cold_start_ceiling_uses_attempt_count() {
        let three = PgdogConnectAttempts::new(3).expect("positive attempts");

        let ceiling = PgdogTimeouts::cold_start_ceiling_for_attempt_timeout(secs(30), three);

        assert!(ceiling == secs(90));
    }

    #[test]
    fn derived_timeouts_use_attempt_count_and_keep_one_second_minimum() {
        let attempts = PgdogConnectAttempts::new(4).expect("positive attempts");

        let rounded = PgdogTimeouts::for_cold_start_ceiling(secs(10), attempts);
        let minimum = PgdogTimeouts::for_cold_start_ceiling(millis(1), attempts);

        check!(rounded.connect_timeout == millis(2_500));
        check!(rounded.checkout_timeout == secs(10));
        check!(minimum.connect_timeout == secs(1));
        check!(minimum.checkout_timeout == secs(1));
    }

    #[test]
    fn rendered_milliseconds_round_a_partial_millisecond_up() {
        struct Case {
            name: &'static str,
            extent: Time,
            expected: u64,
        }

        let cases = [
            Case {
                name: "whole millisecond",
                extent: millis(678),
                expected: 678,
            },
            Case {
                name: "sub-millisecond remainder rounds up",
                extent: Time::from_secs_f64(0.000_25),
                expected: 1,
            },
            Case {
                name: "an indivisible attempt share rounds up",
                extent: secs(10) / 3.0,
                expected: 3_334,
            },
            Case {
                name: "a century is exact",
                extent: DISABLED_IDLE_HEALTHCHECK_DELAY,
                expected: 3_155_760_000_000,
            },
            Case {
                name: "a negative extent floors at zero",
                extent: secs(0) - secs(1),
                expected: 0,
            },
        ];

        let actual = cases
            .iter()
            .map(|case| (case.name, milliseconds_rounded_up(case.extent)))
            .collect::<Vec<_>>();
        let expected = cases
            .iter()
            .map(|case| (case.name, case.expected))
            .collect::<Vec<_>>();

        assert!(actual == expected);
    }

    #[test]
    fn tenant_pooler_mode_overrides_fleet_mode() {
        let general = PgdogGeneral {
            pooler_mode: PgdogPoolerMode::Session,
            ..PgdogGeneral::default()
        };
        let mut overridden = active_tenant("blue", "blue-0.gres.svc", 5432);
        overridden.pooler_mode = Some(PgdogPoolerMode::Transaction);
        let tenants = vec![overridden, active_tenant("green", "green-0.gres.svc", 5432)];
        let input = PgdogRenderInput {
            tenants: &tenants,
            activator: None,
            general,
        };

        let rendered = render_pgdog_toml(&input).expect("PgDog render succeeds");

        assert!(rendered.contains(
            "name = \"blue\"\nhost = \"blue-0.gres.svc\"\nport = 5432\npooler_mode = \"transaction\""
        ));
        assert!(rendered.contains(
            "name = \"green\"\nhost = \"green-0.gres.svc\"\nport = 5432\npooler_mode = \"session\""
        ));
    }

    #[test]
    fn omits_suspended_tenants_without_activator() {
        let general = PgdogGeneral::default();
        let tenants = vec![suspended_tenant("blue")];
        let input = PgdogRenderInput {
            tenants: &tenants,
            activator: None,
            general,
        };

        let rendered = render_pgdog_toml(&input).expect("pgdog render succeeds");

        check!(!rendered.contains("[[databases]]"));
        check!(!rendered.contains("blue"));
    }

    #[test]
    fn routes_suspended_tenants_to_activator_when_present() {
        let general = PgdogGeneral::default();
        let tenants = vec![suspended_tenant("blue")];
        let input = PgdogRenderInput {
            tenants: &tenants,
            activator: Some(("activator.gres.svc".to_owned(), 6543)),
            general,
        };

        let rendered = render_pgdog_toml(&input).expect("pgdog render succeeds");

        check!(rendered.contains("name = \"blue\""));
        check!(rendered.contains("host = \"activator.gres.svc\""));
        check!(rendered.contains("port = 6543"));
    }

    #[test]
    fn routes_resume_requested_tenants_to_activator_when_present() {
        let general = PgdogGeneral::default();
        let mut tenant = suspended_tenant("blue");
        tenant.state = TenantState::ResumeRequested;
        let tenants = vec![tenant];
        let input = PgdogRenderInput {
            tenants: &tenants,
            activator: Some(("activator.gres.svc".to_owned(), 6543)),
            general,
        };

        let rendered = render_pgdog_toml(&input).expect("pgdog render succeeds");

        check!(rendered.contains("name = \"blue\""));
        check!(rendered.contains("host = \"activator.gres.svc\""));
        check!(rendered.contains("port = 6543"));
    }

    fn test_general() -> PgdogGeneral {
        PgdogGeneral {
            listen_port: 6432,
            tls_cert_path: Some("/etc/pgdog/tls/tls.crt".to_owned()),
            tls_key_path: Some("/etc/pgdog/tls/tls.key".to_owned()),
            tls_client_ca_path: Some("/etc/pgdog/tls/ca.crt".to_owned()),
            pooler_mode: PgdogPoolerMode::Session,
            cold_start_ceiling: secs(30),
            connect_attempts: PgdogConnectAttempts::new(5).expect("positive attempts"),
            idle_timeout: secs(45),
            server_lifetime: minutes(4),
            users: vec![PgdogUser {
                name: "alice".to_owned(),
                database: "blue".to_owned(),
                password: Some("SCRAM-SHA-256$4096:salt$stored:server".to_owned()),
            }],
            ..PgdogGeneral::default()
        }
    }

    fn test_tenants() -> Vec<TenantEndpoint> {
        let mut blue = active_tenant("blue", "blue-0.gres.svc", 5432);
        blue.pooler_mode = Some(PgdogPoolerMode::Transaction);
        vec![blue, active_tenant("green", "green-0.gres.svc", 5432)]
    }

    fn active_tenant(name: &str, host: &str, port: u16) -> TenantEndpoint {
        TenantEndpoint {
            name: name.to_owned(),
            backend_host: host.to_owned(),
            backend_port: port,
            state: TenantState::Active,
            pooler_mode: None,
        }
    }

    fn suspended_tenant(name: &str) -> TenantEndpoint {
        TenantEndpoint {
            name: name.to_owned(),
            backend_host: "suspended.gres.svc".to_owned(),
            backend_port: 5432,
            state: TenantState::Suspended,
            pooler_mode: None,
        }
    }
}
