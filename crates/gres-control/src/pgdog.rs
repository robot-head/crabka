//! Typed `PgDog` configuration renderers for Gres fleets.
//!
//! The renderers intentionally serialize typed structs through `toml` instead of
//! assembling text by hand. When the pinned `PgDog` image changes, re-run the G-4
//! front-door e2e leg against these goldens before accepting any output change.

use std::{collections::HashSet, time::Duration};

use serde::Serialize;

use crate::{ControlError, TenantState};

const DEFAULT_LISTEN_PORT: u16 = 6_432;
const DEFAULT_IDLE_TIMEOUT: Duration = Duration::from_mins(1);
const DEFAULT_SERVER_LIFETIME: Duration = Duration::from_mins(5);
const DEFAULT_CONNECT_ATTEMPTS: u16 = 3;

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

/// Explicit timeout overrides for a `PgDog` render.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PgdogTimeouts {
    /// Backend connection attempt timeout.
    pub connect_timeout: Duration,
    /// Number of backend connection attempts.
    pub connect_attempts: u16,
    /// Time a client may wait for a pooled server connection.
    pub checkout_timeout: Duration,
}

impl PgdogTimeouts {
    /// Build timeout values that can cover the supplied cold-start ceiling.
    #[must_use]
    pub fn for_cold_start_ceiling(cold_start_ceiling: Duration) -> Self {
        let connect_attempts = DEFAULT_CONNECT_ATTEMPTS;
        let attempt_count = u32::from(connect_attempts);
        let connect_timeout =
            divide_rounding_up(cold_start_ceiling, attempt_count).max(Duration::from_secs(1));
        let checkout_timeout = cold_start_ceiling.max(Duration::from_secs(1));

        Self {
            connect_timeout,
            connect_attempts,
            checkout_timeout,
        }
    }

    fn ensure_covers_cold_start(self, cold_start_ceiling: Duration) -> Result<Self, ControlError> {
        if self.connect_attempts == 0 {
            return Err(ControlError::invalid_field(
                "connect_attempts",
                "must be greater than zero",
            ));
        }

        let Some(connect_budget) = self
            .connect_timeout
            .checked_mul(u32::from(self.connect_attempts))
        else {
            return Err(ControlError::invalid_field(
                "connect_timeout",
                "connection budget overflowed",
            ));
        };
        let Some(total_budget) = connect_budget.checked_add(self.checkout_timeout) else {
            return Err(ControlError::invalid_field(
                "checkout_timeout",
                "timeout budget overflowed",
            ));
        };

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
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PgdogGeneral {
    /// `PgDog` frontend listen port.
    pub listen_port: u16,
    /// Optional TLS certificate path for client connections.
    pub tls_cert_path: Option<String>,
    /// Optional TLS private-key path for client connections.
    pub tls_key_path: Option<String>,
    /// Enable `PgDog` passthrough authentication.
    pub passthrough_auth: bool,
    /// Fleet-wide pooler mode.
    pub pooler_mode: PgdogPoolerMode,
    /// Maximum acceptable tenant wake latency.
    pub cold_start_ceiling: Duration,
    /// Optional explicit timeout knobs. Omitted values are derived from the ceiling.
    pub timeouts: Option<PgdogTimeouts>,
    /// Idle pooled-server disconnect window.
    pub idle_timeout: Duration,
    /// Maximum lifetime for pooled backend connections.
    pub server_lifetime: Duration,
    /// Optional local/dev users rendered to `users.toml`.
    pub users: Vec<PgdogUser>,
}

impl Default for PgdogGeneral {
    fn default() -> Self {
        Self {
            listen_port: DEFAULT_LISTEN_PORT,
            tls_cert_path: None,
            tls_key_path: None,
            passthrough_auth: true,
            pooler_mode: PgdogPoolerMode::Transaction,
            cold_start_ceiling: Duration::from_secs(30),
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
        let timeouts = input
            .general
            .timeouts
            .unwrap_or_else(|| {
                PgdogTimeouts::for_cold_start_ceiling(input.general.cold_start_ceiling)
            })
            .ensure_covers_cold_start(input.general.cold_start_ceiling)?;

        let general = RenderGeneral::from_settings(&input.general, timeouts);
        let databases = render_databases(input)?;

        Ok(Self { general, databases })
    }
}

#[derive(Debug, Serialize)]
struct RenderGeneral<'a> {
    port: u16,
    pooler_mode: PgdogPoolerMode,
    passthrough_auth: &'static str,
    connect_timeout: u64,
    connect_attempts: u16,
    checkout_timeout: u64,
    idle_timeout: u64,
    server_lifetime: u64,
    #[serde(skip_serializing_if = "Option::is_none")]
    tls_certificate: Option<&'a str>,
    #[serde(skip_serializing_if = "Option::is_none")]
    tls_private_key: Option<&'a str>,
    tls_client_required: bool,
}

impl<'a> RenderGeneral<'a> {
    fn from_settings(general: &'a PgdogGeneral, timeouts: PgdogTimeouts) -> Self {
        Self {
            port: general.listen_port,
            pooler_mode: general.pooler_mode,
            passthrough_auth: if general.passthrough_auth {
                "enabled"
            } else {
                "disabled"
            },
            connect_timeout: milliseconds_rounded_up(timeouts.connect_timeout),
            connect_attempts: timeouts.connect_attempts,
            checkout_timeout: milliseconds_rounded_up(timeouts.checkout_timeout),
            idle_timeout: milliseconds_rounded_up(general.idle_timeout),
            server_lifetime: milliseconds_rounded_up(general.server_lifetime),
            tls_certificate: general.tls_cert_path.as_deref(),
            tls_private_key: general.tls_key_path.as_deref(),
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
            )?,
            TenantState::Parking | TenantState::Suspended | TenantState::ResumeRequested => {
                if let Some((activator_host, activator_port)) = &input.activator {
                    push_database(&mut databases, tenant, activator_host, *activator_port)?;
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
        pooler_mode: tenant.pooler_mode.unwrap_or_default(),
    });

    Ok(())
}

fn divide_rounding_up(duration: Duration, divisor: u32) -> Duration {
    let nanos = duration.as_nanos().div_ceil(u128::from(divisor));
    Duration::from_nanos(u64::try_from(nanos).unwrap_or(u64::MAX))
}

fn milliseconds_rounded_up(duration: Duration) -> u64 {
    let millis = u64::try_from(duration.as_millis()).unwrap_or(u64::MAX);
    if duration.subsec_nanos().is_multiple_of(1_000_000) {
        return millis;
    }
    millis.saturating_add(1)
}

#[cfg(test)]
mod tests {
    use assert2::{assert, check};

    use super::*;

    const EXPECTED_PGDOG: &str = include_str!("../tests/golden/pgdog.toml");
    const EXPECTED_USERS: &str = include_str!("../tests/golden/users.toml");

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
            cold_start_ceiling: Duration::from_secs(30),
            timeouts: Some(PgdogTimeouts {
                connect_timeout: Duration::from_secs(3),
                connect_attempts: 2,
                checkout_timeout: Duration::from_secs(5),
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
            cold_start_ceiling: Duration::from_secs(30),
            idle_timeout: Duration::from_secs(45),
            server_lifetime: Duration::from_mins(4),
            users: vec![PgdogUser {
                name: "alice".to_owned(),
                database: "blue".to_owned(),
                password: Some("SCRAM-SHA-256$4096:salt$stored:server".to_owned()),
            }],
            ..PgdogGeneral::default()
        }
    }

    fn test_tenants() -> Vec<TenantEndpoint> {
        vec![
            active_tenant("blue", "blue-0.gres.svc", 5432),
            active_tenant("green", "green-0.gres.svc", 5432),
        ]
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
