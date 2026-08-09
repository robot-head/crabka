//! External-cluster mode: benchmark any pgwire-speaking SQL system.
//!
//! `run --external` launches no crabka cluster. It points the unchanged
//! workload, measurement, and reporting pipeline at a set of existing
//! `host:port` SQL endpoints, such as `CockroachDB`, `YugabyteDB`,
//! `PostgreSQL`, or a remote crabka cluster. This module holds everything
//! specific to that mode:
//!
//! - [`HostPort`] and [`parse_endpoint_list`] — the `host:port[,host:port...]`
//!   list of the `--external` flag, and its resolution to [`SqlEndpoint`]s.
//! - [`validate_scenario`] — the external-mode scenario constraints. There are
//!   no faults, because no chaos proxy fronts an external system.
//! - [`pids_for_ports`] — local-process discovery for resource sampling.
//!   `/proc/net/tcp{,6}` maps a listening port to its socket inode, and a scan
//!   of `/proc/<pid>/fd` maps the inode to the owning pid, under the label
//!   `ext:<port>`. This is best-effort only. On a non-Linux host, for a remote
//!   endpoint, or under restrictive permissions it finds nothing, and the run
//!   continues with an empty resource roster.
//! - [`parse_pid_overrides`] — the manual `--external-pids "label=pid,..."`
//!   override. Use it for a multi-process system, such as a `YugabyteDB`
//!   master and tserver, or when `/proc` discovery is not permitted.

use std::{
    collections::BTreeMap,
    fmt,
    net::{SocketAddr, ToSocketAddrs as _},
    str::FromStr,
};

use anyhow::{Context as _, ensure};

use crate::{
    cluster::{ProcessInfo, SqlEndpoint},
    scenario::Scenario,
};

/// One `host:port` endpoint from the `--external` flag. It stays unresolved,
/// so hostnames survive until connect time.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HostPort {
    /// Hostname or IP literal. An IPv6 literal keeps its brackets.
    pub host: String,
    /// TCP port.
    pub port: u16,
}

impl fmt::Display for HostPort {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}:{}", self.host, self.port)
    }
}

/// Error from parsing a [`HostPort`] string.
#[derive(Debug, thiserror::Error, PartialEq, Eq)]
#[error("invalid endpoint {input:?}: expected host:port")]
pub struct HostPortParseError {
    input: String,
}

impl FromStr for HostPort {
    type Err = HostPortParseError;

    fn from_str(input: &str) -> Result<Self, Self::Err> {
        let invalid = || HostPortParseError {
            input: input.to_owned(),
        };
        let (host, port) = input.rsplit_once(':').ok_or_else(invalid)?;
        if host.is_empty() {
            return Err(invalid());
        }
        let port: u16 = port.parse().map_err(|_| invalid())?;
        Ok(Self {
            host: host.to_owned(),
            port,
        })
    }
}

/// Parses the `--external` flag, a non-empty comma-separated `host:port`
/// list. The parser ignores whitespace around an entry.
///
/// # Errors
///
/// Returns an error if the list is empty or any entry is not `host:port`
/// with a valid port.
pub fn parse_endpoint_list(list: &str) -> anyhow::Result<Vec<HostPort>> {
    let endpoints: Vec<HostPort> = list
        .split(',')
        .map(|entry| entry.trim().parse())
        .collect::<Result<_, _>>()
        .context("parse --external")?;
    ensure!(
        !endpoints.is_empty(),
        "--external needs at least one host:port endpoint"
    );
    Ok(endpoints)
}

/// Parses the `--external-pids` flag, a comma-separated `label=pid` list. The
/// parser ignores whitespace around an entry.
///
/// # Errors
///
/// Returns an error if the list is empty or any entry is not `label=pid`
/// with a non-empty label and a numeric pid.
pub fn parse_pid_overrides(list: &str) -> anyhow::Result<Vec<ProcessInfo>> {
    let parse_entry = |entry: &str| -> anyhow::Result<ProcessInfo> {
        let (label, pid) = entry
            .split_once('=')
            .with_context(|| format!("invalid entry {entry:?}: expected label=pid"))?;
        ensure!(!label.is_empty(), "invalid entry {entry:?}: empty label");
        let pid: u32 = pid
            .parse()
            .with_context(|| format!("invalid entry {entry:?}: pid must be a number"))?;
        Ok(ProcessInfo {
            label: label.to_owned(),
            pid,
        })
    };
    let processes: Vec<ProcessInfo> = list
        .split(',')
        .map(|entry| parse_entry(entry.trim()))
        .collect::<Result<_, _>>()
        .context("parse --external-pids")?;
    ensure!(
        !processes.is_empty(),
        "--external-pids needs at least one label=pid entry"
    );
    Ok(processes)
}

/// Everything the `--external` flag family describes: the endpoints to drive,
/// the credentials to present, and an optional manual resource roster.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ExternalTarget {
    /// SQL endpoints to spread the workload over.
    pub endpoints: Vec<HostPort>,
    /// SQL user, from `--external-user`.
    pub user: String,
    /// SQL password, from `--external-password`. An empty value means no
    /// password.
    pub password: String,
    /// Database name, from `--external-database`.
    pub database: String,
    /// Manual resource roster, from `--external-pids`. `None` means that
    /// [`pids_for_ports`] discovers the roster.
    pub pids_override: Option<Vec<ProcessInfo>>,
}

impl ExternalTarget {
    /// Resolves every endpoint to a connectable [`SqlEndpoint`]. The first
    /// resolved address wins.
    ///
    /// # Errors
    ///
    /// Returns an error if an endpoint's host does not resolve.
    pub fn sql_endpoints(&self) -> anyhow::Result<Vec<SqlEndpoint>> {
        self.endpoints
            .iter()
            .map(|endpoint| {
                let addr = resolve(endpoint)?;
                Ok(SqlEndpoint {
                    addr,
                    user: self.user.clone(),
                    password: self.password.clone(),
                    database: self.database.clone(),
                })
            })
            .collect()
    }
}

/// Resolves one `host:port` to its first address.
fn resolve(endpoint: &HostPort) -> anyhow::Result<SocketAddr> {
    endpoint
        .to_string()
        .to_socket_addrs()
        .with_context(|| format!("resolve external endpoint {endpoint}"))?
        .next()
        .with_context(|| format!("external endpoint {endpoint} resolved to no address"))
}

/// The ports worth a local `/proc` discovery attempt: the endpoints that
/// resolved to a loopback address. The local `/proc` cannot sample a remote
/// system's processes, and a local probe of its port number could attribute an
/// unrelated local listener to it.
#[must_use]
pub fn loopback_ports(endpoints: &[SqlEndpoint]) -> Vec<u16> {
    endpoints
        .iter()
        .filter(|endpoint| endpoint.addr.ip().is_loopback())
        .map(|endpoint| endpoint.addr.port())
        .collect()
}

/// Checks external-mode scenario constraints beyond [`Scenario::validate`].
///
/// The topology still has meaning. `topology.ranges` is the number of
/// `t{i * 1_000_000}` workload tables that the harness creates, and the
/// external system spreads them by its own sharding. Faults have no meaning
/// here, because no chaos proxy fronts an external system.
///
/// # Errors
///
/// Returns an error if the scenario declares any faults.
pub fn validate_scenario(scenario: &Scenario) -> anyhow::Result<()> {
    ensure!(
        scenario.faults.is_empty(),
        "scenario {} declares {} fault(s), but external mode cannot inject faults: \
         no chaos proxies front an external system (remove the faults or run \
         against a harness-launched cluster)",
        scenario.name,
        scenario.faults.len()
    );
    Ok(())
}

/// Discovers the local processes listening on the given TCP ports via
/// `/proc`, labeled `ext:<port>`.
///
/// `/proc/net/tcp` and `/proc/net/tcp6` map each listening port to its socket
/// inode. A scan of every readable `/proc/<pid>/fd` then maps the inode to the
/// owning pid. A pid that serves several of the ports appears once, under the
/// first port in ascending order, so the resource sampler never counts a
/// process twice. The results are ordered by port and then by pid.
///
/// This function is best-effort by design. On a non-Linux host, or when
/// `/proc` entries are unreadable, such as other users' processes without
/// elevated privileges, the result is empty or partial. The caller warns and
/// continues, and `--external-pids` exists as the manual override.
#[must_use]
pub fn pids_for_ports(ports: &[u16]) -> Vec<ProcessInfo> {
    let mut inode_to_port: BTreeMap<u64, u16> = BTreeMap::new();
    for table in ["/proc/net/tcp", "/proc/net/tcp6"] {
        let Ok(contents) = std::fs::read_to_string(table) else {
            continue;
        };
        for listener in contents.lines().filter_map(parse_tcp_listener_line) {
            if ports.contains(&listener.port) {
                let port = inode_to_port.entry(listener.inode).or_insert(listener.port);
                *port = (*port).min(listener.port);
            }
        }
    }
    if inode_to_port.is_empty() {
        return Vec::new();
    }
    let mut found: Vec<(u16, u32)> = Vec::new();
    let Ok(proc_entries) = std::fs::read_dir("/proc") else {
        return Vec::new();
    };
    for entry in proc_entries.flatten() {
        let Some(pid) = entry
            .file_name()
            .to_str()
            .and_then(|name| name.parse::<u32>().ok())
        else {
            continue;
        };
        let Ok(fd_entries) = std::fs::read_dir(entry.path().join("fd")) else {
            continue;
        };
        let port = fd_entries
            .flatten()
            .filter_map(|fd| std::fs::read_link(fd.path()).ok())
            .filter_map(|target| socket_inode(target.to_str()?))
            .filter_map(|inode| inode_to_port.get(&inode).copied())
            .min();
        if let Some(port) = port {
            found.push((port, pid));
        }
    }
    found.sort_unstable();
    found
        .into_iter()
        .map(|(port, pid)| ProcessInfo {
            label: format!("ext:{port}"),
            pid,
        })
        .collect()
}

/// One listening socket parsed from a `/proc/net/tcp{,6}` line.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct TcpListener {
    port: u16,
    inode: u64,
}

/// TCP state code for `LISTEN` in `/proc/net/tcp{,6}`.
const TCP_LISTEN: &str = "0A";

/// Parses one `/proc/net/tcp{,6}` line and keeps only `LISTEN` sockets.
///
/// The line shape is
/// `sl local_address rem_address st tx:rx tr:tm retrnsmt uid timeout inode ...`
/// where `local_address` is `HEXIP:HEXPORT` and `st` is the hex state. A
/// header line fails the parse.
fn parse_tcp_listener_line(line: &str) -> Option<TcpListener> {
    let mut fields = line.split_ascii_whitespace();
    let _slot = fields.next()?;
    let local = fields.next()?;
    let _remote = fields.next()?;
    if fields.next()? != TCP_LISTEN {
        return None;
    }
    let (_ip, port_hex) = local.rsplit_once(':')?;
    let port = u16::from_str_radix(port_hex, 16).ok()?;
    // Skip tx/rx queues, tr/tm->when, retrnsmt, uid, timeout.
    let inode = fields.nth(5)?.parse().ok()?;
    Some(TcpListener { port, inode })
}

/// The inode from a `/proc/<pid>/fd` symlink target of the form
/// `socket:[<inode>]`.
fn socket_inode(target: &str) -> Option<u64> {
    target
        .strip_prefix("socket:[")?
        .strip_suffix(']')?
        .parse()
        .ok()
}

#[cfg(test)]
mod tests {
    use assert2::assert;

    use super::*;

    #[test]
    fn endpoint_list_parses_hosts_ports_and_rejects_garbage() {
        let good: [(&str, &[(&str, u16)]); 4] = [
            ("127.0.0.1:5432", &[("127.0.0.1", 5432)]),
            (
                "db1.internal:26257 , db2.internal:26257",
                &[("db1.internal", 26257), ("db2.internal", 26257)],
            ),
            ("[::1]:5433", &[("[::1]", 5433)]),
            (
                "localhost:5432,localhost:5433,localhost:5434",
                &[
                    ("localhost", 5432),
                    ("localhost", 5433),
                    ("localhost", 5434),
                ],
            ),
        ];
        for (input, expected) in good {
            let expected: Vec<HostPort> = expected
                .iter()
                .map(|(host, port)| HostPort {
                    host: (*host).to_owned(),
                    port: *port,
                })
                .collect();
            let parsed = parse_endpoint_list(input).expect(input);
            assert!(parsed == expected, "input {input:?}");
        }
        let bad = [
            "",
            "no-port",
            "host:",
            "host:notaport",
            ":5432",
            "host:70000",
            "a:1,,b:2",
            "a:1,b:2,",
        ];
        for input in bad {
            assert!(let Err(_) = parse_endpoint_list(input), "input {input:?}");
        }
    }

    #[test]
    fn host_port_displays_as_given() {
        let cases = [("localhost:5432"), ("[::1]:9999"), ("10.0.0.7:26257")];
        for input in cases {
            let parsed: HostPort = input.parse().expect(input);
            assert!(parsed.to_string() == input, "input {input:?}");
        }
    }

    #[test]
    fn pid_override_list_parses_labels_and_rejects_garbage() {
        let good: [(&str, &[(&str, u32)]); 3] = [
            ("postgres=4242", &[("postgres", 4242)]),
            (
                "master=100, tserver=200",
                &[("master", 100), ("tserver", 200)],
            ),
            ("ext:5432=7", &[("ext:5432", 7)]),
        ];
        for (input, expected) in good {
            let expected: Vec<ProcessInfo> = expected
                .iter()
                .map(|(label, pid)| ProcessInfo {
                    label: (*label).to_owned(),
                    pid: *pid,
                })
                .collect();
            let parsed = parse_pid_overrides(input).expect(input);
            assert!(parsed == expected, "input {input:?}");
        }
        let bad = ["", "nopid", "=42", "label=", "label=abc", "a=1,,b=2"];
        for input in bad {
            assert!(let Err(_) = parse_pid_overrides(input), "input {input:?}");
        }
    }

    #[test]
    fn tcp_listener_lines_parse_listen_state_only() {
        let header = "  sl  local_address rem_address   st tx_queue rx_queue tr tm->when \
                      retrnsmt   uid  timeout inode";
        let listening = "   0: 0100007F:1538 00000000:0000 0A 00000000:00000000 00:00000000 \
                         00000000  1000        0 31337 1 0000000000000000 100 0 0 10 0";
        let established = "   1: 0100007F:1538 0100007F:A2C4 01 00000000:00000000 00:00000000 \
                           00000000  1000        0 31338 1 0000000000000000 20 4 30 10 -1";
        let v6_listening = "   2: 00000000000000000000000001000000:14E9 \
                            00000000000000000000000000000000:0000 0A 00000000:00000000 \
                            00:00000000 00000000  1000        0 555 1 0000000000000000 100 0 0 10 0";
        let cases = [
            (header, None),
            (
                listening,
                Some(TcpListener {
                    port: 0x1538,
                    inode: 31337,
                }),
            ),
            (established, None),
            (
                v6_listening,
                Some(TcpListener {
                    port: 0x14E9,
                    inode: 555,
                }),
            ),
            ("", None),
            ("garbage", None),
        ];
        for (line, expected) in cases {
            assert!(parse_tcp_listener_line(line) == expected, "line {line:?}");
        }
    }

    #[test]
    fn socket_inode_strips_the_socket_wrapper() {
        let cases = [
            ("socket:[31337]", Some(31337)),
            ("socket:[0]", Some(0)),
            ("pipe:[8]", None),
            ("/dev/null", None),
            ("socket:[x]", None),
            ("socket:[9", None),
        ];
        for (target, expected) in cases {
            assert!(socket_inode(target) == expected, "target {target:?}");
        }
    }

    #[test]
    fn loopback_ports_keep_only_local_endpoints() {
        let endpoint = |addr: &str| SqlEndpoint {
            addr: addr.parse().expect("addr"),
            user: "u".to_owned(),
            password: String::new(),
            database: "d".to_owned(),
        };
        let endpoints = [
            endpoint("127.0.0.1:5432"),
            endpoint("10.1.2.3:26257"),
            endpoint("[::1]:5433"),
        ];
        assert!(loopback_ports(&endpoints) == vec![5432, 5433]);
        assert!(loopback_ports(&[]) == Vec::<u16>::new());
    }

    #[test]
    fn sql_endpoints_resolve_and_carry_credentials() {
        let target = ExternalTarget {
            endpoints: vec![
                HostPort {
                    host: "127.0.0.1".to_owned(),
                    port: 5432,
                },
                HostPort {
                    host: "127.0.0.1".to_owned(),
                    port: 26257,
                },
            ],
            user: "roach".to_owned(),
            password: "s3cret".to_owned(),
            database: "bench".to_owned(),
            pids_override: None,
        };
        let endpoints = target.sql_endpoints().expect("resolve loopback");
        assert!(endpoints.len() == 2);
        assert!(endpoints[0].addr == "127.0.0.1:5432".parse().expect("addr"));
        assert!(endpoints[1].addr == "127.0.0.1:26257".parse().expect("addr"));
        for endpoint in &endpoints {
            assert!(endpoint.user == "roach");
            assert!(endpoint.password == "s3cret");
            assert!(endpoint.database == "bench");
        }
    }

    // `/proc`-based discovery needs a real `/proc` — Linux-only, like the
    // sampler tests in `metrics.rs`.
    #[cfg(target_os = "linux")]
    #[test]
    fn pids_for_ports_finds_the_tests_own_listener() {
        let listener =
            std::net::TcpListener::bind("127.0.0.1:0").expect("bind an ephemeral listener");
        let port = listener.local_addr().expect("listener addr").port();
        let found = pids_for_ports(&[port]);
        let own = ProcessInfo {
            label: format!("ext:{port}"),
            pid: std::process::id(),
        };
        assert!(
            found.contains(&own),
            "expected {own:?} in {found:?} for port {port}"
        );
        drop(listener);
    }

    #[cfg(target_os = "linux")]
    #[test]
    fn pids_for_ports_returns_empty_when_nothing_listens() {
        // Bind then immediately release a port so nothing listens on it.
        let listener =
            std::net::TcpListener::bind("127.0.0.1:0").expect("bind an ephemeral listener");
        let port = listener.local_addr().expect("listener addr").port();
        drop(listener);
        assert!(pids_for_ports(&[port]) == Vec::new());
        assert!(pids_for_ports(&[]) == Vec::new());
    }
}
