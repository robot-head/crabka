//! Construction-time config for `Controller::start`.

use std::net::SocketAddr;
use std::path::PathBuf;
use std::time::Duration;

use crate::types::NodeId;

#[derive(Debug, Clone)]
pub struct ControllerConfig {
    pub node_id: NodeId,
    pub voters: Vec<(NodeId, SocketAddr)>,
    pub controller_listen_addr: SocketAddr,
    pub log_dir: PathBuf,
    pub election_timeout: Duration,
    pub heartbeat_interval: Duration,
    pub client_id: String,
}

impl ControllerConfig {
    #[must_use]
    pub fn for_tests(node_id: NodeId, log_dir: PathBuf) -> Self {
        Self {
            node_id,
            voters: vec![(node_id, "127.0.0.1:0".parse().expect("static"))],
            controller_listen_addr: "127.0.0.1:0".parse().expect("static"),
            log_dir,
            election_timeout: Duration::from_secs(1),
            heartbeat_interval: Duration::from_millis(200),
            client_id: "crabka-controller-test".into(),
        }
    }
}
