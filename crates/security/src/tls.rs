//! TLS config — implemented in task 4.

use std::path::PathBuf;

#[derive(Debug, Clone)]
pub struct TlsConfig {
    pub cert_chain_path: PathBuf,
    pub private_key_path: PathBuf,
    pub trust_roots_path: Option<PathBuf>,
}
