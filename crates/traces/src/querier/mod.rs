//! The querier role and Tempo HTTP API.

pub mod http;
pub mod live;
pub mod store;

#[derive(Clone, Debug, PartialEq)]
pub struct QuerierConfig {
    pub listen_addr: std::net::SocketAddr,
    pub default_search_limit: usize,
    pub default_spss: usize,
    pub max_traces: usize,
}

impl Default for QuerierConfig {
    fn default() -> Self {
        Self {
            listen_addr: ([0, 0, 0, 0], 3200).into(),
            default_search_limit: 20,
            default_spss: 3,
            max_traces: 1000,
        }
    }
}

#[cfg(test)]
mod tests {
    use assert2::assert;

    use super::*;

    #[test]
    fn default_config_matches_tempo_defaults() {
        let c = QuerierConfig::default();
        assert!(
            c == QuerierConfig {
                listen_addr: ([0, 0, 0, 0], 3200).into(),
                default_search_limit: 20,
                default_spss: 3,
                max_traces: 1000,
            }
        );
    }
}
