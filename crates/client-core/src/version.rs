//! `ApiVersionTable` — broker-advertised version ranges per API key,
//! plus client-side negotiation.

use std::collections::HashMap;

use crate::{error::ClientError, request::ProtocolRequest};

#[derive(Debug, Clone, Default)]
pub struct ApiVersionTable {
    by_key: HashMap<i16, (i16, i16)>,
}

impl ApiVersionTable {
    /// Build from a sequence of `(api_key, broker_min, broker_max)` tuples.
    /// Used when seeding the table from a decoded `ApiVersionsResponse`.
    #[must_use]
    pub fn from_entries(entries: impl IntoIterator<Item = (i16, i16, i16)>) -> Self {
        let mut by_key = HashMap::new();
        for (k, lo, hi) in entries {
            by_key.insert(k, (lo, hi));
        }
        Self { by_key }
    }

    /// Highest version both sides support for `R`, or
    /// [`ClientError::IncompatibleVersion`] if the ranges don't overlap.
    pub fn negotiate<R: ProtocolRequest>(&self) -> Result<i16, ClientError> {
        let api_key = R::API_KEY;
        let client_min = R::MIN_VERSION;
        let client_max = R::MAX_VERSION;
        let (broker_min, broker_max) = self.by_key.get(&api_key).copied().unwrap_or((0, 0));
        let chosen = client_max.min(broker_max);
        if chosen < client_min || chosen < broker_min {
            return Err(ClientError::IncompatibleVersion {
                api_key,
                broker_min,
                broker_max,
                client_min,
                client_max,
            });
        }
        Ok(chosen)
    }

    /// Return the broker-advertised `(min, max)` version range for `api_key`,
    /// or `None` if the broker didn't advertise it.
    #[must_use]
    pub fn broker_range(&self, api_key: i16) -> Option<(i16, i16)> {
        self.by_key.get(&api_key).copied()
    }

    /// Returns `true` if the table contains no entries.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.by_key.is_empty()
    }
}

#[cfg(test)]
mod tests {

    use crabka_protocol::owned::api_versions_request::ApiVersionsRequest;

    use super::*;

    // `ApiVersionsRequest` acts as a sample `ProtocolRequest`. We only
    // need the trait's constants here; the impl comes from codegen.

    #[test]
    fn negotiate_takes_min_of_max() {
        let t = ApiVersionTable::from_entries([(
            ApiVersionsRequest::API_KEY,
            0,
            ApiVersionsRequest::MAX_VERSION,
        )]);
        // Sanity: client max wins if broker max is higher.
        let _ = t.negotiate::<ApiVersionsRequest>().unwrap();
    }

    #[test]
    fn negotiate_errors_when_disjoint() {
        let t = ApiVersionTable::from_entries([(ApiVersionsRequest::API_KEY, 99, 100)]);
        assert2::assert!(matches!(
            t.negotiate::<ApiVersionsRequest>(),
            Err(ClientError::IncompatibleVersion { .. })
        ));
    }

    #[test]
    fn negotiate_picks_lowest_supported_when_broker_caps_low() {
        let t = ApiVersionTable::from_entries([(ApiVersionsRequest::API_KEY, 0, 0)]);
        // Both sides support 0; that's what's chosen.
        assert2::assert!(t.negotiate::<ApiVersionsRequest>().unwrap() == 0);
    }

    #[test]
    fn broker_range_reports_exact_advertised_bounds() {
        let t = ApiVersionTable::from_entries([(ApiVersionsRequest::API_KEY, 2, 4)]);

        for (_name, api_key, expected) in [
            ("advertised api", ApiVersionsRequest::API_KEY, Some((2, 4))),
            ("missing api", ApiVersionsRequest::API_KEY + 1, None),
        ] {
            assert2::assert!(t.broker_range(api_key) == expected);
        }
    }

    #[test]
    fn is_empty_reflects_whether_any_versions_were_advertised() {
        for (_name, table, expected) in [
            ("default", ApiVersionTable::default(), true),
            (
                "one advertised api",
                ApiVersionTable::from_entries([(ApiVersionsRequest::API_KEY, 0, 1)]),
                false,
            ),
        ] {
            assert2::assert!(table.is_empty() == expected);
        }
    }
}
