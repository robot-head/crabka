//! SP37: the zone database every gres zone-name lookup goes through.
//!
//! `PostgreSQL` installs its own copy of the IANA database under
//! `share/timezone` and resolves `SET TimeZone`, `AT TIME ZONE` and zone-bearing
//! literals against that copy rather than the host's. The set of names it
//! accepts and the offsets it renders are therefore a property of the server
//! build, not of the machine it happens to run on.
//!
//! gres does the same by resolving against the copy `jiff` compiles in (the
//! `tzdb-bundle-always` feature). It deliberately does *not* use
//! [`jiff::tz::TimeZone::get`], which consults [`jiff::tz::db`] — that prefers a
//! system installation at `TZDIR` or `/usr/share/zoneinfo`, and distributions
//! routinely trim it. A host whose `tzdata` omits the IANA "backward" links has
//! no `PST8PDT`, `US/Pacific`, `EST5EDT` or `Navajo`, so resolving through it
//! made `SET TimeZone = 'PST8PDT'` succeed or fail depending on which package
//! was installed. Going through the bundle makes zone resolution a property of
//! the binary, which is both reproducible and the wider vocabulary.

use jiff::tz::{TimeZone, TimeZoneDatabase};

/// Look a zone-database name up in the bundled IANA database.
///
/// The lookup ignores ASCII case and the resolved zone carries the database's
/// own spelling of the name, so `us/pacific` and `US/Pacific` both resolve and
/// both report `US/Pacific` as their
/// [`iana_name`](jiff::tz::TimeZone::iana_name). Legacy link names resolve to
/// whatever the database links them to, which is what keeps pre-1970 timestamps
/// honest: no alias table of our own gets between the name and the data.
#[must_use]
pub fn zone_by_name(name: &str) -> Option<TimeZone> {
    // `TimeZoneDatabase::bundled` is a handle, not a parse: the TZif bytes are
    // `static` and jiff keeps its own process-wide cache of parsed zones, so
    // building one per lookup costs nothing.
    TimeZoneDatabase::bundled().get(name).ok()
}

#[cfg(test)]
mod tests {
    use assert2::assert;

    use super::zone_by_name;

    #[test]
    fn resolves_canonical_names() {
        for name in [
            "America/Los_Angeles",
            "Europe/Rome",
            "EST",
            "UTC",
            "America/Denver",
        ] {
            assert!(zone_by_name(name).is_some(), "{name} should resolve");
        }
    }

    /// The IANA "backward" links. A trimmed system `tzdata` drops these, so this
    /// is the case that fails when resolution goes through `jiff::tz::db`.
    #[test]
    fn resolves_legacy_link_names() {
        for name in [
            "PST8PDT",
            "EST5EDT",
            "CST6CDT",
            "MST7MDT",
            "US/Pacific",
            "US/Eastern",
            "Navajo",
            "Japan",
            "GB",
        ] {
            assert!(zone_by_name(name).is_some(), "{name} should resolve");
        }
    }

    #[test]
    fn lookup_ignores_ascii_case_and_reports_canonical_spelling() {
        let tz = zone_by_name("us/pacific").expect("us/pacific resolves");
        assert!(tz.iana_name() == Some("US/Pacific"));
        let tz = zone_by_name("america/los_angeles").expect("lowercase resolves");
        assert!(tz.iana_name() == Some("America/Los_Angeles"));
        let tz = zone_by_name("PST8PDT").expect("PST8PDT resolves");
        assert!(tz.iana_name() == Some("PST8PDT"));
    }

    #[test]
    fn rejects_names_the_database_does_not_have() {
        for name in ["Not/AZone", "PST", "posixrules", ""] {
            assert!(zone_by_name(name).is_none(), "{name} should not resolve");
        }
    }
}
