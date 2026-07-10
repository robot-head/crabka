use std::{fmt, str::FromStr};

use crabka_connect::{OffsetMap, OffsetValue, SourceOffset};

use crate::PostgresConnectError;

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct PgLsn(pub u64);

impl PgLsn {
    #[must_use]
    pub fn to_source_offset(self, database: &str, slot: &str) -> SourceOffset {
        let partition = OffsetMap::from([
            (
                "database".to_owned(),
                OffsetValue::String(database.to_owned()),
            ),
            ("slot".to_owned(), OffsetValue::String(slot.to_owned())),
        ]);
        let position = OffsetMap::from([("lsn".to_owned(), OffsetValue::String(self.to_string()))]);

        SourceOffset::new(partition.into(), position.into())
    }

    pub fn from_source_offset(
        offset: &SourceOffset,
        expected_slot: &str,
    ) -> Result<PgLsn, PostgresConnectError> {
        match offset.partition.get("slot") {
            Some(OffsetValue::String(slot)) if slot == expected_slot => {}
            Some(OffsetValue::String(slot)) => {
                return Err(PostgresConnectError::Offset(format!(
                    "source offset slot {slot:?} does not match expected slot {expected_slot:?}"
                )));
            }
            _ => {
                return Err(PostgresConnectError::Offset(
                    "source offset missing string slot".to_owned(),
                ));
            }
        }

        match offset.position.get("lsn") {
            Some(OffsetValue::String(lsn)) => lsn.parse(),
            _ => Err(PostgresConnectError::Offset(
                "source offset missing string lsn".to_owned(),
            )),
        }
    }
}

impl fmt::Display for PgLsn {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let hi = self.0 >> 32;
        let lo = self.0 & 0xffff_ffff;

        write!(f, "{hi:X}/{lo:X}")
    }
}

impl FromStr for PgLsn {
    type Err = PostgresConnectError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let (hi, lo) = s
            .split_once('/')
            .ok_or_else(|| PostgresConnectError::Offset(format!("invalid Postgres LSN {s:?}")))?;

        let hi = parse_lsn_half(hi, s)?;
        let lo = parse_lsn_half(lo, s)?;

        Ok(PgLsn((hi << 32) | lo))
    }
}

fn parse_lsn_half(half: &str, lsn: &str) -> Result<u64, PostgresConnectError> {
    let value = u64::from_str_radix(half, 16)
        .map_err(|_| PostgresConnectError::Offset(format!("invalid Postgres LSN {lsn:?}")))?;

    if value > 0xffff_ffff {
        return Err(PostgresConnectError::Offset(format!(
            "invalid Postgres LSN {lsn:?}: component exceeds 32 bits"
        )));
    }

    Ok(value)
}

#[cfg(test)]
mod tests {
    use crabka_connect::{OffsetValue, SourceOffset};

    use super::PgLsn;
    use crate::PostgresConnectError;

    #[test]
    fn lsn_round_trips_postgres_text_form() {
        let lsn: PgLsn = "16/B374D848".parse().expect("valid LSN");

        assert_eq!(
            (
                lsn,
                lsn.to_string(),
                "FFFFFFFF/FFFFFFFF".parse::<PgLsn>().expect("max LSN"),
            ),
            (
                PgLsn(0x16_b374_d848),
                "16/B374D848".to_owned(),
                PgLsn(u64::MAX),
            )
        );
    }

    #[test]
    fn source_offset_round_trips_and_checks_slot() {
        let lsn: PgLsn = "0/2A".parse().expect("valid LSN");
        let offset = lsn.to_source_offset("app", "slot_a");

        assert_eq!(
            PgLsn::from_source_offset(&offset, "slot_a").expect("matching slot"),
            lsn
        );
        match PgLsn::from_source_offset(&offset, "slot_b").expect_err("slot mismatch") {
            PostgresConnectError::Offset(message) => {
                for needle in ["does not match expected slot", "slot_a", "slot_b"] {
                    assert!(message.contains(needle), "missing {needle:?} in {message}");
                }
            }
            error => panic!("expected offset error, got {error:?}"),
        }
    }

    #[test]
    fn source_offset_rejects_missing_or_non_string_slot() {
        let missing = SourceOffset::default();
        let mut non_string = PgLsn(42).to_source_offset("app", "slot_a");
        non_string
            .partition
            .0
            .insert("slot".to_owned(), OffsetValue::Long(7));

        for (name, offset) in [("missing", missing), ("non_string", non_string)] {
            match PgLsn::from_source_offset(&offset, "slot_a").expect_err("slot should fail") {
                PostgresConnectError::Offset(message) => {
                    assert_eq!(message, "source offset missing string slot", "case {name}");
                }
                error => panic!("expected offset error, got {error:?}"),
            }
        }
    }

    #[test]
    fn malformed_lsn_is_offset_error() {
        for (name, input) in [
            ("missing_separator", "not-a-lsn"),
            ("invalid_hex", "1/XYZ"),
            ("component_overflow", "100000000/0"),
        ] {
            assert!(
                matches!(input.parse::<PgLsn>(), Err(PostgresConnectError::Offset(_))),
                "case {name}"
            );
        }
    }
}
