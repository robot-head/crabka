use std::fmt;
use std::str::FromStr;

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

        SourceOffset::new(partition, position)
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
    use super::PgLsn;
    use crate::PostgresConnectError;

    #[test]
    fn lsn_round_trips_postgres_text_form() {
        let lsn: PgLsn = "16/B374D848".parse().expect("valid LSN");

        assert_eq!(lsn, PgLsn(0x16_b374_d848));
        assert_eq!(lsn.to_string(), "16/B374D848");
    }

    #[test]
    fn source_offset_round_trips_and_checks_slot() {
        let lsn: PgLsn = "0/2A".parse().expect("valid LSN");
        let offset = lsn.to_source_offset("app", "slot_a");

        assert_eq!(
            PgLsn::from_source_offset(&offset, "slot_a").expect("matching slot"),
            lsn
        );
        assert!(PgLsn::from_source_offset(&offset, "slot_b").is_err());
    }

    #[test]
    fn malformed_lsn_is_offset_error() {
        for input in ["not-a-lsn", "1/XYZ", "100000000/0"] {
            assert!(matches!(
                input.parse::<PgLsn>(),
                Err(PostgresConnectError::Offset(_))
            ));
        }
    }
}
