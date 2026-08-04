//! `PostgreSQL`'s hash-partition row hash, ported byte-exactly.
//!
//! Hash partitioning routes a row by `rowHash % modulus == remainder`, so we
//! are not free to choose the hash. A leaf partition stores
//! `(modulus, remainder)` and nothing else, and `satisfies_hash_partition` is
//! user-visible SQL. Any deviation from `PostgreSQL`'s arithmetic puts rows in
//! the wrong leaf. You find that mistake only if you read every row back.
//!
//! This module reproduces this chain from `src/backend/partitioning/partbounds.c`:
//!
//! ```text
//! rowHash = 0;
//! for each key column i:
//!     if (!isnull[i])
//!         rowHash = hash_combine64(rowHash, hash_i(values[i], HASH_PARTITION_SEED));
//! ```
//!
//! NULL columns contribute nothing, so an all-NULL key hashes to 0 and lands in
//! the `remainder = 0` partition.
//!
//! Every per-type hash below ends in Bob Jenkins' lookup3, in the shape
//! `PostgreSQL` vendors in `src/common/hashfn.c`. The port targets
//! **little-endian** byte order, which is where `PostgreSQL`'s word-at-a-time
//! and byte-at-a-time paths agree. A big-endian server produces different
//! hashes for the same value, and `PostgreSQL` makes no promise otherwise.
//!
//! A type whose `PostgreSQL` hash function is not ported here is *refused*, not
//! approximated. A refused `INSERT` is recoverable. A row routed silently to
//! the wrong partition is not.

use crabka_pgtypes::{Datum, datetime};

use crate::error::ExecError;

/// `HASH_PARTITION_SEED`: the fixed seed every partition-key hash uses.
const HASH_PARTITION_SEED: u64 = 0x7a5b_2236_7996_dcfd;

/// lookup3 consumes the key twelve bytes at a time.
const CHUNK_LEN: usize = 12;

/// `hash_uint32_extended` seeds its state with `sizeof(uint32)`.
const U32_KEY_LEN: u32 = 4;

/// `PostgreSQL`'s `compute_partition_hash_value` for one row's partition key.
///
/// `values` are the row's partition-key columns in key order. This function
/// returns the 64-bit row hash. The caller takes `% modulus` to pick the leaf
/// partition.
pub(crate) fn partition_hash(values: &[Datum]) -> Result<u64, ExecError> {
    let mut row_hash = 0_u64;
    for value in values {
        if let Some(hash) = column_hash(value, HASH_PARTITION_SEED)? {
            row_hash = hash_combine64(row_hash, hash);
        }
    }
    Ok(row_hash)
}

/// One partition-key column's extended hash, or `None` for NULL.
///
/// This is `PostgreSQL`'s `if (isnull[i]) continue;`. A NULL column is
/// therefore not the same as a column that hashes to zero.
///
/// The dispatch mirrors the `hashextended` support procedures (`amprocnum = 2`)
/// that `pg_amproc` records for each type's default hash operator family.
fn column_hash(value: &Datum, seed: u64) -> Result<Option<u64>, ExecError> {
    let hash = match value {
        Datum::Null => return Ok(None),
        // `hashboolextended` widens the bool to int32 first, so it agrees with
        // `hashint4extended` on 0 and 1.
        Datum::Bool(v) => hash_uint32_extended(u32::from(*v), seed),
        // `hashint2extended` sign-extends to int32 before the bit-cast, so -1
        // hashes as 0xffffffff rather than 0x0000ffff.
        Datum::Int2(v) => hash_uint32_extended(i32::from(*v).cast_unsigned(), seed),
        Datum::Int4(v) => hash_uint32_extended(v.cast_unsigned(), seed),
        // `regclass` hashes through the oid operator family, whose
        // `hashoidextended` is `hashint4extended` over the oid bits.
        Datum::Regclass(v) => hash_uint32_extended(v.oid.cast_unsigned(), seed),
        Datum::Int8(v) => hash_int64_extended(*v, seed),
        // `hashtextextended` under a deterministic collation hashes the raw
        // bytes. `bpchar` differs — `hashbpcharextended` strips trailing spaces
        // first — but `Datum` does not distinguish `bpchar` from `text`, so a
        // `char(n)` partition key would need that trim adding here, alongside
        // whatever carries the type distinction.
        Datum::Text(v) => hash_bytes_extended(v.as_bytes(), seed)?,
        Datum::JsonPath(_) => return Err(unsupported("jsonpath")),
        Datum::Bytea(v) => hash_bytes_extended(v, seed)?,
        // The date/time types hash their internal representation, which the
        // binary send functions already produce: days or microseconds relative
        // to the PostgreSQL epoch (2000-01-01), including the reserved
        // INT32_MIN / INT64_MAX encodings of -infinity / infinity.
        Datum::Date(v) => {
            let days = i32::from_be_bytes(datetime::date_to_binary(*v));
            hash_uint32_extended(days.cast_unsigned(), seed)
        }
        Datum::Time(v) => {
            hash_int64_extended(i64::from_be_bytes(datetime::time_to_binary(*v)), seed)
        }
        Datum::Timestamp(v) => {
            hash_int64_extended(i64::from_be_bytes(datetime::timestamp_to_binary(*v)), seed)
        }
        Datum::Timestamptz(v) => hash_int64_extended(
            i64::from_be_bytes(datetime::timestamptz_to_binary(*v)),
            seed,
        ),
        Datum::Float4(_) => return Err(unsupported("real")),
        Datum::Float8(_) => return Err(unsupported("double precision")),
        Datum::Point(_) => return Err(unsupported("point")),
        Datum::Path(_) => return Err(unsupported("path")),
        Datum::Lseg(_) => return Err(unsupported("lseg")),
        Datum::Line(_) => return Err(unsupported("line")),
        Datum::Numeric(_) => return Err(unsupported("numeric")),
        Datum::Timetz(_) => return Err(unsupported("time with time zone")),
        Datum::Interval(_) => return Err(unsupported("interval")),
        Datum::Jsonb(_) => return Err(unsupported("jsonb")),
        Datum::Array(_) => return Err(unsupported("array")),
        Datum::OidVector(_) => return Err(unsupported("oidvector")),
        Datum::Record(_) => return Err(unsupported("record")),
        Datum::Enum(_) => return Err(unsupported("enum")),
        Datum::TsVector(_) => return Err(unsupported("tsvector")),
        Datum::TsQuery(_) => return Err(unsupported("tsquery")),
        Datum::Range(range) => return Err(unsupported(range.ty.name)),
        Datum::Multirange(multirange) => return Err(unsupported(multirange.ty.name)),
    };
    Ok(Some(hash))
}

fn unsupported(type_name: &str) -> ExecError {
    ExecError::Unsupported(format!(
        "hash partitioning on {type_name} is not supported: PostgreSQL's {type_name} hash \
         function is not implemented, so a row could route to the wrong partition"
    ))
}

/// `hash_combine64` from `src/include/common/hashfn.h`.
///
/// The shifts are 54 and 7. These are the 64-bit constants, not the 6 and 2
/// that the 32-bit `hash_combine` uses.
fn hash_combine64(a: u64, b: u64) -> u64 {
    a ^ b
        .wrapping_add(0x49a0_f4dd_15e5_a8e3)
        .wrapping_add(a << 54)
        .wrapping_add(a >> 7)
}

/// `hashint8extended`: fold the high half into the low half so an int8 and an
/// int4 of equal value hash alike, then hash the folded word.
fn hash_int64_extended(value: i64, seed: u64) -> u64 {
    let (lohalf, hihalf) = halves(value.cast_unsigned());
    let folded = lohalf ^ if value >= 0 { hihalf } else { !hihalf };
    hash_uint32_extended(folded, seed)
}

/// `hash_uint32_extended` from `src/common/hashfn.c`.
fn hash_uint32_extended(key: u32, seed: u64) -> u64 {
    let (a, b, c) = seeded_state(U32_KEY_LEN, seed);
    let (_, b, c) = final_mix(a.wrapping_add(key), b, c);
    join(b, c)
}

/// `hash_bytes_extended` from `src/common/hashfn.c`: lookup3 over `key`.
///
/// `PostgreSQL` has an aligned word-at-a-time path and an unaligned
/// byte-at-a-time path. On little-endian hardware the two agree by
/// construction, and this is that shared result.
fn hash_bytes_extended(key: &[u8], seed: u64) -> Result<u64, ExecError> {
    let (mut a, mut b, mut c) = seeded_state(hash_key_len(key.len() as u64)?, seed);

    let mut chunks = key.chunks_exact(CHUNK_LEN);
    for chunk in chunks.by_ref() {
        a = a.wrapping_add(u32::from_le_bytes([chunk[0], chunk[1], chunk[2], chunk[3]]));
        b = b.wrapping_add(u32::from_le_bytes([chunk[4], chunk[5], chunk[6], chunk[7]]));
        c = c.wrapping_add(u32::from_le_bytes([
            chunk[8], chunk[9], chunk[10], chunk[11],
        ]));
        (a, b, c) = mix(a, b, c);
    }

    // The trailing bytes, zero-padded: exactly what lookup3's fall-through
    // `switch` accumulates. At most 11 bytes reach here — 12 would have gone
    // round the loop — so byte 11 is never read, and the lowest byte of `c`
    // stays reserved for the length already folded into the initial state.
    let rest = chunks.remainder();
    let mut tail = [0_u8; CHUNK_LEN];
    tail[..rest.len()].copy_from_slice(rest);
    a = a.wrapping_add(u32::from_le_bytes([tail[0], tail[1], tail[2], tail[3]]));
    b = b.wrapping_add(u32::from_le_bytes([tail[4], tail[5], tail[6], tail[7]]));
    c = c.wrapping_add(u32::from_le_bytes([0, tail[8], tail[9], tail[10]]));

    let (_, b, c) = final_mix(a, b, c);
    Ok(join(b, c))
}

/// `PostgreSQL` passes the key length to lookup3 as a C `int`, and its varlena
/// format caps a value at 1 GB, so a longer key cannot have come from a
/// `PostgreSQL`-compatible value. A truncated length would change the hash and
/// route the row elsewhere, so refuse the key instead.
fn hash_key_len(len: u64) -> Result<u32, ExecError> {
    u32::try_from(len).map_err(|_| {
        ExecError::Unsupported(format!(
            "hash partitioning on a {len}-byte key is not supported: PostgreSQL hashes at most \
             {} bytes",
            u32::MAX
        ))
    })
}

/// lookup3's internal state after the golden-ratio/length initialisation and the
/// optional seed perturbation, which treats the seed as a 12-byte chunk padded
/// with four zero bytes.
fn seeded_state(key_len: u32, seed: u64) -> (u32, u32, u32) {
    let init = 0x9e37_79b9_u32
        .wrapping_add(key_len)
        .wrapping_add(3_923_095);
    if seed == 0 {
        return (init, init, init);
    }
    let (seed_lo, seed_hi) = halves(seed);
    mix(init.wrapping_add(seed_hi), init.wrapping_add(seed_lo), init)
}

/// The low and high 32-bit halves of a 64-bit value: C's `(uint32) v` and
/// `(uint32) (v >> 32)`.
fn halves(value: u64) -> (u32, u32) {
    let [b0, b1, b2, b3, b4, b5, b6, b7] = value.to_le_bytes();
    (
        u32::from_le_bytes([b0, b1, b2, b3]),
        u32::from_le_bytes([b4, b5, b6, b7]),
    )
}

/// lookup3 reports `((uint64) b << 32) | c`.
fn join(b: u32, c: u32) -> u64 {
    (u64::from(b) << 32) | u64::from(c)
}

/// lookup3's `mix()` macro.
fn mix(mut a: u32, mut b: u32, mut c: u32) -> (u32, u32, u32) {
    a = a.wrapping_sub(c);
    a ^= c.rotate_left(4);
    c = c.wrapping_add(b);
    b = b.wrapping_sub(a);
    b ^= a.rotate_left(6);
    a = a.wrapping_add(c);
    c = c.wrapping_sub(b);
    c ^= b.rotate_left(8);
    b = b.wrapping_add(a);
    a = a.wrapping_sub(c);
    a ^= c.rotate_left(16);
    c = c.wrapping_add(b);
    b = b.wrapping_sub(a);
    b ^= a.rotate_left(19);
    a = a.wrapping_add(c);
    c = c.wrapping_sub(b);
    c ^= b.rotate_left(4);
    b = b.wrapping_add(a);
    (a, b, c)
}

/// lookup3's `final()` macro, renamed because `final` is a reserved word.
fn final_mix(mut a: u32, mut b: u32, mut c: u32) -> (u32, u32, u32) {
    c ^= b;
    c = c.wrapping_sub(b.rotate_left(14));
    a ^= c;
    a = a.wrapping_sub(c.rotate_left(11));
    b ^= a;
    b = b.wrapping_sub(a.rotate_left(25));
    c ^= b;
    c = c.wrapping_sub(b.rotate_left(16));
    a ^= c;
    a = a.wrapping_sub(c.rotate_left(4));
    b ^= a;
    b = b.wrapping_sub(a.rotate_left(14));
    c ^= b;
    c = c.wrapping_sub(b.rotate_left(24));
    (a, b, c)
}

#[cfg(test)]
mod tests {
    use assert2::assert;
    use crabka_pgtypes::{
        ArrayValue, ElemType, EnumValue, JsonbValue, RecordValue,
        datetime::{Interval, TimeTz},
        numeric::NumericValue,
        usertype::UserTypeRef,
    };

    use super::*;

    // Every expected value below was read from a live PostgreSQL 18.4 server
    // ("PostgreSQL 18.4 (Debian 18.4-1.pgdg13+1) on x86_64-pc-linux-gnu"). The
    // per-type tables are `select <hashproc>(<value>, <seed>)`, which PostgreSQL
    // returns as a signed int8 — hence the i64 literals. The row-hash tables are
    // the value satisfies_hash_partition() compares against, recovered exactly
    // from the remainders it reports for five coprime moduli.
    const SEED: i64 = 8_816_678_312_871_386_365;

    // The source of the length sweep; the oracle was given the same prefixes as
    // substr('abcdefghij0123456789ABCDEFGHIJklmnopqrst', 1, n).
    const TEXT_PATTERN: &str = "abcdefghij0123456789ABCDEFGHIJklmnopqrst";

    fn hash_of(value: Datum) -> u64 {
        column_hash(&value, SEED.cast_unsigned())
            .expect("supported key type")
            .expect("not null")
    }

    #[test]
    fn seed_constant_is_postgresqls_hash_partition_seed() {
        assert!(HASH_PARTITION_SEED == SEED.cast_unsigned());
    }

    #[test]
    fn int2_matches_hashint2extended() {
        let vectors: [(i16, i64); 8] = [
            (i16::MIN, 5_885_242_512_097_991_578),
            (-1234, 3_294_891_953_056_742_581),
            (-1, -5_017_072_347_659_237_694),
            (0, -4_403_592_609_991_167_795),
            (1, 5_968_994_663_651_403_477),
            (42, 7_363_975_540_656_877_951),
            (12345, 3_476_363_059_597_467_753),
            (i16::MAX, -6_436_696_229_286_267_413),
        ];
        for (value, expected) in vectors {
            assert!(hash_of(Datum::Int2(value)) == expected.cast_unsigned());
        }
    }

    #[test]
    fn int4_matches_hashint4extended() {
        let vectors: [(i32, i64); 8] = [
            (i32::MIN, 4_938_542_303_000_433_043),
            (-100_000, -4_441_764_559_288_001_488),
            (-1, -5_017_072_347_659_237_694),
            (0, -4_403_592_609_991_167_795),
            (1, 5_968_994_663_651_403_477),
            (42, 7_363_975_540_656_877_951),
            (1_000_000, 4_098_724_818_956_435_368),
            (i32::MAX, -6_050_265_599_104_649_060),
        ];
        for (value, expected) in vectors {
            assert!(hash_of(Datum::Int4(value)) == expected.cast_unsigned());
        }
    }

    #[test]
    fn int8_matches_hashint8extended() {
        // 4294967296 folding onto the same hash as 1, and i64::MAX onto
        // i32::MIN's, is the high-half fold at work rather than a slipped
        // transcription.
        let vectors: [(i64, i64); 8] = [
            (i64::MIN, -6_050_265_599_104_649_060),
            (-4_294_967_297, 2_147_707_635_919_668_551),
            (-1, -5_017_072_347_659_237_694),
            (0, -4_403_592_609_991_167_795),
            (1, 5_968_994_663_651_403_477),
            (42, 7_363_975_540_656_877_951),
            (4_294_967_296, 5_968_994_663_651_403_477),
            (i64::MAX, 4_938_542_303_000_433_043),
        ];
        for (value, expected) in vectors {
            assert!(hash_of(Datum::Int8(value)) == expected.cast_unsigned());
        }
    }

    #[test]
    fn bool_matches_hashboolextended() {
        let vectors: [(bool, i64); 2] = [
            (false, -4_403_592_609_991_167_795),
            (true, 5_968_994_663_651_403_477),
        ];
        for (value, expected) in vectors {
            assert!(hash_of(Datum::Bool(value)) == expected.cast_unsigned());
        }
    }

    #[test]
    fn text_matches_hashtextextended_at_every_tail_length() {
        // 0..=40 walks every arm of lookup3's 12-byte fall-through tail, three
        // times over, either side of the whole-chunk loop.
        let vectors: [(usize, i64); 41] = [
            (0, -5_700_645_584_453_517_373),
            (1, -6_705_225_459_120_232_837),
            (2, 596_009_030_900_183_869),
            (3, 3_628_778_498_291_917_250),
            (4, 4_029_384_952_263_848_046),
            (5, 4_809_259_092_934_399_961),
            (6, -658_605_897_836_517_514),
            (7, -1_516_236_981_150_442_578),
            (8, -7_754_745_184_861_941_070),
            (9, -7_860_738_947_571_725_408),
            (10, -8_306_284_229_451_770_207),
            (11, -6_293_252_113_577_552_983),
            (12, -86_348_435_575_071_401),
            (13, -5_725_591_609_343_730_479),
            (14, -100_044_587_194_739_909),
            (15, -5_005_925_166_496_941_275),
            (16, 5_842_676_254_129_454_599),
            (17, -8_791_520_172_293_485_859),
            (18, 301_157_554_710_597_337),
            (19, 4_937_504_844_326_616_921),
            (20, 5_554_781_384_778_602_987),
            (21, -1_333_013_943_083_860_647),
            (22, -5_829_956_874_446_472_734),
            (23, 4_442_336_628_380_298_555),
            (24, -6_881_001_208_903_177_844),
            (25, -6_039_824_830_261_728_220),
            (26, 8_431_369_192_429_408_116),
            (27, -8_680_534_265_435_538_845),
            (28, 7_138_980_750_346_225_128),
            (29, -7_238_812_066_653_318_592),
            (30, 4_218_724_800_189_183_667),
            (31, -2_509_921_389_755_927_125),
            (32, 8_132_456_262_361_840_205),
            (33, -8_868_281_998_813_632_041),
            (34, 4_497_901_827_848_378_165),
            (35, 2_527_171_647_792_789_315),
            (36, 25_841_451_471_791_934),
            (37, 8_806_444_157_371_053_260),
            (38, -4_837_453_906_008_103_775),
            (39, 9_010_932_821_310_084_515),
            (40, 816_148_702_143_151_921),
        ];
        for (len, expected) in vectors {
            let value = Datum::Text(TEXT_PATTERN[..len].to_string());
            assert!(hash_of(value) == expected.cast_unsigned());
        }
    }

    #[test]
    fn text_matches_hashtextextended_for_multibyte_and_long_input() {
        let vectors: [(&str, i64); 5] = [
            ("x", 862_710_693_865_977_740),
            ("hello", -2_635_279_551_741_001_039),
            ("héllo wörld", -145_319_379_157_667_650),
            ("日本語テキスト", 3_781_793_241_886_115_942),
            ("🦀🦀🦀", 1_557_130_743_457_566_776),
        ];
        for (value, expected) in vectors {
            assert!(hash_of(Datum::Text(value.to_string())) == expected.cast_unsigned());
        }

        // repeat('The quick brown fox jumps over the lazy dog. ', 5)
        let long = "The quick brown fox jumps over the lazy dog. ".repeat(5);
        assert!(hash_of(Datum::Text(long)) == 8_101_312_192_083_244_144_i64.cast_unsigned());
    }

    #[test]
    fn bytea_matches_hashbyteaextended() {
        // Bytes that text cannot carry: an embedded NUL, and a run of zeros
        // whose length is the only thing separating it from the empty string.
        let vectors: [(&[u8], i64); 5] = [
            (&[], -5_700_645_584_453_517_373),
            (&[0x00], -8_583_321_429_529_518_889),
            (&[0xff, 0x00, 0xff], 2_883_197_842_719_367_882),
            (&[0x00; 16], -5_281_082_146_396_728_638),
            (
                &[
                    0x03, 0x14, 0x25, 0x36, 0x47, 0x58, 0x69, 0x7a, 0x8b, 0x9c, 0xad, 0xbe, 0xcf,
                    0xe0, 0xf1, 0x02, 0x13, 0x24, 0x35, 0x46,
                ],
                -4_607_420_937_047_470_105,
            ),
        ];
        for (value, expected) in vectors {
            assert!(hash_of(Datum::Bytea(value.to_vec())) == expected.cast_unsigned());
        }
    }

    #[test]
    fn date_matches_hashdateextended() {
        // The endpoints of jiff's range are how this engine spells the
        // non-finite dates, so 9999-12-30 is the largest finite day count a
        // `Datum::Date` can carry.
        let vectors: [((i16, i8, i8), i64); 7] = [
            ((2000, 1, 1), -4_403_592_609_991_167_795),
            ((1999, 12, 31), -5_017_072_347_659_237_694),
            ((1970, 1, 1), -7_791_128_061_482_025_433),
            ((2026, 7, 29), 1_801_797_112_842_608_657),
            ((1900, 1, 1), -1_831_648_286_028_531_366),
            ((9999, 12, 30), -8_797_254_705_092_355_939),
            ((1, 1, 1), -7_515_971_407_706_873_240),
        ];
        for ((year, month, day), expected) in vectors {
            let value = Datum::Date(jiff::civil::date(year, month, day));
            assert!(hash_of(value) == expected.cast_unsigned());
        }
    }

    #[test]
    fn non_finite_dates_and_timestamps_hash_their_reserved_encodings() {
        // infinity and -infinity are INT32_MIN/MAX and INT64_MIN/MAX, so their
        // hashes coincide with the corresponding integer extremes.
        let vectors: [(Datum, i64); 6] = [
            (
                Datum::Date(datetime::DATE_INFINITY),
                -6_050_265_599_104_649_060,
            ),
            (
                Datum::Date(datetime::DATE_NEG_INFINITY),
                4_938_542_303_000_433_043,
            ),
            (
                Datum::Timestamp(datetime::TIMESTAMP_INFINITY),
                4_938_542_303_000_433_043,
            ),
            (
                Datum::Timestamp(datetime::TIMESTAMP_NEG_INFINITY),
                -6_050_265_599_104_649_060,
            ),
            (
                Datum::Timestamptz(jiff::Timestamp::MAX),
                4_938_542_303_000_433_043,
            ),
            (
                Datum::Timestamptz(jiff::Timestamp::MIN),
                -6_050_265_599_104_649_060,
            ),
        ];
        for (value, expected) in vectors {
            assert!(hash_of(value) == expected.cast_unsigned());
        }
    }

    #[test]
    fn time_matches_time_hash_extended() {
        let vectors: [((i8, i8, i8, i32), i64); 4] = [
            ((0, 0, 0, 0), -4_403_592_609_991_167_795),
            ((1, 0, 0, 0), -3_574_253_606_679_879_661),
            ((12, 34, 56, 789_012_000), -188_368_993_302_954_488),
            ((23, 59, 59, 999_999_000), 9_209_345_884_917_619_299),
        ];
        for ((hour, minute, second, nanos), expected) in vectors {
            let value = Datum::Time(jiff::civil::time(hour, minute, second, nanos));
            assert!(hash_of(value) == expected.cast_unsigned());
        }
    }

    #[test]
    fn timestamp_matches_timestamp_hash_extended() {
        let vectors: [(&str, i64); 5] = [
            ("2000-01-01T00:00:00", -4_403_592_609_991_167_795),
            ("1999-12-31T23:59:59.999999", -5_017_072_347_659_237_694),
            ("2026-07-29T13:45:00.123456", -4_798_227_605_085_807_304),
            ("1970-01-01T00:00:00", -3_440_498_327_794_531_266),
            ("2038-01-19T03:14:07", -6_083_228_224_583_970_603),
        ];
        for (text, expected) in vectors {
            let civil: jiff::civil::DateTime = text.parse().expect("civil datetime");
            assert!(hash_of(Datum::Timestamp(civil)) == expected.cast_unsigned());
        }
    }

    #[test]
    fn timestamptz_matches_timestamptz_hash_extended() {
        let vectors: [(&str, i64); 4] = [
            ("2000-01-01T00:00:00Z", -4_403_592_609_991_167_795),
            ("1999-12-31T23:59:59.999999Z", -5_017_072_347_659_237_694),
            ("2026-07-29T13:45:00.123456Z", -4_798_227_605_085_807_304),
            ("1970-01-01T00:00:00Z", -3_440_498_327_794_531_266),
        ];
        for (text, expected) in vectors {
            let instant: jiff::Timestamp = text.parse().expect("RFC 3339 instant");
            assert!(hash_of(Datum::Timestamptz(instant)) == expected.cast_unsigned());
        }
    }

    #[test]
    fn row_hash_matches_satisfies_hash_partition() {
        // Oracle tables:
        //   create table _h1 (a int)        partition by hash (a);
        //   create table _h2 (a int, b int) partition by hash (a, b);
        //   create table _hv (a int, b text) partition by hash (a, b);
        let vectors: [(Vec<Datum>, u64); 9] = [
            (vec![], 0),
            (vec![Datum::Null, Datum::Null], 0),
            (vec![Datum::Int4(42)], 12_669_485_132_091_644_514),
            (
                vec![Datum::Int4(42), Datum::Null],
                12_669_485_132_091_644_514,
            ),
            (
                vec![Datum::Null, Datum::Text("x".to_string())],
                6_168_220_285_300_744_303,
            ),
            (
                vec![Datum::Int4(42), Datum::Text("x".to_string())],
                4_659_708_685_511_176_001,
            ),
            (
                vec![Datum::Int4(42), Datum::Int4(1)],
                11_062_448_616_865_480_206,
            ),
            (
                vec![Datum::Int4(7), Datum::Int4(999)],
                18_435_676_830_060_938_028,
            ),
            (
                vec![Datum::Int4(-5), Datum::Int4(7)],
                8_592_046_512_590_124_605,
            ),
        ];
        for (values, expected) in vectors {
            assert!(partition_hash(&values) == Ok(expected));
        }
    }

    #[test]
    fn row_hash_composes_mixed_key_types() {
        // create table _h3 (a int, b text, c date, d bool, e int8)
        //   partition by hash (a, b, c, d, e);
        let values = [
            Datum::Int4(42),
            Datum::Text("hello".to_string()),
            Datum::Date(jiff::civil::date(2026, 7, 29)),
            Datum::Bool(true),
            Datum::Int8(-1),
        ];
        assert!(partition_hash(&values) == Ok(18_360_130_902_422_390_541));
    }

    #[test]
    fn remainder_selects_the_partition_postgresql_picks() {
        // The remainder for which satisfies_hash_partition(rel, 8, r, …) is true.
        let vectors: [(Vec<Datum>, u64); 5] = [
            (vec![Datum::Null, Datum::Null], 0),
            (vec![Datum::Int4(42), Datum::Text("x".to_string())], 1),
            (vec![Datum::Int4(42), Datum::Null], 2),
            (vec![Datum::Null, Datum::Text("x".to_string())], 7),
            (
                vec![
                    Datum::Int4(42),
                    Datum::Text("hello".to_string()),
                    Datum::Date(jiff::civil::date(2026, 7, 29)),
                    Datum::Bool(true),
                    Datum::Int8(-1),
                ],
                5,
            ),
        ];
        for (values, expected_remainder) in vectors {
            let row_hash = partition_hash(&values).expect("supported key types");
            assert!(row_hash % 8 == expected_remainder);
        }
    }

    #[test]
    fn unsupported_key_types_are_refused() {
        let vectors: [(Datum, &str); 9] = [
            (Datum::Float4(1.5), "real"),
            (Datum::Float8(1.5), "double precision"),
            (Datum::Numeric(NumericValue::from(1_i64)), "numeric"),
            (
                Datum::Timetz(TimeTz {
                    time: jiff::civil::time(1, 2, 3, 0),
                    offset: jiff::tz::Offset::UTC,
                }),
                "time with time zone",
            ),
            (
                Datum::Interval(Interval {
                    months: 1,
                    days: 2,
                    micros: 3,
                }),
                "interval",
            ),
            (Datum::Jsonb(JsonbValue::Bool(true)), "jsonb"),
            (
                Datum::Array(ArrayValue::new(ElemType::Int4, vec![Datum::Int4(1)])),
                "array",
            ),
            (
                Datum::Record(RecordValue::anonymous(vec![Datum::Int4(1)])),
                "record",
            ),
            (
                Datum::Enum(EnumValue {
                    ty: UserTypeRef {
                        oid: 90_000,
                        name: "mood",
                    },
                    label: "happy".to_string(),
                }),
                "enum",
            ),
        ];
        for (value, type_name) in vectors {
            assert!(partition_hash(&[value]) == Err(unsupported(type_name)));
        }
    }

    #[test]
    fn refusal_names_the_type_and_the_consequence() {
        assert!(
            partition_hash(&[Datum::Numeric(NumericValue::from(1_i64))])
                == Err(ExecError::Unsupported(
                    "hash partitioning on numeric is not supported: PostgreSQL's numeric hash \
                     function is not implemented, so a row could route to the wrong partition"
                        .to_string()
                ))
        );
    }

    #[test]
    fn an_unsupported_column_refuses_the_whole_row() {
        let values = [Datum::Int4(42), Datum::Float8(1.5), Datum::Int4(1)];
        assert!(partition_hash(&values) == Err(unsupported("double precision")));
    }

    #[test]
    fn key_longer_than_postgresql_can_hash_is_refused() {
        assert!(hash_key_len(0) == Ok(0));
        assert!(hash_key_len(u64::from(u32::MAX)) == Ok(u32::MAX));
        assert!(hash_key_len(u64::from(u32::MAX) + 1).is_err());
    }

    #[test]
    fn a_zero_seed_skips_the_perturbation_round() {
        // The seed is only folded into the state when it is non-zero, so an
        // unseeded state is the bare golden-ratio/length initialisation.
        let init = 0x9e37_79b9_u32.wrapping_add(7).wrapping_add(3_923_095);
        assert!(seeded_state(7, 0) == (init, init, init));
        assert!(seeded_state(7, 1) != (init, init, init));
    }
}
