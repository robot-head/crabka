//! `PostgreSQL`'s network address types: `inet`, `cidr`, `macaddr`, `macaddr8`.
//!
//! `inet` and `cidr` share one representation ([`Inet`]) exactly as `PostgreSQL`'s
//! `inet` struct does — an address family, a netmask length, and the address
//! bytes in network byte order. What separates the two SQL types is the output
//! function and the input check, so [`Inet`] carries an `is_cidr` flag that
//! [`Inet::to_text`] and the abbreviation/masklen helpers read. The flag is NOT
//! part of the value's identity: `'10.0.0.0/8'::cidr = '10.0.0.0/8'::inet` is
//! true in `PostgreSQL` because both sides go through one `network_cmp`, so
//! [`Inet`]'s `PartialEq`/`Ord`/`Hash` deliberately ignore it.

use std::{
    cmp::Ordering,
    fmt::Write as _,
    hash::{Hash, Hasher},
};

use crate::TypeError;

/// The address family of an [`Inet`].
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub enum InetFamily {
    /// IPv4 — 4 address bytes, at most 32 mask bits.
    V4,
    /// IPv6 — 16 address bytes, at most 128 mask bits.
    V6,
}

impl InetFamily {
    /// The number of significant address bytes (`ip_addrsize`).
    #[must_use]
    pub const fn addr_len(self) -> usize {
        match self {
            InetFamily::V4 => 4,
            InetFamily::V6 => 16,
        }
    }

    /// The widest netmask this family accepts (`ip_maxbits`).
    #[must_use]
    pub const fn max_bits(self) -> u8 {
        match self {
            InetFamily::V4 => 32,
            InetFamily::V6 => 128,
        }
    }

    /// What `family(inet)` reports: 4 or 6.
    #[must_use]
    pub const fn number(self) -> i32 {
        match self {
            InetFamily::V4 => 4,
            InetFamily::V6 => 6,
        }
    }

    /// The byte `PostgreSQL`'s binary `inet` format carries — `PGSQL_AF_INET`
    /// (2) and `PGSQL_AF_INET6` (3), which are `AF_INET` and `AF_INET + 1`.
    #[must_use]
    pub const fn wire_code(self) -> u8 {
        match self {
            InetFamily::V4 => 2,
            InetFamily::V6 => 3,
        }
    }

    /// The family a binary `inet` value's leading byte names.
    #[must_use]
    pub const fn from_wire_code(code: u8) -> Option<Self> {
        match code {
            2 => Some(InetFamily::V4),
            3 => Some(InetFamily::V6),
            _ => None,
        }
    }
}

/// A `PostgreSQL` `inet` or `cidr` value.
///
/// `addr` is always 16 bytes with everything past `family.addr_len()` zeroed, so
/// two values compare and hash on the whole buffer regardless of family.
#[derive(Debug, Clone, Copy)]
pub struct Inet {
    /// Whether this value is a `cidr` (`true`) or an `inet` (`false`). Read by
    /// the output and abbreviation functions only — never by comparison.
    pub is_cidr: bool,
    /// The address family.
    pub family: InetFamily,
    /// The netmask length, at most `family.max_bits()`.
    pub bits: u8,
    /// The address in network byte order, zero-padded to 16 bytes.
    pub addr: [u8; 16],
}

impl PartialEq for Inet {
    fn eq(&self, other: &Self) -> bool {
        // `is_cidr` is excluded on purpose: PostgreSQL routes `inet = cidr`
        // through the same `network_cmp`, so the two spellings of one address
        // are one value.
        self.family == other.family && self.bits == other.bits && self.addr == other.addr
    }
}

impl Eq for Inet {}

impl Hash for Inet {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.family.hash(state);
        self.bits.hash(state);
        self.addr.hash(state);
    }
}

impl PartialOrd for Inet {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for Inet {
    /// `network_cmp_internal`: IPv4 sorts before IPv6; within a family the
    /// common network bits are the major key, then the netmask length, then the
    /// whole unmasked address.
    fn cmp(&self, other: &Self) -> Ordering {
        if self.family != other.family {
            return self.family.cmp(&other.family);
        }
        let common = self.bits.min(other.bits);
        let order = bitncmp(&self.addr, &other.addr, usize::from(common));
        if order != Ordering::Equal {
            return order;
        }
        let order = self.bits.cmp(&other.bits);
        if order != Ordering::Equal {
            return order;
        }
        bitncmp(&self.addr, &other.addr, usize::from(self.family.max_bits()))
    }
}

/// `bitncmp`: compare the first `n` bits of two network-order addresses.
fn bitncmp(left: &[u8; 16], right: &[u8; 16], n: usize) -> Ordering {
    let whole = n / 8;
    let order = left[..whole].cmp(&right[..whole]);
    if order != Ordering::Equal || n.is_multiple_of(8) {
        return order;
    }
    let keep = 8 - (n % 8);
    let mask = 0xffu8 << keep;
    (left[whole] & mask).cmp(&(right[whole] & mask))
}

/// `bitncommon`: how many of the first `n` bits two addresses share.
fn bitncommon(left: &[u8; 16], right: &[u8; 16], n: usize) -> u8 {
    let mut byte = 0;
    let mut nbits = n % 8;
    while byte < n / 8 {
        if left[byte] != right[byte] {
            nbits = 7;
            break;
        }
        byte += 1;
    }
    if nbits != 0 {
        let diff = u32::from(left[byte] ^ right[byte]);
        while (diff >> (8 - nbits)) != 0 {
            nbits -= 1;
        }
    }
    u8::try_from(8 * byte + nbits).expect("at most 128 common bits")
}

fn invalid_inet(is_cidr: bool, value: &str) -> TypeError {
    TypeError::InvalidText {
        type_name: if is_cidr { "cidr" } else { "inet" },
        value: value.to_string(),
    }
}

impl Inet {
    /// Build a value from its parts, zeroing everything past the family's
    /// address length so equality and hashing see one canonical buffer.
    #[must_use]
    pub fn new(is_cidr: bool, family: InetFamily, bits: u8, addr: [u8; 16]) -> Self {
        let mut addr = addr;
        addr[family.addr_len()..].fill(0);
        Inet {
            is_cidr,
            family,
            bits,
            addr,
        }
    }

    /// `inet_in` / `cidr_in`.
    ///
    /// A `:` anywhere in the input selects IPv6, as `PostgreSQL`'s `network_in`
    /// does. `cidr` additionally rejects any address bit set to the right of
    /// the netmask.
    ///
    /// # Errors
    ///
    /// `22P02` `invalid input syntax for type inet/cidr` for anything the
    /// address grammar rejects, and `22P02` `invalid cidr value` (with
    /// `PostgreSQL`'s `Value has bits set to right of mask.` detail) for a `cidr`
    /// whose host part is non-zero.
    pub fn parse(input: &str, is_cidr: bool) -> Result<Self, TypeError> {
        let family = if input.contains(':') {
            InetFamily::V6
        } else {
            InetFamily::V4
        };
        let mut addr = [0u8; 16];
        let bits = match (family, is_cidr) {
            (InetFamily::V4, true) => cidr_pton_v4(input.as_bytes(), &mut addr),
            (InetFamily::V4, false) => net_pton_v4(input.as_bytes(), &mut addr),
            // `inet_net_pton_ipv6` is `inet_cidr_pton_ipv6` with size 16, so
            // both SQL types share one IPv6 grammar.
            (InetFamily::V6, _) => cidr_pton_v6(input.as_bytes(), &mut addr),
        }
        .ok_or_else(|| invalid_inet(is_cidr, input))?;
        if bits > family.max_bits() {
            return Err(invalid_inet(is_cidr, input));
        }
        if is_cidr && !address_ok(&addr, bits, family) {
            return Err(TypeError::InvalidCidr {
                value: input.to_string(),
            });
        }
        Ok(Inet::new(is_cidr, family, bits, addr))
    }

    /// `inet_out` / `cidr_out`: the default text rendering. `cidr` always shows
    /// its netmask; `inet` hides a netmask equal to the family's maximum.
    #[must_use]
    pub fn to_text(&self) -> String {
        let mut out = net_ntop(self.family, &self.addr, self.bits);
        if self.is_cidr && !out.contains('/') {
            out.push('/');
            out.push_str(&self.bits.to_string());
        }
        out
    }

    /// `network_show`: what the `inet`/`cidr` → `text` cast produces. Unlike
    /// [`Inet::to_text`] it prints the full host address and always appends the
    /// netmask.
    #[must_use]
    pub fn show(&self) -> String {
        let mut out = net_ntop(self.family, &self.addr, self.family.max_bits());
        if !out.contains('/') {
            out.push('/');
            out.push_str(&self.bits.to_string());
        }
        out
    }

    /// `host(inet)`: the address with no netmask.
    #[must_use]
    pub fn host(&self) -> String {
        let text = net_ntop(self.family, &self.addr, self.family.max_bits());
        match text.split_once('/') {
            Some((head, _)) => head.to_string(),
            None => text,
        }
    }

    /// `abbrev(inet)` / `abbrev(cidr)`: the shortest text that still identifies
    /// the value. For `inet` that is the same as `inet_out`; for `cidr` it is
    /// `inet_cidr_ntop`, which drops octets and words past the netmask.
    #[must_use]
    pub fn abbrev(&self) -> String {
        if self.is_cidr {
            cidr_ntop(self.family, &self.addr, self.bits)
        } else {
            net_ntop(self.family, &self.addr, self.bits)
        }
    }

    /// `masklen(inet)`.
    #[must_use]
    pub fn masklen(&self) -> i32 {
        i32::from(self.bits)
    }

    /// `family(inet)`: 4 or 6.
    #[must_use]
    pub fn family_number(&self) -> i32 {
        self.family.number()
    }

    /// The same address seen as an `inet` (`cidr` → `inet` is implicit and
    /// binary-coercible in `PostgreSQL`).
    #[must_use]
    pub fn as_inet(&self) -> Self {
        Inet {
            is_cidr: false,
            ..*self
        }
    }

    /// `inet_to_cidr` — the `inet` → `cidr` cast, which zeroes every bit to the
    /// right of the netmask.
    #[must_use]
    pub fn to_cidr(&self) -> Self {
        self.with_cidr_masklen(self.bits)
    }

    /// `cidr_set_masklen_internal`: keep `bits` netmask bits, zero the rest.
    #[must_use]
    fn with_cidr_masklen(&self, bits: u8) -> Self {
        let mut addr = [0u8; 16];
        if bits > 0 {
            let whole = usize::from(bits) / 8;
            let partial = usize::from(bits) % 8;
            addr[..whole].copy_from_slice(&self.addr[..whole]);
            if partial != 0 {
                addr[whole] = self.addr[whole] & !(0xffu8 >> partial);
            }
        }
        Inet::new(true, self.family, bits, addr)
    }

    /// `inet_set_masklen`: change the netmask, keeping every address bit.
    ///
    /// # Errors
    ///
    /// `22023` `invalid mask length: n` when `bits` is outside `-1 ..= maxbits`.
    pub fn set_masklen(&self, bits: i32) -> Result<Self, TypeError> {
        let bits = self.checked_masklen(bits)?;
        Ok(Inet { bits, ..*self })
    }

    /// `cidr_set_masklen`: change the netmask and zero everything to its right.
    ///
    /// # Errors
    ///
    /// `22023` `invalid mask length: n` when `bits` is outside `-1 ..= maxbits`.
    pub fn set_cidr_masklen(&self, bits: i32) -> Result<Self, TypeError> {
        let bits = self.checked_masklen(bits)?;
        Ok(self.with_cidr_masklen(bits))
    }

    fn checked_masklen(&self, bits: i32) -> Result<u8, TypeError> {
        let bits = if bits == -1 {
            i32::from(self.family.max_bits())
        } else {
            bits
        };
        if bits < 0 || bits > i32::from(self.family.max_bits()) {
            return Err(TypeError::Coded {
                sqlstate: "22023",
                message: format!("invalid mask length: {bits}"),
            });
        }
        Ok(u8::try_from(bits).expect("range-checked above"))
    }

    /// `broadcast(inet)`: every bit to the right of the netmask set. The result
    /// is an `inet` with the same netmask.
    #[must_use]
    pub fn broadcast(&self) -> Self {
        let mut addr = [0u8; 16];
        let mut bits = usize::from(self.bits);
        for (slot, source) in addr.iter_mut().zip(self.addr).take(self.family.addr_len()) {
            let mask = if bits >= 8 {
                bits -= 8;
                0x00
            } else if bits == 0 {
                0xff
            } else {
                let mask = 0xffu8 >> bits;
                bits = 0;
                mask
            };
            *slot = source | mask;
        }
        Inet::new(false, self.family, self.bits, addr)
    }

    /// `network(inet)`: the address with its host part zeroed, as a `cidr`.
    #[must_use]
    pub fn network(&self) -> Self {
        let mut result = self.with_cidr_masklen(self.bits);
        result.bits = self.bits;
        result
    }

    /// `netmask(inet)`: the netmask itself, as an `inet` with a full masklen.
    #[must_use]
    pub fn netmask(&self) -> Self {
        let mut addr = [0u8; 16];
        let mut bits = usize::from(self.bits);
        let mut byte = 0;
        while bits > 0 {
            let mask = if bits >= 8 {
                bits -= 8;
                0xffu8
            } else {
                let mask = 0xffu8 << (8 - bits);
                bits = 0;
                mask
            };
            addr[byte] = mask;
            byte += 1;
        }
        Inet::new(false, self.family, self.family.max_bits(), addr)
    }

    /// `hostmask(inet)`: the complement of the netmask, as an `inet` with a full
    /// masklen.
    #[must_use]
    pub fn hostmask(&self) -> Self {
        let mut addr = [0u8; 16];
        let mut bits = usize::from(self.family.max_bits() - self.bits);
        let mut byte = self.family.addr_len();
        while bits > 0 {
            byte -= 1;
            let mask = if bits >= 8 {
                bits -= 8;
                0xffu8
            } else {
                let mask = 0xffu8 >> (8 - bits);
                bits = 0;
                mask
            };
            addr[byte] = mask;
        }
        Inet::new(false, self.family, self.family.max_bits(), addr)
    }

    /// `~inet`: every address bit inverted, netmask unchanged.
    #[must_use]
    pub fn not(&self) -> Self {
        let mut addr = [0u8; 16];
        for (slot, source) in addr.iter_mut().zip(self.addr).take(self.family.addr_len()) {
            *slot = !source;
        }
        Inet::new(false, self.family, self.bits, addr)
    }

    /// `inet & inet`. The result's netmask is the wider of the two.
    ///
    /// # Errors
    ///
    /// `22023` `cannot AND inet values of different sizes` across families.
    pub fn and(&self, other: &Self) -> Result<Self, TypeError> {
        self.bitwise(other, "AND", |a, b| a & b)
    }

    /// `inet | inet`. The result's netmask is the wider of the two.
    ///
    /// # Errors
    ///
    /// `22023` `cannot OR inet values of different sizes` across families.
    pub fn or(&self, other: &Self) -> Result<Self, TypeError> {
        self.bitwise(other, "OR", |a, b| a | b)
    }

    fn bitwise(
        &self,
        other: &Self,
        verb: &'static str,
        op: fn(u8, u8) -> u8,
    ) -> Result<Self, TypeError> {
        if self.family != other.family {
            return Err(TypeError::Coded {
                sqlstate: "22023",
                message: format!("cannot {verb} inet values of different sizes"),
            });
        }
        let mut addr = [0u8; 16];
        for (slot, (left, right)) in addr
            .iter_mut()
            .zip(self.addr.into_iter().zip(other.addr))
            .take(self.family.addr_len())
        {
            *slot = op(left, right);
        }
        Ok(Inet::new(
            false,
            self.family,
            self.bits.max(other.bits),
            addr,
        ))
    }

    /// `inet + bigint` (and, with a negated addend, `inet - bigint`).
    ///
    /// # Errors
    ///
    /// `22003` `result is out of range` when the sum leaves the address space.
    pub fn add_offset(&self, addend: i64) -> Result<Self, TypeError> {
        let len = self.family.addr_len();
        // `internal_inetpl` consumes `addend` one byte at a time, low byte
        // first, clearing the low byte before each division so the rounding
        // direction is defined for a negative addend. That sequence of bytes is
        // exactly the two's-complement little-endian spelling of `addend`, sign
        // extended past the eighth byte.
        let bytes = addend.to_le_bytes();
        let sign_byte = if addend < 0 { 0xFFu8 } else { 0x00 };
        let mut addr = [0u8; 16];
        let mut carry: u16 = 0;
        for (index, byte) in (0..len).rev().enumerate() {
            let low = bytes.get(index).copied().unwrap_or(sign_byte);
            let sum = u16::from(self.addr[byte]) + u16::from(low) + carry;
            let [result, _] = sum.to_le_bytes();
            addr[byte] = result;
            carry = sum >> 8;
        }
        // What `internal_inetpl` has left over: zero and no carry for a
        // non-negative addend that fitted, -1 and a carry of one for a negative
        // one. Anything else overflowed the address space.
        let residual = if len >= 8 {
            -i64::from(addend < 0)
        } else {
            addend >> (8 * u32::try_from(len).unwrap_or(8))
        };
        if !((residual == 0 && carry == 0) || (residual == -1 && carry == 1)) {
            return Err(TypeError::OutOfRange {
                message: "result is out of range".to_string(),
            });
        }
        Ok(Inet::new(false, self.family, self.bits, addr))
    }

    /// `inet - inet` → `bigint`.
    ///
    /// # Errors
    ///
    /// `22023` across families, `22003` when the difference exceeds `bigint`.
    pub fn difference(&self, other: &Self) -> Result<i64, TypeError> {
        if self.family != other.family {
            return Err(TypeError::Coded {
                sqlstate: "22023",
                message: "cannot subtract inet values of different sizes".to_string(),
            });
        }
        // Two's complement: add the complement of `other` with the carry primed
        // at one.
        let len = self.family.addr_len();
        let mut result = 0u64;
        let mut carry: u16 = 1;
        for (index, byte) in (0..len).rev().enumerate() {
            let sum = u16::from(self.addr[byte]) + u16::from(!other.addr[byte]) + carry;
            let [lobyte, _] = sum.to_le_bytes();
            carry = sum >> 8;
            if index < 8 {
                result |= u64::from(lobyte) << (index * 8);
                continue;
            }
            // Wider than `int8`: every byte past the eighth must agree with the
            // sign of what fits, or the difference does not.
            let negative = result & (1 << 63) != 0;
            let expected = if negative { 0xFFu8 } else { 0x00 };
            if lobyte != expected {
                return Err(TypeError::OutOfRange {
                    message: "result is out of range".to_string(),
                });
            }
        }
        // Narrower than `int8` (IPv4): sign-extend rather than range-check.
        if carry == 0 && len < 8 {
            result |= u64::MAX << (len * 8);
        }
        Ok(result.cast_signed())
    }

    /// `inet << inet`: strictly contained by.
    #[must_use]
    pub fn is_subnet_of(&self, other: &Self) -> bool {
        self.family == other.family
            && self.bits > other.bits
            && bitncmp(&self.addr, &other.addr, usize::from(other.bits)) == Ordering::Equal
    }

    /// `inet <<= inet`: contained by or equal.
    #[must_use]
    pub fn is_subnet_of_or_eq(&self, other: &Self) -> bool {
        self.family == other.family
            && self.bits >= other.bits
            && bitncmp(&self.addr, &other.addr, usize::from(other.bits)) == Ordering::Equal
    }

    /// `inet >> inet`: strictly contains.
    #[must_use]
    pub fn is_supernet_of(&self, other: &Self) -> bool {
        other.is_subnet_of(self)
    }

    /// `inet >>= inet`: contains or equal.
    #[must_use]
    pub fn is_supernet_of_or_eq(&self, other: &Self) -> bool {
        other.is_subnet_of_or_eq(self)
    }

    /// `inet && inet`: either address contains the other.
    #[must_use]
    pub fn overlaps(&self, other: &Self) -> bool {
        self.family == other.family
            && bitncmp(
                &self.addr,
                &other.addr,
                usize::from(self.bits.min(other.bits)),
            ) == Ordering::Equal
    }

    /// `inet_same_family`.
    #[must_use]
    pub fn same_family(&self, other: &Self) -> bool {
        self.family == other.family
    }

    /// `inet_merge`: the smallest `cidr` containing both addresses.
    ///
    /// # Errors
    ///
    /// `22023` `cannot merge addresses from different families`.
    pub fn merge(&self, other: &Self) -> Result<Self, TypeError> {
        if self.family != other.family {
            return Err(TypeError::Coded {
                sqlstate: "22023",
                message: "cannot merge addresses from different families".to_string(),
            });
        }
        let common = bitncommon(
            &self.addr,
            &other.addr,
            usize::from(self.bits.min(other.bits)),
        );
        Ok(self.with_cidr_masklen(common))
    }

    /// `inet_send` / `cidr_send`: family, netmask, the `is_cidr` flag, the
    /// address length, then the address.
    #[must_use]
    pub fn to_binary(&self) -> Vec<u8> {
        let len = self.family.addr_len();
        let mut out = Vec::with_capacity(len + 4);
        out.push(self.family.wire_code());
        out.push(self.bits);
        out.push(u8::from(self.is_cidr));
        out.push(match self.family {
            InetFamily::V4 => 4,
            InetFamily::V6 => 16,
        });
        out.extend_from_slice(&self.addr[..len]);
        out
    }

    /// `inet_recv` / `cidr_recv`. The transmitted `is_cidr` byte is ignored, as
    /// `PostgreSQL` ignores it; `is_cidr` comes from the type being decoded into.
    ///
    /// # Errors
    ///
    /// `22P03` for a malformed family, netmask, or length, and for a `cidr`
    /// with bits set to the right of the mask.
    pub fn from_binary(bytes: &[u8], is_cidr: bool) -> Result<Self, TypeError> {
        let type_name = if is_cidr { "cidr" } else { "inet" };
        let malformed = |message: String| TypeError::Coded {
            sqlstate: "22P03",
            message,
        };
        let [family, bits, _ignored_is_cidr, len, rest @ ..] = bytes else {
            return Err(malformed(format!(
                "invalid length in external \"{type_name}\" value"
            )));
        };
        let family = InetFamily::from_wire_code(*family).ok_or_else(|| {
            malformed(format!(
                "invalid address family in external \"{type_name}\" value"
            ))
        })?;
        if *bits > family.max_bits() {
            return Err(malformed(format!(
                "invalid bits in external \"{type_name}\" value"
            )));
        }
        if usize::from(*len) != family.addr_len() || rest.len() != family.addr_len() {
            return Err(malformed(format!(
                "invalid length in external \"{type_name}\" value"
            )));
        }
        let mut addr = [0u8; 16];
        addr[..rest.len()].copy_from_slice(rest);
        if is_cidr && !address_ok(&addr, *bits, family) {
            return Err(malformed("invalid external \"cidr\" value".to_string()));
        }
        Ok(Inet::new(is_cidr, family, *bits, addr))
    }
}

/// `addressOK`: no address bit may be set to the right of a `cidr`'s netmask.
fn address_ok(addr: &[u8; 16], bits: u8, family: InetFamily) -> bool {
    if bits == family.max_bits() {
        return true;
    }
    let mut byte = usize::from(bits) / 8;
    let mut mask = if bits == 0 {
        0xffu8
    } else {
        0xffu8 >> (bits % 8)
    };
    while byte < family.addr_len() {
        if addr[byte] & mask != 0 {
            return false;
        }
        mask = 0xff;
        byte += 1;
    }
    true
}

// ---------------------------------------------------------------------------
// Input: pg_inet_net_pton
// ---------------------------------------------------------------------------

fn hex_value(byte: u8) -> Option<u8> {
    match byte {
        b'0'..=b'9' => Some(byte - b'0'),
        b'a'..=b'f' => Some(byte - b'a' + 10),
        b'A'..=b'F' => Some(byte - b'A' + 10),
        _ => None,
    }
}

fn digit_value(byte: u8) -> Option<u8> {
    byte.is_ascii_digit().then(|| byte - b'0')
}

/// The netmask a partial dotted-decimal `cidr` gets when none is written —
/// `inet_cidr_pton_ipv4`'s classful inference.
fn classful_bits(first: u8, written: usize) -> u8 {
    let mut bits: u8 = if first >= 240 {
        32
    } else if first >= 224 {
        8
    } else if first >= 192 {
        24
    } else if first >= 128 {
        16
    } else {
        8
    };
    let written_bits = u8::try_from(written * 8).unwrap_or(u8::MAX);
    if bits < written_bits {
        bits = written_bits;
    }
    if bits == 8 && first == 224 {
        bits = 4;
    }
    bits
}

/// Read `/nnn` starting just past the slash. `None` when the tail is not a
/// well-formed decimal run, or when it exceeds `limit`.
fn trailing_bits(src: &[u8], limit: u32) -> Option<u8> {
    if src.is_empty() || !src.iter().all(u8::is_ascii_digit) {
        return None;
    }
    let mut bits: u32 = 0;
    for &byte in src {
        bits = bits
            .checked_mul(10)?
            .checked_add(u32::from(digit_value(byte)?))?;
        if bits > limit {
            return None;
        }
    }
    u8::try_from(bits).ok()
}

/// `inet_cidr_pton_ipv4` — the `cidr` IPv4 grammar. Accepts a `0x` nybble
/// string, one to four dotted decimal octets, and an optional `/nnn`; infers a
/// classful netmask when none is written.
fn cidr_pton_v4(src: &[u8], dst: &mut [u8; 16]) -> Option<u8> {
    let mut written = 0usize;
    let mut index = 0usize;
    if src.len() >= 3
        && src[0] == b'0'
        && (src[1] == b'x' || src[1] == b'X')
        && src[2].is_ascii_hexdigit()
    {
        index = 2;
        let mut nybbles: Vec<u8> = Vec::new();
        while index < src.len() {
            match hex_value(src[index]) {
                Some(value) => {
                    nybbles.push(value);
                    index += 1;
                }
                None => break,
            }
        }
        for pair in nybbles.chunks(2) {
            if written >= 4 {
                return None;
            }
            dst[written] = match pair {
                [high, low] => (high << 4) | low,
                [odd] => odd << 4,
                _ => unreachable!("chunks(2) yields one or two elements"),
            };
            written += 1;
        }
    } else if src.first().is_some_and(u8::is_ascii_digit) {
        loop {
            let mut octet: u32 = 0;
            let mut digits = 0;
            while index < src.len() && src[index].is_ascii_digit() {
                octet = octet * 10 + u32::from(digit_value(src[index])?);
                if octet > 255 {
                    return None;
                }
                digits += 1;
                index += 1;
            }
            if digits == 0 {
                return None;
            }
            if written >= 4 {
                return None;
            }
            dst[written] = u8::try_from(octet).expect("range-checked above");
            written += 1;
            match src.get(index) {
                None | Some(b'/') => break,
                Some(b'.') => index += 1,
                Some(_) => return None,
            }
        }
    } else {
        return None;
    }

    let mut bits = None;
    if src.get(index) == Some(&b'/') && written > 0 {
        // A `/` not followed by a digit is not a width specifier; the trailing
        // check below then rejects the input, as PostgreSQL does.
        if src.get(index + 1).is_some_and(u8::is_ascii_digit) {
            bits = Some(trailing_bits(&src[index + 1..], 32)?);
            index = src.len();
        }
    }
    if index != src.len() {
        return None;
    }
    if written == 0 {
        return None;
    }
    let bits = bits.unwrap_or_else(|| classful_bits(dst[0], written));
    // Extend the address so it covers the whole netmask.
    while usize::from(bits) > written * 8 {
        if written >= 4 {
            return None;
        }
        dst[written] = 0;
        written += 1;
    }
    Some(bits)
}

/// `inet_net_pton_ipv4` — the `inet` IPv4 grammar. Unlike `cidr`, it takes no
/// hex form and requires all four octets whenever no `/nnn` is written.
fn net_pton_v4(src: &[u8], dst: &mut [u8; 16]) -> Option<u8> {
    let mut written = 0usize;
    let mut index = 0usize;
    let mut terminator: Option<u8> = None;
    while index < src.len() && src[index].is_ascii_digit() {
        let mut octet: u32 = 0;
        while index < src.len() && src[index].is_ascii_digit() {
            octet = octet * 10 + u32::from(digit_value(src[index])?);
            if octet > 255 {
                return None;
            }
            index += 1;
        }
        if written >= 4 {
            return None;
        }
        dst[written] = u8::try_from(octet).expect("range-checked above");
        written += 1;
        match src.get(index) {
            None => {
                terminator = None;
                break;
            }
            Some(b'/') => {
                terminator = Some(b'/');
                break;
            }
            Some(b'.') => index += 1,
            Some(_) => return None,
        }
    }

    let mut bits = None;
    if terminator == Some(b'/') && written > 0 && src.get(index + 1).is_some_and(u8::is_ascii_digit)
    {
        bits = Some(trailing_bits(&src[index + 1..], 32)?);
        index = src.len();
        terminator = None;
    }
    if terminator.is_some() || index != src.len() {
        return None;
    }
    let bits = match bits {
        Some(bits) => bits,
        // Only a complete dotted quad may default to /32.
        None if written == 4 => 32,
        None => return None,
    };
    if written == 0 || usize::from(bits) / 8 > written {
        return None;
    }
    Some(bits)
}

/// `getv4`: the dotted-quad tail of an IPv4-in-IPv6 address, with an optional
/// `/nnn`. Rejects leading zeros, as `PostgreSQL`'s `getv4` does.
fn embedded_v4(src: &[u8], dst: &mut [u8], bits: &mut Option<u8>) -> bool {
    let mut written = 0usize;
    let mut value: u32 = 0;
    let mut digits = 0;
    for (index, &byte) in src.iter().enumerate() {
        if let Some(digit) = digit_value(byte) {
            if digits != 0 && value == 0 {
                return false;
            }
            digits += 1;
            value = value * 10 + u32::from(digit);
            if value > 255 {
                return false;
            }
            continue;
        }
        if byte == b'.' || byte == b'/' {
            if written > 3 {
                return false;
            }
            dst[written] = u8::try_from(value).expect("range-checked above");
            written += 1;
            if byte == b'/' {
                return match trailing_bits(&src[index + 1..], 128) {
                    Some(parsed) => {
                        *bits = Some(parsed);
                        true
                    }
                    None => false,
                };
            }
            value = 0;
            digits = 0;
            continue;
        }
        return false;
    }
    if digits == 0 || written > 3 {
        return false;
    }
    dst[written] = u8::try_from(value).expect("range-checked above");
    true
}

/// `inet_cidr_pton_ipv6`, which `inet_net_pton_ipv6` also delegates to.
fn cidr_pton_v6(src: &[u8], dst: &mut [u8; 16]) -> Option<u8> {
    let mut tmp = [0u8; 16];
    let mut tp = 0usize;
    let mut colonp: Option<usize> = None;
    let mut index = 0usize;
    // A leading `:` is only legal as part of `::`.
    if src.first() == Some(&b':') {
        index += 1;
        if src.get(index) != Some(&b':') {
            return None;
        }
    }
    let mut curtok = index;
    let mut saw_xdigit = false;
    let mut value: u32 = 0;
    let mut digits = 0;
    let mut bits: Option<u8> = None;
    while index < src.len() {
        let byte = src[index];
        index += 1;
        if let Some(hex) = hex_value(byte) {
            value = (value << 4) | u32::from(hex);
            digits += 1;
            if digits > 4 {
                return None;
            }
            saw_xdigit = true;
            continue;
        }
        if byte == b':' {
            curtok = index;
            if !saw_xdigit {
                if colonp.is_some() {
                    return None;
                }
                colonp = Some(tp);
                continue;
            }
            if index >= src.len() {
                return None;
            }
            if tp + 2 > 16 {
                return None;
            }
            tmp[tp] = u8::try_from((value >> 8) & 0xff).expect("masked to one byte");
            tmp[tp + 1] = u8::try_from(value & 0xff).expect("masked to one byte");
            tp += 2;
            saw_xdigit = false;
            digits = 0;
            value = 0;
            continue;
        }
        if byte == b'.' && tp + 4 <= 16 && embedded_v4(&src[curtok..], &mut tmp[tp..], &mut bits) {
            tp += 4;
            saw_xdigit = false;
            break;
        }
        if byte == b'/' {
            bits = Some(trailing_bits(&src[index..], 128)?);
            break;
        }
        return None;
    }
    if saw_xdigit {
        if tp + 2 > 16 {
            return None;
        }
        tmp[tp] = u8::try_from((value >> 8) & 0xff).expect("masked to one byte");
        tmp[tp + 1] = u8::try_from(value & 0xff).expect("masked to one byte");
        tp += 2;
    }
    let bits = bits.unwrap_or(128);
    if let Some(colonp) = colonp {
        // Slide everything written after the `::` to the end of the buffer.
        let n = tp - colonp;
        if tp == 16 {
            return None;
        }
        for offset in 1..=n {
            tmp[16 - offset] = tmp[colonp + n - offset];
            tmp[colonp + n - offset] = 0;
        }
        tp = 16;
    }
    if tp != 16 {
        return None;
    }
    *dst = tmp;
    Some(bits)
}

// ---------------------------------------------------------------------------
// Output: pg_inet_net_ntop and pg_inet_cidr_ntop
// ---------------------------------------------------------------------------

/// `pg_inet_net_ntop`: the host-address rendering, printing every octet or word
/// regardless of the netmask and suffixing `/bits` unless it is the maximum.
fn net_ntop(family: InetFamily, addr: &[u8; 16], bits: u8) -> String {
    match family {
        InetFamily::V4 => {
            let mut out = format!("{}.{}.{}.{}", addr[0], addr[1], addr[2], addr[3]);
            if bits != 32 {
                out.push('/');
                out.push_str(&bits.to_string());
            }
            out
        }
        InetFamily::V6 => {
            let mut out = net_ntop_v6_body(addr);
            if bits != 128 {
                out.push('/');
                out.push_str(&bits.to_string());
            }
            out
        }
    }
}

/// The `::`-shortened IPv6 body `inet_net_ntop_ipv6` produces, without a
/// netmask suffix.
fn net_ntop_v6_body(addr: &[u8; 16]) -> String {
    let words: [u16; 8] =
        std::array::from_fn(|i| u16::from_be_bytes([addr[i * 2], addr[i * 2 + 1]]));
    let (best_base, best_len) = longest_zero_run(&words);

    let mut out = String::new();
    for (index, word) in words.iter().enumerate() {
        if let Some(base) = best_base
            && index >= base
            && index < base + best_len
        {
            if index == base {
                out.push(':');
            }
            continue;
        }
        if index != 0 {
            out.push(':');
        }
        // An IPv4-mapped or IPv4-compatible tail prints in dotted-quad form.
        if index == 6
            && best_base == Some(0)
            && (best_len == 6
                || (best_len == 7 && words[7] != 0x0001)
                || (best_len == 5 && words[5] == 0xffff))
        {
            let _ = write!(out, "{}.{}.{}.{}", addr[12], addr[13], addr[14], addr[15]);
            return out;
        }
        let _ = write!(out, "{word:x}");
    }
    if let Some(base) = best_base
        && base + best_len == 8
    {
        out.push(':');
    }
    out
}

/// The longest run of at least two zero words, which is what `::` replaces.
fn longest_zero_run(words: &[u16; 8]) -> (Option<usize>, usize) {
    let mut best: Option<(usize, usize)> = None;
    let mut current: Option<(usize, usize)> = None;
    for (index, word) in words.iter().enumerate() {
        if *word == 0 {
            current = Some(match current {
                Some((base, len)) => (base, len + 1),
                None => (index, 1),
            });
        } else if let Some(run) = current.take()
            && best.is_none_or(|(_, len)| run.1 > len)
        {
            best = Some(run);
        }
    }
    if let Some(run) = current
        && best.is_none_or(|(_, len)| run.1 > len)
    {
        best = Some(run);
    }
    match best {
        Some((base, len)) if len >= 2 => (Some(base), len),
        _ => (None, 0),
    }
}

/// `pg_inet_cidr_ntop`: the network-only rendering `abbrev(cidr)` uses, which
/// drops every octet or word past the netmask and always shows `/bits`.
fn cidr_ntop(family: InetFamily, addr: &[u8; 16], bits: u8) -> String {
    match family {
        InetFamily::V4 => cidr_ntop_v4(addr, bits),
        InetFamily::V6 => cidr_ntop_v6(addr, bits),
    }
}

fn cidr_ntop_v4(addr: &[u8; 16], bits: u8) -> String {
    let mut out = String::new();
    if bits == 0 {
        out.push('0');
    }
    for octet in addr.iter().take(usize::from(bits) / 8) {
        if !out.is_empty() {
            out.push('.');
        }
        out.push_str(&octet.to_string());
    }
    let partial = bits % 8;
    if partial > 0 {
        if !out.is_empty() {
            out.push('.');
        }
        let mask = ((1u16 << partial) - 1) << (8 - partial);
        out.push_str(&(u16::from(addr[usize::from(bits) / 8]) & mask).to_string());
    }
    out.push('/');
    out.push_str(&bits.to_string());
    out
}

fn cidr_ntop_v6(addr: &[u8; 16], bits: u8) -> String {
    if bits == 0 {
        return "::/0".to_string();
    }
    // Zero the host part before shortening, so the `::` run reflects the
    // network only.
    let mut masked = [0u8; 16];
    let whole = usize::from(bits).div_ceil(8);
    masked[..whole].copy_from_slice(&addr[..whole]);
    let partial = bits % 8;
    if partial != 0 {
        masked[whole - 1] &= 0xffu8 << (8 - partial);
    }

    // Only the words the netmask reaches are printed (always at least two).
    let words = usize::from(bits).div_ceil(16).max(2);
    let all: [u16; 8] =
        std::array::from_fn(|i| u16::from_be_bytes([masked[i * 2], masked[i * 2 + 1]]));
    let (zero_s, zero_l) = longest_zero_run_prefix(&all[..words]);
    let is_ipv4 = zero_l != words
        && zero_s == 0
        && (zero_l == 6
            || (zero_l == 5 && masked[10] == 0xff && masked[11] == 0xff)
            || (zero_l == 7 && masked[14] != 0 && masked[15] != 1));

    let mut out = String::new();
    for word in 0..words {
        if zero_l != 0 && word >= zero_s && word < zero_s + zero_l {
            if word == zero_s {
                out.push(':');
            }
            if word == words - 1 {
                out.push(':');
            }
            continue;
        }
        if is_ipv4 && word > 5 {
            out.push(if word == 6 { ':' } else { '.' });
            out.push_str(&masked[word * 2].to_string());
            // The very last octet is dropped unless the netmask reaches it.
            if word != 7 || bits > 120 {
                out.push('.');
                out.push_str(&masked[word * 2 + 1].to_string());
            }
        } else {
            if !out.is_empty() {
                out.push(':');
            }
            let _ = write!(out, "{:x}", all[word]);
        }
    }
    out.push('/');
    out.push_str(&bits.to_string());
    out
}

/// `inet_cidr_ntop_ipv6`'s zero-run search, which — unlike the `inet` one —
/// keeps a run of length one and scans only the printed prefix.
fn longest_zero_run_prefix(words: &[u16]) -> (usize, usize) {
    let (mut zero_s, mut zero_l) = (0usize, 0usize);
    let (mut cur_s, mut cur_l) = (0usize, 0usize);
    for (index, word) in words.iter().enumerate() {
        if *word == 0 {
            if cur_l == 0 {
                cur_s = index;
            }
            cur_l += 1;
        } else if cur_l != 0 && zero_l < cur_l {
            zero_s = cur_s;
            zero_l = cur_l;
            cur_l = 0;
        }
    }
    if cur_l != 0 && zero_l < cur_l {
        zero_s = cur_s;
        zero_l = cur_l;
    }
    (zero_s, zero_l)
}

// ---------------------------------------------------------------------------
// macaddr / macaddr8
// ---------------------------------------------------------------------------

/// A `PostgreSQL` `macaddr`: six bytes, an EUI-48 address.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct MacAddr(pub [u8; 6]);

impl MacAddr {
    /// `macaddr_in`. Accepts `xx:xx:xx:xx:xx:xx`, `xx-xx-…`, `xxxxxx:xxxxxx`,
    /// `xxxxxx-xxxxxx`, `xxxx.xxxx.xxxx`, `xxxx-xxxx-xxxx` and bare
    /// `xxxxxxxxxxxx`, with optional surrounding whitespace.
    ///
    /// # Errors
    ///
    /// `22P02` `invalid input syntax for type macaddr`.
    pub fn parse(input: &str) -> Result<Self, TypeError> {
        parse_macaddr(input)
    }

    /// `macaddr_out`: six lowercase hex bytes separated by colons.
    #[must_use]
    pub fn to_text(&self) -> String {
        let mut out = String::with_capacity(17);
        for (index, byte) in self.0.iter().enumerate() {
            if index != 0 {
                out.push(':');
            }
            let _ = write!(out, "{byte:02x}");
        }
        out
    }

    /// `trunc(macaddr)`: keep the manufacturer prefix, zero the rest.
    #[must_use]
    pub fn trunc(&self) -> Self {
        MacAddr([self.0[0], self.0[1], self.0[2], 0, 0, 0])
    }

    /// `~macaddr`.
    #[must_use]
    pub fn not(&self) -> Self {
        MacAddr(self.0.map(|byte| !byte))
    }

    /// `macaddr & macaddr`.
    #[must_use]
    pub fn and(&self, other: &Self) -> Self {
        MacAddr(std::array::from_fn(|i| self.0[i] & other.0[i]))
    }

    /// `macaddr | macaddr`.
    #[must_use]
    pub fn or(&self, other: &Self) -> Self {
        MacAddr(std::array::from_fn(|i| self.0[i] | other.0[i]))
    }

    /// `macaddrtomacaddr8`: widen to EUI-64 by inserting `ff:fe` in the middle.
    #[must_use]
    pub fn to_macaddr8(&self) -> MacAddr8 {
        let [oui0, oui1, oui2, nic0, nic1, nic2] = self.0;
        MacAddr8([oui0, oui1, oui2, 0xFF, 0xFE, nic0, nic1, nic2])
    }
}

/// A `PostgreSQL` `macaddr8`: eight bytes, an EUI-64 address.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct MacAddr8(pub [u8; 8]);

impl MacAddr8 {
    /// `macaddr8_in`. Accepts six or eight hex byte pairs separated by one
    /// consistent `:`, `-` or `.` (or none at all), with optional surrounding
    /// whitespace. A six-byte input is widened to EUI-64 by inserting `ff:fe`.
    ///
    /// # Errors
    ///
    /// `22P02` `invalid input syntax for type macaddr8`.
    pub fn parse(input: &str) -> Result<Self, TypeError> {
        parse_macaddr8(input.as_bytes()).ok_or_else(|| TypeError::InvalidText {
            type_name: "macaddr8",
            value: input.to_string(),
        })
    }

    /// `macaddr8_out`: eight lowercase hex bytes separated by colons.
    #[must_use]
    pub fn to_text(&self) -> String {
        let mut out = String::with_capacity(23);
        for (index, byte) in self.0.iter().enumerate() {
            if index != 0 {
                out.push(':');
            }
            let _ = write!(out, "{byte:02x}");
        }
        out
    }

    /// `trunc(macaddr8)`: keep the manufacturer prefix, zero the rest.
    #[must_use]
    pub fn trunc(&self) -> Self {
        MacAddr8([self.0[0], self.0[1], self.0[2], 0, 0, 0, 0, 0])
    }

    /// `~macaddr8`.
    #[must_use]
    pub fn not(&self) -> Self {
        MacAddr8(self.0.map(|byte| !byte))
    }

    /// `macaddr8 & macaddr8`.
    #[must_use]
    pub fn and(&self, other: &Self) -> Self {
        MacAddr8(std::array::from_fn(|i| self.0[i] & other.0[i]))
    }

    /// `macaddr8 | macaddr8`.
    #[must_use]
    pub fn or(&self, other: &Self) -> Self {
        MacAddr8(std::array::from_fn(|i| self.0[i] | other.0[i]))
    }

    /// `macaddr8_set7bit`: set the modified-EUI-64 universal/local bit, which
    /// is what an IPv6 interface identifier needs.
    #[must_use]
    pub fn set7bit(&self) -> Self {
        let mut bytes = self.0;
        bytes[0] |= 0x02;
        MacAddr8(bytes)
    }

    /// `macaddr8tomacaddr`: narrow to EUI-48 by removing the middle `ff:fe`.
    ///
    /// # Errors
    ///
    /// `22003` when the 4th and 5th bytes are not `ff` and `fe`, with
    /// `PostgreSQL`'s hint naming the eligible shape.
    pub fn to_macaddr(&self) -> Result<MacAddr, TypeError> {
        let [oui0, oui1, oui2, marker0, marker1, nic0, nic1, nic2] = self.0;
        if marker0 != 0xFF || marker1 != 0xFE {
            return Err(TypeError::CodedWithHint {
                sqlstate: "22003",
                message: "macaddr8 data out of range to convert to macaddr".to_string(),
                hint: "Only addresses that have FF and FE as values in the 4th and 5th bytes \
                       from the left, for example xx:xx:xx:ff:fe:xx:xx:xx, are eligible to be \
                       converted from macaddr8 to macaddr.",
            });
        }
        Ok(MacAddr([oui0, oui1, oui2, nic0, nic1, nic2]))
    }
}

/// `macaddr_in`'s seven `sscanf` layouts, in the order `PostgreSQL` tries them.
///
/// `width` is `sscanf`'s field width — a MAXIMUM number of characters, not an
/// exact count — which is why `'223.255.255'` is the perfectly good address
/// `22:03:25:05:25:05` under the `%2x%2x.%2x%2x.%2x%2x` layout. `0` means the
/// unbounded `%x`.
const MAC_LAYOUTS: [(usize, [Option<u8>; 6]); 7] = [
    // %x:%x:%x:%x:%x:%x
    (
        0,
        [
            Some(b':'),
            Some(b':'),
            Some(b':'),
            Some(b':'),
            Some(b':'),
            None,
        ],
    ),
    // %x-%x-%x-%x-%x-%x
    (
        0,
        [
            Some(b'-'),
            Some(b'-'),
            Some(b'-'),
            Some(b'-'),
            Some(b'-'),
            None,
        ],
    ),
    // %2x%2x%2x:%2x%2x%2x
    (2, [None, None, Some(b':'), None, None, None]),
    // %2x%2x%2x-%2x%2x%2x
    (2, [None, None, Some(b'-'), None, None, None]),
    // %2x%2x.%2x%2x.%2x%2x
    (2, [None, Some(b'.'), None, Some(b'.'), None, None]),
    // %2x%2x-%2x%2x-%2x%2x
    (2, [None, Some(b'-'), None, Some(b'-'), None, None]),
    // %2x%2x%2x%2x%2x%2x
    (2, [None, None, None, None, None, None]),
];

/// `macaddr_in`: the first layout that consumes the whole string wins, and only
/// then are the six values range-checked — so a structurally valid address with
/// an octet over 255 is `22003`, not `22P02`.
fn parse_macaddr(input: &str) -> Result<MacAddr, TypeError> {
    // `sscanf` skips leading whitespace and the trailing `%1s` matches only
    // non-whitespace garbage, so whitespace around the address is accepted.
    let text = input
        .trim_matches(|c: char| c.is_ascii_whitespace())
        .as_bytes();
    let Some(octets) = MAC_LAYOUTS
        .iter()
        .find_map(|(width, separators)| match_mac_layout(text, *width, separators))
    else {
        return Err(TypeError::InvalidText {
            type_name: "macaddr",
            value: input.to_string(),
        });
    };
    let mut bytes = [0u8; 6];
    for (slot, octet) in bytes.iter_mut().zip(octets) {
        *slot = u8::try_from(octet).map_err(|_| TypeError::Coded {
            sqlstate: "22003",
            message: format!("invalid octet value in \"macaddr\" value: \"{input}\""),
        })?;
    }
    Ok(MacAddr(bytes))
}

/// Match one `macaddr_in` layout: six `%x` conversions joined by literal
/// separator characters, consuming the whole input.
fn match_mac_layout(src: &[u8], width: usize, separators: &[Option<u8>; 6]) -> Option<[i64; 6]> {
    let mut octets = [0i64; 6];
    let mut index = 0usize;
    for (slot, separator) in octets.iter_mut().zip(separators) {
        let (value, next) = scan_hex(src, index, width)?;
        *slot = value;
        index = next;
        if let Some(separator) = separator {
            if src.get(index).copied() != Some(*separator) {
                return None;
            }
            index += 1;
        }
    }
    (index == src.len()).then_some(octets)
}

/// One `%x` / `%<width>x` conversion: leading whitespace is skipped for free,
/// then at most `width` characters (unbounded when `width` is 0) of an optional
/// sign, an optional `0x` prefix and at least one hex digit.
///
/// The value is saturated rather than wrapped on overflow, because every caller
/// range-checks it against `0 ..= 255` and `PostgreSQL`'s own behaviour there is
/// its C library's undefined integer overflow.
fn scan_hex(src: &[u8], from: usize, width: usize) -> Option<(i64, usize)> {
    let mut index = from;
    while src.get(index).is_some_and(u8::is_ascii_whitespace) {
        index += 1;
    }
    let start = index;
    let negative = match src.get(index) {
        Some(b'-') if width == 0 || width > 1 => {
            index += 1;
            true
        }
        Some(b'+') if width == 0 || width > 1 => {
            index += 1;
            false
        }
        _ => false,
    };
    // `%x` accepts an optional `0x` prefix, but only when a hex digit follows
    // it inside the field width.
    if src.get(index) == Some(&b'0')
        && matches!(src.get(index + 1), Some(b'x' | b'X'))
        && src.get(index + 2).is_some_and(u8::is_ascii_hexdigit)
        && (width == 0 || index + 2 - start < width)
    {
        index += 2;
    }
    let mut value: i64 = 0;
    let mut digits = 0usize;
    while index < src.len() && (width == 0 || index - start < width) {
        let Some(hex) = hex_value(src[index]) else {
            break;
        };
        value = value
            .checked_mul(16)
            .and_then(|v| v.checked_add(i64::from(hex)))
            .unwrap_or(i64::MAX);
        digits += 1;
        index += 1;
    }
    if digits == 0 {
        return None;
    }
    Some((if negative { -value } else { value }, index))
}

/// `macaddr8_in`: hex digit pairs with one consistent separator, six or eight
/// of them, with optional surrounding whitespace.
fn parse_macaddr8(src: &[u8]) -> Option<MacAddr8> {
    let mut index = 0usize;
    while src.get(index).is_some_and(u8::is_ascii_whitespace) {
        index += 1;
    }
    let mut bytes = [0u8; 8];
    let mut count = 0usize;
    let mut spacer: Option<u8> = None;
    // Digits must always come in pairs, so a lone trailing character ends the
    // scan without consuming.
    while index + 1 < src.len() {
        if count >= 8 {
            return None;
        }
        bytes[count] = (hex_value(src[index])? << 4) | hex_value(src[index + 1])?;
        count += 1;
        index += 2;
        if matches!(src.get(index), Some(b':' | b'-' | b'.')) {
            let separator = src[index];
            match spacer {
                None => spacer = Some(separator),
                Some(seen) if seen == separator => {}
                Some(_) => return None,
            }
            index += 1;
        }
        // Trailing whitespace is allowed once a whole address has been read,
        // but nothing may follow it.
        if (count == 6 || count == 8) && src.get(index).is_some_and(u8::is_ascii_whitespace) {
            while src.get(index).is_some_and(u8::is_ascii_whitespace) {
                index += 1;
            }
            if index < src.len() {
                return None;
            }
        }
    }
    if index != src.len() {
        return None;
    }
    match count {
        // A six-byte EUI-48 widens to EUI-64 with `ff:fe` in the middle.
        6 => Some(MacAddr8([
            bytes[0], bytes[1], bytes[2], 0xFF, 0xFE, bytes[3], bytes[4], bytes[5],
        ])),
        8 => Some(MacAddr8(bytes)),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use assert2::assert;

    use super::*;

    fn inet(text: &str) -> Inet {
        Inet::parse(text, false).expect("valid inet")
    }

    fn cidr(text: &str) -> Inet {
        Inet::parse(text, true).expect("valid cidr")
    }

    #[test]
    fn inet_and_cidr_input_output_round_trip() {
        // (input, is_cidr, output, abbrev, host, text-cast)
        let cases: [(&str, bool, &str, &str, &str, &str); 14] = [
            (
                "192.168.1.226/24",
                false,
                "192.168.1.226/24",
                "192.168.1.226/24",
                "192.168.1.226",
                "192.168.1.226/24",
            ),
            (
                "192.168.1.226",
                false,
                "192.168.1.226",
                "192.168.1.226",
                "192.168.1.226",
                "192.168.1.226/32",
            ),
            (
                "192.168.1",
                true,
                "192.168.1.0/24",
                "192.168.1/24",
                "192.168.1.0",
                "192.168.1.0/24",
            ),
            ("10", true, "10.0.0.0/8", "10/8", "10.0.0.0", "10.0.0.0/8"),
            (
                "10.0.0.0",
                true,
                "10.0.0.0/32",
                "10.0.0.0/32",
                "10.0.0.0",
                "10.0.0.0/32",
            ),
            (
                "10.1.2",
                true,
                "10.1.2.0/24",
                "10.1.2/24",
                "10.1.2.0",
                "10.1.2.0/24",
            ),
            (
                "0.0.0.0/0",
                false,
                "0.0.0.0/0",
                "0.0.0.0/0",
                "0.0.0.0",
                "0.0.0.0/0",
            ),
            (
                "10:23::f1/64",
                false,
                "10:23::f1/64",
                "10:23::f1/64",
                "10:23::f1",
                "10:23::f1/64",
            ),
            (
                "10:23::f1",
                true,
                "10:23::f1/128",
                "10:23::f1/128",
                "10:23::f1",
                "10:23::f1/128",
            ),
            (
                "10:23::8000/113",
                true,
                "10:23::8000/113",
                "10:23::8000/113",
                "10:23::8000",
                "10:23::8000/113",
            ),
            (
                "::ffff:1.2.3.4",
                true,
                "::ffff:1.2.3.4/128",
                "::ffff:1.2.3.4/128",
                "::ffff:1.2.3.4",
                "::ffff:1.2.3.4/128",
            ),
            (
                "::4.3.2.1/24",
                false,
                "::4.3.2.1/24",
                "::4.3.2.1/24",
                "::4.3.2.1",
                "::4.3.2.1/24",
            ),
            ("::", false, "::", "::", "::", "::/128"),
            ("::1", false, "::1", "::1", "::1", "::1/128"),
        ];
        for (input, is_cidr, out, abbrev, host, show) in cases {
            let value = Inet::parse(input, is_cidr).expect(input);
            assert!(value.to_text() == out, "to_text of {input}");
            assert!(value.abbrev() == abbrev, "abbrev of {input}");
            assert!(value.host() == host, "host of {input}");
            assert!(value.show() == show, "show of {input}");
        }
    }

    #[test]
    fn cidr_rejects_bits_right_of_the_mask() {
        let error = Inet::parse("192.168.1.2/30", true).expect_err("host bits set");
        assert!(error.sqlstate() == "22P02");
        assert!(error.to_string() == "invalid cidr value: \"192.168.1.2/30\"");
        assert!(error.detail().as_deref() == Some("Value has bits set to right of mask."));
        // The identical text is a perfectly good `inet`.
        assert!(inet("192.168.1.2/30").to_text() == "192.168.1.2/30");
    }

    #[test]
    fn inet_requires_all_four_octets_without_a_netmask() {
        // `cidr` infers a classful netmask from a partial address; `inet` does
        // not accept one at all unless the width is written out.
        assert!(Inet::parse("192.168.1", false).is_err());
        assert!(Inet::parse("10", false).is_err());
        assert!(inet("10/8").to_text() == "10.0.0.0/8");
        assert!(cidr("192.168.1").to_text() == "192.168.1.0/24");
    }

    #[test]
    fn rejected_inet_input() {
        for input in [
            "",
            " ",
            "abc",
            "1234",
            "1.2.3.4.5",
            "1.2.3.4/33",
            "1.2.3.4/-1",
            "1.2.3.4/",
            "256.1.1.1",
            "1234::1234::1234",
            "1::2::3",
            ":::",
            "1:2:3:4:5:6:7:8:9",
            "fe80::1%eth0",
            "1.2.3.4 x",
        ] {
            assert!(Inet::parse(input, false).is_err(), "inet {input:?}");
            assert!(Inet::parse(input, true).is_err(), "cidr {input:?}");
        }
    }

    #[test]
    fn cidr_accepts_hex_and_classful_forms_inet_does_not() {
        assert!(cidr("0xc0a80101").to_text() == "192.168.1.1/32");
        assert!(cidr("0x0a").to_text() == "10.0.0.0/8");
        assert!(cidr("224.1.2.3").to_text() == "224.1.2.3/32");
        assert!(cidr("224").to_text() == "224.0.0.0/4");
        assert!(cidr("240.1").to_text() == "240.1.0.0/32");
        assert!(Inet::parse("0xc0a80101", false).is_err());
    }

    #[test]
    fn derived_addresses() {
        // (input, broadcast, network, netmask, hostmask)
        let cases: [(&str, &str, &str, &str, &str); 5] = [
            (
                "192.168.1.226/24",
                "192.168.1.255/24",
                "192.168.1.0/24",
                "255.255.255.0",
                "0.0.0.255",
            ),
            (
                "192.168.1.226",
                "192.168.1.226",
                "192.168.1.226/32",
                "255.255.255.255",
                "0.0.0.0",
            ),
            (
                "10:23::f1/64",
                "10:23::ffff:ffff:ffff:ffff/64",
                "10:23::/64",
                "ffff:ffff:ffff:ffff::",
                "::ffff:ffff:ffff:ffff",
            ),
            (
                "10:23::ffff",
                "10:23::ffff",
                "10:23::ffff/128",
                "ffff:ffff:ffff:ffff:ffff:ffff:ffff:ffff",
                "::",
            ),
            (
                "::4.3.2.1/24",
                "0:ff:ffff:ffff:ffff:ffff:ffff:ffff/24",
                "::/24",
                "ffff:ff00::",
                "0:ff:ffff:ffff:ffff:ffff:ffff:ffff",
            ),
        ];
        for (input, broadcast, network, netmask, hostmask) in cases {
            let value = inet(input);
            assert!(
                value.broadcast().to_text() == broadcast,
                "broadcast {input}"
            );
            assert!(value.network().to_text() == network, "network {input}");
            assert!(value.netmask().to_text() == netmask, "netmask {input}");
            assert!(value.hostmask().to_text() == hostmask, "hostmask {input}");
        }
    }

    #[test]
    fn set_masklen_keeps_host_bits_but_cidr_masklen_clears_them() {
        assert!(inet("10.1.2.3/8").set_masklen(24).expect("valid").to_text() == "10.1.2.3/24");
        assert!(cidr("10").set_cidr_masklen(24).expect("valid").to_text() == "10.0.0.0/24");
        assert!(
            cidr("10.1.2.3")
                .set_cidr_masklen(24)
                .expect("valid")
                .to_text()
                == "10.1.2.0/24"
        );
        // -1 means "as wide as the family allows".
        assert!(
            inet("192.168.1.226/24")
                .set_masklen(-1)
                .expect("valid")
                .to_text()
                == "192.168.1.226"
        );
        assert!(
            cidr("::ffff:1.2.3.4")
                .set_cidr_masklen(24)
                .expect("valid")
                .to_text()
                == "::/24"
        );
        for bad in [-2, 33] {
            let error = inet("10.1.2.3/8")
                .set_masklen(bad)
                .expect_err("out of range");
            assert!(error.sqlstate() == "22023");
            assert!(error.to_string() == format!("invalid mask length: {bad}"));
        }
        assert!(inet("::1").set_masklen(128).is_ok());
        assert!(inet("::1").set_masklen(129).is_err());
    }

    #[test]
    fn ordering_puts_ipv4_first_then_network_then_masklen_then_host() {
        let mut values = [
            "10:23::f1/64",
            "0.0.0.0/0",
            "192.168.1.0/25",
            "::1",
            "192.168.1.0/24",
            "0.0.0.0/32",
            "192.168.1.1/24",
        ]
        .map(inet);
        values.sort_unstable();
        let sorted: Vec<String> = values.iter().map(Inet::to_text).collect();
        assert!(
            sorted
                == vec![
                    "0.0.0.0/0",
                    "0.0.0.0",
                    "192.168.1.0/24",
                    "192.168.1.1/24",
                    "192.168.1.0/25",
                    "::1",
                    "10:23::f1/64",
                ]
        );
    }

    #[test]
    fn cidr_and_inet_spellings_of_one_address_are_one_value() {
        let as_cidr = cidr("10.0.0.0/8");
        let as_inet = inet("10.0.0.0/8");
        assert!(as_cidr == as_inet);
        assert!(as_cidr.cmp(&as_inet) == Ordering::Equal);
        // …and they still render differently.
        assert!(as_cidr.to_text() == "10.0.0.0/8");
        assert!(as_inet.to_text() == "10.0.0.0/8");
        assert!(cidr("10.1.2.3").to_text() == "10.1.2.3/32");
        assert!(inet("10.1.2.3").to_text() == "10.1.2.3");
    }

    #[test]
    fn containment_and_overlap() {
        // (left, right, <<, <<=, >>, >>=, &&)
        let cases: [(&str, &str, bool, bool, bool, bool, bool); 6] = [
            (
                "192.168.1.226/24",
                "192.168.1.0/24",
                false,
                true,
                false,
                true,
                true,
            ),
            (
                "192.168.1.0/25",
                "192.168.1.0/24",
                true,
                true,
                false,
                false,
                true,
            ),
            ("10.1.2.3/8", "10.0.0.0/32", false, false, true, true, true),
            (
                "11.1.2.3/8",
                "10.0.0.0/8",
                false,
                false,
                false,
                false,
                false,
            ),
            (
                "10:23::ffff",
                "10:23::8000/113",
                true,
                true,
                false,
                false,
                true,
            ),
            // Different families never relate.
            ("10.0.0.0/8", "::/8", false, false, false, false, false),
        ];
        for (left, right, sub, subeq, sup, supeq, overlap) in cases {
            let (a, b) = (inet(left), inet(right));
            assert!(a.is_subnet_of(&b) == sub, "{left} << {right}");
            assert!(a.is_subnet_of_or_eq(&b) == subeq, "{left} <<= {right}");
            assert!(a.is_supernet_of(&b) == sup, "{left} >> {right}");
            assert!(a.is_supernet_of_or_eq(&b) == supeq, "{left} >>= {right}");
            assert!(a.overlaps(&b) == overlap, "{left} && {right}");
        }
    }

    #[test]
    fn arithmetic_and_bitwise() {
        assert!(inet("127.0.0.1").add_offset(257).expect("fits").to_text() == "127.0.1.2");
        assert!(
            inet("127.0.0.1")
                .add_offset(257)
                .and_then(|v| v.add_offset(-257))
                .expect("fits")
                .to_text()
                == "127.0.0.1"
        );
        assert!(inet("127::1").add_offset(257).expect("fits").to_text() == "127::102");
        assert!(
            inet("127.0.0.2")
                .difference(&inet("127.0.0.2").add_offset(500).expect("fits"))
                .expect("in range")
                == -500
        );
        assert!(
            inet("127::2")
                .difference(&inet("127::2").add_offset(-500).expect("fits"))
                .expect("in range")
                == 500
        );
        // IPv4 cannot overflow int8 but can leave the address space.
        assert!(inet("127.0.0.1").add_offset(10_000_000_000).is_err());
        assert!(inet("127.0.0.1").add_offset(-10_000_000_000).is_err());
        // A wide IPv6 difference does not fit in int8 — in either sign, and
        // including the case where the low eight bytes look perfectly ordinary.
        assert!(inet("126::1").difference(&inet("127::2")).is_err());
        assert!(inet("10:23::f1").difference(&inet("::")).is_err());
        assert!(inet("::").difference(&inet("10:23::f1")).is_err());
        assert!(inet("127::1").difference(&inet("127::2")).expect("fits") == -1);
        assert!(inet("127::1").add_offset(10_000_000_000).is_ok());

        assert!(inet("192.168.1.226/24").not().to_text() == "63.87.254.29/24");
        assert!(
            inet("192.168.1.226/24")
                .and(&cidr("192.168.1"))
                .expect("same family")
                .to_text()
                == "192.168.1.0/24"
        );
        assert!(
            inet("192.168.1.226/24")
                .or(&cidr("192.168.1"))
                .expect("same family")
                .to_text()
                == "192.168.1.226/24"
        );
        let error = inet("10.0.0.0/8").and(&inet("::/8")).expect_err("mixed");
        assert!(error.sqlstate() == "22023");
        assert!(error.to_string() == "cannot AND inet values of different sizes");
    }

    #[test]
    fn merge_and_same_family() {
        assert!(
            cidr("192.168.1")
                .merge(&inet("192.168.1.226/24"))
                .expect("same family")
                .to_text()
                == "192.168.1.0/24"
        );
        assert!(
            cidr("10")
                .merge(&inet("11.1.2.3/8"))
                .expect("same family")
                .to_text()
                == "10.0.0.0/7"
        );
        assert!(cidr("10").same_family(&inet("10::/8")) == false);
        let error = cidr("10").merge(&inet("10::/8")).expect_err("mixed");
        assert!(error.sqlstate() == "22023");
        assert!(error.to_string() == "cannot merge addresses from different families");
    }

    #[test]
    fn binary_round_trip() {
        for (text, is_cidr) in [
            ("192.168.1.226/24", false),
            ("192.168.1.0/24", true),
            ("10:23::f1/64", false),
            ("::ffff:1.2.3.4/128", true),
        ] {
            let value = Inet::parse(text, is_cidr).expect(text);
            let bytes = value.to_binary();
            assert!(bytes[0] == value.family.wire_code());
            assert!(bytes[2] == u8::from(is_cidr));
            let back = Inet::from_binary(&bytes, is_cidr).expect("round trip");
            assert!(back == value);
            assert!(back.to_text() == value.to_text());
        }
        assert!(Inet::from_binary(&[9, 8, 0, 4, 1, 2, 3, 4], false).is_err());
        assert!(Inet::from_binary(&[2, 33, 0, 4, 1, 2, 3, 4], false).is_err());
        assert!(Inet::from_binary(&[2, 8, 1, 4, 10, 1, 2, 3], true).is_err());
    }

    #[test]
    fn macaddr_input_spellings() {
        let expected = MacAddr([0x08, 0x00, 0x2b, 0x01, 0x02, 0x03]);
        for input in [
            "08:00:2b:01:02:03",
            "08-00-2b-01-02-03",
            "08002b:010203",
            "08002b-010203",
            "0800.2b01.0203",
            "0800-2b01-0203",
            "08002b010203",
            "  08:00:2b:01:02:03  ",
        ] {
            assert!(MacAddr::parse(input).expect(input) == expected, "{input}");
        }
        for input in [
            "0800:2b01:0203",
            "not even close",
            "08:00:2b:01:02:ZZ",
            "08:00:2b:01:02:",
            "1.2.3",
            "12.34.56",
            "08:00:2b:01:02:03 x",
        ] {
            let error = MacAddr::parse(input).expect_err(input);
            assert!(error.sqlstate() == "22P02", "{input}");
            assert!(
                error.to_string() == format!("invalid input syntax for type macaddr: \"{input}\""),
                "{input}"
            );
        }
        // `%x` takes a variable number of digits, an optional sign and an
        // optional `0x` prefix; `%2x`'s width is a MAXIMUM, which is why a
        // dotted-decimal string is a perfectly good address.
        for (input, text) in [
            ("1:2:3:4:5:6", "01:02:03:04:05:06"),
            ("8:0:2b:1:2:3", "08:00:2b:01:02:03"),
            ("+1:2:3:4:5:6", "01:02:03:04:05:06"),
            ("0x8:0:2b:1:2:3", "08:00:2b:01:02:03"),
            ("08: 00:2b:01:02:03", "08:00:2b:01:02:03"),
            ("223.255.255", "22:03:25:05:25:05"),
        ] {
            assert!(
                MacAddr::parse(input).expect(input).to_text() == text,
                "{input}"
            );
        }
        // A structurally valid address whose octet does not fit a byte is
        // 22003, not 22P02.
        for input in ["111:2:3:4:5:6", "-1:2:3:4:5:6", "1111111111:2:3:4:5:6"] {
            let error = MacAddr::parse(input).expect_err(input);
            assert!(error.sqlstate() == "22003", "{input}");
            assert!(
                error.to_string()
                    == format!("invalid octet value in \"macaddr\" value: \"{input}\""),
                "{input}"
            );
        }
        assert!(expected.to_text() == "08:00:2b:01:02:03");
        assert!(expected.trunc().to_text() == "08:00:2b:00:00:00");
        assert!(expected.not().to_text() == "f7:ff:d4:fe:fd:fc");
        assert!(
            expected
                .and(&MacAddr::parse("00:00:00:ff:ff:ff").expect("valid"))
                .to_text()
                == "00:00:00:01:02:03"
        );
        assert!(
            expected
                .or(&MacAddr::parse("01:02:03:04:05:06").expect("valid"))
                .to_text()
                == "09:02:2b:05:07:07"
        );
        assert!(expected.to_macaddr8().to_text() == "08:00:2b:ff:fe:01:02:03");
    }

    #[test]
    fn macaddr8_input_spellings() {
        let six = MacAddr8([0x08, 0x00, 0x2b, 0xff, 0xfe, 0x01, 0x02, 0x03]);
        for input in [
            "08:00:2b:01:02:03",
            "08-00-2b-01-02-03",
            "08002b:010203",
            "08002b-010203",
            "0800.2b01.0203",
            "0800-2b01-0203",
            "08002b010203",
            "0800:2b01:0203",
            "08:00:2b:01:02:03     ",
            "    08:00:2b:01:02:03     ",
            "    08:00:2b:01:02:03",
        ] {
            assert!(MacAddr8::parse(input).expect(input) == six, "{input}");
        }
        let eight = MacAddr8([0x08, 0x00, 0x2b, 0x01, 0x02, 0x03, 0x04, 0x05]);
        for input in [
            "08:00:2b:01:02:03:04:05",
            "08-00-2b-01-02-03-04-05",
            "08002b:0102030405",
            "08002b-0102030405",
            "0800.2b01.0203.0405",
            "08002b01:02030405",
            "08002b0102030405",
            "    08:00:2b:01:02:03:04:05     ",
        ] {
            assert!(MacAddr8::parse(input).expect(input) == eight, "{input}");
        }
        for input in [
            "123    08:00:2b:01:02:03",
            "08:00:2b:01:02:03  123",
            "123    08:00:2b:01:02:03:04:05",
            "08:00:2b:01:02:03:04:05  123",
            "08:00:2b:01:02:03:04:05:06:07",
            "08-00-2b-01-02-03-04-05-06-07",
            "08002b:01020304050607",
            "08002b01020304050607",
            "0z002b0102030405",
            "08002b010203xyza",
            "08:00-2b:01:02:03:04:05",
            "08:00:2b:01.02:03:04:05",
            "not even close",
            "08:00:2b:01:02:03:04:ZZ",
            "08:00:2b:01:02:03:04:",
        ] {
            let error = MacAddr8::parse(input).expect_err(input);
            assert!(error.sqlstate() == "22P02");
            assert!(
                error.to_string() == format!("invalid input syntax for type macaddr8: \"{input}\"")
            );
        }
        assert!(six.to_text() == "08:00:2b:ff:fe:01:02:03");
        assert!(eight.trunc().to_text() == "08:00:2b:00:00:00:00:00");
        assert!(eight.not().to_text() == "f7:ff:d4:fe:fd:fc:fb:fa");
        assert!(
            MacAddr8::parse("00:08:2b:01:02:03")
                .expect("valid")
                .set7bit()
                .to_text()
                == "02:08:2b:ff:fe:01:02:03"
        );
        assert!(six.to_macaddr().expect("eligible").to_text() == "08:00:2b:01:02:03");
        let error = eight.to_macaddr().expect_err("not eligible");
        assert!(error.sqlstate() == "22003");
        assert!(error.to_string() == "macaddr8 data out of range to convert to macaddr");
        assert!(error.hint().is_some());
    }
}
