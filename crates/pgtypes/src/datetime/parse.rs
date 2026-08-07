//! `PostgreSQL`-compatible decoding of date/time literal text.
//!
//! This module mirrors the two-stage shape of `PostgreSQL`'s `datetime.c`. The
//! first stage splits text into *typed fields* (`ParseDateTime`). The second
//! stage interprets the fields against a running mask of what it has already
//! seen (`DecodeDateTime`). That structure is what makes the accepted set match.
//! Nothing decides the format of a literal up front. The format falls out of
//! the order the fields arrive in and which slots are still free.
//!
//! The decoder is deliberately type-agnostic: it yields whichever of date, time
//! and zone the text supplied, and each SQL type's input function decides what to
//! do with the parts (a `timestamp` discards the zone, a `time` discards the
//! date, a `date` keeps only the date).

use jiff::{
    Span,
    civil::Date,
    tz::{Offset, TimeZone},
};

use super::tzdb::zone_by_name;

/// The field-order component of `DateStyle`, which decides how an all-numeric
/// date with no unambiguous field is read (`01/02/03`).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum DateOrder {
    /// `month/day/year`, `PostgreSQL`'s default.
    #[default]
    Mdy,
    /// `day/month/year`.
    Dmy,
    /// `year/month/day`.
    Ymd,
}

impl DateOrder {
    /// Read the ordering out of a `DateStyle` setting (`ISO, DMY`) and ignore
    /// the output-format component. Text that names no ordering keeps the
    /// default.
    #[must_use]
    pub fn from_datestyle(style: &str) -> Self {
        for part in style.split(',') {
            let part = part.trim();
            if part.eq_ignore_ascii_case("dmy")
                || part.eq_ignore_ascii_case("euro")
                || part.eq_ignore_ascii_case("european")
                || part.eq_ignore_ascii_case("german")
            {
                return DateOrder::Dmy;
            }
            if part.eq_ignore_ascii_case("ymd") {
                return DateOrder::Ymd;
            }
            if part.eq_ignore_ascii_case("mdy")
                || part.eq_ignore_ascii_case("us")
                || part.eq_ignore_ascii_case("noneuro")
                || part.eq_ignore_ascii_case("noneuropean")
            {
                return DateOrder::Mdy;
            }
        }
        DateOrder::Mdy
    }
}

/// A reserved spelling that stands for something other than a fixed instant.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Special {
    /// `infinity` / `+infinity`.
    Infinity,
    /// `-infinity`.
    NegInfinity,
    /// `now`: the current transaction timestamp.
    Now,
    /// `today`: midnight of the current date.
    Today,
    /// `tomorrow`: midnight of the following date.
    Tomorrow,
    /// `yesterday`: midnight of the preceding date.
    Yesterday,
    /// `epoch`: 1970-01-01 00:00:00 UTC.
    Epoch,
}

/// The zone a literal named, if it named one.
#[derive(Debug, Clone)]
pub enum Zone {
    /// A fixed UTC offset: `Z`, `-08`, `+05:30`, or a `POSIX` `GMT+8` spec.
    Offset(Offset),
    /// A zone-database name or dynamic abbreviation, whose offset depends on the
    /// instant it is applied to.
    Named(TimeZone),
}

impl Zone {
    /// The jiff zone this stands for.
    #[must_use]
    pub fn into_time_zone(self) -> TimeZone {
        match self {
            Zone::Offset(offset) => TimeZone::fixed(offset),
            Zone::Named(tz) => tz,
        }
    }
}

/// Everything a literal supplied, before any per-type interpretation.
#[derive(Debug, Clone)]
pub struct Parts {
    /// The calendar date, when the literal carried one.
    pub date: Option<Date>,
    /// Microseconds since midnight, `0..=86_400_000_000`. The top value is
    /// `PostgreSQL`'s legal `24:00:00`.
    pub micros_of_day: i64,
    /// Whether a time-of-day was given at all (a bare date decodes as midnight).
    pub has_time: bool,
    /// The zone the literal named.
    pub zone: Option<Zone>,
}

/// The outcome of decoding a date/time literal.
#[derive(Debug, Clone)]
pub enum Decoded {
    /// A reserved spelling the caller must resolve against the clock.
    Special(Special),
    /// A concrete date and/or time.
    Parts(Parts),
}

/// Why a literal could not be decoded. Each variant maps to the SQLSTATE
/// `PostgreSQL` raises for that class of failure.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum DecodeError {
    /// The text is not a well-formed literal (22007).
    Syntax,
    /// A field is outside its range, or the value leaves the type's range
    /// (22008).
    FieldOverflow,
    /// A UTC offset outside ±15:59:59 (22009).
    TzDisplacement,
    /// A zone name the zone database does not know (22023).
    UnknownZone(String),
}

/// The largest UTC-offset hour `PostgreSQL` accepts in a literal.
const MAX_TZDISP_HOUR: i32 = 15;

/// Microseconds in one day; also the encoding of the legal `24:00:00`.
const MICROS_PER_DAY: i64 = 86_400_000_000;

// ---------------------------------------------------------------------------
// Stage 1: field splitting (PostgreSQL's ParseDateTime)
// ---------------------------------------------------------------------------

/// One lexical field of a date/time literal.
#[derive(Debug, Clone, PartialEq, Eq)]
enum Field {
    /// A run of digits, optionally carrying one decimal point (`1997`, `173201`,
    /// `040506.789`).
    Number(String),
    /// Digits and/or an embedded text month joined by `-`, `/` or `.`; also the
    /// shape a zone name with embedded punctuation arrives in.
    Date(String),
    /// A clock reading, `HH:MM[:SS[.fff]]`.
    Time(String),
    /// A lowercased run of letters.
    Word(String),
    /// A signed numeric UTC offset (`-0800`, `+05:30`).
    Tz(String),
    /// A signed word (`-infinity`), kept with its sign.
    SignedWord(String),
}

/// Split a literal into typed fields.
fn split_fields(text: &str) -> Result<Vec<Field>, DecodeError> {
    let chars: Vec<char> = text.chars().collect();
    let mut out = Vec::new();
    let mut i = 0;
    while i < chars.len() {
        let c = chars[i];
        if c.is_whitespace() || c == ',' {
            i += 1;
            continue;
        }
        if c.is_ascii_digit() {
            out.push(split_numeric_field(&chars, &mut i));
        } else if c.is_ascii_alphabetic() {
            out.push(split_alpha_field(&chars, &mut i));
        } else if c == '+' || c == '-' {
            out.push(split_signed_field(&chars, &mut i)?);
        } else {
            // Any other punctuation separates fields and is dropped, so
            // `19971)24` is a year and a month rather than a lexical error.
            i += 1;
        }
    }
    if out.is_empty() {
        return Err(DecodeError::Syntax);
    }
    Ok(out)
}

/// Split a digit-led field: a clock reading, a punctuated date, or a number.
fn split_numeric_field(chars: &[char], i: &mut usize) -> Field {
    let start = *i;
    while *i < chars.len() && chars[*i].is_ascii_digit() {
        *i += 1;
    }
    if chars.get(*i) == Some(&':') {
        while *i < chars.len()
            && (chars[*i].is_ascii_digit() || chars[*i] == ':' || chars[*i] == '.')
        {
            *i += 1;
        }
        return Field::Time(chars[start..*i].iter().collect());
    }
    // A date field, but only when the delimiters match: `2001-09-22T18:19:20`
    // must stop at the `T`, and `1997.041` stays a number so it can be read as a
    // year with a day-of-year.
    if let Some(&delim) = chars.get(*i)
        && matches!(delim, '-' | '/' | '.')
    {
        match chars.get(*i + 1) {
            Some(c) if c.is_ascii_digit() => {
                let mut j = *i + 1;
                while j < chars.len() && chars[j].is_ascii_digit() {
                    j += 1;
                }
                if chars.get(j) == Some(&delim) {
                    j += 1;
                    while j < chars.len() && (chars[j].is_ascii_digit() || chars[j] == delim) {
                        j += 1;
                    }
                    *i = j;
                    return Field::Date(chars[start..*i].iter().collect());
                }
                *i = j;
                let text: String = chars[start..*i].iter().collect();
                return if delim == '.' {
                    Field::Number(text)
                } else {
                    Field::Date(text)
                };
            }
            Some(c) if c.is_ascii_alphabetic() => {
                while *i < chars.len()
                    && (chars[*i].is_ascii_alphanumeric() || matches!(chars[*i], '-' | '/' | '.'))
                {
                    *i += 1;
                }
                return Field::Date(chars[start..*i].iter().collect::<String>().to_lowercase());
            }
            _ => {}
        }
    }
    Field::Number(chars[start..*i].iter().collect())
}

/// Split a letter-led field: a keyword, or a punctuated date / zone name.
fn split_alpha_field(chars: &[char], i: &mut usize) -> Field {
    let start = *i;
    while *i < chars.len() && chars[*i].is_ascii_alphabetic() {
        *i += 1;
    }
    let word: String = chars[start..*i].iter().collect::<String>().to_lowercase();
    // `-`, `/` and `.` always continue the field (`Feb-10-1997`,
    // `America/New_York`). A `+` or a digit continues it only when the letters so
    // far are not a keyword of their own, so `2001-09-22T18:19:20` keeps its `T`
    // separator while `EST5EDT` and `GMT+8` stay whole.
    let continues = match chars.get(*i) {
        Some('-' | '/' | '.') => true,
        Some('+') => !is_non_zone_keyword(&word),
        Some(c) if c.is_ascii_digit() => !is_non_zone_keyword(&word),
        _ => false,
    };
    if continues {
        while *i < chars.len()
            && (chars[*i].is_ascii_alphanumeric()
                || matches!(chars[*i], '+' | '-' | '/' | '_' | '.' | ':'))
        {
            *i += 1;
        }
        return Field::Date(chars[start..*i].iter().collect::<String>().to_lowercase());
    }
    Field::Word(word)
}

/// Split a `+`/`-`-led field: a numeric zone offset, or a signed keyword.
fn split_signed_field(chars: &[char], i: &mut usize) -> Result<Field, DecodeError> {
    let sign = chars[*i];
    *i += 1;
    while *i < chars.len() && chars[*i].is_whitespace() {
        *i += 1;
    }
    let start = *i;
    match chars.get(start) {
        Some(c) if c.is_ascii_digit() => {
            while *i < chars.len()
                && (chars[*i].is_ascii_digit() || matches!(chars[*i], ':' | '.' | '-'))
            {
                *i += 1;
            }
            let mut text = String::new();
            text.push(sign);
            text.extend(chars[start..*i].iter());
            Ok(Field::Tz(text))
        }
        Some(c) if c.is_ascii_alphabetic() => {
            while *i < chars.len() && chars[*i].is_ascii_alphabetic() {
                *i += 1;
            }
            let mut text = String::new();
            text.push(sign);
            text.extend(chars[start..*i].iter().map(char::to_ascii_lowercase));
            Ok(Field::SignedWord(text))
        }
        _ => Err(DecodeError::Syntax),
    }
}

/// Whether a letter run is a keyword in its own right, so a digit right after it
/// starts a new field and does not continue a zone name.
fn is_non_zone_keyword(word: &str) -> bool {
    matches!(
        word,
        "t" | "j" | "jd" | "on" | "at" | "of" | "am" | "pm" | "bc" | "ad"
    ) || month_number(word).is_some()
        || is_weekday(word)
        || special_word(word).is_some()
}

// ---------------------------------------------------------------------------
// Stage 2: field interpretation (PostgreSQL's DecodeDateTime)
// ---------------------------------------------------------------------------

/// The date/time slots filled in so far, in `PostgreSQL`'s `struct pg_tm` shape.
#[derive(Debug, Clone, Copy, Default)]
struct Tm {
    year: Option<i32>,
    month: Option<i32>,
    day: Option<i32>,
    yday: Option<i32>,
    hour: Option<i32>,
    minute: Option<i32>,
    second: Option<i32>,
    micro: i64,
    /// The year field came from one or two digits, so the decoder must apply
    /// the 1900/2000 window unless an era overrides it.
    two_digit_year: bool,
}

impl Tm {
    fn has_full_date(&self) -> bool {
        self.year.is_some() && self.month.is_some() && self.day.is_some()
    }

    fn has_full_time(&self) -> bool {
        self.hour.is_some() && self.minute.is_some() && self.second.is_some()
    }
}

/// Decode a date/time literal into whichever parts it supplied.
///
/// `order` decides only how an otherwise-ambiguous all-numeric date is read.
pub fn decode(text: &str, order: DateOrder) -> Result<Decoded, DecodeError> {
    let fields = split_fields(text)?;
    let mut tm = Tm::default();
    let mut zone: Option<Zone> = None;
    let mut special: Option<Special> = None;
    let mut is_bc = false;
    let mut meridiem: Option<bool> = None;
    let mut want_julian = false;
    let mut is_julian = false;
    let mut have_text_month = false;
    // Whether a field outside the date itself has already been read. A
    // punctuated date field insists nothing but a day-of-year or a zone has been
    // seen yet, so a leading weekday makes `Fri 1-January-1999` malformed while
    // the same literal with the weekday at the end is fine.
    let mut have_non_date = false;

    for field in &fields {
        match field {
            Field::Time(text) => {
                decode_time(text, &mut tm)?;
                have_non_date = true;
            }
            Field::Date(text) => {
                // A Julian day with its zone run together — `J2452271-08` — is
                // one punctuated field, so the day number and the offset have to
                // be split apart here.
                if std::mem::take(&mut want_julian) {
                    let digits = text
                        .find(|c: char| !c.is_ascii_digit())
                        .unwrap_or(text.len());
                    let (jd, offset) = text.split_at(digits);
                    set_julian_date(jd, &mut tm)?;
                    is_julian = true;
                    zone = Some(Zone::Offset(decode_tz_offset(offset)?));
                }
                // Once month and day are known a punctuated field can only be a
                // zone name — this is what makes `America/New_York` legal after a
                // date and a syntax error without one.
                else if tm.month.is_some() && tm.day.is_some() {
                    zone = Some(lookup_zone_spec(text)?);
                } else {
                    if have_non_date {
                        return Err(DecodeError::Syntax);
                    }
                    decode_date(text, order, &mut tm, &mut have_text_month)?;
                }
            }
            Field::Number(text) => {
                if std::mem::take(&mut want_julian) {
                    // A fractional Julian day carries the time of day with it.
                    let (jd, fraction) = text.split_once('.').unwrap_or((text, ""));
                    set_julian_date(jd, &mut tm)?;
                    is_julian = true;
                    if !fraction.is_empty() {
                        let micros = day_fraction_to_micros(fraction)?;
                        tm.hour = Some((micros / 3_600_000_000) as i32);
                        tm.minute = Some((micros / 60_000_000 % 60) as i32);
                        tm.second = Some((micros / 1_000_000 % 60) as i32);
                        tm.micro = micros % 1_000_000;
                    }
                } else {
                    decode_number_token(text, order, &mut tm, have_text_month)?;
                }
            }
            Field::Tz(text) => zone = Some(Zone::Offset(decode_tz_offset(text)?)),
            Field::SignedWord(text) => match text.as_str() {
                "-infinity" => special = Some(Special::NegInfinity),
                "+infinity" => special = Some(Special::Infinity),
                _ => return Err(DecodeError::Syntax),
            },
            Field::Word(word) => match word.as_str() {
                // The ISO `T` separator and the noise words PostgreSQL drops.
                "t" | "on" | "at" | "of" => {}
                "j" | "jd" | "julian" => want_julian = true,
                "am" => {
                    meridiem = Some(false);
                    have_non_date = true;
                }
                "pm" => {
                    meridiem = Some(true);
                    have_non_date = true;
                }
                "bc" => {
                    is_bc = true;
                    have_non_date = true;
                }
                "ad" => {
                    is_bc = false;
                    have_non_date = true;
                }
                // `DST` as a word of its own moves the zone already named an
                // hour east: `MET DST` is +02. It modifies a zone rather than
                // being one, so it needs a fixed offset to modify.
                "dst" => {
                    let Some(Zone::Offset(offset)) = zone else {
                        return Err(DecodeError::Syntax);
                    };
                    zone = Some(Zone::Offset(
                        Offset::from_seconds(offset.seconds() + 3_600)
                            .map_err(|_| DecodeError::TzDisplacement)?,
                    ));
                }
                "allballs" => {
                    tm.hour = Some(0);
                    tm.minute = Some(0);
                    tm.second = Some(0);
                    tm.micro = 0;
                    zone = Some(Zone::Offset(Offset::UTC));
                }
                _ => {
                    if let Some(found) = special_word(word) {
                        special = Some(found);
                    } else if let Some(month) = month_number(word) {
                        // A number already read into the month slot was really
                        // the day: `08 Jan 1999`. PostgreSQL demotes it as soon
                        // as an unambiguous text month arrives.
                        if let Some(numeric) = tm.month {
                            if have_text_month || tm.day.is_some() || !(1..=31).contains(&numeric) {
                                return Err(DecodeError::Syntax);
                            }
                            tm.day = Some(numeric);
                        }
                        tm.month = Some(month);
                        have_text_month = true;
                    } else if is_weekday(word) {
                        // Day-of-week names are decoration; PostgreSQL does not
                        // cross-check them against the date.
                        have_non_date = true;
                    } else if let Some(found) =
                        lookup_abbrev(word).or_else(|| lookup_zone_name(word))
                    {
                        // An abbreviation wins over a same-spelled database
                        // name, as in PostgreSQL: `EST` is the fixed -05 of the
                        // default abbreviation set, not the `EST` zone. Whole
                        // single-word zone names (`Japan`, `Navajo`, `Turkey`)
                        // fall through to the database.
                        zone = Some(found);
                    } else {
                        return Err(DecodeError::Syntax);
                    }
                }
            },
        }
    }
    if want_julian {
        return Err(DecodeError::Syntax);
    }
    if let Some(special) = special {
        return Ok(Decoded::Special(special));
    }

    if let Some(pm) = meridiem {
        let hour = tm.hour.ok_or(DecodeError::Syntax)?;
        if !(1..=12).contains(&hour) {
            return Err(DecodeError::FieldOverflow);
        }
        tm.hour = Some(match (pm, hour) {
            (false, 12) => 0,
            (true, 12) => 12,
            (true, h) => h + 12,
            (false, h) => h,
        });
    }

    let date = finish_date(&tm, is_bc, is_julian)?;
    let micros_of_day = finish_time(&tm)?;
    Ok(Decoded::Parts(Parts {
        date,
        micros_of_day,
        has_time: tm.hour.is_some(),
        zone,
    }))
}

/// Fill the calendar fields from a Julian day number.
fn set_julian_date(digits: &str, tm: &mut Tm) -> Result<(), DecodeError> {
    let jd: i32 = digits.parse().map_err(|_| DecodeError::FieldOverflow)?;
    let date = julian_to_date(jd)?;
    tm.year = Some(i32::from(date.year()));
    tm.month = Some(i32::from(date.month()));
    tm.day = Some(i32::from(date.day()));
    tm.two_digit_year = false;
    Ok(())
}

/// Turn the fractional part of a Julian day into microseconds since midnight.
fn day_fraction_to_micros(digits: &str) -> Result<i64, DecodeError> {
    if digits.is_empty() || !digits.bytes().all(|b| b.is_ascii_digit()) {
        return Err(DecodeError::Syntax);
    }
    let fraction: f64 = format!("0.{digits}")
        .parse()
        .map_err(|_| DecodeError::Syntax)?;
    Ok((fraction * MICROS_PER_DAY as f64).round() as i64)
}

/// Apply the era and two-digit-year windows and build the calendar date.
///
/// `PostgreSQL`'s `ValidateDate` range-checks each field that arrived BEFORE
/// anything insists the date is complete, and that order is observable:
/// `19971)24` is a year and a month with no day, and it reports the month being
/// out of range rather than the missing day.
fn finish_date(tm: &Tm, is_bc: bool, is_julian: bool) -> Result<Option<Date>, DecodeError> {
    // PostgreSQL stores BC years astronomically: 1 BC is year 0, so `n BC` is
    // `-(n - 1)`. jiff numbers years the same way, so no further shift is needed.
    let year = match tm.year {
        Some(year) if is_julian => Some(year),
        Some(year) if is_bc => {
            if year <= 0 {
                return Err(DecodeError::FieldOverflow);
            }
            Some(-(year - 1))
        }
        Some(year) if tm.two_digit_year && year < 100 => {
            Some(if year < 70 { year + 2000 } else { year + 1900 })
        }
        Some(year) => {
            // Year zero has no spelling outside the BC era.
            if year <= 0 {
                return Err(DecodeError::FieldOverflow);
            }
            Some(year)
        }
        None => None,
    };
    if let Some(month) = tm.month
        && !(1..=12).contains(&month)
    {
        return Err(DecodeError::FieldOverflow);
    }
    if let Some(day) = tm.day
        && !(1..=31).contains(&day)
    {
        return Err(DecodeError::FieldOverflow);
    }
    let Some(year) = year else {
        if tm.month.is_some() || tm.day.is_some() || tm.yday.is_some() {
            return Err(DecodeError::Syntax);
        }
        return Ok(None);
    };
    let year = i16::try_from(year).map_err(|_| DecodeError::FieldOverflow)?;

    if let Some(yday) = tm.yday {
        let jan1 = Date::new(year, 1, 1).map_err(|_| DecodeError::FieldOverflow)?;
        return jan1
            .checked_add(
                Span::new()
                    .try_days(yday - 1)
                    .map_err(|_| DecodeError::FieldOverflow)?,
            )
            .map(Some)
            .map_err(|_| DecodeError::FieldOverflow);
    }

    let month = tm.month.ok_or(DecodeError::Syntax)?;
    let day = tm.day.ok_or(DecodeError::Syntax)?;
    let month = i8::try_from(month).map_err(|_| DecodeError::FieldOverflow)?;
    let day = i8::try_from(day).map_err(|_| DecodeError::FieldOverflow)?;
    Date::new(year, month, day)
        .map(Some)
        .map_err(|_| DecodeError::FieldOverflow)
}

/// Range-check the clock fields and fold them into microseconds since midnight.
fn finish_time(tm: &Tm) -> Result<i64, DecodeError> {
    let hour = i64::from(tm.hour.unwrap_or(0));
    let minute = i64::from(tm.minute.unwrap_or(0));
    let second = i64::from(tm.second.unwrap_or(0));
    if !(0..=24).contains(&hour) || !(0..60).contains(&minute) || !(0..=60).contains(&second) {
        return Err(DecodeError::FieldOverflow);
    }
    // `24:00:00` is the one legal reading at the top of the day; anything past it
    // is out of range. A `:60` seconds spelling rounds up to the next minute, but
    // only when it carries no fraction of its own.
    if (hour == 24 || second == 60) && (tm.micro > 0 || (hour == 24 && (minute > 0 || second > 0)))
    {
        return Err(DecodeError::FieldOverflow);
    }
    let micros = hour * 3_600_000_000 + minute * 60_000_000 + second * 1_000_000 + tm.micro;
    if micros > MICROS_PER_DAY {
        return Err(DecodeError::FieldOverflow);
    }
    Ok(micros)
}

/// Decode a `HH:MM[:SS[.fff]]` clock reading.
fn decode_time(text: &str, tm: &mut Tm) -> Result<(), DecodeError> {
    let mut parts = text.split(':');
    let hour = parts.next().ok_or(DecodeError::Syntax)?;
    let minute = parts.next().ok_or(DecodeError::Syntax)?;
    let seconds = parts.next();
    if parts.next().is_some() {
        return Err(DecodeError::Syntax);
    }
    // A fraction on the second of two fields means the reading was minutes and
    // seconds all along, so the fields shift down an hour: PostgreSQL reads
    // `12:34.5` as `00:12:34.5`.
    if let Some((whole, frac)) = minute.split_once('.') {
        if seconds.is_some() {
            return Err(DecodeError::Syntax);
        }
        tm.second = Some(parse_int(whole)?);
        tm.minute = Some(parse_int(hour)?);
        tm.hour = Some(0);
        tm.micro = parse_fraction(frac)?;
        return Ok(());
    }
    tm.hour = Some(parse_int(hour)?);
    tm.minute = Some(parse_int(minute)?);
    match seconds {
        None => {
            tm.second = Some(0);
            tm.micro = 0;
        }
        Some(sec) => match sec.split_once('.') {
            Some((whole, frac)) => {
                tm.second = Some(parse_int(whole)?);
                tm.micro = parse_fraction(frac)?;
            }
            None => {
                tm.second = Some(parse_int(sec)?);
                tm.micro = 0;
            }
        },
    }
    Ok(())
}

/// Decode a punctuated date field. This function takes any text month first, so
/// the numeric fields that remain are unambiguous.
fn decode_date(
    text: &str,
    order: DateOrder,
    tm: &mut Tm,
    have_text_month: &mut bool,
) -> Result<(), DecodeError> {
    // `DecodeDate` reads a group, then skips separators hunting the next one;
    // running out of text there is an error. So a trailing separator is
    // malformed — which is the whole reason `1999-08-Jan` is rejected, since the
    // field splitter hands the date half over as `1999-08-`.
    if text.ends_with(|c: char| !c.is_ascii_alphanumeric()) {
        return Err(DecodeError::Syntax);
    }
    let raw: Vec<&str> = text
        .split(|c: char| !c.is_ascii_alphanumeric())
        .filter(|s| !s.is_empty())
        .collect();
    if raw.is_empty() {
        return Err(DecodeError::Syntax);
    }
    let mut numeric: Vec<&str> = Vec::with_capacity(raw.len());
    for part in raw {
        if part.starts_with(|c: char| c.is_ascii_alphabetic()) {
            let month = month_number(part).ok_or(DecodeError::Syntax)?;
            if tm.month.is_some() {
                return Err(DecodeError::Syntax);
            }
            tm.month = Some(month);
            *have_text_month = true;
        } else {
            numeric.push(part);
        }
    }
    for part in numeric {
        decode_number(part, order, tm, *have_text_month)?;
    }
    // A punctuated date field has to have carried a WHOLE date. PostgreSQL
    // checks this at the end of `DecodeDate`, ahead of any range check, which is
    // why `040506.07` — a year and a month and nothing else — is a syntax error
    // rather than a complaint about the year.
    if tm.year.is_none() || (tm.yday.is_none() && (tm.month.is_none() || tm.day.is_none())) {
        return Err(DecodeError::Syntax);
    }
    Ok(())
}

/// Decode a standalone numeric field. This function chooses between the
/// run-together forms (`19970210`, `173201`) and a single date or time
/// component.
fn decode_number_token(
    text: &str,
    order: DateOrder,
    tm: &mut Tm,
    have_text_month: bool,
) -> Result<(), DecodeError> {
    match text.find('.') {
        // `1997.041` — a year and a day-of-year, so read the whole field as a date.
        Some(_) if !tm.has_full_date() => {
            let mut text_month = have_text_month;
            decode_date(text, order, tm, &mut text_month)
        }
        // `040506.789` — a run-together time with fractional seconds.
        Some(dot) if dot > 2 => decode_number_field(text, tm),
        // A run-together `YYYYMMDD` or `HHMMSS` needs SIX digits. Five is a
        // year, which is what leaves `19971)24` a month-out-of-range complaint
        // rather than an unreadable field.
        _ if text.len() >= 6 && !(tm.has_full_date() && tm.has_full_time()) => {
            decode_number_field(text, tm)
        }
        _ => decode_number(text, order, tm, have_text_month),
    }
}

/// Assign one numeric field to the next free date slot, as `PostgreSQL`'s
/// `DecodeNumber` does.
fn decode_number(
    text: &str,
    order: DateOrder,
    tm: &mut Tm,
    have_text_month: bool,
) -> Result<(), DecodeError> {
    let (digits, fraction) = match text.split_once('.') {
        Some((whole, frac)) => (whole, Some(frac)),
        None => (text, None),
    };
    let len = digits.len();
    let value = parse_int(digits)?;
    if let Some(frac) = fraction {
        // More than two digits before the point is a run-together date or time.
        if len > 2 {
            return decode_number_field(text, tm);
        }
        tm.micro = parse_fraction(frac)?;
        tm.second = Some(value);
        return Ok(());
    }

    // A three-digit field with only the year known is a day of the year.
    if len == 3
        && tm.year.is_some()
        && tm.month.is_none()
        && tm.day.is_none()
        && (1..=366).contains(&value)
    {
        tm.yday = Some(value);
        return Ok(());
    }

    let mut assigned_year = false;
    match (tm.year.is_some(), tm.month.is_some(), tm.day.is_some()) {
        (false, false, false) => {
            if len >= 3 || order == DateOrder::Ymd {
                tm.year = Some(value);
                assigned_year = true;
            } else if order == DateOrder::Dmy {
                tm.day = Some(value);
            } else {
                tm.month = Some(value);
            }
        }
        (true, false, false) => tm.month = Some(value),
        (false, true, false) => {
            if have_text_month && (len >= 3 || order == DateOrder::Ymd) {
                tm.year = Some(value);
                assigned_year = true;
            } else {
                tm.day = Some(value);
            }
        }
        (true, true, false) => {
            if have_text_month && len >= 3 && tm.two_digit_year {
                // `08-Jan-1999` read in YMD order: the first number was taken as
                // the year, but a later long field means it was the day.
                tm.day = tm.year;
                tm.year = Some(value);
                tm.two_digit_year = false;
            } else {
                tm.day = Some(value);
            }
        }
        (false, false, true) => tm.month = Some(value),
        (false, true, true) => {
            tm.year = Some(value);
            assigned_year = true;
        }
        (true, false, true) => return Err(DecodeError::Syntax),
        (true, true, true) => return decode_number_field(text, tm),
    }
    if assigned_year {
        tm.two_digit_year = len <= 2;
    }
    Ok(())
}

/// Decode a run-together field: `YYYYMMDD`/`YYMMDD` when no date is known yet,
/// otherwise `HHMMSS`/`HHMM`.
fn decode_number_field(text: &str, tm: &mut Tm) -> Result<(), DecodeError> {
    let (digits, fraction) = match text.split_once('.') {
        Some((whole, frac)) => (whole, Some(frac)),
        None => (text, None),
    };
    let len = digits.len();
    if !digits.is_empty() && digits.bytes().all(|b| b.is_ascii_digit()) {
        if fraction.is_none() && !tm.has_full_date() && len >= 6 {
            let (year, rest) = digits.split_at(len - 4);
            let (month, day) = rest.split_at(2);
            tm.year = Some(parse_int(year)?);
            tm.month = Some(parse_int(month)?);
            tm.day = Some(parse_int(day)?);
            tm.two_digit_year = year.len() == 2;
            return Ok(());
        }
        if !tm.has_full_time() && (len == 6 || len == 4) {
            tm.hour = Some(parse_int(&digits[..2])?);
            tm.minute = Some(parse_int(&digits[2..4])?);
            tm.second = Some(if len == 6 {
                parse_int(&digits[4..])?
            } else {
                0
            });
            tm.micro = match fraction {
                Some(frac) => parse_fraction(frac)?,
                None => 0,
            };
            return Ok(());
        }
    }
    Err(DecodeError::Syntax)
}

/// Parse a non-negative decimal field.
fn parse_int(text: &str) -> Result<i32, DecodeError> {
    if text.is_empty() || !text.bytes().all(|b| b.is_ascii_digit()) {
        return Err(DecodeError::Syntax);
    }
    text.parse().map_err(|_| DecodeError::FieldOverflow)
}

/// Round a fractional-seconds digit string to microseconds. A value of
/// `1_000_000` is a carry of one whole second the caller folds in.
fn parse_fraction(digits: &str) -> Result<i64, DecodeError> {
    super::round_fraction_to_micros(digits)
        .map(i64::from)
        .ok_or(DecodeError::Syntax)
}

// ---------------------------------------------------------------------------
// Zones
// ---------------------------------------------------------------------------

/// Parse a signed numeric UTC offset (`-08`, `-0800`, `+05:30:15`) with the ISO
/// sign, where a leading `-` is *behind* UTC.
///
/// This is `PostgreSQL`'s `DecodeTimezone`, the grammar it applies to the zone
/// of a literal and to `make_timestamptz`'s zone argument. It is deliberately
/// **not** the grammar behind a zone *specification* — `AT TIME ZONE '-08:00'`
/// and `SET TimeZone = '-08:00'` both count the offset west of Greenwich
/// instead. See [`posix_zone`] and [`resolve_guc_time_zone`].
///
/// # Errors
///
/// [`DecodeError::Syntax`] when the text is not of this shape at all, and
/// [`DecodeError::TzDisplacement`] when it is but names an offset beyond
/// ±15:59:59.
pub fn decode_numeric_time_zone(text: &str) -> Result<Offset, DecodeError> {
    decode_tz_offset(text)
}

/// Read the leading run of digits as an `i32` with the tail left over, C
/// `strtoint` over one zone-offset field. No digits at all consumes nothing, so
/// the caller's delimiter test is what rejects the text.
fn take_offset_int(text: &str) -> Result<(i32, &str), DecodeError> {
    let digits = text.strip_prefix(['+', '-']).unwrap_or(text);
    let width = digits.bytes().take_while(u8::is_ascii_digit).count();
    if width == 0 {
        return Ok((0, text));
    }
    let end = text.len() - digits.len() + width;
    let value = text[..end]
        .parse()
        .map_err(|_| DecodeError::TzDisplacement)?;
    Ok((value, &text[end..]))
}

/// Parse a signed numeric UTC offset (`-08`, `-0800`, `+05:30:15`).
///
/// The shape of this grammar is what makes a *negative year* report a zone
/// problem. `ParseDateTime` hands any `-` followed by digits to this function as
/// a zone field, so `-44-02-01` arrives here as one offset: the hours read as
/// 44, and the range check fires before the trailing `-02-01` is ever noticed as
/// leftover text. `time zone displacement out of range` for what looks like a BC
/// date is that ordering showing through, not a special case.
fn decode_tz_offset(text: &str) -> Result<Offset, DecodeError> {
    let negative = match text.as_bytes().first() {
        Some(b'+') => false,
        Some(b'-') => true,
        _ => return Err(DecodeError::Syntax),
    };
    let (mut hour, rest) = take_offset_int(&text[1..])?;
    let mut minute = 0;
    let mut second = 0;
    let rest = if let Some(after_colon) = rest.strip_prefix(':') {
        let (parsed, rest) = take_offset_int(after_colon)?;
        minute = parsed;
        match rest.strip_prefix(':') {
            Some(after_colon) => {
                let (parsed, rest) = take_offset_int(after_colon)?;
                second = parsed;
                rest
            }
            None => rest,
        }
    } else if rest.is_empty() && text.len() > 3 {
        // Run together, so the last two digits are minutes: `-0800`. There is no
        // run-together `hhmmss` — `-080000` reads as 8000 hours and is rejected.
        minute = hour % 100;
        hour /= 100;
        rest
    } else {
        rest
    };
    if !(0..=MAX_TZDISP_HOUR).contains(&hour)
        || !(0..60).contains(&minute)
        || !(0..60).contains(&second)
    {
        return Err(DecodeError::TzDisplacement);
    }
    // Trailing text is only a syntax error once the fields themselves are in
    // range, which is the ordering that decides the message.
    if !rest.is_empty() {
        return Err(DecodeError::Syntax);
    }
    let seconds = hour * 3600 + minute * 60 + second;
    Offset::from_seconds(if negative { -seconds } else { seconds })
        .map_err(|_| DecodeError::TzDisplacement)
}

/// Resolve a punctuated zone specification: a zone-database name, or a `POSIX`
/// `TZ` specification whose sign counts *west* of UTC and so inverts.
fn lookup_zone_spec(text: &str) -> Result<Zone, DecodeError> {
    lookup_zone_name(text)
        .or_else(|| posix_zone(text))
        .ok_or_else(|| DecodeError::UnknownZone(text.to_string()))
}

/// The transition rule `PostgreSQL` substitutes when a `POSIX` specification
/// names a daylight abbreviation but gives no rule — tzcode's `TZDEFRULESTRING`,
/// the United States rules in force since 2007. It is what makes `FOO8BAR` a
/// zone that springs forward on the second Sunday in March.
const POSIX_DEFAULT_RULE: &str = ",M3.2.0,M11.1.0";

/// The largest hour `PostgreSQL`'s `POSIX` offset grammar accepts.
const MAX_POSIX_HOUR: i64 = 167;

/// Resolve a `POSIX` `TZ` specification — `STD offset [DST [offset] [,rule]]`.
///
/// The offset counts hours *west* of Greenwich, the opposite of the ISO sign, so
/// `UTC-2` names UTC+2 and a naked `-08:00` names UTC+8. `PostgreSQL` is laxer
/// than `POSIX` about the standard abbreviation — its `tzparse` allows one
/// shorter than three characters, or none at all — which is precisely what makes
/// a bare offset a legal specification. A specification with no daylight part
/// therefore resolves to a fixed offset here rather than going through jiff,
/// whose parser holds to the stricter `POSIX` grammar.
///
/// Two corners of `PostgreSQL`'s grammar stay out of reach and resolve to `None`
/// instead. Its `tzparse` accepts an offset of up to 167 hours, where a jiff
/// [`Offset`] stops at ±25:59:59, so `UTC-99` and `+30:00` are accepted there and
/// rejected here. And a fractional spelling such as `-1.5` reads, to `tzparse`,
/// as a one-hour standard offset followed by a daylight abbreviation spelled `.`
/// — a zone that then observes United States daylight saving. Neither is
/// reachable from the regression suite, whose fractional zone *settings* all
/// arrive by the numeric route in [`resolve_guc_time_zone`] instead.
fn posix_zone(spec: &str) -> Option<Zone> {
    let after_abbrev = posix_abbrev(spec)?;
    let (west_seconds, rest) = posix_offset(after_abbrev)?;
    if rest.is_empty() {
        let seconds = i32::try_from(-west_seconds).ok()?;
        return Offset::from_seconds(seconds).ok().map(Zone::Offset);
    }
    // A daylight abbreviation follows, so the zone has two offsets and needs a
    // transition rule to switch between them. jiff owns that part; all this adds
    // is PostgreSQL's default rule for a specification that names a daylight
    // abbreviation and then stops.
    let completed;
    let full = if rest.contains([',', ';']) {
        spec
    } else {
        completed = format!("{spec}{POSIX_DEFAULT_RULE}");
        &completed
    };
    TimeZone::posix(full).ok().map(Zone::Named)
}

/// Skip a `POSIX` zone abbreviation — `<quoted>` or a bare run of characters —
/// and return what follows it. `PostgreSQL` accepts an abbreviation of any
/// length, including an empty one, so this never fails on a short name.
fn posix_abbrev(spec: &str) -> Option<&str> {
    if let Some(quoted) = spec.strip_prefix('<') {
        let end = quoted.find('>')?;
        return Some(&quoted[end + '>'.len_utf8()..]);
    }
    let end = spec
        .find(|c: char| c.is_ascii_digit() || c == ',' || c == '+' || c == '-')
        .unwrap_or(spec.len());
    Some(&spec[end..])
}

/// Decode a `POSIX` offset — `[±]hh[:mm[:ss]]` — into seconds *west* of
/// Greenwich, and return it with the text it did not consume.
fn posix_offset(spec: &str) -> Option<(i64, &str)> {
    let (negative, rest) = match spec.as_bytes().first()? {
        b'-' => (true, &spec[1..]),
        b'+' => (false, &spec[1..]),
        _ => (false, spec),
    };
    let (hour, mut rest) = posix_number(rest, MAX_POSIX_HOUR)?;
    let mut seconds = hour * 3600;
    if let Some(after_hour) = rest.strip_prefix(':') {
        let (minute, tail) = posix_number(after_hour, 59)?;
        seconds += minute * 60;
        rest = tail;
        if let Some(after_minute) = rest.strip_prefix(':') {
            // tzcode's `getsecs` allows a leap second here.
            let (second, tail) = posix_number(after_minute, 60)?;
            seconds += second;
            rest = tail;
        }
    }
    Some((if negative { -seconds } else { seconds }, rest))
}

/// Read the leading run of digits as a number no greater than `max`, and return
/// it with the text it did not consume. An empty run is not a number.
fn posix_number(spec: &str, max: i64) -> Option<(i64, &str)> {
    let end = spec
        .find(|c: char| !c.is_ascii_digit())
        .unwrap_or(spec.len());
    let value: i64 = spec[..end].parse().ok()?;
    (value <= max).then_some((value, &spec[end..]))
}

/// Look a name up in the zone database. Literals reach here lowercased, which
/// costs nothing: the bundled database matches without regard to ASCII case.
fn lookup_zone_name(name: &str) -> Option<Zone> {
    if name.eq_ignore_ascii_case("utc") || name.eq_ignore_ascii_case("gmt") {
        return Some(Zone::Offset(Offset::UTC));
    }
    zone_by_name(name).map(Zone::Named)
}

/// Resolve a zone the way `AT TIME ZONE`, the `timezone()` function and
/// `date_trunc`'s third argument do — `PostgreSQL`'s `DecodeTimezoneName`: one
/// of `PostgreSQL`'s abbreviations, a zone-database name, or a `POSIX` `TZ`
/// specification.
///
/// Database names come from the bundled database (see [`super::tzdb`]), so the
/// vocabulary is the server build's, not the host's.
///
/// The abbreviation table is consulted *before* the database, because the
/// database reuses a few spellings the default abbreviation set already binds:
/// `AT TIME ZONE 'CET'` is a fixed +01 rather than the zone whose summers run
/// at +02.
///
/// Everything that is neither an abbreviation nor a database name is a `POSIX`
/// specification, whose offset counts *west* of Greenwich — so `AT TIME ZONE
/// '-08:00'` yields UTC+8, and `'UTC-2'` yields UTC+2. Zone-bearing *literals*
/// do not come through here: their numeric offsets go to
/// [`decode_numeric_time_zone`], which is ISO, and only a punctuated zone
/// *name* in a literal reaches the `POSIX` grammar.
#[must_use]
pub fn resolve_time_zone(name: &str) -> Option<TimeZone> {
    let trimmed = name.trim();
    if trimmed.is_empty() {
        return None;
    }
    let lower = trimmed.to_ascii_lowercase();
    let zone = lookup_abbrev(&lower)
        .or_else(|| lookup_zone_name(trimmed))
        .or_else(|| posix_zone(trimmed))?;
    Some(zone.into_time_zone())
}

/// Resolve the value of the `TimeZone` setting, which `PostgreSQL`'s
/// `check_timezone` reads by rules of its own.
///
/// A value that is wholly a number — `'-08'`, `'+2'`, `'0'`, `'-1.5'` — is a
/// count of hours *east* of Greenwich, the ISO sign. Anything else is a
/// zone-database name or a `POSIX` specification, where the sign is *west*.
/// The two conventions therefore meet inside this one function: `SET TimeZone =
/// '-08'` puts a session on UTC-8, while `SET TimeZone = '-08:00'` — the same
/// offset written with a colon, which no longer parses as a number — puts it on
/// UTC+8.
///
/// The abbreviation table is not consulted at all, so `SET TimeZone = 'PST'`
/// fails here while `AT TIME ZONE 'PST'` succeeds.
#[must_use]
pub fn resolve_guc_time_zone(value: &str) -> Option<TimeZone> {
    let trimmed = value.trim();
    if trimmed.is_empty() {
        return None;
    }
    if let Some(offset) = numeric_hours_offset(trimmed) {
        return Some(TimeZone::fixed(offset));
    }
    let zone = lookup_zone_name(trimmed).or_else(|| posix_zone(trimmed))?;
    Some(zone.into_time_zone())
}

/// Decode the `TimeZone` setting's plain-number form: a possibly fractional
/// count of hours *east* of Greenwich. `None` when the text is not wholly a
/// number, which is how `PostgreSQL` distinguishes this form (`strtod` must
/// consume the entire string) from a zone name or `POSIX` specification.
fn numeric_hours_offset(value: &str) -> Option<Offset> {
    let (negative, magnitude) = match value.as_bytes().first()? {
        b'-' => (true, &value[1..]),
        b'+' => (false, &value[1..]),
        _ => (false, value),
    };
    let (whole, fraction) = magnitude.split_once('.').unwrap_or((magnitude, ""));
    if whole.is_empty() && fraction.is_empty() {
        return None;
    }
    if !whole
        .bytes()
        .chain(fraction.bytes())
        .all(|b| b.is_ascii_digit())
    {
        return None;
    }
    // Digits past the fourth cannot move a whole second, so dropping them keeps
    // the arithmetic in range without changing the answer.
    let fraction = &fraction[..fraction.len().min(9)];
    let mut seconds = if whole.is_empty() {
        0
    } else {
        whole.parse::<i64>().ok()?.checked_mul(3600)?
    };
    if !fraction.is_empty() {
        let scale = 10_i64.checked_pow(u32::try_from(fraction.len()).ok()?)?;
        // Truncating, like the C conversion PostgreSQL's `strtod` result goes
        // through on the way to a `long`.
        seconds = seconds.checked_add(fraction.parse::<i64>().ok()? * 3600 / scale)?;
    }
    let seconds = i32::try_from(if negative { -seconds } else { seconds }).ok()?;
    Offset::from_seconds(seconds).ok()
}

/// Resolve a bare timezone abbreviation.
fn lookup_abbrev(word: &str) -> Option<Zone> {
    // The dynamic abbreviations PostgreSQL's default set maps to a zone rather
    // than a fixed offset, so their offset follows that zone's history.
    if let Some(zone) = DYNAMIC_ABBREVS
        .iter()
        .find(|(abbrev, _)| *abbrev == word)
        .and_then(|(_, name)| zone_by_name(name))
    {
        return Some(Zone::Named(zone));
    }
    FIXED_ABBREVS
        .iter()
        .find(|(abbrev, _)| *abbrev == word)
        .and_then(|(_, seconds)| Offset::from_seconds(*seconds).ok())
        .map(Zone::Offset)
}

/// Convert a Julian Day Number to a calendar date.
fn julian_to_date(jd: i32) -> Result<Date, DecodeError> {
    /// The Julian Day Number of 2000-01-01, the anchor for the offset.
    const JD_2000_01_01: i32 = 2_451_545;
    let days = jd - JD_2000_01_01;
    Date::constant(2000, 1, 1)
        .checked_add(
            Span::new()
                .try_days(days)
                .map_err(|_| DecodeError::FieldOverflow)?,
        )
        .map_err(|_| DecodeError::FieldOverflow)
}

// ---------------------------------------------------------------------------
// Keyword tables
// ---------------------------------------------------------------------------

/// Map a reserved spelling to the value it stands for.
fn special_word(word: &str) -> Option<Special> {
    Some(match word {
        "infinity" => Special::Infinity,
        "now" => Special::Now,
        "today" => Special::Today,
        "tomorrow" => Special::Tomorrow,
        "yesterday" => Special::Yesterday,
        "epoch" => Special::Epoch,
        _ => return None,
    })
}

/// The month spellings `PostgreSQL` accepts, longest first within each month.
const MONTHS: [&[&str]; 12] = [
    &["january", "jan"],
    &["february", "feb"],
    &["march", "mar"],
    &["april", "apr"],
    &["may"],
    &["june", "jun"],
    &["july", "jul"],
    &["august", "aug"],
    &["september", "sept", "sep"],
    &["october", "oct"],
    &["november", "nov"],
    &["december", "dec"],
];

/// The 1-based month a name or abbreviation denotes.
fn month_number(word: &str) -> Option<i32> {
    let lower = word.to_ascii_lowercase();
    MONTHS
        .iter()
        .position(|names| names.contains(&lower.as_str()))
        .and_then(|i| i32::try_from(i).ok())
        .map(|i| i + 1)
}

/// The day-of-week spellings `PostgreSQL` accepts and then ignores.
const WEEKDAYS: [&str; 21] = [
    "sunday",
    "sun",
    "sund",
    "monday",
    "mon",
    "mond",
    "tuesday",
    "tue",
    "tues",
    "wednesday",
    "wed",
    "weds",
    "thursday",
    "thu",
    "thur",
    "thurs",
    "friday",
    "fri",
    "frid",
    "saturday",
    "sat",
];

/// Whether a word is a day-of-week name.
fn is_weekday(word: &str) -> bool {
    let lower = word.to_ascii_lowercase();
    WEEKDAYS.contains(&lower.as_str())
}

/// Abbreviations `PostgreSQL`'s default set resolves through a zone's history
/// rather than a fixed offset.
const DYNAMIC_ABBREVS: [(&str, &str); 1] = [("msk", "Europe/Moscow")];

/// `PostgreSQL`'s default timezone-abbreviation set, as UTC offsets in seconds.
const FIXED_ABBREVS: &[(&str, i32)] = &[
    ("acdt", 37800),
    ("acsst", 37800),
    ("acst", 34200),
    ("act", -18000),
    ("acwst", 31500),
    ("adt", -10800),
    ("aedt", 39600),
    ("aesst", 39600),
    ("aest", 36000),
    ("aft", 16200),
    ("akdt", -28800),
    ("akst", -32400),
    ("almst", 25200),
    ("almt", 21600),
    ("amst", 14400),
    ("amt", -14400),
    ("anast", 43200),
    ("anat", 43200),
    ("arst", -10800),
    ("art", -10800),
    ("ast", -14400),
    ("awsst", 32400),
    ("awst", 28800),
    ("azost", 0),
    ("azot", -3600),
    ("azst", 14400),
    ("azt", 14400),
    ("bdst", 7200),
    ("bdt", 21600),
    ("bnt", 28800),
    ("bort", 28800),
    ("bot", -14400),
    ("bra", -10800),
    ("brst", -7200),
    ("brt", -10800),
    ("bst", 3600),
    ("btt", 21600),
    ("cadt", 37800),
    ("cast", 34200),
    ("cct", 28800),
    ("cdt", -18000),
    ("cest", 7200),
    ("cet", 3600),
    ("cetdst", 7200),
    ("chadt", 49500),
    ("chast", 45900),
    ("chut", 36000),
    ("ckt", -36000),
    ("clst", -10800),
    ("clt", -14400),
    ("cot", -18000),
    ("cst", -21600),
    ("cxt", 25200),
    ("davt", 25200),
    ("ddut", 36000),
    ("easst", -21600),
    ("east", -21600),
    ("eat", 10800),
    ("edt", -14400),
    ("eest", 10800),
    ("eet", 7200),
    ("eetdst", 10800),
    ("egst", 0),
    ("egt", -3600),
    ("est", -18000),
    ("fet", 10800),
    ("fjst", 46800),
    ("fjt", 43200),
    ("fkst", -10800),
    ("fkt", -10800),
    ("fnst", -3600),
    ("fnt", -7200),
    ("galt", -21600),
    ("gamt", -32400),
    ("gest", 14400),
    ("get", 14400),
    ("gft", -10800),
    ("gilt", 43200),
    ("gmt", 0),
    ("gyt", -14400),
    ("hkt", 28800),
    ("hst", -36000),
    ("ict", 25200),
    ("idt", 10800),
    ("iot", 21600),
    ("irkst", 28800),
    ("irkt", 28800),
    ("irt", 12600),
    ("ist", 7200),
    ("jayt", 32400),
    ("jst", 32400),
    ("kdt", 36000),
    ("kgst", 21600),
    ("kgt", 21600),
    ("kost", 39600),
    ("krast", 25200),
    ("krat", 25200),
    ("kst", 32400),
    ("lhdt", 37800),
    ("lhst", 37800),
    ("ligt", 36000),
    ("lint", 50400),
    ("lkt", 19800),
    ("magst", 43200),
    ("magt", 43200),
    ("mart", -34200),
    ("mawt", 18000),
    ("mdt", -21600),
    ("mest", 7200),
    ("met", 3600),
    ("metdst", 7200),
    ("mht", 43200),
    ("mmt", 23400),
    ("mpt", 36000),
    ("mst", -25200),
    ("mut", 14400),
    ("mvt", 18000),
    ("myt", 28800),
    ("nct", 39600),
    ("ndt", -9000),
    ("nft", -12600),
    ("npt", 20700),
    ("nst", -12600),
    ("nut", -39600),
    ("nzdt", 46800),
    ("nzst", 43200),
    ("nzt", 43200),
    ("pdt", -25200),
    ("pet", -18000),
    ("petst", 43200),
    ("pett", 43200),
    ("pgt", 36000),
    ("phot", 46800),
    ("pht", 28800),
    ("pkt", 18000),
    ("pmdt", -7200),
    ("pmst", -10800),
    ("pont", 39600),
    ("pst", -28800),
    ("pwt", 32400),
    ("pyst", -10800),
    ("pyt", -14400),
    ("ret", 14400),
    ("rott", -10800),
    ("sadt", 37800),
    ("sast", 7200),
    ("sct", 14400),
    ("sgt", 28800),
    ("tahat", -36000),
    ("tft", 18000),
    ("tjt", 18000),
    ("tkt", 46800),
    ("tmt", 18000),
    ("tot", 46800),
    ("tvt", 43200),
    ("uct", 0),
    ("ulast", 32400),
    ("ulat", 28800),
    ("ut", 0),
    ("utc", 0),
    ("uyst", -7200),
    ("uyt", -10800),
    ("uzst", 18000),
    ("uzt", 18000),
    ("vet", -14400),
    ("vlast", 36000),
    ("vlat", 36000),
    ("vut", 39600),
    ("wadt", 28800),
    ("wakt", 43200),
    ("wast", 25200),
    ("wat", 3600),
    ("wdt", 32400),
    ("west", 3600),
    ("wet", 0),
    ("wetdst", 3600),
    ("wft", 43200),
    ("wgst", -7200),
    ("wgt", -10800),
    ("xjt", 21600),
    ("yakst", 32400),
    ("yakt", 32400),
    ("yapt", 36000),
    ("yekst", 18000),
    ("yekt", 18000),
    ("z", 0),
    ("zulu", 0),
];

#[cfg(test)]
mod zone_resolution_tests {
    use assert2::assert;
    use jiff::Timestamp;

    use super::{resolve_guc_time_zone, resolve_time_zone};

    /// `1970-01-01T00:00:00Z` — northern-hemisphere winter, standard time.
    fn winter() -> Timestamp {
        Timestamp::from_second(0).expect("epoch")
    }

    /// `2001-07-01T12:00:00Z` — northern-hemisphere summer, daylight time.
    fn summer() -> Timestamp {
        Timestamp::from_second(993_988_800).expect("summer instant")
    }

    /// The offset in seconds and the zone abbreviation a resolved zone reports.
    ///
    /// Resolution goes through the `TimeZone` setting's entry point, the one
    /// that reaches the zone database without the abbreviation table in front
    /// of it — and also the only entry point whose result is rendered with an
    /// abbreviation at all, since `AT TIME ZONE` yields a bare `timestamp`.
    fn rendered(name: &str, at: Timestamp) -> (i32, String) {
        let tz = resolve_guc_time_zone(name).unwrap_or_else(|| panic!("{name} should resolve"));
        let info = tz.to_offset_info(at);
        (info.offset().seconds(), info.abbreviation().to_string())
    }

    /// Every row was read off `PostgreSQL` 18.4, which resolves against the copy
    /// of the IANA database it ships. The legacy "backward" link names in the
    /// second half are the ones a trimmed host `tzdata` does not carry, so they
    /// only resolve because gres goes through the bundled database.
    #[test]
    fn zone_names_render_the_offsets_postgresql_renders() {
        let cases: &[(&str, i32, &str, i32, &str)] = &[
            // name, winter offset/abbrev, summer offset/abbrev
            ("UTC", 0, "UTC", 0, "UTC"),
            ("America/Los_Angeles", -8 * 3600, "PST", -7 * 3600, "PDT"),
            ("America/Denver", -7 * 3600, "MST", -6 * 3600, "MDT"),
            ("Europe/Rome", 3600, "CET", 2 * 3600, "CEST"),
            ("EST", -5 * 3600, "EST", -5 * 3600, "EST"),
            ("PST8PDT", -8 * 3600, "PST", -7 * 3600, "PDT"),
            ("EST5EDT", -5 * 3600, "EST", -4 * 3600, "EDT"),
            ("CST6CDT", -6 * 3600, "CST", -5 * 3600, "CDT"),
            ("MST7MDT", -7 * 3600, "MST", -6 * 3600, "MDT"),
            ("US/Pacific", -8 * 3600, "PST", -7 * 3600, "PDT"),
            ("US/Eastern", -5 * 3600, "EST", -4 * 3600, "EDT"),
            ("Navajo", -7 * 3600, "MST", -6 * 3600, "MDT"),
            ("Japan", 9 * 3600, "JST", 9 * 3600, "JST"),
            // Britain kept `BST` right through the 1968-1971 winters, so the
            // 1970 sample is *not* `GMT`.
            ("GB", 3600, "BST", 3600, "BST"),
        ];
        for &(name, winter_offset, winter_abbrev, summer_offset, summer_abbrev) in cases {
            assert!(
                rendered(name, winter()) == (winter_offset, winter_abbrev.to_string()),
                "{name} in winter"
            );
            assert!(
                rendered(name, summer()) == (summer_offset, summer_abbrev.to_string()),
                "{name} in summer"
            );
        }
    }

    /// Zone names are matched without regard to ASCII case, exactly as
    /// `PostgreSQL` matches them against its own database.
    #[test]
    fn zone_names_resolve_without_regard_to_case() {
        for (spelling, canonical) in [
            ("america/los_angeles", "America/Los_Angeles"),
            ("AMERICA/LOS_ANGELES", "America/Los_Angeles"),
            ("us/pacific", "US/Pacific"),
            ("pst8pdt", "PST8PDT"),
            ("Europe/rome", "Europe/Rome"),
        ] {
            let tz =
                resolve_time_zone(spelling).unwrap_or_else(|| panic!("{spelling} should resolve"));
            assert!(tz.iana_name() == Some(canonical), "{spelling}");
        }
    }

    /// Resolution must stay a property of the binary, so a name the bundled
    /// database does not carry is rejected however the host is configured.
    #[test]
    fn unknown_zone_names_are_rejected() {
        for name in ["Not/AZone", "posixrules", "America/Nowhere", "  "] {
            assert!(
                resolve_time_zone(name).is_none(),
                "{name} should not resolve"
            );
        }
    }
}

/// The three sign conventions `PostgreSQL` applies to a zone, and where each one
/// applies. Every expectation below was read off `PostgreSQL` 18.4.
#[cfg(test)]
mod posix_zone_tests {
    use assert2::assert;
    use jiff::{Timestamp, tz::TimeZone};

    use super::{resolve_guc_time_zone, resolve_time_zone};
    use crate::datetime::parse_timestamptz;

    /// `1970-01-01T00:00:00Z`, the instant every offset row is measured at.
    fn epoch() -> Timestamp {
        Timestamp::from_second(0).expect("epoch")
    }

    fn instant(text: &str) -> Timestamp {
        text.parse().expect("instant")
    }

    /// The seconds *east* of UTC a resolved zone is at `at`.
    fn seconds_east(tz: &TimeZone, at: Timestamp) -> i32 {
        tz.to_offset_info(at).offset().seconds()
    }

    /// `AT TIME ZONE`, `timezone()` and `date_trunc`'s zone argument all read
    /// their text as a `POSIX` specification, whose offset counts *west* of
    /// Greenwich. Writing `'-08:00'` there therefore selects UTC+8.
    #[test]
    fn a_zone_specification_counts_its_offset_west_of_greenwich() {
        let cases: &[(&str, i32)] = &[
            ("-08:00", 28800),
            ("+08:00", -28800),
            ("08:00", -28800),
            ("-13:00", 46800),
            ("-00:30", 1800),
            ("00:30", -1800),
            ("-04:15", 15300),
            ("04:15", -15300),
            ("-04:30", 16200),
            ("04:30", -16200),
            ("UTC-2", 7200),
            ("UTC+10", -36000),
            ("-08", 28800),
            ("+10", -36000),
            ("2", -7200),
            ("0", 0),
            ("+16:00", -57600),
            ("00:00", 0),
            ("+02:00", -7200),
        ];
        for &(spec, east) in cases {
            let tz = resolve_time_zone(spec).unwrap_or_else(|| panic!("{spec} should resolve"));
            assert!(seconds_east(&tz, epoch()) == east, "AT TIME ZONE {spec}");
        }
    }

    /// The `TimeZone` setting is the one place a bare number keeps the ISO sign:
    /// a value that is wholly a number counts hours *east*. The very same offset
    /// written with a colon is no longer a number, falls through to the `POSIX`
    /// grammar, and lands on the opposite side of Greenwich.
    #[test]
    fn the_time_zone_setting_reads_a_whole_number_as_hours_east() {
        let cases: &[(&str, i32)] = &[
            // Numeric: east.
            ("-08", -28800),
            ("+10", 36000),
            ("2", 7200),
            ("0", 0),
            ("-1.5", -5400),
            ("-0.5", -1800),
            ("16", 57600),
            ("+00", 0),
            // POSIX: west.
            ("00:00", 0),
            ("+02:00", -7200),
            ("-13:00", 46800),
            ("-00:30", 1800),
            ("00:30", -1800),
            ("-04:30", 16200),
            ("04:30", -16200),
            ("-04:15", 15300),
            ("04:15", -15300),
            ("UTC-2", 7200),
        ];
        for &(value, east) in cases {
            let tz = resolve_guc_time_zone(value).unwrap_or_else(|| panic!("{value} should set"));
            assert!(seconds_east(&tz, epoch()) == east, "SET TimeZone = {value}");
        }
    }

    /// The setting does not consult the abbreviation table, so a spelling that is
    /// only an abbreviation is a `SET` error even though `AT TIME ZONE` takes it.
    #[test]
    fn the_time_zone_setting_refuses_abbreviations_at_time_zone_accepts() {
        for abbrev in ["PST", "EDT", "ACST", "MSK"] {
            assert!(
                resolve_guc_time_zone(abbrev).is_none(),
                "SET TimeZone = {abbrev} should fail"
            );
            assert!(
                resolve_time_zone(abbrev).is_some(),
                "AT TIME ZONE {abbrev} should resolve"
            );
        }
    }

    /// Where a spelling is both an abbreviation and a database name, each entry
    /// point picks a different one — visible only outside standard time, since
    /// the abbreviation is a fixed offset and the database name is not.
    #[test]
    fn an_abbreviation_outranks_a_database_name_of_the_same_spelling() {
        let summer = instant("2001-07-01T12:00:00Z");
        for (spelling, abbrev_east, zone_east) in [
            ("CET", 3600, 7200),
            ("MET", 3600, 7200),
            ("EET", 7200, 10800),
        ] {
            let by_spec = resolve_time_zone(spelling).expect("abbreviation resolves");
            assert!(seconds_east(&by_spec, summer) == abbrev_east, "{spelling}");
            let by_setting = resolve_guc_time_zone(spelling).expect("database name resolves");
            assert!(seconds_east(&by_setting, summer) == zone_east, "{spelling}");
        }
    }

    /// A `POSIX` specification may carry a daylight-saving transition rule, and
    /// the transition lands on the instant the rule names.
    #[test]
    fn posix_specifications_carry_daylight_saving_rules() {
        let cases: &[(&str, &str, i32)] = &[
            ("PST8PDT,M3.2.0,M11.1.0", "2014-03-09T09:59:00Z", -28800),
            ("PST8PDT,M3.2.0,M11.1.0", "2014-03-09T10:01:00Z", -25200),
            ("PST8PDT,M3.2.0,M11.1.0", "2014-11-02T08:59:00Z", -25200),
            ("PST8PDT,M3.2.0,M11.1.0", "2014-11-02T09:01:00Z", -28800),
            ("CST7CDT,M4.1.0,M10.5.0", "2014-01-15T12:00:00Z", -25200),
            ("CST7CDT,M4.1.0,M10.5.0", "2014-07-15T12:00:00Z", -21600),
            // A daylight abbreviation with no rule of its own takes
            // PostgreSQL's default, which is the United States rule.
            ("FOO8BAR", "2014-01-15T12:00:00Z", -28800),
            ("FOO8BAR", "2014-07-15T12:00:00Z", -25200),
        ];
        for &(spec, at, east) in cases {
            let at = instant(at);
            let by_spec =
                resolve_time_zone(spec).unwrap_or_else(|| panic!("{spec} should resolve"));
            assert!(seconds_east(&by_spec, at) == east, "{spec} at {at}");
            let by_setting =
                resolve_guc_time_zone(spec).unwrap_or_else(|| panic!("{spec} should set"));
            assert!(seconds_east(&by_setting, at) == east, "SET {spec} at {at}");
        }
    }

    /// The zone of a *literal* is a separate grammar that neither resolver is
    /// reachable from: a numeric offset there keeps the ISO sign, so
    /// `'... -08:00'` is eight hours behind UTC and not eight ahead. Only a
    /// zone *name* in a literal reaches the `POSIX` grammar, which is why
    /// `'UTC-2'` still inverts.
    #[test]
    fn a_literals_own_offset_keeps_the_iso_sign() {
        let cases: &[(&str, &str)] = &[
            ("1997-02-10 17:32:01 -0800", "1997-02-11T01:32:01Z"),
            ("1997-02-10 17:32:01 -08:00", "1997-02-11T01:32:01Z"),
            ("1997-02-10 17:32:01 -08", "1997-02-11T01:32:01Z"),
            ("1997-02-10 17:32:01 +08:00", "1997-02-10T09:32:01Z"),
            ("1997-02-10 17:32:01 +0815", "1997-02-10T09:17:01Z"),
            // An abbreviation, and then a POSIX specification.
            ("1997-02-10 17:32:01 PST", "1997-02-11T01:32:01Z"),
            ("1997-02-10 17:32:01 UTC-2", "1997-02-10T15:32:01Z"),
        ];
        for &(literal, expected) in cases {
            let parsed = parse_timestamptz(literal, &TimeZone::UTC)
                .unwrap_or_else(|e| panic!("{literal}: {e}"));
            assert!(parsed == instant(expected), "{literal}");
        }
    }

    /// Text that is neither a number, a name nor a well-formed specification
    /// resolves nowhere.
    #[test]
    fn malformed_specifications_resolve_nowhere() {
        for spec in [
            "Not/AZone",
            "PST8PDT,nonsense",
            "-",
            "+",
            "UTC-",
            "UTC-99",
            "",
            "   ",
        ] {
            assert!(resolve_time_zone(spec).is_none(), "AT TIME ZONE {spec}");
            assert!(resolve_guc_time_zone(spec).is_none(), "SET TimeZone {spec}");
        }
    }
}
