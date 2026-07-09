use std::{
    collections::BTreeSet,
    fmt::Write as _,
    path::{Path, PathBuf},
};

use assert2::assert;
use crabka_postgres_wal::{Lsn, WalStreamDecoder, XLogRecord};

const FIXTURE_MANIFEST: &str = include_str!("fixtures/manifest.toml");
const WAL_ORACLE: &str = include_str!("fixtures/oracle.waldump");
const REQUIRED_PG4B_WORKLOADS: &[&str] = &[
    "heap_dml_fpi",
    "btree_primary_key",
    "brin_index",
    "hash_index",
    "gist_index",
    "spgist_index",
    "gin_index",
    "multixact_row_locks",
    "truncate_drop_lifecycle",
    "database_wal_log",
];
const REQUIRED_STANDBY_ORACLES: &[&str] = &[
    "oracle.waldump",
    "standby/relations.tsv",
    "standby/relations",
    "standby/slru/pg_xact",
    "standby/slru/pg_multixact",
];
const MISSING_STANDBY_ORACLES_IN_SYNTHETIC_CORPUS: &[&str] = &[
    "standby/relations.tsv",
    "standby/relations",
    "standby/slru/pg_xact",
    "standby/slru/pg_multixact",
];
const REQUIRED_PG6_FORK_ORACLES: &[&str] = &[
    "fork/manifest.toml",
    "fork/parent/wal",
    "fork/child/wal",
    "fork/parent/standby-oracle",
    "fork/child/standby-oracle",
];
const SHA256_INITIAL_STATE: [u32; 8] = [
    0x6a09_e667,
    0xbb67_ae85,
    0x3c6e_f372,
    0xa54f_f53a,
    0x510e_527f,
    0x9b05_688c,
    0x1f83_d9ab,
    0x5be0_cd19,
];
const SHA256_ROUND_CONSTANTS: [u32; 64] = [
    0x428a_2f98,
    0x7137_4491,
    0xb5c0_fbcf,
    0xe9b5_dba5,
    0x3956_c25b,
    0x59f1_11f1,
    0x923f_82a4,
    0xab1c_5ed5,
    0xd807_aa98,
    0x1283_5b01,
    0x2431_85be,
    0x550c_7dc3,
    0x72be_5d74,
    0x80de_b1fe,
    0x9bdc_06a7,
    0xc19b_f174,
    0xe49b_69c1,
    0xefbe_4786,
    0x0fc1_9dc6,
    0x240c_a1cc,
    0x2de9_2c6f,
    0x4a74_84aa,
    0x5cb0_a9dc,
    0x76f9_88da,
    0x983e_5152,
    0xa831_c66d,
    0xb003_27c8,
    0xbf59_7fc7,
    0xc6e0_0bf3,
    0xd5a7_9147,
    0x06ca_6351,
    0x1429_2967,
    0x27b7_0a85,
    0x2e1b_2138,
    0x4d2c_6dfc,
    0x5338_0d13,
    0x650a_7354,
    0x766a_0abb,
    0x81c2_c92e,
    0x9272_2c85,
    0xa2bf_e8a1,
    0xa81a_664b,
    0xc24b_8b70,
    0xc76c_51a3,
    0xd192_e819,
    0xd699_0624,
    0xf40e_3585,
    0x106a_a070,
    0x19a4_c116,
    0x1e37_6c08,
    0x2748_774c,
    0x34b0_bcb5,
    0x391c_0cb3,
    0x4ed8_aa4a,
    0x5b9c_ca4f,
    0x682e_6ff3,
    0x748f_82ee,
    0x78a5_636f,
    0x84c8_7814,
    0x8cc7_0208,
    0x90be_fffa,
    0xa450_6ceb,
    0xbef9_a3f7,
    0xc671_78f2,
];

#[test]
fn fixture_decoding_matches_pg_waldump_header_oracle() {
    let manifest = fixture_manifest();
    let records = decode_fixture_records(&manifest);
    let actual = records
        .iter()
        .map(WalRecordHeaderOracle::from)
        .collect::<Vec<_>>();
    let expected = oracle_record_headers();

    assert!(!actual.is_empty());
    assert!(actual.len() == manifest.records);
    assert!(actual.len() == expected.len());
    for (index, (actual_record, expected_record)) in actual.iter().zip(&expected).enumerate() {
        assert!((index, actual_record) == (index, expected_record));
    }
}

#[test]
fn fixture_body_grammar_decodes_real_pg17_corpus() {
    let manifest = fixture_manifest();
    let records = decode_fixture_records(&manifest);
    let decoded_records = records
        .iter()
        .map(|record| {
            record
                .decode()
                .expect("fixture record body is grammar-decodable")
        })
        .collect::<Vec<_>>();

    assert!(decoded_records.len() == manifest.records);
    assert!(
        decoded_records
            .iter()
            .any(|record| record.blocks.iter().any(|block| block.image.is_some()))
    );
}

#[test]
fn fixture_manifest_declares_current_corpus_honestly() {
    let manifest = fixture_manifest();

    assert!(manifest.pg_major == 17);
    match manifest.corpus.as_str() {
        "real-pg17-standby-oracle" => assert_real_pg17_manifest_shape(&manifest),
        "synthetic-nonempty-records" => assert_synthetic_manifest_shape(&manifest),
        other => panic!("unsupported fixture corpus {other}"),
    }
}

#[test]
fn fixture_manifest_declares_pg4b_workload_and_oracle_shape() {
    let manifest = fixture_manifest();

    assert!(manifest.required_workloads == string_vec(REQUIRED_PG4B_WORKLOADS));
    assert!(manifest.required_oracles == string_vec(REQUIRED_STANDBY_ORACLES));
    for required_oracle in REQUIRED_STANDBY_ORACLES {
        assert!(fixture_path(required_oracle).exists());
        assert!(manifest.file_or_directory_declared(required_oracle));
    }
}

#[test]
fn fixture_manifest_declares_pg6_promote_diverge_shape() {
    let manifest = fixture_manifest();

    assert!(manifest.required_fork_oracles == string_vec(REQUIRED_PG6_FORK_ORACLES));
    assert!(manifest.pg6_fork_capture == "required-external-promote-and-diverge-workflow");
}

#[test]
fn fixture_manifest_checksums_match_committed_files() {
    let manifest = fixture_manifest();
    let manifest_files = manifest.file_set();
    let mut expected_files = manifest_files.clone();
    expected_files.insert("manifest.toml".to_owned());

    assert!(fixture_file_set() == expected_files);
    for file in &manifest.files {
        assert_manifest_checksum(file);
    }
}

#[test]
fn fixture_manifest_capture_lsn_is_inside_committed_wal_and_oracle_bounds() {
    let manifest = fixture_manifest();
    let wal_range = manifest.committed_wal_range();
    let oracle_records = oracle_record_headers();

    assert!(wal_range.contains_capture_lsn(manifest.capture_lsn));
    assert!(!oracle_records.is_empty());
    assert!(oracle_records.len() == manifest.records);
    assert!(
        oracle_records
            .iter()
            .all(|record| wal_range.contains_record_lsn(record.start_lsn))
    );
    assert!(
        oracle_records
            .iter()
            .all(|record| record.start_lsn < manifest.capture_lsn)
    );
    for adjacent_records in oracle_records.windows(2) {
        assert!(adjacent_records[0].start_lsn < adjacent_records[1].start_lsn);
    }
}

fn assert_real_pg17_manifest_shape(manifest: &FixtureManifest) {
    let wal_segments = manifest.wal_segments();
    let wal_range = manifest.committed_wal_range();

    assert!(manifest.provenance == "real-postgresql-17");
    assert!(manifest.wal_segment_count == wal_segments.len());
    assert!(manifest.wal_segment_count >= 4);
    assert!(manifest.missing_plan_oracles.is_none());
    assert!(wal_range.contains_capture_lsn(manifest.capture_lsn));
    assert!(manifest.file_or_directory_declared("oracle.waldump"));
    assert!(manifest.file_or_directory_declared("standby/relations"));
    assert!(manifest.file_or_directory_declared("standby/slru/pg_xact"));
    assert!(manifest.file_or_directory_declared("standby/slru/pg_multixact"));
}

fn assert_synthetic_manifest_shape(manifest: &FixtureManifest) {
    let Some(missing_plan_oracles) = manifest.missing_plan_oracles.as_ref() else {
        panic!("synthetic fixture manifest must declare missing_plan_oracles");
    };

    assert!(manifest.provenance == "synthetic-dev");
    assert_contains_all(
        missing_plan_oracles,
        MISSING_STANDBY_ORACLES_IN_SYNTHETIC_CORPUS,
    );
    assert_contains_all(missing_plan_oracles, REQUIRED_PG6_FORK_ORACLES);
}

fn decode_fixture_records(manifest: &FixtureManifest) -> Vec<XLogRecord> {
    let wal_segments = manifest.wal_segments();
    let Some(first_segment) = wal_segments.first() else {
        panic!("fixture manifest must list at least one WAL segment");
    };
    let mut decoder = WalStreamDecoder::new(first_segment.base_lsn);
    let mut records = Vec::new();

    for segment in wal_segments {
        let bytes = std::fs::read(fixture_path(&segment.path)).expect("fixture segment reads");
        let bytes_len = u64::try_from(bytes.len()).expect("fixture segment length fits in u64");
        let committed_len = segment.committed_len(manifest.wal_segsize, manifest.capture_lsn);

        assert!(bytes_len == manifest.wal_segsize);
        if committed_len == 0 {
            continue;
        }
        decoder
            .feed(segment.base_lsn, &bytes[..committed_len])
            .expect("fixture committed WAL prefix feeds at its manifest-derived LSN");
        while let Some(record) = decoder
            .poll_record()
            .expect("fixture WAL record is frame-decodable")
        {
            records.push(record);
        }
    }

    records
}

fn oracle_record_headers() -> Vec<WalRecordHeaderOracle> {
    WAL_ORACLE
        .lines()
        .map(str::trim)
        .filter(|line| !line.is_empty() && !line.starts_with('#'))
        .map(parse_waldump_record_header)
        .collect()
}

fn parse_waldump_record_header(line: &str) -> WalRecordHeaderOracle {
    let Some((_, after_len_label)) = line.split_once("len (rec/tot):") else {
        panic!("pg_waldump oracle line is missing length fields: {line}");
    };
    let Some((_, after_record_len)) = after_len_label.split_once('/') else {
        panic!("pg_waldump oracle line is missing total length: {line}");
    };
    let Some((total_len, after_total_len)) = after_record_len.split_once(", tx:") else {
        panic!("pg_waldump oracle line is missing transaction id: {line}");
    };
    let Some((xid, after_xid)) = after_total_len.split_once(", lsn:") else {
        panic!("pg_waldump oracle line is missing start LSN: {line}");
    };
    let Some((start_lsn, after_start_lsn)) = after_xid.split_once(", prev ") else {
        panic!("pg_waldump oracle line is missing previous LSN: {line}");
    };
    let Some((prev_lsn, _description)) = after_start_lsn.split_once(", desc:") else {
        panic!("pg_waldump oracle line is missing description: {line}");
    };

    WalRecordHeaderOracle {
        start_lsn: parse_lsn(start_lsn.trim()),
        prev_lsn: parse_lsn(prev_lsn.trim()),
        xid: parse_u32(xid.trim(), "pg_waldump xid"),
        total_len: parse_u32(total_len.trim(), "pg_waldump total length"),
    }
}

#[derive(Debug, Clone, Copy, Eq, PartialEq)]
struct WalRecordHeaderOracle {
    start_lsn: Lsn,
    prev_lsn: Lsn,
    xid: u32,
    total_len: u32,
}

impl From<&XLogRecord> for WalRecordHeaderOracle {
    fn from(record: &XLogRecord) -> Self {
        Self {
            start_lsn: record.start_lsn,
            prev_lsn: record.header.prev_lsn,
            xid: record.header.xid,
            total_len: record.total_len,
        }
    }
}

#[derive(Debug, Eq, PartialEq)]
struct FixtureManifest {
    pg_major: i64,
    wal_segsize: u64,
    corpus: String,
    provenance: String,
    capture_lsn: Lsn,
    wal_segment_count: usize,
    records: usize,
    required_workloads: Vec<String>,
    required_oracles: Vec<String>,
    required_fork_oracles: Vec<String>,
    pg6_fork_capture: String,
    missing_plan_oracles: Option<Vec<String>>,
    files: Vec<ManifestFile>,
}

impl FixtureManifest {
    fn wal_segments(&self) -> Vec<WalSegment> {
        let mut segments = self
            .files
            .iter()
            .filter(|file| is_wal_manifest_path(&file.path))
            .map(|file| WalSegment {
                path: file.path.clone(),
                base_lsn: segment_base_lsn_from_filename(&file.path, self.wal_segsize),
            })
            .collect::<Vec<_>>();
        segments.sort_by_key(|segment| segment.base_lsn);

        assert!(segments.len() == self.wal_segment_count);
        segments
    }

    fn committed_wal_range(&self) -> WalRange {
        let segments = self.wal_segments();
        let Some(first_segment) = segments.first() else {
            panic!("fixture manifest must list at least one WAL segment");
        };
        let Some(last_segment) = segments.last() else {
            panic!("fixture manifest must list at least one WAL segment");
        };

        assert_contiguous_wal_segments(&segments, self.wal_segsize);
        WalRange {
            start_lsn: first_segment.base_lsn,
            end_lsn: last_segment.end_lsn(self.wal_segsize),
        }
    }

    fn file_set(&self) -> BTreeSet<String> {
        self.files.iter().map(|file| file.path.clone()).collect()
    }

    fn file_or_directory_declared(&self, required_path: &str) -> bool {
        let required_directory = format!("{required_path}/");

        self.files
            .iter()
            .any(|file| file.path == required_path || file.path.starts_with(&required_directory))
    }
}

#[derive(Debug, Eq, PartialEq)]
struct ManifestFile {
    path: String,
    sha256: String,
    crc32c: String,
}

#[derive(Debug, Eq, PartialEq)]
struct WalSegment {
    path: String,
    base_lsn: Lsn,
}

impl WalSegment {
    fn end_lsn(&self, wal_segsize: u64) -> Lsn {
        Lsn(self
            .base_lsn
            .value()
            .checked_add(wal_segsize)
            .expect("WAL segment end LSN fits in u64"))
    }

    fn committed_len(&self, wal_segsize: u64, capture_lsn: Lsn) -> usize {
        if capture_lsn <= self.base_lsn {
            return 0;
        }

        let committed_end_lsn = capture_lsn.next_page_start().min(self.end_lsn(wal_segsize));
        usize::try_from(committed_end_lsn.value() - self.base_lsn.value())
            .expect("committed WAL segment prefix length fits in usize")
    }
}

#[derive(Debug, Clone, Copy, Eq, PartialEq)]
struct WalRange {
    start_lsn: Lsn,
    end_lsn: Lsn,
}

impl WalRange {
    fn contains_capture_lsn(self, lsn: Lsn) -> bool {
        self.start_lsn < lsn && lsn <= self.end_lsn
    }

    fn contains_record_lsn(self, lsn: Lsn) -> bool {
        self.start_lsn <= lsn && lsn < self.end_lsn
    }
}

fn fixture_manifest() -> FixtureManifest {
    let table = FIXTURE_MANIFEST
        .parse::<toml::Table>()
        .expect("fixture manifest is valid TOML");

    FixtureManifest {
        pg_major: toml_integer(&table, "pg_major"),
        wal_segsize: parse_wal_segsize(&toml_string(&table, "wal_segsize")),
        corpus: toml_string(&table, "corpus"),
        provenance: toml_string(&table, "provenance"),
        capture_lsn: parse_lsn(&toml_string(&table, "capture_lsn")),
        wal_segment_count: toml_usize(&table, "wal_segment_count"),
        records: toml_usize(&table, "records"),
        required_workloads: toml_string_array(&table, "required_workloads"),
        required_oracles: toml_string_array(&table, "required_oracles"),
        required_fork_oracles: toml_string_array(&table, "required_fork_oracles"),
        pg6_fork_capture: toml_string(&table, "pg6_fork_capture"),
        missing_plan_oracles: optional_toml_string_array(&table, "missing_plan_oracles"),
        files: toml_manifest_files(&table),
    }
}

fn toml_value<'a>(table: &'a toml::Table, key: &str) -> &'a toml::Value {
    table
        .get(key)
        .unwrap_or_else(|| panic!("manifest key {key} is present"))
}

fn toml_string(table: &toml::Table, key: &str) -> String {
    let Some(string) = toml_value(table, key).as_str() else {
        panic!("manifest key {key} must be a string");
    };

    string.to_owned()
}

fn toml_integer(table: &toml::Table, key: &str) -> i64 {
    let Some(integer) = toml_value(table, key).as_integer() else {
        panic!("manifest key {key} must be an integer");
    };

    integer
}

fn toml_usize(table: &toml::Table, key: &str) -> usize {
    usize::try_from(toml_integer(table, key))
        .unwrap_or_else(|_| panic!("manifest key {key} must fit in usize"))
}

fn toml_string_array(table: &toml::Table, key: &str) -> Vec<String> {
    let Some(array) = toml_value(table, key).as_array() else {
        panic!("manifest key {key} must be a string array");
    };

    parse_toml_string_array_entries(array, key)
}

fn optional_toml_string_array(table: &toml::Table, key: &str) -> Option<Vec<String>> {
    let array = table.get(key)?.as_array().unwrap_or_else(|| {
        panic!("manifest key {key} must be a string array");
    });

    Some(parse_toml_string_array_entries(array, key))
}

fn parse_toml_string_array_entries(array: &[toml::Value], key: &str) -> Vec<String> {
    array
        .iter()
        .map(|entry| {
            let Some(string) = entry.as_str() else {
                panic!("manifest key {key} contains a non-string entry");
            };

            string.to_owned()
        })
        .collect()
}

fn toml_manifest_files(table: &toml::Table) -> Vec<ManifestFile> {
    let Some(files) = toml_value(table, "files").as_array() else {
        panic!("manifest key files must be an array of tables");
    };

    files
        .iter()
        .map(|entry| {
            let Some(table) = entry.as_table() else {
                panic!("manifest files entry must be a table");
            };

            ManifestFile {
                path: toml_string(table, "path"),
                sha256: toml_string(table, "sha256"),
                crc32c: toml_string(table, "crc32c"),
            }
        })
        .collect()
}

fn fixture_path(relative_path: &str) -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("tests/fixtures")
        .join(relative_path)
}

fn is_wal_manifest_path(path: &str) -> bool {
    Path::new(path)
        .extension()
        .is_some_and(|extension| extension.eq_ignore_ascii_case("wal"))
}

fn assert_contiguous_wal_segments(segments: &[WalSegment], wal_segsize: u64) {
    for adjacent_segments in segments.windows(2) {
        let expected_next_lsn = adjacent_segments[0].end_lsn(wal_segsize);

        assert!(adjacent_segments[1].base_lsn == expected_next_lsn);
    }
}

fn fixture_file_set() -> BTreeSet<String> {
    let fixture_dir = fixture_path("");
    let mut files = BTreeSet::new();

    collect_fixture_files(&fixture_dir, &fixture_dir, &mut files);
    files
}

fn collect_fixture_files(root: &Path, directory: &Path, files: &mut BTreeSet<String>) {
    for entry in std::fs::read_dir(directory).expect("fixture directory reads") {
        let entry = entry.expect("fixture directory entry reads");
        let path = entry.path();
        let file_type = entry.file_type().expect("fixture directory entry has type");

        if file_type.is_dir() {
            collect_fixture_files(root, &path, files);
            continue;
        }
        if !file_type.is_file() {
            continue;
        }

        files.insert(relative_fixture_path(root, &path));
    }
}

fn relative_fixture_path(root: &Path, path: &Path) -> String {
    path.strip_prefix(root)
        .expect("fixture path is inside fixture root")
        .iter()
        .map(|component| component.to_str().expect("fixture paths are valid UTF-8"))
        .collect::<Vec<_>>()
        .join("/")
}

fn parse_wal_segsize(raw_size: &str) -> u64 {
    let Some(megabytes) = raw_size.strip_suffix("MB") else {
        panic!("unsupported wal_segsize {raw_size}");
    };
    let megabytes = megabytes
        .parse::<u64>()
        .unwrap_or_else(|source| panic!("wal_segsize must start with a number: {source}"));

    megabytes
        .checked_mul(1024 * 1024)
        .expect("wal_segsize fits in u64")
}

fn segment_base_lsn_from_filename(path: &str, wal_segsize: u64) -> Lsn {
    let Some(filename) = Path::new(path).file_name().and_then(|name| name.to_str()) else {
        panic!("WAL path must have a UTF-8 file name: {path}");
    };
    let Some(wal_name) = filename.strip_suffix(".wal") else {
        panic!("WAL file must use .wal extension: {path}");
    };
    assert!(wal_name.len() == 24);

    let log = parse_hex_u64(&wal_name[8..16], "WAL log number");
    let segment = parse_hex_u64(&wal_name[16..24], "WAL segment number");
    let lsn_space_per_log = u64::from(u32::MAX) + 1;
    assert!(lsn_space_per_log % wal_segsize == 0);
    let segments_per_log = lsn_space_per_log / wal_segsize;
    let segment_number = log
        .checked_mul(segments_per_log)
        .and_then(|value| value.checked_add(segment))
        .expect("WAL segment number fits in u64");
    let base_lsn = segment_number
        .checked_mul(wal_segsize)
        .expect("WAL segment base LSN fits in u64");

    Lsn(base_lsn)
}

fn parse_lsn(raw_lsn: &str) -> Lsn {
    raw_lsn
        .parse::<Lsn>()
        .unwrap_or_else(|source| panic!("invalid LSN {raw_lsn}: {source}"))
}

fn parse_u32(raw: &str, label: &str) -> u32 {
    raw.parse::<u32>()
        .unwrap_or_else(|source| panic!("invalid {label} {raw}: {source}"))
}

fn parse_hex_u64(raw: &str, label: &str) -> u64 {
    u64::from_str_radix(raw, 16).unwrap_or_else(|source| panic!("invalid {label} {raw}: {source}"))
}

fn assert_contains_all(actual_values: &[String], expected_values: &[&str]) {
    for expected_value in expected_values {
        assert!(actual_values.iter().any(|actual| actual == expected_value));
    }
}

fn string_vec(values: &[&str]) -> Vec<String> {
    values.iter().map(|value| (*value).to_owned()).collect()
}

fn assert_manifest_checksum(file: &ManifestFile) {
    let bytes = std::fs::read(fixture_path(&file.path)).expect("manifest fixture path reads");
    let actual_sha256 = sha256_hex(&bytes);
    let actual_crc32c = format!("{:08x}", crc32c::crc32c(&bytes));

    assert!(actual_sha256 == file.sha256);
    assert!(actual_crc32c == file.crc32c);
}

fn sha256_hex(bytes: &[u8]) -> String {
    let digest = sha256_digest(bytes);
    let mut hex = String::with_capacity(digest.len() * 2);
    for byte in digest {
        write!(&mut hex, "{byte:02x}").expect("writing to a String cannot fail");
    }
    hex
}

fn sha256_digest(bytes: &[u8]) -> [u8; 32] {
    let mut state = SHA256_INITIAL_STATE;
    let padded = sha256_padded_message(bytes);

    for chunk in padded.chunks_exact(64) {
        let schedule = sha256_message_schedule(chunk);
        sha256_compress_chunk(&mut state, schedule);
    }

    sha256_state_to_digest(state)
}

fn sha256_padded_message(bytes: &[u8]) -> Vec<u8> {
    let mut padded = Vec::with_capacity((bytes.len() + 72) & !63);
    padded.extend_from_slice(bytes);
    padded.push(0x80);
    while padded.len() % 64 != 56 {
        padded.push(0);
    }
    padded.extend_from_slice(&((bytes.len() as u64) * 8).to_be_bytes());
    padded
}

fn sha256_message_schedule(chunk: &[u8]) -> [u32; 64] {
    assert!(chunk.len() == 64);

    let mut schedule = [0_u32; 64];
    for (word, word_bytes) in schedule.iter_mut().take(16).zip(chunk.chunks_exact(4)) {
        *word = u32::from_be_bytes([word_bytes[0], word_bytes[1], word_bytes[2], word_bytes[3]]);
    }

    for index in 16..64 {
        let previous_second = schedule[index - 2];
        let previous_fifteenth = schedule[index - 15];
        let sigma_zero = previous_fifteenth.rotate_right(7)
            ^ previous_fifteenth.rotate_right(18)
            ^ (previous_fifteenth >> 3);
        let sigma_one = previous_second.rotate_right(17)
            ^ previous_second.rotate_right(19)
            ^ (previous_second >> 10);
        schedule[index] = schedule[index - 16]
            .wrapping_add(sigma_zero)
            .wrapping_add(schedule[index - 7])
            .wrapping_add(sigma_one);
    }

    schedule
}

fn sha256_compress_chunk(state: &mut [u32; 8], schedule: [u32; 64]) {
    let mut working = *state;
    for (round_constant, word) in SHA256_ROUND_CONSTANTS.into_iter().zip(schedule) {
        sha256_round(&mut working, round_constant, word);
    }

    for (state_word, round_word) in state.iter_mut().zip(working) {
        *state_word = state_word.wrapping_add(round_word);
    }
}

fn sha256_round(working: &mut [u32; 8], round_constant: u32, word: u32) {
    let choice = (working[4] & working[5]) ^ (!working[4] & working[6]);
    let majority =
        (working[0] & working[1]) ^ (working[0] & working[2]) ^ (working[1] & working[2]);
    let sum_zero =
        working[0].rotate_right(2) ^ working[0].rotate_right(13) ^ working[0].rotate_right(22);
    let sum_one =
        working[4].rotate_right(6) ^ working[4].rotate_right(11) ^ working[4].rotate_right(25);
    let temporary_one = working[7]
        .wrapping_add(sum_one)
        .wrapping_add(choice)
        .wrapping_add(round_constant)
        .wrapping_add(word);
    let temporary_two = sum_zero.wrapping_add(majority);

    working[7] = working[6];
    working[6] = working[5];
    working[5] = working[4];
    working[4] = working[3].wrapping_add(temporary_one);
    working[3] = working[2];
    working[2] = working[1];
    working[1] = working[0];
    working[0] = temporary_one.wrapping_add(temporary_two);
}

fn sha256_state_to_digest(state: [u32; 8]) -> [u8; 32] {
    let mut digest = [0_u8; 32];
    for (output, word) in digest.chunks_exact_mut(4).zip(state) {
        output.copy_from_slice(&word.to_be_bytes());
    }
    digest
}
