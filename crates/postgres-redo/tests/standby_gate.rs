use std::{
    env,
    path::{Path, PathBuf},
};

use assert2::assert;
use bytes::Bytes;
use crabka_page_store::{PAGE_SIZE, PageKey};
use crabka_postgres_redo::{
    IndexRedoFamily, PageImage, RedoError, UnsupportedRedoFamily, apply_decoded_record_block,
    consts_v17, deterministic_page_hash,
};
use crabka_postgres_wal::{
    BlockFlags, BlockImage, BlockRef, DecodedRecord, Lsn, RelFileLocator, XLogRecordHeader,
};

const START_LSN: Lsn = Lsn(0x0100_0000);
const TOTAL_LEN: u32 = 128;
const TOTAL_LEN_U64: u64 = 128;
const END_LSN: Lsn = Lsn(START_LSN.0 + TOTAL_LEN_U64);
const PG17_FIXTURE_MANIFEST: &str = include_str!("../../postgres-wal/tests/fixtures/manifest.toml");
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

#[test]
fn heap_full_page_image_matches_standby_page() {
    let standby = page_with_fill(0xA5);
    let record = decoded_record(
        consts_v17::RM_HEAP_ID,
        0x20,
        Some(standby.clone()),
        Vec::new(),
    );

    let page = apply_decoded_record_block(None, &record, 0);

    assert!(let Ok(page) = page);
    assert!(page.key() == page_key());
    assert!(page.lsn() == END_LSN);
    assert!(page.bytes().as_ref() == page_with_lsn(standby, END_LSN).as_slice());
}

#[test]
fn xlog_apply_full_page_image_matches_standby_page() {
    let standby = page_with_fill(0xF1);
    let record = decoded_record(
        consts_v17::RM_XLOG_ID,
        consts_v17::XLOG_FPI,
        Some(standby.clone()),
        Vec::new(),
    );

    let page = apply_decoded_record_block(None, &record, 0);

    assert!(let Ok(page) = page);
    assert!(page.key() == page_key());
    assert!(page.lsn() == END_LSN);
    assert!(page.bytes().as_ref() == page_with_lsn(standby, END_LSN).as_slice());
}

#[test]
fn non_apply_full_page_images_are_rejected_for_supported_rmgrs() {
    for (rmid, info) in supported_full_page_image_rmgrs() {
        let record = decoded_record_with_image_apply(rmid, info, Some(page_with_fill(0xAA)), false);

        let page = apply_decoded_record_block(None, &record, 0);

        assert!(
            page == Err(RedoError::BadRecord {
                lsn: START_LSN,
                context: "non-apply block image is unsupported",
            })
        );
    }
}

#[test]
fn heap_init_record_builds_deterministic_zero_page() {
    let record = decoded_record(
        consts_v17::RM_HEAP_ID,
        consts_v17::XLOG_HEAP_INSERT | consts_v17::XLOG_HEAP_INIT_PAGE,
        None,
        Vec::new(),
    );

    let page = apply_decoded_record_block(None, &record, 0);

    assert!(let Ok(page) = page);
    assert!(page.bytes().as_ref() == page_with_lsn(vec![0_u8; PAGE_SIZE], END_LSN).as_slice());
}

#[test]
fn heap_page_sized_delta_payload_is_unsupported() {
    let page_sized_delta = page_with_fill(0x4A);
    let record = decoded_record(
        consts_v17::RM_HEAP_ID,
        consts_v17::XLOG_HEAP_INSERT,
        None,
        page_sized_delta,
    );

    let page = apply_decoded_record_block(None, &record, 0);

    assert!(
        page == Err(RedoError::UnsupportedRedoFamily {
            family: UnsupportedRedoFamily::Heap,
            info: consts_v17::XLOG_HEAP_INSERT,
            lsn: START_LSN,
        })
    );
}

#[test]
fn heap_insert_tuple_payload_is_unsupported_without_exact_tuple_reconstruction() {
    let record = decoded_record(
        consts_v17::RM_HEAP_ID,
        consts_v17::XLOG_HEAP_INSERT | consts_v17::XLOG_HEAP_INIT_PAGE,
        None,
        b"partial tuple payload".to_vec(),
    );

    let page = apply_decoded_record_block(None, &record, 0);

    assert!(
        page == Err(RedoError::UnsupportedRedoFamily {
            family: UnsupportedRedoFamily::Heap,
            info: consts_v17::XLOG_HEAP_INSERT | consts_v17::XLOG_HEAP_INIT_PAGE,
            lsn: START_LSN,
        })
    );
}

#[test]
fn heap2_visible_sets_all_visible_flag_and_advances_lsn() {
    let mut before_bytes = page_with_fill(3);
    before_bytes[10..12].copy_from_slice(&0_u16.to_le_bytes());
    let before = PageImage::new(
        page_key(),
        Lsn(7),
        Bytes::from(page_with_lsn(before_bytes, Lsn(7))),
    )
    .expect("test page has one block");
    let record = decoded_record(
        consts_v17::RM_HEAP2_ID,
        consts_v17::XLOG_HEAP2_VISIBLE,
        None,
        Vec::new(),
    );

    let page = apply_decoded_record_block(Some(before), &record, 0);

    assert!(let Ok(page) = page);
    let mut expected = page_with_fill(3);
    expected[0..8].copy_from_slice(&END_LSN.0.to_le_bytes());
    expected[10..12].copy_from_slice(&0x0004_u16.to_le_bytes());
    assert!(page.bytes().as_ref() == expected.as_slice());
}

#[test]
fn btree_page_sized_delta_payload_is_family_unsupported() {
    let record = decoded_record(
        consts_v17::RM_BTREE_ID,
        consts_v17::XLOG_BTREE_INSERT_LEAF,
        None,
        page_with_fill(0xB7),
    );

    let page = apply_decoded_record_block(None, &record, 0);

    assert!(
        page == Err(RedoError::UnsupportedRedoFamily {
            family: UnsupportedRedoFamily::Btree,
            info: consts_v17::XLOG_BTREE_INSERT_LEAF,
            lsn: START_LSN,
        })
    );
}

#[test]
fn sequence_page_sized_delta_payload_replaces_page_bytes() {
    let decoded_page = page_with_fill(0x5E);
    let expected = PageImage::new(
        page_key(),
        END_LSN,
        Bytes::from(page_with_lsn(decoded_page.clone(), END_LSN)),
    )
    .expect("test page has one block");
    let record = decoded_record(
        consts_v17::RM_SEQ_ID,
        consts_v17::XLOG_SEQ_LOG,
        None,
        decoded_page,
    );

    let page = apply_decoded_record_block(None, &record, 0);

    assert!(page == Ok(expected));
}

#[test]
fn btree_empty_delta_payload_is_family_unsupported() {
    let record = decoded_record(
        consts_v17::RM_BTREE_ID,
        consts_v17::XLOG_BTREE_INSERT_LEAF,
        None,
        Vec::new(),
    );

    let page = apply_decoded_record_block(None, &record, 0);

    assert!(
        page == Err(RedoError::UnsupportedRedoFamily {
            family: UnsupportedRedoFamily::Btree,
            info: consts_v17::XLOG_BTREE_INSERT_LEAF,
            lsn: START_LSN,
        })
    );
}

#[test]
fn sequence_non_page_sized_delta_payload_is_bad_record() {
    let record = decoded_record(
        consts_v17::RM_SEQ_ID,
        consts_v17::XLOG_SEQ_LOG,
        None,
        Vec::new(),
    );

    let page = apply_decoded_record_block(None, &record, 0);

    assert!(
        page == Err(RedoError::BadRecord {
            lsn: START_LSN,
            context: "sequence log record does not carry an exact full-page payload",
        })
    );
}

#[test]
fn btree_and_sequence_page_image_paths_are_hash_stable() {
    for (rmid, info, fill) in [
        (
            consts_v17::RM_BTREE_ID,
            consts_v17::XLOG_BTREE_INSERT_LEAF,
            0xB7,
        ),
        (consts_v17::RM_SEQ_ID, consts_v17::XLOG_SEQ_LOG, 0x5E),
    ] {
        let standby = page_with_fill(fill);
        let expected = PageImage::new(
            page_key(),
            END_LSN,
            Bytes::from(page_with_lsn(standby.clone(), END_LSN)),
        )
        .expect("test page has one block");
        let record = decoded_record(rmid, info, Some(standby), Vec::new());

        let page = apply_decoded_record_block(None, &record, 0);

        assert!(let Ok(page) = page);
        assert!(deterministic_page_hash(&page) == deterministic_page_hash(&expected));
    }
}

#[test]
fn index_family_full_page_images_replace_page_bytes() {
    for (rmid, fill) in index_families().map(|(rmid, _, fill)| (rmid, fill)) {
        let standby = page_with_fill(fill);
        let expected = PageImage::new(
            page_key(),
            END_LSN,
            Bytes::from(page_with_lsn(standby.clone(), END_LSN)),
        )
        .expect("test page has one block");
        let record = decoded_record(rmid, 0x10, Some(standby), Vec::new());

        let page = apply_decoded_record_block(None, &record, 0);

        assert!(page == Ok(expected));
    }
}

#[test]
fn index_family_delta_records_are_explicitly_unsupported_by_family() {
    for (rmid, family, _) in index_families() {
        let record = decoded_record(rmid, 0x10, None, Vec::new());

        let page = apply_decoded_record_block(None, &record, 0);

        assert!(
            page == Err(RedoError::UnsupportedRedoFamily {
                family: UnsupportedRedoFamily::Index(family),
                info: 0x10,
                lsn: START_LSN,
            })
        );
    }
}

#[test]
fn unsupported_heap_opcode_refuses_loudly_instead_of_skipping() {
    let record = decoded_record(consts_v17::RM_HEAP_ID, 0x20, None, Vec::new());

    let page = apply_decoded_record_block(None, &record, 0);

    assert!(
        page == Err(RedoError::UnsupportedRedoFamily {
            family: UnsupportedRedoFamily::Heap,
            info: 0x20,
            lsn: START_LSN,
        })
    );
}

#[test]
fn unsupported_heap2_opcode_refuses_loudly_by_family() {
    let record = decoded_record(consts_v17::RM_HEAP2_ID, 0x20, None, Vec::new());

    let page = apply_decoded_record_block(None, &record, 0);

    assert!(
        page == Err(RedoError::UnsupportedRedoFamily {
            family: UnsupportedRedoFamily::Heap2,
            info: 0x20,
            lsn: START_LSN,
        })
    );
}

#[test]
fn unsupported_sequence_opcode_refuses_loudly_by_family() {
    let record = decoded_record(consts_v17::RM_SEQ_ID, 0x10, None, Vec::new());

    let page = apply_decoded_record_block(None, &record, 0);

    assert!(
        page == Err(RedoError::UnsupportedRedoFamily {
            family: UnsupportedRedoFamily::Sequence,
            info: 0x10,
            lsn: START_LSN,
        })
    );
}

#[test]
fn pg17_manifest_declares_real_standby_oracle_gate_shape() {
    let manifest = pg17_fixture_manifest();

    assert!(manifest.pg_major == 17);
    assert_contains_all(&manifest.required_oracles, REQUIRED_STANDBY_ORACLES);
    match manifest.corpus.as_str() {
        "real-pg17-standby-oracle" => {
            assert!(manifest.provenance == "real-postgresql-17");
            assert!(manifest.capture_lsn.is_some_and(|lsn| lsn > Lsn(0)));
            assert!(manifest.missing_plan_oracles.is_none());
            for required_oracle in REQUIRED_STANDBY_ORACLES {
                assert!(manifest.file_or_directory_declared(required_oracle));
            }
        }
        "synthetic-nonempty-records" => {
            let Some(missing_plan_oracles) = manifest.missing_plan_oracles.as_ref() else {
                panic!("synthetic fixture manifest must declare missing_plan_oracles");
            };
            assert_contains_all(
                missing_plan_oracles,
                MISSING_STANDBY_ORACLES_IN_SYNTHETIC_CORPUS,
            );
        }
        other => panic!("unsupported PG17 fixture corpus {other}"),
    }
}

#[test]
#[ignore = "requires an external PG17 standby oracle corpus; in-repo tests cover synthetic FPI and loud-refusal gates only"]
fn real_pg17_standby_oracle_corpus_is_available_for_byte_gate() {
    let Some(raw_corpus) = env::var_os("CRABKA_PG17_STANDBY_ORACLE") else {
        panic!("set CRABKA_PG17_STANDBY_ORACLE to the external PG17 standby oracle root");
    };
    let requested_corpus = PathBuf::from(raw_corpus);
    let corpus = resolve_standby_oracle_path(&requested_corpus);

    for required_oracle in REQUIRED_STANDBY_ORACLES {
        assert!(corpus.join(required_oracle).exists());
    }
}

fn resolve_standby_oracle_path(requested_corpus: &Path) -> PathBuf {
    if requested_corpus.is_absolute() {
        return requested_corpus.to_path_buf();
    }

    for candidate in standby_oracle_candidates(requested_corpus) {
        if is_standby_oracle_root(&candidate) {
            return candidate;
        }
    }

    requested_corpus.to_path_buf()
}

fn standby_oracle_candidates(requested_corpus: &Path) -> Vec<PathBuf> {
    let mut candidates = Vec::new();
    if let Ok(current_dir) = env::current_dir() {
        candidates.push(current_dir.join(requested_corpus));
    }

    let package_dir = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    candidates.push(package_dir.join(requested_corpus));
    candidates.push(workspace_dir().join(requested_corpus));
    candidates
}

fn workspace_dir() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .and_then(Path::parent)
        .expect("postgres-redo crate lives under workspace/crates/postgres-redo")
        .to_path_buf()
}

fn is_standby_oracle_root(path: &Path) -> bool {
    REQUIRED_STANDBY_ORACLES
        .iter()
        .all(|required_oracle| path.join(required_oracle).exists())
}

fn pg17_fixture_manifest() -> Pg17FixtureManifest {
    let table = PG17_FIXTURE_MANIFEST
        .parse::<toml::Table>()
        .expect("PG17 fixture manifest is valid TOML");

    Pg17FixtureManifest {
        pg_major: toml_integer(&table, "pg_major"),
        corpus: toml_string(&table, "corpus"),
        provenance: toml_string(&table, "provenance"),
        capture_lsn: optional_toml_lsn(&table, "capture_lsn"),
        required_oracles: toml_string_array(&table, "required_oracles"),
        missing_plan_oracles: optional_toml_string_array(&table, "missing_plan_oracles"),
        files: toml_manifest_file_paths(&table),
    }
}

#[derive(Debug, Eq, PartialEq)]
struct Pg17FixtureManifest {
    pg_major: i64,
    corpus: String,
    provenance: String,
    capture_lsn: Option<Lsn>,
    required_oracles: Vec<String>,
    missing_plan_oracles: Option<Vec<String>>,
    files: Vec<String>,
}

impl Pg17FixtureManifest {
    fn file_or_directory_declared(&self, required_path: &str) -> bool {
        let required_directory = format!("{required_path}/");

        self.files
            .iter()
            .any(|file| file == required_path || file.starts_with(&required_directory))
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

fn optional_toml_lsn(table: &toml::Table, key: &str) -> Option<Lsn> {
    let raw_lsn = table.get(key)?.as_str().unwrap_or_else(|| {
        panic!("manifest key {key} must be an LSN string");
    });

    Some(
        raw_lsn
            .parse::<Lsn>()
            .unwrap_or_else(|source| panic!("invalid manifest LSN {raw_lsn}: {source}")),
    )
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

fn toml_manifest_file_paths(table: &toml::Table) -> Vec<String> {
    let Some(files) = toml_value(table, "files").as_array() else {
        panic!("manifest key files must be an array of tables");
    };

    files
        .iter()
        .map(|entry| {
            let Some(table) = entry.as_table() else {
                panic!("manifest files entry must be a table");
            };

            toml_string(table, "path")
        })
        .collect()
}

fn assert_contains_all(actual_values: &[String], expected_values: &[&str]) {
    for expected_value in expected_values {
        assert!(actual_values.iter().any(|actual| actual == expected_value));
    }
}

fn decoded_record(rmid: u8, info: u8, image: Option<Vec<u8>>, data: Vec<u8>) -> DecodedRecord {
    decoded_record_with_image_apply_and_data(rmid, info, image, true, data)
}

fn decoded_record_with_image_apply(
    rmid: u8,
    info: u8,
    image: Option<Vec<u8>>,
    apply: bool,
) -> DecodedRecord {
    decoded_record_with_image_apply_and_data(rmid, info, image, apply, Vec::new())
}

fn decoded_record_with_image_apply_and_data(
    rmid: u8,
    info: u8,
    image: Option<Vec<u8>>,
    apply: bool,
    data: Vec<u8>,
) -> DecodedRecord {
    DecodedRecord {
        start_lsn: START_LSN,
        total_len: TOTAL_LEN,
        header: XLogRecordHeader {
            total_len: TOTAL_LEN,
            xid: 0,
            prev_lsn: Lsn(0),
            info,
            rmid,
            crc: 0,
        },
        blocks: vec![BlockRef {
            id: 0,
            fork: 0,
            flags: BlockFlags { raw: 0 },
            rel: rel(),
            blkno: 4,
            image: image.map(|bytes| BlockImage {
                bytes: bytes.into_boxed_slice(),
                hole_offset: None,
                apply,
            }),
            data: data.into_boxed_slice(),
        }],
        main_data: Box::default(),
        origin: None,
        toplevel_xid: None,
    }
}

fn supported_full_page_image_rmgrs() -> impl Iterator<Item = (u8, u8)> {
    [
        (consts_v17::RM_XLOG_ID, consts_v17::XLOG_FPI),
        (consts_v17::RM_HEAP_ID, 0x20),
        (consts_v17::RM_HEAP2_ID, consts_v17::XLOG_HEAP2_VISIBLE),
        (consts_v17::RM_BTREE_ID, consts_v17::XLOG_BTREE_INSERT_LEAF),
        (consts_v17::RM_SEQ_ID, consts_v17::XLOG_SEQ_LOG),
        (consts_v17::RM_HASH_ID, 0x10),
        (consts_v17::RM_GIN_ID, 0x10),
        (consts_v17::RM_GIST_ID, 0x10),
        (consts_v17::RM_SPGIST_ID, 0x10),
        (consts_v17::RM_BRIN_ID, 0x10),
    ]
    .into_iter()
}

fn rel() -> RelFileLocator {
    RelFileLocator {
        spc_oid: 1663,
        db_oid: 5,
        rel_number: 16_384,
    }
}

fn page_key() -> PageKey {
    PageKey::new(1663, 5, 16_384, 0, 4)
}

fn index_families() -> impl Iterator<Item = (u8, IndexRedoFamily, u8)> {
    [
        (consts_v17::RM_HASH_ID, IndexRedoFamily::Hash, 0x48),
        (consts_v17::RM_GIN_ID, IndexRedoFamily::Gin, 0x13),
        (consts_v17::RM_GIST_ID, IndexRedoFamily::Gist, 0x47),
        (consts_v17::RM_SPGIST_ID, IndexRedoFamily::SpGist, 0x51),
        (consts_v17::RM_BRIN_ID, IndexRedoFamily::Brin, 0xB1),
    ]
    .into_iter()
}

fn page_with_fill(fill: u8) -> Vec<u8> {
    vec![fill; PAGE_SIZE]
}

fn page_with_lsn(mut page: Vec<u8>, lsn: Lsn) -> Vec<u8> {
    page[0..8].copy_from_slice(&lsn.0.to_le_bytes());
    page
}
