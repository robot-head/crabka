use std::collections::{BTreeMap, BTreeSet};

use assert2::assert;
use crabka_page_store::{PAGE_SIZE, PageKey};
use crabka_postgres_redo::{
    IndexRedoFamily, MetadataState, RedoError, RedoOpcodeFamily, RedoRelMetaKey, SlruKey, SlruKind,
    UnsupportedRedoFamily, apply_decoded_metadata_update, apply_decoded_record_block, consts_v17,
};
use crabka_postgres_wal::{
    BlockFlags, BlockImage, BlockRef, DecodedRecord, Lsn, RelFileLocator, XLogRecordHeader,
};

const MANIFEST: &str = include_str!("fixtures/decoder_model_blockers.toml");
const START_LSN: Lsn = Lsn(0x0200_0000);
const SUPPORTED_METADATA_ARMS: [&str; 3] = [
    "slru_zero_truncate_clog_status_metadata",
    "relmap_exact_payload_metadata",
    "seq_log_full_page_payload",
];
const CANONICAL_RELATION_DELTA_BLOCKER_PROBES: [&str; 4] = ["heap", "heap2", "btree", "sequence"];
const CANONICAL_INDEX_DELTA_BLOCKER_PROBES: [&str; 5] = ["hash", "gin", "gist", "spgist", "brin"];
const CANONICAL_TRANSACTION_BLOCKER_PROBES: [&str; 1] = ["xact_unknown"];
const CANONICAL_METADATA_DELTA_BLOCKER_PROBES: [&str; 1] = ["multixact_create_id_opcode"];

#[test]
fn blocker_manifest_is_strict_toml_with_known_shape() {
    let manifest = blocker_manifest();

    assert!(manifest.pg_major == 17);
    assert!(manifest.status == "strict-audit-decoder-model-blockers");
    assert!(
        manifest.implemented_exact_in_repo
            == BTreeSet::from([
                "heap2_visible_pd_all_visible".to_owned(),
                "heap_init_empty_zero_page".to_owned(),
                "relmap_exact_payload_metadata".to_owned(),
                "rmgr_fpi".to_owned(),
                "seq_log_full_page_payload".to_owned(),
                "slru_zero_truncate_clog_status_metadata".to_owned(),
                "xlog_fpi".to_owned(),
            ])
    );
}

#[test]
fn manifest_blockers_are_derived_from_observed_unsupported_family_behavior() {
    let manifest = blocker_manifest();

    assert!(observed_relation_delta_blockers() == manifest.unsupported_relation_delta_families);
    assert!(observed_index_delta_blockers() == manifest.unsupported_long_tail_index_families);
    assert!(observed_transaction_blockers() == manifest.unsupported_transaction_opcodes);
    assert!(observed_metadata_delta_blockers() == manifest.unsupported_metadata_delta_families);
}

#[test]
fn manifest_declares_decoder_fields_and_oracles_needed_to_unblock_redo() {
    let manifest = blocker_manifest();

    assert!(
        manifest.required_decoder_model_fields
            == BTreeSet::from([
                "brin_revmap_summary_payloads".to_owned(),
                "btree_item_split_delete_payloads".to_owned(),
                "gin_pending_posting_tree_payloads".to_owned(),
                "gist_split_page_payloads".to_owned(),
                "hash_bucket_split_pageopaque_payloads".to_owned(),
                "heap2_prune_visibility_map_payloads".to_owned(),
                "heap_tuple_headers_and_offsets".to_owned(),
                "multixact_create_id_member_payloads".to_owned(),
                "spgist_tuple_node_payloads".to_owned(),
            ])
    );
    assert!(
        manifest.oracle_prerequisites
            == BTreeSet::from([
                "pg_waldump_for_corpus".to_owned(),
                "real_postgresql_17_wal_corpus".to_owned(),
                "standby_pg_multixact_segments_at_capture_lsn".to_owned(),
                "standby_pg_xact_segments_at_capture_lsn".to_owned(),
                "standby_relation_bytes_at_capture_lsn".to_owned(),
            ])
    );
    assert!(
        manifest.unsupported_metadata_delta_families
            == BTreeSet::from(["multixact_create_id_opcode".to_owned()])
    );
}

#[test]
fn sequence_manifest_distinguishes_bad_log_payloads_from_unsupported_opcodes() {
    let manifest = blocker_manifest();
    let bad_log_record = decoded_record(
        consts_v17::RM_SEQ_ID,
        consts_v17::XLOG_SEQ_LOG,
        None,
        vec![0xA5; PAGE_SIZE - 1],
    );

    let error = apply_decoded_record_block(None, &bad_log_record, 0);

    assert!(
        error
            == Err(RedoError::BadRecord {
                lsn: START_LSN,
                context: "sequence log record does not carry an exact full-page payload",
            })
    );
    assert!(
        manifest
            .unsupported_relation_delta_families
            .contains("sequence")
    );
    assert!(
        !manifest
            .all_blocker_entries()
            .contains("seq_log_non_page_payload")
    );
}

#[test]
fn supported_metadata_examples_are_not_manifest_blockers_and_apply_successfully() {
    let manifest = blocker_manifest();
    for supported_arm in SUPPORTED_METADATA_ARMS {
        assert!(!manifest.all_blocker_entries().contains(supported_arm));
        assert!(manifest.implemented_exact_in_repo.contains(supported_arm));
    }

    let mut state = MetadataState::default();
    apply_metadata_record(
        &mut state,
        &metadata_record(consts_v17::RM_CLOG_ID, 0x00, zero_page_data(7)),
    );
    assert!(state.slru_page(slru_key(SlruKind::Clog, 7)).is_some());

    apply_metadata_record(
        &mut state,
        &xact_status_record(consts_v17::XLOG_XACT_HAS_INFO, 100),
    );
    assert!(state.slru_page(slru_key(SlruKind::Clog, 0)).is_some());

    apply_metadata_record(
        &mut state,
        &metadata_record(
            consts_v17::RM_RELMAP_ID,
            0x00,
            relmap_data(5, 1663, b"relmap-bytes"),
        ),
    );
    assert!(let Some(relmap_bytes) = state.relmeta_value(RedoRelMetaKey::relmap(5, 1663)));
    assert!(relmap_bytes.as_ref() == b"relmap-bytes");
}

#[test]
fn full_page_images_remain_the_only_exact_path_for_long_tail_index_families() {
    for case in index_delta_cases() {
        let record = decoded_record(case.rmid, 0x10, Some(vec![0xA5; PAGE_SIZE]), Vec::new());

        let page = apply_decoded_record_block(None, &record, 0);

        assert!(let Ok(page) = page);
        assert!(page.key() == page_key());
    }
}

fn observed_relation_delta_blockers() -> BTreeSet<String> {
    CANONICAL_RELATION_DELTA_BLOCKER_PROBES
        .iter()
        .map(|family| observed_relation_delta_blocker(family))
        .collect()
}

fn observed_relation_delta_blocker(family: &str) -> String {
    let case = relation_delta_case(family);
    let record = decoded_record(case.rmid, case.info, None, Vec::new());

    let error = apply_decoded_record_block(None, &record, 0);

    assert!(
        error
            == Err(RedoError::UnsupportedRedoFamily {
                family: case.expected_family,
                info: case.info,
                lsn: START_LSN,
            })
    );
    unsupported_family_name(case.expected_family)
}

fn observed_index_delta_blockers() -> BTreeSet<String> {
    CANONICAL_INDEX_DELTA_BLOCKER_PROBES
        .iter()
        .map(|family| observed_index_delta_blocker(family))
        .collect()
}

fn observed_index_delta_blocker(family: &str) -> String {
    let case = index_delta_case(family);
    let record = decoded_record(case.rmid, 0x10, None, Vec::new());

    let error = apply_decoded_record_block(None, &record, 0);

    assert!(
        error
            == Err(RedoError::UnsupportedRedoFamily {
                family: UnsupportedRedoFamily::Index(case.family),
                info: 0x10,
                lsn: START_LSN,
            })
    );
    index_family_name(case.family).to_owned()
}

fn observed_transaction_blockers() -> BTreeSet<String> {
    CANONICAL_TRANSACTION_BLOCKER_PROBES
        .iter()
        .map(|opcode| observed_transaction_blocker(opcode))
        .collect()
}

fn observed_transaction_blocker(opcode: &str) -> String {
    let info = transaction_opcode_info(opcode);
    let record = metadata_record(consts_v17::RM_XACT_ID, info, Box::default());
    let mut state = MetadataState::default();

    let error = apply_decoded_metadata_update(&mut state, &record);

    assert!(
        error
            == Err(RedoError::UnsupportedRedoFamily {
                family: UnsupportedRedoFamily::Transaction,
                info,
                lsn: START_LSN,
            })
    );
    opcode.to_owned()
}

fn observed_metadata_delta_blockers() -> BTreeSet<String> {
    CANONICAL_METADATA_DELTA_BLOCKER_PROBES
        .iter()
        .map(|delta| observed_metadata_delta_blocker(delta))
        .collect()
}

fn observed_metadata_delta_blocker(delta: &str) -> String {
    let info = metadata_delta_info(delta);
    let record = metadata_record(
        consts_v17::RM_MULTIXACT_ID,
        info,
        multixact_create_id_data(),
    );
    let mut state = MetadataState::default();

    let error = apply_decoded_metadata_update(&mut state, &record);

    assert!(
        error
            == Err(RedoError::UnsupportedRedoOpcode {
                family: RedoOpcodeFamily::Slru(SlruKind::MultiXactMember),
                opcode: info,
                lsn: START_LSN,
            })
    );
    delta.to_owned()
}

fn apply_metadata_record(state: &mut MetadataState, record: &DecodedRecord) {
    let result = apply_decoded_metadata_update(state, record);

    assert!(result == Ok(()));
}

#[derive(Debug, Clone, Copy)]
struct RelationDeltaCase {
    rmid: u8,
    info: u8,
    expected_family: UnsupportedRedoFamily,
}

fn relation_delta_case(family: &str) -> RelationDeltaCase {
    match family {
        "heap" => RelationDeltaCase {
            rmid: consts_v17::RM_HEAP_ID,
            info: consts_v17::XLOG_HEAP_INSERT,
            expected_family: UnsupportedRedoFamily::Heap,
        },
        "heap2" => RelationDeltaCase {
            rmid: consts_v17::RM_HEAP2_ID,
            info: 0x20,
            expected_family: UnsupportedRedoFamily::Heap2,
        },
        "btree" => RelationDeltaCase {
            rmid: consts_v17::RM_BTREE_ID,
            info: consts_v17::XLOG_BTREE_INSERT_LEAF,
            expected_family: UnsupportedRedoFamily::Btree,
        },
        "sequence" => RelationDeltaCase {
            rmid: consts_v17::RM_SEQ_ID,
            info: 0x10,
            expected_family: UnsupportedRedoFamily::Sequence,
        },
        _ => panic!("unsupported relation delta manifest family {family}"),
    }
}

#[derive(Debug, Clone, Copy)]
struct IndexDeltaCase {
    rmid: u8,
    family: IndexRedoFamily,
}

fn index_delta_cases() -> impl Iterator<Item = IndexDeltaCase> {
    CANONICAL_INDEX_DELTA_BLOCKER_PROBES
        .iter()
        .copied()
        .map(index_delta_case)
}

fn index_delta_case(family: &str) -> IndexDeltaCase {
    match family {
        "hash" => IndexDeltaCase {
            rmid: consts_v17::RM_HASH_ID,
            family: IndexRedoFamily::Hash,
        },
        "gin" => IndexDeltaCase {
            rmid: consts_v17::RM_GIN_ID,
            family: IndexRedoFamily::Gin,
        },
        "gist" => IndexDeltaCase {
            rmid: consts_v17::RM_GIST_ID,
            family: IndexRedoFamily::Gist,
        },
        "spgist" => IndexDeltaCase {
            rmid: consts_v17::RM_SPGIST_ID,
            family: IndexRedoFamily::SpGist,
        },
        "brin" => IndexDeltaCase {
            rmid: consts_v17::RM_BRIN_ID,
            family: IndexRedoFamily::Brin,
        },
        _ => panic!("unsupported index delta manifest family {family}"),
    }
}

fn transaction_opcode_info(opcode: &str) -> u8 {
    match opcode {
        "xact_unknown" => 0x50 | consts_v17::XLOG_XACT_HAS_INFO,
        _ => panic!("unsupported transaction opcode manifest entry {opcode}"),
    }
}

fn metadata_delta_info(delta: &str) -> u8 {
    match delta {
        "multixact_create_id_opcode" => 0x20,
        _ => panic!("unsupported metadata delta manifest entry {delta}"),
    }
}

fn unsupported_family_name(family: UnsupportedRedoFamily) -> String {
    match family {
        UnsupportedRedoFamily::Transaction => "transaction".to_owned(),
        UnsupportedRedoFamily::Heap => "heap".to_owned(),
        UnsupportedRedoFamily::Heap2 => "heap2".to_owned(),
        UnsupportedRedoFamily::Btree => "btree".to_owned(),
        UnsupportedRedoFamily::Sequence => "sequence".to_owned(),
        UnsupportedRedoFamily::Slru(kind) => slru_family_name(kind).to_owned(),
        UnsupportedRedoFamily::RelMeta => "relmeta".to_owned(),
        UnsupportedRedoFamily::Index(family) => index_family_name(family).to_owned(),
    }
}

fn index_family_name(family: IndexRedoFamily) -> &'static str {
    match family {
        IndexRedoFamily::Hash => "hash",
        IndexRedoFamily::Gin => "gin",
        IndexRedoFamily::Gist => "gist",
        IndexRedoFamily::SpGist => "spgist",
        IndexRedoFamily::Brin => "brin",
    }
}

fn slru_family_name(kind: SlruKind) -> &'static str {
    match kind {
        SlruKind::Clog => "clog",
        SlruKind::MultiXactOffset | SlruKind::MultiXactMember => "multixact",
        SlruKind::CommitTs => "commit_ts",
    }
}

fn decoded_record(rmid: u8, info: u8, image: Option<Vec<u8>>, data: Vec<u8>) -> DecodedRecord {
    DecodedRecord {
        start_lsn: START_LSN,
        total_len: 128,
        header: XLogRecordHeader {
            total_len: 128,
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
            rel: RelFileLocator {
                spc_oid: 1663,
                db_oid: 5,
                rel_number: 16_384,
            },
            blkno: 4,
            image: image.map(|bytes| BlockImage {
                bytes: bytes.into_boxed_slice(),
                hole_offset: None,
                apply: true,
            }),
            data: data.into_boxed_slice(),
        }],
        main_data: Box::default(),
        origin: None,
        toplevel_xid: None,
    }
}

fn metadata_record(rmid: u8, info: u8, main_data: Box<[u8]>) -> DecodedRecord {
    DecodedRecord {
        start_lsn: START_LSN,
        total_len: 128,
        header: XLogRecordHeader {
            total_len: 128,
            xid: 0,
            prev_lsn: Lsn(0),
            info,
            rmid,
            crc: 0,
        },
        blocks: Vec::new(),
        main_data,
        origin: None,
        toplevel_xid: None,
    }
}

fn xact_status_record(info: u8, xid: u32) -> DecodedRecord {
    let mut record = metadata_record(consts_v17::RM_XACT_ID, info, xact_status_main_data());
    record.header.xid = xid;
    record
}

fn zero_page_data(page_number: u32) -> Box<[u8]> {
    Box::new(page_number.to_le_bytes())
}

fn xact_status_main_data() -> Box<[u8]> {
    let mut bytes = Vec::new();
    bytes.extend_from_slice(&0_i64.to_le_bytes());
    bytes.extend_from_slice(&0_u32.to_le_bytes());
    bytes.into_boxed_slice()
}

fn multixact_create_id_data() -> Box<[u8]> {
    let mut bytes = Vec::new();
    bytes.extend_from_slice(&42_u32.to_le_bytes());
    bytes.extend_from_slice(&7_u32.to_le_bytes());
    bytes.extend_from_slice(&1_u32.to_le_bytes());
    bytes.extend_from_slice(&100_u32.to_le_bytes());
    bytes.extend_from_slice(&0_u32.to_le_bytes());
    bytes.into_boxed_slice()
}

fn relmap_data(db_oid: u32, spc_oid: u32, payload: &[u8]) -> Box<[u8]> {
    let mut bytes = Vec::new();
    bytes.extend_from_slice(&db_oid.to_le_bytes());
    bytes.extend_from_slice(&spc_oid.to_le_bytes());
    bytes.extend_from_slice(&relmap_payload_len(payload).to_le_bytes());
    bytes.extend_from_slice(payload);
    bytes.into_boxed_slice()
}

fn relmap_payload_len(payload: &[u8]) -> i32 {
    i32::try_from(payload.len()).expect("test relmap payload length fits in i32")
}

fn slru_key(kind: SlruKind, page_number: u32) -> SlruKey {
    const SLRU_PAGES_PER_SEGMENT: u32 = 32;

    SlruKey {
        kind,
        segment_number: page_number / SLRU_PAGES_PER_SEGMENT,
        block_number: page_number % SLRU_PAGES_PER_SEGMENT,
    }
}

fn page_key() -> PageKey {
    PageKey::new(1663, 5, 16_384, 0, 4)
}

#[derive(Debug, PartialEq, Eq)]
struct BlockerManifest {
    pg_major: i64,
    status: String,
    implemented_exact_in_repo: BTreeSet<String>,
    unsupported_relation_delta_families: BTreeSet<String>,
    unsupported_long_tail_index_families: BTreeSet<String>,
    unsupported_transaction_opcodes: BTreeSet<String>,
    unsupported_metadata_delta_families: BTreeSet<String>,
    required_decoder_model_fields: BTreeSet<String>,
    oracle_prerequisites: BTreeSet<String>,
}

impl BlockerManifest {
    fn all_blocker_entries(&self) -> BTreeSet<&str> {
        self.unsupported_relation_delta_families
            .iter()
            .chain(&self.unsupported_long_tail_index_families)
            .chain(&self.unsupported_transaction_opcodes)
            .chain(&self.unsupported_metadata_delta_families)
            .map(String::as_str)
            .collect()
    }
}

fn blocker_manifest() -> BlockerManifest {
    let manifest = MANIFEST
        .parse::<toml::Table>()
        .expect("blocker manifest is valid TOML");
    let mut entries = manifest.into_iter().collect::<BTreeMap<_, _>>();
    let parsed = BlockerManifest {
        pg_major: take_integer(&mut entries, "pg_major"),
        status: take_string(&mut entries, "status"),
        implemented_exact_in_repo: take_string_set(&mut entries, "implemented_exact_in_repo"),
        unsupported_relation_delta_families: take_string_set(
            &mut entries,
            "unsupported_relation_delta_families",
        ),
        unsupported_long_tail_index_families: take_string_set(
            &mut entries,
            "unsupported_long_tail_index_families",
        ),
        unsupported_transaction_opcodes: take_string_set(
            &mut entries,
            "unsupported_transaction_opcodes",
        ),
        unsupported_metadata_delta_families: take_string_set(
            &mut entries,
            "unsupported_metadata_delta_families",
        ),
        required_decoder_model_fields: take_string_set(
            &mut entries,
            "required_decoder_model_fields",
        ),
        oracle_prerequisites: take_string_set(&mut entries, "oracle_prerequisites"),
    };

    if entries.is_empty() {
        return parsed;
    }

    panic!("blocker manifest has unexpected keys: {:?}", entries.keys());
}

fn take_integer(entries: &mut BTreeMap<String, toml::Value>, key: &str) -> i64 {
    let value = take_entry(entries, key);
    let Some(integer) = value.as_integer() else {
        panic!("manifest key {key} must be an integer");
    };

    integer
}

fn take_string(entries: &mut BTreeMap<String, toml::Value>, key: &str) -> String {
    let value = take_entry(entries, key);
    let Some(string) = value.as_str() else {
        panic!("manifest key {key} must be a string");
    };

    string.to_owned()
}

fn take_string_set(entries: &mut BTreeMap<String, toml::Value>, key: &str) -> BTreeSet<String> {
    let value = take_entry(entries, key);
    let Some(array) = value.as_array() else {
        panic!("manifest key {key} must be a string array");
    };

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

fn take_entry(entries: &mut BTreeMap<String, toml::Value>, key: &str) -> toml::Value {
    entries
        .remove(key)
        .unwrap_or_else(|| panic!("manifest key {key} is present"))
}
