use std::{
    path::{Path, PathBuf},
    sync::Arc,
};

use assert2::assert;
use bytes::Bytes;
use crabka_object_store::ObjectStoreClient;
use crabka_page_store::{
    LayerMap, OpenLayer, PAGE_SIZE, PageKey, TenantId, TimelineId, TimelinePath, Value,
};
use crabka_postgres_wal::{Lsn, Sharded, WalStreamDecoder, shard_record};
use object_store::memory::InMemory;

type FpiFixture = (PageKey, Lsn, Bytes);

#[tokio::test]
async fn fixture_fpi_images_round_trip_byte_exactly_and_reingest_is_idempotent() {
    let ops = ObjectStoreClient::new(Arc::new(InMemory::new()));
    let timeline = timeline();
    let mut open = OpenLayer::builder(timeline.clone()).build();
    let fixture = fixture_corpus();
    let mut fpis = Vec::new();
    let mut max_lsn = ingest_fixture_segments_and_collect_fpis(&fixture, &mut open, &mut fpis)
        .max(fixture.capture_lsn);
    let synthetic_fpi_key = PageKey::new(9_999, 9_999, 9_999, 0, 0);
    let synthetic_fpi_lsn = Lsn(max_lsn.value() + 8);
    let synthetic_fpi = Bytes::from(vec![0x5a; PAGE_SIZE]);
    open.put_value(
        synthetic_fpi_key,
        synthetic_fpi_lsn,
        Value::image(synthetic_fpi.clone()).expect("synthetic FPI is one page"),
    )
    .expect("synthetic FPI ingests");
    fpis.push((synthetic_fpi_key, synthetic_fpi_lsn, synthetic_fpi));
    let synthetic_key = PageKey::new(9_999, 9_999, 9_999, 0, 1);
    let synthetic_delta_lsn = Lsn(max_lsn.value() + 16);
    open.put_value(
        synthetic_key,
        synthetic_delta_lsn,
        Value::Wal {
            will_init: true,
            rec: Bytes::from_static(b"synthetic-init"),
        },
    )
    .expect("synthetic delta ingests");
    max_lsn = synthetic_delta_lsn;
    open.flush(&ops, max_lsn)
        .await
        .expect("fixture flush succeeds")
        .expect("fixture produced a layer");

    let map = LayerMap::rebuild(&ops, timeline.clone())
        .await
        .expect("map rebuilds from fixture layer");
    for (key, lsn, image) in &fpis {
        let rd = map
            .get_reconstruct_data(&ops, *key, *lsn)
            .await
            .expect("FPI has reconstruct data");
        assert!(rd.base == Some((*lsn, image.clone())));
        assert!(rd.deltas.is_empty());
        assert!(image.len() == PAGE_SIZE);
    }
    let synthetic = map
        .get_reconstruct_data(&ops, synthetic_key, max_lsn)
        .await
        .expect("synthetic delta-only chain reconstructs structurally");
    assert!(synthetic.base.is_none());
    assert!(synthetic.deltas == vec![(max_lsn, Bytes::from_static(b"synthetic-init"))]);

    let before = object_names(&ops).await;
    ingest_fixture_segments(&fixture, &mut open);
    open.flush(&ops, max_lsn)
        .await
        .expect("idempotent flush succeeds");
    let after = object_names(&ops).await;

    assert!(after == before);
}

fn ingest_fixture_segments(fixture: &FixtureCorpus, open: &mut OpenLayer) -> Lsn {
    let mut fpis = Vec::new();

    ingest_fixture_segments_and_collect_fpis(fixture, open, &mut fpis)
}

fn ingest_fixture_segments_and_collect_fpis(
    fixture: &FixtureCorpus,
    open: &mut OpenLayer,
    fpis: &mut Vec<FpiFixture>,
) -> Lsn {
    let mut wal_decoder = WalStreamDecoder::new(fixture.first_segment_lsn());
    let mut max_lsn = fixture.first_segment_lsn();

    for segment in &fixture.segments {
        let bytes = std::fs::read(&segment.path).expect("fixture segment reads");
        let bytes_len = u64::try_from(bytes.len()).expect("fixture segment length fits in u64");
        let committed_len = segment.committed_len(fixture.wal_segsize, fixture.capture_lsn);

        assert!(bytes_len == fixture.wal_segsize);
        if committed_len == 0 {
            continue;
        }
        wal_decoder
            .feed(segment.base_lsn, &bytes[..committed_len])
            .expect("fixture committed WAL prefix feeds at manifest-derived segment LSN");
        while let Some(record) = wal_decoder.poll_record().expect("fixture WAL decodes") {
            max_lsn = max_lsn.max(record.start_lsn);
            let decoded_record = Arc::new(record.decode().expect("fixture record body decodes"));
            for item in shard_record(Arc::clone(&decoded_record)) {
                collect_fpi(&item, fpis);
                open.ingest_sharded(item).expect("fixture shard ingests");
            }
        }
    }

    max_lsn
}

fn collect_fpi(item: &Sharded, fpis: &mut Vec<FpiFixture>) {
    if let Sharded::Page {
        key,
        lsn,
        blk_idx,
        rec,
    } = item
        && let Some(image) = rec.blocks[*blk_idx].image.as_ref()
    {
        fpis.push((
            page_key_from_wal(*key),
            *lsn,
            Bytes::copy_from_slice(image.as_ref()),
        ));
    }
}

async fn object_names(ops: &dyn crabka_object_store::ObjectOps) -> Vec<String> {
    let mut names = ops
        .list(None)
        .await
        .expect("object listing succeeds")
        .into_iter()
        .map(|meta| meta.location.to_string())
        .collect::<Vec<_>>();
    names.sort();
    names
}

fn timeline() -> TimelinePath {
    TimelinePath::new(
        TenantId::parse("tenant").expect("tenant id is valid"),
        TimelineId::parse("timeline").expect("timeline id is valid"),
    )
}

fn page_key_from_wal(key: crabka_postgres_wal::PageKey) -> PageKey {
    let rel = key.0;
    PageKey::new(rel.spc_oid, rel.db_oid, rel.rel_number, rel.fork, key.1)
}

#[derive(Debug, Eq, PartialEq)]
struct FixtureCorpus {
    wal_segsize: u64,
    capture_lsn: Lsn,
    segments: Vec<FixtureSegment>,
}

impl FixtureCorpus {
    fn first_segment_lsn(&self) -> Lsn {
        self.segments
            .first()
            .expect("fixture manifest lists at least one WAL segment")
            .base_lsn
    }

    fn last_segment_end_lsn(&self) -> Lsn {
        let last_segment = self
            .segments
            .last()
            .expect("fixture manifest lists at least one WAL segment");

        Lsn(last_segment
            .base_lsn
            .value()
            .checked_add(self.wal_segsize)
            .expect("fixture WAL range end fits in u64"))
    }

    fn assert_capture_lsn_is_committed(&self) {
        assert_contiguous_wal_segments(&self.segments, self.wal_segsize);

        let first_segment_lsn = self.first_segment_lsn();
        let last_segment_end_lsn = self.last_segment_end_lsn();
        assert!(first_segment_lsn < self.capture_lsn);
        assert!(self.capture_lsn <= last_segment_end_lsn);
    }
}

#[derive(Debug, Eq, PartialEq)]
struct FixtureSegment {
    path: PathBuf,
    base_lsn: Lsn,
}

impl FixtureSegment {
    fn end_lsn(&self, wal_segsize: u64) -> Lsn {
        Lsn(self
            .base_lsn
            .value()
            .checked_add(wal_segsize)
            .expect("fixture WAL segment end fits in u64"))
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

fn fixture_corpus() -> FixtureCorpus {
    let table = std::fs::read_to_string(fixture_path("manifest.toml"))
        .expect("fixture manifest reads")
        .parse::<toml::Table>()
        .expect("fixture manifest is valid TOML");
    let wal_segsize = parse_wal_segsize(&toml_string(&table, "wal_segsize"));
    let wal_segment_count = toml_usize(&table, "wal_segment_count");
    let mut segments = fixture_segments(&table, wal_segsize);
    segments.sort_by_key(|segment| segment.base_lsn);

    let fixture = FixtureCorpus {
        wal_segsize,
        capture_lsn: parse_lsn(&toml_string(&table, "capture_lsn")),
        segments,
    };

    assert!(fixture.segments.len() == wal_segment_count);
    fixture.assert_capture_lsn_is_committed();
    fixture
}

fn fixture_segments(table: &toml::Table, wal_segsize: u64) -> Vec<FixtureSegment> {
    let Some(files) = toml_value(table, "files").as_array() else {
        panic!("manifest key files must be an array of tables");
    };

    files
        .iter()
        .filter_map(|entry| {
            let table = entry.as_table()?;
            let path = toml_string(table, "path");
            is_wal_manifest_path(&path).then(|| FixtureSegment {
                path: fixture_path(&path),
                base_lsn: segment_base_lsn_from_filename(&path, wal_segsize),
            })
        })
        .collect()
}

fn fixture_path(relative_path: &str) -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("../postgres-wal/tests/fixtures")
        .join(relative_path)
}

fn is_wal_manifest_path(path: &str) -> bool {
    Path::new(path)
        .extension()
        .is_some_and(|extension| extension.eq_ignore_ascii_case("wal"))
}

fn assert_contiguous_wal_segments(segments: &[FixtureSegment], wal_segsize: u64) {
    for adjacent_segments in segments.windows(2) {
        let expected_next_lsn = Lsn(adjacent_segments[0]
            .base_lsn
            .value()
            .checked_add(wal_segsize)
            .expect("fixture WAL segment end fits in u64"));

        assert!(adjacent_segments[1].base_lsn == expected_next_lsn);
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

fn parse_hex_u64(raw: &str, label: &str) -> u64 {
    u64::from_str_radix(raw, 16).unwrap_or_else(|source| panic!("invalid {label} {raw}: {source}"))
}
