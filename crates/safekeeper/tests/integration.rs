use std::path::{Path, PathBuf};

use assert2::assert;
use bytes::Bytes;
use crabka_safekeeper::{
    Lsn,
    conn::{
        IdentifiedSystem, LivePostgresConnectPlan, LivePostgresTransport, ReplicationCommand,
        ReplicationCommandOutcome, ReplicationSession, ReplicationSessionError,
        ReplicationTransport, TimelineId,
    },
    frame::{WalFrame, WalFrameError},
    ingest::{
        AppendAck, DecodeGate, IngestConfig, IngestError, Result as IngestResult, SafekeeperIngest,
        WalTopic, frame_end_lsn, wal_topic_name,
    },
    protocol::{CopyBothMessage, PrimaryKeepalive, StandbyStatusUpdate, XLogData},
    topic::InMemoryWalTopic,
};

const FIXTURE_BYTES_UNDER_TEST: usize = 64 * 1024;

#[derive(Debug, Default)]
struct ScriptedTransport {
    outcomes: Vec<ReplicationCommandOutcome>,
    commands: Vec<ReplicationCommand>,
    inbound_copy_data: Vec<Vec<u8>>,
    outbound_copy_data: Vec<Vec<u8>>,
}

impl ScriptedTransport {
    fn with_messages(messages: Vec<CopyBothMessage>) -> Self {
        Self {
            outcomes: default_start_outcomes(),
            inbound_copy_data: messages
                .into_iter()
                .map(|message| message.encode())
                .collect(),
            ..Self::default()
        }
    }
}

impl ReplicationTransport for ScriptedTransport {
    fn execute_replication_command(
        &mut self,
        command: &ReplicationCommand,
    ) -> Result<ReplicationCommandOutcome, ReplicationSessionError> {
        self.commands.push(command.clone());
        if self.outcomes.is_empty() {
            return Err(ReplicationSessionError::Transport {
                message: "script exhausted".to_owned(),
            });
        }

        Ok(self.outcomes.remove(0))
    }

    fn receive_copy_data(&mut self) -> Result<Option<Vec<u8>>, ReplicationSessionError> {
        if self.inbound_copy_data.is_empty() {
            return Ok(None);
        }

        Ok(Some(self.inbound_copy_data.remove(0)))
    }

    fn send_copy_data(&mut self, bytes: &[u8]) -> Result<(), ReplicationSessionError> {
        self.outbound_copy_data.push(bytes.to_vec());
        Ok(())
    }
}

#[derive(Debug, Default, Clone)]
struct MismatchedAckTopic {
    inner: InMemoryWalTopic,
    ack_delta: u64,
}

impl WalTopic for MismatchedAckTopic {
    fn ensure_topic(&mut self, topic: &str) -> IngestResult<()> {
        self.inner.ensure_topic(topic)
    }

    fn append_frame(&mut self, topic: &str, frame: &WalFrame) -> IngestResult<AppendAck> {
        let ack = self.inner.append_frame(topic, frame)?;
        Ok(AppendAck {
            end_lsn: Lsn(ack.end_lsn.value() + self.ack_delta),
        })
    }

    fn last_frame(&self, topic: &str) -> IngestResult<Option<Vec<u8>>> {
        self.inner.last_frame(topic)
    }
}

struct PostgresWalDecodeGate {
    decoder: crabka_postgres_wal::WalStreamDecoder,
}

impl PostgresWalDecodeGate {
    fn new(base_lsn: Lsn) -> Self {
        Self {
            decoder: crabka_postgres_wal::WalStreamDecoder::new(to_decoder_lsn(base_lsn)),
        }
    }
}

impl DecodeGate for PostgresWalDecodeGate {
    fn accept_frame(&mut self, frame: &WalFrame) -> IngestResult<()> {
        self.decoder
            .feed(to_decoder_lsn(frame.lsn), &frame.payload)
            .map_err(|source| IngestError::DecodeGate {
                message: source.to_string(),
            })?;
        loop {
            let decoded = self
                .decoder
                .poll_record()
                .map_err(|source| IngestError::DecodeGate {
                    message: source.to_string(),
                })?;
            if decoded.is_none() {
                return Ok(());
            }
        }
    }
}

#[test]
fn ingest_resumes_from_last_flushed_lsn() {
    let existing_frame = WalFrame::new(Lsn(10), Bytes::from_static(b"0123456789"));
    assert!(let Ok(existing_frame) = existing_frame);
    let transport = ScriptedTransport::with_messages(vec![xlog_data(20, b"abc")]);
    let topic = InMemoryWalTopic::with_frame(&wal_topic_name("alpha"), &existing_frame)
        .expect("test topic stores frame");
    let session = ReplicationSession::new(transport, "alpha").expect("valid session");
    let mut ingest = SafekeeperIngest::new(session, topic, config("alpha"));

    let report = ingest.run_bounded();

    assert!(let Ok(report) = report);
    assert!(report.resume_lsn == Lsn(20));
    assert!(report.flushed_lsn == Lsn(23));

    let (session, topic, _) = ingest.into_parts();
    let transport = session.into_transport();
    assert!(matches!(
        &transport.commands[2],
        ReplicationCommand::StartPhysicalReplication {
            resume_lsn: Lsn(20),
            ..
        }
    ));
    assert!(topic.ensured_topics() == [wal_topic_name("alpha"), wal_topic_name("alpha")]);
}

#[test]
fn ingest_sequences_replication_commands_and_status_updates() {
    let transport = ScriptedTransport::with_messages(vec![
        xlog_data(0, b"abcd"),
        CopyBothMessage::PrimaryKeepalive(PrimaryKeepalive {
            wal_end: Lsn(4),
            send_time: 42,
            reply_requested: true,
        }),
    ]);
    let topic = InMemoryWalTopic::default();
    let session = ReplicationSession::new(transport, "alpha").expect("valid session");
    let mut ingest = SafekeeperIngest::new(session, topic, config("alpha"));

    let report = ingest.run_bounded();

    assert!(let Ok(report) = report);
    assert!(report.resume_lsn == Lsn(0));
    assert!(report.written_lsn == Lsn(4));
    assert!(report.flushed_lsn == Lsn(4));

    let (session, topic, _) = ingest.into_parts();
    let records = topic.records(&wal_topic_name("alpha"));
    assert!(records.len() == 1);
    let stored_frame = WalFrame::decode(&records[0]).expect("stored frame decodes");
    assert!(stored_frame.lsn == Lsn(0));
    assert!(stored_frame.payload.as_ref() == b"abcd");
    assert!(&records[0][..4] == b"PGW1");
    assert!(u64::from_le_bytes(records[0][4..12].try_into().expect("lsn bytes")) == 0);

    let transport = session.into_transport();
    assert!(
        transport
            .commands
            .iter()
            .map(ReplicationCommand::to_simple_query)
            .collect::<Vec<_>>()
            == vec![
                "IDENTIFY_SYSTEM".to_owned(),
                "CREATE_REPLICATION_SLOT crabka_sk_alpha PHYSICAL".to_owned(),
                "START_REPLICATION SLOT crabka_sk_alpha PHYSICAL 0/0 TIMELINE 1".to_owned(),
            ]
    );
    assert!(transport.outbound_copy_data.len() == 1);
    assert!(
        CopyBothMessage::parse(&transport.outbound_copy_data[0])
            == Ok(CopyBothMessage::StandbyStatusUpdate(StandbyStatusUpdate {
                write_lsn: Lsn(4),
                flush_lsn: Lsn(4),
                apply_lsn: Lsn(4),
                client_time: 42,
                reply_requested: false,
            }))
    );
}

#[test]
fn ingest_rejects_corrupt_tail_frame_before_starting_replication() {
    let transport = ScriptedTransport::with_messages(vec![xlog_data(0, b"abcd")]);
    let topic = InMemoryWalTopic::with_records(
        &wal_topic_name("alpha"),
        vec![b"BAD!\0\0\0\0\0\0\0\0payload".to_vec()],
    );
    let session = ReplicationSession::new(transport, "alpha").expect("valid session");
    let mut ingest = SafekeeperIngest::new(session, topic, config("alpha"));

    let result = ingest.run_bounded();

    assert!(
        result
            == Err(IngestError::WalFrame(WalFrameError::InvalidMagic {
                got: *b"BAD!"
            }))
    );
    let (session, topic, _) = ingest.into_parts();
    assert!(session.into_transport().commands.is_empty());
    assert!(topic.records(&wal_topic_name("alpha")).len() == 1);
}

#[test]
fn ingest_treats_duplicate_and_partial_overlap_as_idempotent_resume() {
    let existing_frame = WalFrame::new(Lsn(100), Bytes::from_static(b"abcdefghij"));
    assert!(let Ok(existing_frame) = existing_frame);
    let transport = ScriptedTransport::with_messages(vec![xlog_data(105, b"fghijklmno")]);
    let topic = InMemoryWalTopic::with_frame(&wal_topic_name("alpha"), &existing_frame)
        .expect("test topic stores frame");
    let session = ReplicationSession::new(transport, "alpha").expect("valid session");
    let mut ingest = SafekeeperIngest::new(session, topic, config("alpha"));

    let report = ingest.run_bounded();

    assert!(let Ok(report) = report);
    assert!(report.resume_lsn == Lsn(110));
    assert!(report.flushed_lsn == Lsn(115));

    let (_session, topic, _) = ingest.into_parts();
    let records = topic.records(&wal_topic_name("alpha"));
    assert!(records.len() == 2);
    let appended = WalFrame::decode(&records[1]).expect("appended frame decodes");
    assert!(appended.lsn == Lsn(110));
    assert!(appended.payload.as_ref() == b"klmno");
}

#[test]
fn ingest_rejects_gap_before_topic_append() {
    let existing_frame = WalFrame::new(Lsn(10), Bytes::from_static(b"abc"));
    assert!(let Ok(existing_frame) = existing_frame);
    let transport = ScriptedTransport::with_messages(vec![xlog_data(20, b"gap")]);
    let topic = InMemoryWalTopic::with_frame(&wal_topic_name("alpha"), &existing_frame)
        .expect("test topic stores frame");
    let session = ReplicationSession::new(transport, "alpha").expect("valid session");
    let mut ingest = SafekeeperIngest::new(session, topic, config("alpha"));

    let result = ingest.run_bounded();

    assert!(
        result
            == Err(IngestError::LsnGap {
                expected: Lsn(13),
                got: Lsn(20)
            })
    );
    let (_session, topic, _) = ingest.into_parts();
    assert!(topic.records(&wal_topic_name("alpha")).len() == 1);
}

#[test]
fn ingest_rejects_append_ack_that_would_overstate_last_durable_lsn() {
    let transport = ScriptedTransport::with_messages(vec![xlog_data(0, b"abcd")]);
    let topic = MismatchedAckTopic {
        inner: InMemoryWalTopic::default(),
        ack_delta: 1,
    };
    let session = ReplicationSession::new(transport, "alpha").expect("valid session");
    let mut ingest = SafekeeperIngest::new(session, topic, config("alpha"));

    let result = ingest.run_bounded();

    assert!(
        result
            == Err(IngestError::AppendAckMismatch {
                expected: Lsn(4),
                got: Lsn(5)
            })
    );
}

#[test]
fn stored_pgw1_fixture_stream_decodes_across_restart_resume_seam() {
    let fixture = first_fixture_segment();

    assert!(fixture.bytes.len() >= FIXTURE_BYTES_UNDER_TEST);
    let first_stop_offset = 21 * 1024;
    let resume_overlap_bytes = 1024;
    let mut topic = InMemoryWalTopic::default();

    let first_transport = with_identify_flush_lsn(
        ScriptedTransport::with_messages(fixture_xlog_messages(
            &fixture,
            0,
            first_stop_offset,
            3072,
        )),
        fixture.base_lsn,
    );
    let first_session = ReplicationSession::new(first_transport, "fixture").expect("valid session");
    let mut first_ingest = SafekeeperIngest::new(
        first_session,
        topic,
        fixture_config("fixture", first_stop_offset),
    );
    let first_report = first_ingest
        .run_bounded()
        .expect("first ingest run succeeds");
    assert!(first_report.resume_lsn == fixture.base_lsn);
    assert!(first_report.flushed_lsn == Lsn(fixture.base_lsn.value() + first_stop_offset as u64));
    let (_first_session, first_topic, _) = first_ingest.into_parts();
    topic = first_topic;

    let restart_offset = first_stop_offset - resume_overlap_bytes;
    let second_transport = ScriptedTransport::with_messages(fixture_xlog_messages(
        &fixture,
        restart_offset,
        FIXTURE_BYTES_UNDER_TEST,
        5000,
    ));
    let second_session =
        ReplicationSession::new(second_transport, "fixture").expect("valid session");
    let mut second_ingest = SafekeeperIngest::new(
        second_session,
        topic,
        fixture_config("fixture", FIXTURE_BYTES_UNDER_TEST - restart_offset),
    );
    let second_report = second_ingest
        .run_bounded()
        .expect("second ingest run succeeds");
    assert!(second_report.resume_lsn == first_report.flushed_lsn);
    assert!(
        second_report.flushed_lsn
            == Lsn(fixture.base_lsn.value() + FIXTURE_BYTES_UNDER_TEST as u64)
    );

    let (second_session, topic, _) = second_ingest.into_parts();
    let second_transport = second_session.into_transport();
    assert!(matches!(
        &second_transport.commands[2],
        ReplicationCommand::StartPhysicalReplication { resume_lsn, .. }
            if *resume_lsn == first_report.flushed_lsn
    ));

    let records = topic.records(&wal_topic_name("fixture"));
    let decoded_records = decode_stored_topic_records(records, fixture.base_lsn);
    let frame_lsns = records
        .iter()
        .map(|record| WalFrame::decode(record).expect("stored frame decodes").lsn)
        .collect::<Vec<_>>();

    assert!(!decoded_records.is_empty());
    assert!(decoded_records[0].start_lsn >= to_decoder_lsn(fixture.base_lsn));
    assert!(frame_lsns.iter().any(|lsn| *lsn < first_report.flushed_lsn));
    assert!(frame_lsns.contains(&first_report.flushed_lsn));
}

#[test]
fn decode_gate_failure_halts_without_appending_frame() {
    let page_len = usize::try_from(crabka_postgres_wal::XLOG_BLCKSZ)
        .expect("PostgreSQL WAL page size fits in usize");
    let bad_page = vec![0_u8; page_len];
    let transport = ScriptedTransport::with_messages(vec![CopyBothMessage::XLogData(XLogData {
        wal_start: Lsn(0),
        wal_end: Lsn(bad_page.len() as u64),
        send_time: 1,
        data: Bytes::from(bad_page),
    })]);
    let topic = InMemoryWalTopic::default();
    let session = ReplicationSession::new(transport, "alpha").expect("valid session");
    let mut ingest = SafekeeperIngest::with_decode_gate(
        session,
        topic,
        PostgresWalDecodeGate::new(Lsn(0)),
        config("alpha"),
    );

    let result = ingest.run_bounded();

    assert!(let Err(IngestError::DecodeGate { message }) = result);
    assert!(message.contains("bad PostgreSQL WAL page magic"));
    let (_session, topic, _) = ingest.into_parts();
    assert!(topic.records(&wal_topic_name("alpha")).is_empty());
}

#[test]
fn live_postgres_plan_parses_inputs_and_requires_explicit_raw_prerequisites() {
    let plan = LivePostgresConnectPlan::parse("host=localhost user=postgres", "alpha");
    assert!(let Ok(plan) = plan);
    assert!(plan.slot_name().as_str() == "crabka_sk_alpha");

    let result = LivePostgresTransport::connect(&plan);

    assert!(matches!(
        result,
        Err(ReplicationSessionError::LivePostgresPrerequisite { message })
            if message.contains("sslmode=disable")
    ));
}

#[test]
fn live_postgres_session_connect_uses_raw_transport_prerequisite_errors() {
    let missing_prerequisite = ReplicationSession::<LivePostgresTransport>::connect(
        "host=localhost user=postgres",
        "alpha",
    );
    assert!(matches!(
        missing_prerequisite,
        Err(ReplicationSessionError::LivePostgresPrerequisite { message })
            if message.contains("sslmode=disable")
    ));
}

#[test]
#[ignore = "requires a manually started PostgreSQL 17 primary with wal_level=replica, max_wal_senders>0, trust/no-password TCP auth, sslmode=disable, and CRABKA_SAFEKEEPER_PG17_REPLICATION_URL"]
fn live_postgres17_physical_replication_harness_identifies_primary_and_starts_slot() {
    let url = std::env::var("CRABKA_SAFEKEEPER_PG17_REPLICATION_URL")
        .expect("set CRABKA_SAFEKEEPER_PG17_REPLICATION_URL for the ignored live harness");
    let mut session = ReplicationSession::<LivePostgresTransport>::connect(&url, "live_pg17")
        .expect("live PG17 replication connection opens");

    let system = session.identify().expect("IDENTIFY_SYSTEM succeeds");
    session
        .ensure_slot()
        .expect("physical slot is idempotently ready");
    session
        .start(system.flush_lsn)
        .expect("START_REPLICATION enters CopyBoth");
}

#[test]
#[ignore = "requires feature=kafka, a manually started PostgreSQL 17 primary that is actively generating WAL with wal_level=replica/max_wal_senders>0/trust TCP auth/sslmode=disable, a reachable Kafka-compatible broker, CRABKA_SAFEKEEPER_PG17_REPLICATION_URL, and CRABKA_SAFEKEEPER_KAFKA_BOOTSTRAP"]
#[cfg(feature = "kafka")]
fn live_postgres17_to_kafka_topic_ingest_decodes_stored_pgw1_stream() {
    let pg_url = std::env::var("CRABKA_SAFEKEEPER_PG17_REPLICATION_URL")
        .expect("set CRABKA_SAFEKEEPER_PG17_REPLICATION_URL for the ignored live gate");
    let kafka_bootstrap = std::env::var("CRABKA_SAFEKEEPER_KAFKA_BOOTSTRAP")
        .expect("set CRABKA_SAFEKEEPER_KAFKA_BOOTSTRAP for the ignored live gate");
    let cluster = std::env::var("CRABKA_SAFEKEEPER_LIVE_CLUSTER")
        .unwrap_or_else(|_| "live_pg17_kafka".to_owned());

    let mut probe = ReplicationSession::<LivePostgresTransport>::connect(&pg_url, &cluster)
        .expect("live PG17 replication connection opens");
    let system = probe.identify().expect("IDENTIFY_SYSTEM succeeds");
    drop(probe);

    let session = ReplicationSession::<LivePostgresTransport>::connect(&pg_url, &cluster)
        .expect("live PG17 replication connection opens for ingest");
    let topic = crabka_safekeeper::topic::KafkaWalTopic::connect(
        crabka_safekeeper::topic::KafkaWalTopicConfig::new(kafka_bootstrap),
    )
    .expect("Kafka WAL topic sink connects");
    let mut ingest = SafekeeperIngest::with_decode_gate(
        session,
        topic,
        PostgresWalDecodeGate::new(system.flush_lsn),
        IngestConfig {
            cluster,
            target_frame_bytes: 64 * 1024,
            max_messages: 128,
        },
    );

    let report = ingest
        .run_bounded()
        .expect("live PG17 WAL ingests to Kafka and decodes through PGW1 gate");

    assert!(report.frames_appended > 0);
    assert!(report.flushed_lsn >= report.written_lsn);
}

#[test]
#[ignore = "enable cargo feature `kafka` plus live PG17/Kafka prerequisites to run this external gate"]
#[cfg(not(feature = "kafka"))]
fn live_postgres17_to_kafka_topic_ingest_decodes_stored_pgw1_stream() {
    panic!("rerun with `--features kafka` and the live PG17/Kafka environment variables");
}

fn config(cluster: &str) -> IngestConfig {
    IngestConfig {
        cluster: cluster.to_owned(),
        target_frame_bytes: 4,
        max_messages: 8,
    }
}

fn default_start_outcomes() -> Vec<ReplicationCommandOutcome> {
    vec![
        ReplicationCommandOutcome::IdentifiedSystem(IdentifiedSystem {
            system_id: "sys".to_owned(),
            timeline: TimelineId(1),
            flush_lsn: Lsn(0),
        }),
        ReplicationCommandOutcome::SlotAlreadyExists,
        ReplicationCommandOutcome::CopyBothStarted {
            timeline: TimelineId(1),
        },
    ]
}

fn with_identify_flush_lsn(mut transport: ScriptedTransport, flush_lsn: Lsn) -> ScriptedTransport {
    transport.outcomes[0] = ReplicationCommandOutcome::IdentifiedSystem(IdentifiedSystem {
        system_id: "sys".to_owned(),
        timeline: TimelineId(1),
        flush_lsn,
    });
    transport
}

fn xlog_data(wal_start: u64, data: &'static [u8]) -> CopyBothMessage {
    CopyBothMessage::XLogData(XLogData {
        wal_start: Lsn(wal_start),
        wal_end: Lsn(wal_start + data.len() as u64),
        send_time: 1,
        data: Bytes::from_static(data),
    })
}

#[derive(Debug, Eq, PartialEq)]
struct FixtureSegment {
    base_lsn: Lsn,
    bytes: Vec<u8>,
}

#[derive(Debug, Eq, PartialEq)]
struct ManifestWalSegment {
    path: String,
    base_lsn: Lsn,
}

fn first_fixture_segment() -> FixtureSegment {
    let table = std::fs::read_to_string(fixture_path("manifest.toml"))
        .expect("fixture manifest reads")
        .parse::<toml::Table>()
        .expect("fixture manifest is valid TOML");
    let wal_segsize = parse_wal_segsize(&toml_string(&table, "wal_segsize"));
    let segment = first_manifest_wal_segment(&table, wal_segsize);
    let bytes = std::fs::read(fixture_path(&segment.path)).expect("fixture segment reads");
    let bytes_len = u64::try_from(bytes.len()).expect("fixture segment length fits in u64");

    assert!(bytes_len == wal_segsize);
    FixtureSegment {
        base_lsn: segment.base_lsn,
        bytes,
    }
}

fn first_manifest_wal_segment(table: &toml::Table, wal_segsize: u64) -> ManifestWalSegment {
    let Some(files) = toml_value(table, "files").as_array() else {
        panic!("manifest key files must be an array of tables");
    };
    let mut segments = files
        .iter()
        .filter_map(|entry| {
            let table = entry.as_table()?;
            let path = toml_string(table, "path");
            is_wal_manifest_path(&path).then(|| ManifestWalSegment {
                base_lsn: segment_base_lsn_from_filename(&path, wal_segsize),
                path,
            })
        })
        .collect::<Vec<_>>();
    segments.sort_by_key(|segment| segment.base_lsn);

    segments
        .into_iter()
        .next()
        .expect("fixture manifest lists at least one WAL segment")
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

fn parse_hex_u64(raw: &str, label: &str) -> u64 {
    u64::from_str_radix(raw, 16).unwrap_or_else(|source| panic!("invalid {label} {raw}: {source}"))
}

fn fixture_config(cluster: &str, max_messages: usize) -> IngestConfig {
    IngestConfig {
        cluster: cluster.to_owned(),
        target_frame_bytes: 8192,
        max_messages,
    }
}

fn fixture_xlog_messages(
    fixture: &FixtureSegment,
    start_offset: usize,
    end_offset: usize,
    chunk_size: usize,
) -> Vec<CopyBothMessage> {
    assert!(start_offset < end_offset);
    assert!(end_offset <= FIXTURE_BYTES_UNDER_TEST);
    assert!(chunk_size > 0);

    let mut messages = Vec::new();
    let mut offset = start_offset;
    while offset < end_offset {
        let next_offset = offset.saturating_add(chunk_size).min(end_offset);
        let wal_start = Lsn(fixture.base_lsn.value() + offset as u64);
        messages.push(CopyBothMessage::XLogData(XLogData {
            wal_start,
            wal_end: Lsn(fixture.base_lsn.value() + next_offset as u64),
            send_time: 1,
            data: Bytes::copy_from_slice(&fixture.bytes[offset..next_offset]),
        }));
        offset = next_offset;
    }
    messages
}

fn decode_stored_topic_records(
    records: &[Vec<u8>],
    base_lsn: Lsn,
) -> Vec<crabka_postgres_wal::XLogRecord> {
    let mut expected_lsn = base_lsn;
    let mut decoder = crabka_postgres_wal::WalStreamDecoder::new(to_decoder_lsn(base_lsn));
    let mut decoded_records = Vec::new();

    for record in records {
        let frame = WalFrame::decode(record).expect("stored PGW1 frame decodes");
        assert!(frame.lsn == expected_lsn);
        expected_lsn = frame_end_lsn(&frame).expect("frame end LSN computes");
        decoder
            .feed(to_decoder_lsn(frame.lsn), &frame.payload)
            .expect("stored frame payload feeds WAL decoder contiguously");
        while let Some(decoded_record) = decoder
            .poll_record()
            .expect("stored fixture WAL is CRC-valid and frame-decodable")
        {
            decoded_record
                .decode()
                .expect("stored fixture WAL body is grammar-decodable");
            decoded_records.push(decoded_record);
        }
    }

    assert!(expected_lsn == Lsn(base_lsn.value() + FIXTURE_BYTES_UNDER_TEST as u64));
    decoded_records
}

fn to_decoder_lsn(lsn: Lsn) -> crabka_postgres_wal::Lsn {
    crabka_postgres_wal::Lsn(lsn.value())
}
