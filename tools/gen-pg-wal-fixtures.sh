#!/usr/bin/env bash
# Generate the committed PostgreSQL 17 WAL fixture corpus.
#
# This harness intentionally has no automatic synthetic fallback. Real PG-2/PG-4
# fixtures must come from stock PostgreSQL 17: WAL segment bytes, pg_waldump text,
# and standby oracle files captured at the same LSN. The currently committed
# synthetic development corpus can still be used by decoder unit tests, but this
# script will not recreate it unless a developer writes such bytes explicitly in a
# separate tool.
set -euo pipefail

readonly OUT=${CRABKA_PG_WAL_FIXTURE_OUT:-crates/postgres-wal/tests/fixtures}
readonly PORT=${CRABKA_PG_WAL_FIXTURE_PORT:-55432}
readonly SOCKET_DIR_PARENT=${TMPDIR:-/tmp}
readonly REQUIRED_MAJOR=17
readonly WAL_SEGMENT_SIZE=$((1024 * 1024))
readonly LSN_SPACE_PER_LOG=$((1 << 32))
readonly PG4B_REQUIRED_WORKLOADS=(
  heap_dml_fpi
  btree_primary_key
  brin_index
  hash_index
  gist_index
  spgist_index
  gin_index
  multixact_row_locks
  truncate_drop_lifecycle
  database_wal_log
)
readonly PG6_REQUIRED_FORK_ORACLES=(
  fork/manifest.toml
  fork/parent/wal
  fork/child/wal
  fork/parent/standby-oracle
  fork/child/standby-oracle
)

usage() {
  cat <<'EOF'
usage: tools/gen-pg-wal-fixtures.sh --real

Generates the plan-aligned PostgreSQL 17 WAL/oracle corpus. This requires
PostgreSQL 17 client and server tools on PATH:
  initdb pg_ctl psql pg_waldump pg_basebackup pg_controldata

The command refuses to run unless --real (or CRABKA_GENERATE_REAL_PG_WAL=1) is
provided, so default test runs never attempt live regeneration.

Environment:
  CRABKA_PG_WAL_FIXTURE_OUT   output directory (default: crates/postgres-wal/tests/fixtures)
  CRABKA_PG_WAL_FIXTURE_PORT  local temporary server port (default: 55432)
EOF
}

die() {
  printf 'error: %s\n' "$*" >&2
  exit 1
}

log() {
  printf '%s\n' "$*" >&2
}

require_real_generation_request() {
  if [[ "${CRABKA_GENERATE_REAL_PG_WAL:-}" == "1" ]]; then
    return 0
  fi

  if [[ "${1:-}" == "--real" ]]; then
    return 0
  fi

  usage
  die "live PG17 fixture regeneration is opt-in; pass --real or set CRABKA_GENERATE_REAL_PG_WAL=1"
}

require_command() {
  local command_name=$1
  command -v "$command_name" >/dev/null 2>&1 || die "missing required PostgreSQL 17 command: $command_name"
}

require_postgres_17_command() {
  local command_name=$1
  local version_output

  require_command "$command_name"
  version_output=$("$command_name" --version 2>&1 || true)
  if [[ "$version_output" != *" $REQUIRED_MAJOR."* && "$version_output" != *" $REQUIRED_MAJOR"* ]]; then
    die "$command_name must be PostgreSQL $REQUIRED_MAJOR; got: $version_output"
  fi
}

require_prerequisites() {
  require_postgres_17_command initdb
  require_postgres_17_command pg_ctl
  require_postgres_17_command psql
  require_postgres_17_command pg_waldump
  require_postgres_17_command pg_basebackup
  require_postgres_17_command pg_controldata
  require_command sha256sum
  require_crc32c_tool
}

require_crc32c_tool() {
  if command -v crc32c >/dev/null 2>&1; then
    return 0
  fi

  require_command python3
}

crc32c_file() {
  local path=$1
  local cli_checksum
  local cli_output

  if command -v crc32c >/dev/null 2>&1; then
    cli_output=$(crc32c "$path" 2>/dev/null || true)
    cli_checksum=${cli_output%%[[:space:]]*}
    if [[ "$cli_checksum" =~ ^[0-9A-Fa-f]{8}$ ]]; then
      printf '%s\n' "${cli_checksum,,}"
      return 0
    fi
  fi

  python3 - "$path" <<'PY'
import pathlib
import sys

POLYNOMIAL = 0x82F63B78


def build_crc32c_table():
    table = []
    for byte in range(256):
        checksum = byte
        for _ in range(8):
            if checksum & 1:
                checksum = (checksum >> 1) ^ POLYNOMIAL
            else:
                checksum >>= 1
        table.append(checksum & 0xFFFFFFFF)
    return table


def crc32c_file(path):
    table = build_crc32c_table()
    checksum = 0xFFFFFFFF
    with pathlib.Path(path).open("rb") as file:
        for chunk in iter(lambda: file.read(1024 * 1024), b""):
            for byte in chunk:
                checksum = table[(checksum ^ byte) & 0xFF] ^ (checksum >> 8)
    return (checksum ^ 0xFFFFFFFF) & 0xFFFFFFFF


print(f"{crc32c_file(sys.argv[1]):08x}")
PY
}

select_committed_wal_segments() {
  local capture_lsn_value last_committed_lsn previous_base_lsn selected_count

  capture_lsn_value=$(parse_lsn_value "$capture_lsn")
  last_committed_lsn=0
  previous_base_lsn=
  selected_count=0

  while IFS= read -r wal_segment; do
    local base_lsn expected_next_lsn

    base_lsn=$(segment_base_lsn_from_path "$wal_segment") || continue
    if ((base_lsn >= capture_lsn_value)); then
      continue
    fi
    if [[ -n "$previous_base_lsn" ]]; then
      expected_next_lsn=$((previous_base_lsn + WAL_SEGMENT_SIZE))
      if ((base_lsn != expected_next_lsn)); then
        die "missing contiguous WAL segment before $(basename "$wal_segment"): expected base $(format_lsn "$expected_next_lsn"), got $(format_lsn "$base_lsn")"
      fi
    fi

    printf '%s\n' "$wal_segment"
    previous_base_lsn=$base_lsn
    last_committed_lsn=$((base_lsn + WAL_SEGMENT_SIZE))
    selected_count=$((selected_count + 1))
  done < <(find "$work_dir/primary/pg_wal" -maxdepth 1 -type f -name '0000000*' | sort)

  if ((selected_count == 0)); then
    die "no WAL segments cover capture_lsn $(format_lsn "$capture_lsn_value")"
  fi
  if ((capture_lsn_value > last_committed_lsn)); then
    die "capture_lsn $(format_lsn "$capture_lsn_value") is beyond copied WAL end $(format_lsn "$last_committed_lsn")"
  fi
}

parse_lsn_value() {
  local raw_lsn=$1
  local high=${raw_lsn%%/*}
  local low=${raw_lsn#*/}

  if [[ "$high" == "$raw_lsn" || "$low" == */* || -z "$high" || -z "$low" ]]; then
    die "invalid capture_lsn $raw_lsn"
  fi
  if [[ ! "$high" =~ ^[0-9A-Fa-f]+$ || ! "$low" =~ ^[0-9A-Fa-f]+$ ]]; then
    die "invalid capture_lsn $raw_lsn"
  fi

  printf '%u\n' $(((16#$high << 32) + 16#$low))
}

segment_base_lsn_from_path() {
  local path=$1
  local filename=${path##*/}
  local log segment segments_per_log segment_number

  if [[ ! "$filename" =~ ^[0-9A-Fa-f]{24}$ ]]; then
    return 1
  fi

  log=$((16#${filename:8:8}))
  segment=$((16#${filename:16:8}))
  segments_per_log=$((LSN_SPACE_PER_LOG / WAL_SEGMENT_SIZE))
  segment_number=$((log * segments_per_log + segment))

  printf '%u\n' $((segment_number * WAL_SEGMENT_SIZE))
}

format_lsn() {
  local value=$1

  printf '%X/%X' $((value >> 32)) $((value & 0xffffffff))
}

psql_primary() {
  psql -h "$socket_dir" -p "$PORT" -d postgres -qAt -v ON_ERROR_STOP=1 "$@"
}

psql_primary_command() {
  psql_primary -c "$1"
}

write_primary_config() {
  cat >>"$work_dir/primary/postgresql.conf" <<EOF
wal_level = replica
wal_compression = off
max_wal_senders = 4
max_replication_slots = 4
full_page_writes = on
fsync = on
synchronous_commit = on
wal_keep_size = '64MB'
min_wal_size = '64MB'
max_wal_size = '128MB'
listen_addresses = ''
port = $PORT
unix_socket_directories = '$socket_dir'
EOF
}

start_primary() {
  initdb -D "$work_dir/primary" --wal-segsize=1 --no-instructions >/dev/null
  write_primary_config
  pg_ctl -D "$work_dir/primary" -l "$work_dir/primary.log" -w start >/dev/null
}

stop_primary() {
  if [[ -d "${work_dir:-}/primary" ]]; then
    pg_ctl -D "$work_dir/primary" -m immediate stop >/dev/null 2>&1 || true
  fi
}

stop_standby() {
  if [[ -d "${work_dir:-}/standby" ]]; then
    pg_ctl -D "$work_dir/standby" -m immediate stop >/dev/null 2>&1 || true
  fi
}

run_fixture_workload() {
  psql_primary_command "CREATE TABLE wal_fixture_t(id bigserial PRIMARY KEY, pad text NOT NULL);"
  psql_primary_command "INSERT INTO wal_fixture_t(pad) SELECT repeat('x', 500) FROM generate_series(1, 3000);"
  psql_primary_command "UPDATE wal_fixture_t SET pad = repeat('y', 500) WHERE id % 7 = 0;"
  psql_primary_command "DELETE FROM wal_fixture_t WHERE id % 11 = 0;"
  psql_primary_command "INSERT INTO wal_fixture_t(pad) VALUES (repeat('z', 900000));"
  psql_primary_command "CHECKPOINT;"
  psql_primary_command "UPDATE wal_fixture_t SET pad = 'post-ckpt' WHERE id = 1;"
  psql_primary_command "VACUUM wal_fixture_t;"
  run_pg4b_index_workloads
  run_pg4b_multixact_workload
  run_pg4b_lifecycle_workloads
}

run_pg4b_index_workloads() {
  psql_primary <<'SQL'
CREATE TABLE wal_fixture_indexes(
  id bigserial PRIMARY KEY,
  brin_key bigint NOT NULL,
  hash_key text NOT NULL,
  gist_key point NOT NULL,
  spgist_key point NOT NULL,
  gin_key text[] NOT NULL,
  pad text NOT NULL
);
INSERT INTO wal_fixture_indexes(brin_key, hash_key, gist_key, spgist_key, gin_key, pad)
SELECT value,
       'hash-' || (value % 97),
       point(value % 101, value % 89),
       point(value % 83, value % 79),
       ARRAY['gin', 'token-' || (value % 31), 'row-' || value],
       repeat('i', 300)
FROM generate_series(1, 5000) AS value;
CREATE INDEX wal_fixture_indexes_brin ON wal_fixture_indexes USING brin (brin_key);
CREATE INDEX wal_fixture_indexes_hash ON wal_fixture_indexes USING hash (hash_key);
CREATE INDEX wal_fixture_indexes_gist ON wal_fixture_indexes USING gist (gist_key);
CREATE INDEX wal_fixture_indexes_spgist ON wal_fixture_indexes USING spgist (spgist_key);
CREATE INDEX wal_fixture_indexes_gin ON wal_fixture_indexes USING gin (gin_key);
UPDATE wal_fixture_indexes SET pad = repeat('j', 300) WHERE id % 13 = 0;
DELETE FROM wal_fixture_indexes WHERE id % 17 = 0;
VACUUM wal_fixture_indexes;
SQL
}

run_pg4b_multixact_workload() {
  psql_primary_command "CREATE TABLE wal_fixture_multixact(id integer PRIMARY KEY, pad text NOT NULL);"
  psql_primary_command "INSERT INTO wal_fixture_multixact VALUES (1, 'locked');"

  psql -h "$socket_dir" -p "$PORT" -d postgres -qAt -v ON_ERROR_STOP=1 <<'SQL' &
BEGIN;
SELECT * FROM wal_fixture_multixact WHERE id = 1 FOR KEY SHARE;
SELECT pg_sleep(2);
COMMIT;
SQL
  local locker_one=$!

  psql -h "$socket_dir" -p "$PORT" -d postgres -qAt -v ON_ERROR_STOP=1 <<'SQL' &
BEGIN;
SELECT * FROM wal_fixture_multixact WHERE id = 1 FOR KEY SHARE;
SELECT pg_sleep(2);
COMMIT;
SQL
  local locker_two=$!

  wait "$locker_one"
  wait "$locker_two"
}

run_pg4b_lifecycle_workloads() {
  psql_primary <<'SQL'
CREATE TABLE wal_fixture_lifecycle(id integer PRIMARY KEY, pad text NOT NULL);
INSERT INTO wal_fixture_lifecycle SELECT value, repeat('l', 200) FROM generate_series(1, 300) AS value;
TRUNCATE wal_fixture_lifecycle;
DROP TABLE wal_fixture_lifecycle;
CREATE DATABASE wal_fixture_database_wal;
DROP DATABASE wal_fixture_database_wal;
SQL
}

write_relation_map() {
  psql_primary -F $'\t' -c "SELECT c.relname, c.relkind, pg_relation_filepath(c.oid) FROM pg_class c WHERE c.relname LIKE 'wal_fixture_%' AND pg_relation_filepath(c.oid) IS NOT NULL ORDER BY c.relname;" >"$work_dir/relations.tsv"
}

psql_standby() {
  psql -h "$socket_dir" -p "$((PORT + 1))" -d postgres -qAt -v ON_ERROR_STOP=1 "$@"
}

psql_standby_command() {
  psql_standby -c "$1"
}

create_standby_basebackup() {
  pg_basebackup -D "$work_dir/standby" -h "$socket_dir" -p "$PORT" -X stream -c fast >/dev/null
}

toml_string_array() {
  local -n values=$1
  local separator=""

  printf '['
  for value in "${values[@]}"; do
    printf '%s"%s"' "$separator" "$value"
    separator=", "
  done
  printf ']'
}

start_and_wait_for_standby_shutdown() {
  cat >"$work_dir/standby/postgresql.auto.conf" <<EOF
port = $((PORT + 1))
unix_socket_directories = '$socket_dir'
restore_command = 'cp $work_dir/primary/pg_wal/%f %p'
recovery_target_lsn = '$capture_lsn'
recovery_target_action = 'pause'
EOF
  touch "$work_dir/standby/standby.signal"
  pg_ctl -D "$work_dir/standby" -l "$work_dir/standby.log" -w start >/dev/null

  for _ in {1..120}; do
    local replay_paused

    replay_paused=$(psql_standby_command "SELECT pg_is_wal_replay_paused();" 2>/dev/null || true)
    if [[ "$replay_paused" == "t" ]]; then
      pg_ctl -D "$work_dir/standby" -m fast -w stop >/dev/null
      return 0
    fi
    sleep 1
  done

  pg_ctl -D "$work_dir/standby" -m immediate stop >/dev/null 2>&1 || true
  die "standby did not pause at recovery_target_lsn=$capture_lsn"
}

copy_wal_segments() {
  mkdir -p "$OUT"
  find "$OUT" -maxdepth 1 -type f \( -name '*.wal' -o -name '0000000*' \) -delete

  mapfile -t wal_segments < <(select_committed_wal_segments)
  if [[ ${#wal_segments[@]} -lt 2 ]]; then
    die "expected at least two committed WAL segments through capture_lsn=$capture_lsn; got ${#wal_segments[@]}"
  fi

  for wal_segment in "${wal_segments[@]}"; do
    cp "$wal_segment" "$OUT/$(basename "$wal_segment").wal"
  done

  pg_waldump -e "$capture_lsn" "${wal_segments[0]}" "${wal_segments[-1]}" >"$OUT/oracle.waldump"
  if [[ ! -s "$OUT/oracle.waldump" ]]; then
    die "pg_waldump produced an empty oracle"
  fi
}

copy_standby_oracles() {
  rm -rf "$OUT/standby"
  mkdir -p "$OUT/standby/relations" "$OUT/standby/slru/pg_xact" "$OUT/standby/slru/pg_multixact"

  while IFS=$'\t' read -r relname relkind relation_path; do
    [[ -n "$relation_path" ]] || continue
    mkdir -p "$OUT/standby/relations/$relname"
    for fork_suffix in "" _vm; do
      local source_path="$work_dir/standby/$relation_path$fork_suffix"
      if [[ -f "$source_path" ]]; then
        cp "$source_path" "$OUT/standby/relations/$relname/$(basename "$relation_path")$fork_suffix"
      fi
    done
    printf '%s\t%s\t%s\n' "$relname" "$relkind" "$relation_path" >>"$OUT/standby/relations.tsv"
  done <"$work_dir/relations.tsv"

  find "$work_dir/standby/pg_xact" -maxdepth 1 -type f -exec cp '{}' "$OUT/standby/slru/pg_xact/" \;
  find "$work_dir/standby/pg_multixact" -maxdepth 2 -type f -exec sh -c 'for f do mkdir -p "$0/$(basename "$(dirname "$f")")"; cp "$f" "$0/$(basename "$(dirname "$f")")/"; done' "$OUT/standby/slru/pg_multixact" '{}' +
}

append_file_checksums() {
  local path

  find "$OUT" -type f ! -name manifest.toml | sort | while read -r path; do
    local relative_path=${path#"$OUT"/}
    local checksum
    local crc32c
    checksum=$(sha256sum "$path" | cut -d' ' -f1)
    crc32c=$(crc32c_file "$path")
    printf '[[files]]\npath = "%s"\nsha256 = "%s"\ncrc32c = "%s"\n\n' "$relative_path" "$checksum" "$crc32c" >>"$OUT/manifest.toml"
  done
}

write_manifest() {
  local initdb_version pg_waldump_version segment_count record_count required_workloads required_fork_oracles

  initdb_version=$(initdb --version | sed 's/"/\\"/g')
  pg_waldump_version=$(pg_waldump --version | sed 's/"/\\"/g')
  segment_count=$(find "$OUT" -maxdepth 1 -type f -name '*.wal' | wc -l | tr -d ' ')
  record_count=$(grep -c '^[[:space:]]*rmgr:' "$OUT/oracle.waldump" || true)
  required_workloads=$(toml_string_array PG4B_REQUIRED_WORKLOADS)
  required_fork_oracles=$(toml_string_array PG6_REQUIRED_FORK_ORACLES)

  cat >"$OUT/manifest.toml" <<EOF
pg_major = 17
wal_segsize = "1MB"
wal_compression = "off"
platform = "little-endian"
corpus = "real-pg17-standby-oracle"
provenance = "real-postgresql-17"
generator = "tools/gen-pg-wal-fixtures.sh"
initdb_version = "$initdb_version"
pg_waldump_version = "$pg_waldump_version"
capture_lsn = "$capture_lsn"
wal_segment_count = $segment_count
records = $record_count
required_workloads = $required_workloads
required_oracles = ["oracle.waldump", "standby/relations.tsv", "standby/relations", "standby/slru/pg_xact", "standby/slru/pg_multixact"]
required_fork_oracles = $required_fork_oracles
pg6_fork_capture = "required-external-promote-and-diverge-workflow"
notes = "Generated from stock PostgreSQL 17 with WAL bytes, pg_waldump text, relation-file standby oracle, and SLRU standby oracle captured at capture_lsn. PG6 promote-and-diverge fork WAL remains a separate required oracle workflow under required_fork_oracles."

EOF
  append_file_checksums
}

main() {
  if [[ "${1:-}" == "--help" || "${1:-}" == "-h" ]]; then
    usage
    exit 0
  fi

  require_real_generation_request "${1:-}"
  require_prerequisites

  work_dir=$(mktemp -d "$SOCKET_DIR_PARENT/crabka-pg-wal.XXXXXX")
  socket_dir="$work_dir/socket"
  mkdir -p "$socket_dir"
  trap 'stop_standby; stop_primary; rm -rf "$work_dir"' EXIT

  start_primary
  create_standby_basebackup
  run_fixture_workload
  write_relation_map
  capture_lsn=$(psql_primary_command "SELECT pg_current_wal_flush_lsn();")
  psql_primary_command "SELECT pg_switch_wal();" >/dev/null
  copy_wal_segments
  start_and_wait_for_standby_shutdown
  copy_standby_oracles
  write_manifest

  log "real PostgreSQL 17 WAL fixtures written to $OUT"
}

main "$@"
