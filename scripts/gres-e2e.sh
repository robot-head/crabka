#!/usr/bin/env bash
# Front-door Gres E2E gate: tenant registry -> substrate computes -> PgDog.
set -euo pipefail

cd "$(dirname "$0")/.."

usage() {
    cat <<'EOF'
Usage: scripts/gres-e2e.sh [--skip-pgdog]

Boots a local Crabka broker, creates three Gres tenants with passwords supplied
by files, starts substrate-mode computes, renders PgDog configuration, and runs
front-door authentication/isolation assertions through PgDog when Docker/PgDog is
available.

Options:
  --skip-pgdog   Explicitly skip Docker/PgDog assertions for local development.
  --help         Show this help.

Environment:
  CRABKA_GRES_SKIP_BUILD=1              Reuse existing target/debug binaries.
  CRABKA_GRES_E2E_SKIP_PGDOG=1          Same as --skip-pgdog.
  CRABKA_GRES_E2E_KEEP_ARTIFACTS=1      Keep logs and generated configs.
  CRABKA_GRES_PGDOG_IMAGE=<image>       Override the pinned PgDog image.
  CRABKA_GRES_POSTGRES_IMAGE=<image>    Override the pinned Postgres oracle.
  CRABKA_GRES_KAFKA_IMAGE=<image>       Override the pinned Kafka CLI image.
  CRABKA_GRES_EXPECT_KAFKA_ACL=0        Disable mandatory Kafka ACL assertions.
EOF
}

SKIP_PGDOG="${CRABKA_GRES_E2E_SKIP_PGDOG:-0}"
case "${1:-}" in
    "") ;;
    --skip-pgdog) SKIP_PGDOG=1 ;;
    --help|-h) usage; exit 0 ;;
    *) echo "FAIL: unknown argument $1" >&2; usage >&2; exit 2 ;;
esac

PGDOG_IMAGE="${CRABKA_GRES_PGDOG_IMAGE:-ghcr.io/pgdogdev/pgdog:0.1.6}"
POSTGRES_IMAGE="${CRABKA_GRES_POSTGRES_IMAGE:-postgres:18}"
KAFKA_IMAGE="${CRABKA_GRES_KAFKA_IMAGE:-mirror.gcr.io/apache/kafka:4.0.0}"
CLUSTER_ID="00000000-0000-0000-0000-000000000001"
ARTIFACT_DIR="${CRABKA_GRES_E2E_ARTIFACT_DIR:-target/gres-e2e-artifacts}"
BROKER_PID=""
PGDOG_CONTAINER=""
ORACLE_CONTAINER=""
EXPECT_KAFKA_ACL="${CRABKA_GRES_EXPECT_KAFKA_ACL:-1}"
TENANT_A_PID=""
TENANT_B_PID=""
TENANT_C_PID=""

log() {
    printf 'gres-e2e: %s\n' "$*"
}

fail() {
    echo "FAIL: $*" >&2
    dump_diagnostics
    exit 1
}

is_kafka_authorization_denial() {
    local status="$1"
    local output_file="$2"
    local topic="$3"

    [ "$status" -eq 1 ] &&
        grep -Fqx "crabka gres: topic ${topic} metadata: UNKNOWN (29)" "$output_file"
}

# Shell-level contract test hook: keep denial classification deterministic and
# prove that an empty/successful fetch cannot satisfy the ACL assertion.
if [ -n "${CRABKA_GRES_E2E_TEST_CLASSIFY_STATUS:-}" ]; then
    classifier_output=$(mktemp)
    trap 'rm -f "$classifier_output"' EXIT
    printf '%s\n' "${CRABKA_GRES_E2E_TEST_CLASSIFY_OUTPUT:-}" >"$classifier_output"
    if is_kafka_authorization_denial "$CRABKA_GRES_E2E_TEST_CLASSIFY_STATUS" "$classifier_output" "${CRABKA_GRES_E2E_TEST_CLASSIFY_TOPIC:-__gres_tenants}"; then
        echo denied
        exit 0
    fi
    echo not-denied
    exit 1
fi

dump_diagnostics() {
    echo "---- gres-e2e artifacts: ${ARTIFACT_DIR} ----" >&2
    for file in "${ARTIFACT_DIR}"/*.log; do
        [ -f "$file" ] || continue
        echo "---- ${file} ----" >&2
        tail -n 120 "$file" >&2 || true
    done
}

cleanup() {
    local status=$?
    docker rm -f "${PGDOG_CONTAINER:-}" "${ORACLE_CONTAINER:-}" >/dev/null 2>&1 || true
    kill "${TENANT_A_PID:-}" "${TENANT_B_PID:-}" "${TENANT_C_PID:-}" "${BROKER_PID:-}" 2>/dev/null || true
    wait "${TENANT_A_PID:-}" "${TENANT_B_PID:-}" "${TENANT_C_PID:-}" "${BROKER_PID:-}" 2>/dev/null || true
    if [ "$status" -ne 0 ]; then
        dump_diagnostics
    fi
    if [ "${CRABKA_GRES_E2E_KEEP_ARTIFACTS:-0}" != "1" ] && [ "$status" -eq 0 ]; then
        rm -rf "$ARTIFACT_DIR"
    else
        log "kept artifacts in ${ARTIFACT_DIR}"
    fi
}
trap cleanup EXIT

require_command() {
    command -v "$1" >/dev/null 2>&1 || fail "$1 is required"
}

choose_ports() {
    python3 - <<'PY'
import socket

sockets = []
try:
    for _ in range(4):
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.bind(("127.0.0.1", 0))
        sockets.append(sock)
    for sock in sockets:
        print(sock.getsockname()[1])
finally:
    for sock in sockets:
        sock.close()
PY
}

wait_for_tcp_port() {
    local host="$1"
    local port="$2"
    local label="$3"

    for _ in $(seq 80); do
        if python3 - "$host" "$port" <<'PY' >/dev/null 2>&1
import socket
import sys

with socket.create_connection((sys.argv[1], int(sys.argv[2])), timeout=0.2):
    pass
PY
        then
            return 0
        fi
        sleep 0.25
    done

    fail "${label} did not open ${host}:${port}"
}

wait_for_sql() {
    local label="$1"
    local conninfo="$2"
    local password="$3"

    for _ in $(seq 80); do
        if PGAPPNAME= PGPASSWORD="$password" psql "$conninfo" -tAc 'SELECT 1' >/dev/null 2>&1; then
            return 0
        fi
        sleep 0.25
    done

    fail "${label} did not become SQL-ready"
}

expect_sql_equals() {
    local label="$1"
    local conninfo="$2"
    local password="$3"
    local sql="$4"
    local expected="$5"
    local actual

    actual=$(PGAPPNAME= PGPASSWORD="$password" psql "$conninfo" -tAc "$sql")
    if [ "$actual" = "$expected" ]; then
        log "PASS: ${label} -> ${actual}"
        return 0
    fi

    fail "${label}: expected ${expected}, got ${actual}"
}

expect_sql_fails() {
    local label="$1"
    local conninfo="$2"
    local password="$3"
    local sql="$4"

    if PGAPPNAME= PGPASSWORD="$password" psql "$conninfo" -tAc "$sql" >"${ARTIFACT_DIR}/${label}.out" 2>"${ARTIFACT_DIR}/${label}.err"; then
        fail "${label}: command unexpectedly succeeded"
    fi
    log "PASS: ${label} failed as expected"
}

docker_is_available() {
    command -v docker >/dev/null 2>&1 && timeout 10s docker info >/dev/null 2>&1
}

expect_compute_denied() {
    local label="$1"
    local tenant="$2"
    local username="$3"
    local password="$4"

    if GRES_KAFKA_USERNAME="$username" GRES_KAFKA_PASSWORD="$password" timeout 20s \
        ./target/debug/crabka-gres \
            --listen "127.0.0.5:0" \
            --substrate-bootstrap "127.0.0.1:${SASL_PORT}" \
            --tenant "$tenant" \
            >"${ARTIFACT_DIR}/${label}.log" 2>&1; then
        fail "${label}: unauthorized tenant Kafka read unexpectedly succeeded"
    fi
    if grep -Eq 'authorization|AUTHORIZATION|denied|not authorized|TOPIC_AUTHORIZATION_FAILED|CLUSTER_AUTHORIZATION_FAILED|GROUP_AUTHORIZATION_FAILED|UNKNOWN \(29\)' "${ARTIFACT_DIR}/${label}.log"; then
        log "PASS: ${label} denied by Kafka ACL"
        return 0
    fi
    fail "${label}: failed, but not with a recognizable Kafka authorization denial"
}

expect_kafka_topic_read_denied() {
    local label="$1"
    local topic="$2"
    local username="$3"
    local password="$4"
    local client_properties="${ARTIFACT_DIR}/${label}.password"
    local output="${ARTIFACT_DIR}/${label}.log"
    local status

    printf '%s\n' "$password" >"$client_properties"
    chmod 600 "$client_properties"

    set +e
    timeout 20s ./target/debug/crabka gres probe-topic-read \
        --bootstrap "127.0.0.1:${SASL_PORT}" \
        --topic "$topic" \
        --username "$username" \
        --password-file "$client_properties" \
        >"$output" 2>&1
    status=$?
    set -e

    if is_kafka_authorization_denial "$status" "$output" "$topic"; then
        log "PASS: ${label} denied by Kafka ACL"
        return 0
    fi
    if [ "$status" -eq 0 ]; then
        fail "${label}: unauthorized topic read unexpectedly succeeded"
    fi
    fail "${label}: topic read failed without an explicit Kafka authorization denial"
}

assert_kafka_acl_enforcement() {
    if [ "$EXPECT_KAFKA_ACL" != "1" ]; then
        log "SKIP: Kafka ACL assertions disabled by CRABKA_GRES_EXPECT_KAFKA_ACL=0"
        return 0
    fi
    docker_is_available || fail "Docker/Kafka CLI runtime unavailable for mandatory Kafka ACL assertions"
    timeout 120s docker pull "$KAFKA_IMAGE" >"${ARTIFACT_DIR}/pull-kafka.log" 2>&1 ||
        fail "Kafka CLI image pull failed or did not complete within 120 seconds"
    expect_compute_denied tenant-a-cannot-read-tenant-b tenant-b gres-tenant-a alice-secret
    expect_compute_denied tenant-b-cannot-read-tenant-a tenant-a gres-tenant-b bob-secret
    expect_kafka_topic_read_denied tenant-a-cannot-read-global-registry __gres_tenants gres-tenant-a alice-secret
}

start_broker() {
    ./target/debug/crabka format \
        --log-dir "${ARTIFACT_DIR}/broker-data" \
        --cluster-id "$CLUSTER_ID" \
        --standalone \
        --node-id 1 \
        --controller-listener "127.0.0.1:${CONTROLLER_PORT}" \
        >"${ARTIFACT_DIR}/format.log" 2>&1

    cat >"${ARTIFACT_DIR}/broker.toml" <<EOF
broker_id = 1
log_dir = "${ARTIFACT_DIR}/broker-data"
cluster_id = "${CLUSTER_ID}"
inter_broker_listener_name = "plain"

[[listeners]]
name = "plain"
bind_addr = "127.0.0.1:${BROKER_PORT}"
advertised = "127.0.0.1:${BROKER_PORT}"
protocol = "Plaintext"

[[listeners]]
name = "sasl"
bind_addr = "127.0.0.1:${SASL_PORT}"
advertised = "127.0.0.1:${SASL_PORT}"
protocol = "SaslPlaintext"
sasl_config = { enabled_mechanisms = ["SCRAM-SHA-512"] }

[authorization]
type = "simple"
super_users = ["ANONYMOUS"]
EOF

    ./target/debug/crabka-broker \
        --log-dir "${ARTIFACT_DIR}/broker-data" \
        --cluster-id "$CLUSTER_ID" \
        --broker-id 1 \
        --config-file "${ARTIFACT_DIR}/broker.toml" \
        >"${ARTIFACT_DIR}/broker.log" 2>&1 &
    BROKER_PID=$!
    wait_for_tcp_port 127.0.0.1 "$BROKER_PORT" broker
}

create_tenant() {
    local tenant="$1"
    local user="$2"
    local password="$3"
    local password_file="${ARTIFACT_DIR}/${tenant}.password"

    printf '%s\n' "$password" >"$password_file"
    ./target/debug/crabka gres create-tenant \
        --bootstrap "127.0.0.1:${BROKER_PORT}" \
        --name "$tenant" \
        --user "$user" \
        --password-file "$password_file" \
        >"${ARTIFACT_DIR}/create-${tenant}.log" 2>&1
}

start_compute() {
    local tenant="$1"
    local host="$2"
    local log_name="$3"
    local pid_var="$4"

    GRES_KAFKA_USERNAME="gres-${tenant}" GRES_KAFKA_PASSWORD="${tenant_passwords[$tenant]}" \
        ./target/debug/crabka-gres \
        --listen "${host}:5432" \
        --substrate-bootstrap "127.0.0.1:${SASL_PORT}" \
        --tenant "$tenant" \
        >"${ARTIFACT_DIR}/${log_name}.log" 2>&1 &
    printf -v "$pid_var" '%s' "$!"
    wait_for_sql "$tenant compute" "host=${host} port=5432 user=${tenant_users[$tenant]} dbname=crab sslmode=prefer" "${tenant_passwords[$tenant]}"
}

patch_pgdog_listen_port() {
    python3 - "$PGDOG_PORT" "${ARTIFACT_DIR}/pgdog/pgdog.toml" <<'PY'
import pathlib
import sys

port = sys.argv[1]
path = pathlib.Path(sys.argv[2])
text = path.read_text()
text = text.replace("listen_port = 6432", f"listen_port = {port}", 1)
text = text.replace(
    "pooler_mode = \"transaction\"",
    'pooler_mode = "transaction"\ndefault_pool_size = 10',
    1,
)
tenant_b = 'name = "tenant-b"\nhost = "tenant-b.gres.svc"'
text = text.replace(tenant_b, tenant_b + "\npool_size = 1", 1)
if (
    "default_pool_size = 10" not in text
    or "pool_size = 1" not in text
):
    raise SystemExit("failed to configure tenant-b's one-backend PgDog pool")
path.write_text(text)
PY
}

patch_pgdog_local_users() {
    cat >"${ARTIFACT_DIR}/pgdog/users.toml" <<EOF
[[users]]
name = "alice"
database = "tenant-a"
password = "alice-secret"

[[users]]
name = "bob"
database = "tenant-b"
password = "bob-secret"

[[users]]
name = "carol"
database = "tenant-c"
password = "carol-secret"
EOF
}

start_pgdog() {
    PGGDOG_RUN_ARGS=(
        run -d --network host
        --name "crabka-gres-e2e-pgdog-${PGDOG_PORT}"
        --add-host "tenant-a.gres.svc:127.0.0.2"
        --add-host "tenant-b.gres.svc:127.0.0.3"
        --add-host "tenant-c.gres.svc:127.0.0.4"
        -v "${PWD}/${ARTIFACT_DIR}/pgdog:/etc/pgdog:ro"
        "$PGDOG_IMAGE"
        /usr/local/bin/pgdog --config /etc/pgdog/pgdog.toml --users /etc/pgdog/users.toml run
    )
    PGDOG_CONTAINER=$(docker "${PGGDOG_RUN_ARGS[@]}")
    docker logs -f "$PGDOG_CONTAINER" >"${ARTIFACT_DIR}/pgdog-container.log" 2>&1 &
    wait_for_tcp_port 127.0.0.1 "$PGDOG_PORT" pgdog
}

start_oracle() {
    ORACLE_CONTAINER=$(docker run -d --rm \
        -e POSTGRES_HOST_AUTH_METHOD=trust \
        -p "127.0.0.1:${ORACLE_PORT}:5432" \
        "$POSTGRES_IMAGE")
    for _ in $(seq 80); do
        if PGAPPNAME= psql "host=127.0.0.1 port=${ORACLE_PORT} user=postgres dbname=postgres" -tAc 'SELECT 1' >/dev/null 2>&1; then
            return 0
        fi
        sleep 0.5
    done
    fail "Postgres oracle did not become ready"
}

require_command python3
require_command psql
mapfile -t PORTS < <(choose_ports)
BROKER_PORT="${PORTS[0]}"
CONTROLLER_PORT="${PORTS[1]}"
SASL_PORT="${PORTS[2]}"
PGDOG_PORT="${CRABKA_GRES_PGDOG_PORT:-6432}"
ORACLE_PORT="${PORTS[3]}"

rm -rf "$ARTIFACT_DIR"
mkdir -p "${ARTIFACT_DIR}/pgdog"

if [ "${CRABKA_GRES_SKIP_BUILD:-}" != "1" ]; then
    cargo build --locked -p crabka-cli -p crabka-broker -p crabka-gres -p crabka-gres-conformance
fi

start_broker

declare -A tenant_users=( [tenant-a]=alice [tenant-b]=bob [tenant-c]=carol )
declare -A tenant_passwords=( [tenant-a]=alice-secret [tenant-b]=bob-secret [tenant-c]=carol-secret )

create_tenant tenant-a alice alice-secret
create_tenant tenant-b bob bob-secret
create_tenant tenant-c carol carol-secret

start_compute tenant-a 127.0.0.2 tenant-a-compute TENANT_A_PID
start_compute tenant-b 127.0.0.3 tenant-b-compute TENANT_B_PID
start_compute tenant-c 127.0.0.4 tenant-c-compute TENANT_C_PID

./target/debug/crabka gres render-pgdog \
    --bootstrap "127.0.0.1:${BROKER_PORT}" \
    --out-dir "${ARTIFACT_DIR}/pgdog" \
    >"${ARTIFACT_DIR}/render-pgdog.log" 2>&1
patch_pgdog_listen_port
patch_pgdog_local_users

assert_kafka_acl_enforcement

if [ "$SKIP_PGDOG" = "1" ]; then
    log "SKIP: PgDog assertions explicitly skipped"
    exit 0
fi
python3 -c 'import psycopg' >/dev/null 2>&1 || fail "Python psycopg is required for PgDog driver smoke tests"
docker_is_available || fail "Docker/PgDog runtime unavailable; pass --skip-pgdog only for local development"

docker pull "$PGDOG_IMAGE" >"${ARTIFACT_DIR}/pull-pgdog.log" 2>&1
docker pull "$POSTGRES_IMAGE" >"${ARTIFACT_DIR}/pull-postgres.log" 2>&1
start_pgdog

TENANT_A_CONN="host=127.0.0.1 port=${PGDOG_PORT} dbname=tenant-a user=alice sslmode=prefer"
TENANT_B_CONN="host=127.0.0.1 port=${PGDOG_PORT} dbname=tenant-b user=bob sslmode=prefer"
TENANT_C_CONN="host=127.0.0.1 port=${PGDOG_PORT} dbname=tenant-c user=carol sslmode=prefer"
WRONG_TENANT_CONN="host=127.0.0.1 port=${PGDOG_PORT} dbname=tenant-b user=alice sslmode=prefer"

wait_for_sql "tenant A through PgDog" "$TENANT_A_CONN" alice-secret
wait_for_sql "tenant B through PgDog" "$TENANT_B_CONN" bob-secret
expect_sql_equals "tenant A SCRAM" "$TENANT_A_CONN" alice-secret 'SELECT 1' 1
expect_sql_equals "tenant B SCRAM" "$TENANT_B_CONN" bob-secret 'SELECT 1' 1
expect_sql_fails "wrong-password" "$TENANT_A_CONN" wrong-secret 'SELECT 1'
expect_sql_fails "wrong-tenant-credentials" "$WRONG_TENANT_CONN" alice-secret 'SELECT 1'

PGAPPNAME= PGPASSWORD=alice-secret psql "$TENANT_A_CONN" -v ON_ERROR_STOP=1 -c \
    "CREATE TABLE e2e_marker (id int4, name text); INSERT INTO e2e_marker VALUES (1, 'tenant-a');"
PGAPPNAME= PGPASSWORD=bob-secret psql "$TENANT_B_CONN" -v ON_ERROR_STOP=1 -c \
    "CREATE TABLE e2e_marker (id int4, name text); INSERT INTO e2e_marker VALUES (1, 'tenant-b');"
expect_sql_equals "tenant A data isolation" "$TENANT_A_CONN" alice-secret "SELECT name FROM e2e_marker WHERE id = 1" tenant-a
expect_sql_equals "tenant B data isolation" "$TENANT_B_CONN" bob-secret "SELECT name FROM e2e_marker WHERE id = 1" tenant-b

kill "$TENANT_A_PID"
wait "$TENANT_A_PID" 2>/dev/null || true
TENANT_A_PID=""
expect_sql_equals "tenant B survives tenant A compute death" "$TENANT_B_CONN" bob-secret "SELECT name FROM e2e_marker WHERE id = 1" tenant-b

start_oracle
CRABKA_GRES_PGDOG_TEST_URL="postgresql://carol:carol-secret@127.0.0.1:${PGDOG_PORT}/tenant-c?sslmode=disable&connect_timeout=5" \
    cargo test --locked -p crabka-gres-conformance \
    --test extended_case_lifecycle -- --nocapture \
    >"${ARTIFACT_DIR}/extended-case-lifecycle.log" 2>&1 || \
    fail "extended case lifecycle regression failed"
./target/debug/crabka-gres-conformance \
    --oracle-url "host=127.0.0.1 port=${ORACLE_PORT} user=postgres dbname=postgres" \
    --subject-url "$TENANT_C_CONN password=carol-secret" \
    --corpus crates/gres-conformance/corpus \
    --baseline crates/gres-conformance/baseline.json \
    --extended-corpus crates/gres-conformance/corpus-extended \
    --extended-baseline crates/gres-conformance/corpus-extended/baseline.json \
    --extended-out "${ARTIFACT_DIR}/extended-parity-pgdog.json" \
    --extended-summary "${ARTIFACT_DIR}/extended-parity-pgdog.md" \
    --out "${ARTIFACT_DIR}/parity-pgdog.json" \
    --summary "${ARTIFACT_DIR}/parity-pgdog.md" \
    >"${ARTIFACT_DIR}/conformance-pgdog.log" 2>&1

DATABASE_URL="postgresql://bob:bob-secret@127.0.0.1:${PGDOG_PORT}/tenant-b?sslmode=prefer&connect_timeout=5" \
    timeout 30s ./target/debug/crabka-gres-driver-smoke \
    >"${ARTIFACT_DIR}/rust-driver-smoke.log" 2>&1 || fail "Rust driver smoke failed or timed out"

DATABASE_URL="postgresql://bob:bob-secret@127.0.0.1:${PGDOG_PORT}/tenant-b?sslmode=prefer&connect_timeout=5" \
timeout 30s python3 - <<'PY' >"${ARTIFACT_DIR}/python-driver-smoke.log" 2>&1 || fail "Python driver smoke failed or timed out"
import os
import psycopg

with psycopg.connect(os.environ["DATABASE_URL"]) as connection:
    for expected in (61, 62):
        with connection.transaction():
            with connection.cursor() as cursor:
                cursor.execute("SELECT %s::int4", (expected,))
                actual = cursor.fetchone()[0]
                if actual != expected:
                    raise AssertionError(f"psycopg returned {actual}, expected {expected}")
print("PASS: psycopg parameterized transaction-pooling smoke")
PY

DATABASE_URL="postgresql://bob:bob-secret@127.0.0.1:${PGDOG_PORT}/tenant-b?sslmode=prefer&connect_timeout=5" \
timeout 30s python3 - <<'F1PY' >"${ARTIFACT_DIR}/f1-pooler-guc.log" 2>&1 || fail "F-1 PgDog GUC gate failed or timed out"
import os
import psycopg

with (
    # PgDog 0.1.6 tracks session-control statements on its simple-query path.
    # ClientCursor keeps the gate on that documented transaction-pooler path.
    psycopg.connect(
        os.environ["DATABASE_URL"],
        application_name="f1-client-one",
        cursor_factory=psycopg.ClientCursor,
    ) as first_connection,
    psycopg.connect(
        os.environ["DATABASE_URL"],
        cursor_factory=psycopg.ClientCursor,
    ) as second_connection,
):
    first_connection.autocommit = True
    second_connection.autocommit = True

    with first_connection.transaction(), first_connection.cursor() as cursor:
        # The pinned pooler tracks this value from startup; SET exercises Gres
        # without changing it to a value PgDog 0.1.6 cannot track inside BEGIN.
        cursor.execute("SET application_name = 'f1-client-one'")
        cursor.execute("SELECT current_setting('application_name')")
        assert cursor.fetchone()[0] == "f1-client-one"

    # Both logical clients remain connected while PgDog has exactly one backend.
    # Client two must receive a reset backend, not client one's committed value.
    with second_connection.cursor() as cursor:
        cursor.execute("BEGIN")
        cursor.execute("SHOW application_name")
        second_name = cursor.fetchone()[0]
        assert second_name == "", f"client two inherited application_name={second_name!r}"
        cursor.execute("SET LOCAL statement_timeout = 17")
        cursor.execute("SHOW statement_timeout")
        local_timeout = cursor.fetchone()[0]
        assert local_timeout == "17", f"pinned PgDog SET LOCAL baseline changed: {local_timeout!r}"
        second_connection.rollback()

    with second_connection.transaction(), second_connection.cursor() as cursor:
        cursor.execute("SHOW statement_timeout")
        assert cursor.fetchone()[0] == "0"

    # Returning to client one proves PgDog replays that logical client's state
    # onto the same single backend rather than leaking or losing it.
    with first_connection.transaction(), first_connection.cursor() as cursor:
        cursor.execute("SHOW application_name")
        assert cursor.fetchone()[0] == "f1-client-one"

    # Keep RESET and its observation in one simple-query checkout. PgDog 0.1.6
    # does not retain RESET in its logical parameter map (pinned baseline).
    with first_connection.cursor() as cursor:
        cursor.execute("RESET application_name; SHOW application_name")
        assert cursor.nextset()
        assert cursor.fetchone()[0] == ""

    with second_connection.transaction(), second_connection.cursor() as cursor:
        cursor.execute("SHOW application_name")
        assert cursor.fetchone()[0] == ""
print("PASS: F-1 PgDog GUC transaction-pooler gate")
F1PY

log "PASS: Gres front-door e2e completed"
