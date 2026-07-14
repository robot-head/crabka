#!/usr/bin/env python3
"""Deterministically recapture the F-1 pinned-driver/PgDog connect fixture."""

from __future__ import annotations

import argparse
import datetime
import json
import pathlib
import re
import socket
import subprocess
import sys
import tempfile
import time

ROOT = pathlib.Path(__file__).resolve().parents[1]
FIXTURE = ROOT / "crates/gres-conformance/fixtures/driver-connect-v1.json"
RECORDER = ROOT / "tools/gres-wire-recorder.py"
PGDOG_DIGEST = "sha256:5d21fa668d091ae6ce30e5cb1536c7bcaba1f96b0d492227b1a46852d1f3ab2c"
PGDOG_IMAGE = f"ghcr.io/pgdogdev/pgdog@{PGDOG_DIGEST}"
PGDOG_REVISION = "c99282e9001f66194b03b108ba2a66ad7a27a75d"
POSTGRES_DIGEST = "sha256:22c89fe0d0f507606260237fd55e51f6137f58b2d5bcf6152242b96d9fe8f9a4"
POSTGRES_IMAGE = f"postgres@{POSTGRES_DIGEST}"
POSTGRES_CONTAINER = "crabka-driver-golden-postgres"
PGDOG_CONTAINER = "crabka-driver-golden-pgdog"
CAPTURE_USER = "capture_user"
CAPTURE_PASSWORD = "capture_password"
CAPTURE_DB = "capture"
SUBPROCESS_TIMEOUT = 30
RECORDER_DEADLINE = 20


class CaptureLifecycleError(RuntimeError):
    """Safe summary preserving failure classes without payload-bearing details."""

    def __init__(self, primary: Exception | None, cleanup_failures: list[tuple[str, Exception]]):
        if isinstance(primary, CaptureLifecycleError):
            primary_kind = primary.primary_kind
            inherited = primary.cleanup_failures
        else:
            primary_kind = "none" if primary is None else type(primary).__name__
            inherited = []
        self.primary_kind = primary_kind
        self.cleanup_failures = inherited + [
            (label, type(error).__name__) for label, error in cleanup_failures
        ]
        cleanup = ",".join(f"{label}:{kind}" for label, kind in self.cleanup_failures)
        message = f"capture lifecycle failure: primary={primary_kind}"
        if cleanup:
            message += f"; cleanup={cleanup}"
        super().__init__(message)


def run_with_cleanup(operation, cleanup_actions):
    primary = None
    result = None
    try:
        result = operation()
    except Exception as error:  # cleanup must run for every ordinary failure
        primary = error

    cleanup_failures = []
    for label, cleanup in reversed(cleanup_actions):
        try:
            cleanup()
        except Exception as error:
            cleanup_failures.append((label, error))

    if primary is not None or cleanup_failures:
        raise CaptureLifecycleError(primary, cleanup_failures) from None
    return result


def run(
    *args: str,
    check: bool = True,
    timeout_seconds: float = SUBPROCESS_TIMEOUT,
    **kwargs,
) -> subprocess.CompletedProcess:
    return subprocess.run(
        args,
        check=check,
        text=True,
        timeout=timeout_seconds,
        **kwargs,
    )


def free_port() -> int:
    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        return sock.getsockname()[1]


def wait_port(port: int) -> None:
    deadline = time.monotonic() + 15
    while time.monotonic() < deadline:
        try:
            with socket.create_connection(("127.0.0.1", port), timeout=0.2):
                return
        except OSError:
            time.sleep(0.1)
    raise RuntimeError(f"port {port} did not become ready")


def wait_postgres() -> None:
    deadline = time.monotonic() + 30
    while time.monotonic() < deadline:
        ready = run(
            "docker", "exec", POSTGRES_CONTAINER, "psql",
            "-U", CAPTURE_USER, "-d", CAPTURE_DB, "-Atqc", "SELECT 1",
            check=False,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
        )
        if ready.returncode == 0:
            return
        time.sleep(0.2)
    raise RuntimeError("postgres:18 capture target did not become ready")


def start_recorder(listen_port: int, upstream_port: int, output: pathlib.Path) -> subprocess.Popen:
    return subprocess.Popen(
        [sys.executable, str(RECORDER), "--listen", f"127.0.0.1:{listen_port}",
         "--upstream", f"127.0.0.1:{upstream_port}", "--out", str(output),
         "--deadline-seconds", str(RECORDER_DEADLINE)],
        cwd=ROOT,
    )


def stop_process(process: subprocess.Popen) -> None:
    if process.poll() is None:
        process.kill()
    try:
        process.wait(timeout=5)
    except subprocess.TimeoutExpired:
        process.terminate()
        process.wait(timeout=5)


def reap_recorder(process: subprocess.Popen, wait_for_capture: bool) -> None:
    if wait_for_capture:
        try:
            return_code = process.wait(timeout=10)
        except subprocess.TimeoutExpired:
            stop_process(process)
            raise RuntimeError("recorder did not exit after completed capture") from None
        if return_code != 0:
            raise RuntimeError("recorder exited unsuccessfully")
        return
    stop_process(process)


def remove_container(name: str) -> None:
    removed = run(
        "docker",
        "rm",
        "-f",
        name,
        check=False,
        capture_output=True,
    )
    if removed.returncode != 0 and "No such container" not in (removed.stderr or ""):
        raise RuntimeError("container removal failed")


def rust_driver(driver: str, port: int) -> None:
    password = CAPTURE_PASSWORD
    url = f"postgresql://{CAPTURE_USER}:{password}@127.0.0.1:{port}/{CAPTURE_DB}?sslmode=disable"
    run(
        str(ROOT / "target/debug/crabka-gres-driver-smoke"),
        "--driver", driver,
        "--database-url", url,
        cwd=ROOT,
        stdout=subprocess.DEVNULL,
        timeout_seconds=20,
    )


def psycopg_driver(port: int) -> None:
    code = """
import psycopg
assert psycopg.__version__ == '3.2.9', psycopg.__version__
with psycopg.connect(host='127.0.0.1', port=PORT, user='capture_user',
                     password='capture_password', dbname='capture', sslmode='disable') as conn:
    with conn.cursor() as cur:
        cur.execute('SELECT %s::int4', (61,))
        assert cur.fetchone() == (61,)
""".replace("PORT", str(port))
    run(sys.executable, "-c", code, cwd=ROOT, stdout=subprocess.DEVNULL, timeout_seconds=20)


def direct_capture(driver: str, postgres_port: int, temp: pathlib.Path) -> dict:
    recorder_port = free_port()
    output = temp / f"direct-{driver}.json"
    recorder = start_recorder(recorder_port, postgres_port, output)
    state = {"complete": False}
    cleanups = [("direct recorder", lambda: reap_recorder(recorder, state["complete"]))]

    def operation() -> None:
        time.sleep(0.1)
        if driver == "psycopg":
            psycopg_driver(recorder_port)
        else:
            rust_driver(driver, recorder_port)
        state["complete"] = True

    run_with_cleanup(operation, cleanups)
    captured = json.loads(output.read_text())
    if captured["set_batches"]:
        raise RuntimeError(f"direct {driver} unexpectedly emitted simple-query SET batches")
    return captured


def pgdog_files(temp: pathlib.Path, pgdog_port: int, recorder_port: int) -> pathlib.Path:
    directory = temp / f"pgdog-{pgdog_port}"
    directory.mkdir()
    (directory / "pgdog.toml").write_text(f"""[general]
port = {pgdog_port}
pooler_mode = "transaction"
passthrough_auth = "enabled"
connect_timeout = 5
connect_attempts = 1
checkout_timeout = 5
idle_timeout = 10
server_lifetime = 60

[[databases]]
name = "capture"
host = "127.0.0.1"
port = {recorder_port}
pooler_mode = "transaction"
pool_size = 1
""")
    (directory / "users.toml").write_text("""[[users]]
name = "capture_user"
database = "capture"
password = "capture_password"
""")
    return directory


def pgdog_capture(driver: str, postgres_port: int, temp: pathlib.Path) -> dict:
    recorder_port, pgdog_port = free_port(), free_port()
    output = temp / f"pgdog-backend-{driver}.json"
    recorder = start_recorder(recorder_port, postgres_port, output)
    state = {"complete": False}
    cleanups = [("PgDog recorder", lambda: reap_recorder(recorder, state["complete"]))]

    def operation() -> None:
        config = pgdog_files(temp, pgdog_port, recorder_port)
        remove_container(PGDOG_CONTAINER)
        # Ownership begins before `docker run`: a timeout can occur after Docker
        # creates the named container but before the client receives its id.
        cleanups.append(("PgDog container", lambda: remove_container(PGDOG_CONTAINER)))
        run(
            "docker", "run", "-d", "--rm", "--network", "host", "--name", PGDOG_CONTAINER,
            "-v", f"{config}:/etc/pgdog:ro", PGDOG_IMAGE,
            "/usr/local/bin/pgdog", "--config", "/etc/pgdog/pgdog.toml",
            "--users", "/etc/pgdog/users.toml", "run",
            stdout=subprocess.DEVNULL,
        )
        wait_port(pgdog_port)
        if driver == "psycopg":
            psycopg_driver(pgdog_port)
        else:
            rust_driver(driver, pgdog_port)
        state["complete"] = True

    run_with_cleanup(operation, cleanups)
    return json.loads(output.read_text())


def cargo_pin(name: str, version: str) -> str:
    lock = (ROOT / "Cargo.lock").read_text()
    marker = f'name = "{name}"\nversion = "{version}"'
    start = lock.find(marker)
    if start < 0:
        raise RuntimeError(f"Cargo.lock does not pin {name} {version}")
    checksum_marker = 'checksum = "'
    end = lock.find("[[package]]", start + len(marker))
    block = lock[start : len(lock) if end < 0 else end]
    checksum_start = block.find(checksum_marker) + len(checksum_marker)
    if checksum_start < len(checksum_marker):
        raise RuntimeError(f"Cargo.lock package {name} {version} has no checksum")
    checksum_end = block.find('"', checksum_start)
    return f"Cargo.lock registry checksum {block[checksum_start:checksum_end]}"


def requirement_pin() -> str:
    requirements = (ROOT / "crates/gres-conformance/requirements-driver-smoke.txt").read_text()
    matches = re.findall(r"^psycopg==3\.2\.9 --hash=sha256:([0-9a-f]{64})$", requirements, re.MULTILINE)
    if len(matches) != 1:
        raise RuntimeError("psycopg requirement pin drifted")
    return f"requirements-driver-smoke.txt sha256:{matches[0]}"


def inspect_image(reference: str) -> tuple[str, dict | None]:
    inspected = run(
        "docker", "image", "inspect", reference,
        "--format", "{{json .}}",
        capture_output=True,
    )
    document = json.loads(inspected.stdout)
    return document["Id"], document["Config"].get("Labels")


def verify_environment() -> dict:
    run(
        "cargo", "build", "--locked", "-p", "crabka-gres-conformance",
        "--bin", "crabka-gres-driver-smoke", cwd=ROOT, timeout_seconds=120,
    )
    run("docker", "pull", PGDOG_IMAGE, stdout=subprocess.DEVNULL, timeout_seconds=120)
    run("docker", "pull", POSTGRES_IMAGE, stdout=subprocess.DEVNULL, timeout_seconds=120)
    pgdog_id, labels = inspect_image(PGDOG_IMAGE)
    if pgdog_id != PGDOG_DIGEST:
        raise RuntimeError(f"PgDog image id drift: {pgdog_id}")
    expected_labels = {
        "org.opencontainers.image.revision": PGDOG_REVISION,
        "org.opencontainers.image.source": "https://github.com/pgdogdev/pgdog",
        "org.opencontainers.image.version": "v0.1.6",
    }
    if labels is None or any(labels.get(key) != value for key, value in expected_labels.items()):
        raise RuntimeError("PgDog OCI provenance labels drifted")
    postgres_id, postgres_labels = inspect_image(POSTGRES_IMAGE)
    if postgres_id != POSTGRES_DIGEST:
        raise RuntimeError(f"PostgreSQL image id drift: {postgres_id}")
    if postgres_labels is not None:
        raise RuntimeError("PostgreSQL image unexpectedly gained unvalidated OCI labels")
    requirement_pin()
    run(sys.executable, "-c", "import psycopg; assert psycopg.__version__ == '3.2.9'")
    return {
        "postgres": {"version": "18", "image": POSTGRES_IMAGE, "image_id": postgres_id},
        "pgdog": {
            "version": "0.1.6",
            "image": PGDOG_IMAGE,
            "image_id": pgdog_id,
            "revision": labels["org.opencontainers.image.revision"],
            "source": labels["org.opencontainers.image.source"],
            "oci_version": labels["org.opencontainers.image.version"],
        },
    }


def capture() -> dict:
    provenance = verify_environment()
    postgres_port = free_port()
    remove_container(POSTGRES_CONTAINER)
    cleanups = [
        ("PostgreSQL container", lambda: remove_container(POSTGRES_CONTAINER)),
        ("PgDog container", lambda: remove_container(PGDOG_CONTAINER)),
    ]

    def operation():
        # PostgreSQL ownership also begins before the start attempt because the
        # Docker client can time out after creating the named container.
        run(
            "docker", "run", "-d", "--rm", "--name", POSTGRES_CONTAINER,
            "-e", f"POSTGRES_USER={CAPTURE_USER}", "-e", f"POSTGRES_PASSWORD={CAPTURE_PASSWORD}",
            "-e", f"POSTGRES_DB={CAPTURE_DB}", "-p", f"127.0.0.1:{postgres_port}:5432",
            POSTGRES_IMAGE,
            stdout=subprocess.DEVNULL,
        )
        wait_postgres()
        with tempfile.TemporaryDirectory(prefix="gres-driver-capture-") as raw_temp:
            temp = pathlib.Path(raw_temp)
            direct = {driver: direct_capture(driver, postgres_port, temp) for driver in ("tokio-postgres", "sqlx", "psycopg")}
            backend = {driver: pgdog_capture(driver, postgres_port, temp) for driver in ("tokio-postgres", "sqlx", "psycopg")}
        return direct, backend

    direct, backend = run_with_cleanup(operation, cleanups)

    versions = {"tokio-postgres": "0.7.18", "sqlx": "0.9.0", "psycopg": "3.2.9"}
    sources = {
        "tokio-postgres": cargo_pin("tokio-postgres", "0.7.18"),
        "sqlx": cargo_pin("sqlx", "0.9.0"),
        "psycopg": requirement_pin(),
    }
    target = "pinned PostgreSQL 18 and PgDog 0.1.6 via payload-safe TCP recorder"
    drivers = []
    for driver in ("tokio-postgres", "sqlx", "psycopg"):
        drivers.append({
            "driver": driver,
            "version": versions[driver],
            "lock_source": sources[driver],
            "capture_target": target,
            "startup_parameters": direct[driver]["startup_parameters"],
            "pgdog_backend_startup_parameters": backend[driver]["startup_parameters"],
            "pgdog_backend_set_batches": backend[driver]["set_batches"],
        })
    document = {
        "schema_version": 2,
        "captured_on": datetime.datetime.now(datetime.UTC).date().isoformat(),
        "recapture_command": "python3 tools/capture-gres-driver-goldens.py --write",
        **provenance,
        "drivers": drivers,
    }
    assert_expected_capture(document)
    return document


def assert_expected_capture(document: dict) -> None:
    expected = {
        "tokio-postgres": ({"client_encoding": "UTF8"}, []),
        "sqlx": (
            {"DateStyle": "ISO, MDY", "TimeZone": "UTC", "client_encoding": "UTF8", "extra_float_digits": "2"},
            ["SET \"datestyle\" TO 'ISO, MDY'", "SET \"extra_float_digits\" TO '2'", "SET \"timezone\" TO 'UTC'"],
        ),
        "psycopg": ({}, []),
    }
    backend_startup = {"application_name": "PgDog", "client_encoding": "utf-8"}
    if [capture["driver"] for capture in document["drivers"]] != list(expected):
        raise RuntimeError("driver capture order drifted")
    for capture in document["drivers"]:
        startup, batches = expected[capture["driver"]]
        if capture["startup_parameters"] != startup:
            raise RuntimeError(f"{capture['driver']} direct startup capture drifted")
        if capture["pgdog_backend_startup_parameters"] != backend_startup:
            raise RuntimeError(f"{capture['driver']} PgDog backend startup capture drifted")
        if capture["pgdog_backend_set_batches"] != batches:
            raise RuntimeError(f"{capture['driver']} PgDog backend SET capture drifted")


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--write", action="store_true", help="replace the checked fixture")
    args = parser.parse_args()
    document = json.dumps(capture(), indent=2) + "\n"
    if args.write:
        FIXTURE.write_text(document)
        print(f"wrote {FIXTURE.relative_to(ROOT)}")
    else:
        sys.stdout.write(document)


if __name__ == "__main__":
    main()
