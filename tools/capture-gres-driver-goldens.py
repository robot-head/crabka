#!/usr/bin/env python3
"""Deterministically recapture the F-1 pinned-driver/PgDog connect fixture."""

from __future__ import annotations

import argparse
import datetime
import json
import pathlib
import socket
import subprocess
import sys
import tempfile
import time

ROOT = pathlib.Path(__file__).resolve().parents[1]
FIXTURE = ROOT / "crates/gres-conformance/fixtures/driver-connect-v1.json"
RECORDER = ROOT / "tools/gres-wire-recorder.py"
PGDOG_IMAGE = "ghcr.io/pgdogdev/pgdog:0.1.6"
PGDOG_DIGEST = "sha256:5d21fa668d091ae6ce30e5cb1536c7bcaba1f96b0d492227b1a46852d1f3ab2c"
PGDOG_COMMIT = "c99282e"
POSTGRES_IMAGE = "postgres:18"
POSTGRES_CONTAINER = "crabka-driver-golden-postgres"
PGDOG_CONTAINER = "crabka-driver-golden-pgdog"
CAPTURE_USER = "capture_user"
CAPTURE_PASSWORD = "capture_password"
CAPTURE_DB = "capture"


def run(*args: str, check: bool = True, **kwargs) -> subprocess.CompletedProcess:
    return subprocess.run(args, check=check, text=True, **kwargs)


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
         "--upstream", f"127.0.0.1:{upstream_port}", "--out", str(output)],
        cwd=ROOT,
    )


def rust_driver(driver: str, port: int) -> None:
    password = CAPTURE_PASSWORD
    url = f"postgresql://{CAPTURE_USER}:{password}@127.0.0.1:{port}/{CAPTURE_DB}?sslmode=disable"
    run(
        str(ROOT / "target/debug/crabka-gres-driver-smoke"),
        "--driver", driver,
        "--database-url", url,
        cwd=ROOT,
        stdout=subprocess.DEVNULL,
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
    run(sys.executable, "-c", code, cwd=ROOT, stdout=subprocess.DEVNULL)


def direct_capture(driver: str, postgres_port: int, temp: pathlib.Path) -> dict:
    recorder_port = free_port()
    output = temp / f"direct-{driver}.json"
    recorder = start_recorder(recorder_port, postgres_port, output)
    time.sleep(0.1)
    try:
        if driver == "psycopg":
            psycopg_driver(recorder_port)
        else:
            rust_driver(driver, recorder_port)
        if recorder.wait(timeout=10) != 0:
            raise RuntimeError(f"direct {driver} recorder failed")
    finally:
        if recorder.poll() is None:
            recorder.kill()
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
    config = pgdog_files(temp, pgdog_port, recorder_port)
    run("docker", "rm", "-f", PGDOG_CONTAINER, check=False, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    run(
        "docker", "run", "-d", "--rm", "--network", "host", "--name", PGDOG_CONTAINER,
        "-v", f"{config}:/etc/pgdog:ro", PGDOG_IMAGE,
        "/usr/local/bin/pgdog", "--config", "/etc/pgdog/pgdog.toml",
        "--users", "/etc/pgdog/users.toml", "run",
        stdout=subprocess.DEVNULL,
    )
    try:
        wait_port(pgdog_port)
        if driver == "psycopg":
            psycopg_driver(pgdog_port)
        else:
            rust_driver(driver, pgdog_port)
    finally:
        run("docker", "rm", "-f", PGDOG_CONTAINER, check=False, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    try:
        if recorder.wait(timeout=10) != 0:
            raise RuntimeError(f"PgDog {driver} recorder failed")
    finally:
        if recorder.poll() is None:
            recorder.kill()
    return json.loads(output.read_text())


def cargo_pin(name: str, version: str) -> str:
    lock = (ROOT / "Cargo.lock").read_text()
    marker = f'name = "{name}"\nversion = "{version}"'
    start = lock.find(marker)
    if start < 0:
        raise RuntimeError(f"Cargo.lock does not pin {name} {version}")
    checksum_marker = 'checksum = "'
    checksum_start = lock.find(checksum_marker, start) + len(checksum_marker)
    checksum_end = lock.find('"', checksum_start)
    return f"Cargo.lock registry checksum {lock[checksum_start:checksum_end]}"


def verify_environment() -> None:
    run("cargo", "build", "--locked", "-p", "crabka-gres-conformance", "--bin", "crabka-gres-driver-smoke", cwd=ROOT)
    inspect = run("docker", "image", "inspect", PGDOG_IMAGE, "--format", "{{.Id}}", capture_output=True).stdout.strip()
    if inspect != PGDOG_DIGEST:
        raise RuntimeError(f"PgDog image digest drift: {inspect}")
    requirements = (ROOT / "crates/gres-conformance/requirements-driver-smoke.txt").read_text()
    if "psycopg==3.2.9 --hash=sha256:2fbb46fcd17bc81f993f28c47f1ebea38d66ae97cc2dbc3cad73b37cefbff700" not in requirements:
        raise RuntimeError("psycopg requirement pin drifted")
    run(sys.executable, "-c", "import psycopg; assert psycopg.__version__ == '3.2.9'")


def capture() -> dict:
    verify_environment()
    postgres_port = free_port()
    run("docker", "rm", "-f", POSTGRES_CONTAINER, check=False, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    run(
        "docker", "run", "-d", "--rm", "--name", POSTGRES_CONTAINER,
        "-e", f"POSTGRES_USER={CAPTURE_USER}", "-e", f"POSTGRES_PASSWORD={CAPTURE_PASSWORD}",
        "-e", f"POSTGRES_DB={CAPTURE_DB}", "-p", f"127.0.0.1:{postgres_port}:5432",
        POSTGRES_IMAGE,
        stdout=subprocess.DEVNULL,
    )
    try:
        wait_postgres()
        with tempfile.TemporaryDirectory(prefix="gres-driver-capture-") as raw_temp:
            temp = pathlib.Path(raw_temp)
            direct = {driver: direct_capture(driver, postgres_port, temp) for driver in ("tokio-postgres", "sqlx", "psycopg")}
            backend = {driver: pgdog_capture(driver, postgres_port, temp) for driver in ("tokio-postgres", "sqlx", "psycopg")}
    finally:
        run("docker", "rm", "-f", PGDOG_CONTAINER, check=False, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        run("docker", "rm", "-f", POSTGRES_CONTAINER, check=False, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)

    versions = {"tokio-postgres": "0.7.18", "sqlx": "0.9.0", "psycopg": "3.2.9"}
    sources = {
        "tokio-postgres": cargo_pin("tokio-postgres", "0.7.18"),
        "sqlx": cargo_pin("sqlx", "0.9.0"),
        "psycopg": "requirements-driver-smoke.txt sha256:2fbb46fcd17bc81f993f28c47f1ebea38d66ae97cc2dbc3cad73b37cefbff700",
    }
    target = "postgres:18 via payload-safe TCP recorder; PgDog 0.1.6 backend via the same recorder"
    drivers = []
    for driver in ("tokio-postgres", "sqlx", "psycopg"):
        drivers.append({
            "driver": driver,
            "version": versions[driver],
            "lock_source": sources[driver],
            "capture_target": target,
            "startup_parameters": direct[driver]["startup_parameters"],
            "pgdog_backend_set_batches": backend[driver]["set_batches"],
        })
    return {
        "schema_version": 1,
        "captured_on": datetime.datetime.now(datetime.UTC).date().isoformat(),
        "recapture_command": "python3 tools/capture-gres-driver-goldens.py --write",
        "pgdog": {
            "version": "0.1.6",
            "image": f"{PGDOG_IMAGE}@{PGDOG_DIGEST}",
            "commit": PGDOG_COMMIT,
        },
        "drivers": drivers,
    }


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
