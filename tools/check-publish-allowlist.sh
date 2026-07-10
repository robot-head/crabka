#!/usr/bin/env bash
set -euo pipefail

metadata_file="$(mktemp)"
trap 'rm -f "${metadata_file}"' EXIT

cargo metadata --no-deps --format-version 1 >"${metadata_file}"

python3 - "${metadata_file}" <<'PY'
import json
import pathlib
import sys
import tomllib

allowlist = {
    "crabka-client-admin",
    "crabka-client-consumer",
    "crabka-client-core",
    "crabka-client-producer",
    "crabka-client-streams",
    "crabka-compression",
    "crabka-connect",
    "crabka-connect-derive",
    "crabka-log",
    "crabka-pgcatalog",
    "crabka-pgexec",
    "crabka-pgkv",
    "crabka-pgmvcc",
    "crabka-pgparser",
    "crabka-pgtypes",
    "crabka-pgwire",
    "crabka-protocol",
    "crabka-schema-serde",
    "crabka-security",
}

metadata_path = pathlib.Path(sys.argv[1])
metadata = json.loads(metadata_path.read_text())
packages = {package["name"]: package for package in metadata["packages"]}

def is_publishable(package):
    publish = package.get("publish")
    return publish is None or publish != []

publishable = {
    name for name, package in packages.items() if is_publishable(package)
}
unexpected_publishable = sorted(publishable - allowlist)
private_allowlisted = sorted(allowlist - publishable)

release_plz = tomllib.loads(pathlib.Path("release-plz.toml").read_text())
release_package_entries = release_plz.get("package", [])
release_names = [package["name"] for package in release_package_entries]
release_entries = {
    package["name"]: package for package in release_package_entries
}
duplicate_release_entries = sorted(
    name for name in set(release_names) if release_names.count(name) > 1
)
unknown_release_entries = sorted(release_entries.keys() - packages.keys())

missing_public_entries = sorted(allowlist - release_entries.keys())
misconfigured_public_entries = sorted(
    name
    for name in allowlist & release_entries.keys()
    if release_entries[name].get("publish") is not True
    or release_entries[name].get("release") is not True
)

private_packages = set(packages) - allowlist
missing_private_entries = sorted(private_packages - release_entries.keys())
misconfigured_private_entries = sorted(
    name
    for name in private_packages & release_entries.keys()
    if release_entries[name].get("publish") is not False
    or release_entries[name].get("release") is not False
)

errors = []
if unexpected_publishable:
    errors.append(
        "unexpected publishable workspace packages:\n"
        + "\n".join(unexpected_publishable)
    )
if private_allowlisted:
    errors.append(
        "allowlisted packages are not publishable in Cargo metadata:\n"
        + "\n".join(private_allowlisted)
    )
if duplicate_release_entries:
    errors.append(
        "duplicate release-plz package entries:\n"
        + "\n".join(duplicate_release_entries)
    )
if unknown_release_entries:
    errors.append(
        "release-plz package entries without workspace packages:\n"
        + "\n".join(unknown_release_entries)
    )
if missing_public_entries:
    errors.append(
        "allowlisted packages missing release-plz public entries:\n"
        + "\n".join(missing_public_entries)
    )
if misconfigured_public_entries:
    errors.append(
        "allowlisted packages without release-plz publish=true/release=true:\n"
        + "\n".join(misconfigured_public_entries)
    )
if missing_private_entries:
    errors.append(
        "private packages missing release-plz private entries:\n"
        + "\n".join(missing_private_entries)
    )
if misconfigured_private_entries:
    errors.append(
        "private packages without release-plz publish=false/release=false:\n"
        + "\n".join(misconfigured_private_entries)
    )

if errors:
    print("\n\n".join(errors), file=sys.stderr)
    print(
        "add publish = false, update release-plz.toml, or update the allowlist intentionally",
        file=sys.stderr,
    )
    raise SystemExit(1)
PY
