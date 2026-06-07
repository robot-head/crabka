#!/usr/bin/env python3
"""Bump Crabka workspace crate versions in checked-in Cargo files.

The script updates:
  * the root workspace package version,
  * any explicit package versions for workspace crates,
  * Crabka path-dependency version requirements in Cargo.toml files, and
  * Crabka package versions recorded in Cargo.lock.

It intentionally only rewrites versions associated with packages whose names
start with `crabka-` (plus the root workspace package version), so unrelated
third-party dependency versions are left untouched.
"""

from __future__ import annotations

import argparse
import re
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
CRATE_NAME_RE = re.compile(r'^name\s*=\s*"(crabka-[^"]+)"$')
VERSION_RE = re.compile(r'^(?P<prefix>version\s*=\s*)"(?P<version>[^"]+)"(?P<suffix>.*)$')
WORKSPACE_VERSION_RE = re.compile(
    r'(?ms)(^\[workspace\.package\]\n(?:.*?\n)*?^version\s*=\s*)"[^"]+"'
)
CRABKA_DEP_VERSION_RE = re.compile(
    r'(?m)^(?P<prefix>crabka-[A-Za-z0-9_-]+\s*=\s*\{[^\n}]*?\bversion\s*=\s*)"[^"]+"'
)


def replace_workspace_package_version(text: str, version: str) -> str:
    text, count = WORKSPACE_VERSION_RE.subn(rf'\g<1>"{version}"', text, count=1)
    if count != 1:
        raise RuntimeError("could not find [workspace.package] version in root Cargo.toml")
    return text


def replace_crabka_dependency_versions(text: str, version: str) -> str:
    return CRABKA_DEP_VERSION_RE.sub(rf'\g<prefix>"{version}"', text)


def replace_explicit_crate_package_version(text: str, version: str) -> str:
    """Replace an explicit package version when this manifest is a crabka crate."""
    lines = text.splitlines(keepends=True)
    in_package = False
    is_crabka_package = False
    package_version_index: int | None = None

    for index, line in enumerate(lines):
        stripped = line.strip()
        if stripped.startswith('['):
            if in_package:
                break
            in_package = stripped == '[package]'
            continue
        if not in_package:
            continue
        if CRATE_NAME_RE.match(stripped):
            is_crabka_package = True
        if VERSION_RE.match(stripped):
            package_version_index = index

    if is_crabka_package and package_version_index is not None:
        line = lines[package_version_index]
        newline = '\n' if line.endswith('\n') else ''
        bare = line[:-1] if newline else line
        lines[package_version_index] = VERSION_RE.sub(
            rf'\g<prefix>"{version}"\g<suffix>', bare
        ) + newline
    return ''.join(lines)


def update_cargo_toml(path: Path, version: str) -> bool:
    original = path.read_text()
    text = original
    if path == ROOT / 'Cargo.toml':
        text = replace_workspace_package_version(text, version)
    text = replace_explicit_crate_package_version(text, version)
    text = replace_crabka_dependency_versions(text, version)
    if text != original:
        path.write_text(text)
        return True
    return False


def update_cargo_lock(path: Path, version: str) -> bool:
    original = path.read_text()
    lines = original.splitlines(keepends=True)
    in_package = False
    current_is_crabka = False

    for index, line in enumerate(lines):
        stripped = line.strip()
        if stripped == '[[package]]':
            in_package = True
            current_is_crabka = False
            continue
        if in_package and stripped.startswith('name = '):
            current_is_crabka = CRATE_NAME_RE.match(stripped) is not None
            continue
        if in_package and current_is_crabka and stripped.startswith('version = '):
            newline = '\n' if line.endswith('\n') else ''
            lines[index] = f'version = "{version}"{newline}'
            current_is_crabka = False

    text = ''.join(lines)
    if text != original:
        path.write_text(text)
        return True
    return False


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('version', help='target crate version, for example 0.3.1')
    args = parser.parse_args()

    changed: list[Path] = []
    for path in [ROOT / 'Cargo.toml', *sorted((ROOT / 'crates').glob('*/Cargo.toml'))]:
        if update_cargo_toml(path, args.version):
            changed.append(path)

    lock_path = ROOT / 'Cargo.lock'
    if lock_path.exists() and update_cargo_lock(lock_path, args.version):
        changed.append(lock_path)

    for path in changed:
        print(path.relative_to(ROOT))
    print(f'updated {len(changed)} file(s) to {args.version}')
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
