#!/usr/bin/env bash
set -euo pipefail

usage() {
  cat >&2 <<'USAGE'
usage: tools/gres-adopt-regress.sh <regress-name>

Fetch src/test/regress/sql/<regress-name>.sql and expected/<regress-name>.out
from a pinned PostgreSQL tag into crates/gres-conformance/corpus-regress/.

Environment:
  POSTGRES_TAG   PostgreSQL git tag to fetch (default: REL_18_0)
  POSTGRES_RAW   raw file URL prefix for the postgres repository
USAGE
}

if [[ $# -ne 1 || "$1" == "-h" || "$1" == "--help" ]]; then
  usage
  exit 2
fi

case "$1" in
  *[!A-Za-z0-9_]* | "")
    echo "regress-name must contain only letters, digits, and underscores" >&2
    exit 2
    ;;
esac

name="$1"
tag="${POSTGRES_TAG:-REL_18_0}"
raw_prefix="${POSTGRES_RAW:-https://raw.githubusercontent.com/postgres/postgres/${tag}}"
repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
target_dir="$repo_root/crates/gres-conformance/corpus-regress/$name"

mkdir -p "$target_dir"

fetch_with_header() {
  local source_path="$1"
  local target_path="$2"
  local source_url="$raw_prefix/$source_path"
  local tmp
  tmp="$(mktemp)"
  trap 'rm -f "$tmp"' RETURN
  curl --fail --location --silent --show-error "$source_url" --output "$tmp"
  {
    printf '%s\n' "-- Vendored from PostgreSQL $tag: $source_path"
    printf '%s\n' '-- License: PostgreSQL License; see repository NOTICE before committing adopted files.'
    cat "$tmp"
  } > "$target_path"
}

fetch_with_header "src/test/regress/sql/$name.sql" "$target_dir/$name.sql"
fetch_with_header "src/test/regress/expected/$name.out" "$target_dir/$name.out"

cat <<EOF
adopted PostgreSQL regress corpus pair: $name
  $target_dir/$name.sql
  $target_dir/$name.out

Before committing adopted files, ensure NOTICE includes PostgreSQL regress corpus
provenance for tag $tag under the PostgreSQL License.
EOF
