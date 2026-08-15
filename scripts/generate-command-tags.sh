#!/usr/bin/env bash
#
# Generate crates/pgexec/src/event_command_tags.rs from PostgreSQL's
# cmdtaglist.h, the one place that says which command tags exist and which of
# them an event trigger may filter on.
#
# usage: generate-command-tags.sh CMDTAGLIST_H OUTPUT_RS

set -euo pipefail

header="${1:?usage: $0 CMDTAGLIST_H OUTPUT_RS}"
out="${2:?usage: $0 CMDTAGLIST_H OUTPUT_RS}"

version="$(sed -nE 's/.*PostgreSQL ([0-9]+\.[0-9]+).*/\1/p' "$header" | head -1)"
version="${version:-18.4}"

{
	cat <<PREAMBLE
// Every command tag PostgreSQL knows, with the two flags event triggers ask
// about. Generated; do not edit by hand.
//
// Source: src/include/tcop/cmdtaglist.h of PostgreSQL ${version}, which holds
// one PG_CMDTAG(symbol, name, event_trigger_ok, table_rewrite_ok, rowcount)
// line per tag. rowcount is dropped: nothing here reports a row count.
// CMDTAG_UNKNOWN ("???") is dropped too, because a lookup miss already means
// "unknown tag" and keeping the sentinel would make '???' a tag that CREATE
// EVENT TRIGGER accepts by name.
//
// Regenerate with:
//
//     scripts/generate-command-tags.sh \\
//         target/pg-regress-postgresql-${version}/source/src/include/tcop/cmdtaglist.h \\
//         crates/pgexec/src/event_command_tags.rs
//
// trigger.rs include!s this file, so it declares no module of its own and
// CommandTag is the type that module defines. Ordinary comments rather than
// doc comments, because an included file is spliced into the middle of a
// module, where an inner doc comment does not parse.

// The tag table, in the header's order, which is alphabetical by name.
const COMMAND_TAGS: &[CommandTag] = &[
PREAMBLE
	grep '^PG_CMDTAG(' "$header" |
		grep -v '^PG_CMDTAG(CMDTAG_UNKNOWN,' |
		sed -E 's/^PG_CMDTAG\([A-Z_0-9]+, ("[^"]*"), (true|false), (true|false), (true|false)\)$/    CommandTag {\n        name: \1,\n        event_trigger_ok: \2,\n        table_rewrite_ok: \3,\n    },/'
	echo '];'
} >"$out"
