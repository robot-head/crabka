//! Open-time recovery for log directories.
//!
//! - `Segment::open_active` handles partial trailing batches in the
//!   active segment.
//! - [`swap_orphan_recover`] handles `.swap` files left behind by an
//!   interrupted [`crate::compact::atomic_swap`].

use std::{collections::HashSet, path::Path};

use tracing::instrument;

use crate::{error::LogError, name};

/// Heal any `<base>.log.swap` triples found in `dir`:
///
/// - If the matching plain `<base>.log` exists, the swap was in
///   step 1 or 2 (originals still authoritative) → delete the swap
///   triple.
/// - Else the swap was in step 3 mid-rename (originals deleted,
///   `.swap` files complete) → finish the rename to final names.
///
/// Idempotent. Safe to call on every `Log::open`.
#[instrument(
    level = "info",
    skip_all,
    fields(dir = %dir.display(), swaps = tracing::field::Empty),
    err,
)]
pub fn swap_orphan_recover(dir: &Path) -> Result<(), LogError> {
    let entries = std::fs::read_dir(dir)?;
    let mut log_swaps: Vec<i64> = Vec::new();
    let mut existing_log_bases: HashSet<i64> = HashSet::new();
    for entry in entries {
        let entry = entry?;
        let file_name = entry.file_name();
        let Some(name) = file_name.to_str() else {
            continue;
        };
        if let Some(stem) = name.strip_suffix(".log.swap")
            && stem.len() == name::FILENAME_DIGITS
            && let Ok(base) = stem.parse::<i64>()
        {
            log_swaps.push(base);
        }
        if let Ok(base) = name::parse_log_filename(name) {
            existing_log_bases.insert(base);
        }
    }

    tracing::Span::current().record("swaps", log_swaps.len());
    for base in log_swaps {
        let log_swap = swap_triple(dir, base, "log");
        let index_swap = swap_triple(dir, base, "index");
        let timeindex_swap = swap_triple(dir, base, "timeindex");
        let txnindex_swap = swap_triple(dir, base, "txnindex");

        if existing_log_bases.contains(&base) {
            // Orphan partial — discard. The `.txnindex.swap` is only
            // produced when the survivor segment retains aborted txns
            // (`RewriteOutput::txnindex_swap` is `Option`), so it may be
            // absent; `remove_file` no-ops on a missing path.
            let _ = std::fs::remove_file(&log_swap);
            let _ = std::fs::remove_file(&index_swap);
            let _ = std::fs::remove_file(&timeindex_swap);
            let _ = std::fs::remove_file(&txnindex_swap);
        } else {
            // Complete swap interrupted mid-rename — promote.
            std::fs::rename(&log_swap, name::log_path(dir, base))?;
            // The index / timeindex .swap files may not exist if the
            // crash happened *between* the three renames. Tolerate
            // missing sidecars — `Segment::open` accepts empty index files
            // and rebuilds on tail-scan.
            if index_swap.exists() {
                std::fs::rename(&index_swap, name::index_path(dir, base))?;
            } else {
                std::fs::File::create(name::index_path(dir, base))?;
            }
            if timeindex_swap.exists() {
                std::fs::rename(&timeindex_swap, name::timeindex_path(dir, base))?;
            } else {
                std::fs::File::create(name::timeindex_path(dir, base))?;
            }
            // The `.txnindex` is an OPTIONAL sidecar (`atomic_swap` only
            // renames it when the survivor retained aborted txns). Unlike
            // the index / timeindex, a segment with no aborted txns has NO
            // `.txnindex` at all and `Segment::open` tolerates its absence,
            // so we must NOT synthesize an empty one. Promote the survivor
            // `.txnindex.swap` if present; otherwise remove any leftover
            // `.txnindex` at the target offset so a stale pre-swap index
            // can't outlive the segment it described.
            if txnindex_swap.exists() {
                std::fs::rename(&txnindex_swap, name::txnindex_path(dir, base))?;
            } else {
                let _ = std::fs::remove_file(name::txnindex_path(dir, base));
            }
        }
    }
    Ok(())
}

fn swap_triple(dir: &Path, base: i64, ext: &str) -> std::path::PathBuf {
    dir.join(format!("{}.{}.swap", name::format_base_offset(base), ext))
}

#[cfg(test)]
mod tests {
    use assert2::check;
    use tempfile::tempdir;

    use super::*;

    fn touch(path: &std::path::Path) {
        std::fs::File::create(path).unwrap();
    }

    #[test]
    fn discards_swap_when_original_log_still_present() {
        let dir = tempdir().unwrap();
        let p = dir.path();
        touch(&name::log_path(p, 0));
        touch(&name::index_path(p, 0));
        touch(&name::timeindex_path(p, 0));
        touch(&p.join("00000000000000000000.log.swap"));
        touch(&p.join("00000000000000000000.index.swap"));
        touch(&p.join("00000000000000000000.timeindex.swap"));
        touch(&p.join("00000000000000000000.txnindex.swap"));
        swap_orphan_recover(p).unwrap();
        check!(name::log_path(p, 0).exists());
        check!(!p.join("00000000000000000000.log.swap").exists());
        check!(!p.join("00000000000000000000.index.swap").exists());
        check!(!p.join("00000000000000000000.timeindex.swap").exists());
        // The orphaned survivor `.txnindex.swap` is discarded too.
        check!(!p.join("00000000000000000000.txnindex.swap").exists());
    }

    #[test]
    fn promotes_swap_when_original_log_missing() {
        let dir = tempdir().unwrap();
        let p = dir.path();
        // No originals — only .swap triples (= post-step-2, pre-step-3).
        touch(&p.join("00000000000000000000.log.swap"));
        touch(&p.join("00000000000000000000.index.swap"));
        touch(&p.join("00000000000000000000.timeindex.swap"));
        swap_orphan_recover(p).unwrap();
        check!(name::log_path(p, 0).exists());
        check!(name::index_path(p, 0).exists());
        check!(name::timeindex_path(p, 0).exists());
        check!(!p.join("00000000000000000000.log.swap").exists());
    }

    #[test]
    fn promotes_txnindex_swap_when_present() {
        let dir = tempdir().unwrap();
        let p = dir.path();
        // Complete swap (originals gone) whose survivor retained aborted
        // txns, so a `.txnindex.swap` exists alongside the other three.
        touch(&p.join("00000000000000000000.log.swap"));
        touch(&p.join("00000000000000000000.index.swap"));
        touch(&p.join("00000000000000000000.timeindex.swap"));
        touch(&p.join("00000000000000000000.txnindex.swap"));
        swap_orphan_recover(p).unwrap();
        check!(name::log_path(p, 0).exists());
        check!(name::txnindex_path(p, 0).exists());
        check!(!p.join("00000000000000000000.txnindex.swap").exists());
    }

    #[test]
    fn promote_without_txnindex_swap_synthesizes_none_and_clears_stale() {
        let dir = tempdir().unwrap();
        let p = dir.path();
        // A stale `.txnindex` from a prior segment sits at the target
        // offset, but the survivor of THIS swap had no aborted txns, so
        // no `.txnindex.swap` was produced. Recovery must NOT synthesize
        // an empty `.txnindex` and must clear the stale one so it can't
        // outlive the segment it described.
        touch(&name::txnindex_path(p, 0));
        touch(&p.join("00000000000000000000.log.swap"));
        touch(&p.join("00000000000000000000.index.swap"));
        touch(&p.join("00000000000000000000.timeindex.swap"));
        swap_orphan_recover(p).unwrap();
        check!(name::log_path(p, 0).exists());
        // No `.txnindex` is synthesized when the survivor had none.
        check!(!name::txnindex_path(p, 0).exists());
        check!(!p.join("00000000000000000000.txnindex.swap").exists());
    }
}
