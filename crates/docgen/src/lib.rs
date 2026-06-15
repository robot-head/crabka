//! Generates Zola markdown reference pages from Crabka's in-process
//! source-of-truth data structures (CRDs, broker config schema, topic
//! configs, protocol API catalog).

pub mod broker;
pub mod emit;
pub mod operator;
pub mod schema_md;
pub mod snippets;

/// Rewrite snippet blocks in every `.md` under `content_dir`, pulling code from
/// source files under `crates_dir`. Returns the number of files changed.
///
/// # Errors
/// Returns an error if a directory walk, file read/write, or snippet sync fails.
pub fn sync_snippets(
    content_dir: &std::path::Path,
    crates_dir: &std::path::Path,
) -> anyhow::Result<usize> {
    use std::fs;
    let mut changed = 0;
    let mut stack = vec![content_dir.to_path_buf()];
    while let Some(d) = stack.pop() {
        for entry in fs::read_dir(&d)? {
            let path = entry?.path();
            if path.is_dir() {
                stack.push(path);
            } else if path.extension().is_some_and(|e| e == "md") {
                let before = fs::read_to_string(&path)?;
                let after = snippets::sync_markdown(&before, crates_dir)
                    .map_err(|e| anyhow::anyhow!("{}: {e}", path.display()))?;
                if after != before {
                    fs::write(&path, after)?;
                    changed += 1;
                }
            }
        }
    }
    Ok(changed)
}
