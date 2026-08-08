//! Generates Zola markdown reference pages from Crabka's in-process
//! source-of-truth data structures.
//!
//! These structures are the CRDs, the broker config schema, the topic configs,
//! and the protocol API catalog.

pub mod broker;
pub mod emit;
pub mod operator;
pub mod scenarios;
pub mod schema_md;
pub mod snippets;

/// Rewrite snippet blocks in every `.md` under `content_dir`.
///
/// The function pulls the code from source files under `crates_dir`. It returns
/// the number of files changed.
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

#[cfg(test)]
mod tests {
    use std::fs;

    #[test]
    fn sync_snippets_rewrites_nested_markdown_and_is_idempotent() {
        let dir = tempfile::tempdir().unwrap();
        let crates = dir.path().join("crates");
        let content = dir.path().join("content");
        fs::create_dir_all(crates.join("c/examples")).unwrap();
        fs::create_dir_all(content.join("sub")).unwrap();
        fs::write(
            crates.join("c/examples/e.rs"),
            "// docs:begin a\nlet z = 9;\n// docs:end a\n",
        )
        .unwrap();
        let md = content.join("sub/page.md");
        fs::write(
            &md,
            "intro\n<!-- snippet: c/examples/e.rs#a -->\nOLD\n<!-- /snippet -->\nend\n",
        )
        .unwrap();
        // A non-markdown file is left untouched (extension filter).
        fs::write(content.join("ignore.txt"), "OLD").unwrap();

        let changed = super::sync_snippets(&content, &crates).unwrap();
        assert2::assert!(changed == 1);
        let out = fs::read_to_string(&md).unwrap();
        assert2::assert!(out.contains("```rust\nlet z = 9;\n```"));
        assert2::assert!(!out.contains("OLD"));
        assert2::assert!(fs::read_to_string(content.join("ignore.txt")).unwrap() == "OLD");

        // Second run is a no-op: already in sync.
        assert2::assert!(super::sync_snippets(&content, &crates).unwrap() == 0);
    }
}
