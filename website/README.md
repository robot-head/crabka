# Crabka documentation site

Built with [Zola](https://www.getzola.org/) and deployed to GitHub Pages by
`.github/workflows/docs.yml` on push to `main`.

## Local preview

    # 1. Generate the reference tree from source (operator CRDs, broker config,
    #    topic configs, protocol API table).
    cargo run -p crabka-docgen -- all --out website/content/docs/reference

    # 2. (Optional) build rustdoc and stage it under /api/rust/.
    cargo doc --no-deps --workspace
    mkdir -p website/static/api/rust && cp -r target/doc/* website/static/api/rust/

    # 3. Serve with live reload (requires Zola >= 0.22).
    cd website && zola serve

Generated content under `content/docs/reference/` and everything under
`static/api/` and `static/images/` is git-ignored — it is regenerated at build
time and never committed.
