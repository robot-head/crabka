# Crabka documentation site

`.github/workflows/docs.yml` builds this site with
[Zola](https://www.getzola.org/) and deploys it to GitHub Pages on each push to
`main`.

## Local preview

    # 1. Generate the reference tree from source (operator CRDs, broker config,
    #    topic configs, protocol API table).
    cargo run -p crabka-docgen -- all --out website/content/docs/reference

    # 2. (Optional) build rustdoc and stage it under /api/rust/.
    cargo doc --no-deps --workspace
    mkdir -p website/static/api/rust && cp -r target/doc/* website/static/api/rust/

    # 3. Serve with live reload (requires Zola >= 0.22).
    cd website && zola serve

Git ignores the generated content under `content/docs/reference/` and everything
under `static/api/` and `static/images/`. The build regenerates these files.
Never commit them.
