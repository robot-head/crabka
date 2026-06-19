# Vendored JavaScript

Third-party JS served directly from the docs site so pages have **no runtime
dependency on an external CDN**. Files here are committed deliberately.

## `mermaid.min.js`

- **Library:** [Mermaid](https://github.com/mermaid-js/mermaid) (diagram rendering)
- **Pinned version:** `11.4.1`
- **Build:** the standalone UMD bundle (`dist/mermaid.min.js`), which exposes a
  global `mermaid` and has no dynamic chunk imports — load with a plain
  `<script src>` then `mermaid.initialize(...)` + `mermaid.run()`.
- **Used by:** `templates/docs/page.html` and `templates/docs/section.html`,
  gated on a page's `extra.mermaid = true` front matter; authored via the
  `mermaid` shortcode (`templates/shortcodes/mermaid.html`).

### Refreshing to a new version

```sh
ver=11.4.1   # set the target version
curl -sL "https://registry.npmjs.org/mermaid/-/mermaid-${ver}.tgz" -o /tmp/mermaid.tgz
tar -xzf /tmp/mermaid.tgz -C /tmp
cp /tmp/package/dist/mermaid.min.js website/static/js/mermaid.min.js
```

Then bump the pinned version above and confirm `zola build` renders the
diagrams on a page such as `/docs/reference/concepts/failure-scenarios/`.
