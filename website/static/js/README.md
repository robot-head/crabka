# Vendored JavaScript

The docs site serves this third-party JavaScript directly, so pages have **no
runtime dependency on an external CDN**. These files are committed deliberately.

## `mermaid.min.js`

- **Library:** [Mermaid](https://github.com/mermaid-js/mermaid) (diagram rendering)
- **Pinned version:** `11.4.1`
- **Build:** the standalone UMD bundle `dist/mermaid.min.js`. It exposes a
  global `mermaid` and has no dynamic chunk imports. Load it with a plain
  `<script src>`, then call `mermaid.initialize(...)` and `mermaid.run()`.
- **Used by:** `templates/docs/page.html` and `templates/docs/section.html`.
  They render a diagram only when the page sets `extra.mermaid = true` in its
  front matter. Authors write the diagrams with the `mermaid` shortcode in
  `templates/shortcodes/mermaid.html`.

### Refreshing to a new version

```sh
ver=11.4.1   # set the target version
curl -sL "https://registry.npmjs.org/mermaid/-/mermaid-${ver}.tgz" -o /tmp/mermaid.tgz
tar -xzf /tmp/mermaid.tgz -C /tmp
cp /tmp/package/dist/mermaid.min.js website/static/js/mermaid.min.js
```

Then update the pinned version above. Confirm that `zola build` renders the
diagrams on a page such as `/docs/reference/concepts/failure-scenarios/`.
