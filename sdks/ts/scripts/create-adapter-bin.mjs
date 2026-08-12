import { chmodSync, mkdirSync, writeFileSync } from "node:fs";

mkdirSync("bin", { recursive: true });
writeFileSync("bin/conformance-adapter", "#!/usr/bin/env node\nimport '../dist/src/conformance-adapter.js';\n");
chmodSync("bin/conformance-adapter", 0o755);
