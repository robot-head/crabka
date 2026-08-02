#!/usr/bin/env bash
set -euo pipefail
bazel-bin/crates/operator/crabka-operator__bin gen-crds deploy/crds
echo "Regenerated. Review the diff with: git diff deploy/crds"
