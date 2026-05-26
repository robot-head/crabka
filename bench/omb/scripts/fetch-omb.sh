#!/usr/bin/env bash
# Clone openmessaging-benchmark at the commit pinned in
# bench/omb/.pinned-omb-commit, into <repo-root>/.omb/.
# Re-runs are cheap: if the checkout already exists at the right
# commit we no-op.

set -euo pipefail
source "$(dirname "${BASH_SOURCE[0]}")/common.sh"

require git

UPSTREAM="https://github.com/openmessaging/openmessaging-benchmark.git"

if [[ -d "$OMB_CHECKOUT/.git" ]]; then
  current="$(git -C "$OMB_CHECKOUT" rev-parse HEAD)"
  if [[ "$current" == "$PINNED_OMB_COMMIT" ]]; then
    log "OMB already at pinned commit $PINNED_OMB_COMMIT"
    exit 0
  fi
  log "Updating OMB checkout: $current → $PINNED_OMB_COMMIT"
  git -C "$OMB_CHECKOUT" fetch --depth 50 origin "$PINNED_OMB_COMMIT"
  git -C "$OMB_CHECKOUT" checkout --detach "$PINNED_OMB_COMMIT"
  exit 0
fi

log "Cloning OMB at $PINNED_OMB_COMMIT → $OMB_CHECKOUT"
mkdir -p "$OMB_CHECKOUT"
git clone --filter=blob:none "$UPSTREAM" "$OMB_CHECKOUT"
git -C "$OMB_CHECKOUT" checkout --detach "$PINNED_OMB_COMMIT"
log "Done. $OMB_CHECKOUT now at $(git -C "$OMB_CHECKOUT" rev-parse --short HEAD)."
