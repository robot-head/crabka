# Benchmark Status

**Branch:** `deploy-operator-fixes`  
**Last Updated:** 2026-06-13 (16:25 PT)

---

## 1. Executive Summary & Root Cause Resolution
* **The Problem:** The `crabka/failover` benchmark was failing with `INVALID_REPLICATION_FACTOR (38)` when trying to spin up topics with replication factor 3.
* **The Root Cause:** The Crabka operator was not rendering the `controller_quorum_voters` parameter in the ConfigMap TOML files. This caused all three brokers to run as isolated single-node KRaft clusters, meaning they could not elect a joint quorum or support multi-node replication.
* **The Fix:** We modified the operator to dynamically construct the voter strings (e.g., `id@broker-pod.service.svc.cluster.local:9093`) and pass them to the rendered TOML config. We also updated the broker binary to support voter list parsing with synchronized DNS lookups and retries (to handle startup races where the broker starts before the headless service DNS is fully populated).

---

## 2. Completed Code Changes
The following files have been modified to address the KRaft clustering and DNS resolution issues:
* **[file_config.rs](file:///C:/Users/Matt%20Stone/git/crabka/crates/broker/src/file_config.rs):** Added `controller_quorum_voters` to `FileConfig`, parsing logic, and retried DNS lookups (up to 5 seconds) to avoid startup races.
* **[listeners.rs](file:///C:/Users/Matt%20Stone/git/crabka/crates/operator/src/controller/listeners.rs):** Added `render_broker_toml_with_voters` to append the voter list to the ConfigMap TOML while maintaining compatibility with older tests.
* **[common.rs](file:///C:/Users/Matt%20Stone/git/crabka/crates/operator/src/controller/common.rs):** Modified `render_configmap` to dynamically build the `controller_quorum_voters` list of all brokers and pass it to the rendering logic.
* **Compilation Status:** Standard checks (`cargo check -p crabka-operator -p crabka-broker`) are passing.

---

## 3. Current Blocker: Docker & Packaging Pipeline
We are attempting to package and push the new images for testing on GKE:
* **Constraints:** Packaging must use the official Chainguard containers (`cgr.dev/chainguard/melange` and `cgr.dev/chainguard/apko`) rather than standard Dockerfiles.
* **Docker daemon issues:**
  * When executing `melange` and `apko` commands on the host Windows system via Docker Desktop, the backend daemon (`com.docker.backend` and related VM processes) exits shortly after startup.
  * This is causing connection failures to the named pipe (`npipe:////./pipe/dockerDesktopLinuxEngine`), blocking the `docker run` commands needed to execute `melange` builds.
* **WSL integration:**
  * The WSL Ubuntu distribution reports `The command 'docker' could not be found in this WSL 2 distro.` indicating that WSL integration needs to be fully initialized/synchronized.

---

## 4. Next Steps
1. **Stabilize Docker / WSL Integration:**
   * Get the Docker daemon fully stable on Windows or activate/repair the Docker CLI integration inside the WSL 2 Ubuntu distribution.
2. **Rebuild Packages & Images:**
   * Run the unified `melange` package build inside `cgr.dev/chainguard/melange` to produce APKs for all Crabka binaries.
   * Run `apko` builds inside `cgr.dev/chainguard/apko` to build OCI images for the operator and broker.
3. **Deploy & Benchmark:**
   * Deploy the newly built images to GKE (`test-crabka-cluster`) by upgrading the Helm deployment.
   * Trigger the benchmark matrix via `bench/run-matrix.sh`.
   * Generate the comparative benchmark report (`SUMMARY.md`) using `just bench-report`.
