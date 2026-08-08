# Crabka container images

The Crabka images are built **without a Dockerfile** from APK packages using
Chainguard's [melange](https://github.com/chainguard-dev/melange) (compile the
packages) and [apko](https://github.com/chainguard-dev/apko) (assemble the OCI
image). See [`melange/crabka.yaml`](melange/crabka.yaml) for the production
service build, [`melange/crabka-demo.yaml`](melange/crabka-demo.yaml) for the
all-in-one observability demo payload, and the per-image [`apko/`](apko)
configs for the image contents.

`.github/workflows/publish-images.yml` compiles the packages natively per
architecture (`linux/amd64` + `linux/arm64`, no QEMU), assembles a single
multi-arch image index per service with apko, and publishes each to both
registries on a `crabka-broker-v*` release tag:

| Image                    | Docker Hub                      | GHCR                                       |
| ------------------------ | ------------------------------- | ------------------------------------------ |
| `crabka-broker`          | `robothead/crabka-broker`          | `ghcr.io/robot-head/crabka-broker`          |
| `crabka-operator`        | `robothead/crabka-operator`        | `ghcr.io/robot-head/crabka-operator`        |
| `crabka-schema-registry` | `robothead/crabka-schema-registry` | `ghcr.io/robot-head/crabka-schema-registry` |
| `crabka-connect-worker`  | `robothead/crabka-connect-worker`  | `ghcr.io/robot-head/crabka-connect-worker`  |
| `bench-driver`           | `robothead/bench-driver`           | `ghcr.io/robot-head/bench-driver`           |

The observability demo image is published manually by
`.github/workflows/publish-demo-image.yml`:

| Image         | GHCR                             |
| ------------- | -------------------------------- |
| `crabka-demo` | `ghcr.io/robot-head/crabka-demo` |

## Creusot verifier toolchain

The `creusot-toolchain` recipe builds the dev/CI verifier image used for
formal proofs. It is single-arch, runs as root, and is unattested by design
because it never ships to users. See
[`docs/verification.md`](../docs/verification.md) for verifier usage and
pin-management details.

## Architectures

Each user-facing image is a multi-arch OCI index covering:

| Platform      | apko arch | Runs natively on                              |
| ------------- | --------- | --------------------------------------------- |
| `linux/amd64` | `x86_64`  | Intel/AMD hosts                               |
| `linux/arm64` | `aarch64` | **Apple Silicon (M1–M4)**, AWS Graviton, etc. |

Because the tag points at the index, `docker pull` / `docker run` (and
Kubernetes) automatically select the matching variant — on an Apple Silicon Mac
that's the native `linux/arm64` image, no emulation:

```sh
docker run --rm mirror.gcr.io/robothead/crabka-broker:latest --version
docker image inspect mirror.gcr.io/robothead/crabka-broker:latest --format '{{.Architecture}}'  # arm64 on Apple Silicon
```

## Attestations

Every user-facing published image carries two cryptographically signed, keyless
([Sigstore](https://www.sigstore.dev/)) attestations:

- **SLSA build provenance** — how, where, and from which commit the image was
  built ([SLSA v1](https://slsa.dev/)).
- **SPDX SBOM** — the software bill of materials apko generated for the image.

Attestations are stored both in GitHub's attestation store and pushed to GHCR as
OCI referrers.

### Verifying

Using the GitHub CLI (resolves the digest, then checks GitHub's attestation
store — works for both registries):

```sh
# Provenance
gh attestation verify oci://ghcr.io/robot-head/crabka-broker:latest \
  --repo robot-head/crabka

# SBOM (in-toto SPDX predicate)
gh attestation verify oci://ghcr.io/robot-head/crabka-broker:latest \
  --repo robot-head/crabka \
  --predicate-type https://spdx.dev/Document

# Docker Hub mirror verifies the same way
gh attestation verify oci://mirror.gcr.io/robothead/crabka-broker:latest \
  --repo robot-head/crabka
```

The GHCR referrers can also be inspected directly with `cosign`:

```sh
cosign verify-attestation \
  --type slsaprovenance1 \
  --certificate-oidc-issuer https://token.actions.githubusercontent.com \
  --certificate-identity-regexp '^https://github.com/robot-head/crabka/' \
  ghcr.io/robot-head/crabka-broker:latest
```
