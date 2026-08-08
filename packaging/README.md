# Crabka container images

The build makes the Crabka images **without a Dockerfile** from APK packages. It
uses Chainguard's [melange](https://github.com/chainguard-dev/melange) to compile
the packages and [apko](https://github.com/chainguard-dev/apko) to assemble the
OCI image. See [`melange/crabka.yaml`](melange/crabka.yaml) for the production
service build and [`melange/crabka-demo.yaml`](melange/crabka-demo.yaml) for the
all-in-one observability demo payload. The per-image [`apko/`](apko) configs give
the image contents.

`.github/workflows/publish-images.yml` compiles the packages natively for each
architecture, `linux/amd64` and `linux/arm64`, without QEMU. It then assembles
one multi-arch image index per service with apko. It publishes each image to both
registries on a `crabka-broker-v*` release tag:

| Image                    | Docker Hub                      | GHCR                                       |
| ------------------------ | ------------------------------- | ------------------------------------------ |
| `crabka-broker`          | `robothead/crabka-broker`          | `ghcr.io/robot-head/crabka-broker`          |
| `crabka-operator`        | `robothead/crabka-operator`        | `ghcr.io/robot-head/crabka-operator`        |
| `crabka-schema-registry` | `robothead/crabka-schema-registry` | `ghcr.io/robot-head/crabka-schema-registry` |
| `crabka-connect-worker`  | `robothead/crabka-connect-worker`  | `ghcr.io/robot-head/crabka-connect-worker`  |
| `bench-driver`           | `robothead/bench-driver`           | `ghcr.io/robot-head/bench-driver`           |

`.github/workflows/publish-demo-image.yml` publishes the observability demo
image manually:

| Image         | GHCR                             |
| ------------- | -------------------------------- |
| `crabka-demo` | `ghcr.io/robot-head/crabka-demo` |

## Creusot verifier toolchain

The `creusot-toolchain` recipe builds the dev and CI verifier image for formal
proofs. The image is single-arch, runs as root, and has no attestation by
design, because the project never ships it to users. See
[`docs/verification.md`](../docs/verification.md) for verifier usage and
pin-management details.

## Architectures

Each user-facing image is a multi-arch OCI index for these platforms:

| Platform      | apko arch | Runs natively on                              |
| ------------- | --------- | --------------------------------------------- |
| `linux/amd64` | `x86_64`  | Intel/AMD hosts                               |
| `linux/arm64` | `aarch64` | **Apple Silicon (M1–M4)**, AWS Graviton, etc. |

The tag points at the index, so `docker pull`, `docker run`, and Kubernetes
select the matching variant automatically. On an Apple Silicon Mac, this is the
native `linux/arm64` image, and there is no emulation:

```sh
docker run --rm mirror.gcr.io/robothead/crabka-broker:latest --version
docker image inspect mirror.gcr.io/robothead/crabka-broker:latest --format '{{.Architecture}}'  # arm64 on Apple Silicon
```

## Attestations

Every user-facing published image carries two cryptographically signed, keyless
attestations from [Sigstore](https://www.sigstore.dev/):

- **SLSA build provenance** records how, where, and from which commit the build
  made the image. See [SLSA v1](https://slsa.dev/).
- **SPDX SBOM** is the software bill of materials that apko generated for the
  image.

The build stores the attestations in GitHub's attestation store and also pushes
them to GHCR as OCI referrers.

### Verifying

Use the GitHub CLI. It resolves the digest, then checks GitHub's attestation
store. This works for both registries:

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

You can also inspect the GHCR referrers directly with `cosign`:

```sh
cosign verify-attestation \
  --type slsaprovenance1 \
  --certificate-oidc-issuer https://token.actions.githubusercontent.com \
  --certificate-identity-regexp '^https://github.com/robot-head/crabka/' \
  ghcr.io/robot-head/crabka-broker:latest
```
