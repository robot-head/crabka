# Crabka Helm charts

Published Helm repository: **https://robot-head.github.io/crabka/charts**

```sh
helm repo add crabka https://robot-head.github.io/crabka/charts
helm repo update
helm search repo crabka
```

| Chart | Description |
| ----- | ----------- |
| `crabka/crabka-operator` | Kubernetes operator for Crabka clusters |
| `crabka/crabka-schema-registry` | Confluent Schema Registry-compatible service |
| `crabka/crabka-rebalancer` | Cruise-Control-equivalent partition rebalancer |

The chart `version` and `appVersion` fields and the default image tags track the
crate release automatically. The package step derives them from the workspace
`Cargo.toml`, so there is no hardcoded version to edit by hand.

## Verifying charts

The project signs every published chart tarball, and each tarball carries
supply-chain provenance. The chart-signing **public key is in this directory**
as [`crabka-charts.pub.asc`](crabka-charts.pub.asc). A mirror is at
`https://robot-head.github.io/crabka/charts/crabka-charts.pub.asc`.

### PGP provenance (`helm install --verify`)

```sh
curl -fsSL https://robot-head.github.io/crabka/charts/crabka-charts.pub.asc \
  | gpg --dearmor > crabka-keyring.gpg
helm install my-op crabka/crabka-operator --verify --keyring ./crabka-keyring.gpg
```

### Keyless cosign signature

Each tarball has a detached Sigstore bundle `<chart>.tgz.cosign.bundle` next to
it in the repository:

```sh
cosign verify-blob \
  --bundle crabka-operator-<version>.tgz.cosign.bundle \
  --certificate-oidc-issuer https://token.actions.githubusercontent.com \
  --certificate-identity-regexp '^https://github.com/robot-head/crabka/' \
  crabka-operator-<version>.tgz
```

### SLSA build provenance attestation

```sh
gh attestation verify crabka-operator-<version>.tgz --repo robot-head/crabka
```

## Maintainers: signing keys

`.github/workflows/docs.yml` does the chart signing:

- **cosign** and **SLSA attestation** are keyless through Sigstore OIDC. They
  need no secrets, and they are active on every non-PR build.
- **PGP `.prov`** activates when you set the `HELM_GPG_KEY`, `HELM_GPG_KEY_ID`,
  and `HELM_GPG_PASSPHRASE` repository secrets. `HELM_GPG_KEY` is the base64
  ASCII-armored private key. The matching public key is
  [`crabka-charts.pub.asc`](crabka-charts.pub.asc). Rotate both keys together.
  Keep the private key and its revocation certificate in a secrets manager.
