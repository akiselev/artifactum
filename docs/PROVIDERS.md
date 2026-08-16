# Provider ecosystem plan

Artifactum should keep providers independently installable rather than accumulating provider features in the main CLI.

## Tier 0: implemented in this workspace

| Provider | Scheme | Status |
| --- | --- | --- |
| Local filesystem | `local:`, `file:` | implemented; file + recursive directory resolution |
| HTTP/HTTPS | `http:`, `https:` | implemented; optional `#sha256=` integrity hint |
| GitHub Releases | `github:`, `gh:` | implemented; latest/tag release assets, token auth |
| Hugging Face Hub | `huggingface:`, `hf:` | implemented; model/dataset/Space metadata, selection, search, token auth |

The current Hugging Face provider uses the public Hub API plus resolved download URLs. A production follow-up should evaluate delegating acquisition to `hf-hub` 1.x so Artifactum automatically inherits Hugging Face's native cache/Xet transfer behavior while still importing completed bytes into Artifactum's CAS.

## Tier 1: high-value general providers

- OCI registries / ORAS: `oci:`
- S3 and S3-compatible object storage: `s3:`
- Google Cloud Storage: `gs:`
- Azure Blob Storage: `azblob:`
- Git repositories: `git:`
- Git LFS
- GitLab Releases
- GitLab Generic Package Registry
- Bitbucket Downloads

S3 compatibility should cover R2, MinIO, Backblaze B2 S3, DigitalOcean Spaces, Wasabi and similar services without separate provider implementations unless their semantic APIs add value.

## Tier 2: ML/model registries

- ModelScope
- Kaggle models/datasets/competition resources
- Weights & Biases Artifacts
- MLflow artifacts/model registry
- DVC remotes/artifacts
- Civitai
- OpenXLab / OpenDataLab

These should be providers rather than generic HTTP aliases because mutable stages, versions, metadata and authentication are part of their semantic identity.

## Tier 3: scientific/research artifact sources

- Zenodo
- Figshare
- OSF
- Dataverse
- Dryad
- DOI resolver
- arXiv source/supplementary artifacts where useful

These are important for workflows where the reproducible identity is a record/version/DOI rather than a raw URL.

## Tier 4: generic remote storage

- WebDAV
- SFTP/SSH
- FTP (only where unavoidable)
- Dropbox
- Google Drive
- OneDrive / SharePoint

Generic remote storage providers should prefer durable object/version IDs over share URLs when their APIs expose them.

## Tier 5: content-addressed/distributed

- IPFS / CID
- BitTorrent / magnet references where appropriate

Content-addressed upstream systems map especially well onto Artifactum because their provider identity can often include an integrity identity before acquisition.

## Not initially targeted

Crates.io, PyPI, npm, Maven, NuGet, Conda, Go modules and RubyGems already have mature package managers. Adapters may eventually be useful for cross-language build/data pipelines, but Artifactum's first priority is large heterogeneous artifacts that are poorly served by language dependency managers.

## Shared transport crates

Provider implementations should reuse transport libraries where it reduces duplicated code, without exposing transport as the public artifact identity.

Likely internal crates:

```text
artifactum-transport-http
artifactum-transport-object-store
artifactum-transport-git
artifactum-transport-oci
```

For example, Zenodo and Figshare can both resolve record semantics and delegate ordinary file transfer to the HTTP transport; MLflow may resolve a model version to an object-store identity; Hugging Face may use provider-native Xet acquisition.
