# Artifactum providers

Artifactum provider packages are independently installable. Concrete provider packages include a library and an `artifactum-provider-*` executable; SDK packages are library-only.

## Foundation / native providers

### Local

```text
local:./path
file:/absolute/path
```

Files and recursively selected directory contents resolve without network access. Acquisition is a host `LocalCopy` plan.

### HTTP/HTTPS

```text
https://example.com/model.onnx
https://example.com/model.onnx#sha256=<64 hex>
```

Direct URLs are durable artifact identity. Do not put signed URLs or embedded credentials into a source-controlled project file.

### GitHub Releases

```text
github:owner/repo
gh:owner/repo@v1.2.3#asset=model-*.onnx
```

Omitting a tag selects the latest release. `GITHUB_TOKEN` or `GH_TOKEN` is read at request/acquisition time.

### Hugging Face

```text
hf:owner/model@main
huggingface:dataset:owner/dataset@main
huggingface:space:owner/space@main
```

`HF_TOKEN` is read at runtime. 401 is surfaced as authentication; 403 is surfaced as a structured manual/gated-access challenge rather than persisted auth state.

### OCI

```text
oci:ghcr.io/org/model:latest
oci:ghcr.io/org/model@sha256:<digest>
```

Tags resolve through the registry to an OCI manifest digest. Artifact files correspond to manifest layers, using the OCI title annotation when available and otherwise a deterministic layer name. Layer SHA-256 descriptors are preserved and host-verified after download.

Profile/env auth defaults to `OCI_USERNAME` / `OCI_PASSWORD`.

### Git / Git LFS

```text
git:https://github.com/org/repo.git#path/to/file
git:ssh://git@example.com/org/repo.git#models/
```

Use project `revision = "branch|tag|commit"` to pin resolution. The provider resolves to a commit ID, recursively lists the selected tree, detects Git LFS pointer files, and uses LFS SHA-256 OIDs as upstream integrity assertions.

A provider-local bare-ish working cache is stored below the user's Artifactum cache by default. Missing LFS objects are fetched with `git lfs fetch` when online. Git and Git LFS therefore need to be installed for LFS-backed repositories.

## OpenDAL storage providers

`artifactum-provider-opendal` centralizes service construction, recursive listing, metadata, lazy acquisition, chunked/concurrent reads, profile merging, and optional object-version reads. Each concrete plugin only enables its own OpenDAL service feature.

### Authority-style providers

Without a profile, the first locator segment supplies the authority:

```text
s3:bucket/path/to/object
gs:bucket/path/to/object
azure:container/path/to/blob
lakefs:repository/path/to/object
swift:container/path/to/object
oss:bucket/path/to/object
obs:bucket/path/to/object
cos:bucket/path/to/object
```

With a profile containing `bucket`, `container`, or `repository`, the entire locator is interpreted as a path:

```toml
[providers.lab]
kind = "s3"
bucket = "models"
endpoint = "https://minio.internal"

[artifacts.foo]
source = "lab:production/foo.bin"
```

S3/GCS/Azure support an explicit single-object `revision` through OpenDAL's version-aware stat/read path.

### Path-style providers

```text
ipfs:<path/config-dependent locator>
sftp:<path>
webdav:<path>
ftp:<path>
hdfs:<path>
webhdfs:<path>
gdrive:<path>
onedrive:<path>
dropbox:<path>
```

These normally need a named profile carrying the backend root/endpoint/auth configuration expected by the relevant OpenDAL service.

Profile values exactly matching `${ENV_NAME}` are expanded at backend construction time. This makes it possible to keep secret bytes out of `Artifacts.toml` while still using native OpenDAL configuration keys.

## Official-client bridge providers

These providers intentionally invoke official/existing ecosystem tools. They are useful when the vendor client already owns nontrivial credential storage, model-registry semantics, or backend dispatch.

### DVC

```text
dvc:<repository>#<artifact-or-path>
```

Uses `dvc get`, with revision/profile options where configured.

### Kaggle

Datasets:

```text
kaggle:dataset:owner/name#path/to/file.csv
```

Models:

```text
kaggle:model:owner/model/framework/variation/version#path/to/file
```

Uses the installed `kaggle` client. Model references include an explicit variation version so the Artifactum source is naturally versioned.

### ModelScope

```text
modelscope:model:owner/model#path
modelscope:dataset:owner/dataset#path
```

Uses `modelscope download`.

### MLflow

```text
mlflow:<artifact-uri>
```

Uses `mlflow artifacts download`. Artifact URIs such as `runs:/...` or `models:/...` remain MLflow-owned semantic identity.

### Weights & Biases

```text
wandb:entity/project/artifact:v12#path/to/file
```

Uses `wandb artifact get`.

### ClearML

```text
clearml:dataset:<dataset-id>#path/to/file
```

Uses `clearml-data get`.

### Comet

```text
comet:workspace/artifacts/name/version#path/to/file
```

Uses `cometx download` in an isolated temporary directory.

### Locking caveat for bridge providers

Where a vendor CLI accepts a mutable alias but does not expose a cheap immutable resolution API through the bridge, Artifactum still protects the CAS after first fetch: the lock records the host-computed SHA-256, so locked reacquisition of changed bytes fails integrity verification rather than silently replacing the artifact. Providers with accessible immutable IDs should progressively move alias resolution into `resolve()`.

## REST/catalog providers

### GitLab repository files

```text
gitlab:namespace/project#path/to/file
```

`revision` selects the Git ref. The provider uses the raw-file API with LFS dereferencing, captures `x-gitlab-commit-id` when available, and uses `GITLAB_TOKEN` by default. Profiles can set `api_base` and `token_env` for self-hosted GitLab.

### NVIDIA NGC

```text
ngc:model:org/name:version#file
ngc:model:org/team/name:version#file
ngc:resource:org/name:version#file
```

Profiles may override `api_base` and `token_env`; default token variable is `NGC_API_KEY`.

### Zenodo

```text
zenodo:<record-id>
```

Resolves record files and respects Artifactum include/exclude selection. Upstream record checksums are retained. Optional auth comes from `ZENODO_TOKEN` or profile `token_env`.

### Figshare

```text
figshare:<article-id>
```

Resolves article files/download URLs and supplied checksums.

### OSF

```text
osf:<file-id>
```

Resolves one OSF file and its current download link. Explicit historical file-version lookup is not yet implemented. Optional token variable: `OSF_TOKEN`.

### Dataverse

With a profile:

```toml
[providers.research]
kind = "dataverse"
base_url = "https://dataverse.example.edu"
token_env = "DATAVERSE_TOKEN"

[artifacts.sample]
source = "research:12345"
```

Without a profile:

```text
dataverse:https://dataverse.example.edu#12345
```

The provider uses the Data Access endpoint and `X-Dataverse-key` when the configured token variable exists.

## Provider packages in this workspace

Concrete provider crates:

```text
artifactum-provider-local
artifactum-provider-http
artifactum-provider-github
artifactum-provider-huggingface
artifactum-provider-oci
artifactum-provider-git
artifactum-provider-s3
artifactum-provider-gcs
artifactum-provider-azure
artifactum-provider-lakefs
artifactum-provider-ipfs
artifactum-provider-sftp
artifactum-provider-webdav
artifactum-provider-ftp
artifactum-provider-hdfs
artifactum-provider-webhdfs
artifactum-provider-gdrive
artifactum-provider-onedrive
artifactum-provider-dropbox
artifactum-provider-swift
artifactum-provider-oss
artifactum-provider-obs
artifactum-provider-cos
artifactum-provider-dvc
artifactum-provider-kaggle
artifactum-provider-modelscope
artifactum-provider-mlflow
artifactum-provider-wandb
artifactum-provider-clearml
artifactum-provider-comet
artifactum-provider-gitlab
artifactum-provider-ngc
artifactum-provider-zenodo
artifactum-provider-figshare
artifactum-provider-osf
artifactum-provider-dataverse
```

SDK crates:

```text
artifactum-provider-opendal
artifactum-provider-command
artifactum-provider-api
```
