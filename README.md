# Artifactum

Artifactum is a provider-extensible dependency manager and content-addressed store for large external artifacts: ML models, datasets, release assets, scientific records, object-store data, Git/LFS content, and generated inputs that do not naturally belong to a language package manager.

Version 0.3 implements the provider waves and core refactors that turn the original downloader into a reusable artifact-resolution substrate:

- semantic provider resolution separated from acquisition;
- host-executed `AcquisitionPlan`s for generic transfers;
- SHA-256 content-addressed storage with partial-artifact GC roots;
- named provider profiles/instances;
- lazy per-file fetches and glob selection;
- bounded concurrent acquisition;
- persistent, multiplexed provider processes owned by a daemonkit-backed local host;
- protocol-level inspect/version/file-list pagination;
- structured authentication/license/terms/manual-approval/tool access challenges;
- OpenDAL-backed storage-provider SDK;
- external-tool bridge SDK for vendors whose official CLI owns complex auth/storage behavior;
- HTTP/JSON provider SDK for catalog-style services;
- 30+ provider implementations, each independently installable.

## Architecture

```text
Artifacts.toml
     │
     ▼
ArtifactRequirement
     │
     ├── provider profile routing  (lab:foo -> s3 provider instance "lab")
     ▼
ArtifactProvider::resolve
     │
     ▼
Resolution ── immutable revision / upstream IDs / per-file source state
     │
     ▼
ArtifactProvider::prepare_acquisition
     │
     ├── Http
     ├── LocalCopy
     ├── ObjectStore
     ├── Git
     ├── OCI
     └── ProviderManaged
     │
     ▼
Artifactum acquisition scheduler
     │
     ├── bounded file concurrency
     ├── host-owned staging files
     ├── HTTP retry/resume
     └── provider-managed fallback where specialization is valuable
     │
     ▼
SHA-256 verification + CAS commit
     │
     ├── partial blob pins
     └── complete stored manifest when every selected file is present
     │
     ▼
materialized file tree
```

Providers never decide CAS identity. Artifactum hashes completed staging files itself, verifies any upstream SHA-256 assertion, and only then commits to the store.

## Workspace

The workspace is intentionally split so the main CLI does not grow one build feature for every backend.

Core/runtime crates:

| Crate | Purpose |
| --- | --- |
| `artifactum-core` | Domain types, provider trait, access challenges, acquisition plans |
| `artifactum-store` | SHA-256 CAS, manifests, pins, partial roots, materialization, GC |
| `artifactum-resolver` | Provider registry, profiles, lazy/concurrent fetch, project + lock formats |
| `artifactum-plugin-protocol` | Protocol 2.0 framing, multiplexed plugin sessions, server adapter |
| `artifactum-plugin-host` | daemonkit-backed persistent cross-invocation provider-session owner |
| `artifactum-transport-http` | Host-owned HTTP transfer with retry and within-call resume |
| `artifactum-provider-opendal` | SDK for independently packaged OpenDAL providers |
| `artifactum-provider-command` | SDK for official vendor CLI bridges |
| `artifactum-provider-api` | SDK for semantic HTTP/JSON catalog providers |
| `artifactum` | CLI binary (`artifactum-cli` package) |

Every concrete provider crate has both a Rust library target and an executable named `artifactum-provider-<name>`. Applications can statically link exactly the providers they want; the generic CLI discovers installed executables dynamically.

## Provider coverage

### Native/semantic and foundational providers

| Provider | Schemes / shape | Implementation |
| --- | --- | --- |
| Local filesystem | `local:`, `file:` | native, host `LocalCopy` plan |
| HTTP(S) | `http:`, `https:` | native, host HTTP plan |
| GitHub Releases | `github:`, `gh:` | GitHub Releases API -> host HTTP plan |
| Hugging Face | `huggingface:`, `hf:` | Hub API -> host HTTP plan, structured gated-access errors |
| OCI registries | `oci:` | `oci-client`; tags resolve to manifest digest and layer SHA-256 |
| Git + Git LFS | `git:` | Git commits + LFS pointer OIDs, local provider cache |
| GitLab repository files | `gitlab:` | API/raw-file resolution -> host HTTP plan |
| NVIDIA NGC | `ngc:` | semantic model/resource file URLs -> host HTTP plan |

### OpenDAL-backed storage providers

Each is a separate Artifactum plugin crate and enables only its OpenDAL service implementation.

| Provider | Scheme(s) |
| --- | --- |
| S3 / S3-compatible | `s3:` |
| Google Cloud Storage | `gcs:`, `gs:` |
| Azure Blob | `azure:`, `azblob:` |
| lakeFS | `lakefs:` |
| IPFS | `ipfs:` |
| SFTP | `sftp:` |
| WebDAV | `webdav:` |
| FTP/FTPS | `ftp:`, `ftps:` |
| HDFS native | `hdfs:` |
| WebHDFS | `webhdfs:` |
| Google Drive | `gdrive:` |
| OneDrive | `onedrive:` |
| Dropbox | `dropbox:` |
| OpenStack Swift | `swift:` |
| Aliyun OSS | `oss:` |
| Huawei OBS | `obs:` |
| Tencent COS | `cos:` |

S3, GCS, and Azure support explicit object-version resolution for a single-file requirement through `revision = "..."`; Artifactum uses OpenDAL's version-aware stat/read operations and locks the returned version/ETag identity when available.

### Official-client bridge providers

These deliberately delegate vendor-specific authentication/storage behavior to the vendor's installed CLI instead of duplicating it inside Artifactum.

| Provider | Tool expected |
| --- | --- |
| DVC | `dvc` |
| Kaggle | `kaggle` |
| ModelScope | `modelscope` |
| MLflow | `mlflow` |
| Weights & Biases | `wandb` |
| ClearML | `clearml-data` |
| Comet | `cometx` |

If the command is absent, providers return a structured `AccessRequirement::ExternalTool` challenge rather than an opaque spawn error.

### Scientific/catalog providers

| Provider | Scheme |
| --- | --- |
| Zenodo | `zenodo:` |
| Figshare | `figshare:` |
| OSF | `osf:` |
| Dataverse | `dataverse:` |

Upstream checksums such as MD5 are preserved as provenance/integrity hints, but Artifactum's own CAS identity is always SHA-256 computed by the host.

## Project format

`Artifacts.toml` format version 2 adds provider profiles:

```toml
version = 2

# Named provider instance. Secrets should normally stay in environment variables;
# config should contain credential variable names, endpoints, bucket IDs, etc.
[providers.lab]
kind = "s3"
endpoint = "https://minio.example.internal"
bucket = "models"
access_key_id = "${LAB_S3_ACCESS_KEY}"
secret_access_key = "${LAB_S3_SECRET_KEY}"

[providers.gitlab_work]
kind = "gitlab"
api_base = "https://gitlab.example.com/api/v4"
token_env = "GITLAB_WORK_TOKEN"

[artifacts.embedding]
source = "hf:BAAI/bge-small-en-v1.5@main"
include = [
  "config.json",
  "tokenizer.json",
  "onnx/model.onnx",
]
materialize = ".artifactum/embedding"

# The leading scheme can be a provider profile name.
[artifacts.private_model]
source = "lab:production/reranker/model.onnx"

[artifacts.internal_source]
source = "gitlab_work:team/models#weights/model.onnx"
revision = "main"
```

A profile name is routing identity. For example `lab:foo/bar` is transformed into a requirement for provider kind `s3` with profile `lab`; the profile identity is preserved in `Artifacts.lock`, so a locked reacquisition goes back through the same provider instance.

### Credential handling

First-party semantic providers do not serialize live auth headers/tokens into resolutions or lockfiles. `ResolvedFile.source` stores stable reacquisition identity; `prepare_acquisition()` re-reads credentials from the active profile/environment when bytes are actually needed.

Do not embed credentials or signed temporary URLs directly in an `http:`/`https:` requirement, because a direct URL is itself artifact identity and is necessarily lockable.

## Lockfile and lazy acquisition

`Artifacts.lock` format version 2 can represent a partially acquired artifact. Each resolved file records:

- path;
- size/media type when known;
- provider-owned reacquisition state;
- optional host-computed SHA-256 when that particular file has been fetched.

The top-level manifest digest remains absent until every resolved file is present and verified in the CAS.

This permits workflows such as:

```bash
artifactum fetch huge-model --file 'tokenizer*'
artifactum fetch huge-model --file 'config.json'
artifactum fetch huge-model --file 'weights/model-00003-*'
```

Artifactum merges those partial results into the lockfile. If the final missing files are fetched later, it finalizes the complete stored manifest without redownloading already verified blobs.

Old file digests are only reused when provider, canonical reference, resolved revision, and provider profile still match. A mutable branch/tag changing revision cannot silently inherit blobs from the prior resolution.

## Concurrent acquisition

`--jobs` controls bounded per-file concurrency:

```bash
artifactum --jobs 16 fetch dataset
```

The scheduler is owned by `artifactum-resolver`, so concurrency policy is consistent across providers. Generic HTTP acquisition is host-owned. OpenDAL/provider-native transfers may also perform internal chunk concurrency where their backend supports it.

## Persistent plugin sessions with daemonkit

Artifactum does not spawn and tear down every provider for every RPC anymore.

The `artifactum-plugin-host` crate uses the `daemonkit` repository pinned in `Cargo.toml` to own a secure local daemon lifecycle. The host:

1. is entered through daemonkit's authenticated private bootstrap path before normal CLI parsing;
2. accepts daemonkit-authenticated local application streams;
3. keeps a `PluginSession` cache keyed by provider executable;
4. multiplexes multiple in-flight protocol requests to each provider process;
5. evicts and respawns a provider session after transport/EOF/protocol failure;
6. leaves provider-originated errors intact rather than retrying them as crashes;
7. idles out after 30 minutes with no active client streams.

Provider child processes use `kill_on_drop`, so daemon shutdown also releases the provider process pool.

The provider executable protocol itself is independent of daemonkit. This keeps provider crates lightweight and lets applications use `artifactum-plugin-protocol::PluginSession` directly if they do not want the cross-process host.

## Plugin installation

Examples:

```bash
cargo install artifactum-provider-huggingface
cargo install artifactum-provider-oci
cargo install artifactum-provider-s3
cargo install artifactum-provider-kaggle
cargo install artifactum-provider-zenodo
```

The CLI searches `ARTIFACTUM_PLUGIN_PATH` and then `PATH` for executables named:

```text
artifactum-provider-*
```

A provider is also a normal Rust crate:

```rust
use artifactum_provider_huggingface::HuggingFaceProvider;
use artifactum_resolver::ArtifactResolver;

# async fn example() -> anyhow::Result<()> {
let resolver = ArtifactResolver::builder()
    .provider(HuggingFaceProvider::new())?
    .max_concurrent_files(8)
    .build()
    .await?;

let resolved = resolver
    .get("hf:BAAI/bge-small-en-v1.5@main")
    .await?;

let weights = resolved.ensure_file("onnx/model.onnx").await?;
println!("{}", weights.digest);
# Ok(())
# }
```

No Rust dynamic-library ABI is involved.

## CLI

Typical project flow:

```bash
artifactum add embedding 'hf:BAAI/bge-small-en-v1.5@main' \
  --include config.json \
  --include tokenizer.json \
  --include onnx/model.onnx

artifactum resolve embedding
artifactum fetch embedding
artifactum fetch embedding --file 'tokenizer*'
artifactum --jobs 16 fetch embedding
artifactum fetch --locked
artifactum fetch --frozen
artifactum materialize embedding --to ./models/embedding
artifactum inspect embedding
artifactum files embedding
artifactum verify embedding
artifactum gc --dry-run
```

Provider profiles:

```bash
artifactum provider add lab \
  --kind s3 \
  --set endpoint=https://minio.example.internal \
  --set bucket=models \
  --set 'access_key_id=${LAB_S3_ACCESS_KEY}' \
  --set 'secret_access_key=${LAB_S3_SECRET_KEY}'

artifactum provider list
artifactum provider remove lab
```

Catalog/discovery protocol:

```bash
artifactum search hf 'bge embedding' --limit 20
artifactum catalog inspect 'hf:BAAI/bge-small-en-v1.5'
artifactum catalog versions 'some-provider:resource'
artifactum catalog files 'some-provider:resource' --revision v3
```

Providers advertise capabilities; unsupported catalog methods return structured `Unsupported` errors rather than requiring every provider to fake version/search semantics.

## Reference examples

```text
local:./fixtures/model
https://example.com/model.onnx#sha256=<64-hex>
github:owner/repo@v1.2.3#asset=model-*.onnx
hf:owner/model@main
huggingface:dataset:owner/dataset@main
oci:ghcr.io/org/model:latest
git:https://github.com/org/models.git#weights/model.gguf
s3:bucket/path/model.onnx
gs:bucket/path/model.onnx
azure:container/path/model.onnx
lakefs:repository/path/to/data
kaggle:dataset:owner/name#file.csv
kaggle:model:owner/model/framework/variation/version#model.bin
modelscope:model:owner/model#model.safetensors
mlflow:runs:/<run-id>/model
wandb:entity/project/artifact:v12#model.onnx
ngc:model:org/name:1.0#model.onnx
gitlab:group/project#models/model.onnx
zenodo:1234567
figshare:1234567
osf:<file-id>
```

Provider-specific details and profile keys live in [`docs/PROVIDERS.md`](docs/PROVIDERS.md).

## Structured access requirements

Providers can return an `AccessChallenge` with one of:

```text
Authentication
LicenseAcceptance
TermsAcceptance
Membership
ManualApproval
ExternalTool
```

This matters for agent-driven workflows. A caller can distinguish “install the Kaggle CLI”, “authenticate”, and “this Hugging Face repository is gated and needs approval” instead of interpreting arbitrary HTTP 403/process errors.

## CAS layout

```text
$CACHE/artifactum/
├── blobs/
│   └── sha256/
├── manifests/
│   └── sha256/
├── refs/
│   └── pins/
├── staging/
└── locks/
```

Pins may reference either a complete manifest or explicit blobs belonging to a partial fetch. GC walks both forms, so lazily fetched pieces are retained even before a complete artifact manifest exists.

## Validation status

The generation environment for this archive does not contain a Rust toolchain, and outbound access to Rust distribution/package infrastructure was unavailable. `cargo check`, `cargo test`, `cargo fmt`, and Clippy therefore could not be executed here.

The workspace is accompanied by `scripts/static_validate.py`, which checks:

- every Cargo manifest parses;
- every workspace crate is covered;
- path dependencies resolve;
- concrete provider crates have library + plugin binary targets;
- plugin binaries enter the common server adapter;
- provider-wave coverage;
- source delimiter balance with strings/comments stripped;
- no obvious persisted `headers` field in resolved provider source state.

Run both validations in a Rust-enabled environment:

```bash
python3 scripts/static_validate.py
./scripts/validate.sh
```

The Rust validation script runs format, workspace check, tests, and Clippy with warnings denied.

## Documentation

- [`docs/ARCHITECTURE.md`](docs/ARCHITECTURE.md) — invariants and data flow
- [`docs/PROTOCOL.md`](docs/PROTOCOL.md) — provider protocol 2.0 + daemon host
- [`docs/PROVIDERS.md`](docs/PROVIDERS.md) — provider matrix, reference syntax, external tools
- [`docs/PROVIDER_AUTHORING.md`](docs/PROVIDER_AUTHORING.md) — building a new dual lib/plugin provider
- [`docs/ROADMAP.md`](docs/ROADMAP.md) — remaining work after provider waves 1–3
