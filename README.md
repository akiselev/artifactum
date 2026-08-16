# Artifactum

Artifactum is a provider-extensible artifact resolver and content-addressed store for large external artifacts: ML models, datasets, release assets, scientific data, archives, generated assets, and other files that do not naturally belong to a language package manager.

The design separates five concerns:

- **requirements**: what an application or project asks for;
- **providers**: how semantic references such as `hf:org/model@main` are resolved;
- **acquisition**: how a resolved file is written into a host-owned staging path;
- **storage**: SHA-256 content-addressed blobs and immutable stored manifests;
- **materialization**: constructing an ordinary file tree for applications.

Provider crates can be linked directly into an application or installed as executables named `artifactum-provider-*`. The CLI discovers those executables on `PATH`, analogous to Cargo's external subcommand convention, but communicates with them through Artifactum's own versioned protocol.

## Workspace

| Crate | Purpose |
| --- | --- |
| `artifactum-core` | Reference, manifest, selection, digest and `ArtifactProvider` API |
| `artifactum-store` | SHA-256 CAS, staging, verification, materialization, pins and GC |
| `artifactum-resolver` | Provider registry, resolution/acquisition orchestration, project manifest and lockfile |
| `artifactum-plugin-protocol` | Versioned `Content-Length` framed subprocess protocol, plugin server adapter and discovery |
| `artifactum-transport-http` | Shared HTTP response-to-staging transport used by HTTP-backed providers |
| `artifactum` | Main CLI |
| `artifactum-provider-local` | Local file/directory provider; library + plugin binary |
| `artifactum-provider-http` | HTTP/HTTPS provider; library + plugin binary |
| `artifactum-provider-github` | GitHub Releases provider; library + plugin binary |
| `artifactum-provider-huggingface` | Hugging Face model/dataset/Space provider; library + plugin binary |

## Project format

`Artifacts.toml` is intentionally small:

```toml
version = 1

[artifacts.embedding]
source = "hf:BAAI/bge-small-en-v1.5@main"
include = [
  "config.json",
  "tokenizer.json",
  "onnx/model.onnx",
]

[artifacts.tool]
source = "github:owner/project@v1.4.0#asset=tool-*.tar.zst"
materialize = ".models/tool"

[artifacts.fixture]
source = "local:./fixtures/model"
```

`Artifacts.lock` is generated after fetching and records the resolved provider revision, manifest digest, per-file SHA-256 digest, size, and provider-owned reacquisition identity. First-party semantic providers never persist environment-supplied credentials. A direct HTTP URL is itself artifact identity, so URLs containing embedded credentials or signed query parameters should not be committed to `Artifacts.toml`/`Artifacts.lock`.

## CLI

```text
artifactum add embedding 'hf:BAAI/bge-small-en-v1.5@main' \
  --include config.json \
  --include tokenizer.json \
  --include onnx/model.onnx

artifactum resolve embedding
artifactum fetch embedding
artifactum fetch --locked
artifactum fetch --frozen
artifactum files embedding
artifactum path embedding
artifactum path embedding onnx/model.onnx
artifactum inspect embedding
artifactum verify embedding
artifactum gc --dry-run
artifactum provider list
artifactum plugin list
artifactum search hf 'bge embedding' --limit 10
```

`--locked` uses the existing provider resolution recorded in `Artifacts.lock` rather than resolving mutable names again. `--frozen` additionally forbids network acquisition, so every required blob must already exist in the CAS.

Default materializations are placed under `.artifactum/<artifact-name>/`. The global CAS is located in the platform cache directory unless `--store` is supplied.

## Installing provider plugins

Every provider package contains both a library and an executable target. For example:

```text
cargo install artifactum-provider-huggingface
cargo install artifactum-provider-github
```

The resulting executables are:

```text
artifactum-provider-huggingface
artifactum-provider-github
```

When found on `ARTIFACTUM_PLUGIN_PATH` or `PATH`, the main CLI initializes them and registers their advertised schemes.

A provider can also be statically linked:

```rust
use artifactum_provider_huggingface::HuggingFaceProvider;
use artifactum_resolver::ArtifactResolver;

# async fn example() -> anyhow::Result<()> {
let resolver = ArtifactResolver::builder()
    .provider(HuggingFaceProvider::new())?
    .build()
    .await?;

let model = resolver
    .get("hf:BAAI/bge-small-en-v1.5@main")
    .await?;

println!("manifest: {}", model.manifest.digest);
# Ok(())
# }
```

No dynamic Rust ABI is involved and no provider feature flags are required in `artifactum` itself.

## Provider reference syntax

The core parses only `<scheme>:<opaque locator>`. Everything after the first colon belongs to the provider.

Implemented providers currently accept:

```text
local:./path/to/file-or-directory
file:/absolute/path
https://example.com/model.onnx
https://example.com/model.onnx#sha256=<64-hex-digits>
github:owner/repo
GitHub alias: gh:owner/repo@v1.2.3#asset=model-*.onnx
hf:owner/model@main
huggingface:dataset:owner/dataset@main
huggingface:space:owner/space@main
```

For GitHub, omitting a release tag means the latest release. For Hugging Face, omitting a revision means `main`.

## Security/integrity model

Providers do not write into the CAS directly. A provider receives a random path under Artifactum's staging directory. Once acquisition finishes, the host:

1. hashes the completed staging file;
2. compares it with a provider-declared SHA-256 when one exists;
3. takes a per-digest store lock;
4. atomically commits the file to `blobs/sha256/<prefix>/<digest>`;
5. creates a stored artifact manifest that references only host-computed blob identities.

Artifact-relative paths reject absolute paths and `..` traversal before materialization.

## CAS layout

```text
$CACHE/artifactum/
├── blobs/
│   └── sha256/
│       └── ab/
│           └── abcdef...
├── manifests/
│   └── sha256/
├── refs/
│   └── pins/
├── staging/
└── locks/
```

Materialization currently supports hardlinks and copies. `auto` attempts a hardlink and falls back to copying.

## Plugin protocol

Plugins are invoked with the hidden `--artifactum-plugin` flag. stdin/stdout use a JSON RPC-style protocol with LSP-compatible `Content-Length` framing. stderr remains available for diagnostics.

Protocol 1.0 implements:

- `initialize`
- `resolve`
- `acquire`
- `search`

The host currently launches a fresh plugin process for each operation. The wire protocol is deliberately session-capable so a later host can keep providers alive, multiplex request IDs, add cancellation and receive progress notifications without changing provider implementations.

See [`docs/PROTOCOL.md`](docs/PROTOCOL.md).

## Validation status of this archive

The source and Cargo manifests were structurally checked in the generation environment, including parsing every `Cargo.toml`, checking generated Rust source delimiters, and checking the workspace dependency graph mechanically. The environment did **not** contain Rust and could not resolve `sh.rustup.rs`, so `cargo check` / `cargo test` could not be run here. The first action after extracting should therefore be:

```text
cargo fmt --all -- --check
cargo check --workspace
cargo test --workspace
# or run ./scripts/validate.sh for fmt/check/test/clippy
```

Known architectural TODOs are documented in [`docs/ROADMAP.md`](docs/ROADMAP.md), including persistent plugin sessions, cancellation/progress, stale-lock recovery, resumable range acquisition, reflink materialization, provider-native Hugging Face/Xet transfers, and more providers.
