# Artifactum architecture

## Invariants

Artifactum is designed around a few invariants rather than provider-specific download behavior.

1. The core owns artifact identity.
2. Providers never commit blobs into the CAS.
3. A provider reference is opaque beyond its leading scheme.
4. Resolution and acquisition are separate operations.
5. Credentials are runtime inputs, never durable artifact identity.
6. A lockfile records enough provider-owned state to reacquire a resolved artifact without re-resolving mutable names.
7. The subprocess protocol and in-process trait expose the same conceptual operations.
8. Provider crates are independently versioned and installable; the main CLI does not accumulate one Cargo feature per provider.

## Data flow

```text
ArtifactRequirement
        │
        ▼
 ArtifactProvider::resolve
        │
        ▼
    Resolution
        │
        │ one ResolvedFile at a time
        ▼
 ArtifactProvider::acquire
        │
        ▼
 host-owned staging path
        │
        ▼
 SHA-256 + integrity check
        │
        ▼
      CAS blob
        │
        ▼
  StoredArtifact manifest
        │
        ├── pin / GC reachability
        │
        └── materialize ordinary tree
```

A `Resolution` is provider-facing. It can contain provider-specific source state required for reacquisition. A `StoredArtifact` is store-facing: it contains only safe artifact paths and host-computed CAS identities.

## Core provider API

The essential API is intentionally narrow:

```rust
#[async_trait]
pub trait ArtifactProvider: Send + Sync + 'static {
    fn descriptor(&self) -> ProviderDescriptor;

    async fn resolve(
        &self,
        requirement: &ArtifactRequirement,
        context: &ResolveContext,
    ) -> Result<Resolution>;

    async fn acquire(
        &self,
        file: &ResolvedFile,
        destination: &Path,
        context: &AcquireContext,
    ) -> Result<Acquisition>;

    async fn search(
        &self,
        request: &SearchRequest,
        context: &ResolveContext,
    ) -> Result<Vec<SearchResult>>;
}
```

`search` has a default `Unsupported` implementation. Provider capabilities advertise optional behavior to callers.

## Static and plugin providers

A provider crate contains its implementation in a normal Rust library:

```text
artifactum-provider-huggingface/
├── src/lib.rs       # HuggingFaceProvider: ArtifactProvider
└── src/main.rs      # artifactum_plugin_protocol::serve(provider)
```

Applications that need a fixed provider set link the library directly. The generic Artifactum CLI discovers the executable on `PATH` and wraps it in `PluginProvider`, which itself implements `ArtifactProvider`. Code above the registry is therefore agnostic about the boundary.

There is intentionally no `cdylib` plugin ABI. Rust has no stable language ABI for this use case, and a subprocess boundary permits provider implementations to evolve independently.

## Why the provider owns reacquisition state

A generic transport URL is often the wrong durable identity. A service may return a temporary signed URL or select a storage backend dynamically. Therefore `ResolvedFile::source` is opaque provider state.

Examples:

```json
{"bucket":"models","key":"foo/model.onnx","version_id":"abc"}
```

or:

```json
{"asset_id":12345,"api_url":"https://api.github.com/repos/.../assets/12345"}
```

The provider can later turn that identity into whatever temporary transport operation is necessary. The current first-party providers use ordinary URLs where those URLs are durable enough, but the type boundary does not require that.

The generic HTTP provider is an explicit escape hatch: its URL is the durable identity. Artifactum cannot infer whether an arbitrary query parameter is a credential, so projects must not put presigned/signed URLs or embedded credentials in direct HTTP requirements intended for source control. Semantic providers should instead persist stable IDs and mint temporary URLs only during `acquire`.

## Lockfiles

`Artifacts.toml` is mutable project intent. `Artifacts.lock` is resolved state.

The lockfile stores:

- provider name;
- canonical reference;
- resolved revision;
- stored manifest SHA-256;
- a SHA-256 fingerprint of the originating project requirement, used to reject stale `--locked` fetches;
- each artifact path;
- each host-computed SHA-256;
- byte size;
- media type when available;
- provider-owned reacquisition state, encoded as opaque JSON text so TOML cannot narrow the provider value model.

`requirement_digest` fingerprints the complete serialized `ArtifactRequirement` (source, explicit revision, selection and metadata). This intentionally treats even semantically harmless manifest edits such as include-order changes as lockfile drift.

`artifactum fetch --locked` reconstructs a `Resolution` from the lockfile and skips semantic resolution. Existing blobs are reused. Missing blobs are reacquired from the provider and verified against the locked digest.

`--frozen` sets both locked and offline behavior, requiring all locked blobs to already exist.

## CAS and manifests

Blob paths are derived only from SHA-256:

```text
blobs/sha256/<first-two-hex>/<full-hex>
```

Stored artifact manifests are serialized as deterministic struct-shaped JSON and are themselves SHA-256 addressed under `manifests/sha256/`.

Pins refer to manifest digests. GC computes the set of blob digests reachable from pinned manifests and removes everything else.

The current GC is intentionally simple. Future versions should also retain manifests referenced by explicit leases, active project roots, and potentially a configurable recent-use window.

## Materialization

The CAS is not an application-facing filesystem layout. `materialize` reconstructs a tree from a `StoredArtifact`.

Current modes:

- `hardlink`
- `copy`
- `auto` (hardlink then copy)

Planned modes:

- reflink / clonefile;
- symlink as an explicit opt-in;
- read-only materializations;
- atomic whole-tree swaps.

## Provider vs transport vs extractor

These should remain separate concepts.

- **Provider**: understands semantic identity and revisions (`huggingface`, `github`, `wandb`, `mlflow`).
- **Transport**: moves bytes (`HTTP`, `S3`, `OCI`, `Git`, provider-native Xet).
- **Extractor**: interprets containers (`tar`, `zip`, `7z`).
- **Transform**: derives another artifact (`ONNX optimization`, `GGUF conversion`, model sharding).
- **Store**: owns content identity and persistence.
- **Resolver**: orchestrates the graph.

The first implementation ships provider plugins plus a shared `artifactum-transport-http` crate used by HTTP-backed providers. Extractor/transform plugins should reuse the same dual library/executable pattern but use a separate capability protocol rather than pretending an archive is a remote provider.
