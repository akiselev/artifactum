# Artifactum 0.4

Artifactum is a local-first artifact lifecycle system for large external and derived artifacts. It combines semantic source resolution, immutable content-addressed storage, reproducible transformations, execution history, lineage, verification, and distribution behind one Rust library/CLI stack.

The central model is deliberately split:

```text
mutable external reference                  computation
        |                                       |
        v                                       v
   Requirement                              ActionSpec
        |                                       |
        v                                       v
    Resolution                           immutable inputs
        |                                       |
        v                                       v
   Acquisition -----> Artifact -----> Attempt / Executor
                        ^                    |
                        |                    v
                        +------------- Realization

Content identity is never provenance. The same bytes can be observed from multiple providers or produced by multiple actions without becoming multiple physical objects.
```

## Major capabilities

- SHA-256 global CAS with separate content and semantic artifact identities.
- Blobs, directory trees, logical collections, and optional content-defined chunking for very large derived blobs.
- Atomic tree materialization; copy/hardlink/reflink-mode fallbacks.
- Mutable refs, immutable tags, leases, integrity verification, graph reachability, and GC.
- SQLite metadata plane for actions, attempts, realizations, source observations, attestations, checkpoints, and operational keys.
- Canonical action keys that exclude scheduling/budget/name noise while including inputs, code, command, environment, output contracts, sandbox/network policy, and platform.
- `pure`, `reproducible`, `volatile`, and `effect` cache semantics. Effectful actions receive immutable receipt artifacts rather than being incorrectly replayed from cache.
- Local, bubblewrap, OCI-container, SSH, Slurm, Kubernetes, and executable-plugin executor boundaries.
- Cancellation, timeout/budget accounting, stdout/stderr capture, checkpoints, retry, and determinism auditing.
- `Artifactum.toml` v3 DAGs, `foreach` over trees/collections, fine-grained per-item cache reuse, level-parallel scheduling, refs, source profiles, and source locks.
- Artifactum provider architecture for local/HTTP, GitHub, Hugging Face, GitLab, Git/OCI/NGC bridges, scientific archives, ML/data tools, and a broad storage-provider surface.
- Persistent daemonkit-backed plugin host with request-ID multiplexing and ABI-free JSON protocol.
- Native file/HTTP remote CAS mirroring with digest verification and a minimal Artifactum CAS server.
- in-toto/SLSA representation helpers, attestations, trust policy evaluation, digest/Sigstore/PGP verifier boundaries, OCI-layout export, and ORAS publication.
- Legacy Artifactum 0.3 blob-CAS migration.

## Build

Rust 1.85+ is required.

```bash
cargo build --workspace --all-targets
cargo test --workspace --all-targets
cargo run -p artifactum-cli -- --help
```

For development validation use:

```bash
./scripts/validate.sh
```

For the required real workflow test, read **[AGENT_TESTING.md](AGENT_TESTING.md)** and then run:

```bash
ARTIFACTUM_E2E_KEEP=1 ./scripts/e2e_observe.sh
```

Do not declare the implementation validated merely because unit/integration tests pass. The e2e runbook requires inspecting actual artifacts, cache behavior, lineage, failure recovery, remote round-trips, and store corruption behavior.

## Minimal pipeline

```toml
version = 3

[project]
name = "example"

[artifacts.raw]
source = "local:./documents"

[tasks.extract]
foreach = "@raw"
run = ["my-extractor", "{in.item}", "{out.text}"]
cache = "reproducible"
network = "deny"

[tasks.extract.outputs.text]
kind = "blob"
media_type = "text/markdown"

[tasks.index]
run = ["my-indexer", "{in.docs}", "{out.index}"]
inputs.docs = "extract.text"
cache = "pure"

[tasks.index.outputs.index]
kind = "blob"

[refs.latest-index]
target = "index.index"
```

```bash
artifactum plan index
artifactum run index
artifactum artifact inspect @latest-index
artifactum lineage @latest-index
artifactum run index              # cache hit
```

If a single member of `documents` changes, Artifactum gives that member a new artifact identity. The unchanged mapped actions remain cache hits; only the changed mapped action, the collection realization, and its affected downstream actions execute again.

## External source locking

`Artifactum.lock` freezes mutable external references separately from computational realizations. A normal run re-resolves sources. `--frozen` requires the lock entry to match project intent and its artifact graph to already exist locally.

```bash
artifactum run --frozen index
```

Derived action results belong in the SQLite realization database, not the source lockfile.

## Ad-hoc computation

```bash
artifactum exec \
  --input pdf=report.pdf \
  --output text=blob \
  -- \
  pdftotext '{in.pdf}' '{out.text}'
```

Every declared input becomes immutable in the execution sandbox. Every declared output is imported into the CAS only after successful execution.

## Stores

The default durable store is under the platform XDG data directory, not the cache directory. Scratch/download state lives beneath store staging and may be discarded; committed artifacts are durable until graph-aware GC proves them unreachable.

Useful commands:

```bash
artifactum store stats
artifactum store verify @result
artifactum store gc --dry-run
artifactum store gc
```

## Providers and plugins

Provider plugins are normal executables named `artifactum-provider-*`. Executor plugins use `artifactum-executor-*`. There is no Rust dynamic-library ABI.

The plugin host persists across CLI invocations through daemonkit and multiplexes concurrent request IDs onto long-lived provider processes. Set `ARTIFACTUM_PLUGIN_PATH` to add discovery locations.

Provider-specific network/authentication behavior remains in provider crates. The host owns CAS identity: downloaded bytes are always SHA-256 verified/committed by Artifactum rather than trusting a provider-selected path.

## Compatibility with Artifactum 0.3

Artifactum 0.4 accepts the 0.3/v2 `Artifacts.toml` source/profile format and the CLI automatically falls back to `Artifacts.toml` when the default `Artifactum.toml` is absent. Saving the project writes the v3 model. Existing v2 lockfiles are deliberately **not** treated as v3 frozen locks because store-v2 separates content identity from source provenance; run an ordinary (non-`--frozen`) resolve/fetch once to create `Artifactum.lock` v3. The legacy blob importer is available as `artifactum migrate-legacy <old-store>`.

Provider profiles remain first-class and can be edited without hand-writing TOML:

```bash
artifactum provider add lab s3 --set endpoint=https://minio.example --set bucket=models
artifactum provider profiles
artifactum provider remove lab
```

The native S3/GCS/Azure plugins use OpenDAL and preserve version-aware object identity where supported. Native Git resolution pins commits and understands Git LFS object OIDs. Native OCI resolution pins registry tags to manifest/layer digests before acquisition. Generic long-tail providers remain independently installable plugins.

## Promotion and trust

Verification is separate from naming/release promotion. Attestations are immutable metadata over an artifact; a trust policy can require predicates, issuers, signatures, and a minimum attestation count. `promote` evaluates that policy before creating the release ref, immutable by default:

```bash
artifactum attest add @candidate dev.example.tests/v1 result.json --issuer ci
artifactum verify @candidate --policy release-policy.toml
artifactum promote @candidate release --policy release-policy.toml
```

The provenance crate includes in-toto Statement/link and SLSA provenance emitters plus digest, Sigstore/cosign-command, and PGP/gpg-command verifier boundaries. OCI layout export and ORAS publication provide interoperable distribution.

## Resume, remote caches, and large data

HTTP acquisition journals partial files under a stable acquisition key. Cross-process range resume uses `ETag`/`Last-Modified` with `If-Range`; when the server cannot prove representation continuity, Artifactum restarts from byte zero instead of appending unsafe bytes. Expected upstream SHA-256 is verified before CAS commit.

Artifactum-to-Artifactum remotes are independent of origin providers. File and native HTTP remotes stream content with incremental SHA-256 verification and recursively mirror artifact manifests, Merkle trees, collections, schemas, and CDC chunk manifests. Very large derived blobs can opt into deterministic content-defined chunking with `artifactum artifact import --chunked`.

## Documentation

- [Architecture](docs/ARCHITECTURE.md)
- [Project format](docs/PROJECT_FORMAT.md)
- [Store v2](docs/STORE_V2.md)
- [Execution and cache semantics](docs/EXECUTION.md)
- [Providers and plugins](docs/PLUGINS.md)
- [Validation / observational testing](AGENT_TESTING.md)
