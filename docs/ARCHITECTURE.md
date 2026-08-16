# Artifactum architecture

## Core invariants

1. The Artifactum host owns content identity. Providers never choose CAS paths.
2. Resolution and acquisition are distinct. Mutable semantic names are resolved before bytes move.
3. `ResolvedFile::source` is stable provider-owned reacquisition state, not a place for live credentials.
4. Every acquired file is written to a host-created staging path and SHA-256 hashed by the host before CAS commit.
5. Partial acquisition is valid state. A resolved artifact may have zero, some, or all blobs locally.
6. A complete stored manifest exists only when every resolved file has a verified CAS blob.
7. Provider profiles are part of acquisition identity and are preserved by lockfiles.
8. Static providers and external provider processes expose the same `ArtifactProvider` abstraction.
9. Provider process lifetime is an implementation concern below the resolver; daemonkit owns persistent CLI sessions.
10. Provider capabilities are additive. Search/inspect/version/file listing may be unsupported independently.

## Layers

```text
                 project/application
                        │
                        ▼
                 ArtifactResolver
                        │
          ┌─────────────┴─────────────┐
          │                           │
   linked providers          daemon plugin providers
                                  │
                                  ▼
                         daemonkit plugin host
                                  │
                         multiplexed sessions
                                  │
                                  ▼
                        provider executables
          └─────────────┬─────────────┘
                        ▼
                    Resolution
                        │
                        ▼
                AcquisitionPlan
                        │
         ┌──────────────┼──────────────┐
         ▼              ▼              ▼
      host HTTP      LocalCopy    ProviderManaged
                                      │
                         OpenDAL / OCI / Git / vendor CLI
         └──────────────┬──────────────┘
                        ▼
                 staging file
                        │
                  host SHA-256
                        │
                        ▼
                       CAS
                        │
              ┌─────────┴─────────┐
              ▼                   ▼
          partial pin      complete manifest
                                  │
                                  ▼
                            materialization
```

## Requirement, resolution, acquisition

`ArtifactRequirement` is mutable project intent:

```rust
ArtifactRequirement {
    reference,
    revision,
    selection,
    metadata,
}
```

`Resolution` is the provider's concrete interpretation:

```rust
Resolution {
    provider,
    canonical_ref,
    revision,
    files,
    provider_state,
    metadata,
}
```

Each `ResolvedFile` contains an artifact-relative path, optional upstream size/digests/media type, and opaque reacquisition source state.

The provider then produces an `AcquisitionPlan`:

```rust
pub enum AcquisitionPlan {
    Http(HttpAcquisition),
    LocalCopy { source: PathBuf },
    ObjectStore(ObjectStoreAcquisition),
    Git(GitAcquisition),
    Oci(OciAcquisition),
    ProviderManaged { state: serde_json::Value },
}
```

The enum deliberately has more generic plan variants than the current host executes. HTTP and local copy are fully host-executed today. OpenDAL/OCI/Git providers currently use `ProviderManaged` where keeping service-specific dependencies inside independently installable provider crates is preferable to putting every backend into the main binary.

## Provider profiles

A project can define named provider instances:

```toml
[providers.lab]
kind = "s3"
endpoint = "https://minio.internal"
bucket = "models"

[artifacts.foo]
source = "lab:path/to/foo.bin"
```

The resolver routes scheme `lab` to provider kind `s3`, rewrites the request for that provider, and injects `ProviderProfile { name: "lab", ... }` into resolve/acquire contexts. The resolved metadata records `artifactum_profile = "lab"`; `Artifacts.lock` preserves it.

This lets multiple instances of the same provider coexist without separate binaries or Cargo features.

## Lazy acquisition

Resolution does not imply acquisition. `ResolvedArtifact` exposes:

```rust
ensure_file(path)
ensure_matching(globs)
ensure_all()
```

The resolver uses a bounded `buffer_unordered` scheduler. Each selected file is independently checked against the CAS, planned/acquired when missing, verified, and committed.

A `PartialFetch` contains the resolution plus only the newly/known acquired `StoredFile`s. `finalize_cached()` checks every resolved file against the CAS; if all are present it writes the complete `StoredArtifact` manifest.

## Lockfile merging

The lockfile can retain file-level CAS identities across separate lazy fetches. Previous file digests are reusable only when all of these match:

- provider name;
- canonical reference;
- resolved revision;
- provider profile.

This prevents a mutable tag/branch update from reusing stale file identities.

`requirement_digest` separately detects drift in project intent for `--locked` operation.

## CAS and partial GC roots

Complete artifacts are pinned by stored-manifest digest. Partial artifacts are pinned by explicit blob digests:

```json
{
  "name": "project:artifact",
  "manifest": null,
  "blobs": [
    {"algorithm":"sha256","value":"..."}
  ]
}
```

GC traverses both forms. Once an artifact becomes complete, its pin can point to the manifest instead.

## Persistent plugin process model

Provider binaries still implement the ordinary Artifactum protocol and know nothing about daemonkit.

The main CLI uses `artifactum-plugin-host`:

```text
CLI invocation A ─┐
CLI invocation B ─┼─ daemonkit authenticated stream ─> Artifactum host daemon
CLI invocation C ─┘                                      │
                                                         ├─ hf plugin session
                                                         ├─ s3 plugin session
                                                         └─ kaggle plugin session
```

The host uses daemonkit's embedded-service mode. daemonkit owns instance identity, startup serialization, authenticated local transport, generation changes, stale-state repair, shutdown, and process lifecycle. Artifactum owns only its host request framing and provider-session pool.

Provider sessions support concurrent request IDs. A provider process crash/EOF/protocol failure evicts that session and causes one respawn/retry; provider-domain errors are returned unchanged.

The daemon has a 30-minute idle timeout. Provider child processes use Tokio `kill_on_drop` so daemon teardown releases them.

## Credentials and access

Credentials are runtime inputs. First-party semantic HTTP providers store only durable URLs/IDs in `ResolvedFile::source`; `prepare_acquisition` reconstructs headers from the active profile/environment.

Access failures can carry:

```rust
AccessChallenge {
    provider,
    requirement: Authentication | LicenseAcceptance | TermsAcceptance |
                 Membership | ManualApproval | ExternalTool,
    message,
    action_url,
    tool,
}
```

The plugin protocol transports this structured value rather than flattening it into a string.

## OpenDAL provider SDK

Storage plugins share `artifactum-provider-opendal`. A thin provider chooses:

- Artifactum name/schemes;
- OpenDAL service scheme;
- locator interpretation (`Path` or authority + path);
- optional backend-default config;
- whether explicit object versions should use OpenDAL version-aware stat/read.

Provider profile config is merged at runtime and `${ENV}` values are expanded only when constructing the backend. Direct authority-bearing references persist only non-secret authority identity required for locked reacquisition.

## Provider vs transport vs extractor

Artifactum keeps these concepts separate:

- **provider** — semantic identity, versions, catalog metadata;
- **transport/acquisition plan** — how bytes move;
- **store** — content identity and persistence;
- **resolver** — orchestration;
- **extractor** — future container interpretation (`tar`, `zip`, `zstd`);
- **transform** — future reproducible derived artifacts (`GGUF`, ONNX optimization, sharding);
- **verifier** — future provenance/signature policy.

Provider waves 1–3 are implemented here. Extractor/transform/provenance graphs remain a later layer rather than being encoded as providers.
