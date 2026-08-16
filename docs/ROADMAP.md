# Artifactum roadmap after 0.3

Provider waves 1–3 and the acquisition/profile/lazy/concurrency/persistent-session/protocol/access refactors are implemented in this workspace. The next work should deepen correctness and artifact-graph functionality rather than immediately adding another long tail of storage plugins.

## 0.3 hardening

- Compile/test on Linux, macOS, and Windows; generation environment lacked a Rust toolchain.
- Add provider conformance testkit shared by every plugin.
- Add integration fixtures for S3-compatible MinIO, OCI registry, Git LFS, WebDAV, and GitLab.
- Add deterministic daemonkit lifecycle/fault tests with `daemonkit-testkit`.
- Add process-crash resume journals so HTTP `.partial` data survives CLI/daemon restarts rather than only retrying ranges within one acquisition call.
- Add per-origin concurrency/rate limiting in addition to global file `--jobs`.
- Add timeout/retry policy objects to acquisition context.
- Add atomic whole-tree materialization and reflink/clonefile support.
- Add cache leases so in-flight blobs cannot race GC.
- Add canonical lockfile serialization tests and migration tooling for v1 -> v2.

## Provider semantics to deepen

- Hugging Face: evaluate provider-native Xet acquisition while retaining Artifactum CAS import.
- GitHub/GitLab: richer release/package/file listing and version enumeration.
- S3/GCS/Azure: version-listing APIs and exact backend-specific immutable identity tests.
- DVC/W&B/MLflow/ClearML/Comet: resolve mutable aliases/stages to immutable upstream IDs before invoking bridge acquisition.
- Kaggle: catalog search/inspect/version/file listing through official API semantics.
- ModelScope: search/version listing and immutable revision normalization.
- NGC: catalog APIs and signed-model verification metadata.
- OSF/Dataverse: historical file/dataset version enumeration.
- Scientific providers: DOI meta-provider that redirects to Zenodo/Figshare/Dataverse/etc.

## Plugin security/trust

- Cache plugin descriptors so listing does not execute every discovered binary.
- Trust database keyed by executable path + SHA-256.
- `artifactum plugin trust/revoke/doctor`.
- Environment allow-listing so each plugin receives only declared credentials/config.
- Optional bubblewrap/sandbox execution on Linux.
- Signed provider metadata/package verification where available.
- Protocol cancellation and progress/rate-limit notifications.

## Remote cache

Add Artifactum-to-Artifactum content mirroring independent of origin provider:

```text
locked SHA-256
    -> local CAS
    -> configured remote CAS
    -> origin provider
```

Planned commands:

```text
artifactum cache remote add
artifactum cache push
artifactum cache pull
artifactum cache sync
artifactum serve
```

A minimal read-only cache protocol can expose `HEAD/GET /blobs/sha256/<digest>` and manifest endpoints. Object-store-backed remote caches should use the same CAS namespace.

## Extractor/transform graph

Introduce a separate plugin family rather than overloading providers:

```text
artifactum-extractor-tar
artifactum-extractor-zip
artifactum-extractor-zstd
artifactum-transform-gguf
artifactum-transform-onnx
```

Derived identity should hash:

```text
input manifest digest
+ transform implementation identity/version
+ canonical parameters
```

This makes model conversion/sharding/quantization deterministic and cacheable.

## Provenance/verifiers

Add verifier plugins for trust rather than conflating upstream checksum and authorship:

```text
artifactum-verifier-sigstore
artifactum-verifier-slsa
artifactum-verifier-pgp
```

Artifactum's SHA-256 guarantees byte identity; provenance answers who produced/asserted those bytes and under what process.

## Publishing

Add the inverse of acquisition:

```text
artifactum publish ./tree oci:...
artifactum publish ./tree s3:...
artifactum publish ./tree hf:...
artifactum publish ./tree github:...
```

Publishing should consume a stored manifest/CAS, not arbitrary provider-local paths, so the outbound artifact has known identity before upload.
