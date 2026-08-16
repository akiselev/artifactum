# Roadmap

## Before publishing 0.1

- Run `cargo fmt`, `cargo check`, `cargo test` on Linux, macOS and Windows.
- Add CI for MSRV 1.85 and stable.
- Add integration tests that spawn provider binaries through the real protocol.
- Decide the crates.io namespace/name availability and repository URL.
- Add stale lock detection/recovery (current store lockfiles time out but do not reclaim dead owners).
- Make whole-tree materialization atomic.
- Define lockfile forward-compatibility rules and golden fixtures.
- Harden Windows file URI/path handling.
- Add provider conformance test helpers.

## Store

- Reflink / clonefile materialization.
- Read-only materialization option.
- Leases for active acquisitions and materializations.
- Recent-use retention policy in addition to pins.
- Manifest GC.
- CAS statistics and `artifactum cache du`.
- Repair operation that reacquires corrupt/missing locked blobs.
- Optional alternate hash algorithms while retaining SHA-256 as baseline interoperability identity.

## Acquisition

- Range/resume negotiation.
- Parallel chunk acquisition.
- Retry/backoff policy split between resolver and provider.
- Bandwidth/concurrency limits.
- Host progress events.
- Cancellation.
- Mirror ordering/fallback.

## Plugin host

Before treating arbitrary shared `PATH` entries as a mature plugin ecosystem, add lazy activation / descriptor caching and an explicit trust policy so merely listing unrelated plugins does not require executing every matching binary. `ARTIFACTUM_PLUGIN_PATH` already provides a narrower discovery path for controlled environments.

The protocol is session-shaped, but the current host intentionally launches a fresh process per call for implementation simplicity. Upgrade it to:

- persistent provider processes;
- monotonically increasing/multiplexed request IDs;
- concurrent requests;
- health checking and restart;
- cancellation;
- progress and rate-limit notifications;
- provider-specific command forwarding;
- protocol minor-version negotiation.

## Providers

Next implementation order:

1. OCI
2. S3-compatible
3. Git + Git LFS
4. ModelScope
5. Kaggle
6. W&B Artifacts
7. MLflow
8. Zenodo
9. Figshare
10. GCS / Azure Blob

## Hugging Face

The initial provider uses Hub REST metadata and resolved HTTP file downloads. Evaluate `hf-hub` 1.x as the acquisition engine to gain provider-native Xet transfers and Hugging Face cache interop without moving CAS ownership out of Artifactum. One clean approach is to ask `hf-hub` to acquire into a provider-local/temp path and then copy/link into the host staging path returned by Artifactum.

## Extractors and transforms

Keep these separate from remote providers.

Proposed package families:

```text
artifactum-extractor-tar
artifactum-extractor-zip
artifactum-extractor-7z
artifactum-extractor-zstd
artifactum-transform-onnx
artifactum-transform-gguf
```

A future artifact graph can express:

```text
remote provider resolution
        ↓
compressed source blob
        ↓
extractor
        ↓
file-tree artifact
        ↓
optional transform
        ↓
derived artifact
```

Derived artifacts should themselves land in the CAS with provenance linking them to source manifest(s), transform identity/version, and canonical transform parameters.
