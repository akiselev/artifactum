# Store v2

The store is durable application data.

```text
$XDG_DATA_HOME/.../artifactum/store/
  content/sha256/<prefix>/<digest>
  artifacts/sha256/<prefix>/<digest>
  refs/*.json
  leases/*.json
  staging/
  locks/
```

## Content objects

- Blob: exact bytes.
- Tree: canonical manifest of artifact-relative entries and content IDs.
- Collection: canonical logical-key -> ArtifactId mapping.
- Chunked blob: semantic `Blob` whose manifest content points at a deterministic content-defined chunk list and logical whole-file digest.

Chunking is opt-in because external provider checksums normally describe exact whole-file bytes and should remain directly addressable by those SHA-256 values.

## Atomicity

Files are copied/streamed into staging, hashed, then atomically renamed into the digest path where possible. Existing same-digest content is verified before reuse. Tree materialization is assembled into a temporary directory and renamed into place.

## Refs, leases, GC

Mutable refs are human names. Immutable refs/tags cannot be moved in place. Execution leases temporarily root action inputs/code so GC cannot race long-running work.

GC computes transitive reachability across semantic artifacts, schemas, tree contents, collection members, and chunk lists. The metadata layer contributes retained realization/source/checkpoint/attestation roots.

## Compatibility

Artifactum 0.3 used a flat cache-oriented blob layout. `artifactum migrate-legacy <old-cache-root>` imports and verifies those blobs into store-v2. Old provider manifests are intentionally not treated as v2 semantic artifact manifests because v2 separates provenance from content identity.
