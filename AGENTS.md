# Agent guide

Artifactum is a Rust workspace implementing a provider-extensible artifact manager.

Start with:

1. `README.md`
2. `docs/ARCHITECTURE.md`
3. `crates/artifactum-core/src/lib.rs`
4. `crates/artifactum-resolver/src/lib.rs`
5. `crates/artifactum-plugin-protocol/src/lib.rs`
6. one provider crate

Architectural constraints:

- Do not let providers write directly into the CAS.
- Do not add provider-specific fields to `ArtifactRef`; locator syntax belongs to the provider.
- Do not add a Cargo feature to the main CLI for every new provider.
- Every provider should be usable as a library and, where applicable, an `artifactum-provider-*` executable.
- Never persist secrets or expiring signed URLs as artifact identity.
- Preserve lockfile reproducibility: mutable references resolve before acquisition, and locked fetches skip semantic resolution.
- Keep extraction/transformation separate from provider acquisition.
