# Writing an Artifactum provider

A provider package should expose the implementation as a library and the same implementation through an executable plugin.

## Package shape

```toml
[package]
name = "artifactum-provider-example"

[lib]
name = "artifactum_provider_example"

[[bin]]
name = "artifactum-provider-example"
path = "src/main.rs"
```

The library depends on `artifactum-core`. The binary additionally depends on `artifactum-plugin-protocol`.

## Implement the trait

```rust
use artifactum_core::{ArtifactProvider, ProviderDescriptor};

pub struct ExampleProvider;

#[async_trait::async_trait]
impl ArtifactProvider for ExampleProvider {
    fn descriptor(&self) -> ProviderDescriptor {
        // Advertise a globally sensible provider name and one or more schemes.
        todo!()
    }

    async fn resolve(&self, requirement: &ArtifactRequirement, cx: &ResolveContext)
        -> artifactum_core::Result<Resolution>
    {
        todo!()
    }

    async fn acquire(&self, file: &ResolvedFile, destination: &Path, cx: &AcquireContext)
        -> artifactum_core::Result<Acquisition>
    {
        todo!()
    }
}
```

The core treats the portion after `<scheme>:` as opaque. Provider authors should therefore define reference syntax in their own README rather than trying to extend a central parser.

## Binary adapter

```rust
#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let provider = ExampleProvider::new();
    if std::env::args().any(|arg| arg == artifactum_plugin_protocol::PLUGIN_MODE_FLAG) {
        artifactum_plugin_protocol::serve(provider).await?;
    } else {
        println!("{}", serde_json::to_string_pretty(&provider.descriptor())?);
    }
    Ok(())
}
```

This keeps the library and plugin behavior on exactly the same implementation.

## Resolution rules

A good `resolve` implementation should:

- turn mutable names (`main`, `latest`, aliases, stages) into the strongest provider-native immutable revision available;
- enumerate only files selected by `ArtifactRequirement::selection` when the remote API permits it;
- populate `size` without downloading contents when possible;
- populate SHA-256 if the provider exposes a trustworthy one;
- put only non-secret, durable reacquisition identity in `ResolvedFile::source`;
- avoid signed URLs, bearer tokens, cookies, and other expiring credentials in durable state;
- make `canonical_ref` sufficiently precise for diagnostics and lockfiles.

A provider does **not** need to calculate a digest when the upstream service does not expose one. Artifactum computes SHA-256 after acquisition and writes that digest into the lockfile.

## Acquisition rules

`acquire` owns only the staging destination it receives. It must not:

- write elsewhere in the Artifactum store;
- create its own CAS identity;
- trust a remote checksum as proof that the host received those bytes;
- persist credentials in source metadata.

Resumable acquisition can later be negotiated by protocol capability. In protocol 1.0, a provider should treat the destination as a fresh staging file.

## Authentication

First-party provider implementations currently read conventional environment variables (`HF_TOKEN`, `GITHUB_TOKEN` / `GH_TOKEN`). Future host-mediated credential requests should preserve the same principle: credentials are runtime capabilities, not artifact identity.

## Conformance tests to add next

The workspace should grow a reusable provider test harness covering:

- descriptor validity and unique schemes;
- mutable -> immutable resolution;
- selection include/exclude behavior;
- deterministic resolution;
- missing revision/file errors;
- authentication failures;
- acquisition into an arbitrary host path;
- integrity mismatch handling by the host;
- offline behavior;
- plugin/in-process behavior equivalence.
