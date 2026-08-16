# Authoring an Artifactum provider

## Package shape

A concrete provider should normally be one package with a library and plugin executable:

```text
artifactum-provider-example/
├── Cargo.toml
└── src/
    ├── lib.rs
    └── main.rs
```

```toml
[package]
name = "artifactum-provider-example"

[dependencies]
artifactum-core = { path = "../artifactum-core" }
artifactum-plugin-protocol = { path = "../artifactum-plugin-protocol" }
async-trait = "0.1"
```

`main.rs` should be nearly trivial:

```rust
#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    artifactum_plugin_protocol::serve(
        artifactum_provider_example::provider()
    ).await?;
    Ok(())
}
```

Use the common plugin-mode adapter rather than writing protocol framing manually.

## Provider trait

Implement `ArtifactProvider` with the smallest capability surface the backend genuinely supports.

Required conceptual operations:

```rust
fn descriptor(&self) -> ProviderDescriptor;
async fn resolve(...) -> Result<Resolution>;
async fn prepare_acquisition(...) -> Result<AcquisitionPlan>;
```

If `prepare_acquisition` returns a provider-managed plan, implement `acquire_managed`.

Optional operations:

```text
search
inspect
list_versions
list_files
```

Do not advertise capabilities whose methods just return fabricated data.

## Resolution rules

`resolve()` should do semantic work, not byte transfer:

- turn mutable names into immutable revision IDs where the upstream permits it;
- enumerate/select artifact files;
- capture size/media type;
- retain trustworthy upstream digests;
- put only durable reacquisition state into `ResolvedFile::source`;
- put whole-artifact provider state into `Resolution::provider_state`.

Never put live bearer tokens, cookies, API keys, presigned URLs with short expiry, or other credentials into source/provider state that will be serialized into `Artifacts.lock`.

## Acquisition plans

Prefer a generic plan when practical:

```rust
AcquisitionPlan::Http(...)
AcquisitionPlan::LocalCopy(...)
```

This lets the Artifactum host own retries, staging, and policy.

Use `ProviderManaged` when the transfer needs provider-specific dependencies or behavior that should remain outside the main host binary. Current examples are OpenDAL services, OCI, Git/LFS, and vendor CLI bridges.

Even managed providers only receive a host-owned staging path. They must never construct/write a CAS location.

## Provider profiles

`ResolveContext.profile` / `AcquireContext.profile` contain the named instance that routed the reference.

Profiles are ideal for:

- endpoints;
- bucket/container/repository names;
- tenant/workspace IDs;
- credential *environment-variable names*;
- non-secret service configuration.

If profile values refer to secrets, prefer references such as `${MY_TOKEN}` or `token_env = "MY_TOKEN"` rather than literal secret bytes.

## Structured access errors

Use `AccessChallenge` when user/actionable state blocks resolution/acquisition.

Examples:

```rust
AccessRequirement::Authentication
AccessRequirement::LicenseAcceptance
AccessRequirement::TermsAcceptance
AccessRequirement::Membership
AccessRequirement::ManualApproval
AccessRequirement::ExternalTool
```

This is preferable to `provider_error("HTTP 403")` or `No such file or directory` for a missing vendor executable.

## SDK choices

### OpenDAL-backed storage

Use `artifactum-provider-opendal` when the provider is fundamentally a storage backend. Keep the concrete crate thin and enable only its OpenDAL service feature.

### Official client bridge

Use `artifactum-provider-command` when an installed official/vendor client already provides the correct auth/storage semantics. The SDK provides tool detection, temp directories, checked commands, selection helpers, and structured `ExternalTool` requirements.

### REST/JSON catalog

Use `artifactum-provider-api` for semantic APIs that eventually produce ordinary HTTP file acquisition. It provides:

- shared reqwest client;
- profile token/env helpers;
- header application;
- auth-status normalization;
- percent encoding;
- HTTP acquisition-plan construction.

## Concurrency

Provider protocol 2.0 may call a provider concurrently. Implementations must therefore be `Send + Sync` and avoid global mutable state without synchronization.

Do not assume request-response order on stdout; the server adapter handles request IDs and output serialization.

## Provider conformance expectations

At minimum test:

- reference parsing;
- mutable -> immutable revision resolution where applicable;
- include/exclude selection;
- profile routing/config;
- locked reacquisition state roundtrip;
- missing auth/access behavior;
- offline behavior;
- malformed provider source state;
- acquisition integrity mismatch handled by the host;
- multiple concurrent requests if the provider holds mutable/session state.
