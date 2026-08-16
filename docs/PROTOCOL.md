# Artifactum provider protocol 2.0

## Discovery

Executable plugins are named:

```text
artifactum-provider-*
```

The host searches `ARTIFACTUM_PLUGIN_PATH` and then `PATH`. A plugin is launched with:

```text
artifactum-provider-foo --artifactum-plugin
```

stdout is protocol-only; stderr is diagnostics.

## Framing

Provider stdin/stdout use LSP-style framing:

```text
Content-Length: <bytes>\r\n
\r\n
<UTF-8 JSON payload>
```

Frames are capped at 64 MiB.

## Concurrency

Protocol 2.0 is session-oriented. Requests have independent numeric IDs and a provider server may process several concurrently. Responses may therefore arrive out of order.

`PluginSession` maintains an ID -> oneshot map and one stdout dispatcher. stdin writes are serialized only long enough to emit a frame.

## Initialization

Host request:

```json
{
  "jsonrpc": "2.0",
  "id": 1,
  "method": "initialize",
  "params": {
    "protocol_major": 2,
    "protocol_minor": 0
  }
}
```

The response includes negotiated protocol numbers and `ProviderDescriptor`.

Major versions must match. Minor versions are additive within a major version.

## Methods

### `initialize`

Returns provider name, version, schemes, capabilities, and metadata.

### `resolve`

Input:

```text
ArtifactRequirement + ResolveContext
```

Output:

```text
Resolution
```

### `prepare_acquisition`

Input:

```text
ResolvedFile + AcquireContext
```

Output:

```text
AcquisitionPlan
```

This operation must not write bytes. It may mint temporary transport information, evaluate current credentials, or select provider-native acquisition.

### `acquire_managed`

Used only when the returned plan requires provider execution. Input contains:

```text
ResolvedFile + AcquisitionPlan + host-owned staging path + AcquireContext
```

The provider writes exactly the requested file into the staging path. The host hashes and verifies it afterward.

### `search`

Returns:

```rust
SearchPage {
    items,
    next_cursor,
}
```

### `inspect`

Returns provider-specific catalog metadata normalized into `InspectResult`.

### `versions`

Returns paginated `VersionPage` for providers that support version enumeration.

### `files`

Returns paginated `FilePage` without requiring full artifact acquisition.

## Capabilities

`ProviderCapabilities` advertises operations independently:

```text
resolve
acquire
search
inspect
list
versions
push
 auth
range
```

An unsupported optional operation returns the core `Unsupported` error.

## Structured errors

Provider errors can include a serialized `AccessChallenge`. The plugin host preserves this payload across both protocol hops:

```text
provider process
  -> Artifactum provider protocol
  -> daemon host protocol
  -> CLI/application
```

This allows callers to distinguish authentication, gated/manual access, terms/license requirements, membership, and a missing external vendor tool.

## Persistent cross-invocation host

The provider protocol remains ordinary subprocess RPC. A separate daemonkit-backed host owns process persistence:

```text
CLI -> daemonkit AuthenticatedStream -> plugin host -> PluginSession -> provider
```

Host framing is intentionally private/internal and currently consists of one length-prefixed JSON `HostRequest` and `HostResponse` per authenticated connection. daemonkit supplies transport authentication and lifecycle; Artifactum supplies this small application protocol.

The host pools one `PluginSession` per executable. A dead transport/protocol session is removed and respawned once. Provider-originated remote errors are not retried as crashes.

## Security properties

- Provider plugins never receive CAS paths.
- Managed acquisition receives only a random staging path.
- Credentials should not appear in durable `Resolution` source state.
- Host-executed HTTP plans may contain ephemeral auth headers; plans are not lockfile state.
- stdout is reserved for protocol frames, preventing diagnostics from corrupting framing.
- daemonkit authenticates the local host stream and validates private bootstrap mode.

## Future additive methods

Not implemented in 0.3:

- cancellation notifications;
- progress/rate-limit notifications;
- push/publish;
- plugin command descriptions;
- trust/permission manifests;
- streaming plan handoff between host and provider;
- extractor/transform/verifier protocols.
