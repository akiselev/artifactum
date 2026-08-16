# Artifactum provider plugin protocol 1.0

## Discovery

The host scans `ARTIFACTUM_PLUGIN_PATH` first and then `PATH` for regular files whose names begin with:

```text
artifactum-provider-
```

Windows `.exe` suffixes are ignored when checking the prefix.

The provider process is launched as:

```text
artifactum-provider-foo --artifactum-plugin
```

stdout is reserved for protocol frames. Provider diagnostics belong on stderr.

## Framing

Frames use an LSP-style content length header:

```text
Content-Length: 123\r\n
\r\n
{...123 bytes of UTF-8 JSON...}
```

Protocol 1.0 limits an individual frame to 16 MiB in the host implementation.

## Request shape

```json
{
  "jsonrpc": "2.0",
  "id": 1,
  "method": "initialize",
  "params": {}
}
```

Response:

```json
{
  "jsonrpc": "2.0",
  "id": 1,
  "result": {}
}
```

or:

```json
{
  "jsonrpc": "2.0",
  "id": 1,
  "error": {
    "code": -32000,
    "message": "provider error"
  }
}
```

The protocol resembles JSON-RPC but is intentionally defined by Artifactum; clients should not assume support for arbitrary JSON-RPC extensions.

## `initialize`

Request:

```json
{
  "protocol_major": 1,
  "protocol_minor": 0
}
```

Response contains the negotiated protocol and provider descriptor:

```json
{
  "protocol_major": 1,
  "protocol_minor": 0,
  "provider": {
    "name": "huggingface",
    "version": "0.1.0",
    "schemes": ["huggingface", "hf"],
    "capabilities": {
      "resolve": true,
      "acquire": true,
      "search": true,
      "list": true,
      "versions": true,
      "push": false,
      "auth": true,
      "range": true
    },
    "metadata": {}
  }
}
```

Major versions must match. Minor versions are additive within a major version.

## `resolve`

Parameters:

```json
{
  "requirement": {
    "reference": {"scheme":"hf","locator":"owner/model@main"},
    "revision": null,
    "selection": {"include":[],"exclude":[]},
    "metadata": {}
  },
  "context": {
    "offline": false,
    "environment": {}
  }
}
```

The result is `artifactum_core::Resolution` serialized with Serde.

## `acquire`

Parameters contain a `ResolvedFile`, a host-owned staging path, and an `AcquireContext`.

The provider must:

1. create/truncate the destination;
2. write the complete file;
3. flush/sync as appropriate;
4. return only after acquisition is complete.

The host then hashes and commits the staging file. The provider must never infer or construct a CAS path.

## `search`

Optional. Parameters contain a `SearchRequest` and `ResolveContext`; result is a list of `SearchResult` values.

## Planned protocol 1.x additions

The framing and request IDs intentionally leave room for:

- persistent plugin sessions;
- multiple concurrent requests;
- `$/cancelRequest`-style cancellation;
- progress notifications;
- rate-limit notifications;
- explicit authentication requests;
- range/resume acquisition negotiation;
- provider-specific CLI command descriptions;
- push/upload operations;
- version enumeration and metadata listing.
