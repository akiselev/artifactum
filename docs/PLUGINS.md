# Providers and plugins

Artifactum deliberately avoids Rust dynamic-library ABI coupling.

Executable families:

```text
artifactum-provider-<name>
artifactum-executor-<name>
artifactum-verifier-<name>   # protocol family reserved/available
```

The generic protocol is newline-delimited JSON with UUID request IDs, response routing, notifications, protocol versioning, and plugin kind/capability descriptors. Provider SDKs translate that protocol to the typed `ArtifactProvider` trait.

`artifactum-plugin-host` uses daemonkit to keep provider processes alive across CLI invocations. Within a process session, writes are serialized but multiple requests remain in flight and responses are routed by request ID.

Providers perform semantic resolution and may propose acquisition plans. The host/store computes the final SHA-256 identity. Stable reacquisition state can be stored in provider resolutions; live credentials should not be serialized into locks or artifact manifests.

Built-in sources are local files/directories and HTTP(S). Additional crates cover GitHub Releases, Hugging Face, GitLab, Zenodo/Figshare/OSF/Dataverse, Git/OCI/NGC, DVC/Kaggle/ModelScope/MLflow/W&B/ClearML/Comet bridges, and many object/storage schemes.
