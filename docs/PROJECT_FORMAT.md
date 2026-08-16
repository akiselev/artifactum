# `Artifactum.toml` v3

```toml
version = 3

[project]
name = "research-corpus"

[providers.lab]
kind = "s3"
bucket = "models"
endpoint = "https://minio.example"

[artifacts.model]
source = "hf:owner/model@main"
include = ["config.json", "weights/*.safetensors"]

[artifacts.private]
source = "lab:production/model.bin"

[tasks.convert]
run = ["converter", "{in.model}", "{out.converted}"]
inputs.model = "@model"
code = ["scripts/converter-config.json"]
cache = "reproducible"
network = "deny"
executor = "local"

[tasks.convert.environment]
container = "ghcr.io/example/converter@sha256:..."

[tasks.convert.resources]
cpus = 4
memory_bytes = 8589934592
timeout_seconds = 3600
cost_usd_micros_per_hour = 1090000

[tasks.convert.budget]
max_wall_seconds = 3600
max_usd_micros = 2000000

[tasks.convert.outputs.converted]
kind = "tree"

[refs.latest]
target = "convert.converted"
immutable = false

[remotes.home]
kind = "file"
path = "/mnt/archive/artifactum"
```

Task references may address a source (`@model`) or a task output (`convert.converted`). If a task has exactly one output, its task name can be resolved implicitly in contexts that permit it.

`foreach = "@source"` or `foreach = "task.collection"` expands a tree/collection into one independently keyed action per member, then records a deterministic intrinsic collection realization.

## Lockfile

`Artifactum.lock` v3 stores only external-source resolution state: requirement hash, acquired artifact ID, and canonical provider resolution JSON. Normal runs re-resolve mutable sources. `--frozen` uses the locked artifact and fails if project intent differs or its CAS graph is unavailable.
