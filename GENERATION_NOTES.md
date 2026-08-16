# Generation and validation notes

This archive was produced from the connected GitHub repository `akiselev/artifactum`, using commit `67dfcb672b486f79f4ffb9ab47c19885b4877287` ("wave 1-3 and improvements to the core") as the source architecture/base.

## What changed

Artifactum was evolved from an external-artifact resolver/downloader into the complete artifact lifecycle system described in the design discussion: store-v2 content/provenance separation; Merkle trees, collections, CDC chunked blobs, graph GC and leases; SQLite action/source/provenance metadata; action hashing, attempts and realizations; cache policies, budgets, checkpoints, cancellation and determinism auditing; local/bubblewrap/container/SSH/Slurm/Kubernetes/plugin executors; pipeline DAGs and item-granular `foreach`; v3 source locks; remote file/HTTP CAS; OCI/in-toto/SLSA/trust primitives; provider conformance tests; persistent daemonkit provider sessions; native S3/GCS/Azure, Git/LFS, and OCI resolution; legacy project/store compatibility; and one unified CLI.

The workspace contains 56 crates.

## Validation performed in the generation environment

The generation environment has no Rust toolchain (`cargo`/`rustc` are absent), public binary download is unavailable, and package indexes are unavailable. Therefore **no claim is made that this archive was compiled or that its Rust tests were executed here**.

The following checks were performed here instead:

```text
python3 scripts/static_validate.py
  -> static validation ok: 56 crates

bash -n scripts/*.sh
  -> success
```

`static_validate.py` checks workspace/path dependency coverage, TOML parsing, provider plugin target shape, key invariants, and approximate Rust delimiter balance. It is deliberately not represented as a substitute for compilation.

## Required downstream validation

A Rust-enabled agent must begin with:

```bash
./scripts/validate.sh
ARTIFACTUM_E2E_KEEP=1 ./scripts/e2e_observe.sh
```

Then follow `AGENT_TESTING.md` and inspect the generated evidence, not just exit codes. If Docker is available, run `scripts/integration_minio.sh`; if Git LFS is available, run `scripts/integration_git_lfs.sh`. Fix every compile/test/runtime defect found and restart the validation sequence from the top.
