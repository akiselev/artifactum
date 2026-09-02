# Architecture

## Invariants

1. Content identity and provenance are separate.
2. Providers never choose CAS identity.
3. A mutable external reference is resolved before bytes move.
4. An action is a canonical computation request; an attempt is one execution; a realization binds a successful action to artifact outputs.
5. Cache hits are legal only for `pure` and `reproducible` actions.
6. `volatile` actions always execute. `effect` actions always execute and produce immutable receipts when they have no declared data output.
7. Scheduling/budget/task-name fields do not enter `ActionKey`.
8. External source locks and derived action realizations are separate state planes.
9. Every remote object is verified against its requested SHA-256 before becoming trusted local content.
10. GC is reachability-based and must honor refs, active leases, recent realizations, source observations, checkpoints, attestations, and caller-supplied roots.
11. Pipeline `foreach` is fine-grained: one item is one action identity.
12. Executors operate on already-materialized sandboxes and do not own artifact identity.

## Planes

```text
                   Artifactum.toml / Rust API
                            |
                         planner
                            |
                     ActionSpec DAG
                            |
               +------------+------------+
               |                         |
          source plane               engine plane
 Requirement -> Resolution        action cache lookup
       |                              |       |
 AcquisitionPlan                 hit |       | execute
       |                              |       v
       +----------> Artifact <--------+   Executor
                      |                    |
                      |                Attempt
                      |                    |
                      +------------- Realization

    durable CAS                         SQLite metadata
 content + manifests       actions / attempts / realizations / refs-to-history
         |                                  |
         +----------------+-----------------+
                          |
                 provenance / remote
```

## Crates

- `artifactum-core`: I/O-free stable identity/domain types.
- `artifactum-store`: durable CAS, trees, collections, chunked blobs, refs, leases, GC, materialization.
- `artifactum-metadata`: SQLite append/history plane.
- `artifactum-resolver`: semantic provider routing and source acquisition.
- `artifactum-transport-http`: resumable host-owned HTTP transfer.
- `artifactum-action`: builders and structural action diffs.
- `artifactum-executor`: execution backends.
- `artifactum-engine`: action cache, sandbox, attempts, realizations, checkpoints, lineage.
- `artifactum-pipeline`: project/lock format, DAG planner, maps, scheduler.
- `artifactum-remote`: origin-independent CAS mirroring/server.
- `artifactum-provenance`: in-toto/SLSA/verification/OCI.
- `artifactum-receipt`: producer-neutral research receipt contracts (`ReceiptEnvelope`).
- `artifactum-evidence`: durable raw-evidence assets, content-addressed runs and sealed claims with re-hash verification and lineage (`docs/EVIDENCE.md`).
- `artifactum-plugin-protocol`: generic multiplexable framing.
- `artifactum-plugin-host`: daemonkit-backed process/session owner.
- `artifactum-provider-sdk`: provider-to-plugin adapter.
- `artifactum-provider-*`: independently distributable provider implementations.
- `artifactum-cli`: unified CLI.

## Identity layers

`ContentId` is SHA-256 over exact stored bytes. For structured content those bytes are the canonical JSON representation.

`ArtifactId` is SHA-256 over `ArtifactManifest`, which gives content semantic kind/media/schema/annotations without embedding source or producer provenance.

`ActionKey` is SHA-256 over the computation identity projection:

```text
format version
command argv
input artifact IDs
code artifact IDs
canonical parameters
environment variables + immutable container reference
output contracts
network/sandbox policy
platform constraint
```

It deliberately excludes task name, executor selection, CPU/memory/GPU reservation, timeout/budget accounting, cache policy, timestamps, retry number, worker identity, and priority.

## Source versus computation

Source provenance is represented by `SourceObservation`. Action provenance is represented by `AttemptRecord` and `Realization`. This permits the same artifact to have many valid origins without changing its identity.

## Failure semantics

Outputs are staged inside the attempt sandbox. They enter the CAS only after the executor returns success and every declared output exists. stdout/stderr and checkpoint outputs are preserved independently of successful realization, so debugging/recovery does not require pretending a failed attempt produced a valid result.

## Distributed execution

The engine has one executor interface. Local/bubblewrap/container executors operate directly on the local sandbox. SSH and Kubernetes backends stage the sandbox to a remote execution root and copy successful state back. Slurm assumes a shared filesystem. Executable executor plugins form an ABI-free extension boundary.
