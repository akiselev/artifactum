# Required agent validation runbook

This file is intentionally stricter than a normal `cargo test` checklist. Artifactum's value is in cross-component behavior: a unit test can pass while cache invalidation, locking, materialization, lineage, or remote restore is wrong.

An agent validating this repository must perform **both** automated test layers and observational workflow validation.

## 0. Establish the environment

Record these in the validation report:

```bash
rustc --version --verbose
cargo --version
uname -a
command -v bwrap || true
command -v docker || command -v podman || true
command -v oras || true
```

Use a disposable store for testing. Never point destructive GC/corruption tests at a real Artifactum store.

## 1. Compile and test every crate/target

Run:

```bash
./scripts/validate.sh
```

At minimum this runs structural validation, rustfmt check, workspace/all-target check, all tests, and high-severity Clippy classes. If compilation exposes generated-code mistakes, fix them and rerun from the beginning. Do not skip provider crates just because a particular provider is not used locally.

Then run:

```bash
cargo test --workspace --all-targets -- --nocapture
```

Read failures and output. Do not only quote the final test count.

## 2. Run the complete observational fixture

Keep its evidence:

```bash
ARTIFACTUM_E2E_KEEP=1 ./scripts/e2e_observe.sh
```

The script prints the temporary workspace path. Do not delete it until the following inspection is complete.

### 2.1 Inspect produced bytes

Open/`cat` the `final1.txt` and `final2.txt` files. Confirm the first contains exactly the uppercased three initial inputs and the second contains exactly the changed `b.txt` plus unchanged `a.txt`/`c.txt`. `cmp` already checks this mechanically; the agent must also state what it observed.

### 2.2 Inspect cache reuse

Read `run1.json`, `run2.json`, and `run3.json`.

Expected:

- run 1: all three map actions, collection realization, and aggregate are misses;
- run 2: all are hits;
- after changing only `b.txt`, frozen run remains all hits and retains the old final artifact ID;
- normal run: mapped `a.txt` and `c.txt` hit, mapped `b.txt` misses, collection realization misses, aggregate misses.

Explain why the exact hit/miss pattern proves item-granular invalidation instead of whole-directory invalidation.

### 2.3 Inspect provenance

Read `lineage.json`. Pick the final output node and trace at least one chain back to a source observation. Confirm that a mapped member has a source observation and that producer realizations refer to actions with immutable input IDs.

### 2.4 Inspect determinism audit

Read `determinism.json`. Confirm multiple uncached realizations exist for the same action key and every named output has exactly one artifact variant. Do not treat a cache hit as a determinism test.

### 2.5 Inspect failure/recovery

Read `checkpoint-first.err` and `checkpoint-retry.json`. The first action must really fail after writing a checkpoint; the retry must really execute (not cache-hit) and consume the saved checkpoint to produce output.

### 2.6 Inspect cancellation

Confirm the cancellation fixture returns well before its 60-second `sleep` could naturally complete, the attempt is recorded as failed, and no successful realization exists for it.

### 2.7 Inspect chunked large blob

Compare `big.bin` and `big-roundtrip.bin`, inspect `artifactum artifact inspect @big`, and confirm the manifest annotation reports `artifactum.storage = "cdc-v1"`. Run `store verify @big` and inspect the content/chunk counts in the store.

For a deeper CDC test, create a second large file with a small insertion near the beginning, import both with `--chunked`, and compare physical chunk-object growth against total logical bytes. Most later chunks should be reused rather than shifted wholesale.

### 2.8 Inspect attestation/trust gate

Read the stored attestation with `artifactum attest list @final`; verify its predicate and issuer. Modify the policy to require a nonexistent predicate and demonstrate that `artifactum verify` fails. Restore the policy and demonstrate success.

### 2.9 Inspect OCI export

Read `oci/index.json` and the referenced manifest under `oci/blobs/sha256`. Confirm every descriptor points to an existing digest-named blob and that the root artifact annotation matches the Artifactum artifact ID. If `oras` and a disposable registry are available, publish and pull it there as an additional test.

### 2.10 Inspect remote restore

The fixture pushes the final artifact graph to a file remote and pulls it into a completely empty second store. Confirm `from-remote.txt` equals the original materialization. Then remove one remote content object in a copy of that remote and verify pull fails with a missing/integrity error rather than silently constructing a partial artifact.

### 2.11 Inspect daemonized plugin reuse

The fixture provider writes its PID at process startup. Confirm two separate CLI invocations reported the same provider PID. Then kill that provider PID, invoke the source again, and verify the daemon host respawns it and the operation still succeeds with a new PID.

### 2.12 Inspect corruption detection

The fixture copies the store, corrupts content bytes, and expects `store verify` to fail. Inspect the error manually. It should name an integrity/content failure, not an unrelated parse error.

### 2.13 Inspect GC

Read `gc-dry.json` and `gc.json`. Confirm reachable refs (`@final`, immutable `release`, `@big`) still verify afterward. Explain whether the deliberately orphaned artifact was retained by metadata retention or reclaimed; both can be valid depending on whether a recent metadata record roots it.

## 3. Exercise optional executors where the machine supports them

These tests are environment-dependent and therefore are not hidden behind fake mocks.

### Bubblewrap (Linux)

Run a small task with `executor = "bubblewrap"`, `network = "deny"`. Confirm writing to inputs fails/read-only behavior is preserved and output writing succeeds.

### Container

Use an immutable image digest and `executor = "container"`. Confirm `{in.*}` and `{out.*}` paths are translated into `/work`, network-denied actions cannot make outbound connections, and results return to the host CAS.

### SSH

Point `ARTIFACTUM_SSH_HOST` at a disposable host. Run a task that reads an input and writes output. Inspect the remote staging directory during execution, then verify it is cleaned and the local result bytes match.

### Slurm

On a shared-filesystem Slurm host, verify CPU/GPU arguments are reflected in `srun` and that successful outputs enter the initiating store.

### Kubernetes

Use a disposable namespace. Verify pod creation, sandbox copy-in, command path translation, copy-back, and pod deletion. Force command failure once and verify cleanup still occurs.

## 4. Exercise real providers

Two disposable provider harnesses are included and should be run when their dependencies are available:

```bash
ARTIFACTUM_TEST_KEEP=1 ./scripts/integration_minio.sh
ARTIFACTUM_TEST_KEEP=1 ./scripts/integration_git_lfs.sh
```

The MinIO harness exercises the native OpenDAL S3 provider and credential-safe provider profiles. The Git LFS harness proves branch movement changes the resolved commit/LFS OID, while `--frozen` retains the prior locked bytes. Keep their workspaces and inspect both the materialized bytes and `Artifactum.lock`.


At least one semantic provider and one storage/scientific provider should be tested with public disposable data when network is available, for example:

- Hugging Face public tiny model;
- a GitHub release asset;
- a Zenodo test/small record;
- an S3-compatible MinIO fixture.

For each, inspect `Artifactum.lock` and confirm it contains stable reacquisition identity rather than credentials. Re-run and confirm the CAS prevents redundant byte acquisition where upstream SHA-256 is known.

For gated authentication, force an unauthenticated/gated case and verify the provider returns a structured access condition rather than a generic transfer corruption error.

## 5. Test remote HTTP CAS

The main observational fixture already performs an authenticated native HTTP-CAS round trip for both an ordinary artifact graph and a chunked large artifact. Repeat it manually if modifying the remote protocol.


Start a disposable server:

```bash
artifactum remote serve /tmp/artifactum-remote --bind 127.0.0.1:8173
```

Configure an HTTP remote, push a nontrivial tree/collection/chunked artifact, pull into an empty store, and compare materialized bytes recursively. Repeat with a bearer token and verify unauthenticated access fails.

## 6. Destructive/property stress

Use a disposable store and run concurrent imports of identical files from several processes. Confirm they converge on one content object and no `.partial` file becomes visible as committed content.

Interrupt HTTP acquisition mid-transfer and retry. Confirm staging/resume behavior never commits truncated bytes. The transport unit test also requires an ETag-backed `Range` + `If-Range` continuation and verifies that an ETag change causes a full restart instead of stale-byte append.

Run GC concurrently with a long-running action and verify the active lease prevents its input artifacts from disappearing.

Generate random safe/unsafe artifact paths and assert traversal forms (`..`, absolute paths, platform prefixes) are rejected.

## 7. What the final validation report must contain

Do not write “tests passed” as the entire report. Include:

- Rust/platform versions;
- exact commit/archive tested;
- commands executed;
- unit/integration test result;
- e2e workspace path or archived evidence;
- observed cache-hit counts for all three pipeline phases;
- first and changed final artifact IDs;
- one lineage chain explained in words;
- checkpoint failure + retry evidence;
- cancellation evidence;
- remote round-trip evidence;
- corruption-detection evidence;
- GC before/after numbers;
- which optional executors/providers were actually exercised;
- any features not exercised and the concrete environmental reason.

Only after those observations agree with the intended semantics should an agent declare the implementation validated.
