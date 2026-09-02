# Artifactum status

**Updated:** 2026-09-01 (W7 lane 7, Sinbad SV0-C1/C2 durable raw evidence and lineage)

Artifactum is an optional supporting system for the Sinbad ecosystem (workspace `PLAN.md`
§8): it is used once a concrete durable-artifact need is demonstrated. The first such need
was demonstrated by Sinbad's first sealed `IndependentlyVerified` claim (GX-CONTRACTS
C11.18), whose raw assets survived only in a temporary directory and whose sealed manifest was
never persisted. This file records what Artifactum now provides for that need and what is
honestly open.

## Landed

### `artifactum-evidence` (SV0-C1; Artifactum half of SV0-C2)

Content-addressed raw-evidence store with immutable assets and a lineage graph
asset → producing run/receipt → claim, plus re-hash verification. Design and the exact
Sinbad-side wiring: [`docs/EVIDENCE.md`](docs/EVIDENCE.md).

- **Assets** are blob artifacts: opaque bytes + declared media type + producer-declared
  digests (`sha256` or `blake3`, Sinbad's `blake3:<hex>` form parses directly). Every declared
  digest is verified against the bytes on ingest; a mismatch is refused before any byte enters
  the CAS. Identity = content + media type + declared digests.
- **Runs** are collection artifacts `{ receipt, code/executable, input/<role>, output/<role> }`
  whose receipt is a canonical `artifactum-receipt` `ReceiptEnvelope`; the action key is
  derived from argv, inputs, executable, outputs, policy and platform, and the run is recorded
  in the metadata plane as an intrinsic realization.
- **Claims** are collection artifacts `{ record, run/<role>, asset/<role> }` whose record
  snapshots every cited receipt id, action key, executable and asset digest. A claim is a
  permanent GC root through its own attestation and can be tagged with an immutable ref.
- **Verification** (`verify_claim`) re-hashes every cited asset to its SHA-256 content id and
  to every declared digest, re-validates every receipt, checks manifests and collection
  members against the record, and reports every failure by path.
- **Lineage** (`explain`) answers produced-by / consumed-by / executed-by / cited-by from
  attestations, one hop through runs to claims; `Engine::lineage` sees runs as realizations.
- **No physics, no producer vocabulary** in the crate: roles, subjects, states, payloads and
  media types are opaque. The Sinbad-specific knowledge lives only in the C11.18 test.

Tests (all run 2026-09-01, `cargo test -p artifactum-evidence`): **17 passed, 0 failed**
(3 unit; 10 in `tests/evidence_roundtrip.rs`; 4 in `tests/sinbad_c11_18.rs`).

Proof points:

- deterministic identity across stores:
  `evidence_roundtrip::run_and_claim_identities_are_deterministic_across_stores`,
  `sinbad_c11_18::c11_18_claim_identity_is_deterministic`;
- declared-digest refusal before storage:
  `evidence_roundtrip::declared_digest_mismatch_is_refused_before_anything_is_stored`;
- re-hash verification and tamper detection by name:
  `evidence_roundtrip::{sealed_claim_verifies_and_snapshots_every_digest,
  tampered_bytes_fail_verification_by_name, tampered_receipt_fails_verification}`,
  `sinbad_c11_18::c11_18_tampered_result_is_caught_by_its_sinbad_digest`;
- lineage and GC reachability:
  `evidence_roundtrip::{lineage_walks_from_asset_to_run_to_claim,
  gc_keeps_everything_a_sealed_claim_reaches, tagging_a_claim_is_immutable}`;
- the C11.18 layout, ingested as it exists on disk:
  `sinbad_c11_18::{fixture_bytes_are_the_c11_18_bytes, c11_18_layout_ingests_seals_and_verifies}`.

### The C11.18 evidence, ingested

`crates/artifactum-evidence/tests/fixtures/sinbad-c11-18/` holds byte-for-byte copies (SHA-256
pinned in the test) of what the C11.18 run left on disk: `/tmp/sinbad-sv0-c5-238077/`
(adapter wrapper, Dockerfile, four `request.json`/`result.json` pairs; identical across the
five retained run directories of 2026-09-01) plus `physics/corpus/01-poisson.res` and
`cases/01-poisson.toml` from Sinbad `4f66d3cd`. The raw files were **not** in the Sinbad
repository; nothing of C11.18's raw evidence is committed there.

Missing from disk, therefore not ingested (recorded in the claim payload): adapter raw
stdout/stderr bytes (hashed in memory by Sinbad and dropped), the sealed `CampaignManifest`
bytes (identity `blake3:59b83a6c...`, 88386 bytes, never written), the `CaseExecution` bytes,
Sinbad's meshes and solutions (in-memory only), and the container image digest.

## Open

- **SV0-C2 Sinbad side** is not landed (Sinbad is another lane's repository). The exact calls
  and the five wiring steps are in `docs/EVIDENCE.md` § "Sinbad wiring". Until Sinbad calls
  `put_asset` at the moment stdout/stderr and result bytes exist and `record_claim` after
  `promote_and_seal`, future claims will keep losing their raw bytes.
- **No CLI surface yet** (`artifactum evidence verify <claim>` / `explain <artifact>`); the
  Rust API is the deliverable Sinbad consumes. Existing `artifactum lineage` and
  `artifact inspect` already work on evidence artifacts.
- **SV0-F4** (remote mirror / promotion policy example over evidence roots) is not started;
  runs and claims are ordinary collections, so `artifactum remote push` of a claim already
  mirrors its whole graph, but no example or policy is documented.
- The workspace `Cargo.lock` was first committed in this lane (no previous commit carried
  one); it pins the dependency set the tests above ran against.

## Validation record (2026-09-01)

- Toolchain: `rustc 1.91.0`, `cargo 1.91.0`, Linux x86_64.
- `python3 scripts/static_validate.py` → `static validation ok: 58 crates`.
- `cargo fmt --all -- --check` → clean.
- `cargo clippy -p artifactum-evidence --all-targets -- -D warnings -A clippy::pedantic`
  (the CI rule) → clean; `-D clippy::correctness -D clippy::suspicious` (`validate.sh`) →
  clean for the new crate (pre-existing advisory pedantic warnings in `artifactum-core`).
- `cargo test --workspace` (foreground, one run) → **38 passed, 0 failed** across 154 test
  binaries/doc-test targets (17 of the 38 are `artifactum-evidence`; the rest are the
  pre-existing core/store/metadata/receipt/transport/plugin/fixture unit tests).
- Not run: `scripts/e2e_observe.sh`, MinIO/Git-LFS harnesses, optional executors (unchanged
  by this lane; no Docker/bwrap required for the evidence crate).
