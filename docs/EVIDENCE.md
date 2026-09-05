# Durable raw evidence and lineage (`artifactum-evidence`)

The scientific-assets profile Sinbad SV0-C1/C2 asked for: how a producer such as Sinbad
stores raw evidence (oracle outputs, meshes, solutions, logs, frozen sources, sealed
manifests) durably in Artifactum, ties every asset to the run that produced it and to the
claim that cites it, and later proves that the stored bytes still are the bytes the claim
recorded, without Artifactum learning any physics.

## What Artifactum owns and what it does not

Artifactum owns identity, immutability, reachability, and re-hash verification. It never
interprets an asset (opaque bytes + declared media type), a receipt payload (opaque JSON), a
claim subject or state (opaque strings), or a role (a producer-chosen name). A producer's own
digest algorithm is honored as a **declared digest**: Artifactum's content identity stays
SHA-256, but a declared `blake3:<hex>` (Sinbad's `BenchmarkAssetRef.digest`) is verified
against the bytes on ingest and again on every claim verification, so the producer's evidence
graph and Artifactum's CAS provably describe the same bytes.

## Objects

All three objects are ordinary store-v2 artifacts whose identities are pure functions of
their contents. Ingesting the same evidence twice, in two different stores, yields the same
ids (proved by `tests/evidence_roundtrip.rs::run_and_claim_identities_are_deterministic_across_stores`
and `tests/sinbad_c11_18.rs::c11_18_claim_identity_is_deterministic`).

```text
asset   Blob artifact         content = sha256(bytes); manifest = { content, media_type,
                              annotations: artifactum.evidence.digest.<algorithm> = <hex> }

run     Collection artifact   { receipt, code/executable, input/<role>..., output/<role>... }
                              format_version = artifactum-evidence-run/1
        receipt Blob          canonical `ReceiptEnvelope<Value>` (artifactum-receipt), whose
                              `receipt_id` is recomputed on every load; media type
                              application/vnd.artifactum.receipt+json

claim   Collection artifact   { record, run/<role>..., asset/<role>... }
                              format_version = artifactum-evidence-claim/1
                              annotations: artifactum.evidence.subject / .state
        record  Blob          `ClaimRecord`: schema, subject, state, every cited run's
                              receipt id, action key and executable, and every cited asset's
                              artifact id, content id, size, media type and declared digests;
                              media type application/vnd.artifactum.evidence-claim+json
```

Because runs and claims are collections, the existing graph GC reaches everything a sealed
claim cites (`tests/evidence_roundtrip.rs::gc_keeps_everything_a_sealed_claim_reaches`), and
a claim can be named with an immutable ref (`EvidenceStore::tag_claim`).

### The action behind a run

`record_run` derives one `ActionSpec` from the description: `command` = the recorded argv,
`inputs` = the input bindings, `code.executable` = the producer executable artifact,
`outputs` = the output bindings' manifests, plus the declared network/sandbox policy and
platform. Its `ActionKey` is the receipt's `activity.action`. The run is recorded in the
metadata plane as an **intrinsic realization** (the computation happened outside Artifactum
and is never replayed from cache), so `Engine::lineage`, `why` and determinism audits see it.
Two runs of the same action at different times share the action key and differ in receipt id.

### The history plane

Attestations (issuer `artifactum-evidence`) form the reverse index and the GC roots:

| subject | predicate | statement |
|---|---|---|
| output asset | `artifactum.evidence/produced-by/1` | `{run, receipt, receipt_id, role}` |
| input asset | `artifactum.evidence/consumed-by/1` | `{run, receipt, receipt_id, role}` |
| executable | `artifactum.evidence/executed-by/1` | `{run, receipt, receipt_id, role: "executable"}` |
| run or direct asset | `artifactum.evidence/cited-by/1` | `{claim, subject, state, role}` |
| claim | `artifactum.evidence/claim/1` | `{subject, state, schema, record}` |

Recording the same run or claim again adds no duplicate attestation.

## API

```rust
let evidence = EvidenceStore::open(root).await?;          // root/store + root/metadata.sqlite

// 1. assets: bytes + media type + the digests the producer recorded
let result = evidence.put_asset(&bytes, "application/json",
    &[ExternalDigest::parse("blake3:<hex>")?]).await?;    // refuses a mismatch, stores nothing
let mesh = evidence.put_asset_file(path, "application/octet-stream", &[...]).await?; // streaming

// 2. runs: one completed activity over stored assets
let run = evidence.record_run(RunDescription {
    schema, producer /* incl. executable: ArtifactId */, environment, command,
    network, sandbox, inputs: vec![RunBinding { role, artifact }], outputs: vec![...],
    diagnostics, started_at, finished_at, parent, payload /* opaque JSON */,
}).await?;                                                // -> StoredRun { run, receipt, receipt_id, action, ... }

// 3. claims: a sealed statement over runs and assets
let claim = evidence.record_claim(ClaimDescription {
    schema, subject, state, runs: vec![RunBinding{..}], assets: vec![RunBinding{..}], payload,
}).await?;                                                // -> StoredClaim { claim, record_artifact, record }
evidence.tag_claim(&claim.claim, "sinbad/claims/<subject>").await?;

// 4. later, anywhere the store is mirrored
let report = evidence.verify_claim(&claim.claim).await?;  // ClaimVerification { ok, runs, assets, failures }
let why = evidence.explain(&result.artifact).await?;      // produced_by / consumed_by / executed_by / cited_by
```

`verify_claim` re-hashes every cited asset's bytes to the recorded SHA-256 content id and to
every recorded declared digest, checks every asset manifest still carries exactly the
declared digests the record snapshotted, re-validates every cited receipt, and checks the
claim collection's members against the record. Member failures are reported by path
(`run/<role>/output/<role>`), never turned into an `Err`, so a partially corrupted store
still yields a complete report (`tests/evidence_roundtrip.rs::tampered_bytes_fail_verification_by_name`).

## Sinbad wiring (SV0-C2, to be landed in the Sinbad repository)

Sinbad already records `BenchmarkAssetRef { digest: "blake3:<hex>", media_type, locator,
provenance }` on every `OracleRun.raw_output`/`log`, on `CampaignManifest.source`, and on
every `ComparisonReport.result_artifacts` entry. The wiring is:

1. **Ingest at the moment bytes exist.** In `execute_oracle_request` (or its callers in
   `independent_comparison.rs`), after `finish_invocation` returns, call `put_asset` for
   `request.json`, `result.json`, stdout and stderr with `ExternalDigest::parse(&hash(bytes))`
   as the declared digest. Today stdout/stderr are hashed and dropped; this is the only way
   their bytes survive.
2. **One `record_run` per adapter invocation.** `producer` = the adapter identity
   (`OracleToolIdentity` name/version → package/package_version; the wrapper script or
   adapter executable ingested once as the `executable` asset); `command` = the
   `command request-file result-file` argv; `inputs` = `request` (+ the frozen case source and
   any mesh); `outputs` = `result`, `stdout`, `stderr`; `payload` = the `OracleRun` JSON.
   Sinbad-side execution (`run_plan`) records its `CaseExecution` the same way, with the
   encoded execution bytes as the `execution` output and the frozen source as an input.
3. **Locator.** Set `BenchmarkAssetRef.locator` to `artifactum:<ArtifactId>` (the
   `StoredAsset.artifact`), keeping `digest` as Sinbad's blake3 and `provenance` as prose.
   `validate_campaign`'s existing `validate_asset` needs no change.
4. **Seal after `promote_and_seal`.** Ingest `encode_campaign_manifest(&manifest)` as an asset
   (declared digest = the blake3 of the encoded bytes, media type
   `application/vnd.sinbad.campaign-manifest+json`; the manifest *identity* is a projection
   hash, not a hash of the bytes, so it cannot be the declared digest — the store verifies
   declared digests against bytes; carry the identity in the claim payload instead) and call
   `record_claim` with
   `subject` = the `SupportClaim.id`, `state` = the `SupportState` name, `runs` = every
   cited `OracleRun`'s run artifact plus the `CaseExecution` run, `assets` = the sealed
   manifest and the frozen source, `payload` = the `PromotionDecision` JSON. Store the claim id
   next to the decision (`PromotionDecision` or `SupportClaim.evidence_ids` gains an
   `artifactum:<ArtifactId>` entry).
5. **Gate on verification.** `evaluate_promotion`'s `RawEvidenceRetention` check currently
   accepts any nonempty digest; with the claim stored it can call `verify_claim` and require
   `ok`, which is the "missing-raw-evidence campaigns are refused" gate SV0-F asks for.

Runtime note: the store API is `async` (tokio). Sinbad is synchronous; wrap calls in a
`tokio::runtime::Runtime::block_on` at the campaign boundary.

## Worked example: the C11.18 evidence

`crates/artifactum-evidence/tests/sinbad_c11_18.rs` ingests the raw layout behind Sinbad's
first sealed `IndependentlyVerified` claim exactly as it survived on disk (see the fixture
provenance in that file): the adapter wrapper and Dockerfile, four `request.json`/`result.json`
pairs (identity probe and the `[4,4]`, `[8,8]`, `[16,16]` levels), the frozen `01-poisson.res`
source and `01-poisson.toml` case. It seals one claim over four runs and two direct assets,
verifies all 18 cited assets by SHA-256 and by Sinbad's blake3, walks lineage from the finest
`result.json` to the claim, survives GC, and catches a one-digit tamper of a stored
observable by name.

What the C11.18 run did **not** leave on disk, recorded in the claim payload's `missing`
list: the adapter's raw stdout/stderr bytes (hashed in memory, never written), the sealed
`CampaignManifest` bytes (identity `blake3:59b83a6c...`, 88386 bytes, never persisted), the
`CaseExecution` bytes, Sinbad's meshes and solutions, and the container image digest. Items 1
and 4 of the wiring above close those gaps for every future run.
