//! Ingests the raw evidence layout behind Sinbad's first sealed `IndependentlyVerified`
//! claim (GX-CONTRACTS C11.18) exactly as it survived on disk, and proves the resulting
//! claim verifies, has deterministic identity, and explains its own lineage.
//!
//! Provenance of `tests/fixtures/sinbad-c11-18/` (copied 2026-09-01):
//!
//! - `adapter-wrapper.sh`, `Dockerfile`, `io/identity-probe/{request,result}.json`,
//!   `io/comparison/level-{4x4,8x8,16x16}/{request,result}.json` are byte-for-byte copies of
//!   `/tmp/sinbad-sv0-c5-238077/` (the retained work directory of
//!   `tests/sv0_c5_fenicsx_oracle.rs::independent_fenicsx_comparison_and_independently_verified_promotion`,
//!   run against the real `sinbad-oracle-fenicsx` adapter at `70c52131`). Five such
//!   directories from five runs that day carried identical bytes for every file.
//! - `sinbad/01-poisson.res` is `physics/corpus/01-poisson.res` and `sinbad/01-poisson.toml`
//!   is `cases/01-poisson.toml` from the Sinbad repository at `4f66d3cd`.
//!
//! What the C11.18 run did **not** leave on disk, and so is not ingested here (the claim
//! payload records the gap): the adapter's raw stdout/stderr bytes (Sinbad hashed them in
//! memory and never wrote them), the sealed `CampaignManifest` bytes (identity
//! `blake3:59b83a6c...`, held in memory only), the `CaseExecution` bytes, Sinbad's own meshes
//! and solutions (the generic runner keeps them in memory), and the container image digest.
//!
//! This test knows Sinbad's directory layout; the `artifactum-evidence` crate does not.

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};

use artifactum_core::{NetworkPolicy, SandboxPolicy};
use artifactum_evidence::{
    ClaimDescription, EvidenceKind, EvidenceStore, ExternalDigest, RunBinding, RunDescription,
    StoredAsset, StoredClaim, StoredRun,
};
use artifactum_receipt::{ExecutionEnvironment, ProducerIdentity, RecordedCommand, SchemaIdentity};
use chrono::{TimeZone, Utc};
use serde_json::{Value, json};
use sha2::Digest as _;

const SINBAD_COMMIT: &str = "4f66d3cd8cd7f35574a57c80e5b79e53d4fcfe6a";
const ADAPTER_COMMIT: &str = "70c52131230b94756c42a838841d58cb6ab426ba";
const LEVELS: [&str; 4] = [
    "identity-probe",
    "comparison/level-4x4",
    "comparison/level-8x8",
    "comparison/level-16x16",
];

/// SHA-256 of every fixture file, so the fixture cannot drift silently from the bytes the
/// C11.18 run produced.
const FIXTURE_SHA256: &[(&str, &str)] = &[
    (
        "adapter-wrapper.sh",
        "5aec0e9c80cc80dc08ee6fddbff8b0de020e4a3e08609a1e2ab8d5cd21eba925",
    ),
    (
        "Dockerfile",
        "1c0e67814f0e7f7af8d39cf722653ca4d30d6c09c8e8578acc74dbd79be8c5d9",
    ),
    (
        "io/identity-probe/request.json",
        "7bce3c26b88e793489fe0d074efc4dcbc270aeab78073f408a4d4db114d6a4fb",
    ),
    (
        "io/identity-probe/result.json",
        "9a09b9c39714026de8d3a870159bed33332a79ef954d3ed5631f7eeaddacf7ea",
    ),
    (
        "io/comparison/level-4x4/request.json",
        "dce425449e701b225ed92238c7c2b54a8bd74d1fc4902ab1f1430c8365149ae4",
    ),
    (
        "io/comparison/level-4x4/result.json",
        "6132d850eb81532f53ce08c18b325366f72dbc0040e96ac7b8ad897e4710fbb7",
    ),
    (
        "io/comparison/level-8x8/request.json",
        "1b2bc915d177b93795d09af37b0d24dd7e57b4cf80d85fcf4621bd119fd007bf",
    ),
    (
        "io/comparison/level-8x8/result.json",
        "abf5c1a68945903930bb559b1332149c74fba67870d32494cb2860b3cff8cf20",
    ),
    (
        "io/comparison/level-16x16/request.json",
        "341668630da199ce4f08035e1906d3403196659cfb6c1f920dc585ecb9de90aa",
    ),
    (
        "io/comparison/level-16x16/result.json",
        "0de03dbfd4cfd6a0c1c5493667d7fc5d2e82803d080a290430e740356f671dc6",
    ),
    (
        "sinbad/01-poisson.res",
        "6f641de60073342c9915a833fc0116ef77fb752a732784980d7e8c812f19dffa",
    ),
    (
        "sinbad/01-poisson.toml",
        "a37e36377aa87953ca3c8c3abcd059f13d8c9f9ab822934f0661c1f73fb17d30",
    ),
];

fn fixture_root() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("tests")
        .join("fixtures")
        .join("sinbad-c11-18")
}

/// Sinbad's `oracle::hash`: `blake3:<hex>` over the exact bytes.
fn sinbad_hash(bytes: &[u8]) -> ExternalDigest {
    ExternalDigest::parse(&format!("blake3:{}", blake3::hash(bytes).to_hex())).unwrap()
}

struct IngestedLevel {
    name: String,
    request: StoredAsset,
    result: StoredAsset,
    run: StoredRun,
}

struct Ingested {
    wrapper: StoredAsset,
    dockerfile: StoredAsset,
    source: StoredAsset,
    case: StoredAsset,
    levels: Vec<IngestedLevel>,
    claim: StoredClaim,
}

/// Ingest a file as a Sinbad raw asset: the bytes, the media type Sinbad's
/// `BenchmarkAssetRef` would declare, and Sinbad's blake3 digest as the declared digest.
async fn put_sinbad_file(store: &EvidenceStore, path: &Path, media_type: &str) -> StoredAsset {
    let bytes = std::fs::read(path).unwrap_or_else(|e| panic!("{}: {e}", path.display()));
    let declared = sinbad_hash(&bytes);
    let asset = store
        .put_asset_file(path, media_type, std::slice::from_ref(&declared))
        .await
        .unwrap();
    assert_eq!(asset.declared, vec![declared]);
    asset
}

async fn ingest_c11_18(store: &EvidenceStore, root: &Path) -> Ingested {
    let wrapper = put_sinbad_file(
        store,
        &root.join("adapter-wrapper.sh"),
        "text/x-shellscript",
    )
    .await;
    let dockerfile = put_sinbad_file(store, &root.join("Dockerfile"), "text/plain").await;
    // The exact media type Sinbad's `plan_campaign` declares for the frozen `.res` source.
    let source = put_sinbad_file(
        store,
        &root.join("sinbad/01-poisson.res"),
        "text/vnd.sinbad.scientia-source",
    )
    .await;
    let case = put_sinbad_file(
        store,
        &root.join("sinbad/01-poisson.toml"),
        "application/toml",
    )
    .await;

    let mut levels = Vec::new();
    for level in LEVELS {
        let dir = root.join("io").join(level);
        let request = put_sinbad_file(store, &dir.join("request.json"), "application/json").await;
        let result = put_sinbad_file(store, &dir.join("result.json"), "application/json").await;
        let request_json: Value =
            serde_json::from_slice(&std::fs::read(dir.join("request.json")).unwrap()).unwrap();
        let result_json: Value =
            serde_json::from_slice(&std::fs::read(dir.join("result.json")).unwrap()).unwrap();
        let name = level.rsplit('/').next().unwrap().to_owned();

        let run = store
            .record_run(RunDescription {
                schema: SchemaIdentity {
                    name: "sinbad-oracle-run".into(),
                    version: 1,
                    // Schema digest of `sinbad-oracle-protocol/1`, which Sinbad owns; the
                    // protocol has no published digest, so its name is hashed here.
                    digest: artifactum_core::hash_bytes(b"sinbad-oracle-protocol/1"),
                },
                producer: ProducerIdentity {
                    repository: "akiselev/sinbad-oracle-fenicsx".into(),
                    commit: ADAPTER_COMMIT.into(),
                    package: result_json["tool"]["name"].as_str().unwrap().into(),
                    package_version: result_json["tool"]["version"].as_str().unwrap().into(),
                    executable: wrapper.artifact.clone(),
                },
                environment: ExecutionEnvironment {
                    platform: "x86_64-unknown-linux-gnu".into(),
                    // The `docker run` image digest was never recorded by the C11.18 run.
                    container: None,
                    environment_lock: None,
                    runtimes: BTreeMap::from([(
                        "dolfinx".to_owned(),
                        result_json["tool"]["version"].as_str().unwrap().to_owned(),
                    )]),
                    metadata: BTreeMap::from([(
                        "image".to_owned(),
                        json!(
                            "sinbad-oracle-fenicsx-sv0-c5-test:local (built from the \
                               image-recipe input over the adapter checkout; digest not \
                               recorded)"
                        ),
                    )]),
                },
                command: Some(RecordedCommand {
                    argv: vec![
                        "adapter-wrapper.sh".into(),
                        "request.json".into(),
                        "result.json".into(),
                    ],
                    working_directory: Some(format!("io/{level}")),
                    declared_environment: BTreeMap::new(),
                }),
                network: NetworkPolicy::Deny,
                sandbox: SandboxPolicy::Container,
                inputs: vec![
                    RunBinding {
                        role: "request".into(),
                        artifact: request.artifact.clone(),
                    },
                    RunBinding {
                        role: "image-recipe".into(),
                        artifact: dockerfile.artifact.clone(),
                    },
                ],
                outputs: vec![RunBinding {
                    role: "result".into(),
                    artifact: result.artifact.clone(),
                }],
                diagnostics: vec![],
                // The retained directory's mtimes (2026-09-01 08:32-08:33 local); the C11.18
                // run recorded no per-level timestamps.
                started_at: Utc.with_ymd_and_hms(2026, 9, 1, 8, 32, 0).unwrap(),
                finished_at: Utc.with_ymd_and_hms(2026, 9, 1, 8, 33, 0).unwrap(),
                parent: None,
                payload: json!({
                    "protocol": request_json["schema"],
                    "case_id": request_json["case_id"],
                    "capability": request_json["capability"],
                    "refinement": request_json["refinement"],
                    "requested_tool": request_json["tool"],
                    "reported_tool": result_json["tool"],
                    "status": result_json["status"],
                    "result_sha256": result.declared[0].as_qualified(),
                    "raw_stdout_sha256": Value::Null,
                    "raw_stderr_sha256": Value::Null,
                    "missing": [
                        "raw stdout bytes (hashed in memory by Sinbad, never written)",
                        "raw stderr bytes (hashed in memory by Sinbad, never written)",
                    ],
                }),
            })
            .await
            .unwrap();
        levels.push(IngestedLevel {
            name,
            request,
            result,
            run,
        });
    }

    let claim = store
        .record_claim(ClaimDescription {
            schema: SchemaIdentity {
                name: "sinbad-support-claim".into(),
                version: 1,
                digest: artifactum_core::hash_bytes(b"sinbad-support-claim/1"),
            },
            subject: "sinbad:sv0-c5-poisson-c11-18/support-claim".into(),
            state: "IndependentlyVerified".into(),
            runs: levels
                .iter()
                .map(|l| RunBinding {
                    role: l.name.clone(),
                    artifact: l.run.run.clone(),
                })
                .collect(),
            assets: vec![
                RunBinding {
                    role: "source".into(),
                    artifact: source.artifact.clone(),
                },
                RunBinding {
                    role: "case".into(),
                    artifact: case.artifact.clone(),
                },
            ],
            payload: json!({
                "contract": "GX-CONTRACTS C11.18",
                "sinbad_commit": SINBAD_COMMIT,
                "adapter_commit": ADAPTER_COMMIT,
                "reference_executed_manifest_identity":
                    "blake3:850c04553c546505a244b9a93900e618394e99520cc6bbff72da802b4ee566d8",
                "sealed_manifest_identity":
                    "blake3:59b83a6cb250bca6058162581d3b342c8bb2b3d7f8adf08a10ac65e0409799f7",
                "promotion_evidence_ids": [
                    "Poisson-verification/run/Poisson-external-oracle-c09991b7a54a",
                    "blake3:fe8be80c2f3fd2909f1c72981718eb4f5ca040a0c95a9a37185d526b69ce8ca1",
                ],
                "energy_relative_difference": 0.004319,
                "tolerances": {"energy_relative": 0.01, "convergence_order_floor": 1.8},
                "missing": [
                    "sealed CampaignManifest bytes (88386 bytes; identity above; never persisted)",
                    "CaseExecution bytes (identity blake3:fe8be80c...; never persisted)",
                    "Sinbad meshes and solutions (kept in memory by the generic runner)",
                    "adapter raw stdout/stderr bytes",
                    "container image digest",
                ],
            }),
        })
        .await
        .unwrap();

    Ingested {
        wrapper,
        dockerfile,
        source,
        case,
        levels,
        claim,
    }
}

#[test]
fn fixture_bytes_are_the_c11_18_bytes() {
    let root = fixture_root();
    for (relative, expected) in FIXTURE_SHA256 {
        let bytes = std::fs::read(root.join(relative)).unwrap();
        assert_eq!(
            hex::encode(sha2::Sha256::digest(&bytes)),
            *expected,
            "{relative} drifted from the retained C11.18 bytes"
        );
    }
    // The Sinbad source asset digest is Sinbad's `blake3:` over the exact `.res` bytes.
    let source = std::fs::read(root.join("sinbad/01-poisson.res")).unwrap();
    assert_eq!(source.len(), 702);
    assert_eq!(sinbad_hash(&source).algorithm, "blake3");
}

#[tokio::test]
async fn c11_18_layout_ingests_seals_and_verifies() {
    let dir = tempfile::tempdir().unwrap();
    let store = EvidenceStore::open(dir.path()).await.unwrap();
    let ingested = ingest_c11_18(&store, &fixture_root()).await;

    // Every asset carries the media type and the blake3 digest Sinbad's evidence graph keys
    // it by; the CAS keys it by sha256 underneath.
    assert_eq!(
        ingested.source.media_type,
        "text/vnd.sinbad.scientia-source"
    );
    assert_eq!(
        ingested.source.content.0.value,
        "6f641de60073342c9915a833fc0116ef77fb752a732784980d7e8c812f19dffa"
    );
    assert_eq!(ingested.source.declared[0].algorithm, "blake3");
    assert_eq!(ingested.levels.len(), 4);
    let finest = ingested
        .levels
        .iter()
        .find(|l| l.name == "level-16x16")
        .unwrap();
    assert_eq!(
        finest.result.content.0.value,
        "0de03dbfd4cfd6a0c1c5493667d7fc5d2e82803d080a290430e740356f671dc6"
    );
    assert_eq!(finest.run.outputs.len(), 1);
    assert_eq!(finest.run.inputs.len(), 2);
    assert!(!finest.run.realization_reused);

    // Identity-probe and comparison runs are distinct activities of one executable.
    let actions: std::collections::BTreeSet<_> = ingested
        .levels
        .iter()
        .map(|l| l.run.action.clone())
        .collect();
    assert_eq!(
        actions.len(),
        4,
        "each level is its own action (request differs)"
    );

    // The claim snapshots every run receipt and every asset digest, and verifies.
    let record = &ingested.claim.record;
    assert_eq!(record.state, "IndependentlyVerified");
    assert_eq!(record.runs.len(), 4);
    assert_eq!(record.assets.len(), 2);
    let cited = ingested.claim.cited_assets();
    assert_eq!(
        cited.len(),
        4 * 4 + 2,
        "executable + request + image-recipe + result per level, plus source and case"
    );
    assert!(
        cited
            .iter()
            .all(|(_, a)| a.declared.iter().any(|d| d.algorithm == "blake3"))
    );

    let verification = store.verify_claim(&ingested.claim.claim).await.unwrap();
    assert!(verification.ok, "{:?}", verification.failures);
    assert_eq!(verification.assets.len(), 18);
    assert_eq!(verification.runs.len(), 4);
    assert!(verification.failures.is_empty());

    // Lineage: the finest result -> its run -> the sealed claim.
    let explanation = store.explain(&finest.result.artifact).await.unwrap();
    assert_eq!(explanation.kind, EvidenceKind::Asset);
    assert_eq!(explanation.produced_by.len(), 1);
    assert_eq!(explanation.produced_by[0].run, finest.run.run);
    assert_eq!(explanation.produced_by[0].receipt_id, finest.run.receipt_id);
    assert_eq!(explanation.cited_by.len(), 1);
    assert_eq!(explanation.cited_by[0].claim, ingested.claim.claim);
    assert_eq!(explanation.cited_by[0].state, "IndependentlyVerified");
    assert_eq!(explanation.cited_by[0].role, "run/level-16x16/result");

    // The Dockerfile is consumed by all four runs and cited by the claim through each.
    let recipe = store.explain(&ingested.dockerfile.artifact).await.unwrap();
    assert_eq!(recipe.consumed_by.len(), 4);
    assert_eq!(recipe.cited_by.len(), 4);
    // The wrapper is the executable of every run, and code lineage in the engine's own walk.
    let wrapper = store.explain(&ingested.wrapper.artifact).await.unwrap();
    assert_eq!(wrapper.executed_by.len(), 4);
    assert_eq!(wrapper.cited_by.len(), 4);
    let nodes = store.engine().lineage(&finest.result.artifact).unwrap();
    let node = nodes
        .iter()
        .find(|n| n.artifact == finest.result.artifact)
        .unwrap();
    assert!(node.inputs.contains(&ingested.wrapper.artifact));
    assert!(node.inputs.contains(&finest.request.artifact));
    // The source and case are cited directly.
    let source = store.explain(&ingested.source.artifact).await.unwrap();
    assert_eq!(source.cited_by[0].role, "asset/source");
    let case = store.explain(&ingested.case.artifact).await.unwrap();
    assert_eq!(case.cited_by[0].role, "asset/case");

    // Tag, then GC with the CLI's root rule; the whole claim survives.
    store
        .tag_claim(
            &ingested.claim.claim,
            "sinbad/c11-18/independently-verified",
        )
        .await
        .unwrap();
    let roots = store.engine().gc_roots(0).unwrap();
    store.store().gc(false, &roots).await.unwrap();
    let verification = store.verify_claim(&ingested.claim.claim).await.unwrap();
    assert!(verification.ok, "{:?}", verification.failures);
    for level in &ingested.levels {
        assert!(
            store
                .store()
                .verify_content(&level.result.content)
                .await
                .unwrap()
        );
        assert!(
            store
                .store()
                .verify_content(&level.request.content)
                .await
                .unwrap()
        );
    }

    eprintln!(
        "C11.18 claim {} (record {}); finest run {} receipt {}",
        ingested.claim.claim,
        ingested.claim.record_artifact,
        finest.run.run,
        finest.run.receipt_id.0
    );
}

#[tokio::test]
async fn c11_18_claim_identity_is_deterministic() {
    let dir_a = tempfile::tempdir().unwrap();
    let dir_b = tempfile::tempdir().unwrap();
    let a = ingest_c11_18(
        &EvidenceStore::open(dir_a.path()).await.unwrap(),
        &fixture_root(),
    )
    .await;
    let b = ingest_c11_18(
        &EvidenceStore::open(dir_b.path()).await.unwrap(),
        &fixture_root(),
    )
    .await;
    assert_eq!(a.claim.claim, b.claim.claim);
    assert_eq!(a.claim.record_artifact, b.claim.record_artifact);
    for (x, y) in a.levels.iter().zip(&b.levels) {
        assert_eq!(x.run.run, y.run.run);
        assert_eq!(x.run.receipt_id, y.run.receipt_id);
        assert_eq!(x.run.action, y.run.action);
    }
    assert_eq!(a.source.artifact, b.source.artifact);
    assert_eq!(a.case.artifact, b.case.artifact);
    assert_eq!(a.wrapper.artifact, b.wrapper.artifact);
}

#[tokio::test]
async fn c11_18_tampered_result_is_caught_by_its_sinbad_digest() {
    let dir = tempfile::tempdir().unwrap();
    let store = EvidenceStore::open(dir.path()).await.unwrap();
    let ingested = ingest_c11_18(&store, &fixture_root()).await;
    let finest = ingested
        .levels
        .iter()
        .find(|l| l.name == "level-16x16")
        .unwrap();

    // Flip one observable bit pattern in the stored result bytes.
    let path = store.store().content_path(&finest.result.content).unwrap();
    let text = std::fs::read_to_string(&path).unwrap();
    assert!(text.contains("4612685232237614162"));
    std::fs::write(
        &path,
        text.replace("4612685232237614162", "4612685232237614163"),
    )
    .unwrap();

    let verification = store.verify_claim(&ingested.claim.claim).await.unwrap();
    assert!(!verification.ok);
    let bad = verification
        .assets
        .iter()
        .find(|a| a.path == "run/level-16x16/output/result")
        .unwrap();
    assert!(!bad.content_ok);
    assert!(!bad.declared[0].ok);
    assert_eq!(bad.declared[0].algorithm, "blake3");
    assert_eq!(
        verification.assets.iter().filter(|a| !a.ok).count(),
        1,
        "only the tampered asset fails"
    );
}
