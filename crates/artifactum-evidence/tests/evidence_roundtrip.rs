//! Generic behavior of the evidence API: deterministic identity, declared-digest
//! verification, sealed claims, re-hash verification, tamper detection, lineage, and GC
//! reachability. No producer vocabulary appears here; roles and payloads are opaque.

use std::collections::BTreeMap;

use artifactum_core::{ArtifactId, Digest, NetworkPolicy, SandboxPolicy};
use artifactum_evidence::{
    ATTESTATION_ISSUER, CITED_BY_PREDICATE, CLAIM_PREDICATE, ClaimDescription, Error, EvidenceKind,
    EvidenceStore, ExternalDigest, PRODUCED_BY_PREDICATE, RunBinding, RunDescription, StoredAsset,
    StoredClaim, StoredRun,
};
use artifactum_receipt::{ExecutionEnvironment, ProducerIdentity, RecordedCommand, SchemaIdentity};
use artifactum_store::ContentStore;
use chrono::{TimeZone, Utc};
use serde_json::json;
use sha2::Digest as _;

fn digest(byte: char) -> Digest {
    Digest::sha256(byte.to_string().repeat(64)).unwrap()
}

fn schema(name: &str) -> SchemaIdentity {
    SchemaIdentity {
        name: name.into(),
        version: 1,
        digest: digest('1'),
    }
}

fn producer(executable: ArtifactId) -> ProducerIdentity {
    ProducerIdentity {
        repository: "example/producer".into(),
        commit: "0123abcd".into(),
        package: "producer".into(),
        package_version: "0.1.0".into(),
        executable,
    }
}

fn environment() -> ExecutionEnvironment {
    ExecutionEnvironment {
        platform: "x86_64-unknown-linux-gnu".into(),
        container: None,
        environment_lock: None,
        runtimes: BTreeMap::new(),
        metadata: BTreeMap::new(),
    }
}

fn blake3_of(bytes: &[u8]) -> ExternalDigest {
    ExternalDigest::compute("blake3", bytes).unwrap()
}

struct Fixture {
    request: StoredAsset,
    result: StoredAsset,
    log: StoredAsset,
    tool: StoredAsset,
    run: StoredRun,
    claim: StoredClaim,
}

/// Ingest one opaque activity (request -> result + log) and seal one claim over it.
async fn ingest(store: &EvidenceStore) -> Fixture {
    let request_bytes = br#"{"case":"opaque","refinement":[4,4]}"#;
    let result_bytes = br#"{"observables":{"energy":1.5}}"#;
    let log_bytes = b"solver ran\n";
    let tool_bytes = b"#!/bin/sh\nexec tool \"$@\"\n";

    let request = store
        .put_asset(
            request_bytes,
            "application/json",
            &[blake3_of(request_bytes)],
        )
        .await
        .unwrap();
    let result = store
        .put_asset(result_bytes, "application/json", &[blake3_of(result_bytes)])
        .await
        .unwrap();
    let log = store
        .put_asset(log_bytes, "text/plain", &[blake3_of(log_bytes)])
        .await
        .unwrap();
    let tool = store
        .put_asset(tool_bytes, "text/x-shellscript", &[])
        .await
        .unwrap();

    let run = store
        .record_run(RunDescription {
            schema: schema("example-run"),
            producer: producer(tool.artifact.clone()),
            environment: environment(),
            command: Some(RecordedCommand {
                argv: vec!["tool".into(), "request.json".into(), "result.json".into()],
                working_directory: None,
                declared_environment: BTreeMap::new(),
            }),
            network: NetworkPolicy::Deny,
            sandbox: SandboxPolicy::None,
            inputs: vec![RunBinding {
                role: "request".into(),
                artifact: request.artifact.clone(),
            }],
            outputs: vec![
                RunBinding {
                    role: "result".into(),
                    artifact: result.artifact.clone(),
                },
                RunBinding {
                    role: "log".into(),
                    artifact: log.artifact.clone(),
                },
            ],
            diagnostics: vec![],
            started_at: Utc.with_ymd_and_hms(2026, 9, 1, 8, 32, 0).unwrap(),
            finished_at: Utc.with_ymd_and_hms(2026, 9, 1, 8, 33, 0).unwrap(),
            parent: None,
            payload: json!({"z": 1, "a": {"nested": true}}),
        })
        .await
        .unwrap();

    let claim = store
        .record_claim(ClaimDescription {
            schema: schema("example-claim"),
            subject: "claim/opaque-subject".into(),
            state: "OpaqueState".into(),
            runs: vec![RunBinding {
                role: "oracle".into(),
                artifact: run.run.clone(),
            }],
            assets: vec![RunBinding {
                role: "source".into(),
                artifact: request.artifact.clone(),
            }],
            payload: json!({"cited": "opaque"}),
        })
        .await
        .unwrap();

    Fixture {
        request,
        result,
        log,
        tool,
        run,
        claim,
    }
}

#[tokio::test]
async fn asset_identity_is_content_media_type_and_declared_digests() {
    let dir = tempfile::tempdir().unwrap();
    let store = EvidenceStore::open(dir.path()).await.unwrap();
    let bytes = b"mesh bytes";
    let b3 = blake3_of(bytes);

    let a = store
        .put_asset(bytes, "application/octet-stream", std::slice::from_ref(&b3))
        .await
        .unwrap();
    let again = store
        .put_asset(bytes, "application/octet-stream", std::slice::from_ref(&b3))
        .await
        .unwrap();
    assert_eq!(a, again, "same bytes, media type, digests: same identity");
    assert_eq!(a.size, bytes.len() as u64);
    assert_eq!(a.declared, vec![b3.clone()]);
    assert_eq!(a.content.0.value, hex::encode(sha2::Sha256::digest(bytes)));

    let other_media = store
        .put_asset(bytes, "text/plain", std::slice::from_ref(&b3))
        .await
        .unwrap();
    assert_eq!(
        other_media.content, a.content,
        "same bytes: same content id"
    );
    assert_ne!(
        other_media.artifact, a.artifact,
        "different media type: different semantic identity"
    );

    let undeclared = store
        .put_asset(bytes, "application/octet-stream", &[])
        .await
        .unwrap();
    assert_ne!(undeclared.artifact, a.artifact);
    assert!(undeclared.declared.is_empty());

    // Declaring the sha256 too is allowed and recorded.
    let both = store
        .put_asset(
            bytes,
            "application/octet-stream",
            &[ExternalDigest::sha256(&a.content.0.value).unwrap(), b3],
        )
        .await
        .unwrap();
    assert_eq!(both.declared.len(), 2);

    let loaded = store.load_asset(&both.artifact).await.unwrap();
    assert_eq!(loaded, both);
    assert_eq!(store.read_asset(&both.artifact).await.unwrap(), bytes);
}

#[tokio::test]
async fn declared_digest_mismatch_is_refused_before_anything_is_stored() {
    let dir = tempfile::tempdir().unwrap();
    let store = EvidenceStore::open(dir.path()).await.unwrap();
    let bytes = b"real bytes";
    let wrong = blake3_of(b"other bytes");

    let error = store
        .put_asset(
            bytes,
            "application/octet-stream",
            std::slice::from_ref(&wrong),
        )
        .await
        .unwrap_err();
    assert!(
        matches!(&error, Error::DeclaredDigestMismatch { algorithm, declared, .. }
            if algorithm == "blake3" && declared == &wrong.value),
        "{error}"
    );
    let content = artifactum_core::ContentId(artifactum_core::hash_bytes(bytes));
    assert!(
        !store.store().contains_content(&content).await.unwrap(),
        "refused bytes never enter the CAS"
    );

    let wrong_sha = ExternalDigest::sha256(&"0".repeat(64)).unwrap();
    assert!(matches!(
        store
            .put_asset(bytes, "application/octet-stream", &[wrong_sha])
            .await,
        Err(Error::DeclaredDigestMismatch { .. })
    ));

    let file = dir.path().join("big.bin");
    std::fs::write(&file, bytes).unwrap();
    assert!(matches!(
        store
            .put_asset_file(&file, "application/octet-stream", &[wrong])
            .await,
        Err(Error::DeclaredDigestMismatch { .. })
    ));
    let ok = store
        .put_asset_file(&file, "application/octet-stream", &[blake3_of(bytes)])
        .await
        .unwrap();
    let in_memory = store
        .put_asset(bytes, "application/octet-stream", &[blake3_of(bytes)])
        .await
        .unwrap();
    assert_eq!(ok, in_memory, "file and in-memory ingest agree on identity");
}

#[tokio::test]
async fn run_and_claim_identities_are_deterministic_across_stores() {
    let dir_a = tempfile::tempdir().unwrap();
    let dir_b = tempfile::tempdir().unwrap();
    let store_a = EvidenceStore::open(dir_a.path()).await.unwrap();
    let store_b = EvidenceStore::open(dir_b.path()).await.unwrap();

    let a = ingest(&store_a).await;
    let b = ingest(&store_b).await;
    assert_eq!(
        a.run.run, b.run.run,
        "run collection identity is content-derived"
    );
    assert_eq!(a.run.receipt, b.run.receipt);
    assert_eq!(a.run.receipt_id, b.run.receipt_id);
    assert_eq!(a.run.action, b.run.action);
    assert_eq!(
        a.claim.claim, b.claim.claim,
        "claim identity is content-derived"
    );
    assert_eq!(a.claim.record, b.claim.record);
    assert!(!a.run.realization_reused);

    // Recording the same run again in the same store is idempotent in identity and reuses
    // the realization; the history plane gains no duplicate attestations.
    let before = store_a
        .metadata()
        .attestations(&a.result.artifact)
        .unwrap()
        .len();
    let again = ingest(&store_a).await;
    assert_eq!(again.run.run, a.run.run);
    assert!(again.run.realization_reused);
    assert_eq!(again.claim.claim, a.claim.claim);
    let after = store_a
        .metadata()
        .attestations(&a.result.artifact)
        .unwrap()
        .len();
    assert_eq!(before, after, "attestations are recorded once");

    // The receipt is canonical: payload keys are sorted regardless of authoring order.
    let loaded = store_a.load_run(&a.run.run).await.unwrap();
    assert_eq!(loaded.executable, a.tool.artifact);
    assert_eq!(a.run.executable, a.tool.artifact);
    let text =
        String::from_utf8(store_a.read_asset(&loaded.receipt_artifact).await.unwrap()).unwrap();
    assert!(
        text.contains(r#""payload":{"a":{"nested":true},"z":1}"#),
        "{text}"
    );
    assert_eq!(loaded.receipt.activity.action, a.run.action);
    assert_eq!(loaded.receipt.inputs.len(), 1);
    assert_eq!(loaded.receipt.outputs.len(), 2);
}

#[tokio::test]
async fn timestamps_and_payload_change_the_run_but_not_the_action() {
    let dir = tempfile::tempdir().unwrap();
    let store = EvidenceStore::open(dir.path()).await.unwrap();
    let f = ingest(&store).await;

    let mut description = RunDescription {
        schema: schema("example-run"),
        producer: producer(f.tool.artifact.clone()),
        environment: environment(),
        command: Some(RecordedCommand {
            argv: vec!["tool".into(), "request.json".into(), "result.json".into()],
            working_directory: None,
            declared_environment: BTreeMap::new(),
        }),
        network: NetworkPolicy::Deny,
        sandbox: SandboxPolicy::None,
        inputs: vec![RunBinding {
            role: "request".into(),
            artifact: f.request.artifact.clone(),
        }],
        outputs: vec![
            RunBinding {
                role: "result".into(),
                artifact: f.result.artifact.clone(),
            },
            RunBinding {
                role: "log".into(),
                artifact: f.log.artifact.clone(),
            },
        ],
        diagnostics: vec![],
        started_at: Utc.with_ymd_and_hms(2026, 9, 1, 9, 0, 0).unwrap(),
        finished_at: Utc.with_ymd_and_hms(2026, 9, 1, 9, 1, 0).unwrap(),
        parent: None,
        payload: json!({"z": 1, "a": {"nested": true}}),
    };
    let later = store.record_run(description.clone()).await.unwrap();
    assert_eq!(later.action, f.run.action, "same computation identity");
    assert_ne!(
        later.receipt_id, f.run.receipt_id,
        "a different activity instance"
    );
    assert_ne!(later.run, f.run.run);
    assert!(later.realization_reused, "same action, same outputs");

    description
        .command
        .as_mut()
        .unwrap()
        .argv
        .push("--flag".into());
    let other_action = store.record_run(description).await.unwrap();
    assert_ne!(
        other_action.action, f.run.action,
        "argv enters the action key"
    );
    assert!(!other_action.realization_reused);

    // Producing a run over an unknown artifact is refused.
    let missing = ArtifactId(digest('9'));
    let error = store
        .record_run(RunDescription {
            schema: schema("example-run"),
            producer: producer(f.tool.artifact.clone()),
            environment: environment(),
            command: None,
            network: NetworkPolicy::Deny,
            sandbox: SandboxPolicy::None,
            inputs: vec![],
            outputs: vec![RunBinding {
                role: "ghost".into(),
                artifact: missing,
            }],
            diagnostics: vec![],
            started_at: Utc.with_ymd_and_hms(2026, 9, 1, 9, 0, 0).unwrap(),
            finished_at: Utc.with_ymd_and_hms(2026, 9, 1, 9, 1, 0).unwrap(),
            parent: None,
            payload: json!(null),
        })
        .await
        .unwrap_err();
    assert!(matches!(error, Error::Store(_)), "{error}");
}

#[tokio::test]
async fn sealed_claim_verifies_and_snapshots_every_digest() {
    let dir = tempfile::tempdir().unwrap();
    let store = EvidenceStore::open(dir.path()).await.unwrap();
    let f = ingest(&store).await;

    let record = &f.claim.record;
    assert_eq!(record.subject, "claim/opaque-subject");
    assert_eq!(record.state, "OpaqueState");
    assert_eq!(record.runs.len(), 1);
    assert_eq!(record.runs[0].receipt_id, f.run.receipt_id);
    assert_eq!(record.runs[0].inputs[0].artifact, f.request.artifact);
    assert_eq!(record.runs[0].inputs[0].declared, f.request.declared);
    assert_eq!(record.runs[0].outputs.len(), 2);
    assert_eq!(record.assets[0].role, "source");
    let cited = f.claim.cited_assets();
    let paths: Vec<&str> = cited.iter().map(|(p, _)| p.as_str()).collect();
    assert_eq!(
        paths,
        vec![
            "run/oracle/code/executable",
            "run/oracle/input/request",
            "run/oracle/output/log",
            "run/oracle/output/result",
            "asset/source"
        ]
    );

    let verification = store.verify_claim(&f.claim.claim).await.unwrap();
    assert!(verification.ok, "{:?}", verification.failures);
    assert!(verification.record_ok);
    assert_eq!(verification.runs.len(), 1);
    assert!(verification.runs[0].receipt_ok && verification.runs[0].bindings_ok);
    assert_eq!(verification.assets.len(), 5);
    for asset in &verification.assets {
        assert!(asset.content_ok && asset.manifest_ok, "{asset:?}");
        for d in &asset.declared {
            assert!(d.ok, "{d:?}");
            assert_eq!(d.actual.as_deref(), Some(d.expected.as_str()));
        }
    }
    let result_check = verification
        .assets
        .iter()
        .find(|a| a.path == "run/oracle/output/result")
        .unwrap();
    assert_eq!(result_check.declared[0].algorithm, "blake3");

    let loaded = store.load_claim(&f.claim.claim).await.unwrap();
    assert_eq!(loaded, f.claim);

    // Claims over unknown runs, non-runs, or bad roles are refused.
    assert!(matches!(
        store
            .record_claim(ClaimDescription {
                schema: schema("example-claim"),
                subject: "s".into(),
                state: "S".into(),
                runs: vec![RunBinding {
                    role: "asset-as-run".into(),
                    artifact: f.result.artifact.clone()
                }],
                assets: vec![],
                payload: json!(null),
            })
            .await,
        Err(Error::NotARun(..))
    ));
    assert!(matches!(
        store
            .record_claim(ClaimDescription {
                schema: schema("example-claim"),
                subject: "".into(),
                state: "S".into(),
                runs: vec![],
                assets: vec![],
                payload: json!(null),
            })
            .await,
        Err(Error::InvalidRole(_))
    ));
    assert!(matches!(
        store.verify_claim(&f.run.run).await,
        Err(Error::NotAClaim(..))
    ));
}

#[tokio::test]
async fn tampered_bytes_fail_verification_by_name() {
    let dir = tempfile::tempdir().unwrap();
    let store = EvidenceStore::open(dir.path()).await.unwrap();
    let f = ingest(&store).await;

    // Corrupt the result bytes in place (same length so size alone cannot catch it).
    let path = store.store().content_path(&f.result.content).unwrap();
    let mut bytes = std::fs::read(&path).unwrap();
    bytes[0] ^= 0xff;
    std::fs::write(&path, &bytes).unwrap();

    let verification = store.verify_claim(&f.claim.claim).await.unwrap();
    assert!(!verification.ok);
    let bad = verification
        .assets
        .iter()
        .find(|a| a.path == "run/oracle/output/result")
        .unwrap();
    assert!(!bad.content_ok);
    assert!(bad.manifest_ok, "the manifest itself is untouched");
    assert!(
        !bad.declared[0].ok,
        "the declared blake3 no longer re-hashes"
    );
    assert!(bad.declared[0].actual.is_some());
    let untouched = verification
        .assets
        .iter()
        .filter(|a| a.path != "run/oracle/output/result")
        .all(|a| a.ok);
    assert!(untouched, "only the corrupted asset fails");
    assert!(
        verification
            .failures
            .iter()
            .any(|m| m.contains("run/oracle/output/result") && m.contains("blake3")),
        "{:?}",
        verification.failures
    );

    // Remove the log bytes entirely: reported, not an error.
    std::fs::remove_file(store.store().content_path(&f.log.content).unwrap()).unwrap();
    let verification = store.verify_claim(&f.claim.claim).await.unwrap();
    let log = verification
        .assets
        .iter()
        .find(|a| a.path == "run/oracle/output/log")
        .unwrap();
    assert!(!log.content_ok && !log.ok);
    assert!(
        verification
            .failures
            .iter()
            .any(|m| m.contains("run/oracle/output/log") && m.contains("failed to load"))
    );
}

#[tokio::test]
async fn tampered_receipt_fails_verification() {
    let dir = tempfile::tempdir().unwrap();
    let store = EvidenceStore::open(dir.path()).await.unwrap();
    let f = ingest(&store).await;

    let receipt_manifest = store.store().load_artifact(&f.run.receipt).await.unwrap();
    let path = store
        .store()
        .content_path(&receipt_manifest.content)
        .unwrap();
    let text = std::fs::read_to_string(&path).unwrap();
    std::fs::write(&path, text.replace(r#""z":1"#, r#""z":2"#)).unwrap();

    assert!(store.load_run(&f.run.run).await.is_err());
    let verification = store.verify_claim(&f.claim.claim).await.unwrap();
    assert!(!verification.ok);
    assert!(!verification.runs[0].ok);
    assert!(
        verification
            .failures
            .iter()
            .any(|m| m.contains("run `oracle`")),
        "{:?}",
        verification.failures
    );
}

#[tokio::test]
async fn lineage_walks_from_asset_to_run_to_claim() {
    let dir = tempfile::tempdir().unwrap();
    let store = EvidenceStore::open(dir.path()).await.unwrap();
    let f = ingest(&store).await;

    let result = store.explain(&f.result.artifact).await.unwrap();
    assert_eq!(result.kind, EvidenceKind::Asset);
    assert_eq!(result.produced_by.len(), 1);
    assert_eq!(result.produced_by[0].run, f.run.run);
    assert_eq!(result.produced_by[0].receipt_id, f.run.receipt_id);
    assert_eq!(result.produced_by[0].role, "result");
    assert!(result.consumed_by.is_empty());
    assert_eq!(result.cited_by.len(), 1);
    assert_eq!(result.cited_by[0].claim, f.claim.claim);
    assert_eq!(result.cited_by[0].subject, "claim/opaque-subject");
    assert_eq!(result.cited_by[0].role, "run/oracle/result");
    assert_eq!(result.cited_by[0].via_run.as_ref(), Some(&f.run.run));

    let request = store.explain(&f.request.artifact).await.unwrap();
    assert_eq!(request.consumed_by.len(), 1);
    assert!(request.produced_by.is_empty());
    let roles: Vec<&str> = request.cited_by.iter().map(|c| c.role.as_str()).collect();
    assert_eq!(roles, vec!["asset/source", "run/oracle/request"]);

    let run = store.explain(&f.run.run).await.unwrap();
    assert_eq!(run.kind, EvidenceKind::Run);
    assert_eq!(run.cited_by.len(), 1);
    assert_eq!(run.cited_by[0].role, "run/oracle");

    let claim = store.explain(&f.claim.claim).await.unwrap();
    assert_eq!(claim.kind, EvidenceKind::Claim);
    let sealed = store.metadata().attestations(&f.claim.claim).unwrap();
    assert_eq!(sealed.len(), 1);
    assert_eq!(sealed[0].predicate_type, CLAIM_PREDICATE);
    assert_eq!(sealed[0].issuer.as_deref(), Some(ATTESTATION_ISSUER));

    let receipt = store.explain(&f.run.receipt).await.unwrap();
    assert_eq!(receipt.kind, EvidenceKind::Receipt);
    let tool = store.explain(&f.tool.artifact).await.unwrap();
    assert_eq!(tool.executed_by.len(), 1);
    assert_eq!(tool.executed_by[0].run, f.run.run);
    assert_eq!(tool.executed_by[0].role, "executable");
    assert_eq!(tool.cited_by.len(), 1);
    assert_eq!(tool.cited_by[0].role, "run/oracle/executable");
    let record = store.explain(&f.claim.record_artifact).await.unwrap();
    assert_eq!(record.kind, EvidenceKind::ClaimRecord);

    // The engine's own lineage walk sees the intrinsic realization and its inputs.
    let nodes = store.engine().lineage(&f.result.artifact).unwrap();
    let node = nodes
        .iter()
        .find(|n| n.artifact == f.result.artifact)
        .unwrap();
    assert_eq!(node.producers.len(), 1);
    assert_eq!(node.producers[0].action, f.run.action);
    assert!(node.inputs.contains(&f.request.artifact));
    assert!(
        node.inputs.contains(&f.tool.artifact),
        "the executable is code lineage"
    );

    let predicates: Vec<String> = store
        .metadata()
        .attestations(&f.result.artifact)
        .unwrap()
        .into_iter()
        .map(|a| a.predicate_type)
        .collect();
    assert_eq!(predicates, vec![PRODUCED_BY_PREDICATE]);
    let run_predicates: Vec<String> = store
        .metadata()
        .attestations(&f.run.run)
        .unwrap()
        .into_iter()
        .map(|a| a.predicate_type)
        .collect();
    assert_eq!(run_predicates, vec![CITED_BY_PREDICATE]);
}

#[tokio::test]
async fn gc_keeps_everything_a_sealed_claim_reaches() {
    let dir = tempfile::tempdir().unwrap();
    let store = EvidenceStore::open(dir.path()).await.unwrap();
    let f = ingest(&store).await;
    let orphan = store
        .put_asset(b"never cited", "application/octet-stream", &[])
        .await
        .unwrap();

    // Roots exactly as the CLI computes them: refs plus metadata roots. Use a zero-day
    // realization window so only attestations (the claim itself) root the graph.
    let roots = store.engine().gc_roots(0).unwrap();
    assert!(
        roots.contains(&f.claim.claim),
        "the sealed claim is a permanent root"
    );
    let report = store.store().gc(false, &roots).await.unwrap();
    assert!(report.objects_removed >= 1, "{report:?}");

    for asset in [&f.request, &f.result, &f.log, &f.tool] {
        assert!(store.store().verify_content(&asset.content).await.unwrap());
    }
    assert!(
        !store
            .store()
            .contains_content(&orphan.content)
            .await
            .unwrap(),
        "an orphaned, never-cited asset is collectable"
    );
    let verification = store.verify_claim(&f.claim.claim).await.unwrap();
    assert!(verification.ok, "{:?}", verification.failures);
    assert!(store.load_run(&f.run.run).await.is_ok());
}

#[tokio::test]
async fn tagging_a_claim_is_immutable() {
    let dir = tempfile::tempdir().unwrap();
    let store = EvidenceStore::open(dir.path()).await.unwrap();
    let f = ingest(&store).await;

    store
        .tag_claim(&f.claim.claim, "claims/opaque")
        .await
        .unwrap();
    assert_eq!(
        store.store().get_ref("claims/opaque").await.unwrap(),
        Some(f.claim.claim.clone())
    );
    assert!(
        store
            .tag_claim(&f.claim.claim, "claims/opaque")
            .await
            .is_err()
    );
    assert!(matches!(
        store.tag_claim(&f.run.run, "claims/not-a-claim").await,
        Err(Error::NotAClaim(..))
    ));
    let roots = store.engine().gc_roots(0).unwrap();
    let (artifacts, _) = store.store().reachable_graph(&roots).await.unwrap();
    assert!(artifacts.contains(&f.result.artifact.to_string()));
}
