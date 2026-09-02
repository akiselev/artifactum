//! Durable raw evidence with lineage over the Artifactum CAS.
//!
//! A producer (Sinbad, or any other system) hands Artifactum opaque bytes with a declared
//! media type and the digests it recorded for them. Artifactum stores the bytes immutably,
//! verifies every declared digest against the bytes, and records three content-addressed
//! objects whose identities are pure functions of their contents:
//!
//! ```text
//!   asset   = Blob artifact          (bytes + media type + declared digests)
//!   run     = Collection artifact    { receipt, code/executable, input/<role>, output/<role> }
//!             receipt = Blob artifact holding a canonical `ReceiptEnvelope`
//!   claim   = Collection artifact    { record, run/<role>, asset/<role> }
//!             record  = Blob artifact holding a `ClaimRecord` that snapshots every
//!                       cited run receipt id and every asset digest
//! ```
//!
//! Artifactum never interprets an asset, a receipt payload, a claim subject, or a claim
//! state: they are opaque bytes, opaque JSON, and opaque strings. Roles are producer-chosen
//! names. What Artifactum owns is identity, immutability, reachability, and the
//! re-hash verification that a stored claim's assets still are the bytes the claim cites.
//!
//! The metadata plane receives the reverse index (which runs produced or consumed an asset,
//! which claims cite a run or an asset) as attestations, and every run as an intrinsic
//! realization so `Engine::lineage` and graph GC see evidence like any other artifact.

use std::collections::{BTreeMap, BTreeSet};
use std::path::Path;

use artifactum_core::{
    ActionKey, ActionSpec, ArtifactId, ArtifactManifest, CachePolicy, CollectionEntry,
    CollectionManifest, ContentId, ContentKind, Digest, NetworkPolicy, OutputSpec, SandboxPolicy,
};
use artifactum_engine::Engine;
use artifactum_metadata::MetadataStore;
use artifactum_receipt::{
    ActivityIdentity, ArtifactBinding, ExecutionEnvironment, PortableDiagnostic, ProducerIdentity,
    RECEIPT_ENVELOPE_SCHEMA, ReceiptEnvelope, ReceiptId, RecordedCommand, SchemaIdentity,
};
use artifactum_store::{ArtifactStore, ContentStore};
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use sha2::{Digest as _, Sha256};
use thiserror::Error;
use tokio::io::AsyncReadExt;

/// Format version recorded on every run collection artifact.
pub const EVIDENCE_RUN_SCHEMA: &str = "artifactum-evidence-run/1";
/// Format version recorded on every claim collection artifact.
pub const EVIDENCE_CLAIM_SCHEMA: &str = "artifactum-evidence-claim/1";
/// Media type of the canonical receipt blob inside a run.
pub const RECEIPT_MEDIA_TYPE: &str = "application/vnd.artifactum.receipt+json";
/// Media type of the canonical claim-record blob inside a claim.
pub const CLAIM_RECORD_MEDIA_TYPE: &str = "application/vnd.artifactum.evidence-claim+json";

/// Manifest annotation naming what an evidence artifact is (`receipt`, `run`, `claim-record`,
/// `claim`). Plain assets carry no kind annotation.
pub const KIND_ANNOTATION: &str = "artifactum.evidence.kind";
/// Manifest annotation prefix under which each declared digest is recorded, keyed by
/// algorithm: `artifactum.evidence.digest.blake3 = <hex>`.
pub const DIGEST_ANNOTATION_PREFIX: &str = "artifactum.evidence.digest.";
/// Manifest annotation carrying a claim's opaque subject.
pub const SUBJECT_ANNOTATION: &str = "artifactum.evidence.subject";
/// Manifest annotation carrying a claim's opaque state.
pub const STATE_ANNOTATION: &str = "artifactum.evidence.state";

/// Attestation predicate on an asset: a run produced it.
pub const PRODUCED_BY_PREDICATE: &str = "artifactum.evidence/produced-by/1";
/// Attestation predicate on an asset: a run consumed it.
pub const CONSUMED_BY_PREDICATE: &str = "artifactum.evidence/consumed-by/1";
/// Attestation predicate on an asset: a run executed it (the producer executable).
pub const EXECUTED_BY_PREDICATE: &str = "artifactum.evidence/executed-by/1";
/// Attestation predicate on a run or asset: a claim cites it.
pub const CITED_BY_PREDICATE: &str = "artifactum.evidence/cited-by/1";
/// Attestation predicate on a claim collection itself; makes the claim a metadata GC root.
pub const CLAIM_PREDICATE: &str = "artifactum.evidence/claim/1";
/// Issuer recorded on every attestation this crate writes.
pub const ATTESTATION_ISSUER: &str = "artifactum-evidence";

const RECEIPT_KEY: &str = "receipt";
const EXECUTABLE_KEY: &str = "code/executable";
const RECORD_KEY: &str = "record";
const INPUT_PREFIX: &str = "input/";
const OUTPUT_PREFIX: &str = "output/";
const RUN_PREFIX: &str = "run/";
const ASSET_PREFIX: &str = "asset/";

#[derive(Debug, Error)]
pub enum Error {
    #[error("store error: {0}")]
    Store(#[from] artifactum_store::Error),
    #[error("metadata error: {0}")]
    Metadata(#[from] artifactum_metadata::Error),
    #[error("engine error: {0}")]
    Engine(#[from] artifactum_engine::Error),
    #[error("core error: {0}")]
    Core(#[from] artifactum_core::Error),
    #[error("receipt contract error: {0}")]
    Receipt(#[from] artifactum_receipt::ContractError),
    #[error("json error: {0}")]
    Json(#[from] serde_json::Error),
    #[error("I/O error: {0}")]
    Io(#[from] std::io::Error),
    #[error("unsupported digest algorithm `{0}` (supported: sha256, blake3)")]
    UnsupportedDigestAlgorithm(String),
    #[error("invalid digest `{0}`; expected <algorithm>:<64 lowercase hex>")]
    InvalidDigest(String),
    #[error("declared {algorithm} digest {declared} does not match the bytes ({actual})")]
    DeclaredDigestMismatch {
        algorithm: String,
        declared: String,
        actual: String,
    },
    #[error("conflicting declared {algorithm} digests: {first} and {second}")]
    ConflictingDeclaredDigests {
        algorithm: String,
        first: String,
        second: String,
    },
    #[error("evidence role must be nonempty and unique; `{0}` is not")]
    InvalidRole(String),
    #[error("artifact {0} is not an evidence asset (expected a blob manifest)")]
    NotAnAsset(ArtifactId),
    #[error("artifact {0} is not an evidence run: {1}")]
    NotARun(ArtifactId, String),
    #[error("artifact {0} is not an evidence claim: {1}")]
    NotAClaim(ArtifactId, String),
    #[error("file `{path}` changed while it was being imported: hashed {hashed}, stored {stored}")]
    FileChangedDuringImport {
        path: String,
        hashed: String,
        stored: String,
    },
}
pub type Result<T, E = Error> = std::result::Result<T, E>;

/// A digest the producer recorded for some bytes, in the producer's own algorithm.
///
/// Artifactum's content identity stays SHA-256; a declared digest is verified against the
/// bytes on ingest and again on claim verification, so a producer whose evidence graph is
/// keyed by blake3 (Sinbad) can prove its recorded digests are the stored bytes.
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct ExternalDigest {
    pub algorithm: String,
    pub value: String,
}

impl ExternalDigest {
    /// Parse `<algorithm>:<hex>`; the algorithm must be supported and the hex 64 lowercase
    /// characters (both supported algorithms are 256-bit).
    pub fn parse(qualified: &str) -> Result<Self> {
        let (algorithm, value) = qualified
            .split_once(':')
            .ok_or_else(|| Error::InvalidDigest(qualified.to_owned()))?;
        Self::new(algorithm, value)
    }

    pub fn new(algorithm: &str, value: &str) -> Result<Self> {
        if !is_supported_algorithm(algorithm) {
            return Err(Error::UnsupportedDigestAlgorithm(algorithm.to_owned()));
        }
        if value.len() != 64
            || !value
                .bytes()
                .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b))
        {
            return Err(Error::InvalidDigest(format!("{algorithm}:{value}")));
        }
        Ok(Self {
            algorithm: algorithm.to_owned(),
            value: value.to_owned(),
        })
    }

    pub fn blake3(value: &str) -> Result<Self> {
        Self::new("blake3", value)
    }

    pub fn sha256(value: &str) -> Result<Self> {
        Self::new("sha256", value)
    }

    /// Compute the digest of `bytes` under a supported algorithm.
    pub fn compute(algorithm: &str, bytes: &[u8]) -> Result<Self> {
        let value = match algorithm {
            "sha256" => hex::encode(Sha256::digest(bytes)),
            "blake3" => blake3::hash(bytes).to_hex().to_string(),
            other => return Err(Error::UnsupportedDigestAlgorithm(other.to_owned())),
        };
        Ok(Self {
            algorithm: algorithm.to_owned(),
            value,
        })
    }

    #[must_use]
    pub fn as_qualified(&self) -> String {
        format!("{}:{}", self.algorithm, self.value)
    }
}

impl std::fmt::Display for ExternalDigest {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}:{}", self.algorithm, self.value)
    }
}

fn is_supported_algorithm(algorithm: &str) -> bool {
    matches!(algorithm, "sha256" | "blake3")
}

/// Streaming multi-algorithm hasher used for file imports.
struct MultiHasher {
    sha256: Sha256,
    blake3: Option<blake3::Hasher>,
    size: u64,
}

impl MultiHasher {
    fn new(want_blake3: bool) -> Self {
        Self {
            sha256: Sha256::new(),
            blake3: want_blake3.then(blake3::Hasher::new),
            size: 0,
        }
    }
    fn update(&mut self, bytes: &[u8]) {
        self.sha256.update(bytes);
        if let Some(h) = self.blake3.as_mut() {
            h.update(bytes);
        }
        self.size = self.size.saturating_add(bytes.len() as u64);
    }
    fn finish(self) -> (BTreeMap<String, String>, u64) {
        let mut out = BTreeMap::new();
        out.insert("sha256".to_owned(), hex::encode(self.sha256.finalize()));
        if let Some(h) = self.blake3 {
            out.insert("blake3".to_owned(), h.finalize().to_hex().to_string());
        }
        (out, self.size)
    }
}

async fn hash_path(path: &Path, want_blake3: bool) -> Result<(BTreeMap<String, String>, u64)> {
    let mut file = tokio::fs::File::open(path).await?;
    let mut hasher = MultiHasher::new(want_blake3);
    let mut buf = vec![0u8; 256 * 1024];
    loop {
        let n = file.read(&mut buf).await?;
        if n == 0 {
            break;
        }
        hasher.update(&buf[..n]);
    }
    Ok(hasher.finish())
}

/// Dedupe declared digests by algorithm; two different values for one algorithm are refused.
fn normalize_declared(declared: &[ExternalDigest]) -> Result<Vec<ExternalDigest>> {
    let mut by_algorithm: BTreeMap<String, ExternalDigest> = BTreeMap::new();
    for digest in declared {
        // Re-validate so a hand-built struct cannot smuggle an unsupported algorithm.
        let digest = ExternalDigest::new(&digest.algorithm, &digest.value)?;
        match by_algorithm.get(&digest.algorithm) {
            Some(existing) if existing.value != digest.value => {
                return Err(Error::ConflictingDeclaredDigests {
                    algorithm: digest.algorithm.clone(),
                    first: existing.value.clone(),
                    second: digest.value.clone(),
                });
            }
            Some(_) => {}
            None => {
                by_algorithm.insert(digest.algorithm.clone(), digest);
            }
        }
    }
    Ok(by_algorithm.into_values().collect())
}

fn check_declared(declared: &[ExternalDigest], actual: &BTreeMap<String, String>) -> Result<()> {
    for digest in declared {
        let observed = actual
            .get(&digest.algorithm)
            .ok_or_else(|| Error::UnsupportedDigestAlgorithm(digest.algorithm.clone()))?;
        if observed != &digest.value {
            return Err(Error::DeclaredDigestMismatch {
                algorithm: digest.algorithm.clone(),
                declared: digest.value.clone(),
                actual: observed.clone(),
            });
        }
    }
    Ok(())
}

fn declared_from_manifest(manifest: &ArtifactManifest) -> Result<Vec<ExternalDigest>> {
    let mut out = Vec::new();
    for (key, value) in &manifest.annotations {
        if let Some(algorithm) = key.strip_prefix(DIGEST_ANNOTATION_PREFIX) {
            out.push(ExternalDigest::new(algorithm, value)?);
        }
    }
    Ok(out)
}

/// One immutable evidence asset: exact bytes under a declared media type.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct StoredAsset {
    /// Semantic identity: content + media type + declared digests.
    pub artifact: ArtifactId,
    /// SHA-256 of the exact bytes.
    pub content: ContentId,
    pub size: u64,
    pub media_type: String,
    /// Producer-declared digests, sorted by algorithm; each was verified against the bytes.
    pub declared: Vec<ExternalDigest>,
}

impl StoredAsset {
    /// The declared digest under `algorithm`, if the producer declared one.
    #[must_use]
    pub fn declared_digest(&self, algorithm: &str) -> Option<&ExternalDigest> {
        self.declared.iter().find(|d| d.algorithm == algorithm)
    }
}

/// One role-named artifact bound to a run.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct RunBinding {
    pub role: String,
    pub artifact: ArtifactId,
}

/// Everything a producer states about one completed activity whose outputs are already
/// stored assets. Artifactum derives the canonical `ActionSpec`/`ActionKey` and the
/// `ReceiptEnvelope` from it; nothing here is interpreted.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct RunDescription {
    /// Schema of `payload`, owned by the producer (for example `sinbad-oracle-run`).
    pub schema: SchemaIdentity,
    pub producer: ProducerIdentity,
    pub environment: ExecutionEnvironment,
    /// The command the producer actually ran, if any. Its argv enters the `ActionKey`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub command: Option<RecordedCommand>,
    /// Network policy the producer declares its activity ran under. Artifactum did not
    /// enforce it; it is recorded because it enters the computation identity.
    pub network: NetworkPolicy,
    /// Sandbox policy the producer declares its activity ran under (see `network`).
    pub sandbox: SandboxPolicy,
    /// Role-named input assets (already stored).
    pub inputs: Vec<RunBinding>,
    /// Role-named output assets (already stored).
    pub outputs: Vec<RunBinding>,
    #[serde(default)]
    pub diagnostics: Vec<PortableDiagnostic>,
    pub started_at: DateTime<Utc>,
    pub finished_at: DateTime<Utc>,
    /// Receipt of a parent activity, if this run is a step of a larger one.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub parent: Option<ReceiptId>,
    /// Opaque producer receipt body. Object keys are canonicalized (sorted) on storage.
    pub payload: serde_json::Value,
}

/// A recorded run: the run collection, its receipt, and the action identity behind it.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct StoredRun {
    pub run: ArtifactId,
    pub receipt: ArtifactId,
    pub receipt_id: ReceiptId,
    pub action: ActionKey,
    pub executable: ArtifactId,
    pub inputs: Vec<RunBinding>,
    pub outputs: Vec<RunBinding>,
    /// True when the metadata plane already held a realization with these exact outputs.
    pub realization_reused: bool,
}

/// A run loaded back from the store, with its receipt re-validated.
#[derive(Clone, Debug)]
pub struct LoadedRun {
    pub run: ArtifactId,
    pub receipt_artifact: ArtifactId,
    pub receipt: ReceiptEnvelope<serde_json::Value>,
    pub executable: ArtifactId,
    pub inputs: Vec<RunBinding>,
    pub outputs: Vec<RunBinding>,
}

/// A claim a producer wants sealed over stored runs and assets.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ClaimDescription {
    /// Schema of `payload`, owned by the producer (for example `sinbad-support-claim`).
    pub schema: SchemaIdentity,
    /// Opaque producer claim identity (for example a Sinbad support-claim id).
    pub subject: String,
    /// Opaque producer state (for example `IndependentlyVerified`).
    pub state: String,
    pub runs: Vec<RunBinding>,
    /// Assets cited directly (a sealed manifest, frozen source bytes, ...).
    pub assets: Vec<RunBinding>,
    /// Opaque producer claim body. Object keys are canonicalized (sorted) on storage.
    pub payload: serde_json::Value,
}

/// A snapshot of one asset as a claim cites it.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ClaimAsset {
    pub role: String,
    pub artifact: ArtifactId,
    pub content: ContentId,
    pub size: u64,
    pub media_type: String,
    pub declared: Vec<ExternalDigest>,
}

/// A snapshot of one run as a claim cites it.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ClaimRun {
    pub role: String,
    pub run: ArtifactId,
    pub receipt: ArtifactId,
    pub receipt_id: ReceiptId,
    pub action: ActionKey,
    pub executable: ClaimAsset,
    pub inputs: Vec<ClaimAsset>,
    pub outputs: Vec<ClaimAsset>,
}

/// The canonical claim record: a self-contained ledger of every cited receipt id and every
/// cited asset digest, stored as the `record` member of the claim collection.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ClaimRecord {
    pub format: String,
    pub schema: SchemaIdentity,
    pub subject: String,
    pub state: String,
    pub runs: Vec<ClaimRun>,
    pub assets: Vec<ClaimAsset>,
    pub payload: serde_json::Value,
}

/// A sealed claim.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct StoredClaim {
    pub claim: ArtifactId,
    pub record_artifact: ArtifactId,
    pub record: ClaimRecord,
}

impl StoredClaim {
    /// Every asset the claim cites, directly or through a run, with its path in the claim.
    #[must_use]
    pub fn cited_assets(&self) -> Vec<(String, ClaimAsset)> {
        cited_assets(&self.record)
    }
}

fn cited_assets(record: &ClaimRecord) -> Vec<(String, ClaimAsset)> {
    let mut out = Vec::new();
    for run in &record.runs {
        out.push((
            format!("{RUN_PREFIX}{}/{EXECUTABLE_KEY}", run.role),
            run.executable.clone(),
        ));
        for asset in &run.inputs {
            out.push((
                format!("{RUN_PREFIX}{}/{INPUT_PREFIX}{}", run.role, asset.role),
                asset.clone(),
            ));
        }
        for asset in &run.outputs {
            out.push((
                format!("{RUN_PREFIX}{}/{OUTPUT_PREFIX}{}", run.role, asset.role),
                asset.clone(),
            ));
        }
    }
    for asset in &record.assets {
        out.push((format!("{ASSET_PREFIX}{}", asset.role), asset.clone()));
    }
    out
}

/// Result of recomputing one declared digest.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct DigestCheck {
    pub algorithm: String,
    pub expected: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub actual: Option<String>,
    pub ok: bool,
}

/// Result of re-hashing one cited asset.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct AssetCheck {
    pub path: String,
    pub artifact: ArtifactId,
    pub content: ContentId,
    pub size: u64,
    /// The stored bytes hash to the recorded SHA-256 content identity.
    pub content_ok: bool,
    /// The artifact manifest still carries exactly the declared digests the claim recorded.
    pub manifest_ok: bool,
    pub declared: Vec<DigestCheck>,
    pub ok: bool,
}

/// Result of re-validating one cited run.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct RunCheck {
    pub role: String,
    pub run: ArtifactId,
    pub receipt: ArtifactId,
    pub receipt_id: ReceiptId,
    /// The receipt re-loads, its id recomputes, and it matches the claim's snapshot.
    pub receipt_ok: bool,
    /// The run collection's bindings equal the receipt's and the claim's snapshot.
    pub bindings_ok: bool,
    pub ok: bool,
}

/// The complete verification of one sealed claim. `ok` is true only when every check is.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ClaimVerification {
    pub claim: ArtifactId,
    pub subject: String,
    pub state: String,
    pub record_ok: bool,
    pub runs: Vec<RunCheck>,
    pub assets: Vec<AssetCheck>,
    pub failures: Vec<String>,
    pub ok: bool,
}

/// What kind of evidence object an artifact is.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum EvidenceKind {
    Asset,
    Receipt,
    Run,
    ClaimRecord,
    Claim,
    Unknown,
}

/// A run that produced or consumed an artifact.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct RunCitation {
    pub run: ArtifactId,
    pub receipt: ArtifactId,
    pub receipt_id: ReceiptId,
    pub role: String,
}

/// A claim that cites an artifact, directly or through a run.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ClaimCitation {
    pub claim: ArtifactId,
    pub subject: String,
    pub state: String,
    pub role: String,
    /// Set when the citation reaches this artifact through a run rather than directly.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub via_run: Option<ArtifactId>,
}

/// Upward lineage of one artifact: the runs that produced or consumed it and the claims
/// that cite it.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct EvidenceExplanation {
    pub artifact: ArtifactId,
    pub kind: EvidenceKind,
    pub produced_by: Vec<RunCitation>,
    pub consumed_by: Vec<RunCitation>,
    pub executed_by: Vec<RunCitation>,
    pub cited_by: Vec<ClaimCitation>,
}

/// The evidence API over one Artifactum store and its metadata plane.
#[derive(Clone)]
pub struct EvidenceStore {
    engine: Engine,
}

impl EvidenceStore {
    /// Open (creating if absent) a store rooted at `root`: the CAS under `root/store` and the
    /// metadata plane at `root/metadata.sqlite`.
    pub async fn open(root: impl AsRef<Path>) -> Result<Self> {
        let root = root.as_ref();
        let store = ArtifactStore::open(root.join("store")).await?;
        let metadata = MetadataStore::open(root.join("metadata.sqlite"))?;
        Self::with(store, metadata).await
    }

    /// Wrap an existing store and metadata plane.
    pub async fn with(store: ArtifactStore, metadata: MetadataStore) -> Result<Self> {
        let engine = Engine::builder()
            .store(store)
            .metadata(metadata)
            .build()
            .await?;
        Ok(Self { engine })
    }

    #[must_use]
    pub fn engine(&self) -> &Engine {
        &self.engine
    }
    #[must_use]
    pub fn store(&self) -> &ArtifactStore {
        self.engine.store()
    }
    #[must_use]
    pub fn metadata(&self) -> &MetadataStore {
        self.engine.metadata()
    }

    // ----------------------------------------------------------------------------------
    // Assets
    // ----------------------------------------------------------------------------------

    /// Store `bytes` as an immutable asset after verifying every declared digest.
    pub async fn put_asset(
        &self,
        bytes: &[u8],
        media_type: &str,
        declared: &[ExternalDigest],
    ) -> Result<StoredAsset> {
        let declared = normalize_declared(declared)?;
        let mut hasher = MultiHasher::new(declared.iter().any(|d| d.algorithm == "blake3"));
        hasher.update(bytes);
        let (actual, size) = hasher.finish();
        check_declared(&declared, &actual)?;
        let content = self.store().put_bytes(bytes).await?;
        debug_assert_eq!(content.0.value, actual["sha256"]);
        self.commit_asset(content, size, media_type, declared).await
    }

    /// Stream a file into the store as an immutable asset after verifying every declared
    /// digest. The file is hashed once for every requested algorithm, then imported; if its
    /// bytes change between the two passes the import is refused.
    pub async fn put_asset_file(
        &self,
        path: impl AsRef<Path>,
        media_type: &str,
        declared: &[ExternalDigest],
    ) -> Result<StoredAsset> {
        let path = path.as_ref();
        let declared = normalize_declared(declared)?;
        let (actual, size) =
            hash_path(path, declared.iter().any(|d| d.algorithm == "blake3")).await?;
        check_declared(&declared, &actual)?;
        let (content, _) = self.store().import_file(path).await?;
        if content.0.value != actual["sha256"] {
            return Err(Error::FileChangedDuringImport {
                path: path.display().to_string(),
                hashed: actual["sha256"].clone(),
                stored: content.0.value.clone(),
            });
        }
        self.commit_asset(content, size, media_type, declared).await
    }

    async fn commit_asset(
        &self,
        content: ContentId,
        size: u64,
        media_type: &str,
        declared: Vec<ExternalDigest>,
    ) -> Result<StoredAsset> {
        let mut manifest = ArtifactManifest::new(content.clone(), ContentKind::Blob);
        manifest.media_type = Some(media_type.to_owned());
        for digest in &declared {
            manifest.annotations.insert(
                format!("{DIGEST_ANNOTATION_PREFIX}{}", digest.algorithm),
                digest.value.clone(),
            );
        }
        let artifact = self.store().put_artifact(&manifest).await?;
        Ok(StoredAsset {
            artifact,
            content,
            size,
            media_type: media_type.to_owned(),
            declared,
        })
    }

    /// Load an asset's identity record from its manifest (the manifest is re-hashed on load).
    pub async fn load_asset(&self, artifact: &ArtifactId) -> Result<StoredAsset> {
        let manifest = self.store().load_artifact(artifact).await?;
        if manifest.kind != ContentKind::Blob || manifest.annotations.contains_key(KIND_ANNOTATION)
        {
            return Err(Error::NotAnAsset(artifact.clone()));
        }
        let size = tokio::fs::metadata(self.store().content_path(&manifest.content)?)
            .await
            .map(|m| m.len())
            .map_err(|_| artifactum_store::Error::MissingContent(manifest.content.to_string()))?;
        Ok(StoredAsset {
            artifact: artifact.clone(),
            content: manifest.content.clone(),
            size,
            media_type: manifest.media_type.clone().unwrap_or_default(),
            declared: declared_from_manifest(&manifest)?,
        })
    }

    /// Read an asset's exact bytes.
    pub async fn read_asset(&self, artifact: &ArtifactId) -> Result<Vec<u8>> {
        let manifest = self.store().load_artifact(artifact).await?;
        if manifest.kind != ContentKind::Blob {
            return Err(Error::NotAnAsset(artifact.clone()));
        }
        Ok(self.store().read_content(&manifest.content).await?)
    }

    // ----------------------------------------------------------------------------------
    // Runs
    // ----------------------------------------------------------------------------------

    /// Record one completed activity: derive its canonical action identity, seal a receipt
    /// over its bindings and payload, bundle everything into a run collection, and index the
    /// reverse edges in the metadata plane.
    pub async fn record_run(&self, description: RunDescription) -> Result<StoredRun> {
        let inputs = sorted_unique_bindings(&description.inputs)?;
        let outputs = sorted_unique_bindings(&description.outputs)?;

        let mut spec = ActionSpec::command(
            description.schema.name.clone(),
            description
                .command
                .as_ref()
                .map(|c| c.argv.clone())
                .unwrap_or_default(),
        );
        for binding in &inputs {
            self.store().load_artifact(&binding.artifact).await?;
            spec.inputs
                .insert(binding.role.clone(), binding.artifact.clone());
        }
        for binding in &outputs {
            let manifest = self.store().load_artifact(&binding.artifact).await?;
            spec.outputs.insert(
                binding.role.clone(),
                OutputSpec {
                    kind: manifest.kind,
                    media_type: manifest.media_type,
                    schema: manifest.schema,
                },
            );
        }
        self.store()
            .load_artifact(&description.producer.executable)
            .await?;
        spec.code.insert(
            "executable".to_owned(),
            description.producer.executable.clone(),
        );
        if let Some(command) = &description.command {
            spec.environment.variables = command.declared_environment.clone();
        }
        spec.environment.container = description
            .environment
            .container
            .as_ref()
            .map(ToString::to_string);
        spec.network = description.network;
        spec.sandbox = description.sandbox;
        spec.platform = Some(description.environment.platform.clone());
        // The activity ran outside Artifactum; it must never be replayed from cache.
        spec.cache = CachePolicy::Effect;
        let action = spec.key()?;

        let mut receipt = ReceiptEnvelope {
            schema: description.schema,
            receipt_id: ReceiptId(action.0.clone()),
            producer: description.producer,
            activity: ActivityIdentity {
                action: action.clone(),
                attempt: None,
                parent: description.parent,
            },
            environment: description.environment,
            inputs: inputs
                .iter()
                .map(|b| ArtifactBinding {
                    role: b.role.clone(),
                    artifact: b.artifact.clone(),
                    member: None,
                })
                .collect(),
            outputs: outputs
                .iter()
                .map(|b| ArtifactBinding {
                    role: b.role.clone(),
                    artifact: b.artifact.clone(),
                    member: None,
                })
                .collect(),
            command: description.command,
            diagnostics: description.diagnostics,
            started_at: description.started_at,
            finished_at: description.finished_at,
            payload: description.payload,
        };
        receipt.refresh_id()?;
        receipt.validate()?;
        let receipt_id = receipt.receipt_id.clone();

        let receipt_bytes = serde_json::to_vec(&receipt)?;
        let receipt_content = self.store().put_bytes(&receipt_bytes).await?;
        let mut receipt_manifest = ArtifactManifest::new(receipt_content, ContentKind::Blob);
        receipt_manifest.media_type = Some(RECEIPT_MEDIA_TYPE.to_owned());
        receipt_manifest.format_version = Some(RECEIPT_ENVELOPE_SCHEMA.to_owned());
        receipt_manifest
            .annotations
            .insert(KIND_ANNOTATION.to_owned(), "receipt".to_owned());
        let receipt_artifact = self.store().put_artifact(&receipt_manifest).await?;

        let executable = receipt.producer.executable.clone();
        let mut entries = vec![
            CollectionEntry {
                key: RECEIPT_KEY.to_owned(),
                artifact: receipt_artifact.clone(),
                label: None,
            },
            CollectionEntry {
                key: EXECUTABLE_KEY.to_owned(),
                artifact: executable.clone(),
                label: None,
            },
        ];
        entries.extend(inputs.iter().map(|b| CollectionEntry {
            key: format!("{INPUT_PREFIX}{}", b.role),
            artifact: b.artifact.clone(),
            label: None,
        }));
        entries.extend(outputs.iter().map(|b| CollectionEntry {
            key: format!("{OUTPUT_PREFIX}{}", b.role),
            artifact: b.artifact.clone(),
            label: None,
        }));
        let run = self
            .put_collection(
                CollectionManifest::new(entries),
                EVIDENCE_RUN_SCHEMA,
                "run",
                BTreeMap::new(),
            )
            .await?;

        // History plane: the run is an intrinsic realization of its action, so
        // `Engine::lineage`, `why`, determinism audits and GC roots see it.
        let realized = self.engine.realize_intrinsic(
            spec,
            outputs
                .iter()
                .map(|b| (b.role.clone(), b.artifact.clone()))
                .collect(),
        )?;

        let run_statement = |role: &str| {
            serde_json::json!({
                "run": run.to_string(),
                "receipt": receipt_artifact.to_string(),
                "receipt_id": receipt_id.0.to_string(),
                "role": role,
            })
        };
        for binding in &outputs {
            self.attest_once(
                &binding.artifact,
                PRODUCED_BY_PREDICATE,
                run_statement(&binding.role),
            )?;
        }
        for binding in &inputs {
            self.attest_once(
                &binding.artifact,
                CONSUMED_BY_PREDICATE,
                run_statement(&binding.role),
            )?;
        }
        self.attest_once(
            &executable,
            EXECUTED_BY_PREDICATE,
            run_statement("executable"),
        )?;

        Ok(StoredRun {
            run,
            receipt: receipt_artifact,
            receipt_id,
            action,
            executable,
            inputs,
            outputs,
            realization_reused: realized.cache_hit,
        })
    }

    /// Load a run and re-validate its receipt: the receipt id recomputes from the stored
    /// bytes, and the collection's bindings equal the receipt's.
    pub async fn load_run(&self, run: &ArtifactId) -> Result<LoadedRun> {
        let manifest = self.store().load_artifact(run).await?;
        if manifest.kind != ContentKind::Collection
            || manifest
                .annotations
                .get(KIND_ANNOTATION)
                .map(String::as_str)
                != Some("run")
        {
            return Err(Error::NotARun(
                run.clone(),
                "manifest is not an evidence run collection".into(),
            ));
        }
        let collection = self.store().read_collection(&manifest.content).await?;
        let mut receipt_artifact = None;
        let mut executable = None;
        let mut inputs = Vec::new();
        let mut outputs = Vec::new();
        for entry in collection.entries {
            if entry.key == RECEIPT_KEY {
                receipt_artifact = Some(entry.artifact);
            } else if entry.key == EXECUTABLE_KEY {
                executable = Some(entry.artifact);
            } else if let Some(role) = entry.key.strip_prefix(INPUT_PREFIX) {
                inputs.push(RunBinding {
                    role: role.to_owned(),
                    artifact: entry.artifact,
                });
            } else if let Some(role) = entry.key.strip_prefix(OUTPUT_PREFIX) {
                outputs.push(RunBinding {
                    role: role.to_owned(),
                    artifact: entry.artifact,
                });
            } else {
                return Err(Error::NotARun(
                    run.clone(),
                    format!("unexpected member `{}`", entry.key),
                ));
            }
        }
        let receipt_artifact = receipt_artifact
            .ok_or_else(|| Error::NotARun(run.clone(), "missing receipt member".into()))?;
        let executable = executable
            .ok_or_else(|| Error::NotARun(run.clone(), "missing code/executable member".into()))?;
        let receipt_manifest = self.store().load_artifact(&receipt_artifact).await?;
        if receipt_manifest.media_type.as_deref() != Some(RECEIPT_MEDIA_TYPE) {
            return Err(Error::NotARun(
                run.clone(),
                "receipt member is not a receipt blob".into(),
            ));
        }
        let bytes = self.store().read_content(&receipt_manifest.content).await?;
        let receipt: ReceiptEnvelope<serde_json::Value> = serde_json::from_slice(&bytes)?;
        receipt.validate()?;
        let receipt_inputs: Vec<RunBinding> = receipt
            .inputs
            .iter()
            .map(|b| RunBinding {
                role: b.role.clone(),
                artifact: b.artifact.clone(),
            })
            .collect();
        let receipt_outputs: Vec<RunBinding> = receipt
            .outputs
            .iter()
            .map(|b| RunBinding {
                role: b.role.clone(),
                artifact: b.artifact.clone(),
            })
            .collect();
        if receipt_inputs != inputs
            || receipt_outputs != outputs
            || receipt.producer.executable != executable
        {
            return Err(Error::NotARun(
                run.clone(),
                "collection bindings disagree with the receipt".into(),
            ));
        }
        Ok(LoadedRun {
            run: run.clone(),
            receipt_artifact,
            receipt,
            executable,
            inputs,
            outputs,
        })
    }

    // ----------------------------------------------------------------------------------
    // Claims
    // ----------------------------------------------------------------------------------

    /// Seal a claim over stored runs and assets. Every cited run is re-validated and every
    /// cited asset's digests are snapshotted into the claim record before sealing.
    pub async fn record_claim(&self, description: ClaimDescription) -> Result<StoredClaim> {
        if description.subject.trim().is_empty() {
            return Err(Error::InvalidRole("<subject>".into()));
        }
        if description.state.trim().is_empty() {
            return Err(Error::InvalidRole("<state>".into()));
        }
        description.schema.validate()?;
        let run_bindings = sorted_unique_bindings(&description.runs)?;
        let asset_bindings = sorted_unique_bindings(&description.assets)?;

        let mut runs = Vec::new();
        for binding in &run_bindings {
            let loaded = self.load_run(&binding.artifact).await?;
            let mut inputs = Vec::new();
            for input in &loaded.inputs {
                inputs.push(self.claim_asset(&input.role, &input.artifact).await?);
            }
            let mut outputs = Vec::new();
            for output in &loaded.outputs {
                outputs.push(self.claim_asset(&output.role, &output.artifact).await?);
            }
            let executable = self.claim_asset("executable", &loaded.executable).await?;
            runs.push(ClaimRun {
                role: binding.role.clone(),
                run: loaded.run,
                receipt: loaded.receipt_artifact,
                receipt_id: loaded.receipt.receipt_id.clone(),
                action: loaded.receipt.activity.action.clone(),
                executable,
                inputs,
                outputs,
            });
        }
        let mut assets = Vec::new();
        for binding in &asset_bindings {
            assets.push(self.claim_asset(&binding.role, &binding.artifact).await?);
        }

        let record = ClaimRecord {
            format: EVIDENCE_CLAIM_SCHEMA.to_owned(),
            schema: description.schema,
            subject: description.subject,
            state: description.state,
            runs,
            assets,
            payload: description.payload,
        };
        let record_bytes = serde_json::to_vec(&record)?;
        let record_content = self.store().put_bytes(&record_bytes).await?;
        let mut record_manifest = ArtifactManifest::new(record_content, ContentKind::Blob);
        record_manifest.media_type = Some(CLAIM_RECORD_MEDIA_TYPE.to_owned());
        record_manifest.format_version = Some(EVIDENCE_CLAIM_SCHEMA.to_owned());
        record_manifest
            .annotations
            .insert(KIND_ANNOTATION.to_owned(), "claim-record".to_owned());
        let record_artifact = self.store().put_artifact(&record_manifest).await?;

        let mut entries = vec![CollectionEntry {
            key: RECORD_KEY.to_owned(),
            artifact: record_artifact.clone(),
            label: None,
        }];
        entries.extend(run_bindings.iter().map(|b| CollectionEntry {
            key: format!("{RUN_PREFIX}{}", b.role),
            artifact: b.artifact.clone(),
            label: None,
        }));
        entries.extend(asset_bindings.iter().map(|b| CollectionEntry {
            key: format!("{ASSET_PREFIX}{}", b.role),
            artifact: b.artifact.clone(),
            label: None,
        }));
        let claim = self
            .put_collection(
                CollectionManifest::new(entries),
                EVIDENCE_CLAIM_SCHEMA,
                "claim",
                BTreeMap::from([
                    (SUBJECT_ANNOTATION.to_owned(), record.subject.clone()),
                    (STATE_ANNOTATION.to_owned(), record.state.clone()),
                ]),
            )
            .await?;

        // History plane: the claim roots itself for GC and indexes the reverse edges.
        self.attest_once(
            &claim,
            CLAIM_PREDICATE,
            serde_json::json!({
                "subject": record.subject,
                "state": record.state,
                "schema": record.schema.name,
                "record": record_artifact.to_string(),
            }),
        )?;
        let cited = |role: &str| {
            serde_json::json!({
                "claim": claim.to_string(),
                "subject": record.subject,
                "state": record.state,
                "role": role,
            })
        };
        for binding in &run_bindings {
            self.attest_once(
                &binding.artifact,
                CITED_BY_PREDICATE,
                cited(&format!("{RUN_PREFIX}{}", binding.role)),
            )?;
        }
        for binding in &asset_bindings {
            self.attest_once(
                &binding.artifact,
                CITED_BY_PREDICATE,
                cited(&format!("{ASSET_PREFIX}{}", binding.role)),
            )?;
        }

        Ok(StoredClaim {
            claim,
            record_artifact,
            record,
        })
    }

    async fn claim_asset(&self, role: &str, artifact: &ArtifactId) -> Result<ClaimAsset> {
        let asset = self.load_asset(artifact).await?;
        Ok(ClaimAsset {
            role: role.to_owned(),
            artifact: asset.artifact,
            content: asset.content,
            size: asset.size,
            media_type: asset.media_type,
            declared: asset.declared,
        })
    }

    /// Load a sealed claim's record from the store.
    pub async fn load_claim(&self, claim: &ArtifactId) -> Result<StoredClaim> {
        let (record_artifact, record, _) = self.load_claim_parts(claim).await?;
        Ok(StoredClaim {
            claim: claim.clone(),
            record_artifact,
            record,
        })
    }

    async fn load_claim_parts(
        &self,
        claim: &ArtifactId,
    ) -> Result<(ArtifactId, ClaimRecord, CollectionManifest)> {
        let manifest = self.store().load_artifact(claim).await?;
        if manifest.kind != ContentKind::Collection
            || manifest
                .annotations
                .get(KIND_ANNOTATION)
                .map(String::as_str)
                != Some("claim")
        {
            return Err(Error::NotAClaim(
                claim.clone(),
                "manifest is not an evidence claim collection".into(),
            ));
        }
        let collection = self.store().read_collection(&manifest.content).await?;
        let record_artifact = collection
            .entries
            .iter()
            .find(|e| e.key == RECORD_KEY)
            .map(|e| e.artifact.clone())
            .ok_or_else(|| Error::NotAClaim(claim.clone(), "missing record member".into()))?;
        let record_manifest = self.store().load_artifact(&record_artifact).await?;
        if record_manifest.media_type.as_deref() != Some(CLAIM_RECORD_MEDIA_TYPE) {
            return Err(Error::NotAClaim(
                claim.clone(),
                "record member is not a claim record".into(),
            ));
        }
        let bytes = self.store().read_content(&record_manifest.content).await?;
        let record: ClaimRecord = serde_json::from_slice(&bytes)?;
        if record.format != EVIDENCE_CLAIM_SCHEMA {
            return Err(Error::NotAClaim(
                claim.clone(),
                format!("unknown record format `{}`", record.format),
            ));
        }
        Ok((record_artifact, record, collection))
    }

    /// Name a sealed claim with an immutable ref so it is reachable by name and rooted for
    /// graph GC independently of the metadata plane.
    pub async fn tag_claim(&self, claim: &ArtifactId, name: &str) -> Result<()> {
        self.load_claim_parts(claim).await?;
        Ok(self.store().set_ref(name, claim, true).await?)
    }

    /// Re-verify a sealed claim: the claim and record artifacts re-hash, every cited run's
    /// receipt re-validates and matches the record, every cited asset's bytes re-hash to the
    /// recorded SHA-256 content id and to every recorded declared digest, and every asset
    /// manifest still carries exactly the declared digests the record snapshotted.
    ///
    /// I/O failures on individual members are reported as failures, never as an `Err`, so a
    /// partially corrupted store still yields a complete report.
    pub async fn verify_claim(&self, claim: &ArtifactId) -> Result<ClaimVerification> {
        let (record_artifact, record, collection) = self.load_claim_parts(claim).await?;
        let mut failures = Vec::new();

        // The collection's members must be exactly what the record snapshots.
        let mut expected_members: BTreeMap<String, ArtifactId> = BTreeMap::new();
        expected_members.insert(RECORD_KEY.to_owned(), record_artifact.clone());
        for run in &record.runs {
            expected_members.insert(format!("{RUN_PREFIX}{}", run.role), run.run.clone());
        }
        for asset in &record.assets {
            expected_members.insert(
                format!("{ASSET_PREFIX}{}", asset.role),
                asset.artifact.clone(),
            );
        }
        let actual_members: BTreeMap<String, ArtifactId> = collection
            .entries
            .iter()
            .map(|e| (e.key.clone(), e.artifact.clone()))
            .collect();
        let record_ok = expected_members == actual_members;
        if !record_ok {
            failures.push("claim collection members disagree with the claim record".into());
        }

        let mut runs = Vec::new();
        for cited in &record.runs {
            let mut check = RunCheck {
                role: cited.role.clone(),
                run: cited.run.clone(),
                receipt: cited.receipt.clone(),
                receipt_id: cited.receipt_id.clone(),
                receipt_ok: false,
                bindings_ok: false,
                ok: false,
            };
            match self.load_run(&cited.run).await {
                Ok(loaded) => {
                    check.receipt_ok = loaded.receipt_artifact == cited.receipt
                        && loaded.receipt.receipt_id == cited.receipt_id
                        && loaded.receipt.activity.action == cited.action;
                    let inputs: Vec<RunBinding> = cited
                        .inputs
                        .iter()
                        .map(|a| RunBinding {
                            role: a.role.clone(),
                            artifact: a.artifact.clone(),
                        })
                        .collect();
                    let outputs: Vec<RunBinding> = cited
                        .outputs
                        .iter()
                        .map(|a| RunBinding {
                            role: a.role.clone(),
                            artifact: a.artifact.clone(),
                        })
                        .collect();
                    check.bindings_ok = loaded.inputs == inputs
                        && loaded.outputs == outputs
                        && loaded.executable == cited.executable.artifact;
                    if !check.receipt_ok {
                        failures.push(format!(
                            "run `{}` ({}) receipt disagrees with the claim record",
                            cited.role, cited.run
                        ));
                    }
                    if !check.bindings_ok {
                        failures.push(format!(
                            "run `{}` ({}) bindings disagree with the claim record",
                            cited.role, cited.run
                        ));
                    }
                }
                Err(error) => failures.push(format!(
                    "run `{}` ({}) failed to load: {error}",
                    cited.role, cited.run
                )),
            }
            check.ok = check.receipt_ok && check.bindings_ok;
            runs.push(check);
        }

        let mut assets = Vec::new();
        for (path, cited) in cited_assets(&record) {
            let check = self.check_asset(&path, &cited, &mut failures).await;
            assets.push(check);
        }

        let ok = record_ok && runs.iter().all(|r| r.ok) && assets.iter().all(|a| a.ok);
        Ok(ClaimVerification {
            claim: claim.clone(),
            subject: record.subject,
            state: record.state,
            record_ok,
            runs,
            assets,
            failures,
            ok,
        })
    }

    async fn check_asset(
        &self,
        path: &str,
        cited: &ClaimAsset,
        failures: &mut Vec<String>,
    ) -> AssetCheck {
        let mut check = AssetCheck {
            path: path.to_owned(),
            artifact: cited.artifact.clone(),
            content: cited.content.clone(),
            size: cited.size,
            content_ok: false,
            manifest_ok: false,
            declared: cited
                .declared
                .iter()
                .map(|d| DigestCheck {
                    algorithm: d.algorithm.clone(),
                    expected: d.value.clone(),
                    actual: None,
                    ok: false,
                })
                .collect(),
            ok: false,
        };
        match self.store().load_artifact(&cited.artifact).await {
            Ok(manifest) => {
                let declared = declared_from_manifest(&manifest).unwrap_or_default();
                check.manifest_ok = manifest.kind == ContentKind::Blob
                    && manifest.content == cited.content
                    && manifest.media_type.as_deref() == Some(cited.media_type.as_str())
                    && declared == cited.declared;
                if !check.manifest_ok {
                    failures.push(format!(
                        "asset `{path}` ({}) manifest disagrees with the claim record",
                        cited.artifact
                    ));
                }
            }
            Err(error) => failures.push(format!(
                "asset `{path}` ({}) manifest failed to load: {error}",
                cited.artifact
            )),
        }
        match self.store().read_content(&cited.content).await {
            Ok(bytes) => {
                let mut hasher = MultiHasher::new(true);
                hasher.update(&bytes);
                let (actual, size) = hasher.finish();
                check.content_ok = actual["sha256"] == cited.content.0.value && size == cited.size;
                if !check.content_ok {
                    failures.push(format!(
                        "asset `{path}` ({}) bytes hash to sha256:{} ({size} bytes), not the \
                         recorded {} ({} bytes)",
                        cited.artifact, actual["sha256"], cited.content, cited.size
                    ));
                }
                for digest in &mut check.declared {
                    match actual.get(&digest.algorithm) {
                        Some(observed) => {
                            digest.actual = Some(observed.clone());
                            digest.ok = observed == &digest.expected;
                        }
                        None => digest.ok = false,
                    }
                    if !digest.ok {
                        failures.push(format!(
                            "asset `{path}` ({}) declared {} digest {} does not re-hash \
                             (observed {})",
                            cited.artifact,
                            digest.algorithm,
                            digest.expected,
                            digest.actual.as_deref().unwrap_or("<unsupported>")
                        ));
                    }
                }
            }
            Err(error) => failures.push(format!(
                "asset `{path}` ({}) bytes failed to load: {error}",
                cited.artifact
            )),
        }
        check.ok = check.content_ok && check.manifest_ok && check.declared.iter().all(|d| d.ok);
        check
    }

    // ----------------------------------------------------------------------------------
    // Lineage
    // ----------------------------------------------------------------------------------

    /// Upward lineage of one artifact from the metadata plane's attestations: the runs that
    /// produced or consumed it and the claims that cite it (directly, or through one of
    /// those runs).
    pub async fn explain(&self, artifact: &ArtifactId) -> Result<EvidenceExplanation> {
        let kind = match self.store().load_artifact(artifact).await {
            Ok(manifest) => match (
                manifest.kind,
                manifest
                    .annotations
                    .get(KIND_ANNOTATION)
                    .map(String::as_str),
            ) {
                (ContentKind::Blob, None) => EvidenceKind::Asset,
                (ContentKind::Blob, Some("receipt")) => EvidenceKind::Receipt,
                (ContentKind::Blob, Some("claim-record")) => EvidenceKind::ClaimRecord,
                (ContentKind::Collection, Some("run")) => EvidenceKind::Run,
                (ContentKind::Collection, Some("claim")) => EvidenceKind::Claim,
                _ => EvidenceKind::Unknown,
            },
            Err(artifactum_store::Error::MissingArtifact(_)) => EvidenceKind::Unknown,
            Err(error) => return Err(error.into()),
        };
        let mut produced_by = Vec::new();
        let mut consumed_by = Vec::new();
        let mut executed_by = Vec::new();
        let mut cited_by = Vec::new();
        for attestation in self.metadata().attestations(artifact)? {
            let statement = &attestation.statement;
            match attestation.predicate_type.as_str() {
                PRODUCED_BY_PREDICATE | CONSUMED_BY_PREDICATE | EXECUTED_BY_PREDICATE => {
                    let citation = RunCitation {
                        run: field(statement, "run")?.parse()?,
                        receipt: field(statement, "receipt")?.parse()?,
                        receipt_id: ReceiptId(field(statement, "receipt_id")?.parse::<Digest>()?),
                        role: field(statement, "role")?.to_owned(),
                    };
                    match attestation.predicate_type.as_str() {
                        PRODUCED_BY_PREDICATE => produced_by.push(citation),
                        CONSUMED_BY_PREDICATE => consumed_by.push(citation),
                        _ => executed_by.push(citation),
                    }
                }
                CITED_BY_PREDICATE => cited_by.push(ClaimCitation {
                    claim: field(statement, "claim")?.parse()?,
                    subject: field(statement, "subject")?.to_owned(),
                    state: field(statement, "state")?.to_owned(),
                    role: field(statement, "role")?.to_owned(),
                    via_run: None,
                }),
                _ => {}
            }
        }
        // One hop through runs: a claim citing a run cites the run's assets.
        let mut seen_runs = BTreeSet::new();
        for run in produced_by
            .iter()
            .chain(consumed_by.iter())
            .chain(executed_by.iter())
        {
            if !seen_runs.insert(run.run.to_string()) {
                continue;
            }
            for attestation in self.metadata().attestations(&run.run)? {
                if attestation.predicate_type != CITED_BY_PREDICATE {
                    continue;
                }
                let statement = &attestation.statement;
                cited_by.push(ClaimCitation {
                    claim: field(statement, "claim")?.parse()?,
                    subject: field(statement, "subject")?.to_owned(),
                    state: field(statement, "state")?.to_owned(),
                    role: format!("{}/{}", field(statement, "role")?, run.role),
                    via_run: Some(run.run.clone()),
                });
            }
        }
        produced_by.sort_by(|a, b| (&a.run, &a.role).cmp(&(&b.run, &b.role)));
        consumed_by.sort_by(|a, b| (&a.run, &a.role).cmp(&(&b.run, &b.role)));
        executed_by.sort_by(|a, b| (&a.run, &a.role).cmp(&(&b.run, &b.role)));
        cited_by.sort_by(|a, b| (&a.claim, &a.role).cmp(&(&b.claim, &b.role)));
        cited_by.dedup();
        Ok(EvidenceExplanation {
            artifact: artifact.clone(),
            kind,
            produced_by,
            consumed_by,
            executed_by,
            cited_by,
        })
    }

    // ----------------------------------------------------------------------------------
    // Internals
    // ----------------------------------------------------------------------------------

    async fn put_collection(
        &self,
        collection: CollectionManifest,
        format_version: &str,
        kind: &str,
        mut annotations: BTreeMap<String, String>,
    ) -> Result<ArtifactId> {
        let bytes = artifactum_core::canonical_json(&collection)?;
        let content = self.store().put_bytes(&bytes).await?;
        let mut manifest = ArtifactManifest::new(content, ContentKind::Collection);
        manifest.format_version = Some(format_version.to_owned());
        annotations.insert(KIND_ANNOTATION.to_owned(), kind.to_owned());
        manifest.annotations = annotations;
        Ok(self.store().put_artifact(&manifest).await?)
    }

    /// Record an attestation unless an identical (subject, predicate, statement) one exists,
    /// so re-recording the same run or claim stays idempotent in the history plane.
    fn attest_once(
        &self,
        subject: &ArtifactId,
        predicate: &str,
        statement: serde_json::Value,
    ) -> Result<()> {
        let existing = self.metadata().attestations(subject)?;
        if existing
            .iter()
            .any(|a| a.predicate_type == predicate && a.statement == statement)
        {
            return Ok(());
        }
        self.engine.attest(
            subject.clone(),
            predicate,
            statement,
            Some(ATTESTATION_ISSUER.to_owned()),
        )?;
        Ok(())
    }
}

fn field<'a>(statement: &'a serde_json::Value, name: &str) -> Result<&'a str> {
    statement
        .get(name)
        .and_then(serde_json::Value::as_str)
        .ok_or_else(|| {
            Error::Json(serde::de::Error::custom(format!(
                "attestation statement lacks string field `{name}`"
            )))
        })
}

fn sorted_unique_bindings(bindings: &[RunBinding]) -> Result<Vec<RunBinding>> {
    let mut out = bindings.to_vec();
    out.sort_by(|a, b| a.role.cmp(&b.role));
    let mut seen = BTreeSet::new();
    for binding in &out {
        if binding.role.trim().is_empty()
            || binding.role.contains('/')
            || !seen.insert(binding.role.clone())
        {
            return Err(Error::InvalidRole(binding.role.clone()));
        }
    }
    Ok(out)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn external_digest_parses_and_computes() {
        let bytes = b"raw evidence";
        let sha = ExternalDigest::compute("sha256", bytes).unwrap();
        let b3 = ExternalDigest::compute("blake3", bytes).unwrap();
        assert_eq!(
            ExternalDigest::parse(&sha.as_qualified()).unwrap(),
            sha,
            "sha256 round-trips"
        );
        assert_eq!(ExternalDigest::parse(&b3.as_qualified()).unwrap(), b3);
        assert_eq!(b3.value, blake3::hash(bytes).to_hex().to_string());
        assert!(matches!(
            ExternalDigest::parse("md5:00"),
            Err(Error::UnsupportedDigestAlgorithm(_))
        ));
        assert!(matches!(
            ExternalDigest::parse("blake3:ABC"),
            Err(Error::InvalidDigest(_))
        ));
        assert!(matches!(
            ExternalDigest::parse("nocolon"),
            Err(Error::InvalidDigest(_))
        ));
    }

    #[test]
    fn declared_digests_dedupe_and_refuse_conflicts() {
        let a = ExternalDigest::blake3(&"a".repeat(64)).unwrap();
        let b = ExternalDigest::blake3(&"b".repeat(64)).unwrap();
        assert_eq!(
            normalize_declared(&[a.clone(), a.clone()]).unwrap(),
            vec![a.clone()]
        );
        assert!(matches!(
            normalize_declared(&[a, b]),
            Err(Error::ConflictingDeclaredDigests { .. })
        ));
    }

    #[test]
    fn roles_must_be_unique_and_slash_free() {
        let id = ArtifactId(Digest::sha256("c".repeat(64)).unwrap());
        let binding = |role: &str| RunBinding {
            role: role.into(),
            artifact: id.clone(),
        };
        assert!(sorted_unique_bindings(&[binding("b"), binding("a")]).is_ok());
        assert!(matches!(
            sorted_unique_bindings(&[binding("a"), binding("a")]),
            Err(Error::InvalidRole(_))
        ));
        assert!(matches!(
            sorted_unique_bindings(&[binding("a/b")]),
            Err(Error::InvalidRole(_))
        ));
        assert!(matches!(
            sorted_unique_bindings(&[binding(" ")]),
            Err(Error::InvalidRole(_))
        ));
    }
}
