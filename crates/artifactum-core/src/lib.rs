//! Stable, I/O-free domain model for Artifactum.

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use sha2::{Digest as _, Sha256};
use std::{
    collections::BTreeMap,
    fmt,
    path::{Component, Path, PathBuf},
    str::FromStr,
};
use thiserror::Error;
use uuid::Uuid;

pub type Metadata = BTreeMap<String, serde_json::Value>;

#[derive(Debug, Error)]
pub enum Error {
    #[error("invalid digest `{0}`")]
    InvalidDigest(String),
    #[error("invalid artifact path `{0}`")]
    InvalidArtifactPath(String),
    #[error("invalid reference `{0}`; expected <scheme>:<locator>")]
    InvalidReference(String),
    #[error("canonical serialization failed: {0}")]
    Canonical(#[from] serde_json::Error),
}
pub type Result<T, E = Error> = std::result::Result<T, E>;

/// Public Artifactum content identity. SHA-256 remains the interoperable default.
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct Digest {
    pub algorithm: String,
    pub value: String,
}
impl Digest {
    pub fn sha256(value: impl Into<String>) -> Result<Self> {
        let value = value.into().to_ascii_lowercase();
        if value.len() != 64 || !value.bytes().all(|b| b.is_ascii_hexdigit()) {
            return Err(Error::InvalidDigest(format!("sha256:{value}")));
        }
        Ok(Self {
            algorithm: "sha256".into(),
            value,
        })
    }
    #[must_use]
    pub fn as_qualified(&self) -> String {
        format!("{}:{}", self.algorithm, self.value)
    }
}
impl fmt::Display for Digest {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}:{}", self.algorithm, self.value)
    }
}
impl FromStr for Digest {
    type Err = Error;
    fn from_str(value: &str) -> Result<Self> {
        let (algorithm, digest) = value
            .split_once(':')
            .ok_or_else(|| Error::InvalidDigest(value.into()))?;
        match algorithm {
            "sha256" => Self::sha256(digest),
            _ => Err(Error::InvalidDigest(value.into())),
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub struct ContentId(pub Digest);
impl fmt::Display for ContentId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.fmt(f)
    }
}
impl FromStr for ContentId {
    type Err = Error;
    fn from_str(v: &str) -> Result<Self> {
        Ok(Self(v.parse()?))
    }
}

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub struct ArtifactId(pub Digest);
impl fmt::Display for ArtifactId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.fmt(f)
    }
}
impl FromStr for ArtifactId {
    type Err = Error;
    fn from_str(v: &str) -> Result<Self> {
        Ok(Self(v.parse()?))
    }
}

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub struct ActionKey(pub Digest);
impl fmt::Display for ActionKey {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.fmt(f)
    }
}
impl FromStr for ActionKey {
    type Err = Error;
    fn from_str(v: &str) -> Result<Self> {
        Ok(Self(v.parse()?))
    }
}

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub struct ArtifactPath(String);
impl ArtifactPath {
    pub fn new(raw: impl AsRef<str>) -> Result<Self> {
        let raw = raw.as_ref().replace('\\', "/");
        let path = Path::new(&raw);
        let windows_drive_prefix = raw.as_bytes().get(1) == Some(&b':')
            && raw.as_bytes().first().is_some_and(u8::is_ascii_alphabetic);
        if raw.is_empty()
            || windows_drive_prefix
            || path.is_absolute()
            || path.components().any(|c| {
                matches!(
                    c,
                    Component::ParentDir | Component::RootDir | Component::Prefix(_)
                )
            })
        {
            return Err(Error::InvalidArtifactPath(raw));
        }
        let normalized = path
            .components()
            .filter_map(|c| match c {
                Component::Normal(v) => Some(v.to_string_lossy().into_owned()),
                Component::CurDir => None,
                _ => None,
            })
            .collect::<Vec<_>>()
            .join("/");
        if normalized.is_empty() {
            return Err(Error::InvalidArtifactPath(raw));
        }
        Ok(Self(normalized))
    }
    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.0
    }
    #[must_use]
    pub fn to_path_buf(&self) -> PathBuf {
        self.0.split('/').collect()
    }
}
impl fmt::Display for ArtifactPath {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}
impl FromStr for ArtifactPath {
    type Err = Error;
    fn from_str(v: &str) -> Result<Self> {
        Self::new(v)
    }
}

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct ArtifactRef {
    scheme: String,
    locator: String,
}
impl ArtifactRef {
    pub fn new(scheme: impl Into<String>, locator: impl Into<String>) -> Result<Self> {
        let scheme = scheme.into();
        let locator = locator.into();
        if scheme.is_empty()
            || locator.is_empty()
            || !scheme
                .bytes()
                .all(|b| b.is_ascii_alphanumeric() || matches!(b, b'+' | b'-' | b'.' | b'_'))
        {
            return Err(Error::InvalidReference(format!("{scheme}:{locator}")));
        }
        Ok(Self {
            scheme: scheme.to_ascii_lowercase(),
            locator,
        })
    }
    #[must_use]
    pub fn scheme(&self) -> &str {
        &self.scheme
    }
    #[must_use]
    pub fn locator(&self) -> &str {
        &self.locator
    }
    pub fn with_scheme(&self, scheme: impl Into<String>) -> Result<Self> {
        Self::new(scheme, self.locator.clone())
    }
}
impl fmt::Display for ArtifactRef {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}:{}", self.scheme, self.locator)
    }
}
impl FromStr for ArtifactRef {
    type Err = Error;
    fn from_str(v: &str) -> Result<Self> {
        let (s, l) = v
            .split_once(':')
            .ok_or_else(|| Error::InvalidReference(v.into()))?;
        Self::new(s, l)
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ContentKind {
    Blob,
    Tree,
    Collection,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum TreeEntryKind {
    Blob,
    Tree,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct TreeEntry {
    pub path: ArtifactPath,
    pub kind: TreeEntryKind,
    pub content: ContentId,
    pub size: u64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub executable: Option<bool>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct TreeManifest {
    pub version: u32,
    pub entries: Vec<TreeEntry>,
}
impl TreeManifest {
    #[must_use]
    pub fn new(mut entries: Vec<TreeEntry>) -> Self {
        entries.sort_by(|a, b| a.path.as_str().cmp(b.path.as_str()));
        Self {
            version: 1,
            entries,
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ChunkRef {
    pub content: ContentId,
    pub size: u64,
}

/// Content-defined chunk manifest for very large blobs. `logical_sha256` is the
/// digest of the reassembled byte stream; `ContentId` identifies this canonical
/// manifest, so chunk boundaries and ordering are themselves integrity checked.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ChunkManifest {
    pub version: u32,
    pub logical_size: u64,
    pub logical_sha256: Digest,
    pub min_chunk: u64,
    pub avg_chunk: u64,
    pub max_chunk: u64,
    pub chunks: Vec<ChunkRef>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct CollectionEntry {
    pub key: String,
    pub artifact: ArtifactId,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub label: Option<String>,
}
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct CollectionManifest {
    pub version: u32,
    pub entries: Vec<CollectionEntry>,
}
impl CollectionManifest {
    #[must_use]
    pub fn new(mut entries: Vec<CollectionEntry>) -> Self {
        entries.sort_by(|a, b| a.key.cmp(&b.key));
        Self {
            version: 1,
            entries,
        }
    }
}

/// Semantic interpretation of immutable content. Provenance is intentionally external.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ArtifactManifest {
    pub version: u32,
    pub content: ContentId,
    pub kind: ContentKind,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub media_type: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub schema: Option<ArtifactId>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub format_version: Option<String>,
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub annotations: BTreeMap<String, String>,
}
impl ArtifactManifest {
    #[must_use]
    pub fn new(content: ContentId, kind: ContentKind) -> Self {
        Self {
            version: 1,
            content,
            kind,
            media_type: None,
            schema: None,
            format_version: None,
            annotations: BTreeMap::new(),
        }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct SourceObservation {
    pub id: Uuid,
    pub artifact: ArtifactId,
    pub provider: String,
    pub canonical_ref: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub revision: Option<String>,
    pub observed_at: DateTime<Utc>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub etag: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub last_modified: Option<String>,
    #[serde(default)]
    pub provider_state: serde_json::Value,
    #[serde(default)]
    pub metadata: Metadata,
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum CachePolicy {
    Pure,
    #[default]
    Reproducible,
    Volatile,
    Effect,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct OutputSpec {
    pub kind: ContentKind,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub media_type: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub schema: Option<ArtifactId>,
}
impl OutputSpec {
    #[must_use]
    pub fn blob() -> Self {
        Self {
            kind: ContentKind::Blob,
            media_type: None,
            schema: None,
        }
    }
    #[must_use]
    pub fn tree() -> Self {
        Self {
            kind: ContentKind::Tree,
            media_type: None,
            schema: None,
        }
    }
    #[must_use]
    pub fn collection() -> Self {
        Self {
            kind: ContentKind::Collection,
            media_type: None,
            schema: None,
        }
    }
}

#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct ResourceSpec {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub cpus: Option<f64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub memory_bytes: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub gpus: Option<u32>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub timeout_seconds: Option<u64>,
    /// Optional executor cost rate used for deterministic run accounting.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub cost_usd_micros_per_hour: Option<u64>,
}

#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct BudgetSpec {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub max_usd_micros: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub max_wall_seconds: Option<u64>,
}

#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum NetworkPolicy {
    #[default]
    Deny,
    Allow,
    SourceOnly,
}

#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SandboxPolicy {
    None,
    #[default]
    ReadOnlyInputs,
    Bubblewrap,
    Container,
}

#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct EnvironmentSpec {
    #[serde(default)]
    pub variables: BTreeMap<String, String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub container: Option<String>,
}
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ActionSpec {
    pub version: u32,
    pub name: String,
    pub command: Vec<String>,
    #[serde(default)]
    pub inputs: BTreeMap<String, ArtifactId>,
    #[serde(default)]
    pub code: BTreeMap<String, ArtifactId>,
    #[serde(default)]
    pub parameters: serde_json::Value,
    #[serde(default)]
    pub environment: EnvironmentSpec,
    #[serde(default)]
    pub outputs: BTreeMap<String, OutputSpec>,
    #[serde(default)]
    pub resources: ResourceSpec,
    #[serde(default)]
    pub budget: BudgetSpec,
    #[serde(default)]
    pub network: NetworkPolicy,
    #[serde(default)]
    pub sandbox: SandboxPolicy,
    #[serde(default)]
    pub cache: CachePolicy,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub platform: Option<String>,
}
impl ActionSpec {
    #[must_use]
    pub fn command(name: impl Into<String>, command: Vec<String>) -> Self {
        Self {
            version: 1,
            name: name.into(),
            command,
            inputs: BTreeMap::new(),
            code: BTreeMap::new(),
            parameters: serde_json::Value::Null,
            environment: EnvironmentSpec::default(),
            outputs: BTreeMap::new(),
            resources: ResourceSpec::default(),
            budget: BudgetSpec::default(),
            network: NetworkPolicy::Deny,
            sandbox: SandboxPolicy::ReadOnlyInputs,
            cache: CachePolicy::Reproducible,
            platform: None,
        }
    }
    pub fn key(&self) -> Result<ActionKey> {
        // The action key identifies the requested computation, not how/where it
        // is scheduled. Names, retry/cache policy, budgets, and resource
        // reservations therefore do not poison cache sharing. Network/sandbox
        // policy and platform *do* affect the computation's observable world.
        #[derive(Serialize)]
        struct Identity<'a> {
            version: u32,
            command: &'a [String],
            inputs: &'a BTreeMap<String, ArtifactId>,
            code: &'a BTreeMap<String, ArtifactId>,
            parameters: &'a serde_json::Value,
            environment: &'a EnvironmentSpec,
            outputs: &'a BTreeMap<String, OutputSpec>,
            network: &'a NetworkPolicy,
            sandbox: &'a SandboxPolicy,
            platform: &'a Option<String>,
        }
        Ok(ActionKey(hash_canonical(&Identity {
            version: self.version,
            command: &self.command,
            inputs: &self.inputs,
            code: &self.code,
            parameters: &self.parameters,
            environment: &self.environment,
            outputs: &self.outputs,
            network: &self.network,
            sandbox: &self.sandbox,
            platform: &self.platform,
        })?))
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ExecutionMetrics {
    pub wall_millis: u64,
    #[serde(default)]
    pub bytes_read: u64,
    #[serde(default)]
    pub bytes_written: u64,
    #[serde(default)]
    pub estimated_cost_usd_micros: u64,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct AttemptRecord {
    pub id: Uuid,
    pub action: ActionKey,
    pub executor: String,
    pub started_at: DateTime<Utc>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub finished_at: Option<DateTime<Utc>>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub exit_code: Option<i32>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub stdout: Option<ContentId>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub stderr: Option<ContentId>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub metrics: Option<ExecutionMetrics>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Realization {
    pub id: Uuid,
    pub action: ActionKey,
    pub attempt: Uuid,
    pub created_at: DateTime<Utc>,
    pub outputs: BTreeMap<String, ArtifactId>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Attestation {
    pub id: Uuid,
    pub subject: ArtifactId,
    pub predicate_type: String,
    pub statement: serde_json::Value,
    pub created_at: DateTime<Utc>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub issuer: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub signature: Option<String>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Checkpoint {
    pub id: Uuid,
    pub action: ActionKey,
    pub name: String,
    pub artifact: ArtifactId,
    pub created_at: DateTime<Utc>,
}

pub fn canonical_json<T: Serialize>(value: &T) -> Result<Vec<u8>> {
    // All identity-bearing Artifactum types use BTreeMap and pre-sorted vectors.
    Ok(serde_json::to_vec(value)?)
}
pub fn hash_bytes(bytes: &[u8]) -> Digest {
    let mut hasher = Sha256::new();
    hasher.update(bytes);
    let out = hasher.finalize();
    Digest {
        algorithm: "sha256".into(),
        value: hex::encode(out),
    }
}
pub fn hash_canonical<T: Serialize>(value: &T) -> Result<Digest> {
    Ok(hash_bytes(&canonical_json(value)?))
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn artifact_path_rejects_escape() {
        assert!(ArtifactPath::new("../x").is_err());
        assert!(ArtifactPath::new("..\\x").is_err());
        assert!(ArtifactPath::new("/etc/passwd").is_err());
        assert!(ArtifactPath::new("C:/Windows/system32").is_err());
        assert!(ArtifactPath::new("C:\\Windows\\system32").is_err());
        assert!(ArtifactPath::new("a/b").is_ok());
    }
    #[test]
    fn action_key_is_stable() {
        let a = ActionSpec::command("x", vec!["echo".into(), "hi".into()]);
        assert_eq!(a.key().unwrap(), a.key().unwrap());
    }
    #[test]
    fn scheduling_does_not_change_action_key() {
        let a = ActionSpec::command("x", vec!["echo".into(), "hi".into()]);
        let mut b = a.clone();
        b.name = "renamed".into();
        b.resources.cpus = Some(32.0);
        b.resources.memory_bytes = Some(1 << 30);
        b.resources.timeout_seconds = Some(1);
        b.budget.max_wall_seconds = Some(1);
        b.cache = CachePolicy::Volatile;
        assert_eq!(a.key().unwrap(), b.key().unwrap());
        b.command.push("there".into());
        assert_ne!(a.key().unwrap(), b.key().unwrap());
    }
    #[test]
    fn trees_sort_entries() {
        let d = ContentId(hash_bytes(b"x"));
        let t = TreeManifest::new(vec![
            TreeEntry {
                path: "z".parse().unwrap(),
                kind: TreeEntryKind::Blob,
                content: d.clone(),
                size: 1,
                executable: None,
            },
            TreeEntry {
                path: "a".parse().unwrap(),
                kind: TreeEntryKind::Blob,
                content: d,
                size: 1,
                executable: None,
            },
        ]);
        assert_eq!(t.entries[0].path.as_str(), "a");
    }
}
