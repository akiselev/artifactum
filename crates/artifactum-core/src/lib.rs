//! Stable domain types and provider API for Artifactum.
//!
//! Providers resolve semantic references into file manifests and acquire files
//! into host-owned staging paths. The core deliberately does not expose the
//! content-addressed store: blob identity remains a host concern.

use std::{
    collections::BTreeMap,
    fmt,
    path::{Component, Path, PathBuf},
    str::FromStr,
    sync::Arc,
};

use async_trait::async_trait;
use globset::{Glob, GlobSet, GlobSetBuilder};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use thiserror::Error;
use uuid::Uuid;

pub type Metadata = BTreeMap<String, Value>;

#[derive(Debug, Error)]
pub enum Error {
    #[error("invalid artifact reference `{0}`; expected <scheme>:<locator>")]
    InvalidReference(String),
    #[error("invalid artifact path `{0}`")]
    InvalidArtifactPath(String),
    #[error("invalid digest `{0}`")]
    InvalidDigest(String),
    #[error("invalid selection glob `{pattern}`: {message}")]
    InvalidGlob { pattern: String, message: String },
    #[error("provider `{provider}` does not support operation `{operation}`")]
    Unsupported {
        provider: String,
        operation: &'static str,
    },
    #[error("provider `{provider}`: {message}")]
    Provider { provider: String, message: String },
    #[error("I/O error: {0}")]
    Io(#[from] std::io::Error),
    #[error("serialization error: {0}")]
    Serialization(#[from] serde_json::Error),
}

pub type Result<T, E = Error> = std::result::Result<T, E>;

/// An opaque provider reference such as `hf:BAAI/bge-small-en-v1.5`.
///
/// Artifactum parses only the leading scheme. The locator is owned entirely by
/// the provider so the core never accumulates provider-specific syntax.
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
                .all(|b| b.is_ascii_alphanumeric() || matches!(b, b'+' | b'-' | b'.'))
        {
            return Err(Error::InvalidReference(format!("{scheme}:{locator}")));
        }
        Ok(Self { scheme, locator })
    }

    #[must_use]
    pub fn scheme(&self) -> &str {
        &self.scheme
    }

    #[must_use]
    pub fn locator(&self) -> &str {
        &self.locator
    }
}

impl FromStr for ArtifactRef {
    type Err = Error;

    fn from_str(value: &str) -> Result<Self> {
        let (scheme, locator) = value
            .split_once(':')
            .ok_or_else(|| Error::InvalidReference(value.to_owned()))?;
        Self::new(scheme.to_ascii_lowercase(), locator)
    }
}

impl fmt::Display for ArtifactRef {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}:{}", self.scheme, self.locator)
    }
}

/// A normalized relative path inside an artifact tree.
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub struct ArtifactPath(String);

impl ArtifactPath {
    pub fn new(path: impl AsRef<str>) -> Result<Self> {
        let raw = path.as_ref().replace('\\', "/");
        let path = Path::new(&raw);
        if raw.is_empty()
            || path.is_absolute()
            || path
                .components()
                .any(|part| matches!(part, Component::ParentDir | Component::RootDir | Component::Prefix(_)))
        {
            return Err(Error::InvalidArtifactPath(raw));
        }

        let normalized = path
            .components()
            .filter_map(|component| match component {
                Component::Normal(value) => Some(value.to_string_lossy().into_owned()),
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

    fn from_str(value: &str) -> Result<Self> {
        Self::new(value)
    }
}

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct Digest {
    pub algorithm: String,
    pub value: String,
}

impl Digest {
    pub fn sha256(hex: impl Into<String>) -> Result<Self> {
        let value = hex.into().to_ascii_lowercase();
        if value.len() != 64 || !value.bytes().all(|b| b.is_ascii_hexdigit()) {
            return Err(Error::InvalidDigest(format!("sha256:{value}")));
        }
        Ok(Self {
            algorithm: "sha256".to_owned(),
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
            .ok_or_else(|| Error::InvalidDigest(value.to_owned()))?;
        match algorithm {
            "sha256" => Self::sha256(digest),
            _ => Err(Error::InvalidDigest(value.to_owned())),
        }
    }
}

#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct DigestSet(pub BTreeMap<String, String>);

impl DigestSet {
    #[must_use]
    pub fn sha256(&self) -> Option<&str> {
        self.0.get("sha256").map(String::as_str)
    }

    pub fn insert(&mut self, digest: Digest) {
        self.0.insert(digest.algorithm, digest.value);
    }
}

#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct Selection {
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub include: Vec<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub exclude: Vec<String>,
}

impl Selection {
    #[must_use]
    pub fn all() -> Self {
        Self::default()
    }

    pub fn compile(&self) -> Result<CompiledSelection> {
        fn build(patterns: &[String]) -> Result<GlobSet> {
            let mut builder = GlobSetBuilder::new();
            for pattern in patterns {
                let glob = Glob::new(pattern).map_err(|error| Error::InvalidGlob {
                    pattern: pattern.clone(),
                    message: error.to_string(),
                })?;
                builder.add(glob);
            }
            builder.build().map_err(|error| Error::InvalidGlob {
                pattern: "<set>".to_owned(),
                message: error.to_string(),
            })
        }

        Ok(CompiledSelection {
            include_all: self.include.is_empty(),
            include: build(&self.include)?,
            exclude: build(&self.exclude)?,
        })
    }
}

#[derive(Debug)]
pub struct CompiledSelection {
    include_all: bool,
    include: GlobSet,
    exclude: GlobSet,
}

impl CompiledSelection {
    #[must_use]
    pub fn matches(&self, path: &str) -> bool {
        (self.include_all || self.include.is_match(path)) && !self.exclude.is_match(path)
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ArtifactRequirement {
    pub reference: ArtifactRef,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub revision: Option<String>,
    #[serde(default)]
    pub selection: Selection,
    #[serde(default)]
    pub metadata: Metadata,
}

impl ArtifactRequirement {
    #[must_use]
    pub fn new(reference: ArtifactRef) -> Self {
        Self {
            reference,
            revision: None,
            selection: Selection::default(),
            metadata: Metadata::default(),
        }
    }
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct ProviderCapabilities {
    pub resolve: bool,
    pub acquire: bool,
    pub search: bool,
    pub list: bool,
    pub versions: bool,
    pub push: bool,
    pub auth: bool,
    pub range: bool,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ProviderDescriptor {
    pub name: String,
    pub version: String,
    pub schemes: Vec<String>,
    pub capabilities: ProviderCapabilities,
    #[serde(default)]
    pub metadata: Metadata,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ResolvedRevision {
    /// Provider's canonical immutable-ish revision identifier.
    pub id: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub requested: Option<String>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ResolvedFile {
    pub path: ArtifactPath,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub size: Option<u64>,
    #[serde(default)]
    pub digests: DigestSet,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub media_type: Option<String>,
    /// Opaque provider-owned state required to reacquire this exact file.
    #[serde(default)]
    pub source: Value,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Resolution {
    pub provider: String,
    pub canonical_ref: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub revision: Option<ResolvedRevision>,
    pub files: Vec<ResolvedFile>,
    /// Opaque provider state that is safe to persist in lockfiles.
    #[serde(default)]
    pub provider_state: Value,
    #[serde(default)]
    pub metadata: Metadata,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Acquisition {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub bytes_written: Option<u64>,
    #[serde(default)]
    pub metadata: Metadata,
}

#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct ResolveContext {
    pub offline: bool,
    #[serde(default)]
    pub environment: BTreeMap<String, String>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct AcquireContext {
    pub offline: bool,
    pub request_id: Uuid,
    #[serde(default)]
    pub environment: BTreeMap<String, String>,
}

impl Default for AcquireContext {
    fn default() -> Self {
        Self {
            offline: false,
            request_id: Uuid::new_v4(),
            environment: BTreeMap::new(),
        }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct SearchRequest {
    pub query: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub limit: Option<usize>,
    #[serde(default)]
    pub metadata: Metadata,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct SearchResult {
    pub reference: ArtifactRef,
    pub name: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub description: Option<String>,
    #[serde(default)]
    pub metadata: Metadata,
}

#[async_trait]
pub trait ArtifactProvider: Send + Sync + 'static {
    fn descriptor(&self) -> ProviderDescriptor;

    async fn resolve(
        &self,
        requirement: &ArtifactRequirement,
        context: &ResolveContext,
    ) -> Result<Resolution>;

    async fn acquire(
        &self,
        file: &ResolvedFile,
        destination: &Path,
        context: &AcquireContext,
    ) -> Result<Acquisition>;

    async fn search(
        &self,
        _request: &SearchRequest,
        _context: &ResolveContext,
    ) -> Result<Vec<SearchResult>> {
        Err(Error::Unsupported {
            provider: self.descriptor().name,
            operation: "search",
        })
    }
}

pub type DynProvider = Arc<dyn ArtifactProvider>;

pub fn provider_error(provider: impl Into<String>, error: impl fmt::Display) -> Error {
    Error::Provider {
        provider: provider.into(),
        message: error.to_string(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn reference_keeps_provider_owned_locator() {
        let reference: ArtifactRef = "hf:dataset:org/name@main".parse().unwrap();
        assert_eq!(reference.scheme(), "hf");
        assert_eq!(reference.locator(), "dataset:org/name@main");
    }

    #[test]
    fn artifact_paths_reject_traversal() {
        assert!(ArtifactPath::new("../secret").is_err());
        assert!(ArtifactPath::new("/absolute").is_err());
        assert_eq!(ArtifactPath::new("./a/b").unwrap().as_str(), "a/b");
    }

    #[test]
    fn selections_include_then_exclude() {
        let selection = Selection {
            include: vec!["**/*.json".into()],
            exclude: vec!["private/**".into()],
        }
        .compile()
        .unwrap();
        assert!(selection.matches("config/model.json"));
        assert!(!selection.matches("private/model.json"));
        assert!(!selection.matches("weights.bin"));
    }
}
