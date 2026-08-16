//! Artifact resolution and acquisition orchestration.

use std::{
    collections::{BTreeMap, HashMap},
    path::{Path, PathBuf},
    sync::Arc,
};

use artifactum_core::{
    AcquireContext, ArtifactProvider, ArtifactRef, ArtifactRequirement, Digest, DynProvider,
    ProviderDescriptor, ResolveContext, Resolution, SearchRequest, SearchResult, Selection,
};
use artifactum_store::{
    ArtifactStore, MaterializationMode, StoredArtifact, StoredFile, StoredManifest,
};
use serde::{Deserialize, Serialize};
use thiserror::Error;
use tokio::fs;
use uuid::Uuid;

#[derive(Debug, Error)]
pub enum Error {
    #[error("core error: {0}")]
    Core(#[from] artifactum_core::Error),
    #[error("store error: {0}")]
    Store(#[from] artifactum_store::Error),
    #[error("I/O error: {0}")]
    Io(#[from] std::io::Error),
    #[error("TOML decode error: {0}")]
    TomlDecode(#[from] toml::de::Error),
    #[error("TOML encode error: {0}")]
    TomlEncode(#[from] toml::ser::Error),
    #[error("JSON error: {0}")]
    Json(#[from] serde_json::Error),
    #[error("no provider is registered for scheme `{0}`")]
    UnknownScheme(String),
    #[error("scheme `{scheme}` is already owned by provider `{provider}`")]
    SchemeConflict { scheme: String, provider: String },
    #[error("provider name `{0}` is already registered")]
    ProviderNameConflict(String),
    #[error("project manifest version {0} is unsupported")]
    UnsupportedProjectVersion(u32),
    #[error("lockfile version {0} is unsupported")]
    UnsupportedLockVersion(u32),
    #[error("artifact `{0}` does not exist in the project manifest")]
    UnknownArtifact(String),
}

pub type Result<T, E = Error> = std::result::Result<T, E>;

#[derive(Clone, Default)]
pub struct ProviderRegistry {
    by_scheme: HashMap<String, DynProvider>,
    by_name: HashMap<String, DynProvider>,
}

impl ProviderRegistry {
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    pub fn register<P: ArtifactProvider>(&mut self, provider: P) -> Result<()> {
        self.register_arc(Arc::new(provider), false)
    }

    pub fn register_override<P: ArtifactProvider>(&mut self, provider: P) -> Result<()> {
        self.register_arc(Arc::new(provider), true)
    }

    pub fn register_dyn(&mut self, provider: DynProvider) -> Result<()> {
        self.register_arc(provider, false)
    }

    pub fn register_dyn_override(&mut self, provider: DynProvider) -> Result<()> {
        self.register_arc(provider, true)
    }

    fn register_arc(&mut self, provider: DynProvider, replace: bool) -> Result<()> {
        let descriptor = provider.descriptor();
        let schemes = descriptor
            .schemes
            .iter()
            .map(|scheme| scheme.to_ascii_lowercase())
            .collect::<Vec<_>>();

        if !replace {
            if self.by_name.contains_key(&descriptor.name) {
                return Err(Error::ProviderNameConflict(descriptor.name));
            }
            for scheme in &schemes {
                if let Some(existing) = self.by_scheme.get(scheme) {
                    return Err(Error::SchemeConflict {
                        scheme: scheme.clone(),
                        provider: existing.descriptor().name,
                    });
                }
            }
        } else {
            let mut displaced = Vec::new();
            if self.by_name.contains_key(&descriptor.name) {
                displaced.push(descriptor.name.clone());
            }
            for scheme in &schemes {
                if let Some(existing) = self.by_scheme.get(scheme) {
                    let name = existing.descriptor().name;
                    if !displaced.contains(&name) {
                        displaced.push(name);
                    }
                }
            }
            for name in displaced {
                self.remove_provider(&name);
            }
        }

        for scheme in schemes {
            self.by_scheme.insert(scheme, Arc::clone(&provider));
        }
        self.by_name.insert(descriptor.name, provider);
        Ok(())
    }

    fn remove_provider(&mut self, name: &str) {
        self.by_name.remove(name);
        self.by_scheme
            .retain(|_, provider| provider.descriptor().name != name);
    }

    #[must_use]
    pub fn get(&self, scheme: &str) -> Option<DynProvider> {
        self.by_scheme.get(&scheme.to_ascii_lowercase()).cloned()
    }

    #[must_use]
    pub fn descriptors(&self) -> Vec<ProviderDescriptor> {
        let mut providers = self
            .by_name
            .values()
            .map(|provider| provider.descriptor())
            .collect::<Vec<_>>();
        providers.sort_by(|a, b| a.name.cmp(&b.name));
        providers
    }

    #[must_use]
    pub fn get_by_name(&self, name: &str) -> Option<DynProvider> {
        self.by_name.get(name).cloned()
    }
}

pub struct ArtifactResolverBuilder {
    store: Option<ArtifactStore>,
    providers: ProviderRegistry,
    offline: bool,
}

impl Default for ArtifactResolverBuilder {
    fn default() -> Self {
        Self {
            store: None,
            providers: ProviderRegistry::new(),
            offline: false,
        }
    }
}

impl ArtifactResolverBuilder {
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    #[must_use]
    pub fn store(mut self, store: ArtifactStore) -> Self {
        self.store = Some(store);
        self
    }

    pub fn provider<P: ArtifactProvider>(mut self, provider: P) -> Result<Self> {
        self.providers.register(provider)?;
        Ok(self)
    }

    pub fn provider_dyn(mut self, provider: DynProvider) -> Result<Self> {
        self.providers.register_dyn(provider)?;
        Ok(self)
    }

    pub fn provider_dyn_override(mut self, provider: DynProvider) -> Result<Self> {
        self.providers.register_dyn_override(provider)?;
        Ok(self)
    }

    #[must_use]
    pub fn offline(mut self, offline: bool) -> Self {
        self.offline = offline;
        self
    }

    pub async fn build(self) -> Result<ArtifactResolver> {
        let store = match self.store {
            Some(store) => store,
            None => ArtifactStore::xdg().await?,
        };
        Ok(ArtifactResolver {
            store,
            providers: self.providers,
            offline: self.offline,
        })
    }
}

#[derive(Clone)]
pub struct ArtifactResolver {
    store: ArtifactStore,
    providers: ProviderRegistry,
    offline: bool,
}

impl ArtifactResolver {
    #[must_use]
    pub fn builder() -> ArtifactResolverBuilder {
        ArtifactResolverBuilder::new()
    }

    #[must_use]
    pub fn store(&self) -> &ArtifactStore {
        &self.store
    }

    #[must_use]
    pub fn providers(&self) -> Vec<ProviderDescriptor> {
        self.providers.descriptors()
    }

    pub async fn resolve(&self, requirement: &ArtifactRequirement) -> Result<Resolution> {
        let provider = self.provider_for(&requirement.reference)?;
        let context = ResolveContext {
            offline: self.offline,
            ..ResolveContext::default()
        };
        Ok(provider.resolve(requirement, &context).await?)
    }

    pub async fn resolve_ref(&self, reference: &str) -> Result<Resolution> {
        let requirement = ArtifactRequirement::new(reference.parse()?);
        self.resolve(&requirement).await
    }

    pub async fn fetch(&self, requirement: &ArtifactRequirement) -> Result<FetchedArtifact> {
        let resolution = self.resolve(requirement).await?;
        self.fetch_resolution(resolution).await
    }

    pub async fn get(&self, reference: &str) -> Result<FetchedArtifact> {
        let requirement = ArtifactRequirement::new(reference.parse()?);
        self.fetch(&requirement).await
    }

    pub async fn fetch_resolution(&self, resolution: Resolution) -> Result<FetchedArtifact> {
        let provider = self
            .providers
            .get_by_name(&resolution.provider)
            .ok_or_else(|| Error::UnknownScheme(resolution.provider.clone()))?;

        let mut files = Vec::with_capacity(resolution.files.len());
        for file in &resolution.files {
            let expected = match file.digests.sha256() {
                Some(value) => Some(Digest::sha256(value.to_owned())?),
                None => None,
            };

            let (digest, size) = if let Some(expected) = expected.as_ref() {
                if self.store.contains_blob(expected).await? && self.store.verify_blob(expected).await? {
                    let actual_size = fs::metadata(self.store.blob_path(expected)?).await?.len();
                    (expected.clone(), actual_size)
                } else {
                    self.acquire_file(provider.as_ref(), file, expected.as_ref()).await?
                }
            } else {
                self.acquire_file(provider.as_ref(), file, None).await?
            };

            files.push(StoredFile {
                path: file.path.clone(),
                digest,
                size,
                media_type: file.media_type.clone(),
            });
        }

        let artifact = StoredArtifact {
            provider: resolution.provider.clone(),
            canonical_ref: resolution.canonical_ref.clone(),
            revision: resolution.revision.as_ref().map(|revision| revision.id.clone()),
            files,
            provider_state: resolution.provider_state.clone(),
            metadata: resolution.metadata.clone(),
        };
        let manifest = self.store.store_manifest(&artifact).await?;
        Ok(FetchedArtifact {
            resolution,
            manifest,
        })
    }

    async fn acquire_file(
        &self,
        provider: &dyn ArtifactProvider,
        file: &artifactum_core::ResolvedFile,
        expected: Option<&Digest>,
    ) -> Result<(Digest, u64)> {
        if self.offline {
            return Err(artifactum_core::Error::Provider {
                provider: provider.descriptor().name,
                message: format!("artifact `{}` is not cached and resolver is offline", file.path),
            }
            .into());
        }

        let staging = self.store.staging_path().await?;
        let context = AcquireContext {
            offline: self.offline,
            request_id: Uuid::new_v4(),
            ..AcquireContext::default()
        };
        if let Err(error) = provider.acquire(file, &staging, &context).await {
            let _ = fs::remove_file(&staging).await;
            return Err(error.into());
        }
        Ok(self.store.commit_staging(staging, expected).await?)
    }

    pub async fn materialize(
        &self,
        fetched: &FetchedArtifact,
        destination: impl AsRef<Path>,
        mode: MaterializationMode,
    ) -> Result<()> {
        Ok(self
            .store
            .materialize(&fetched.manifest.artifact, destination, mode)
            .await?)
    }

    pub async fn search(&self, scheme: &str, request: &SearchRequest) -> Result<Vec<SearchResult>> {
        let provider = self
            .providers
            .get(scheme)
            .ok_or_else(|| Error::UnknownScheme(scheme.to_owned()))?;
        let context = ResolveContext {
            offline: self.offline,
            ..ResolveContext::default()
        };
        Ok(provider.search(request, &context).await?)
    }

    fn provider_for(&self, reference: &ArtifactRef) -> Result<DynProvider> {
        self.providers
            .get(reference.scheme())
            .ok_or_else(|| Error::UnknownScheme(reference.scheme().to_owned()))
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct FetchedArtifact {
    pub resolution: Resolution,
    pub manifest: StoredManifest,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ProjectManifest {
    #[serde(default = "project_version")]
    pub version: u32,
    #[serde(default)]
    pub artifacts: BTreeMap<String, ProjectArtifact>,
}

const fn project_version() -> u32 {
    1
}

impl Default for ProjectManifest {
    fn default() -> Self {
        Self {
            version: project_version(),
            artifacts: BTreeMap::new(),
        }
    }
}

impl ProjectManifest {
    pub async fn load(path: impl AsRef<Path>) -> Result<Self> {
        let path = path.as_ref();
        if !fs::try_exists(path).await? {
            return Ok(Self::default());
        }
        let manifest: Self = toml::from_str(&fs::read_to_string(path).await?)?;
        if manifest.version != project_version() {
            return Err(Error::UnsupportedProjectVersion(manifest.version));
        }
        Ok(manifest)
    }

    pub async fn save(&self, path: impl AsRef<Path>) -> Result<()> {
        let text = toml::to_string_pretty(self)?;
        fs::write(path, text).await?;
        Ok(())
    }

    pub fn requirement(&self, name: &str) -> Result<ArtifactRequirement> {
        let artifact = self
            .artifacts
            .get(name)
            .ok_or_else(|| Error::UnknownArtifact(name.to_owned()))?;
        artifact.requirement()
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ProjectArtifact {
    pub source: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub revision: Option<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub include: Vec<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub exclude: Vec<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub materialize: Option<PathBuf>,
}

impl ProjectArtifact {
    pub fn requirement(&self) -> Result<ArtifactRequirement> {
        Ok(ArtifactRequirement {
            reference: self.source.parse()?,
            revision: self.revision.clone(),
            selection: Selection {
                include: self.include.clone(),
                exclude: self.exclude.clone(),
            },
            metadata: BTreeMap::new(),
        })
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Lockfile {
    #[serde(default = "lock_version")]
    pub version: u32,
    #[serde(default, rename = "artifact")]
    pub artifacts: Vec<LockedArtifact>,
}

const fn lock_version() -> u32 {
    1
}

impl Default for Lockfile {
    fn default() -> Self {
        Self {
            version: lock_version(),
            artifacts: Vec::new(),
        }
    }
}

impl Lockfile {
    pub async fn load(path: impl AsRef<Path>) -> Result<Self> {
        let path = path.as_ref();
        if !fs::try_exists(path).await? {
            return Ok(Self::default());
        }
        let lockfile: Self = toml::from_str(&fs::read_to_string(path).await?)?;
        if lockfile.version != lock_version() {
            return Err(Error::UnsupportedLockVersion(lockfile.version));
        }
        Ok(lockfile)
    }

    pub async fn save(&self, path: impl AsRef<Path>) -> Result<()> {
        fs::write(path, toml::to_string_pretty(self)?).await?;
        Ok(())
    }

    pub fn upsert(&mut self, artifact: LockedArtifact) {
        if let Some(existing) = self
            .artifacts
            .iter_mut()
            .find(|existing| existing.name == artifact.name)
        {
            *existing = artifact;
        } else {
            self.artifacts.push(artifact);
            self.artifacts.sort_by(|a, b| a.name.cmp(&b.name));
        }
    }

    #[must_use]
    pub fn get(&self, name: &str) -> Option<&LockedArtifact> {
        self.artifacts.iter().find(|artifact| artifact.name == name)
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct LockedArtifact {
    pub name: String,
    pub provider: String,
    pub reference: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub revision: Option<String>,
    pub manifest: String,
    /// SHA-256 of the serialized project requirement that produced this lock
    /// entry. `--locked` uses it to detect manifest drift.
    pub requirement_digest: String,
    /// Opaque provider state encoded as JSON so the lockfile can preserve any
    /// JSON value without depending on TOML's value model.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub provider_state_json: Option<String>,
    #[serde(default)]
    pub files: Vec<LockedFile>,
}

impl LockedArtifact {
    pub fn from_fetched(
        name: impl Into<String>,
        requirement: &ArtifactRequirement,
        fetched: &FetchedArtifact,
    ) -> Result<Self> {
        Ok(Self {
            name: name.into(),
            provider: fetched.manifest.artifact.provider.clone(),
            reference: fetched.manifest.artifact.canonical_ref.clone(),
            revision: fetched.manifest.artifact.revision.clone(),
            manifest: fetched.manifest.digest.to_string(),
            requirement_digest: requirement_digest(requirement)?,
            provider_state_json: if fetched.resolution.provider_state.is_null() {
                None
            } else {
                Some(fetched.resolution.provider_state.to_string())
            },
            files: fetched
                .manifest
                .artifact
                .files
                .iter()
                .map(|file| {
                    let resolved = fetched
                        .resolution
                        .files
                        .iter()
                        .find(|resolved| resolved.path == file.path);
                    LockedFile {
                        path: file.path.to_string(),
                        digest: file.digest.to_string(),
                        size: file.size,
                        media_type: file.media_type.clone(),
                        source_json: resolved.and_then(|resolved| {
                            (!resolved.source.is_null()).then(|| resolved.source.to_string())
                        }),
                    }
                })
                .collect(),
        })
    }

    pub fn matches_requirement(&self, requirement: &ArtifactRequirement) -> Result<bool> {
        Ok(self.requirement_digest == requirement_digest(requirement)?)
    }

    pub fn to_resolution(&self) -> Result<Resolution> {
        let files = self
            .files
            .iter()
            .map(|file| -> Result<artifactum_core::ResolvedFile> {
                let digest: Digest = file.digest.parse()?;
                let mut digests = artifactum_core::DigestSet::default();
                digests.insert(digest);
                let source = match &file.source_json {
                    Some(source) => serde_json::from_str(source)?,
                    None => serde_json::Value::Null,
                };
                Ok(artifactum_core::ResolvedFile {
                    path: file.path.parse()?,
                    size: Some(file.size),
                    digests,
                    media_type: file.media_type.clone(),
                    source,
                })
            })
            .collect::<Result<Vec<_>>>()?;
        let provider_state = match &self.provider_state_json {
            Some(state) => serde_json::from_str(state)?,
            None => serde_json::Value::Null,
        };
        Ok(Resolution {
            provider: self.provider.clone(),
            canonical_ref: self.reference.clone(),
            revision: self.revision.as_ref().map(|revision| artifactum_core::ResolvedRevision {
                id: revision.clone(),
                requested: Some(revision.clone()),
            }),
            files,
            provider_state,
            metadata: BTreeMap::new(),
        })
    }
}

fn requirement_digest(requirement: &ArtifactRequirement) -> Result<String> {
    let bytes = serde_json::to_vec(requirement)?;
    Ok(artifactum_store::sha256_bytes(&bytes)?.to_string())
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct LockedFile {
    pub path: String,
    pub digest: String,
    pub size: u64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub media_type: Option<String>,
    /// Provider-owned reacquisition data encoded as opaque JSON.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub source_json: Option<String>,
}

#[cfg(test)]
mod tests {
    use std::path::Path;

    use artifactum_core::{
        AcquireContext, Acquisition, ArtifactPath, ProviderCapabilities, ProviderDescriptor,
        ResolvedFile,
    };
    use async_trait::async_trait;
    use tempfile::tempdir;

    use super::*;

    struct FakeProvider;

    #[async_trait]
    impl ArtifactProvider for FakeProvider {
        fn descriptor(&self) -> ProviderDescriptor {
            ProviderDescriptor {
                name: "fake".into(),
                version: "1".into(),
                schemes: vec!["fake".into()],
                capabilities: ProviderCapabilities {
                    resolve: true,
                    acquire: true,
                    ..ProviderCapabilities::default()
                },
                metadata: BTreeMap::new(),
            }
        }

        async fn resolve(
            &self,
            requirement: &ArtifactRequirement,
            _context: &ResolveContext,
        ) -> artifactum_core::Result<Resolution> {
            Ok(Resolution {
                provider: "fake".into(),
                canonical_ref: requirement.reference.to_string(),
                revision: None,
                files: vec![ResolvedFile {
                    path: ArtifactPath::new("model.bin")?,
                    size: Some(5),
                    digests: Default::default(),
                    media_type: None,
                    source: serde_json::json!({}),
                }],
                provider_state: serde_json::Value::Null,
                metadata: BTreeMap::new(),
            })
        }

        async fn acquire(
            &self,
            _file: &ResolvedFile,
            destination: &Path,
            _context: &AcquireContext,
        ) -> artifactum_core::Result<Acquisition> {
            fs::write(destination, b"model").await?;
            Ok(Acquisition {
                bytes_written: Some(5),
                metadata: BTreeMap::new(),
            })
        }
    }

    #[test]
    fn registry_conflicts_do_not_partially_register_provider() {
        struct First;
        struct Second;

        #[async_trait]
        impl ArtifactProvider for First {
            fn descriptor(&self) -> ProviderDescriptor {
                ProviderDescriptor {
                    name: "first".into(),
                    version: "1".into(),
                    schemes: vec!["one".into(), "shared".into()],
                    capabilities: ProviderCapabilities::default(),
                    metadata: BTreeMap::new(),
                }
            }

            async fn resolve(&self, _: &ArtifactRequirement, _: &ResolveContext) -> artifactum_core::Result<Resolution> {
                unreachable!()
            }

            async fn acquire(&self, _: &ResolvedFile, _: &Path, _: &AcquireContext) -> artifactum_core::Result<Acquisition> {
                unreachable!()
            }
        }

        #[async_trait]
        impl ArtifactProvider for Second {
            fn descriptor(&self) -> ProviderDescriptor {
                ProviderDescriptor {
                    name: "second".into(),
                    version: "1".into(),
                    schemes: vec!["two".into(), "shared".into()],
                    capabilities: ProviderCapabilities::default(),
                    metadata: BTreeMap::new(),
                }
            }

            async fn resolve(&self, _: &ArtifactRequirement, _: &ResolveContext) -> artifactum_core::Result<Resolution> {
                unreachable!()
            }

            async fn acquire(&self, _: &ResolvedFile, _: &Path, _: &AcquireContext) -> artifactum_core::Result<Acquisition> {
                unreachable!()
            }
        }

        let mut registry = ProviderRegistry::new();
        registry.register(First).unwrap();
        assert!(registry.register(Second).is_err());
        assert!(registry.get("two").is_none());
    }

    #[test]
    fn lockfile_round_trips_opaque_provider_json() {
        let locked = LockedArtifact {
            name: "fixture".into(),
            provider: "fake".into(),
            reference: "fake:fixture@immutable".into(),
            revision: Some("immutable".into()),
            manifest: format!("sha256:{}", "0".repeat(64)),
            requirement_digest: format!("sha256:{}", "2".repeat(64)),
            provider_state_json: Some(serde_json::json!({"nested": [null, true, {"x": 1}]}).to_string()),
            files: vec![LockedFile {
                path: "model.bin".into(),
                digest: format!("sha256:{}", "1".repeat(64)),
                size: 42,
                media_type: None,
                source_json: Some(serde_json::json!({"temporary": null, "id": 7}).to_string()),
            }],
        };
        let encoded = toml::to_string_pretty(&Lockfile {
            version: 1,
            artifacts: vec![locked],
        })
        .unwrap();
        let decoded: Lockfile = toml::from_str(&encoded).unwrap();
        let resolution = decoded.artifacts[0].to_resolution().unwrap();
        assert_eq!(resolution.provider_state["nested"][1].as_bool(), Some(true));
        assert_eq!(resolution.files[0].source["id"].as_u64(), Some(7));
        assert!(resolution.files[0].source["temporary"].is_null());
    }

    #[tokio::test]
    async fn resolver_fetches_into_store() {
        let directory = tempdir().unwrap();
        let store = ArtifactStore::open(directory.path()).await.unwrap();
        let resolver = ArtifactResolver::builder()
            .store(store)
            .provider(FakeProvider)
            .unwrap()
            .build()
            .await
            .unwrap();
        let fetched = resolver.get("fake:anything").await.unwrap();
        assert_eq!(fetched.manifest.artifact.files.len(), 1);
        assert!(resolver
            .store()
            .contains_blob(&fetched.manifest.artifact.files[0].digest)
            .await
            .unwrap());
    }
}
