//! External source resolution and acquisition plane.

pub use artifactum_core::ArtifactRef;
use artifactum_core::{
    ArtifactId, ArtifactPath, ContentKind, Digest, Metadata, SourceObservation, TreeEntry,
    TreeEntryKind, TreeManifest,
};
use artifactum_metadata::MetadataStore;
use artifactum_store::{ArtifactStore, ContentStore};
use artifactum_transport_http::{HttpRequest, HttpTransport};
use async_trait::async_trait;
use chrono::Utc;
use futures_util::{StreamExt, TryStreamExt, stream};
use globset::{Glob, GlobSet, GlobSetBuilder};
use serde::{Deserialize, Serialize};
use std::{
    collections::{BTreeMap, HashMap},
    fmt,
    path::{Path, PathBuf},
    str::FromStr,
    sync::Arc,
};
use thiserror::Error;
use tokio::fs;
use uuid::Uuid;
use walkdir::WalkDir;

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum AccessRequirement {
    Authentication,
    LicenseAcceptance,
    TermsAcceptance,
    Membership,
    ManualApproval,
    ExternalTool,
}
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct AccessChallenge {
    pub provider: String,
    pub requirement: AccessRequirement,
    pub message: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub action_url: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub tool: Option<String>,
}

#[derive(Debug, Error)]
pub enum Error {
    #[error("core error: {0}")]
    Core(#[from] artifactum_core::Error),
    #[error("store error: {0}")]
    Store(#[from] artifactum_store::Error),
    #[error("metadata error: {0}")]
    Metadata(#[from] artifactum_metadata::Error),
    #[error("transport error: {0}")]
    Http(#[from] artifactum_transport_http::Error),
    #[error("I/O error: {0}")]
    Io(#[from] std::io::Error),
    #[error("toml decode error: {0}")]
    Toml(#[from] toml::de::Error),
    #[error("provider `{provider}`: {message}")]
    Provider { provider: String, message: String },
    #[error("provider access requirement: {}",.0.message)]
    AccessRequired(AccessChallenge),
    #[error("unknown provider scheme/profile `{0}`")]
    UnknownScheme(String),
    #[error("provider name conflict `{0}`")]
    ProviderConflict(String),
    #[error("invalid selection `{0}`")]
    Selection(String),
    #[error("artifact resolution produced no files")]
    EmptyResolution,
    #[error("offline and content is not present locally: {0}")]
    Offline(String),
}
pub type Result<T, E = Error> = std::result::Result<T, E>;

#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct Selection {
    #[serde(default)]
    pub include: Vec<String>,
    #[serde(default)]
    pub exclude: Vec<String>,
}
impl Selection {
    fn compile(&self) -> Result<CompiledSelection> {
        fn build(v: &[String]) -> Result<GlobSet> {
            let mut b = GlobSetBuilder::new();
            for p in v {
                b.add(Glob::new(p).map_err(|e| Error::Selection(e.to_string()))?);
            }
            b.build().map_err(|e| Error::Selection(e.to_string()))
        }
        Ok(CompiledSelection {
            all: self.include.is_empty(),
            include: build(&self.include)?,
            exclude: build(&self.exclude)?,
        })
    }
    pub fn matches(&self, path: &str) -> Result<bool> {
        Ok(self.compile()?.matches(path))
    }
}
struct CompiledSelection {
    all: bool,
    include: GlobSet,
    exclude: GlobSet,
}
impl CompiledSelection {
    fn matches(&self, p: &str) -> bool {
        (self.all || self.include.is_match(p)) && !self.exclude.is_match(p)
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
#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct ProviderProfile {
    pub name: String,
    pub provider: String,
    #[serde(default)]
    pub config: BTreeMap<String, String>,
}
#[derive(Clone, Copy, Debug, Default, Serialize, Deserialize)]
pub struct ProviderCapabilities {
    pub resolve: bool,
    pub acquire: bool,
    pub search: bool,
    pub inspect: bool,
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
}
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ResolvedRevision {
    pub id: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub requested: Option<String>,
}
#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct DigestSet(pub BTreeMap<String, String>);
impl DigestSet {
    pub fn sha256(&self) -> Option<&str> {
        self.0.get("sha256").map(String::as_str)
    }
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
    #[serde(default)]
    pub source: serde_json::Value,
}
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Resolution {
    pub provider: String,
    pub canonical_ref: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub revision: Option<ResolvedRevision>,
    pub files: Vec<ResolvedFile>,
    #[serde(default)]
    pub provider_state: serde_json::Value,
    #[serde(default)]
    pub metadata: Metadata,
}
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum AcquisitionPlan {
    Http {
        url: String,
        #[serde(default)]
        headers: BTreeMap<String, String>,
        #[serde(default = "yes")]
        resume: bool,
    },
    LocalCopy {
        path: PathBuf,
    },
    ProviderManaged {
        #[serde(default)]
        state: serde_json::Value,
    },
}
const fn yes() -> bool {
    true
}
#[derive(Clone, Debug, Default)]
pub struct ResolveContext {
    pub offline: bool,
    pub profile: Option<ProviderProfile>,
}
#[derive(Clone, Debug)]
pub struct AcquireContext {
    pub offline: bool,
    pub request_id: Uuid,
    pub profile: Option<ProviderProfile>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct SearchRequest {
    pub query: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub limit: Option<usize>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub cursor: Option<String>,
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
#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct SearchPage {
    #[serde(default)]
    pub items: Vec<SearchResult>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub next_cursor: Option<String>,
}
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct InspectResult {
    pub reference: ArtifactRef,
    #[serde(default)]
    pub metadata: Metadata,
}
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct VersionInfo {
    pub id: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub name: Option<String>,
    #[serde(default)]
    pub metadata: Metadata,
}
#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct VersionPage {
    #[serde(default)]
    pub items: Vec<VersionInfo>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub next_cursor: Option<String>,
}
#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct FilePage {
    #[serde(default)]
    pub items: Vec<ResolvedFile>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub next_cursor: Option<String>,
}

#[async_trait]
pub trait ArtifactProvider: Send + Sync + 'static {
    fn descriptor(&self) -> ProviderDescriptor;
    async fn resolve(
        &self,
        requirement: &ArtifactRequirement,
        context: &ResolveContext,
    ) -> Result<Resolution>;
    async fn prepare_acquisition(
        &self,
        file: &ResolvedFile,
        _context: &AcquireContext,
    ) -> Result<AcquisitionPlan> {
        Ok(AcquisitionPlan::ProviderManaged {
            state: file.source.clone(),
        })
    }
    async fn acquire_managed(
        &self,
        _file: &ResolvedFile,
        _plan: &AcquisitionPlan,
        _destination: &Path,
        _context: &AcquireContext,
    ) -> Result<u64> {
        Err(Error::Provider {
            provider: self.descriptor().name,
            message: "managed acquisition unsupported".into(),
        })
    }
    async fn search(
        &self,
        _request: &SearchRequest,
        _context: &ResolveContext,
    ) -> Result<SearchPage> {
        Err(Error::Provider {
            provider: self.descriptor().name,
            message: "search unsupported".into(),
        })
    }
    async fn inspect(
        &self,
        reference: &ArtifactRef,
        _context: &ResolveContext,
    ) -> Result<InspectResult> {
        Ok(InspectResult {
            reference: reference.clone(),
            metadata: BTreeMap::new(),
        })
    }
    async fn list_versions(
        &self,
        _reference: &ArtifactRef,
        _cursor: Option<&str>,
        _context: &ResolveContext,
    ) -> Result<VersionPage> {
        Err(Error::Provider {
            provider: self.descriptor().name,
            message: "version listing unsupported".into(),
        })
    }
    async fn list_files(
        &self,
        requirement: &ArtifactRequirement,
        cursor: Option<&str>,
        context: &ResolveContext,
    ) -> Result<FilePage> {
        if cursor.is_some() {
            return Ok(FilePage::default());
        }
        let r = self.resolve(requirement, context).await?;
        Ok(FilePage {
            items: r.files,
            next_cursor: None,
        })
    }
}
pub type DynProvider = Arc<dyn ArtifactProvider>;

#[derive(Clone, Default)]
pub struct ProviderRegistry {
    by_scheme: HashMap<String, DynProvider>,
    by_name: HashMap<String, DynProvider>,
}
impl ProviderRegistry {
    pub fn register_dyn(&mut self, p: DynProvider) -> Result<()> {
        let d = p.descriptor();
        if self.by_name.contains_key(&d.name) {
            return Err(Error::ProviderConflict(d.name));
        }
        for s in &d.schemes {
            if self.by_scheme.contains_key(&s.to_ascii_lowercase()) {
                return Err(Error::ProviderConflict(s.clone()));
            }
        }
        for s in &d.schemes {
            self.by_scheme
                .insert(s.to_ascii_lowercase(), Arc::clone(&p));
        }
        self.by_name.insert(d.name, p);
        Ok(())
    }
    pub fn register<P: ArtifactProvider>(&mut self, p: P) -> Result<()> {
        self.register_dyn(Arc::new(p))
    }
    fn by_scheme(&self, s: &str) -> Option<DynProvider> {
        self.by_scheme.get(&s.to_ascii_lowercase()).cloned()
    }
    fn by_name(&self, s: &str) -> Option<DynProvider> {
        self.by_name.get(s).cloned()
    }
    pub fn descriptors(&self) -> Vec<ProviderDescriptor> {
        let mut v = self
            .by_name
            .values()
            .map(|p| p.descriptor())
            .collect::<Vec<_>>();
        v.sort_by(|a, b| a.name.cmp(&b.name));
        v
    }
}

#[derive(Clone)]
pub struct ArtifactResolver {
    store: ArtifactStore,
    metadata: MetadataStore,
    providers: ProviderRegistry,
    profiles: BTreeMap<String, ProviderProfile>,
    http: HttpTransport,
    offline: bool,
    max_concurrency: usize,
}
pub struct ArtifactResolverBuilder {
    store: Option<ArtifactStore>,
    metadata: Option<MetadataStore>,
    providers: ProviderRegistry,
    profiles: BTreeMap<String, ProviderProfile>,
    offline: bool,
    max_concurrency: usize,
}
impl Default for ArtifactResolverBuilder {
    fn default() -> Self {
        let mut providers = ProviderRegistry::default();
        providers.register(LocalProvider).expect("local provider");
        providers.register(HttpProvider).expect("http provider");
        Self {
            store: None,
            metadata: None,
            providers,
            profiles: BTreeMap::new(),
            offline: false,
            max_concurrency: 8,
        }
    }
}
impl ArtifactResolverBuilder {
    pub fn store(mut self, v: ArtifactStore) -> Self {
        self.store = Some(v);
        self
    }
    pub fn metadata(mut self, v: MetadataStore) -> Self {
        self.metadata = Some(v);
        self
    }
    pub fn offline(mut self, v: bool) -> Self {
        self.offline = v;
        self
    }
    pub fn max_concurrency(mut self, v: usize) -> Self {
        self.max_concurrency = v.max(1);
        self
    }
    pub fn provider<P: ArtifactProvider>(mut self, p: P) -> Result<Self> {
        self.providers.register(p)?;
        Ok(self)
    }
    pub fn provider_dyn(mut self, p: DynProvider) -> Result<Self> {
        self.providers.register_dyn(p)?;
        Ok(self)
    }
    pub fn profile(mut self, p: ProviderProfile) -> Self {
        self.profiles.insert(p.name.to_ascii_lowercase(), p);
        self
    }
    pub async fn build(self) -> Result<ArtifactResolver> {
        Ok(ArtifactResolver {
            store: match self.store {
                Some(v) => v,
                None => ArtifactStore::xdg().await?,
            },
            metadata: self.metadata.unwrap_or(MetadataStore::xdg()?),
            providers: self.providers,
            profiles: self.profiles,
            http: HttpTransport::new(),
            offline: self.offline,
            max_concurrency: self.max_concurrency,
        })
    }
}
impl ArtifactResolver {
    pub fn builder() -> ArtifactResolverBuilder {
        ArtifactResolverBuilder::default()
    }
    pub fn store(&self) -> &ArtifactStore {
        &self.store
    }
    pub fn metadata(&self) -> &MetadataStore {
        &self.metadata
    }
    pub fn providers(&self) -> Vec<ProviderDescriptor> {
        self.providers.descriptors()
    }
    fn routed(
        &self,
        r: &ArtifactRequirement,
    ) -> Result<(DynProvider, ArtifactRequirement, Option<ProviderProfile>)> {
        let s = r.reference.scheme().to_ascii_lowercase();
        if let Some(profile) = self.profiles.get(&s) {
            let p = self
                .providers
                .by_name(&profile.provider)
                .or_else(|| self.providers.by_scheme(&profile.provider))
                .ok_or_else(|| Error::UnknownScheme(profile.provider.clone()))?;
            let target = p
                .descriptor()
                .schemes
                .first()
                .cloned()
                .unwrap_or(profile.provider.clone());
            let mut rr = r.clone();
            rr.reference = rr.reference.with_scheme(target)?;
            return Ok((p, rr, Some(profile.clone())));
        }
        let p = self
            .providers
            .by_scheme(&s)
            .ok_or_else(|| Error::UnknownScheme(s))?;
        Ok((p, r.clone(), None))
    }
    pub async fn resolve(&self, r: &ArtifactRequirement) -> Result<Resolution> {
        let (p, rr, profile) = self.routed(r)?;
        let mut out = p
            .resolve(
                &rr,
                &ResolveContext {
                    offline: self.offline,
                    profile: profile.clone(),
                },
            )
            .await?;
        if let Some(pr) = profile {
            out.metadata.insert(
                "artifactum_profile".into(),
                serde_json::Value::String(pr.name),
            );
        }
        Ok(out)
    }
    pub async fn get(&self, reference: &str) -> Result<ResolvedSource> {
        let r = ArtifactRequirement::new(ArtifactRef::from_str(reference)?);
        self.acquire(&r).await
    }
    pub async fn acquire(&self, r: &ArtifactRequirement) -> Result<ResolvedSource> {
        let res = self.resolve(r).await?;
        self.acquire_resolution(res, r.selection.clone()).await
    }
    pub async fn acquire_resolution(
        &self,
        res: Resolution,
        selection: Selection,
    ) -> Result<ResolvedSource> {
        let matcher = selection.compile()?;
        let selected = res
            .files
            .iter()
            .filter(|f| matcher.matches(f.path.as_str()))
            .cloned()
            .collect::<Vec<_>>();
        if selected.is_empty() {
            return Err(Error::EmptyResolution);
        }
        let provider = self
            .providers
            .by_name(&res.provider)
            .or_else(|| self.providers.by_scheme(&res.provider))
            .ok_or_else(|| Error::UnknownScheme(res.provider.clone()))?;
        let profile = res
            .metadata
            .get("artifactum_profile")
            .and_then(|v| v.as_str())
            .and_then(|n| self.profiles.get(&n.to_ascii_lowercase()))
            .cloned();
        let this = self.clone();
        let mut stored = stream::iter(selected.into_iter().map(|f| {
            let this = this.clone();
            let p = Arc::clone(&provider);
            let profile = profile.clone();
            async move { this.acquire_one(p, &f, profile).await }
        }))
        .buffer_unordered(self.max_concurrency)
        .try_collect::<Vec<_>>()
        .await?;
        stored.sort_by(|a, b| a.0.as_str().cmp(b.0.as_str()));
        let artifact = if stored.len() == 1 && res.files.len() == 1 {
            let (_, content, media, _) = stored[0].clone();
            let mut a = artifactum_core::ArtifactManifest::new(content, ContentKind::Blob);
            a.media_type = media;
            self.store.put_artifact(&a).await?
        } else {
            let tree = TreeManifest::new(
                stored
                    .iter()
                    .cloned()
                    .map(|(path, content, _, size)| TreeEntry {
                        path,
                        kind: TreeEntryKind::Blob,
                        content,
                        size,
                        executable: None,
                    })
                    .collect(),
            );
            self.store.put_tree_manifest(&tree).await?
        };
        if res.files.len() > 1 {
            for (path, content, _, _) in &stored {
                let file_artifact = self
                    .store
                    .put_artifact(&artifactum_core::ArtifactManifest::new(
                        content.clone(),
                        ContentKind::Blob,
                    ))
                    .await?;
                let file_obs = SourceObservation {
                    id: Uuid::new_v4(),
                    artifact: file_artifact,
                    provider: res.provider.clone(),
                    canonical_ref: format!("{}#{}", res.canonical_ref, path),
                    revision: res.revision.as_ref().map(|x| x.id.clone()),
                    observed_at: Utc::now(),
                    etag: None,
                    last_modified: None,
                    provider_state: serde_json::Value::Null,
                    metadata: res.metadata.clone(),
                };
                self.metadata.record_source_observation(&file_obs)?;
            }
        }
        let obs = SourceObservation {
            id: Uuid::new_v4(),
            artifact: artifact.clone(),
            provider: res.provider.clone(),
            canonical_ref: res.canonical_ref.clone(),
            revision: res.revision.as_ref().map(|x| x.id.clone()),
            observed_at: Utc::now(),
            etag: None,
            last_modified: None,
            provider_state: res.provider_state.clone(),
            metadata: res.metadata.clone(),
        };
        self.metadata.record_source_observation(&obs)?;
        Ok(ResolvedSource {
            resolution: res,
            artifact,
            observation: obs,
        })
    }
    async fn acquire_one(
        &self,
        p: DynProvider,
        f: &ResolvedFile,
        profile: Option<ProviderProfile>,
    ) -> Result<(
        ArtifactPath,
        artifactum_core::ContentId,
        Option<String>,
        u64,
    )> {
        let expected = f
            .digests
            .sha256()
            .map(|hex| Digest::sha256(hex.to_owned()))
            .transpose()?;
        if let Some(expected) = expected.as_ref() {
            let id = artifactum_core::ContentId(expected.clone());
            if self.store.contains_content(&id).await? && self.store.verify_content(&id).await? {
                let size = fs::metadata(self.store.content_path(&id)?).await?.len();
                return Ok((f.path.clone(), id, f.media_type.clone(), size));
            }
        }
        if self.offline {
            return Err(Error::Offline(f.path.to_string()));
        }
        // Reacquisition identity deliberately includes only durable source state and
        // provider-profile identity, never profile credentials/config values.
        let journal = serde_json::json!({"provider":p.descriptor().name,"path":f.path,"source":f.source,"expected":expected,"profile":profile.as_ref().map(|profile|profile.name.as_str())});
        let acquisition_key = artifactum_core::hash_canonical(&journal)?.to_string();
        let _lock = self
            .store
            .acquire_lock(&format!("acquire:{acquisition_key}"))
            .await?;
        // Another process may have completed the same acquisition while we waited.
        if let Some(expected) = expected.as_ref() {
            let id = artifactum_core::ContentId(expected.clone());
            if self.store.contains_content(&id).await? && self.store.verify_content(&id).await? {
                let size = fs::metadata(self.store.content_path(&id)?).await?.len();
                return Ok((f.path.clone(), id, f.media_type.clone(), size));
            }
        }
        let staging = self.store.resumable_staging_path(&acquisition_key).await?;
        let ctx = AcquireContext {
            offline: false,
            request_id: Uuid::new_v4(),
            profile,
        };
        let plan = p.prepare_acquisition(f, &ctx).await?;
        match &plan {
            AcquisitionPlan::Http {
                url,
                headers,
                resume,
            } => {
                if !*resume {
                    let _ = fs::remove_file(&staging).await;
                }
                self.http
                    .execute(
                        &HttpRequest {
                            url: url.clone(),
                            headers: headers.clone(),
                            resume: *resume,
                            retries: 4,
                        },
                        &staging,
                    )
                    .await?;
            }
            AcquisitionPlan::LocalCopy { path } => {
                let _ = fs::remove_file(&staging).await;
                fs::copy(path, &staging).await?;
            }
            AcquisitionPlan::ProviderManaged { .. } => {
                let _ = fs::remove_file(&staging).await;
                p.acquire_managed(f, &plan, &staging, &ctx).await?;
            }
        }
        let content = self
            .store
            .commit_staging_expected(&staging, expected.as_ref())
            .await?;
        let size = fs::metadata(self.store.content_path(&content)?)
            .await?
            .len();
        Ok((f.path.clone(), content, f.media_type.clone(), size))
    }
    pub async fn search(&self, scheme: &str, request: &SearchRequest) -> Result<SearchPage> {
        let reference = ArtifactRef::new(scheme, "_")?;
        let req = ArtifactRequirement::new(reference);
        let (p, _, profile) = self.routed(&req)?;
        p.search(
            request,
            &ResolveContext {
                offline: self.offline,
                profile,
            },
        )
        .await
    }
    pub async fn inspect(&self, reference: &ArtifactRef) -> Result<InspectResult> {
        let req = ArtifactRequirement::new(reference.clone());
        let (p, routed, profile) = self.routed(&req)?;
        p.inspect(
            &routed.reference,
            &ResolveContext {
                offline: self.offline,
                profile,
            },
        )
        .await
    }
    pub async fn list_versions(
        &self,
        reference: &ArtifactRef,
        cursor: Option<&str>,
    ) -> Result<VersionPage> {
        let req = ArtifactRequirement::new(reference.clone());
        let (p, routed, profile) = self.routed(&req)?;
        p.list_versions(
            &routed.reference,
            cursor,
            &ResolveContext {
                offline: self.offline,
                profile,
            },
        )
        .await
    }
    pub async fn list_files(
        &self,
        requirement: &ArtifactRequirement,
        cursor: Option<&str>,
    ) -> Result<FilePage> {
        let (p, routed, profile) = self.routed(requirement)?;
        p.list_files(
            &routed,
            cursor,
            &ResolveContext {
                offline: self.offline,
                profile,
            },
        )
        .await
    }
}
#[derive(Clone, Debug)]
pub struct ResolvedSource {
    pub resolution: Resolution,
    pub artifact: ArtifactId,
    pub observation: SourceObservation,
}

/// Built-in local provider supports a file or recursively enumerated directory.
pub struct LocalProvider;
#[async_trait]
impl ArtifactProvider for LocalProvider {
    fn descriptor(&self) -> ProviderDescriptor {
        ProviderDescriptor {
            name: "local".into(),
            version: env!("CARGO_PKG_VERSION").into(),
            schemes: vec!["local".into(), "file".into()],
            capabilities: ProviderCapabilities {
                resolve: true,
                acquire: true,
                ..Default::default()
            },
        }
    }
    async fn resolve(&self, r: &ArtifactRequirement, _: &ResolveContext) -> Result<Resolution> {
        let root = PathBuf::from(r.reference.locator());
        let meta = fs::metadata(&root).await?;
        let mut files = Vec::new();
        if meta.is_file() {
            files.push(ResolvedFile {
                path: ArtifactPath::new(
                    root.file_name()
                        .and_then(|x| x.to_str())
                        .unwrap_or("artifact"),
                )?,
                size: Some(meta.len()),
                digests: DigestSet(BTreeMap::new()),
                media_type: None,
                source: serde_json::json!({"path":root}),
            });
        } else {
            for e in WalkDir::new(&root)
                .into_iter()
                .filter_map(std::result::Result::ok)
            {
                if !e.file_type().is_file() {
                    continue;
                };
                let rel = e.path().strip_prefix(&root).map_err(|x| Error::Provider {
                    provider: "local".into(),
                    message: x.to_string(),
                })?;
                files.push(ResolvedFile {
                    path: ArtifactPath::new(rel.to_string_lossy())?,
                    size: e.metadata().ok().map(|m| m.len()),
                    digests: DigestSet(BTreeMap::new()),
                    media_type: None,
                    source: serde_json::json!({"path":e.path()}),
                });
            }
        }
        Ok(Resolution {
            provider: "local".into(),
            canonical_ref: format!("local:{}", root.display()),
            revision: None,
            files,
            provider_state: serde_json::Value::Null,
            metadata: BTreeMap::new(),
        })
    }
    async fn prepare_acquisition(
        &self,
        f: &ResolvedFile,
        _: &AcquireContext,
    ) -> Result<AcquisitionPlan> {
        let p = f
            .source
            .get("path")
            .and_then(|v| v.as_str())
            .ok_or_else(|| Error::Provider {
                provider: "local".into(),
                message: "missing path state".into(),
            })?;
        Ok(AcquisitionPlan::LocalCopy { path: p.into() })
    }
}

pub struct HttpProvider;
#[async_trait]
impl ArtifactProvider for HttpProvider {
    fn descriptor(&self) -> ProviderDescriptor {
        ProviderDescriptor {
            name: "http".into(),
            version: env!("CARGO_PKG_VERSION").into(),
            schemes: vec!["http".into(), "https".into()],
            capabilities: ProviderCapabilities {
                resolve: true,
                acquire: true,
                ..Default::default()
            },
        }
    }
    async fn resolve(&self, r: &ArtifactRequirement, _: &ResolveContext) -> Result<Resolution> {
        let url = r.reference.to_string();
        let name = r
            .reference
            .locator()
            .rsplit('/')
            .next()
            .filter(|x| !x.is_empty())
            .unwrap_or("download");
        Ok(Resolution {
            provider: "http".into(),
            canonical_ref: url.clone(),
            revision: r.revision.as_ref().map(|id| ResolvedRevision {
                id: id.clone(),
                requested: Some(id.clone()),
            }),
            files: vec![ResolvedFile {
                path: ArtifactPath::new(name)?,
                size: None,
                digests: DigestSet(BTreeMap::new()),
                media_type: None,
                source: serde_json::json!({"url":url}),
            }],
            provider_state: serde_json::Value::Null,
            metadata: BTreeMap::new(),
        })
    }
    async fn prepare_acquisition(
        &self,
        f: &ResolvedFile,
        _: &AcquireContext,
    ) -> Result<AcquisitionPlan> {
        let url = f
            .source
            .get("url")
            .and_then(|v| v.as_str())
            .ok_or_else(|| Error::Provider {
                provider: "http".into(),
                message: "missing URL".into(),
            })?;
        Ok(AcquisitionPlan::Http {
            url: url.into(),
            headers: BTreeMap::new(),
            resume: true,
        })
    }
}

pub fn access_required(
    provider: impl Into<String>,
    requirement: AccessRequirement,
    message: impl Into<String>,
) -> Error {
    Error::AccessRequired(AccessChallenge {
        provider: provider.into(),
        requirement,
        message: message.into(),
        action_url: None,
        tool: None,
    })
}
impl fmt::Display for Resolution {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "{}:{} ({} files)",
            self.provider,
            self.canonical_ref,
            self.files.len()
        )
    }
}
