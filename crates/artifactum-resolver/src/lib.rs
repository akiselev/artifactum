//! Artifact resolution, profile routing, lazy acquisition, and CAS orchestration.

use std::{collections::{BTreeMap,HashMap}, path::{Path,PathBuf}, sync::Arc};

use artifactum_core::{
    AcquireContext, AcquisitionPlan, ArtifactProvider, ArtifactRef, ArtifactRequirement, Digest,
    DynProvider, ProviderDescriptor, ProviderProfile, ResolveContext, Resolution, ResolvedFile,
    FilePage, InspectRequest, InspectResult, SearchPage, SearchRequest, Selection, VersionPage,
};
use artifactum_store::{ArtifactStore,MaterializationMode,StoredArtifact,StoredFile,StoredManifest};
use artifactum_transport_http::HttpTransport;
use futures_util::{stream, StreamExt, TryStreamExt};
use serde::{Deserialize,Serialize};
use thiserror::Error;
use tokio::fs;
use uuid::Uuid;

#[derive(Debug,Error)]
pub enum Error {
    #[error("core error: {0}")] Core(#[from] artifactum_core::Error),
    #[error("store error: {0}")] Store(#[from] artifactum_store::Error),
    #[error("HTTP transport error: {0}")] Http(#[from] artifactum_transport_http::Error),
    #[error("I/O error: {0}")] Io(#[from] std::io::Error),
    #[error("TOML decode error: {0}")] TomlDecode(#[from] toml::de::Error),
    #[error("TOML encode error: {0}")] TomlEncode(#[from] toml::ser::Error),
    #[error("JSON error: {0}")] Json(#[from] serde_json::Error),
    #[error("no provider is registered for scheme or profile `{0}`")] UnknownScheme(String),
    #[error("scheme `{scheme}` is already owned by provider `{provider}`")] SchemeConflict{scheme:String,provider:String},
    #[error("provider name `{0}` is already registered")] ProviderNameConflict(String),
    #[error("provider profile `{0}` refers to an unavailable provider")] UnknownProfileProvider(String),
    #[error("project manifest version {0} is unsupported")] UnsupportedProjectVersion(u32),
    #[error("lockfile version {0} is unsupported")] UnsupportedLockVersion(u32),
    #[error("artifact `{0}` does not exist in the project manifest")] UnknownArtifact(String),
    #[error("artifact `{artifact}` has no resolved file `{path}`")] UnknownFile{artifact:String,path:String},
}
pub type Result<T,E=Error>=std::result::Result<T,E>;

#[derive(Clone,Default)]
pub struct ProviderRegistry { by_scheme:HashMap<String,DynProvider>, by_name:HashMap<String,DynProvider> }
impl ProviderRegistry {
    #[must_use] pub fn new()->Self{Self::default()}
    pub fn register<P:ArtifactProvider>(&mut self,p:P)->Result<()>{self.register_arc(Arc::new(p),false)}
    pub fn register_dyn(&mut self,p:DynProvider)->Result<()>{self.register_arc(p,false)}
    pub fn register_dyn_override(&mut self,p:DynProvider)->Result<()>{self.register_arc(p,true)}
    fn register_arc(&mut self,p:DynProvider,replace:bool)->Result<()> {
        let d=p.descriptor(); let schemes=d.schemes.iter().map(|s|s.to_ascii_lowercase()).collect::<Vec<_>>();
        if !replace {
            if self.by_name.contains_key(&d.name){return Err(Error::ProviderNameConflict(d.name));}
            for s in &schemes {if let Some(existing)=self.by_scheme.get(s){return Err(Error::SchemeConflict{scheme:s.clone(),provider:existing.descriptor().name});}}
        } else {
            let mut displaced=Vec::new(); if self.by_name.contains_key(&d.name){displaced.push(d.name.clone());}
            for s in &schemes {if let Some(existing)=self.by_scheme.get(s){let n=existing.descriptor().name;if !displaced.contains(&n){displaced.push(n);}}}
            for n in displaced {self.remove_provider(&n);}
        }
        for s in schemes {self.by_scheme.insert(s,Arc::clone(&p));} self.by_name.insert(d.name,p); Ok(())
    }
    fn remove_provider(&mut self,name:&str){self.by_name.remove(name);self.by_scheme.retain(|_,p|p.descriptor().name!=name);}
    #[must_use] pub fn get(&self,scheme:&str)->Option<DynProvider>{self.by_scheme.get(&scheme.to_ascii_lowercase()).cloned()}
    #[must_use] pub fn get_by_name(&self,name:&str)->Option<DynProvider>{self.by_name.get(name).cloned()}
    #[must_use] pub fn descriptors(&self)->Vec<ProviderDescriptor>{let mut v=self.by_name.values().map(|p|p.descriptor()).collect::<Vec<_>>();v.sort_by(|a,b|a.name.cmp(&b.name));v}
}

pub struct ArtifactResolverBuilder {
    store:Option<ArtifactStore>, providers:ProviderRegistry, profiles:BTreeMap<String,ProviderProfile>, offline:bool,
    max_concurrent_files:usize, http:HttpTransport,
}
impl Default for ArtifactResolverBuilder {fn default()->Self{Self{store:None,providers:ProviderRegistry::new(),profiles:BTreeMap::new(),offline:false,max_concurrent_files:8,http:HttpTransport::new()}}}
impl ArtifactResolverBuilder {
    #[must_use] pub fn new()->Self{Self::default()}
    #[must_use] pub fn store(mut self,s:ArtifactStore)->Self{self.store=Some(s);self}
    #[must_use] pub fn offline(mut self,v:bool)->Self{self.offline=v;self}
    #[must_use] pub fn max_concurrent_files(mut self,v:usize)->Self{self.max_concurrent_files=v.max(1);self}
    #[must_use] pub fn profile(mut self,p:ProviderProfile)->Self{self.profiles.insert(p.name.to_ascii_lowercase(),p);self}
    pub fn profiles(mut self,profiles:impl IntoIterator<Item=ProviderProfile>)->Self{for p in profiles{self=self.profile(p);}self}
    pub fn provider<P:ArtifactProvider>(mut self,p:P)->Result<Self>{self.providers.register(p)?;Ok(self)}
    pub fn provider_dyn(mut self,p:DynProvider)->Result<Self>{self.providers.register_dyn(p)?;Ok(self)}
    pub fn provider_dyn_override(mut self,p:DynProvider)->Result<Self>{self.providers.register_dyn_override(p)?;Ok(self)}
    pub async fn build(self)->Result<ArtifactResolver>{
        let store=match self.store{Some(s)=>s,None=>ArtifactStore::xdg().await?};
        Ok(ArtifactResolver{store,providers:self.providers,profiles:self.profiles,offline:self.offline,max_concurrent_files:self.max_concurrent_files,http:self.http})
    }
}

#[derive(Clone)]
pub struct ArtifactResolver { store:ArtifactStore, providers:ProviderRegistry, profiles:BTreeMap<String,ProviderProfile>, offline:bool,max_concurrent_files:usize,http:HttpTransport }
impl ArtifactResolver {
    #[must_use] pub fn builder()->ArtifactResolverBuilder{ArtifactResolverBuilder::new()}
    #[must_use] pub fn store(&self)->&ArtifactStore{&self.store}
    #[must_use] pub fn providers(&self)->Vec<ProviderDescriptor>{self.providers.descriptors()}
    #[must_use] pub fn profiles(&self)->Vec<ProviderProfile>{self.profiles.values().cloned().collect()}

    fn routed(&self,requirement:&ArtifactRequirement)->Result<(DynProvider,ArtifactRequirement,Option<ProviderProfile>)>{
        let scheme=requirement.reference.scheme().to_ascii_lowercase();
        if let Some(profile)=self.profiles.get(&scheme) {
            let provider=self.providers.get_by_name(&profile.provider).or_else(||self.providers.get(&profile.provider)).ok_or_else(||Error::UnknownProfileProvider(profile.provider.clone()))?;
            let target_scheme=provider.descriptor().schemes.first().cloned().unwrap_or_else(||profile.provider.clone());
            let mut routed=requirement.clone(); routed.reference=requirement.reference.with_scheme(target_scheme)?;
            return Ok((provider,routed,Some(profile.clone())));
        }
        let provider=self.providers.get(&scheme).ok_or_else(||Error::UnknownScheme(scheme))?;
        Ok((provider,requirement.clone(),None))
    }
    fn resolve_context(&self,profile:Option<ProviderProfile>)->ResolveContext{ResolveContext{offline:self.offline,environment:BTreeMap::new(),profile}}
    fn acquire_context(&self,profile:Option<ProviderProfile>)->AcquireContext{AcquireContext{offline:self.offline,request_id:Uuid::new_v4(),environment:BTreeMap::new(),profile}}

    pub async fn resolve(&self,requirement:&ArtifactRequirement)->Result<Resolution>{let (p,r,profile)=self.routed(requirement)?;let mut resolution=p.resolve(&r,&self.resolve_context(profile.clone())).await?;if let Some(profile)=profile{resolution.metadata.insert("artifactum_profile".into(),serde_json::Value::String(profile.name));}Ok(resolution)}
    pub async fn resolve_ref(&self,reference:&str)->Result<Resolution>{self.resolve(&ArtifactRequirement::new(reference.parse()?)).await}
    pub async fn resolved(&self,requirement:&ArtifactRequirement)->Result<ResolvedArtifact>{Ok(ResolvedArtifact{resolver:self.clone(),resolution:self.resolve(requirement).await?})}
    pub async fn get_resolved(&self,reference:&str)->Result<ResolvedArtifact>{self.resolved(&ArtifactRequirement::new(reference.parse()?)).await}
    pub async fn fetch(&self,requirement:&ArtifactRequirement)->Result<FetchedArtifact>{let r=self.resolve(requirement).await?;self.fetch_resolution(r).await}
    pub async fn get(&self,reference:&str)->Result<FetchedArtifact>{self.fetch(&ArtifactRequirement::new(reference.parse()?)).await}

    pub async fn ensure_file(&self,resolution:&Resolution,path:&str)->Result<StoredFile>{
        let file=resolution.files.iter().find(|f|f.path.as_str()==path).cloned().ok_or_else(||Error::UnknownFile{artifact:resolution.canonical_ref.clone(),path:path.into()})?;
        self.ensure_one(resolution,&file).await
    }

    pub async fn fetch_selected(&self,resolution:Resolution,patterns:&[String])->Result<PartialFetch>{
        let matcher=Selection{include:patterns.to_vec(),exclude:Vec::new()}.compile()?;
        let selected=resolution.files.iter().filter(|f|patterns.is_empty()||matcher.matches(f.path.as_str())).cloned().collect::<Vec<_>>();
        let provider=self.providers.get_by_name(&resolution.provider).ok_or_else(||Error::UnknownScheme(resolution.provider.clone()))?;
        let profile=self.profile_for_resolution(&resolution);
        let this=self.clone();
        let mut files=stream::iter(selected.into_iter().map(|file|{let this=this.clone();let provider=Arc::clone(&provider);let resolution=resolution.clone();let profile=profile.clone();async move {this.ensure_one_with(provider,&resolution,&file,profile).await}})).buffer_unordered(self.max_concurrent_files).try_collect::<Vec<_>>().await?;
        files.sort_by(|a,b|a.path.as_str().cmp(b.path.as_str()));
        let complete=files.len()==resolution.files.len();
        let manifest=if complete {Some(self.commit_artifact(&resolution,files.clone()).await?)} else {None};
        Ok(PartialFetch{resolution,files,manifest})
    }

    pub async fn fetch_resolution(&self,resolution:Resolution)->Result<FetchedArtifact>{
        let partial=self.fetch_selected(resolution,&[]).await?;
        let manifest=partial.manifest.expect("all files selected must produce a manifest");
        Ok(FetchedArtifact{resolution:partial.resolution,manifest})
    }

    async fn ensure_one(&self,resolution:&Resolution,file:&ResolvedFile)->Result<StoredFile>{
        let provider=self.providers.get_by_name(&resolution.provider).ok_or_else(||Error::UnknownScheme(resolution.provider.clone()))?;
        self.ensure_one_with(provider,resolution,file,self.profile_for_resolution(resolution)).await
    }
    fn profile_for_resolution(&self,resolution:&Resolution)->Option<ProviderProfile>{
        resolution.metadata.get("artifactum_profile").and_then(|v|v.as_str()).and_then(|name|self.profiles.get(&name.to_ascii_lowercase())).cloned()
    }
    async fn ensure_one_with(&self,provider:DynProvider,_resolution:&Resolution,file:&ResolvedFile,profile:Option<ProviderProfile>)->Result<StoredFile>{
        let expected=file.digests.sha256().map(|v|Digest::sha256(v.to_owned())).transpose()?;
        if let Some(expected)=expected.as_ref(){
            if self.store.contains_blob(expected).await? && self.store.verify_blob(expected).await? {
                let size=fs::metadata(self.store.blob_path(expected)?).await?.len();
                return Ok(StoredFile{path:file.path.clone(),digest:expected.clone(),size,media_type:file.media_type.clone()});
            }
        }
        if self.offline {return Err(artifactum_core::provider_error(provider.descriptor().name,format!("artifact `{}` is not cached and resolver is offline",file.path)).into());}
        let staging=self.store.staging_path().await?;
        let context=self.acquire_context(profile);
        let plan=provider.prepare_acquisition(file,&context).await?;
        let transfer=match &plan {
            AcquisitionPlan::Http(http)=>self.http.execute(http,&staging).await.map(|n|artifactum_core::Acquisition{bytes_written:Some(n),metadata:BTreeMap::new()}).map_err(Error::from),
            AcquisitionPlan::LocalCopy{path}=>fs::copy(path,&staging).await.map(|n|artifactum_core::Acquisition{bytes_written:Some(n),metadata:BTreeMap::new()}).map_err(Error::from),
            AcquisitionPlan::ObjectStore(_) | AcquisitionPlan::Git(_) | AcquisitionPlan::Oci(_) | AcquisitionPlan::ProviderManaged{..}=>provider.acquire_managed(file,&plan,&staging,&context).await.map_err(Error::from),
        };
        if let Err(error)=transfer{let _=fs::remove_file(&staging).await;return Err(error);}
        let (digest,size)=self.store.commit_staging(staging,expected.as_ref()).await?;
        Ok(StoredFile{path:file.path.clone(),digest,size,media_type:file.media_type.clone()})
    }
    async fn commit_artifact(&self,resolution:&Resolution,files:Vec<StoredFile>)->Result<StoredManifest>{
        self.store.store_manifest(&StoredArtifact{provider:resolution.provider.clone(),canonical_ref:resolution.canonical_ref.clone(),revision:resolution.revision.as_ref().map(|r|r.id.clone()),files,provider_state:resolution.provider_state.clone(),metadata:resolution.metadata.clone()}).await.map_err(Into::into)
    }
    pub async fn materialize(&self,fetched:&FetchedArtifact,destination:impl AsRef<Path>,mode:MaterializationMode)->Result<()>{self.store.materialize(&fetched.manifest.artifact,destination,mode).await?;Ok(())}
    pub async fn materialize_partial(&self,partial:&PartialFetch,destination:impl AsRef<Path>,mode:MaterializationMode)->Result<()>{self.store.materialize_files(&partial.files,destination,mode).await?;Ok(())}

    /// Commit a complete artifact manifest when every resolved file already has
    /// a verified SHA-256 blob in the CAS. Useful after several lazy fetches.
    pub async fn finalize_cached(&self,resolution:&Resolution)->Result<Option<StoredManifest>>{
        let mut files=Vec::with_capacity(resolution.files.len());
        for file in &resolution.files {
            let Some(value)=file.digests.sha256() else{return Ok(None)};
            let digest=Digest::sha256(value.to_owned())?;
            if !self.store.contains_blob(&digest).await? || !self.store.verify_blob(&digest).await? {return Ok(None)}
            let size=fs::metadata(self.store.blob_path(&digest)?).await?.len();
            files.push(StoredFile{path:file.path.clone(),digest,size,media_type:file.media_type.clone()});
        }
        Ok(Some(self.commit_artifact(resolution,files).await?))
    }

    pub async fn search(&self,scheme:&str,request:&SearchRequest)->Result<SearchPage>{
        let reference=ArtifactRef::new(scheme,"_")?; let requirement=ArtifactRequirement::new(reference); let (provider,_,profile)=self.routed(&requirement)?;
        Ok(provider.search(request,&self.resolve_context(profile)).await?)
    }
    pub async fn inspect(&self,reference:&ArtifactRef)->Result<InspectResult>{
        let requirement=ArtifactRequirement::new(reference.clone());let(provider,routed,profile)=self.routed(&requirement)?;
        Ok(provider.inspect(&InspectRequest{reference:routed.reference,metadata:BTreeMap::new()},&self.resolve_context(profile)).await?)
    }
    pub async fn list_versions(&self,reference:&ArtifactRef,cursor:Option<&str>)->Result<VersionPage>{
        let requirement=ArtifactRequirement::new(reference.clone());let(provider,routed,profile)=self.routed(&requirement)?;
        Ok(provider.list_versions(&routed.reference,cursor,&self.resolve_context(profile)).await?)
    }
    pub async fn list_files(&self,requirement:&ArtifactRequirement,cursor:Option<&str>)->Result<FilePage>{
        let(provider,routed,profile)=self.routed(requirement)?;Ok(provider.list_files(&routed,cursor,&self.resolve_context(profile)).await?)
    }
}

#[derive(Clone,Debug,Serialize,Deserialize)] pub struct FetchedArtifact{pub resolution:Resolution,pub manifest:StoredManifest}
#[derive(Clone,Debug,Serialize,Deserialize)] pub struct PartialFetch{pub resolution:Resolution,pub files:Vec<StoredFile>,#[serde(default,skip_serializing_if="Option::is_none")]pub manifest:Option<StoredManifest>}

#[derive(Clone)]
pub struct ResolvedArtifact{resolver:ArtifactResolver,pub resolution:Resolution}
impl ResolvedArtifact{
    pub async fn ensure_file(&self,path:&str)->Result<StoredFile>{self.resolver.ensure_file(&self.resolution,path).await}
    pub async fn ensure_all(&self)->Result<FetchedArtifact>{self.resolver.fetch_resolution(self.resolution.clone()).await}
    pub async fn ensure_matching(&self,patterns:&[String])->Result<PartialFetch>{self.resolver.fetch_selected(self.resolution.clone(),patterns).await}
}

#[derive(Clone,Debug,Serialize,Deserialize)]
pub struct ProjectManifest{#[serde(default="project_version")]pub version:u32,#[serde(default)]pub providers:BTreeMap<String,ProjectProvider>,#[serde(default)]pub artifacts:BTreeMap<String,ProjectArtifact>}
const fn project_version()->u32{2}
impl Default for ProjectManifest{fn default()->Self{Self{version:project_version(),providers:BTreeMap::new(),artifacts:BTreeMap::new()}}}
impl ProjectManifest{
    pub async fn load(path:impl AsRef<Path>)->Result<Self>{let path=path.as_ref();if !fs::try_exists(path).await?{return Ok(Self::default());}let m:Self=toml::from_str(&fs::read_to_string(path).await?)?;if m.version!=project_version(){return Err(Error::UnsupportedProjectVersion(m.version));}Ok(m)}
    pub async fn save(&self,path:impl AsRef<Path>)->Result<()>{fs::write(path,toml::to_string_pretty(self)?).await?;Ok(())}
    pub fn requirement(&self,name:&str)->Result<ArtifactRequirement>{self.artifacts.get(name).ok_or_else(||Error::UnknownArtifact(name.into()))?.requirement()}
    pub fn profiles(&self)->Vec<ProviderProfile>{self.providers.iter().map(|(name,p)|ProviderProfile{name:name.clone(),provider:p.kind.clone(),config:p.config.clone()}).collect()}
}
#[derive(Clone,Debug,Serialize,Deserialize)] pub struct ProjectProvider{pub kind:String,#[serde(default)]pub config:BTreeMap<String,String>}
#[derive(Clone,Debug,Serialize,Deserialize)] pub struct ProjectArtifact{pub source:String,#[serde(default,skip_serializing_if="Option::is_none")]pub revision:Option<String>,#[serde(default,skip_serializing_if="Vec::is_empty")]pub include:Vec<String>,#[serde(default,skip_serializing_if="Vec::is_empty")]pub exclude:Vec<String>,#[serde(default,skip_serializing_if="Option::is_none")]pub materialize:Option<PathBuf>}
impl ProjectArtifact{pub fn requirement(&self)->Result<ArtifactRequirement>{Ok(ArtifactRequirement{reference:self.source.parse()?,revision:self.revision.clone(),selection:Selection{include:self.include.clone(),exclude:self.exclude.clone()},metadata:BTreeMap::new()})}}

#[derive(Clone,Debug,Serialize,Deserialize)] pub struct Lockfile{#[serde(default="lock_version")]pub version:u32,#[serde(default,rename="artifact")]pub artifacts:Vec<LockedArtifact>}
const fn lock_version()->u32{2}
impl Default for Lockfile{fn default()->Self{Self{version:lock_version(),artifacts:Vec::new()}}}
impl Lockfile{
    pub async fn load(path:impl AsRef<Path>)->Result<Self>{let p=path.as_ref();if !fs::try_exists(p).await?{return Ok(Self::default());}let l:Self=toml::from_str(&fs::read_to_string(p).await?)?;if l.version!=lock_version(){return Err(Error::UnsupportedLockVersion(l.version));}Ok(l)}
    pub async fn save(&self,path:impl AsRef<Path>)->Result<()>{fs::write(path,toml::to_string_pretty(self)?).await?;Ok(())}
    pub fn upsert(&mut self,a:LockedArtifact){if let Some(e)=self.artifacts.iter_mut().find(|e|e.name==a.name){*e=a}else{self.artifacts.push(a);self.artifacts.sort_by(|a,b|a.name.cmp(&b.name));}}
    #[must_use] pub fn get(&self,name:&str)->Option<&LockedArtifact>{self.artifacts.iter().find(|a|a.name==name)}
    pub fn remove(&mut self,name:&str){self.artifacts.retain(|a|a.name!=name);}
}

#[derive(Clone,Debug,Serialize,Deserialize)]
pub struct LockedArtifact{
    pub name:String,pub provider:String,pub reference:String,#[serde(default,skip_serializing_if="Option::is_none")]pub revision:Option<String>,
    #[serde(default,skip_serializing_if="Option::is_none")]pub profile:Option<String>,
    #[serde(default,skip_serializing_if="Option::is_none")]pub manifest:Option<String>,pub requirement_digest:String,
    #[serde(default,skip_serializing_if="Option::is_none")]pub provider_state_json:Option<String>,#[serde(default)]pub files:Vec<LockedFile>,
}
impl LockedArtifact{
    pub fn from_partial(name:impl Into<String>,requirement:&ArtifactRequirement,partial:&PartialFetch,previous:Option<&LockedArtifact>)->Result<Self>{
        let name=name.into(); let fetched=partial.files.iter().map(|f|(f.path.to_string(),f)).collect::<HashMap<_,_>>();
        let current_revision=partial.resolution.revision.as_ref().map(|r|r.id.as_str());
        let current_profile=partial.resolution.metadata.get("artifactum_profile").and_then(|value|value.as_str());
        let same_resolution=previous.is_some_and(|p|p.provider==partial.resolution.provider&&p.reference==partial.resolution.canonical_ref&&p.revision.as_deref()==current_revision&&p.profile.as_deref()==current_profile);
        let previous_files=previous.filter(|_|same_resolution).map(|p|p.files.iter().map(|f|(f.path.clone(),f)).collect::<HashMap<_,_>>()).unwrap_or_default();
        let files=partial.resolution.files.iter().map(|r|{
            let f=fetched.get(r.path.as_str()); let old=previous_files.get(r.path.as_str());
            LockedFile{path:r.path.to_string(),digest:f.map(|f|f.digest.to_string()).or_else(||old.and_then(|x|x.digest.clone())),size:f.map(|f|f.size).or(r.size).or_else(||old.and_then(|x|x.size)),media_type:r.media_type.clone(),source_json:(!r.source.is_null()).then(||r.source.to_string())}
        }).collect();
        let profile=partial.resolution.metadata.get("artifactum_profile").and_then(|value|value.as_str()).map(str::to_owned);
        Ok(Self{name,provider:partial.resolution.provider.clone(),reference:partial.resolution.canonical_ref.clone(),revision:partial.resolution.revision.as_ref().map(|r|r.id.clone()),profile,manifest:partial.manifest.as_ref().map(|m|m.digest.to_string()),requirement_digest:requirement_digest(requirement)?,provider_state_json:(!partial.resolution.provider_state.is_null()).then(||partial.resolution.provider_state.to_string()),files})
    }
    pub fn from_fetched(name:impl Into<String>,requirement:&ArtifactRequirement,fetched:&FetchedArtifact)->Result<Self>{Self::from_partial(name,requirement,&PartialFetch{resolution:fetched.resolution.clone(),files:fetched.manifest.artifact.files.clone(),manifest:Some(fetched.manifest.clone())},None)}
    pub fn matches_requirement(&self,r:&ArtifactRequirement)->Result<bool>{Ok(self.requirement_digest==requirement_digest(r)?)}
    pub fn to_resolution(&self)->Result<Resolution>{
        let files=self.files.iter().map(|f|->Result<ResolvedFile>{let mut digests=artifactum_core::DigestSet::default();if let Some(d)=&f.digest{digests.insert(d.parse()?);}Ok(ResolvedFile{path:f.path.parse()?,size:f.size,digests,media_type:f.media_type.clone(),source:f.source_json.as_ref().map(|s|serde_json::from_str(s)).transpose()?.unwrap_or(serde_json::Value::Null)})}).collect::<Result<Vec<_>>>()?;
        let mut metadata=BTreeMap::new();
        if let Some(profile)=&self.profile{metadata.insert("artifactum_profile".into(),serde_json::Value::String(profile.clone()));}
        Ok(Resolution{provider:self.provider.clone(),canonical_ref:self.reference.clone(),revision:self.revision.as_ref().map(|id|artifactum_core::ResolvedRevision{id:id.clone(),requested:Some(id.clone())}),files,provider_state:self.provider_state_json.as_ref().map(|s|serde_json::from_str(s)).transpose()?.unwrap_or(serde_json::Value::Null),metadata})
    }
}
fn requirement_digest(r:&ArtifactRequirement)->Result<String>{Ok(artifactum_store::sha256_bytes(&serde_json::to_vec(r)?)?.to_string())}
#[derive(Clone,Debug,Serialize,Deserialize)] pub struct LockedFile{pub path:String,#[serde(default,skip_serializing_if="Option::is_none")]pub digest:Option<String>,#[serde(default,skip_serializing_if="Option::is_none")]pub size:Option<u64>,#[serde(default,skip_serializing_if="Option::is_none")]pub media_type:Option<String>,#[serde(default,skip_serializing_if="Option::is_none")]pub source_json:Option<String>}


#[cfg(test)]
mod tests {
    use super::*;
    use artifactum_core::{ArtifactPath, ResolvedRevision};

    fn partial(profile: &str, revision: &str, digest: Option<&str>) -> PartialFetch {
        let mut metadata = BTreeMap::new();
        metadata.insert("artifactum_profile".into(), serde_json::Value::String(profile.into()));
        let path = ArtifactPath::new("model.bin").unwrap();
        let resolved_file = ResolvedFile {
            path: path.clone(),
            size: Some(4),
            digests: Default::default(),
            media_type: None,
            source: serde_json::json!({"opaque": "state"}),
        };
        let stored = digest.map(|digest| StoredFile {
            path,
            digest: digest.parse().unwrap(),
            size: 4,
            media_type: None,
        });
        PartialFetch {
            resolution: Resolution {
                provider: "s3".into(),
                canonical_ref: "s3:bucket/model.bin".into(),
                revision: Some(ResolvedRevision { id: revision.into(), requested: Some("latest".into()) }),
                files: vec![resolved_file],
                provider_state: serde_json::json!({"version_id": revision}),
                metadata,
            },
            files: stored.into_iter().collect(),
            manifest: None,
        }
    }

    #[test]
    fn lock_roundtrips_provider_profile_and_opaque_state() {
        let requirement = ArtifactRequirement::new("lab:model.bin".parse().unwrap());
        let lock = LockedArtifact::from_partial(
            "model",
            &requirement,
            &partial("lab", "v1", None),
            None,
        )
        .unwrap();
        assert_eq!(lock.profile.as_deref(), Some("lab"));
        let resolution = lock.to_resolution().unwrap();
        assert_eq!(
            resolution.metadata.get("artifactum_profile").and_then(|v| v.as_str()),
            Some("lab")
        );
        assert_eq!(resolution.provider_state["version_id"], "v1");
        assert_eq!(resolution.files[0].source["opaque"], "state");
    }

    #[test]
    fn prior_digest_is_not_reused_when_revision_changes() {
        let requirement = ArtifactRequirement::new("lab:model.bin".parse().unwrap());
        let sha = format!("sha256:{}", "11".repeat(32));
        let old = LockedArtifact::from_partial(
            "model",
            &requirement,
            &partial("lab", "v1", Some(&sha)),
            None,
        )
        .unwrap();
        let new = LockedArtifact::from_partial(
            "model",
            &requirement,
            &partial("lab", "v2", None),
            Some(&old),
        )
        .unwrap();
        assert!(new.files[0].digest.is_none());
    }

    #[test]
    fn prior_digest_is_reused_for_same_partial_resolution() {
        let requirement = ArtifactRequirement::new("lab:model.bin".parse().unwrap());
        let sha = format!("sha256:{}", "22".repeat(32));
        let old = LockedArtifact::from_partial(
            "model",
            &requirement,
            &partial("lab", "v1", Some(&sha)),
            None,
        )
        .unwrap();
        let merged = LockedArtifact::from_partial(
            "model",
            &requirement,
            &partial("lab", "v1", None),
            Some(&old),
        )
        .unwrap();
        assert_eq!(merged.files[0].digest.as_deref(), Some(sha.as_str()));
    }
}
