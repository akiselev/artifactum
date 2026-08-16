//! Stable domain types and provider API for Artifactum.
//!
//! Providers resolve semantic references into immutable-ish manifests and
//! prepare acquisition plans. The host owns generic transfers and CAS identity;
//! providers only perform byte transfer when a service requires a native or
//! proprietary transport.

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
pub type ConfigMap = BTreeMap<String, String>;

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
    #[error("invalid artifact reference `{0}`; expected <scheme>:<locator>")]
    InvalidReference(String),
    #[error("invalid artifact path `{0}`")]
    InvalidArtifactPath(String),
    #[error("invalid digest `{0}`")]
    InvalidDigest(String),
    #[error("invalid selection glob `{pattern}`: {message}")]
    InvalidGlob { pattern: String, message: String },
    #[error("provider `{provider}` does not support operation `{operation}`")]
    Unsupported { provider: String, operation: &'static str },
    #[error("provider `{provider}`: {message}")]
    Provider { provider: String, message: String },
    #[error("access required by provider `{}`: {}", .0.provider, .0.message)]
    AccessRequired(AccessChallenge),
    #[error("I/O error: {0}")]
    Io(#[from] std::io::Error),
    #[error("serialization error: {0}")]
    Serialization(#[from] serde_json::Error),
}

pub type Result<T, E = Error> = std::result::Result<T, E>;

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct ArtifactRef {
    scheme: String,
    locator: String,
}

impl ArtifactRef {
    pub fn new(scheme: impl Into<String>, locator: impl Into<String>) -> Result<Self> {
        let scheme = scheme.into();
        let locator = locator.into();
        if scheme.is_empty() || locator.is_empty() || !scheme.bytes().all(|b| b.is_ascii_alphanumeric() || matches!(b, b'+' | b'-' | b'.' | b'_')) {
            return Err(Error::InvalidReference(format!("{scheme}:{locator}")));
        }
        Ok(Self { scheme, locator })
    }
    #[must_use] pub fn scheme(&self) -> &str { &self.scheme }
    #[must_use] pub fn locator(&self) -> &str { &self.locator }
    #[must_use] pub fn with_scheme(&self, scheme: impl Into<String>) -> Result<Self> { Self::new(scheme, self.locator.clone()) }
}
impl FromStr for ArtifactRef {
    type Err = Error;
    fn from_str(value: &str) -> Result<Self> {
        let (scheme, locator) = value.split_once(':').ok_or_else(|| Error::InvalidReference(value.to_owned()))?;
        Self::new(scheme.to_ascii_lowercase(), locator)
    }
}
impl fmt::Display for ArtifactRef {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result { write!(f, "{}:{}", self.scheme, self.locator) }
}

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub struct ArtifactPath(String);
impl ArtifactPath {
    pub fn new(path: impl AsRef<str>) -> Result<Self> {
        let raw = path.as_ref().replace('\\', "/");
        let path = Path::new(&raw);
        if raw.is_empty() || path.is_absolute() || path.components().any(|part| matches!(part, Component::ParentDir | Component::RootDir | Component::Prefix(_))) {
            return Err(Error::InvalidArtifactPath(raw));
        }
        let normalized = path.components().filter_map(|component| match component {
            Component::Normal(value) => Some(value.to_string_lossy().into_owned()),
            Component::CurDir => None,
            _ => None,
        }).collect::<Vec<_>>().join("/");
        if normalized.is_empty() { return Err(Error::InvalidArtifactPath(raw)); }
        Ok(Self(normalized))
    }
    #[must_use] pub fn as_str(&self) -> &str { &self.0 }
    #[must_use] pub fn to_path_buf(&self) -> PathBuf { self.0.split('/').collect() }
}
impl fmt::Display for ArtifactPath { fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result { f.write_str(&self.0) } }
impl FromStr for ArtifactPath { type Err = Error; fn from_str(value: &str) -> Result<Self> { Self::new(value) } }

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct Digest { pub algorithm: String, pub value: String }
impl Digest {
    pub fn sha256(hex: impl Into<String>) -> Result<Self> {
        let value = hex.into().to_ascii_lowercase();
        if value.len() != 64 || !value.bytes().all(|b| b.is_ascii_hexdigit()) { return Err(Error::InvalidDigest(format!("sha256:{value}"))); }
        Ok(Self { algorithm: "sha256".into(), value })
    }
    #[must_use] pub fn as_qualified(&self) -> String { format!("{}:{}", self.algorithm, self.value) }
}
impl fmt::Display for Digest { fn fmt(&self, f:&mut fmt::Formatter<'_>)->fmt::Result { write!(f,"{}:{}",self.algorithm,self.value) } }
impl FromStr for Digest { type Err=Error; fn from_str(value:&str)->Result<Self>{ let (algorithm,digest)=value.split_once(':').ok_or_else(||Error::InvalidDigest(value.into()))?; match algorithm {"sha256"=>Self::sha256(digest), _=>Err(Error::InvalidDigest(value.into()))}}}

#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct DigestSet(pub BTreeMap<String,String>);
impl DigestSet { #[must_use] pub fn sha256(&self)->Option<&str>{self.0.get("sha256").map(String::as_str)} pub fn insert(&mut self,digest:Digest){self.0.insert(digest.algorithm,digest.value);} }

#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct Selection { #[serde(default, skip_serializing_if="Vec::is_empty")] pub include:Vec<String>, #[serde(default, skip_serializing_if="Vec::is_empty")] pub exclude:Vec<String> }
impl Selection {
    #[must_use] pub fn all()->Self{Self::default()}
    pub fn compile(&self)->Result<CompiledSelection>{
        fn build(patterns:&[String])->Result<GlobSet>{ let mut builder=GlobSetBuilder::new(); for pattern in patterns { builder.add(Glob::new(pattern).map_err(|e|Error::InvalidGlob{pattern:pattern.clone(),message:e.to_string()})?); } builder.build().map_err(|e|Error::InvalidGlob{pattern:"<set>".into(),message:e.to_string()}) }
        Ok(CompiledSelection{include_all:self.include.is_empty(),include:build(&self.include)?,exclude:build(&self.exclude)?})
    }
}
pub struct CompiledSelection { include_all:bool, include:GlobSet, exclude:GlobSet }
impl CompiledSelection { #[must_use] pub fn matches(&self,path:&str)->bool{(self.include_all||self.include.is_match(path))&&!self.exclude.is_match(path)} }

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ArtifactRequirement { pub reference:ArtifactRef, #[serde(default,skip_serializing_if="Option::is_none")] pub revision:Option<String>, #[serde(default)] pub selection:Selection, #[serde(default)] pub metadata:Metadata }
impl ArtifactRequirement { #[must_use] pub fn new(reference:ArtifactRef)->Self{Self{reference,revision:None,selection:Selection::default(),metadata:Metadata::default()}} }

#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct ProviderProfile {
    pub name: String,
    pub provider: String,
    #[serde(default)] pub config: ConfigMap,
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct ProviderCapabilities {
    pub resolve:bool, pub acquire:bool, pub search:bool, pub inspect:bool, pub list:bool, pub versions:bool, pub push:bool, pub auth:bool, pub range:bool,
}
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ProviderDescriptor { pub name:String, pub version:String, pub schemes:Vec<String>, pub capabilities:ProviderCapabilities, #[serde(default)] pub metadata:Metadata }

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ResolvedRevision { pub id:String, #[serde(default,skip_serializing_if="Option::is_none")] pub requested:Option<String> }
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ResolvedFile {
    pub path:ArtifactPath,
    #[serde(default,skip_serializing_if="Option::is_none")] pub size:Option<u64>,
    #[serde(default)] pub digests:DigestSet,
    #[serde(default,skip_serializing_if="Option::is_none")] pub media_type:Option<String>,
    #[serde(default)] pub source:Value,
}
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Resolution { pub provider:String, pub canonical_ref:String, #[serde(default,skip_serializing_if="Option::is_none")] pub revision:Option<ResolvedRevision>, pub files:Vec<ResolvedFile>, #[serde(default)] pub provider_state:Value, #[serde(default)] pub metadata:Metadata }

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct HttpAcquisition {
    pub url:String,
    #[serde(default)] pub headers:BTreeMap<String,String>,
    #[serde(default)] pub resume:bool,
}
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ObjectStoreAcquisition { pub scheme:String, pub path:String, #[serde(default,skip_serializing_if="Option::is_none")] pub version:Option<String>, #[serde(default)] pub config:ConfigMap }
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct GitAcquisition { pub repository:String, pub revision:String, pub path:String, #[serde(default)] pub lfs:bool }
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct OciAcquisition { pub reference:String, pub digest:String, #[serde(default,skip_serializing_if="Option::is_none")] pub media_type:Option<String> }

/// A provider's proposed way to obtain a resolved file. Generic variants are
/// executed by the host; `ProviderManaged` invokes the provider's native path.
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(tag="kind", rename_all="snake_case")]
pub enum AcquisitionPlan {
    Http(HttpAcquisition),
    LocalCopy { path:PathBuf },
    ObjectStore(ObjectStoreAcquisition),
    Git(GitAcquisition),
    Oci(OciAcquisition),
    ProviderManaged { #[serde(default)] state:Value },
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Acquisition { #[serde(default,skip_serializing_if="Option::is_none")] pub bytes_written:Option<u64>, #[serde(default)] pub metadata:Metadata }

#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct ResolveContext { pub offline:bool, #[serde(default)] pub environment:BTreeMap<String,String>, #[serde(default,skip_serializing_if="Option::is_none")] pub profile:Option<ProviderProfile> }
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct AcquireContext { pub offline:bool, pub request_id:Uuid, #[serde(default)] pub environment:BTreeMap<String,String>, #[serde(default,skip_serializing_if="Option::is_none")] pub profile:Option<ProviderProfile> }
impl Default for AcquireContext { fn default()->Self{Self{offline:false,request_id:Uuid::new_v4(),environment:BTreeMap::new(),profile:None}} }

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct SearchRequest { pub query:String, #[serde(default,skip_serializing_if="Option::is_none")] pub limit:Option<usize>, #[serde(default,skip_serializing_if="Option::is_none")] pub cursor:Option<String>, #[serde(default)] pub metadata:Metadata }
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct SearchResult { pub reference:ArtifactRef, pub name:String, #[serde(default,skip_serializing_if="Option::is_none")] pub description:Option<String>, #[serde(default)] pub metadata:Metadata }
#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct SearchPage { #[serde(default)] pub items:Vec<SearchResult>, #[serde(default,skip_serializing_if="Option::is_none")] pub next_cursor:Option<String> }

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct InspectRequest { pub reference:ArtifactRef, #[serde(default)] pub metadata:Metadata }
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct InspectResult { pub reference:ArtifactRef, #[serde(default)] pub metadata:Metadata }
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct VersionInfo { pub id:String, #[serde(default,skip_serializing_if="Option::is_none")] pub name:Option<String>, #[serde(default)] pub metadata:Metadata }
#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct VersionPage { #[serde(default)] pub items:Vec<VersionInfo>, #[serde(default,skip_serializing_if="Option::is_none")] pub next_cursor:Option<String> }
#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct FilePage { #[serde(default)] pub items:Vec<ResolvedFile>, #[serde(default,skip_serializing_if="Option::is_none")] pub next_cursor:Option<String> }

#[async_trait]
pub trait ArtifactProvider:Send+Sync+'static {
    fn descriptor(&self)->ProviderDescriptor;
    async fn resolve(&self, requirement:&ArtifactRequirement, context:&ResolveContext)->Result<Resolution>;

    async fn prepare_acquisition(&self, file:&ResolvedFile, _context:&AcquireContext)->Result<AcquisitionPlan> {
        Ok(AcquisitionPlan::ProviderManaged { state:file.source.clone() })
    }

    async fn acquire_managed(&self, _file:&ResolvedFile, _plan:&AcquisitionPlan, _destination:&Path, _context:&AcquireContext)->Result<Acquisition> {
        Err(Error::Unsupported{provider:self.descriptor().name,operation:"managed acquisition"})
    }

    async fn search(&self,_request:&SearchRequest,_context:&ResolveContext)->Result<SearchPage>{ Err(Error::Unsupported{provider:self.descriptor().name,operation:"search"}) }
    async fn inspect(&self,_request:&InspectRequest,_context:&ResolveContext)->Result<InspectResult>{ Err(Error::Unsupported{provider:self.descriptor().name,operation:"inspect"}) }
    async fn list_versions(&self,_reference:&ArtifactRef,_cursor:Option<&str>,_context:&ResolveContext)->Result<VersionPage>{ Err(Error::Unsupported{provider:self.descriptor().name,operation:"versions"}) }
    async fn list_files(&self,requirement:&ArtifactRequirement,cursor:Option<&str>,context:&ResolveContext)->Result<FilePage>{
        if cursor.is_some(){ return Ok(FilePage::default()); }
        let resolution=self.resolve(requirement,context).await?;
        Ok(FilePage{items:resolution.files,next_cursor:None})
    }
}

pub type DynProvider=Arc<dyn ArtifactProvider>;
pub fn provider_error(provider:impl Into<String>,error:impl fmt::Display)->Error{Error::Provider{provider:provider.into(),message:error.to_string()}}
pub fn access_required(provider:impl Into<String>,requirement:AccessRequirement,message:impl Into<String>,action_url:Option<String>)->Error{Error::AccessRequired(AccessChallenge{provider:provider.into(),requirement,message:message.into(),action_url,tool:None})}
pub fn external_tool_required(provider:impl Into<String>,tool:impl Into<String>,message:impl Into<String>)->Error{ let provider=provider.into(); Error::AccessRequired(AccessChallenge{provider,requirement:AccessRequirement::ExternalTool,message:message.into(),action_url:None,tool:Some(tool.into())}) }

#[cfg(test)] mod tests { use super::*; #[test] fn reference_keeps_locator(){let r:ArtifactRef="hf:dataset:org/name@main".parse().unwrap();assert_eq!(r.locator(),"dataset:org/name@main");} #[test] fn paths_reject_traversal(){assert!(ArtifactPath::new("../secret").is_err());assert!(ArtifactPath::new("/absolute").is_err());} }
