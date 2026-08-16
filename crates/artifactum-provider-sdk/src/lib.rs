//! Adapter between `ArtifactProvider` and the generic executable plugin protocol.

use std::{path::{Path,PathBuf}};
use artifactum_plugin_host::{DaemonPluginSession,Error as HostError};
use artifactum_plugin_protocol::{Frame,failure_data,read_frame,request,success,write_frame};
use artifactum_resolver::{AcquireContext,AcquisitionPlan,ArtifactProvider,ArtifactRequirement,ProviderDescriptor,ResolveContext,Resolution,ResolvedFile,Result as ProviderResult,Error as ProviderError,SearchRequest,SearchPage,InspectResult,VersionPage,FilePage,ArtifactRef};
use async_trait::async_trait;
use serde_json::Value;
use tokio::io::{BufReader,BufWriter};

pub async fn serve_provider<P:ArtifactProvider>(provider:P)->std::result::Result<(),String> {
    let mut input=BufReader::new(tokio::io::stdin());let mut output=BufWriter::new(tokio::io::stdout());
    loop {
        let frame=match read_frame(&mut input).await{Ok(f)=>f,Err(artifactum_plugin_protocol::Error::Eof)=>break,Err(e)=>return Err(e.to_string())};
        let Frame::Request(req)=frame else{continue};
        let result:ProviderResult<Value>=match req.method.as_str(){
            "descriptor"=>to_value(provider.descriptor()),
            "resolve"=>match parse::<(ArtifactRequirement,ResolveContextWire)>(req.params){Ok((r,c))=>provider.resolve(&r,&c.into()).await.and_then(to_value),Err(e)=>Err(e)},
            "prepare_acquisition"=>match parse::<(ResolvedFile,AcquireContextWire)>(req.params){Ok((f,c))=>provider.prepare_acquisition(&f,&c.into()).await.and_then(to_value),Err(e)=>Err(e)},
            "acquire_managed"=>match parse::<(ResolvedFile,AcquisitionPlan,PathBuf,AcquireContextWire)>(req.params){Ok((f,p,d,c))=>provider.acquire_managed(&f,&p,&d,&c.into()).await.and_then(to_value),Err(e)=>Err(e)},
            "search"=>match parse::<(SearchRequest,ResolveContextWire)>(req.params){Ok((r,c))=>provider.search(&r,&c.into()).await.and_then(to_value),Err(e)=>Err(e)},
            "inspect"=>match parse::<(ArtifactRef,ResolveContextWire)>(req.params){Ok((r,c))=>provider.inspect(&r,&c.into()).await.and_then(to_value),Err(e)=>Err(e)},
            "versions"=>match parse::<(ArtifactRef,Option<String>,ResolveContextWire)>(req.params){Ok((r,cursor,c))=>provider.list_versions(&r,cursor.as_deref(),&c.into()).await.and_then(to_value),Err(e)=>Err(e)},
            "files"=>match parse::<(ArtifactRequirement,Option<String>,ResolveContextWire)>(req.params){Ok((r,cursor,c))=>provider.list_files(&r,cursor.as_deref(),&c.into()).await.and_then(to_value),Err(e)=>Err(e)},
            _=>Err(ProviderError::Provider{provider:provider.descriptor().name,message:format!("unsupported provider method {}",req.method)}),
        };
        let response=match result {
            Ok(v)=>success(req.id,v),
            Err(ProviderError::AccessRequired(challenge))=>failure_data(req.id,challenge.message.clone(),serde_json::json!({"kind":"access_challenge","challenge":challenge})),
            Err(e)=>artifactum_plugin_protocol::failure(req.id,e.to_string()),
        };
        write_frame(&mut output,&Frame::Response(response)).await.map_err(|e|e.to_string())?;
    }
    Ok(())
}
fn parse<T:serde::de::DeserializeOwned>(v:Value)->ProviderResult<T>{serde_json::from_value(v).map_err(|e|ProviderError::Provider{provider:"plugin-protocol".into(),message:e.to_string()})}
fn to_value<T:serde::Serialize>(v:T)->ProviderResult<Value>{serde_json::to_value(v).map_err(|e|ProviderError::Provider{provider:"plugin-protocol".into(),message:e.to_string()})}

#[derive(Clone,serde::Serialize,serde::Deserialize)]struct ResolveContextWire{offline:bool,profile:Option<artifactum_resolver::ProviderProfile>}
impl From<ResolveContextWire> for ResolveContext{fn from(v:ResolveContextWire)->Self{Self{offline:v.offline,profile:v.profile}}}
impl From<&ResolveContext> for ResolveContextWire{fn from(v:&ResolveContext)->Self{Self{offline:v.offline,profile:v.profile.clone()}}}
#[derive(Clone,serde::Serialize,serde::Deserialize)]struct AcquireContextWire{offline:bool,request_id:uuid::Uuid,profile:Option<artifactum_resolver::ProviderProfile>}
impl From<AcquireContextWire> for AcquireContext{fn from(v:AcquireContextWire)->Self{Self{offline:v.offline,request_id:v.request_id,profile:v.profile}}}
impl From<&AcquireContext> for AcquireContextWire{fn from(v:&AcquireContext)->Self{Self{offline:v.offline,request_id:v.request_id,profile:v.profile.clone()}}}

#[derive(Clone)]pub struct PluginProvider{path:PathBuf,session:DaemonPluginSession,descriptor:ProviderDescriptor}
impl PluginProvider{pub async fn connect(path:impl Into<PathBuf>)->ProviderResult<Self>{let path=path.into();let session=DaemonPluginSession::new(path.clone()).map_err(|e|host_error(&path,e))?;let value=session.call(request("descriptor",Value::Null)).await.map_err(|e|host_error(&path,e))?;let descriptor=serde_json::from_value(value).map_err(|e|ProviderError::Provider{provider:path.display().to_string(),message:e.to_string()})?;Ok(Self{path,session,descriptor})}#[must_use]pub fn path(&self)->&Path{&self.path}}
#[async_trait]impl ArtifactProvider for PluginProvider{fn descriptor(&self)->ProviderDescriptor{self.descriptor.clone()}async fn resolve(&self,r:&ArtifactRequirement,c:&ResolveContext)->ProviderResult<Resolution>{call(&self.session,"resolve",&(r,ResolveContextWire::from(c)),&self.descriptor.name).await}async fn prepare_acquisition(&self,f:&ResolvedFile,c:&AcquireContext)->ProviderResult<AcquisitionPlan>{call(&self.session,"prepare_acquisition",&(f,AcquireContextWire::from(c)),&self.descriptor.name).await}async fn acquire_managed(&self,f:&ResolvedFile,p:&AcquisitionPlan,d:&Path,c:&AcquireContext)->ProviderResult<u64>{call(&self.session,"acquire_managed",&(f,p,d.to_path_buf(),AcquireContextWire::from(c)),&self.descriptor.name).await}async fn search(&self,r:&SearchRequest,c:&ResolveContext)->ProviderResult<SearchPage>{call(&self.session,"search",&(r,ResolveContextWire::from(c)),&self.descriptor.name).await}async fn inspect(&self,r:&ArtifactRef,c:&ResolveContext)->ProviderResult<InspectResult>{call(&self.session,"inspect",&(r,ResolveContextWire::from(c)),&self.descriptor.name).await}async fn list_versions(&self,r:&ArtifactRef,cursor:Option<&str>,c:&ResolveContext)->ProviderResult<VersionPage>{call(&self.session,"versions",&(r,cursor.map(str::to_owned),ResolveContextWire::from(c)),&self.descriptor.name).await}async fn list_files(&self,r:&ArtifactRequirement,cursor:Option<&str>,c:&ResolveContext)->ProviderResult<FilePage>{call(&self.session,"files",&(r,cursor.map(str::to_owned),ResolveContextWire::from(c)),&self.descriptor.name).await}}
async fn call<T:serde::Serialize,R:serde::de::DeserializeOwned>(s:&DaemonPluginSession,method:&str,p:&T,provider:&str)->ProviderResult<R>{let params=serde_json::to_value(p).map_err(|e|ProviderError::Provider{provider:provider.into(),message:e.to_string()})?;let v=s.call(request(method,params)).await.map_err(|e|match e{HostError::Remote{message,data}=>{if let Some(challenge)=data.as_ref().and_then(|v|v.get("challenge")).and_then(|v|serde_json::from_value(v.clone()).ok()){ProviderError::AccessRequired(challenge)}else{ProviderError::Provider{provider:provider.into(),message}}},other=>ProviderError::Provider{provider:provider.into(),message:other.to_string()}})?;serde_json::from_value(v).map_err(|e|ProviderError::Provider{provider:provider.into(),message:e.to_string()})}
fn host_error(path:&Path,e:HostError)->ProviderError{match e{HostError::Remote{message,data}=>{if let Some(challenge)=data.as_ref().and_then(|v|v.get("challenge")).and_then(|v|serde_json::from_value(v.clone()).ok()){ProviderError::AccessRequired(challenge)}else{ProviderError::Provider{provider:path.display().to_string(),message}}},other=>ProviderError::Provider{provider:path.display().to_string(),message:other.to_string()}}}
