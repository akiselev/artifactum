//! Daemonkit-backed process owner for persistent Artifactum provider sessions.
//!
//! The host daemon is the Artifactum executable itself in daemonkit bootstrap
//! mode. It owns a pool of multiplexed provider subprocess sessions. CLI
//! processes attach over daemonkit-authenticated local streams.

use std::{collections::HashMap, path::{Path,PathBuf}, sync::Arc, time::Duration};
use artifactum_core::{AcquireContext,Acquisition,AcquisitionPlan,ArtifactProvider,ArtifactRef,ArtifactRequirement,Error as CoreError,FilePage,InspectRequest,InspectResult,ProviderDescriptor,ResolveContext,ResolvedFile,Resolution,SearchPage,SearchRequest,VersionPage};
use artifactum_plugin_protocol::{discover_plugin_paths,AcquireManagedParams,FilesParams,InitializeRequest,InitializeResponse,InspectParams,PluginSession,PrepareAcquisitionParams,ResolveParams,SearchParams,VersionsParams,PROTOCOL_MAJOR,PROTOCOL_MINOR};
use async_trait::async_trait;
use daemonkit::{Bootstrap,Daemon,DaemonSpec,Embedded,Spawn};
use futures_util::StreamExt;
use serde::{de::DeserializeOwned,Deserialize,Serialize};
use serde_json::Value;
use thiserror::Error;
use tokio::{io::{AsyncBufReadExt,AsyncReadExt,AsyncWriteExt,BufReader},sync::Mutex};

#[derive(Debug,Error)]pub enum Error{
    #[error("daemonkit error: {0}")]Daemon(#[from]daemonkit::Error),
    #[error("daemonkit configuration error: {0}")]DaemonConfig(#[from]daemonkit::ConfigError),
    #[error("daemonkit bootstrap error: {0}")]Bootstrap(#[from]daemonkit::BootstrapError),
    #[error("plugin protocol error: {0}")]Plugin(#[from]artifactum_plugin_protocol::Error),
    #[error("I/O error: {0}")]Io(#[from]std::io::Error),
    #[error("serialization error: {0}")]Serde(#[from]serde_json::Error),
    #[error("plugin host: {0}")]Host(String),
    #[error("plugin host remote error: {message}")]Remote { message:String, data:Option<Value> },
}
pub type Result<T,E=Error>=std::result::Result<T,E>;

#[derive(Clone,Debug,Serialize,Deserialize)]struct HostRequest{executable:PathBuf,method:String,params:Value}
#[derive(Clone,Debug,Serialize,Deserialize)]struct HostResponse{result:Option<Value>,error:Option<HostRemoteError>}
#[derive(Clone,Debug,Serialize,Deserialize)]struct HostRemoteError{message:String,#[serde(default,skip_serializing_if="Option::is_none")]data:Option<Value>}

#[derive(Default)]struct HostState{sessions:Mutex<HashMap<PathBuf,PluginSession>>}
impl HostState{
    async fn session(&self,path:&Path)->Result<PluginSession>{
        if let Some(session)=self.sessions.lock().await.get(path).cloned(){return Ok(session);}
        let session=PluginSession::spawn(path).await?;self.sessions.lock().await.insert(path.to_path_buf(),session.clone());Ok(session)
    }
    async fn call(&self,request:HostRequest)->HostResponse{
        let params=request.params.clone();
        let mut result=async{let session=self.session(&request.executable).await?;session.call_value(&request.method,params).await.map_err(Error::from)}.await;
        let restart=matches!(&result,Err(Error::Plugin(artifactum_plugin_protocol::Error::UnexpectedEof{..}|artifactum_plugin_protocol::Error::Io(_)|artifactum_plugin_protocol::Error::InvalidFrame(_))));
        if restart {
            self.sessions.lock().await.remove(&request.executable);
            result=async{let session=self.session(&request.executable).await?;session.call_value(&request.method,request.params).await.map_err(Error::from)}.await;
        }
        match result{Ok(value)=>HostResponse{result:Some(value),error:None},Err(error)=>{let data=match &error{Error::Plugin(artifactum_plugin_protocol::Error::Remote{data,..})=>data.clone(),_=>None};HostResponse{result:None,error:Some(HostRemoteError{message:error.to_string(),data})}}}
    }
}

/// Enter the daemonkit bootstrap service if this process was spawned as the
/// Artifactum plugin host. Call before CLI argument parsing.
pub async fn maybe_run_daemon()->Result<bool>{
    let Some(bootstrap)=Bootstrap::detect()? else{return Ok(false)};
    bootstrap.run_embedded_fn(|_context,mut incoming,mut shutdown|async move{
        let state=Arc::new(HostState::default());
        loop{
            tokio::select!{
                _=shutdown.requested()=>break,
                item=incoming.next()=>{
                    let Some(item)=item else{break};
                    if let Ok(stream)=item{let state=Arc::clone(&state);tokio::spawn(async move{let _=handle_connection(state,stream).await;});}
                }
            }
        }
        Ok::<_,std::io::Error>(())
    }).await?;
    Ok(true)
}

async fn handle_connection(state:Arc<HostState>,stream:daemonkit::AuthenticatedStream)->Result<()> {
    let (read,mut write)=tokio::io::split(stream);let mut reader=BufReader::new(read);
    let mut len_line=String::new();reader.read_line(&mut len_line).await?;let len=len_line.trim().parse::<usize>().map_err(|e|Error::Host(e.to_string()))?;
    if len>64*1024*1024{return Err(Error::Host("request too large".into()));}
    let mut bytes=vec![0;len];reader.read_exact(&mut bytes).await?;let request:HostRequest=serde_json::from_slice(&bytes)?;let response=state.call(request).await;let bytes=serde_json::to_vec(&response)?;
    write.write_all(format!("{}\n",bytes.len()).as_bytes()).await?;write.write_all(&bytes).await?;write.shutdown().await?;Ok(())
}

fn daemon()->Result<Daemon<Embedded>>{let spec=DaemonSpec::new("io.artifactum.plugin-host")?.idle_timeout(Some(Duration::from_secs(30*60)));Ok(Daemon::embedded(spec,Spawn::current_exe()?.arg("__artifactum_plugin_host"))?)}

#[derive(Clone)]pub struct DaemonPluginProvider{executable:PathBuf,descriptor:ProviderDescriptor,daemon:Arc<Daemon<Embedded>>}
impl std::fmt::Debug for DaemonPluginProvider{fn fmt(&self,f:&mut std::fmt::Formatter<'_>)->std::fmt::Result{f.debug_struct("DaemonPluginProvider").field("executable",&self.executable).field("descriptor",&self.descriptor.name).finish()}}
impl DaemonPluginProvider{
    pub async fn load(executable:impl Into<PathBuf>)->Result<Self>{let executable=executable.into();let daemon=Arc::new(daemon()?);let temp=Self{executable,descriptor:ProviderDescriptor{name:"pending".into(),version:"0".into(),schemes:Vec::new(),capabilities:Default::default(),metadata:Default::default()},daemon};let init:InitializeResponse=temp.call_host("initialize",&InitializeRequest{protocol_major:PROTOCOL_MAJOR,protocol_minor:PROTOCOL_MINOR}).await?;Ok(Self{descriptor:init.provider,..temp})}
    #[must_use]pub fn executable(&self)->&Path{&self.executable}
    async fn call_host<P:Serialize+?Sized,R:DeserializeOwned>(&self,method:&str,params:&P)->Result<R>{
        let instance=self.daemon.ensure().await?;let stream=instance.connect().await?;let (read,mut write)=tokio::io::split(stream);let request=HostRequest{executable:self.executable.clone(),method:method.into(),params:serde_json::to_value(params)?};let body=serde_json::to_vec(&request)?;write.write_all(format!("{}\n",body.len()).as_bytes()).await?;write.write_all(&body).await?;write.flush().await?;
        let mut reader=BufReader::new(read);let mut len_line=String::new();reader.read_line(&mut len_line).await?;let len=len_line.trim().parse::<usize>().map_err(|e|Error::Host(e.to_string()))?;let mut bytes=vec![0;len];reader.read_exact(&mut bytes).await?;let response:HostResponse=serde_json::from_slice(&bytes)?;if let Some(error)=response.error{return Err(Error::Remote{message:error.message,data:error.data});}let result=response.result.ok_or_else(||Error::Host("host response missing result".into()))?;Ok(serde_json::from_value(result)?)
    }
    async fn provider_call<P:Serialize+?Sized,R:DeserializeOwned>(&self,method:&str,p:&P)->artifactum_core::Result<R>{
        match self.call_host(method,p).await {
            Ok(value)=>Ok(value),
            Err(Error::Remote{message,data})=>{
                if let Some(challenge)=data.as_ref().and_then(|v|v.get("challenge")).and_then(|v|serde_json::from_value(v.clone()).ok()){
                    Err(CoreError::AccessRequired(challenge))
                }else{Err(CoreError::Provider{provider:self.descriptor.name.clone(),message})}
            }
            Err(error)=>Err(CoreError::Provider{provider:self.descriptor.name.clone(),message:error.to_string()}),
        }
    }
}
#[async_trait]impl ArtifactProvider for DaemonPluginProvider{
    fn descriptor(&self)->ProviderDescriptor{self.descriptor.clone()}
    async fn resolve(&self,r:&ArtifactRequirement,c:&ResolveContext)->artifactum_core::Result<Resolution>{self.provider_call("resolve",&ResolveParams{requirement:r.clone(),context:c.clone()}).await}
    async fn prepare_acquisition(&self,f:&ResolvedFile,c:&AcquireContext)->artifactum_core::Result<AcquisitionPlan>{self.provider_call("prepare_acquisition",&PrepareAcquisitionParams{file:f.clone(),context:c.clone()}).await}
    async fn acquire_managed(&self,f:&ResolvedFile,p:&AcquisitionPlan,d:&Path,c:&AcquireContext)->artifactum_core::Result<Acquisition>{self.provider_call("acquire_managed",&AcquireManagedParams{file:f.clone(),plan:p.clone(),destination:d.to_path_buf(),context:c.clone()}).await}
    async fn search(&self,r:&SearchRequest,c:&ResolveContext)->artifactum_core::Result<SearchPage>{self.provider_call("search",&SearchParams{request:r.clone(),context:c.clone()}).await}
    async fn inspect(&self,r:&InspectRequest,c:&ResolveContext)->artifactum_core::Result<InspectResult>{self.provider_call("inspect",&InspectParams{request:r.clone(),context:c.clone()}).await}
    async fn list_versions(&self,r:&ArtifactRef,cursor:Option<&str>,c:&ResolveContext)->artifactum_core::Result<VersionPage>{self.provider_call("versions",&VersionsParams{reference:r.clone(),cursor:cursor.map(str::to_owned),context:c.clone()}).await}
    async fn list_files(&self,r:&ArtifactRequirement,cursor:Option<&str>,c:&ResolveContext)->artifactum_core::Result<FilePage>{self.provider_call("files",&FilesParams{requirement:r.clone(),cursor:cursor.map(str::to_owned),context:c.clone()}).await}
}

#[derive(Clone,Debug)]pub struct PluginLoadError{pub path:PathBuf,pub error:String}
#[derive(Clone,Debug,Default)]pub struct DaemonPluginDiscovery{pub providers:Vec<DaemonPluginProvider>,pub errors:Vec<PluginLoadError>}
pub async fn discover_plugins_via_daemon()->DaemonPluginDiscovery{let mut out=DaemonPluginDiscovery::default();for path in discover_plugin_paths(){match DaemonPluginProvider::load(path.clone()).await{Ok(p)=>out.providers.push(p),Err(e)=>out.errors.push(PluginLoadError{path,error:e.to_string()})}}out}
