//! Versioned provider subprocess protocol for Artifactum.
//!
//! Provider executables named `artifactum-provider-*` speak multiplexed JSON
//! RPC over stdin/stdout using LSP-style Content-Length framing. A session is
//! long-lived and supports concurrent in-flight requests.

use std::{collections::{BTreeSet,HashMap},ffi::OsStr,path::{Path,PathBuf},process::Stdio,sync::{Arc,atomic::{AtomicU64,Ordering}}};
use artifactum_core::{AcquireContext,Acquisition,AcquisitionPlan,ArtifactProvider,ArtifactRef,ArtifactRequirement,Error as CoreError,FilePage,InspectRequest,InspectResult,ProviderDescriptor,ResolveContext,ResolvedFile,Resolution,SearchPage,SearchRequest,VersionPage};
use async_trait::async_trait;
use serde::{de::DeserializeOwned,Deserialize,Serialize};
use serde_json::Value;
use thiserror::Error;
use tokio::{io::{AsyncBufRead,AsyncBufReadExt,AsyncReadExt,AsyncWrite,AsyncWriteExt,BufReader,BufWriter},process::{Child,ChildStdin,Command},sync::{Mutex,oneshot}};

pub const PROTOCOL_MAJOR:u32=2; pub const PROTOCOL_MINOR:u32=0; pub const JSONRPC_VERSION:&str="2.0"; pub const PLUGIN_MODE_FLAG:&str="--artifactum-plugin"; pub const PLUGIN_PREFIX:&str="artifactum-provider-";

#[derive(Debug,Error)] pub enum Error{
    #[error("I/O error: {0}")]Io(#[from]std::io::Error),#[error("protocol serialization error: {0}")]Serialization(#[from]serde_json::Error),#[error("invalid plugin frame: {0}")]InvalidFrame(String),
    #[error("plugin `{plugin}` exited before returning a response")]UnexpectedEof{plugin:String},#[error("plugin `{plugin}` error {code}: {message}")]Remote{plugin:String,code:i64,message:String,data:Option<Value>},
    #[error("plugin protocol {major}.{minor} is incompatible with host {host_major}.{host_minor}")]IncompatibleVersion{major:u32,minor:u32,host_major:u32,host_minor:u32},
}
pub type Result<T,E=Error>=std::result::Result<T,E>;

#[derive(Clone,Debug,Serialize,Deserialize)]pub struct InitializeRequest{pub protocol_major:u32,pub protocol_minor:u32}
#[derive(Clone,Debug,Serialize,Deserialize)]pub struct InitializeResponse{pub protocol_major:u32,pub protocol_minor:u32,pub provider:ProviderDescriptor}
#[derive(Clone,Debug,Serialize,Deserialize)]pub struct RpcRequest{pub jsonrpc:String,pub id:u64,pub method:String,#[serde(default)]pub params:Value}
#[derive(Clone,Debug,Serialize,Deserialize)]pub struct RpcResponse{pub jsonrpc:String,pub id:u64,#[serde(skip_serializing_if="Option::is_none")]pub result:Option<Value>,#[serde(skip_serializing_if="Option::is_none")]pub error:Option<RpcError>}
#[derive(Clone,Debug,Serialize,Deserialize)]pub struct RpcError{pub code:i64,pub message:String,#[serde(default,skip_serializing_if="Option::is_none")]pub data:Option<Value>}

#[derive(Clone,Debug,Serialize,Deserialize)]pub struct ResolveParams{pub requirement:ArtifactRequirement,pub context:ResolveContext}
#[derive(Clone,Debug,Serialize,Deserialize)]pub struct PrepareAcquisitionParams{pub file:ResolvedFile,pub context:AcquireContext}
#[derive(Clone,Debug,Serialize,Deserialize)]pub struct AcquireManagedParams{pub file:ResolvedFile,pub plan:AcquisitionPlan,pub destination:PathBuf,pub context:AcquireContext}
#[derive(Clone,Debug,Serialize,Deserialize)]pub struct SearchParams{pub request:SearchRequest,pub context:ResolveContext}
#[derive(Clone,Debug,Serialize,Deserialize)]pub struct InspectParams{pub request:InspectRequest,pub context:ResolveContext}
#[derive(Clone,Debug,Serialize,Deserialize)]pub struct VersionsParams{pub reference:ArtifactRef,pub cursor:Option<String>,pub context:ResolveContext}
#[derive(Clone,Debug,Serialize,Deserialize)]pub struct FilesParams{pub requirement:ArtifactRequirement,pub cursor:Option<String>,pub context:ResolveContext}

fn core_rpc_error(error:CoreError)->RpcError{
    let data=match &error{CoreError::AccessRequired(challenge)=>serde_json::to_value(challenge).ok().map(|v|serde_json::json!({"kind":"access_required","challenge":v})),_=>None};
    RpcError{code:-32000,message:error.to_string(),data}
}
fn remote_to_core(provider:&str,error:Error)->CoreError{
    match error{
        Error::Remote{message,data,..}=>{
            if let Some(challenge)=data.as_ref().and_then(|d|d.get("challenge")).and_then(|v|serde_json::from_value(v.clone()).ok()){return CoreError::AccessRequired(challenge);}
            CoreError::Provider{provider:provider.into(),message}
        }
        other=>CoreError::Provider{provider:provider.into(),message:other.to_string()},
    }
}

pub async fn serve<P:ArtifactProvider>(provider:P)->Result<()> {
    let provider=Arc::new(provider); let stdin=tokio::io::stdin(); let reader=BufReader::new(stdin); let writer=Arc::new(Mutex::new(BufWriter::new(tokio::io::stdout()))); serve_stream(provider,reader,writer).await
}
async fn serve_stream<P,R,W>(provider:Arc<P>,mut reader:R,writer:Arc<Mutex<W>>)->Result<()>
where P:ArtifactProvider,R:AsyncBufRead+Unpin,W:AsyncWrite+Unpin+Send+'static {
    while let Some(request)=read_frame::<_,RpcRequest>(&mut reader).await?{
        let provider=Arc::clone(&provider);let writer=Arc::clone(&writer);
        tokio::spawn(async move{let id=request.id;let response=match dispatch(provider.as_ref(),request).await{Ok(v)=>RpcResponse{jsonrpc:JSONRPC_VERSION.into(),id,result:Some(v),error:None},Err(e)=>RpcResponse{jsonrpc:JSONRPC_VERSION.into(),id,result:None,error:Some(core_rpc_error(e))}};let mut w=writer.lock().await;let _=write_frame(&mut *w,&response).await;let _=w.flush().await;});
    } Ok(())
}
async fn dispatch<P:ArtifactProvider>(provider:&P,request:RpcRequest)->artifactum_core::Result<Value>{match request.method.as_str(){
    "initialize"=>{let p:InitializeRequest=serde_json::from_value(request.params)?;if p.protocol_major!=PROTOCOL_MAJOR{return Err(CoreError::Provider{provider:provider.descriptor().name,message:format!("unsupported protocol {}.{}",p.protocol_major,p.protocol_minor)});}Ok(serde_json::to_value(InitializeResponse{protocol_major:PROTOCOL_MAJOR,protocol_minor:PROTOCOL_MINOR,provider:provider.descriptor()})?)},
    "resolve"=>{let p:ResolveParams=serde_json::from_value(request.params)?;Ok(serde_json::to_value(provider.resolve(&p.requirement,&p.context).await?)?)},
    "prepare_acquisition"=>{let p:PrepareAcquisitionParams=serde_json::from_value(request.params)?;Ok(serde_json::to_value(provider.prepare_acquisition(&p.file,&p.context).await?)?)},
    "acquire_managed"=>{let p:AcquireManagedParams=serde_json::from_value(request.params)?;Ok(serde_json::to_value(provider.acquire_managed(&p.file,&p.plan,&p.destination,&p.context).await?)?)},
    "search"=>{let p:SearchParams=serde_json::from_value(request.params)?;Ok(serde_json::to_value(provider.search(&p.request,&p.context).await?)?)},
    "inspect"=>{let p:InspectParams=serde_json::from_value(request.params)?;Ok(serde_json::to_value(provider.inspect(&p.request,&p.context).await?)?)},
    "versions"=>{let p:VersionsParams=serde_json::from_value(request.params)?;Ok(serde_json::to_value(provider.list_versions(&p.reference,p.cursor.as_deref(),&p.context).await?)?)},
    "files"=>{let p:FilesParams=serde_json::from_value(request.params)?;Ok(serde_json::to_value(provider.list_files(&p.requirement,p.cursor.as_deref(),&p.context).await?)?)},
    method=>Err(CoreError::Provider{provider:provider.descriptor().name,message:format!("unknown plugin method `{method}`")}),
}}

struct SessionInner{plugin:String,stdin:Mutex<ChildStdin>,pending:Mutex<HashMap<u64,oneshot::Sender<RpcResponse>>>,next_id:AtomicU64,_child:Mutex<Child>}
#[derive(Clone)]pub struct PluginSession{inner:Arc<SessionInner>}
impl std::fmt::Debug for PluginSession{fn fmt(&self,f:&mut std::fmt::Formatter<'_>)->std::fmt::Result{f.debug_struct("PluginSession").field("plugin",&self.inner.plugin).finish()}}
impl PluginSession{
    pub async fn spawn(executable:impl AsRef<Path>)->Result<Self>{
        let path=executable.as_ref();let plugin=path.display().to_string();let mut child=Command::new(path).arg(PLUGIN_MODE_FLAG).stdin(Stdio::piped()).stdout(Stdio::piped()).stderr(Stdio::inherit()).kill_on_drop(true).spawn()?;
        let stdin=child.stdin.take().ok_or_else(||Error::UnexpectedEof{plugin:plugin.clone()})?;let stdout=child.stdout.take().ok_or_else(||Error::UnexpectedEof{plugin:plugin.clone()})?;
        let inner=Arc::new(SessionInner{plugin:plugin.clone(),stdin:Mutex::new(stdin),pending:Mutex::new(HashMap::new()),next_id:AtomicU64::new(1),_child:Mutex::new(child)});
        let reader_inner=Arc::clone(&inner);tokio::spawn(async move{let mut reader=BufReader::new(stdout);loop{match read_frame::<_,RpcResponse>(&mut reader).await{Ok(Some(response))=>{if let Some(tx)=reader_inner.pending.lock().await.remove(&response.id){let _=tx.send(response);}},_=>{let mut pending=reader_inner.pending.lock().await;pending.clear();break;}}}});
        Ok(Self{inner})
    }
    pub async fn call<P:Serialize+?Sized,R:DeserializeOwned>(&self,method:&str,params:&P)->Result<R>{
        let id=self.inner.next_id.fetch_add(1,Ordering::Relaxed);let (tx,rx)=oneshot::channel();self.inner.pending.lock().await.insert(id,tx);
        let request=RpcRequest{jsonrpc:JSONRPC_VERSION.into(),id,method:method.into(),params:serde_json::to_value(params)?};
        {let mut stdin=self.inner.stdin.lock().await;if let Err(e)=write_frame(&mut *stdin,&request).await{self.inner.pending.lock().await.remove(&id);return Err(e);}stdin.flush().await?;}
        let response=rx.await.map_err(|_|Error::UnexpectedEof{plugin:self.inner.plugin.clone()})?;
        if let Some(error)=response.error{return Err(Error::Remote{plugin:self.inner.plugin.clone(),code:error.code,message:error.message,data:error.data});}
        let result=response.result.ok_or_else(||Error::InvalidFrame("response contained neither result nor error".into()))?;Ok(serde_json::from_value(result)?)
    }
    pub async fn call_value(&self,method:&str,params:Value)->Result<Value>{self.call(method,&params).await}
}

#[derive(Clone,Debug)]pub struct PluginProvider{executable:PathBuf,descriptor:ProviderDescriptor,session:PluginSession}
impl PluginProvider{
    pub async fn load(executable:impl Into<PathBuf>)->Result<Self>{let executable=executable.into();let session=PluginSession::spawn(&executable).await?;let response:InitializeResponse=session.call("initialize",&InitializeRequest{protocol_major:PROTOCOL_MAJOR,protocol_minor:PROTOCOL_MINOR}).await?;if response.protocol_major!=PROTOCOL_MAJOR{return Err(Error::IncompatibleVersion{major:response.protocol_major,minor:response.protocol_minor,host_major:PROTOCOL_MAJOR,host_minor:PROTOCOL_MINOR});}Ok(Self{executable,descriptor:response.provider,session})}
    #[must_use]pub fn executable(&self)->&Path{&self.executable}
    #[must_use]pub fn descriptor_ref(&self)->&ProviderDescriptor{&self.descriptor}
    async fn call<P:Serialize+?Sized,R:DeserializeOwned>(&self,method:&str,p:&P)->artifactum_core::Result<R>{self.session.call(method,p).await.map_err(|e|remote_to_core(&self.descriptor.name,e))}
}
#[async_trait]impl ArtifactProvider for PluginProvider{
    fn descriptor(&self)->ProviderDescriptor{self.descriptor.clone()}
    async fn resolve(&self,r:&ArtifactRequirement,c:&ResolveContext)->artifactum_core::Result<Resolution>{self.call("resolve",&ResolveParams{requirement:r.clone(),context:c.clone()}).await}
    async fn prepare_acquisition(&self,f:&ResolvedFile,c:&AcquireContext)->artifactum_core::Result<AcquisitionPlan>{self.call("prepare_acquisition",&PrepareAcquisitionParams{file:f.clone(),context:c.clone()}).await}
    async fn acquire_managed(&self,f:&ResolvedFile,p:&AcquisitionPlan,d:&Path,c:&AcquireContext)->artifactum_core::Result<Acquisition>{self.call("acquire_managed",&AcquireManagedParams{file:f.clone(),plan:p.clone(),destination:d.to_path_buf(),context:c.clone()}).await}
    async fn search(&self,r:&SearchRequest,c:&ResolveContext)->artifactum_core::Result<SearchPage>{self.call("search",&SearchParams{request:r.clone(),context:c.clone()}).await}
    async fn inspect(&self,r:&InspectRequest,c:&ResolveContext)->artifactum_core::Result<InspectResult>{self.call("inspect",&InspectParams{request:r.clone(),context:c.clone()}).await}
    async fn list_versions(&self,r:&ArtifactRef,cursor:Option<&str>,c:&ResolveContext)->artifactum_core::Result<VersionPage>{self.call("versions",&VersionsParams{reference:r.clone(),cursor:cursor.map(str::to_owned),context:c.clone()}).await}
    async fn list_files(&self,r:&ArtifactRequirement,cursor:Option<&str>,c:&ResolveContext)->artifactum_core::Result<FilePage>{self.call("files",&FilesParams{requirement:r.clone(),cursor:cursor.map(str::to_owned),context:c.clone()}).await}
}

#[derive(Clone,Debug)]pub struct PluginLoadError{pub path:PathBuf,pub error:String}
#[derive(Clone,Debug,Default)]pub struct PluginDiscovery{pub providers:Vec<PluginProvider>,pub errors:Vec<PluginLoadError>}
pub fn discover_plugin_paths()->Vec<PathBuf>{let mut paths=BTreeSet::new();for path_var in [std::env::var_os("ARTIFACTUM_PLUGIN_PATH"),std::env::var_os("PATH")].into_iter().flatten(){for dir in std::env::split_paths(&path_var){let Ok(entries)=std::fs::read_dir(dir)else{continue};for entry in entries.flatten(){let path=entry.path();let Some(name)=path.file_name().and_then(OsStr::to_str)else{continue};let normalized=name.strip_suffix(".exe").unwrap_or(name);if normalized.starts_with(PLUGIN_PREFIX)&&path.is_file(){paths.insert(path);}}}}paths.into_iter().collect()}
pub async fn discover_plugins()->PluginDiscovery{let mut d=PluginDiscovery::default();for path in discover_plugin_paths(){match PluginProvider::load(path.clone()).await{Ok(p)=>d.providers.push(p),Err(e)=>d.errors.push(PluginLoadError{path,error:e.to_string()})}}d}

pub async fn write_frame<W:AsyncWrite+Unpin,T:Serialize>(writer:&mut W,value:&T)->Result<()>{let payload=serde_json::to_vec(value)?;writer.write_all(format!("Content-Length: {}\r\n\r\n",payload.len()).as_bytes()).await?;writer.write_all(&payload).await?;writer.flush().await?;Ok(())}
pub async fn read_frame<R:AsyncBufRead+Unpin,T:DeserializeOwned>(reader:&mut R)->Result<Option<T>>{let mut len=None;let mut saw=false;loop{let mut line=String::new();let n=reader.read_line(&mut line).await?;if n==0{if saw{return Err(Error::InvalidFrame("EOF inside frame headers".into()));}return Ok(None);}saw=true;let line=line.trim_end_matches(&['\r','\n'][..]);if line.is_empty(){break;}if let Some((name,value))=line.split_once(':'){if name.eq_ignore_ascii_case("Content-Length"){len=Some(value.trim().parse::<usize>().map_err(|_|Error::InvalidFrame(format!("invalid Content-Length `{value}`")))?);}}}let len=len.ok_or_else(||Error::InvalidFrame("missing Content-Length header".into()))?;if len>64*1024*1024{return Err(Error::InvalidFrame(format!("frame too large: {len}")));}let mut payload=vec![0_u8;len];reader.read_exact(&mut payload).await?;Ok(Some(serde_json::from_slice(&payload)?))}
