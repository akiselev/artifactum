//! `Artifactum.toml` project model, DAG planner, source locking, map/collection
//! expansion and level-parallel scheduler.

use std::{collections::{BTreeMap,BTreeSet,HashMap},path::{Path,PathBuf},str::FromStr,sync::Arc};
use artifactum_core::{ActionSpec,ArtifactId,ArtifactManifest,BudgetSpec,CachePolicy,CollectionEntry,CollectionManifest,ContentKind,EnvironmentSpec,NetworkPolicy,OutputSpec,ResourceSpec,SandboxPolicy};
use artifactum_engine::{Engine,RunResult};
use artifactum_resolver::{ArtifactRequirement,ArtifactResolver,ArtifactResolverBuilder,ProviderProfile,Resolution,Selection};
use artifactum_store::{ArtifactStore,ContentStore,MaterializationMode};
use serde::{Deserialize,Serialize};
use thiserror::Error;
use tokio::{fs,sync::Semaphore,task::JoinSet};

#[derive(Debug,Error)]pub enum Error{
 #[error("I/O error: {0}")]Io(#[from]std::io::Error),
 #[error("TOML decode error: {0}")]TomlDecode(#[from]toml::de::Error),
 #[error("TOML encode error: {0}")]TomlEncode(#[from]toml::ser::Error),
 #[error("core error: {0}")]Core(#[from]artifactum_core::Error),
 #[error("resolver error: {0}")]Resolver(#[from]artifactum_resolver::Error),
 #[error("engine error: {0}")]Engine(#[from]artifactum_engine::Error),
 #[error("store error: {0}")]Store(#[from]artifactum_store::Error),
 #[error("unsupported project version {0}")]Version(u32),
 #[error("unknown target `{0}`") ]UnknownTarget(String),
 #[error("dependency cycle or unresolved references among: {0:?}")]Cycle(Vec<String>),
 #[error("frozen lock mismatch for source `{0}`") ]FrozenMismatch(String),
 #[error("locked artifact `{0}` is absent from local CAS") ]LockedMissing(String),
 #[error("foreach target `{0}` is not a collection") ]NotCollection(String),
 #[error("task `{0}` has no outputs") ]NoOutputs(String),
 #[error("task join error: {0}")]Join(String),
}
pub type Result<T,E=Error>=std::result::Result<T,E>;

#[derive(Clone,Debug,Default,Serialize,Deserialize)]pub struct ProjectInfo{#[serde(default)]pub name:String}
#[derive(Clone,Debug,Default,Serialize,Deserialize)]pub struct ProjectProvider{pub kind:String,#[serde(default)]pub config:BTreeMap<String,String>}
#[derive(Clone,Debug,Serialize,Deserialize)]pub struct ProjectArtifact{pub source:String,#[serde(default,skip_serializing_if="Option::is_none")]pub revision:Option<String>,#[serde(default)]pub include:Vec<String>,#[serde(default)]pub exclude:Vec<String>,#[serde(default,skip_serializing_if="Option::is_none")]pub materialize:Option<PathBuf>}
#[derive(Clone,Debug,Default,Serialize,Deserialize)]pub struct EnvironmentConfig{#[serde(default)]pub variables:BTreeMap<String,String>,#[serde(default,skip_serializing_if="Option::is_none")]pub container:Option<String>}
#[derive(Clone,Debug,Serialize,Deserialize)]pub struct OutputConfig{#[serde(default="default_blob")]pub kind:ContentKind,#[serde(default,skip_serializing_if="Option::is_none")]pub media_type:Option<String>,#[serde(default,skip_serializing_if="Option::is_none")]pub schema:Option<String>}
fn default_blob()->ContentKind{ContentKind::Blob}
#[derive(Clone,Debug,Serialize,Deserialize)]pub struct TaskSpec{
 #[serde(rename="run")]pub command:Vec<String>,
 #[serde(default)]pub inputs:BTreeMap<String,String>,
 #[serde(default)]pub code:Vec<String>,
 #[serde(default)]pub outputs:BTreeMap<String,OutputConfig>,
 #[serde(default,skip_serializing_if="json_null")]pub parameters:serde_json::Value,
 #[serde(default)]pub environment:EnvironmentConfig,
 #[serde(default)]pub resources:ResourceSpec,
 #[serde(default)]pub budget:BudgetSpec,
 #[serde(default)]pub cache:CachePolicy,
 #[serde(default)]pub network:NetworkPolicy,
 #[serde(default)]pub sandbox:SandboxPolicy,
 #[serde(default="default_executor")]pub executor:String,
 #[serde(default,skip_serializing_if="Option::is_none")]pub foreach:Option<String>,
}
fn json_null(v:&serde_json::Value)->bool{v.is_null()}
fn default_executor()->String{"local".into()}
#[derive(Clone,Debug,Serialize,Deserialize)]pub struct RefSpec{pub target:String,#[serde(default)]pub immutable:bool}
#[derive(Clone,Debug,Serialize,Deserialize)]pub struct RemoteSpec{pub kind:String,#[serde(default,skip_serializing_if="Option::is_none")]pub path:Option<PathBuf>,#[serde(default,skip_serializing_if="Option::is_none")]pub url:Option<String>,#[serde(default,skip_serializing_if="Option::is_none")]pub token_env:Option<String>,#[serde(default)]pub read_only:bool}
#[derive(Clone,Debug,Serialize,Deserialize)]pub struct ProjectManifest{#[serde(default="project_version")]pub version:u32,#[serde(default)]pub project:ProjectInfo,#[serde(default)]pub providers:BTreeMap<String,ProjectProvider>,#[serde(default)]pub remotes:BTreeMap<String,RemoteSpec>,#[serde(default)]pub artifacts:BTreeMap<String,ProjectArtifact>,#[serde(default)]pub tasks:BTreeMap<String,TaskSpec>,#[serde(default)]pub refs:BTreeMap<String,RefSpec>}
const fn project_version()->u32{3}
impl Default for ProjectManifest{fn default()->Self{Self{version:3,project:ProjectInfo::default(),providers:BTreeMap::new(),remotes:BTreeMap::new(),artifacts:BTreeMap::new(),tasks:BTreeMap::new(),refs:BTreeMap::new()}}}
impl ProjectManifest{
 pub async fn load(path:impl AsRef<Path>)->Result<Self>{
  let text=fs::read_to_string(path).await?;let value:toml::Value=toml::from_str(&text)?;let version=value.get("version").and_then(toml::Value::as_integer).unwrap_or(2)as u32;
  match version{3=>Ok(toml::from_str(&text)?),2=>{let legacy:LegacyProjectV2=toml::from_str(&text)?;Ok(Self{version:3,project:ProjectInfo::default(),providers:legacy.providers,remotes:BTreeMap::new(),artifacts:legacy.artifacts,tasks:BTreeMap::new(),refs:BTreeMap::new()})},other=>Err(Error::Version(other))}
 }
 pub async fn save(&self,path:impl AsRef<Path>)->Result<()> {fs::write(path,toml::to_string_pretty(self)?).await?;Ok(())}
}
#[derive(Clone,Debug,Default,Deserialize)]struct LegacyProjectV2{#[serde(default)]providers:BTreeMap<String,ProjectProvider>,#[serde(default)]artifacts:BTreeMap<String,ProjectArtifact>}


#[derive(Clone,Debug,Serialize,Deserialize)]pub struct Lockfile{#[serde(default="lock_version")]pub version:u32,#[serde(default,rename="source")]pub sources:Vec<LockedSource>}
const fn lock_version()->u32{3}
impl Default for Lockfile{fn default()->Self{Self{version:3,sources:Vec::new()}}}
#[derive(Clone,Debug,Serialize,Deserialize)]pub struct LockedSource{pub name:String,pub requirement_hash:String,pub artifact:ArtifactId,pub resolution_json:String}
impl Lockfile{pub async fn load(path:impl AsRef<Path>)->Result<Self>{let p=path.as_ref();if !fs::try_exists(p).await?{return Ok(Self::default())}let v:Self=toml::from_str(&fs::read_to_string(p).await?)?;if v.version!=3{return Err(Error::Version(v.version))}Ok(v)}pub async fn save(&self,path:impl AsRef<Path>)->Result<()> {fs::write(path,toml::to_string_pretty(self)?).await?;Ok(())}fn get(&self,name:&str)->Option<&LockedSource>{self.sources.iter().find(|s|s.name==name)}fn upsert(&mut self,v:LockedSource){if let Some(x)=self.sources.iter_mut().find(|x|x.name==v.name){*x=v}else{self.sources.push(v);self.sources.sort_by(|a,b|a.name.cmp(&b.name));}}}

#[derive(Clone,Debug,Serialize,Deserialize)]pub struct PlannedTask{pub name:String,pub dependencies:Vec<String>,pub foreach:Option<String>}
#[derive(Clone,Debug,Serialize,Deserialize)]pub struct Plan{pub levels:Vec<Vec<PlannedTask>>}
#[derive(Clone,Debug,Serialize,Deserialize)]pub struct PipelineRun{pub sources:BTreeMap<String,ArtifactId>,pub outputs:BTreeMap<String,ArtifactId>,pub actions:BTreeMap<String,Vec<RunResult>>}

pub fn plan(project:&ProjectManifest,targets:&[String])->Result<Plan>{let selected=closure(project,targets)?;let mut remaining:selected.clone();let mut done=BTreeSet::new();let mut levels=Vec::new();while !remaining.is_empty(){let mut level=Vec::new();for name in&remaining{let task=&project.tasks[name];let deps=task_dependencies(project,task);if deps.iter().all(|d|!project.tasks.contains_key(d)||done.contains(d)){level.push(name.clone());}}if level.is_empty(){return Err(Error::Cycle(remaining.into_iter().collect()))}let planned=level.iter().map(|name|PlannedTask{name:name.clone(),dependencies:task_dependencies(project,&project.tasks[name]).into_iter().filter(|d|project.tasks.contains_key(d)).collect(),foreach:project.tasks[name].foreach.clone()}).collect();for n in level{remaining.remove(&n);done.insert(n);}levels.push(planned);}Ok(Plan{levels})}
fn closure(project:&ProjectManifest,targets:&[String])->Result<BTreeSet<String>>{let roots=if targets.is_empty(){project.tasks.keys().cloned().collect::<Vec<_>>()}else{targets.to_vec()};let mut out=BTreeSet::new();let mut stack=roots;while let Some(t)=stack.pop(){let name=task_name(&t);if project.artifacts.contains_key(name){continue}if !project.tasks.contains_key(name){return Err(Error::UnknownTarget(t))}if out.insert(name.into()){stack.extend(task_dependencies(project,&project.tasks[name]).into_iter().filter(|d|project.tasks.contains_key(d)));}}Ok(out)}
fn task_dependencies(project:&ProjectManifest,t:&TaskSpec)->Vec<String>{let mut d=t.inputs.values().map(|x|task_name(x).to_string()).collect::<Vec<_>>();if let Some(x)=&t.foreach{d.push(task_name(x).into())}d.sort();d.dedup();d.into_iter().filter(|x|project.tasks.contains_key(x)).collect()}
fn task_name(reference:&str)->&str{let r=reference.strip_prefix('@').unwrap_or(reference);r.split('.').next().unwrap_or(r)}

pub struct PipelineRunner{project:ProjectManifest,project_dir:PathBuf,lock_path:PathBuf,engine:Engine,resolver:ArtifactResolver,max_parallel:usize}
impl PipelineRunner{
 pub async fn from_file(path:impl AsRef<Path>,engine:Engine,mut resolver_builder:ArtifactResolverBuilder)->Result<Self>{let path=path.as_ref();let project=ProjectManifest::load(path).await?;for(name,p)in&project.providers{resolver_builder=resolver_builder.profile(ProviderProfile{name:name.clone(),provider:p.kind.clone(),config:p.config.clone()});}let resolver=resolver_builder.store(engine.store().clone()).metadata(engine.metadata().clone()).build().await?;Ok(Self{project,project_dir:path.parent().unwrap_or(Path::new(".")).to_path_buf(),lock_path:path.parent().unwrap_or(Path::new(".")).join("Artifactum.lock"),engine,resolver,max_parallel:8})}
 pub fn max_parallel(mut self,n:usize)->Self{self.max_parallel=n.max(1);self}
 pub fn project(&self)->&ProjectManifest{&self.project}
 pub fn plan(&self,targets:&[String])->Result<Plan>{plan(&self.project,targets)}
 pub async fn run(&self,targets:&[String],frozen:bool)->Result<PipelineRun>{let(mut lock,sources)=self.acquire_sources(frozen).await?;let p=self.plan(targets)?;let outputs=Arc::new(tokio::sync::Mutex::new(BTreeMap::<String,ArtifactId>::new()));let actions=Arc::new(tokio::sync::Mutex::new(BTreeMap::<String,Vec<RunResult>>::new()));for level in p.levels{let sem=Arc::new(Semaphore::new(self.max_parallel));let mut joins=JoinSet::new();for pt in level{let permit=sem.clone().acquire_owned().await.map_err(|e|Error::Join(e.to_string()))?;let runner=self.clone_for_task();let src=sources.clone();let out=Arc::clone(&outputs);let acts=Arc::clone(&actions);joins.spawn(async move{let _permit=permit;let resolved={out.lock().await.clone()};let rs=runner.run_task(&pt.name,&src,&resolved).await?;let mut om=out.lock().await;for(k,v)in&rs.outputs{om.insert(format!("{}.{}",pt.name,k),v);}acts.lock().await.insert(pt.name.clone(),rs.runs);Ok::<(),Error>(())});}while let Some(r)=joins.join_next().await{r.map_err(|e|Error::Join(e.to_string()))??;}}
  let final_outputs=outputs.lock().await.clone();for(name,r)in&self.project.refs{let id=resolve_target(&r.target,&sources,&final_outputs)?;self.engine.store().set_ref(name,&id,r.immutable).await?;}lock.save(&self.lock_path).await?;Ok(PipelineRun{sources,outputs:final_outputs,actions:actions.lock().await.clone()})}
 fn clone_for_task(&self)->TaskRunner{TaskRunner{project:self.project.clone(),project_dir:self.project_dir.clone(),engine:self.engine.clone(),max_parallel:self.max_parallel}}
 pub async fn acquire_sources(&self,frozen:bool)->Result<(Lockfile,BTreeMap<String,ArtifactId>)>{let mut lock=Lockfile::load(&self.lock_path).await?;let mut out=BTreeMap::new();for(name,source)in&self.project.artifacts{let id=self.fetch_source_with_lock(name,frozen,&mut lock).await?;if let Some(destination)=&source.materialize{self.engine.store().materialize(&id,self.project_dir.join(destination),MaterializationMode::Auto).await?;}out.insert(name.clone(),id);}lock.save(&self.lock_path).await?;Ok((lock,out))}
 pub async fn fetch_source(&self,name:&str,frozen:bool)->Result<ArtifactId>{let mut lock=Lockfile::load(&self.lock_path).await?;let id=self.fetch_source_with_lock(name,frozen,&mut lock).await?;lock.save(&self.lock_path).await?;if let Some(destination)=self.project.artifacts.get(name).and_then(|artifact|artifact.materialize.as_ref()){self.engine.store().materialize(&id,self.project_dir.join(destination),MaterializationMode::Auto).await?;}Ok(id)}
 async fn fetch_source_with_lock(&self,name:&str,frozen:bool,lock:&mut Lockfile)->Result<ArtifactId>{let a=self.project.artifacts.get(name).ok_or_else(||Error::UnknownTarget(name.into()))?;let req=ArtifactRequirement{reference:a.source.parse()?,revision:a.revision.clone(),selection:Selection{include:a.include.clone(),exclude:a.exclude.clone()},metadata:BTreeMap::new()};let hash=artifactum_core::hash_canonical(&req)?.to_string();if frozen{let l=lock.get(name).ok_or_else(||Error::FrozenMismatch(name.into()))?;if l.requirement_hash!=hash{return Err(Error::FrozenMismatch(name.into()))}if self.engine.store().load_artifact(&l.artifact).await.is_err(){return Err(Error::LockedMissing(name.into()))}return Ok(l.artifact.clone())}let got=self.resolver.acquire(&req).await?;let artifact=got.artifact.clone();lock.upsert(LockedSource{name:name.into(),requirement_hash:hash,artifact:got.artifact,resolution_json:serde_json::to_string(&got.resolution).map_err(|e|Error::Resolver(artifactum_resolver::Error::Provider{provider:"lockfile".into(),message:e.to_string()}))?});Ok(artifact)}
}

struct TaskRunner{project:ProjectManifest,project_dir:PathBuf,engine:Engine,max_parallel:usize}
struct TaskRunResult{outputs:BTreeMap<String,ArtifactId>,runs:Vec<RunResult>}
impl TaskRunner{
 async fn run_task(&self,name:&str,sources:&BTreeMap<String,ArtifactId>,outputs:&BTreeMap<String,ArtifactId>)->Result<TaskRunResult>{let task=self.project.tasks.get(name).ok_or_else(||Error::UnknownTarget(name.into()))?.clone();if task.outputs.is_empty(){return Err(Error::NoOutputs(name.into()))}if let Some(each)=&task.foreach{let collection_id=resolve_target(each,sources,outputs)?;let manifest=self.engine.store().load_artifact(&collection_id).await?;let collection=match manifest.kind{ContentKind::Collection=>self.engine.store().read_collection(&manifest.content).await?,ContentKind::Tree=>{let tree=self.engine.store().read_tree(&manifest.content).await?;let mut entries=Vec::new();for e in tree.entries{if e.kind==artifactum_core::TreeEntryKind::Blob{let id=self.engine.store().put_artifact(&artifactum_core::ArtifactManifest::new(e.content,ContentKind::Blob)).await?;entries.push(CollectionEntry{key:e.path.to_string(),artifact:id,label:Some(e.path.to_string())});}}CollectionManifest::new(entries)},ContentKind::Blob=>return Err(Error::NotCollection(each.clone()))};let sem=Arc::new(Semaphore::new(self.max_parallel));let mut joins=JoinSet::new();for item in collection.entries{let permit=sem.clone().acquire_owned().await.map_err(|e|Error::Join(e.to_string()))?;let this=self.clone();let task=task.clone();let sources=sources.clone();let outputs=outputs.clone();joins.spawn(async move{let _p=permit;let mut extra=BTreeMap::new();extra.insert("item".into(),item.artifact.clone());let spec=this.build_action(name,&task,&sources,&outputs,extra,Some(&item.key)).await?;let run=this.engine.run(spec,&task.executor).await?;Ok::<(String,RunResult),Error>((item.key,run))});}let mut runs=Vec::new();let mut by_output:BTreeMap<String,Vec<CollectionEntry>>=BTreeMap::new();while let Some(r)=joins.join_next().await{let(key,run)=r.map_err(|e|Error::Join(e.to_string()))??;for(out_name,id)in&run.realization.outputs{by_output.entry(out_name.clone()).or_default().push(CollectionEntry{key:key.clone(),artifact:id.clone(),label:Some(key.clone())});}runs.push(run);}let mut result=BTreeMap::new();for(n,entries)in by_output{let mut collect_inputs=BTreeMap::new();for e in &entries{collect_inputs.insert(e.key.clone(),e.artifact.clone());}let collection=self.engine.store().put_collection(&CollectionManifest::new(entries)).await?;let mut collect_outputs=BTreeMap::new();collect_outputs.insert(n.clone(),collection.clone());let collect_spec=ActionSpec{version:1,name:format!("{name}::collect::{n}"),command:vec!["artifactum:collect".into()],inputs:collect_inputs,code:BTreeMap::new(),parameters:serde_json::Value::Null,environment:EnvironmentSpec::default(),outputs:BTreeMap::from([(n.clone(),OutputSpec::collection())]),resources:ResourceSpec::default(),budget:BudgetSpec::default(),network:NetworkPolicy::Deny,sandbox:SandboxPolicy::None,cache:CachePolicy::Pure,platform:None};runs.push(self.engine.realize_intrinsic(collect_spec,collect_outputs)?);result.insert(n,collection);}Ok(TaskRunResult{outputs:result,runs})}else{let spec=self.build_action(name,&task,sources,outputs,BTreeMap::new(),None).await?;let run=self.engine.run(spec,&task.executor).await?;Ok(TaskRunResult{outputs:run.realization.outputs.clone(),runs:vec![run]})}}
 async fn build_action(&self,name:&str,task:&TaskSpec,sources:&BTreeMap<String,ArtifactId>,outputs:&BTreeMap<String,ArtifactId>,extra:BTreeMap<String,ArtifactId>,map_key:Option<&str>)->Result<ActionSpec>{let mut inputs=BTreeMap::new();for(n,r)in&task.inputs{inputs.insert(n.clone(),resolve_target(r,sources,outputs)?);}inputs.extend(extra);let mut code=BTreeMap::new();for p in&task.code{let abs=self.project_dir.join(p);let id=if fs::metadata(&abs).await?.is_dir(){self.engine.store().import_tree(&abs).await?}else{self.engine.store().import_blob_artifact(&abs,None).await?};code.insert(p.clone(),id);}let mut out=BTreeMap::new();for(n,o)in&task.outputs{out.insert(n.clone(),OutputSpec{kind:o.kind,media_type:o.media_type.clone(),schema:o.schema.as_ref().map(|s|ArtifactId::from_str(s)).transpose()?});}let mut params=task.parameters.clone();if let Some(k)=map_key{if !params.is_object(){params=serde_json::json!({});}params.as_object_mut().expect("object").insert("artifactum_map_key".into(),serde_json::Value::String(k.into()));}Ok(ActionSpec{version:1,name:if let Some(k)=map_key{format!("{name}[{k}]")}else{name.into()},command:task.command.clone(),inputs,code,parameters:params,environment:EnvironmentSpec{variables:task.environment.variables.clone(),container:task.environment.container.clone()},outputs:out,resources:task.resources.clone(),budget:task.budget.clone(),network:task.network.clone(),sandbox:task.sandbox.clone(),cache:task.cache,platform:None})}
}
impl Clone for TaskRunner{fn clone(&self)->Self{Self{project:self.project.clone(),project_dir:self.project_dir.clone(),engine:self.engine.clone(),max_parallel:self.max_parallel}}}

fn resolve_target(target:&str,sources:&BTreeMap<String,ArtifactId>,outputs:&BTreeMap<String,ArtifactId>)->Result<ArtifactId>{let t=target.strip_prefix('@').unwrap_or(target);if let Some(v)=sources.get(t){return Ok(v.clone())}if let Some(v)=outputs.get(t){return Ok(v.clone())}if !t.contains('.') {let matches=outputs.iter().filter(|(k,_)|k.starts_with(&format!("{t}."))).collect::<Vec<_>>();if matches.len()==1{return Ok(matches[0].1.clone())}}Err(Error::UnknownTarget(target.into()))}
