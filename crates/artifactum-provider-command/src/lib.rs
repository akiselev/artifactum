//! Helpers for providers which delegate vendor-specific acquisition to an
//! official command line client while keeping Artifactum's provider contract.

use std::{ffi::OsStr,path::{Path,PathBuf},process::Output};
use artifactum_core::{access_required,external_tool_required,provider_error,AccessRequirement,ArtifactPath,ArtifactRequirement,ResolvedFile,ResolvedRevision,Resolution};
use serde_json::{json,Value};
use tokio::{fs,process::Command};

pub fn split_fragment(locator:&str)->(&str,Option<&str>){locator.split_once('#').map_or((locator,None),|(a,b)|(a,Some(b)))}
pub fn required_fragment<'a>(provider:&str,locator:&'a str,what:&str)->artifactum_core::Result<(&'a str,&'a str)>{let(root,path)=split_fragment(locator);let path=path.ok_or_else(||provider_error(provider,format!("reference must include #{what}")))?;if root.is_empty()||path.is_empty(){return Err(provider_error(provider,"reference identity/path cannot be empty"));}Ok((root,path))}
pub fn tool_on_path(tool:&str)->bool{std::env::var_os("PATH").is_some_and(|paths|std::env::split_paths(&paths).any(|dir|{let p=dir.join(tool);if p.is_file(){return true}#[cfg(windows)]{if dir.join(format!("{tool}.exe")).is_file(){return true}}false}))}
pub fn require_tool(provider:&str,tool:&str)->artifactum_core::Result<()>{if tool_on_path(tool){Ok(())}else{Err(external_tool_required(provider,tool,format!("{provider} acquisition requires the `{tool}` command on PATH")))}}

pub async fn run_checked<I,S>(provider:&str,tool:&str,args:I)->artifactum_core::Result<Output>
where I:IntoIterator<Item=S>,S:AsRef<OsStr>{
    require_tool(provider,tool)?;
    let output=Command::new(tool).args(args).kill_on_drop(true).output().await.map_err(|e|provider_error(provider,e))?;
    if !output.status.success(){return Err(command_failure(provider,tool,&output));}
    Ok(output)
}

pub async fn run_checked_in<I,S>(provider:&str,tool:&str,args:I,current_dir:&Path)->artifactum_core::Result<Output>
where I:IntoIterator<Item=S>,S:AsRef<OsStr>{
    require_tool(provider,tool)?;
    let output=Command::new(tool).args(args).current_dir(current_dir).kill_on_drop(true).output().await.map_err(|e|provider_error(provider,e))?;
    if !output.status.success(){return Err(command_failure(provider,tool,&output));}
    Ok(output)
}

fn command_failure(provider:&str,tool:&str,output:&Output)->artifactum_core::Error{
    let stderr=String::from_utf8_lossy(&output.stderr);
    let stdout=String::from_utf8_lossy(&output.stdout);
    let detail=if stderr.trim().is_empty(){stdout.trim()}else{stderr.trim()};
    let lower=detail.to_ascii_lowercase();
    let auth_markers=["unauthorized","unauthenticated","authentication","not logged in","login required","log in","api key","api_key","credential","access token","token expired","401","403 forbidden"];
    if auth_markers.iter().any(|marker|lower.contains(marker)){
        access_required(provider,AccessRequirement::Authentication,format!("{tool} could not access the requested resource: {}",detail.chars().take(1024).collect::<String>()),None)
    }else{provider_error(provider,format!("{tool} exited with {}: {}",output.status,detail))}
}

pub fn single_file_resolution(provider:&str,canonical_ref:String,requirement:&ArtifactRequirement,path:&str,source:Value)->artifactum_core::Result<Resolution>{
    Ok(Resolution{provider:provider.into(),canonical_ref,revision:requirement.revision.as_ref().map(|id|ResolvedRevision{id:id.clone(),requested:Some(id.clone())}),files:vec![ResolvedFile{path:ArtifactPath::new(path)?,size:None,digests:Default::default(),media_type:None,source}],provider_state:Value::Null,metadata:Default::default()})
}

pub async fn copy_from_tree(root:&Path,relative:&str,destination:&Path,provider:&str)->artifactum_core::Result<u64>{
    let relative=ArtifactPath::new(relative)?;let source=root.join(relative.as_str());
    let meta=fs::metadata(&source).await.map_err(|e|provider_error(provider,format!("downloaded artifact does not contain {}: {e}",relative.as_str())))?;
    if !meta.is_file(){return Err(provider_error(provider,format!("downloaded artifact path {} is not a file",relative.as_str())))}
    fs::copy(source,destination).await.map_err(|e|provider_error(provider,e))
}

pub async fn find_and_copy(root:&Path,relative:&str,destination:&Path,provider:&str)->artifactum_core::Result<u64>{
    let preferred=root.join(relative);
    if preferred.is_file(){return fs::copy(preferred,destination).await.map_err(|e|provider_error(provider,e));}
    let basename=Path::new(relative).file_name().ok_or_else(||provider_error(provider,"invalid requested file path"))?;
    let mut stack=vec![root.to_path_buf()];
    while let Some(dir)=stack.pop(){let mut rd=fs::read_dir(&dir).await.map_err(|e|provider_error(provider,e))?;while let Some(entry)=rd.next_entry().await.map_err(|e|provider_error(provider,e))?{let ty=entry.file_type().await.map_err(|e|provider_error(provider,e))?;if ty.is_dir(){stack.push(entry.path())}else if ty.is_file()&&entry.file_name()==basename{return fs::copy(entry.path(),destination).await.map_err(|e|provider_error(provider,e));}}}
    Err(provider_error(provider,format!("download completed but `{relative}` was not found")))
}

pub fn source_with(fields:impl IntoIterator<Item=(&'static str,String)>)->Value{let mut map=serde_json::Map::new();for(k,v)in fields{map.insert(k.into(),Value::String(v));}Value::Object(map)}
pub fn string_field<'a>(source:&'a Value,key:&str,provider:&str)->artifactum_core::Result<&'a str>{source.get(key).and_then(Value::as_str).ok_or_else(||provider_error(provider,format!("resolved source missing `{key}`")))}
pub fn tempdir(provider:&str)->artifactum_core::Result<tempfile::TempDir>{tempfile::tempdir().map_err(|e|provider_error(provider,e))}
pub fn json_output(output:&Output,provider:&str)->artifactum_core::Result<Value>{serde_json::from_slice(&output.stdout).map_err(|e|provider_error(provider,format!("invalid JSON from upstream tool: {e}")))}
pub fn file_name(path:&str)->String{PathBuf::from(path).file_name().and_then(|v|v.to_str()).unwrap_or("artifact").to_owned()}
pub fn access_json(tool:&str)->Value{json!({"external_tool":tool})}
