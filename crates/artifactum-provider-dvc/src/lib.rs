use std::path::Path;
use artifactum_core::{AcquireContext,Acquisition,AcquisitionPlan,ArtifactProvider,ArtifactRequirement,ProviderCapabilities,ProviderDescriptor,ResolveContext,Resolution,ResolvedFile};
use artifactum_provider_command::{required_fragment,run_checked,single_file_resolution,source_with,string_field};
use async_trait::async_trait;

#[derive(Clone,Debug,Default)]pub struct DvcProvider;
#[async_trait]impl ArtifactProvider for DvcProvider{
 fn descriptor(&self)->ProviderDescriptor{ProviderDescriptor{name:"dvc".into(),version:env!("CARGO_PKG_VERSION").into(),schemes:vec!["dvc".into()],capabilities:ProviderCapabilities{resolve:true,acquire:true,list:true,auth:true,..Default::default()},metadata:Default::default()}}
 async fn resolve(&self,r:&ArtifactRequirement,_:&ResolveContext)->artifactum_core::Result<Resolution>{let(repo,path)=required_fragment("dvc",r.reference.locator(),"path")?;single_file_resolution("dvc",format!("dvc:{repo}#{path}"),r,path,source_with([("repo",repo.into()),("path",path.into()),("revision",r.revision.clone().unwrap_or_default())]))}
 async fn prepare_acquisition(&self,f:&ResolvedFile,_:&AcquireContext)->artifactum_core::Result<AcquisitionPlan>{Ok(AcquisitionPlan::ProviderManaged{state:f.source.clone()})}
 async fn acquire_managed(&self,f:&ResolvedFile,_:&AcquisitionPlan,d:&Path,c:&AcquireContext)->artifactum_core::Result<Acquisition>{let repo=string_field(&f.source,"repo","dvc")?;let path=string_field(&f.source,"path","dvc")?;let rev=f.source.get("revision").and_then(|v|v.as_str()).filter(|v|!v.is_empty());let mut args=vec!["get".into(),"-f".into(),repo.into(),path.into(),"-o".into(),d.display().to_string()];if let Some(rev)=rev{args.extend(["--rev".into(),rev.into()]);}if let Some(remote)=c.profile.as_ref().and_then(|p|p.config.get("remote")){args.extend(["--remote".into(),remote.clone()]);}run_checked("dvc","dvc",args).await?;let n=tokio::fs::metadata(d).await?.len();Ok(Acquisition{bytes_written:Some(n),metadata:Default::default()})}
}
pub fn provider()->DvcProvider{DvcProvider}
