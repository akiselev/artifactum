use std::path::Path;
use artifactum_core::{AcquireContext,Acquisition,AcquisitionPlan,ArtifactProvider,ArtifactRequirement,ProviderCapabilities,ProviderDescriptor,ResolveContext,Resolution,ResolvedFile};
use artifactum_provider_command::{file_name,find_and_copy,run_checked,single_file_resolution,source_with,string_field,tempdir};use async_trait::async_trait;
#[derive(Clone,Debug,Default)]pub struct MlflowProvider;
#[async_trait]impl ArtifactProvider for MlflowProvider{
 fn descriptor(&self)->ProviderDescriptor{ProviderDescriptor{name:"mlflow".into(),version:env!("CARGO_PKG_VERSION").into(),schemes:vec!["mlflow".into()],capabilities:ProviderCapabilities{resolve:true,acquire:true,auth:true,..Default::default()},metadata:Default::default()}}
 async fn resolve(&self,r:&ArtifactRequirement,_:&ResolveContext)->artifactum_core::Result<Resolution>{let uri=r.reference.locator();let path=file_name(uri);single_file_resolution("mlflow",format!("mlflow:{uri}"),r,&path,source_with([("uri",uri.into()),("path",path)]))}
 async fn prepare_acquisition(&self,f:&ResolvedFile,_:&AcquireContext)->artifactum_core::Result<AcquisitionPlan>{Ok(AcquisitionPlan::ProviderManaged{state:f.source.clone()})}
 async fn acquire_managed(&self,f:&ResolvedFile,_:&AcquisitionPlan,d:&Path,_:&AcquireContext)->artifactum_core::Result<Acquisition>{let uri=string_field(&f.source,"uri","mlflow")?;let path=string_field(&f.source,"path","mlflow")?;let tmp=tempdir("mlflow")?;run_checked("mlflow","mlflow",["artifacts","download","--artifact-uri",uri,"--dst-path",tmp.path().to_str().unwrap_or(".")]).await?;let n=find_and_copy(tmp.path(),path,d,"mlflow").await?;Ok(Acquisition{bytes_written:Some(n),metadata:Default::default()})}
}
pub fn provider()->MlflowProvider{MlflowProvider}
