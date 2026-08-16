use std::path::Path;
use artifactum_core::{provider_error,AcquireContext,Acquisition,AcquisitionPlan,ArtifactProvider,ArtifactRequirement,ProviderCapabilities,ProviderDescriptor,ResolveContext,Resolution,ResolvedFile};
use artifactum_provider_command::{find_and_copy,required_fragment,run_checked,single_file_resolution,source_with,string_field,tempdir};
use async_trait::async_trait;

#[derive(Clone,Debug,Default)]pub struct KaggleProvider;
fn parse(locator:&str)->artifactum_core::Result<(&str,&str,&str)>{let(kind,rest)=locator.split_once(':').ok_or_else(||provider_error("kaggle","expected kaggle:dataset:<owner/name>#<path> or kaggle:model:<owner/model/framework/variation/version>#<path>"))?;if !matches!(kind,"dataset"|"model"){return Err(provider_error("kaggle","resource kind must be dataset or model"));}let(resource,path)=required_fragment("kaggle",rest,"path")?;Ok((kind,resource,path))}
#[async_trait]impl ArtifactProvider for KaggleProvider{
 fn descriptor(&self)->ProviderDescriptor{ProviderDescriptor{name:"kaggle".into(),version:env!("CARGO_PKG_VERSION").into(),schemes:vec!["kaggle".into()],capabilities:ProviderCapabilities{resolve:true,acquire:true,list:true,auth:true,..Default::default()},metadata:Default::default()}}
 async fn resolve(&self,r:&ArtifactRequirement,_:&ResolveContext)->artifactum_core::Result<Resolution>{let(kind,resource,path)=parse(r.reference.locator())?;single_file_resolution("kaggle",format!("kaggle:{kind}:{resource}#{path}"),r,path,source_with([("kind",kind.into()),("resource",resource.into()),("path",path.into())]))}
 async fn prepare_acquisition(&self,f:&ResolvedFile,_:&AcquireContext)->artifactum_core::Result<AcquisitionPlan>{Ok(AcquisitionPlan::ProviderManaged{state:f.source.clone()})}
 async fn acquire_managed(&self,f:&ResolvedFile,_:&AcquisitionPlan,d:&Path,_:&AcquireContext)->artifactum_core::Result<Acquisition>{let kind=string_field(&f.source,"kind","kaggle")?;let resource=string_field(&f.source,"resource","kaggle")?;let path=string_field(&f.source,"path","kaggle")?;let tmp=tempdir("kaggle")?;
   if kind=="dataset"{run_checked("kaggle","kaggle",["datasets","download",resource,"-f",path,"-p",tmp.path().to_str().unwrap_or("."),"-q","-o"]).await?;}
   else{let base=["models","variations","versions","download",resource,"-p",tmp.path().to_str().unwrap_or("."),"-q","-f"];
     let mut args=base.iter().map(|s|(*s).to_owned()).collect::<Vec<_>>();args.push("--untar".into());let first=run_checked("kaggle","kaggle",args).await;
     if first.is_err()||!tmp.path().join(path).is_file(){let mut args=base.iter().map(|s|(*s).to_owned()).collect::<Vec<_>>();args.push("--unzip".into());run_checked("kaggle","kaggle",args).await?;}
   }
   let n=find_and_copy(tmp.path(),path,d,"kaggle").await?;Ok(Acquisition{bytes_written:Some(n),metadata:Default::default()})}
}
pub fn provider()->KaggleProvider{KaggleProvider}
