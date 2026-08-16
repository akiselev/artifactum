//! Deterministic provider used by protocol/daemon/conformance tests.
use std::{collections::BTreeMap, path::PathBuf};
use artifactum_core::ArtifactPath;
use artifactum_resolver::{AcquireContext,AcquisitionPlan,ArtifactProvider,ArtifactRequirement,DigestSet,ProviderCapabilities,ProviderDescriptor,ResolveContext,ResolvedFile,Resolution,Result};
use async_trait::async_trait;

#[derive(Clone,Debug,Default)]
pub struct Fixture;
#[async_trait]
impl ArtifactProvider for Fixture {
    fn descriptor(&self)->ProviderDescriptor{ProviderDescriptor{name:"fixture".into(),version:env!("CARGO_PKG_VERSION").into(),schemes:vec!["fixture".into()],capabilities:ProviderCapabilities{resolve:true,acquire:true,inspect:true,list:true,..Default::default()}}}
    async fn resolve(&self,r:&ArtifactRequirement,_:&ResolveContext)->Result<Resolution>{let path=PathBuf::from(r.reference.locator());let meta=tokio::fs::metadata(&path).await?;let name=path.file_name().and_then(|x|x.to_str()).unwrap_or("fixture");Ok(Resolution{provider:"fixture".into(),canonical_ref:format!("fixture:{}",path.display()),revision:None,files:vec![ResolvedFile{path:ArtifactPath::new(name)?,size:Some(meta.len()),digests:DigestSet(BTreeMap::new()),media_type:Some("text/plain".into()),source:serde_json::json!({"path":path})}],provider_state:serde_json::json!({"fixture":true}),metadata:BTreeMap::new()})}
    async fn prepare_acquisition(&self,f:&ResolvedFile,_:&AcquireContext)->Result<AcquisitionPlan>{Ok(AcquisitionPlan::LocalCopy{path:f.source.get("path").and_then(|v|v.as_str()).unwrap_or_default().into()})}
}

#[cfg(test)]
mod tests{
    use super::*;
    use artifactum_provider_testkit::{ConformanceOptions,acquire_case,check_provider,dynamic};
    #[tokio::test]
    async fn passes_provider_conformance_and_real_acquisition(){
        let temp=tempfile::tempdir().unwrap();let source=temp.path().join("fixture.txt");tokio::fs::write(&source,b"provider conformance\n").await.unwrap();
        let requirement=ArtifactRequirement::new(format!("fixture:{}",source.display()).parse().unwrap());
        let report=check_provider(&Fixture,&requirement,&ResolveContext::default(),&ConformanceOptions{stable_resolution:true,require_nonempty:true}).await.unwrap();
        assert_eq!(report.files,1);
        let acquired=acquire_case(dynamic(Fixture),requirement).await.unwrap();assert_eq!(acquired.source_observations,1);
    }
}
