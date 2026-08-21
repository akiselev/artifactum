use artifactum_core::ArtifactPath;
use artifactum_provider_api::ApiClient;
use artifactum_resolver::{
    AcquireContext, AcquisitionPlan, ArtifactProvider, ArtifactRequirement, DigestSet,
    ProviderCapabilities, ProviderDescriptor, Resolution, ResolveContext, ResolvedFile,
    ResolvedRevision, Result,
};
use async_trait::async_trait;
use std::collections::BTreeMap;
#[derive(Clone, Default)]
pub struct FigshareProvider {
    api: ApiClient,
}
#[async_trait]
impl ArtifactProvider for FigshareProvider {
    fn descriptor(&self) -> ProviderDescriptor {
        ProviderDescriptor {
            name: "figshare".into(),
            version: env!("CARGO_PKG_VERSION").into(),
            schemes: vec!["figshare".into()],
            capabilities: ProviderCapabilities {
                resolve: true,
                acquire: true,
                ..Default::default()
            },
        }
    }
    async fn resolve(&self, r: &ArtifactRequirement, _: &ResolveContext) -> Result<Resolution> {
        let id = r.reference.locator();
        let v: serde_json::Value = self
            .api
            .get_json(
                "figshare",
                &format!("https://api.figshare.com/v2/articles/{id}"),
                &BTreeMap::new(),
            )
            .await?;
        let mut files = Vec::new();
        if let Some(arr) = v["files"].as_array() {
            for f in arr {
                files.push(ResolvedFile{path:ArtifactPath::new(f["name"].as_str().unwrap_or("file"))?,size:f["size"].as_u64(),digests:DigestSet(BTreeMap::new()),media_type:None,source:serde_json::json!({"url":f["download_url"].as_str().unwrap_or_default(),"md5":f["computed_md5"].as_str()})});
            }
        }
        Ok(Resolution {
            provider: "figshare".into(),
            canonical_ref: format!("figshare:{id}"),
            revision: Some(ResolvedRevision {
                id: v["modified_date"].as_str().unwrap_or(id).into(),
                requested: None,
            }),
            files,
            provider_state: serde_json::Value::Null,
            metadata: BTreeMap::new(),
        })
    }
    async fn prepare_acquisition(
        &self,
        f: &ResolvedFile,
        _: &AcquireContext,
    ) -> Result<AcquisitionPlan> {
        Ok(AcquisitionPlan::Http {
            url: f.source["url"].as_str().unwrap_or_default().into(),
            headers: BTreeMap::new(),
            resume: true,
        })
    }
}
