use artifactum_core::ArtifactPath;
use artifactum_resolver::{
    AcquireContext, AcquisitionPlan, ArtifactProvider, ArtifactRequirement, DigestSet, Error,
    ProviderCapabilities, ProviderDescriptor, Resolution, ResolveContext, ResolvedFile,
    ResolvedRevision, Result,
};
use async_trait::async_trait;
use std::collections::BTreeMap;
pub struct GitLabProvider;
#[async_trait]
impl ArtifactProvider for GitLabProvider {
    fn descriptor(&self) -> ProviderDescriptor {
        ProviderDescriptor {
            name: "gitlab".into(),
            version: env!("CARGO_PKG_VERSION").into(),
            schemes: vec!["gitlab".into()],
            capabilities: ProviderCapabilities {
                resolve: true,
                acquire: true,
                ..Default::default()
            },
        }
    }
    async fn resolve(&self, r: &ArtifactRequirement, c: &ResolveContext) -> Result<Resolution> {
        let (raw, path) = r
            .reference
            .locator()
            .split_once('#')
            .ok_or_else(|| Error::Provider {
                provider: "gitlab".into(),
                message: "expected gitlab:group/project#path".into(),
            })?;
        let rev = r.revision.as_deref().unwrap_or("main");
        let base = c
            .profile
            .as_ref()
            .and_then(|p| p.config.get("base_url"))
            .cloned()
            .unwrap_or_else(|| "https://gitlab.com".into());
        let url = format!("{base}/{raw}/-/raw/{rev}/{path}");
        Ok(Resolution {
            provider: "gitlab".into(),
            canonical_ref: format!("gitlab:{raw}@{rev}#{path}"),
            revision: Some(ResolvedRevision {
                id: rev.into(),
                requested: r.revision.clone(),
            }),
            files: vec![ResolvedFile {
                path: ArtifactPath::new(path.rsplit('/').next().unwrap_or("file"))?,
                size: None,
                digests: DigestSet(BTreeMap::new()),
                media_type: None,
                source: serde_json::json!({"url":url}),
            }],
            provider_state: serde_json::json!({"base_url":base}),
            metadata: BTreeMap::new(),
        })
    }
    async fn prepare_acquisition(
        &self,
        f: &ResolvedFile,
        c: &AcquireContext,
    ) -> Result<AcquisitionPlan> {
        let mut headers = BTreeMap::new();
        if let Some(token) = c
            .profile
            .as_ref()
            .and_then(|p| p.config.get("token"))
            .cloned()
            .or_else(|| std::env::var("GITLAB_TOKEN").ok())
        {
            headers.insert("PRIVATE-TOKEN".into(), token);
        }
        Ok(AcquisitionPlan::Http {
            url: f.source["url"].as_str().unwrap_or_default().into(),
            headers,
            resume: true,
        })
    }
}
