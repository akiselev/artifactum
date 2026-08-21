use artifactum_core::ArtifactPath;
use artifactum_provider_api::ApiClient;
use artifactum_resolver::{
    AcquireContext, AcquisitionPlan, ArtifactProvider, ArtifactRequirement, DigestSet,
    ProviderCapabilities, ProviderDescriptor, Resolution, ResolveContext, ResolvedFile,
    ResolvedRevision, Result,
};
use async_trait::async_trait;
use serde::Deserialize;
use std::collections::BTreeMap;
#[derive(Clone, Default)]
pub struct ZenodoProvider {
    api: ApiClient,
}
#[derive(Deserialize)]
struct Record {
    id: u64,
    #[serde(default)]
    files: Vec<File>,
}
#[derive(Deserialize)]
struct File {
    key: String,
    size: u64,
    checksum: Option<String>,
    links: Links,
}
#[derive(Deserialize)]
struct Links {
    self_: Option<String>,
    content: Option<String>,
}
#[async_trait]
impl ArtifactProvider for ZenodoProvider {
    fn descriptor(&self) -> ProviderDescriptor {
        ProviderDescriptor {
            name: "zenodo".into(),
            version: env!("CARGO_PKG_VERSION").into(),
            schemes: vec!["zenodo".into()],
            capabilities: ProviderCapabilities {
                resolve: true,
                acquire: true,
                ..Default::default()
            },
        }
    }
    async fn resolve(&self, r: &ArtifactRequirement, _: &ResolveContext) -> Result<Resolution> {
        let id = r.reference.locator().trim_start_matches("record:");
        let url = format!("https://zenodo.org/api/records/{id}");
        let v: serde_json::Value = self.api.get_json("zenodo", &url, &BTreeMap::new()).await?;
        let rid = v["id"].as_u64().unwrap_or_else(|| id.parse().unwrap_or(0));
        let mut files = Vec::new();
        if let Some(arr) = v["files"].as_array() {
            for f in arr {
                let name = f["key"].as_str().unwrap_or("file");
                let download = f["links"]["self"]
                    .as_str()
                    .or_else(|| f["links"]["content"].as_str())
                    .unwrap_or_default();
                let mut ds = BTreeMap::new();
                if let Some(c) = f["checksum"]
                    .as_str()
                    .and_then(|c| c.strip_prefix("sha256:"))
                {
                    ds.insert("sha256".into(), c.into());
                }
                files.push(ResolvedFile {
                    path: ArtifactPath::new(name)?,
                    size: f["size"].as_u64(),
                    digests: DigestSet(ds),
                    media_type: None,
                    source: serde_json::json!({"url":download}),
                });
            }
        }
        Ok(Resolution {
            provider: "zenodo".into(),
            canonical_ref: format!("zenodo:{rid}"),
            revision: Some(ResolvedRevision {
                id: rid.to_string(),
                requested: Some(id.into()),
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
