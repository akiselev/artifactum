use std::{collections::BTreeMap, path::Path};

use artifactum_core::{
    provider_error, AcquireContext, Acquisition, ArtifactPath, ArtifactProvider, ArtifactRequirement,
    Digest, DigestSet, ProviderCapabilities, ProviderDescriptor, ResolveContext, ResolvedFile,
    ResolvedRevision, Resolution,
};
use async_trait::async_trait;
use reqwest::{header, Client};
use url::Url;

#[derive(Clone, Debug)]
pub struct HttpProvider {
    client: Client,
}

impl Default for HttpProvider {
    fn default() -> Self {
        Self::new()
    }
}

impl HttpProvider {
    #[must_use]
    pub fn new() -> Self {
        let client = Client::builder()
            .user_agent(concat!("artifactum/", env!("CARGO_PKG_VERSION")))
            .build()
            .expect("reqwest client configuration should be valid");
        Self { client }
    }
}

#[async_trait]
impl ArtifactProvider for HttpProvider {
    fn descriptor(&self) -> ProviderDescriptor {
        ProviderDescriptor {
            name: "http".into(),
            version: env!("CARGO_PKG_VERSION").into(),
            schemes: vec!["http".into(), "https".into()],
            capabilities: ProviderCapabilities {
                resolve: true,
                acquire: true,
                range: true,
                ..ProviderCapabilities::default()
            },
            metadata: BTreeMap::new(),
        }
    }

    async fn resolve(
        &self,
        requirement: &ArtifactRequirement,
        context: &ResolveContext,
    ) -> artifactum_core::Result<Resolution> {
        if context.offline {
            return Err(provider_error("http", "cannot resolve an HTTP artifact while offline"));
        }
        let raw_url = requirement.reference.to_string();
        let mut url = Url::parse(&raw_url).map_err(|error| provider_error("http", error))?;
        let expected = parse_integrity_fragment(url.fragment())?;
        url.set_fragment(None);

        let response = self
            .client
            .head(url.clone())
            .send()
            .await
            .map_err(|error| provider_error("http", error))?
            .error_for_status()
            .map_err(|error| provider_error("http", error))?;
        let final_url = response.url().clone();
        let size = response
            .headers()
            .get(header::CONTENT_LENGTH)
            .and_then(|value| value.to_str().ok())
            .and_then(|value| value.parse::<u64>().ok());
        let etag = response
            .headers()
            .get(header::ETAG)
            .and_then(|value| value.to_str().ok())
            .map(ToOwned::to_owned);
        let last_modified = response
            .headers()
            .get(header::LAST_MODIFIED)
            .and_then(|value| value.to_str().ok())
            .map(ToOwned::to_owned);

        let name = final_url
            .path_segments()
            .and_then(|mut segments| segments.next_back())
            .filter(|name| !name.is_empty())
            .unwrap_or("artifact");
        let path = ArtifactPath::new(name)?;
        let selection = requirement.selection.compile()?;
        let files = if selection.matches(path.as_str()) {
            let mut digests = DigestSet::default();
            if let Some(digest) = expected {
                digests.insert(digest);
            }
            vec![ResolvedFile {
                path,
                size,
                digests,
                media_type: response
                    .headers()
                    .get(header::CONTENT_TYPE)
                    .and_then(|value| value.to_str().ok())
                    .map(ToOwned::to_owned),
                source: serde_json::json!({ "url": final_url.as_str() }),
            }]
        } else {
            Vec::new()
        };

        let revision = etag.clone().or(last_modified.clone()).map(|id| ResolvedRevision {
            id,
            requested: requirement.revision.clone(),
        });
        Ok(Resolution {
            provider: "http".into(),
            canonical_ref: final_url.to_string(),
            revision,
            files,
            provider_state: serde_json::json!({
                "etag": etag,
                "last_modified": last_modified,
            }),
            metadata: BTreeMap::new(),
        })
    }

    async fn acquire(
        &self,
        file: &ResolvedFile,
        destination: &Path,
        context: &AcquireContext,
    ) -> artifactum_core::Result<Acquisition> {
        if context.offline {
            return Err(provider_error("http", "cannot acquire an HTTP artifact while offline"));
        }
        let url = file
            .source
            .get("url")
            .and_then(serde_json::Value::as_str)
            .ok_or_else(|| provider_error("http", "resolved file is missing source.url"))?;
        let response = self
            .client
            .get(url)
            .send()
            .await
            .map_err(|error| provider_error("http", error))?
            .error_for_status()
            .map_err(|error| provider_error("http", error))?;
        let bytes_written = artifactum_transport_http::write_response(response, destination)
            .await
            .map_err(|error| provider_error("http", error))?;
        Ok(Acquisition {
            bytes_written: Some(bytes_written),
            metadata: BTreeMap::new(),
        })
    }
}

fn parse_integrity_fragment(fragment: Option<&str>) -> artifactum_core::Result<Option<Digest>> {
    let Some(fragment) = fragment else {
        return Ok(None);
    };
    for item in fragment.split('&') {
        if let Some(value) = item.strip_prefix("sha256=") {
            return Ok(Some(Digest::sha256(value.to_owned())?));
        }
    }
    Ok(None)
}
