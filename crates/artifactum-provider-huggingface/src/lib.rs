use std::{collections::BTreeMap, path::Path};

use artifactum_core::{
    provider_error, AcquireContext, Acquisition, ArtifactPath, ArtifactProvider, ArtifactRef,
    ArtifactRequirement, Digest, DigestSet, ProviderCapabilities, ProviderDescriptor, ResolveContext,
    ResolvedFile, ResolvedRevision, Resolution, SearchRequest, SearchResult,
};
use async_trait::async_trait;
use reqwest::{header, Client, RequestBuilder};
use serde::Deserialize;
use url::Url;

#[derive(Clone, Debug)]
pub struct HuggingFaceProvider {
    client: Client,
    endpoint: Url,
}

impl Default for HuggingFaceProvider {
    fn default() -> Self {
        Self::new()
    }
}

impl HuggingFaceProvider {
    #[must_use]
    pub fn new() -> Self {
        let endpoint = std::env::var("HF_ENDPOINT")
            .ok()
            .and_then(|value| Url::parse(&value).ok())
            .unwrap_or_else(|| Url::parse("https://huggingface.co/").expect("constant URL is valid"));
        let client = Client::builder()
            .user_agent(concat!("artifactum/", env!("CARGO_PKG_VERSION")))
            .build()
            .expect("reqwest client configuration should be valid");
        Self { client, endpoint }
    }

    fn authenticated(&self, request: RequestBuilder) -> RequestBuilder {
        if let Ok(token) = std::env::var("HF_TOKEN") {
            request.bearer_auth(token)
        } else {
            request
        }
    }

    fn api_info_url(
        &self,
        repo_type: RepoType,
        repo: &str,
        revision: &str,
    ) -> artifactum_core::Result<Url> {
        let mut url = self.endpoint.clone();
        {
            let mut segments = url
                .path_segments_mut()
                .map_err(|_| provider_error("huggingface", "HF endpoint cannot be a base URL"))?;
            segments.pop_if_empty();
            segments.extend(["api", repo_type.api_segment()]);
            for segment in repo.split('/') {
                segments.push(segment);
            }
            segments.extend(["revision", revision]);
        }
        url.query_pairs_mut().append_pair("blobs", "true");
        Ok(url)
    }

    fn download_url(
        &self,
        repo_type: RepoType,
        repo: &str,
        revision: &str,
        filename: &str,
    ) -> artifactum_core::Result<Url> {
        let mut url = self.endpoint.clone();
        {
            let mut segments = url
                .path_segments_mut()
                .map_err(|_| provider_error("huggingface", "HF endpoint cannot be a base URL"))?;
            segments.pop_if_empty();
            if let Some(prefix) = repo_type.download_prefix() {
                segments.push(prefix);
            }
            for segment in repo.split('/') {
                segments.push(segment);
            }
            segments.extend(["resolve", revision]);
            for segment in filename.split('/') {
                segments.push(segment);
            }
        }
        url.query_pairs_mut().append_pair("download", "true");
        Ok(url)
    }
}

#[async_trait]
impl ArtifactProvider for HuggingFaceProvider {
    fn descriptor(&self) -> ProviderDescriptor {
        ProviderDescriptor {
            name: "huggingface".into(),
            version: env!("CARGO_PKG_VERSION").into(),
            schemes: vec!["huggingface".into(), "hf".into()],
            capabilities: ProviderCapabilities {
                resolve: true,
                acquire: true,
                search: true,
                list: true,
                versions: true,
                auth: true,
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
            return Err(provider_error("huggingface", "cannot resolve a Hub repository while offline"));
        }
        let parsed = ParsedReference::parse(requirement.reference.locator(), requirement.revision.as_deref())?;
        let requested_revision = parsed.revision.clone().unwrap_or_else(|| "main".to_owned());
        let info_url = self.api_info_url(parsed.repo_type, &parsed.repo, &requested_revision)?;
        let info: RepoInfo = self
            .authenticated(self.client.get(info_url))
            .send()
            .await
            .map_err(|error| provider_error("huggingface", error))?
            .error_for_status()
            .map_err(|error| provider_error("huggingface", error))?
            .json()
            .await
            .map_err(|error| provider_error("huggingface", error))?;
        let resolved_revision = info.sha.unwrap_or_else(|| requested_revision.clone());
        let selection = requirement.selection.compile()?;
        let mut files = Vec::new();

        for sibling in info.siblings {
            if !selection.matches(&sibling.rfilename) {
                continue;
            }
            let mut digests = DigestSet::default();
            if let Some(lfs) = sibling.lfs.as_ref() {
                if let Some(sha256) = lfs.sha256.as_ref() {
                    if let Ok(digest) = Digest::sha256(sha256.clone()) {
                        digests.insert(digest);
                    }
                }
            }
            let size = sibling
                .size
                .or_else(|| sibling.lfs.as_ref().and_then(|lfs| lfs.size));
            let download_url = self.download_url(
                parsed.repo_type,
                &parsed.repo,
                &resolved_revision,
                &sibling.rfilename,
            )?;
            files.push(ResolvedFile {
                path: ArtifactPath::new(&sibling.rfilename)?,
                size,
                digests,
                media_type: None,
                source: serde_json::json!({
                    "url": download_url.as_str(),
                    "repo": &parsed.repo,
                    "repo_type": parsed.repo_type.as_str(),
                    "revision": &resolved_revision,
                    "filename": &sibling.rfilename,
                }),
            });
        }
        files.sort_by(|a, b| a.path.cmp(&b.path));

        let type_prefix = parsed.repo_type.reference_prefix();
        Ok(Resolution {
            provider: "huggingface".into(),
            canonical_ref: format!(
                "huggingface:{type_prefix}{}@{}",
                parsed.repo, resolved_revision
            ),
            revision: Some(ResolvedRevision {
                id: resolved_revision.clone(),
                requested: Some(requested_revision),
            }),
            files,
            provider_state: serde_json::json!({
                "repo": parsed.repo,
                "repo_type": parsed.repo_type.as_str(),
                "revision": resolved_revision,
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
            return Err(provider_error("huggingface", "cannot acquire a Hub file while offline"));
        }
        let url = file
            .source
            .get("url")
            .and_then(serde_json::Value::as_str)
            .ok_or_else(|| provider_error("huggingface", "resolved file is missing source.url"))?;
        let response = self
            .authenticated(self.client.get(url))
            .send()
            .await
            .map_err(|error| provider_error("huggingface", error))?
            .error_for_status()
            .map_err(|error| provider_error("huggingface", error))?;
        let bytes_written = artifactum_transport_http::write_response(response, destination)
            .await
            .map_err(|error| provider_error("huggingface", error))?;
        Ok(Acquisition {
            bytes_written: Some(bytes_written),
            metadata: BTreeMap::new(),
        })
    }

    async fn search(
        &self,
        request: &SearchRequest,
        context: &ResolveContext,
    ) -> artifactum_core::Result<Vec<SearchResult>> {
        if context.offline {
            return Err(provider_error("huggingface", "cannot search the Hub while offline"));
        }
        let repo_type = request
            .metadata
            .get("repo_type")
            .and_then(serde_json::Value::as_str)
            .and_then(RepoType::from_str)
            .unwrap_or(RepoType::Model);
        let mut url = self.endpoint.clone();
        {
            let mut segments = url
                .path_segments_mut()
                .map_err(|_| provider_error("huggingface", "HF endpoint cannot be a base URL"))?;
            segments.pop_if_empty();
            segments.extend(["api", repo_type.api_segment()]);
        }
        url.query_pairs_mut()
            .append_pair("search", &request.query)
            .append_pair("limit", &request.limit.unwrap_or(20).to_string());
        let results: Vec<SearchRepo> = self
            .authenticated(self.client.get(url).header(header::ACCEPT, "application/json"))
            .send()
            .await
            .map_err(|error| provider_error("huggingface", error))?
            .error_for_status()
            .map_err(|error| provider_error("huggingface", error))?
            .json()
            .await
            .map_err(|error| provider_error("huggingface", error))?;

        results
            .into_iter()
            .map(|result| {
                Ok(SearchResult {
                    reference: ArtifactRef::new(
                        "huggingface",
                        format!("{}{}", repo_type.reference_prefix(), result.id),
                    )?,
                    name: result.id,
                    description: None,
                    metadata: BTreeMap::new(),
                })
            })
            .collect()
    }
}

#[derive(Clone, Copy, Debug)]
enum RepoType {
    Model,
    Dataset,
    Space,
}

impl RepoType {
    fn from_str(value: &str) -> Option<Self> {
        match value {
            "model" | "models" => Some(Self::Model),
            "dataset" | "datasets" => Some(Self::Dataset),
            "space" | "spaces" => Some(Self::Space),
            _ => None,
        }
    }

    const fn as_str(self) -> &'static str {
        match self {
            Self::Model => "model",
            Self::Dataset => "dataset",
            Self::Space => "space",
        }
    }

    const fn api_segment(self) -> &'static str {
        match self {
            Self::Model => "models",
            Self::Dataset => "datasets",
            Self::Space => "spaces",
        }
    }

    const fn download_prefix(self) -> Option<&'static str> {
        match self {
            Self::Model => None,
            Self::Dataset => Some("datasets"),
            Self::Space => Some("spaces"),
        }
    }

    const fn reference_prefix(self) -> &'static str {
        match self {
            Self::Model => "",
            Self::Dataset => "dataset:",
            Self::Space => "space:",
        }
    }
}

#[derive(Debug)]
struct ParsedReference {
    repo_type: RepoType,
    repo: String,
    revision: Option<String>,
}

impl ParsedReference {
    fn parse(locator: &str, explicit_revision: Option<&str>) -> artifactum_core::Result<Self> {
        let (repo_type, rest) = if let Some(rest) = locator.strip_prefix("dataset:") {
            (RepoType::Dataset, rest)
        } else if let Some(rest) = locator.strip_prefix("space:") {
            (RepoType::Space, rest)
        } else if let Some(rest) = locator.strip_prefix("model:") {
            (RepoType::Model, rest)
        } else {
            (RepoType::Model, locator)
        };
        let (repo, inline_revision) = rest
            .rsplit_once('@')
            .map_or((rest, None), |(repo, revision)| (repo, Some(revision)));
        if repo.is_empty() || !repo.contains('/') {
            return Err(provider_error(
                "huggingface",
                "expected hf:<owner>/<repo>[@revision], hf:dataset:<owner>/<repo>, or hf:space:<owner>/<repo>",
            ));
        }
        Ok(Self {
            repo_type,
            repo: repo.to_owned(),
            revision: explicit_revision
                .map(ToOwned::to_owned)
                .or_else(|| inline_revision.map(ToOwned::to_owned)),
        })
    }
}

#[derive(Debug, Deserialize)]
struct RepoInfo {
    #[serde(default)]
    sha: Option<String>,
    #[serde(default)]
    siblings: Vec<Sibling>,
}

#[derive(Debug, Deserialize)]
struct Sibling {
    rfilename: String,
    #[serde(default)]
    size: Option<u64>,
    #[serde(default)]
    lfs: Option<LfsInfo>,
}

#[derive(Debug, Deserialize)]
struct LfsInfo {
    #[serde(default)]
    sha256: Option<String>,
    #[serde(default)]
    size: Option<u64>,
}

#[derive(Debug, Deserialize)]
struct SearchRepo {
    id: String,
}
