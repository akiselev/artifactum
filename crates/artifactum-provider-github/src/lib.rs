use std::collections::BTreeMap;

use artifactum_core::{
    provider_error, AcquireContext, AcquisitionPlan, ArtifactPath, ArtifactProvider, ArtifactRequirement,
    Digest, DigestSet, ProviderCapabilities, ProviderDescriptor, ResolveContext, ResolvedFile,
    ResolvedRevision, Resolution, Selection,
};
use async_trait::async_trait;
use reqwest::{header, Client, RequestBuilder};
use serde::Deserialize;
use url::Url;

#[derive(Clone, Debug)]
pub struct GitHubProvider {
    client: Client,
    api_base: Url,
}

impl Default for GitHubProvider {
    fn default() -> Self {
        Self::new()
    }
}

impl GitHubProvider {
    #[must_use]
    pub fn new() -> Self {
        let api_base = std::env::var("GITHUB_API_URL")
            .ok()
            .and_then(|value| Url::parse(&value).ok())
            .unwrap_or_else(|| Url::parse("https://api.github.com/").expect("constant URL is valid"));
        let client = Client::builder()
            .user_agent(concat!("artifactum/", env!("CARGO_PKG_VERSION")))
            .build()
            .expect("reqwest client configuration should be valid");
        Self { client, api_base }
    }

    fn authenticated(&self, request: RequestBuilder) -> RequestBuilder {
        if let Ok(token) = std::env::var("GITHUB_TOKEN") {
            request.bearer_auth(token)
        } else if let Ok(token) = std::env::var("GH_TOKEN") {
            request.bearer_auth(token)
        } else {
            request
        }
    }

    fn release_url(&self, owner: &str, repo: &str, tag: Option<&str>) -> artifactum_core::Result<Url> {
        let mut url = self.api_base.clone();
        {
            let mut segments = url
                .path_segments_mut()
                .map_err(|_| provider_error("github", "GitHub API base URL cannot be a base"))?;
            segments.pop_if_empty();
            segments.extend(["repos", owner, repo, "releases"]);
            match tag {
                Some(tag) => segments.extend(["tags", tag]),
                None => segments.push("latest"),
            }
        }
        Ok(url)
    }
}

#[async_trait]
impl ArtifactProvider for GitHubProvider {
    fn descriptor(&self) -> ProviderDescriptor {
        ProviderDescriptor {
            name: "github".into(),
            version: env!("CARGO_PKG_VERSION").into(),
            schemes: vec!["github".into(), "gh".into()],
            capabilities: ProviderCapabilities {
                resolve: true,
                acquire: true,
                auth: true,
                versions: true,
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
            return Err(provider_error("github", "cannot resolve a GitHub release while offline"));
        }
        let parsed = ParsedReference::parse(requirement.reference.locator(), requirement.revision.as_deref())?;
        let url = self.release_url(&parsed.owner, &parsed.repo, parsed.tag.as_deref())?;
        let release: Release = self
            .authenticated(
                self.client
                    .get(url)
                    .header(header::ACCEPT, "application/vnd.github+json")
                    .header("X-GitHub-Api-Version", "2022-11-28"),
            )
            .send()
            .await
            .map_err(|error| provider_error("github", error))?
            .error_for_status()
            .map_err(|error| provider_error("github", error))?
            .json()
            .await
            .map_err(|error| provider_error("github", error))?;

        let selection = requirement.selection.compile()?;
        let asset_selection = parsed
            .asset_glob
            .as_ref()
            .map(|pattern| Selection {
                include: vec![pattern.clone()],
                exclude: Vec::new(),
            })
            .transpose_compile()?;

        let mut files = Vec::new();
        for asset in release.assets {
            if !selection.matches(&asset.name)
                || asset_selection
                    .as_ref()
                    .is_some_and(|selection| !selection.matches(&asset.name))
            {
                continue;
            }
            let mut digests = DigestSet::default();
            if let Some(raw) = asset.digest.as_deref() {
                if let Ok(digest) = raw.parse::<Digest>() {
                    digests.insert(digest);
                }
            }
            files.push(ResolvedFile {
                path: ArtifactPath::new(&asset.name)?,
                size: Some(asset.size),
                digests,
                media_type: asset.content_type,
                source: serde_json::json!({
                    "asset_id": asset.id,
                    "api_url": asset.url,
                    "browser_download_url": asset.browser_download_url,
                }),
            });
        }
        files.sort_by(|a, b| a.path.cmp(&b.path));

        Ok(Resolution {
            provider: "github".into(),
            canonical_ref: format!("github:{}/{}@{}", parsed.owner, parsed.repo, release.tag_name),
            revision: Some(ResolvedRevision {
                id: format!("release:{}:{}", release.id, release.tag_name),
                requested: parsed.tag,
            }),
            files,
            provider_state: serde_json::json!({
                "release_id": release.id,
                "tag": release.tag_name,
                "target_commitish": release.target_commitish,
            }),
            metadata: BTreeMap::new(),
        })
    }

    async fn prepare_acquisition(
        &self,
        file: &ResolvedFile,
        context: &AcquireContext,
    ) -> artifactum_core::Result<AcquisitionPlan> {
        if context.offline { return Err(provider_error("github", "cannot acquire a GitHub release asset while offline")); }
        let api_url=file.source.get("api_url").and_then(serde_json::Value::as_str)
            .ok_or_else(||provider_error("github","resolved file is missing source.api_url"))?;
        let mut headers=BTreeMap::from([(String::from("Accept"),String::from("application/octet-stream")),(String::from("X-GitHub-Api-Version"),String::from("2022-11-28"))]);
        if let Ok(token)=std::env::var("GITHUB_TOKEN").or_else(|_|std::env::var("GH_TOKEN")){headers.insert("Authorization".into(),format!("Bearer {token}"));}
        Ok(AcquisitionPlan::Http(artifactum_core::HttpAcquisition{url:api_url.to_owned(),headers,resume:true}))
    }
}

trait SelectionOptionExt {
    fn transpose_compile(self) -> artifactum_core::Result<Option<artifactum_core::CompiledSelection>>;
}

impl SelectionOptionExt for Option<Selection> {
    fn transpose_compile(self) -> artifactum_core::Result<Option<artifactum_core::CompiledSelection>> {
        self.map(|selection| selection.compile()).transpose()
    }
}

#[derive(Debug)]
struct ParsedReference {
    owner: String,
    repo: String,
    tag: Option<String>,
    asset_glob: Option<String>,
}

impl ParsedReference {
    fn parse(locator: &str, explicit_revision: Option<&str>) -> artifactum_core::Result<Self> {
        let (base, fragment) = locator.split_once('#').map_or((locator, None), |(a, b)| (a, Some(b)));
        let (repo_path, inline_tag) = base
            .rsplit_once('@')
            .map_or((base, None), |(path, tag)| (path, Some(tag)));
        let mut pieces = repo_path.split('/');
        let owner = pieces.next().unwrap_or_default();
        let repo = pieces.next().unwrap_or_default();
        if owner.is_empty() || repo.is_empty() || pieces.next().is_some() {
            return Err(provider_error(
                "github",
                "expected github:<owner>/<repo>[@tag][#asset=<glob>]",
            ));
        }
        let asset_glob = fragment.and_then(|fragment| {
            fragment
                .split('&')
                .find_map(|part| part.strip_prefix("asset=").map(ToOwned::to_owned))
        });
        Ok(Self {
            owner: owner.to_owned(),
            repo: repo.to_owned(),
            tag: explicit_revision
                .map(ToOwned::to_owned)
                .or_else(|| inline_tag.map(ToOwned::to_owned)),
            asset_glob,
        })
    }
}

#[derive(Debug, Deserialize)]
struct Release {
    id: u64,
    tag_name: String,
    target_commitish: String,
    assets: Vec<ReleaseAsset>,
}

#[derive(Debug, Deserialize)]
struct ReleaseAsset {
    id: u64,
    name: String,
    size: u64,
    url: String,
    browser_download_url: String,
    #[serde(default)]
    digest: Option<String>,
    #[serde(default)]
    content_type: Option<String>,
}
