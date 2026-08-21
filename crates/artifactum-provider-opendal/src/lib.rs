//! Shared OpenDAL provider implementation for Artifactum storage plugins.
//! Provider profiles are runtime configuration; persisted source state contains
//! only stable authority/version identity and never expanded credentials.

use artifactum_core::ArtifactPath;
use artifactum_resolver::{
    AcquireContext, AcquisitionPlan, ArtifactProvider, ArtifactRequirement, DigestSet, Error,
    ProviderCapabilities, ProviderDescriptor, ProviderProfile, Resolution, ResolveContext,
    ResolvedFile, ResolvedRevision, Result,
};
use async_trait::async_trait;
use futures_util::TryStreamExt;
use opendal::Operator;
use std::{collections::BTreeMap, path::Path};
use tokio::io::AsyncWriteExt;

#[derive(Clone, Debug)]
pub enum LocatorMode {
    Path,
    Authority {
        config_key: &'static str,
        revision_key: Option<&'static str>,
    },
}

#[derive(Clone, Debug)]
pub struct OpenDalProvider {
    name: &'static str,
    schemes: &'static [&'static str],
    service_scheme: &'static str,
    mode: LocatorMode,
    default_config: BTreeMap<String, String>,
    object_version: bool,
}

impl OpenDalProvider {
    #[must_use]
    pub fn new(
        name: &'static str,
        schemes: &'static [&'static str],
        service_scheme: &'static str,
        mode: LocatorMode,
    ) -> Self {
        Self {
            name,
            schemes,
            service_scheme,
            mode,
            default_config: BTreeMap::new(),
            object_version: false,
        }
    }
    #[must_use]
    pub fn default_config(mut self, key: impl Into<String>, value: impl Into<String>) -> Self {
        self.default_config.insert(key.into(), value.into());
        self
    }
    #[must_use]
    pub fn object_version(mut self, enabled: bool) -> Self {
        self.object_version = enabled;
        self
    }

    fn fail(&self, e: impl std::fmt::Display) -> Error {
        Error::Provider {
            provider: self.name.into(),
            message: e.to_string(),
        }
    }
    fn config_and_path(
        &self,
        requirement: &ArtifactRequirement,
        profile: Option<&ProviderProfile>,
    ) -> Result<(BTreeMap<String, String>, String, BTreeMap<String, String>)> {
        let mut config = self.default_config.clone();
        if let Some(profile) = profile {
            for (k, v) in &profile.config {
                config.insert(k.clone(), expand_env(v));
            }
        }
        let locator = requirement.reference.locator().trim_start_matches('/');
        let mut identity = BTreeMap::new();
        let path = match self.mode {
            LocatorMode::Path => locator.to_owned(),
            LocatorMode::Authority {
                config_key,
                revision_key,
            } => {
                let path = if config.contains_key(config_key) {
                    locator.to_owned()
                } else {
                    let (authority, path) = locator.split_once('/').unwrap_or((locator, ""));
                    config.insert(config_key.into(), authority.into());
                    identity.insert(config_key.into(), authority.into());
                    path.to_owned()
                };
                if let (Some(key), Some(revision)) = (revision_key, requirement.revision.as_ref()) {
                    config.insert(key.into(), revision.clone());
                    identity.insert(key.into(), revision.clone());
                }
                path
            }
        };
        Ok((
            config,
            if path.is_empty() { "/".into() } else { path },
            identity,
        ))
    }
    fn operator(&self, config: &BTreeMap<String, String>) -> Result<Operator> {
        opendal::install_default();
        Operator::via_iter(self.service_scheme, config.clone()).map_err(|e| self.fail(e))
    }
    fn source(
        path: &str,
        meta: &opendal::Metadata,
        identity: &BTreeMap<String, String>,
    ) -> serde_json::Value {
        serde_json::json!({"path":path,"version":meta.version(),"etag":meta.etag(),"identity_config":identity})
    }
    fn resolved_file(
        &self,
        path: &str,
        artifact_path: &str,
        meta: &opendal::Metadata,
        identity: &BTreeMap<String, String>,
    ) -> Result<ResolvedFile> {
        Ok(ResolvedFile {
            path: ArtifactPath::new(artifact_path)?,
            size: Some(meta.content_length()),
            digests: DigestSet(BTreeMap::new()),
            media_type: meta.content_type().map(str::to_owned),
            source: Self::source(path, meta, identity),
        })
    }
}

fn expand_env(value: &str) -> String {
    if let Some(name) = value.strip_prefix("${").and_then(|v| v.strip_suffix('}')) {
        std::env::var(name).unwrap_or_else(|_| value.into())
    } else {
        value.into()
    }
}

#[async_trait]
impl ArtifactProvider for OpenDalProvider {
    fn descriptor(&self) -> ProviderDescriptor {
        ProviderDescriptor {
            name: self.name.into(),
            version: env!("CARGO_PKG_VERSION").into(),
            schemes: self.schemes.iter().map(|s| (*s).into()).collect(),
            capabilities: ProviderCapabilities {
                resolve: true,
                acquire: true,
                list: true,
                auth: true,
                range: true,
                ..Default::default()
            },
        }
    }
    async fn resolve(
        &self,
        requirement: &ArtifactRequirement,
        context: &ResolveContext,
    ) -> Result<Resolution> {
        if context.offline {
            return Err(self.fail("cannot resolve remote storage while offline"));
        }
        let (config, path, identity) =
            self.config_and_path(requirement, context.profile.as_ref())?;
        let op = self.operator(&config)?;
        let mut files = Vec::new();
        let mut revision_id = requirement.revision.clone();
        let stat = if self.object_version {
            if let Some(version) = requirement.revision.as_deref() {
                op.stat_with(&path).version(version).await
            } else {
                op.stat(&path).await
            }
        } else {
            op.stat(&path).await
        };
        match stat {
            Ok(meta) if meta.is_file() => {
                let artifact_path = Path::new(&path)
                    .file_name()
                    .and_then(|v| v.to_str())
                    .unwrap_or("artifact");
                if requirement.selection.matches(artifact_path)? {
                    revision_id = meta
                        .version()
                        .map(str::to_owned)
                        .or_else(|| meta.etag().map(str::to_owned))
                        .or(revision_id);
                    files.push(self.resolved_file(&path, artifact_path, &meta, &identity)?);
                }
            }
            Err(error) if self.object_version && requirement.revision.is_some() => {
                return Err(self.fail(error));
            }
            Ok(_) if self.object_version && requirement.revision.is_some() => {
                return Err(self.fail("explicit object versions require a file reference"));
            }
            _ => {
                let prefix = if path == "/" {
                    String::new()
                } else if path.ends_with('/') {
                    path.clone()
                } else {
                    format!("{path}/")
                };
                let entries = op
                    .list_with(&prefix)
                    .recursive(true)
                    .await
                    .map_err(|e| self.fail(e))?;
                for entry in entries {
                    let meta = entry.metadata();
                    if !meta.is_file() {
                        continue;
                    }
                    let remote = entry.path();
                    let relative = remote
                        .strip_prefix(&prefix)
                        .unwrap_or(remote)
                        .trim_start_matches('/');
                    if relative.is_empty() || !requirement.selection.matches(relative)? {
                        continue;
                    }
                    files.push(self.resolved_file(remote, relative, meta, &identity)?);
                }
            }
        }
        files.sort_by(|a, b| a.path.as_str().cmp(b.path.as_str()));
        Ok(Resolution {
            provider: self.name.into(),
            canonical_ref: format!("{}:{}", self.schemes[0], requirement.reference.locator()),
            revision: revision_id.map(|id| ResolvedRevision {
                id,
                requested: requirement.revision.clone(),
            }),
            files,
            provider_state: serde_json::json!({"service":self.service_scheme}),
            metadata: BTreeMap::new(),
        })
    }
    async fn prepare_acquisition(
        &self,
        file: &ResolvedFile,
        _: &AcquireContext,
    ) -> Result<AcquisitionPlan> {
        Ok(AcquisitionPlan::ProviderManaged {
            state: file.source.clone(),
        })
    }
    async fn acquire_managed(
        &self,
        file: &ResolvedFile,
        _: &AcquisitionPlan,
        destination: &Path,
        context: &AcquireContext,
    ) -> Result<u64> {
        if context.offline {
            return Err(self.fail("cannot acquire remote storage while offline"));
        }
        let mut config = self.default_config.clone();
        if let Some(profile) = context.profile.as_ref() {
            for (k, v) in &profile.config {
                config.insert(k.clone(), expand_env(v));
            }
        }
        if let Some(identity) = file
            .source
            .get("identity_config")
            .and_then(|v| v.as_object())
        {
            for (k, v) in identity {
                if let Some(v) = v.as_str() {
                    config.insert(k.clone(), v.to_owned());
                }
            }
        }
        let op = self.operator(&config)?;
        let remote = file
            .source
            .get("path")
            .and_then(|v| v.as_str())
            .ok_or_else(|| self.fail("resolved file missing source.path"))?;
        let version = file.source.get("version").and_then(|v| v.as_str());
        let mut rb = op.reader_with(remote).concurrent(4).chunk(8 * 1024 * 1024);
        if let Some(v) = version {
            rb = rb.version(v)
        }
        let reader = rb.await.map_err(|e| self.fail(e))?;
        let mut stream = reader
            .into_bytes_stream(..)
            .await
            .map_err(|e| self.fail(e))?;
        let mut output = tokio::fs::File::create(destination).await?;
        let mut written = 0u64;
        while let Some(chunk) = stream.try_next().await.map_err(|e| self.fail(e))? {
            output.write_all(&chunk).await?;
            written = written.saturating_add(chunk.len() as u64)
        }
        output.sync_all().await?;
        Ok(written)
    }
}
