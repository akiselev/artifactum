//! Shared provider conformance harness. This intentionally tests public provider
//! semantics rather than provider implementation details.

use std::{collections::BTreeSet, sync::Arc};

use artifactum_core::{ArtifactId, canonical_json};
use artifactum_metadata::MetadataStore;
use artifactum_resolver::{
    ArtifactProvider, ArtifactRequirement, DynProvider, ProviderCapabilities, ResolveContext,
};
use artifactum_store::{ArtifactStore, ContentStore};
use serde::{Deserialize, Serialize};
use thiserror::Error;

#[derive(Debug, Error)]
pub enum Error {
    #[error("provider conformance failure: {0}")] Conformance(String),
    #[error("core error: {0}")] Core(#[from] artifactum_core::Error),
    #[error("resolver error: {0}")] Resolver(#[from] artifactum_resolver::Error),
    #[error("store error: {0}")] Store(#[from] artifactum_store::Error),
    #[error("metadata error: {0}")] Metadata(#[from] artifactum_metadata::Error),
    #[error("serialization error: {0}")] Serde(#[from] serde_json::Error),
    #[error("I/O error: {0}")] Io(#[from] std::io::Error),
}
pub type Result<T, E = Error> = std::result::Result<T, E>;

#[derive(Clone, Debug, Default)]
pub struct ConformanceOptions {
    /// Re-resolve immediately and require byte-identical canonical Resolution JSON.
    pub stable_resolution: bool,
    pub require_nonempty: bool,
}
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ConformanceReport {
    pub provider: String,
    pub schemes: Vec<String>,
    pub files: usize,
    pub listed_files: Option<usize>,
    pub stable_resolution_checked: bool,
}
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct AcquisitionReport {
    pub provider: String,
    pub artifact: ArtifactId,
    pub source_observations: usize,
}

pub async fn check_provider(provider: &dyn ArtifactProvider, requirement: &ArtifactRequirement, context: &ResolveContext, options: &ConformanceOptions) -> Result<ConformanceReport> {
    let descriptor = provider.descriptor();
    if descriptor.name.trim().is_empty() { return Err(Error::Conformance("descriptor name is empty".into())); }
    if descriptor.schemes.is_empty() { return Err(Error::Conformance("provider advertises no schemes".into())); }
    if !descriptor.capabilities.resolve { return Err(Error::Conformance("provider must advertise resolve capability".into())); }
    let resolution = provider.resolve(requirement, context).await?;
    if resolution.provider != descriptor.name { return Err(Error::Conformance(format!("resolution provider `{}` != descriptor `{}`", resolution.provider, descriptor.name))); }
    if resolution.canonical_ref.trim().is_empty() { return Err(Error::Conformance("canonical_ref is empty".into())); }
    if options.require_nonempty && resolution.files.is_empty() { return Err(Error::Conformance("resolution unexpectedly contains no files".into())); }
    let mut paths = BTreeSet::new();
    for file in &resolution.files {
        if !paths.insert(file.path.to_string()) { return Err(Error::Conformance(format!("duplicate resolved path `{}`", file.path))); }
        if !requirement.selection.matches(file.path.as_str())? { return Err(Error::Conformance(format!("resolved path `{}` violates requirement selection", file.path))); }
        if descriptor.capabilities.acquire { let _ = provider.prepare_acquisition(file, &artifactum_resolver::AcquireContext { offline: context.offline, request_id: uuid_for_test(), profile: context.profile.clone() }).await?; }
    }
    if options.stable_resolution {
        let again = provider.resolve(requirement, context).await?;
        if canonical_json(&resolution)? != canonical_json(&again)? { return Err(Error::Conformance("immediate repeated resolution changed".into())); }
    }
    let listed_files = if descriptor.capabilities.list {
        let page = provider.list_files(requirement, None, context).await?;
        let listed = page.items.iter().map(|file| file.path.to_string()).collect::<BTreeSet<_>>();
        if listed != paths { return Err(Error::Conformance("list_files does not agree with resolve".into())); }
        Some(page.items.len())
    } else { None };
    if descriptor.capabilities.inspect { let inspected = provider.inspect(&requirement.reference, context).await?; if inspected.reference.scheme().is_empty() { return Err(Error::Conformance("inspect returned invalid reference".into())); } }
    Ok(ConformanceReport { provider: descriptor.name, schemes: descriptor.schemes, files: resolution.files.len(), listed_files, stable_resolution_checked: options.stable_resolution })
}

/// Run full provider -> resolver -> disposable CAS acquisition. This verifies
/// that provider output is accepted only through the host hashing boundary and
/// that the resulting artifact has a persisted source observation.
pub async fn acquire_case(provider: DynProvider, requirement: ArtifactRequirement) -> Result<AcquisitionReport> {
    let temp = tempfile::tempdir()?;
    let store = ArtifactStore::open(temp.path().join("store")).await?;
    let metadata = MetadataStore::open(temp.path().join("metadata.sqlite"))?;
    let descriptor = provider.descriptor();
    let resolver = artifactum_resolver::ArtifactResolver::builder().store(store.clone()).metadata(metadata.clone()).provider_dyn(provider)?.build().await?;
    let resolved = resolver.acquire(&requirement).await?;
    let _ = store.load_artifact(&resolved.artifact).await?;
    let observations = metadata.source_observations(&resolved.artifact)?.len();
    if observations == 0 { return Err(Error::Conformance("acquired artifact has no source observation".into())); }
    Ok(AcquisitionReport { provider: descriptor.name, artifact: resolved.artifact, source_observations: observations })
}

fn uuid_for_test() -> uuid::Uuid { uuid::Uuid::new_v4() }

/// Convenience for statically linked provider crates.
pub fn dynamic<P: ArtifactProvider>(provider: P) -> DynProvider { Arc::new(provider) }

#[must_use]
pub fn capability_names(capabilities: ProviderCapabilities) -> Vec<&'static str> {
    let pairs = [
        (capabilities.resolve, "resolve"), (capabilities.acquire, "acquire"),
        (capabilities.search, "search"), (capabilities.inspect, "inspect"),
        (capabilities.list, "list"), (capabilities.versions, "versions"),
        (capabilities.push, "push"), (capabilities.auth, "auth"), (capabilities.range, "range"),
    ];
    pairs.into_iter().filter_map(|(enabled, name)| enabled.then_some(name)).collect()
}
