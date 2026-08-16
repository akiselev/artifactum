use std::{collections::BTreeMap, path::Path};

use artifactum_core::{ArtifactPath, Digest};
use artifactum_resolver::{
    access_required, AccessRequirement, AcquireContext, AcquisitionPlan, ArtifactProvider,
    ArtifactRequirement, DigestSet, Error, ProviderCapabilities, ProviderDescriptor,
    ProviderProfile, ResolveContext, ResolvedFile, ResolvedRevision, Resolution, Result,
};
use async_trait::async_trait;
use oci_client::{client::Client, manifest::OciDescriptor, secrets::RegistryAuth, Reference};

#[derive(Clone, Debug, Default)]
pub struct OciProvider;

fn auth(profile: Option<&ProviderProfile>) -> RegistryAuth {
    let user_env = profile.and_then(|p| p.config.get("username_env")).map(String::as_str).unwrap_or("OCI_USERNAME");
    let pass_env = profile.and_then(|p| p.config.get("password_env")).map(String::as_str).unwrap_or("OCI_PASSWORD");
    match (std::env::var(user_env).ok(), std::env::var(pass_env).ok()) {
        (Some(user), Some(password)) => RegistryAuth::Basic(user, password),
        _ => RegistryAuth::Anonymous,
    }
}
fn provider_error(message: impl std::fmt::Display) -> Error { Error::Provider { provider: "oci".into(), message: message.to_string() } }
fn oci_error(error: impl std::fmt::Display) -> Error {
    let text = error.to_string(); let lower = text.to_ascii_lowercase();
    if lower.contains("unauthor") || text.contains("401") || text.contains("403") { access_required("oci", AccessRequirement::Authentication, text) } else { provider_error(text) }
}

#[async_trait]
impl ArtifactProvider for OciProvider {
    fn descriptor(&self) -> ProviderDescriptor {
        ProviderDescriptor { name: "oci".into(), version: env!("CARGO_PKG_VERSION").into(), schemes: vec!["oci".into()], capabilities: ProviderCapabilities { resolve: true, acquire: true, list: true, auth: true, ..Default::default() } }
    }
    async fn resolve(&self, requirement: &ArtifactRequirement, context: &ResolveContext) -> Result<Resolution> {
        if context.offline { return Err(provider_error("cannot resolve while offline")); }
        let reference: Reference = requirement.reference.locator().parse().map_err(provider_error)?;
        let client = Client::new(Default::default());
        let (manifest, digest) = client.pull_image_manifest(&reference, &auth(context.profile.as_ref())).await.map_err(oci_error)?;
        let mut files = Vec::new();
        for (index, layer) in manifest.layers.iter().enumerate() {
            let title = layer.annotations.as_ref().and_then(|annotations| annotations.get("org.opencontainers.image.title")).cloned().unwrap_or_else(|| format!("layer-{index}-{}", layer.digest.replace(':', "-")));
            if !requirement.selection.matches(&title)? { continue; }
            let mut digests = DigestSet(BTreeMap::new());
            if let Ok(layer_digest) = layer.digest.parse::<Digest>() { digests.0.insert(layer_digest.algorithm, layer_digest.value); }
            files.push(ResolvedFile {
                path: ArtifactPath::new(title)?, size: u64::try_from(layer.size).ok(), digests,
                media_type: Some(layer.media_type.clone()),
                source: serde_json::json!({"reference": reference.whole(), "descriptor": layer}),
            });
        }
        Ok(Resolution {
            provider: "oci".into(), canonical_ref: format!("oci:{}", reference.whole()),
            revision: Some(ResolvedRevision { id: digest, requested: requirement.revision.clone() }),
            files, provider_state: serde_json::Value::Null, metadata: BTreeMap::new(),
        })
    }
    async fn prepare_acquisition(&self, file: &ResolvedFile, _: &AcquireContext) -> Result<AcquisitionPlan> { Ok(AcquisitionPlan::ProviderManaged { state: file.source.clone() }) }
    async fn acquire_managed(&self, file: &ResolvedFile, _: &AcquisitionPlan, destination: &Path, context: &AcquireContext) -> Result<u64> {
        if context.offline { return Err(provider_error("cannot acquire while offline")); }
        let reference_text = file.source.get("reference").and_then(|value| value.as_str()).ok_or_else(|| provider_error("missing reference"))?;
        let reference: Reference = reference_text.parse().map_err(provider_error)?;
        let descriptor: OciDescriptor = serde_json::from_value(file.source.get("descriptor").cloned().ok_or_else(|| provider_error("missing layer descriptor"))?).map_err(provider_error)?;
        let client = Client::new(Default::default());
        client.store_auth_if_needed(reference.registry(), &auth(context.profile.as_ref())).await;
        let output = tokio::fs::File::create(destination).await?;
        client.pull_blob(&reference, &descriptor, output).await.map_err(oci_error)?;
        Ok(tokio::fs::metadata(destination).await?.len())
    }
}

#[must_use]
pub fn provider() -> OciProvider { OciProvider }
