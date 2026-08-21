//! Helper for provider plugins whose official CLI owns authentication/storage.

use artifactum_core::ArtifactPath;
use artifactum_resolver::{
    AccessRequirement, AcquireContext, AcquisitionPlan, ArtifactProvider, ArtifactRequirement,
    DigestSet, Error, ProviderCapabilities, ProviderDescriptor, Resolution, ResolveContext,
    ResolvedFile, ResolvedRevision, Result, access_required,
};
use async_trait::async_trait;
use std::{collections::BTreeMap, path::Path, process::Stdio};
use tokio::{fs, process::Command};

#[derive(Clone, Debug)]
pub struct CommandProvider {
    pub name: String,
    pub schemes: Vec<String>,
    pub executable: String,
    pub acquire_args: Vec<String>,
    pub default_filename: String,
}
impl CommandProvider {
    pub fn new(name: &str, schemes: &[&str], executable: &str, args: &[&str]) -> Self {
        Self {
            name: name.into(),
            schemes: schemes.iter().map(|s| (*s).into()).collect(),
            executable: executable.into(),
            acquire_args: args.iter().map(|s| (*s).into()).collect(),
            default_filename: "artifact.bin".into(),
        }
    }
    fn exists(&self) -> bool {
        std::env::var_os("PATH")
            .is_some_and(|p| std::env::split_paths(&p).any(|d| d.join(&self.executable).is_file()))
    }
}
#[async_trait]
impl ArtifactProvider for CommandProvider {
    fn descriptor(&self) -> ProviderDescriptor {
        ProviderDescriptor {
            name: self.name.clone(),
            version: env!("CARGO_PKG_VERSION").into(),
            schemes: self.schemes.clone(),
            capabilities: ProviderCapabilities {
                resolve: true,
                acquire: true,
                ..Default::default()
            },
        }
    }
    async fn resolve(&self, r: &ArtifactRequirement, _: &ResolveContext) -> Result<Resolution> {
        let file = r
            .reference
            .locator()
            .rsplit(['#', '/'])
            .next()
            .filter(|v| v.contains('.'))
            .unwrap_or(&self.default_filename);
        Ok(Resolution {
            provider: self.name.clone(),
            canonical_ref: r.reference.to_string(),
            revision: r.revision.as_ref().map(|id| ResolvedRevision {
                id: id.clone(),
                requested: Some(id.clone()),
            }),
            files: vec![ResolvedFile {
                path: ArtifactPath::new(file)?,
                size: None,
                digests: DigestSet(BTreeMap::new()),
                media_type: None,
                source: serde_json::json!({"reference":r.reference.to_string(),"locator":r.reference.locator(),"revision":r.revision}),
            }],
            provider_state: serde_json::Value::Null,
            metadata: BTreeMap::new(),
        })
    }
    async fn prepare_acquisition(
        &self,
        f: &ResolvedFile,
        _: &AcquireContext,
    ) -> Result<AcquisitionPlan> {
        if !self.exists() {
            return Err(access_required(
                self.name.clone(),
                AccessRequirement::ExternalTool,
                format!("install `{}` to use this provider", self.executable),
            ));
        }
        Ok(AcquisitionPlan::ProviderManaged {
            state: f.source.clone(),
        })
    }
    async fn acquire_managed(
        &self,
        f: &ResolvedFile,
        _: &AcquisitionPlan,
        d: &Path,
        ctx: &AcquireContext,
    ) -> Result<u64> {
        let reference = f
            .source
            .get("reference")
            .and_then(|v| v.as_str())
            .unwrap_or("");
        let locator = f
            .source
            .get("locator")
            .and_then(|v| v.as_str())
            .unwrap_or("");
        let revision = f
            .source
            .get("revision")
            .and_then(|v| v.as_str())
            .unwrap_or("");
        let dest = d.display().to_string();
        let args = self
            .acquire_args
            .iter()
            .map(|a| {
                let mut x = a
                    .replace("{reference}", reference)
                    .replace("{locator}", locator)
                    .replace("{revision}", revision)
                    .replace("{destination}", &dest);
                if let Some(profile) = &ctx.profile {
                    for (k, v) in &profile.config {
                        x = x.replace(&format!("{{profile_{k}}}"), v);
                    }
                }
                x = x.replace("{profile_remote}", &self.name);
                x
            })
            .collect::<Vec<_>>();
        let out = Command::new(&self.executable)
            .args(args)
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .output()
            .await?;
        if !out.status.success() {
            return Err(Error::Provider {
                provider: self.name.clone(),
                message: String::from_utf8_lossy(&out.stderr).into_owned(),
            });
        }
        if !fs::try_exists(d).await? {
            fs::write(d, &out.stdout).await?;
        }
        if fs::metadata(d).await?.is_dir() {
            let packed = d.with_extension("artifactum.tar");
            let status = Command::new("tar")
                .arg("-cf")
                .arg(&packed)
                .arg("-C")
                .arg(d)
                .arg(".")
                .status()
                .await?;
            if !status.success() {
                return Err(Error::Provider {
                    provider: self.name.clone(),
                    message: "failed to package provider-managed directory".into(),
                });
            }
            fs::remove_dir_all(d).await?;
            fs::rename(packed, d).await?;
        }
        Ok(fs::metadata(d).await?.len())
    }
}
