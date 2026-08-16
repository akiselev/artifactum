use std::{collections::BTreeMap, path::{Path, PathBuf}};

use artifactum_core::{
    provider_error, AcquireContext, Acquisition, ArtifactPath, ArtifactProvider, ArtifactRequirement,
    Digest, DigestSet, ProviderCapabilities, ProviderDescriptor, ResolveContext, ResolvedFile,
    ResolvedRevision, Resolution,
};
use async_trait::async_trait;
use sha2::{Digest as _, Sha256};
use tokio::{fs, io::AsyncReadExt};
use walkdir::WalkDir;

#[derive(Clone, Debug, Default)]
pub struct LocalProvider;

impl LocalProvider {
    #[must_use]
    pub fn new() -> Self {
        Self
    }
}

#[async_trait]
impl ArtifactProvider for LocalProvider {
    fn descriptor(&self) -> ProviderDescriptor {
        ProviderDescriptor {
            name: "local".into(),
            version: env!("CARGO_PKG_VERSION").into(),
            schemes: vec!["local".into(), "file".into()],
            capabilities: ProviderCapabilities {
                resolve: true,
                acquire: true,
                ..ProviderCapabilities::default()
            },
            metadata: BTreeMap::new(),
        }
    }

    async fn resolve(
        &self,
        requirement: &ArtifactRequirement,
        _context: &ResolveContext,
    ) -> artifactum_core::Result<Resolution> {
        let path = locator_path(requirement.reference.scheme(), requirement.reference.locator());
        let root = fs::canonicalize(&path)
            .await
            .map_err(|error| provider_error("local", error))?;
        let metadata = fs::metadata(&root)
            .await
            .map_err(|error| provider_error("local", error))?;
        let selection = requirement.selection.compile()?;
        let mut files = Vec::new();

        if metadata.is_file() {
            let name = root
                .file_name()
                .and_then(|value| value.to_str())
                .unwrap_or("artifact");
            if selection.matches(name) {
                files.push(resolve_file(&root, ArtifactPath::new(name)?).await?);
            }
        } else if metadata.is_dir() {
            for entry in WalkDir::new(&root).follow_links(false) {
                let entry = entry.map_err(|error| provider_error("local", error))?;
                if !entry.file_type().is_file() {
                    continue;
                }
                let relative = entry
                    .path()
                    .strip_prefix(&root)
                    .map_err(|error| provider_error("local", error))?
                    .to_string_lossy()
                    .replace('\\', "/");
                if selection.matches(&relative) {
                    files.push(resolve_file(entry.path(), ArtifactPath::new(relative)?).await?);
                }
            }
        } else {
            return Err(provider_error("local", "only regular files and directories are supported"));
        }

        files.sort_by(|a, b| a.path.cmp(&b.path));
        let revision = tree_revision(&files);
        Ok(Resolution {
            provider: "local".into(),
            canonical_ref: format!("local:{}", root.display()),
            revision: Some(ResolvedRevision {
                id: revision,
                requested: requirement.revision.clone(),
            }),
            files,
            provider_state: serde_json::json!({ "root": root }),
            metadata: BTreeMap::new(),
        })
    }

    async fn acquire(
        &self,
        file: &ResolvedFile,
        destination: &Path,
        context: &AcquireContext,
    ) -> artifactum_core::Result<Acquisition> {
        let _ = context;
        let source = file
            .source
            .get("path")
            .and_then(serde_json::Value::as_str)
            .ok_or_else(|| provider_error("local", "resolved file is missing source.path"))?;
        let bytes = fs::copy(source, destination)
            .await
            .map_err(|error| provider_error("local", error))?;
        Ok(Acquisition {
            bytes_written: Some(bytes),
            metadata: BTreeMap::new(),
        })
    }
}

async fn resolve_file(path: &Path, artifact_path: ArtifactPath) -> artifactum_core::Result<ResolvedFile> {
    let (digest, size) = hash_file(path).await?;
    let mut digests = DigestSet::default();
    digests.insert(digest);
    Ok(ResolvedFile {
        path: artifact_path,
        size: Some(size),
        digests,
        media_type: None,
        source: serde_json::json!({ "path": path }),
    })
}

async fn hash_file(path: &Path) -> artifactum_core::Result<(Digest, u64)> {
    let mut file = fs::File::open(path)
        .await
        .map_err(|error| provider_error("local", error))?;
    let mut hasher = Sha256::new();
    let mut buffer = vec![0_u8; 1024 * 1024];
    let mut size = 0_u64;
    loop {
        let count = file
            .read(&mut buffer)
            .await
            .map_err(|error| provider_error("local", error))?;
        if count == 0 {
            break;
        }
        hasher.update(&buffer[..count]);
        size += count as u64;
    }
    Ok((Digest::sha256(hex::encode(hasher.finalize()))?, size))
}

fn tree_revision(files: &[ResolvedFile]) -> String {
    let mut hasher = Sha256::new();
    for file in files {
        hasher.update(file.path.as_str().as_bytes());
        hasher.update([0]);
        if let Some(digest) = file.digests.sha256() {
            hasher.update(digest.as_bytes());
        }
        hasher.update([0]);
    }
    hex::encode(hasher.finalize())
}

fn locator_path(scheme: &str, locator: &str) -> PathBuf {
    if scheme == "file" && locator.starts_with("//") {
        #[cfg(windows)]
        {
            return PathBuf::from(locator.trim_start_matches('/'));
        }
        #[cfg(not(windows))]
        {
            return PathBuf::from(&locator[1..]);
        }
    }
    PathBuf::from(locator)
}
