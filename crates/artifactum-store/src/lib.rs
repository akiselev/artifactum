//! Artifactum's content-addressed store.

use std::{
    collections::{BTreeMap, BTreeSet},
    io::ErrorKind,
    path::{Path, PathBuf},
    time::Duration,
};

use artifactum_core::{ArtifactPath, Digest, Metadata};
use directories::ProjectDirs;
use serde::{Deserialize, Serialize};
use sha2::{Digest as _, Sha256};
use thiserror::Error;
use tokio::{
    fs,
    io::{AsyncReadExt, AsyncWriteExt},
    time::sleep,
};
use uuid::Uuid;
use walkdir::WalkDir;

#[derive(Debug, Error)]
pub enum Error {
    #[error("could not determine an Artifactum cache directory")]
    CacheDirectoryUnavailable,
    #[error("I/O error: {0}")]
    Io(#[from] std::io::Error),
    #[error("serialization error: {0}")]
    Serialization(#[from] serde_json::Error),
    #[error("core error: {0}")]
    Core(#[from] artifactum_core::Error),
    #[error("artifact integrity mismatch: expected {expected}, got {actual}")]
    Integrity { expected: String, actual: String },
    #[error("unsupported digest algorithm `{0}`")]
    UnsupportedDigest(String),
    #[error("manifest `{0}` is missing from the store")]
    MissingManifest(String),
    #[error("blob `{0}` is missing from the store")]
    MissingBlob(String),
    #[error("timed out waiting for store lock `{0}`")]
    LockTimeout(String),
}

pub type Result<T, E = Error> = std::result::Result<T, E>;

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum MaterializationMode {
    #[default]
    Auto,
    Copy,
    Hardlink,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct StoredFile {
    pub path: ArtifactPath,
    pub digest: Digest,
    pub size: u64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub media_type: Option<String>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct StoredArtifact {
    pub provider: String,
    pub canonical_ref: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub revision: Option<String>,
    pub files: Vec<StoredFile>,
    #[serde(default)]
    pub provider_state: serde_json::Value,
    #[serde(default)]
    pub metadata: Metadata,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct StoredManifest {
    pub digest: Digest,
    pub artifact: StoredArtifact,
}

#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct GcReport {
    pub blobs_removed: usize,
    pub bytes_reclaimed: u64,
    pub blobs_retained: usize,
    pub dry_run: bool,
}

#[derive(Clone, Debug)]
pub struct ArtifactStore {
    root: PathBuf,
}

impl ArtifactStore {
    pub async fn xdg() -> Result<Self> {
        let dirs = ProjectDirs::from("org", "artifactum", "artifactum")
            .ok_or(Error::CacheDirectoryUnavailable)?;
        Self::open(dirs.cache_dir()).await
    }

    pub async fn open(root: impl Into<PathBuf>) -> Result<Self> {
        let store = Self { root: root.into() };
        for directory in [
            store.blobs_dir(),
            store.manifests_dir(),
            store.staging_dir(),
            store.locks_dir(),
            store.pins_dir(),
        ] {
            fs::create_dir_all(directory).await?;
        }
        Ok(store)
    }

    #[must_use]
    pub fn root(&self) -> &Path {
        &self.root
    }

    #[must_use]
    pub fn blobs_dir(&self) -> PathBuf {
        self.root.join("blobs").join("sha256")
    }

    #[must_use]
    pub fn manifests_dir(&self) -> PathBuf {
        self.root.join("manifests").join("sha256")
    }

    #[must_use]
    pub fn staging_dir(&self) -> PathBuf {
        self.root.join("staging")
    }

    #[must_use]
    pub fn locks_dir(&self) -> PathBuf {
        self.root.join("locks")
    }

    #[must_use]
    pub fn pins_dir(&self) -> PathBuf {
        self.root.join("refs").join("pins")
    }

    pub async fn staging_path(&self) -> Result<PathBuf> {
        fs::create_dir_all(self.staging_dir()).await?;
        Ok(self
            .staging_dir()
            .join(format!("{}.partial", Uuid::new_v4())))
    }

    #[must_use]
    pub fn blob_path(&self, digest: &Digest) -> Result<PathBuf> {
        if digest.algorithm != "sha256" {
            return Err(Error::UnsupportedDigest(digest.algorithm.clone()));
        }
        let prefix = digest.value.get(..2).unwrap_or("00");
        Ok(self.blobs_dir().join(prefix).join(&digest.value))
    }

    #[must_use]
    pub fn manifest_path(&self, digest: &Digest) -> Result<PathBuf> {
        if digest.algorithm != "sha256" {
            return Err(Error::UnsupportedDigest(digest.algorithm.clone()));
        }
        let prefix = digest.value.get(..2).unwrap_or("00");
        Ok(self.manifests_dir().join(prefix).join(&digest.value))
    }

    pub async fn contains_blob(&self, digest: &Digest) -> Result<bool> {
        Ok(fs::try_exists(self.blob_path(digest)?).await?)
    }

    pub async fn commit_staging(
        &self,
        staging: impl AsRef<Path>,
        expected: Option<&Digest>,
    ) -> Result<(Digest, u64)> {
        let staging = staging.as_ref();
        let (actual, size) = hash_file(staging).await?;
        if let Some(expected) = expected {
            if expected != &actual {
                let _ = fs::remove_file(staging).await;
                return Err(Error::Integrity {
                    expected: expected.to_string(),
                    actual: actual.to_string(),
                });
            }
        }

        let _lock = StoreLock::acquire(self, &format!("blob-{}", actual.value)).await?;
        let destination = self.blob_path(&actual)?;
        if fs::try_exists(&destination).await? {
            if self.verify_blob(&actual).await? {
                let _ = fs::remove_file(staging).await;
                return Ok((actual, size));
            }
            // A path derived from this digest exists but its bytes are corrupt.
            // The digest lock prevents another writer from racing this repair.
            fs::remove_file(&destination).await?;
        }

        if let Some(parent) = destination.parent() {
            fs::create_dir_all(parent).await?;
        }
        fs::rename(staging, &destination).await?;
        Ok((actual, size))
    }

    pub async fn import_file(
        &self,
        source: impl AsRef<Path>,
        expected: Option<&Digest>,
    ) -> Result<(Digest, u64)> {
        let staging = self.staging_path().await?;
        fs::copy(source, &staging).await?;
        self.commit_staging(staging, expected).await
    }

    pub async fn verify_blob(&self, digest: &Digest) -> Result<bool> {
        let path = self.blob_path(digest)?;
        if !fs::try_exists(&path).await? {
            return Ok(false);
        }
        let (actual, _) = hash_file(path).await?;
        Ok(&actual == digest)
    }

    pub async fn store_manifest(&self, artifact: &StoredArtifact) -> Result<StoredManifest> {
        let bytes = serde_json::to_vec(artifact)?;
        let digest = sha256_bytes(&bytes)?;
        let _lock = StoreLock::acquire(self, &format!("manifest-{}", digest.value)).await?;
        let path = self.manifest_path(&digest)?;
        let needs_write = if fs::try_exists(&path).await? {
            let (stored_digest, _) = hash_file(&path).await?;
            stored_digest != digest
        } else {
            true
        };
        if needs_write {
            if let Some(parent) = path.parent() {
                fs::create_dir_all(parent).await?;
            }
            let temp = path.with_extension(format!("{}.partial", Uuid::new_v4()));
            let mut file = fs::File::create(&temp).await?;
            file.write_all(&bytes).await?;
            file.sync_all().await?;
            drop(file);
            if fs::try_exists(&path).await? {
                fs::remove_file(&path).await?;
            }
            fs::rename(temp, &path).await?;
        }
        Ok(StoredManifest {
            digest,
            artifact: artifact.clone(),
        })
    }

    pub async fn load_manifest(&self, digest: &Digest) -> Result<StoredArtifact> {
        let path = self.manifest_path(digest)?;
        if !fs::try_exists(&path).await? {
            return Err(Error::MissingManifest(digest.to_string()));
        }
        let bytes = fs::read(&path).await?;
        let actual = sha256_bytes(&bytes)?;
        if &actual != digest {
            return Err(Error::Integrity {
                expected: digest.to_string(),
                actual: actual.to_string(),
            });
        }
        Ok(serde_json::from_slice(&bytes)?)
    }

    pub async fn pin(&self, name: &str, manifest: &Digest) -> Result<()> {
        let safe_name = sanitize_pin(name);
        let path = self.pins_dir().join(format!("{safe_name}.json"));
        fs::create_dir_all(self.pins_dir()).await?;
        let body = serde_json::to_vec_pretty(&PinRecord {
            name: name.to_owned(),
            manifest: Some(manifest.clone()),
            blobs: Vec::new(),
        })?;
        let temp = path.with_extension(format!("{}.partial", Uuid::new_v4()));
        fs::write(&temp, body).await?;
        fs::rename(temp, path).await?;
        Ok(())
    }

    /// Pin an explicit set of blobs without asserting that a complete artifact
    /// manifest exists. This keeps lazily fetched subsets reachable by GC.
    pub async fn pin_blobs(&self, name: &str, blobs: &[Digest]) -> Result<()> {
        let safe_name = sanitize_pin(name);
        let path = self.pins_dir().join(format!("{safe_name}.json"));
        fs::create_dir_all(self.pins_dir()).await?;
        let body = serde_json::to_vec_pretty(&PinRecord {
            name: name.to_owned(),
            manifest: None,
            blobs: blobs.to_vec(),
        })?;
        let temp = path.with_extension(format!("{}.partial", Uuid::new_v4()));
        fs::write(&temp, body).await?;
        fs::rename(temp, path).await?;
        Ok(())
    }

    pub async fn unpin(&self, name: &str) -> Result<()> {
        let path = self.pins_dir().join(format!("{}.json", sanitize_pin(name)));
        match fs::remove_file(path).await {
            Ok(()) => Ok(()),
            Err(error) if error.kind() == ErrorKind::NotFound => Ok(()),
            Err(error) => Err(error.into()),
        }
    }

    pub async fn pins(&self) -> Result<BTreeMap<String, Digest>> {
        let mut pins = BTreeMap::new();
        let mut entries = fs::read_dir(self.pins_dir()).await?;
        while let Some(entry) = entries.next_entry().await? {
            if !entry.file_type().await?.is_file() {
                continue;
            }
            let record: PinRecord = serde_json::from_slice(&fs::read(entry.path()).await?)?;
            if let Some(manifest)=record.manifest { pins.insert(record.name, manifest); }
        }
        Ok(pins)
    }

    pub async fn materialize(
        &self,
        artifact: &StoredArtifact,
        destination: impl AsRef<Path>,
        mode: MaterializationMode,
    ) -> Result<()> {
        self.materialize_files(&artifact.files, destination, mode).await
    }

    pub async fn materialize_files(
        &self,
        files: &[StoredFile],
        destination: impl AsRef<Path>,
        mode: MaterializationMode,
    ) -> Result<()> {
        let destination = destination.as_ref();
        fs::create_dir_all(destination).await?;
        for file in files {
            let source = self.blob_path(&file.digest)?;
            if !fs::try_exists(&source).await? {
                return Err(Error::MissingBlob(file.digest.to_string()));
            }
            let target = destination.join(file.path.to_path_buf());
            if let Some(parent) = target.parent() {
                fs::create_dir_all(parent).await?;
            }
            if fs::try_exists(&target).await? {
                fs::remove_file(&target).await?;
            }

            match mode {
                MaterializationMode::Copy => {
                    fs::copy(&source, &target).await?;
                }
                MaterializationMode::Hardlink => {
                    fs::hard_link(&source, &target).await?;
                }
                MaterializationMode::Auto => {
                    if fs::hard_link(&source, &target).await.is_err() {
                        fs::copy(&source, &target).await?;
                    }
                }
            }
        }
        Ok(())
    }

    pub async fn gc(&self, dry_run: bool) -> Result<GcReport> {
        let mut reachable = BTreeSet::new();
        let mut entries = fs::read_dir(self.pins_dir()).await?;
        while let Some(entry)=entries.next_entry().await? {
            if !entry.file_type().await?.is_file(){continue;}
            let record:PinRecord=serde_json::from_slice(&fs::read(entry.path()).await?)?;
            reachable.extend(record.blobs.into_iter().map(|digest|digest.value));
            if let Some(manifest_digest)=record.manifest {
                let artifact=self.load_manifest(&manifest_digest).await?;
                reachable.extend(artifact.files.into_iter().map(|file|file.digest.value));
            }
        }

        let blobs_dir = self.blobs_dir();
        let mut report = GcReport {
            dry_run,
            ..GcReport::default()
        };
        if !blobs_dir.exists() {
            return Ok(report);
        }

        for entry in WalkDir::new(&blobs_dir).into_iter().filter_map(std::result::Result::ok) {
            if !entry.file_type().is_file() {
                continue;
            }
            let name = entry.file_name().to_string_lossy();
            if reachable.contains(name.as_ref()) {
                report.blobs_retained += 1;
                continue;
            }
            let size = std::fs::metadata(entry.path())?.len();
            report.blobs_removed += 1;
            report.bytes_reclaimed += size;
            if !dry_run {
                fs::remove_file(entry.path()).await?;
            }
        }
        Ok(report)
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
struct PinRecord {
    name: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    manifest: Option<Digest>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    blobs: Vec<Digest>,
}

struct StoreLock {
    path: PathBuf,
}

impl StoreLock {
    async fn acquire(store: &ArtifactStore, key: &str) -> Result<Self> {
        let path = store.locks_dir().join(format!("{}.lock", sanitize_pin(key)));
        for _ in 0..600 {
            match fs::OpenOptions::new()
                .create_new(true)
                .write(true)
                .open(&path)
                .await
            {
                Ok(mut file) => {
                    file.write_all(std::process::id().to_string().as_bytes()).await?;
                    return Ok(Self { path });
                }
                Err(error) if error.kind() == ErrorKind::AlreadyExists => {
                    sleep(Duration::from_millis(100)).await;
                }
                Err(error) => return Err(error.into()),
            }
        }
        Err(Error::LockTimeout(key.to_owned()))
    }
}

impl Drop for StoreLock {
    fn drop(&mut self) {
        let _ = std::fs::remove_file(&self.path);
    }
}

pub async fn hash_file(path: impl AsRef<Path>) -> Result<(Digest, u64)> {
    let mut file = fs::File::open(path).await?;
    let mut hasher = Sha256::new();
    let mut buffer = vec![0_u8; 1024 * 1024];
    let mut size = 0_u64;
    loop {
        let read = file.read(&mut buffer).await?;
        if read == 0 {
            break;
        }
        hasher.update(&buffer[..read]);
        size += u64::try_from(read).unwrap_or(u64::MAX);
    }
    let digest = Digest::sha256(hex::encode(hasher.finalize()))?;
    Ok((digest, size))
}

pub fn sha256_bytes(bytes: &[u8]) -> Result<Digest> {
    let mut hasher = Sha256::new();
    hasher.update(bytes);
    Ok(Digest::sha256(hex::encode(hasher.finalize()))?)
}

fn sanitize_pin(value: &str) -> String {
    value
        .chars()
        .map(|character| {
            if character.is_ascii_alphanumeric() || matches!(character, '-' | '_' | '.') {
                character
            } else {
                '_'
            }
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

    #[tokio::test]
    async fn imports_and_deduplicates_by_digest() {
        let root = tempdir().unwrap();
        let store = ArtifactStore::open(root.path()).await.unwrap();
        let input = root.path().join("input");
        fs::write(&input, b"artifactum").await.unwrap();
        let first = store.import_file(&input, None).await.unwrap();
        let second = store.import_file(&input, None).await.unwrap();
        assert_eq!(first.0, second.0);
        assert!(store.contains_blob(&first.0).await.unwrap());
    }

    #[tokio::test]
    async fn commit_repairs_corrupt_existing_blob() {
        let root = tempdir().unwrap();
        let store = ArtifactStore::open(root.path().join("store")).await.unwrap();
        let input = root.path().join("input");
        fs::write(&input, b"correct").await.unwrap();
        let (digest, _) = hash_file(&input).await.unwrap();
        let destination = store.blob_path(&digest).unwrap();
        fs::create_dir_all(destination.parent().unwrap()).await.unwrap();
        fs::write(&destination, b"corrupt").await.unwrap();

        let staging = store.staging_path().await.unwrap();
        fs::copy(&input, &staging).await.unwrap();
        store.commit_staging(&staging, Some(&digest)).await.unwrap();

        assert_eq!(fs::read(&destination).await.unwrap(), b"correct");
        assert!(store.verify_blob(&digest).await.unwrap());
    }

    #[tokio::test]
    async fn explicit_blob_pins_survive_gc() {
        let root = tempdir().unwrap();
        let store = ArtifactStore::open(root.path().join("store")).await.unwrap();
        let pinned_input = root.path().join("pinned");
        let garbage_input = root.path().join("garbage");
        fs::write(&pinned_input, b"keep-me").await.unwrap();
        fs::write(&garbage_input, b"delete-me").await.unwrap();
        let (pinned, _) = store.import_file(&pinned_input, None).await.unwrap();
        let (garbage, _) = store.import_file(&garbage_input, None).await.unwrap();
        store.pin_blobs("partial-artifact", std::slice::from_ref(&pinned)).await.unwrap();

        let report = store.gc(false).await.unwrap();
        assert!(store.contains_blob(&pinned).await.unwrap());
        assert!(!store.contains_blob(&garbage).await.unwrap());
        assert_eq!(report.blobs_removed, 1);
    }

    #[tokio::test]
    async fn materializes_artifact_tree() {
        let root = tempdir().unwrap();
        let store = ArtifactStore::open(root.path().join("store")).await.unwrap();
        let input = root.path().join("input");
        fs::write(&input, b"hello").await.unwrap();
        let (digest, size) = store.import_file(&input, None).await.unwrap();
        let artifact = StoredArtifact {
            provider: "test".into(),
            canonical_ref: "test:fixture".into(),
            revision: None,
            files: vec![StoredFile {
                path: ArtifactPath::new("nested/model.bin").unwrap(),
                digest,
                size,
                media_type: None,
            }],
            provider_state: serde_json::Value::Null,
            metadata: Metadata::default(),
        };
        let destination = root.path().join("out");
        store
            .materialize(&artifact, &destination, MaterializationMode::Auto)
            .await
            .unwrap();
        assert_eq!(fs::read(destination.join("nested/model.bin")).await.unwrap(), b"hello");
    }
}
