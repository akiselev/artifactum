//! Artifactum store-v2: durable content-addressed blobs, Merkle trees,
//! collections, semantic artifact manifests, leases, refs and graph-aware GC.

use artifactum_core::{
    ArtifactId, ArtifactManifest, ArtifactPath, ChunkManifest, ChunkRef, CollectionManifest,
    ContentId, ContentKind, Digest, TreeEntry, TreeEntryKind, TreeManifest, hash_bytes,
};
use async_trait::async_trait;
use chrono::{DateTime, Duration, Utc};
use directories::ProjectDirs;
use serde::{Deserialize, Serialize};
use sha2::{Digest as ShaDigest, Sha256};
use std::{
    collections::{BTreeMap, BTreeSet, VecDeque},
    io::ErrorKind,
    path::{Path, PathBuf},
    sync::Arc,
};
use thiserror::Error;
use tokio::{
    fs,
    io::{AsyncRead, AsyncReadExt, AsyncWriteExt},
    process::Command,
};
use uuid::Uuid;
use walkdir::WalkDir;

#[derive(Debug, Error)]
pub enum Error {
    #[error("could not determine Artifactum data directory")]
    DataDirectoryUnavailable,
    #[error("I/O error: {0}")]
    Io(#[from] std::io::Error),
    #[error("serialization error: {0}")]
    Serialization(#[from] serde_json::Error),
    #[error("core error: {0}")]
    Core(#[from] artifactum_core::Error),
    #[error("missing content `{0}`")]
    MissingContent(String),
    #[error("missing artifact `{0}`")]
    MissingArtifact(String),
    #[error("integrity mismatch: expected {expected}, got {actual}")]
    Integrity { expected: String, actual: String },
    #[error("unsupported digest algorithm `{0}`")]
    UnsupportedDigest(String),
    #[error("lease `{0}` does not exist")]
    MissingLease(String),
    #[error("invalid legacy manifest: {0}")]
    Legacy(String),
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
    Reflink,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct StoredObjectInfo {
    pub id: ContentId,
    pub kind: ContentKind,
    pub size: u64,
}
#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct GcReport {
    pub objects_removed: usize,
    pub bytes_reclaimed: u64,
    pub objects_retained: usize,
    pub dry_run: bool,
}
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct StoreStats {
    pub content_objects: u64,
    pub artifacts: u64,
    pub physical_bytes: u64,
    pub refs: u64,
    pub leases: u64,
}
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct LeaseRecord {
    pub id: Uuid,
    pub owner: String,
    pub roots: Vec<ArtifactId>,
    pub created_at: DateTime<Utc>,
    pub expires_at: DateTime<Utc>,
}
#[derive(Clone, Debug, Serialize, Deserialize)]
struct RefRecord {
    name: String,
    artifact: ArtifactId,
    updated_at: DateTime<Utc>,
    immutable: bool,
}

#[async_trait]
pub trait ContentStore: Send + Sync {
    async fn contains_content(&self, id: &ContentId) -> Result<bool>;
    async fn read_content(&self, id: &ContentId) -> Result<Vec<u8>>;
    async fn put_bytes(&self, bytes: &[u8]) -> Result<ContentId>;
    async fn load_artifact(&self, id: &ArtifactId) -> Result<ArtifactManifest>;
    async fn put_artifact(&self, manifest: &ArtifactManifest) -> Result<ArtifactId>;
}

#[derive(Clone, Debug)]
pub struct ArtifactStore {
    root: Arc<PathBuf>,
}
impl ArtifactStore {
    pub async fn xdg() -> Result<Self> {
        let dirs = ProjectDirs::from("org", "artifactum", "artifactum")
            .ok_or(Error::DataDirectoryUnavailable)?;
        Self::open(dirs.data_dir().join("store")).await
    }
    pub async fn open(root: impl Into<PathBuf>) -> Result<Self> {
        let s = Self {
            root: Arc::new(root.into()),
        };
        for d in [
            s.content_dir(),
            s.artifacts_dir(),
            s.staging_dir(),
            s.refs_dir(),
            s.leases_dir(),
            s.locks_dir(),
        ] {
            fs::create_dir_all(d).await?;
        }
        Ok(s)
    }
    #[must_use]
    pub fn root(&self) -> &Path {
        self.root.as_ref()
    }
    fn content_dir(&self) -> PathBuf {
        self.root.join("content").join("sha256")
    }
    fn artifacts_dir(&self) -> PathBuf {
        self.root.join("artifacts").join("sha256")
    }
    fn staging_dir(&self) -> PathBuf {
        self.root.join("staging")
    }
    fn refs_dir(&self) -> PathBuf {
        self.root.join("refs")
    }
    fn leases_dir(&self) -> PathBuf {
        self.root.join("leases")
    }
    fn locks_dir(&self) -> PathBuf {
        self.root.join("locks")
    }
    fn digest_path(base: &Path, d: &Digest) -> Result<PathBuf> {
        if d.algorithm != "sha256" {
            return Err(Error::UnsupportedDigest(d.algorithm.clone()));
        }
        let p = d.value.get(..2).unwrap_or("00");
        Ok(base.join(p).join(&d.value))
    }
    #[must_use]
    pub fn content_path(&self, id: &ContentId) -> Result<PathBuf> {
        Self::digest_path(&self.content_dir(), &id.0)
    }
    #[must_use]
    pub fn artifact_path(&self, id: &ArtifactId) -> Result<PathBuf> {
        Self::digest_path(&self.artifacts_dir(), &id.0)
    }
    pub async fn staging_path(&self) -> Result<PathBuf> {
        fs::create_dir_all(self.staging_dir()).await?;
        Ok(self
            .staging_dir()
            .join(format!("{}.partial", Uuid::new_v4())))
    }
    /// Stable staging path for resumable operations. `key` must describe durable
    /// reacquisition identity, never live credentials.
    pub async fn resumable_staging_path(&self, key: &str) -> Result<PathBuf> {
        fs::create_dir_all(self.staging_dir()).await?;
        let digest = hash_bytes(key.as_bytes());
        Ok(self.staging_dir().join(format!("{}.resume", digest.value)))
    }
    pub async fn acquire_lock(&self, key: &str) -> Result<StoreLock> {
        let digest = hash_bytes(key.as_bytes());
        let path = self.locks_dir().join(format!("{}.lock", digest.value));
        for _ in 0..600 {
            match fs::OpenOptions::new()
                .create_new(true)
                .write(true)
                .open(&path)
                .await
            {
                Ok(mut file) => {
                    file.write_all(key.as_bytes()).await?;
                    return Ok(StoreLock { path });
                }
                Err(error) if error.kind() == ErrorKind::AlreadyExists => {
                    tokio::time::sleep(std::time::Duration::from_millis(100)).await
                }
                Err(error) => return Err(error.into()),
            }
        }
        Err(Error::LockTimeout(key.into()))
    }

    pub async fn import_reader<R: AsyncRead + Unpin + Send>(
        &self,
        reader: &mut R,
    ) -> Result<(ContentId, u64)> {
        let staging = self.staging_path().await?;
        let mut out = fs::File::create(&staging).await?;
        let n = tokio::io::copy(reader, &mut out).await?;
        out.sync_all().await?;
        drop(out);
        let id = self.commit_staging(&staging).await?;
        Ok((id, n))
    }
    pub async fn import_file(&self, path: impl AsRef<Path>) -> Result<(ContentId, u64)> {
        let mut f = fs::File::open(path).await?;
        self.import_reader(&mut f).await
    }
    pub async fn commit_staging(&self, path: impl AsRef<Path>) -> Result<ContentId> {
        self.commit_staging_expected(path, None).await
    }
    pub async fn commit_staging_expected(
        &self,
        path: impl AsRef<Path>,
        expected: Option<&Digest>,
    ) -> Result<ContentId> {
        let path = path.as_ref();
        let (digest, _) = hash_file(path).await?;
        if let Some(expected) = expected {
            if &digest != expected {
                let _ = fs::remove_file(path).await;
                return Err(Error::Integrity {
                    expected: expected.to_string(),
                    actual: digest.to_string(),
                });
            }
        }
        let id = ContentId(digest);
        let dest = self.content_path(&id)?;
        if let Some(p) = dest.parent() {
            fs::create_dir_all(p).await?;
        }
        if fs::try_exists(&dest).await? {
            let (actual, _) = hash_file(&dest).await?;
            if actual != id.0 {
                fs::remove_file(&dest).await?;
            } else {
                let _ = fs::remove_file(path).await;
                return Ok(id);
            }
        }
        match fs::rename(path, &dest).await {
            Ok(()) => {}
            Err(_) => {
                fs::copy(path, &dest).await?;
                let _ = fs::remove_file(path).await;
            }
        }
        Ok(id)
    }
    pub async fn verify_content(&self, id: &ContentId) -> Result<bool> {
        if !self.contains_content(id).await? {
            return Ok(false);
        };
        let (actual, _) = hash_file(self.content_path(id)?).await?;
        Ok(actual == id.0)
    }

    /// Import a large blob using deterministic content-defined chunking. The
    /// returned artifact still has semantic kind `Blob`; its storage encoding
    /// annotation tells materialization/verification to walk the chunk manifest.
    pub async fn import_chunked_blob_artifact(
        &self,
        path: impl AsRef<Path>,
        media_type: Option<String>,
    ) -> Result<ArtifactId> {
        let cfg = ChunkingConfig::default();
        let (manifest, _) = self.chunk_file(path.as_ref(), cfg).await?;
        let manifest_content = self.put_structured(&manifest).await?;
        let mut a = ArtifactManifest::new(manifest_content, ContentKind::Blob);
        a.media_type = media_type;
        a.annotations
            .insert("artifactum.storage".into(), "cdc-v1".into());
        self.put_artifact(&a).await
    }
    async fn chunk_file(&self, path: &Path, cfg: ChunkingConfig) -> Result<(ChunkManifest, u64)> {
        let mut file = fs::File::open(path).await?;
        let mut whole = Sha256::new();
        let mut chunks = Vec::new();
        let mut chunk = Vec::with_capacity(cfg.avg);
        let mut rolling = 0u64;
        let mut total = 0u64;
        let mut buf = vec![0u8; 256 * 1024];
        loop {
            let n = file.read(&mut buf).await?;
            if n == 0 {
                break;
            }
            whole.update(&buf[..n]);
            total = total.saturating_add(n as u64);
            for &b in &buf[..n] {
                chunk.push(b);
                rolling = rolling.rotate_left(1) ^ gear(b);
                let len = chunk.len();
                let boundary = len >= cfg.min && ((rolling & cfg.mask) == 0 || len >= cfg.max);
                if boundary {
                    let id = self.put_bytes(&chunk).await?;
                    chunks.push(ChunkRef {
                        content: id,
                        size: len as u64,
                    });
                    chunk.clear();
                    rolling = 0;
                }
            }
        }
        if !chunk.is_empty() {
            let id = self.put_bytes(&chunk).await?;
            chunks.push(ChunkRef {
                content: id,
                size: chunk.len() as u64,
            });
        }
        let logical_sha256 = Digest::sha256(hex::encode(whole.finalize()))?;
        Ok((
            ChunkManifest {
                version: 1,
                logical_size: total,
                logical_sha256,
                min_chunk: cfg.min as u64,
                avg_chunk: cfg.avg as u64,
                max_chunk: cfg.max as u64,
                chunks,
            },
            total,
        ))
    }
    pub async fn read_chunk_manifest(&self, id: &ContentId) -> Result<ChunkManifest> {
        Ok(serde_json::from_slice(&self.read_content(id).await?)?)
    }

    pub async fn import_tree(&self, path: impl AsRef<Path>) -> Result<ArtifactId> {
        let root = path.as_ref();
        let mut entries = Vec::new();
        for entry in WalkDir::new(root)
            .follow_links(false)
            .into_iter()
            .filter_map(std::result::Result::ok)
        {
            if entry.path() == root || entry.file_type().is_dir() {
                continue;
            }
            let rel = entry
                .path()
                .strip_prefix(root)
                .map_err(|e| Error::Legacy(e.to_string()))?;
            let ap = ArtifactPath::new(rel.to_string_lossy())?;
            let (id, size) = self.import_file(entry.path()).await?;
            let executable = entry.metadata().ok().map(|m| is_executable(&m));
            entries.push(TreeEntry {
                path: ap,
                kind: TreeEntryKind::Blob,
                content: id,
                size,
                executable,
            });
        }
        let tree = TreeManifest::new(entries);
        let content = self.put_structured(&tree).await?;
        let artifact = ArtifactManifest::new(content, ContentKind::Tree);
        self.put_artifact(&artifact).await
    }
    pub async fn put_tree_manifest(&self, tree: &TreeManifest) -> Result<ArtifactId> {
        let content = self.put_structured(tree).await?;
        self.put_artifact(&ArtifactManifest::new(content, ContentKind::Tree))
            .await
    }
    pub async fn put_collection(&self, collection: &CollectionManifest) -> Result<ArtifactId> {
        let content = self.put_structured(collection).await?;
        self.put_artifact(&ArtifactManifest::new(content, ContentKind::Collection))
            .await
    }
    async fn put_structured<T: Serialize>(&self, value: &T) -> Result<ContentId> {
        let bytes = artifactum_core::canonical_json(value)?;
        self.put_bytes(&bytes).await
    }
    pub async fn read_tree(&self, id: &ContentId) -> Result<TreeManifest> {
        Ok(serde_json::from_slice(&self.read_content(id).await?)?)
    }
    pub async fn read_collection(&self, id: &ContentId) -> Result<CollectionManifest> {
        Ok(serde_json::from_slice(&self.read_content(id).await?)?)
    }

    pub async fn import_blob_artifact(
        &self,
        path: impl AsRef<Path>,
        media_type: Option<String>,
    ) -> Result<ArtifactId> {
        let (content, _) = self.import_file(path).await?;
        let mut a = ArtifactManifest::new(content, ContentKind::Blob);
        a.media_type = media_type;
        self.put_artifact(&a).await
    }
    pub async fn artifact_from_bytes(
        &self,
        bytes: &[u8],
        media_type: Option<String>,
    ) -> Result<ArtifactId> {
        let content = self.put_bytes(bytes).await?;
        let mut a = ArtifactManifest::new(content, ContentKind::Blob);
        a.media_type = media_type;
        self.put_artifact(&a).await
    }

    pub async fn materialize(
        &self,
        id: &ArtifactId,
        destination: impl AsRef<Path>,
        mode: MaterializationMode,
    ) -> Result<()> {
        let a = self.load_artifact(id).await?;
        let dest = destination.as_ref();
        match a.kind {
            ContentKind::Blob => {
                if let Some(parent) = dest.parent() {
                    fs::create_dir_all(parent).await?;
                }
                if a.annotations
                    .get("artifactum.storage")
                    .is_some_and(|v| v == "cdc-v1")
                {
                    self.materialize_chunked(&a.content, dest).await?;
                } else {
                    self.materialize_blob(&a.content, dest, mode).await?;
                }
            }
            ContentKind::Tree => {
                self.materialize_tree(&a.content, dest, mode).await?;
            }
            ContentKind::Collection => {
                fs::create_dir_all(dest).await?;
                let c = self.read_collection(&a.content).await?;
                for e in c.entries {
                    Box::pin(self.materialize(&e.artifact, dest.join(sanitize_name(&e.key)), mode))
                        .await?;
                }
            }
        }
        Ok(())
    }
    async fn materialize_chunked(&self, id: &ContentId, target: &Path) -> Result<()> {
        let manifest = self.read_chunk_manifest(id).await?;
        let tmp = target.with_extension(format!("{}.partial", Uuid::new_v4()));
        let mut out = fs::File::create(&tmp).await?;
        let mut whole = Sha256::new();
        let mut size = 0u64;
        for c in manifest.chunks {
            let p = self.content_path(&c.content)?;
            let mut input = fs::File::open(p).await?;
            let mut buf = vec![0u8; 256 * 1024];
            loop {
                let n = input.read(&mut buf).await?;
                if n == 0 {
                    break;
                }
                out.write_all(&buf[..n]).await?;
                whole.update(&buf[..n]);
                size = size.saturating_add(n as u64);
            }
        }
        out.sync_all().await?;
        drop(out);
        let actual = Digest::sha256(hex::encode(whole.finalize()))?;
        if actual != manifest.logical_sha256 || size != manifest.logical_size {
            let _ = fs::remove_file(&tmp).await;
            return Err(Error::Integrity {
                expected: manifest.logical_sha256.to_string(),
                actual: actual.to_string(),
            });
        }
        if fs::try_exists(target).await? {
            fs::remove_file(target).await?;
        }
        fs::rename(tmp, target).await?;
        Ok(())
    }
    async fn materialize_tree(
        &self,
        id: &ContentId,
        dest: &Path,
        mode: MaterializationMode,
    ) -> Result<()> {
        let tree = self.read_tree(id).await?;
        let temp = dest.with_extension(format!("artifactum-{}", Uuid::new_v4()));
        if fs::try_exists(&temp).await? {
            fs::remove_dir_all(&temp).await?;
        }
        fs::create_dir_all(&temp).await?;
        for e in tree.entries {
            let target = temp.join(e.path.to_path_buf());
            if let Some(parent) = target.parent() {
                fs::create_dir_all(parent).await?;
            }
            let executable = e.executable.unwrap_or(false);
            // An executable entry must have an independent inode before chmod;
            // chmod on a hardlink would mutate the CAS object's metadata.
            self.materialize_blob_inner(&e.content, &target, mode, executable)
                .await?;
            if executable {
                set_executable(&target).await?;
            }
        }
        if fs::try_exists(dest).await? {
            if dest.is_dir() {
                fs::remove_dir_all(dest).await?
            } else {
                fs::remove_file(dest).await?
            }
        }
        fs::rename(temp, dest).await?;
        Ok(())
    }
    async fn materialize_blob(
        &self,
        id: &ContentId,
        target: &Path,
        mode: MaterializationMode,
    ) -> Result<()> {
        self.materialize_blob_inner(id, target, mode, false).await
    }
    async fn materialize_blob_inner(
        &self,
        id: &ContentId,
        target: &Path,
        mode: MaterializationMode,
        independent_inode: bool,
    ) -> Result<()> {
        let src = self.content_path(id)?;
        if !fs::try_exists(&src).await? {
            return Err(Error::MissingContent(id.to_string()));
        };
        if fs::try_exists(target).await? {
            let _ = fs::remove_file(target).await;
        }
        match mode {
            MaterializationMode::Copy => {
                fs::copy(&src, target).await?;
            }
            MaterializationMode::Hardlink if !independent_inode => {
                fs::hard_link(&src, target).await?;
            }
            MaterializationMode::Hardlink => {
                fs::copy(&src, target).await?;
            }
            MaterializationMode::Reflink => {
                if !try_reflink(&src, target).await? {
                    return Err(Error::Legacy(
                        "reflink materialization is unsupported by this filesystem/platform".into(),
                    ));
                }
            }
            MaterializationMode::Auto => {
                if !try_reflink(&src, target).await? {
                    if independent_inode || fs::hard_link(&src, target).await.is_err() {
                        fs::copy(&src, target).await?;
                    }
                }
            }
        }
        Ok(())
    }

    pub async fn set_ref(&self, name: &str, artifact: &ArtifactId, immutable: bool) -> Result<()> {
        let path = self
            .refs_dir()
            .join(format!("{}.json", sanitize_name(name)));
        if immutable && fs::try_exists(&path).await? {
            return Err(Error::Legacy(format!(
                "immutable ref `{name}` already exists"
            )));
        }
        let r = RefRecord {
            name: name.into(),
            artifact: artifact.clone(),
            updated_at: Utc::now(),
            immutable,
        };
        atomic_json(&path, &r).await
    }
    pub async fn get_ref(&self, name: &str) -> Result<Option<ArtifactId>> {
        let p = self
            .refs_dir()
            .join(format!("{}.json", sanitize_name(name)));
        if !fs::try_exists(&p).await? {
            return Ok(None);
        };
        let r: RefRecord = serde_json::from_slice(&fs::read(p).await?)?;
        Ok(Some(r.artifact))
    }
    pub async fn list_refs(&self) -> Result<BTreeMap<String, ArtifactId>> {
        let mut out = BTreeMap::new();
        let mut rd = fs::read_dir(self.refs_dir()).await?;
        while let Some(e) = rd.next_entry().await? {
            if e.file_type().await?.is_file() {
                let r: RefRecord = serde_json::from_slice(&fs::read(e.path()).await?)?;
                out.insert(r.name, r.artifact);
            }
        }
        Ok(out)
    }
    pub async fn delete_ref(&self, name: &str) -> Result<()> {
        let p = self
            .refs_dir()
            .join(format!("{}.json", sanitize_name(name)));
        match fs::remove_file(p).await {
            Ok(()) => Ok(()),
            Err(e) if e.kind() == ErrorKind::NotFound => Ok(()),
            Err(e) => Err(e.into()),
        }
    }

    pub async fn create_lease(
        &self,
        owner: impl Into<String>,
        roots: Vec<ArtifactId>,
        ttl: std::time::Duration,
    ) -> Result<LeaseRecord> {
        let now = Utc::now();
        let r = LeaseRecord {
            id: Uuid::new_v4(),
            owner: owner.into(),
            roots,
            created_at: now,
            expires_at: now + Duration::from_std(ttl).unwrap_or(Duration::hours(1)),
        };
        atomic_json(&self.leases_dir().join(format!("{}.json", r.id)), &r).await?;
        Ok(r)
    }
    pub async fn renew_lease(&self, id: Uuid, ttl: std::time::Duration) -> Result<LeaseRecord> {
        let p = self.leases_dir().join(format!("{id}.json"));
        if !fs::try_exists(&p).await? {
            return Err(Error::MissingLease(id.to_string()));
        };
        let mut r: LeaseRecord = serde_json::from_slice(&fs::read(&p).await?)?;
        r.expires_at = Utc::now() + Duration::from_std(ttl).unwrap_or(Duration::hours(1));
        atomic_json(&p, &r).await?;
        Ok(r)
    }
    pub async fn release_lease(&self, id: Uuid) -> Result<()> {
        let p = self.leases_dir().join(format!("{id}.json"));
        let _ = fs::remove_file(p).await;
        Ok(())
    }
    async fn active_lease_roots(&self) -> Result<Vec<ArtifactId>> {
        let mut roots = Vec::new();
        let mut rd = fs::read_dir(self.leases_dir()).await?;
        while let Some(e) = rd.next_entry().await? {
            if !e.file_type().await?.is_file() {
                continue;
            };
            let r: LeaseRecord = serde_json::from_slice(&fs::read(e.path()).await?)?;
            if r.expires_at > Utc::now() {
                roots.extend(r.roots)
            } else {
                let _ = fs::remove_file(e.path()).await;
            }
        }
        Ok(roots)
    }

    pub async fn reachable_graph(
        &self,
        extra_roots: &[ArtifactId],
    ) -> Result<(BTreeSet<String>, BTreeSet<String>)> {
        let mut queue: VecDeque<ArtifactId> = self.list_refs().await?.into_values().collect();
        queue.extend(self.active_lease_roots().await?);
        queue.extend(extra_roots.iter().cloned());
        let mut artifacts = BTreeSet::new();
        let mut content = BTreeSet::new();
        while let Some(id) = queue.pop_front() {
            if !artifacts.insert(id.to_string()) {
                continue;
            }
            let a = match self.load_artifact(&id).await {
                Ok(a) => a,
                Err(Error::MissingArtifact(_)) => continue,
                Err(e) => return Err(e),
            };
            content.insert(a.content.to_string());
            if let Some(schema) = a.schema {
                queue.push_back(schema)
            };
            match a.kind {
                ContentKind::Collection => {
                    for e in self.read_collection(&a.content).await?.entries {
                        queue.push_back(e.artifact)
                    }
                }
                ContentKind::Tree => {
                    for e in self.read_tree(&a.content).await?.entries {
                        content.insert(e.content.to_string());
                    }
                }
                ContentKind::Blob => {
                    if a.annotations
                        .get("artifactum.storage")
                        .is_some_and(|v| v == "cdc-v1")
                    {
                        for c in self.read_chunk_manifest(&a.content).await?.chunks {
                            content.insert(c.content.to_string());
                        }
                    }
                }
            }
        }
        Ok((artifacts, content))
    }
    pub async fn reachable_content(&self, extra_roots: &[ArtifactId]) -> Result<BTreeSet<String>> {
        Ok(self.reachable_graph(extra_roots).await?.1)
    }
    pub async fn gc(&self, dry_run: bool, extra_roots: &[ArtifactId]) -> Result<GcReport> {
        let (reachable_artifacts, reachable_content) = self.reachable_graph(extra_roots).await?;
        let mut r = GcReport {
            dry_run,
            ..Default::default()
        };
        for (base, reachable) in [
            (self.content_dir(), &reachable_content),
            (self.artifacts_dir(), &reachable_artifacts),
        ] {
            for e in WalkDir::new(base)
                .into_iter()
                .filter_map(std::result::Result::ok)
            {
                if !e.file_type().is_file() {
                    continue;
                };
                let name = e.file_name().to_string_lossy();
                let qualified = format!("sha256:{name}");
                if reachable.contains(&qualified) {
                    r.objects_retained += 1;
                } else {
                    let size = std::fs::metadata(e.path())?.len();
                    r.objects_removed += 1;
                    r.bytes_reclaimed += size;
                    if !dry_run {
                        fs::remove_file(e.path()).await?;
                    }
                }
            }
        }
        Ok(r)
    }
    pub async fn stats(&self) -> Result<StoreStats> {
        let mut content = 0;
        let mut bytes = 0;
        for e in WalkDir::new(self.content_dir())
            .into_iter()
            .filter_map(std::result::Result::ok)
        {
            if e.file_type().is_file() {
                content += 1;
                bytes += std::fs::metadata(e.path())?.len();
            }
        }
        let artifacts = count_files(&self.artifacts_dir());
        for e in WalkDir::new(self.artifacts_dir())
            .into_iter()
            .filter_map(std::result::Result::ok)
        {
            if e.file_type().is_file() {
                bytes += std::fs::metadata(e.path())?.len();
            }
        }
        let refs = count_files(&self.refs_dir());
        let leases = count_files(&self.leases_dir());
        Ok(StoreStats {
            content_objects: content,
            artifacts,
            physical_bytes: bytes,
            refs,
            leases,
        })
    }

    /// Imports Artifactum 0.3's flat SHA-256 blob CAS. Old manifests are intentionally
    /// left untouched: callers can retain the old cache until external artifacts have
    /// been re-resolved into store-v2 observations.
    pub async fn migrate_legacy_blobs(&self, legacy_root: impl AsRef<Path>) -> Result<u64> {
        let root = legacy_root.as_ref().join("blobs").join("sha256");
        if !root.exists() {
            return Ok(0);
        };
        let mut n = 0;
        for e in WalkDir::new(root)
            .into_iter()
            .filter_map(std::result::Result::ok)
        {
            if !e.file_type().is_file() {
                continue;
            };
            let mut f = fs::File::open(e.path()).await?;
            let (id, _) = self.import_reader(&mut f).await?;
            if id.0.value != e.file_name().to_string_lossy() {
                return Err(Error::Legacy(format!(
                    "legacy blob {} failed digest verification",
                    e.path().display()
                )));
            }
            n += 1;
        }
        Ok(n)
    }
}

pub struct StoreLock {
    path: PathBuf,
}
impl Drop for StoreLock {
    fn drop(&mut self) {
        let _ = std::fs::remove_file(&self.path);
    }
}

#[async_trait]
impl ContentStore for ArtifactStore {
    async fn contains_content(&self, id: &ContentId) -> Result<bool> {
        Ok(fs::try_exists(self.content_path(id)?).await?)
    }
    async fn read_content(&self, id: &ContentId) -> Result<Vec<u8>> {
        let p = self.content_path(id)?;
        if !fs::try_exists(&p).await? {
            return Err(Error::MissingContent(id.to_string()));
        };
        Ok(fs::read(p).await?)
    }
    async fn put_bytes(&self, bytes: &[u8]) -> Result<ContentId> {
        let id = ContentId(hash_bytes(bytes));
        let p = self.content_path(&id)?;
        if !fs::try_exists(&p).await? {
            if let Some(parent) = p.parent() {
                fs::create_dir_all(parent).await?;
            }
            let tmp = self.staging_path().await?;
            let mut f = fs::File::create(&tmp).await?;
            f.write_all(bytes).await?;
            f.sync_all().await?;
            drop(f);
            let _ = self.commit_staging(tmp).await?;
        }
        Ok(id)
    }
    async fn load_artifact(&self, id: &ArtifactId) -> Result<ArtifactManifest> {
        let p = self.artifact_path(id)?;
        if !fs::try_exists(&p).await? {
            return Err(Error::MissingArtifact(id.to_string()));
        };
        let bytes = fs::read(&p).await?;
        let actual = ArtifactId(hash_bytes(&bytes));
        if actual != *id {
            return Err(Error::Integrity {
                expected: id.to_string(),
                actual: actual.to_string(),
            });
        };
        Ok(serde_json::from_slice(&bytes)?)
    }
    async fn put_artifact(&self, manifest: &ArtifactManifest) -> Result<ArtifactId> {
        let bytes = artifactum_core::canonical_json(manifest)?;
        let id = ArtifactId(hash_bytes(&bytes));
        let p = self.artifact_path(&id)?;
        if !fs::try_exists(&p).await? {
            if let Some(parent) = p.parent() {
                fs::create_dir_all(parent).await?;
            }
            let tmp = p.with_extension(format!("{}.partial", Uuid::new_v4()));
            fs::write(&tmp, &bytes).await?;
            fs::rename(tmp, p).await?;
        }
        Ok(id)
    }
}

#[derive(Clone, Copy)]
struct ChunkingConfig {
    min: usize,
    avg: usize,
    max: usize,
    mask: u64,
}
impl Default for ChunkingConfig {
    fn default() -> Self {
        let avg = 2 * 1024 * 1024;
        Self {
            min: 512 * 1024,
            avg,
            max: 8 * 1024 * 1024,
            mask: (avg as u64) - 1,
        }
    }
}
fn gear(byte: u8) -> u64 {
    let mut x = u64::from(byte).wrapping_add(0x9e3779b97f4a7c15);
    x = (x ^ (x >> 30)).wrapping_mul(0xbf58476d1ce4e5b9);
    x = (x ^ (x >> 27)).wrapping_mul(0x94d049bb133111eb);
    x ^ (x >> 31)
}
async fn hash_file(path: impl AsRef<Path>) -> Result<(Digest, u64)> {
    let mut f = fs::File::open(path).await?;
    let mut h = Sha256::new();
    let mut size = 0u64;
    let mut buf = vec![0u8; 256 * 1024];
    loop {
        let n = f.read(&mut buf).await?;
        if n == 0 {
            break;
        }
        h.update(&buf[..n]);
        size = size.saturating_add(n as u64);
    }
    Ok((Digest::sha256(hex::encode(h.finalize()))?, size))
}

async fn atomic_json<T: Serialize>(path: &Path, value: &T) -> Result<()> {
    if let Some(p) = path.parent() {
        fs::create_dir_all(p).await?;
    }
    let tmp = path.with_extension(format!("{}.partial", Uuid::new_v4()));
    fs::write(&tmp, serde_json::to_vec_pretty(value)?).await?;
    fs::rename(tmp, path).await?;
    Ok(())
}
fn sanitize_name(v: &str) -> String {
    v.chars()
        .map(|c| {
            if c.is_ascii_alphanumeric() || matches!(c, '-' | '_' | '.') {
                c
            } else {
                '_'
            }
        })
        .collect()
}
fn count_files(path: &Path) -> u64 {
    WalkDir::new(path)
        .into_iter()
        .filter_map(std::result::Result::ok)
        .filter(|e| e.file_type().is_file())
        .count() as u64
}

async fn try_reflink(source: &Path, target: &Path) -> Result<bool> {
    #[cfg(target_os = "linux")]
    {
        let status = Command::new("cp")
            .arg("--reflink=always")
            .arg("--")
            .arg(source)
            .arg(target)
            .status()
            .await;
        if status.is_ok_and(|s| s.success()) {
            return Ok(true);
        }
        let _ = fs::remove_file(target).await;
        return Ok(false);
    }
    #[cfg(target_os = "macos")]
    {
        let status = Command::new("cp")
            .arg("-c")
            .arg(source)
            .arg(target)
            .status()
            .await;
        if status.is_ok_and(|s| s.success()) {
            return Ok(true);
        }
        let _ = fs::remove_file(target).await;
        return Ok(false);
    }
    #[cfg(not(any(target_os = "linux", target_os = "macos")))]
    {
        let _ = (source, target);
        Ok(false)
    }
}
#[cfg(unix)]
async fn set_executable(path: &Path) -> Result<()> {
    use std::os::unix::fs::PermissionsExt;
    let mut p = fs::metadata(path).await?.permissions();
    p.set_mode(p.mode() | 0o111);
    fs::set_permissions(path, p).await?;
    Ok(())
}
#[cfg(not(unix))]
async fn set_executable(_: &Path) -> Result<()> {
    Ok(())
}
#[cfg(unix)]
fn is_executable(meta: &std::fs::Metadata) -> bool {
    use std::os::unix::fs::PermissionsExt;
    meta.permissions().mode() & 0o111 != 0
}
#[cfg(not(unix))]
fn is_executable(_: &std::fs::Metadata) -> bool {
    false
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn deduplicates_bytes() {
        let d = tempfile::tempdir().unwrap();
        let s = ArtifactStore::open(d.path()).await.unwrap();
        let a = s.put_bytes(b"hello").await.unwrap();
        let b = s.put_bytes(b"hello").await.unwrap();
        assert_eq!(a, b);
    }

    #[tokio::test]
    async fn provenance_does_not_change_artifact_identity() {
        let d = tempfile::tempdir().unwrap();
        let s = ArtifactStore::open(d.path()).await.unwrap();
        let c = s.put_bytes(b"x").await.unwrap();
        let a = ArtifactManifest::new(c, ContentKind::Blob);
        assert_eq!(
            s.put_artifact(&a).await.unwrap(),
            s.put_artifact(&a).await.unwrap()
        );
    }

    #[tokio::test]
    async fn concurrent_identical_imports_converge() {
        let d = tempfile::tempdir().unwrap();
        let source = d.path().join("source.bin");
        tokio::fs::write(&source, vec![0x5a; 1024 * 1024])
            .await
            .unwrap();
        let s = ArtifactStore::open(d.path().join("store")).await.unwrap();
        let mut set = tokio::task::JoinSet::new();
        for _ in 0..8 {
            let s = s.clone();
            let source = source.clone();
            set.spawn(async move { s.import_file(source).await.unwrap().0 });
        }
        let mut ids = BTreeSet::new();
        while let Some(result) = set.join_next().await {
            ids.insert(result.unwrap());
        }
        assert_eq!(ids.len(), 1);
        assert_eq!(s.stats().await.unwrap().content_objects, 1);
    }

    #[tokio::test]
    async fn corrupted_content_is_detected() {
        let d = tempfile::tempdir().unwrap();
        let s = ArtifactStore::open(d.path()).await.unwrap();
        let id = s.put_bytes(b"trusted").await.unwrap();
        tokio::fs::write(s.content_path(&id).unwrap(), b"corrupt")
            .await
            .unwrap();
        assert!(!s.verify_content(&id).await.unwrap());
    }

    #[tokio::test]
    async fn chunked_blob_roundtrips() {
        let d = tempfile::tempdir().unwrap();
        let source = d.path().join("large.bin");
        let mut bytes = Vec::new();
        for i in 0..(3 * 1024 * 1024) {
            bytes.push((i % 251) as u8);
        }
        tokio::fs::write(&source, &bytes).await.unwrap();
        let s = ArtifactStore::open(d.path().join("store")).await.unwrap();
        let artifact = s.import_chunked_blob_artifact(&source, None).await.unwrap();
        let out = d.path().join("out.bin");
        s.materialize(&artifact, &out, MaterializationMode::Copy)
            .await
            .unwrap();
        assert_eq!(tokio::fs::read(out).await.unwrap(), bytes);
        let manifest = s.load_artifact(&artifact).await.unwrap();
        assert_eq!(
            manifest
                .annotations
                .get("artifactum.storage")
                .map(String::as_str),
            Some("cdc-v1")
        );
    }

    #[tokio::test]
    async fn active_lease_roots_artifact_against_gc() {
        let d = tempfile::tempdir().unwrap();
        let s = ArtifactStore::open(d.path()).await.unwrap();
        let artifact = s.artifact_from_bytes(b"leased", None).await.unwrap();
        let lease = s
            .create_lease(
                "test",
                vec![artifact.clone()],
                std::time::Duration::from_secs(60),
            )
            .await
            .unwrap();
        let first = s.gc(false, &[]).await.unwrap();
        assert_eq!(first.objects_removed, 0);
        s.release_lease(lease.id).await.unwrap();
        let second = s.gc(false, &[]).await.unwrap();
        assert!(second.objects_removed >= 2);
        assert!(matches!(
            s.load_artifact(&artifact).await,
            Err(Error::MissingArtifact(_))
        ));
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn executable_tree_entry_materializes_executable_without_mutating_cas_mode() {
        use std::os::unix::fs::PermissionsExt;
        let d = tempfile::tempdir().unwrap();
        let s = ArtifactStore::open(d.path().join("store")).await.unwrap();
        let content = s.put_bytes(b"#!/bin/sh\necho ok\n").await.unwrap();
        let artifact = s
            .put_tree_manifest(&TreeManifest::new(vec![TreeEntry {
                path: ArtifactPath::new("run.sh").unwrap(),
                kind: TreeEntryKind::Blob,
                content: content.clone(),
                size: 18,
                executable: Some(true),
            }]))
            .await
            .unwrap();
        let out = d.path().join("tree");
        s.materialize(&artifact, &out, MaterializationMode::Auto)
            .await
            .unwrap();
        assert_ne!(
            tokio::fs::metadata(out.join("run.sh"))
                .await
                .unwrap()
                .permissions()
                .mode()
                & 0o111,
            0
        );
        assert_eq!(
            tokio::fs::metadata(s.content_path(&content).unwrap())
                .await
                .unwrap()
                .permissions()
                .mode()
                & 0o111,
            0
        );
    }
}
