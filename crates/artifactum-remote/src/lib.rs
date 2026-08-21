//! Remote CAS mirroring. Origin providers resolve mutable external names; this
//! module only mirrors already-identified Artifactum objects. Content transfer
//! is streaming so large unchunked provider objects do not have to fit in RAM.

use std::{
    path::{Path, PathBuf},
    sync::Arc,
};

use artifactum_core::{
    ArtifactId, ArtifactManifest, ChunkManifest, CollectionManifest, ContentId, ContentKind,
    Digest, TreeManifest, hash_bytes,
};
use artifactum_store::{ArtifactStore, ContentStore};
use async_trait::async_trait;
use futures_util::StreamExt;
use reqwest::StatusCode;
use sha2::{Digest as ShaDigest, Sha256};
use thiserror::Error;
use tokio::{
    fs,
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, TcpStream},
};
use tokio_util::io::ReaderStream;

#[derive(Debug, Error)]
pub enum Error {
    #[error("store error: {0}")]
    Store(#[from] artifactum_store::Error),
    #[error("core error: {0}")]
    Core(#[from] artifactum_core::Error),
    #[error("I/O error: {0}")]
    Io(#[from] std::io::Error),
    #[error("HTTP error: {0}")]
    Http(#[from] reqwest::Error),
    #[error("JSON error: {0}")]
    Json(#[from] serde_json::Error),
    #[error("remote object missing: {0}")]
    Missing(String),
    #[error("remote integrity mismatch: expected {expected}, got {actual}")]
    Integrity { expected: String, actual: String },
    #[error("remote is read-only")]
    ReadOnly,
    #[error("invalid remote protocol request")]
    Protocol,
}
pub type Result<T, E = Error> = std::result::Result<T, E>;

#[async_trait]
pub trait RemoteCache: Send + Sync {
    /// Stream a content object into `destination`, returning false for 404/miss.
    async fn get_content_to(&self, id: &ContentId, destination: &Path) -> Result<bool>;
    async fn put_content_from(&self, id: &ContentId, source: &Path) -> Result<()>;
    /// Artifact manifests are deliberately small canonical JSON objects.
    async fn get_artifact(&self, id: &ArtifactId) -> Result<Option<Vec<u8>>>;
    async fn put_artifact(&self, id: &ArtifactId, bytes: &[u8]) -> Result<()>;
}

#[derive(Clone, Debug)]
pub struct FileRemote {
    root: PathBuf,
    read_only: bool,
}
impl FileRemote {
    pub async fn open(root: impl Into<PathBuf>, read_only: bool) -> Result<Self> {
        let s = Self {
            root: root.into(),
            read_only,
        };
        if !read_only {
            for d in ["content/sha256", "artifacts/sha256", "staging"] {
                fs::create_dir_all(s.root.join(d)).await?;
            }
        }
        Ok(s)
    }
    fn path(&self, kind: &str, value: &str) -> PathBuf {
        self.root
            .join(kind)
            .join("sha256")
            .join(&value[..2])
            .join(value)
    }
    fn staging(&self) -> PathBuf {
        self.root
            .join("staging")
            .join(format!("{}.partial", uuid_like()))
    }
}
#[async_trait]
impl RemoteCache for FileRemote {
    async fn get_content_to(&self, id: &ContentId, destination: &Path) -> Result<bool> {
        let source = self.path("content", &id.0.value);
        if !fs::try_exists(&source).await? {
            return Ok(false);
        }
        if let Some(parent) = destination.parent() {
            fs::create_dir_all(parent).await?;
        }
        fs::copy(&source, destination).await?;
        verify_file(id.to_string(), destination).await?;
        Ok(true)
    }
    async fn put_content_from(&self, id: &ContentId, source: &Path) -> Result<()> {
        if self.read_only {
            return Err(Error::ReadOnly);
        }
        verify_file(id.to_string(), source).await?;
        let destination = self.path("content", &id.0.value);
        if fs::try_exists(&destination).await? {
            verify_file(id.to_string(), &destination).await?;
            return Ok(());
        }
        if let Some(parent) = destination.parent() {
            fs::create_dir_all(parent).await?;
        }
        let staging = self.staging();
        if let Some(parent) = staging.parent() {
            fs::create_dir_all(parent).await?;
        }
        fs::copy(source, &staging).await?;
        match fs::rename(&staging, &destination).await {
            Ok(()) => {}
            Err(_) if fs::try_exists(&destination).await? => {
                let _ = fs::remove_file(&staging).await;
            }
            Err(error) => return Err(error.into()),
        }
        Ok(())
    }
    async fn get_artifact(&self, id: &ArtifactId) -> Result<Option<Vec<u8>>> {
        read_optional(self.path("artifacts", &id.0.value)).await
    }
    async fn put_artifact(&self, id: &ArtifactId, bytes: &[u8]) -> Result<()> {
        if self.read_only {
            return Err(Error::ReadOnly);
        }
        verify_bytes(id.to_string(), bytes)?;
        write_atomic(self.path("artifacts", &id.0.value), bytes).await
    }
}

#[derive(Clone)]
pub struct HttpRemote {
    base: String,
    client: reqwest::Client,
    token: Option<String>,
    read_only: bool,
}
impl HttpRemote {
    pub fn new(base: impl Into<String>, token: Option<String>, read_only: bool) -> Self {
        Self {
            base: base.into().trim_end_matches('/').into(),
            client: reqwest::Client::new(),
            token,
            read_only,
        }
    }
    fn url(&self, kind: &str, value: &str) -> String {
        format!("{}/{}/sha256/{}", self.base, kind, value)
    }
    fn auth(&self, request: reqwest::RequestBuilder) -> reqwest::RequestBuilder {
        match self.token.as_ref() {
            Some(token) => request.bearer_auth(token),
            None => request,
        }
    }
    async fn get_small(&self, url: String) -> Result<Option<Vec<u8>>> {
        let response = self.auth(self.client.get(url)).send().await?;
        if response.status() == StatusCode::NOT_FOUND {
            return Ok(None);
        }
        Ok(Some(response.error_for_status()?.bytes().await?.to_vec()))
    }
    async fn put_small(&self, url: String, bytes: &[u8]) -> Result<()> {
        if self.read_only {
            return Err(Error::ReadOnly);
        }
        self.auth(self.client.put(url).body(bytes.to_vec()))
            .send()
            .await?
            .error_for_status()?;
        Ok(())
    }
}
#[async_trait]
impl RemoteCache for HttpRemote {
    async fn get_content_to(&self, id: &ContentId, destination: &Path) -> Result<bool> {
        let response = self
            .auth(self.client.get(self.url("content", &id.0.value)))
            .send()
            .await?;
        if response.status() == StatusCode::NOT_FOUND {
            return Ok(false);
        }
        let response = response.error_for_status()?;
        if let Some(parent) = destination.parent() {
            fs::create_dir_all(parent).await?;
        }
        let mut out = fs::File::create(destination).await?;
        let mut stream = response.bytes_stream();
        let mut hasher = Sha256::new();
        while let Some(chunk) = stream.next().await {
            let chunk = chunk?;
            hasher.update(&chunk);
            out.write_all(&chunk).await?;
        }
        out.sync_all().await?;
        let actual = format!("sha256:{}", hex::encode(hasher.finalize()));
        if actual != id.to_string() {
            let _ = fs::remove_file(destination).await;
            return Err(Error::Integrity {
                expected: id.to_string(),
                actual,
            });
        }
        Ok(true)
    }
    async fn put_content_from(&self, id: &ContentId, source: &Path) -> Result<()> {
        if self.read_only {
            return Err(Error::ReadOnly);
        }
        verify_file(id.to_string(), source).await?;
        let file = fs::File::open(source).await?;
        let length = file.metadata().await?.len();
        let body = reqwest::Body::wrap_stream(ReaderStream::new(file));
        self.auth(
            self.client
                .put(self.url("content", &id.0.value))
                .header(reqwest::header::CONTENT_LENGTH, length)
                .body(body),
        )
        .send()
        .await?
        .error_for_status()?;
        Ok(())
    }
    async fn get_artifact(&self, id: &ArtifactId) -> Result<Option<Vec<u8>>> {
        self.get_small(self.url("artifacts", &id.0.value)).await
    }
    async fn put_artifact(&self, id: &ArtifactId, bytes: &[u8]) -> Result<()> {
        verify_bytes(id.to_string(), bytes)?;
        self.put_small(self.url("artifacts", &id.0.value), bytes)
            .await
    }
}

#[derive(Clone)]
pub struct Mirror {
    local: ArtifactStore,
    remote: Arc<dyn RemoteCache>,
}
impl Mirror {
    pub fn new(local: ArtifactStore, remote: Arc<dyn RemoteCache>) -> Self {
        Self { local, remote }
    }
    pub async fn push(&self, id: &ArtifactId) -> Result<()> {
        Box::pin(self.push_artifact(id)).await
    }
    async fn push_artifact(&self, id: &ArtifactId) -> Result<()> {
        let manifest = self.local.load_artifact(id).await?;
        let bytes = artifactum_core::canonical_json(&manifest)?;
        self.remote.put_artifact(id, &bytes).await?;
        self.push_content(&manifest.content).await?;
        if let Some(schema) = manifest.schema {
            Box::pin(self.push_artifact(&schema)).await?;
        }
        match manifest.kind {
            ContentKind::Blob => {
                if manifest
                    .annotations
                    .get("artifactum.storage")
                    .is_some_and(|value| value == "cdc-v1")
                {
                    let chunks: ChunkManifest =
                        serde_json::from_slice(&self.local.read_content(&manifest.content).await?)?;
                    for chunk in chunks.chunks {
                        self.push_content(&chunk.content).await?;
                    }
                }
            }
            ContentKind::Tree => {
                let tree: TreeManifest =
                    serde_json::from_slice(&self.local.read_content(&manifest.content).await?)?;
                for entry in tree.entries {
                    self.push_content(&entry.content).await?;
                }
            }
            ContentKind::Collection => {
                let collection: CollectionManifest =
                    serde_json::from_slice(&self.local.read_content(&manifest.content).await?)?;
                for entry in collection.entries {
                    Box::pin(self.push_artifact(&entry.artifact)).await?;
                }
            }
        }
        Ok(())
    }
    async fn push_content(&self, id: &ContentId) -> Result<()> {
        self.remote
            .put_content_from(id, &self.local.content_path(id)?)
            .await
    }

    pub async fn pull(&self, id: &ArtifactId) -> Result<()> {
        Box::pin(self.pull_artifact(id)).await
    }
    async fn pull_artifact(&self, id: &ArtifactId) -> Result<()> {
        if self.local.load_artifact(id).await.is_ok() {
            return Ok(());
        }
        let bytes = self
            .remote
            .get_artifact(id)
            .await?
            .ok_or_else(|| Error::Missing(id.to_string()))?;
        verify_bytes(id.to_string(), &bytes)?;
        let manifest: ArtifactManifest = serde_json::from_slice(&bytes)?;
        let stored = self.local.put_artifact(&manifest).await?;
        if stored != *id {
            return Err(Error::Integrity {
                expected: id.to_string(),
                actual: stored.to_string(),
            });
        }
        self.pull_content(&manifest.content).await?;
        if let Some(schema) = manifest.schema {
            Box::pin(self.pull_artifact(&schema)).await?;
        }
        match manifest.kind {
            ContentKind::Blob => {
                if manifest
                    .annotations
                    .get("artifactum.storage")
                    .is_some_and(|value| value == "cdc-v1")
                {
                    let chunks: ChunkManifest =
                        serde_json::from_slice(&self.local.read_content(&manifest.content).await?)?;
                    for chunk in chunks.chunks {
                        self.pull_content(&chunk.content).await?;
                    }
                }
            }
            ContentKind::Tree => {
                let tree: TreeManifest =
                    serde_json::from_slice(&self.local.read_content(&manifest.content).await?)?;
                for entry in tree.entries {
                    self.pull_content(&entry.content).await?;
                }
            }
            ContentKind::Collection => {
                let collection: CollectionManifest =
                    serde_json::from_slice(&self.local.read_content(&manifest.content).await?)?;
                for entry in collection.entries {
                    Box::pin(self.pull_artifact(&entry.artifact)).await?;
                }
            }
        }
        Ok(())
    }
    async fn pull_content(&self, id: &ContentId) -> Result<()> {
        if self.local.contains_content(id).await? {
            return Ok(());
        }
        let staging = self.local.staging_path().await?;
        if !self.remote.get_content_to(id, &staging).await? {
            return Err(Error::Missing(id.to_string()));
        }
        let got = self.local.commit_staging(&staging).await?;
        if got != *id {
            return Err(Error::Integrity {
                expected: id.to_string(),
                actual: got.to_string(),
            });
        }
        Ok(())
    }
}

/// Minimal Artifactum remote-cache server supporting streaming GET/HEAD/PUT over
/// the native CAS namespace. A bearer token can protect reads and writes.
pub async fn serve(
    store_root: impl Into<PathBuf>,
    bind: &str,
    token: Option<String>,
    read_only: bool,
) -> Result<()> {
    let remote = Arc::new(FileRemote::open(store_root, read_only).await?);
    let listener = TcpListener::bind(bind).await?;
    loop {
        let (stream, _) = listener.accept().await?;
        let remote = Arc::clone(&remote);
        let token = token.clone();
        tokio::spawn(async move {
            let _ = handle(stream, remote, token, read_only).await;
        });
    }
}
async fn handle(
    mut stream: TcpStream,
    remote: Arc<FileRemote>,
    token: Option<String>,
    read_only: bool,
) -> Result<()> {
    let mut header = Vec::new();
    let mut chunk = [0_u8; 8192];
    let header_end;
    loop {
        let n = stream.read(&mut chunk).await?;
        if n == 0 {
            return Err(Error::Protocol);
        }
        header.extend_from_slice(&chunk[..n]);
        if let Some(pos) = find_headers(&header) {
            header_end = pos;
            break;
        }
        if header.len() > 1024 * 1024 {
            return Err(Error::Protocol);
        }
    }
    let head = String::from_utf8_lossy(&header[..header_end]);
    let mut lines = head.lines();
    let first = lines.next().ok_or(Error::Protocol)?;
    let mut request = first.split_whitespace();
    let method = request.next().ok_or(Error::Protocol)?;
    let path = request.next().ok_or(Error::Protocol)?;
    let mut content_length = 0_u64;
    let mut authorized = token.is_none();
    for line in lines {
        if let Some((key, value)) = line.split_once(':') {
            if key.eq_ignore_ascii_case("content-length") {
                content_length = value.trim().parse().map_err(|_| Error::Protocol)?;
            }
            if key.eq_ignore_ascii_case("authorization") {
                if let Some(token) = &token {
                    authorized = value.trim() == format!("Bearer {token}");
                }
            }
        }
    }
    if !authorized {
        respond_empty(&mut stream, 401).await?;
        return Ok(());
    }
    let segments = path.trim_start_matches('/').split('/').collect::<Vec<_>>();
    if segments.len() != 3 || segments[1] != "sha256" {
        respond_empty(&mut stream, 404).await?;
        return Ok(());
    }
    let digest = Digest::sha256(segments[2])?;
    let body_start = header_end + 4;
    let initial_body = &header[body_start..];

    match (method, segments[0]) {
        ("GET" | "HEAD", "content") => {
            let path = remote.path("content", &digest.value);
            if !fs::try_exists(&path).await? {
                respond_empty(&mut stream, 404).await?;
            } else {
                send_file(&mut stream, &path, method == "HEAD").await?;
            }
        }
        ("GET" | "HEAD", "artifacts") => {
            let path = remote.path("artifacts", &digest.value);
            if !fs::try_exists(&path).await? {
                respond_empty(&mut stream, 404).await?;
            } else {
                send_file(&mut stream, &path, method == "HEAD").await?;
            }
        }
        ("PUT", "content") if !read_only => {
            let id = ContentId(digest);
            let staging = remote.staging();
            if let Some(parent) = staging.parent() {
                fs::create_dir_all(parent).await?;
            }
            receive_body(&mut stream, &staging, content_length, initial_body).await?;
            remote.put_content_from(&id, &staging).await?;
            let _ = fs::remove_file(&staging).await;
            respond_empty(&mut stream, 201).await?;
        }
        ("PUT", "artifacts") if !read_only => {
            if content_length > 64 * 1024 * 1024 {
                return Err(Error::Protocol);
            }
            let bytes = receive_small_body(&mut stream, content_length, initial_body).await?;
            let id = ArtifactId(digest);
            remote.put_artifact(&id, &bytes).await?;
            respond_empty(&mut stream, 201).await?;
        }
        _ => respond_empty(&mut stream, 405).await?,
    }
    Ok(())
}

async fn receive_body(
    stream: &mut TcpStream,
    destination: &Path,
    length: u64,
    initial: &[u8],
) -> Result<()> {
    let mut out = fs::File::create(destination).await?;
    let first = usize::try_from(length.min(initial.len() as u64)).map_err(|_| Error::Protocol)?;
    out.write_all(&initial[..first]).await?;
    let mut remaining = length.saturating_sub(first as u64);
    let mut buf = vec![0_u8; 256 * 1024];
    while remaining > 0 {
        let want = usize::try_from(remaining.min(buf.len() as u64)).map_err(|_| Error::Protocol)?;
        let n = stream.read(&mut buf[..want]).await?;
        if n == 0 {
            return Err(Error::Protocol);
        }
        out.write_all(&buf[..n]).await?;
        remaining -= n as u64;
    }
    out.sync_all().await?;
    Ok(())
}
async fn receive_small_body(
    stream: &mut TcpStream,
    length: u64,
    initial: &[u8],
) -> Result<Vec<u8>> {
    let capacity = usize::try_from(length).map_err(|_| Error::Protocol)?;
    let mut bytes = Vec::with_capacity(capacity);
    let first = capacity.min(initial.len());
    bytes.extend_from_slice(&initial[..first]);
    while bytes.len() < capacity {
        let mut chunk = vec![0_u8; (capacity - bytes.len()).min(64 * 1024)];
        let n = stream.read(&mut chunk).await?;
        if n == 0 {
            return Err(Error::Protocol);
        }
        bytes.extend_from_slice(&chunk[..n]);
    }
    Ok(bytes)
}
async fn send_file(stream: &mut TcpStream, path: &Path, head_only: bool) -> Result<()> {
    let length = fs::metadata(path).await?.len();
    let headers =
        format!("HTTP/1.1 200 OK\r\nContent-Length: {length}\r\nConnection: close\r\n\r\n");
    stream.write_all(headers.as_bytes()).await?;
    if !head_only {
        let mut file = fs::File::open(path).await?;
        tokio::io::copy(&mut file, stream).await?;
    }
    Ok(())
}
fn find_headers(bytes: &[u8]) -> Option<usize> {
    bytes.windows(4).position(|window| window == b"\r\n\r\n")
}
async fn respond_empty(stream: &mut TcpStream, status: u16) -> std::io::Result<()> {
    let reason = match status {
        201 => "Created",
        401 => "Unauthorized",
        404 => "Not Found",
        405 => "Method Not Allowed",
        _ => "Error",
    };
    stream
        .write_all(
            format!("HTTP/1.1 {status} {reason}\r\nContent-Length: 0\r\nConnection: close\r\n\r\n")
                .as_bytes(),
        )
        .await
}
fn verify_bytes(expected: String, bytes: &[u8]) -> Result<()> {
    let actual = hash_bytes(bytes).to_string();
    if actual == expected {
        Ok(())
    } else {
        Err(Error::Integrity { expected, actual })
    }
}
async fn verify_file(expected: String, path: &Path) -> Result<()> {
    let mut file = fs::File::open(path).await?;
    let mut hasher = Sha256::new();
    let mut buf = vec![0_u8; 256 * 1024];
    loop {
        let n = file.read(&mut buf).await?;
        if n == 0 {
            break;
        }
        hasher.update(&buf[..n]);
    }
    let actual = format!("sha256:{}", hex::encode(hasher.finalize()));
    if actual == expected {
        Ok(())
    } else {
        Err(Error::Integrity { expected, actual })
    }
}
async fn read_optional(path: PathBuf) -> Result<Option<Vec<u8>>> {
    if !fs::try_exists(&path).await? {
        return Ok(None);
    }
    Ok(Some(fs::read(path).await?))
}
async fn write_atomic(path: PathBuf, bytes: &[u8]) -> Result<()> {
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent).await?;
    }
    let temporary = path.with_extension(format!("partial-{}", uuid_like()));
    fs::write(&temporary, bytes).await?;
    if fs::try_exists(&path).await? {
        let _ = fs::remove_file(&temporary).await;
    } else {
        fs::rename(temporary, path).await?;
    }
    Ok(())
}
fn uuid_like() -> String {
    format!("{}-{}", std::process::id(), chrono_like_nonce())
}
fn chrono_like_nonce() -> u128 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_or(0, |duration| duration.as_nanos())
}
