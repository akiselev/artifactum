use reqwest::{
    StatusCode,
    header::{
        CONTENT_RANGE, ETAG, HeaderMap, HeaderName, HeaderValue, IF_RANGE, LAST_MODIFIED, RANGE,
    },
};
use serde::{Deserialize, Serialize};
use std::{
    collections::BTreeMap,
    path::{Path, PathBuf},
    time::Duration,
};
use thiserror::Error;
use tokio::{fs, io::AsyncWriteExt, time::sleep};

#[derive(Debug, Error)]
pub enum Error {
    #[error("http error: {0}")]
    Http(#[from] reqwest::Error),
    #[error("io error: {0}")]
    Io(#[from] std::io::Error),
    #[error("serialization error: {0}")]
    Serde(#[from] serde_json::Error),
    #[error("invalid header `{0}`")]
    Header(String),
    #[error("server returned {status} for {url}")]
    Status { status: StatusCode, url: String },
    #[error("invalid resume response for {url}: {message}")]
    Resume { url: String, message: String },
}
pub type Result<T, E = Error> = std::result::Result<T, E>;
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct HttpRequest {
    pub url: String,
    #[serde(default)]
    pub headers: BTreeMap<String, String>,
    #[serde(default = "yes")]
    pub resume: bool,
    #[serde(default = "default_retries")]
    pub retries: u32,
}
const fn yes() -> bool {
    true
}
const fn default_retries() -> u32 {
    4
}
#[derive(Clone, Debug, Serialize, Deserialize)]
struct ResumeJournal {
    validator: String,
}
#[derive(Clone)]
pub struct HttpTransport {
    client: reqwest::Client,
}
impl Default for HttpTransport {
    fn default() -> Self {
        Self::new()
    }
}
impl HttpTransport {
    pub fn new() -> Self {
        Self {
            client: reqwest::Client::builder()
                .user_agent("artifactum/0.4")
                .build()
                .expect("reqwest client"),
        }
    }
    pub async fn execute(&self, request: &HttpRequest, dest: impl AsRef<Path>) -> Result<u64> {
        let dest = dest.as_ref();
        let mut attempt = 0;
        loop {
            match self.once(request, dest).await {
                Ok(n) => return Ok(n),
                Err(_error) if attempt < request.retries => {
                    attempt += 1;
                    sleep(Duration::from_millis(
                        200u64.saturating_mul(1 << attempt.min(6)),
                    ))
                    .await;
                }
                Err(e) => return Err(e),
            }
        }
    }
    async fn once(&self, r: &HttpRequest, dest: &Path) -> Result<u64> {
        let journal_path = resume_journal_path(dest);
        let mut existing = if r.resume && fs::try_exists(dest).await? {
            fs::metadata(dest).await?.len()
        } else {
            0
        };
        let mut journal = if existing > 0 {
            load_resume_journal(&journal_path).await?
        } else {
            None
        };
        // A byte range by itself does not prove the remote representation is still
        // the one that produced our partial. Resume only with a validator suitable
        // for If-Range; otherwise restart from byte zero.
        if existing > 0 && journal.is_none() {
            remove_partial(dest, &journal_path).await;
            existing = 0;
        }
        let mut headers = HeaderMap::new();
        for (k, v) in &r.headers {
            headers.insert(
                HeaderName::from_bytes(k.as_bytes()).map_err(|_| Error::Header(k.clone()))?,
                HeaderValue::from_str(v).map_err(|_| Error::Header(k.clone()))?,
            );
        }
        let mut req = self.client.get(&r.url).headers(headers);
        if existing > 0 {
            req = req.header(RANGE, format!("bytes={existing}-"));
            if let Some(value) = journal.as_ref().map(|j| j.validator.as_str()) {
                req = req.header(IF_RANGE, value);
            }
        }
        let res = req.send().await?;
        if existing > 0 && res.status() == StatusCode::RANGE_NOT_SATISFIABLE {
            remove_partial(dest, &journal_path).await;
            return Err(Error::Resume {
                url: r.url.clone(),
                message: "server rejected stored partial range; partial reset".into(),
            });
        }
        if !res.status().is_success() && res.status() != StatusCode::PARTIAL_CONTENT {
            return Err(Error::Status {
                status: res.status(),
                url: r.url.clone(),
            });
        }
        let append = existing > 0 && res.status() == StatusCode::PARTIAL_CONTENT;
        if append {
            let range = res
                .headers()
                .get(CONTENT_RANGE)
                .and_then(|value| value.to_str().ok())
                .ok_or_else(|| Error::Resume {
                    url: r.url.clone(),
                    message: "206 response omitted Content-Range".into(),
                })?;
            let start = parse_content_range_start(range).ok_or_else(|| Error::Resume {
                url: r.url.clone(),
                message: format!("invalid Content-Range `{range}`"),
            })?;
            if start != existing {
                remove_partial(dest, &journal_path).await;
                return Err(Error::Resume {
                    url: r.url.clone(),
                    message: format!(
                        "expected resumed offset {existing}, server returned {start}; partial reset"
                    ),
                });
            }
        }
        // A 200 after an If-Range request means the validator no longer matches;
        // overwrite the stale partial with the newly selected representation.
        if !append {
            existing = 0;
            journal =
                response_validator(res.headers()).map(|validator| ResumeJournal { validator });
            if let Some(value) = journal.as_ref() {
                write_resume_journal(&journal_path, value).await?
            } else {
                let _ = fs::remove_file(&journal_path).await;
            }
        }
        let mut file = if append {
            fs::OpenOptions::new().append(true).open(dest).await?
        } else {
            fs::File::create(dest).await?
        };
        let mut total = if append { existing } else { 0 };
        let mut res = res;
        while let Some(chunk) = res.chunk().await? {
            file.write_all(&chunk).await?;
            total += chunk.len() as u64;
        }
        file.sync_all().await?;
        let _ = fs::remove_file(&journal_path).await;
        Ok(total)
    }
}
fn resume_journal_path(dest: &Path) -> PathBuf {
    let mut name = dest.as_os_str().to_os_string();
    name.push(".resume.json");
    PathBuf::from(name)
}
async fn load_resume_journal(path: &Path) -> Result<Option<ResumeJournal>> {
    if !fs::try_exists(path).await? {
        return Ok(None);
    };
    let bytes = fs::read(path).await?;
    match serde_json::from_slice(&bytes) {
        Ok(value) => Ok(Some(value)),
        Err(_) => {
            let _ = fs::remove_file(path).await;
            Ok(None)
        }
    }
}
async fn write_resume_journal(path: &Path, journal: &ResumeJournal) -> Result<()> {
    let temp = path.with_extension("resume.json.partial");
    fs::write(&temp, serde_json::to_vec(journal)?).await?;
    fs::rename(temp, path).await?;
    Ok(())
}
async fn remove_partial(dest: &Path, journal: &Path) {
    let _ = fs::remove_file(dest).await;
    let _ = fs::remove_file(journal).await;
}
fn response_validator(headers: &HeaderMap) -> Option<String> {
    headers
        .get(ETAG)
        .or_else(|| headers.get(LAST_MODIFIED))
        .and_then(|value| value.to_str().ok())
        .map(str::to_owned)
}
fn parse_content_range_start(value: &str) -> Option<u64> {
    let range = value.strip_prefix("bytes ")?.split('/').next()?;
    range.split('-').next()?.parse().ok()
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::{
        io::{AsyncReadExt, AsyncWriteExt},
        net::TcpListener,
    };
    #[tokio::test]
    async fn interrupted_transfer_resumes_across_execute_calls() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let body = b"abcdefghijklmnopqrstuvwxyz0123456789".to_vec();
        let body_for_server = body.clone();
        let server = tokio::spawn(async move {
            // First request: claim full length, provide a validator, then disconnect
            // after ten bytes. The journal must survive the failed execute call.
            let (mut socket, _) = listener.accept().await.unwrap();
            let mut request = vec![0u8; 4096];
            let _ = socket.read(&mut request).await.unwrap();
            socket.write_all(format!("HTTP/1.1 200 OK\r\nContent-Length: {}\r\nETag: \"v1\"\r\nConnection: close\r\n\r\n",body_for_server.len()).as_bytes()).await.unwrap();
            socket.write_all(&body_for_server[..10]).await.unwrap();
            drop(socket);
            // Second execute call must carry Range and If-Range, then append only the
            // validated remainder.
            let (mut socket, _) = listener.accept().await.unwrap();
            let n = socket.read(&mut request).await.unwrap();
            let text = String::from_utf8_lossy(&request[..n]).to_ascii_lowercase();
            assert!(text.contains("range: bytes=10-"));
            assert!(text.contains("if-range: \"v1\""));
            socket.write_all(format!("HTTP/1.1 206 Partial Content\r\nContent-Length: {}\r\nContent-Range: bytes 10-{}/{}\r\nETag: \"v1\"\r\nConnection: close\r\n\r\n",body_for_server.len()-10,body_for_server.len()-1,body_for_server.len()).as_bytes()).await.unwrap();
            socket.write_all(&body_for_server[10..]).await.unwrap();
        });
        let temp = tempfile::tempdir().unwrap();
        let destination = temp.path().join("partial");
        let transport = HttpTransport::new();
        let request = HttpRequest {
            url: format!("http://{address}/data"),
            headers: BTreeMap::new(),
            resume: true,
            retries: 0,
        };
        assert!(transport.execute(&request, &destination).await.is_err());
        assert_eq!(tokio::fs::metadata(&destination).await.unwrap().len(), 10);
        assert!(
            tokio::fs::try_exists(resume_journal_path(&destination))
                .await
                .unwrap()
        );
        let size = transport.execute(&request, &destination).await.unwrap();
        assert_eq!(size, body.len() as u64);
        assert_eq!(tokio::fs::read(&destination).await.unwrap(), body);
        assert!(
            !tokio::fs::try_exists(resume_journal_path(&destination))
                .await
                .unwrap()
        );
        server.await.unwrap();
    }
    #[tokio::test]
    async fn validator_change_restarts_instead_of_appending() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let old = b"old-old-old".to_vec();
        let new = b"new representation".to_vec();
        let server = tokio::spawn({
            let old = old.clone();
            let new = new.clone();
            async move {
                let (mut socket, _) = listener.accept().await.unwrap();
                let mut request = vec![0u8; 4096];
                let _ = socket.read(&mut request).await.unwrap();
                socket.write_all(format!("HTTP/1.1 200 OK\r\nContent-Length: {}\r\nETag: \"old\"\r\nConnection: close\r\n\r\n",old.len()).as_bytes()).await.unwrap();
                socket.write_all(&old[..4]).await.unwrap();
                drop(socket);
                let (mut socket, _) = listener.accept().await.unwrap();
                let n = socket.read(&mut request).await.unwrap();
                let text = String::from_utf8_lossy(&request[..n]).to_ascii_lowercase();
                assert!(text.contains("range: bytes=4-"));
                assert!(text.contains("if-range: \"old\""));
                socket.write_all(format!("HTTP/1.1 200 OK\r\nContent-Length: {}\r\nETag: \"new\"\r\nConnection: close\r\n\r\n",new.len()).as_bytes()).await.unwrap();
                socket.write_all(&new).await.unwrap();
            }
        });
        let temp = tempfile::tempdir().unwrap();
        let destination = temp.path().join("partial");
        let transport = HttpTransport::new();
        let request = HttpRequest {
            url: format!("http://{address}/data"),
            headers: BTreeMap::new(),
            resume: true,
            retries: 0,
        };
        assert!(transport.execute(&request, &destination).await.is_err());
        transport.execute(&request, &destination).await.unwrap();
        assert_eq!(tokio::fs::read(destination).await.unwrap(), new);
        server.await.unwrap();
    }
}
