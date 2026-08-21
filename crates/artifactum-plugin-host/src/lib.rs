//! Persistent, multiplexed plugin process sessions plus a daemonkit-backed
//! cross-invocation host. Plugin executables know nothing about daemonkit.

use artifactum_plugin_protocol::{Frame, Request, Response, read_frame, write_frame};
use daemonkit::{Bootstrap, Daemon, DaemonSpec, Embedded, Spawn};
use futures_util::StreamExt;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::{
    collections::HashMap,
    path::{Path, PathBuf},
    process::Stdio,
    sync::Arc,
    time::Duration,
};
use thiserror::Error;
use tokio::{
    io::{AsyncBufReadExt, AsyncReadExt, AsyncWriteExt, BufReader, BufWriter},
    process::{Child, ChildStdin, Command},
    sync::{Mutex, oneshot},
};

#[derive(Debug, Error)]
pub enum Error {
    #[error("I/O error: {0}")]
    Io(#[from] std::io::Error),
    #[error("protocol error: {0}")]
    Protocol(#[from] artifactum_plugin_protocol::Error),
    #[error("daemonkit error: {0}")]
    Daemon(#[from] daemonkit::Error),
    #[error("daemonkit configuration error: {0}")]
    DaemonConfig(#[from] daemonkit::ConfigError),
    #[error("daemonkit bootstrap error: {0}")]
    Bootstrap(#[from] daemonkit::BootstrapError),
    #[error("serialization error: {0}")]
    Serde(#[from] serde_json::Error),
    #[error("plugin returned error: {message}")]
    Remote {
        message: String,
        data: Option<Value>,
    },
    #[error("plugin transport unavailable")]
    Transport,
    #[error("plugin host: {0}")]
    Host(String),
}
pub type Result<T, E = Error> = std::result::Result<T, E>;

#[derive(Clone, Debug, Serialize, Deserialize)]
struct RemoteFailure {
    message: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    data: Option<Value>,
}
type Pending =
    Arc<Mutex<HashMap<uuid::Uuid, oneshot::Sender<std::result::Result<Value, RemoteFailure>>>>>;
struct Running {
    child: Child,
    writer: Arc<Mutex<BufWriter<ChildStdin>>>,
    pending: Pending,
}

/// One provider/executor/verifier process. Writes are serialized, responses are
/// dispatched by request id, so calls can remain in flight concurrently.
#[derive(Clone)]
pub struct PluginSession {
    path: PathBuf,
    state: Arc<Mutex<Option<Running>>>,
}
impl PluginSession {
    pub fn new(path: impl Into<PathBuf>) -> Self {
        Self {
            path: path.into(),
            state: Arc::new(Mutex::new(None)),
        }
    }
    async fn endpoints(&self) -> Result<(Arc<Mutex<BufWriter<ChildStdin>>>, Pending)> {
        let mut state = self.state.lock().await;
        let restart = match state.as_mut() {
            Some(r) => r.child.try_wait()?.is_some(),
            None => true,
        };
        if restart {
            *state = None;
            let mut child = Command::new(&self.path)
                .arg("serve-plugin")
                .stdin(Stdio::piped())
                .stdout(Stdio::piped())
                .stderr(Stdio::inherit())
                .kill_on_drop(true)
                .spawn()?;
            let stdin = child.stdin.take().ok_or(Error::Transport)?;
            let stdout = child.stdout.take().ok_or(Error::Transport)?;
            let writer = Arc::new(Mutex::new(BufWriter::new(stdin)));
            let pending: Pending = Arc::new(Mutex::new(HashMap::new()));
            let reader_pending = Arc::clone(&pending);
            tokio::spawn(async move {
                let mut reader = BufReader::new(stdout);
                loop {
                    match read_frame(&mut reader).await {
                        Ok(Frame::Response(Response {
                            id,
                            ok,
                            result,
                            error,
                            error_data,
                        })) => {
                            if let Some(tx) = reader_pending.lock().await.remove(&id) {
                                let _ = tx.send(if ok {
                                    Ok(result)
                                } else {
                                    Err(RemoteFailure {
                                        message: error
                                            .unwrap_or_else(|| "unknown plugin error".into()),
                                        data: error_data,
                                    })
                                });
                            }
                        }
                        Ok(Frame::Notification(_)) => {}
                        Ok(Frame::Request(_)) => {}
                        Err(e) => {
                            let message = e.to_string();
                            let mut p = reader_pending.lock().await;
                            for (_, tx) in p.drain() {
                                let _ = tx.send(Err(RemoteFailure {
                                    message: message.clone(),
                                    data: None,
                                }));
                            }
                            break;
                        }
                    }
                }
            });
            *state = Some(Running {
                child,
                writer: Arc::clone(&writer),
                pending: Arc::clone(&pending),
            });
            return Ok((writer, pending));
        }
        let r = state.as_ref().ok_or(Error::Transport)?;
        Ok((Arc::clone(&r.writer), Arc::clone(&r.pending)))
    }
    pub async fn call(&self, request: Request) -> Result<Value> {
        let (writer, pending) = self.endpoints().await?;
        let (tx, rx) = oneshot::channel();
        pending.lock().await.insert(request.id, tx);
        if let Err(e) =
            write_frame(&mut *writer.lock().await, &Frame::Request(request.clone())).await
        {
            pending.lock().await.remove(&request.id);
            return Err(e.into());
        }
        match rx.await {
            Ok(Ok(v)) => Ok(v),
            Ok(Err(e)) => Err(Error::Remote {
                message: e.message,
                data: e.data,
            }),
            Err(_) => Err(Error::Transport),
        }
    }
    pub async fn restart(&self) {
        let mut state = self.state.lock().await;
        if let Some(mut r) = state.take() {
            let _ = r.child.kill().await;
            let mut p = r.pending.lock().await;
            for (_, tx) in p.drain() {
                let _ = tx.send(Err(RemoteFailure {
                    message: "plugin restarted".into(),
                    data: None,
                }));
            }
        }
    }
}

#[derive(Clone, Default)]
pub struct PluginHost {
    sessions: Arc<Mutex<HashMap<PathBuf, PluginSession>>>,
}
impl PluginHost {
    pub async fn session(&self, path: impl Into<PathBuf>) -> PluginSession {
        let path = path.into();
        let mut s = self.sessions.lock().await;
        s.entry(path.clone())
            .or_insert_with(|| PluginSession::new(path))
            .clone()
    }
    pub async fn call(&self, path: impl Into<PathBuf>, request: Request) -> Result<Value> {
        let session = self.session(path).await;
        match session.call(request.clone()).await {
            Ok(v) => Ok(v),
            Err(Error::Protocol(_) | Error::Transport) => {
                session.restart().await;
                session.call(request).await
            }
            Err(e) => Err(e),
        }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
struct HostRequest {
    executable: PathBuf,
    request: Request,
}
#[derive(Clone, Debug, Serialize, Deserialize)]
struct HostResponse {
    result: Option<Value>,
    error: Option<RemoteFailure>,
}
#[derive(Default)]
struct HostState {
    host: PluginHost,
}

/// Must be called before normal CLI parsing. Returns true only in a daemonkit
/// bootstrap child, after serving until idle/shutdown.
pub async fn maybe_run_daemon() -> Result<bool> {
    let Some(bootstrap) = Bootstrap::detect()? else {
        return Ok(false);
    };
    bootstrap.run_embedded_fn(|_context,mut incoming,mut shutdown|async move{let state=Arc::new(HostState::default());loop{tokio::select!{_ = shutdown.requested()=>break,item=incoming.next()=>{let Some(item)=item else{break};if let Ok(stream)=item{let state=Arc::clone(&state);tokio::spawn(async move{let _=handle_connection(state,stream).await;});}}}}Ok::<_,std::io::Error>(())}).await?;
    Ok(true)
}
async fn handle_connection(
    state: Arc<HostState>,
    stream: daemonkit::AuthenticatedStream,
) -> Result<()> {
    let (read, mut write) = tokio::io::split(stream);
    let mut reader = BufReader::new(read);
    let mut line = String::new();
    reader.read_line(&mut line).await?;
    let len = line
        .trim()
        .parse::<usize>()
        .map_err(|e| Error::Host(e.to_string()))?;
    if len > 64 * 1024 * 1024 {
        return Err(Error::Host("request too large".into()));
    }
    let mut body = vec![0; len];
    reader.read_exact(&mut body).await?;
    let req: HostRequest = serde_json::from_slice(&body)?;
    let response = match state.host.call(req.executable, req.request).await {
        Ok(v) => HostResponse {
            result: Some(v),
            error: None,
        },
        Err(Error::Remote { message, data }) => HostResponse {
            result: None,
            error: Some(RemoteFailure { message, data }),
        },
        Err(e) => HostResponse {
            result: None,
            error: Some(RemoteFailure {
                message: e.to_string(),
                data: None,
            }),
        },
    };
    let bytes = serde_json::to_vec(&response)?;
    write
        .write_all(format!("{}\n", bytes.len()).as_bytes())
        .await?;
    write.write_all(&bytes).await?;
    write.shutdown().await?;
    Ok(())
}
fn daemon() -> Result<Daemon<Embedded>> {
    let spec = DaemonSpec::new("io.artifactum.plugin-host")?
        .idle_timeout(Some(Duration::from_secs(30 * 60)));
    Ok(Daemon::embedded(
        spec,
        Spawn::current_exe()?.arg("__artifactum_plugin_host"),
    )?)
}

/// Client used by ordinary CLI processes. The daemon owns the child plugin
/// process across CLI invocations and multiplexes requests into it.
#[derive(Clone)]
pub struct DaemonPluginSession {
    path: PathBuf,
    daemon: Arc<Daemon<Embedded>>,
}
impl DaemonPluginSession {
    pub fn new(path: impl Into<PathBuf>) -> Result<Self> {
        Ok(Self {
            path: path.into(),
            daemon: Arc::new(daemon()?),
        })
    }
    pub async fn call(&self, request: Request) -> Result<Value> {
        let instance = self.daemon.ensure().await?;
        let stream = instance.connect().await?;
        let (read, mut write) = tokio::io::split(stream);
        let body = serde_json::to_vec(&HostRequest {
            executable: self.path.clone(),
            request,
        })?;
        write
            .write_all(format!("{}\n", body.len()).as_bytes())
            .await?;
        write.write_all(&body).await?;
        write.flush().await?;
        let mut reader = BufReader::new(read);
        let mut line = String::new();
        reader.read_line(&mut line).await?;
        let len = line
            .trim()
            .parse::<usize>()
            .map_err(|e| Error::Host(e.to_string()))?;
        let mut bytes = vec![0; len];
        reader.read_exact(&mut bytes).await?;
        let response: HostResponse = serde_json::from_slice(&bytes)?;
        match (response.result, response.error) {
            (Some(v), None) => Ok(v),
            (_, Some(e)) => Err(Error::Remote {
                message: e.message,
                data: e.data,
            }),
            _ => Err(Error::Host("invalid host response".into())),
        }
    }
}

pub fn discover(prefix: &str) -> Vec<PathBuf> {
    let mut out = Vec::new();
    if let Some(paths) = std::env::var_os("ARTIFACTUM_PLUGIN_PATH") {
        for dir in std::env::split_paths(&paths) {
            discover_in(&dir, prefix, &mut out)
        }
    }
    if let Some(paths) = std::env::var_os("PATH") {
        for dir in std::env::split_paths(&paths) {
            discover_in(&dir, prefix, &mut out)
        }
    }
    out.sort();
    out.dedup();
    out
}
fn discover_in(dir: &Path, prefix: &str, out: &mut Vec<PathBuf>) {
    if let Ok(rd) = std::fs::read_dir(dir) {
        for e in rd.flatten() {
            let n = e.file_name().to_string_lossy().into_owned();
            if n.starts_with(prefix) && e.path().is_file() {
                out.push(e.path());
            }
        }
    }
}
