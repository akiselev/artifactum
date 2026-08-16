//! Artifactum provider subprocess protocol.
//!
//! Provider executables are named `artifactum-provider-*` and are discovered on
//! `PATH`. When launched with `--artifactum-plugin` they speak a small,
//! versioned JSON RPC protocol over stdin/stdout using LSP-style Content-Length
//! framing. Provider logs belong on stderr.

use std::{
    collections::BTreeSet,
    ffi::OsStr,
    path::{Path, PathBuf},
    process::Stdio,
};

use artifactum_core::{
    AcquireContext, Acquisition, ArtifactProvider, ArtifactRequirement, Error as CoreError,
    ProviderDescriptor, ResolveContext, ResolvedFile, Resolution, SearchRequest, SearchResult,
};
use async_trait::async_trait;
use serde::{Deserialize, Serialize, de::DeserializeOwned};
use serde_json::Value;
use thiserror::Error;
use tokio::{
    io::{
        AsyncBufRead, AsyncBufReadExt, AsyncReadExt, AsyncWrite, AsyncWriteExt, BufReader,
        BufWriter,
    },
    process::Command,
};

pub const PROTOCOL_MAJOR: u32 = 1;
pub const PROTOCOL_MINOR: u32 = 0;
pub const JSONRPC_VERSION: &str = "2.0";
pub const PLUGIN_MODE_FLAG: &str = "--artifactum-plugin";
pub const PLUGIN_PREFIX: &str = "artifactum-provider-";

#[derive(Debug, Error)]
pub enum Error {
    #[error("I/O error: {0}")]
    Io(#[from] std::io::Error),
    #[error("protocol serialization error: {0}")]
    Serialization(#[from] serde_json::Error),
    #[error("invalid plugin frame: {0}")]
    InvalidFrame(String),
    #[error("plugin `{plugin}` exited before returning a response")]
    UnexpectedEof { plugin: String },
    #[error("plugin `{plugin}` error {code}: {message}")]
    Remote {
        plugin: String,
        code: i64,
        message: String,
    },
    #[error("plugin `{plugin}` returned response id {actual}, expected {expected}")]
    ResponseId {
        plugin: String,
        expected: u64,
        actual: u64,
    },
    #[error("plugin protocol {major}.{minor} is incompatible with host {host_major}.{host_minor}")]
    IncompatibleVersion {
        major: u32,
        minor: u32,
        host_major: u32,
        host_minor: u32,
    },
}

pub type Result<T, E = Error> = std::result::Result<T, E>;

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct InitializeRequest {
    pub protocol_major: u32,
    pub protocol_minor: u32,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct InitializeResponse {
    pub protocol_major: u32,
    pub protocol_minor: u32,
    pub provider: ProviderDescriptor,
}

#[derive(Debug, Serialize, Deserialize)]
struct RpcRequest {
    jsonrpc: String,
    id: u64,
    method: String,
    #[serde(default)]
    params: Value,
}

#[derive(Debug, Serialize, Deserialize)]
struct RpcResponse {
    jsonrpc: String,
    id: u64,
    #[serde(skip_serializing_if = "Option::is_none")]
    result: Option<Value>,
    #[serde(skip_serializing_if = "Option::is_none")]
    error: Option<RpcError>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
struct RpcError {
    code: i64,
    message: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    data: Option<Value>,
}

#[derive(Debug, Serialize, Deserialize)]
struct ResolveParams {
    requirement: ArtifactRequirement,
    context: ResolveContext,
}

#[derive(Debug, Serialize, Deserialize)]
struct AcquireParams {
    file: ResolvedFile,
    destination: PathBuf,
    context: AcquireContext,
}

#[derive(Debug, Serialize, Deserialize)]
struct SearchParams {
    request: SearchRequest,
    context: ResolveContext,
}

/// Run a provider as an Artifactum plugin server.
pub async fn serve<P: ArtifactProvider>(provider: P) -> Result<()> {
    let stdin = tokio::io::stdin();
    let stdout = tokio::io::stdout();
    let mut reader = BufReader::new(stdin);
    let mut writer = BufWriter::new(stdout);

    while let Some(request) = read_frame::<_, RpcRequest>(&mut reader).await? {
        let id = request.id;
        let response = match dispatch(&provider, request).await {
            Ok(result) => RpcResponse {
                jsonrpc: JSONRPC_VERSION.to_owned(),
                id,
                result: Some(result),
                error: None,
            },
            Err(error) => RpcResponse {
                jsonrpc: JSONRPC_VERSION.to_owned(),
                id,
                result: None,
                error: Some(RpcError {
                    code: -32_000,
                    message: error.to_string(),
                    data: None,
                }),
            },
        };
        write_frame(&mut writer, &response).await?;
        writer.flush().await?;
    }
    Ok(())
}

async fn dispatch<P: ArtifactProvider>(provider: &P, request: RpcRequest) -> artifactum_core::Result<Value> {
    match request.method.as_str() {
        "initialize" => {
            let params: InitializeRequest = serde_json::from_value(request.params)?;
            if params.protocol_major != PROTOCOL_MAJOR {
                return Err(CoreError::Provider {
                    provider: provider.descriptor().name,
                    message: format!(
                        "unsupported protocol {}.{}; provider speaks {}.{}",
                        params.protocol_major, params.protocol_minor, PROTOCOL_MAJOR, PROTOCOL_MINOR
                    ),
                });
            }
            Ok(serde_json::to_value(InitializeResponse {
                protocol_major: PROTOCOL_MAJOR,
                protocol_minor: PROTOCOL_MINOR,
                provider: provider.descriptor(),
            })?)
        }
        "resolve" => {
            let params: ResolveParams = serde_json::from_value(request.params)?;
            Ok(serde_json::to_value(
                provider.resolve(&params.requirement, &params.context).await?,
            )?)
        }
        "acquire" => {
            let params: AcquireParams = serde_json::from_value(request.params)?;
            Ok(serde_json::to_value(
                provider
                    .acquire(&params.file, &params.destination, &params.context)
                    .await?,
            )?)
        }
        "search" => {
            let params: SearchParams = serde_json::from_value(request.params)?;
            Ok(serde_json::to_value(
                provider.search(&params.request, &params.context).await?,
            )?)
        }
        method => Err(CoreError::Provider {
            provider: provider.descriptor().name,
            message: format!("unknown plugin method `{method}`"),
        }),
    }
}

#[derive(Clone, Debug)]
pub struct PluginProvider {
    executable: PathBuf,
    descriptor: ProviderDescriptor,
}

impl PluginProvider {
    pub async fn load(executable: impl Into<PathBuf>) -> Result<Self> {
        let executable = executable.into();
        let response: InitializeResponse = call_plugin(
            &executable,
            1,
            "initialize",
            &InitializeRequest {
                protocol_major: PROTOCOL_MAJOR,
                protocol_minor: PROTOCOL_MINOR,
            },
        )
        .await?;
        if response.protocol_major != PROTOCOL_MAJOR {
            return Err(Error::IncompatibleVersion {
                major: response.protocol_major,
                minor: response.protocol_minor,
                host_major: PROTOCOL_MAJOR,
                host_minor: PROTOCOL_MINOR,
            });
        }
        Ok(Self {
            executable,
            descriptor: response.provider,
        })
    }

    #[must_use]
    pub fn executable(&self) -> &Path {
        &self.executable
    }

    async fn call<P: Serialize + ?Sized, R: DeserializeOwned>(
        &self,
        method: &str,
        params: &P,
    ) -> std::result::Result<R, CoreError> {
        call_plugin(&self.executable, 1, method, params)
            .await
            .map_err(|error| CoreError::Provider {
                provider: self.descriptor.name.clone(),
                message: error.to_string(),
            })
    }
}

#[async_trait]
impl ArtifactProvider for PluginProvider {
    fn descriptor(&self) -> ProviderDescriptor {
        self.descriptor.clone()
    }

    async fn resolve(
        &self,
        requirement: &ArtifactRequirement,
        context: &ResolveContext,
    ) -> artifactum_core::Result<Resolution> {
        self.call(
            "resolve",
            &ResolveParams {
                requirement: requirement.clone(),
                context: context.clone(),
            },
        )
        .await
    }

    async fn acquire(
        &self,
        file: &ResolvedFile,
        destination: &Path,
        context: &AcquireContext,
    ) -> artifactum_core::Result<Acquisition> {
        self.call(
            "acquire",
            &AcquireParams {
                file: file.clone(),
                destination: destination.to_path_buf(),
                context: context.clone(),
            },
        )
        .await
    }

    async fn search(
        &self,
        request: &SearchRequest,
        context: &ResolveContext,
    ) -> artifactum_core::Result<Vec<SearchResult>> {
        self.call(
            "search",
            &SearchParams {
                request: request.clone(),
                context: context.clone(),
            },
        )
        .await
    }
}

async fn call_plugin<P: Serialize + ?Sized, R: DeserializeOwned>(
    executable: &Path,
    id: u64,
    method: &str,
    params: &P,
) -> Result<R> {
    let plugin_name = executable.display().to_string();
    let mut child = Command::new(executable)
        .arg(PLUGIN_MODE_FLAG)
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::inherit())
        .kill_on_drop(true)
        .spawn()?;

    let mut stdin = child.stdin.take().ok_or_else(|| Error::UnexpectedEof {
        plugin: plugin_name.clone(),
    })?;
    let stdout = child.stdout.take().ok_or_else(|| Error::UnexpectedEof {
        plugin: plugin_name.clone(),
    })?;
    let mut stdout = BufReader::new(stdout);

    let request = RpcRequest {
        jsonrpc: JSONRPC_VERSION.to_owned(),
        id,
        method: method.to_owned(),
        params: serde_json::to_value(params)?,
    };
    write_frame(&mut stdin, &request).await?;
    stdin.shutdown().await?;
    drop(stdin);

    let response = read_frame::<_, RpcResponse>(&mut stdout)
        .await?
        .ok_or_else(|| Error::UnexpectedEof {
            plugin: plugin_name.clone(),
        })?;

    let _ = child.wait().await?;

    if response.id != id {
        return Err(Error::ResponseId {
            plugin: plugin_name,
            expected: id,
            actual: response.id,
        });
    }
    if let Some(error) = response.error {
        return Err(Error::Remote {
            plugin: plugin_name,
            code: error.code,
            message: error.message,
        });
    }
    let result = response.result.ok_or_else(|| Error::InvalidFrame(
        "response contained neither result nor error".to_owned(),
    ))?;
    Ok(serde_json::from_value(result)?)
}

pub async fn write_frame<W: AsyncWrite + Unpin, T: Serialize>(writer: &mut W, value: &T) -> Result<()> {
    let payload = serde_json::to_vec(value)?;
    writer
        .write_all(format!("Content-Length: {}\r\n\r\n", payload.len()).as_bytes())
        .await?;
    writer.write_all(&payload).await?;
    writer.flush().await?;
    Ok(())
}

pub async fn read_frame<R: AsyncBufRead + Unpin, T: DeserializeOwned>(reader: &mut R) -> Result<Option<T>> {
    let mut content_length = None;
    let mut saw_header = false;
    loop {
        let mut line = String::new();
        let bytes = reader.read_line(&mut line).await?;
        if bytes == 0 {
            if saw_header {
                return Err(Error::InvalidFrame("EOF inside frame headers".to_owned()));
            }
            return Ok(None);
        }
        saw_header = true;
        let line = line.trim_end_matches(&['\r', '\n'][..]);
        if line.is_empty() {
            break;
        }
        if let Some((name, value)) = line.split_once(':') {
            if name.eq_ignore_ascii_case("Content-Length") {
                content_length = Some(
                    value
                        .trim()
                        .parse::<usize>()
                        .map_err(|_| Error::InvalidFrame(format!("invalid Content-Length `{value}`")))?,
                );
            }
        }
    }

    let content_length = content_length
        .ok_or_else(|| Error::InvalidFrame("missing Content-Length header".to_owned()))?;
    if content_length > 16 * 1024 * 1024 {
        return Err(Error::InvalidFrame(format!(
            "frame is too large: {content_length} bytes"
        )));
    }
    let mut payload = vec![0_u8; content_length];
    reader.read_exact(&mut payload).await?;
    Ok(Some(serde_json::from_slice(&payload)?))
}

#[derive(Clone, Debug)]
pub struct PluginLoadError {
    pub path: PathBuf,
    pub error: String,
}

#[derive(Clone, Debug, Default)]
pub struct PluginDiscovery {
    pub providers: Vec<PluginProvider>,
    pub errors: Vec<PluginLoadError>,
}

pub fn discover_plugin_paths() -> Vec<PathBuf> {
    let mut paths = BTreeSet::new();
    let search_paths = [
        std::env::var_os("ARTIFACTUM_PLUGIN_PATH"),
        std::env::var_os("PATH"),
    ];

    for path_var in search_paths.into_iter().flatten() {
        for directory in std::env::split_paths(&path_var) {
            let Ok(entries) = std::fs::read_dir(directory) else {
                continue;
            };
            for entry in entries.flatten() {
                let path = entry.path();
                let Some(file_name) = path.file_name().and_then(OsStr::to_str) else {
                    continue;
                };
                let normalized = file_name.strip_suffix(".exe").unwrap_or(file_name);
                if normalized.starts_with(PLUGIN_PREFIX) && path.is_file() {
                    paths.insert(path);
                }
            }
        }
    }
    paths.into_iter().collect()
}

pub async fn discover_plugins() -> PluginDiscovery {
    let mut discovery = PluginDiscovery::default();
    for path in discover_plugin_paths() {
        match PluginProvider::load(path.clone()).await {
            Ok(provider) => discovery.providers.push(provider),
            Err(error) => discovery.errors.push(PluginLoadError {
                path,
                error: error.to_string(),
            }),
        }
    }
    discovery
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::io::AsyncWriteExt as _;

    #[tokio::test]
    async fn frame_round_trip() {
        let expected = InitializeRequest {
            protocol_major: 1,
            protocol_minor: 0,
        };
        let (mut writer, reader) = tokio::io::duplex(4096);
        write_frame(&mut writer, &expected).await.unwrap();
        writer.shutdown().await.unwrap();
        let mut reader = BufReader::new(reader);
        let actual: InitializeRequest = read_frame(&mut reader).await.unwrap().unwrap();
        assert_eq!(actual.protocol_major, expected.protocol_major);
    }
}
