//! Execution backends. Executors schedule already-materialized commands; they do
//! not own artifact identity or caching.

use artifactum_core::{ActionKey, ExecutionMetrics, NetworkPolicy, ResourceSpec};
use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use std::{
    collections::BTreeMap,
    path::{Path, PathBuf},
    process::Stdio,
    time::{Duration, Instant},
};
use thiserror::Error;
use tokio::{
    fs,
    io::{AsyncReadExt, AsyncWriteExt},
    process::Command,
    time::{sleep, timeout},
};

#[derive(Debug, Error)]
pub enum Error {
    #[error("I/O error: {0}")]
    Io(#[from] std::io::Error),
    #[error("executor `{0}` is unavailable")]
    Unavailable(String),
    #[error("execution timed out after {0}s")]
    Timeout(u64),
    #[error("plugin protocol error: {0}")]
    Protocol(String),
    #[error("serialization error: {0}")]
    Json(#[from] serde_json::Error),
}
pub type Result<T, E = Error> = std::result::Result<T, E>;

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ExecutionRequest {
    pub action: ActionKey,
    pub command: Vec<String>,
    pub cwd: PathBuf,
    #[serde(default)]
    pub env: BTreeMap<String, String>,
    #[serde(default)]
    pub resources: ResourceSpec,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub container: Option<String>,
    #[serde(default)]
    pub network: NetworkPolicy,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub cancel_file: Option<PathBuf>,
}
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ExecutionResult {
    pub exit_code: i32,
    pub stdout: Vec<u8>,
    pub stderr: Vec<u8>,
    pub metrics: ExecutionMetrics,
}

#[async_trait]
pub trait Executor: Send + Sync {
    fn name(&self) -> &str;
    async fn execute(&self, request: &ExecutionRequest) -> Result<ExecutionResult>;
    async fn cancel(&self, _action: &ActionKey) -> Result<()> {
        Ok(())
    }
}

#[derive(Clone, Default)]
pub struct LocalExecutor;
#[async_trait]
impl Executor for LocalExecutor {
    fn name(&self) -> &str {
        "local"
    }
    async fn execute(&self, r: &ExecutionRequest) -> Result<ExecutionResult> {
        run_process(
            &r.command,
            &r.cwd,
            &r.env,
            &r.resources,
            r.cancel_file.as_deref(),
        )
        .await
    }
}

#[derive(Clone, Default)]
pub struct BubblewrapExecutor;
#[async_trait]
impl Executor for BubblewrapExecutor {
    fn name(&self) -> &str {
        "bubblewrap"
    }
    async fn execute(&self, r: &ExecutionRequest) -> Result<ExecutionResult> {
        if !executable_exists("bwrap") {
            return Err(Error::Unavailable("bwrap".into()));
        }
        if r.command.is_empty() {
            return Err(Error::Protocol("empty command".into()));
        }
        let mut cmd = vec![
            "bwrap".into(),
            "--die-with-parent".into(),
            "--unshare-pid".into(),
        ];
        if matches!(r.network, NetworkPolicy::Deny | NetworkPolicy::SourceOnly) {
            cmd.push("--unshare-net".into());
        }
        cmd.extend([
            "--ro-bind".into(),
            "/".into(),
            "/".into(),
            "--bind".into(),
            r.cwd.display().to_string(),
            r.cwd.display().to_string(),
            "--chdir".into(),
            r.cwd.display().to_string(),
            "--".into(),
        ]);
        cmd.extend(r.command.clone());
        run_process(&cmd, &r.cwd, &r.env, &r.resources, r.cancel_file.as_deref()).await
    }
}

#[derive(Clone, Debug)]
pub struct ContainerExecutor {
    pub runtime: String,
    pub image: String,
}
#[async_trait]
impl Executor for ContainerExecutor {
    fn name(&self) -> &str {
        "container"
    }
    async fn execute(&self, r: &ExecutionRequest) -> Result<ExecutionResult> {
        if !executable_exists(&self.runtime) {
            return Err(Error::Unavailable(self.runtime.clone()));
        }
        let cwd = std::fs::canonicalize(&r.cwd)?;
        let mut cmd = vec![
            self.runtime.clone(),
            "run".into(),
            "--rm".into(),
            "-v".into(),
            format!("{}:/work", cwd.display()),
            "-w".into(),
            "/work".into(),
        ];
        if matches!(r.network, NetworkPolicy::Deny | NetworkPolicy::SourceOnly) {
            cmd.push("--network".into());
            cmd.push("none".into());
        }
        for (k, v) in &r.env {
            cmd.push("-e".into());
            cmd.push(format!("{k}={}", translate_path(v, &cwd, "/work")));
        }
        cmd.push(r.container.clone().unwrap_or_else(|| self.image.clone()));
        cmd.extend(r.command.iter().map(|v| translate_path(v, &cwd, "/work")));
        run_process(
            &cmd,
            &r.cwd,
            &BTreeMap::new(),
            &r.resources,
            r.cancel_file.as_deref(),
        )
        .await
    }
}

#[derive(Clone, Debug)]
pub struct SshExecutor {
    pub host: String,
    pub remote_dir: String,
}
#[async_trait]
impl Executor for SshExecutor {
    fn name(&self) -> &str {
        "ssh"
    }
    async fn execute(&self, r: &ExecutionRequest) -> Result<ExecutionResult> {
        if !executable_exists("ssh") {
            return Err(Error::Unavailable("ssh".into()));
        }
        let remote_root = format!(
            "{}/{}",
            self.remote_dir.trim_end_matches('/'),
            &r.action.0.value[..16]
        );
        let prep = vec![
            "ssh".into(),
            self.host.clone(),
            format!(
                "rm -rf {} && mkdir -p {}",
                shell_quote(&remote_root),
                shell_quote(&remote_root)
            ),
        ];
        let pr = run_process(
            &prep,
            &r.cwd,
            &BTreeMap::new(),
            &ResourceSpec::default(),
            None,
        )
        .await?;
        if pr.exit_code != 0 {
            return Ok(pr);
        }
        let source = format!("{}/.", r.cwd.display());
        let target = format!("{}:{}/", self.host, remote_root);
        let copy = if executable_exists("rsync") {
            vec![
                "rsync".into(),
                "-a".into(),
                "--delete".into(),
                source.clone(),
                target.clone(),
            ]
        } else if executable_exists("scp") {
            vec!["scp".into(), "-r".into(), source.clone(), target.clone()]
        } else {
            return Err(Error::Unavailable("rsync or scp".into()));
        };
        let cr = run_process(
            &copy,
            &r.cwd,
            &BTreeMap::new(),
            &ResourceSpec::default(),
            None,
        )
        .await?;
        if cr.exit_code != 0 {
            return Ok(cr);
        }
        let local = std::fs::canonicalize(&r.cwd)?;
        let translated = r
            .command
            .iter()
            .map(|v| translate_path(v, &local, &remote_root))
            .collect::<Vec<_>>();
        let env = r
            .env
            .iter()
            .map(|(k, v)| {
                format!(
                    "{}={}",
                    shell_quote(k),
                    shell_quote(&translate_path(v, &local, &remote_root))
                )
            })
            .collect::<Vec<_>>()
            .join(" ");
        let remote = format!(
            "cd {} && env {} {}",
            shell_quote(&remote_root),
            env,
            shell_join(&translated)
        );
        let cmd = vec!["ssh".into(), self.host.clone(), remote];
        let result = run_process(
            &cmd,
            &r.cwd,
            &BTreeMap::new(),
            &r.resources,
            r.cancel_file.as_deref(),
        )
        .await?;
        if result.exit_code == 0 {
            let back = if executable_exists("rsync") {
                vec![
                    "rsync".into(),
                    "-a".into(),
                    format!("{}:{}/", self.host, remote_root),
                    format!("{}/", r.cwd.display()),
                ]
            } else {
                vec![
                    "scp".into(),
                    "-r".into(),
                    format!("{}:{}/.", self.host, remote_root),
                    r.cwd.display().to_string(),
                ]
            };
            let br = run_process(
                &back,
                &r.cwd,
                &BTreeMap::new(),
                &ResourceSpec::default(),
                None,
            )
            .await?;
            if br.exit_code != 0 {
                return Ok(br);
            }
        }
        let _ = run_process(
            &[
                "ssh".into(),
                self.host.clone(),
                format!("rm -rf {}", shell_quote(&remote_root)),
            ],
            &r.cwd,
            &BTreeMap::new(),
            &ResourceSpec::default(),
            None,
        )
        .await;
        Ok(result)
    }
}

#[derive(Clone, Debug, Default)]
pub struct SlurmExecutor {
    pub partition: Option<String>,
}
#[async_trait]
impl Executor for SlurmExecutor {
    fn name(&self) -> &str {
        "slurm"
    }
    async fn execute(&self, r: &ExecutionRequest) -> Result<ExecutionResult> {
        if !executable_exists("srun") {
            return Err(Error::Unavailable("srun".into()));
        }
        let mut cmd = vec!["srun".into(), "--wait=60".into()];
        if let Some(p) = &self.partition {
            cmd.push("--partition".into());
            cmd.push(p.clone());
        }
        if let Some(c) = r.resources.cpus {
            cmd.push("--cpus-per-task".into());
            cmd.push(c.ceil().to_string());
        }
        if let Some(g) = r.resources.gpus {
            cmd.push("--gpus".into());
            cmd.push(g.to_string());
        }
        cmd.extend(r.command.clone());
        run_process(&cmd, &r.cwd, &r.env, &r.resources, r.cancel_file.as_deref()).await
    }
}

#[derive(Clone, Debug)]
pub struct KubernetesExecutor {
    pub image: String,
    pub namespace: Option<String>,
}
#[async_trait]
impl Executor for KubernetesExecutor {
    fn name(&self) -> &str {
        "kubernetes"
    }
    async fn execute(&self, r: &ExecutionRequest) -> Result<ExecutionResult> {
        if !executable_exists("kubectl") {
            return Err(Error::Unavailable("kubectl".into()));
        }
        let name = format!("artifactum-{}", &r.action.0.value[..12]);
        let ns = namespace_args(self.namespace.as_deref());
        let image = r.container.clone().unwrap_or_else(|| self.image.clone());
        let mut create = vec![
            "kubectl".into(),
            "run".into(),
            name.clone(),
            "--restart=Never".into(),
            "--image".into(),
            image,
            "--command".into(),
        ];
        create.extend(ns.clone());
        create.extend([
            "--".into(),
            "sh".into(),
            "-c".into(),
            "mkdir -p /work && sleep 86400".into(),
        ]);
        let made = run_process(
            &create,
            &r.cwd,
            &BTreeMap::new(),
            &ResourceSpec::default(),
            None,
        )
        .await?;
        if made.exit_code != 0 {
            return Ok(made);
        }
        let mut wait = vec![
            "kubectl".into(),
            "wait".into(),
            "--for=condition=Ready".into(),
            "--timeout=120s".into(),
            format!("pod/{name}"),
        ];
        wait.extend(ns.clone());
        let wr = run_process(
            &wait,
            &r.cwd,
            &BTreeMap::new(),
            &ResourceSpec::default(),
            None,
        )
        .await?;
        if wr.exit_code != 0 {
            let _ = delete_pod(&name, &ns, &r.cwd).await;
            return Ok(wr);
        }
        let mut cp = vec![
            "kubectl".into(),
            "cp".into(),
            format!("{}/.", r.cwd.display()),
            format!("{name}:/work"),
        ];
        cp.extend(ns.clone());
        let cpr = run_process(
            &cp,
            &r.cwd,
            &BTreeMap::new(),
            &ResourceSpec::default(),
            None,
        )
        .await?;
        if cpr.exit_code != 0 {
            let _ = delete_pod(&name, &ns, &r.cwd).await;
            return Ok(cpr);
        }
        let local = std::fs::canonicalize(&r.cwd)?;
        let command = r
            .command
            .iter()
            .map(|v| translate_path(v, &local, "/work"))
            .collect::<Vec<_>>();
        let mut exec = vec!["kubectl".into(), "exec".into(), name.clone()];
        exec.extend(ns.clone());
        exec.push("--".into());
        exec.push("env".into());
        for (k, v) in &r.env {
            exec.push(format!("{k}={}", translate_path(v, &local, "/work")));
        }
        exec.extend(command);
        let result = run_process(
            &exec,
            &r.cwd,
            &BTreeMap::new(),
            &r.resources,
            r.cancel_file.as_deref(),
        )
        .await?;
        if result.exit_code == 0 {
            let mut back = vec![
                "kubectl".into(),
                "cp".into(),
                format!("{name}:/work/."),
                r.cwd.display().to_string(),
            ];
            back.extend(ns.clone());
            let br = run_process(
                &back,
                &r.cwd,
                &BTreeMap::new(),
                &ResourceSpec::default(),
                None,
            )
            .await?;
            if br.exit_code != 0 {
                let _ = delete_pod(&name, &ns, &r.cwd).await;
                return Ok(br);
            }
        }
        let _ = delete_pod(&name, &ns, &r.cwd).await;
        Ok(result)
    }
}

/// Generic JSON-lines executor plugin. Executables can be distributed as
/// `artifactum-executor-<name>` without a Rust ABI.
#[derive(Clone, Debug)]
pub struct PluginExecutor {
    pub executable: PathBuf,
    pub plugin_name: String,
}
#[async_trait]
impl Executor for PluginExecutor {
    fn name(&self) -> &str {
        &self.plugin_name
    }
    async fn execute(&self, r: &ExecutionRequest) -> Result<ExecutionResult> {
        let mut child = Command::new(&self.executable)
            .arg("execute-json")
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn()?;
        let body = serde_json::to_vec(r)?;
        let mut stdin = child
            .stdin
            .take()
            .ok_or_else(|| Error::Protocol("plugin stdin unavailable".into()))?;
        stdin.write_all(&body).await?;
        drop(stdin);
        let out = child.wait_with_output().await?;
        if !out.status.success() {
            return Err(Error::Protocol(
                String::from_utf8_lossy(&out.stderr).into_owned(),
            ));
        }
        Ok(serde_json::from_slice(&out.stdout)?)
    }
}

async fn run_process(
    command: &[String],
    cwd: &Path,
    env: &BTreeMap<String, String>,
    resources: &ResourceSpec,
    cancel_file: Option<&Path>,
) -> Result<ExecutionResult> {
    if command.is_empty() {
        return Err(Error::Protocol("empty command".into()));
    }
    let start = Instant::now();
    let mut cmd = Command::new(&command[0]);
    cmd.args(&command[1..])
        .current_dir(cwd)
        .envs(env)
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .kill_on_drop(true);
    let mut child = cmd.spawn()?;
    let mut stdout = child
        .stdout
        .take()
        .ok_or_else(|| Error::Protocol("stdout unavailable".into()))?;
    let mut stderr = child
        .stderr
        .take()
        .ok_or_else(|| Error::Protocol("stderr unavailable".into()))?;
    let stdout_task = tokio::spawn(async move {
        let mut v = Vec::new();
        stdout.read_to_end(&mut v).await.map(|_| v)
    });
    let stderr_task = tokio::spawn(async move {
        let mut v = Vec::new();
        stderr.read_to_end(&mut v).await.map(|_| v)
    });
    let wait = async {
        loop {
            tokio::select! {status=child.wait()=>return status,_=sleep(Duration::from_millis(150))=>{if let Some(path)=cancel_file{if fs::try_exists(path).await.unwrap_or(false){let _=child.kill().await;return child.wait().await;}}}}
        }
    };
    let status = if let Some(sec) = resources.timeout_seconds {
        match timeout(Duration::from_secs(sec), wait).await {
            Ok(v) => v?,
            Err(_) => {
                let _ = child.kill().await;
                let _ = child.wait().await;
                return Err(Error::Timeout(sec));
            }
        }
    } else {
        wait.await?
    };
    let stdout = stdout_task
        .await
        .map_err(|e| Error::Protocol(e.to_string()))??;
    let mut stderr = stderr_task
        .await
        .map_err(|e| Error::Protocol(e.to_string()))??;
    let cancelled = cancel_file.is_some_and(|p| p.exists());
    if cancelled {
        stderr.extend_from_slice(b"\nartifactum: cancellation requested\n");
    }
    let wall_millis: u64 = start.elapsed().as_millis().try_into().unwrap_or(u64::MAX);
    let estimated_cost_usd_micros = resources.cost_usd_micros_per_hour.map_or(0, |rate| {
        ((u128::from(rate) * u128::from(wall_millis)) / 3_600_000)
            .try_into()
            .unwrap_or(u64::MAX)
    });
    Ok(ExecutionResult {
        exit_code: if cancelled {
            130
        } else {
            status.code().unwrap_or(-1)
        },
        stdout,
        stderr,
        metrics: ExecutionMetrics {
            wall_millis,
            bytes_read: 0,
            bytes_written: 0,
            estimated_cost_usd_micros,
        },
    })
}
fn namespace_args(namespace: Option<&str>) -> Vec<String> {
    namespace
        .map(|n| vec!["--namespace".into(), n.into()])
        .unwrap_or_default()
}
async fn delete_pod(name: &str, ns: &[String], cwd: &Path) -> Result<ExecutionResult> {
    let mut cmd = vec![
        "kubectl".into(),
        "delete".into(),
        "pod".into(),
        name.into(),
        "--ignore-not-found=true".into(),
        "--wait=false".into(),
    ];
    cmd.extend(ns.to_vec());
    run_process(&cmd, cwd, &BTreeMap::new(), &ResourceSpec::default(), None).await
}
fn executable_exists(name: &str) -> bool {
    if name.contains(std::path::MAIN_SEPARATOR) {
        return Path::new(name).exists();
    }
    std::env::var_os("PATH")
        .is_some_and(|paths| std::env::split_paths(&paths).any(|p| p.join(name).is_file()))
}
fn translate_path(v: &str, local: &Path, remote: &str) -> String {
    let prefix = local.display().to_string();
    if v.starts_with(&prefix) {
        format!("{remote}{}", &v[prefix.len()..])
    } else {
        v.to_owned()
    }
}
fn shell_quote(v: &str) -> String {
    format!("'{}'", v.replace('\'', "'\\''"))
}
fn shell_join(v: &[String]) -> String {
    v.iter()
        .map(|x| shell_quote(x))
        .collect::<Vec<_>>()
        .join(" ")
}
