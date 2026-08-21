use std::{
    collections::BTreeMap,
    path::{Path, PathBuf},
    process::Stdio,
};

use artifactum_core::{ArtifactPath, Digest};
use artifactum_resolver::{
    AccessChallenge, AccessRequirement, AcquireContext, AcquisitionPlan, ArtifactProvider,
    ArtifactRequirement, DigestSet, Error, ProviderCapabilities, ProviderDescriptor,
    ProviderProfile, Resolution, ResolveContext, ResolvedFile, ResolvedRevision, Result,
};
use async_trait::async_trait;
use sha2::{Digest as _, Sha256};
use tokio::{fs, process::Command};

#[derive(Clone, Debug, Default)]
pub struct GitProvider;
fn provider_error(message: impl std::fmt::Display) -> Error {
    Error::Provider {
        provider: "git".into(),
        message: message.to_string(),
    }
}
fn external_tool(tool: &str, message: impl Into<String>) -> Error {
    Error::AccessRequired(AccessChallenge {
        provider: "git".into(),
        requirement: AccessRequirement::ExternalTool,
        message: message.into(),
        action_url: None,
        tool: Some(tool.into()),
    })
}
fn split(locator: &str) -> Result<(&str, &str)> {
    locator
        .split_once('#')
        .ok_or_else(|| provider_error("expected git:<repository-url>#<path>"))
}
fn cache_root(profile: Option<&ProviderProfile>) -> Result<PathBuf> {
    if let Some(value) = profile.and_then(|p| p.config.get("cache_dir")) {
        return Ok(PathBuf::from(value));
    }
    let base = directories::BaseDirs::new()
        .ok_or_else(|| provider_error("cannot determine user cache directory"))?;
    Ok(base.cache_dir().join("artifactum/provider-git"))
}
fn repo_dir(repo: &str, profile: Option<&ProviderProfile>) -> Result<PathBuf> {
    Ok(cache_root(profile)?.join(hex::encode(Sha256::digest(repo.as_bytes()))))
}
fn tool_exists(name: &str) -> bool {
    std::env::var_os("PATH")
        .is_some_and(|path| std::env::split_paths(&path).any(|dir| dir.join(name).is_file()))
}
async fn git_output(args: &[String]) -> Result<std::process::Output> {
    if !tool_exists("git") {
        return Err(external_tool("git", "Git provider requires `git`"));
    }
    let output = Command::new("git")
        .env("GIT_LFS_SKIP_SMUDGE", "1")
        .args(args)
        .kill_on_drop(true)
        .output()
        .await
        .map_err(provider_error)?;
    if !output.status.success() {
        return Err(provider_error(
            String::from_utf8_lossy(&output.stderr).trim(),
        ));
    }
    Ok(output)
}
async fn ensure_repo(
    repo: &str,
    profile: Option<&ProviderProfile>,
    allow_network: bool,
) -> Result<PathBuf> {
    let dir = repo_dir(repo, profile)?;
    if dir.join(".git").is_dir() {
        if allow_network {
            git_output(&[
                "-C".into(),
                dir.display().to_string(),
                "fetch".into(),
                "--prune".into(),
                "--tags".into(),
                "origin".into(),
            ])
            .await?;
        }
    } else {
        if !allow_network {
            return Err(provider_error(
                "repository is not cached and resolver is offline",
            ));
        }
        if let Some(parent) = dir.parent() {
            fs::create_dir_all(parent).await?;
        }
        git_output(&[
            "clone".into(),
            "--filter=blob:none".into(),
            "--no-checkout".into(),
            repo.into(),
            dir.display().to_string(),
        ])
        .await?;
    }
    Ok(dir)
}
async fn rev_parse(dir: &Path, requested: &str) -> Result<String> {
    let candidates = if requested == "HEAD" {
        vec![
            "refs/remotes/origin/HEAD^{commit}".into(),
            "HEAD^{commit}".into(),
        ]
    } else {
        vec![
            format!("origin/{requested}^{{commit}}"),
            format!("{requested}^{{commit}}"),
        ]
    };
    for candidate in candidates {
        if let Ok(output) = git_output(&[
            "-C".into(),
            dir.display().to_string(),
            "rev-parse".into(),
            "--verify".into(),
            candidate,
        ])
        .await
        {
            return Ok(String::from_utf8_lossy(&output.stdout).trim().into());
        }
    }
    Err(provider_error(format!(
        "cannot resolve revision `{requested}`"
    )))
}
fn parse_pointer(bytes: &[u8]) -> Option<(String, u64)> {
    if bytes.len() > 1024 {
        return None;
    }
    let text = std::str::from_utf8(bytes).ok()?;
    if !text.starts_with("version https://git-lfs.github.com/spec/") {
        return None;
    }
    let oid = text
        .lines()
        .find_map(|line| line.strip_prefix("oid sha256:"))?
        .trim();
    let size = text
        .lines()
        .find_map(|line| line.strip_prefix("size "))?
        .trim()
        .parse()
        .ok()?;
    if oid.len() != 64 || !oid.bytes().all(|b| b.is_ascii_hexdigit()) {
        return None;
    }
    Some((oid.to_ascii_lowercase(), size))
}
async fn blob_bytes(dir: &Path, commit: &str, path: &str) -> Result<Vec<u8>> {
    Ok(git_output(&[
        "-C".into(),
        dir.display().to_string(),
        "show".into(),
        format!("{commit}:{path}"),
    ])
    .await?
    .stdout)
}
fn source_field<'a>(value: &'a serde_json::Value, key: &str) -> Result<&'a str> {
    value
        .get(key)
        .and_then(serde_json::Value::as_str)
        .ok_or_else(|| provider_error(format!("resolved file missing `{key}`")))
}

#[async_trait]
impl ArtifactProvider for GitProvider {
    fn descriptor(&self) -> ProviderDescriptor {
        ProviderDescriptor {
            name: "git".into(),
            version: env!("CARGO_PKG_VERSION").into(),
            schemes: vec!["git".into()],
            capabilities: ProviderCapabilities {
                resolve: true,
                acquire: true,
                list: true,
                auth: true,
                ..Default::default()
            },
        }
    }
    async fn resolve(
        &self,
        requirement: &ArtifactRequirement,
        context: &ResolveContext,
    ) -> Result<Resolution> {
        if context.offline {
            return Err(provider_error(
                "cannot resolve a Git remote while offline; use a frozen lockfile",
            ));
        }
        let (repo, prefix) = split(requirement.reference.locator())?;
        let dir = ensure_repo(repo, context.profile.as_ref(), true).await?;
        let requested = requirement.revision.as_deref().unwrap_or("HEAD");
        let commit = rev_parse(&dir, requested).await?;
        let output = git_output(&[
            "-C".into(),
            dir.display().to_string(),
            "ls-tree".into(),
            "-r".into(),
            "-l".into(),
            commit.clone(),
            "--".into(),
            prefix.into(),
        ])
        .await?;
        let mut files = Vec::new();
        for line in String::from_utf8_lossy(&output.stdout).lines() {
            let Some((meta, path)) = line.split_once('\t') else {
                continue;
            };
            if !requirement.selection.matches(path)? {
                continue;
            }
            let fields = meta.split_whitespace().collect::<Vec<_>>();
            if fields.len() < 4 || fields[1] != "blob" {
                continue;
            }
            let object = fields[2];
            let pointer = if fields[3]
                .parse::<u64>()
                .ok()
                .is_some_and(|size| size <= 1024)
            {
                blob_bytes(&dir, &commit, path)
                    .await
                    .ok()
                    .and_then(|bytes| parse_pointer(&bytes))
            } else {
                None
            };
            let mut size = fields[3].parse::<u64>().ok();
            let mut digests = DigestSet(BTreeMap::new());
            let mut lfs_oid = None;
            if let Some((oid, lfs_size)) = pointer {
                size = Some(lfs_size);
                if let Ok(digest) = Digest::sha256(oid.clone()) {
                    digests.0.insert(digest.algorithm, digest.value);
                }
                lfs_oid = Some(oid);
            }
            files.push(ResolvedFile{path:ArtifactPath::new(path)?,size,digests,media_type:None,source:serde_json::json!({"repo":repo,"commit":commit,"path":path,"git_object":object,"lfs_oid":lfs_oid})});
        }
        if files.is_empty() {
            return Err(provider_error(format!(
                "no files matched `{prefix}` at {commit}"
            )));
        }
        Ok(Resolution {
            provider: "git".into(),
            canonical_ref: format!("git:{repo}#{prefix}"),
            revision: Some(ResolvedRevision {
                id: commit,
                requested: requirement.revision.clone(),
            }),
            files,
            provider_state: serde_json::Value::Null,
            metadata: BTreeMap::new(),
        })
    }
    async fn prepare_acquisition(
        &self,
        file: &ResolvedFile,
        _: &AcquireContext,
    ) -> Result<AcquisitionPlan> {
        Ok(AcquisitionPlan::ProviderManaged {
            state: file.source.clone(),
        })
    }
    async fn acquire_managed(
        &self,
        file: &ResolvedFile,
        _: &AcquisitionPlan,
        destination: &Path,
        context: &AcquireContext,
    ) -> Result<u64> {
        let repo = source_field(&file.source, "repo")?;
        let commit = source_field(&file.source, "commit")?;
        let path = source_field(&file.source, "path")?;
        let dir = ensure_repo(repo, context.profile.as_ref(), !context.offline).await?;
        if let Some(oid) = file
            .source
            .get("lfs_oid")
            .and_then(serde_json::Value::as_str)
        {
            let object = dir
                .join(".git/lfs/objects")
                .join(&oid[0..2])
                .join(&oid[2..4])
                .join(oid);
            if !object.is_file() {
                if context.offline {
                    return Err(provider_error(format!(
                        "Git LFS object {oid} is not cached and resolver is offline"
                    )));
                }
                let check = Command::new("git")
                    .args(["lfs", "version"])
                    .output()
                    .await
                    .map_err(provider_error)?;
                if !check.status.success() {
                    return Err(external_tool(
                        "git-lfs",
                        "this file is a Git LFS pointer and requires git-lfs",
                    ));
                }
                git_output(&[
                    "-C".into(),
                    dir.display().to_string(),
                    "lfs".into(),
                    "fetch".into(),
                    format!("--include={path}"),
                    "--exclude=".into(),
                    "origin".into(),
                    commit.into(),
                ])
                .await?;
            }
            if !object.is_file() {
                return Err(provider_error(format!(
                    "Git LFS object {oid} is absent from the local cache"
                )));
            }
            fs::copy(object, destination).await?;
        } else {
            let output = fs::File::create(destination).await?;
            let std_file = output.into_std().await;
            let mut command = Command::new("git");
            command.env("GIT_LFS_SKIP_SMUDGE", "1");
            if context.offline {
                command.env("GIT_NO_LAZY_FETCH", "1");
            }
            let status = command
                .args([
                    "-C",
                    dir.to_str().unwrap_or("."),
                    "show",
                    &format!("{commit}:{path}"),
                ])
                .stdout(Stdio::from(std_file))
                .stderr(Stdio::piped())
                .kill_on_drop(true)
                .status()
                .await
                .map_err(provider_error)?;
            if !status.success() {
                return Err(provider_error(format!("git show failed with {status}")));
            }
        }
        Ok(fs::metadata(destination).await?.len())
    }
}
#[must_use]
pub fn provider() -> GitProvider {
    GitProvider
}
