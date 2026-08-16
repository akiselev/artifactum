use std::{
    collections::BTreeMap,
    path::{Path, PathBuf},
    process::Stdio,
};

use artifactum_core::{
    external_tool_required, provider_error, AcquireContext, Acquisition, AcquisitionPlan,
    ArtifactPath, ArtifactProvider, ArtifactRequirement, Digest, ProviderCapabilities,
    ProviderDescriptor, ProviderProfile, ResolveContext, ResolvedFile, ResolvedRevision, Resolution,
};
use artifactum_provider_command::{require_tool, string_field};
use async_trait::async_trait;
use sha2::{Digest as _, Sha256};
use tokio::{fs, process::Command};

#[derive(Clone, Debug, Default)]
pub struct GitProvider;

fn split(locator: &str) -> artifactum_core::Result<(&str, &str)> {
    locator
        .split_once('#')
        .ok_or_else(|| provider_error("git", "expected git:<repository-url>#<path>"))
}

fn cache_root(profile: Option<&ProviderProfile>) -> artifactum_core::Result<PathBuf> {
    if let Some(value) = profile.and_then(|p| p.config.get("cache_dir")) {
        return Ok(PathBuf::from(value));
    }
    let base = directories::BaseDirs::new()
        .ok_or_else(|| provider_error("git", "cannot determine user cache directory"))?;
    Ok(base.cache_dir().join("artifactum/provider-git"))
}

fn repo_dir(repo: &str, profile: Option<&ProviderProfile>) -> artifactum_core::Result<PathBuf> {
    let key = hex::encode(Sha256::digest(repo.as_bytes()));
    Ok(cache_root(profile)?.join(key))
}

async fn git_output(args: &[String]) -> artifactum_core::Result<std::process::Output> {
    require_tool("git", "git")?;
    let output = Command::new("git")
        .env("GIT_LFS_SKIP_SMUDGE", "1")
        .args(args)
        .kill_on_drop(true)
        .output()
        .await
        .map_err(|error| provider_error("git", error))?;
    if !output.status.success() {
        return Err(provider_error(
            "git",
            String::from_utf8_lossy(&output.stderr).trim(),
        ));
    }
    Ok(output)
}

async fn ensure_repo(
    repo: &str,
    profile: Option<&ProviderProfile>,
    allow_network: bool,
) -> artifactum_core::Result<PathBuf> {
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
                "git",
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

async fn rev_parse(dir: &Path, requested: &str) -> artifactum_core::Result<String> {
    let candidates = if requested == "HEAD" {
        vec![
            "refs/remotes/origin/HEAD^{commit}".to_owned(),
            "HEAD^{commit}".to_owned(),
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
    Err(provider_error(
        "git",
        format!("cannot resolve revision `{requested}`"),
    ))
}

fn parse_pointer(bytes: &[u8]) -> Option<(String, u64)> {
    if bytes.len() > 1024 {
        return None;
    }
    let text = std::str::from_utf8(bytes).ok()?;
    if !text.starts_with("version https://git-lfs.github.com/spec/") {
        return None;
    }
    let oid = text.lines().find_map(|line| line.strip_prefix("oid sha256:"))?.trim();
    let size = text
        .lines()
        .find_map(|line| line.strip_prefix("size "))?
        .trim()
        .parse()
        .ok()?;
    if oid.len() != 64 || !oid.bytes().all(|byte| byte.is_ascii_hexdigit()) {
        return None;
    }
    Some((oid.to_ascii_lowercase(), size))
}

async fn blob_bytes(dir: &Path, commit: &str, path: &str) -> artifactum_core::Result<Vec<u8>> {
    let spec = format!("{commit}:{path}");
    Ok(git_output(&[
        "-C".into(),
        dir.display().to_string(),
        "show".into(),
        spec,
    ])
    .await?
    .stdout)
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
            metadata: Default::default(),
        }
    }

    async fn resolve(
        &self,
        requirement: &ArtifactRequirement,
        context: &ResolveContext,
    ) -> artifactum_core::Result<Resolution> {
        if context.offline {
            return Err(provider_error(
                "git",
                "cannot resolve a Git remote while offline; use a lockfile",
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
        let selection = requirement.selection.compile()?;
        let mut files = Vec::new();

        for line in String::from_utf8_lossy(&output.stdout).lines() {
            let Some((meta, path)) = line.split_once('\t') else { continue };
            if !selection.matches(path) {
                continue;
            }
            let fields = meta.split_whitespace().collect::<Vec<_>>();
            if fields.len() < 4 || fields[1] != "blob" {
                continue;
            }
            let object = fields[2];
            let pointer = if fields[3].parse::<u64>().ok().is_some_and(|size| size <= 1024) {
                blob_bytes(&dir, &commit, path)
                    .await
                    .ok()
                    .and_then(|bytes| parse_pointer(&bytes))
            } else {
                None
            };
            let mut size = fields[3].parse::<u64>().ok();
            let mut digests = artifactum_core::DigestSet::default();
            let mut lfs_oid = None;
            if let Some((oid, lfs_size)) = pointer {
                size = Some(lfs_size);
                if let Ok(digest) = Digest::sha256(oid.clone()) {
                    digests.insert(digest);
                }
                lfs_oid = Some(oid);
            }
            files.push(ResolvedFile {
                path: ArtifactPath::new(path)?,
                size,
                digests,
                media_type: None,
                source: serde_json::json!({
                    "repo": repo,
                    "commit": commit,
                    "path": path,
                    "git_object": object,
                    "lfs_oid": lfs_oid,
                }),
            });
        }
        if files.is_empty() {
            return Err(provider_error(
                "git",
                format!("no files matched `{prefix}` at {commit}"),
            ));
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
        _context: &AcquireContext,
    ) -> artifactum_core::Result<AcquisitionPlan> {
        Ok(AcquisitionPlan::ProviderManaged {
            state: file.source.clone(),
        })
    }

    async fn acquire_managed(
        &self,
        file: &ResolvedFile,
        _plan: &AcquisitionPlan,
        destination: &Path,
        context: &AcquireContext,
    ) -> artifactum_core::Result<Acquisition> {
        let repo = string_field(&file.source, "repo", "git")?;
        let commit = string_field(&file.source, "commit", "git")?;
        let path = string_field(&file.source, "path", "git")?;
        let dir = ensure_repo(repo, context.profile.as_ref(), !context.offline).await?;

        if let Some(oid) = file.source.get("lfs_oid").and_then(|value| value.as_str()) {
            let object = dir
                .join(".git/lfs/objects")
                .join(&oid[0..2])
                .join(&oid[2..4])
                .join(oid);
            if !object.is_file() {
                if context.offline {
                    return Err(provider_error(
                        "git",
                        format!("Git LFS object {oid} is not cached and resolver is offline"),
                    ));
                }
                require_tool("git", "git")?;
                let check = Command::new("git")
                    .args(["lfs", "version"])
                    .output()
                    .await
                    .map_err(|error| provider_error("git", error))?;
                if !check.status.success() {
                    return Err(external_tool_required(
                        "git",
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
                return Err(provider_error(
                    "git",
                    format!("Git LFS object {oid} is absent from the local cache"),
                ));
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
                .map_err(|error| provider_error("git", error))?;
            if !status.success() {
                return Err(provider_error("git", format!("git show failed with {status}")));
            }
        }
        let size = fs::metadata(destination).await?.len();
        Ok(Acquisition {
            bytes_written: Some(size),
            metadata: Default::default(),
        })
    }
}

pub fn provider() -> GitProvider { GitProvider }
