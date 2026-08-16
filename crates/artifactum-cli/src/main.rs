use std::{
    collections::BTreeSet,
    path::{Path, PathBuf},
    sync::Arc,
};

use anyhow::{anyhow, bail};
use artifactum_core::{ArtifactProvider, ArtifactRequirement, Digest, DynProvider, SearchRequest};
use artifactum_plugin_protocol::{PluginDiscovery, discover_plugins};
use artifactum_provider_http::HttpProvider;
use artifactum_provider_local::LocalProvider;
use artifactum_resolver::{
    ArtifactResolver, Lockfile, LockedArtifact, ProjectArtifact, ProjectManifest,
};
use artifactum_store::{ArtifactStore, MaterializationMode};
use clap::{Args, Parser, Subcommand, ValueEnum};
use serde::Serialize;
use sha2::{Digest as _, Sha256};

#[derive(Debug, Parser)]
#[command(name = "artifactum", version, about = "Provider-extensible artifact manager")]
struct Cli {
    /// Path to Artifacts.toml.
    #[arg(long, global = true, default_value = "Artifacts.toml")]
    manifest: PathBuf,

    /// Path to Artifacts.lock.
    #[arg(long, global = true, default_value = "Artifacts.lock")]
    lockfile: PathBuf,

    /// Override the Artifactum content-addressed store root.
    #[arg(long, global = true)]
    store: Option<PathBuf>,

    /// Forbid provider network acquisition.
    #[arg(long, global = true)]
    offline: bool,

    /// Emit machine-readable JSON where supported.
    #[arg(long, global = true)]
    json: bool,

    #[command(subcommand)]
    command: Command,
}

#[derive(Debug, Subcommand)]
enum Command {
    /// Add or replace a project artifact requirement.
    Add(AddArgs),
    /// Remove a project artifact requirement and lock entry.
    Remove { name: String },
    /// Resolve a project artifact or direct reference without downloading it.
    Resolve { target: String },
    /// Resolve/fetch artifacts into the shared CAS and materialize them.
    Fetch(FetchArgs),
    /// Materialize a locked artifact from the CAS.
    Materialize(MaterializeArgs),
    /// Inspect a locked artifact and its stored manifest.
    Inspect { name: String },
    /// Print a materialized path, or the backing blob path for a file.
    Path { name: String, file: Option<String> },
    /// List files in a locked artifact.
    Files { name: String },
    /// Verify cached blobs referenced by the lockfile.
    Verify { name: Option<String> },
    /// Garbage-collect unpinned CAS blobs.
    Gc {
        #[arg(long)]
        dry_run: bool,
    },
    /// Print the content-addressed store root.
    Cache,
    /// Inspect registered providers.
    Provider {
        #[command(subcommand)]
        command: ProviderCommand,
    },
    /// Inspect external provider plugins discovered on PATH.
    Plugin {
        #[command(subcommand)]
        command: PluginCommand,
    },
    /// Search a provider that implements the optional search capability.
    Search(SearchArgs),
}

#[derive(Debug, Args)]
struct AddArgs {
    name: String,
    source: String,
    #[arg(long)]
    revision: Option<String>,
    #[arg(long)]
    include: Vec<String>,
    #[arg(long)]
    exclude: Vec<String>,
    #[arg(long)]
    materialize: Option<PathBuf>,
}

#[derive(Debug, Args)]
struct FetchArgs {
    /// Artifact names. Defaults to every artifact in Artifacts.toml.
    names: Vec<String>,
    /// Do not change Artifacts.lock; use its existing resolutions.
    #[arg(long)]
    locked: bool,
    /// Equivalent to --locked plus --offline.
    #[arg(long)]
    frozen: bool,
    /// Populate the CAS but do not materialize project trees.
    #[arg(long)]
    no_materialize: bool,
}

#[derive(Debug, Args)]
struct MaterializeArgs {
    name: String,
    #[arg(long)]
    to: Option<PathBuf>,
    #[arg(long, value_enum, default_value = "auto")]
    mode: MaterializeModeArg,
}

#[derive(Clone, Copy, Debug, ValueEnum)]
enum MaterializeModeArg {
    Auto,
    Copy,
    Hardlink,
}

impl From<MaterializeModeArg> for MaterializationMode {
    fn from(value: MaterializeModeArg) -> Self {
        match value {
            MaterializeModeArg::Auto => Self::Auto,
            MaterializeModeArg::Copy => Self::Copy,
            MaterializeModeArg::Hardlink => Self::Hardlink,
        }
    }
}

#[derive(Debug, Subcommand)]
enum ProviderCommand {
    List,
}

#[derive(Debug, Subcommand)]
enum PluginCommand {
    List,
    Inspect { name: String },
}

#[derive(Debug, Args)]
struct SearchArgs {
    scheme: String,
    query: String,
    #[arg(long, default_value_t = 20)]
    limit: usize,
    /// Provider-specific repository kind, e.g. model/dataset/space for Hugging Face.
    #[arg(long)]
    repo_type: Option<String>,
}

struct Runtime {
    resolver: ArtifactResolver,
    plugins: PluginDiscovery,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let cli = Cli::parse();

    match &cli.command {
        Command::Add(args) => return add(&cli.manifest, args).await,
        Command::Remove { name } => {
            return remove(&cli.manifest, &cli.lockfile, cli.store.as_deref(), name).await;
        }
        _ => {}
    }

    let frozen = matches!(&cli.command, Command::Fetch(args) if args.frozen);
    let runtime = build_runtime(cli.store.as_deref(), cli.offline || frozen).await?;

    match cli.command {
        Command::Add(_) | Command::Remove { .. } => unreachable!(),
        Command::Resolve { target } => {
            let project = ProjectManifest::load(&cli.manifest).await?;
            let requirement = target_requirement(&project, &target)?;
            let resolution = runtime.resolver.resolve(&requirement).await?;
            output(&resolution, cli.json)?;
        }
        Command::Fetch(args) => {
            fetch(&runtime.resolver, &cli.manifest, &cli.lockfile, args, cli.json).await?;
        }
        Command::Materialize(args) => {
            materialize(&runtime.resolver, &cli.manifest, &cli.lockfile, args).await?;
        }
        Command::Inspect { name } => {
            inspect(&runtime.resolver, &cli.lockfile, &name, cli.json).await?;
        }
        Command::Path { name, file } => {
            print_path(&runtime.resolver, &cli.manifest, &cli.lockfile, &name, file.as_deref()).await?;
        }
        Command::Files { name } => {
            files(&cli.lockfile, &name, cli.json).await?;
        }
        Command::Verify { name } => {
            verify(&runtime.resolver, &cli.lockfile, name.as_deref(), cli.json).await?;
        }
        Command::Gc { dry_run } => {
            let report = runtime.resolver.store().gc(dry_run).await?;
            output(&report, cli.json)?;
        }
        Command::Cache => println!("{}", runtime.resolver.store().root().display()),
        Command::Provider { command: ProviderCommand::List } => {
            output(&runtime.resolver.providers(), cli.json)?;
        }
        Command::Plugin { command } => plugin_command(&runtime.plugins, command, cli.json)?,
        Command::Search(args) => {
            let mut request = SearchRequest {
                query: args.query,
                limit: Some(args.limit),
                metadata: Default::default(),
            };
            if let Some(repo_type) = args.repo_type {
                request
                    .metadata
                    .insert("repo_type".into(), serde_json::Value::String(repo_type));
            }
            let results = runtime.resolver.search(&args.scheme, &request).await?;
            output(&results, cli.json)?;
        }
    }

    Ok(())
}

async fn build_runtime(store_path: Option<&Path>, offline: bool) -> anyhow::Result<Runtime> {
    let store = match store_path {
        Some(path) => ArtifactStore::open(path).await?,
        None => ArtifactStore::xdg().await?,
    };
    let mut builder = ArtifactResolver::builder().store(store).offline(offline);
    builder = builder.provider(LocalProvider::new())?;
    builder = builder.provider(HttpProvider::new())?;

    let plugins = discover_plugins().await;
    let mut occupied = BTreeSet::from([
        "local".to_owned(),
        "file".to_owned(),
        "http".to_owned(),
        "https".to_owned(),
    ]);
    for plugin in &plugins.providers {
        let descriptor = plugin.descriptor();
        let schemes = descriptor
            .schemes
            .iter()
            .map(|scheme| scheme.to_ascii_lowercase())
            .collect::<Vec<_>>();
        if schemes.iter().any(|scheme| occupied.contains(scheme)) {
            continue;
        }
        occupied.extend(schemes);
        let provider: DynProvider = Arc::new(plugin.clone());
        builder = builder.provider_dyn(provider)?;
    }

    Ok(Runtime {
        resolver: builder.build().await?,
        plugins,
    })
}

async fn add(path: &Path, args: &AddArgs) -> anyhow::Result<()> {
    let mut project = ProjectManifest::load(path).await?;
    project.artifacts.insert(
        args.name.clone(),
        ProjectArtifact {
            source: args.source.clone(),
            revision: args.revision.clone(),
            include: args.include.clone(),
            exclude: args.exclude.clone(),
            materialize: args.materialize.clone(),
        },
    );
    project.save(path).await?;
    println!("added {} -> {}", args.name, args.source);
    Ok(())
}

async fn remove(
    manifest_path: &Path,
    lockfile_path: &Path,
    store_path: Option<&Path>,
    name: &str,
) -> anyhow::Result<()> {
    let mut project = ProjectManifest::load(manifest_path).await?;
    if project.artifacts.remove(name).is_none() {
        bail!("artifact `{name}` is not present in {}", manifest_path.display());
    }
    project.save(manifest_path).await?;

    let mut lockfile = Lockfile::load(lockfile_path).await?;
    lockfile.artifacts.retain(|artifact| artifact.name != name);
    lockfile.save(lockfile_path).await?;

    let store = match store_path {
        Some(path) => ArtifactStore::open(path).await?,
        None => ArtifactStore::xdg().await?,
    };
    store.unpin(&project_pin_name(manifest_path, name)).await?;
    println!("removed {name}");
    Ok(())
}

fn target_requirement(project: &ProjectManifest, target: &str) -> anyhow::Result<ArtifactRequirement> {
    if project.artifacts.contains_key(target) {
        Ok(project.requirement(target)?)
    } else {
        Ok(ArtifactRequirement::new(target.parse()?))
    }
}

async fn fetch(
    resolver: &ArtifactResolver,
    manifest_path: &Path,
    lockfile_path: &Path,
    args: FetchArgs,
    json: bool,
) -> anyhow::Result<()> {
    let project = ProjectManifest::load(manifest_path).await?;
    let mut lockfile = Lockfile::load(lockfile_path).await?;
    let locked = args.locked || args.frozen;
    let names = if args.names.is_empty() {
        project.artifacts.keys().cloned().collect::<Vec<_>>()
    } else {
        args.names
    };
    if names.is_empty() {
        bail!("no artifacts are defined in {}", manifest_path.display());
    }

    let mut results = Vec::new();
    for name in names {
        let project_artifact = project
            .artifacts
            .get(&name)
            .ok_or_else(|| anyhow!("artifact `{name}` is not defined"))?;
        let requirement = project_artifact.requirement()?;
        let fetched = if locked {
            let locked_artifact = lockfile
                .get(&name)
                .ok_or_else(|| anyhow!("--locked requires `{name}` in {}", lockfile_path.display()))?;
            if !locked_artifact.matches_requirement(&requirement)? {
                bail!(
                    "--locked refused to use stale resolution for `{name}`; {} has changed",
                    manifest_path.display()
                );
            }
            resolver.fetch_resolution(locked_artifact.to_resolution()?).await?
        } else {
            resolver.fetch(&requirement).await?
        };

        let locked_artifact = LockedArtifact::from_fetched(&name, &requirement, &fetched)?;
        if !locked {
            lockfile.upsert(locked_artifact.clone());
        }

        let pin = project_pin_name(manifest_path, &name);
        resolver
            .store()
            .pin(&pin, &fetched.manifest.digest)
            .await?;

        if !args.no_materialize {
            let destination = materialization_path(manifest_path, &name, project_artifact);
            resolver
                .materialize(&fetched, &destination, MaterializationMode::Auto)
                .await?;
        }
        results.push(locked_artifact);
    }

    if !locked {
        lockfile.save(lockfile_path).await?;
    }
    output(&results, json)?;
    Ok(())
}

async fn materialize(
    resolver: &ArtifactResolver,
    manifest_path: &Path,
    lockfile_path: &Path,
    args: MaterializeArgs,
) -> anyhow::Result<()> {
    let project = ProjectManifest::load(manifest_path).await?;
    let lockfile = Lockfile::load(lockfile_path).await?;
    let locked = lockfile
        .get(&args.name)
        .ok_or_else(|| anyhow!("artifact `{}` is not locked", args.name))?;
    let digest: Digest = locked.manifest.parse()?;
    let artifact = resolver.store().load_manifest(&digest).await?;
    let destination = args.to.unwrap_or_else(|| {
        project
            .artifacts
            .get(&args.name)
            .map_or_else(
                || project_base(manifest_path).join(".artifactum").join(&args.name),
                |artifact| materialization_path(manifest_path, &args.name, artifact),
            )
    });
    resolver
        .store()
        .materialize(&artifact, &destination, args.mode.into())
        .await?;
    println!("{}", destination.display());
    Ok(())
}

async fn inspect(
    resolver: &ArtifactResolver,
    lockfile_path: &Path,
    name: &str,
    json: bool,
) -> anyhow::Result<()> {
    let lockfile = Lockfile::load(lockfile_path).await?;
    let locked = lockfile
        .get(name)
        .ok_or_else(|| anyhow!("artifact `{name}` is not locked"))?;
    let digest: Digest = locked.manifest.parse()?;
    let stored = resolver.store().load_manifest(&digest).await?;
    let value = serde_json::json!({ "lock": locked, "stored": stored });
    output(&value, json)?;
    Ok(())
}

async fn print_path(
    resolver: &ArtifactResolver,
    manifest_path: &Path,
    lockfile_path: &Path,
    name: &str,
    file: Option<&str>,
) -> anyhow::Result<()> {
    let project = ProjectManifest::load(manifest_path).await?;
    let project_artifact = project
        .artifacts
        .get(name)
        .ok_or_else(|| anyhow!("artifact `{name}` is not defined"))?;
    let materialized = materialization_path(manifest_path, name, project_artifact);
    let Some(file) = file else {
        println!("{}", materialized.display());
        return Ok(());
    };
    let candidate = materialized.join(file);
    if tokio::fs::try_exists(&candidate).await? {
        println!("{}", candidate.display());
        return Ok(());
    }

    let lockfile = Lockfile::load(lockfile_path).await?;
    let locked = lockfile
        .get(name)
        .ok_or_else(|| anyhow!("artifact `{name}` is not locked"))?;
    let locked_file = locked
        .files
        .iter()
        .find(|locked_file| locked_file.path == file)
        .ok_or_else(|| anyhow!("artifact `{name}` contains no file `{file}`"))?;
    let digest: Digest = locked_file.digest.parse()?;
    println!("{}", resolver.store().blob_path(&digest)?.display());
    Ok(())
}

async fn files(lockfile_path: &Path, name: &str, json: bool) -> anyhow::Result<()> {
    let lockfile = Lockfile::load(lockfile_path).await?;
    let locked = lockfile
        .get(name)
        .ok_or_else(|| anyhow!("artifact `{name}` is not locked"))?;
    if json {
        output(&locked.files, true)?;
    } else {
        for file in &locked.files {
            println!("{}\t{}\t{}", file.size, file.digest, file.path);
        }
    }
    Ok(())
}

#[derive(Serialize)]
struct VerifyResult {
    artifact: String,
    path: String,
    digest: String,
    valid: bool,
}

async fn verify(
    resolver: &ArtifactResolver,
    lockfile_path: &Path,
    name: Option<&str>,
    json: bool,
) -> anyhow::Result<()> {
    let lockfile = Lockfile::load(lockfile_path).await?;
    if let Some(name) = name {
        if lockfile.get(name).is_none() {
            bail!("artifact `{name}` is not locked");
        }
    }
    let mut results = Vec::new();
    for artifact in &lockfile.artifacts {
        if name.is_some_and(|name| name != artifact.name) {
            continue;
        }
        for file in &artifact.files {
            let digest: Digest = file.digest.parse()?;
            results.push(VerifyResult {
                artifact: artifact.name.clone(),
                path: file.path.clone(),
                digest: file.digest.clone(),
                valid: resolver.store().verify_blob(&digest).await?,
            });
        }
    }
    if json {
        output(&results, true)?;
    } else {
        for result in &results {
            println!(
                "{}\t{}\t{}",
                if result.valid { "ok" } else { "MISSING/CORRUPT" },
                result.artifact,
                result.path
            );
        }
    }
    if results.iter().any(|result| !result.valid) {
        bail!("one or more locked blobs failed verification");
    }
    Ok(())
}

fn plugin_command(discovery: &PluginDiscovery, command: PluginCommand, json: bool) -> anyhow::Result<()> {
    match command {
        PluginCommand::List => {
            let providers = discovery
                .providers
                .iter()
                .map(|provider| {
                    serde_json::json!({
                        "path": provider.executable(),
                        "provider": provider.descriptor(),
                    })
                })
                .collect::<Vec<_>>();
            let value = serde_json::json!({
                "providers": providers,
                "errors": discovery.errors.iter().map(|error| serde_json::json!({
                    "path": error.path,
                    "error": error.error,
                })).collect::<Vec<_>>(),
            });
            output(&value, json)?;
        }
        PluginCommand::Inspect { name } => {
            let provider = discovery
                .providers
                .iter()
                .find(|provider| provider.descriptor().name == name)
                .ok_or_else(|| anyhow!("plugin provider `{name}` was not discovered"))?;
            let value = serde_json::json!({
                "path": provider.executable(),
                "provider": provider.descriptor(),
            });
            output(&value, json)?;
        }
    }
    Ok(())
}

fn materialization_path(manifest_path: &Path, name: &str, artifact: &ProjectArtifact) -> PathBuf {
    match artifact.materialize.as_ref() {
        Some(path) if path.is_absolute() => path.clone(),
        Some(path) => project_base(manifest_path).join(path),
        None => project_base(manifest_path).join(".artifactum").join(name),
    }
}

fn project_base(manifest_path: &Path) -> PathBuf {
    manifest_path
        .parent()
        .filter(|parent| !parent.as_os_str().is_empty())
        .unwrap_or_else(|| Path::new("."))
        .to_path_buf()
}

fn project_pin_name(manifest_path: &Path, artifact: &str) -> String {
    let identity = std::fs::canonicalize(manifest_path)
        .unwrap_or_else(|_| manifest_path.to_path_buf())
        .display()
        .to_string();
    let mut hasher = Sha256::new();
    hasher.update(identity.as_bytes());
    let hash = hex::encode(hasher.finalize());
    let prefix = &hash[..16];
    format!("project-{prefix}-{artifact}")
}

fn output<T: Serialize>(value: &T, json: bool) -> anyhow::Result<()> {
    if json {
        println!("{}", serde_json::to_string_pretty(value)?);
    } else {
        // Human mode intentionally remains stable enough to read, not parse.
        println!("{}", serde_json::to_string_pretty(value)?);
    }
    Ok(())
}
