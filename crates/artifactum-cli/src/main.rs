use std::{
    collections::BTreeSet,
    path::{Path, PathBuf},
    sync::Arc,
};

use anyhow::{anyhow, bail};
use artifactum_core::{ArtifactRef, ArtifactRequirement, Digest, DynProvider, SearchRequest};
use artifactum_plugin_host::{DaemonPluginDiscovery, discover_plugins_via_daemon};
use artifactum_provider_http::HttpProvider;
use artifactum_provider_local::LocalProvider;
use artifactum_resolver::{
    ArtifactResolver, Lockfile, LockedArtifact, ProjectArtifact, ProjectManifest, ProjectProvider,
};
use artifactum_store::{ArtifactStore, MaterializationMode};
use clap::{Args, Parser, Subcommand, ValueEnum};
use serde::Serialize;
use sha2::{Digest as _, Sha256};

#[derive(Debug, Parser)]
#[command(name = "artifactum", version, about = "Provider-extensible artifact manager")]
struct Cli {
    #[arg(long, global = true, default_value = "Artifacts.toml")]
    manifest: PathBuf,
    #[arg(long, global = true, default_value = "Artifacts.lock")]
    lockfile: PathBuf,
    #[arg(long, global = true)]
    store: Option<PathBuf>,
    #[arg(long, global = true)]
    offline: bool,
    #[arg(long, global = true, default_value_t = 8)]
    jobs: usize,
    #[arg(long, global = true)]
    json: bool,
    #[command(subcommand)]
    command: Command,
}

#[derive(Debug, Subcommand)]
enum Command {
    Add(AddArgs),
    Remove { name: String },
    Resolve { target: String },
    Fetch(FetchArgs),
    Materialize(MaterializeArgs),
    Inspect { name: String },
    Path { name: String, file: Option<String> },
    Files { name: String },
    Verify { name: Option<String> },
    Gc { #[arg(long)] dry_run: bool },
    Cache,
    Provider { #[command(subcommand)] command: ProviderCommand },
    Plugin { #[command(subcommand)] command: PluginCommand },
    Search(SearchArgs),
    Catalog { #[command(subcommand)] command: CatalogCommand },
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
    /// Fetch only files matching these globs. Repeatable.
    #[arg(long = "file")]
    files: Vec<String>,
    #[arg(long)]
    locked: bool,
    #[arg(long)]
    frozen: bool,
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
enum MaterializeModeArg { Auto, Copy, Hardlink }
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
    /// Show available provider implementations and configured provider profiles.
    List,
    /// Add or replace a named provider profile in Artifacts.toml.
    Add {
        name: String,
        #[arg(long)]
        kind: String,
        /// Provider setting in KEY=VALUE form. Repeatable; ${ENV} is resolved by providers.
        #[arg(long = "set")]
        settings: Vec<String>,
    },
    /// Remove a named provider profile from Artifacts.toml.
    Remove { name: String },
}

#[derive(Debug, Subcommand)]
enum PluginCommand { List, Inspect { name: String } }

#[derive(Debug, Args)]
struct SearchArgs {
    scheme: String,
    query: String,
    #[arg(long, default_value_t = 20)]
    limit: usize,
    #[arg(long)]
    cursor: Option<String>,
    #[arg(long)]
    repo_type: Option<String>,
}

#[derive(Debug, Subcommand)]
enum CatalogCommand {
    Inspect { reference: String },
    Versions { reference: String, #[arg(long)] cursor: Option<String> },
    Files {
        reference: String,
        #[arg(long)] revision: Option<String>,
        #[arg(long)] cursor: Option<String>,
    },
}

struct Runtime { resolver: ArtifactResolver, plugins: DaemonPluginDiscovery }

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    // daemonkit validates its private bootstrap channel, so this is safe before
    // normal argument parsing and does not expose a user-controlled daemon mode.
    if artifactum_plugin_host::maybe_run_daemon().await? { return Ok(()); }

    let cli = Cli::parse();
    match &cli.command {
        Command::Add(args) => return add(&cli.manifest, args).await,
        Command::Remove { name } => return remove(&cli.manifest, &cli.lockfile, cli.store.as_deref(), name).await,
        Command::Provider { command: ProviderCommand::Add { name, kind, settings } } => {
            return provider_add(&cli.manifest, name, kind, settings).await;
        }
        Command::Provider { command: ProviderCommand::Remove { name } } => {
            return provider_remove(&cli.manifest, name).await;
        }
        _ => {}
    }

    let project = ProjectManifest::load(&cli.manifest).await?;
    let frozen = matches!(&cli.command, Command::Fetch(args) if args.frozen);
    let runtime = build_runtime(
        cli.store.as_deref(),
        cli.offline || frozen,
        cli.jobs,
        &project,
    ).await?;

    match cli.command {
        Command::Add(_) | Command::Remove { .. } => unreachable!(),
        Command::Resolve { target } => {
            let requirement = target_requirement(&project, &target)?;
            output(&runtime.resolver.resolve(&requirement).await?, cli.json)?;
        }
        Command::Fetch(args) => fetch(&runtime.resolver, &cli.manifest, &cli.lockfile, args, cli.json).await?,
        Command::Materialize(args) => materialize(&runtime.resolver, &cli.manifest, &cli.lockfile, args).await?,
        Command::Inspect { name } => inspect(&runtime.resolver, &cli.lockfile, &name, cli.json).await?,
        Command::Path { name, file } => print_path(&runtime.resolver, &cli.manifest, &cli.lockfile, &name, file.as_deref()).await?,
        Command::Files { name } => files(&cli.lockfile, &name, cli.json).await?,
        Command::Verify { name } => verify(&runtime.resolver, &cli.lockfile, name.as_deref(), cli.json).await?,
        Command::Gc { dry_run } => output(&runtime.resolver.store().gc(dry_run).await?, cli.json)?,
        Command::Cache => println!("{}", runtime.resolver.store().root().display()),
        Command::Provider { command: ProviderCommand::List } => {
            output(&serde_json::json!({"implementations": runtime.resolver.providers(), "profiles": runtime.resolver.profiles()}), cli.json)?;
        }
        Command::Provider { .. } => unreachable!(),
        Command::Plugin { command } => plugin_command(&runtime.plugins, command, cli.json)?,
        Command::Search(args) => {
            let mut request = SearchRequest { query: args.query, limit: Some(args.limit), cursor: args.cursor, metadata: Default::default() };
            if let Some(repo_type) = args.repo_type {
                request.metadata.insert("repo_type".into(), serde_json::Value::String(repo_type));
            }
            output(&runtime.resolver.search(&args.scheme, &request).await?, cli.json)?;
        }
        Command::Catalog { command } => catalog(&runtime.resolver, command, cli.json).await?,
    }
    Ok(())
}

async fn build_runtime(store_path: Option<&Path>, offline: bool, jobs: usize, project: &ProjectManifest) -> anyhow::Result<Runtime> {
    let store = match store_path { Some(path) => ArtifactStore::open(path).await?, None => ArtifactStore::xdg().await? };
    let mut builder = ArtifactResolver::builder()
        .store(store)
        .offline(offline)
        .max_concurrent_files(jobs)
        .profiles(project.profiles());
    builder = builder.provider(LocalProvider::new())?;
    builder = builder.provider(HttpProvider::new())?;

    let plugins = discover_plugins_via_daemon().await;
    let mut occupied = BTreeSet::from(["local".to_owned(), "file".to_owned(), "http".to_owned(), "https".to_owned()]);
    for plugin in &plugins.providers {
        let descriptor = artifactum_core::ArtifactProvider::descriptor(plugin);
        let schemes = descriptor.schemes.iter().map(|scheme| scheme.to_ascii_lowercase()).collect::<Vec<_>>();
        if schemes.iter().any(|scheme| occupied.contains(scheme)) { continue; }
        occupied.extend(schemes);
        let provider: DynProvider = Arc::new(plugin.clone());
        builder = builder.provider_dyn(provider)?;
    }
    Ok(Runtime { resolver: builder.build().await?, plugins })
}

async fn add(path: &Path, args: &AddArgs) -> anyhow::Result<()> {
    let mut project = ProjectManifest::load(path).await?;
    project.artifacts.insert(args.name.clone(), ProjectArtifact {
        source: args.source.clone(), revision: args.revision.clone(), include: args.include.clone(),
        exclude: args.exclude.clone(), materialize: args.materialize.clone(),
    });
    project.save(path).await?;
    println!("added {} -> {}", args.name, args.source);
    Ok(())
}

async fn remove(manifest_path: &Path, lockfile_path: &Path, store_path: Option<&Path>, name: &str) -> anyhow::Result<()> {
    let mut project = ProjectManifest::load(manifest_path).await?;
    if project.artifacts.remove(name).is_none() { bail!("artifact `{name}` is not present in {}", manifest_path.display()); }
    project.save(manifest_path).await?;
    let mut lockfile = Lockfile::load(lockfile_path).await?;
    lockfile.remove(name);
    lockfile.save(lockfile_path).await?;
    let store = match store_path { Some(path) => ArtifactStore::open(path).await?, None => ArtifactStore::xdg().await? };
    store.unpin(&project_pin_name(manifest_path, name)).await?;
    println!("removed {name}");
    Ok(())
}

async fn provider_add(path: &Path, name: &str, kind: &str, settings: &[String]) -> anyhow::Result<()> {
    let mut project = ProjectManifest::load(path).await?;
    let mut config = std::collections::BTreeMap::new();
    for setting in settings {
        let (key, value) = setting.split_once('=').ok_or_else(|| anyhow!("provider setting `{setting}` must be KEY=VALUE"))?;
        if key.is_empty() { bail!("provider setting key cannot be empty"); }
        config.insert(key.to_owned(), value.to_owned());
    }
    project.providers.insert(name.to_owned(), ProjectProvider { kind: kind.to_owned(), config });
    project.save(path).await?;
    println!("configured provider profile {name} ({kind})");
    Ok(())
}

async fn provider_remove(path: &Path, name: &str) -> anyhow::Result<()> {
    let mut project = ProjectManifest::load(path).await?;
    if project.providers.remove(name).is_none() { bail!("provider profile `{name}` is not configured"); }
    project.save(path).await?;
    println!("removed provider profile {name}");
    Ok(())
}

fn target_requirement(project: &ProjectManifest, target: &str) -> anyhow::Result<ArtifactRequirement> {
    if project.artifacts.contains_key(target) { Ok(project.requirement(target)?) } else { Ok(ArtifactRequirement::new(target.parse()?)) }
}

async fn fetch(resolver: &ArtifactResolver, manifest_path: &Path, lockfile_path: &Path, args: FetchArgs, json: bool) -> anyhow::Result<()> {
    let project = ProjectManifest::load(manifest_path).await?;
    let mut lockfile = Lockfile::load(lockfile_path).await?;
    let locked_mode = args.locked || args.frozen;
    let names = if args.names.is_empty() { project.artifacts.keys().cloned().collect::<Vec<_>>() } else { args.names };
    if names.is_empty() { bail!("no artifacts are defined in {}", manifest_path.display()); }

    let mut results = Vec::new();
    for name in names {
        let project_artifact = project.artifacts.get(&name).ok_or_else(|| anyhow!("artifact `{name}` is not defined"))?;
        let requirement = project_artifact.requirement()?;
        let previous = lockfile.get(&name).cloned();
        let resolution = if locked_mode {
            let locked = previous.as_ref().ok_or_else(|| anyhow!("--locked requires `{name}` in {}", lockfile_path.display()))?;
            if !locked.matches_requirement(&requirement)? {
                bail!("--locked refused stale resolution for `{name}` because {} changed", manifest_path.display());
            }
            locked.to_resolution()?
        } else {
            resolver.resolve(&requirement).await?
        };

        let partial = resolver.fetch_selected(resolution, &args.files).await?;
        let mut merged = LockedArtifact::from_partial(&name, &requirement, &partial, previous.as_ref())?;
        if merged.manifest.is_none() {
            if let Some(manifest) = resolver.finalize_cached(&merged.to_resolution()?).await? {
                merged.manifest = Some(manifest.digest.to_string());
            }
        }

        if !locked_mode { lockfile.upsert(merged.clone()); }
        pin_locked(resolver.store(), manifest_path, &merged).await?;

        if !args.no_materialize && !partial.files.is_empty() {
            let destination = materialization_path(manifest_path, &name, project_artifact);
            resolver.materialize_partial(&partial, &destination, MaterializationMode::Auto).await?;
        }
        results.push(merged);
    }
    if !locked_mode { lockfile.save(lockfile_path).await?; }
    output(&results, json)?;
    Ok(())
}

async fn pin_locked(store: &ArtifactStore, manifest_path: &Path, artifact: &LockedArtifact) -> anyhow::Result<()> {
    let pin = project_pin_name(manifest_path, &artifact.name);
    if let Some(manifest) = &artifact.manifest {
        let digest: Digest = manifest.parse()?;
        store.pin(&pin, &digest).await?;
    } else {
        let blobs = artifact.files.iter().filter_map(|file| file.digest.as_ref()).map(|digest| digest.parse()).collect::<Result<Vec<Digest>, _>>()?;
        store.pin_blobs(&pin, &blobs).await?;
    }
    Ok(())
}

async fn materialize(resolver: &ArtifactResolver, manifest_path: &Path, lockfile_path: &Path, args: MaterializeArgs) -> anyhow::Result<()> {
    let project = ProjectManifest::load(manifest_path).await?;
    let lockfile = Lockfile::load(lockfile_path).await?;
    let locked = lockfile.get(&args.name).ok_or_else(|| anyhow!("artifact `{}` is not locked", args.name))?;
    let manifest = locked.manifest.as_ref().ok_or_else(|| anyhow!("artifact `{}` is only partially fetched; fetch remaining files first", args.name))?;
    let digest: Digest = manifest.parse()?;
    let artifact = resolver.store().load_manifest(&digest).await?;
    let destination = args.to.unwrap_or_else(|| project.artifacts.get(&args.name).map_or_else(
        || project_base(manifest_path).join(".artifactum").join(&args.name),
        |artifact| materialization_path(manifest_path, &args.name, artifact),
    ));
    resolver.store().materialize(&artifact, &destination, args.mode.into()).await?;
    println!("{}", destination.display());
    Ok(())
}

async fn inspect(resolver: &ArtifactResolver, lockfile_path: &Path, name: &str, json: bool) -> anyhow::Result<()> {
    let lockfile = Lockfile::load(lockfile_path).await?;
    let locked = lockfile.get(name).ok_or_else(|| anyhow!("artifact `{name}` is not locked"))?;
    let stored = match &locked.manifest {
        Some(manifest) => Some(resolver.store().load_manifest(&manifest.parse()?).await?),
        None => None,
    };
    output(&serde_json::json!({ "lock": locked, "stored": stored }), json)?;
    Ok(())
}

async fn print_path(resolver: &ArtifactResolver, manifest_path: &Path, lockfile_path: &Path, name: &str, file: Option<&str>) -> anyhow::Result<()> {
    let project = ProjectManifest::load(manifest_path).await?;
    let project_artifact = project.artifacts.get(name).ok_or_else(|| anyhow!("artifact `{name}` is not defined"))?;
    let materialized = materialization_path(manifest_path, name, project_artifact);
    let Some(file) = file else { println!("{}", materialized.display()); return Ok(()); };
    let candidate = materialized.join(file);
    if tokio::fs::try_exists(&candidate).await? { println!("{}", candidate.display()); return Ok(()); }
    let lockfile = Lockfile::load(lockfile_path).await?;
    let locked = lockfile.get(name).ok_or_else(|| anyhow!("artifact `{name}` is not locked"))?;
    let locked_file = locked.files.iter().find(|locked_file| locked_file.path == file).ok_or_else(|| anyhow!("artifact `{name}` contains no file `{file}`"))?;
    let digest = locked_file.digest.as_ref().ok_or_else(|| anyhow!("file `{file}` is resolved but has not been fetched"))?;
    println!("{}", resolver.store().blob_path(&digest.parse()?)?.display());
    Ok(())
}

async fn files(lockfile_path: &Path, name: &str, json: bool) -> anyhow::Result<()> {
    let lockfile = Lockfile::load(lockfile_path).await?;
    let locked = lockfile.get(name).ok_or_else(|| anyhow!("artifact `{name}` is not locked"))?;
    if json { output(&locked.files, true)?; } else {
        for file in &locked.files {
            println!("{}\t{}\t{}", file.size.map_or_else(|| "-".into(), |v| v.to_string()), file.digest.as_deref().unwrap_or("unfetched"), file.path);
        }
    }
    Ok(())
}

#[derive(Serialize)]
struct VerifyResult { artifact: String, path: String, digest: Option<String>, status: String }

async fn verify(resolver: &ArtifactResolver, lockfile_path: &Path, name: Option<&str>, json: bool) -> anyhow::Result<()> {
    let lockfile = Lockfile::load(lockfile_path).await?;
    if let Some(name) = name { if lockfile.get(name).is_none() { bail!("artifact `{name}` is not locked"); } }
    let mut results = Vec::new();
    let mut invalid = false;
    for artifact in &lockfile.artifacts {
        if name.is_some_and(|name| name != artifact.name) { continue; }
        for file in &artifact.files {
            let status = if let Some(value) = &file.digest {
                let digest: Digest = value.parse()?;
                if resolver.store().verify_blob(&digest).await? { "ok" } else { invalid = true; "missing_or_corrupt" }
            } else { "unfetched" };
            results.push(VerifyResult { artifact: artifact.name.clone(), path: file.path.clone(), digest: file.digest.clone(), status: status.into() });
        }
    }
    if json { output(&results, true)?; } else { for result in &results { println!("{}\t{}\t{}", result.status, result.artifact, result.path); } }
    if invalid { bail!("one or more cached blobs failed verification"); }
    Ok(())
}

fn plugin_command(discovery: &DaemonPluginDiscovery, command: PluginCommand, json: bool) -> anyhow::Result<()> {
    match command {
        PluginCommand::List => {
            let providers = discovery.providers.iter().map(|provider| serde_json::json!({
                "path": provider.executable(), "provider": artifactum_core::ArtifactProvider::descriptor(provider),
            })).collect::<Vec<_>>();
            output(&serde_json::json!({
                "providers": providers,
                "errors": discovery.errors.iter().map(|error| serde_json::json!({"path": error.path, "error": error.error})).collect::<Vec<_>>(),
            }), json)?;
        }
        PluginCommand::Inspect { name } => {
            let provider = discovery.providers.iter().find(|provider| artifactum_core::ArtifactProvider::descriptor(*provider).name == name)
                .ok_or_else(|| anyhow!("plugin provider `{name}` was not discovered"))?;
            output(&serde_json::json!({"path": provider.executable(), "provider": artifactum_core::ArtifactProvider::descriptor(provider)}), json)?;
        }
    }
    Ok(())
}

async fn catalog(resolver: &ArtifactResolver, command: CatalogCommand, json: bool) -> anyhow::Result<()> {
    match command {
        CatalogCommand::Inspect { reference } => {
            let reference: ArtifactRef = reference.parse()?;
            output(&resolver.inspect(&reference).await?, json)?;
        }
        CatalogCommand::Versions { reference, cursor } => {
            let reference: ArtifactRef = reference.parse()?;
            output(&resolver.list_versions(&reference, cursor.as_deref()).await?, json)?;
        }
        CatalogCommand::Files { reference, revision, cursor } => {
            let mut requirement = ArtifactRequirement::new(reference.parse()?);
            requirement.revision = revision;
            output(&resolver.list_files(&requirement, cursor.as_deref()).await?, json)?;
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
fn project_base(manifest_path: &Path) -> PathBuf { manifest_path.parent().filter(|p| !p.as_os_str().is_empty()).unwrap_or_else(|| Path::new(".")).to_path_buf() }
fn project_pin_name(manifest_path: &Path, artifact: &str) -> String {
    let identity = std::fs::canonicalize(manifest_path).unwrap_or_else(|_| manifest_path.to_path_buf()).display().to_string();
    let mut hasher = Sha256::new(); hasher.update(identity.as_bytes()); let hash = hex::encode(hasher.finalize());
    format!("project-{}-{artifact}", &hash[..16])
}
fn output<T: Serialize>(value: &T, _json: bool) -> anyhow::Result<()> { println!("{}", serde_json::to_string_pretty(value)?); Ok(()) }
