use anyhow::{Context, Result, anyhow};
use artifactum_action::{ActionBuilder, diff};
use artifactum_core::{ActionKey, ArtifactId, CachePolicy, ContentKind, OutputSpec};
use artifactum_engine::Engine;
use artifactum_executor::{
    BubblewrapExecutor, ContainerExecutor, KubernetesExecutor, PluginExecutor, SlurmExecutor,
    SshExecutor,
};
use artifactum_metadata::MetadataStore;
use artifactum_pipeline::{
    PipelineRunner, ProjectArtifact, ProjectManifest, ProjectProvider, RemoteSpec,
};
use artifactum_plugin_host::discover;
use artifactum_provenance::{TrustPolicy, evaluate_policy, export_oci, publish_oci_with_oras};
use artifactum_provider_sdk::PluginProvider;
use artifactum_remote::{FileRemote, HttpRemote, Mirror, RemoteCache};
use artifactum_resolver::{ArtifactRequirement, ArtifactResolverBuilder, Selection};
use artifactum_store::{ArtifactStore, ContentStore, MaterializationMode};
use clap::{Args, Parser, Subcommand};
use std::{
    collections::BTreeMap,
    path::{Path, PathBuf},
    str::FromStr,
    sync::Arc,
};
use tokio::fs;

#[derive(Parser)]
#[command(
    name = "artifactum",
    version,
    about = "Content-addressed artifact resolution, transformation, provenance and distribution"
)]
struct Cli {
    #[arg(long, default_value = "Artifactum.toml")]
    project: PathBuf,
    #[arg(long)]
    store: Option<PathBuf>,
    #[arg(long)]
    metadata: Option<PathBuf>,
    #[arg(long, default_value_t = 8)]
    jobs: usize,
    #[arg(long)]
    offline: bool,
    #[arg(long)]
    json: bool,
    #[command(subcommand)]
    command: Command,
}
#[derive(Subcommand)]
enum Command {
    Init {
        #[arg(long)]
        name: Option<String>,
    },
    Add {
        name: String,
        source: String,
        #[arg(long)]
        revision: Option<String>,
        #[arg(long = "include")]
        include: Vec<String>,
        #[arg(long = "exclude")]
        exclude: Vec<String>,
    },
    Resolve {
        name: String,
    },
    Fetch {
        name: String,
        #[arg(long)]
        frozen: bool,
    },
    Provider {
        #[command(subcommand)]
        command: ProviderCmd,
    },
    Plan {
        targets: Vec<String>,
    },
    Run {
        targets: Vec<String>,
        #[arg(long)]
        frozen: bool,
    },
    Exec(ExecArgs),
    Status,
    Artifact {
        #[command(subcommand)]
        command: ArtifactCmd,
    },
    Runs {
        #[command(subcommand)]
        command: RunsCmd,
    },
    Lineage {
        artifact: String,
    },
    Why {
        action_or_name: String,
    },
    Audit {
        #[command(subcommand)]
        command: AuditCmd,
    },
    Ref {
        #[command(subcommand)]
        command: RefCmd,
    },
    Checkpoint {
        #[command(subcommand)]
        command: CheckpointCmd,
    },
    Attest {
        #[command(subcommand)]
        command: AttestCmd,
    },
    Verify {
        artifact: String,
        #[arg(long)]
        policy: Option<PathBuf>,
    },
    Promote {
        artifact: String,
        name: String,
        #[arg(long)]
        policy: Option<PathBuf>,
        #[arg(long)]
        mutable: bool,
    },
    Store {
        #[command(subcommand)]
        command: StoreCmd,
    },
    Remote {
        #[command(subcommand)]
        command: RemoteCmd,
    },
    Export {
        #[command(subcommand)]
        command: ExportCmd,
    },
    Publish {
        #[command(subcommand)]
        command: PublishCmd,
    },
    MigrateLegacy {
        path: PathBuf,
    },
}
#[derive(Subcommand)]
enum ProviderCmd {
    List,
    Plugins,
    Profiles,
    Add {
        name: String,
        kind: String,
        #[arg(long = "set")]
        set: Vec<String>,
    },
    Remove {
        name: String,
    },
    Search {
        scheme: String,
        query: String,
        #[arg(long, default_value_t = 20)]
        limit: usize,
    },
    Inspect {
        reference: String,
    },
    Versions {
        reference: String,
    },
    Files {
        reference: String,
    },
}
#[derive(Args)]
struct ExecArgs {
    #[arg(long = "input")]
    inputs: Vec<String>,
    #[arg(long = "code")]
    code: Vec<String>,
    #[arg(long = "output")]
    outputs: Vec<String>,
    #[arg(long, default_value = "reproducible")]
    cache: String,
    #[arg(long, default_value = "local")]
    executor: String,
    #[arg(last = true, required = true)]
    command: Vec<String>,
}
#[derive(Subcommand)]
enum ArtifactCmd {
    Import {
        path: PathBuf,
        #[arg(long)]
        chunked: bool,
        #[arg(long)]
        media_type: Option<String>,
        #[arg(long)]
        set_ref: Option<String>,
    },
    Inspect {
        artifact: String,
    },
    Cat {
        artifact: String,
    },
    Ls {
        artifact: String,
    },
    Materialize {
        artifact: String,
        to: PathBuf,
        #[arg(long, default_value = "auto")]
        mode: String,
    },
    Diff {
        left: String,
        right: String,
    },
}
#[derive(Subcommand)]
enum RunsCmd {
    List {
        #[arg(long, default_value_t = 20)]
        limit: usize,
    },
    Show {
        id: String,
    },
    Logs {
        id: String,
        #[arg(long)]
        stderr: bool,
    },
    Retry {
        id: String,
    },
    Cancel {
        id: String,
    },
}
#[derive(Subcommand)]
enum AuditCmd {
    Determinism {
        action: String,
        #[arg(long, default_value_t = 2)]
        runs: usize,
        #[arg(long, default_value = "local")]
        executor: String,
    },
}
#[derive(Subcommand)]
enum RefCmd {
    List,
    Set {
        name: String,
        artifact: String,
        #[arg(long)]
        immutable: bool,
    },
    Delete {
        name: String,
    },
}
#[derive(Subcommand)]
enum CheckpointCmd {
    Put {
        action: String,
        name: String,
        artifact: String,
    },
    Get {
        action: String,
        name: String,
    },
}
#[derive(Subcommand)]
enum AttestCmd {
    Add {
        artifact: String,
        predicate_type: String,
        statement: PathBuf,
        #[arg(long)]
        issuer: Option<String>,
    },
    List {
        artifact: String,
    },
}
#[derive(Subcommand)]
enum StoreCmd {
    Stats,
    Verify {
        artifact: String,
    },
    Gc {
        #[arg(long)]
        dry_run: bool,
        #[arg(long, default_value_t = 30)]
        retention_days: i64,
    },
}
#[derive(Subcommand)]
enum RemoteCmd {
    Add {
        name: String,
        kind: String,
        location: String,
        #[arg(long)]
        read_only: bool,
        #[arg(long)]
        token_env: Option<String>,
    },
    List,
    Push {
        name: String,
        artifact: String,
    },
    Pull {
        name: String,
        artifact: String,
    },
    Serve {
        path: PathBuf,
        #[arg(long, default_value = "127.0.0.1:8173")]
        bind: String,
        #[arg(long)]
        token_env: Option<String>,
        #[arg(long)]
        read_only: bool,
    },
}
#[derive(Subcommand)]
enum ExportCmd {
    Oci { artifact: String, to: PathBuf },
}
#[derive(Subcommand)]
enum PublishCmd {
    Oci {
        artifact: String,
        reference: String,
        #[arg(long)]
        layout: Option<PathBuf>,
    },
}

#[tokio::main]
async fn main() {
    if let Err(e) = entry().await {
        eprintln!("error: {e:#}");
        std::process::exit(1)
    }
}
async fn entry() -> Result<()> {
    if artifactum_plugin_host::maybe_run_daemon().await? {
        return Ok(());
    }
    run().await
}
async fn run() -> Result<()> {
    let mut cli = Cli::parse();
    if cli.project == PathBuf::from("Artifactum.toml")
        && !fs::try_exists(&cli.project).await?
        && fs::try_exists("Artifacts.toml").await?
    {
        cli.project = PathBuf::from("Artifacts.toml");
    }
    let store = match &cli.store {
        Some(p) => ArtifactStore::open(p).await?,
        None => ArtifactStore::xdg().await?,
    };
    let metadata = match &cli.metadata {
        Some(p) => MetadataStore::open(p)?,
        None => MetadataStore::xdg()?,
    };
    let engine = build_engine(store.clone(), metadata.clone()).await?;
    match cli.command {
        Command::Init { name } => {
            if fs::try_exists(&cli.project).await? {
                return Err(anyhow!("{} already exists", cli.project.display()));
            }
            let mut p = ProjectManifest::default();
            p.project.name = name.unwrap_or_else(|| {
                std::env::current_dir()
                    .ok()
                    .and_then(|p| p.file_name().map(|n| n.to_string_lossy().into_owned()))
                    .unwrap_or_else(|| "artifactum-project".into())
            });
            p.save(&cli.project).await?;
            println!("initialized {}", cli.project.display());
        }
        Command::Add {
            name,
            source,
            revision,
            include,
            exclude,
        } => {
            let mut p = load_or_default(&cli.project).await?;
            p.artifacts.insert(
                name.clone(),
                ProjectArtifact {
                    source,
                    revision,
                    include,
                    exclude,
                    materialize: None,
                },
            );
            p.save(&cli.project).await?;
            println!("added source {name}");
        }
        Command::Resolve { name } => {
            let p = ProjectManifest::load(&cli.project).await?;
            let a = p
                .artifacts
                .get(&name)
                .ok_or_else(|| anyhow!("unknown source {name}"))?;
            let resolver =
                build_resolver(&p, store.clone(), metadata.clone(), cli.offline, cli.jobs).await?;
            let r = resolver.resolve(&requirement(a)?).await?;
            print_value(cli.json, &r)?;
        }
        Command::Fetch { name, frozen } => {
            let rb = resolver_builder_with_plugins(
                store.clone(),
                metadata.clone(),
                cli.offline,
                cli.jobs,
            )
            .await?;
            let runner = PipelineRunner::from_file(&cli.project, engine.clone(), rb)
                .await?
                .max_parallel(cli.jobs);
            println!("{}", runner.fetch_source(&name, frozen).await?);
        }
        Command::Provider { command } => {
            let mut p = load_or_default(&cli.project).await?;
            match command {
                ProviderCmd::Profiles => {
                    for (name, profile) in &p.providers {
                        println!("{name}\t{}", profile.kind)
                    }
                }
                ProviderCmd::Add { name, kind, set } => {
                    let mut config = BTreeMap::new();
                    for value in set {
                        let (k, v) = split_assignment(&value)?;
                        config.insert(k.to_owned(), v.to_owned());
                    }
                    p.providers
                        .insert(name.clone(), ProjectProvider { kind, config });
                    p.save(&cli.project).await?;
                    println!("configured provider profile {name}");
                }
                ProviderCmd::Remove { name } => {
                    p.providers.remove(&name);
                    p.save(&cli.project).await?;
                    println!("removed provider profile {name}");
                }
                other => {
                    let resolver =
                        build_resolver(&p, store.clone(), metadata.clone(), cli.offline, cli.jobs)
                            .await?;
                    match other {
                        ProviderCmd::List => {
                            for d in resolver.providers() {
                                println!("{}\t{}\t{}", d.name, d.version, d.schemes.join(","))
                            }
                        }
                        ProviderCmd::Plugins => {
                            for path in discover("artifactum-provider-") {
                                println!("{}", path.display())
                            }
                        }
                        ProviderCmd::Search {
                            scheme,
                            query,
                            limit,
                        } => print_value(
                            cli.json,
                            &resolver
                                .search(
                                    &scheme,
                                    &artifactum_resolver::SearchRequest {
                                        query,
                                        limit: Some(limit),
                                        cursor: None,
                                    },
                                )
                                .await?,
                        )?,
                        ProviderCmd::Inspect { reference } => {
                            print_value(cli.json, &resolver.inspect(&reference.parse()?).await?)?
                        }
                        ProviderCmd::Versions { reference } => print_value(
                            cli.json,
                            &resolver.list_versions(&reference.parse()?, None).await?,
                        )?,
                        ProviderCmd::Files { reference } => print_value(
                            cli.json,
                            &resolver
                                .list_files(&ArtifactRequirement::new(reference.parse()?), None)
                                .await?,
                        )?,
                        ProviderCmd::Profiles
                        | ProviderCmd::Add { .. }
                        | ProviderCmd::Remove { .. } => unreachable!(),
                    }
                }
            }
        }
        Command::Plan { targets } => {
            let p = ProjectManifest::load(&cli.project).await?;
            print_value(cli.json, &artifactum_pipeline::plan(&p, &targets)?)?;
        }
        Command::Run { targets, frozen } => {
            let p = ProjectManifest::load(&cli.project).await?;
            let rb = resolver_builder_with_plugins(
                store.clone(),
                metadata.clone(),
                cli.offline,
                cli.jobs,
            )
            .await?;
            let runner = PipelineRunner::from_file(&cli.project, engine.clone(), rb)
                .await?
                .max_parallel(cli.jobs);
            let run = runner.run(&targets, frozen).await?;
            print_value(cli.json, &run)?;
        }
        Command::Exec(args) => exec_command(args, &engine, &store, cli.json).await?,
        Command::Status => {
            let stats = store.stats().await?;
            let runs = metadata.recent_attempts(5)?;
            print_value(
                cli.json,
                &serde_json::json!({"store":stats,"recent_attempts":runs}),
            )?;
        }
        Command::Artifact { command } => {
            artifact_command(command, &store, &metadata, cli.json).await?
        }
        Command::Runs { command } => {
            runs_command(command, &store, &metadata, &engine, cli.json).await?
        }
        Command::Lineage { artifact } => {
            let id = resolve_artifact(&store, &artifact).await?;
            print_value(cli.json, &engine.lineage(&id)?)?;
        }
        Command::Why { action_or_name } => why_command(&metadata, &action_or_name, cli.json)?,
        Command::Audit { command } => match command {
            AuditCmd::Determinism {
                action,
                runs,
                executor,
            } => {
                let key = ActionKey::from_str(&action)?;
                let spec = metadata
                    .action(&key)?
                    .ok_or_else(|| anyhow!("unknown action {key}"))?;
                let report = engine.audit_determinism(spec, &executor, runs).await?;
                print_value(cli.json, &report)?;
            }
        },
        Command::Ref { command } => match command {
            RefCmd::List => print_value(cli.json, &store.list_refs().await?)?,
            RefCmd::Set {
                name,
                artifact,
                immutable,
            } => {
                let id = resolve_artifact(&store, &artifact).await?;
                store.set_ref(&name, &id, immutable).await?;
            }
            RefCmd::Delete { name } => store.delete_ref(&name).await?,
        },
        Command::Checkpoint { command } => match command {
            CheckpointCmd::Put {
                action,
                name,
                artifact,
            } => {
                let key = ActionKey::from_str(&action)?;
                let id = resolve_artifact(&store, &artifact).await?;
                print_value(cli.json, &engine.checkpoint(&key, name, id)?)?;
            }
            CheckpointCmd::Get { action, name } => {
                let key = ActionKey::from_str(&action)?;
                print_value(cli.json, &engine.latest_checkpoint(&key, &name)?)?;
            }
        },
        Command::Attest { command } => match command {
            AttestCmd::Add {
                artifact,
                predicate_type,
                statement,
                issuer,
            } => {
                let id = resolve_artifact(&store, &artifact).await?;
                let st: serde_json::Value = serde_json::from_slice(&fs::read(statement).await?)?;
                print_value(cli.json, &engine.attest(id, predicate_type, st, issuer)?)?;
            }
            AttestCmd::List { artifact } => {
                let id = resolve_artifact(&store, &artifact).await?;
                print_value(cli.json, &metadata.attestations(&id)?)?;
            }
        },
        Command::Verify { artifact, policy } => {
            let id = resolve_artifact(&store, &artifact).await?;
            let at = metadata.attestations(&id)?;
            let policy = if let Some(p) = policy {
                toml::from_str::<TrustPolicy>(&fs::read_to_string(p).await?)?
            } else {
                TrustPolicy::default()
            };
            evaluate_policy(&policy, &at)?;
            println!(
                "verified {id}: policy satisfied with {} attestations",
                at.len()
            );
        }
        Command::Promote {
            artifact,
            name,
            policy,
            mutable,
        } => {
            let id = resolve_artifact(&store, &artifact).await?;
            let at = metadata.attestations(&id)?;
            let policy = if let Some(p) = policy {
                toml::from_str::<TrustPolicy>(&fs::read_to_string(p).await?)?
            } else {
                TrustPolicy::default()
            };
            evaluate_policy(&policy, &at)?;
            store.set_ref(&name, &id, !mutable).await?;
            println!("promoted {id} -> @{name}");
        }
        Command::Store { command } => match command {
            StoreCmd::Stats => print_value(cli.json, &store.stats().await?)?,
            StoreCmd::Verify { artifact } => {
                let id = resolve_artifact(&store, &artifact).await?;
                verify_graph(&store, &id).await?;
                println!("verified {id}");
            }
            StoreCmd::Gc {
                dry_run,
                retention_days,
            } => {
                let roots = engine.gc_roots(retention_days)?;
                print_value(cli.json, &store.gc(dry_run, &roots).await?)?;
            }
        },
        Command::Remote { command } => remote_command(command, &cli.project, &store).await?,
        Command::Export { command } => match command {
            ExportCmd::Oci { artifact, to } => {
                let id = resolve_artifact(&store, &artifact).await?;
                let p = export_oci(&store, &id, to).await?;
                println!("{}", p.display());
            }
        },
        Command::Publish { command } => match command {
            PublishCmd::Oci {
                artifact,
                reference,
                layout,
            } => {
                let id = resolve_artifact(&store, &artifact).await?;
                let path = layout.unwrap_or_else(|| {
                    std::env::temp_dir().join(format!("artifactum-oci-{}", &id.0.value[..12]))
                });
                export_oci(&store, &id, &path).await?;
                publish_oci_with_oras(&path, &reference).await?;
                println!("published {id} -> {reference}");
            }
        },
        Command::MigrateLegacy { path } => {
            println!("migrated {} blobs", store.migrate_legacy_blobs(path).await?)
        }
    }
    Ok(())
}

async fn build_engine(store: ArtifactStore, metadata: MetadataStore) -> Result<Engine> {
    let mut b = Engine::builder()
        .store(store)
        .metadata(metadata)
        .executor(BubblewrapExecutor)
        .executor(ContainerExecutor {
            runtime: std::env::var("ARTIFACTUM_OCI_RUNTIME").unwrap_or_else(|_| "docker".into()),
            image: std::env::var("ARTIFACTUM_CONTAINER_IMAGE")
                .unwrap_or_else(|_| "ubuntu:latest".into()),
        })
        .executor(SlurmExecutor::default())
        .executor(KubernetesExecutor {
            image: std::env::var("ARTIFACTUM_K8S_IMAGE").unwrap_or_else(|_| "ubuntu:latest".into()),
            namespace: std::env::var("ARTIFACTUM_K8S_NAMESPACE").ok(),
        });
    if let Ok(host) = std::env::var("ARTIFACTUM_SSH_HOST") {
        b = b.executor(SshExecutor {
            host,
            remote_dir: std::env::var("ARTIFACTUM_SSH_DIR")
                .unwrap_or_else(|_| "/tmp/artifactum".into()),
        });
    }
    for p in discover("artifactum-executor-") {
        let name = p
            .file_name()
            .and_then(|x| x.to_str())
            .unwrap_or("artifactum-executor-plugin")
            .trim_start_matches("artifactum-executor-")
            .to_owned();
        b = b.executor(PluginExecutor {
            executable: p,
            plugin_name: name,
        });
    }
    Ok(b.build().await?)
}
async fn resolver_builder_with_plugins(
    store: ArtifactStore,
    metadata: MetadataStore,
    offline: bool,
    jobs: usize,
) -> Result<ArtifactResolverBuilder> {
    let mut b = ArtifactResolverBuilder::default()
        .store(store)
        .metadata(metadata)
        .offline(offline)
        .max_concurrency(jobs);
    for path in discover("artifactum-provider-") {
        let file = path
            .file_name()
            .and_then(|x| x.to_str())
            .unwrap_or_default();
        if matches!(
            file,
            "artifactum-provider-local" | "artifactum-provider-http"
        ) {
            continue;
        }
        match PluginProvider::connect(path.clone()).await {
            Ok(p) => b = b.provider(p).map_err(|e| anyhow!(e))?,
            Err(e) => eprintln!("warning: ignoring provider plugin {}: {e}", path.display()),
        }
    }
    Ok(b)
}
async fn build_resolver(
    project: &ProjectManifest,
    store: ArtifactStore,
    metadata: MetadataStore,
    offline: bool,
    jobs: usize,
) -> Result<artifactum_resolver::ArtifactResolver> {
    let mut b = resolver_builder_with_plugins(store, metadata, offline, jobs).await?;
    for (name, p) in &project.providers {
        b = b.profile(artifactum_resolver::ProviderProfile {
            name: name.clone(),
            provider: p.kind.clone(),
            config: p.config.clone(),
        });
    }
    Ok(b.build().await?)
}
fn requirement(a: &ProjectArtifact) -> Result<ArtifactRequirement> {
    Ok(ArtifactRequirement {
        reference: a.source.parse()?,
        revision: a.revision.clone(),
        selection: Selection {
            include: a.include.clone(),
            exclude: a.exclude.clone(),
        },
        metadata: BTreeMap::new(),
    })
}
async fn load_or_default(path: &Path) -> Result<ProjectManifest> {
    if fs::try_exists(path).await? {
        Ok(ProjectManifest::load(path).await?)
    } else {
        Ok(ProjectManifest::default())
    }
}

async fn exec_command(
    args: ExecArgs,
    engine: &Engine,
    store: &ArtifactStore,
    json: bool,
) -> Result<()> {
    let mut b = ActionBuilder::new(
        "exec",
        args.command
            .first()
            .cloned()
            .ok_or_else(|| anyhow!("empty command"))?,
    )
    .args(args.command.iter().skip(1).cloned())
    .cache(parse_cache(&args.cache)?);
    for i in args.inputs {
        let (n, v) = split_assignment(&i)?;
        let id = if Path::new(v).exists() {
            if std::fs::metadata(v)?.is_dir() {
                store.import_tree(v).await?
            } else {
                store.import_blob_artifact(v, None).await?
            }
        } else {
            resolve_artifact(store, v).await?
        };
        b = b.input(n, id);
    }
    for c in args.code {
        let (n, v) = split_assignment(&c)?;
        let id = if std::fs::metadata(v)?.is_dir() {
            store.import_tree(v).await?
        } else {
            store.import_blob_artifact(v, None).await?
        };
        b = b.code(n, id);
    }
    for o in args.outputs {
        let (n, k) = split_assignment(&o)?;
        let spec = match k {
            "blob" => OutputSpec::blob(),
            "tree" => OutputSpec::tree(),
            "collection" => OutputSpec::collection(),
            _ => return Err(anyhow!("unknown output kind {k}")),
        };
        b = b.output(n, spec);
    }
    let r = engine.run(b.build()?, &args.executor).await?;
    print_value(json, &r)
}
async fn artifact_command(
    c: ArtifactCmd,
    store: &ArtifactStore,
    metadata: &MetadataStore,
    json: bool,
) -> Result<()> {
    match c {
        ArtifactCmd::Import {
            path,
            chunked,
            media_type,
            set_ref,
        } => {
            let id = if std::fs::metadata(&path)?.is_dir() {
                store.import_tree(&path).await?
            } else if chunked {
                store
                    .import_chunked_blob_artifact(&path, media_type)
                    .await?
            } else {
                store.import_blob_artifact(&path, media_type).await?
            };
            if let Some(name) = set_ref {
                store.set_ref(&name, &id, false).await?;
            }
            println!("{id}")
        }
        ArtifactCmd::Inspect { artifact } => {
            let id = resolve_artifact(store, &artifact).await?;
            let m = store.load_artifact(&id).await?;
            print_value(
                json,
                &serde_json::json!({"id":id,"manifest":m,"sources":metadata.source_observations(&id)?,"producers":metadata.producers_of(&id)?,"attestations":metadata.attestations(&id)?}),
            )?
        }
        ArtifactCmd::Cat { artifact } => {
            let id = resolve_artifact(store, &artifact).await?;
            let m = store.load_artifact(&id).await?;
            if m.kind != ContentKind::Blob {
                return Err(anyhow!("cat requires a blob artifact"));
            }
            print!(
                "{}",
                String::from_utf8_lossy(&store.read_content(&m.content).await?)
            );
        }
        ArtifactCmd::Ls { artifact } => {
            let id = resolve_artifact(store, &artifact).await?;
            let m = store.load_artifact(&id).await?;
            match m.kind {
                ContentKind::Blob => println!("{}", m.content),
                ContentKind::Tree => {
                    for e in store.read_tree(&m.content).await?.entries {
                        println!("{}\t{}\t{}", e.path, e.size, e.content)
                    }
                }
                ContentKind::Collection => {
                    for e in store.read_collection(&m.content).await?.entries {
                        println!("{}\t{}", e.key, e.artifact)
                    }
                }
            }
        }
        ArtifactCmd::Materialize { artifact, to, mode } => {
            let id = resolve_artifact(store, &artifact).await?;
            store.materialize(&id, to, parse_mode(&mode)?).await?
        }
        ArtifactCmd::Diff { left, right } => {
            let l = resolve_artifact(store, &left).await?;
            let r = resolve_artifact(store, &right).await?;
            let lm = store.load_artifact(&l).await?;
            let rm = store.load_artifact(&r).await?;
            print_value(json, &serde_json::json!({"same":l==r,"left":lm,"right":rm}))?
        }
    }
    Ok(())
}
async fn runs_command(
    c: RunsCmd,
    store: &ArtifactStore,
    metadata: &MetadataStore,
    engine: &Engine,
    json: bool,
) -> Result<()> {
    match c {
        RunsCmd::List { limit } => print_value(json, &metadata.recent_attempts(limit)?)?,
        RunsCmd::Show { id } => print_value(json, &metadata.attempt(uuid::Uuid::parse_str(&id)?)?)?,
        RunsCmd::Logs { id, stderr } => {
            let a = metadata
                .attempt(uuid::Uuid::parse_str(&id)?)?
                .ok_or_else(|| anyhow!("unknown attempt"))?;
            let cid = if stderr { a.stderr } else { a.stdout }
                .ok_or_else(|| anyhow!("log not recorded"))?;
            print!(
                "{}",
                String::from_utf8_lossy(&store.read_content(&cid).await?)
            );
        }
        RunsCmd::Retry { id } => {
            let result = engine.retry_attempt(uuid::Uuid::parse_str(&id)?).await?;
            print_value(json, &result)?
        }
        RunsCmd::Cancel { id } => {
            let id = uuid::Uuid::parse_str(&id)?;
            engine.request_cancel(id).await?;
            println!("cancellation requested for {id}");
        }
    }
    Ok(())
}
fn why_command(metadata: &MetadataStore, v: &str, json: bool) -> Result<()> {
    let key = ActionKey::from_str(v)
        .ok()
        .or_else(|| {
            metadata
                .get_kv(&format!("last-action:{v}"))
                .ok()
                .flatten()
                .and_then(|x| x.parse().ok())
        })
        .ok_or_else(|| anyhow!("unknown action or task {v}"))?;
    let spec = metadata.action(&key)?;
    let previous = metadata
        .get_kv(&format!("previous-action:{key}"))?
        .and_then(|value| value.parse::<ActionKey>().ok());
    let changed = match (&previous, &spec) {
        (Some(previous), Some(current)) => metadata
            .action(previous)?
            .map(|old| diff(&old, current))
            .transpose()?,
        _ => None,
    };
    let realizations = metadata.realizations_for_action(&key)?;
    print_value(
        json,
        &serde_json::json!({"action":key,"previous":previous,"changed":changed,"spec":spec,"realizations":realizations,"output_variants":metadata.determinism_report(&key)?}),
    )
}
async fn remote_command(c: RemoteCmd, project_path: &Path, store: &ArtifactStore) -> Result<()> {
    match c {
        RemoteCmd::Add {
            name,
            kind,
            location,
            read_only,
            token_env,
        } => {
            let mut p = load_or_default(project_path).await?;
            let spec = if kind == "file" {
                RemoteSpec {
                    kind,
                    path: Some(location.into()),
                    url: None,
                    token_env,
                    read_only,
                }
            } else {
                RemoteSpec {
                    kind,
                    path: None,
                    url: Some(location),
                    token_env,
                    read_only,
                }
            };
            p.remotes.insert(name, spec);
            p.save(project_path).await?
        }
        RemoteCmd::List => {
            let p = load_or_default(project_path).await?;
            for (n, r) in p.remotes {
                println!(
                    "{n}\t{}\t{}",
                    r.kind,
                    r.path
                        .map(|p| p.display().to_string())
                        .or(r.url)
                        .unwrap_or_default()
                )
            }
        }
        RemoteCmd::Push { name, artifact } => {
            let p = ProjectManifest::load(project_path).await?;
            let remote = remote_from(&p, &name).await?;
            let id = resolve_artifact(store, &artifact).await?;
            Mirror::new(store.clone(), remote).push(&id).await?
        }
        RemoteCmd::Pull { name, artifact } => {
            let p = ProjectManifest::load(project_path).await?;
            let remote = remote_from(&p, &name).await?;
            let id = ArtifactId::from_str(&artifact)?;
            Mirror::new(store.clone(), remote).pull(&id).await?
        }
        RemoteCmd::Serve {
            path,
            bind,
            token_env,
            read_only,
        } => {
            let token = token_env.and_then(|e| std::env::var(e).ok());
            artifactum_remote::serve(path, &bind, token, read_only).await?
        }
    }
    Ok(())
}
async fn remote_from(p: &ProjectManifest, name: &str) -> Result<Arc<dyn RemoteCache>> {
    let r = p
        .remotes
        .get(name)
        .ok_or_else(|| anyhow!("unknown remote {name}"))?;
    match r.kind.as_str() {
        "file" => Ok(Arc::new(
            FileRemote::open(
                r.path
                    .clone()
                    .ok_or_else(|| anyhow!("file remote missing path"))?,
                r.read_only,
            )
            .await?,
        )),
        "http" | "https" => {
            let token = r.token_env.as_ref().and_then(|e| std::env::var(e).ok());
            Ok(Arc::new(HttpRemote::new(
                r.url
                    .clone()
                    .ok_or_else(|| anyhow!("http remote missing url"))?,
                token,
                r.read_only,
            )))
        }
        x => Err(anyhow!("unsupported remote kind {x}")),
    }
}
async fn resolve_artifact(store: &ArtifactStore, value: &str) -> Result<ArtifactId> {
    if let Some(name) = value.strip_prefix('@') {
        store
            .get_ref(name)
            .await?
            .ok_or_else(|| anyhow!("unknown ref @{name}"))
    } else {
        Ok(ArtifactId::from_str(value)?)
    }
}
async fn verify_graph(store: &ArtifactStore, id: &ArtifactId) -> Result<()> {
    let m = store.load_artifact(id).await?;
    if !store.verify_content(&m.content).await? {
        return Err(anyhow!("content integrity failed for {}", m.content));
    }
    match m.kind {
        ContentKind::Tree => {
            for e in store.read_tree(&m.content).await?.entries {
                if !store.verify_content(&e.content).await? {
                    return Err(anyhow!("tree content failed: {}", e.path));
                }
            }
        }
        ContentKind::Collection => {
            for e in store.read_collection(&m.content).await?.entries {
                Box::pin(verify_graph(store, &e.artifact)).await?
            }
        }
        ContentKind::Blob => {
            if m.annotations
                .get("artifactum.storage")
                .is_some_and(|v| v == "cdc-v1")
            {
                for c in store.read_chunk_manifest(&m.content).await?.chunks {
                    if !store.verify_content(&c.content).await? {
                        return Err(anyhow!("chunk integrity failed: {}", c.content));
                    }
                }
            }
        }
    }
    Ok(())
}
fn parse_cache(v: &str) -> Result<CachePolicy> {
    match v {
        "pure" => Ok(CachePolicy::Pure),
        "reproducible" => Ok(CachePolicy::Reproducible),
        "volatile" => Ok(CachePolicy::Volatile),
        "effect" => Ok(CachePolicy::Effect),
        _ => Err(anyhow!("unknown cache policy {v}")),
    }
}
fn parse_mode(v: &str) -> Result<MaterializationMode> {
    match v {
        "auto" => Ok(MaterializationMode::Auto),
        "copy" => Ok(MaterializationMode::Copy),
        "hardlink" => Ok(MaterializationMode::Hardlink),
        "reflink" => Ok(MaterializationMode::Reflink),
        _ => Err(anyhow!("unknown materialization mode {v}")),
    }
}
fn split_assignment(v: &str) -> Result<(&str, &str)> {
    v.split_once('=')
        .ok_or_else(|| anyhow!("expected NAME=VALUE, got {v}"))
}
fn print_value<T: serde::Serialize>(json: bool, v: &T) -> Result<()> {
    if json {
        println!("{}", serde_json::to_string(v)?)
    } else {
        println!("{}", serde_json::to_string_pretty(v)?)
    }
    Ok(())
}
