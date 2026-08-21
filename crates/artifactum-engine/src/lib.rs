//! Artifact execution engine: sandbox materialization, action cache, attempts,
//! realizations, determinism auditing, lineage and checkpoints.

use artifactum_action::{ActionDiff, diff};
use artifactum_core::{
    ActionKey, ActionSpec, ArtifactId, AttemptRecord, Attestation, CachePolicy, Checkpoint,
    CollectionEntry, CollectionManifest, ContentKind, Realization, SourceObservation,
};
use artifactum_executor::{ExecutionRequest, Executor, LocalExecutor};
use artifactum_metadata::MetadataStore;
use artifactum_store::{ArtifactStore, ContentStore, MaterializationMode};
use chrono::Utc;
use serde::{Deserialize, Serialize};
use std::{
    collections::{BTreeMap, BTreeSet, HashMap},
    path::{Path, PathBuf},
    sync::Arc,
    time::Duration,
};
use thiserror::Error;
use tokio::fs;
use uuid::Uuid;

#[derive(Debug, Error)]
pub enum Error {
    #[error("core error: {0}")]
    Core(#[from] artifactum_core::Error),
    #[error("store error: {0}")]
    Store(#[from] artifactum_store::Error),
    #[error("metadata error: {0}")]
    Metadata(#[from] artifactum_metadata::Error),
    #[error("executor error: {0}")]
    Executor(#[from] artifactum_executor::Error),
    #[error("action error: {0}")]
    Action(#[from] artifactum_action::Error),
    #[error("I/O error: {0}")]
    Io(#[from] std::io::Error),
    #[error("json error: {0}")]
    Json(#[from] serde_json::Error),
    #[error("unknown executor `{0}`")]
    UnknownExecutor(String),
    #[error("action `{name}` failed with exit code {exit_code}; attempt {attempt}")]
    ExecutionFailed {
        name: String,
        exit_code: i32,
        attempt: Uuid,
    },
    #[error("declared output `{0}` was not produced")]
    MissingOutput(String),
    #[error("budget exceeded: {0}")]
    BudgetExceeded(String),
    #[error(
        "determinism violation for action {action}: output `{output}` had {variants} distinct artifacts"
    )]
    Nondeterministic {
        action: ActionKey,
        output: String,
        variants: usize,
    },
    #[error("invalid collection output `{0}`")]
    InvalidCollection(String),
}
pub type Result<T, E = Error> = std::result::Result<T, E>;

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct RunResult {
    pub action: ActionKey,
    pub attempt: Option<Uuid>,
    pub realization: Realization,
    pub cache_hit: bool,
}
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct WhyResult {
    pub action: ActionKey,
    pub cache_hit: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub previous: Option<ActionKey>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub diff: Option<ActionDiff>,
}
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct LineageNode {
    pub artifact: ArtifactId,
    pub sources: Vec<SourceObservation>,
    pub producers: Vec<Realization>,
    pub inputs: Vec<ArtifactId>,
}
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct DeterminismReport {
    pub action: ActionKey,
    pub runs: Vec<Realization>,
    pub output_variants: BTreeMap<String, Vec<ArtifactId>>,
    pub deterministic: bool,
}

#[derive(Clone)]
pub struct Engine {
    store: ArtifactStore,
    metadata: MetadataStore,
    executors: Arc<HashMap<String, Arc<dyn Executor>>>,
}
pub struct EngineBuilder {
    store: Option<ArtifactStore>,
    metadata: Option<MetadataStore>,
    executors: HashMap<String, Arc<dyn Executor>>,
}
impl Default for EngineBuilder {
    fn default() -> Self {
        let mut executors: HashMap<String, Arc<dyn Executor>> = HashMap::new();
        executors.insert("local".into(), Arc::new(LocalExecutor));
        Self {
            store: None,
            metadata: None,
            executors,
        }
    }
}
impl EngineBuilder {
    pub fn store(mut self, v: ArtifactStore) -> Self {
        self.store = Some(v);
        self
    }
    pub fn metadata(mut self, v: MetadataStore) -> Self {
        self.metadata = Some(v);
        self
    }
    pub fn executor<E: Executor + 'static>(mut self, e: E) -> Self {
        self.executors.insert(e.name().into(), Arc::new(e));
        self
    }
    pub async fn build(self) -> Result<Engine> {
        Ok(Engine {
            store: match self.store {
                Some(v) => v,
                None => ArtifactStore::xdg().await?,
            },
            metadata: self.metadata.unwrap_or(MetadataStore::xdg()?),
            executors: Arc::new(self.executors),
        })
    }
}
impl Engine {
    pub fn builder() -> EngineBuilder {
        EngineBuilder::default()
    }
    pub fn store(&self) -> &ArtifactStore {
        &self.store
    }
    pub fn metadata(&self) -> &MetadataStore {
        &self.metadata
    }
    pub async fn run(&self, spec: ActionSpec, executor: &str) -> Result<RunResult> {
        self.run_inner(spec, executor, false).await
    }
    pub async fn run_uncached(&self, spec: ActionSpec, executor: &str) -> Result<RunResult> {
        self.run_inner(spec, executor, true).await
    }
    async fn run_inner(
        &self,
        mut spec: ActionSpec,
        executor: &str,
        force: bool,
    ) -> Result<RunResult> {
        if let Some(max) = spec.budget.max_wall_seconds {
            spec.resources.timeout_seconds =
                Some(spec.resources.timeout_seconds.map_or(max, |v| v.min(max)));
        }
        let key = spec.key()?;
        self.metadata.record_action(&key, &spec)?;
        let previous_name_key = self
            .metadata
            .get_kv(&format!("last-action:{}", spec.name))?
            .and_then(|v| v.parse::<ActionKey>().ok());
        if let Some(previous) = previous_name_key
            .as_ref()
            .filter(|previous| *previous != &key)
        {
            self.metadata
                .set_kv(&format!("previous-action:{key}"), &previous.to_string())?;
        }
        if !force && matches!(spec.cache, CachePolicy::Pure | CachePolicy::Reproducible) {
            if let Some(r) = self.metadata.latest_realization(&key)? {
                if self.realization_available(&r).await? {
                    self.metadata
                        .set_kv(&format!("last-action:{}", spec.name), &key.to_string())?;
                    return Ok(RunResult {
                        action: key,
                        attempt: None,
                        realization: r,
                        cache_hit: true,
                    });
                }
            }
        }
        let exec = self
            .executors
            .get(executor)
            .cloned()
            .ok_or_else(|| Error::UnknownExecutor(executor.into()))?;
        let lease_roots = spec
            .inputs
            .values()
            .chain(spec.code.values())
            .cloned()
            .collect::<Vec<_>>();
        let lease = self
            .store
            .create_lease(
                format!("action:{key}"),
                lease_roots,
                Duration::from_secs(
                    spec.resources
                        .timeout_seconds
                        .unwrap_or(3600)
                        .saturating_add(600),
                ),
            )
            .await?;
        let tmp = tempfile::Builder::new()
            .prefix("artifactum-run-")
            .tempdir()?;
        let root = tmp.path();
        let in_dir = root.join("in");
        let code_dir = root.join("code");
        let out_dir = root.join("out");
        let tmp_dir = root.join("tmp");
        let checkpoint_in = root.join("checkpoint/in");
        let checkpoint_out = root.join("checkpoint/out");
        for d in [
            &in_dir,
            &code_dir,
            &out_dir,
            &tmp_dir,
            &checkpoint_in,
            &checkpoint_out,
        ] {
            fs::create_dir_all(d).await?;
        }
        let mut env = spec.environment.variables.clone();
        env.insert("ARTIFACTUM_ACTION_KEY".into(), key.to_string());
        env.insert("ARTIFACTUM_TMPDIR".into(), tmp_dir.display().to_string());
        env.insert("ARTIFACTUM_OUT".into(), out_dir.display().to_string());
        env.insert(
            "ARTIFACTUM_CHECKPOINT_IN".into(),
            checkpoint_in.display().to_string(),
        );
        env.insert(
            "ARTIFACTUM_CHECKPOINT_OUT".into(),
            checkpoint_out.display().to_string(),
        );
        for checkpoint in self.metadata.latest_checkpoints(&key)? {
            let p = checkpoint_in.join(safe(&checkpoint.name));
            self.store
                .materialize(&checkpoint.artifact, &p, MaterializationMode::Auto)
                .await?;
            make_readonly(&p).await?;
            env.insert(
                format!("ARTIFACTUM_CHECKPOINT_{}", env_name(&checkpoint.name)),
                p.display().to_string(),
            );
        }
        let mut replacements = BTreeMap::new();
        for (name, id) in &spec.inputs {
            let p = in_dir.join(safe(name));
            self.store
                .materialize(id, &p, MaterializationMode::Auto)
                .await?;
            make_readonly(&p).await?;
            env.insert(
                format!("ARTIFACTUM_INPUT_{}", env_name(name)),
                p.display().to_string(),
            );
            replacements.insert(format!("{{in.{name}}}"), p.display().to_string());
        }
        for (name, id) in &spec.code {
            let p = code_dir.join(safe(name));
            self.store
                .materialize(id, &p, MaterializationMode::Auto)
                .await?;
            make_readonly(&p).await?;
            env.insert(
                format!("ARTIFACTUM_CODE_{}", env_name(name)),
                p.display().to_string(),
            );
            replacements.insert(format!("{{code.{name}}}"), p.display().to_string());
        }
        for (name, o) in &spec.outputs {
            let p = out_dir.join(safe(name));
            if matches!(o.kind, ContentKind::Tree | ContentKind::Collection) {
                fs::create_dir_all(&p).await?;
            }
            env.insert(
                format!("ARTIFACTUM_OUTPUT_{}", env_name(name)),
                p.display().to_string(),
            );
            replacements.insert(format!("{{out.{name}}}"), p.display().to_string());
        }
        let cmd = spec
            .command
            .iter()
            .map(|x| replace_all(x, &replacements))
            .collect::<Vec<_>>();
        let attempt_id = Uuid::new_v4();
        let started = Utc::now();
        let mut attempt = AttemptRecord {
            id: attempt_id,
            action: key.clone(),
            executor: executor.into(),
            started_at: started,
            finished_at: None,
            exit_code: None,
            stdout: None,
            stderr: None,
            metrics: None,
            error: None,
        };
        self.metadata.record_attempt(&attempt)?;
        let control_dir = self.store.root().join("control");
        fs::create_dir_all(&control_dir).await?;
        let cancel_file = control_dir.join(format!("{attempt_id}.cancel"));
        let _ = fs::remove_file(&cancel_file).await;
        self.metadata.set_kv(
            &format!("attempt-cancel:{attempt_id}"),
            &cancel_file.display().to_string(),
        )?;
        let request = ExecutionRequest {
            action: key.clone(),
            command: cmd,
            cwd: root.to_path_buf(),
            env,
            resources: spec.resources.clone(),
            container: spec.environment.container.clone(),
            network: spec.network.clone(),
            cancel_file: Some(cancel_file.clone()),
        };
        let result = match exec.execute(&request).await {
            Ok(v) => v,
            Err(e) => {
                attempt.finished_at = Some(Utc::now());
                attempt.error = Some(e.to_string());
                self.metadata.record_attempt(&attempt)?;
                self.capture_checkpoint_dir(&key, &checkpoint_out).await?;
                self.store.release_lease(lease.id).await?;
                let _ = fs::remove_file(&cancel_file).await;
                return Err(e.into());
            }
        };
        attempt.finished_at = Some(Utc::now());
        attempt.exit_code = Some(result.exit_code);
        attempt.stdout = Some(self.store.put_bytes(&result.stdout).await?);
        attempt.stderr = Some(self.store.put_bytes(&result.stderr).await?);
        attempt.metrics = Some(result.metrics.clone());
        self.metadata.record_attempt(&attempt)?;
        self.capture_checkpoint_dir(&key, &checkpoint_out).await?;
        if let Some(max) = spec.budget.max_usd_micros {
            if result.metrics.estimated_cost_usd_micros > max {
                self.store.release_lease(lease.id).await?;
                let _ = fs::remove_file(&cancel_file).await;
                return Err(Error::BudgetExceeded(format!(
                    "cost {}µUSD > {}µUSD",
                    result.metrics.estimated_cost_usd_micros, max
                )));
            }
        }
        if result.exit_code != 0 {
            self.store.release_lease(lease.id).await?;
            let _ = fs::remove_file(&cancel_file).await;
            return Err(Error::ExecutionFailed {
                name: spec.name.clone(),
                exit_code: result.exit_code,
                attempt: attempt_id,
            });
        }
        let mut outputs = BTreeMap::new();
        for (name, o) in &spec.outputs {
            let p = out_dir.join(safe(name));
            if !fs::try_exists(&p).await? {
                self.store.release_lease(lease.id).await?;
                let _ = fs::remove_file(&cancel_file).await;
                return Err(Error::MissingOutput(name.clone()));
            }
            let id = match o.kind {
                ContentKind::Blob => {
                    self.store
                        .import_blob_artifact(&p, o.media_type.clone())
                        .await?
                }
                ContentKind::Tree => self.store.import_tree(&p).await?,
                ContentKind::Collection => {
                    self.import_collection_dir(&p, o.media_type.clone()).await?
                }
            };
            outputs.insert(name.clone(), id);
        }
        if outputs.is_empty() && matches!(spec.cache, CachePolicy::Effect) {
            let receipt = serde_json::to_vec(
                &serde_json::json!({"action":key.to_string(),"attempt":attempt_id,"exit_code":result.exit_code,"finished_at":Utc::now(),"stdout":attempt.stdout.as_ref().map(ToString::to_string),"stderr":attempt.stderr.as_ref().map(ToString::to_string)}),
            )?;
            outputs.insert(
                "receipt".into(),
                self.store
                    .artifact_from_bytes(
                        &receipt,
                        Some("application/vnd.artifactum.effect-receipt+json".into()),
                    )
                    .await?,
            );
        }
        let realization = Realization {
            id: Uuid::new_v4(),
            action: key.clone(),
            attempt: attempt_id,
            created_at: Utc::now(),
            outputs,
        };
        self.metadata.record_realization(&realization)?;
        self.metadata
            .set_kv(&format!("last-action:{}", spec.name), &key.to_string())?;
        self.store.release_lease(lease.id).await?;
        let _ = fs::remove_file(&cancel_file).await;
        if matches!(spec.cache, CachePolicy::Pure) {
            self.assert_deterministic_history(&key)?;
        }
        Ok(RunResult {
            action: key,
            attempt: Some(attempt_id),
            realization,
            cache_hit: false,
        })
    }
    async fn realization_available(&self, r: &Realization) -> Result<bool> {
        for id in r.outputs.values() {
            if self.store.load_artifact(id).await.is_err() {
                return Ok(false);
            }
        }
        Ok(true)
    }
    async fn import_collection_dir(
        &self,
        path: &Path,
        media: Option<String>,
    ) -> Result<ArtifactId> {
        let mut entries = Vec::new();
        let mut rd = fs::read_dir(path).await?;
        while let Some(e) = rd.next_entry().await? {
            let key = e.file_name().to_string_lossy().into_owned();
            let p = e.path();
            let id = if e.file_type().await?.is_dir() {
                self.store.import_tree(&p).await?
            } else {
                self.store.import_blob_artifact(&p, media.clone()).await?
            };
            entries.push(CollectionEntry {
                key: key.clone(),
                artifact: id,
                label: Some(key),
            });
        }
        if entries.is_empty() {
            return Err(Error::InvalidCollection(path.display().to_string()));
        }
        Ok(self
            .store
            .put_collection(&CollectionManifest::new(entries))
            .await?)
    }
    pub fn why(&self, spec: &ActionSpec) -> Result<WhyResult> {
        let key = spec.key()?;
        let hit = self.metadata.latest_realization(&key)?.is_some();
        let previous = self
            .metadata
            .get_kv(&format!("last-action:{}", spec.name))?
            .and_then(|v| v.parse().ok());
        let diff_result = match &previous {
            Some(p) if p != &key => self
                .metadata
                .action(p)?
                .map(|old| diff(&old, spec))
                .transpose()?,
            _ => None,
        };
        Ok(WhyResult {
            action: key,
            cache_hit: hit,
            previous,
            diff: diff_result,
        })
    }
    pub fn assert_deterministic_history(&self, key: &ActionKey) -> Result<()> {
        for (output, variants) in self.metadata.determinism_report(key)? {
            if variants.len() > 1 {
                return Err(Error::Nondeterministic {
                    action: key.clone(),
                    output,
                    variants: variants.len(),
                });
            }
        }
        Ok(())
    }
    pub async fn audit_determinism(
        &self,
        spec: ActionSpec,
        executor: &str,
        runs: usize,
    ) -> Result<DeterminismReport> {
        let key = spec.key()?;
        let mut rs = Vec::new();
        for _ in 0..runs.max(2) {
            rs.push(self.run_uncached(spec.clone(), executor).await?.realization)
        }
        let mut variants: BTreeMap<String, Vec<ArtifactId>> = BTreeMap::new();
        for r in &rs {
            for (n, id) in &r.outputs {
                variants.entry(n.clone()).or_default().push(id.clone())
            }
        }
        for v in variants.values_mut() {
            v.sort();
            v.dedup();
        }
        let deterministic = variants.values().all(|v| v.len() == 1);
        Ok(DeterminismReport {
            action: key,
            runs: rs,
            output_variants: variants,
            deterministic,
        })
    }
    async fn capture_checkpoint_dir(&self, action: &ActionKey, dir: &Path) -> Result<()> {
        let mut rd = fs::read_dir(dir).await?;
        while let Some(e) = rd.next_entry().await? {
            let name = e.file_name().to_string_lossy().into_owned();
            let id = if e.file_type().await?.is_dir() {
                self.store.import_tree(e.path()).await?
            } else {
                self.store.import_blob_artifact(e.path(), None).await?
            };
            self.checkpoint(action, name, id)?;
        }
        Ok(())
    }
    pub fn checkpoint(
        &self,
        action: &ActionKey,
        name: impl Into<String>,
        artifact: ArtifactId,
    ) -> Result<Checkpoint> {
        let c = Checkpoint {
            id: Uuid::new_v4(),
            action: action.clone(),
            name: name.into(),
            artifact,
            created_at: Utc::now(),
        };
        self.metadata.record_checkpoint(&c)?;
        Ok(c)
    }
    pub fn latest_checkpoint(&self, action: &ActionKey, name: &str) -> Result<Option<Checkpoint>> {
        Ok(self.metadata.latest_checkpoint(action, name)?)
    }
    pub fn attest(
        &self,
        subject: ArtifactId,
        predicate_type: impl Into<String>,
        statement: serde_json::Value,
        issuer: Option<String>,
    ) -> Result<Attestation> {
        let a = Attestation {
            id: Uuid::new_v4(),
            subject,
            predicate_type: predicate_type.into(),
            statement,
            created_at: Utc::now(),
            issuer,
            signature: None,
        };
        self.metadata.record_attestation(&a)?;
        Ok(a)
    }
    pub fn lineage(&self, artifact: &ArtifactId) -> Result<Vec<LineageNode>> {
        let mut out = Vec::new();
        let mut queue = vec![artifact.clone()];
        let mut seen = BTreeSet::new();
        while let Some(id) = queue.pop() {
            if !seen.insert(id.to_string()) {
                continue;
            }
            let sources = self.metadata.source_observations(&id)?;
            let producers = self.metadata.producers_of(&id)?;
            let mut inputs = Vec::new();
            for p in &producers {
                if let Some(action) = self.metadata.action(&p.action)? {
                    inputs.extend(action.inputs.values().cloned());
                    inputs.extend(action.code.values().cloned());
                }
            }
            queue.extend(inputs.iter().cloned());
            out.push(LineageNode {
                artifact: id,
                sources,
                producers,
                inputs,
            });
        }
        Ok(out)
    }
    pub fn gc_roots(&self, retention_days: i64) -> Result<Vec<ArtifactId>> {
        Ok(self.metadata.gc_roots(retention_days)?)
    }
    pub async fn request_cancel(&self, attempt: Uuid) -> Result<()> {
        let path = self
            .metadata
            .get_kv(&format!("attempt-cancel:{attempt}"))?
            .map(PathBuf::from)
            .unwrap_or_else(|| {
                self.store
                    .root()
                    .join("control")
                    .join(format!("{attempt}.cancel"))
            });
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent).await?;
        }
        fs::write(path, b"cancel\n").await?;
        Ok(())
    }
    pub async fn retry_attempt(&self, attempt: Uuid) -> Result<RunResult> {
        let record = self
            .metadata
            .attempt(attempt)?
            .ok_or_else(|| Error::ExecutionFailed {
                name: "unknown-attempt".into(),
                exit_code: -1,
                attempt,
            })?;
        let spec = self
            .metadata
            .action(&record.action)?
            .ok_or_else(|| Error::ExecutionFailed {
                name: "missing-action".into(),
                exit_code: -1,
                attempt,
            })?;
        self.run_uncached(spec, &record.executor).await
    }
    pub fn realize_intrinsic(
        &self,
        spec: ActionSpec,
        outputs: BTreeMap<String, ArtifactId>,
    ) -> Result<RunResult> {
        let key = spec.key()?;
        self.metadata.record_action(&key, &spec)?;
        if let Some(existing) = self.metadata.latest_realization(&key)? {
            if existing.outputs == outputs {
                return Ok(RunResult {
                    action: key,
                    attempt: None,
                    realization: existing,
                    cache_hit: true,
                });
            }
        }
        let attempt_id = Uuid::new_v4();
        let now = Utc::now();
        let attempt = AttemptRecord {
            id: attempt_id,
            action: key.clone(),
            executor: "intrinsic".into(),
            started_at: now,
            finished_at: Some(now),
            exit_code: Some(0),
            stdout: None,
            stderr: None,
            metrics: Some(artifactum_core::ExecutionMetrics {
                wall_millis: 0,
                bytes_read: 0,
                bytes_written: 0,
                estimated_cost_usd_micros: 0,
            }),
            error: None,
        };
        self.metadata.record_attempt(&attempt)?;
        let realization = Realization {
            id: Uuid::new_v4(),
            action: key.clone(),
            attempt: attempt_id,
            created_at: now,
            outputs,
        };
        self.metadata.record_realization(&realization)?;
        self.metadata
            .set_kv(&format!("last-action:{}", spec.name), &key.to_string())?;
        Ok(RunResult {
            action: key,
            attempt: Some(attempt_id),
            realization,
            cache_hit: false,
        })
    }
}

fn replace_all(s: &str, repl: &BTreeMap<String, String>) -> String {
    let mut out = s.to_owned();
    for (k, v) in repl {
        out = out.replace(k, v);
    }
    out
}
fn safe(v: &str) -> String {
    v.chars()
        .map(|c| {
            if c.is_ascii_alphanumeric() || matches!(c, '-' | '_' | '.') {
                c
            } else {
                '_'
            }
        })
        .collect()
}
fn env_name(v: &str) -> String {
    v.chars()
        .map(|c| {
            if c.is_ascii_alphanumeric() {
                c.to_ascii_uppercase()
            } else {
                '_'
            }
        })
        .collect()
}
async fn make_readonly(path: &Path) -> std::io::Result<()> {
    if path.is_file() {
        let mut p = fs::metadata(path).await?.permissions();
        p.set_readonly(true);
        fs::set_permissions(path, p).await?;
        return Ok(());
    }
    if path.is_dir() {
        let mut rd = fs::read_dir(path).await?;
        while let Some(e) = rd.next_entry().await? {
            Box::pin(make_readonly(&e.path())).await?;
        }
    }
    Ok(())
}
