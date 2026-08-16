//! SQLite metadata plane for Artifactum. Content bytes never live here.

use std::{collections::BTreeMap,path::{Path,PathBuf},sync::{Arc,Mutex}};
use artifactum_core::{ActionKey,ActionSpec,ArtifactId,Attestation,AttemptRecord,Checkpoint,Realization,SourceObservation};
use directories::ProjectDirs;
use rusqlite::{params,Connection,OptionalExtension};
use thiserror::Error;
use uuid::Uuid;

#[derive(Debug,Error)]
pub enum Error {
    #[error("could not determine Artifactum data directory")]
    DataDirectoryUnavailable,
    #[error("sqlite error: {0}")]
    Sqlite(#[from] rusqlite::Error),
    #[error("json error: {0}")]
    Json(#[from] serde_json::Error),
    #[error("core error: {0}")]
    Core(#[from] artifactum_core::Error),
    #[error("metadata mutex poisoned")]
    Poisoned,
}
pub type Result<T,E=Error>=std::result::Result<T,E>;

#[derive(Clone)]
pub struct MetadataStore { path:Arc<PathBuf>, conn:Arc<Mutex<Connection>> }
impl MetadataStore {
    pub fn xdg()->Result<Self>{let d=ProjectDirs::from("org","artifactum","artifactum").ok_or(Error::DataDirectoryUnavailable)?;Self::open(d.data_dir().join("metadata.sqlite"))}
    pub fn open(path:impl Into<PathBuf>)->Result<Self>{let path=path.into();if let Some(p)=path.parent(){std::fs::create_dir_all(p).map_err(|e|rusqlite::Error::ToSqlConversionFailure(Box::new(e)))?;}let conn=Connection::open(&path)?;let s=Self{path:Arc::new(path),conn:Arc::new(Mutex::new(conn))};s.migrate()?;Ok(s)}
    #[must_use] pub fn path(&self)->&Path{self.path.as_ref()}
    fn conn(&self)->Result<std::sync::MutexGuard<'_,Connection>>{self.conn.lock().map_err(|_|Error::Poisoned)}
    fn migrate(&self)->Result<()> {
        self.conn()?.execute_batch(r#"
PRAGMA journal_mode=WAL;
PRAGMA foreign_keys=ON;
CREATE TABLE IF NOT EXISTS actions(action_key TEXT PRIMARY KEY,spec_json TEXT NOT NULL,created_at TEXT NOT NULL);
CREATE TABLE IF NOT EXISTS attempts(id TEXT PRIMARY KEY,action_key TEXT NOT NULL,record_json TEXT NOT NULL,started_at TEXT NOT NULL,finished_at TEXT,status TEXT NOT NULL);
CREATE INDEX IF NOT EXISTS idx_attempts_action ON attempts(action_key,started_at DESC);
CREATE TABLE IF NOT EXISTS realizations(id TEXT PRIMARY KEY,action_key TEXT NOT NULL,attempt_id TEXT NOT NULL,record_json TEXT NOT NULL,created_at TEXT NOT NULL);
CREATE INDEX IF NOT EXISTS idx_realizations_action ON realizations(action_key,created_at DESC);
CREATE TABLE IF NOT EXISTS realization_outputs(realization_id TEXT NOT NULL,name TEXT NOT NULL,artifact_id TEXT NOT NULL,PRIMARY KEY(realization_id,name));
CREATE INDEX IF NOT EXISTS idx_outputs_artifact ON realization_outputs(artifact_id);
CREATE TABLE IF NOT EXISTS source_observations(id TEXT PRIMARY KEY,artifact_id TEXT NOT NULL,provider TEXT NOT NULL,canonical_ref TEXT NOT NULL,record_json TEXT NOT NULL,observed_at TEXT NOT NULL);
CREATE INDEX IF NOT EXISTS idx_source_artifact ON source_observations(artifact_id,observed_at DESC);
CREATE TABLE IF NOT EXISTS attestations(id TEXT PRIMARY KEY,subject_artifact TEXT NOT NULL,predicate_type TEXT NOT NULL,record_json TEXT NOT NULL,created_at TEXT NOT NULL);
CREATE INDEX IF NOT EXISTS idx_attest_subject ON attestations(subject_artifact,created_at DESC);
CREATE TABLE IF NOT EXISTS checkpoints(id TEXT PRIMARY KEY,action_key TEXT NOT NULL,name TEXT NOT NULL,artifact_id TEXT NOT NULL,record_json TEXT NOT NULL,created_at TEXT NOT NULL);
CREATE INDEX IF NOT EXISTS idx_checkpoint_action ON checkpoints(action_key,name,created_at DESC);
CREATE TABLE IF NOT EXISTS kv(key TEXT PRIMARY KEY,value TEXT NOT NULL);
"#)?;Ok(())
    }
    pub fn record_action(&self,key:&ActionKey,spec:&ActionSpec)->Result<()> {self.conn()?.execute("INSERT OR IGNORE INTO actions(action_key,spec_json,created_at) VALUES(?1,?2,datetime('now'))",params![key.to_string(),serde_json::to_string(spec)?])?;Ok(())}
    pub fn action(&self,key:&ActionKey)->Result<Option<ActionSpec>>{let s:Option<String>=self.conn()?.query_row("SELECT spec_json FROM actions WHERE action_key=?1",[key.to_string()],|r|r.get(0)).optional()?;s.map(|v|Ok(serde_json::from_str(&v)?)).transpose()}
    pub fn record_attempt(&self,a:&AttemptRecord)->Result<()> {let status=if a.finished_at.is_none(){"running"}else if a.exit_code==Some(0){"success"}else{"failed"};self.conn()?.execute("INSERT OR REPLACE INTO attempts(id,action_key,record_json,started_at,finished_at,status) VALUES(?1,?2,?3,?4,?5,?6)",params![a.id.to_string(),a.action.to_string(),serde_json::to_string(a)?,a.started_at.to_rfc3339(),a.finished_at.map(|x|x.to_rfc3339()),status])?;Ok(())}
    pub fn attempt(&self,id:Uuid)->Result<Option<AttemptRecord>>{let s:Option<String>=self.conn()?.query_row("SELECT record_json FROM attempts WHERE id=?1",[id.to_string()],|r|r.get(0)).optional()?;decode_opt(s)}
    pub fn attempts_for_action(&self,key:&ActionKey,limit:usize)->Result<Vec<AttemptRecord>>{let c=self.conn()?;let mut st=c.prepare("SELECT record_json FROM attempts WHERE action_key=?1 ORDER BY started_at DESC LIMIT ?2")?;let rows=st.query_map(params![key.to_string(),limit as i64],|r|r.get::<_,String>(0))?;decode_rows(rows)}
    pub fn recent_attempts(&self,limit:usize)->Result<Vec<AttemptRecord>>{let c=self.conn()?;let mut st=c.prepare("SELECT record_json FROM attempts ORDER BY started_at DESC LIMIT ?1")?;let rows=st.query_map([limit as i64],|r|r.get::<_,String>(0))?;decode_rows(rows)}
    pub fn record_realization(&self,r:&Realization)->Result<()> {let mut c=self.conn()?;let tx=c.transaction()?;tx.execute("INSERT OR REPLACE INTO realizations(id,action_key,attempt_id,record_json,created_at) VALUES(?1,?2,?3,?4,?5)",params![r.id.to_string(),r.action.to_string(),r.attempt.to_string(),serde_json::to_string(r)?,r.created_at.to_rfc3339()])?;tx.execute("DELETE FROM realization_outputs WHERE realization_id=?1",[r.id.to_string()])?;for(name,id)in&r.outputs{tx.execute("INSERT INTO realization_outputs(realization_id,name,artifact_id) VALUES(?1,?2,?3)",params![r.id.to_string(),name,id.to_string()])?;}tx.commit()?;Ok(())}
    pub fn latest_realization(&self,key:&ActionKey)->Result<Option<Realization>>{let s:Option<String>=self.conn()?.query_row("SELECT record_json FROM realizations WHERE action_key=?1 ORDER BY created_at DESC LIMIT 1",[key.to_string()],|r|r.get(0)).optional()?;decode_opt(s)}
    pub fn realizations_for_action(&self,key:&ActionKey)->Result<Vec<Realization>>{let c=self.conn()?;let mut st=c.prepare("SELECT record_json FROM realizations WHERE action_key=?1 ORDER BY created_at DESC")?;let rows=st.query_map([key.to_string()],|r|r.get::<_,String>(0))?;decode_rows(rows)}
    pub fn producers_of(&self,artifact:&ArtifactId)->Result<Vec<Realization>>{let c=self.conn()?;let mut st=c.prepare("SELECT r.record_json FROM realizations r JOIN realization_outputs o ON o.realization_id=r.id WHERE o.artifact_id=?1 ORDER BY r.created_at DESC")?;let rows=st.query_map([artifact.to_string()],|r|r.get::<_,String>(0))?;decode_rows(rows)}
    pub fn record_source_observation(&self,o:&SourceObservation)->Result<()> {self.conn()?.execute("INSERT OR REPLACE INTO source_observations(id,artifact_id,provider,canonical_ref,record_json,observed_at) VALUES(?1,?2,?3,?4,?5,?6)",params![o.id.to_string(),o.artifact.to_string(),o.provider,o.canonical_ref,serde_json::to_string(o)?,o.observed_at.to_rfc3339()])?;Ok(())}
    pub fn source_observations(&self,artifact:&ArtifactId)->Result<Vec<SourceObservation>>{let c=self.conn()?;let mut st=c.prepare("SELECT record_json FROM source_observations WHERE artifact_id=?1 ORDER BY observed_at DESC")?;let rows=st.query_map([artifact.to_string()],|r|r.get::<_,String>(0))?;decode_rows(rows)}
    pub fn record_attestation(&self,a:&Attestation)->Result<()> {self.conn()?.execute("INSERT OR REPLACE INTO attestations(id,subject_artifact,predicate_type,record_json,created_at) VALUES(?1,?2,?3,?4,?5)",params![a.id.to_string(),a.subject.to_string(),a.predicate_type,serde_json::to_string(a)?,a.created_at.to_rfc3339()])?;Ok(())}
    pub fn attestations(&self,subject:&ArtifactId)->Result<Vec<Attestation>>{let c=self.conn()?;let mut st=c.prepare("SELECT record_json FROM attestations WHERE subject_artifact=?1 ORDER BY created_at DESC")?;let rows=st.query_map([subject.to_string()],|r|r.get::<_,String>(0))?;decode_rows(rows)}
    pub fn record_checkpoint(&self,c:&Checkpoint)->Result<()> {self.conn()?.execute("INSERT OR REPLACE INTO checkpoints(id,action_key,name,artifact_id,record_json,created_at) VALUES(?1,?2,?3,?4,?5,?6)",params![c.id.to_string(),c.action.to_string(),c.name,c.artifact.to_string(),serde_json::to_string(c)?,c.created_at.to_rfc3339()])?;Ok(())}
    pub fn latest_checkpoint(&self,key:&ActionKey,name:&str)->Result<Option<Checkpoint>>{let s:Option<String>=self.conn()?.query_row("SELECT record_json FROM checkpoints WHERE action_key=?1 AND name=?2 ORDER BY created_at DESC LIMIT 1",params![key.to_string(),name],|r|r.get(0)).optional()?;decode_opt(s)}
    pub fn latest_checkpoints(&self,key:&ActionKey)->Result<Vec<Checkpoint>>{let c=self.conn()?;let mut st=c.prepare("SELECT c.record_json FROM checkpoints c JOIN (SELECT name,MAX(created_at) AS created_at FROM checkpoints WHERE action_key=?1 GROUP BY name) x ON c.name=x.name AND c.created_at=x.created_at WHERE c.action_key=?1 ORDER BY c.name")?;let rows=st.query_map([key.to_string()],|r|r.get::<_,String>(0))?;decode_rows(rows)}
    pub fn set_kv(&self,key:&str,value:&str)->Result<()> {self.conn()?.execute("INSERT INTO kv(key,value) VALUES(?1,?2) ON CONFLICT(key) DO UPDATE SET value=excluded.value",params![key,value])?;Ok(())}
    pub fn get_kv(&self,key:&str)->Result<Option<String>>{Ok(self.conn()?.query_row("SELECT value FROM kv WHERE key=?1",[key],|r|r.get(0)).optional()?)}
    pub fn gc_roots(&self,retention_days:i64)->Result<Vec<ArtifactId>>{let c=self.conn()?;let modifier=format!("-{} days",retention_days.max(0));let mut out=BTreeMap::<String,ArtifactId>::new();{let mut st=c.prepare("SELECT DISTINCT o.artifact_id FROM realization_outputs o JOIN realizations r ON r.id=o.realization_id WHERE datetime(r.created_at)>=datetime('now',?1)")?;for row in st.query_map([modifier],|r|r.get::<_,String>(0))?{let id:ArtifactId=row?.parse()?;out.insert(id.to_string(),id);}}for sql in ["SELECT DISTINCT artifact_id FROM source_observations","SELECT DISTINCT artifact_id FROM checkpoints","SELECT DISTINCT subject_artifact FROM attestations"]{let mut st=c.prepare(sql)?;for row in st.query_map([],|r|r.get::<_,String>(0))?{let id:ArtifactId=row?.parse()?;out.insert(id.to_string(),id);}}Ok(out.into_values().collect())}
    pub fn output_roots(&self)->Result<Vec<ArtifactId>>{self.gc_roots(30)}
    pub fn determinism_report(&self,key:&ActionKey)->Result<BTreeMap<String,Vec<String>>>{let rs=self.realizations_for_action(key)?;let mut out:BTreeMap<String,Vec<String>>=BTreeMap::new();for r in rs{for(name,id)in r.outputs{out.entry(name).or_default().push(id.to_string());}}for ids in out.values_mut(){ids.sort();ids.dedup();}Ok(out)}
}

fn decode_opt<T:serde::de::DeserializeOwned>(s:Option<String>)->Result<Option<T>>{s.map(|v|Ok(serde_json::from_str(&v)?)).transpose()}
fn decode_rows<T,I>(rows:I)->Result<Vec<T>> where T:serde::de::DeserializeOwned,I:IntoIterator<Item=rusqlite::Result<String>>{let mut out=Vec::new();for r in rows{out.push(serde_json::from_str(&r?)?);}Ok(out)}

#[cfg(test)]
mod tests {
    use super::*; use artifactum_core::{ActionSpec,AttemptRecord}; use chrono::Utc;
    #[test]
    fn realization_roundtrip(){let d=tempfile::tempdir().unwrap();let db=MetadataStore::open(d.path().join("m.db")).unwrap();let a=ActionSpec::command("x",vec!["true".into()]);let k=a.key().unwrap();db.record_action(&k,&a).unwrap();let at=AttemptRecord{id:Uuid::new_v4(),action:k.clone(),executor:"local".into(),started_at:Utc::now(),finished_at:Some(Utc::now()),exit_code:Some(0),stdout:None,stderr:None,metrics:None,error:None};db.record_attempt(&at).unwrap();let r=Realization{id:Uuid::new_v4(),action:k.clone(),attempt:at.id,created_at:Utc::now(),outputs:BTreeMap::new()};db.record_realization(&r).unwrap();assert!(db.latest_realization(&k).unwrap().is_some());}
}
