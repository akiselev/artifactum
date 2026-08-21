//! Action construction, canonical identity and structural explanations.

use artifactum_core::{
    ActionKey, ActionSpec, ArtifactId, BudgetSpec, CachePolicy, EnvironmentSpec, NetworkPolicy,
    OutputSpec, ResourceSpec, SandboxPolicy,
};
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use thiserror::Error;

#[derive(Debug, Error)]
pub enum Error {
    #[error("action needs at least one command argv element")]
    EmptyCommand,
    #[error("core error: {0}")]
    Core(#[from] artifactum_core::Error),
}
pub type Result<T, E = Error> = std::result::Result<T, E>;

pub struct ActionBuilder {
    spec: ActionSpec,
}
impl ActionBuilder {
    pub fn new(name: impl Into<String>, program: impl Into<String>) -> Self {
        Self {
            spec: ActionSpec::command(name, vec![program.into()]),
        }
    }
    pub fn arg(mut self, arg: impl Into<String>) -> Self {
        self.spec.command.push(arg.into());
        self
    }
    pub fn args(mut self, args: impl IntoIterator<Item = impl Into<String>>) -> Self {
        self.spec.command.extend(args.into_iter().map(Into::into));
        self
    }
    pub fn input(mut self, name: impl Into<String>, artifact: ArtifactId) -> Self {
        self.spec.inputs.insert(name.into(), artifact);
        self
    }
    pub fn code(mut self, name: impl Into<String>, artifact: ArtifactId) -> Self {
        self.spec.code.insert(name.into(), artifact);
        self
    }
    pub fn output(mut self, name: impl Into<String>, spec: OutputSpec) -> Self {
        self.spec.outputs.insert(name.into(), spec);
        self
    }
    pub fn parameter(mut self, key: &str, value: serde_json::Value) -> Self {
        if !self.spec.parameters.is_object() {
            self.spec.parameters = serde_json::json!({});
        }
        self.spec
            .parameters
            .as_object_mut()
            .expect("object")
            .insert(key.into(), value);
        self
    }
    pub fn env(mut self, key: impl Into<String>, value: impl Into<String>) -> Self {
        self.spec
            .environment
            .variables
            .insert(key.into(), value.into());
        self
    }
    pub fn environment(mut self, v: EnvironmentSpec) -> Self {
        self.spec.environment = v;
        self
    }
    pub fn resources(mut self, v: ResourceSpec) -> Self {
        self.spec.resources = v;
        self
    }
    pub fn budget(mut self, v: BudgetSpec) -> Self {
        self.spec.budget = v;
        self
    }
    pub fn cache(mut self, v: CachePolicy) -> Self {
        self.spec.cache = v;
        self
    }
    pub fn network(mut self, v: NetworkPolicy) -> Self {
        self.spec.network = v;
        self
    }
    pub fn sandbox(mut self, v: SandboxPolicy) -> Self {
        self.spec.sandbox = v;
        self
    }
    pub fn platform(mut self, v: impl Into<String>) -> Self {
        self.spec.platform = Some(v.into());
        self
    }
    pub fn build(self) -> Result<ActionSpec> {
        if self.spec.command.is_empty() {
            return Err(Error::EmptyCommand);
        }
        Ok(self.spec)
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ActionDiff {
    pub old: ActionKey,
    pub new: ActionKey,
    pub changes: Vec<ActionChange>,
}
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ActionChange {
    pub field: String,
    pub old: String,
    pub new: String,
}
pub fn diff(old: &ActionSpec, new: &ActionSpec) -> Result<ActionDiff> {
    let mut changes = Vec::new();
    cmp("command", &old.command, &new.command, &mut changes);
    cmp("inputs", &old.inputs, &new.inputs, &mut changes);
    cmp("code", &old.code, &new.code, &mut changes);
    cmp("parameters", &old.parameters, &new.parameters, &mut changes);
    cmp(
        "environment",
        &old.environment,
        &new.environment,
        &mut changes,
    );
    cmp("outputs", &old.outputs, &new.outputs, &mut changes);
    cmp("resources", &old.resources, &new.resources, &mut changes);
    cmp("budget", &old.budget, &new.budget, &mut changes);
    cmp("network", &old.network, &new.network, &mut changes);
    cmp("sandbox", &old.sandbox, &new.sandbox, &mut changes);
    cmp("cache", &old.cache, &new.cache, &mut changes);
    cmp("platform", &old.platform, &new.platform, &mut changes);
    Ok(ActionDiff {
        old: old.key()?,
        new: new.key()?,
        changes,
    })
}
fn cmp<T: Serialize>(field: &str, a: &T, b: &T, out: &mut Vec<ActionChange>) {
    let aa = serde_json::to_string(a).unwrap_or_default();
    let bb = serde_json::to_string(b).unwrap_or_default();
    if aa != bb {
        out.push(ActionChange {
            field: field.into(),
            old: aa,
            new: bb,
        });
    }
}

/// Inputs affecting action identity. Scheduling priority, worker selection and
/// retry counters are deliberately absent from ActionSpec.
pub fn identity_summary(spec: &ActionSpec) -> BTreeMap<&'static str, usize> {
    BTreeMap::from([
        ("inputs", spec.inputs.len()),
        ("code", spec.code.len()),
        ("outputs", spec.outputs.len()),
        ("argv", spec.command.len()),
    ])
}
