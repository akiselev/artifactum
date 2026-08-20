//! Producer-neutral research receipt contracts over Artifactum identities.
//!
//! The envelope records an immutable activity. Its generic payload remains
//! producer-owned: a Lean proof check, Solverang solve and Sinbad simulation do
//! not become variants of one universal scientific result enum.

use artifactum_core::{ActionKey, ArtifactId, Digest, Metadata};
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use sha2::{Digest as _, Sha256};
use std::collections::BTreeMap;
use thiserror::Error;

pub const RECEIPT_ENVELOPE_SCHEMA: &str = "artifactum-receipt-envelope-v1";
pub const SOURCE_ANCHOR_SCHEMA: &str = "artifactum-source-anchor-v1";

#[derive(Debug, Error)]
pub enum ContractError {
    #[error("canonical receipt serialization failed: {0}")]
    Canonical(#[from] serde_json::Error),
    #[error("receipt id mismatch: expected {expected}, observed {observed}")]
    ReceiptIdMismatch { expected: Digest, observed: Digest },
    #[error("receipt interval is invalid: finished_at precedes started_at")]
    InvalidInterval,
    #[error("schema name must not be empty")]
    EmptySchemaName,
    #[error("producer field `{0}` must not be empty")]
    EmptyProducerField(&'static str),
}

pub type Result<T, E = ContractError> = std::result::Result<T, E>;

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct SchemaIdentity {
    pub name: String,
    pub version: u32,
    pub digest: Digest,
}

impl SchemaIdentity {
    pub fn validate(&self) -> Result<()> {
        if self.name.trim().is_empty() {
            return Err(ContractError::EmptySchemaName);
        }
        Ok(())
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(transparent)]
pub struct ReceiptId(pub Digest);

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ProducerIdentity {
    pub repository: String,
    pub commit: String,
    pub package: String,
    pub package_version: String,
    pub executable: ArtifactId,
}

impl ProducerIdentity {
    pub fn validate(&self) -> Result<()> {
        for (name, value) in [
            ("repository", self.repository.as_str()),
            ("commit", self.commit.as_str()),
            ("package", self.package.as_str()),
            ("package_version", self.package_version.as_str()),
        ] {
            if value.trim().is_empty() {
                return Err(ContractError::EmptyProducerField(name));
            }
        }
        Ok(())
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ActivityIdentity {
    pub action: ActionKey,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub attempt: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub parent: Option<ReceiptId>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ExecutionEnvironment {
    pub platform: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub container: Option<ArtifactId>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub environment_lock: Option<ArtifactId>,
    #[serde(default)]
    pub runtimes: BTreeMap<String, String>,
    #[serde(default)]
    pub metadata: Metadata,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ArtifactBinding {
    pub role: String,
    pub artifact: ArtifactId,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub member: Option<String>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct RecordedCommand {
    pub argv: Vec<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub working_directory: Option<String>,
    #[serde(default)]
    pub declared_environment: BTreeMap<String, String>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum DiagnosticSeverity {
    Info,
    Warning,
    Error,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct PortableDiagnostic {
    pub code: String,
    pub severity: DiagnosticSeverity,
    pub message: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub source: Option<SourceAnchor>,
    #[serde(default)]
    pub details: Metadata,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum SourceAnchor {
    Repository {
        repository: String,
        commit: String,
        path: String,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        start_line: Option<u32>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        end_line: Option<u32>,
    },
    Paper {
        citation: String,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        doi: Option<String>,
        locator: String,
    },
    Dataset {
        snapshot: ArtifactId,
        locator: String,
    },
    Lean {
        environment: ArtifactId,
        declaration: String,
    },
    Artifact {
        artifact: ArtifactId,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        member: Option<String>,
    },
    Generated {
        artifact: ArtifactId,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        span: Option<String>,
    },
}

/// Common outer activity record. `payload` is versioned by `schema` and owned
/// by the producing system. Atlas or another consumer may separately interpret
/// the receipt as evidence for one or more scoped claims.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ReceiptEnvelope<P> {
    pub schema: SchemaIdentity,
    pub receipt_id: ReceiptId,
    pub producer: ProducerIdentity,
    pub activity: ActivityIdentity,
    pub environment: ExecutionEnvironment,
    #[serde(default)]
    pub inputs: Vec<ArtifactBinding>,
    #[serde(default)]
    pub outputs: Vec<ArtifactBinding>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub command: Option<RecordedCommand>,
    #[serde(default)]
    pub diagnostics: Vec<PortableDiagnostic>,
    pub started_at: DateTime<Utc>,
    pub finished_at: DateTime<Utc>,
    pub payload: P,
}

impl<P> ReceiptEnvelope<P>
where
    P: Serialize,
{
    pub fn calculate_id(&self) -> Result<ReceiptId> {
        #[derive(Serialize)]
        struct Identity<'a, P> {
            schema: &'a SchemaIdentity,
            producer: &'a ProducerIdentity,
            activity: &'a ActivityIdentity,
            environment: &'a ExecutionEnvironment,
            inputs: &'a [ArtifactBinding],
            outputs: &'a [ArtifactBinding],
            command: &'a Option<RecordedCommand>,
            diagnostics: &'a [PortableDiagnostic],
            started_at: &'a DateTime<Utc>,
            finished_at: &'a DateTime<Utc>,
            payload: &'a P,
        }

        let identity = Identity {
            schema: &self.schema,
            producer: &self.producer,
            activity: &self.activity,
            environment: &self.environment,
            inputs: &self.inputs,
            outputs: &self.outputs,
            command: &self.command,
            diagnostics: &self.diagnostics,
            started_at: &self.started_at,
            finished_at: &self.finished_at,
            payload: &self.payload,
        };
        let bytes = serde_json::to_vec(&identity)?;
        let value = hex::encode(Sha256::digest(bytes));
        Ok(ReceiptId(Digest::sha256(value).expect("sha256 output is valid")))
    }

    pub fn validate(&self) -> Result<()> {
        self.schema.validate()?;
        self.producer.validate()?;
        if self.finished_at < self.started_at {
            return Err(ContractError::InvalidInterval);
        }
        let expected = self.calculate_id()?;
        if expected != self.receipt_id {
            return Err(ContractError::ReceiptIdMismatch {
                expected: expected.0,
                observed: self.receipt_id.0.clone(),
            });
        }
        Ok(())
    }

    pub fn refresh_id(&mut self) -> Result<()> {
        self.receipt_id = self.calculate_id()?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use artifactum_core::{ActionSpec, OutputSpec};
    use chrono::TimeZone;
    use serde_json::json;

    fn digest(byte: char) -> Digest {
        Digest::sha256(byte.to_string().repeat(64)).unwrap()
    }

    fn artifact(byte: char) -> ArtifactId {
        ArtifactId(digest(byte))
    }

    fn fixture() -> ReceiptEnvelope<serde_json::Value> {
        let action = ActionSpec::command("fixture", vec!["fixture".into()]);
        let mut receipt = ReceiptEnvelope {
            schema: SchemaIdentity {
                name: "solverang-run-v1".into(),
                version: 1,
                digest: digest('1'),
            },
            receipt_id: ReceiptId(digest('0')),
            producer: ProducerIdentity {
                repository: "akiselev/solverang".into(),
                commit: "abc123".into(),
                package: "solverang".into(),
                package_version: "0.1.0".into(),
                executable: artifact('2'),
            },
            activity: ActivityIdentity {
                action: action.key().unwrap(),
                attempt: Some("attempt-1".into()),
                parent: None,
            },
            environment: ExecutionEnvironment {
                platform: "x86_64-unknown-linux-gnu".into(),
                container: None,
                environment_lock: Some(artifact('3')),
                runtimes: BTreeMap::from([("rust".into(), "1.97.1".into())]),
                metadata: Metadata::new(),
            },
            inputs: vec![ArtifactBinding {
                role: "problem".into(),
                artifact: artifact('4'),
                member: None,
            }],
            outputs: vec![ArtifactBinding {
                role: "result".into(),
                artifact: artifact('5'),
                member: None,
            }],
            command: Some(RecordedCommand {
                argv: vec!["solverang".into(), "solve".into()],
                working_directory: None,
                declared_environment: BTreeMap::new(),
            }),
            diagnostics: vec![],
            started_at: Utc.with_ymd_and_hms(2026, 8, 19, 1, 2, 3).unwrap(),
            finished_at: Utc.with_ymd_and_hms(2026, 8, 19, 1, 2, 4).unwrap(),
            payload: json!({"converged": true}),
        };
        receipt.refresh_id().unwrap();
        receipt
    }

    #[test]
    fn canonical_id_roundtrips() {
        let receipt = fixture();
        receipt.validate().unwrap();
        let encoded = serde_json::to_vec(&receipt).unwrap();
        let decoded: ReceiptEnvelope<serde_json::Value> =
            serde_json::from_slice(&encoded).unwrap();
        assert_eq!(decoded, receipt);
        decoded.validate().unwrap();
    }

    #[test]
    fn payload_change_changes_identity() {
        let mut receipt = fixture();
        let original = receipt.receipt_id.clone();
        receipt.payload = json!({"converged": false});
        assert!(matches!(
            receipt.validate(),
            Err(ContractError::ReceiptIdMismatch { .. })
        ));
        receipt.refresh_id().unwrap();
        assert_ne!(receipt.receipt_id, original);
    }

    #[test]
    fn invalid_interval_is_rejected() {
        let mut receipt = fixture();
        std::mem::swap(&mut receipt.started_at, &mut receipt.finished_at);
        receipt.refresh_id().unwrap();
        assert!(matches!(receipt.validate(), Err(ContractError::InvalidInterval)));
    }

    #[test]
    fn source_anchor_roundtrips_without_interpreting_truth() {
        let anchor = SourceAnchor::Paper {
            citation: "Example et al. (2026)".into(),
            doi: Some("10.0000/example".into()),
            locator: "Eq. (7)".into(),
        };
        let encoded = serde_json::to_string(&anchor).unwrap();
        assert_eq!(serde_json::from_str::<SourceAnchor>(&encoded).unwrap(), anchor);
    }

    #[test]
    fn action_output_schema_remains_artifactum_owned() {
        let output = OutputSpec::blob();
        assert!(matches!(output.kind, artifactum_core::ContentKind::Blob));
    }
}
