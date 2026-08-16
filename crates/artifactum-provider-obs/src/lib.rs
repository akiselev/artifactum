//! Artifactum obs provider backed by OpenDAL.
use artifactum_provider_opendal::{LocatorMode,OpenDalProvider};
#[must_use] pub fn provider()->OpenDalProvider{OpenDalProvider::new("obs",&["obs"],"obs",LocatorMode::Authority{config_key:"bucket",revision_key:None})}
