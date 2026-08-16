//! Artifactum cos provider backed by OpenDAL.
use artifactum_provider_opendal::{LocatorMode,OpenDalProvider};
#[must_use] pub fn provider()->OpenDalProvider{OpenDalProvider::new("cos",&["cos"],"cos",LocatorMode::Authority{config_key:"bucket",revision_key:None})}
