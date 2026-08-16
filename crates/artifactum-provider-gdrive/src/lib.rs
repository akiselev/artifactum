//! Artifactum gdrive provider backed by OpenDAL.
use artifactum_provider_opendal::{LocatorMode,OpenDalProvider};
#[must_use] pub fn provider()->OpenDalProvider{OpenDalProvider::new("gdrive",&["gdrive"],"gdrive",LocatorMode::Path)}
