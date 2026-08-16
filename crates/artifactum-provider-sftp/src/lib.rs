//! Artifactum sftp provider backed by OpenDAL.
use artifactum_provider_opendal::{LocatorMode,OpenDalProvider};
#[must_use] pub fn provider()->OpenDalProvider{OpenDalProvider::new("sftp",&["sftp"],"sftp",LocatorMode::Path)}
