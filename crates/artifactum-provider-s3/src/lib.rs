//! Artifactum s3 provider backed by OpenDAL.
use artifactum_provider_opendal::{LocatorMode,OpenDalProvider};
#[must_use] pub fn provider()->OpenDalProvider{OpenDalProvider::new("s3",&["s3"],"s3",LocatorMode::Authority{config_key:"bucket",revision_key:None}).object_version(true)}
