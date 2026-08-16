//! Artifactum lakefs provider backed by OpenDAL.
use artifactum_provider_opendal::{LocatorMode,OpenDalProvider};
#[must_use] pub fn provider()->OpenDalProvider{OpenDalProvider::new("lakefs",&["lakefs"],"lakefs",LocatorMode::Authority{config_key:"repository",revision_key:Some("branch")})}
