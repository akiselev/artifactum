use artifactum_provider_opendal::{LocatorMode,OpenDalProvider};
#[must_use] pub fn provider()->OpenDalProvider{OpenDalProvider::new("azure",&["azure","azblob"],"azblob",LocatorMode::Authority{config_key:"container",revision_key:None}).object_version(true)}
