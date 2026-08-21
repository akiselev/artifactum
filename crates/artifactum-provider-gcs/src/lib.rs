use artifactum_provider_opendal::{LocatorMode, OpenDalProvider};
#[must_use]
pub fn provider() -> OpenDalProvider {
    OpenDalProvider::new(
        "gcs",
        &["gcs", "gs"],
        "gcs",
        LocatorMode::Authority {
            config_key: "bucket",
            revision_key: None,
        },
    )
    .object_version(true)
}
