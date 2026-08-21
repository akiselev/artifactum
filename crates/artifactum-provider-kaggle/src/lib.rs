use artifactum_provider_command::CommandProvider;
#[must_use]
pub fn provider() -> CommandProvider {
    CommandProvider::new(
        "kaggle",
        &["kaggle"],
        "kaggle",
        &[
            "datasets",
            "download",
            "-d",
            "{locator}",
            "-p",
            "{destination}",
        ],
    )
}
