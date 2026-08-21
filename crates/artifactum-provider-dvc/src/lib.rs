use artifactum_provider_command::CommandProvider;
#[must_use]
pub fn provider() -> CommandProvider {
    CommandProvider::new(
        "dvc",
        &["dvc"],
        "dvc",
        &["get-url", "{locator}", "-o", "{destination}"],
    )
}
