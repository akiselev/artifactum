use artifactum_provider_command::CommandProvider;
#[must_use]
pub fn provider() -> CommandProvider {
    CommandProvider::new(
        "comet",
        &["comet"],
        "cometx",
        &[
            "artifact",
            "download",
            "{locator}",
            "--output",
            "{destination}",
        ],
    )
}
