use artifactum_provider_command::CommandProvider;
#[must_use]
pub fn provider() -> CommandProvider {
    CommandProvider::new(
        "swift",
        &["swift"],
        "rclone",
        &["cat", "{profile_remote}:{locator}"],
    )
}
