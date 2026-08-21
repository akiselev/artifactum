use artifactum_provider_command::CommandProvider;
#[must_use]
pub fn provider() -> CommandProvider {
    CommandProvider::new(
        "onedrive",
        &["onedrive"],
        "rclone",
        &["cat", "{profile_remote}:{locator}"],
    )
}
