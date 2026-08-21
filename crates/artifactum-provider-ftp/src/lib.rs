use artifactum_provider_command::CommandProvider;
#[must_use]
pub fn provider() -> CommandProvider {
    CommandProvider::new(
        "ftp",
        &["ftp", "ftps"],
        "rclone",
        &["cat", "{profile_remote}:{locator}"],
    )
}
