use artifactum_provider_command::CommandProvider;
#[must_use] pub fn provider()->CommandProvider{CommandProvider::new("sftp",&["sftp"],"rclone",&["cat","{profile_remote}:{locator}"])}
