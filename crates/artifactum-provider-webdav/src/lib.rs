use artifactum_provider_command::CommandProvider;
#[must_use] pub fn provider()->CommandProvider{CommandProvider::new("webdav",&["webdav"],"rclone",&["cat","{profile_remote}:{locator}"])}
