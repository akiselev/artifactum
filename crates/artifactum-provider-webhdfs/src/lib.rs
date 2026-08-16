use artifactum_provider_command::CommandProvider;
#[must_use] pub fn provider()->CommandProvider{CommandProvider::new("webhdfs",&["webhdfs"],"rclone",&["cat","{profile_remote}:{locator}"])}
