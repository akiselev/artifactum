use artifactum_provider_command::CommandProvider;
#[must_use] pub fn provider()->CommandProvider{CommandProvider::new("clearml",&["clearml"],"clearml-data",&["get","--id","{locator}","--local-folder","{destination}"])}
