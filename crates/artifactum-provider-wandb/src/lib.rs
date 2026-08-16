use artifactum_provider_command::CommandProvider;
#[must_use] pub fn provider()->CommandProvider{CommandProvider::new("wandb",&["wandb"],"wandb",&["artifact","get","{locator}","--root","{destination}"])}
