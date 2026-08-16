use artifactum_provider_command::CommandProvider;
#[must_use] pub fn provider()->CommandProvider{CommandProvider::new("mlflow",&["mlflow"],"mlflow",&["artifacts","download","--artifact-uri","{locator}","--dst-path","{destination}"])}
