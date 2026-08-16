use artifactum_provider_command::CommandProvider;
#[must_use] pub fn provider()->CommandProvider{CommandProvider::new("modelscope",&["modelscope"],"modelscope",&["download","--model","{locator}","--local_dir","{destination}"])}
