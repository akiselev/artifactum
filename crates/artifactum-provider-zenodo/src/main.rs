use artifactum_provider_zenodo::provider;#[tokio::main]async fn main()->Result<(),Box<dyn std::error::Error>>{artifactum_plugin_protocol::serve(provider()).await?;Ok(())}
