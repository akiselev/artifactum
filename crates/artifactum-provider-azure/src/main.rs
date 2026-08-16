#[tokio::main]
async fn main()->anyhow::Result<()>{artifactum_plugin_protocol::serve(artifactum_provider_azure::provider()).await?;Ok(())}
