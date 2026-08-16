#[tokio::main]
async fn main()->anyhow::Result<()>{artifactum_plugin_protocol::serve(artifactum_provider_dropbox::provider()).await?;Ok(())}
